"""
Integration test: runs the full pipeline (Bronze -> Silver -> Gold) on data/raw and checks
the results that do NOT depend on the execution date.

Some outputs depend on current_date() by design (age, tenure, months in the turnover table,
future-dated salaries becoming current, the under-18 rule...). Those are intentionally not
asserted with exact values, so the test stays stable over time.
"""

import csv
from pathlib import Path

import pytest
from pyspark.sql.functions import countDistinct

pytestmark = pytest.mark.integration

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

SILVER_ENTITIES = [
    "departments", "positions", "employees", "contracts", "salary_history",
    "salaries", "trainings", "performance_reviews", "job_postings", "candidates",
]

GOLD_TABLES = [
    "gold_dim_departments", "gold_dim_positions", "gold_dim_employees", "gold_dim_date",
    "gold_fact_current_salaries", "gold_fact_salary_history", "gold_fact_performance_reviews",
    "gold_fact_recruitment_funnel", "gold_agg_monthly_headcount_turnover",
    "gold_agg_turnover_data_quality", "gold_agg_recruitment_funnel_kpis",
    "gold_agg_compensation_equity", "gold_agg_salary_coverage_data_quality",
    "gold_agg_referential_integrity_data_quality", "gold_agg_recruitment_coverage_data_quality",
    "gold_fact_candidate_stage_history",
]


def _csv_records(table: str) -> int:
    with open(RAW / f"rrhh_{table}.csv", newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def _t(pipeline, name):
    return pipeline.table(f"hr_database.{name}")


# ---------------------------------------------------------------------------- Bronze

@pytest.mark.parametrize("table", SILVER_ENTITIES)
def test_bronze_has_one_row_per_logical_csv_record(pipeline, table):
    """multiLine reading: quoted values with line breaks must not split records."""
    assert _t(pipeline, f"bronze.bronze_{table}").count() == _csv_records(table)


# ---------------------------------------------------------------------------- Silver

def test_no_crammed_records_left_in_quarantine(pipeline):
    for table in SILVER_ENTITIES:
        if table == "trainings":  # no quarantine table by design
            continue
        quarantine = _t(pipeline, f"silver.silver_quarantine_{table}")
        assert quarantine.filter("rejection_reason like 'potential_crammed%'").count() == 0, table


@pytest.mark.parametrize(
    "table, pk",
    [("salary_history", "history_id"), ("salaries", "salary_id"), ("performance_reviews", "review_id")],
)
def test_null_pk_records_are_recovered_with_surrogate_keys(pipeline, table, pk):
    assert _t(pipeline, f"silver.silver_quarantine_{table}").filter(f"rejection_reason = '{pk}_null'").count() == 0
    assert _t(pipeline, f"silver.silver_{table}").filter(f"{pk} like 'SK-%'").count() > 0


def test_legacy_ids_are_normalized(pipeline):
    for table, pk in [("salary_history", "history_id"), ("performance_reviews", "review_id"), ("job_postings", "posting_id")]:
        assert _t(pipeline, f"silver.silver_{table}").filter(f"try_cast({pk} AS long) >= 100000").count() == 0, table
    # legacy posting 100009 is posting 10 (it used to overwrite posting 9)
    postings = {r["posting_id"] for r in _t(pipeline, "silver.silver_job_postings").select("posting_id").collect()}
    assert "10" in postings
    quarantined = {r["posting_id"] for r in _t(pipeline, "silver.silver_quarantine_job_postings").select("posting_id").collect()}
    assert "9" in quarantined  # negative salary_max


def test_contracts_with_null_employee_are_quarantined_not_lost(pipeline):
    quarantine = _t(pipeline, "silver.silver_quarantine_contracts")
    assert quarantine.filter("rejection_reason = 'employee_id_null'").count() == 37


def test_ambiguous_slash_dates_are_read_as_month_day(pipeline):
    hire = {r["employee_id"]: str(r["hire_date"]) for r in _t(pipeline, "silver.silver_employees").filter("employee_id in ('15', '104')").collect()}
    assert hire == {"15": "2023-02-05", "104": "2019-10-07"}


def test_categories_are_normalized(pipeline):
    employees = _t(pipeline, "silver.silver_employees")
    assert {r[0] for r in employees.select("employment_type").distinct().collect()} <= {"full_time", "part_time", "contractor", "unknown"}
    assert {r[0] for r in employees.select("status").distinct().collect()} <= {"active", "on_leave", "terminated", "unknown"}
    sources = {r[0] for r in _t(pipeline, "silver.silver_candidates").select("source").distinct().collect()}
    assert "LinkedIn" in sources and "Linkedin" not in sources


def test_negative_salaries_are_quarantined_by_business_rule(pipeline):
    quarantine = _t(pipeline, "silver.silver_quarantine_salaries")
    assert quarantine.filter("rejection_reason = 'base_salary_negative'").count() == 4


def test_performance_ratings_na_is_null_not_zero(pipeline):
    reviews = _t(pipeline, "silver.silver_performance_reviews")
    assert reviews.filter("overall_rating < 0 OR overall_rating > 5").count() == 0


# ---------------------------------------------------------------------------- Gold

@pytest.mark.parametrize("table", GOLD_TABLES)
def test_gold_tables_exist_and_are_not_empty(pipeline, table):
    assert _t(pipeline, f"gold.{table}").count() > 0


def test_terminated_employees_are_kept_and_flagged(pipeline):
    terminated = _t(pipeline, "gold.gold_dim_employees").filter("status = 'terminated'")
    assert terminated.count() == 50
    assert terminated.filter("termination_date_missing").count() == 43
    assert terminated.filter("termination_date_missing AND tenure_years IS NOT NULL").count() == 0

    total = _t(pipeline, "gold.gold_agg_turnover_data_quality").filter("department_id = 'TOTAL'").collect()[0]
    assert (total["total_terminations"], total["terminations_without_date"]) == (50, 43)


def test_turnover_table_includes_months_without_terminations(pipeline):
    turnover = _t(pipeline, "gold.gold_agg_monthly_headcount_turnover")
    assert turnover.agg({"monthly_terminations": "sum"}).collect()[0][0] == 7
    assert turnover.filter("monthly_terminations = 0").count() > 0
    assert turnover.filter("monthly_terminations = 0 AND avg_tenure_years IS NOT NULL").count() == 0


def test_gold_employees_do_not_expose_personal_contact_data(pipeline):
    columns = set(_t(pipeline, "gold.gold_dim_employees").columns)
    assert not {"email", "phone", "birth_date"} & columns
    assert "age" in columns


def test_compa_ratio_is_never_imputed(pipeline):
    equity = _t(pipeline, "gold.gold_agg_compensation_equity")
    assert equity.filter("avg_compa_ratio = 0").count() == 0
    assert equity.filter("band_incomplete AND avg_compa_ratio IS NOT NULL").count() == 0
    incomplete = {r[0] for r in _t(pipeline, "gold.gold_dim_positions").filter("band_incomplete").select("position_title").collect()}
    assert incomplete == {"Junior Developer", "Financial Analyst", "Senior Developer"}


def test_current_salaries_are_never_future_dated(pipeline):
    salaries = _t(pipeline, "gold.gold_fact_current_salaries")
    assert salaries.filter("effective_date > current_date()").count() == 0
    assert salaries.groupBy("employee_id").count().filter("count > 1").count() == 0


def test_time_to_hire_only_uses_hired_candidates(pipeline):
    kpis = _t(pipeline, "gold.gold_agg_recruitment_funnel_kpis")
    assert kpis.filter("total_hired = 0 AND avg_time_to_hire_days IS NOT NULL").count() == 0
    # time to fill is a position-level metric: one value per position
    per_position = kpis.groupBy("position_id").agg(countDistinct("avg_time_to_fill_days").alias("n"))
    assert per_position.filter("n > 1").count() == 0


# ---------------------------------------------------------------------------- Star schema integrity

EMPLOYEE_FKS = [
    ("gold_dim_employees", "manager_id"),
    ("gold_fact_current_salaries", "employee_id"),
    ("gold_fact_salary_history", "employee_id"),
    ("gold_fact_salary_history", "approved_by"),
    ("gold_fact_performance_reviews", "employee_id"),
    ("gold_fact_performance_reviews", "reviewer_id"),
]

DATE_FKS = [
    ("gold_dim_employees", "hire_date"),
    ("gold_dim_employees", "termination_date"),
    ("gold_fact_current_salaries", "effective_date"),
    ("gold_fact_salary_history", "effective_date"),
    ("gold_fact_performance_reviews", "review_date"),
    ("gold_fact_recruitment_funnel", "applied_date"),
    ("gold_fact_candidate_stage_history", "stage_entered_date"),
]


def test_unknown_employee_member_exists_and_is_excluded_from_kpis(pipeline):
    employees = _t(pipeline, "gold.gold_dim_employees")
    unknown = employees.filter("employee_id = '-1'").collect()
    assert len(unknown) == 1 and unknown[0]["is_unknown_member"] and unknown[0]["status"] == "unknown"
    assert employees.filter("is_unknown_member").count() == 1
    # the Unknown member never counts as an active employee
    coverage = _t(pipeline, "gold.gold_agg_salary_coverage_data_quality").filter("department_id = 'TOTAL'").collect()[0]
    assert coverage["active_employees"] == employees.filter("status IN ('active', 'on_leave')").count()


@pytest.mark.parametrize("table, column", EMPLOYEE_FKS)
def test_employee_foreign_keys_always_resolve(pipeline, table, column):
    keys = _t(pipeline, "gold.gold_dim_employees").select("employee_id")
    child = _t(pipeline, f"gold.{table}").filter(f"{column} IS NOT NULL")
    assert child.join(keys, child[column] == keys["employee_id"], "left_anti").count() == 0
    # the original value is kept for traceability
    assert f"source_{column}" in child.columns


@pytest.mark.parametrize("table, column", DATE_FKS)
def test_dates_are_covered_by_dim_date(pipeline, table, column):
    dates = _t(pipeline, "gold.gold_dim_date").select("date_key")
    child = _t(pipeline, f"gold.{table}").filter(f"{column} IS NOT NULL")
    assert child.join(dates, child[column] == dates["date_key"], "left_anti").count() == 0


def test_positions_resolve_to_departments_by_id(pipeline):
    positions = _t(pipeline, "gold.gold_dim_positions")
    departments = _t(pipeline, "gold.gold_dim_departments").select("department_id")
    assert positions.join(departments, "department_id", "left_anti").count() == 0
    unresolved = {r["department_name"] for r in positions.filter("NOT department_resolved").collect()}
    assert unresolved == {"Producto", "Ejecutivo"}


def test_referential_integrity_data_quality_table(pipeline):
    rows = {
        (r["table_name"], r["column_name"]): r["unresolved_rows"]
        for r in _t(pipeline, "gold.gold_agg_referential_integrity_data_quality").collect()
    }
    assert rows[("gold_fact_salary_history", "employee_id")] == 83
    assert rows[("gold_fact_performance_reviews", "employee_id")] == 81
    assert rows[("gold_fact_current_salaries", "employee_id")] == 0
    assert all(rows[(t, c)] == 0 for t, c in DATE_FKS)
    assert rows[("gold_dim_positions", "department_id")] == 2


def test_recruitment_funnel_grain_is_one_row_per_candidate(pipeline):
    funnel = _t(pipeline, "gold.gold_fact_recruitment_funnel")
    assert funnel.count() == funnel.select("candidate_id").distinct().count()


def test_recruitment_coverage_data_quality_table(pipeline):
    coverage = {
        r["exclusion_reason"]: (r["candidates"], r["hired_candidates"])
        for r in _t(pipeline, "gold.gold_agg_recruitment_coverage_data_quality").collect()
    }
    candidates = {reason: n for reason, (n, _) in coverage.items()}
    assert candidates == {
        "quarantined_in_silver": 9,
        "posting_quarantined": 39,
        "applied_before_posting": 26,
        "applied_after_closing": 1,
        "included_in_funnel": 25,
        "TOTAL": 100,
    }
    # the classification mirrors the filter of the fact table
    funnel = _t(pipeline, "gold.gold_fact_recruitment_funnel")
    assert coverage["included_in_funnel"] == (funnel.count(), funnel.filter("candidate_status = 'hired'").count())
    assert coverage["TOTAL"][1] == sum(h for reason, (_, h) in coverage.items() if reason != "TOTAL")
