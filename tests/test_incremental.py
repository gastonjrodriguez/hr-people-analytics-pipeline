"""Tests for incremental loading (v3): simulated deliveries, batch selection and Bronze history."""

import csv
import filecmp
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW = REPO_ROOT / "data" / "raw"
BATCHES = REPO_ROOT / "data" / "batches"
BATCH_DATES = ["2026-08-01", "2026-09-01", "2026-10-01"]


def _load_generator():
    spec = importlib.util.spec_from_file_location("generate_batches", REPO_ROOT / "scripts" / "generate_batches.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _csv_records(path: Path) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def _t(spark, name):
    return spark.table(f"hr_database.{name}")


# ---------------------------------------------------------------------------- simulated deliveries

def test_committed_batches_match_the_generator(tmp_path, monkeypatch):
    """data/batches must be exactly what scripts/generate_batches.py produces (deterministic)."""
    generator = _load_generator()
    monkeypatch.setattr(generator, "OUT_DIR", tmp_path / "batches")
    generator.main()
    for batch_date in BATCH_DATES:
        folder = f"batch_date={batch_date}"
        names = sorted(p.name for p in (BATCHES / folder).glob("*.csv"))
        assert len(names) == 17
        _, mismatch, errors = filecmp.cmpfiles(BATCHES / folder, tmp_path / "batches" / folder, names, shallow=False)
        assert not mismatch and not errors, (folder, mismatch, errors)


def test_first_batch_is_the_original_extract():
    for path in RAW.glob("rrhh_*.csv"):
        assert filecmp.cmp(path, BATCHES / f"batch_date={BATCH_DATES[0]}" / path.name, shallow=False), path.name


def test_generator_rejects_ambiguous_keys():
    generator = _load_generator()
    header = "id,name"
    records = ["1,Ana", "1,Ana bis", "2,Luis"]
    with pytest.raises(AssertionError):
        generator.apply_changes(header, records, [("update", "t", "1", {"name": "X"})])
    # legacy IDs are matched after normalization (100001 -> 2)
    assert generator.apply_changes(header, ["100001,Luis"], [("update", "t", "2", {"name": "Lu"})]) == ["100001,Lu"]


# ---------------------------------------------------------------------------- batch selection

def test_next_batch_picks_the_oldest_pending_batch(utils):
    next_batch = utils["next_batch"]
    assert next_batch(BATCH_DATES, []) == ("2026-08-01", False, 2)
    assert next_batch(BATCH_DATES, ["2026-08-01"]) == ("2026-09-01", False, 1)
    # nothing pending: the latest batch is processed again (idempotent)
    assert next_batch(BATCH_DATES, BATCH_DATES) == ("2026-10-01", True, 0)
    with pytest.raises(RuntimeError):
        next_batch([], [])


def test_list_batches_ignores_other_files(utils, tmp_path):
    for name in ["batch_date=2026-09-01", "batch_date=2026-08-01", "old_files"]:
        (tmp_path / name).mkdir()
    (tmp_path / "rrhh_employees.csv").write_text("x")
    assert utils["list_batches"](str(tmp_path)) == ["2026-08-01", "2026-09-01"]


# ---------------------------------------------------------------------------- pipeline over 3 batches


@pytest.mark.integration
@pytest.mark.parametrize("table", ["employees", "candidates", "contracts", "job_postings", "trainings"])
def test_bronze_keeps_every_batch_once(pipeline_all_batches, table):
    """Bronze is append-only: one copy of every delivery, even after re-processing the last one."""
    bronze = _t(pipeline_all_batches, f"bronze.bronze_{table}")
    per_batch = {str(r["_batch_date"]): r["count"] for r in bronze.groupBy("_batch_date").count().collect()}
    expected = {d: _csv_records(BATCHES / f"batch_date={d}" / f"rrhh_{table}.csv") for d in BATCH_DATES}
    assert per_batch == expected


@pytest.mark.integration
def test_batch_control_records_every_layer(pipeline_all_batches):
    control = _t(pipeline_all_batches, "monitoring.batch_control").filter("status = 'SUCCESS'")
    for layer in ["bronze", "silver", "gold"]:
        dates = sorted({r["batch_date"] for r in control.filter(f"layer = '{layer}'").collect()})
        assert dates == BATCH_DATES, layer


@pytest.mark.integration
def test_silver_reflects_the_latest_batch(pipeline_all_batches):
    employees = {r["employee_id"]: r for r in _t(pipeline_all_batches, "silver.silver_employees").collect()}
    assert employees["166"]["status"] == "terminated"
    assert employees["7"]["position_id"] == "18"
    assert "501" in employees

    candidates = {r["candidate_id"]: r["status"] for r in _t(pipeline_all_batches, "silver.silver_candidates").collect()}
    assert candidates["101"] == "hired"
    assert "106" not in candidates  # removed from the source in the last delivery

    postings = {r["posting_id"] for r in _t(pipeline_all_batches, "silver.silver_job_postings").collect()}
    assert {"5", "13"} <= postings  # posting 5 left quarantine when the source loaded its closing date


@pytest.mark.integration
def test_gold_is_rebuilt_from_the_latest_batch(pipeline_all_batches):
    incomplete = {r[0] for r in _t(pipeline_all_batches, "gold.gold_dim_positions").filter("band_incomplete").select("position_title").collect()}
    assert incomplete == {"Financial Analyst", "Senior Developer"}  # Junior Developer band completed in batch 3


# ---------------------------------------------------------------------------- load strategies (unit)

def _rows(df, *cols):
    return sorted(tuple(None if r[c] is None else str(r[c]) for c in cols) for r in df.collect())


def test_scd2_next_state(spark_session, utils):
    scd2 = utils["scd2_next_state"]
    schema = "id string, status string, _ingested_at string"
    b1 = spark_session.createDataFrame([("1", "active", "t1"), ("2", "active", "t1"), ("3", "active", "t1")], schema)
    b2 = spark_session.createDataFrame([("1", "active", "t2"), ("2", "terminated", "t2"), ("4", "active", "t2")], schema)
    b3 = spark_session.createDataFrame([("1", "active", "t3"), ("2", "terminated", "t3"), ("3", "active", "t3"), ("4", "active", "t3")], schema)

    h1 = scd2(None, b1, "id", "2026-08-01").localCheckpoint()
    h2 = scd2(h1, b2, "id", "2026-09-01").localCheckpoint()
    cols = ("id", "status", "_valid_from", "_valid_to", "_is_current", "_deleted_in_source")
    assert _rows(h2, *cols) == sorted([
        ("1", "active", "2026-08-01", None, "True", "False"),           # unchanged (metadata changes ignored)
        ("2", "active", "2026-08-01", "2026-09-01", "False", "False"),  # changed: old version closed
        ("2", "terminated", "2026-09-01", None, "True", "False"),       # ... and new version opened
        ("3", "active", "2026-08-01", "2026-09-01", "False", "True"),   # missing from the batch
        ("4", "active", "2026-09-01", None, "True", "False"),           # new key
    ])
    # re-applying the same batch changes nothing
    assert _rows(scd2(h2, b2, "id", "2026-09-01"), *cols) == _rows(h2, *cols)
    # a key that comes back gets a new current version
    h3 = scd2(h2, b3, "id", "2026-10-01")
    assert ("3", "active", "2026-10-01", None, "True", "False") in _rows(h3, *cols)
    assert h3.filter("_is_current").groupBy("id").count().filter("count > 1").count() == 0


def test_merge_next_state(spark_session, utils):
    merge = utils["merge_next_state"]
    schema = "id string, amount int, _ingested_at string"
    b1 = spark_session.createDataFrame([("1", 10, "t1"), ("2", 20, "t1")], schema)
    b2 = spark_session.createDataFrame([("1", 10, "t2"), ("3", 30, "t2")], schema)
    b3 = spark_session.createDataFrame([("1", 15, "t3"), ("2", 20, "t3"), ("3", 30, "t3")], schema)

    t1 = merge(None, b1, "id", "2026-08-01").localCheckpoint()
    t2 = merge(t1, b2, "id", "2026-09-01").localCheckpoint()
    cols = ("id", "amount", "_deleted_in_source", "_last_changed_batch")
    assert _rows(t2, *cols) == sorted([
        ("1", "10", "False", "2026-08-01"),  # unchanged: keeps its original batch
        ("2", "20", "True", "2026-09-01"),   # missing from the batch: flagged, not deleted
        ("3", "30", "False", "2026-09-01"),  # new
    ])
    assert _rows(merge(t2, b2, "id", "2026-09-01"), *cols) == _rows(t2, *cols)  # idempotent
    t3 = merge(t2, b3, "id", "2026-10-01")
    assert _rows(t3, *cols) == sorted([
        ("1", "15", "False", "2026-10-01"),  # changed
        ("2", "20", "False", "2026-10-01"),  # back in the source
        ("3", "30", "False", "2026-09-01"),
    ])


def test_write_silver_rejects_duplicated_keys(spark_session, utils):
    df = spark_session.createDataFrame([("1", "a"), ("1", "b")], "id string, v string")
    with pytest.raises(ValueError):
        utils["assert_unique_key"](df, "id", "t")


# ---------------------------------------------------------------------------- load strategies (pipeline)

CHANGED_EMPLOYEES = {"7", "21", "22", "28", "116", "120", "142", "166"}


@pytest.mark.integration
def test_employee_history_has_one_version_per_real_change(pipeline_all_batches):
    history = _t(pipeline_all_batches, "silver.silver_employees_history")
    versions = {r["employee_id"]: r["count"] for r in history.groupBy("employee_id").count().collect()}
    # only the employees changed by the simulated deliveries have more than one version
    assert {e for e, n in versions.items() if n > 1} == CHANGED_EMPLOYEES
    assert all(versions[e] == 2 for e in CHANGED_EMPLOYEES)
    # one current version per employee, and silver_employees is exactly the current versions
    current = history.filter("_is_current")
    assert current.count() == current.select("employee_id").distinct().count()
    assert current.count() == _t(pipeline_all_batches, "silver.silver_employees").count()

    e21 = {r["status"]: (str(r["_valid_from"]), str(r["_valid_to"])) for r in history.filter("employee_id = '21'").collect()}
    assert e21 == {"active": ("2026-08-01", "2026-09-01"), "terminated": ("2026-09-01", "None")}
    new_hire = history.filter("employee_id = '501'").collect()
    assert len(new_hire) == 1 and str(new_hire[0]["_valid_from"]) == "2026-10-01"


@pytest.mark.integration
def test_candidate_history_tracks_stages_and_removals(pipeline_all_batches):
    history = _t(pipeline_all_batches, "silver.silver_candidates_history")
    c101 = sorted((str(r["_valid_from"]), r["status"]) for r in history.filter("candidate_id = '101'").collect())
    assert c101 == [("2026-09-01", "interview"), ("2026-10-01", "hired")]
    c106 = history.filter("candidate_id = '106'").collect()
    assert len(c106) == 1 and c106[0]["_deleted_in_source"] and not c106[0]["_is_current"]
    assert str(c106[0]["_valid_to"]) == "2026-10-01"


@pytest.mark.integration
def test_band_change_is_versioned(pipeline_all_batches):
    history = _t(pipeline_all_batches, "silver.silver_positions_history").filter("position_id = '1'")
    assert sorted((str(r["_valid_from"]), r["max_salary"] is None) for r in history.collect()) == [("2026-08-01", True), ("2026-10-01", False)]


@pytest.mark.integration
def test_merged_event_tables(pipeline_all_batches):
    reviews = {r["review_id"]: r for r in _t(pipeline_all_batches, "silver.silver_performance_reviews").collect()}
    assert {str(reviews[i]["_last_changed_batch"]) for i in ["407", "408", "409"]} == {"2026-10-01"}
    assert not any(r["_deleted_in_source"] for r in reviews.values())
    batches = {str(r["_last_changed_batch"]) for i, r in reviews.items() if i not in {"407", "408", "409"}}
    assert batches == {"2026-08-01"}, "rows unchanged since batch 1 must not be rewritten"

    history = {r["history_id"]: str(r["_last_changed_batch"]) for r in _t(pipeline_all_batches, "silver.silver_salary_history").collect()}
    assert history["396"] == history["397"] == "2026-09-01"


@pytest.mark.integration
def test_quarantine_is_kept_per_batch(pipeline_all_batches):
    quarantine = _t(pipeline_all_batches, "silver.silver_quarantine_candidates")
    per_batch = {str(r["_batch_date"]): r["count"] for r in quarantine.groupBy("_batch_date").count().collect()}
    assert per_batch == {d: 9 for d in BATCH_DATES}


# ---------------------------------------------------------------------------- Gold over the history (v3)

@pytest.mark.integration
def test_termination_date_falls_back_to_the_detected_status_change(pipeline_all_batches):
    employees = {r["employee_id"]: r for r in _t(pipeline_all_batches, "gold.gold_dim_employees").collect()}
    assert (str(employees["21"]["termination_date"]), employees["21"]["termination_date_source"]) == ("2026-08-14", "contract")
    assert (str(employees["142"]["termination_date"]), employees["142"]["termination_date_source"]) == ("2026-09-01", "status_change_detected")
    assert not employees["142"]["termination_date_missing"] and employees["142"]["tenure_years"] is not None
    # employees already terminated in the first batch: no observed change, no invented date
    missing = [e for e in employees.values() if e["termination_date_missing"]]
    assert len(missing) == 43 and all(e["termination_date_source"] is None for e in missing)

    total = _t(pipeline_all_batches, "gold.gold_agg_turnover_data_quality").filter("department_id = 'TOTAL'").collect()[0]
    assert (total["total_terminations"], total["terminations_with_date"], total["terminations_without_date"]) == (54, 11, 43)


@pytest.mark.integration
def test_headcount_uses_the_department_at_each_date(pipeline_all_batches):
    turnover = _t(pipeline_all_batches, "gold.gold_agg_monthly_headcount_turnover")
    employees = _t(pipeline_all_batches, "gold.gold_dim_employees").filter("NOT termination_date_missing AND hire_date IS NOT NULL")
    end_of_august = "DATE'2026-08-31'"
    active = employees.filter(f"hire_date <= {end_of_august} AND (termination_date IS NULL OR termination_date > {end_of_august})")

    point_in_time = {r["department_id"]: r["active_headcount"] for r in turnover.filter("year = 2026 AND month = 8").collect()}
    current_dept = {r["department_id"]: r["count"] for r in active.groupBy("department_id").count().collect()}
    # monthly total unchanged; only the split by department changes
    assert sum(point_in_time.values()) == sum(current_dept.values())
    diff = {d: point_in_time.get(d, 0) - current_dept.get(d, 0) for d in set(point_in_time) | set(current_dept)}
    # in August, employee 28 was still in Operaciones (6) and employee 120 still in Data (10);
    # both were transferred in September (28 to Data, 120 to Recursos Humanos (4))
    assert {d: n for d, n in diff.items() if n} == {"6": 1, "4": -1}


@pytest.mark.integration
def test_candidate_stage_history(pipeline_all_batches):
    stages = _t(pipeline_all_batches, "gold.gold_fact_candidate_stage_history")

    def rows(candidate):
        return [
            (r["stage_sequence"], r["stage"], str(r["stage_entered_date"]), r["stage_date_source"], str(r["stage_exited_date"]), r["is_current_stage"])
            for r in stages.filter(f"candidate_id = '{candidate}'").orderBy("stage_sequence").collect()
        ]

    assert rows("101") == [
        (1, "applied", "2026-08-12", "applied_date", "None", False),
        (2, "interview", "None", "before_first_observation", "2026-10-01", False),
        (3, "hired", "2026-10-01", "status_change_detected", "None", True),
    ]
    assert rows("103") == [
        (1, "applied", "2026-08-18", "applied_date", "2026-10-01", False),
        (2, "rejected", "2026-10-01", "status_change_detected", "None", True),
    ]
    assert stages.filter("candidate_id = '103' AND stage = 'applied'").collect()[0]["days_in_stage"] == 44
    assert stages.filter("candidate_id = '106'").count() == 0  # removed from the source

    # grain and one current stage per candidate
    assert stages.count() == stages.select("candidate_id", "stage_sequence").distinct().count()
    current = stages.filter("is_current_stage")
    assert current.count() == current.select("candidate_id").distinct().count() == _t(pipeline_all_batches, "silver.silver_candidates").count()


# ---------------------------------------------------------------------------- Delta mode only

from pyspark.sql.functions import lit  # noqa: E402

from databricks_local import DELTA_MODE  # noqa: E402

SCD2_COLS = ("id", "status", "_row_hash", "_valid_from", "_valid_to", "_is_current", "_deleted_in_source")
MERGE_COLS = ("id", "amount", "_row_hash", "_deleted_in_source", "_last_changed_batch")


def _deliveries(spark, schema, rows_per_batch):
    dates = ["2026-08-01", "2026-09-01", "2026-09-01", "2026-10-01"]  # the second delivery is applied twice
    return [(spark.createDataFrame(rows, schema), d) for rows, d in zip(rows_per_batch, dates)]


@pytest.mark.delta
@pytest.mark.skipif(not DELTA_MODE, reason="needs Delta Lake (HR_TESTS_DELTA=1)")
def test_delta_scd2_merge_matches_plain_spark(spark_session, utils):
    """apply_scd2() (Delta MERGE) must produce exactly the table computed by scd2_next_state()."""
    utils["spark"].install_writer_patch()
    schema = "id string, status string, _ingested_at string"
    b2 = [("1", "active", "t2"), ("2", "terminated", "t2"), ("4", "active", "t2")]
    deliveries = _deliveries(spark_session, schema, [
        [("1", "active", "t1"), ("2", "active", "t1"), ("3", "active", "t1")],
        b2, b2,
        [("1", "active", "t3"), ("2", "terminated", "t3"), ("3", "active", "t3"), ("4", "active", "t3")],
    ])
    table, expected = "hr_database.oracle.scd2_table", None
    for df, batch_date in deliveries:
        utils["apply_scd2"](df, table, "id", batch_date)
        expected = utils["scd2_next_state"](expected, df, "id", batch_date).localCheckpoint()
        assert _rows(utils["spark"].table(table), *SCD2_COLS) == _rows(expected, *SCD2_COLS), batch_date


@pytest.mark.delta
@pytest.mark.skipif(not DELTA_MODE, reason="needs Delta Lake (HR_TESTS_DELTA=1)")
def test_delta_upsert_merge_matches_plain_spark(spark_session, utils):
    """apply_merge() (Delta MERGE) must produce exactly the table computed by merge_next_state()."""
    utils["spark"].install_writer_patch()
    schema = "id string, amount int, _ingested_at string"
    b2 = [("1", 10, "t2"), ("3", 30, "t2")]
    deliveries = _deliveries(spark_session, schema, [
        [("1", 10, "t1"), ("2", 20, "t1")],
        b2, b2,
        [("1", 15, "t3"), ("2", 20, "t3"), ("3", 30, "t3")],
    ])
    table, expected = "hr_database.oracle.merge_table", None
    for df, batch_date in deliveries:
        utils["apply_merge"](df, table, "id", batch_date)
        expected = utils["merge_next_state"](expected, df, "id", batch_date).localCheckpoint()
        assert _rows(utils["spark"].table(table), *MERGE_COLS) == _rows(expected, *MERGE_COLS), batch_date


@pytest.mark.delta
@pytest.mark.skipif(not DELTA_MODE, reason="needs Delta Lake (HR_TESTS_DELTA=1)")
def test_delta_write_batch_replaces_only_its_batch(spark_session, utils):
    utils["spark"].install_writer_patch()
    table = "hr_database.oracle.batch_table"
    df = spark_session.createDataFrame([("a",), ("b",)], "v string")
    for batch_date, rows in [("2026-08-01", df), ("2026-09-01", df), ("2026-09-01", df.limit(1))]:
        utils["write_batch"](rows.withColumn("_batch_date", lit(batch_date).cast("date")), table, batch_date)
    counts = {str(r["_batch_date"]): r["count"] for r in utils["spark"].table(table).groupBy("_batch_date").count().collect()}
    assert counts == {"2026-08-01": 2, "2026-09-01": 1}
