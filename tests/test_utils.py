"""Unit tests for the functions defined in notebooks/00_utils.py."""

import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, lit


# ----------------------------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------------------------

def _single_column(spark, values, name="value"):
    return spark.createDataFrame([(v,) for v in values], f"{name} string")


def _apply(spark, utils_fn, values):
    df = _single_column(spark, values)
    return [r["result"] for r in df.select(utils_fn("value").alias("result")).collect()]


# ----------------------------------------------------------------------------------------------
# normalize_legacy_id()
# ----------------------------------------------------------------------------------------------

def test_normalize_legacy_id_converts_legacy_ids(spark_session, utils):
    result = _apply(spark_session, utils["normalize_legacy_id"], ["100009", "100000", "199999"])
    # legacy id = 100000 + (real_id - 1)
    assert result == ["10", "1", "100000"]


def test_normalize_legacy_id_keeps_standard_and_invalid_ids(spark_session, utils):
    result = _apply(spark_session, utils["normalize_legacy_id"], ["9", "99999", "abc", None])
    assert result == ["9", "99999", "abc", None]


# ----------------------------------------------------------------------------------------------
# normalize_to_date_format()
# ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2024-01-03", datetime.date(2024, 1, 3)),
        ("2024/11/02", datetime.date(2024, 11, 2)),
        ("18 Mar 2024", datetime.date(2024, 3, 18)),
        ("Feb 02, 2024", datetime.date(2024, 2, 2)),
        # slash dates are MM/dd/yyyy in the source (ambiguous ones must not be read as dd/MM)
        ("02/05/2023", datetime.date(2023, 2, 5)),
        ("10/23/1987", datetime.date(1987, 10, 23)),
        # dash dates are dd-MM-yyyy in the source
        ("18-09-2015", datetime.date(2015, 9, 18)),
        ("2019-12-16 00:17:27", datetime.date(2019, 12, 16)),
        ("not a date", None),
        (None, None),
    ],
)
def test_normalize_to_date_format(spark_session, utils, raw, expected):
    df = _single_column(spark_session, [raw], name="d")
    result = df.select(utils["normalize_to_date_format"]("d").alias("r")).collect()[0]["r"]
    assert result == expected


# ----------------------------------------------------------------------------------------------
# fuzzy_match_category()
# ----------------------------------------------------------------------------------------------

STATUSES = ["active", "on_leave", "terminated"]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("active", "active"),
        ("actiev", "active"),          # transposition: distance 2
        ("on_elave", "on_leave"),
        ("terminatde", "terminated"),
        ("resigned", "unknown"),        # farther than the threshold
        (None, "unknown"),
    ],
)
def test_fuzzy_match_category(spark_session, utils, raw, expected):
    df = _single_column(spark_session, [raw])
    result = df.select(utils["fuzzy_match_category"](col("value"), STATUSES).alias("r")).collect()[0]["r"]
    assert result == expected


def test_fuzzy_match_category_custom_null_value(spark_session, utils):
    df = _single_column(spark_session, [None])
    result = df.select(
        utils["fuzzy_match_category"](col("value"), STATUSES, null_value=None).alias("r")
    ).collect()[0]["r"]
    assert result is None


# ----------------------------------------------------------------------------------------------
# strip_line_breaks()
# ----------------------------------------------------------------------------------------------

def test_strip_line_breaks_only_affects_string_columns(spark_session, utils):
    df = spark_session.createDataFrame([("Ingeniería\n", "a\r\nb", 5)], "dept string, txt string, n int")
    row = utils["strip_line_breaks"](df).collect()[0]
    assert (row["dept"], row["txt"], row["n"]) == ("Ingeniería", "ab", 5)


# ----------------------------------------------------------------------------------------------
# structural_validation() / repair_crammed_records()
# ----------------------------------------------------------------------------------------------

RAW_SCHEMA = "id string, name string, city string, status string, _source_file string, physical_row_idx long"
BUSINESS_COLS = ["name", "city", "status"]


def _raw(spark, rows):
    return spark.createDataFrame(
        [(*r, "file.csv", i) for i, r in enumerate(rows)],
        RAW_SCHEMA,
    )


def _reasons(df):
    return {r["id"]: r["rejection_reason"] for r in df.collect()}


def test_structural_validation_flags(spark_session, utils):
    raw = _raw(
        spark_session,
        [
            ("1", "Ana", None, None),          # next row has a non-numeric PK -> crammed parent
            (",Cordoba,active", None, None, None),  # crammed content
            ("3", "Eva", None, None),          # valid record with empty trailing columns
            ("4", "Luis", "Rosario", "active"),
            (None, "Sin ID", "Mendoza", "active"),
        ],
    )
    reasons = _reasons(utils["structural_validation"](raw, "id", BUSINESS_COLS))
    assert reasons["1"] == "potential_crammed_record"
    assert reasons[",Cordoba,active"] == "potential_crammed_content"
    assert reasons["3"] is None, "trailing empty columns alone must not be flagged (v2)"
    assert reasons["4"] is None
    assert reasons[None] == "id_null"


def _run_repair(spark, utils, rows):
    raw = _raw(spark, rows)
    validated = utils["structural_validation"](raw, "id", BUSINESS_COLS)
    quarantine = validated.filter(col("rejection_reason").isNotNull())
    ready = validated.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    unified, repaired_rows = utils["repair_crammed_records"](raw, quarantine, ready, "id", BUSINESS_COLS)
    remaining = quarantine.join(repaired_rows, on="physical_row_idx", how="left_anti")
    return unified, remaining


def test_repair_crammed_records_rebuilds_the_record(spark_session, utils):
    unified, remaining = _run_repair(
        spark_session,
        utils,
        [
            ("1", "Ana", None, None),
            (",Cordoba,active", None, None, None),
            ("2", "Luis", "Rosario", "active"),
        ],
    )
    records = {r["id"]: (r["name"], r["city"], r["status"]) for r in unified.collect()}
    assert records["1"] == ("Ana", "Cordoba", "active")
    assert records["2"] == ("Luis", "Rosario", "active")
    assert remaining.count() == 0


def test_failed_repair_stays_in_quarantine(spark_session, utils):
    """Regression test (v2): rows of an island whose repair fails must stay in quarantine."""
    unified, remaining = _run_repair(
        spark_session,
        utils,
        [
            ("1", "Ana", None, None),
            (",too,many,fields,here", None, None, None),  # 4 fields > 2 free slots -> cannot repair
            ("2", "Luis", "Rosario", "active"),
        ],
    )
    assert "1" not in {r["id"] for r in unified.collect()}
    assert remaining.count() == 2, "parent and content rows must remain in quarantine"


def test_null_pk_rows_are_not_absorbed_by_islands(spark_session, utils):
    """Regression test (v2): a '<pk>_null' row next to a crammed island must stay in quarantine."""
    _, remaining = _run_repair(
        spark_session,
        utils,
        [
            ("1", "Ana", None, None),
            (",Cordoba,active", None, None, None),
            (None, "Sin ID", "Mendoza", "active"),
            ("2", "Luis", "Rosario", "active"),
        ],
    )
    assert [r["rejection_reason"] for r in remaining.collect()] == ["id_null"]


# ----------------------------------------------------------------------------------------------
# recover_null_pk()
# ----------------------------------------------------------------------------------------------

def test_recover_null_pk_generates_deterministic_surrogate_keys(spark_session, utils):
    schema = "id string, name string, city string, status string, physical_row_idx long, rejection_reason string"
    quarantine = spark_session.createDataFrame(
        [
            (None, "Ana", "Cordoba", "active", 1, "id_null"),
            (None, "Ana", "Cordoba", "active", 2, "id_null"),     # identical record -> same key
            (None, "Luis", "Rosario", "active", 3, "id_null"),
            ("x", "Eva", None, None, 4, "potential_crammed_content"),
        ],
        schema,
    )
    ready = spark_session.createDataFrame(
        [("7", "Sol", "Salta", "active", 0)], "id string, name string, city string, status string, physical_row_idx long"
    )

    recovered, remaining = utils["recover_null_pk"](quarantine, ready, "id", BUSINESS_COLS)

    keys = [r["id"] for r in recovered.filter(col("id").startswith("SK-")).orderBy("physical_row_idx").collect()]
    assert len(keys) == 3
    assert keys[0] == keys[1], "identical records must get the same key"
    assert keys[0] != keys[2]
    assert all(len(k) == len("SK-") + 16 for k in keys)
    assert recovered.count() == 4
    assert [r["rejection_reason"] for r in remaining.collect()] == ["potential_crammed_content"]

    # deterministic across runs
    recovered_again, _ = utils["recover_null_pk"](quarantine, ready, "id", BUSINESS_COLS)
    assert sorted(r["id"] for r in recovered_again.collect()) == sorted(r["id"] for r in recovered.collect())


# ----------------------------------------------------------------------------------------------
# deduplicate_by_completeness()
# ----------------------------------------------------------------------------------------------

def test_deduplicate_by_completeness_keeps_most_complete_record(spark_session, utils):
    df = spark_session.createDataFrame(
        [
            ("1", "Ana", None, None, 1),
            ("1", "Ana", "Cordoba", "active", 2),
            ("1", "Ana", "Cordoba", None, 3),
            ("2", "Luis", None, None, 4),
        ],
        "id string, name string, city string, status string, physical_row_idx long",
    ).withColumn("_ingested_at", lit("2026-01-01").cast("timestamp"))

    result = {r["id"]: r["physical_row_idx"] for r in utils["deduplicate_by_completeness"](df, ["id"], BUSINESS_COLS).collect()}
    assert result == {"1": 2, "2": 4}


# ----------------------------------------------------------------------------------------------
# pct_quarantined_data()
# ----------------------------------------------------------------------------------------------

def test_pct_quarantined_data(spark_session, utils):
    quarantine = spark_session.range(1)
    clean = spark_session.range(3)
    assert utils["pct_quarantined_data"](quarantine, clean) == 25.0
    assert utils["pct_quarantined_data"](spark_session.range(0), spark_session.range(0)) == 0.0


# ----------------------------------------------------------------------------------------------
# resolve_to_unknown_member()
# ----------------------------------------------------------------------------------------------

def test_resolve_to_unknown_member(spark_session, utils):
    dim = spark_session.createDataFrame([("1",), ("2",)], "employee_id string")
    fact = spark_session.createDataFrame([("a", "1"), ("b", "99"), ("c", None)], "fact_id string, employee_id string")

    result = {r["fact_id"]: (r["employee_id"], r["source_employee_id"])
              for r in utils["resolve_to_unknown_member"](fact, "employee_id", dim, "employee_id").collect()}

    assert result == {
        "a": ("1", "1"),        # resolved: unchanged
        "b": ("-1", "99"),      # not in the dimension: Unknown member, original value kept
        "c": (None, None),      # null stays null
    }
