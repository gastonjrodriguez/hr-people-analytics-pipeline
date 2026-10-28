import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from databricks_local import LocalCatalogSpark, build_spark, copy_warehouse, new_namespace, run_pipeline  # noqa: E402


@pytest.fixture(scope="session")
def spark_session():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def utils(spark_session, tmp_path_factory):
    """Functions and variables defined in notebooks/00_utils.py."""
    local_spark = LocalCatalogSpark(spark_session, tmp_path_factory.mktemp("utils_warehouse"))
    return new_namespace(local_spark)


@pytest.fixture(scope="session")
def pipeline(spark_session, tmp_path_factory):
    """Runs the full pipeline once (Bronze -> Silver -> Gold) on the FIRST batch (the original
    extract in data/raw) and returns a table reader."""
    local_spark = LocalCatalogSpark(spark_session, tmp_path_factory.mktemp("pipeline_warehouse"))
    run_pipeline(local_spark)
    return local_spark


@pytest.fixture(scope="session")
def pipeline_all_batches(pipeline, spark_session, tmp_path_factory):
    """Continues from `pipeline` (batch 1) and runs the two remaining batches, plus one extra run
    with nothing pending (re-processes the last batch: must change nothing)."""
    local_spark = copy_warehouse(pipeline, spark_session, tmp_path_factory.mktemp("pipeline_batches_warehouse"))
    run_pipeline(local_spark, runs=3)
    return local_spark
