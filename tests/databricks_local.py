"""
Local runner for the Databricks notebooks (no Databricks workspace required).

The notebooks are exported in Databricks "source" format. This module lets them run on a
local SparkSession so they can be tested with pytest (locally or in GitHub Actions):

- `# MAGIC` lines (%run, %md, %sql cells) are skipped. The `%run 00_utils` dependency is
  replaced by executing 00_utils.py in the same namespace, which is what %run does.
- Three-level table names (`hr_database.<schema>.<table>`) are mapped to Parquet folders in
  a temporary directory: `saveAsTable()` writes there and `spark.table()` reads from there.
  Delta-specific behavior (schema enforcement, time travel) is NOT emulated.
- `USE CATALOG` / `CREATE SCHEMA` statements are ignored.
- The Unity Catalog Volume path is replaced by the local `data/batches` folder (one subfolder per
  simulated delivery, see scripts/generate_batches.py), and `dbutils.fs.ls` lists local folders.
- Delta-specific writes are isolated in 00_utils functions (`write_batch()`, `apply_merge()`,
  `apply_scd2()`). Two modes:
    * default: tables are Parquet folders and those functions are replaced by equivalents built
      on the plain-Spark functions with the same semantics (`merge_next_state()`,
      `scd2_next_state()`). No Delta Lake needed.
    * Delta mode (environment variable HR_TESTS_DELTA=1, used by the `pytest-delta` CI job):
      tables are Delta tables and the real functions run (replaceWhere, MERGE). Requires the
      `delta-spark` package and network access to Maven Central for the Delta jars.
- `try_to_date` exists in Databricks Runtime but not in open-source PySpark 4.0; when it is
  missing it is emulated with `try_to_timestamp(...).cast("date")`.
"""

import os
import re
import shutil
from collections import namedtuple
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrameWriter, SparkSession

DELTA_MODE = os.environ.get("HR_TESTS_DELTA") == "1"
TABLE_FORMAT = "delta" if DELTA_MODE else "parquet"

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"
BATCHES_DIR = REPO_ROOT / "data" / "batches"
VOLUME_PATH = "/Volumes/hr_database/bronze/raw_hr_data"

NOTEBOOKS = [
    "00_utils.py",
    "01_bronze_ingestion.py",
    "02_silver_transformation.py",
    "03_gold_metrics.py",
]


def _install_try_to_date_shim() -> None:
    if hasattr(F, "try_to_date"):
        return

    def try_to_date(column, fmt=None):
        column = F.col(column) if isinstance(column, str) else column
        return F.try_to_timestamp(column, F.lit(fmt)).cast("date")

    F.try_to_date = try_to_date


def build_spark() -> SparkSession:
    _install_try_to_date_shim()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("hr-pipeline-tests")
        .config("spark.sql.ansi.enabled", "true")  # same default as recent Databricks runtimes
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if DELTA_MODE:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    return builder.getOrCreate()


class LocalCatalogSpark:
    """Wraps a SparkSession so that catalog tables are stored as local folders (Parquet, or Delta in Delta mode)."""

    def __init__(self, spark: SparkSession, warehouse_dir: Path):
        self._spark = spark
        self.warehouse_dir = Path(warehouse_dir)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def table_path(self, table_name: str) -> str:
        return str(self.warehouse_dir / table_name.replace("hr_database.", ""))

    def table(self, table_name: str):
        return self._spark.read.format(TABLE_FORMAT).load(self.table_path(table_name))

    def sql(self, query: str):
        if re.match(r"\s*(USE|CREATE SCHEMA)", query, re.IGNORECASE):
            return None
        return self._spark.sql(query)

    def install_writer_patch(self) -> None:
        local = self

        def save_as_table(writer: DataFrameWriter, name: str, *args, **kwargs):
            writer.format(TABLE_FORMAT)
            writer.save(local.table_path(name))

        DataFrameWriter.saveAsTable = save_as_table


def load_notebook_source(notebook_name: str) -> str:
    lines = []
    for line in (NOTEBOOKS_DIR / notebook_name).read_text(encoding="utf-8").splitlines():
        if line.startswith("# MAGIC") or line.startswith("# Databricks notebook source"):
            continue
        lines.append(line)
    return "\n".join(lines).replace(VOLUME_PATH, str(BATCHES_DIR))


FileInfo = namedtuple("FileInfo", ["path", "name", "size"])


class LocalFs:
    """Minimal `dbutils.fs` over the local file system."""

    @staticmethod
    def ls(path: str):
        entries = []
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            is_dir = os.path.isdir(full)
            entries.append(FileInfo(full + ("/" if is_dir else ""), name + ("/" if is_dir else ""), 0 if is_dir else os.path.getsize(full)))
        return entries


class LocalDbutils:
    fs = LocalFs()


def _local_write_batch(spark: LocalCatalogSpark):
    """Parquet equivalent of write_batch(): replaces the rows of one batch, keeps the others."""

    def write_batch(df, table_name, batch_date):
        path = spark.table_path(table_name)
        if os.path.exists(path):
            existing = spark.table(table_name)
            keep = existing.filter(F.col("_batch_date") != F.lit(batch_date).cast("date"))
            df = keep.unionByName(df.select(*existing.columns), allowMissingColumns=True).localCheckpoint()
        # not partitioned: Parquet writes no file (and no schema) for an empty partitioned DataFrame
        df.write.mode("overwrite").parquet(path)

    return write_batch


def _local_apply(spark: LocalCatalogSpark, namespace: dict, next_state_fn: str):
    """Parquet equivalent of apply_merge() / apply_scd2(): computes the whole new table and rewrites it."""

    def apply(source, table_name, key, batch_date):
        path = spark.table_path(table_name)
        current = spark.table(table_name) if os.path.exists(path) else None
        result = namespace[next_state_fn](current, source, key, batch_date).localCheckpoint()
        result.write.mode("overwrite").parquet(path)

    return apply


def _route_delta_table_names(spark: LocalCatalogSpark) -> None:
    """Delta mode: DeltaTable.forName() resolves catalog names to the local table folders."""
    from delta.tables import DeltaTable

    real_session = spark._spark
    DeltaTable.forName = staticmethod(lambda _session, name: DeltaTable.forPath(real_session, spark.table_path(name)))


def new_namespace(spark: LocalCatalogSpark) -> dict:
    """Namespace with 00_utils loaded (equivalent to `%run 00_utils`)."""
    namespace = {"spark": spark, "dbutils": LocalDbutils(), "__name__": "databricks_notebook"}
    exec(compile(load_notebook_source("00_utils.py"), "00_utils.py", "exec"), namespace)
    if DELTA_MODE:
        _route_delta_table_names(spark)  # the real write_batch() / apply_merge() / apply_scd2() run
    else:
        namespace["write_batch"] = _local_write_batch(spark)
        namespace["apply_merge"] = _local_apply(spark, namespace, "merge_next_state")
        namespace["apply_scd2"] = _local_apply(spark, namespace, "scd2_next_state")
    return namespace


def run_pipeline(spark: LocalCatalogSpark, runs: int = 1) -> dict:
    """
    Runs Bronze, Silver and Gold in order, like a Databricks Job, `runs` times (each run processes
    the next pending batch). Raises if a layer fails.
    """
    spark.install_writer_patch()
    namespace = None
    for _ in range(runs):
        namespace = new_namespace(spark)
        for notebook in NOTEBOOKS[1:]:
            exec(compile(load_notebook_source(notebook), notebook, "exec"), namespace)
    return namespace


def copy_warehouse(source: LocalCatalogSpark, spark: SparkSession, target_dir: Path) -> LocalCatalogSpark:
    """Copies a local warehouse, to continue running batches without altering the original."""
    shutil.copytree(source.warehouse_dir, target_dir, dirs_exist_ok=True)
    return LocalCatalogSpark(spark, target_dir)
