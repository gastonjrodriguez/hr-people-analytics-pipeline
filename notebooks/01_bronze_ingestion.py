# Databricks notebook source
# MAGIC %run /hr_analytics_v3/00_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA INGESTION

# COMMAND ----------

# DBTITLE 1,Cell 2
# imports
from pyspark.sql.functions import current_timestamp, lit
import logging


# catalog and schema
spark.sql("USE CATALOG hr_database")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")
spark.sql("USE SCHEMA bronze")


#logs config for monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronzeIngestion") 


print("================ BRONZE LAYER: DATA INGESTION HAS STARTED. ================")

# ingestion variables
vol_path = "/Volumes/hr_database/bronze/raw_hr_data"

# v3: INCREMENTAL INGESTION. Each delivery is a folder 'batch_date=YYYY-MM-DD' in the Volume.
# One batch per run: the oldest one not ingested yet (see next_batch() in 00_utils). Every batch is
# APPENDED to the Bronze tables with its '_batch_date', so Bronze keeps the full history of deliveries.
batch_date, is_reprocess, pending_after = next_batch(list_batches(vol_path), processed_batches("bronze"))
batch_path = f"{vol_path}/{BATCH_FOLDER_PREFIX}{batch_date}"

print(f"BATCH: {batch_date}" + (" (already ingested: re-processing it, idempotent)" if is_reprocess else ""))
print(f"Batches still pending after this run: {pending_after}")

table_names = [
    "benefit_types", "candidates", "contracts", "departments", "employee_benefits", "employee_trainings", "employees",
    "goals", "interviews", "job_postings", "performance_reviews", "positions", "salaries", "salary_history", "time_off_balances",
    "time_off_requests", "trainings"
]

# v2: tables consumed by the Silver layer. A failure in any of them stops the pipeline
# (raise at the end of this notebook). The rest are ingested for future use; their failure
# is only logged as a WARNING and does not stop the pipeline.
critical_tables = [
    "departments", "positions", "employees", "contracts", "salary_history", "salaries",
    "trainings", "performance_reviews", "job_postings", "candidates"
]

# ingestion lists for auditory
successful_data_ingestions = []
failed_data_ingestions = []

# dinamic ingestion with error handling
for table in table_names:
    file_path = f"{batch_path}/rrhh_{table}.csv"
    target_table = f"hr_database.bronze.bronze_{table}"

    try:
        print(f"INGESTING DATA: rrhh_{table}.csv ====> hr_database.bronze.bronze_{table}")

        # v2: multiLine + quote/escape. Source CSVs contain quoted values with embedded
        # line breaks (e.g. "Ingeniería\n"). Without multiLine, Spark splits each of those
        # logical records into 2 physical rows ("crammed records").
        df_raw = (spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(file_path)
                )
        
        if df_raw.isEmpty():
            raise ValueError(f"File rrhh_{table}.csv is EMPTY.")


        df_bronze = df_raw.withColumns({
            "_ingested_at": current_timestamp(),
            "_source_file": df_raw["_metadata.file_path"],
            "_batch_date": lit(batch_date).cast("date")  # v3
            })
        
        # persist to Delta (v3: append of this batch, idempotent; was a full overwrite)
        write_batch(df_bronze, target_table, batch_date)

        successful_data_ingestions.append(table)
        log_pipeline_event(layer="bronze", entity_name=f"bronze_{table}", status="SUCCESS")
    
    except Exception as e:
        failed_data_ingestions.append({"table": table, "error": str(e)})
        log_pipeline_event(layer="bronze", entity_name=f"bronze_{table}", status="ERROR", error_message=str(e))


# EXECUTION SUMMARY
print("\n" + "="*50)
print("EXECUTION SUMMARY")
print(f"Tables processed successfully: {len(successful_data_ingestions)} / {len(table_names)}")

if failed_data_ingestions:
    print("\n" + "="*50)
    print(f"\n Tables with failures: {len(failed_data_ingestions)}")
    for failure in failed_data_ingestions:
        print(f"- Table: {failure['table']} || Error: {failure['error']}")
    print("\n" + "=" * 50)
else:
    print("ALL tables were processed successfully!")
    
print("="*50)

# COMMAND ----------

# DBTITLE 1,v2 - fail on critical errors
# v2: STOP THE PIPELINE ON CRITICAL FAILURES
# Before, errors were logged and the notebook still finished successfully, so a Databricks Job
# would go on to run Silver and Gold over stale data. Now the notebook fails (and the Job stops)
# if any table consumed by Silver could not be ingested.

critical_failures = [f for f in failed_data_ingestions if f["table"] in critical_tables]
non_critical_failures = [f for f in failed_data_ingestions if f["table"] not in critical_tables]

for failure in non_critical_failures:
    log_pipeline_event(
        layer="bronze",
        entity_name=f"bronze_{failure['table']}",
        status="WARNING",
        error_message=f"Non-critical table (not used in Silver) failed: {failure['error']}"
    )

if critical_failures:
    failed_list = ", ".join(f["table"] for f in critical_failures)
    raise RuntimeError(
        f"BRONZE FAILED: {len(critical_failures)} table(s) used by Silver could not be ingested: {failed_list}. "
        "See hr_database.monitoring.pipeline_logs for details."
    )

# COMMAND ----------

# DBTITLE 1,v3 - batch control
# v3: only reached when no critical table failed. Silver will process this batch next.
mark_batch_processed("bronze", batch_date)
print(f"BRONZE: batch {batch_date} recorded in {BATCH_CONTROL_TABLE}")
