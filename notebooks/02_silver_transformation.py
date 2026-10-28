# Databricks notebook source
# DBTITLE 1,00_utils run
# MAGIC %run /hr_analytics_v3/00_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTS

# COMMAND ----------

# DBTITLE 1,imports
# imports
from pyspark.sql.functions import col, trim, when, lit, array, expr, initcap, lower, upper, substring, coalesce, try_to_date, to_date, translate, regexp_replace, concat, levenshtein, current_timestamp, floor, months_between, current_date, row_number, monotonically_increasing_id, sequence, element_at, filter, slice, collect_list, struct, sum, from_csv, size, udf, instr
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType, BooleanType, StringType
import builtins
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### CATALOG AND SCHEMA

# COMMAND ----------

# DBTITLE 1,catalog and schema
# catalog and schema
spark.sql("USE CATALOG hr_database")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOGGING

# COMMAND ----------

# DBTITLE 1,logs configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silverTransformation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORMATION

# COMMAND ----------

# DBTITLE 1,-- Pipeline start --
print("================ SILVER LAYER: STARTING CLEANING AND TRANSFORMATION. ================")

# v3: Silver processes ONE batch per run: the oldest batch already ingested by Bronze and not yet
# processed by Silver. Every Bronze read below is filtered to that batch (read_batch()).
silver_batch_date, silver_is_reprocess, silver_pending_after = next_batch(processed_batches("bronze"), processed_batches("silver"))
print(f"BATCH: {silver_batch_date}" + (" (already processed: re-processing it, idempotent)" if silver_is_reprocess else ""))
print(f"Batches still pending for Silver after this run: {silver_pending_after}")

# v3: LOAD STRATEGY PER TABLE (see "Silver load strategies" in 00_utils)
#   scd2      -> state tables: their changes over time are history worth keeping
#   merge     -> event tables: each row is already a dated fact; only new / changed rows are written
#   overwrite -> small catalogs, and silver_salaries, which is a DERIVED snapshot (current salary per
#                employee, recomputed from salaries + salary_history; its history is salary_history)
SILVER_LOAD_CONFIG = {
    "departments":         {"strategy": "overwrite"},
    "trainings":           {"strategy": "overwrite"},
    "salaries":            {"strategy": "overwrite"},
    "positions":           {"strategy": "scd2", "key": "position_id"},
    "employees":           {"strategy": "scd2", "key": "employee_id"},
    "contracts":           {"strategy": "scd2", "key": "contract_id"},
    "job_postings":        {"strategy": "scd2", "key": "posting_id"},
    "candidates":          {"strategy": "scd2", "key": "candidate_id"},
    "salary_history":      {"strategy": "merge", "key": "history_id"},
    "performance_reviews": {"strategy": "merge", "key": "review_id"},
}

# v3: the BEFORE/AFTER DEDUPLICATION row counts are diagnostics: each one scans the whole table.
# They are off by default (set to True to debug a table).
SILVER_DEBUG_COUNTS = False

# COMMAND ----------

# DBTITLE 1,data quality report (creation)
# data quality report

data_quality_report = []

# v2: entities that failed in this run (the notebook raises at the end if not empty)
failed_entities = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEPARTMENTS -- ok

# COMMAND ----------

# DBTITLE 1,departments
# DEPARTMENTS -----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Sinthetic record (department_id '0')
# 7) Business validation (validation 2/2)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)


try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_departments"
    df_departments_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded
    
    cols_ordered_departments = [
        "department_id",
        "department_name",
        "department_code",
        "budget",
        "location",
        "head_count_limit",
        "cost_center",
        "created_at"
        ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption
    df_structural_validation_dept = structural_validation(
        df_departments_raw,
        cols_ordered_departments[0],
        cols_ordered_departments[1:]
        )

    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_dept = df_structural_validation_dept.filter(
        col("rejection_reason").isNotNull()
        )
    
    # Records that passed structural validation
    df_ready_for_cleaning_dept = df_structural_validation_dept.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation
    df_unified_ready_for_cleaning_dept, df_repaired_physical_rows_dept = repair_crammed_records(
        df_departments_raw,
        df_structural_quarantine_dept,
        df_ready_for_cleaning_dept,
        cols_ordered_departments[0],
        cols_ordered_departments[1:]
        )
    
   
    df_structural_quarantine_dept_remaining = (
        df_structural_quarantine_dept.join(
            df_repaired_physical_rows_dept,
            on="physical_row_idx",
            how="left_anti"
            )
        )
    
    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # clean records
    df_departments_clean_base = df_unified_ready_for_cleaning_dept.select(
        # expr("try_cast()") to avoid spark version failures and ensure that a non-parseable value returns
        # a visible, filterable `null`, rather than a silent error.
        expr("try_cast(department_id AS string)").alias("department_id"),
        initcap(trim(col("department_name"))).alias("department_name"),
        when(col("department_code").isNull(), upper(trim(substring(col("department_name"), 1, 3)))).otherwise(
            upper(trim(col("department_code")))).alias("department_code"),
        expr("try_cast(budget AS decimal(10,2))").alias("budget"),
        when(col("location").isNull(), lit("Unknown")).otherwise(initcap(trim(col("location")))).alias("location"),
        expr("try_cast(head_count_limit AS integer)").alias("head_count_limit"),
        when(col("cost_center").isNull(), concat(lit("CC-00"), expr("try_cast(department_id AS string)"))).otherwise(
            trim(col("cost_center"))).alias("cost_center"),
        expr("try_cast(created_at AS date)").alias("created_at"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )
    

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_departments_clean_base.count())

    # df_before_deduplication_dept = df_departments_clean_base

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_departments_clean_base = deduplicate_by_completeness(
        df_departments_clean_base,
        ["department_id"],
        cols_ordered_departments[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_departments_clean_base.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_dept = (
    #     df_unified_ready_for_cleaning_dept
    #     .groupBy("department_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_dept.count())
    # df_duplicate_ids_dept.show()


    # ==================================================================================================================================================================================
    # SYNTHETIC RECORD (DEPARTMENT_ID '0')
    # ==================================================================================================================================================================================

    # Sinthetic map for department_id '0' which is used for unassigned employees
    df_unassigned_dept = spark.createDataFrame(
        [("0", "Sin Departamento", "SDP", None, "Unknown", None, "CC-000", None, None, "synthetic_file")],
        schema=df_departments_clean_base.schema
        )

    # final union of clean and "unassigned dept"
    df_departments_clean = df_departments_clean_base.unionByName(df_unassigned_dept)

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_departments_final_validation = df_departments_clean.withColumn(
        "rejection_reason",
        when((col("budget") < 0.00) | (col("budget") > 9999999999.00), "budget_out_of_range")
        .when(col("head_count_limit") < 0, "head_count_limit_negative")
        .otherwise(None)
        )

    # final clean table
    df_departments_clean_final = df_departments_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_departments_quarantine_business = df_departments_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_dept = {}

    # apply data type fix + quarantine dfs consolidation
    df_departments_quarantine = consolidate_quarantine(
        df_structural_quarantine_dept_remaining,
        df_departments_quarantine_business,
        problematic_cols_dept,
        bronze_table
        )
    
    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    departments_data_quality = pct_quarantined_data(df_departments_quarantine,df_departments_clean_final)
    data_quality_report.append((bronze_table, departments_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_departments_clean_final, "departments", SILVER_LOAD_CONFIG["departments"], silver_batch_date)
    write_quarantine(df_departments_quarantine, "departments", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_departments")
    print("CREATED: hr_database.silver.silver_quarantine_departments")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_departments_quarantine.isEmpty():
        log_pipeline_event("silver", "departments", "WARNING", f"Data in quarantine for review: {departments_data_quality}%")
    else:
        log_pipeline_event("silver", "departments", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "departments", "ERROR", str(e))
    failed_entities.append({"entity": "departments", "error": str(e)})  # v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### POSITIONS -- ok

# COMMAND ----------

# DBTITLE 1,positions
# POSITIONS -----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Business validation (validation 2/2)
# 7) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 8) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 9) Persist to Delta (Silver)


try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_positions"
    df_positions_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_positions = [
        "position_id",
        "position_title",
        "level",
        "department_name",
        "min_salary",
        "max_salary",
        "is_remote_eligible",
        "requires_travel"
        ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_pos = structural_validation(
        df_positions_raw,
        cols_ordered_positions[0], 
        cols_ordered_positions[1:]
    )

    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_pos = df_structural_validation_pos.filter(
        col("rejection_reason").isNotNull()
        )
    
    # Records that passed structural validation
    df_ready_for_cleaning_pos = df_structural_validation_pos.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation
    df_unified_ready_for_cleaning_pos, df_repaired_physical_rows_pos = repair_crammed_records(
        df_positions_raw,
        df_structural_quarantine_pos,
        df_ready_for_cleaning_pos,
        cols_ordered_positions[0],
        cols_ordered_positions[1:]
        )
    
    df_structural_quarantine_pos_remaining = (
        df_structural_quarantine_pos.join(
            df_repaired_physical_rows_pos,
            on="physical_row_idx",
            how="left_anti"
            )
        )
    
    # # ==================================================================================================================================================================================
    # # DEDUPLICATION
    # # ==================================================================================================================================================================================

    # # Checkpoint before deduplication
    # print("BEFORE DEDUPLICATION:", df_unified_ready_for_cleaning_pos.count())

    # df_before_deduplication_pos = df_unified_ready_for_cleaning_pos

    # # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    # df_unified_ready_for_cleaning_pos = deduplicate_by_completeness(
    #     df_unified_ready_for_cleaning_pos,
    #     ["position_id"],
    #     cols_ordered_positions[1:]
    #     )
    
    # # Checkpoint after deduplication
    # print("AFTER DEDUPLICATION:", df_unified_ready_for_cleaning_pos.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_pos = (
    #     df_unified_ready_for_cleaning_pos
    #     .groupBy("position_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_pos.count())
    # df_duplicate_ids_pos.show()
    
    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # data transformation
    df_positions_clean = df_unified_ready_for_cleaning_pos.select(
        # expr("try_cast()") to avoid spark version failures and ensure that a non-parseable value returns a visible, filterable `null`, rather than a silent error.
        expr("try_cast(position_id AS string)").alias("position_id"),
        initcap(trim(col("position_title"))).alias("position_title"),
        coalesce(trim(col("level")), lit("Unknown")).alias("level"),
        initcap(trim(regexp_replace(col("department_name"), r"\s+", " "))).alias("department_name"),
        # v2: a missing min_salary stays null (it was defaulted to 0, which halved the band midpoint
        # and inflated the compa-ratio, e.g. Senior Developer). Incomplete bands are flagged in Gold.
        expr("try_cast(min_salary AS decimal(10,2))").alias("min_salary"),
        expr("try_cast(max_salary AS decimal(10,2))").alias("max_salary"),
        expr("try_cast(is_remote_eligible AS boolean)").alias("is_remote_eligible"),
        expr("try_cast(requires_travel AS boolean)").alias("requires_travel"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )


    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_positions_clean.count())

    # df_before_deduplication_pos = df_positions_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_positions_clean = deduplicate_by_completeness(
        df_positions_clean,
        ["position_id"],
        cols_ordered_positions[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_positions_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_pos = (
    #     df_positions_clean
    #     .groupBy("position_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_pos.count())
    # df_duplicate_ids_pos.show()


    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_positions_final_validation = df_positions_clean.withColumn(
        "rejection_reason",
        when(col("min_salary") > col("max_salary"), "min_bigger_than_max")
        .otherwise(None)
        )

    # final clean table
    df_positions_clean_final = df_positions_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_positions_quarantine_business = df_positions_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_pos = {
        "min_salary": expr("try_cast(min_salary AS decimal(10,2))"),
        "max_salary": expr("try_cast(max_salary AS decimal(10,2))"),
        "is_remote_eligible": expr("try_cast(is_remote_eligible AS boolean)"),
        "requires_travel": expr("try_cast(requires_travel AS boolean)")
    }
    
    # apply data type fix + quarantine dfs consolidation
    df_positions_quarantine = consolidate_quarantine(
        df_structural_quarantine_pos_remaining,
        df_positions_quarantine_business,
        problematic_cols_pos,
        bronze_table
    )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    positions_data_quality = pct_quarantined_data(df_positions_quarantine,df_positions_clean_final)
    data_quality_report.append((bronze_table, positions_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_positions_clean_final, "positions", SILVER_LOAD_CONFIG["positions"], silver_batch_date)
    write_quarantine(df_positions_quarantine, "positions", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_positions")
    print("CREATED: hr_database.silver.silver_quarantine_positions")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================
    
    if not df_positions_quarantine.isEmpty():
        log_pipeline_event("silver", "positions", "WARNING", f"Data in quarantine for review: {positions_data_quality}%")
    else:
        log_pipeline_event("silver", "positions", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "positions", "ERROR", str(e))
    failed_entities.append({"entity": "positions", "error": str(e)})  # v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### EMPLOYEES -- ok
# MAGIC

# COMMAND ----------

# DBTITLE 1,employees
# EMPLOYEES -----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Levenshtein distance fix
# 6) Deduplication criteria (deduplicate_by_completeness())
# 7) Business validation (validation 2/2)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)

try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================
    bronze_table = "hr_database.bronze.bronze_employees"

    df_employees_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded
    cols_ordered_employees = [
        "employee_id",
        "employee_code",
        "first_name",
        "last_name",
        "email",
        "phone",
        "birth_date",
        "gender",
        "department_id",
        "position_id",
        "manager_id",
        "hire_date",
        "employment_type",
        "work_location",
        "status"
        ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================    

    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption
    df_structural_validation_emp = structural_validation(
        df_employees_raw,
        cols_ordered_employees[0],
        cols_ordered_employees[1:]
        )

    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_emp = df_structural_validation_emp.filter(
        col("rejection_reason").isNotNull()
        )

    # Records that passed structural validation
    df_ready_for_cleaning_emp = df_structural_validation_emp.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation
    df_unified_ready_for_cleaning_emp, df_repaired_physical_rows_emp = repair_crammed_records(
        df_employees_raw,
        df_structural_quarantine_emp,
        df_ready_for_cleaning_emp,
        cols_ordered_employees[0],
        cols_ordered_employees[1:]
        )
    
    df_structural_quarantine_emp_remaining = (
        df_structural_quarantine_emp.join(
            df_repaired_physical_rows_emp,
            on="physical_row_idx",
            how="left_anti"
            )
        )

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # data transformation
    df_employees_clean_base = df_unified_ready_for_cleaning_emp.select(
        expr("try_cast(employee_id AS string)").alias("employee_id"),
        trim(concat(lit("EMP-00"), col("employee_id"))).alias("employee_code"),
        initcap(trim(regexp_replace(col("first_name"), "\\s+", " "))).alias("first_name"),
        initcap(trim(regexp_replace(col("last_name"), "\\s+", " "))).alias("last_name"),
        when(col("first_name").isNotNull() & col("last_name").isNotNull(), translate(lower(
            concat(regexp_replace(trim(col("first_name")), "\\s+", ""), lit("."), regexp_replace(trim(col("last_name")), "\\s+", ""),col("employee_id"),lit("@company.com")
                   )), "áéíóúüñÑ", "aeiouunN")).otherwise(col("email")).alias("email"), # otherwise(col("email")) because the validation will catch this later and the email would be useful to get the missing first/last name
        trim(col("phone")).alias("phone"),
        normalize_to_date_format("birth_date").alias("birth_date"),
        coalesce(trim(initcap(col("gender"))), lit("Not_declared")).alias("gender"),
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        coalesce(
            normalize_legacy_id("department_id"),
            lit("0")
            ).alias("department_id"),
        normalize_legacy_id("position_id").alias("position_id"),
        normalize_legacy_id("manager_id").alias("manager_id"),    
        normalize_to_date_format("hire_date").alias("hire_date"),
        # v2: normalized (lower + trim + '_vN' removed) and matched with fuzzy_match_category() below.
        # Before, only '_v2' was removed, so 'Full_time', 'full_tmie', 'Contractor' stayed as separate values.
        regexp_replace(trim(lower(col("employment_type"))), "_v\\d+$", "").alias("employment_type_base"),
        coalesce(initcap(trim(regexp_replace(col("work_location"), "\\s+", " "))), lit("Unknown")).alias("work_location"),
        when((trim(col("status")) == ""), lit(None))
        .otherwise(regexp_replace(trim(lower(col("status"))), "_v\\d+", "")).alias("status_base"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # LEVENSHTEIN DISTANCE FIX
    # ==================================================================================================================================================================================

    # Levenshtein distance calculation to reconcile status_base with official status classification
    # v2: fuzzy_match_category() with max distance 2 (values farther than that -> 'unknown')
    df_employees_clean = df_employees_clean_base.withColumn(
        "status",
        fuzzy_match_category(col("status_base"), ["active", "on_leave", "terminated"])
    ).withColumn(
        "employment_type",
        fuzzy_match_category(col("employment_type_base"), ["full_time", "part_time", "contractor"])
    ).drop(
        "status_base", "employment_type_base"
    )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================
    
    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_employees_clean.count())

    # df_before_deduplication_emp = df_employees_clean_base

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_employees_clean = deduplicate_by_completeness(
        df_employees_clean,
        ["employee_id"],
        cols_ordered_employees[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_employees_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_emp = (
    #     df_employees_clean
    #     .groupBy("employee_id")
    #     .count()
    #     .filter(col("count") > 1)
    # )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_emp.count())
    # df_duplicate_ids_emp.show()

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (STEP 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_employees_final_validation = df_employees_clean.withColumn(
        "rejection_reason",
        when(col("first_name").isNull() | col("last_name").isNull(), "first_name_null_or_last_name_null")
        .when(col("position_id").isNull() & col("status").isin(["active", "on_leave"]), "position_id_null_when_active_or_on_leave")
        .when(col("position_id").isNull() & (col("status") == "terminated"), "position_id_null_when_terminated")
        .when(col("status").isin(["active", "on_leave"]) & col("hire_date").isNull(), "hire_date_null_when_active_or_on_leave")
        .when(col("hire_date").isNull() & (col("status") == "terminated"), "hire_date_null_when_terminated")
        .when((col("status") == "unknown") & col("hire_date").isNull(), "hire_date_and_status_unknown")
        .when(floor(months_between(current_date(), col("birth_date")) / 12) < 18, "under_18_years_old")
        .otherwise(None)
        )


    # final clean table
    df_employees_clean_final = df_employees_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")
    
    # business quarantined records
    df_employees_quarantine_business = df_employees_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_emp = {
        "birth_date": normalize_to_date_format("birth_date"),
        "hire_date": normalize_to_date_format("hire_date"),
        "department_id": expr("try_cast(department_id AS string)"),
        "position_id": expr("try_cast(position_id AS string)"),
        "manager_id": expr("try_cast(manager_id AS string)")
    }

    # apply data type fix + quarantine dfs consolidation
    df_employees_quarantine = consolidate_quarantine(
        df_structural_quarantine_emp_remaining,
        df_employees_quarantine_business,
        problematic_cols_emp,
        bronze_table
        )
    
    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    employees_data_quality = pct_quarantined_data(df_employees_quarantine,df_employees_clean_final)
    data_quality_report.append((bronze_table, employees_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_employees_clean_final, "employees", SILVER_LOAD_CONFIG["employees"], silver_batch_date)
    write_quarantine(df_employees_quarantine, "employees", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_employees")
    print("CREATED: hr_database.silver.silver_quarantine_employees")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_employees_quarantine.isEmpty():
        log_pipeline_event("silver", "employees", "WARNING", f"Data in quarantine for review: {employees_data_quality}%")
    else:
        log_pipeline_event("silver", "employees", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "employees", "ERROR", str(e))
    failed_entities.append({"entity": "employees", "error": str(e)})  # v2



# COMMAND ----------

# MAGIC %md
# MAGIC ### CONTRACTS -- ok

# COMMAND ----------

# DBTITLE 1,contracts
# CONTRACTS -----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Levenshtein distance fix
# 6) Deduplication criteria (deduplicate_by_completeness())
# 7) Business validation (validation 2/2)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)


try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_contracts"
    df_contracts_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_contracts = [
        "contract_id",
        "employee_id",
        "contract_type",
        "start_date",
        "end_date",
        "probation_end_date",
        "notice_period_days",
        "signed_date"
    ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_cont = structural_validation(
        df_contracts_raw,
        cols_ordered_contracts[0],
        cols_ordered_contracts[1:],
        )

    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_cont = df_structural_validation_cont.filter(
        col("rejection_reason").isNotNull()
        )

    # Records that passed structural validation
    df_ready_for_cleaning_cont = df_structural_validation_cont.filter(
        col("rejection_reason").isNull()
    ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_cont, df_repaired_physical_rows_cont = repair_crammed_records(
        df_contracts_raw,
        df_structural_quarantine_cont,
        df_ready_for_cleaning_cont,
        cols_ordered_contracts[0],
        cols_ordered_contracts[1:]
        )

    df_structural_quarantine_cont_remaining = (
        df_structural_quarantine_cont.join(
            df_repaired_physical_rows_cont,
            on="physical_row_idx",
            how="left_anti"
            )
        )

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    df_contracts_clean_base = df_unified_ready_for_cleaning_cont.select(
        expr("try_cast(contract_id AS string)").alias("contract_id"),
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("employee_id").alias("employee_id"),
        when(
            trim(col("contract_type")) == "", lit(None)
            ).otherwise(regexp_replace(trim(lower(col("contract_type"))), "_v\\d+", "")).alias("contract_type_base"),
        normalize_to_date_format("start_date").alias("start_date"),
        normalize_to_date_format("end_date").alias("end_date"),
        normalize_to_date_format("probation_end_date").alias("probation_end_date"),
        expr("coalesce(try_cast(notice_period_days as int), 0)").alias("notice_period_days"),
        normalize_to_date_format("signed_date").alias("signed_date"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # LEVENSHTEIN DISTANCE FIX
    # ==================================================================================================================================================================================

    # Levenshtein distance to match contract_type_base with contract type classification
    # v2: fuzzy_match_category() with max distance 2. Null stays null (as before);
    # values farther than the threshold -> 'unknown' (before they were forced into a category)
    df_contracts_clean = df_contracts_clean_base.withColumn(
        "contract_type",
        fuzzy_match_category(col("contract_type_base"), ["internship", "fixed_term", "permanent"], null_value=None)
    ).drop(
        "contract_type_base"
    )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================
    
    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_contracts_clean.count())

    # df_before_deduplication_emp = df_contracts_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    # v2: deduplication key is contract_id (PK), not employee_id. An employee can have
    # several contracts (1:N relationship in the ERD). Deduplicating by employee_id kept a
    # single contract per employee and collapsed all contracts with a null employee_id into one
    # (the rest were lost before the 'employee_id_null' business validation could quarantine them).
    # The current contract per employee is selected in Gold (window by start_date).
    df_contracts_clean = deduplicate_by_completeness(
        df_contracts_clean,
        ["contract_id"],
        cols_ordered_contracts[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_contracts_clean.count())

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business data validation (step 2/2)
    df_contracts_final_validation = df_contracts_clean.withColumn(
            "rejection_reason",
            when(col("start_date").isNull() & col("end_date").isNull(), "both_contract_dates_null")
            .when(col("start_date").isNull(), "start_date_null")
            .when(normalize_to_date_format("start_date") > normalize_to_date_format("end_date"), "invalid_contract_dates_end_before_start")
            .when(normalize_to_date_format("probation_end_date") < normalize_to_date_format("start_date"), "invalid_probation_end_before_start_date")
            .when(((expr("try_cast(notice_period_days as int)") < 0) | (expr("try_cast(notice_period_days as int)") > 100)), "notice_period_days_out_of_range")
            .when(col("employee_id").isNull(), "employee_id_null")
            .otherwise(None)
            )
    

    # final clean table
    df_contracts_clean_final = df_contracts_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_contracts_quarantine_business = df_contracts_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_cont = {}
    
    # apply data type fix + quarantine dfs consolidation
    df_contracts_quarantine = consolidate_quarantine(
        df_structural_quarantine_cont_remaining,
        df_contracts_quarantine_business,
        problematic_cols_cont,
        bronze_table
        )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    contracts_data_quality = pct_quarantined_data(df_contracts_quarantine,df_contracts_clean_final)
    data_quality_report.append((bronze_table, contracts_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_contracts_clean_final, "contracts", SILVER_LOAD_CONFIG["contracts"], silver_batch_date)
    write_quarantine(df_contracts_quarantine, "contracts", silver_batch_date)  # v3: kept per batch (was overwritten)
    print("CREATED: hr_database.silver.silver_contracts")
    print("CREATED: hr_database.silver.silver_quarantine_contracts")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_contracts_quarantine.isEmpty():
        log_pipeline_event("silver", "contracts", "WARNING", f"Data in quarantine for review: {contracts_data_quality}%")
    else:
        log_pipeline_event("silver", "contracts", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "contracts", "ERROR", str(e))
    failed_entities.append({"entity": "contracts", "error": str(e)})  # v2





# COMMAND ----------

# MAGIC %md
# MAGIC ### SALARY HISTORY -- ok

# COMMAND ----------

# DBTITLE 1,salary_history
# SALARY_HISTORY ----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Business validation (validation 2/2)
# 7) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 8) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 9) Persist to Delta (Silver)



try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_salary_history"
    df_salary_history_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_salary_history = [
        "history_id",
        "employee_id",
        "old_salary",
        "new_salary",
        "change_reason",
        "effective_date",
        "approved_by"
        ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_sal_hist = structural_validation(
        df_salary_history_raw, 
        cols_ordered_salary_history[0], 
        cols_ordered_salary_history[1:]
        )

    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_sal_hist = df_structural_validation_sal_hist.filter(
        col("rejection_reason").isNotNull()
        )
    
    # Records that passed structural validation
    df_ready_for_cleaning_sal_hist = df_structural_validation_sal_hist.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_sal_hist, df_repaired_physical_rows_sal_hist = repair_crammed_records(
        df_salary_history_raw,
        df_structural_quarantine_sal_hist,
        df_ready_for_cleaning_sal_hist,
        cols_ordered_salary_history[0],
        cols_ordered_salary_history[1:]
        )
    
    df_structural_quarantine_sal_hist_remaining = (
        df_structural_quarantine_sal_hist.join(
        df_repaired_physical_rows_sal_hist,
        on="physical_row_idx",
        how="left_anti"
        )
        )

    # v2: records rejected only because of a null PK get a deterministic surrogate key ('SK-...')
    # and go back to the normal flow (see recover_null_pk() in 00_utils)
    df_unified_ready_for_cleaning_sal_hist, df_structural_quarantine_sal_hist_remaining = recover_null_pk(
        df_structural_quarantine_sal_hist_remaining,
        df_unified_ready_for_cleaning_sal_hist,
        cols_ordered_salary_history[0],
        cols_ordered_salary_history[1:]
        )

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # clean records
    df_salary_history_clean = df_unified_ready_for_cleaning_sal_hist.select(
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("history_id").alias("history_id"),
        normalize_legacy_id("employee_id").alias("employee_id"),
        # in old/new salary just a few values are negative, and they are consistent with human errors in the record input.
        # 0s are turned into nulls to avoid misconceptions in salary changes (old vs new)
        when(col("old_salary") == "N/A", lit(None)).otherwise(expr("nullif(abs(try_cast(old_salary as decimal(10, 2))), 0.0)")).alias("old_salary"),
        when(col("new_salary") == "N/A", lit(None)).otherwise(expr("nullif(abs(try_cast(new_salary as decimal(10, 2))), 0.0)")).alias("new_salary"),
        lower(trim(col("change_reason"))).alias("change_reason"),
        normalize_to_date_format("effective_date").alias("effective_date"),
        when((trim(col("approved_by")) == "N/A") | (col("approved_by").isNull()), lit("unknown"))
        .otherwise(regexp_replace(trim(col("approved_by")), "-", "")).alias("approved_by"),
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_salary_history_clean.count())

    # df_before_deduplication_sal_hist = df_salary_history_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_salary_history_clean = deduplicate_by_completeness(
        df_salary_history_clean,
        ["history_id"],
        cols_ordered_salary_history[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_salary_history_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_sal_hist = (
    #     df_salary_history_clean
    #     .groupBy("history_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_sal_hist.count())
    # df_duplicate_ids_sal_hist.show()

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_salary_history_final_validation = df_salary_history_clean.withColumn(
        "rejection_reason",
        when((col("old_salary") < 0.00)
            | (col("old_salary") > 1000000.00)
            | (col("new_salary") < 0.00)
            | (col("new_salary") > 1000000.00)
            ,"salary_out_of_range")
        .when(
            (lower(trim(col("change_reason"))) == "promotion") & (col("old_salary") > col("new_salary")),
            "promotion_awkward_criteria"
            )
        .when(((lower(trim(col("change_reason"))) == "promotion") & (col("new_salary").isNull())), "promotion_salary_data_missing")
        .when(col("employee_id").isNull(), "employee_id_null")
        .when(col("effective_date").isNull(), "effective_date_null")
        .when(col("old_salary").isNull() & col("new_salary").isNull(), "no_old_or_new_salary_info")
        .when(col("old_salary").isNotNull() & col("new_salary").isNull(), "no_new_salary_info")
        .otherwise(None)
        )

    # final clean table
    df_salary_history_clean_final = df_salary_history_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_salary_history_quarantine_business = df_salary_history_final_validation.filter(
        col("rejection_reason").isNotNull()
        )
    
    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_sal_hist = {
        "effective_date": normalize_to_date_format("effective_date")
        }
    
    # apply data type fix + quarantine dfs consolidation
    df_salary_history_quarantine = consolidate_quarantine(
        df_structural_quarantine_sal_hist_remaining,
        df_salary_history_quarantine_business,
        problematic_cols_sal_hist,
        bronze_table
        )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    salary_history_data_quality = pct_quarantined_data(df_salary_history_quarantine,df_salary_history_clean_final)
    data_quality_report.append((bronze_table, salary_history_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_salary_history_clean_final, "salary_history", SILVER_LOAD_CONFIG["salary_history"], silver_batch_date)
    write_quarantine(df_salary_history_quarantine, "salary_history", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_salary_history")
    print("CREATED: hr_database.silver.silver_quarantine_salary_history")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================
    
    if not df_salary_history_quarantine.isEmpty():
        log_pipeline_event("silver", "salary_history", "WARNING", f"Data in quarantine for review: {salary_history_data_quality}%")
    else:
        log_pipeline_event("silver", "salary_history", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "salary_history", "ERROR", str(e))
    failed_entities.append({"entity": "salary_history", "error": str(e)})  # v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### SALARIES -- ok

# COMMAND ----------

# DBTITLE 1,salaries
# SALARIES ----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Business validation (validation 2/2)
# 7) Deduplication based on the latest effective date (using salary history table)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)




try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_salaries"
    df_salaries_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_salaries = [
        "salary_id",
        "employee_id",
        "base_salary",
        "currency",
        "bonus_percentage",
        "effective_date",
        "pay_frequency"
        ]
    
    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_sal = structural_validation(
        df_salaries_raw, 
        cols_ordered_salaries[0], 
        cols_ordered_salaries[1:]
    )
        
    # structural validation errors (validation's step 1 is complete)
    df_structural_quarantine_sal = df_structural_validation_sal.filter(
        col("rejection_reason").isNotNull()
        )

    # Records that passed structural validation
    df_ready_for_cleaning_sal = df_structural_validation_sal.filter(
        col("rejection_reason").isNull()
    ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_sal, df_repaired_physical_rows_sal = repair_crammed_records(
        df_salaries_raw,
        df_structural_quarantine_sal,
        df_ready_for_cleaning_sal,
        cols_ordered_salaries[0],
        cols_ordered_salaries[1:]
        )
    
    df_structural_quarantine_sal_remaining = (
        df_structural_quarantine_sal.join(
        df_repaired_physical_rows_sal,
        on="physical_row_idx",
        how="left_anti"
        )
        )

    # v2: records rejected only because of a null PK get a deterministic surrogate key ('SK-...')
    # and go back to the normal flow (see recover_null_pk() in 00_utils)
    df_unified_ready_for_cleaning_sal, df_structural_quarantine_sal_remaining = recover_null_pk(
        df_structural_quarantine_sal_remaining,
        df_unified_ready_for_cleaning_sal,
        cols_ordered_salaries[0],
        cols_ordered_salaries[1:]
        )

    # # ==================================================================================================================================================================================
    # # DEDUPLICATION
    # # ==================================================================================================================================================================================

    # # Checkpoint before deduplication
    # print("BEFORE DEDUPLICATION:", df_unified_ready_for_cleaning_sal.count())

    # # df_before_deduplication_sal = df_unified_ready_for_cleaning_sal

    # # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    # df_unified_ready_for_cleaning_sal = deduplicate_by_completeness(
    #     df_unified_ready_for_cleaning_sal,
    #     ["salary_id"],
    #     cols_ordered_salaries[1:]
    #     )
    
    # # Checkpoint after deduplication
    # print("AFTER DEDUPLICATION:", df_unified_ready_for_cleaning_sal.count())

    # # Check for remaining duplicate employee IDs
    # # df_duplicate_ids_sal = (
    # #     df_unified_ready_for_cleaning_sal
    # #     .groupBy("salary_id")
    # #     .count()
    # #     .filter(col("count") > 1)
    # #     )

    # # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_sal.count())
    # # df_duplicate_ids_sal.show()

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # clean records
    df_salaries_clean = df_unified_ready_for_cleaning_sal.select(
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("salary_id").alias("salary_id"),
        normalize_legacy_id("employee_id").alias("employee_id"),
        # v2: missing values (empty / 'N/A') stay null. Before, they were set to 0.0 and quarantined
        # as 'base_salary_out_of_range', which hid that the salary was actually missing.
        expr("try_cast(base_salary as decimal(10, 2))").alias("base_salary"),
        coalesce(upper(trim(regexp_replace(col("currency"), "\\s+", " "))), lit("USD")).alias("currency"),
        when(col("bonus_percentage").isNull() | (trim(col("bonus_percentage")).isin("N/A", "")), lit(0.0))
        .otherwise(expr("try_cast(bonus_percentage AS decimal(10, 2))")).alias("bonus_percentage"),
        normalize_to_date_format("effective_date").alias("effective_date"),
        coalesce(lower(trim(regexp_replace(col("pay_frequency"), "\\s+", " "))), lit("monthly")).alias("pay_frequency"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )
    
    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_salaries_clean.count())

    # df_before_deduplication_sal = df_salaries_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_salaries_clean = deduplicate_by_completeness(
        df_salaries_clean,
        ["salary_id"],
        cols_ordered_salaries[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_salaries_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_sal = (
    #     df_salaries_clean
    #     .groupBy("salary_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_sal.count())
    # df_duplicate_ids_sal.show()

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_salaries_final_validation = df_salaries_clean.withColumn(
            "rejection_reason",
            when(col("employee_id").isNull(), "employee_id_null")
            .when(col("base_salary").isNull(), "base_salary_null")
            # salaries are always monthly (in this case, it would be weird to earn more than 1M per month)
            # v2: specific rejection reasons instead of a single 'base_salary_out_of_range'
            .when(col("base_salary") == 0.00, "base_salary_zero")
            # v2: BUSINESS RULE: negative salaries are quarantined for manual review.
            # Note: their absolute values (31,637 / 39,893 / 129,058 / 354,083 in the current source)
            # look like plausible salaries, so the sign is probably a data-entry error. They are NOT
            # auto-corrected on purpose; salary_history applies abs(), this table intentionally does not.
            .when(col("base_salary") < 0.00, "base_salary_negative")
            .when(col("base_salary") > 1000000.00, "base_salary_out_of_range")
            .when(col("effective_date").isNull(), "effective_date_null")
            .otherwise(None)
            )

    # final clean table
    df_salaries_clean_records = df_salaries_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")
    
    # business quarantined records
    df_salaries_quarantine_business = df_salaries_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # DEDUPLICATION BASED ON THE LATEST EFFECTIVE DATE (USING SALARY_HISTORY TABLE)
    # ==================================================================================================================================================================================

    # silver_salary_history to be used for salary table
    # v3: silver_salary_history is merged: rows no longer in the source are flagged, not deleted
    df_sal_history_clean = spark.table("hr_database.silver.silver_salary_history").filter(~col("_deleted_in_source"))

    # Columns from salary_history ready for the union.
    df_salary_history_emp = df_sal_history_clean.select(
        # history_id as salary_id to keep a string PK
        # v2: 'HIST-' prefix, so history IDs cannot be confused with (or collide with) salary IDs
        concat(lit("HIST-"), col("history_id")).alias("salary_id"),
        col("employee_id"),
        # new_salary as the latest salary
        col("new_salary").alias("base_salary"),
        # default values for compatibility
        lit("USD").alias("currency"),
        expr("try_cast(0.0 as decimal(10, 2))").alias("bonus_percentage"),
        col("effective_date"),
        lit("monthly").alias("pay_frequency"),
        col("_ingested_at"),
        col("_source_file")
    )

    # union
    df_emp_salaries_union = df_salaries_clean_records.unionByName(df_salary_history_emp)

    # v2: future-dated salaries (scheduled changes) are valid data, not errors, so they are not
    # quarantined. They are kept in silver_salaries with is_current = false and do not take part
    # in the "current salary" window (before, they won it because they had the latest date).
    df_emp_salaries_scheduled = df_emp_salaries_union.filter(
        col("effective_date") > current_date()
        ).withColumn("is_current", lit(False))

    df_emp_salaries_union = df_emp_salaries_union.filter(col("effective_date") <= current_date())

    # deduplication using a window function
    # group by employee, ordered by effective_date (desc) 
    window_unification = Window.partitionBy("employee_id").orderBy(
        col("effective_date").desc(),
        col("_ingested_at").desc()
    )

    #  we keep the latest record (no matter the source (salary or salary history))
    df_salaries_clean_final_window = df_emp_salaries_union.withColumn(
        "rn", row_number().over(window_unification)
        ).filter(col("rn") == 1).drop("rn").withColumn("is_current", lit(True))

    # v2: silver_salaries = current salary per employee (is_current = true)
    #                      + scheduled future salaries (is_current = false)
    df_salaries_clean_final_window = df_salaries_clean_final_window.unionByName(df_emp_salaries_scheduled)

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_sal = {
        "effective_date": normalize_to_date_format("effective_date")
        }
    
    # apply data type fix + quarantine dfs consolidation
    df_salaries_quarantine = consolidate_quarantine(
        df_structural_quarantine_sal_remaining,
        df_salaries_quarantine_business,
        problematic_cols_sal,
        bronze_table
        )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    # v2: denominator = validated salary records (before the per-employee window), so the % is
    # comparable with the other tables. Before, it used the final table (one row per employee).
    salaries_data_quality = pct_quarantined_data(df_salaries_quarantine,df_salaries_clean_records)
    data_quality_report.append((bronze_table, salaries_data_quality))
    
    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_salaries_clean_final_window, "salaries", SILVER_LOAD_CONFIG["salaries"], silver_batch_date)
    write_quarantine(df_salaries_quarantine, "salaries", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_salaries")
    print("CREATED: hr_database.silver.silver_quarantine_salaries")
    
    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_salaries_quarantine.isEmpty():
        log_pipeline_event("silver", "salaries", "WARNING", f"Data in quarantine for review: {salaries_data_quality}%")
    else:
        log_pipeline_event("silver", "salaries", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "salaries", "ERROR", str(e))
    failed_entities.append({"entity": "salaries", "error": str(e)})  # v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRAININGS -- ok

# COMMAND ----------

# DBTITLE 1,trainings
# TRAININGS ---------------------------------------------- READY

# no need to build a quarantine table. no data cramming on sight, records are a few and do not change much.
# nulls and "N/A" in duration are coalesced to 0 since they are not errors in the data
# nulls in is_mandatory are changed to false

try:

    bronze_table = strip_line_breaks(read_batch("hr_database.bronze.bronze_trainings", silver_batch_date))

    df_trainings_clean = bronze_table.select(
        expr("try_cast(training_id AS string)").alias("training_id"),
        trim(coalesce(col("training_name"), lit("Unknown"))).alias("training_name"),
        trim(col("provider")).alias("provider"),
        when(col("duration_hours").isNull() | (trim(col("duration_hours")).isin("N/A", "")), lit(0))
        .otherwise(expr("try_cast(duration_hours AS integer)")).alias("duration_hours"),
        expr("try_cast(cost_per_person AS decimal(10, 2))").alias("cost_per_person"),
        # v2: explicit try_cast to boolean. Before, coalesce(string, boolean) mixed types: the column
        # stayed as string and, depending on the runtime's ANSI/type-coercion settings, it could fail.
        coalesce(expr("try_cast(is_mandatory AS boolean)"), lit(False)).alias("is_mandatory"),
        trim(coalesce(col("category"), lit("Unknown"))).alias("category"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file")
        ).dropDuplicates(
            ["training_id"]
            )

    # persist clean table to Delta
    write_silver(df_trainings_clean, "trainings", SILVER_LOAD_CONFIG["trainings"], silver_batch_date)  # v3
    print("CREATED: hr_database.silver.silver_trainings")

    # data quality %
    trainings_data_quality = "not_applicable"
    data_quality_report.append(("hr_database.bronze.bronze_trainings", trainings_data_quality))
    print(f"Data in quarantine for review: {trainings_data_quality}")

    # no if/else since we do not have a quarantine table.
    log_pipeline_event("silver", "trainings", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "trainings", "ERROR", str(e))
    failed_entities.append({"entity": "trainings", "error": str(e)})  # v2


# COMMAND ----------

# MAGIC %md
# MAGIC ### PERFORMANCE REVIEWS -- ok

# COMMAND ----------

# DBTITLE 1,performance_reviews
#  PERFORMANCE_REVIEWS ----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Business validation (validation 2/2)
# 7) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 8) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 9) Persist to Delta (Silver)



try:

    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_performance_reviews"
    df_performance_reviews_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_performance_review = [
        "review_id",
        "employee_id",
        "reviewer_id",
        "review_period",
        "overall_rating",
        "technical_skills",
        "communication",
        "teamwork",
        "leadership",
        "comments",
        "review_date",
        "status"
        ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_perf_rev = structural_validation(
        df_performance_reviews_raw, 
        cols_ordered_performance_review[0], 
        cols_ordered_performance_review[1:]
        )

    # quarantined errors (validation's step 1 is complete)
    df_structural_quarantine_perf_rev = df_structural_validation_perf_rev.filter(
        col("rejection_reason").isNotNull()
        )
    
    # Records that passed structural validation
    df_ready_for_cleaning_perf_rev = df_structural_validation_perf_rev.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_perf_rev, df_repaired_physical_rows_perf_rev = repair_crammed_records(
        df_performance_reviews_raw,
        df_structural_quarantine_perf_rev,
        df_ready_for_cleaning_perf_rev,
        cols_ordered_performance_review[0],
        cols_ordered_performance_review[1:]
        )
    
    df_structural_quarantine_perf_rev_remaining = (
        df_structural_quarantine_perf_rev.join(
        df_repaired_physical_rows_perf_rev,
        on="physical_row_idx",
        how="left_anti"
        )
        )

    # v2: records rejected only because of a null PK get a deterministic surrogate key ('SK-...')
    # and go back to the normal flow (see recover_null_pk() in 00_utils)
    df_unified_ready_for_cleaning_perf_rev, df_structural_quarantine_perf_rev_remaining = recover_null_pk(
        df_structural_quarantine_perf_rev_remaining,
        df_unified_ready_for_cleaning_perf_rev,
        cols_ordered_performance_review[0],
        cols_ordered_performance_review[1:]
        )

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # clean records
    df_performance_reviews_clean = df_unified_ready_for_cleaning_perf_rev.select(
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("review_id").alias("review_id"),
        normalize_legacy_id("employee_id").alias("employee_id"),
        normalize_legacy_id("reviewer_id").alias("reviewer_id"),
        coalesce(initcap(trim(regexp_replace(col("review_period"), "\\s+", " "))), lit("Unknown")).alias("review_period"),
        # v2: 'N/A' -> null (it was 0, a valid score that pulled averages down) and no abs():
        # negative scores are kept negative so the business validation (< 0) quarantines them.
        expr("try_cast(overall_rating as decimal(3, 2))").alias("overall_rating"),
        expr("try_cast(technical_skills as decimal(3, 2))").alias("technical_skills"),
        expr("try_cast(communication as decimal(3, 2))").alias("communication"),
        expr("try_cast(teamwork as decimal(3, 2))").alias("teamwork"),
        expr("try_cast(leadership as decimal(3, 2))").alias("leadership"),
        trim(coalesce(col("comments"), lit(""))).alias("comments"),
        normalize_to_date_format("review_date").alias("review_date"),
        # no need to apply Levenshtein distance here. Quick fix with regexp_replace()
        lower(trim(regexp_replace(when(
            col("status") == "cmopleted", lit("completed")
            ).otherwise(coalesce(col("status"), lit("Unknown"))),"_v2", ""
                                    )
                    )
                ).alias("status"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_performance_reviews_clean.count())

    # df_before_deduplication_perf_rev = df_performance_reviews_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_performance_reviews_clean = deduplicate_by_completeness(
        df_performance_reviews_clean,
        ["review_id"],
        cols_ordered_performance_review[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_performance_reviews_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_perf_rev = (
    #     df_performance_reviews_clean
    #     .groupBy("review_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_perf_rev.count())
    # df_duplicate_ids_perf_rev.show()

    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_performance_reviews_final_validation = df_performance_reviews_clean.withColumn(
        "rejection_reason",
        when(col("employee_id").isNull(), "employee_id_null")
        .when((col("overall_rating") < 0.0) | (col("overall_rating") > 5.0), "overall_rating_out_of_range")
        .when((col("technical_skills") < 0.0) | (col("technical_skills") > 5.0), "technical_skills_out_of_range")
        .when((col("communication") < 0.0) | (col("communication") > 5.0), "communication_out_of_range")
        .when((col("teamwork") < 0.0) | (col("teamwork") > 5.0), "teamwork_out_of_range")
        .when((col("leadership") < 0.0) | (col("leadership") > 5.0), "leadership_out_of_range")
        .when(col("overall_rating").isNull(), "overall_rating_null")
        .when((col("review_date") > current_date()) & (col("status") == "completed"), "review_date_status_mismatch")
        .otherwise(None)
        )

    # final clean table
    df_performance_reviews_clean_final = df_performance_reviews_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_performance_reviews_quarantine_business = df_performance_reviews_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_perf_rev = {
        "overall_rating": expr("try_cast(overall_rating as decimal(3, 2))"),  # v2: 'N/A' -> null
        "technical_skills": expr("try_cast(technical_skills as decimal(3, 2))"),  # v2: 'N/A' -> null
        "communication": expr("try_cast(communication as decimal(3, 2))"),  # v2: 'N/A' -> null
        "teamwork": expr("try_cast(teamwork as decimal(3, 2))"),  # v2: 'N/A' -> null
        "leadership": expr("try_cast(leadership as decimal(3, 2))"),  # v2: 'N/A' -> null
        "review_date": normalize_to_date_format("review_date")
    }
    
    # apply data type fix + quarantine dfs consolidation
    df_performance_reviews_quarantine = consolidate_quarantine(
        df_structural_quarantine_perf_rev_remaining,
        df_performance_reviews_quarantine_business,
        problematic_cols_perf_rev,
        bronze_table
        )


    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    performance_reviews_data_quality = pct_quarantined_data(df_performance_reviews_quarantine,df_performance_reviews_clean_final)
    data_quality_report.append((bronze_table, performance_reviews_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_performance_reviews_clean_final, "performance_reviews", SILVER_LOAD_CONFIG["performance_reviews"], silver_batch_date)
    write_quarantine(df_performance_reviews_quarantine, "performance_reviews", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_performance_reviews")
    print("CREATED: hr_database.silver.silver_quarantine_performance_reviews")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_performance_reviews_quarantine.isEmpty():
        log_pipeline_event("silver", "performance_reviews", "WARNING", f"Data in quarantine for review: {performance_reviews_data_quality}%")
    else:
        log_pipeline_event("silver", "performance_reviews", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "performance_reviews", "ERROR", str(e))
    failed_entities.append({"entity": "performance_reviews", "error": str(e)})  # v2


# COMMAND ----------

# MAGIC %md
# MAGIC ### JOB POSTINGS -- ok

# COMMAND ----------

# DBTITLE 1,job postings
# JOB POSTINGS -----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Deduplication criteria (deduplicate_by_completeness())
# 6) Positions mismatch fix
# 7) Business validation (validation 2/2)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)


try:
    
    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_job_postings"
    df_job_postings_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )



    # v2: business columns in the PHYSICAL order of the CSV (as required by structural_validation()
    # and repair_crammed_records()). Before, the order was different and 4 columns were missing.
    cols_ordered_job_postings = [
        "posting_id",
        "position_id",
        "department_id",
        "title",
        "description",
        "requirements",
        "salary_min",
        "salary_max",
        "location",
        "posted_date",
        "closing_date",
        "status",
        "hiring_manager_id"
    ]

    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption


    df_structural_validation_jp = structural_validation(
        df_job_postings_raw,
        cols_ordered_job_postings[0],
        cols_ordered_job_postings[1:]
        ) 

    # quarantined errors (validation's step 1 is complete)
    df_structural_quarantine_jp = df_structural_validation_jp.filter(
        col("rejection_reason").isNotNull()
        )

    # Records that passed structural validation
    df_ready_for_cleaning_jp = df_structural_validation_jp.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")


    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_jp, df_repaired_physical_rows_jp = repair_crammed_records(
        df_job_postings_raw,
        df_structural_quarantine_jp,
        df_ready_for_cleaning_jp,
        cols_ordered_job_postings[0],
        cols_ordered_job_postings[1:]
        )
    
    df_structural_quarantine_jp_remaining = (
        df_structural_quarantine_jp.join(
            df_repaired_physical_rows_jp,
            on="physical_row_idx",
            how="left_anti"
            )
        )

    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # data transformation
    df_job_postings_start = df_unified_ready_for_cleaning_jp.select(
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("posting_id").alias("posting_id"),
        normalize_legacy_id("position_id").alias("position_id"),
        trim(col("title")).alias("title"),
        coalesce(lower(trim(col("status"))), lit("unknown")).alias("status"),
        initcap(trim(col("location"))).alias("location"),
        expr("try_cast(salary_min AS decimal(10,2))").alias("salary_min"),
        expr("try_cast(salary_max AS decimal(10,2))").alias("salary_max"),
        normalize_to_date_format("posted_date").alias("posted_date"),
        normalize_to_date_format("closing_date").alias("closing_date"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_job_postings_start.count())

    # df_before_deduplication_jp = df_job_postings_start

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    # v2: completeness is measured on the columns kept in Silver (cols_ordered_job_postings now also
    # lists source columns that are not carried into Silver, e.g. description, requirements)
    df_job_postings_start = deduplicate_by_completeness(
        df_job_postings_start,
        ["posting_id"],
        ["position_id", "title", "status", "location", "salary_min", "salary_max", "posted_date", "closing_date"]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_job_postings_start.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_jp = (
    #     df_job_postings_start
    #     .groupBy("posting_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_jp.count())
    # df_duplicate_ids_jp.show()

    # ==================================================================================================================================================================================
    # POSITION MISMATCH FIX
    # ==================================================================================================================================================================================

    df_positions_reference = spark.table("hr_database.silver.silver_positions").select(
            col("position_id").alias("reference_position_id"),
            col("position_title").alias("reference_position_title")
        )

    df_job_postings_joined_id = df_job_postings_start.join(
        df_positions_reference,
        df_job_postings_start.position_id == df_positions_reference.reference_position_id, 
        "left"
    )

    # v2: silver_positions is the source of truth for the title (the posting title is free text and,
    # in the current source, it disagrees with its position in every posting). The original title is
    # kept in source_title and differences are flagged in title_mismatch for traceability.
    df_job_postings_joined_id_result = df_job_postings_joined_id.withColumn(
        "source_title", col("title")
        ).withColumn(
        "title_mismatch",
        col("reference_position_id").isNotNull()
        & (coalesce(lower(trim(col("title"))), lit("")) != lower(trim(col("reference_position_title"))))
        ).withColumn(
        "title",
        when(col("reference_position_id").isNull(), col("title")).otherwise(
            col("reference_position_title")
            )
        ).drop(
            "reference_position_id", "reference_position_title"
            )

    df_positions_ref_by_text = df_positions_reference.select(
        col("reference_position_id").alias("ref_id_by_text"),
        lower(trim(col("reference_position_title"))).alias("ref_title_clean")
    )

    df_job_postings_joined_text = df_job_postings_joined_id_result.join(
        df_positions_ref_by_text,
        lower(trim(df_job_postings_joined_id_result.title)) == df_positions_ref_by_text.ref_title_clean,
        "left"
    )

    # clean records
    df_job_postings_clean = df_job_postings_joined_text.withColumn(
        "position_id",
        when(col("position_id").isNull(), col("ref_id_by_text")).otherwise(
            col("position_id")
            )
    ).drop(
        "ref_id_by_text", "ref_title_clean"
        )
    
    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_job_postings_final_validation = df_job_postings_clean.withColumn(
            "rejection_reason",
            when((col("closing_date").isNull()) & (col("status").isin(["closed", "filled"])), "closing_date_null_status_filled_closed")
            .when((col("closing_date").isNotNull()) & (col("status") == "open"), "closing_date_not_null_status_open")
            .when(normalize_to_date_format("posted_date") > normalize_to_date_format("closing_date"), "posting_date_bigger_than_closing_date")
            .when(col("salary_min") > col("salary_max"), "salary_min_bigger_than_salary_max")
            .otherwise(None)
            )
    
    # final clean table
    df_job_postings_clean_final = df_job_postings_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_job_postings_quarantine_business = df_job_postings_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_jp = {}
    
    # apply data type fix + quarantine dfs consolidation
    df_job_postings_quarantine = consolidate_quarantine(
        df_structural_quarantine_jp_remaining,
        df_job_postings_quarantine_business,
        problematic_cols_jp,
        bronze_table
        )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    job_postings_data_quality = pct_quarantined_data(df_job_postings_quarantine,df_job_postings_clean_final)
    data_quality_report.append((bronze_table, job_postings_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_job_postings_clean_final, "job_postings", SILVER_LOAD_CONFIG["job_postings"], silver_batch_date)
    write_quarantine(df_job_postings_quarantine, "job_postings", silver_batch_date)  # v3: kept per batch (was overwritten)

    print("CREATED: hr_database.silver.silver_job_postings")
    print("CREATED: hr_database.silver.silver_quarantine_job_postings")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_job_postings_quarantine.isEmpty():
        log_pipeline_event("silver", "job_postings", "WARNING", f"Data in quarantine for review: {job_postings_data_quality}%")
    else:
        log_pipeline_event("silver", "job_postings", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "job_postings", "ERROR", str(e))
    failed_entities.append({"entity": "job_postings", "error": str(e)})  # v2


# COMMAND ----------

# MAGIC %md
# MAGIC ### CANDIDATES -- ok

# COMMAND ----------

# DBTITLE 1,candidates
# CANDIDATES ----------------------------------------------

# VALIDATION LOGIC:
# 2 steps validation (step 1: structural validation, step 2: business rules validation): there are crammed data cases that can be missed after transformation (specifically in nulls treatment due to the logic to detect crammed data cases). Along with detection of nulls in the Primary Key (FIRST line of defense) they are step 1 of 2 of validation.
# Crammed records are repaired and put into the ready-to-clean DF. If repair fails, the record remains in quarantine.
# By splitting validation into 2 parts, cast is protected: by filtering out null PKs and crammed patterns at the beginning, we prevent the cleaning process from attempting to transform rows that are technically unreadable, avoiding runtime errors.
# unified quarantine table with consolidate_quarantine(): The final quarantine table will contain all rejected rows in both validation steps.

# ERROR HANDLING:
# try-except is used to handle errors in the transformation process
# if an error occurs, we'll catch it in an action. 'try' does not catch lazy evaluations (code does not execute until the result is necessary).
# action examples:
# 'count()' via the function 'pct_quarantined_data()'. lazy evaluations break to execute the transformation and save the result.
#  writing action when we persist to Delta.
# '.isEmpty()' in logs logic.

# FLOW:
# 1) Raw data from Bronze
# 2) Structural validation (validation 1/2)
# 3) Crammed-records repair (repair_crammed_records())
# 4) Transformation/Casting
# 5) Levenshtein distance fix
# 6) Deduplication criteria (deduplicate_by_completeness())
# 7) Business validation (validation 2/2)
# 8) Quarantine (structural and business) consolidation (consolidate_quarantine())
# 9) Data quality % (pct_quarantined_data()) + append to data_quality_report list
# 10)Persist to Delta (Silver)


try:

    
    # ==================================================================================================================================================================================
    # BRONZE TABLE
    # ==================================================================================================================================================================================

    bronze_table = "hr_database.bronze.bronze_candidates"
    df_candidates_raw = strip_line_breaks(read_batch(bronze_table, silver_batch_date)).withColumn(
        "physical_row_idx",
        monotonically_increasing_id()
        )

    # Business columns in the physical order of the table, including the primary key.
    # Ingestion metadata columns are intentionally excluded

    cols_ordered_candidates = [
        "candidate_id",
        "posting_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "resume_url",
        "source",
        "applied_date",
        "status"
        ]
    
    # ==================================================================================================================================================================================
    # STRUCTURAL VALIDATION (VALIDATION 1/2)
    # ==================================================================================================================================================================================  
    
    # The first column is the primary key (PK); the remaining columns are
    # the business columns inspected for structural corruption

    df_structural_validation_cand = structural_validation(
        df_candidates_raw, 
        cols_ordered_candidates[0], 
        cols_ordered_candidates[1:]
        )

    # quarantined errors (validation's step 1 is complete)
    df_structural_quarantine_cand = df_structural_validation_cand.filter(
        col("rejection_reason").isNotNull()
        )
    
    # Records that passed structural validation
    df_ready_for_cleaning_cand = df_structural_validation_cand.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # ==================================================================================================================================================================================
    # REPAIR QUARANTINED RECORDS WITH STRUCTURAL ISSUES (CRAMMED DATA)
    # ==================================================================================================================================================================================

    # Repairs recoverable crammed records and reintegrates them with the
    # records that already passed structural validation

    df_unified_ready_for_cleaning_cand, df_repaired_physical_rows_cand = repair_crammed_records(
        df_candidates_raw,
        df_structural_quarantine_cand,
        df_ready_for_cleaning_cand,
        cols_ordered_candidates[0],
        cols_ordered_candidates[1:]
        )
    
    df_structural_quarantine_cand_remaining = (
        df_structural_quarantine_cand.join(
            df_repaired_physical_rows_cand,
            on="physical_row_idx",
            how="left_anti"
            )
        )


    # ==================================================================================================================================================================================
    # DATA TRANSFORMATION / CASTING
    # ==================================================================================================================================================================================

    # data transformation
    df_candidates_start = df_unified_ready_for_cleaning_cand.select(
        trim(col("candidate_id")).alias("candidate_id"),
        # v2: ID normalization with normalize_legacy_id() (legacy id = 100000 + real_id - 1)
        normalize_legacy_id("posting_id").alias("posting_id"),
        initcap(trim(col("first_name"))).alias("first_name"),
        initcap(trim(col("last_name"))).alias("last_name"),
        lower(trim(col("email"))).alias("email"),
        trim(col("phone")).alias("phone"),
        trim(col("resume_url")).alias("resume_url"),
        # v2: canonical source names. initcap() turned 'LinkedIn' into 'Linkedin'.
        # Known sources are mapped explicitly; unknown ones keep initcap(); null -> 'Unknown'.
        when(lower(trim(col("source"))) == "linkedin", lit("LinkedIn"))
        .when(lower(trim(col("source"))) == "indeed", lit("Indeed"))
        .when(lower(trim(col("source"))) == "recruiter", lit("Recruiter"))
        .when(lower(trim(col("source"))) == "referral", lit("Referral"))
        .when(lower(trim(col("source"))) == "company website", lit("Company Website"))
        .otherwise(coalesce(initcap(trim(col("source"))), lit("Unknown"))).alias("source"),
        normalize_to_date_format("applied_date").alias("applied_date"),
        # v2: lower + trim + '_vN' removed (before only initcap: 'Hired_v2' was at distance 3 from 'Hired')
        regexp_replace(trim(lower(col("status"))), "_v\\d+$", "").alias("status_start"),
        # we manually select '_ingested_at' and '_source_file' because we are validating data in 2 steps
        # and for the quarantined records in the second validation these columns would be null.
        col("_ingested_at"),
        col("_source_file"),
        col("physical_row_idx")
        )

    # ==================================================================================================================================================================================
    # LEVENSHTEIN DISTANCE FIX
    # ==================================================================================================================================================================================

    # levenshtein distance
    # v2: fuzzy_match_category() with max distance 2 (values farther than that -> 'unknown')
    df_candidates_clean = df_candidates_start.withColumn(
        "status",
        fuzzy_match_category(col("status_start"), ["screening", "offer", "interview", "applied", "hired", "rejected"])
    ).drop(
        "status_start"
    )

    # ==================================================================================================================================================================================
    # DEDUPLICATION
    # ==================================================================================================================================================================================

    # Checkpoint before deduplication   
    if SILVER_DEBUG_COUNTS: print("BEFORE DEDUPLICATION:", df_candidates_clean.count())

    # df_before_deduplication_cand = df_candidates_clean

    # Deduplicates records, prioritizing completeness, ingestion timestamp, and physical row index
    df_candidates_clean = deduplicate_by_completeness(
        df_candidates_clean,
        ["candidate_id"],
        cols_ordered_candidates[1:]
        ).drop("physical_row_idx")
    
    # Checkpoint after deduplication
    if SILVER_DEBUG_COUNTS: print("AFTER DEDUPLICATION:", df_candidates_clean.count())

    # # Check for remaining duplicate employee IDs
    # df_duplicate_ids_cand = (
    #     df_candidates_clean
    #     .groupBy("candidate_id")
    #     .count()
    #     .filter(col("count") > 1)
    #     )

    # print("REMAINING DUPLICATE IDs:", df_duplicate_ids_cand.count())
    # df_duplicate_ids_cand.show()


    # ==================================================================================================================================================================================
    # BUSINESS VALIDATION (VALIDATION 2/2)
    # ==================================================================================================================================================================================

    # business validation (step 2/2)
    df_candidates_final_validation = df_candidates_clean.withColumn(
        "rejection_reason",
        when(col("posting_id").isNull(), "posting_id_null")
        .when(col("applied_date").isNull(), "applied_date_null")
        .when((col("first_name").isNull()) & (col("last_name").isNull()), "name_incomplete")
        .otherwise(None)
        )

    # final clean table
    df_candidates_clean_final = df_candidates_final_validation.filter(
        col("rejection_reason").isNull()
        ).drop("rejection_reason")

    # business quarantined records
    df_candidates_quarantine_business = df_candidates_final_validation.filter(
        col("rejection_reason").isNotNull()
        )

    # ==================================================================================================================================================================================
    # QUARANTINE CONSOLIDATION (STRUCTURAL AND BUSINESS)
    # ==================================================================================================================================================================================

    # data type fix to ensure a correct union
    problematic_cols_cand = {}
    
    # apply data type fix + quarantine dfs consolidation
    df_candidates_quarantine = consolidate_quarantine(
        df_structural_quarantine_cand_remaining,
        df_candidates_quarantine_business,
        problematic_cols_cand,
        bronze_table
        )

    # ==================================================================================================================================================================================
    # DATA QUALITY %
    # ==================================================================================================================================================================================

    # data quality %
    candidates_data_quality = pct_quarantined_data(df_candidates_quarantine,df_candidates_clean_final)
    data_quality_report.append((bronze_table, candidates_data_quality))

    # ==================================================================================================================================================================================
    # PERSIST TO DELTA 
    # ==================================================================================================================================================================================

    # persist clean and quarantine tables to Delta
    # v3: written by load strategy (was a full overwrite)
    write_silver(df_candidates_clean_final, "candidates", SILVER_LOAD_CONFIG["candidates"], silver_batch_date)
    write_quarantine(df_candidates_quarantine, "candidates", silver_batch_date)  # v3: kept per batch (was overwritten)
    print("CREATED: hr_database.silver.silver_candidates")
    print("CREATED: hr_database.silver.silver_quarantine_candidates")

    # ==================================================================================================================================================================================
    # LOG PIPELINE EVENT
    # ==================================================================================================================================================================================

    if not df_candidates_quarantine.isEmpty():
        log_pipeline_event("silver", "candidates", "WARNING", f"Data in quarantine for review: {candidates_data_quality}%")
    else:
        log_pipeline_event("silver", "candidates", "SUCCESS")

except Exception as e:
    log_pipeline_event("silver", "candidates", "ERROR", str(e))
    failed_entities.append({"entity": "candidates", "error": str(e)})  # v2


# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA QUALITY REPORT

# COMMAND ----------

# DBTITLE 1,data quality report
print("\n================ SILVER LAYER: CLEANING AND TRANSFORMATION HAS ENDED. ================")


print("\nDATA QUALITY - SILVER PIPELINE")
print("-" * 66)
print(f"{'PROCESSED SOURCE':<50} | {'QUARANTINE %'}")
print("-" * 66)
for table_name, pct in data_quality_report:
    print(f"{table_name:<50} | {'not_applicable' if pct == 'not_applicable' else f'{pct}%'}")

# COMMAND ----------

# DBTITLE 1,v2 - fail on errors
# v2: STOP THE PIPELINE IF ANY SILVER ENTITY FAILED
# Before, errors were logged and the notebook still finished successfully, so Gold would be
# built over stale Silver tables without any warning.

if failed_entities:
    failed_list = ", ".join(f["entity"] for f in failed_entities)
    raise RuntimeError(
        f"SILVER FAILED: {len(failed_entities)} entity(ies) could not be processed: {failed_list}. "
        "See hr_database.monitoring.pipeline_logs for details."
    )

# COMMAND ----------

# DBTITLE 1,v3 - batch control
# v3: only reached when every Silver entity was processed. Gold will be rebuilt from this state.
mark_batch_processed("silver", silver_batch_date)
print(f"SILVER: batch {silver_batch_date} recorded in {BATCH_CONTROL_TABLE}")
