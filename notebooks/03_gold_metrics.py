# Databricks notebook source
# DBTITLE 1,00_utils run
# MAGIC %run /hr_analytics_v3/00_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTS

# COMMAND ----------

# DBTITLE 1,imports
# imports
from pyspark.sql.functions import lower, trim
from pyspark.sql.functions import col, concat_ws, floor, months_between, current_date, coalesce, year, quarter, month, dayofmonth, date_format, expr, count, when, avg, round, sum, lit, datediff, row_number, greatest
from pyspark.sql.functions import lag, lead, max as spark_max  # v3
from pyspark.sql.window import Window
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### CATALOG AND SCHEMA

# COMMAND ----------

# DBTITLE 1,catalog and schema
# catalog and schema setup
spark.sql("USE CATALOG hr_database")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("USE SCHEMA gold")

print("================ GOLD LAYER: BUILDING STAR SCHEMA ================\n")


# COMMAND ----------

# MAGIC %md
# MAGIC ### LOGGING

# COMMAND ----------

# DBTITLE 1,logs configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("goldAggregation")

# COMMAND ----------

# DBTITLE 1,data processing report (creation)
# data processing report

data_processing_report = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM: DEPARTMENTS -ready

# COMMAND ----------

# DBTITLE 1,dim_departments
# DIM: DEPARTMENTS ----------------------------------------------------------------------

try:
    silver_table = "hr_database.silver.silver_departments"

    df_dim_departments = spark.table(silver_table).select(
        col("department_id"),
        col("department_name"),
        col("budget"),
        col("location"),
        col("head_count_limit")
        )

    # persist to Delta
    df_dim_departments.write.format("delta").mode("overwrite").saveAsTable("hr_database.gold.gold_dim_departments")

    if not df_dim_departments.isEmpty():
        print("CREATED: hr_database.gold.gold_dim_departments")
        log_pipeline_event("gold", "departments", "SUCCESS")
        data_processing_report.append((silver_table, "gold.gold_dim_departments", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_dim_departments")
    log_pipeline_event("gold", "departments", "ERROR", str(e))
    data_processing_report.append((silver_table, "gold.gold_dim_departments", "failed"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_dim_departments
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM: POSITIONS -ready

# COMMAND ----------

# DBTITLE 1,dim_positions
# DIM: POSITIONS ----------------------------------------------------------------------
try:

    silver_table = "hr_database.silver.silver_positions"

    # department_id resolved from the department name: the source only links positions to departments
    # by name. Names that do not exist in departments (e.g. 'Producto', 'Ejecutivo') are assigned to
    # department '0' ('Sin Departamento') and flagged with department_resolved = false.
    df_dep_names = spark.table("hr_database.silver.silver_departments").select(
        col("department_id").alias("dep_department_id"),
        lower(trim(col("department_name"))).alias("dep_name_key")
        )

    df_dim_positions = spark.table(silver_table).join(
        df_dep_names, lower(trim(col("department_name"))) == col("dep_name_key"), "left"
        ).select(
        col("position_id"),
        col("position_title"),
        col("level"),
        coalesce(col("dep_department_id"), lit("0")).alias("department_id"),
        col("department_name"),
        col("dep_department_id").isNotNull().alias("department_resolved"),
        col("min_salary"),
        col("max_salary"),
        # v2: flags positions without a formal salary band (min or max missing)
        (col("min_salary").isNull() | col("max_salary").isNull()).alias("band_incomplete"),
        col("is_remote_eligible"),
        col("requires_travel")
        )

    # persist to Delta
    df_dim_positions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_dim_positions")  # v2: overwriteSchema (schema changed in v2)

    if not df_dim_positions.isEmpty():
        print("CREATED: hr_database.gold.gold_dim_positions")
        log_pipeline_event("gold", "positions", "SUCCESS")
        data_processing_report.append((silver_table, "gold.gold_dim_positions", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_dim_positions")
    log_pipeline_event("gold", "positions", "ERROR", str(e))
    data_processing_report.append((silver_table, "gold.gold_dim_positions", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_dim_positions
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### ENRICHED DIM: EMPLOYEES -ready

# COMMAND ----------

# DBTITLE 1,dim_employees
# ENRICHED DIM: EMPLOYEES (360 view) --------------------------------------------------

try:
    
    silver_employees = "hr_database.silver.silver_employees"
    silver_positions = "hr_database.silver.silver_positions"
    silver_departments = "hr_database.silver.silver_departments"
    silver_contracts = "hr_database.silver.silver_contracts"
    
    df_emp = spark.table(silver_employees).alias("emp")
    df_pos = spark.table(silver_positions).alias("pos")
    df_dep = spark.table(silver_departments).alias("dep")
    
    # only the most recent contract for each employee
    # important: emp.hire_date must be updated. this is crucial for calculating tenure with the right values
    # vars: current hire date and most recent contract start date

    window_contract = Window.partitionBy("employee_id").orderBy(
        col("start_date").desc(), 
        col("contract_id").desc()
    )

    df_ctr = spark.table(silver_contracts).withColumn(
        "rn",
        row_number().over(window_contract)
        ).filter(
            col("rn") == 1
            ).drop("rn").alias("ctr")

    # v3: termination date detected from the employee history (SCD2). When the contract has no valid
    # end date, the termination date is the batch in which the status change to 'terminated' was
    # first OBSERVED (a previous version had another status). Its precision is the load frequency
    # (monthly in the simulated deliveries). Employees already terminated in the first batch have no
    # observed change, so no date is invented for them.
    silver_employees_history = "hr_database.silver.silver_employees_history"
    window_versions = Window.partitionBy("employee_id").orderBy("_valid_from")
    df_detected_termination = (
        spark.table(silver_employees_history)
        .withColumn("previous_status", lag("status").over(window_versions))
        .filter((col("status") == "terminated") & col("previous_status").isNotNull() & (col("previous_status") != "terminated"))
        .groupBy("employee_id")
        .agg(spark_max("_valid_from").alias("detected_termination_date"))
        .alias("det")
    )

    df_dim_employees = df_emp.join(
        df_pos, col("emp.position_id") == col("pos.position_id"), "left"
        ).join(
            df_dep, col("emp.department_id") == col("dep.department_id"), "left"
            ).join(
                df_ctr, col("emp.employee_id") == col("ctr.employee_id"), "left"
                ).join(
                df_detected_termination, col("emp.employee_id") == col("det.employee_id"), "left"
                ).withColumn(
                    # v3: termination date = valid contract end date; otherwise the detected status change
                    "termination_date_source",
                    when(col("emp.status") != "terminated", lit(None))
                    .when(col("ctr.end_date").isNotNull() & (col("ctr.end_date") >= col("emp.hire_date")), lit("contract"))
                    .when(col("det.detected_termination_date") >= col("emp.hire_date"), lit("status_change_detected"))
                ).withColumn(
                    "resolved_termination_date",
                    when(col("termination_date_source") == "contract", col("ctr.end_date"))
                    .when(col("termination_date_source") == "status_change_detected", col("det.detected_termination_date"))
                ).withColumn(
                    # v2: terminated employees are no longer dropped when the source lacks a valid
                    # termination date. They are kept and flagged, so headcount and turnover can exclude
                    # them explicitly and the data gap stays visible (see gold_agg_turnover_data_quality).
                    "termination_date_missing",
                    (col("emp.status") == "terminated") & col("resolved_termination_date").isNull()
                ).filter(
                    col("emp.hire_date").isNotNull()
                    ).select(
                        col("emp.employee_id").alias("employee_id"),
                        col("emp.employee_code").alias("employee_code"),
                        concat_ws(" ", col("emp.first_name"), col("emp.last_name")).alias("full_name"),
                        col("emp.first_name").alias("first_name"),
                        col("emp.last_name").alias("last_name"),
                        # v2: personal contact data (email, phone) and birth_date are no longer exposed in Gold
                        # (the analytical layer only needs age). They remain in Silver. Access to Silver should be
                        # restricted with Unity Catalog grants / column masks.
                        floor(months_between(current_date(), col("emp.birth_date")) / 12).alias("age"),
                        col("emp.gender").alias("gender"),
                        col("emp.department_id").alias("department_id"),
                        col("dep.department_name").alias("department_name"),
                        col("emp.position_id").alias("position_id"),
                        col("pos.position_title").alias("position_title"),
                        col("pos.level").alias("position_level"),
                        col("emp.manager_id").alias("manager_id"),
                        col("emp.hire_date").alias("hire_date"),
                        col("resolved_termination_date").alias("termination_date"),
                        col("termination_date_source"),  # v3: contract | status_change_detected | null
                        col("termination_date_missing"),
                        # calculate an employee's years of service based on the number of months that have elapsed
                        # v2: tenure is null (unknown) for terminated employees without a valid termination date.
                        # Before, greatest(null, 0.0) silently returned 0.0.
                        when(col("termination_date_missing"), lit(None)).otherwise(
                            greatest(round(months_between(
                                when(col("emp.status") == "terminated", col("resolved_termination_date")).otherwise(current_date()), col("emp.hire_date")
                                ) / 12, 2), lit(0.0))
                                ).alias("tenure_years"),
                        col("emp.employment_type").alias("employment_type"),
                        col("emp.work_location").alias("work_location"),
                        col("emp.status").alias("status"),
                        lit(False).alias("is_unknown_member")
                        )

    # manager_id must exist in the dimension itself (self-reference); unresolved -> Unknown member
    df_dim_employees = resolve_to_unknown_member(df_dim_employees, "manager_id", df_dim_employees, "employee_id")

    # "Unknown" member (employee_id = '-1'): target of fact rows whose employee is not in this dimension
    # (e.g. employees quarantined in Silver). It has no hire date and status 'unknown', so it never
    # counts in headcount, turnover or compensation KPIs.
    df_unknown_employee = df_dim_employees.limit(0).unionByName(
        spark.createDataFrame(
            [("-1", "Unknown employee", "0", "Sin Departamento", False, "unknown", "Unknown", "unknown", True)],
            "employee_id string, full_name string, department_id string, department_name string, "
            "termination_date_missing boolean, employment_type string, work_location string, status string, "
            "is_unknown_member boolean"
        ),
        allowMissingColumns=True
    )
    df_dim_employees = df_dim_employees.unionByName(df_unknown_employee)


    # Persist to Delta
    df_dim_employees.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_dim_employees")  # v2: overwriteSchema (schema changed in v2)

    if not df_dim_employees.isEmpty():
        print("CREATED: hr_database.gold.gold_dim_employees")
        log_pipeline_event("gold", "employees", "SUCCESS")
        data_processing_report.append(([silver_employees, silver_positions, silver_departments, silver_contracts, silver_employees_history], "gold.gold_dim_employees", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_dim_employees")
    log_pipeline_event("gold", "employees", "ERROR", str(e))
    data_processing_report.append(([silver_employees, silver_positions, silver_departments, silver_contracts], "gold.gold_dim_employees", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_dim_employees
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM: DATE -ready

# COMMAND ----------

# DBTITLE 1,dim_date
# DIM: DATE (dynamically generated via ANSI SQL) --------------------------------------

try:

    df_dim_date = spark.sql("""
        SELECT 
            date AS date_key,
            year(date) AS year,
            quarter(date) AS quarter,
            month(date) AS month,
            day(date) AS day,
            date_format(date, 'MMMM') AS month_name,
            date_format(date, 'EEEE') AS day_name
        FROM (
            SELECT explode(sequence(DATE '2015-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS date
        )
    """)

    # persist to Delta
    df_dim_date.write.format("delta").mode("overwrite").saveAsTable("hr_database.gold.gold_dim_date")

    if not df_dim_date.isEmpty():
        print("CREATED: hr_database.gold.gold_dim_date")
        log_pipeline_event("gold", "date", "SUCCESS")
        data_processing_report.append(("hr_database.gold.gold_dim_date", "gold.gold_dim_date", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_dim_date")
    log_pipeline_event("gold", "date", "ERROR", str(e))
    data_processing_report.append(("hr_database.gold.gold_dim_date", "gold.gold_dim_date", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_dim_date
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACTS: SALARIES -ready

# COMMAND ----------

# DBTITLE 1,fact_current_salaries
# v2: renamed gold_dim_salaries -> gold_fact_current_salaries (it is a fact table: current salary per employee)
# SALARIES ----------------------------------------------------------------------

try:

    silver_employees = "hr_database.silver.silver_employees"
    silver_salaries = "hr_database.silver.silver_salaries"

    df_emp = spark.table(silver_employees).alias("emp")
    # v2: positions are used to compare each salary with its salary band (salary_vs_band)
    df_pos_band = spark.table("hr_database.silver.silver_positions").select(
        col("position_id").alias("band_position_id"), "min_salary", "max_salary"
        ).alias("pos")
    # v2: only the current salary (scheduled future salaries have is_current = false)
    df_slr = spark.table(silver_salaries).filter(col("is_current")).alias("slr")

    # v2: employees whose latest salary is dated before their hire_date are no longer dropped
    # (it is their last known salary). They are flagged with salary_before_hire_date instead.
    df_slr_validated = df_slr.join(
        df_emp, col("slr.employee_id") == col("emp.employee_id"), "inner").join(
            df_pos_band, col("emp.position_id") == col("pos.band_position_id"), "left")

    df_salaries = df_slr_validated.select(
        col("slr.salary_id").alias("salary_id"),
        col("slr.employee_id").alias("employee_id"),
        col("slr.base_salary").alias("base_salary"),
        col("slr.currency").alias("currency"),
        col("slr.effective_date").alias("effective_date"),
        col("slr.pay_frequency").alias("pay_frequency"),
        (col("slr.effective_date") < col("emp.hire_date")).alias("salary_before_hire_date"),
        # v2: position of the salary relative to its band. Informative only (no quarantine): bands are
        # not fully reliable in the source (e.g. several 'CEO' employees earn far below the CEO band).
        # null when the band is incomplete or the position is unknown.
        expr("""
            case
                when pos.min_salary is null or pos.max_salary is null then null
                when slr.base_salary < pos.min_salary then 'below_band'
                when slr.base_salary > pos.max_salary then 'above_band'
                else 'within_band'
            end
        """).alias("salary_vs_band")
        )

    # every employee_id must resolve to gold_dim_employees (Unknown member otherwise)
    df_salaries = resolve_to_unknown_member(
        df_salaries, "employee_id", spark.table("hr_database.gold.gold_dim_employees"), "employee_id"
        )

    # Persist to Delta
    df_salaries.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_fact_current_salaries")

    if not df_salaries.isEmpty():
        print("CREATED: hr_database.gold.gold_fact_current_salaries")
        log_pipeline_event("gold", "salaries", "SUCCESS")
        data_processing_report.append(([silver_employees, silver_salaries], "gold.gold_fact_current_salaries", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_fact_current_salaries")
    log_pipeline_event("gold", "salaries", "ERROR", str(e))
    data_processing_report.append(([silver_employees, silver_salaries], "gold.gold_fact_current_salaries", "failed"))

    # v2: the filter "effective_date >= hire_date" was replaced by the salary_before_hire_date flag.


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_fact_current_salaries
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACTS: SALARY HISTORY -ready

# COMMAND ----------

# DBTITLE 1,fact_salary_history
# FACTS: SALARY HISTORY ----------------------------------------------------------------

try:

    silver_table = "hr_database.silver.silver_salary_history"

    # v3: merged table: events no longer in the source are flagged (_deleted_in_source), not deleted
    df_fact_salary_history = spark.table(silver_table).filter(~col("_deleted_in_source")).select(
        col("history_id"),
        col("employee_id"),
        col("old_salary"),
        col("new_salary"),
        (col("new_salary") - col("old_salary")).alias("salary_increase_amount"),
        round(((col("new_salary") - col("old_salary")) / expr("nullif(old_salary, 0)")) * 100, 2).alias("salary_increase_pct"),
        col("change_reason"),
        col("effective_date"),
        col("approved_by")
        )

    # employee_id and approved_by must resolve to gold_dim_employees: records of employees that are not in
    # the dimension (quarantined in Silver) are kept and assigned to the Unknown member ('-1')
    df_dim_emp = spark.table("hr_database.gold.gold_dim_employees")
    df_fact_salary_history = resolve_to_unknown_member(df_fact_salary_history, "employee_id", df_dim_emp, "employee_id")
    df_fact_salary_history = resolve_to_unknown_member(df_fact_salary_history, "approved_by", df_dim_emp, "employee_id")

    # persist to Delta
    df_fact_salary_history.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_fact_salary_history")

    if not df_fact_salary_history.isEmpty():
        print("CREATED: hr_database.gold.gold_fact_salary_history")
        log_pipeline_event("gold", "fact_salary_history", "SUCCESS")
        data_processing_report.append((silver_table, "gold.gold_fact_salary_history", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_fact_salary_history")
    log_pipeline_event("gold", "fact_salary_history", "ERROR", str(e))
    data_processing_report.append((silver_table, "gold.gold_fact_salary_history", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_fact_salary_history
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACTS: PERFORMANCE REVIEWS -ready

# COMMAND ----------

# DBTITLE 1,fact_performance_reviews
# FACTS: PERFORMANCE REVIEWS ----------------------------------------------------------

try:

    silver_table = "hr_database.silver.silver_performance_reviews"

    # v3: merged table: reviews no longer in the source are flagged (_deleted_in_source), not deleted
    df_fact_performance = spark.table(silver_table).filter(~col("_deleted_in_source")).select(
        col("review_id"),
        col("employee_id"),
        col("reviewer_id"),
        col("review_period"),
        col("overall_rating"),
        col("technical_skills"),
        col("communication"),
        col("teamwork"),
        col("leadership"),
        col("review_date"),
        col("status")
        )

    # employee_id and reviewer_id must resolve to gold_dim_employees (Unknown member otherwise)
    df_dim_emp = spark.table("hr_database.gold.gold_dim_employees")
    df_fact_performance = resolve_to_unknown_member(df_fact_performance, "employee_id", df_dim_emp, "employee_id")
    df_fact_performance = resolve_to_unknown_member(df_fact_performance, "reviewer_id", df_dim_emp, "employee_id")

    # persist to Delta
    df_fact_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_fact_performance_reviews")

    if not df_fact_performance.isEmpty():
        print("CREATED: hr_database.gold.gold_fact_performance_reviews")
        log_pipeline_event("gold", "fact_performance_reviews", "SUCCESS")
        data_processing_report.append((silver_table, "gold.gold_fact_performance_reviews", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_fact_performance_reviews")
    log_pipeline_event("gold", "fact_performance_reviews", "ERROR", str(e))
    data_processing_report.append((silver_table, "gold.gold_fact_performance_reviews", "failed"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_fact_performance_reviews
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACTS: RECRUITMENT FUNNEL -ready

# COMMAND ----------

# DBTITLE 1,fact_recruitment_funnel
# FACTS: RECRUITMENT FUNNEL ------------------------------------------------------------

try:
    silver_candidates = "hr_database.silver.silver_candidates"
    silver_job_postings = "hr_database.silver.silver_job_postings"

    df_cand = spark.table(silver_candidates).alias("c")
    df_jp = spark.table(silver_job_postings).alias("j")


    df_fact_recruitment = df_cand.join(
        df_jp, col("c.posting_id") == col("j.posting_id"), "left"
    ).filter(
        (col("c.applied_date").isNotNull() & col("j.posted_date").isNotNull())
        & (col("c.applied_date") >= col("j.posted_date"))
        & (col("j.closing_date").isNull() | (col("j.closing_date") >= col("c.applied_date")))
        ).select(
            col("c.candidate_id").alias("candidate_id"),
            col("c.posting_id").alias("posting_id"),
            col("j.position_id").alias("position_id"),
            col("j.title").alias("job_title"),
            col("c.source").alias("candidate_source"),
            col("c.applied_date").alias("applied_date"),
            col("j.posted_date").alias("posted_date"),
            col("j.closing_date").alias("closing_date"),
            # v2: renamed (was days_since_applied): days from the posting date to the application date
            datediff(col("c.applied_date"), col("j.posted_date")).alias("days_from_posting_to_application"),
            col("c.status").alias("candidate_status"),
            col("j.status").alias("job_posting_status")
            )

    # persist to Delta
    df_fact_recruitment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_fact_recruitment_funnel")

    if not df_fact_recruitment.isEmpty():
        print("CREATED: hr_database.gold.gold_fact_recruitment_funnel")
        log_pipeline_event("gold", "fact_recruitment_funnel", "SUCCESS")
        data_processing_report.append(([silver_candidates, silver_job_postings], "gold.gold_fact_recruitment_funnel", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_fact_recruitment_funnel")
    log_pipeline_event("gold", "fact_recruitment_funnel", "ERROR", str(e))
    data_processing_report.append(([silver_candidates, silver_job_postings], "gold.gold_fact_recruitment_funnel", "failed"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_fact_recruitment_funnel
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,fact_candidate_stage_history
# v3: FACTS: CANDIDATE STAGE HISTORY ------------------------------------------------------------
# Grain: one row per candidate and stage reached, built from the SCD2 history of candidates.
#   - every candidate starts with 'applied', entered on applied_date (real date from the source)
#   - every later status observed is a stage. Its entry date is the batch in which the change was
#     detected ('status_change_detected'); for the status a candidate already had when first seen,
#     the entry date is unknown ('before_first_observation', null)
#   - stage_exited_date = entry date of the next stage; days_in_stage only when both dates are known
# Precision of detected dates = load frequency (monthly in the simulated deliveries). For that reason
# avg_time_to_hire_days (recruitment KPIs) keeps the posting closing_date proxy. Candidates removed
# from the source (_deleted_in_source) are excluded.

try:

    silver_candidates_history = "hr_database.silver.silver_candidates_history"
    silver_candidates = "hr_database.silver.silver_candidates"
    silver_job_postings = "hr_database.silver.silver_job_postings"

    df_current_cand = spark.table(silver_candidates).select("candidate_id", "posting_id", "source", "applied_date")
    df_postings_pos = spark.table(silver_job_postings).select(col("posting_id").alias("jp_posting_id"), "position_id")

    window_cand = Window.partitionBy("candidate_id").orderBy("_valid_from")
    df_observed = (
        spark.table(silver_candidates_history)
        .join(df_current_cand.select("candidate_id"), "candidate_id", "left_semi")  # still in the source
        .withColumn("previous_status", lag("status").over(window_cand))
        .withColumn("version_number", row_number().over(window_cand))
        # one row per stage change (versions where only other columns changed are not new stages)
        .filter(col("previous_status").isNull() | (col("status") != col("previous_status")))
        .filter(col("status") != "applied")  # 'applied' comes from applied_date below
        .select(
            "candidate_id",
            col("status").alias("stage"),
            when(col("version_number") == 1, lit(None).cast("date")).otherwise(col("_valid_from")).alias("stage_entered_date"),
            when(col("version_number") == 1, lit("before_first_observation")).otherwise(lit("status_change_detected")).alias("stage_date_source"),
            col("_valid_from").alias("first_observed_date")
        )
    )
    df_applied = df_current_cand.select(
        "candidate_id",
        lit("applied").alias("stage"),
        col("applied_date").alias("stage_entered_date"),
        lit("applied_date").alias("stage_date_source"),
        lit(None).cast("date").alias("first_observed_date")
    )

    window_stage = Window.partitionBy("candidate_id").orderBy(
        when(col("stage") == "applied", lit(0)).otherwise(lit(1)), col("first_observed_date"))
    df_fact_stage_history = (
        df_applied.unionByName(df_observed)
        .withColumn("stage_sequence", row_number().over(window_stage))
        .withColumn("is_current_stage", lead("stage").over(window_stage).isNull())
        .withColumn("stage_exited_date", lead("stage_entered_date").over(window_stage))
        .withColumn("days_in_stage", datediff(col("stage_exited_date"), col("stage_entered_date")))
        .join(df_current_cand.select("candidate_id", "posting_id", col("source").alias("candidate_source")), "candidate_id", "inner")
        .join(df_postings_pos, col("posting_id") == col("jp_posting_id"), "left")
        .select(
            "candidate_id", "posting_id", "position_id", "candidate_source", "stage_sequence", "stage",
            "stage_entered_date", "stage_date_source", "stage_exited_date", "days_in_stage", "is_current_stage",
            "first_observed_date"
        )
    )

    # persist to Delta
    df_fact_stage_history.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_fact_candidate_stage_history")

    if not df_fact_stage_history.isEmpty():
        print("CREATED: hr_database.gold.gold_fact_candidate_stage_history")
        log_pipeline_event("gold", "fact_candidate_stage_history", "SUCCESS")
        data_processing_report.append(([silver_candidates_history, silver_candidates, silver_job_postings], "gold.gold_fact_candidate_stage_history", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_fact_candidate_stage_history")
    log_pipeline_event("gold", "fact_candidate_stage_history", "ERROR", str(e))
    data_processing_report.append(([silver_candidates_history, silver_candidates, silver_job_postings], "gold.gold_fact_candidate_stage_history", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_fact_candidate_stage_history
# MAGIC ORDER BY candidate_id, stage_sequence
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,agg_referential_integrity_data_quality
# DATA QUALITY: REFERENTIAL INTEGRITY OF THE STAR SCHEMA ------------------------------------
# One row per relationship (foreign key -> dimension key). For employee relationships, unresolved
# rows are the ones assigned to the Unknown member ('-1'); for date relationships, dates outside
# gold_dim_date; for positions -> departments, department names that do not exist in departments.

try:

    def employee_fk_check(table, column):
        df = spark.table(f"hr_database.gold.{table}").filter(col(f"source_{column}").isNotNull())
        return df.agg(
            expr("count(*)").alias("rows_with_value"),
            expr(f"sum(case when {column} = '-1' then 1 else 0 end)").alias("unresolved_rows")
        ).select(
            lit(table).alias("table_name"), lit(column).alias("column_name"),
            lit("gold_dim_employees").alias("dimension"), lit("employee_id").alias("dimension_key"),
            "rows_with_value", "unresolved_rows"
        )

    def date_fk_check(table, column):
        dates = spark.table("hr_database.gold.gold_dim_date").select(col("date_key"))
        df = spark.table(f"hr_database.gold.{table}").filter(col(column).isNotNull())
        return df.join(dates, df[column] == dates["date_key"], "left").agg(
            expr("count(*)").alias("rows_with_value"),
            expr("sum(case when date_key is null then 1 else 0 end)").alias("unresolved_rows")
        ).select(
            lit(table).alias("table_name"), lit(column).alias("column_name"),
            lit("gold_dim_date").alias("dimension"), lit("date_key").alias("dimension_key"),
            "rows_with_value", "unresolved_rows"
        )

    checks = [
        employee_fk_check("gold_dim_employees", "manager_id"),
        employee_fk_check("gold_fact_current_salaries", "employee_id"),
        employee_fk_check("gold_fact_salary_history", "employee_id"),
        employee_fk_check("gold_fact_salary_history", "approved_by"),
        employee_fk_check("gold_fact_performance_reviews", "employee_id"),
        employee_fk_check("gold_fact_performance_reviews", "reviewer_id"),
        date_fk_check("gold_dim_employees", "hire_date"),
        date_fk_check("gold_dim_employees", "termination_date"),
        date_fk_check("gold_fact_current_salaries", "effective_date"),
        date_fk_check("gold_fact_salary_history", "effective_date"),
        date_fk_check("gold_fact_performance_reviews", "review_date"),
        date_fk_check("gold_fact_recruitment_funnel", "applied_date"),
        date_fk_check("gold_fact_candidate_stage_history", "stage_entered_date"),  # v3
        spark.table("hr_database.gold.gold_dim_positions").agg(
            expr("count(*)").alias("rows_with_value"),
            expr("sum(case when department_resolved then 0 else 1 end)").alias("unresolved_rows")
        ).select(
            lit("gold_dim_positions").alias("table_name"), lit("department_id").alias("column_name"),
            lit("gold_dim_departments").alias("dimension"), lit("department_id").alias("dimension_key"),
            "rows_with_value", "unresolved_rows"
        ),
    ]

    df_ri_dq = checks[0]
    for check in checks[1:]:
        df_ri_dq = df_ri_dq.unionByName(check)

    df_ri_dq = df_ri_dq.withColumn(
        "pct_unresolved",
        expr("case when rows_with_value > 0 then round(unresolved_rows / rows_with_value * 100, 2) else 0.0 end")
    )

    # persist to Delta
    df_ri_dq.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_referential_integrity_data_quality")

    if not df_ri_dq.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_referential_integrity_data_quality")
        log_pipeline_event("gold", "agg_referential_integrity_data_quality", "SUCCESS")
        data_processing_report.append(("hr_database.gold (star schema)", "gold.gold_agg_referential_integrity_data_quality", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_referential_integrity_data_quality")
    log_pipeline_event("gold", "agg_referential_integrity_data_quality", "ERROR", str(e))
    data_processing_report.append(("hr_database.gold (star schema)", "gold.gold_agg_referential_integrity_data_quality", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_referential_integrity_data_quality
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Cell 33
print("\n================ GOLD LAYER: BUILDING AGGREGATED KPI TABLES ================\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### -ready AGGREGATED TABLE: MONTHLY ROTATION AND RETENTION PER DEPARTMENT

# COMMAND ----------

# DBTITLE 1,agg_monthly_headcount_turnover

# AGGREGATED TABLE: MONTHLY ROTATION AND RETENTION PER DEPARTMENT

try:

    gold_date = "hr_database.gold.gold_dim_date"
    gold_employees = "hr_database.gold.gold_dim_employees" 
    silver_employees_history = "hr_database.silver.silver_employees_history"

    # v3: POINT-IN-TIME DEPARTMENT. Each employee counts in the department they had at that date
    # (headcount: at the end of the month; terminations: on the termination date), taken from the
    # SCD2 history. Before, the CURRENT department was used for every month, so a transfer moved the
    # employee's whole past to the new department. Dates before the first batch use the first known
    # version (there is no earlier history). Monthly totals do not change, only their split by department.
    df_dept_names = spark.table("hr_database.gold.gold_dim_departments").select(
        col("department_id").alias("dn_department_id"), col("department_name").alias("dn_department_name"))
    window_dept_versions = Window.partitionBy("employee_id").orderBy("_valid_from")
    df_dept_periods = (
        spark.table(silver_employees_history)
        .withColumn("version_number", row_number().over(window_dept_versions))
        .select(
            col("employee_id").alias("p_employee_id"),
            col("department_id").alias("p_department_id"),
            when(col("version_number") == 1, lit("1900-01-01").cast("date")).otherwise(col("_valid_from")).alias("p_start"),
            coalesce(col("_valid_to"), lit("9999-12-31").cast("date")).alias("p_end")
        )
        .join(df_dept_names, col("p_department_id") == col("dn_department_id"), "left")
    )

    def with_department_at(df, date_col):
        """Replaces department_id / department_name with the department the employee had at date_col."""
        return (
            df.drop("department_id", "department_name")
            .join(df_dept_periods, (col("employee_id") == col("p_employee_id"))
                  & (col("p_start") <= col(date_col)) & (col(date_col) < col("p_end")), "inner")
            .withColumnRenamed("p_department_id", "department_id")
            .withColumnRenamed("dn_department_name", "department_name")
            .drop("p_employee_id", "p_start", "p_end", "dn_department_id")
        )

    # monthly turnovers (with native SQL expressions)
    df_terminations = (
        with_department_at(spark.table(gold_employees).filter("status = 'terminated' AND termination_date IS NOT NULL"), "termination_date")
        .withColumn("year", expr("year(termination_date)"))
        .withColumn("month", expr("month(termination_date)"))
        .groupBy("year", "month", "department_id", "department_name")
        .agg(
            expr("count(employee_id)").alias("monthly_terminations"),
            expr("sum(case when tenure_years < 1.0 then 1 else 0 end)").alias("early_terminations"),
            expr("round(avg(tenure_years), 2)").alias("avg_tenure_years")
        )
    )

    # monthly historical headcount
    # v2: only months up to the current month (dim_date reaches 2030)
    df_months = (
        spark.table(gold_date)
        .groupBy("year", "month")
        .agg(expr("max(date_key)").alias("end_of_month_date"))
        .filter("end_of_month_date <= last_day(current_date())")
    )

    # v2: terminated employees without a valid termination date are excluded from the historical
    # headcount: without that date they would be counted as active forever.
    df_active_hc_historical = (
        with_department_at(
            spark.table(gold_employees)
            .filter("NOT termination_date_missing")
            .join(df_months, how="inner")
            .filter("hire_date <= end_of_month_date AND (termination_date IS NULL OR termination_date > end_of_month_date)"),
            "end_of_month_date"
        )
        .groupBy("year", "month", "department_id", "department_name")
        .agg(expr("count(employee_id)").alias("active_headcount"))
    )


    # metrics and percentages combined
    # v2: the base is now the monthly headcount per department (FULL OUTER join), so months
    # without terminations are kept with 0 terminations. Before, the table started from
    # terminations (LEFT join) and only months with at least one termination existed.
    # we explicitly state the join condition so that Python does not have to iterate over lists
    df_t = df_terminations.alias("t")
    df_h = df_active_hc_historical.alias("h")

    join_condition = (
        (col("t.year") == col("h.year")) &
        (col("t.month") == col("h.month")) &
        (col("t.department_id") == col("h.department_id"))
    )

    df_agg_turnover = (
        df_t.join(df_h, on=join_condition, how="full_outer")
        .select(
            coalesce(col("t.year"), col("h.year")).alias("year"),
            coalesce(col("t.month"), col("h.month")).alias("month"),
            coalesce(col("t.department_id"), col("h.department_id")).alias("department_id"),
            coalesce(col("t.department_name"), col("h.department_name")).alias("department_name"),
            expr("coalesce(h.active_headcount, 0)").alias("active_headcount"),
            expr("coalesce(t.monthly_terminations, 0)").alias("monthly_terminations"),
            expr("coalesce(t.early_terminations, 0)").alias("early_terminations"),
            # null when there are no terminations in the month (an average of nothing is not 0)
            col("t.avg_tenure_years").alias("avg_tenure_years")
        )
        .withColumn(
            "gross_turnover_pct",
            expr("case when active_headcount > 0 then round((monthly_terminations / active_headcount) * 100, 2) else 0.0 end")
        )
        .withColumn(
            # v2: renamed (was early_turnover_pct): share of the month's terminations with < 1 year of tenure
            "early_terminations_share_pct",
            expr("case when monthly_terminations > 0 then round((early_terminations / monthly_terminations) * 100, 2) else 0.0 end")
        ).select(
            expr("year"),
            expr("month"),
            expr("department_id"),
            expr("department_name"),
            expr("active_headcount"), 
            expr("monthly_terminations"), 
            expr("early_terminations"),
            expr("avg_tenure_years"), 
            expr("gross_turnover_pct"), 
            expr("early_terminations_share_pct")
        )
    )

    # persist to Delta
    df_agg_turnover.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_monthly_headcount_turnover")  # v2: overwriteSchema (schema changed in v2)

    if not df_agg_turnover.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_monthly_headcount_turnover")
        log_pipeline_event("gold", "agg_monthly_headcount_turnover", "SUCCESS")
        data_processing_report.append(([gold_date, gold_employees, silver_employees_history], "gold.gold_agg_monthly_headcount_turnover", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_monthly_headcount_turnover")
    log_pipeline_event("gold", "agg_monthly_headcount_turnover", "ERROR", str(e))
    data_processing_report.append(([gold_date, gold_employees], "gold.gold_agg_monthly_headcount_turnover", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_monthly_headcount_turnover
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,agg_turnover_data_quality
# v2: AGGREGATED TABLE: TURNOVER DATA QUALITY (TERMINATIONS WITHOUT A VALID DATE)
# The source system lacks a termination date for most terminated employees. Those terminations
# cannot be placed in a month, so gold_agg_monthly_headcount_turnover UNDERESTIMATES turnover.
# This table quantifies the gap per department (plus a company-wide 'TOTAL' row) so it can be
# shown next to the turnover KPIs in Power BI.

try:

    gold_employees = "hr_database.gold.gold_dim_employees"

    df_terminated = spark.table(gold_employees).filter("status = 'terminated'")

    df_dq_dept = df_terminated.groupBy("department_id", "department_name").agg(
        expr("count(employee_id)").alias("total_terminations"),
        expr("sum(case when termination_date_missing then 0 else 1 end)").alias("terminations_with_date"),
        expr("sum(case when termination_date_missing then 1 else 0 end)").alias("terminations_without_date")
    )

    df_dq_total = df_terminated.agg(
        expr("count(employee_id)").alias("total_terminations"),
        expr("sum(case when termination_date_missing then 0 else 1 end)").alias("terminations_with_date"),
        expr("sum(case when termination_date_missing then 1 else 0 end)").alias("terminations_without_date")
    ).select(
        lit("TOTAL").alias("department_id"),
        lit("TOTAL").alias("department_name"),
        "total_terminations", "terminations_with_date", "terminations_without_date"
    )

    df_turnover_dq = df_dq_dept.unionByName(df_dq_total).withColumn(
        "pct_terminations_without_date",
        expr("case when total_terminations > 0 then round(terminations_without_date / total_terminations * 100, 2) else 0.0 end")
    )

    # persist to Delta
    df_turnover_dq.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_turnover_data_quality")

    if not df_turnover_dq.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_turnover_data_quality")
        log_pipeline_event("gold", "agg_turnover_data_quality", "SUCCESS")
        data_processing_report.append((gold_employees, "gold.gold_agg_turnover_data_quality", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_turnover_data_quality")
    log_pipeline_event("gold", "agg_turnover_data_quality", "ERROR", str(e))
    data_processing_report.append((gold_employees, "gold.gold_agg_turnover_data_quality", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_turnover_data_quality
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### AGGREGATED TABLE: RECRUITMENT FUNNEL AND PERFORMANCE

# COMMAND ----------

# DBTITLE 1,agg_recruitment_funnel_kpis
# AGGREGATED TABLE: RECRUITMENT FUNNEL AND PERFORMANCE ----------------------------------------------------------------------

try:

    gold_recruitment_funnel = "hr_database.gold.gold_fact_recruitment_funnel"

    df_funnel = spark.table(gold_recruitment_funnel)

    # v2: TIME TO FILL computed at POSTING level (one value per posting), then averaged per position.
    # Before, it was averaged over candidate rows: a posting with 8 applicants weighed 8 times more
    # than one with 1, and the value changed by source only because of that weighting.
    # Only filled postings count for time to fill: status 'filled' OR at least one hired candidate.
    df_posting_ttf = (
        df_funnel
        .groupBy("posting_id", "position_id")
        .agg(
            expr("max(posted_date)").alias("posted_date"),
            expr("max(closing_date)").alias("closing_date"),
            expr("max(case when candidate_status = 'hired' then 1 else 0 end)").alias("has_hire"),
            expr("max(case when job_posting_status = 'filled' then 1 else 0 end)").alias("is_filled_status")
        )
        .filter("(has_hire = 1 OR is_filled_status = 1) AND closing_date IS NOT NULL")
        .withColumn("time_to_fill_days", datediff(col("closing_date"), col("posted_date")))
    )

    df_position_ttf = (
        df_posting_ttf
        .groupBy("position_id")
        .agg(
            round(avg(col("time_to_fill_days")), 1).alias("avg_time_to_fill_days"),
            count("posting_id").alias("filled_postings")
        )
    )

    # v2: TIME TO HIRE follows the standard TA definition: only HIRED candidates.
    # The source has no hire/offer-acceptance date per candidate, so the posting closing_date is
    # used as the proxy for the hire date (application date -> posting closed with the hire).
    df_agg_recruitment = df_funnel.groupBy(
        "position_id", "job_title", "candidate_source"
    ).agg(
        count("candidate_id").alias("total_applicants"),
        count(when(col("candidate_status") == "hired", 1)).alias("total_hired"),
        round(avg(when(col("candidate_status") == "hired", datediff(col("closing_date"), col("applied_date")))), 1).alias("avg_time_to_hire_days")
    ).withColumn(
        "source_yield_pct",
        when(col("total_applicants") > 0, round((col("total_hired") / col("total_applicants")) * 100, 2)).otherwise(lit(0.0))
    ).join(
        # time to fill is a POSITION-level metric: same value in every source row of the position
        df_position_ttf, on="position_id", how="left"
    ).select(
        "position_id", "job_title", "candidate_source",
        "total_applicants", "total_hired",
        "avg_time_to_fill_days", "filled_postings",
        "avg_time_to_hire_days", "source_yield_pct"
    )

    # Persist to Delta
    df_agg_recruitment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_recruitment_funnel_kpis")

    if not df_agg_recruitment.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_recruitment_funnel_kpis")
        log_pipeline_event("gold", "agg_recruitment_funnel_kpis", "SUCCESS")
        data_processing_report.append((gold_recruitment_funnel, "gold.gold_agg_recruitment_funnel_kpis", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_recruitment_funnel_kpis")
    log_pipeline_event("gold", "agg_recruitment_funnel_kpis", "ERROR", str(e))
    data_processing_report.append((gold_recruitment_funnel, "gold.gold_agg_recruitment_funnel_kpis", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_recruitment_funnel_kpis
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,agg_recruitment_coverage_data_quality
# v2.1: AGGREGATED TABLE: RECRUITMENT FUNNEL COVERAGE DATA QUALITY
# gold_fact_recruitment_funnel only keeps applications with a valid posting and dates inside the
# posting window, so the recruitment KPIs describe a subset of the candidates. This table classifies
# every candidate (Silver clean + quarantine) by the reason it is excluded from the funnel, or
# 'included_in_funnel', plus a 'TOTAL' row. The conditions mirror the filter of the fact table.

try:

    silver_candidates = "hr_database.silver.silver_candidates"
    silver_quarantine_candidates = "hr_database.silver.silver_quarantine_candidates"
    silver_job_postings = "hr_database.silver.silver_job_postings"
    silver_quarantine_job_postings = "hr_database.silver.silver_quarantine_job_postings"

    df_cand_cov = spark.table(silver_candidates).alias("c")
    df_jp_cov = spark.table(silver_job_postings).alias("j")
    # v3: quarantine tables keep every batch: only the latest batch describes the current state
    df_jp_quarantined = (
        latest_batch(spark.table(silver_quarantine_job_postings))
        .filter(col("posting_id").isNotNull())
        .select(col("posting_id").alias("q_posting_id")).distinct()
    )

    df_clean_cov = (
        df_cand_cov
        .join(df_jp_cov, col("c.posting_id") == col("j.posting_id"), "left")
        .join(df_jp_quarantined, col("c.posting_id") == col("q_posting_id"), "left")
        .select(
            col("c.candidate_id").alias("candidate_id"),
            col("c.status").alias("candidate_status"),
            when(col("j.posting_id").isNull() & col("q_posting_id").isNotNull(), lit("posting_quarantined"))
            .when(col("j.posting_id").isNull(), lit("posting_not_found"))
            .when(col("c.applied_date").isNull() | col("j.posted_date").isNull(), lit("missing_dates"))
            .when(col("c.applied_date") < col("j.posted_date"), lit("applied_before_posting"))
            .when(col("j.closing_date").isNotNull() & (col("c.applied_date") > col("j.closing_date")), lit("applied_after_closing"))
            .otherwise(lit("included_in_funnel"))
            .alias("exclusion_reason")
        )
    )

    df_quarantine_cov = latest_batch(spark.table(silver_quarantine_candidates)).select(
        col("candidate_id"),
        col("status").alias("candidate_status"),
        lit("quarantined_in_silver").alias("exclusion_reason")
    )

    df_rec_cov_base = df_clean_cov.unionByName(df_quarantine_cov)

    rec_cov_aggs = [
        expr("count(*)").alias("candidates"),
        expr("sum(case when candidate_status = 'hired' then 1 else 0 end)").alias("hired_candidates")
    ]

    df_rec_cov_reason = df_rec_cov_base.groupBy("exclusion_reason").agg(*rec_cov_aggs)
    df_rec_cov_total = df_rec_cov_base.agg(*rec_cov_aggs).select(lit("TOTAL").alias("exclusion_reason"), "candidates", "hired_candidates")
    total_candidates = df_rec_cov_base.count()

    df_recruitment_coverage = df_rec_cov_reason.unionByName(df_rec_cov_total).withColumn(
        "pct_of_candidates",
        round(col("candidates") / lit(total_candidates) * 100, 2) if total_candidates > 0 else lit(0.0)
    )

    # persist to Delta
    df_recruitment_coverage.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_recruitment_coverage_data_quality")

    if not df_recruitment_coverage.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_recruitment_coverage_data_quality")
        log_pipeline_event("gold", "agg_recruitment_coverage_data_quality", "SUCCESS")
        data_processing_report.append(([silver_candidates, silver_quarantine_candidates, silver_job_postings, silver_quarantine_job_postings], "gold.gold_agg_recruitment_coverage_data_quality", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_recruitment_coverage_data_quality")
    log_pipeline_event("gold", "agg_recruitment_coverage_data_quality", "ERROR", str(e))
    data_processing_report.append(([silver_candidates, silver_quarantine_candidates, silver_job_postings, silver_quarantine_job_postings], "gold.gold_agg_recruitment_coverage_data_quality", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_recruitment_coverage_data_quality
# MAGIC ORDER BY candidates DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### AGGREGATE TABLE: EQUITY AND TOTAL PAYROLL (COMPA-RATIO)

# COMMAND ----------

# DBTITLE 1,agg_compensation_equity
try:

    gold_employees = "hr_database.gold.gold_dim_employees"
    gold_salaries = "hr_database.gold.gold_fact_current_salaries"
    gold_positions = "hr_database.gold.gold_dim_positions"

    df_emp = spark.table(gold_employees).alias("emp")
    df_sal = spark.table(gold_salaries).alias("sal")
    df_pos = spark.table(gold_positions).alias("pos")

    df_comp_base = (
        df_emp
        .filter("status = 'active' OR status = 'on_leave'")
        .join(df_sal, col("emp.employee_id") == col("sal.employee_id"), "inner")
        .join(df_pos, col("emp.position_id") == col("pos.position_id"), "inner")
        # v2: compa-ratio only for complete bands (null otherwise; it is never imputed)
        .withColumn("mid_salary", expr("case when pos.band_incomplete then null else (pos.min_salary + pos.max_salary) / 2 end"))
        .withColumn("compa_ratio", expr("round(sal.base_salary / mid_salary, 2)"))
    )

    df_agg_compensation = (
        df_comp_base.groupBy(
            col("emp.department_name"), 
            col("emp.position_title"),
            col("emp.gender")
        ).agg(
            expr("count(emp.employee_id)").alias("active_employees"),
            expr("round(sum(sal.base_salary), 2)").alias("total_payroll"),
            expr("round(avg(sal.base_salary), 2)").alias("avg_current_salary"),
            # v2: null when the position has no complete band. Before, coalesce(..., 0.0) showed 0.00,
            # which reads as "paid far below market" instead of "no band defined".
            expr("round(avg(compa_ratio), 2)").alias("avg_compa_ratio"),
            expr("bool_or(pos.band_incomplete)").alias("band_incomplete")
        )
    )

    # Persist to Delta
    df_agg_compensation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_compensation_equity")

    if not df_agg_compensation.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_compensation_equity")
        log_pipeline_event("gold", "agg_compensation_equity", "SUCCESS")
        data_processing_report.append(([gold_employees, gold_salaries, gold_positions], "gold.gold_agg_compensation_equity", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_compensation_equity")
    log_pipeline_event("gold", "agg_compensation_equity", "ERROR", str(e))
    data_processing_report.append(([gold_employees, gold_salaries, gold_positions], "gold.gold_agg_compensation_equity", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_compensation_equity
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,agg_salary_coverage_data_quality
# v2: AGGREGATED TABLE: SALARY COVERAGE DATA QUALITY
# Active / on-leave employees without a current salary are excluded from total_payroll and
# compa-ratio (inner join), so those KPIs are UNDERESTIMATED for the affected departments.
# This table quantifies the gap per department (plus a company-wide 'TOTAL' row).

try:

    gold_employees = "hr_database.gold.gold_dim_employees"
    gold_salaries = "hr_database.gold.gold_fact_current_salaries"

    df_active = spark.table(gold_employees).filter("status = 'active' OR status = 'on_leave'").alias("emp")
    df_sal_cov = spark.table(gold_salaries).select("employee_id", "salary_before_hire_date").alias("sal")

    df_cov_base = df_active.join(df_sal_cov, col("emp.employee_id") == col("sal.employee_id"), "left").select(
        col("emp.department_id").alias("department_id"),
        col("emp.department_name").alias("department_name"),
        col("emp.employee_id").alias("employee_id"),
        col("sal.employee_id").isNotNull().alias("has_salary"),
        coalesce(col("sal.salary_before_hire_date"), lit(False)).alias("salary_before_hire_date")
    )

    cov_aggs = [
        expr("count(employee_id)").alias("active_employees"),
        expr("sum(case when has_salary then 1 else 0 end)").alias("employees_with_salary"),
        expr("sum(case when has_salary then 0 else 1 end)").alias("employees_without_salary"),
        expr("sum(case when salary_before_hire_date then 1 else 0 end)").alias("salary_before_hire_date")
    ]

    df_cov_dept = df_cov_base.groupBy("department_id", "department_name").agg(*cov_aggs)
    df_cov_total = df_cov_base.agg(*cov_aggs).select(
        lit("TOTAL").alias("department_id"), lit("TOTAL").alias("department_name"),
        "active_employees", "employees_with_salary", "employees_without_salary", "salary_before_hire_date"
    )

    df_salary_coverage = df_cov_dept.unionByName(df_cov_total).withColumn(
        "pct_without_salary",
        expr("case when active_employees > 0 then round(employees_without_salary / active_employees * 100, 2) else 0.0 end")
    )

    # persist to Delta
    df_salary_coverage.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_database.gold.gold_agg_salary_coverage_data_quality")

    if not df_salary_coverage.isEmpty():
        print("CREATED: hr_database.gold.gold_agg_salary_coverage_data_quality")
        log_pipeline_event("gold", "agg_salary_coverage_data_quality", "SUCCESS")
        data_processing_report.append(([gold_employees, gold_salaries], "gold.gold_agg_salary_coverage_data_quality", "processed"))

except Exception as e:
    print("FAILED: hr_database.gold.gold_agg_salary_coverage_data_quality")
    log_pipeline_event("gold", "agg_salary_coverage_data_quality", "ERROR", str(e))
    data_processing_report.append(([gold_employees, gold_salaries], "gold.gold_agg_salary_coverage_data_quality", "failed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hr_database.gold.gold_agg_salary_coverage_data_quality
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA PROCESSING REPORT

# COMMAND ----------

# DBTITLE 1,data processing report
print("\n================ GOLD LAYER: CREATED ================")

print("\nDATA QUALITY - GOLD PIPELINE")
print("-" * 110)
print(f"{'PROCESSED SOURCE':<50} | {'RESULT':<45} | {'STATUS'}")
print("-" * 110)

for table_name, result, status in data_processing_report:
    # several source tables
    if isinstance(table_name, list):
        # first source table with the status (processed / failed)
        print(f"{table_name[0]:<50} | {result:<45} | {status}")
        # remaining source tables below, with an empty status
        for sub_table in table_name[1:]:
            print(f"{sub_table:<50} |")
            
    # single source table (usual case)
    else:
        print(f"{table_name:<50} | {result:<45} | {status}")
    
    print("-" * 110)

# COMMAND ----------

# DBTITLE 1,v2 - fail on errors
# v2: STOP THE PIPELINE IF ANY GOLD TABLE FAILED OR CAME OUT EMPTY
# Before, errors were logged and the notebook still finished successfully. Also, an empty table
# produced no log and no report entry at all (silent). Now both cases make the notebook fail.

expected_gold_tables = [
    "gold.gold_dim_departments", "gold.gold_dim_positions", "gold.gold_dim_employees",
    "gold.gold_dim_date", "gold.gold_fact_current_salaries", "gold.gold_fact_salary_history",
    "gold.gold_fact_performance_reviews", "gold.gold_fact_recruitment_funnel",
    "gold.gold_agg_monthly_headcount_turnover", "gold.gold_agg_turnover_data_quality",
    "gold.gold_agg_recruitment_funnel_kpis", "gold.gold_agg_compensation_equity",
    "gold.gold_agg_salary_coverage_data_quality", "gold.gold_agg_referential_integrity_data_quality",
    "gold.gold_agg_recruitment_coverage_data_quality", "gold.gold_fact_candidate_stage_history"
]

processed_tables = [result for _, result, status in data_processing_report if status == "processed"]
failed_tables = [result for _, result, status in data_processing_report if status == "failed"]
empty_tables = [t for t in expected_gold_tables if t not in processed_tables and t not in failed_tables]

for table in empty_tables:
    log_pipeline_event("gold", table.replace("gold.", ""), "ERROR", "Table was written but it is EMPTY.")

if failed_tables or empty_tables:
    raise RuntimeError(
        f"GOLD FAILED: failed tables: {failed_tables or 'none'} | empty tables: {empty_tables or 'none'}. "
        "See hr_database.monitoring.pipeline_logs for details."
    )


# COMMAND ----------

# DBTITLE 1,v3 - batch control
# v3: Gold is fully rebuilt from the current Silver state on every run (it is derived and small),
# so it reflects the latest batch processed by Silver. Recorded for traceability.
gold_batch_date = processed_batches("silver")[-1]
mark_batch_processed("gold", gold_batch_date)
print(f"GOLD: rebuilt from Silver state as of batch {gold_batch_date}, recorded in {BATCH_CONTROL_TABLE}")
