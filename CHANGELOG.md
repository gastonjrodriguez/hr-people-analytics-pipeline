# Changelog

## v3 — incremental loading (Bronze history, MERGE, SCD Type 2)

v2.1 rewrote every table on each run, so the pipeline only knew "today's picture" of the source. v3 loads one delivery at a time and keeps history: Bronze is append-only, Silver writes each table with a load strategy (`overwrite`, `merge` or `scd2`), and Gold uses the history to recover termination dates, count headcount in the department each employee had at each date, and track candidates through the funnel stages. Every result was validated on Databricks (Free Edition) phase by phase, against the values of the local simulation.

### Breaking changes

- **Volume layout:** one folder per delivery, `raw_hr_data/batch_date=YYYY-MM-DD/rrhh_<table>.csv`. Files in the Volume root are ignored.
- **Workspace folder:** the notebooks load `%run /hr_analytics_v3/00_utils` (was `/hr_analytics_v2/`).
- **Runs:** each run processes one pending delivery per layer; three runs load the three deliveries.
- **Bronze tables** get `_batch_date` and are partitioned by it. v2 Bronze tables are rewritten once with the new schema by `write_batch()`.
- **Silver:** `silver_salary_history` and `silver_performance_reviews` get `_row_hash`, `_deleted_in_source` and `_last_changed_batch`; quarantine tables get `_batch_date` and keep every delivery. Readers must exclude `_deleted_in_source` rows and use the latest batch of the quarantine tables.

### New tables and columns

- `monitoring.batch_control`: delivery processed by each layer (append-only audit trail).
- `silver_<table>_history` for employees, candidates, positions, contracts and job_postings (`_row_hash`, `_valid_from`, `_valid_to`, `_is_current`, `_deleted_in_source`).
- `gold_fact_candidate_stage_history`: one row per candidate and funnel stage reached (`stage_entered_date`, `stage_date_source`, `stage_exited_date`, `days_in_stage`, `is_current_stage`).
- `gold_dim_employees.termination_date_source`: `contract`, `status_change_detected` or null.

### Changes

- **Deliveries** (`scripts/generate_batches.py`, `data/batches/`): the first delivery is the original extract byte by byte; the second and third apply an explicit, deterministic list of changes (see `data/README.md`).
- **Bronze** (`01_bronze_ingestion`): appends the oldest pending delivery; re-loading a delivery replaces only its rows (`replaceWhere`). When nothing is pending, the latest delivery is re-processed and nothing changes.
- **Silver** (`02_silver_transformation`): reads only the delivery being processed; `SILVER_LOAD_CONFIG` sets the strategy per table. The cleaning rules are unchanged.
  - `scd2` (employees, candidates, positions, contracts, job_postings): a change in any business column, detected by hash, closes the current version and opens a new one; a key missing from the delivery closes its version with `_deleted_in_source = true`. `silver_<table>` keeps the current versions with the same columns as in v2.1, so Gold reads it as before.
  - `merge` (salary_history, performance_reviews): Delta `MERGE` by primary key; only new or changed rows are written; missing rows are flagged, never deleted.
  - `overwrite` (departments, trainings, salaries): `silver_salaries` is a derived snapshot (current salary per employee), so merging it would flag the previous current salary as deleted; its history is `salary_history`.
  - Keys are checked to be unique before every `MERGE`.
  - The BEFORE/AFTER DEDUPLICATION row counts (18 full table scans per run) are off by default (`SILVER_DEBUG_COUNTS`).
- **Gold** (`03_gold_metrics`), still fully rebuilt on each run:
  - **Termination date:** valid contract end date; otherwise the delivery in which the history detected the change to `terminated` (only when a previous version had another status, so no date is invented for employees already terminated in the first delivery).
  - **Point-in-time department** in `gold_agg_monthly_headcount_turnover`: headcount by the department at each month end, terminations by the department at the termination date (dates before the first delivery use the first known version). Monthly totals do not change.
  - `gold_fact_candidate_stage_history` (see above). `avg_time_to_hire_days` keeps the `closing_date` proxy: with monthly deliveries a detected hire date can be later than the actual hire (candidate 101: detected on 2026-10-01, posting closed on 2026-09-25).
  - Facts built from merged tables exclude `_deleted_in_source` rows; `gold_agg_recruitment_coverage_data_quality` uses the latest quarantine batch; the stage history entry date is checked against `gold_dim_date`.
- **Delta-specific writes** are isolated in `00_utils` (`write_batch()`, `apply_merge()`, `apply_scd2()`). Each has a plain-Spark equivalent with the same semantics (`merge_next_state()`, `scd2_next_state()`), used by the local test runner and as the reference for the Delta tests.
- **Tests in two modes:** default (Parquet tables, Delta writes replaced by their plain-Spark equivalents) and Delta (`HR_TESTS_DELTA=1`: Delta tables and the real `replaceWhere` / `MERGE`, plus tests checking that each Delta `MERGE` produces exactly the same table as its plain-Spark equivalent). GitHub Actions runs both (`pytest` and `pytest-delta` jobs); `delta-spark` added to `requirements-dev.txt`.
- **Tests:** 115 (was 86; 3 of them run only in Delta mode). Simulated deliveries (reproducible, first delivery = original extract), batch selection, merge and SCD2 semantics (including idempotency), and the pipeline over the three deliveries plus a re-run: one Bronze copy per delivery, versions only for real changes, candidate stages, termination date fallback, point-in-time headcount. The tests of v2.1 still run on the first delivery.

### Results on the simulated deliveries (after the third one)

| Check | Result |
|---|---|
| Versions in `silver_employees_history` | 354 (346 current, 8 closed: exactly the 8 employees changed by the deliveries) |
| Versions in `silver_candidates_history` | 102 (96 current, 6 closed, 1 removed from the source) |
| Terminations with date | 11 of 54 (10 from the contract, 1 detected: employee 142, 2026-09-01) |
| Data headcount, August → September 2026 | 34 → 35 (employee 28 in, employee 120 out, new hire 501) |
| Candidate stages | 176 rows for 96 candidates (5 stage changes detected) |

### Known limitations

- History starts with the first delivery; the 43 historical terminations without date stay without date.
- Detected dates have the precision of the load frequency (monthly here).
- The source CSVs require `multiLine` reading, which is not splittable (one task per file); at scale the source should deliver a splittable format.

## v2.1 — star schema integrity and funnel coverage

A review of the Gold data model found that the star schema did not guarantee referential integrity: 30% of `gold_fact_salary_history` and 25% of `gold_fact_performance_reviews` rows pointed to employees missing from `gold_dim_employees` (employees quarantined in Silver), and BI tools showed them as "(Blank)". The KPI tables are **unchanged** (verified against v2).

- **Unknown member** (`00_utils.py` `resolve_to_unknown_member()`, `03_gold_metrics.py`): `gold_dim_employees` gets the row `employee_id = '-1'` ("Unknown employee", `is_unknown_member = true`, no hire date, status `unknown`, so it never counts in KPIs). Employee keys of the facts (`employee_id`, `approved_by`, `reviewer_id`) and `manager_id` that are not in the dimension are assigned to `'-1'`; the original value is kept in `source_<column>`. Nulls stay null.
- **Positions → departments by ID:** `gold_dim_positions.department_id`, resolved from the department name (the source links them only by name). Names not found (`Producto`, `Ejecutivo`) go to department `'0'` with `department_resolved = false`; they are not mapped by assumption.
- **New table `gold_agg_referential_integrity_data_quality`:** one row per relationship (foreign key → dimension key) with rows, unresolved rows and %. Includes the date relationships to `gold_dim_date` (all resolved).
- **New table `gold_agg_recruitment_coverage_data_quality`:** classifies every candidate (Silver clean + quarantine) by the reason it is excluded from `gold_fact_recruitment_funnel`, or `included_in_funnel`, plus a `TOTAL` row. Only 25 of 100 candidates (6 of 15 hired) reach the funnel: 39 applied to quarantined postings, 26 applied before the posting date, 9 are quarantined in Silver and 1 applied after the posting closed. The fact table and the recruitment KPIs are unchanged; the gap is now measured.
- **Diagrams:** the original ERD was replaced by four generated diagrams in `docs/` (PNG + SVG): source tables used by the pipeline, full source model, Gold star schema and Gold lineage.
- **Tests:** 86 (was 66): Unknown member, employee and date key resolution, positions → departments, integrity table values, funnel grain, funnel coverage values, and a unit test for `resolve_to_unknown_member()`.

| Relationship | Unresolved |
|---|---|
| `gold_fact_salary_history.employee_id` | 83 of 274 (30%) |
| `gold_fact_performance_reviews.employee_id` | 81 of 319 (25%) |
| `gold_fact_performance_reviews.reviewer_id` | 136 of 294 (46%) |
| `gold_fact_salary_history.approved_by` | 140 of 274 (51%) |
| `gold_dim_employees.manager_id` | 160 of 320 (50%) |
| `gold_fact_current_salaries.employee_id` | 0 |
| Dates → `gold_dim_date` | 0 |
| `gold_dim_positions.department_id` | 2 of 20 |

## v2 — data quality and metric fixes

v1 was the first working version of the pipeline. A full review (code + source data) found bugs that silently dropped records and distorted the KPIs. v2 fixes them and adds tests. The numbers below were obtained on the files in `data/raw` and validated on Databricks.

### Impact summary (v1 → v2)

| Metric | v1 | v2 |
|---|---|---|
| Records silently lost (neither in Silver nor in quarantine) | 113 | **0** |
| Bronze `employees` rows | 566 (split records) | **515** (actual records) |
| Silver salaries / salary_history / performance_reviews | 361 / 245 / 291 | **424 / 274 / 319** |
| Gold employees | 298 | **345** |
| Gold terminated employees | 7 | **50** (7 with date, 43 flagged without date) |
| Active employees without a current salary | 89 of 271 | **52 of 272 (19%)** |
| Future-dated salaries taken as current | 5 | **0** |
| Turnover table rows (month × department) | 7 | **one per department and month** |
| Compa-ratio groups shown as `0.00` with no band | 15 | **0** |
| Total payroll | 15,239,198 | **18,118,226** |
| Time to hire (days) | 150.9 (all candidates) | **191.5** (hired only) |

### Breaking changes

| v1 | v2 |
|---|---|
| table `gold_dim_salaries` | `gold_fact_current_salaries` |
| column `early_turnover_pct` | `early_terminations_share_pct` |
| column `days_since_applied` | `days_from_posting_to_application` |
| `gold_dim_employees`: `email`, `phone`, `birth_date` | removed (`age` remains) |
| `avg_compa_ratio` = `0.00` when there is no band | `null` + `band_incomplete = true` |
| `avg_tenure_years` = `0.0` in months without terminations | `null` |
| ratings in `gold_fact_performance_reviews`: `decimal(12,2)` | `decimal(3,2)` |

### New tables and columns

- `gold_agg_turnover_data_quality`: terminations with / without date per department (+ TOTAL row).
- `gold_agg_salary_coverage_data_quality`: salary coverage of active employees per department (+ TOTAL row).
- `gold_dim_employees.termination_date_missing`
- `gold_dim_positions.band_incomplete`, `gold_agg_compensation_equity.band_incomplete`
- `gold_fact_current_salaries.salary_before_hire_date`, `salary_vs_band`
- `gold_agg_recruitment_funnel_kpis.filled_postings`
- `silver_salaries.is_current` (current vs. scheduled salaries)
- `silver_job_postings.source_title`, `title_mismatch`

---

### Fixes

#### 1. "Crammed" records: root cause
- **Files:** `01_bronze_ingestion.py`, `00_utils.py` (`strip_line_breaks()`), `02_silver_transformation.py`.
- **Problem:** some quoted values in the CSVs end with a line break (`"Ingeniería\n"`). Without `multiLine`, Spark split each of those records into 2 physical rows.
- **Fix:** Bronze reads with `multiLine = true`, `quote` and `escape = '"'`. Silver removes `\r\n` from every string column (`trim()` does not).
- **Evidence:** 100% of the "crammed" cases in 7 tables were line breaks; Bronze now has exactly the logical records of each CSV.

#### 2. Silent data loss in `repair_crammed_records()`
- **File:** `00_utils.py`.
- **Problem:** every row of a "repairable island" was removed from quarantine even when the repair failed, as well as null-PK rows that fell inside an island. They disappeared from both Silver and quarantine (113 rows: 34 in salary_history, 61 in salaries, 18 in performance_reviews).
- **Fix:** only `potential_crammed_record` / `potential_crammed_content` rows take part in islands, and only successfully repaired islands leave quarantine. The function is kept as a safety net.

#### 2b. "Crammed" false positives
- **File:** `00_utils.py` (`structural_validation()`).
- **Problem:** every row whose last two columns were empty was flagged as "crammed" (14 valid records in quarantine).
- **Fix:** a record is a crammed parent only when the next row has a non-numeric PK. `check_potential_crammed_record()` is marked as deprecated and kept as a record of the decision.

#### 3. Terminations without date (transparency over imputation)
- **File:** `03_gold_metrics.py`.
- **Problem:** the source has no termination date for 43 of 50 terminated employees (86%). Gold dropped them and turnover was computed with 7 terminations, with no warning.
- **Fix:** they are kept in `gold_dim_employees` with `termination_date_missing = true` and `tenure_years = null` (before, `greatest(null, 0.0)` returned 0). They are excluded from the historical headcount (without a date they would count as active forever). New table `gold_agg_turnover_data_quality`.
- **Warning:** turnover is **underestimated** until the source provides termination dates.

#### 4. Legacy ID offset
- **Files:** `00_utils.py` (`normalize_legacy_id()`), `02_silver_transformation.py` (14 columns).
- **Problem:** the legacy system encodes IDs as `100000 + (real_id - 1)`, but v1 applied `% 100000`. For primary keys, distinct records collided and deduplication dropped one of them; for foreign keys, salaries, contracts, reviews, departments, positions and managers were assigned **to the wrong entity, off by one**.
- **Fix:** `legacy_id - 99999`, with `try_cast` (several columns used `.cast("long")`, which can make the whole table fail with ANSI mode).
- **Evidence:** 60/60 legacy PKs fit their physical position only with `- 99999`; 38/38 legacy `employee_id` FKs in salaries/contracts match with `- 99999` (0/38 with `% 100000`); `department_id` ranges from 100000 to 100009 → departments 1 to 10.

#### 5. Contracts deduplicated by `contract_id`
- **File:** `02_silver_transformation.py`.
- **Problem:** contracts were deduplicated by `employee_id` (the ERD defines 1:N), and all contracts without an employee collapsed into one: 36 were lost before the `employee_id_null` validation.
- **Fix:** deduplication key `contract_id`. The current contract per employee is selected in Gold.

#### 6. Turnover: months without terminations
- **File:** `03_gold_metrics.py`.
- **Problem:** the table started from terminations (LEFT join): only months with at least one termination existed.
- **Fix:** the base is the monthly headcount per department (FULL OUTER join), up to the current month. Months without terminations have `monthly_terminations = 0` and `avg_tenure_years = null`.

#### 7. Time to fill and time to hire
- **File:** `03_gold_metrics.py`.
- **Time to fill:** computed once per filled posting (status `filled` or at least one hire) and averaged per position. Before, it was averaged per candidate (a posting with 8 applicants weighed 8 times more) and varied by source only because of that weighting. New column `filled_postings`.
- **Time to hire:** standard TA definition, **hired candidates only**. Hire date proxy: the posting `closing_date` (the source has no hire date per candidate).

#### 8. Incomplete salary bands
- **Files:** `02_silver_transformation.py`, `03_gold_metrics.py`.
- **Problem:** a missing `min_salary` was replaced by 0 (halving the band midpoint and inflating the compa-ratio of Senior Developer), and the compa-ratio without a band was shown as `0.00`.
- **Fix:** `min_salary` stays null; `band_incomplete` in positions and equity; compa-ratio only for complete bands.

#### 9. Salary coverage and future-dated salaries
- **Files:** `02_silver_transformation.py`, `03_gold_metrics.py`.
- **Future-dated salaries** are valid data (scheduled raises), **not quarantined**: they are kept in `silver_salaries` with `is_current = false` and do not take part in the current salary (before, they won the window because they had the latest date).
- **Salary dated before the hire date:** the employee is no longer excluded; flagged with `salary_before_hire_date`.
- **New table** `gold_agg_salary_coverage_data_quality`.

#### 10. Out-of-range salaries
- **Files:** `02_silver_transformation.py`, `03_gold_metrics.py`.
- **Problem:** empty values and "N/A" were turned into 0 and quarantined as "out of range" (misleading reason).
- **Fix:** specific reasons: `base_salary_null` (34), `base_salary_zero` (4), `base_salary_negative` (4), `base_salary_out_of_range` (> 1M, 3).
- **Business rule – negative salaries:** quarantined for manual review. Their absolute values (31,637 / 39,893 / 129,058 / 354,083) look like plausible salaries, so the sign is probably a data-entry error; they are still not auto-corrected (unlike `salary_history`, which applies `abs()`).
- **New column** `salary_vs_band` in `gold_fact_current_salaries` (informative only: salary bands are not fully reliable in the source).

#### 11. Ambiguous dates
- **File:** `00_utils.py` (`normalize_to_date_format()`).
- **Problem:** `dd/MM/yyyy` was tried before `MM/dd/yyyy`; 18 ambiguous slash dates were misread (e.g. `02/05/2023` → May 2 instead of Feb 5).
- **Fix:** `MM/dd/yyyy` before `dd/MM/yyyy`. **Evidence:** 21/21 non-ambiguous slash dates have the day in 2nd position; 12/12 `hire_date` / contract `start_date` pairs only match when read as `MM/dd`. Dash dates are `dd-MM-yyyy` (21/21) and were already read correctly.

#### 12. Errors stop the pipeline
- **Files:** the three layer notebooks.
- **Problem:** `try/except` blocks logged errors and the notebook still finished successfully, so the next layer ran over stale data.
- **Fix:** a final cell in each notebook raises `RuntimeError`:
  - **Bronze:** only if a CSV used by Silver fails (`critical_tables`); the other 7 tables log a `WARNING`.
  - **Silver:** if any entity fails.
  - **Gold:** if any table fails or is written empty (before, an empty table left no log at all).
- **Trainings:** `try_cast(is_mandatory AS boolean)` (before, `coalesce(string, boolean)` could fail depending on the ANSI settings).

#### 13. Null primary keys → surrogate keys
- **Files:** `00_utils.py` (`recover_null_pk()`), `02_silver_transformation.py`.
- **Problem:** 114 real, almost complete records (no duplicates) stayed in quarantine only because they had no ID.
- **Fix:** deterministic key `SK-<first 16 chars of sha2(business columns)>`; the records continue through the normal flow (if they have other problems, they are still quarantined). Inferring the ID from the row position was discarded because it depends on the physical order of the file.

#### Minor
- **`fuzzy_match_category()`** (`00_utils.py`): replaces 4 Levenshtein blocks; maximum distance 2 (every real typo is at ≤ 2); farther values → `unknown`. Applied to `status` and `employment_type` (employees), `contract_type` and candidate `status` (now normalized: `Hired_v2` was at distance 3).
- **Ratings:** `N/A` → null (it was 0) and no `abs()`: negative scores are quarantined as out of range (13 reviews with out-of-range scores).
- **Candidate sources:** explicit mapping (`LinkedIn`, not `Linkedin`).
- **Job postings:** `silver_positions` is the source of truth for the title (the original title is kept in `source_title`, differences flagged in `title_mismatch`); column list in the physical order of the CSV.
- **Privacy:** Gold no longer exposes email, phone or birth date.
- **Gold `%sql SELECT *`** cells use `LIMIT 100`.
- **Salaries quarantine %** computed over validated records, like the other tables.
- **History IDs** in `silver_salaries` prefixed with `HIST-`.

#### Tests
- Unit tests for `00_utils` and an integration test that runs the full pipeline on `data/raw`, executed on GitHub Actions. Several of them are regression tests for the bugs above (they fail on v1).

---

### Source data issues (reported, not fixed by the pipeline)

- **Termination dates** missing for 86% of terminated employees.
- **Salary coverage:** 19% of active employees without a valid current salary (15 without any salary in the source).
- **Multiple CEOs:** 23 employees with the CEO position; 6 of the active ones with a salary are paid below the minimum of the CEO band.
- **Employee department vs. position department** disagree for most employees.
- **Job posting titles** disagree with their position in every posting that has a title.
- **Salary history** is inconsistent: in 0 of 174 transitions does `old_salary` match the previous `new_salary`.

### Technical debt / next steps

- **Time to hire** uses `closing_date` as a proxy; `interviews` (ingested in Bronze, not processed) could provide real dates.
- **Tables not processed in Silver:** `benefit_types`, `employee_benefits`, `employee_trainings`, `goals`, `interviews`, `time_off_balances`, `time_off_requests`.
- **Currency:** USD and monthly pay frequency are assumed (true for the whole source today); there is no conversion if other values appear.
- **Open question:** check overlapping effective dates in `salary_history`.
- **Scalability:** `repair_crammed_records()` (safety net) and its Python UDF depend on the physical row order; there is no `cache()` between actions. No impact at the current volume.

## v1 — initial version

Medallion pipeline (Bronze → Silver → Gold) with quarantine tables, centralized logging and three KPI modules (turnover, recruitment funnel, compensation equity).
