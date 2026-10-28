# In-depth analysis

This document is the long version of the [README](../README.md): the reasoning behind each People Analytics module, the data quality findings that shaped the pipeline, and how to read the results. The README is enough to understand what the project does; this is for readers who want the analytical detail.

All figures were obtained on the synthetic data in [`data/raw`](../data/raw) (the original extract, which is also the first delivery) and validated on Databricks. The sections marked **v3** use the simulated deliveries in [`data/batches`](../data/batches) to show what the history adds. Code changes and the evidence behind them are documented in the [CHANGELOG](../CHANGELOG.md).

## Contents

1. [Monthly Turnover and Retention per Department](#monthly-turnover-and-retention-per-department)
2. [Recruitment Funnel & Channel Performance](#recruitment-funnel--channel-performance-talent-acquisition-analytics)
3. [Pay Equity & Compa-ratio](#pay-equity--compa-ratio)
4. [Open questions](#open-questions)

---

## Monthly Turnover and Retention per Department

This module works as a **Monthly Accumulating Fact Table** in the Gold layer, consolidating the critical metrics to audit workforce stability and the talent lifecycle:

*   **Gross Turnover Rate (`gross_turnover_pct`):** Terminations in the period divided by the active headcount at the end of that same month (EOM). The table includes **every month** with headcount per department (up to the current month); months without terminations appear with `monthly_terminations = 0`, so it also works as a headcount time series. Measuring against the exact snapshot of the last day of each month avoids using the current headcount, which would introduce retrospective bias. Using today's fixed headcount would introduce **survivorship bias**, underestimating turnover in departments that grew and distorting the metric for departments that shrank or closed.
*   **Early Turnover (`early_terminations_share_pct`):** Applies the *First-Year Rule*, isolating the terminations that occurred before reaching 1.0 year of tenure (`early_terminations`), as a share of the month's terminations.
*   **Knowledge Drain (`avg_tenure_years`):** Average years of service of the employees who left (`null` in months without terminations). The turnover percentage alone can hide local realities; adding tenure brings the **Knowledge Drain** dimension, putting turnover in context according to the tenure of the people who left, e.g. distinguishing the impact of losing an operational profile from losing a senior leader.
*   **Department granularity:** Company-wide turnover often masks local problems. Grouping by department makes it possible to identify departments with turnover patterns that need further investigation (for example, it provides objective data to look into leadership dynamics, operational bottlenecks or misaligned salary bands in specific areas, among other issues).

---

### ⚠️ Data quality warning: terminations without date

The source system **has no termination date for 86% of terminated employees (43 of 50)**. The termination date is taken from the contract `end_date`; in those cases it is missing, earlier than the hire date, or the employee has no valid contract.

*   **Impact:** those terminations cannot be placed in a month, so **turnover in this table is underestimated** (it currently reflects only 7 terminations).
*   **Handling:** the affected employees are kept in `gold_dim_employees` with `termination_date_missing = true`, `termination_date = null` and `tenure_years = null`. They are excluded from the historical headcount, because without a termination date they would count as active indefinitely.
*   **Monitoring:** the table `gold_agg_turnover_data_quality` shows, per department and in total, how many terminations have a date and how many do not (`pct_terminations_without_date`). It is recommended to show it next to the turnover KPIs in Power BI.
*   **Suggested action:** ask the team that owns the source system to load the termination dates.
*   **v3: dates detected from the history.** With incremental loading, Silver keeps every version of each employee (SCD Type 2). When the contract has no end date, the termination date is the delivery in which the change to `terminated` was detected (`termination_date_source = 'status_change_detected'`). Two conditions keep it honest: a previous version must have another status (employees already terminated in the first delivery get no invented date), and the date has the precision of the load frequency (with monthly deliveries, a termination detected on 1 September may have happened in August). On the simulated deliveries, employee 142 gets 2026-09-01; the 43 historical cases stay without date, because their change happened before the history started.

---

### v3: Headcount by the department at each date

Before v3, the monthly headcount used each employee's **current** department for every month: a transfer silently rewrote the employee's past, moving all their history to the new department. With the SCD2 history, each employee counts in the department they had at each month end, and each termination in the department at the termination date. Dates before the first delivery use the first known version, since there is no earlier history.

Monthly totals do not change, only how they split across departments. On the simulated deliveries:

| Department | Aug 2026 | Sep 2026 | Why |
|---|---|---|---|
| Data | 34 | 35 | employee 28 transferred in, employee 120 transferred out, new hire 501 |
| Recursos Humanos | 22 | 22 | employee 120 transferred in, employee 166 terminated |
| Operaciones | 29 | 27 | employee 28 transferred out, employee 142 terminated |

With the current department, employee 28 would also count in Data in August, and employee 120 in Recursos Humanos.

---

### Trade-offs and optimization

Computing historical time-based metrics in Spark without a solid star schema hurts cluster performance. This module applies the following design patterns:

#### 1. Modeling with a calendar dimension (`gold_dim_date`)
*   **Practical optimization:** The calendar dimension provides a compact set of month-end dates, avoiding generating dates with string concatenations or complex date functions.
*   **Technical rationale:** The employees × months combination is resolved with a date-range join (`hire_date <= end_of_month_date` and termination after the month end). Spark executes it as a `BroadcastNestedLoopJoin`, which is enough for the current volume. At large scale it would be better to generate the active months of each employee (`sequence` + `explode` between hire and termination) and join on year and month equality, or to use the Databricks *range join hint*.

#### 2. Protection against Python namespace conflicts
*   **Error resolution (`[NOT_ITERABLE]`):** Functions were imported directly in the Databricks environment. An explicit condition was used because it was necessary to control exactly which columns were compared.
*   **Applied solution:** The ambiguous syntax was replaced by an explicit logical condition bound to the DataFrame instances (since v2, with DataFrame aliases: `col("t.year") == col("h.year")`), and the logical aggregations (such as `CASE WHEN` structures) were wrapped in native SQL expressions with `expr()`. This fully isolated Python variables from Spark's logical engine.

---

### Sneak peek of results

> Since 86% of terminations have no date (see the warning above), the following cases are based on the few dated terminations and should be read as illustrative.

*   **Legal case (September 2016):** High gross turnover (`gross_turnover_pct = 12.5%`, 1 termination over a headcount of 8), with a severe early turnover (`early_terminations_share_pct = 100%`) and low tenure (`avg_tenure_years = 0.5`). **Analytical diagnosis:** the problem would not be the internal climate or long-term salary. The pattern justifies looking into issues related to selection, onboarding, initial adaptation or job conditions.
*   **Product case (August 2025):** Low and healthy turnover (`3.70%`, 1 termination over a headcount of 27), with `0%` early turnover and longer tenure (`1.42`). **Analytical diagnosis:** the pattern could justify looking into retention factors for longer-tenured employees, such as career development, compensation or external opportunities. Terminations of profiles with some tenure could signal **blocked career ceilings (lack of promotions)** or, depending on the case and the context gathered, market or competitor offers that exceed what the company can offer.

---

### Discussion: End-of-Month (EOM) headcount vs. average headcount

Two approaches can be considered:
In a more **classic** approach, the average headcount `(start of month + end of month) / 2` is used to smooth the impact if the company hired a large number of employees on the last day of the month.
From an **analytics engineering** perspective, the end-of-month snapshot is used as the cut-off point. It is an efficient standard for automated pipelines in Lakehouse architectures thanks to its **metadata stability and computation speed**, and a simple, consistent option for automated pipelines and later consumption in Business Intelligence tools (such as Power BI / Tableau).

---

## Recruitment Funnel & Channel Performance (Talent Acquisition Analytics)

This module consolidates the core KPIs to measure the operational efficiency of the hiring process and the performance of the talent sourcing channels, based on three strategic pillars:

*   **Recruitment Channel Effectiveness (`source_yield_pct`):** Measures the final conversion rate of each candidate source. It provides objective input for critical budget questions such as *Where should we allocate more resources? Is it worth paying for the corporate LinkedIn subscription if Indeed's yield for certain positions is equivalent or higher?*. Interpretation must consider candidate volume, since percentages calculated on small samples can be statistically unstable.
*   **Time to Fill (`avg_time_to_fill_days`):** Measures the days between the posting date and the closing date of the job posting. It is calculated **once per filled posting** (status `filled` or at least one hired candidate) and then averaged per position (`filled_postings` shows how many postings are included in the average). It is a **position-level** metric: it has the same value in every source row of that position, because it depends on the vacancy and not on the channel. It is a direct indicator of how quickly hiring managers and the HR team fill open positions, avoiding productivity losses caused by vacant roles.
*   **Time to Hire (`avg_time_to_hire_days`):** Follows the standard Talent Acquisition definition: it considers **hired candidates only** and measures the days from application to hire. Since the source has no hire date per candidate, the posting closing date (`closing_date`) is used as a proxy. A *Time to Hire* that is too long drives qualified talent away in competitive markets. It is `null` for sources without hires.

---

### Data quality and integrity filters

Applicant tracking systems (ATS) often suffer from manual data-entry errors. A detailed analysis of the atomic data revealed severe chronological inconsistencies that required defensive data engineering:

#### 1. Time boundary control (negative time outliers)
*   **Initial problem detected:** The combination of the *HR Business Partner* position via *Recruiter* returned an absurd average of **`-760` days** in *Time to Hire*.
*   **Root cause:** A transactional inconsistency in the source: the candidate's application date (`applied_date`) was more than two years later than the posting closing date (`closing_date`), or the years were entered inverted by human error.
*   **Applied solution (*Filter Early* principle):** Instead of hiding the error with cosmetic patches in the aggregated table, it was fixed at the root in the **Recruitment Fact Table (`gold_fact_recruitment_funnel`)**, built by the Gold notebook (`03_gold_metrics`). A strict chronological integrity rule was added to the join:
    `& (col("j.closing_date").isNull() | (col("j.closing_date") >= col("c.applied_date")))`
    This surgically removed the corrupted records from the whole lineage, ensuring a single clean source of truth.

#### 2. Native handling of unfilled postings
*   **Pipeline behavior:** For positions without filled postings, such as *Engineering Manager* (ID 4), the time metrics return **`null`**.
*   **Rationale:** *Time to Fill* only considers filled postings (status `filled` or at least one hired candidate), and *Time to Hire* only hired candidates. The Engineering Manager posting was closed (`closed`) without hires, so it contributes no values and Spark ignores it in the averages (`avg`), preventing artificial zeros that would distort executive reports. The same applies to postings that are still open (no `closing_date`).

#### 3. Funnel coverage: only 25 of 100 candidates reach Gold
*   **Finding:** Of the 100 unique candidates in the source (103 rows; 3 duplicates are removed in Silver), only **25** reach `gold_fact_recruitment_funnel` (6 of the 15 hired candidates).

    | Reason for exclusion | Candidates |
    |---|---|
    | Quarantined in Silver (6 without `posting_id`, 2 without `applied_date`, 1 with an incomplete name) | 9 |
    | Applied to a posting quarantined in Silver (postings 5, 6, 7 and 9: status and `closing_date` disagree) | 39 |
    | Applied before the posting date (`applied_date < posted_date`) | 26 |
    | Applied after the posting closed (`applied_date > closing_date`) | 1 |
    | **Reach the funnel** | **25** |

*   **Rationale:** the fact table only keeps applications whose dates fit inside the posting window, so that *Time to Hire* and *Time to Fill* are never computed on impossible chronologies (see point 1). Applications to quarantined postings have no valid posting to join to.
*   **Impact:** the recruitment KPIs describe a quarter of the candidate base. Channel samples are 2–6 applicants (aggregate source yield: LinkedIn 2 of 4 hired, Company Website 2 of 5, Unknown 1 of 3, Indeed 1 of 6, Recruiter 0 of 5, Referral 0 of 2), so no channel ranking is statistically reliable.
*   **Monitoring:** the table `gold_agg_recruitment_coverage_data_quality` shows, per exclusion reason and in total, how many candidates (and hired candidates) are excluded from the funnel (`pct_of_candidates`). It is recommended to show it next to the recruitment KPIs in Power BI.
*   **Suggested action:** review with the ATS owners the four quarantined postings (they concentrate 39 applications) and the application dates entered before the posting date.

---

### v3: Candidate stage history

`gold_fact_candidate_stage_history` has one row per candidate and stage reached, built from the SCD2 history of candidates:

*   Every candidate starts with `applied` on the real application date (`stage_date_source = 'applied_date'`).
*   Every later status change is a new stage, entered on the delivery in which it was detected (`status_change_detected`). The status a candidate already had when first seen has an unknown entry date (`before_first_observation`, null).
*   `stage_exited_date` is the entry date of the next stage, and `days_in_stage` is computed only when both dates are known. Candidates removed from the source are excluded.

Example (simulated deliveries): candidate 103 applied on 2026-08-18 and was detected as rejected on 2026-10-01 (44 days in `applied`). Candidate 101 was first seen in `interview`, then detected as `hired` on 2026-10-01.

**Why time to hire keeps the proxy.** The detected hire of candidate 101 (2026-10-01) is later than the posting's closing date (2026-09-25): with monthly deliveries, a detected date can be up to a month late. Mixing detected dates and the `closing_date` proxy in one average would make the KPI less reliable, so `avg_time_to_hire_days` keeps the proxy. With daily loads the detected date would be precise to the day, and it would become the better source.

---

### Analytical application example (results analysis)

Results for the **Sales Representative (ID 7)** position:

*   **LinkedIn:** 1 applicant, 1 hire (**100% yield**), with a *Time to Hire* of 238 days.
*   **Recruiter:** 2 applicants, 0 hires (**0% yield**).
*   **Referral:** 1 applicant, 0 hires (**0% yield**).
*   **Position Time to Fill:** 262 days (1 filled posting).

**Analytical reading:** LinkedIn is the only channel that converted for this position, but with samples of 1 or 2 applicants per channel no percentage is statistically stable. The result is a **signal to monitor**, not a basis for reallocating budget: that decision should be made after accumulating more postings for the same position or grouping similar positions.

> **v2 note:** this example changed from the previous version. After fixing the legacy ID offset (`normalize_legacy_id()`), some candidates previously assigned to position 7 actually belong to the **Sales Manager (ID 8)** posting. Also, *Time to Fill* no longer varies by source: that variation was an effect of weighting by number of candidates.

---

## Pay Equity & Compa-ratio

This module consolidates the core metrics for the strategic management of the organization's salary structures, based on three key pillars:

*   **Internal Pay Equity and Gender Pay Gap:** By grouping by department, position and gender, the model directly shows whether there are pay differences for identical responsibilities, making it possible to audit structural biases.
*   **Active Payroll (`total_payroll`):** By filtering strictly on employees with status `active` or `on_leave`, it gives the finance and HR teams the operating cost of the current fixed payroll **for employees with a registered current salary** (see *Salary coverage*).
*   **Average Compa-Ratio (`avg_compa_ratio`):** Measures the health of the compensation strategy against the market (`Salary / Mid-Point`). A compa-ratio close to `1.0` indicates alignment with the band midpoint. Downward deviations signal a high risk of talent loss, and upward deviations indicate positions reaching the top of their salary band. It is calculated **only for complete bands**; if the band is incomplete it is `null` and `band_incomplete = true`.

Employee-level complements in `gold_fact_current_salaries` (formerly `gold_dim_salaries`):
*   **`salary_vs_band`:** `below_band`, `within_band`, `above_band` or `null` (incomplete band or unknown position). It is informative only; it does not send records to quarantine.
*   **`salary_before_hire_date`:** flags employees whose last known salary is dated before their hire date.

---

### ⚠️ Salary coverage

**19% of active or on-leave employees (52 of 272) have no valid current salary** and are excluded from `total_payroll` and from the compa-ratio. Main causes: their only salary record is in quarantine (empty or zero salary, no date, negative), or they have no salary at all in the source. The table `gold_agg_salary_coverage_data_quality` shows coverage per department (`pct_without_salary`); it is recommended to show it next to the compensation KPIs in Power BI.

---

### Data quality and governance

Processing the data exposed severe inconsistencies in the source systems (ATS / payroll). These findings work as an **automated diagnosis** for the Data Governance and HR teams.

#### 1. Positions without a formal salary band (`band_incomplete = true`)
*   **Example detected:** `Ingeniería | Junior Developer | M | 1 | 44539.00 | 44539.00 | null | true`
*   **Root cause:** In `silver_positions`, three positions have an incomplete band: **Junior Developer** and **Financial Analyst** (no `max_salary`) and **Senior Developer** (no `min_salary`). Without both ends there is no midpoint.
*   **Handling (v2):** the compa-ratio stays `null` (before, it was shown as `0.00`, which read as "far below market"), and the missing `min_salary` is no longer replaced by `0` (that halved the midpoint and inflated the Senior Developer compa-ratio).
*   **Business impact:** It shows which active positions **have no formal salary band, or one that is not up to date with the market**.

#### 2. Extreme deviations (compa-ratio outliers)
*   **Example detected:** `Ventas | Support Agent | F | 1 | 82481.00 | 82481.00 | 2.54` (position band: \$25k–\$40k).
*   **Root cause:** A compa-ratio of `2.54` means the employee earns 254% of her band midpoint, which is theoretically impossible under a healthy pay policy (where the ceiling is usually `1.20` or `1.25`). Since the pay frequency is consistently monthly (`monthly`), the possible root causes are:
    1.  **Band loading error:** the position's `min_salary` and `max_salary` became outdated or were loaded incorrectly, artificially lowering the midpoint.
    2.  **Assignment error:** she may actually earn a salary at that level, but administratively hold a position code of a lower seniority.

> **v2 note:** the previous example (`Finanzas | Marketing Analyst | F | 2.32`) **was a pipeline error**: `% 100000` assigned legacy IDs to the wrong record (off by one), and that salary belonged to another person. With `normalize_legacy_id()` that group has a compa-ratio of `0.96`, within the band.

#### 3. Unusual organizational combinations and incorrect position/department assignments
*   **Root cause:** For most employees, the department in their employee record does not match the department of their position (verified in v2 with the legacy IDs fixed: it is not a pipeline effect). It reflects outdated internal mobility processes: when an employee is transferred, the system usually updates their position or role but not their cost center or department, distorting the organizational matrix.
*   **Critical uniqueness anomaly (multiple CEOs):** The source has **23 employees with the CEO position** (15 active or on leave in Gold), spread across different areas. In addition, 6 of the active CEOs with a salary are paid below the minimum of the CEO band (`salary_vs_band = 'below_band'`, with compa-ratios between `0.11` and `0.27`), which indicates the position is wrongly assigned. This creates severe analytical noise and shows the need for an audit to reassign the correct position titles.

#### 4. Negative salaries (business rule)
*   **Example detected:** 4 records in `salaries` with a negative salary (−31,637 / −39,893 / −129,058 / −354,083).
*   **Handling:** **by business rule they are quarantined** (`base_salary_negative`) for manual review. Their absolute values look like plausible salaries, so the sign is probably a data-entry error; even so, they are not auto-corrected.

---

### Chronological reconciliation strategy (Silver layer)

The most critical technical challenge was the **desynchronization and presence of duplicates (employee FK)** between the fixed salary table (`silver_salaries`) and the salary history (`silver_salary_history`).

#### Evidence of lineage inconsistencies (case `employee_id = 2`):
*   **2018-10-24 (`salary_history`):** Records an initial salary (`old_salary` null) of **\$75,053.00** due to a `market_adjustment`.
*   **2019-12-16 (`salaries`):** Shows a frozen static record of **\$50,115.00**. This number is much lower and has no relation to the history.
*   **2020-11-01 (`salary_history`):** During an `annual_review`, the system states that the previous salary was **\$61,554.00** (an amount that does not exist in either table) and raises it to \$97,797.00.

> **v2 note:** the **2021-06-10** event (`promotion`, previous salary of \$32,856.00 raised to \$69,119.00), previously cited as part of this case, **actually belongs to employee 3**: in the source it has `employee_id = 100002`, a legacy ID that `% 100000` assigned to employee 2. The other inconsistencies of employee 2 do come from the source.

#### Implemented solution:
To deliver a reliable report without biases or massive payroll duplication, a **chronological merge and curation mechanism was designed in the Silver layer**:

1.  **Source unification:** The records of the static table and the salary history events were consolidated with `unionByName`, aligning their schemas (IDs coming from the history have the `HIST-` prefix). `silver_salaries` is established as the reference for each employee's current salary and is used in Gold for the calculations that need it.
2.  **Scheduled salaries (v2):** records with a future `effective_date` are valid data (scheduled raises): they are kept in `silver_salaries` with `is_current = false` and do not take part in the current salary calculation.
3.  **Time-window deduplication:** A window function (`row_number()`) partitioned by `employee_id` and ordered descending by `effective_date` and ingestion date (`_ingested_at`).
4.  **Capture of the actual current salary:** The pipeline selects the record with `rn == 1` (`is_current = true`). This ensures that, whether the employee's latest information came from the fixed table or from the history, the Silver layer consolidates the **latest known contractual reality**.

**Result:** The Gold layer calculates payroll and pay equity on each employee's current salary, isolating temporal gaps and exposing the areas where HR and Data Governance need to act.

---

## Open questions

- How should the pipeline scale to millions of records? (see *Trade-offs and optimization* above and the roadmap in the README)
- Should overlapping effective dates be checked in `salary_history`?
- Can termination dates be derived from the source? Partly, since v3: the history detects the change to `terminated` from the first delivery onwards (see the turnover section). The dates of past terminations still have to come from the source system.
- How often should the source be loaded? Monthly deliveries make detected dates up to a month imprecise; daily loads would make them precise enough to measure time to hire with the observed hire date.
