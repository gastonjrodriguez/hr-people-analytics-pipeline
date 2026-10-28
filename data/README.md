# Data

`raw/` contains the 17 CSV extracts of the original source (one file per table of the HR
database; see [`docs/erd_source_full.svg`](../docs/erd_source_full.svg)).

**The data is synthetic.** Names, emails, phone numbers, salaries and every other value were
generated for this project and do not belong to real people or companies. The files intentionally
contain the kind of defects found in real HR extracts (typos, mixed date formats, legacy IDs,
quoted values with line breaks, missing keys), which the Silver layer detects, fixes or quarantines.

Only 10 of the 17 tables are processed by Silver/Gold; the rest are ingested into Bronze for
future use (`benefit_types`, `employee_benefits`, `employee_trainings`, `goals`, `interviews`,
`time_off_balances`, `time_off_requests`).

## Simulated deliveries (`batches/`)

The pipeline loads incrementally, one delivery at a time, but the source is a single static
extract. `batches/` contains three **simulated** monthly deliveries, built by
[`scripts/generate_batches.py`](../scripts/generate_batches.py). Each one is a full extract (all 17
tables), as the source system would deliver it:

| Delivery | Content |
|---|---|
| `batch_date=2026-08-01/` | The original extract (`raw/`), byte by byte. |
| `batch_date=2026-09-01/` | 3 terminations (employees 21 and 116 with contract end date, 142 without it); promotion of employee 7 (Support Agent → Support Lead) and promotion + transfer of employee 120, both with a salary history event; transfer of employee 28; manager change of employee 22; new job posting 13 (Data Analyst) with 6 candidates (101–106) at different stages. |
| `batch_date=2026-10-01/` | Termination of employee 166 (with contract end date); posting 13 filled: candidate 101 hired, 102–105 rejected; candidate 106 removed from the source; new employee 501 (candidate 101) with contract and salary; source fixes: the Junior Developer band gets its maximum and posting 5 gets its closing date; 3 new performance reviews. |

The script is deterministic (no random values) and lists every change in `CHANGES`; untouched
records keep their original text, including their defects. Running it again reproduces the same
files (`python scripts/generate_batches.py`), and a test checks that `batches/` matches it.
