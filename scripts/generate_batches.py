"""
Generates the simulated monthly deliveries used to demonstrate incremental loading.

The source system delivers FULL extracts (every row of every table, each time). The original
extract in `data/raw` is delivery 1; deliveries 2 and 3 are copies of the previous delivery with
the small, explicit set of changes listed in `CHANGES` below (terminations, promotions, a new
job posting and its candidates moving through the funnel, a new hire, etc.).

Everything is deterministic (no random values): running the script again produces the same files.
Untouched records keep their original text byte by byte, including their data quality defects;
only the records listed in `CHANGES` are rewritten or added.

Usage:
    python scripts/generate_batches.py            # writes data/batches/batch_date=YYYY-MM-DD/
"""

import csv
import io
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
OUT_DIR = REPO_ROOT / "data" / "batches"

BATCH_DATES = ["2026-08-01", "2026-09-01", "2026-10-01"]

# ----------------------------------------------------------------------------------------------
# Changes per delivery. Each operation is (action, table, key, values):
#   ("update", table, key, {column: new_value})  -> the record whose primary key is `key`
#   ("insert", table, None, {column: value})     -> a new record (missing columns are empty)
#   ("delete", table, key, None)                 -> the record disappears from the extract
# The key is matched on the first column (primary key), after normalizing legacy IDs, and must
# identify exactly ONE record of the previous delivery.
# ----------------------------------------------------------------------------------------------

CHANGES = {
    "2026-09-01": [
        # terminations: two with the contract end date loaded, one without it
        ("update", "employees", "21", {"status": "terminated"}),
        ("update", "contracts", "21", {"end_date": "2026-08-14"}),
        ("update", "employees", "116", {"status": "terminated"}),
        ("update", "contracts", "116", {"end_date": "2026-08-28"}),
        ("update", "employees", "142", {"status": "terminated"}),
        # promotion: Support Agent -> Support Lead (salary change through salary_history)
        ("update", "employees", "7", {"position_id": "18"}),
        ("insert", "salary_history", None, {"history_id": "396", "employee_id": "7", "old_salary": "34957",
                                            "new_salary": "52000", "change_reason": "promotion",
                                            "effective_date": "2026-08-15", "approved_by": "2"}),
        # promotion + transfer: Hr Specialist (Data) -> Hr Business Partner (Recursos Humanos)
        ("update", "employees", "120", {"position_id": "12", "department_id": "4"}),
        ("insert", "salary_history", None, {"history_id": "397", "employee_id": "120", "old_salary": "50509",
                                            "new_salary": "62000", "change_reason": "promotion",
                                            "effective_date": "2026-08-15", "approved_by": "2"}),
        # transfer: Operaciones -> Data
        ("update", "employees", "28", {"department_id": "10"}),
        # manager change
        ("update", "employees", "22", {"manager_id": "5"}),
        # new job posting and its first applicants
        ("insert", "job_postings", None, {"posting_id": "13", "position_id": "6", "department_id": "10",
                                          "title": "Data Analyst",
                                          "description": "We are looking for a talented professional to join our team.",
                                          "requirements": "3+ years experience, relevant skills",
                                          "salary_min": "40000", "salary_max": "70000", "location": "Mendoza",
                                          "posted_date": "2026-08-10", "status": "open", "hiring_manager_id": "2"}),
        ("insert", "candidates", None, {"candidate_id": "101", "posting_id": "13", "first_name": "Valentina", "last_name": "Gómez",
                                        "email": "candidate101@email.com", "phone": "+54 9 11 4410-2381",
                                        "resume_url": "https://storage.company.com/resumes/101.pdf",
                                        "source": "LinkedIn", "applied_date": "2026-08-12", "status": "interview"}),
        ("insert", "candidates", None, {"candidate_id": "102", "posting_id": "13", "first_name": "Joaquín", "last_name": "Díaz",
                                        "email": "candidate102@email.com", "phone": "+54 9 11 5532-9104",
                                        "resume_url": "https://storage.company.com/resumes/102.pdf",
                                        "source": "Referral", "applied_date": "2026-08-14", "status": "screening"}),
        ("insert", "candidates", None, {"candidate_id": "103", "posting_id": "13", "first_name": "Camila", "last_name": "Romero",
                                        "email": "candidate103@email.com", "phone": "+54 9 11 6120-4478",
                                        "resume_url": "https://storage.company.com/resumes/103.pdf",
                                        "source": "Indeed", "applied_date": "2026-08-18", "status": "applied"}),
        ("insert", "candidates", None, {"candidate_id": "104", "posting_id": "13", "first_name": "Tomás", "last_name": "Álvarez",
                                        "email": "candidate104@email.com", "phone": "+54 9 11 3398-7712",
                                        "resume_url": "https://storage.company.com/resumes/104.pdf",
                                        "source": "Company Website", "applied_date": "2026-08-20", "status": "applied"}),
        ("insert", "candidates", None, {"candidate_id": "105", "posting_id": "13", "first_name": "Julieta", "last_name": "Torres",
                                        "email": "candidate105@email.com", "phone": "+54 9 11 7765-0293",
                                        "resume_url": "https://storage.company.com/resumes/105.pdf",
                                        "source": "LinkedIn", "applied_date": "2026-08-22", "status": "screening"}),
        ("insert", "candidates", None, {"candidate_id": "106", "posting_id": "13", "first_name": "Nicolás", "last_name": "Ruiz",
                                        "email": "candidate106@email.com", "phone": "+54 9 11 2287-5560",
                                        "resume_url": "https://storage.company.com/resumes/106.pdf",
                                        "source": "Recruiter", "applied_date": "2026-08-25", "status": "applied"}),
    ],
    "2026-10-01": [
        # termination with the contract end date loaded
        ("update", "employees", "166", {"status": "terminated"}),
        ("update", "contracts", "166", {"end_date": "2026-09-19"}),
        # the Data Analyst posting is filled: one hire, the rest rejected
        ("update", "job_postings", "13", {"status": "filled", "closing_date": "2026-09-25"}),
        ("update", "candidates", "101", {"status": "hired"}),
        ("update", "candidates", "102", {"status": "rejected"}),
        ("update", "candidates", "103", {"status": "rejected"}),
        ("update", "candidates", "104", {"status": "rejected"}),
        ("update", "candidates", "105", {"status": "rejected"}),
        # a candidate asks to be removed from the ATS: the record disappears from the extract
        ("delete", "candidates", "106", None),
        # the hired candidate joins as a new employee
        ("insert", "employees", None, {"employee_id": "501", "employee_code": "EMP-00501", "first_name": "Valentina",
                                       "last_name": "Gómez", "email": "valentina.gómez501@company.com",
                                       "phone": "+54 9 11 4410-2381", "birth_date": "1996-04-11", "gender": "F",
                                       "department_id": "10", "position_id": "6", "manager_id": "2",
                                       "hire_date": "2026-09-28", "employment_type": "full_time",
                                       "work_location": "Mendoza", "status": "active"}),
        ("insert", "contracts", None, {"contract_id": "501", "employee_id": "501", "contract_type": "permanent",
                                       "start_date": "2026-09-28", "probation_end_date": "2026-12-28",
                                       "notice_period_days": "30", "signed_date": "2026-09-26"}),
        ("insert", "salaries", None, {"salary_id": "501", "employee_id": "501", "base_salary": "52000", "currency": "USD",
                                      "bonus_percentage": "5", "effective_date": "2026-09-28", "pay_frequency": "monthly"}),
        # source fixes: the Junior Developer band gets its maximum; posting 5 gets its closing date
        ("update", "positions", "1", {"max_salary": "60000"}),
        ("update", "job_postings", "5", {"closing_date": "2024-06-30"}),
        # new performance reviews
        ("insert", "performance_reviews", None, {"review_id": "407", "employee_id": "1", "reviewer_id": "2",
                                                 "review_period": "Q3 2026", "overall_rating": "4.1", "technical_skills": "4.0",
                                                 "communication": "4.3", "teamwork": "4.2", "leadership": "3.8",
                                                 "comments": "Performance review feedback", "review_date": "2026-09-15",
                                                 "status": "completed"}),
        ("insert", "performance_reviews", None, {"review_id": "408", "employee_id": "2", "reviewer_id": "5",
                                                 "review_period": "Q3 2026", "overall_rating": "3.6", "technical_skills": "3.9",
                                                 "communication": "3.4", "teamwork": "3.7", "leadership": "3.2",
                                                 "comments": "Performance review feedback", "review_date": "2026-09-16",
                                                 "status": "completed"}),
        ("insert", "performance_reviews", None, {"review_id": "409", "employee_id": "7", "reviewer_id": "2",
                                                 "review_period": "Q3 2026", "overall_rating": "4.5", "technical_skills": "4.4",
                                                 "communication": "4.6", "teamwork": "4.7", "leadership": "4.2",
                                                 "comments": "Performance review feedback", "review_date": "2026-09-18",
                                                 "status": "completed"}),
    ],
}


# ----------------------------------------------------------------------------------------------
# CSV handling (record level, so untouched records keep their exact original text)
# ----------------------------------------------------------------------------------------------

def read_records(path: Path):
    """Returns (header, records). A record is the raw text of one logical CSV row; quoted values
    with embedded line breaks span several physical lines."""
    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")
    header, records, buffer = lines[0], [], None
    for line in lines[1:]:
        buffer = line if buffer is None else buffer + "\n" + line
        if buffer.count('"') % 2 == 0:
            records.append(buffer)
            buffer = None
    if buffer is not None:
        records.append(buffer)
    if records and records[-1] == "":
        records.pop()
    return header, records


def parse(record: str) -> list:
    return next(csv.reader(io.StringIO(record)))


def to_record(values: list) -> str:
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(values)
    return out.getvalue()


def normalize_id(value: str) -> str:
    """Same rule as normalize_legacy_id() in 00_utils: legacy IDs are 100000 + (id - 1)."""
    try:
        number = int(value)
    except ValueError:
        return value
    return str(number - 99999) if number >= 100000 else str(number)


def apply_changes(header: str, records: list, operations: list) -> list:
    columns = parse(header)
    records = list(records)
    for action, _, key, values in operations:
        if action == "insert":
            unknown = set(values) - set(columns)
            assert not unknown, f"unknown columns {unknown}"
            records.append(to_record([values.get(c, "") for c in columns]))
            continue
        matches = [i for i, r in enumerate(records) if normalize_id(parse(r)[0]) == key]
        assert len(matches) == 1, f"{action} {key}: expected 1 record, found {len(matches)}"
        if action == "delete":
            records.pop(matches[0])
        else:
            row = parse(records[matches[0]])
            for column, value in values.items():
                row[columns.index(column)] = value
            records[matches[0]] = to_record(row)
    return records


def main() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    previous = {path.name: read_records(path) for path in sorted(RAW_DIR.glob("rrhh_*.csv"))}
    for batch_date in BATCH_DATES:
        current = {}
        for file_name, (header, records) in previous.items():
            table = file_name[len("rrhh_"):-len(".csv")]
            operations = [op for op in CHANGES.get(batch_date, []) if op[1] == table]
            current[file_name] = (header, apply_changes(header, records, operations))
        batch_dir = OUT_DIR / f"batch_date={batch_date}"
        batch_dir.mkdir(parents=True)
        for file_name, (header, records) in current.items():
            (batch_dir / file_name).write_text("\n".join([header] + records), encoding="utf-8")
        changed = len(CHANGES.get(batch_date, []))
        print(f"batch_date={batch_date}: {len(current)} files, {changed} changes")
        previous = current


if __name__ == "__main__":
    main()
