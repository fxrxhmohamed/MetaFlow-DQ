# MetaFlow-DQ

Metadata-driven data quality and ETL for the Egypt banking transactions
dataset.

What the data should look like (columns, types, allowed values, ranges,
relationships) is described in a schema registry, and how strictly it is
enforced and repaired in a rules file. The pipeline lands each source file
unchanged, validates and repairs it, holds back the records it cannot
trust, lets people correct them (in the browser or with a file), merges
the result into a clean dataset and reports data-quality metrics for
Power BI. Every record keeps one id from ingestion to the final dataset.

```
source CSV
  -> ingest       read-only copy, ingestion id, manifest
  -> profile      profile.json
  -> validate     valid records | quarantine (+ why) | repair audit
  -> correct      correction UI or a correction file   <- people
  -> revalidate   fixed records resolved, the rest back to quarantine
  -> merge        final clean.csv (valid + resolved)
  -> metrics      dq_run, dq_rule_result, dq_quarantine, dq_correction
```

## Setup

Python 3.13 (what CI tests with). From the project root:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      Linux/macOS: source .venv/bin/activate
python -m pip install -r services/dq-service/requirements.txt \
                      -r services/metadata-service/requirements.txt
```

The data is not in the repository. Put the source file at
`data/raw/banking/banking_transactions.csv`; everything the pipeline
writes goes under `data/` (ignored by git).

## Run the pipeline

Every step is one command of `dq.py`, run from the project root. Each
calls the service script that does the work (`python dq.py --help`).

```bash
# 1. Ingest: read-only copy, ingestion id, manifest, record ids
#    (the last output line is the new ingestion id)
python dq.py ingest --input data/raw/banking/banking_transactions.csv

# 2. Profile the ingested copy (profile.json next to the manifest)
python dq.py profile

# 3-4. Validate: valid records, quarantine round 1 with one error line per
#      failed rule, audit of every automatic repair
python dq.py validate

# 5. Correct: export the template, fill in new_value / action / reason,
#    import it (or use the correction UI below)
python dq.py export-template --output fixes.csv
python dq.py import-corrections --file fixes.csv

# 6. Re-validate the corrections against the whole dataset; repeat 5-6
python dq.py revalidate
python dq.py status

# 7. Merge into the clean dataset: valid + resolved, one row per record
python dq.py merge

# 8. Data-quality metric tables for PostgreSQL / Power BI (all runs)
python dq.py metrics

# Steps 1, 2, 3-4, 7 and 8 in one go
python dq.py pipeline --input data/raw/banking/banking_transactions.csv
```

Every step works on the latest ingestion unless given `--run <ingestion
id>`, and takes `--raw-root`, `--output-root` and `--rules` to use other
folders. `validate --force` starts a run over (it removes that run's
quarantine rounds and corrections).

### Correction file

One line per field to check. Fill in only these columns:

| To... | `new_value` | `action` |
|---|---|---|
| correct a field | the right value | empty or `FIX` |
| empty a field on purpose | `__NULL__` | empty or `FIX` |
| remove the whole record | empty | `DISCARD` |
| accept the automatic repairs of a record held for review | empty | `ACCEPT` |
| leave it for later | empty | empty |

`reason` is optional and kept in the correction log. Lines that are not
filled in are ignored; lines that cannot be used are listed with the
reason in `import_rejections.csv`.

## Correction UI

Steps 5-6 in the browser, for people who do not know the rules:

```bash
python dq.py ui
```

- **Fix records**: problems in plain words ("Missing email", "Duplicate
  transaction ID"). Missing values that other records of the same
  customer or account already have are suggested, and a quick fix adds
  every record they complete. Other records are shown one at a time,
  with inputs that only accept valid values, a side-by-side view for
  duplicates and a Remove button.
- **Many values changed automatically**: a record repaired automatically
  3 or more times (rule `DQ-REV-001`) is held until someone accepts the
  changes, corrects one of them or removes the record. Records with no
  other problem can be accepted in bulk after checking examples.
- **My changes**: nothing is written until you send. Sending runs the
  same import and re-validation as the commands, so the result is
  identical to uploading the same fixes as a file.
- **Progress**: fixed, removed, waiting, needing an expert.
  **Publish to clean data** runs the merge and the metrics; fixed records
  reach `clean.csv` only when published (e.g. once a day).
- **Upload a file**: download the list of problems, fill it in (e.g. in
  Excel), upload it; it is checked before anything is sent.

The wording and suggestions come from the schema registry
(`correction_assist.py`), so they follow the dataset without code changes.

## Airflow

`airflow/dags/data_quality.py`; every task runs one `dq.py` step.

| DAG | Tasks | When |
|---|---|---|
| `dq_ingest_validate` | ingest, profile, validate, merge, metrics | a new file; param `input` |
| `dq_apply_corrections` | import-corrections, revalidate, merge, metrics | a filled-in correction file; params `run`, `file` |
| `dq_publish` | merge, metrics | every day at 18:00 (records fixed in the UI) |

Locally with Docker (Airflow 3, no login, for development only):

```bash
docker compose --progress plain build    # first time: several minutes
docker compose up -d                     # http://localhost:8080
docker compose down                      # stop; data stays in data/
```

The project folder is mounted at `/opt/metaflow`, so the DAGs use the same
`data/` folder as the commands and the UI. The pipeline runs in its own
virtual environment inside the image. Elsewhere, Airflow workers need
`METAFLOW_HOME` (the project folder) and `METAFLOW_PYTHON` (a Python with
the requirements above).

## Tests and CI

`tests/data/banking_sample.csv` has 50 rows: clean ones and one example of
each defect, listed in `tests/data/expected_defects.csv` with the rule
that must catch it and the expected outcome (repaired, flagged,
quarantined or removed). `tests/test_pipeline.py` runs every `dq.py` step
on it, including a correction round, the merge, the metrics and the UI,
and checks each defect.

```bash
python -m pip install pytest
python -m pytest tests
```

GitHub Actions runs the same tests on every push and pull request
(`.github/workflows/data-quality.yml`). `tests/data/build_sample.py`
rebuilds the dataset from a validated run.

## Configuration

| File | Content |
|---|---|
| `config/metadata/schema_registry.yaml` | columns, types, allowed values, ranges, entities and relationships |
| `config/rules/quality_rules.yaml` | severities, automatic repairs, business and cross-row rules |
| `config/reference/` | lookup files for referential checks (banks, branches) |

Severity decides what happens to a record that still fails after
repair: `critical` -> quarantine, `warning` -> accepted and flagged,
`info` -> counted only.

## Data quality metrics

`python dq.py metrics` rebuilds four tables in `data/metrics/<dataset>/`:

| Table | One row per |
|---|---|
| `dq_run` | run and round: processed, valid, auto-fixed, quarantined, resolved, manually fixed, confirmed after review, discarded, unresolved, DQ scores |
| `dq_rule_result` | run, round and rule: checked, failed, repaired, unresolved |
| `dq_quarantine` | status change of a quarantined record |
| `dq_correction` | automatic repair or manual correction, with its outcome |

- initial DQ score = valid / processed
- final DQ score = (valid + resolved) / processed
- records with issues = auto-fixed + quarantined; the quarantined ones end
  up manually fixed, confirmed after review, resolved otherwise,
  discarded or unresolved

The latest round of a run gives the funnel directly, e.g. 350 with
issues -> 200 auto-fixed -> 130 manually fixed -> 20 unresolved.

## Where things are

| Path | Content |
|---|---|
| `dq.py` | one command per pipeline step |
| `services/ingestion-service/` | ingestion (`ingest.py`) |
| `services/metadata-service/` | profiler (`profiler.py`) |
| `services/dq-service/` | validation engine, quarantine and re-validation, merge, metrics, correction UI |
| `airflow/` | DAGs and the Airflow image |
| `tests/` | test dataset and end-to-end tests |
| `data/raw/<dataset>/<ING id>/` | read-only source copy, `manifest.json`, `profile.json` |
| `data/validated/<dataset>/<ING id>/valid.csv` | records that passed validation |
| `data/quarantine/<dataset>/<ING id>/` | status and status history of every quarantined record; open records and their errors per round |
| `data/corrected/<dataset>/<ING id>/round_<n>/` | imported corrections, refused lines, records resolved in that round |
| `data/quality/<dataset>/<ING id>/` | `dq_result.json` and repair audit per round, `correction_log.csv` |
| `data/final/<dataset>/<ING id>/clean.csv` | clean dataset: valid + resolved records |
| `data/metrics/<dataset>/` | the metric tables |

Records are identified by `<ingestion id>_ROW_<row number>` from ingestion
to the final dataset.

`clean.csv` holds about a million rows, close to Excel's limit of
1,048,576; open it in Power BI or pandas instead, and close it in any
program before `merge` (an open file cannot be overwritten on Windows).
