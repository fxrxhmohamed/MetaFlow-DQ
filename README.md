# MetaFlow-DQ

Metadata-driven data quality and ETL for the Egypt banking transactions
dataset.

## Data quality pipeline

Run from the project root with the virtual environment's Python.

```bash
# 1. Ingest: immutable copy, ingestion id, manifest, record ids
python services/ingestion-service/src/ingest.py \
    --input data/raw/banking/banking_transactions.csv

# 2. Profile the ingested copy (profile.json next to the manifest)
python services/metadata-service/src/profiler.py --ingestion latest

# 3-4. Validate: valid records, quarantine round 1 with one error line
#      per failed rule, remediation audit
python services/dq-service/src/dq_engine.py --ingestion latest

# 5. Manual correction: export the template, fill in new_value / action
#    (FIX or DISCARD) / reason, import it
python services/dq-service/src/quarantine.py template
python services/dq-service/src/quarantine.py import \
    --file data/quarantine/banking_transactions/<ingestion id>/round_1/correction_template.csv

# 6. Re-validate the corrections against the whole dataset; repeat 5-6
#    for the next round
python services/dq-service/src/quarantine.py revalidate
python services/dq-service/src/quarantine.py status
```

Every command takes `--run <ingestion id>` (default `latest`) and
`--help`.

### Where things are

| Path | Content |
|---|---|
| `data/raw/<dataset>/<ING id>/` | read-only source copy, `manifest.json`, `profile.json` |
| `data/validated/<dataset>/<ING id>/valid.csv` | records that passed validation |
| `data/quarantine/<dataset>/<ING id>/quarantine_status.csv` | status of every quarantined record: OPEN, RESOLVED, DISCARDED, MANUAL_REVIEW_REQUIRED |
| `data/quarantine/<dataset>/<ING id>/round_<n>/` | open records, their errors, the correction template |
| `data/corrected/<dataset>/<ING id>/round_<n>/` | imported corrections, refused lines, records resolved in that round |
| `data/quality/<dataset>/<ING id>/` | `dq_result.json` and remediation audit per round, `correction_log.csv` |
| `config/metadata/schema_registry.yaml` | what the data should look like |
| `config/rules/quality_rules.yaml` | how strictly it is enforced and repaired |
| `config/reference/` | lookup files for referential checks |

Records are identified by `<ingestion id>_ROW_<row number>` from ingestion
to the final dataset.
