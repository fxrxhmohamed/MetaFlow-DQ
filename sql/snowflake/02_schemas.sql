-- Layers. RAW is written only by loading-service, STAGING/INTERMEDIATE/MARTS
-- only by dbt, QUARANTINE by dq-service / remediation-service, AUDIT by everyone.
USE ROLE METAFLOW_ROLE;
USE DATABASE METAFLOW_DQ;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS QUARANTINE;
CREATE SCHEMA IF NOT EXISTS AUDIT;

-- Every column lands as text: a bad value in one row must never fail a whole file.
-- Typing happens in dbt staging with TRY_ functions, where failures become DQ findings.
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_CSV_TEXT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', 'NULL', 'null')
  ENCODING = 'UTF8'
  REPLACE_INVALID_CHARACTERS = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE FILE FORMAT IF NOT EXISTS RAW.FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

-- Internal stage mirroring the landing-zone layout (trips/year=YYYY/quarter=Q/batch_id=...).
CREATE STAGE IF NOT EXISTS RAW.LANDING_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Files PUT by loading-service from the ingestion landing zone';
