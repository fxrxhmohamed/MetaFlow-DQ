USE ROLE METAFLOW_ROLE;
USE DATABASE METAFLOW_DQ;

-- ---------------------------------------------------------------------------
-- RAW: as published, plus lineage columns. Grain = one source row per batch.
-- trip_id is only unique inside one quarterly file, so lineage (_year, _quarter)
-- is part of the key everywhere downstream.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.TRIPS (
    TRIP_ID              VARCHAR,
    DURATION             VARCHAR,
    START_TIME           VARCHAR,
    END_TIME             VARCHAR,
    START_STATION        VARCHAR,
    START_LAT            VARCHAR,
    START_LON            VARCHAR,
    END_STATION          VARCHAR,
    END_LAT              VARCHAR,
    END_LON              VARCHAR,
    BIKE_ID              VARCHAR,
    PLAN_DURATION        VARCHAR,
    TRIP_ROUTE_CATEGORY  VARCHAR,
    PASSHOLDER_TYPE      VARCHAR,
    BIKE_TYPE            VARCHAR,
    _YEAR                NUMBER(4)     NOT NULL,
    _QUARTER             NUMBER(1)     NOT NULL,
    _BATCH_ID            VARCHAR       NOT NULL,
    _SOURCE_FILE         VARCHAR       NOT NULL,
    _FILE_ROW_NUMBER     NUMBER        NOT NULL,
    _FILE_SHA256         VARCHAR,
    _LOADED_AT           TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (_YEAR, _QUARTER);

CREATE TABLE IF NOT EXISTS RAW.STATIONS (
    KIOSK_ID         VARCHAR,
    KIOSK_NAME       VARCHAR,
    GO_LIVE_DATE     VARCHAR,
    REGION           VARCHAR,
    STATUS           VARCHAR,
    LATITUDE         VARCHAR,
    LONGITUDE        VARCHAR,
    _SNAPSHOT_DATE   DATE          NOT NULL,   -- date in the published file name
    _BATCH_ID        VARCHAR       NOT NULL,
    _SOURCE_FILE     VARCHAR       NOT NULL,
    _FILE_ROW_NUMBER NUMBER        NOT NULL,
    _LOADED_AT       TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- One row per poll of the live GeoJSON feed; features stay nested until staging.
CREATE TABLE IF NOT EXISTS RAW.STATION_STATUS_GEOJSON (
    SNAPSHOT_TS   TIMESTAMP_TZ NOT NULL,
    SOURCE_URL    VARCHAR,
    PAYLOAD       VARIANT      NOT NULL,
    _SOURCE_FILE  VARCHAR      NOT NULL,
    _LOADED_AT    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.STATION_STATUS_GBFS (
    SNAPSHOT_TS   TIMESTAMP_TZ NOT NULL,
    SOURCE_URL    VARCHAR,
    PAYLOAD       VARIANT      NOT NULL,
    _SOURCE_FILE  VARCHAR      NOT NULL,
    _LOADED_AT    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- QUARANTINE: rows the DQ engine rejected, with every rule they failed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS QUARANTINE.TRIPS_REJECTED (
    TRIP_KEY            VARCHAR       NOT NULL,   -- md5(year, quarter, trip_id, row)
    RAW_RECORD          VARIANT       NOT NULL,   -- the RAW.TRIPS row as published
    FAILED_RULES        ARRAY         NOT NULL,   -- rule ids from config/rules/quality_rules.yaml
    SEVERITY            VARCHAR       NOT NULL,   -- reject | warn
    REMEDIATION_STATUS  VARCHAR       NOT NULL DEFAULT 'pending', -- pending|auto_fixed|manual_fixed|discarded
    REMEDIATED_RECORD   VARIANT,
    _BATCH_ID           VARCHAR       NOT NULL,
    _DETECTED_AT        TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _RESOLVED_AT        TIMESTAMP_TZ
);

-- ---------------------------------------------------------------------------
-- AUDIT: one row per file load, read by Splunk / Power BI and by the loader
-- itself to skip files whose checksum is already loaded.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.LOAD_LOG (
    LOAD_ID        VARCHAR       NOT NULL DEFAULT UUID_STRING(),
    DATASET        VARCHAR       NOT NULL,
    BATCH_ID       VARCHAR       NOT NULL,
    SOURCE_FILE    VARCHAR       NOT NULL,
    FILE_SHA256    VARCHAR,
    TARGET_TABLE   VARCHAR       NOT NULL,
    ROWS_IN_FILE   NUMBER,                      -- from the ingestion manifest
    ROWS_PARSED    NUMBER,
    ROWS_LOADED    NUMBER,
    ERRORS_SEEN    NUMBER,
    FIRST_ERROR    VARCHAR,
    STATUS         VARCHAR       NOT NULL,      -- LOADED | PARTIALLY_LOADED | LOAD_FAILED | SKIPPED
    STARTED_AT     TIMESTAMP_TZ  NOT NULL,
    FINISHED_AT    TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS AUDIT.DQ_RESULTS (
    RUN_ID        VARCHAR       NOT NULL,
    DATASET       VARCHAR       NOT NULL,
    BATCH_ID      VARCHAR,
    RULE_ID       VARCHAR       NOT NULL,
    SEVERITY      VARCHAR       NOT NULL,
    ROWS_CHECKED  NUMBER        NOT NULL,
    ROWS_FAILED   NUMBER        NOT NULL,
    SAMPLE        VARIANT,
    CHECKED_AT    TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
