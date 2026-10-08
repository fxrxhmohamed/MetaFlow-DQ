USE ROLE METAFLOW_ROLE;
USE DATABASE METAFLOW_DQ;

-- Live status feed exploded to one row per station per snapshot.
CREATE OR REPLACE VIEW RAW.V_STATION_STATUS_GEOJSON_FLAT AS
SELECT
    s.SNAPSHOT_TS,
    f.value:properties:kioskId::NUMBER                AS KIOSK_ID,
    f.value:properties:name::VARCHAR                  AS NAME,
    f.value:properties:kioskPublicStatus::VARCHAR     AS PUBLIC_STATUS,
    f.value:properties:kioskStatus::VARCHAR           AS KIOSK_STATUS,
    f.value:properties:kioskConnectionStatus::VARCHAR AS CONNECTION_STATUS,
    f.value:properties:kioskUnresponsiveTime::TIMESTAMP_TZ AS UNRESPONSIVE_SINCE,
    f.value:properties:totalDocks::NUMBER             AS TOTAL_DOCKS,
    f.value:properties:docksAvailable::NUMBER         AS DOCKS_AVAILABLE,
    f.value:properties:bikesAvailable::NUMBER         AS BIKES_AVAILABLE,
    f.value:properties:classicBikesAvailable::NUMBER  AS CLASSIC_BIKES_AVAILABLE,
    f.value:properties:smartBikesAvailable::NUMBER    AS SMART_BIKES_AVAILABLE,
    f.value:properties:electricBikesAvailable::NUMBER AS ELECTRIC_BIKES_AVAILABLE,
    f.value:properties:trikesAvailable::NUMBER        AS TRIKES_AVAILABLE,
    f.value:properties:isVirtual::BOOLEAN             AS IS_VIRTUAL,
    f.value:properties:isEventBased::BOOLEAN          AS IS_EVENT_BASED,
    f.value:properties:isArchived::BOOLEAN            AS IS_ARCHIVED,
    f.value:properties:addressStreet::VARCHAR         AS ADDRESS_STREET,
    f.value:properties:addressCity::VARCHAR           AS ADDRESS_CITY,
    f.value:properties:addressZipCode::VARCHAR        AS ADDRESS_ZIP,
    f.value:properties:latitude::FLOAT                AS LATITUDE,
    f.value:properties:longitude::FLOAT               AS LONGITUDE,
    f.value:properties:clientVersion::VARCHAR         AS CLIENT_VERSION
FROM RAW.STATION_STATUS_GEOJSON s,
     LATERAL FLATTEN(INPUT => s.PAYLOAD:features) f;

CREATE OR REPLACE VIEW AUDIT.V_LOAD_SUMMARY AS
SELECT
    DATASET,
    BATCH_ID,
    SOURCE_FILE,
    ROWS_IN_FILE,
    ROWS_LOADED,
    ROWS_IN_FILE - ROWS_LOADED AS ROWS_MISSING,
    ERRORS_SEEN,
    STATUS,
    DATEDIFF('second', STARTED_AT, FINISHED_AT) AS SECONDS,
    FINISHED_AT
FROM AUDIT.LOAD_LOG
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATASET, SOURCE_FILE ORDER BY STARTED_AT DESC) = 1;
