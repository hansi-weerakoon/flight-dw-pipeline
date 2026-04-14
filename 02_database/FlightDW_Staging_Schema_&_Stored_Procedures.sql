-- ============================================================
-- STAGING DATABASE — FlightDW_Staging
-- Mirrors source structure exactly — no transformations.
-- SSIS extracts here first, then loads DW from staging.
-- ============================================================
USE master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'FlightDW_Staging')
    DROP DATABASE FlightDW_Staging;
GO
CREATE DATABASE FlightDW_Staging;
GO
USE FlightDW_Staging;
GO

CREATE TABLE dbo.stg_FlightOperation (
    operation_id        INT, flight_id       INT,
    fl_date             DATE, year            SMALLINT,
    month               TINYINT, day_of_month TINYINT,
    day_of_week         TINYINT, dep_time     FLOAT,
    dep_delay           FLOAT, taxi_out       FLOAT,
    wheels_off          FLOAT, wheels_on      FLOAT,
    taxi_in             FLOAT, arr_time       FLOAT,
    arr_delay           FLOAT, actual_elapsed_time FLOAT,
    air_time            FLOAT, cancelled      TINYINT,
    cancellation_code   CHAR(3), diverted     TINYINT
);
GO

--ALTER TABLE dbo.stg_FlightOperation
--ALTER COLUMN cancellation_code CHAR(3);

--
USE FlightDW_Staging;
GO

IF OBJECT_ID('dbo.usp_Prepare_FlightOperations', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Prepare_FlightOperations;
GO

CREATE PROCEDURE dbo.usp_Prepare_FlightOperations
AS
BEGIN
    SET NOCOUNT ON;

    -- ROOT CAUSE FIX:
    -- Dim_CancellationReason.cancellation_code is now VARCHAR(3).
    -- This procedure outputs VARCHAR(3) to match exactly.
    --
    -- Operated flights  (cancelled = 0) → 'N/A'  VARCHAR(3) length 3
    -- Cancelled flights (cancelled = 1) → 'A'/'B'/'C'/'D'  VARCHAR(3) length 1
    --
    -- LEFT(RTRIM(cancellation_code), 1) is used instead of just RTRIM:
    -- Defensively strips both trailing spaces AND any source padding
    -- that CHAR columns introduce, then takes only the first character.
    -- This guarantees the output is always a clean single letter.
    --
    -- The Merge Join with stg_DelayRecord is done here in SQL Server,
    -- not in SSIS, to avoid Sort + Merge Join buffer exhaustion
    -- which caused partial loads of only ~1.4M rows.

    SELECT
        -- ── Identity ──────────────────────────────────────────────────────
        f.operation_id,
        f.flight_id,
        f.fl_date,
        f.year,
        f.month,
        f.day_of_month,
        f.day_of_week,

        -- ── Actual timing (NULL for cancelled flights — correct) ──────────
        f.dep_time,
        f.dep_delay,
        f.taxi_out,
        f.wheels_off,
        f.wheels_on,
        f.taxi_in,
        f.arr_time,
        f.arr_delay,
        f.actual_elapsed_time,
        f.air_time,

        -- ── Status ────────────────────────────────────────────────────────
        f.cancelled,
        f.diverted,

        -- ── Cancellation code — VARCHAR(3), no padding ────────────────────
        -- CAST to VARCHAR(3) prevents any implicit CHAR padding.
        -- LEFT(..., 1) on cancelled codes strips any source-level padding
        -- so 'A  ' from a CHAR source becomes 'A' before output.
        -- 'N/A' is exactly 3 chars — no trimming needed.
        CAST(
            CASE
                WHEN f.cancelled = 0
                    THEN 'N/A'
                ELSE
                    LEFT(RTRIM(LTRIM(
                        ISNULL(f.cancellation_code, 'A')
                    )), 1)
            END
        AS VARCHAR(3)) AS cancellation_code_clean,

        -- ── Delay components — ISNULL to zero for non-delayed flights ─────
        -- NULL comes from the LEFT JOIN when no delay record exists.
        -- Storing 0 is correct: the flight had no attributable delay.
        ISNULL(d.carrier_delay,         0) AS carrier_delay,
        ISNULL(d.weather_delay,         0) AS weather_delay,
        ISNULL(d.nas_delay,             0) AS nas_delay,
        ISNULL(d.security_delay,        0) AS security_delay,
        ISNULL(d.late_aircraft_delay,   0) AS late_aircraft_delay,

        -- ── Total delay — computed in SQL for reliability ─────────────────
        ISNULL(d.carrier_delay,         0)
        + ISNULL(d.weather_delay,       0)
        + ISNULL(d.nas_delay,           0)
        + ISNULL(d.security_delay,      0)
        + ISNULL(d.late_aircraft_delay, 0) AS total_delay_minutes

    FROM      dbo.stg_FlightOperation  f
    LEFT JOIN dbo.stg_DelayRecord      d
           ON f.operation_id = d.operation_id;

END;
GO
--

USE FlightDW_Staging;
GO

-- ── Validation 1: procedure output codes match dimension exactly ──────────
-- This simulates the SSIS Lookup join.
-- unmatched_rows MUST be 0 before running SSIS.
SELECT
    src.cancellation_code_clean,
    src.source_rows,
    dim.cancellation_key            AS matched_key,
    dim.cancellation_code           AS dim_code,
    CASE
        WHEN dim.cancellation_key IS NULL
        THEN 'FAIL — no match in dimension'
        ELSE 'PASS'
    END                             AS lookup_result
FROM (
    SELECT
        CAST(
            CASE
                WHEN cancelled = 0 THEN 'N/A'
                ELSE LEFT(RTRIM(LTRIM(ISNULL(cancellation_code,'A'))), 1)
            END
        AS VARCHAR(3))              AS cancellation_code_clean,
        COUNT(*)                    AS source_rows
    FROM dbo.stg_FlightOperation
    GROUP BY
        CAST(
            CASE
                WHEN cancelled = 0 THEN 'N/A'
                ELSE LEFT(RTRIM(LTRIM(ISNULL(cancellation_code,'A'))), 1)
            END
        AS VARCHAR(3))
) src
LEFT JOIN FlightDW.dbo.Dim_CancellationReason dim
       ON src.cancellation_code_clean = dim.cancellation_code
ORDER BY src.cancellation_code_clean;
GO
-- Expected:
-- N/A  → matched_key = 0  → PASS
-- A    → matched_key = 1  → PASS
-- B    → matched_key = 2  → PASS
-- C    → matched_key = 3  → PASS
-- D    → matched_key = 4  → PASS
-- unmatched_rows = 0 before proceeding

-- ── Validation 2: expected row totals ────────────────────────────────────
SELECT
    COUNT(*)                                        AS total_stg_rows,
    SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS operated_rows,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_rows
FROM dbo.stg_FlightOperation;
GO
-- Note these numbers — they must match post-run fact table counts exactly

CREATE TABLE dbo.stg_DelayRecord (
    delay_id            INT, operation_id    INT,
    carrier_delay       SMALLINT, weather_delay SMALLINT,
    nas_delay           SMALLINT, security_delay SMALLINT,
    late_aircraft_delay SMALLINT
);
GO
CREATE TABLE dbo.stg_Airline (
    carrier_code        CHAR(2), carrier_name VARCHAR(100)
);
GO
CREATE TABLE dbo.stg_Airport (
    airport_code        CHAR(3), city_name    VARCHAR(100),
    state_name          VARCHAR(100)
);
GO
CREATE TABLE dbo.stg_Flight (
    flight_id           INT, carrier_code    CHAR(2),
    flight_number       INT, origin_code     CHAR(3),
    dest_code           CHAR(3), crs_dep_time SMALLINT,
    crs_arr_time        SMALLINT, crs_elapsed_time FLOAT,
    distance            FLOAT
);
GO
CREATE TABLE dbo.stg_CancellationReason (
    cancellation_code       CHAR(1),
    reason_description      VARCHAR(200),
    responsible_party       VARCHAR(50),
    compensation_eligibility VARCHAR(200)
);
ALTER TABLE stg_CancellationReason
ALTER COLUMN cancellation_code CHAR(3);
GO
CREATE TABLE dbo.stg_RegionMapping (
    state_name          VARCHAR(100),
    region              VARCHAR(20),
    region_code         CHAR(2)
);
GO
-- Error table — receives rows rejected by SSIS Conditional Split
CREATE TABLE dbo.stg_ErrorRows (
    source_table        VARCHAR(50),
    error_code          INT,
    error_description   VARCHAR(500),
    raw_row             NVARCHAR(MAX),
    captured_at         DATETIME DEFAULT GETDATE()
);
GO

PRINT 'FlightDW and FlightDW_Staging schemas created successfully.';

-- Landing zone for the completion CSV
-- Mirrors the CSV structure exactly — no transformation here
IF OBJECT_ID('dbo.stg_AccumCompletion') IS NOT NULL
    DROP TABLE dbo.stg_AccumCompletion;

CREATE TABLE dbo.stg_AccumCompletion (
    operation_id            INT         NOT NULL,
    accm_txn_complete_time  DATETIME    NOT NULL
);
GO
CREATE NONCLUSTERED INDEX IX_stg_Accum_OpId
    ON dbo.stg_AccumCompletion (operation_id);
GO

-- Error table for operation_ids that don't exist in the fact table
IF OBJECT_ID('dbo.stg_AccumErrors') IS NOT NULL
    DROP TABLE dbo.stg_AccumErrors;

CREATE TABLE dbo.stg_AccumErrors (
    operation_id            INT         NOT NULL,
    accm_txn_complete_time  DATETIME    NOT NULL,
    error_reason            VARCHAR(200) NOT NULL,
    captured_at             DATETIME    NOT NULL DEFAULT GETDATE()
);
GO

PRINT 'Staging tables for Task 6 created successfully.';