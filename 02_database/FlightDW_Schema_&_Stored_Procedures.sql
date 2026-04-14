USE FlightDW;
GO

-- ============================================================
-- DIM_DATE
-- Pre-populated calendar dimension covering all of 2024.
-- Hierarchy: Year → Quarter → Month → Week → Day
-- Reason: date attributes must be dimension columns, not
-- derived at query time. SSAS hierarchies require explicit
-- columns for each level.
-- ============================================================

CREATE TABLE dbo.Dim_Date (
    date_key            INT             NOT NULL,   -- YYYYMMDD format, e.g. 20240115
    full_date           DATE            NOT NULL,
    year                SMALLINT        NOT NULL,
    quarter             TINYINT         NOT NULL,   -- 1–4
    quarter_name        VARCHAR(6)      NOT NULL,   -- 'Q1'…'Q4'
    month               TINYINT         NOT NULL,   -- 1–12
    month_name          VARCHAR(10)     NOT NULL,   -- 'January'…
    month_year          VARCHAR(8)      NOT NULL,   -- 'Jan 2024'
    week_of_year        TINYINT         NOT NULL,   -- ISO week 1–53
    day_of_month        TINYINT         NOT NULL,   -- 1–31
    day_of_week         TINYINT         NOT NULL,   -- 1=Monday…7=Sunday (ISO)
    day_name            VARCHAR(10)     NOT NULL,   -- 'Monday'…
    is_weekend          BIT             NOT NULL,   -- 1 if Saturday or Sunday
    is_public_holiday   BIT             NOT NULL    -- 1 for US federal holidays
        DEFAULT 0,
    CONSTRAINT PK_Dim_Date PRIMARY KEY (date_key)
);
GO

-- Populate Dim_Date for full year 2024
-- (366 rows — 2024 is a leap year)
DECLARE @d DATE = '2024-01-01';
DECLARE @end DATE = '2024-12-31';
WHILE @d <= @end
BEGIN
    INSERT INTO dbo.Dim_Date (
        date_key, full_date, year, quarter, quarter_name,
        month, month_name, month_year, week_of_year,
        day_of_month, day_of_week, day_name, is_weekend, is_public_holiday
    )
    VALUES (
        CAST(FORMAT(@d, 'yyyyMMdd') AS INT),
        @d,
        YEAR(@d),
        DATEPART(QUARTER, @d),
        'Q' + CAST(DATEPART(QUARTER, @d) AS VARCHAR(1)),
        MONTH(@d),
        DATENAME(MONTH, @d),
        LEFT(DATENAME(MONTH, @d), 3) + ' ' + CAST(YEAR(@d) AS VARCHAR(4)),
        DATEPART(ISO_WEEK, @d),
        DAY(@d),
        DATEPART(WEEKDAY, @d),      -- SQL Server: 1=Sunday by default
        DATENAME(WEEKDAY, @d),
        CASE WHEN DATEPART(WEEKDAY, @d) IN (1, 7) THEN 1 ELSE 0 END,
        0   -- holidays updated separately
    );
    SET @d = DATEADD(DAY, 1, @d);
END;
GO

-- Mark US federal holidays in 2024
-- Source: US Office of Personnel Management federal holiday schedule
UPDATE dbo.Dim_Date SET is_public_holiday = 1
WHERE date_key IN (
    20240101,   -- New Year's Day
    20240115,   -- Martin Luther King Jr. Day
    20240219,   -- Presidents' Day
    20240527,   -- Memorial Day
    20240619,   -- Juneteenth
    20240704,   -- Independence Day
    20240902,   -- Labor Day
    20241014,   -- Columbus Day
    20241111,   -- Veterans Day
    20241128,   -- Thanksgiving Day
    20241225    -- Christmas Day
);
GO


-- ============================================================
-- DIM_AIRLINE
-- SCD Type 2 — tracks historical changes to carrier names.
-- Reason for SCD2: airlines merge, rebrand, and change status.
-- Spirit Airlines (NK) entered Chapter 11 bankruptcy Nov 2024.
-- SCD2 ensures that operations before/after any name change
-- are attributed to the correct historical version of the carrier.
--
-- Natural key: carrier_code (IATA designator)
-- Surrogate key: airline_key (integer, SSIS Slowly Changing
--   Dimension component generates new key on Type 2 change)
-- ============================================================
--ALTER TABLE dbo.Fact_FlightOperation
--DROP CONSTRAINT FK_Fact_Airline;

--DROP TABLE dbo.Dim_Airline;

--ALTER TABLE dbo.Fact_FlightOperation
--ADD CONSTRAINT FK_Fact_Airline
--FOREIGN KEY (airline_key) REFERENCES dbo.Dim_Airline(airline_key);

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Dim_Airline';

CREATE TABLE dbo.Dim_Airline (
    airline_key         INT             IDENTITY(1,1) PRIMARY KEY,
    carrier_code        CHAR(2)         NOT NULL,   -- IATA natural key
    carrier_name        VARCHAR(100)    NOT NULL,   -- SCD Type 2 tracking columns
    scd_effective_date  DATETIME             NOT NULL    DEFAULT '1900-01-01',
    scd_expiry_date     DATETIME             NOT NULL    DEFAULT '9999-12-31',
    is_current_record   BIT             NOT NULL    DEFAULT 1
);
GO

-- Index on natural key for fast SSIS Lookup performance
CREATE NONCLUSTERED INDEX IX_Dim_Airline_CarrierCode
    ON dbo.Dim_Airline (carrier_code)
    INCLUDE (airline_key, carrier_name)
    WHERE is_current_record = 1;
GO

-- ============================================================
-- DIM_AIRPORT
-- Covers both origin and destination airports — role-playing
-- dimension. One physical table, two FK references in the fact.
-- Hierarchy: Region → State → City → Airport (IATA code)
-- Region column is enriched from the Excel RegionMapping source
-- — it does not exist in the raw BTS data. This is the concrete
-- business value of the Excel source type in the ETL pipeline.
-- ============================================================
CREATE TABLE dbo.Dim_Airport (
    airport_key         INT             NOT NULL    IDENTITY(1,1),
    airport_code        CHAR(3)         NOT NULL,   -- IATA natural key
    city_name           VARCHAR(100)    NOT NULL,
    state_name          VARCHAR(100)    NOT NULL,
    region              VARCHAR(20)     NOT NULL,   -- Enriched from Excel
    region_code         CHAR(2)         NOT NULL,   -- NE/MW/SO/WE/TR
    CONSTRAINT PK_Dim_Airport PRIMARY KEY (airport_key)
);
GO

CREATE NONCLUSTERED INDEX IX_Dim_Airport_Code
    ON dbo.Dim_Airport (airport_code)
    INCLUDE (airport_key, city_name, state_name, region);
GO

CREATE PROCEDURE dbo.Upsert_Dim_Airport
    @airport_code CHAR(3),
    @city_name    VARCHAR(100),
    @state_name   VARCHAR(100),
    @region       VARCHAR(20),
    @region_code  CHAR(2)
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM dbo.Dim_Airport WHERE airport_code = @airport_code)
    BEGIN
        -- Type 1 SCD: overwrite with latest values
        UPDATE dbo.Dim_Airport
        SET city_name   = @city_name,
            state_name  = @state_name,
            region      = @region,
            region_code = @region_code
        WHERE airport_code = @airport_code;
    END
    ELSE
    BEGIN
        INSERT INTO dbo.Dim_Airport (airport_code, city_name, state_name, region, region_code)
        VALUES (@airport_code, @city_name, @state_name, @region, @region_code);
    END
END;
GO

-- ============================================================
-- DIM_ROUTE
-- Represents a scheduled service definition — the timetable
-- entry for a carrier + flight number + origin + destination
-- at a specific scheduled departure time.
-- Reason for separate dimension: route attributes (distance,
-- scheduled times) are fixed properties of the service, not
-- measurements of a daily operation. Storing them here avoids
-- repeating distance and scheduled times across 365 operation
-- rows for the same route, and enables route-level analysis
-- independent of operational performance.
-- distance_band is a derived enrichment: Short (<500mi),
-- Medium (500–1500mi), Long (>1500mi) — adds a useful
-- analytic grouping not present in the source data.
-- ============================================================
CREATE TABLE dbo.Dim_Route (
    route_key           INT             NOT NULL    IDENTITY(1,1),
    flight_id           INT             NOT NULL,   -- Natural key from source
    carrier_code        CHAR(2)         NOT NULL,
    flight_number       INT             NOT NULL,
    origin_code         CHAR(3)         NOT NULL,
    dest_code           CHAR(3)         NOT NULL,
    crs_dep_time        SMALLINT        NOT NULL,   -- Scheduled dep HHMM
    crs_arr_time        SMALLINT        NOT NULL,   -- Scheduled arr HHMM
    crs_elapsed_time    FLOAT           NULL,       -- Scheduled duration mins
    distance            FLOAT           NOT NULL,   -- Route distance miles
    distance_band       VARCHAR(10)     NOT NULL,   -- Short/Medium/Long
    CONSTRAINT PK_Dim_Route PRIMARY KEY (route_key)
);
GO

CREATE NONCLUSTERED INDEX IX_Dim_Route_FlightId
    ON dbo.Dim_Route (flight_id)
    INCLUDE (route_key);
GO

CREATE PROCEDURE dbo.Upsert_Dim_Route
    @flight_id        INT,
    @carrier_code     CHAR(2),
    @flight_number    INT,
    @origin_code      CHAR(3),
    @dest_code        CHAR(3),
    @crs_dep_time     SMALLINT,
    @crs_arr_time     SMALLINT,
    @crs_elapsed_time FLOAT,
    @distance         FLOAT,
    @distance_band    VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM dbo.Dim_Route WHERE flight_id = @flight_id)
    BEGIN
        -- Type 1 SCD: overwrite with latest values
        UPDATE dbo.Dim_Route
        SET carrier_code     = @carrier_code,
            flight_number    = @flight_number,
            origin_code      = @origin_code,
            dest_code        = @dest_code,
            crs_dep_time     = @crs_dep_time,
            crs_arr_time     = @crs_arr_time,
            crs_elapsed_time = @crs_elapsed_time,
            distance         = @distance,
            distance_band    = @distance_band
        WHERE flight_id = @flight_id;
    END
    ELSE
    BEGIN
        INSERT INTO dbo.Dim_Route (
            flight_id, carrier_code, flight_number, origin_code, dest_code,
            crs_dep_time, crs_arr_time, crs_elapsed_time, distance, distance_band
        )
        VALUES (
            @flight_id, @carrier_code, @flight_number, @origin_code, @dest_code,
            @crs_dep_time, @crs_arr_time, @crs_elapsed_time, @distance, @distance_band
        );
    END
END;
GO

-- ============================================================
-- DIM_CANCELLATION_REASON
-- Small reference dimension for DOT/BTS cancellation codes.
-- Includes a default 'N/A' row (key = 0) for non-cancelled
-- operations. This eliminates NULL foreign keys in the fact
-- table, which would break SSAS dimension relationships and
-- violate referential integrity in the DW schema.
-- Source: US DOT BTS Form 41 data dictionary.
-- ============================================================
SELECT 
    fk.name AS FK_name,
    tp.name AS ParentTable,
    ref.name AS ReferencedTable
FROM sys.foreign_keys fk
INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
INNER JOIN sys.tables ref ON fk.referenced_object_id = ref.object_id
WHERE ref.name = 'Dim_CancellationReason';


-- Step 1b: Alter the column type from CHAR(3) to VARCHAR(3)
-- CHAR pads 'A' to 'A  ' — VARCHAR stores 'A' as exactly 'A'
USE FlightDW;
GO

-- Change CHAR(3) → VARCHAR(3)
ALTER TABLE dbo.Dim_CancellationReason
    ALTER COLUMN cancellation_code VARCHAR(3) NOT NULL;
GO

-- Clean up any padded values
UPDATE dbo.Dim_CancellationReason
SET cancellation_code = RTRIM(cancellation_code);
GO

-- Verify lengths
SELECT cancellation_key, cancellation_code,
       LEN(cancellation_code) AS stored_length,
       DATALENGTH(cancellation_code) AS stored_bytes
FROM dbo.Dim_CancellationReason;


CREATE TABLE dbo.Dim_CancellationReason (
    cancellation_key        INT IDENTITY(1,1) PRIMARY KEY,
    cancellation_code       VARCHAR(3)         NOT NULL,   -- A/B/C/D or 'N/A'
    reason_description      VARCHAR(200)    NOT NULL,
    responsible_party       VARCHAR(50)     NOT NULL,
    compensation_eligibility VARCHAR(200)   NOT NULL
);
GO
-- Insert default row for non-cancelled operations (loaded before ETL runs)
INSERT INTO dbo.Dim_CancellationReason (
    cancellation_code,
    reason_description,
    responsible_party,
    compensation_eligibility
)
VALUES
('N/A', 'Flight operated — not cancelled', 'N/A', 'N/A'),
('A',   'Carrier — airline-controllable (mechanical, crew, etc.)',
        'Airline', 'Yes — DOT 14 CFR 250 rebooking/refund obligation'),
('B',   'Weather — meteorological conditions beyond airline control',
        'External/Weather', 'No — force majeure, airline not liable'),
('C',   'National Air System — NAS/ATC traffic or airport operations',
        'FAA/NAS', 'Case by case — depends on ATC fault determination'),
('D',   'Security — TSA screening failure or aircraft security breach',
        'TSA/Airport', 'Case by case — depends on prior notice to airline');
GO

select * from Dim_CancellationReason;

ALTER TABLE dbo.Fact_FlightOperation
ADD CONSTRAINT FK_Fact_Cancellation
FOREIGN KEY (cancellation_key) REFERENCES dbo.Dim_CancellationReason(cancellation_key);

CREATE PROCEDURE dbo.UpdateDimCancellationReason
    @CancellationCode CHAR(3),
    @ReasonDescription VARCHAR(200),
    @ResponsibleParty VARCHAR(50),
    @CompensationEligibility VARCHAR(200)
AS
BEGIN
    -- If the cancellation code does not exist, insert a new row
    IF NOT EXISTS (
        SELECT 1
        FROM dbo.Dim_CancellationReason
        WHERE cancellation_code = @CancellationCode
    )
    BEGIN
        INSERT INTO dbo.Dim_CancellationReason
        (
            cancellation_code,
            reason_description,
            responsible_party,
            compensation_eligibility
        )
        VALUES
        (
            @CancellationCode,
            @ReasonDescription,
            @ResponsibleParty,
            @CompensationEligibility
        );
    END
    ELSE
    BEGIN
        -- If the code exists, update the descriptive attributes (Type 1 overwrite)
        UPDATE dbo.Dim_CancellationReason
        SET reason_description = @ReasonDescription,
            responsible_party = @ResponsibleParty,
            compensation_eligibility = @CompensationEligibility
        WHERE cancellation_code = @CancellationCode;
    END
END;
GO

-- ============================================================
-- FACT_FLIGHTOPERATION
-- Core fact table. Grain: one row per daily flight operation.
-- 7,079,081 rows covering full calendar year 2024 (366 days).
--
-- Measure classification:
--   ADDITIVE    — can be summed across any dimension combination.
--                 All delay minutes, elapsed times, air_time, distance.
--   SEMI-ADDITIVE — summing across time is misleading; use COUNT instead.
--                 cancelled, diverted (flag columns — sum gives count,
--                 which is useful, but average over time is meaningless).
--   NON-ADDITIVE — cannot be summed. dep_time, arr_time are times-of-day
--                 (HHMM format) — averaging is meaningful, summing is not.
--
-- Role-playing dimensions: Dim_Airport is referenced twice —
--   origin_airport_key and dest_airport_key both point to the
--   same Dim_Airport table under two different FK names.
--
-- Accumulating fact columns (Task 6):
--   accm_txn_create_time  — set to GETDATE() when row is first loaded
--   accm_txn_complete_time — updated by Package 3 when completion data arrives
--   txn_process_time_hours — computed: DATEDIFF(HOUR, create, complete)
-- ============================================================
CREATE TABLE dbo.Fact_FlightOperation (
    -- Surrogate primary key
    operation_key           INT         NOT NULL    IDENTITY(1,1),

    -- Foreign keys to dimension tables
    date_key                INT         NOT NULL,   -- → Dim_Date
    airline_key             INT         NOT NULL,   -- → Dim_Airline (current SCD2 row)
    origin_airport_key      INT         NOT NULL,   -- → Dim_Airport (origin role)
    dest_airport_key        INT         NOT NULL,   -- → Dim_Airport (destination role)
    route_key               INT         NOT NULL,   -- → Dim_Route
    cancellation_key        INT         NOT NULL    DEFAULT 0,  -- → Dim_CancellationReason

    -- Degenerate dimension (natural key from source — useful for
    -- tracing back to source data without storing in a dimension)
    operation_id            INT         NOT NULL,

    -- ── Non-additive time-of-day measures (HHMM format) ─────
    dep_time                FLOAT       NULL,       -- Actual departure time
    arr_time                FLOAT       NULL,       -- Actual arrival time

    -- ── Additive delay measures (minutes) ───────────────────
    dep_delay               FLOAT       NULL,       -- + = late, - = early
    arr_delay               FLOAT       NULL,       -- + = late, - = early
    taxi_out                FLOAT       NULL,       -- Gate to wheels-off mins
    taxi_in                 FLOAT       NULL,       -- Wheels-on to gate mins

    -- ── Additive duration measures (minutes) ─────────────────
    actual_elapsed_time     FLOAT       NULL,       -- Gate-to-gate mins
    air_time                FLOAT       NULL,       -- Wheels-off to wheels-on

    -- ── Additive distance measure (miles) ────────────────────
    distance                FLOAT       NOT NULL,   -- Copied from Dim_Route
                                                    -- for fast aggregation

    -- ── Additive delay component measures (minutes) ──────────
    -- Each is independently attributable to a responsible party.
    -- Stored separately per BTS reporting spec — required for
    -- carrier vs weather vs NAS analysis in SSAS cube measures.
    carrier_delay           SMALLINT    NOT NULL    DEFAULT 0,
    weather_delay           SMALLINT    NOT NULL    DEFAULT 0,
    nas_delay               SMALLINT    NOT NULL    DEFAULT 0,
    security_delay          SMALLINT    NOT NULL    DEFAULT 0,
    late_aircraft_delay     SMALLINT    NOT NULL    DEFAULT 0,
    total_delay_minutes     SMALLINT    NOT NULL    DEFAULT 0,  -- Derived sum

    -- ── Semi-additive flag measures ──────────────────────────
    cancelled               TINYINT     NOT NULL    DEFAULT 0,  -- 0 or 1
    diverted                TINYINT     NOT NULL    DEFAULT 0,  -- 0 or 1

    -- ── Accumulating fact columns (Task 6) ───────────────────
    accm_txn_create_time    DATETIME    NOT NULL    DEFAULT GETDATE(),
    accm_txn_complete_time  DATETIME    NULL,       -- Updated by Package 3
    txn_process_time_hours  FLOAT       NULL,       -- Computed on update

    -- ── Constraints ──────────────────────────────────────────
    CONSTRAINT PK_Fact_FlightOperation
        PRIMARY KEY (operation_key),
    CONSTRAINT FK_Fact_Date
        FOREIGN KEY (date_key)
        REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_Fact_Airline
        FOREIGN KEY (airline_key)
        REFERENCES dbo.Dim_Airline (airline_key),
    CONSTRAINT FK_Fact_OriginAirport
        FOREIGN KEY (origin_airport_key)
        REFERENCES dbo.Dim_Airport (airport_key),
    CONSTRAINT FK_Fact_DestAirport
        FOREIGN KEY (dest_airport_key)
        REFERENCES dbo.Dim_Airport (airport_key),
    CONSTRAINT FK_Fact_Route
        FOREIGN KEY (route_key)
        REFERENCES dbo.Dim_Route (route_key),
    CONSTRAINT FK_Fact_Cancellation
        FOREIGN KEY (cancellation_key)
        REFERENCES dbo.Dim_CancellationReason (cancellation_key)
);
GO

-- Performance indexes for common analytical query patterns
CREATE NONCLUSTERED INDEX IX_Fact_DateKey
    ON dbo.Fact_FlightOperation (date_key)
    INCLUDE (dep_delay, arr_delay, cancelled, airline_key);
GO
CREATE NONCLUSTERED INDEX IX_Fact_AirlineKey
    ON dbo.Fact_FlightOperation (airline_key)
    INCLUDE (dep_delay, arr_delay, cancelled, date_key);
GO
CREATE NONCLUSTERED INDEX IX_Fact_OriginAirport
    ON dbo.Fact_FlightOperation (origin_airport_key)
    INCLUDE (arr_delay, cancelled, date_key);
GO
CREATE NONCLUSTERED INDEX IX_Fact_NaturalKey
    ON dbo.Fact_FlightOperation (operation_id);
    -- Used by Package 3 to match accm_txn_complete_time updates
GO



sp_help 'dbo.Dim_Airline';

USE FlightDW;
GO

IF OBJECT_ID('dbo.usp_UpdateTxnProcessHours') IS NOT NULL
    DROP PROCEDURE dbo.usp_UpdateTxnProcessHours;
GO

CREATE PROCEDURE dbo.usp_UpdateTxnProcessHours
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE FlightDW.dbo.Fact_FlightOperation
    SET    txn_process_time_hours =
               CAST(
                   DATEDIFF(MINUTE, accm_txn_create_time, accm_txn_complete_time)
                   AS FLOAT
               ) / 60.0
    WHERE  accm_txn_complete_time IS NOT NULL
    AND    txn_process_time_hours IS NULL;

    PRINT 'Updated txn_process_time_hours: '
        + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
END;
GO