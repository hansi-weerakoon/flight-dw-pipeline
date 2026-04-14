-- After Package 1: dimension row counts
SELECT 'Dim_Date' AS tbl, COUNT(*) AS rows 
FROM FlightDW.dbo.Dim_Date
UNION ALL
SELECT 'Dim_Airline', COUNT(*)         
FROM FlightDW.dbo.Dim_Airline
UNION ALL
SELECT 'Dim_Airline current', COUNT(*)         
FROM FlightDW.dbo.Dim_Airline
WHERE is_current_record = 1
UNION ALL
SELECT 'Dim_Airport', COUNT(*)         
FROM FlightDW.dbo.Dim_Airport
UNION ALL
SELECT 'Dim_Route', COUNT(*)         
FROM FlightDW.dbo.Dim_Route
UNION ALL
SELECT 'Dim_CancellationReason', COUNT(*)         
FROM FlightDW.dbo.Dim_CancellationReason;

--Spot‑Check Enrichment Columns
-- Airport enrichment: region must be populated
SELECT TOP 10 airport_code, city_name, state_name, region, region_code
FROM FlightDW.dbo.Dim_Airport
ORDER BY airport_code;

-- Route enrichment: distance_band must be populated
SELECT TOP 10 flight_id, carrier_code, flight_number, origin_code, dest_code, crs_dep_time, crs_arr_time, crs_elapsed_time, distance, distance_band
FROM FlightDW.dbo.Dim_Route
ORDER BY flight_id;

--Error Logging Validation
SELECT TOP 10 *
FROM FlightDW_Staging.dbo.stg_ErrorRows
ORDER BY captured_at DESC;

--SCD Type 2 Airline Check
SELECT carrier_code, carrier_name, scd_effective_date, scd_expiry_date, is_current_record
FROM FlightDW.dbo.Dim_Airline
WHERE carrier_code = 'WN';  -- Example: Southwest Airlines

--Cancellation Reason Check
SELECT cancellation_code, reason_description
FROM FlightDW.dbo.Dim_CancellationReason;

--Referential Integrity (Optional)
-- Example join check
SELECT f.flight_id, r.route_key, a.airport_key, al.airline_key
FROM FlightDW_Staging.dbo.stg_Flight f
LEFT JOIN FlightDW.dbo.Dim_Route r ON f.flight_id = r.flight_id
LEFT JOIN FlightDW.dbo.Dim_Airport a ON f.origin_code = a.airport_code
LEFT JOIN FlightDW.dbo.Dim_Airline al ON f.carrier_code = al.carrier_code;


-- After Package 2

-- ✅ Fact table row count
SELECT COUNT(*) AS fact_rows
FROM FlightDW.dbo.Fact_FlightOperation;

-- ✅ Check for missing foreign keys
SELECT COUNT(*) AS null_date_keys
FROM FlightDW.dbo.Fact_FlightOperation
WHERE date_key IS NULL;

SELECT COUNT(*) AS null_airline_keys
FROM FlightDW.dbo.Fact_FlightOperation
WHERE airline_key IS NULL;

SELECT COUNT(*) AS null_origin_airport_keys
FROM FlightDW.dbo.Fact_FlightOperation
WHERE origin_airport_key IS NULL;

SELECT COUNT(*) AS null_destination_airport_keys
FROM FlightDW.dbo.Fact_FlightOperation
WHERE dest_airport_key IS NULL;

-- ✅ Error rows captured in staging
SELECT COUNT(*) AS error_rows
FROM FlightDW_Staging.dbo.stg_ErrorRows;

-- ✅ Business logic validation: delay totals
SELECT TOP 10
    operation_id,
    carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay AS computed_total,
    total_delay_minutes AS stored_total
FROM FlightDW.dbo.Fact_FlightOperation
WHERE cancelled = 0
  AND total_delay_minutes > 0;

-- ✅ Sanity check: cancelled flights should have cancellation reason populated
SELECT TOP 10
    operation_id, cancelled, cancellation_key
FROM FlightDW.dbo.Fact_FlightOperation
WHERE cancelled = 1;

--
--
-- Count operated flights in the fact table
SELECT COUNT(*) AS operated_fact_rows
FROM FlightDW.dbo.Fact_FlightOperation
WHERE cancelled = 0;

-- Count cancelled flights in the fact table
SELECT COUNT(*) AS cancelled_fact_rows
FROM FlightDW.dbo.Fact_FlightOperation
WHERE cancelled = 1;

-- Quick ratio check
SELECT 
    SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS operated_fact_rows,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_fact_rows,
    COUNT(*) AS total_fact_rows
FROM FlightDW.dbo.Fact_FlightOperation;


-- After Package 3
--Final validation queries
USE FlightDW;
GO

-- ── 1. Overall accumulating fact status ───────────────────────
SELECT
    COUNT(*)                                    AS total_fact_rows,
    SUM(CASE WHEN accm_txn_complete_time IS NOT NULL
             THEN 1 ELSE 0 END)                 AS completed_rows,
    SUM(CASE WHEN accm_txn_complete_time IS NULL
             THEN 1 ELSE 0 END)                 AS open_rows,
    CAST(
        SUM(CASE WHEN accm_txn_complete_time IS NOT NULL
                 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*)
    AS DECIMAL(5,2))                            AS pct_completed,
    AVG(txn_process_time_hours)                 AS avg_process_hours,
    MIN(txn_process_time_hours)                 AS min_process_hours,
    MAX(txn_process_time_hours)                 AS max_process_hours
FROM FlightDW.dbo.Fact_FlightOperation;

-- ── 2. Business logic validation — completion must be AFTER creation ──────
SELECT COUNT(*) AS invalid_timeline_rows
FROM   FlightDW.dbo.Fact_FlightOperation
WHERE  accm_txn_complete_time IS NOT NULL
AND    accm_txn_complete_time <= accm_txn_create_time;
-- Must return 0

-- ── 3. txn_process_time_hours must equal the manual DATEDIFF ─────────────
SELECT TOP 10
    operation_id,
    accm_txn_create_time,
    accm_txn_complete_time,
    txn_process_time_hours                          AS stored_hours,
    CAST(DATEDIFF(MINUTE,
                  accm_txn_create_time,
                  accm_txn_complete_time)
         AS FLOAT) / 60.0                           AS computed_hours,
    ABS(txn_process_time_hours -
        CAST(DATEDIFF(MINUTE,
                      accm_txn_create_time,
                      accm_txn_complete_time)
             AS FLOAT) / 60.0)                      AS discrepancy
FROM  FlightDW.dbo.Fact_FlightOperation
WHERE accm_txn_complete_time IS NOT NULL
ORDER BY NEWID();
-- discrepancy should be 0.000 for all rows

-- ── 4. Error table — invalid operation_ids ───────────────────────────────
SELECT
    COUNT(*)        AS total_error_rows,
    MIN(operation_id) AS min_invalid_id,
    MAX(operation_id) AS max_invalid_id,
    MIN(captured_at)  AS first_captured,
    MAX(captured_at)  AS last_captured
FROM FlightDW_Staging.dbo.stg_AccumErrors;
-- Should show ~500 rows (the injected invalid IDs)

-- ── 5. Processing time breakdown by flight type ───────────────────────────
SELECT
    da.region                                   AS origin_region,
    f.cancelled,
    COUNT(*)                                    AS completed_operations,
    CAST(AVG(f.txn_process_time_hours)
         AS DECIMAL(10,2))                      AS avg_process_hrs,
    CAST(MIN(f.txn_process_time_hours)
         AS DECIMAL(10,2))                      AS min_process_hrs,
    CAST(MAX(f.txn_process_time_hours)
         AS DECIMAL(10,2))                      AS max_process_hrs
FROM  FlightDW.dbo.Fact_FlightOperation  f
JOIN  FlightDW.dbo.Dim_Airport           da
      ON f.origin_airport_key = da.airport_key
WHERE f.accm_txn_complete_time IS NOT NULL
GROUP BY da.region, f.cancelled
ORDER BY da.region, f.cancelled;

--solution: Problem 1 — Negative Process Hours
--Alter Column Default in SQL Serve
--Step 1 — Identify the Default Constraint
SELECT name
FROM   sys.default_constraints
WHERE  parent_object_id = OBJECT_ID('dbo.Fact_FlightOperation')
AND    col_name(parent_object_id, parent_column_id) = 'accm_txn_create_time';

--Step 2 — Drop the Constraint
ALTER TABLE dbo.Fact_FlightOperation
DROP CONSTRAINT DF__Fact_Flig__accm___60A75C0F;

--Step 3 — Ensure Column Allows Explicit Values
ALTER TABLE dbo.Fact_FlightOperation
ALTER COLUMN accm_txn_create_time DATETIME NULL;