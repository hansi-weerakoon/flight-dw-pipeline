USE AirlineOLTP;
GO

-- Airline source table
CREATE TABLE Airline (
    carrier_code CHAR(2) PRIMARY KEY,
    carrier_name VARCHAR(100)
);

-- Airport source table
CREATE TABLE Airport (
    airport_code CHAR(3) PRIMARY KEY,
    city_name VARCHAR(100),
    state_name VARCHAR(100)
);

-- Flight source table
CREATE TABLE Flight (
    flight_id INT PRIMARY KEY,
    carrier_code CHAR(2) FOREIGN KEY REFERENCES Airline(carrier_code),
    flight_number INT,
    origin_code CHAR(3) FOREIGN KEY REFERENCES Airport(airport_code),
    dest_code CHAR(3) FOREIGN KEY REFERENCES Airport(airport_code),
    crs_dep_time SMALLINT,
    crs_arr_time SMALLINT,
    crs_elapsed_time FLOAT,
    distance FLOAT
);

select * from Airline;

select * from Airport;

select * from Flight;

SELECT flight_id, COUNT(*) AS cnt
FROM   dbo.Flight
GROUP  BY flight_id
HAVING COUNT(*) > 1;

-- Add index on carrier_code for SSIS Lookup performance
CREATE NONCLUSTERED INDEX IX_Flight_Carrier
    ON dbo.Flight (carrier_code)
    INCLUDE (flight_id, origin_code, dest_code);

-- Quick sanity check
SELECT
    COUNT(*)                    AS total_routes,
    COUNT(DISTINCT carrier_code) AS carriers,
    COUNT(DISTINCT origin_code)  AS origins,
    MIN(distance)               AS min_dist_miles,
    MAX(distance)               AS max_dist_miles
FROM dbo.Flight;