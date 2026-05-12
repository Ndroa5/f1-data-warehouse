-- ============================================================
-- SCD2 MIGRATION SCRIPT
-- Transformiše postojeće gold dimenzije iz SCD1 u SCD2
-- ============================================================

-- BACKUP postojećih tabela
CREATE TABLE IF NOT EXISTS gold.dim_driver_backup AS SELECT * FROM gold.dim_driver;
CREATE TABLE IF NOT EXISTS gold.dim_constructor_backup AS SELECT * FROM gold.dim_constructor;
CREATE TABLE IF NOT EXISTS gold.dim_circuit_backup AS SELECT * FROM gold.dim_circuit;
CREATE TABLE IF NOT EXISTS gold.dim_status_backup AS SELECT * FROM gold.dim_status;
CREATE TABLE IF NOT EXISTS gold.dim_date_backup AS SELECT * FROM gold.dim_date;
CREATE TABLE IF NOT EXISTS gold.dim_race_backup AS SELECT * FROM gold.dim_race;

-- DROP postojeće tabele (CASCADE briše i FK u fact tabelama)
DROP TABLE IF EXISTS gold.dim_driver CASCADE;
DROP TABLE IF EXISTS gold.dim_constructor CASCADE;
DROP TABLE IF EXISTS gold.dim_circuit CASCADE;
DROP TABLE IF EXISTS gold.dim_status CASCADE;
DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.dim_race CASCADE;

-- KREIRAJ nove SCD2 tabele
-- dim_driver
CREATE TABLE gold.dim_driver (
    driver_sk SERIAL PRIMARY KEY,
    driverid INTEGER NOT NULL,
    driverref VARCHAR(100),
    code VARCHAR(10),
    forename VARCHAR(100),
    surname VARCHAR(100),
    dob DATE,
    nationality VARCHAR(100),
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_constructor
CREATE TABLE gold.dim_constructor (
    constructor_sk SERIAL PRIMARY KEY,
    constructorid INTEGER NOT NULL,
    constructorref VARCHAR(100),
    name VARCHAR(200),
    nationality_constructors VARCHAR(100),
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_circuit
CREATE TABLE gold.dim_circuit (
    circuit_sk SERIAL PRIMARY KEY,
    circuitid INTEGER NOT NULL,
    circuitref VARCHAR(100),
    circuit_name VARCHAR(200),
    location VARCHAR(100),
    country VARCHAR(100),
    lat FLOAT,
    lng FLOAT,
    alt FLOAT,
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_status
CREATE TABLE gold.dim_status (
    status_sk SERIAL PRIMARY KEY,
    statusid INTEGER NOT NULL,
    status VARCHAR(100),
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_date
CREATE TABLE gold.dim_date (
    date_sk SERIAL PRIMARY KEY,
    dateid INTEGER NOT NULL,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_race
CREATE TABLE gold.dim_race (
    race_sk SERIAL PRIMARY KEY,
    raceid INTEGER NOT NULL,
    circuitid INTEGER,
    year INTEGER,
    round INTEGER,
    race_name VARCHAR(200),
    date DATE,
    time_races TIME,
    fp1_date DATE,
    fp1_time TIME,
    fp2_date DATE,
    fp2_time TIME,
    fp3_date DATE,
    fp3_time TIME,
    quali_date DATE,
    quali_time TIME,
    sprint_date DATE,
    sprint_time TIME,
    dateid INTEGER,
    row_hash VARCHAR(32) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- INDEKSI za brže lookup-ove
CREATE INDEX idx_dim_driver_natural ON gold.dim_driver(driverid, is_current);
CREATE INDEX idx_dim_constructor_natural ON gold.dim_constructor(constructorid, is_current);
CREATE INDEX idx_dim_circuit_natural ON gold.dim_circuit(circuitid, is_current);
CREATE INDEX idx_dim_status_natural ON gold.dim_status(statusid, is_current);
CREATE INDEX idx_dim_date_natural ON gold.dim_date(dateid, is_current);
CREATE INDEX idx_dim_race_natural ON gold.dim_race(raceid, is_current);

-- REKREIRAJ fact tabele (moraš ih dropovati jer CASCADE obrisao FK)
-- Zadržavaju natural keys (driverid, constructorid) - point-in-time JOIN kasnije

DROP TABLE IF EXISTS gold.fact_results CASCADE;
CREATE TABLE gold.fact_results (
    resultid INTEGER PRIMARY KEY,
    raceid INTEGER,
    driverid INTEGER,
    constructorid INTEGER,
    statusid INTEGER,
    grid INTEGER,
    position NUMERIC,
    laps INTEGER,
    milliseconds INTEGER,
    fastestlaptime VARCHAR(20),
    fastestlapspeed FLOAT,
    rank INTEGER
);

DROP TABLE IF EXISTS gold.fact_lap_times CASCADE;
CREATE TABLE gold.fact_lap_times (
    raceid INTEGER,
    driverid INTEGER,
    lap INTEGER,
    position_laptimes INTEGER,
    time_laptimes VARCHAR(20),
    milliseconds_laptimes INTEGER,
    PRIMARY KEY (raceid, driverid, lap)
);

DROP TABLE IF EXISTS gold.fact_pit_stops CASCADE;
CREATE TABLE gold.fact_pit_stops (
    raceid INTEGER,
    driverid INTEGER,
    stop INTEGER,
    lap_pitstops INTEGER,
    time_pitstops VARCHAR(20),
    duration VARCHAR(20),
    milliseconds_pitstops INTEGER,
    PRIMARY KEY (raceid, driverid, stop)
);

DROP TABLE IF EXISTS gold.fact_driver_standings CASCADE;
CREATE TABLE gold.fact_driver_standings (
    driverstandingsid INTEGER PRIMARY KEY,
    raceid INTEGER,
    driverid INTEGER,
    points_driverstandings FLOAT,
    position_driverstandings INTEGER,
    wins INTEGER
);

DROP TABLE IF EXISTS gold.fact_constructor_standings CASCADE;
CREATE TABLE gold.fact_constructor_standings (
    constructorstandingsid INTEGER PRIMARY KEY,
    raceid INTEGER,
    constructorid INTEGER,
    points_constructorstandings FLOAT,
    position_constructorstandings INTEGER,
    wins_constructorstandings INTEGER
);

-- ============================================================
-- MIGRATION ZAVRŠEN
-- Sada možeš pokrenuti novi gold load sa SCD2 logikom
-- ============================================================