-- ============================================================
-- SCHEMAS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- BRONZE (sve TEXT, raw podaci 1:1)
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.raw_data (
    unnamed_0                           TEXT,
    resultId                            TEXT,
    raceId                              TEXT,
    driverId                            TEXT,
    constructorId                       TEXT,
    number                              TEXT,
    grid                                TEXT,
    position                            TEXT,
    positionText                        TEXT,
    positionOrder                       TEXT,
    points                              TEXT,
    laps                                TEXT,
    time                                TEXT,
    milliseconds                        TEXT,
    fastestLap                          TEXT,
    rank                                TEXT,
    fastestLapTime                      TEXT,
    fastestLapSpeed                     TEXT,
    statusId                            TEXT,
    year                                TEXT,
    round                               TEXT,
    circuitId                           TEXT,
    name_x                              TEXT,
    date                                TEXT,
    time_races                          TEXT,
    url_x                               TEXT,
    fp1_date                            TEXT,
    fp1_time                            TEXT,
    fp2_date                            TEXT,
    fp2_time                            TEXT,
    fp3_date                            TEXT,
    fp3_time                            TEXT,
    quali_date                          TEXT,
    quali_time                          TEXT,
    sprint_date                         TEXT,
    sprint_time                         TEXT,
    circuitRef                          TEXT,
    name_y                              TEXT,
    location                            TEXT,
    country                             TEXT,
    lat                                 TEXT,
    lng                                 TEXT,
    alt                                 TEXT,
    url_y                               TEXT,
    driverRef                           TEXT,
    number_drivers                      TEXT,
    code                                TEXT,
    forename                            TEXT,
    surname                             TEXT,
    dob                                 TEXT,
    nationality                         TEXT,
    url                                 TEXT,
    constructorRef                      TEXT,
    name                                TEXT,
    nationality_constructors            TEXT,
    url_constructors                    TEXT,
    lap                                 TEXT,
    position_laptimes                   TEXT,
    time_laptimes                       TEXT,
    milliseconds_laptimes               TEXT,
    stop                                TEXT,
    lap_pitstops                        TEXT,
    time_pitstops                       TEXT,
    duration                            TEXT,
    milliseconds_pitstops               TEXT,
    driverStandingsId                   TEXT,
    points_driverstandings              TEXT,
    position_driverstandings            TEXT,
    positionText_driverstandings        TEXT,
    wins                                TEXT,
    constructorStandingsId              TEXT,
    points_constructorstandings         TEXT,
    position_constructorstandings       TEXT,
    positionText_constructorstandings   TEXT,
    wins_constructorstandings           TEXT,
    status                              TEXT
);

-- ============================================================
-- SILVER (čišćenje, tipovi, bez FK constraintova)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.results (
    resultId            INTEGER,
    raceId              INTEGER,
    driverId            INTEGER,
    constructorId       INTEGER,
    statusId            INTEGER,
    number              VARCHAR(10),
    grid                INTEGER,
    position            NUMERIC,
    positionText        VARCHAR(10),
    positionOrder       INTEGER,
    points              FLOAT,
    laps                INTEGER,
    time                VARCHAR(20),
    milliseconds        INTEGER,
    fastestLap          INTEGER,
    rank                INTEGER,
    fastestLapTime      VARCHAR(20),
    fastestLapSpeed     FLOAT
);

CREATE TABLE IF NOT EXISTS silver.races (
    raceId          INTEGER,
    circuitId       INTEGER,
    year            INTEGER,
    round           INTEGER,
    name_x          VARCHAR(200),
    date            DATE,
    time_races      TIME,
    fp1_date        DATE,
    fp1_time        TIME,
    fp2_date        DATE,
    fp2_time        TIME,
    fp3_date        DATE,
    fp3_time        TIME,
    quali_date      DATE,
    quali_time      TIME,
    sprint_date     DATE,
    sprint_time     TIME
);

CREATE TABLE IF NOT EXISTS silver.circuits (
    circuitId       INTEGER,
    circuitRef      VARCHAR(100),
    name_y          VARCHAR(200),
    location        VARCHAR(100),
    country         VARCHAR(100),
    lat             FLOAT,
    lng             FLOAT,
    alt             FLOAT
);

CREATE TABLE IF NOT EXISTS silver.drivers (
    driverId        INTEGER,
    driverRef       VARCHAR(100),
    code            VARCHAR(10),
    forename        VARCHAR(100),
    surname         VARCHAR(100),
    dob             DATE,
    nationality     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS silver.constructors (
    constructorId            INTEGER,
    constructorRef           VARCHAR(100),
    name                     VARCHAR(200),
    nationality_constructors VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS silver.status (
    statusId        INTEGER,
    status          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS silver.lap_times (
    raceId                  INTEGER,
    driverId                INTEGER,
    lap                     INTEGER,
    position_laptimes       INTEGER,
    time_laptimes           VARCHAR(20),
    milliseconds_laptimes   INTEGER
);

CREATE TABLE IF NOT EXISTS silver.pit_stops (
    raceId                  INTEGER,
    driverId                INTEGER,
    stop                    INTEGER,
    lap_pitstops            INTEGER,
    time_pitstops           VARCHAR(20),
    duration                VARCHAR(20),
    milliseconds_pitstops   INTEGER
);

CREATE TABLE IF NOT EXISTS silver.driver_standings (
    driverStandingsId            INTEGER,
    raceId                       INTEGER,
    driverId                     INTEGER,
    points_driverstandings       FLOAT,
    position_driverstandings     INTEGER,
    positionText_driverstandings VARCHAR(10),
    wins                         INTEGER
);

CREATE TABLE IF NOT EXISTS silver.constructor_standings (
    constructorStandingsId            INTEGER,
    raceId                            INTEGER,
    constructorId                     INTEGER,
    points_constructorstandings       FLOAT,
    position_constructorstandings     INTEGER,
    positionText_constructorstandings VARCHAR(10),
    wins_constructorstandings         INTEGER
);

-- ============================================================
-- GOLD - DIM TABELE
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.dim_circuit (
    circuitId       INTEGER PRIMARY KEY,
    circuitRef      VARCHAR(100),
    name_y          VARCHAR(200),
    location        VARCHAR(100),
    country         VARCHAR(100),
    lat             FLOAT,
    lng             FLOAT,
    alt             FLOAT
);

CREATE TABLE IF NOT EXISTS gold.dim_race (
    raceId          INTEGER PRIMARY KEY,
    circuitId       INTEGER REFERENCES gold.dim_circuit(circuitId),
    year            INTEGER,
    round           INTEGER,
    name_x          VARCHAR(200),
    date            DATE,
    fp1_date        DATE,
    fp1_time        TIME,
    fp2_date        DATE,
    fp2_time        TIME,
    fp3_date        DATE,
    fp3_time        TIME,
    quali_date      DATE,
    quali_time      TIME,
    sprint_date     DATE,
    sprint_time     TIME
);

CREATE TABLE IF NOT EXISTS gold.dim_driver (
    driverId        INTEGER PRIMARY KEY,
    driverRef       VARCHAR(100),
    code            VARCHAR(10),
    forename        VARCHAR(100),
    surname         VARCHAR(100),
    dob             DATE,
    nationality     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS gold.dim_constructor (
    constructorId            INTEGER PRIMARY KEY,
    constructorRef           VARCHAR(100),
    name                     VARCHAR(200),
    nationality_constructors VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS gold.dim_status (
    statusId        INTEGER PRIMARY KEY,
    status          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    dateId          INTEGER PRIMARY KEY,
    full_date       DATE,
    year            INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    quarter         INTEGER,
    day_of_week     INTEGER,
    is_weekend      BOOLEAN
);

-- ============================================================
-- GOLD - FACT TABELE
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.fact_results (
    resultId            INTEGER PRIMARY KEY,
    raceId              INTEGER REFERENCES gold.dim_race(raceId),
    driverId            INTEGER REFERENCES gold.dim_driver(driverId),
    constructorId       INTEGER REFERENCES gold.dim_constructor(constructorId),
    statusId            INTEGER REFERENCES gold.dim_status(statusId),
    grid                INTEGER,
    position            NUMERIC,
    laps                INTEGER,
    milliseconds        INTEGER,
    fastestLapTime      VARCHAR(20),
    fastestLapSpeed     FLOAT,
    rank                INTEGER,
    date                DATE
);

CREATE TABLE IF NOT EXISTS gold.fact_lap_times (
    raceId                INTEGER REFERENCES gold.dim_race(raceId),
    driverId              INTEGER REFERENCES gold.dim_driver(driverId),
    lap                   INTEGER,
    position_laptimes     INTEGER,
    time_laptimes         VARCHAR(20),
    milliseconds_laptimes INTEGER,
    PRIMARY KEY (raceId, driverId, lap)
);

CREATE TABLE IF NOT EXISTS gold.fact_pit_stops (
    raceId                INTEGER REFERENCES gold.dim_race(raceId),
    driverId              INTEGER REFERENCES gold.dim_driver(driverId),
    stop                  INTEGER,
    lap_pitstops          INTEGER,
    time_pitstops         VARCHAR(20),
    duration              VARCHAR(20),
    milliseconds_pitstops INTEGER,
    PRIMARY KEY (raceId, driverId, stop)
);

CREATE TABLE IF NOT EXISTS gold.fact_driver_standings (
    driverStandingsId        INTEGER PRIMARY KEY,
    raceId                   INTEGER REFERENCES gold.dim_race(raceId),
    driverId                 INTEGER REFERENCES gold.dim_driver(driverId),
    points_driverstandings   FLOAT,
    position_driverstandings INTEGER,
    wins                     INTEGER
);

CREATE TABLE IF NOT EXISTS gold.fact_constructor_standings (
    constructorStandingsId          INTEGER PRIMARY KEY,
    raceId                          INTEGER REFERENCES gold.dim_race(raceId),
    constructorId                   INTEGER REFERENCES gold.dim_constructor(constructorId),
    points_constructorstandings     FLOAT,
    position_constructorstandings   INTEGER,
    wins_constructorstandings       INTEGER
);