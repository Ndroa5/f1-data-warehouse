CREATE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS silver;
"""

CREATE_DRIVERS = """
CREATE TABLE IF NOT EXISTS silver.drivers (
    driverid        INTEGER,
    driverref       VARCHAR(100),
    code            VARCHAR(10),
    forename        VARCHAR(100),
    surname         VARCHAR(100),
    dob             DATE,
    nationality     VARCHAR(100)
);
"""

CREATE_CONSTRUCTORS = """
CREATE TABLE IF NOT EXISTS silver.constructors (
    constructorid            INTEGER,
    constructorref           VARCHAR(100),
    name                     VARCHAR(200),
    nationality_constructors VARCHAR(100)
);
"""

CREATE_CIRCUITS = """
CREATE TABLE IF NOT EXISTS silver.circuits (
    circuitid       INTEGER,
    circuitref      VARCHAR(100),
    name_y          VARCHAR(200),
    location        VARCHAR(100),
    country         VARCHAR(100),
    lat             FLOAT,
    lng             FLOAT,
    alt             FLOAT
);
"""

CREATE_STATUS = """
CREATE TABLE IF NOT EXISTS silver.status (
    statusid        INTEGER,
    status          VARCHAR(100)
);
"""

CREATE_RACES = """
CREATE TABLE IF NOT EXISTS silver.races (
    raceid          INTEGER,
    circuitid       INTEGER,
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
"""

CREATE_RESULTS = """
CREATE TABLE IF NOT EXISTS silver.results (
    resultid            INTEGER,
    raceid              INTEGER,
    driverid            INTEGER,
    constructorid       INTEGER,
    statusid            INTEGER,
    number              VARCHAR(10),
    grid                INTEGER,
    position            NUMERIC,
    positiontext        VARCHAR(10),
    positionorder       INTEGER,
    points              FLOAT,
    laps                INTEGER,
    time                VARCHAR(20),
    milliseconds        INTEGER,
    fastestlap          INTEGER,
    rank                INTEGER,
    fastestlaptime      VARCHAR(20),
    fastestlapspeed     FLOAT
);
"""

CREATE_LAP_TIMES = """
CREATE TABLE IF NOT EXISTS silver.lap_times (
    raceid                  INTEGER,
    driverid                INTEGER,
    lap                     INTEGER,
    position_laptimes       INTEGER,
    time_laptimes           VARCHAR(20),
    milliseconds_laptimes   INTEGER
);
"""

CREATE_PIT_STOPS = """
CREATE TABLE IF NOT EXISTS silver.pit_stops (
    raceid                  INTEGER,
    driverid                INTEGER,
    stop                    INTEGER,
    lap_pitstops            INTEGER,
    time_pitstops           VARCHAR(20),
    duration                VARCHAR(20),
    milliseconds_pitstops   INTEGER
);
"""

CREATE_DRIVER_STANDINGS = """
CREATE TABLE IF NOT EXISTS silver.driver_standings (
    driverstandingsid            INTEGER,
    raceid                       INTEGER,
    driverid                     INTEGER,
    points_driverstandings       FLOAT,
    position_driverstandings     INTEGER,
    positiontext_driverstandings VARCHAR(10),
    wins                         INTEGER
);
"""

CREATE_CONSTRUCTOR_STANDINGS = """
CREATE TABLE IF NOT EXISTS silver.constructor_standings (
    constructorstandingsid            INTEGER,
    raceid                            INTEGER,
    constructorid                     INTEGER,
    points_constructorstandings       FLOAT,
    position_constructorstandings     INTEGER,
    positiontext_constructorstandings VARCHAR(10),
    wins_constructorstandings         INTEGER
);
"""