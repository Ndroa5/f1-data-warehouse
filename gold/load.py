from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

# ============================================================
# SCD TYPE 2 DIMENSION LOADS
# ============================================================

def load_dim_driver(engine):
    """SCD2 load za dim_driver"""
    logger.info("Ubacujem gold.dim_driver (SCD2)...")
    try:
        with engine.begin() as conn:
            # Step 1: Expire old versions
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (driverid) driverid, row_hash_driver
                    FROM silver.raw_data WHERE driverid IS NOT NULL
                    ORDER BY driverid
                ),
                current_records AS (
                    SELECT driver_sk, driverid, row_hash
                    FROM gold.dim_driver WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.driver_sk
                    FROM new_data n
                    JOIN current_records cr ON n.driverid = cr.driverid
                    WHERE n.row_hash_driver != cr.row_hash
                )
                UPDATE gold.dim_driver d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.driver_sk = ch.driver_sk;
            """))
            
            # Step 2: Insert new/changed versions
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (driverid)
                        driverid, driverref, code, forename, surname, dob, nationality, row_hash_driver
                    FROM silver.raw_data WHERE driverid IS NOT NULL
                    ORDER BY driverid
                ),
                current_records AS (
                    SELECT driverid, row_hash
                    FROM gold.dim_driver WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.driverid = cr.driverid
                    WHERE cr.driverid IS NULL OR n.row_hash_driver != cr.row_hash
                )
                INSERT INTO gold.dim_driver 
                    (driverid, driverref, code, forename, surname, dob, nationality,
                     row_hash, valid_from, valid_to, is_current)
                SELECT driverid, driverref, code, forename, surname, dob, nationality,
                       row_hash_driver, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
        logger.info("dim_driver SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_driver: {e}")
        raise

def load_dim_constructor(engine):
    """SCD2 load za dim_constructor"""
    logger.info("Ubacujem gold.dim_constructor (SCD2)...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (constructorid) constructorid, row_hash_constructor
                    FROM silver.raw_data WHERE constructorid IS NOT NULL
                    ORDER BY constructorid
                ),
                current_records AS (
                    SELECT constructor_sk, constructorid, row_hash
                    FROM gold.dim_constructor WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.constructor_sk
                    FROM new_data n
                    JOIN current_records cr ON n.constructorid = cr.constructorid
                    WHERE n.row_hash_constructor != cr.row_hash
                )
                UPDATE gold.dim_constructor d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.constructor_sk = ch.constructor_sk;
            """))
            
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (constructorid)
                        constructorid, constructorref, name, nationality_constructors, row_hash_constructor
                    FROM silver.raw_data WHERE constructorid IS NOT NULL
                    ORDER BY constructorid
                ),
                current_records AS (
                    SELECT constructorid, row_hash
                    FROM gold.dim_constructor WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.constructorid = cr.constructorid
                    WHERE cr.constructorid IS NULL OR n.row_hash_constructor != cr.row_hash
                )
                INSERT INTO gold.dim_constructor 
                    (constructorid, constructorref, name, nationality_constructors,
                     row_hash, valid_from, valid_to, is_current)
                SELECT constructorid, constructorref, name, nationality_constructors,
                       row_hash_constructor, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
        logger.info("dim_constructor SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_constructor: {e}")
        raise

def load_dim_circuit(engine):
    """SCD2 load za dim_circuit"""
    logger.info("Ubacujem gold.dim_circuit (SCD2)...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (circuitid) circuitid, row_hash_circuit
                    FROM silver.raw_data WHERE circuitid IS NOT NULL
                    ORDER BY circuitid
                ),
                current_records AS (
                    SELECT circuit_sk, circuitid, row_hash
                    FROM gold.dim_circuit WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.circuit_sk
                    FROM new_data n
                    JOIN current_records cr ON n.circuitid = cr.circuitid
                    WHERE n.row_hash_circuit != cr.row_hash
                )
                UPDATE gold.dim_circuit d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.circuit_sk = ch.circuit_sk;
            """))
            
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (circuitid)
                        circuitid, circuitref, name_y AS circuit_name,
                        location, country, lat, lng, alt, row_hash_circuit
                    FROM silver.raw_data WHERE circuitid IS NOT NULL
                    ORDER BY circuitid
                ),
                current_records AS (
                    SELECT circuitid, row_hash
                    FROM gold.dim_circuit WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.circuitid = cr.circuitid
                    WHERE cr.circuitid IS NULL OR n.row_hash_circuit != cr.row_hash
                )
                INSERT INTO gold.dim_circuit 
                    (circuitid, circuitref, circuit_name, location, country, lat, lng, alt,
                     row_hash, valid_from, valid_to, is_current)
                SELECT circuitid, circuitref, circuit_name, location, country, lat, lng, alt,
                       row_hash_circuit, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
        logger.info("dim_circuit SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_circuit: {e}")
        raise

def load_dim_status(engine):
    """SCD2 load za dim_status"""
    logger.info("Ubacujem gold.dim_status (SCD2)...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (statusid) statusid, row_hash_status
                    FROM silver.raw_data WHERE statusid IS NOT NULL
                    ORDER BY statusid
                ),
                current_records AS (
                    SELECT status_sk, statusid, row_hash
                    FROM gold.dim_status WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.status_sk
                    FROM new_data n
                    JOIN current_records cr ON n.statusid = cr.statusid
                    WHERE n.row_hash_status != cr.row_hash
                )
                UPDATE gold.dim_status d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.status_sk = ch.status_sk;
            """))
            
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (statusid) statusid, status, row_hash_status
                    FROM silver.raw_data WHERE statusid IS NOT NULL
                    ORDER BY statusid
                ),
                current_records AS (
                    SELECT statusid, row_hash
                    FROM gold.dim_status WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.statusid = cr.statusid
                    WHERE cr.statusid IS NULL OR n.row_hash_status != cr.row_hash
                )
                INSERT INTO gold.dim_status 
                    (statusid, status, row_hash, valid_from, valid_to, is_current)
                SELECT statusid, status, row_hash_status, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
        logger.info("dim_status SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_status: {e}")
        raise

def load_dim_date(engine):
    """SCD2 load za dim_date"""
    logger.info("Ubacujem gold.dim_date (SCD2)...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                WITH new_data AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY full_date) AS dateid,
                        full_date, row_hash_date
                    FROM (
                        SELECT DISTINCT date AS full_date, row_hash_date
                        FROM silver.raw_data WHERE date IS NOT NULL
                    ) dates
                ),
                current_records AS (
                    SELECT date_sk, dateid, row_hash
                    FROM gold.dim_date WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.date_sk
                    FROM new_data n
                    JOIN current_records cr ON n.dateid = cr.dateid
                    WHERE n.row_hash_date != cr.row_hash
                )
                UPDATE gold.dim_date d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.date_sk = ch.date_sk;
            """))
            
            conn.execute(text("""
                WITH new_data AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY full_date) AS dateid,
                        full_date,
                        EXTRACT(YEAR FROM full_date)::INTEGER AS year,
                        EXTRACT(MONTH FROM full_date)::INTEGER AS month,
                        TO_CHAR(full_date, 'Month') AS month_name,
                        EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
                        EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week,
                        EXTRACT(DOW FROM full_date) IN (0, 6) AS is_weekend,
                        row_hash_date
                    FROM (
                        SELECT DISTINCT date AS full_date, row_hash_date
                        FROM silver.raw_data WHERE date IS NOT NULL
                    ) dates
                ),
                current_records AS (
                    SELECT dateid, row_hash
                    FROM gold.dim_date WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.dateid = cr.dateid
                    WHERE cr.dateid IS NULL OR n.row_hash_date != cr.row_hash
                )
                INSERT INTO gold.dim_date 
                    (dateid, full_date, year, month, month_name, quarter, day_of_week, is_weekend,
                     row_hash, valid_from, valid_to, is_current)
                SELECT dateid, full_date, year, month, month_name, quarter, day_of_week, is_weekend,
                       row_hash_date, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
        logger.info("dim_date SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_date: {e}")
        raise

def load_dim_race(engine):
    """SCD2 load za dim_race"""
    logger.info("Ubacujem gold.dim_race (SCD2)...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (raceid) raceid, row_hash_race
                    FROM silver.raw_data WHERE raceid IS NOT NULL
                    ORDER BY raceid
                ),
                current_records AS (
                    SELECT race_sk, raceid, row_hash
                    FROM gold.dim_race WHERE is_current = TRUE
                ),
                changed_records AS (
                    SELECT cr.race_sk
                    FROM new_data n
                    JOIN current_records cr ON n.raceid = cr.raceid
                    WHERE n.row_hash_race != cr.row_hash
                )
                UPDATE gold.dim_race d
                SET valid_to = CURRENT_DATE, is_current = FALSE
                FROM changed_records ch
                WHERE d.race_sk = ch.race_sk;
            """))
            
            conn.execute(text("""
                WITH new_data AS (
                    SELECT DISTINCT ON (raceid)
                        raceid, circuitid, year, round, name_x AS race_name,
                        date, time_races, fp1_date, fp1_time, fp2_date, fp2_time,
                        fp3_date, fp3_time, quali_date, quali_time,
                        sprint_date, sprint_time, row_hash_race
                    FROM silver.raw_data WHERE raceid IS NOT NULL
                    ORDER BY raceid
                ),
                current_records AS (
                    SELECT raceid, row_hash
                    FROM gold.dim_race WHERE is_current = TRUE
                ),
                records_to_insert AS (
                    SELECT n.*
                    FROM new_data n
                    LEFT JOIN current_records cr ON n.raceid = cr.raceid
                    WHERE cr.raceid IS NULL OR n.row_hash_race != cr.row_hash
                )
                INSERT INTO gold.dim_race 
                    (raceid, circuitid, year, round, race_name, date, time_races,
                     fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time,
                     quali_date, quali_time, sprint_date, sprint_time, dateid,
                     row_hash, valid_from, valid_to, is_current)
                SELECT raceid, circuitid, year, round, race_name, date, time_races,
                       fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time,
                       quali_date, quali_time, sprint_date, sprint_time, NULL,
                       row_hash_race, CURRENT_DATE, NULL, TRUE
                FROM records_to_insert;
            """))
            
            # Update dateid FK
            conn.execute(text("""
                UPDATE gold.dim_race r
                SET dateid = d.dateid
                FROM gold.dim_date d
                WHERE r.date = d.full_date AND d.is_current = TRUE AND r.is_current = TRUE;
            """))
        logger.info("dim_race SCD2 završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju dim_race: {e}")
        raise

# ============================================================
# FACT TABLE LOADS
# ============================================================

def load_fact_results(engine):
    logger.info("Ubacujem gold.fact_results...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO gold.fact_results (resultid, raceid, driverid, constructorid, statusid,
                    grid, position, laps, milliseconds, fastestlaptime, fastestlapspeed, rank)
                SELECT DISTINCT ON (s.resultid)
                    s.resultid, s.raceid, s.driverid, s.constructorid, s.statusid,
                    s.grid, s.position, s.laps, s.milliseconds,
                    s.fastestlaptime, s.fastestlapspeed, s.rank
                FROM silver.raw_data s
                WHERE s.resultid IS NOT NULL
                ORDER BY s.resultid
                ON CONFLICT (resultid) DO UPDATE SET
                    raceid = EXCLUDED.raceid,
                    driverid = EXCLUDED.driverid,
                    constructorid = EXCLUDED.constructorid,
                    statusid = EXCLUDED.statusid,
                    grid = EXCLUDED.grid,
                    position = EXCLUDED.position,
                    laps = EXCLUDED.laps,
                    milliseconds = EXCLUDED.milliseconds,
                    fastestlaptime = EXCLUDED.fastestlaptime,
                    fastestlapspeed = EXCLUDED.fastestlapspeed,
                    rank = EXCLUDED.rank;
            """))
        logger.info("fact_results završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju fact_results: {e}")
        raise

def load_fact_lap_times(engine):
    logger.info("Ubacujem gold.fact_lap_times...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO gold.fact_lap_times (raceid, driverid, lap, position_laptimes, time_laptimes, milliseconds_laptimes)
                SELECT DISTINCT ON (s.raceid, s.driverid, s.lap)
                    s.raceid, s.driverid, s.lap,
                    s.position_laptimes, s.time_laptimes, s.milliseconds_laptimes
                FROM silver.raw_data s
                WHERE s.lap IS NOT NULL
                ORDER BY s.raceid, s.driverid, s.lap
                ON CONFLICT (raceid, driverid, lap) DO UPDATE SET
                    position_laptimes = EXCLUDED.position_laptimes,
                    time_laptimes = EXCLUDED.time_laptimes,
                    milliseconds_laptimes = EXCLUDED.milliseconds_laptimes;
            """))
        logger.info("fact_lap_times završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju fact_lap_times: {e}")
        raise

def load_fact_pit_stops(engine):
    logger.info("Ubacujem gold.fact_pit_stops...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO gold.fact_pit_stops (raceid, driverid, stop, lap_pitstops, time_pitstops, duration, milliseconds_pitstops)
                SELECT DISTINCT ON (s.raceid, s.driverid, s.stop)
                    s.raceid, s.driverid, s.stop,
                    s.lap_pitstops, s.time_pitstops, s.duration, s.milliseconds_pitstops
                FROM silver.raw_data s
                WHERE s.stop IS NOT NULL
                ORDER BY s.raceid, s.driverid, s.stop
                ON CONFLICT (raceid, driverid, stop) DO UPDATE SET
                    lap_pitstops = EXCLUDED.lap_pitstops,
                    time_pitstops = EXCLUDED.time_pitstops,
                    duration = EXCLUDED.duration,
                    milliseconds_pitstops = EXCLUDED.milliseconds_pitstops;
            """))
        logger.info("fact_pit_stops završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju fact_pit_stops: {e}")
        raise

def load_fact_driver_standings(engine):
    logger.info("Ubacujem gold.fact_driver_standings...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO gold.fact_driver_standings (driverstandingsid, raceid, driverid,
                    points_driverstandings, position_driverstandings, wins)
                SELECT DISTINCT ON (s.driverstandingsid)
                    s.driverstandingsid, s.raceid, s.driverid,
                    s.points_driverstandings, s.position_driverstandings, s.wins
                FROM silver.raw_data s
                WHERE s.driverstandingsid IS NOT NULL
                ORDER BY s.driverstandingsid
                ON CONFLICT (driverstandingsid) DO UPDATE SET
                    raceid = EXCLUDED.raceid,
                    driverid = EXCLUDED.driverid,
                    points_driverstandings = EXCLUDED.points_driverstandings,
                    position_driverstandings = EXCLUDED.position_driverstandings,
                    wins = EXCLUDED.wins;
            """))
        logger.info("fact_driver_standings završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju fact_driver_standings: {e}")
        raise

def load_fact_constructor_standings(engine):
    logger.info("Ubacujem gold.fact_constructor_standings...")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO gold.fact_constructor_standings (constructorstandingsid, raceid, constructorid,
                    points_constructorstandings, position_constructorstandings, wins_constructorstandings)
                SELECT DISTINCT ON (s.constructorstandingsid)
                    s.constructorstandingsid, s.raceid, s.constructorid,
                    s.points_constructorstandings, s.position_constructorstandings,
                    s.wins_constructorstandings
                FROM silver.raw_data s
                WHERE s.constructorstandingsid IS NOT NULL
                ORDER BY s.constructorstandingsid
                ON CONFLICT (constructorstandingsid) DO UPDATE SET
                    raceid = EXCLUDED.raceid,
                    constructorid = EXCLUDED.constructorid,
                    points_constructorstandings = EXCLUDED.points_constructorstandings,
                    position_constructorstandings = EXCLUDED.position_constructorstandings,
                    wins_constructorstandings = EXCLUDED.wins_constructorstandings;
            """))
        logger.info("fact_constructor_standings završen!")
    except Exception as e:
        logger.error(f"Greška pri punjenju fact_constructor_standings: {e}")
        raise