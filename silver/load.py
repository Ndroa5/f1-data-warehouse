from sqlalchemy import text
import logging
import hashlib

logger = logging.getLogger(__name__)

def calculate_driver_hash(forename, surname, nationality, number, code):
    """
    Kalkuliše MD5 hash za dim_driver tracking.
    Koristi se za testiranje/debugging - produkcijski hash je u SQL.
    """
    hash_string = f"{forename or ''}|{surname or ''}|{nationality or ''}|{number or ''}|{code or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def calculate_constructor_hash(name, nationality_constructors):
    """
    Kalkuliše MD5 hash za dim_constructor tracking.
    Koristi se za testiranje/debugging - produkcijski hash je u SQL.
    """
    hash_string = f"{name or ''}|{nationality_constructors or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def calculate_circuit_hash(circuitref, name, location, country):
    """
    Kalkuliše MD5 hash za dim_circuit tracking.
    Koristi se za testiranje/debugging - produkcijski hash je u SQL.
    """
    hash_string = f"{circuitref or ''}|{name or ''}|{location or ''}|{country or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def calculate_status_hash(status):
    """
    Kalkuliše MD5 hash za dim_status tracking.
    """
    hash_string = f"{status or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def calculate_date_hash(date):
    """
    Kalkuliše MD5 hash za dim_date tracking.
    """
    hash_string = f"{date or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def calculate_race_hash(race_name, date, time_races):
    """
    Kalkuliše MD5 hash za dim_race tracking.
    """
    hash_string = f"{race_name or ''}|{date or ''}|{time_races or ''}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def load_to_silver(engine):
    logger.info("Kreiram silver.raw_data tabelu sa SCD2 hash kolonama...")
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS silver.raw_data;"))
            conn.execute(text("""
                CREATE TABLE silver.raw_data AS
                SELECT
                    b.resultid::INTEGER,
                    b.raceid::INTEGER,
                    b.driverid::INTEGER,
                    b.constructorid::INTEGER,
                    b.statusid::INTEGER,
                    NULLIF(b.number, '\\N') AS number,
                    NULLIF(b.grid, '\\N')::INTEGER AS grid,
                    NULLIF(b.position, '\\N')::NUMERIC AS position,
                    NULLIF(b.positiontext, '\\N') AS positiontext,
                    NULLIF(b.positionorder, '\\N')::INTEGER AS positionorder,
                    NULLIF(b.points, '\\N')::FLOAT AS points,
                    NULLIF(b.laps, '\\N')::INTEGER AS laps,
                    NULLIF(b.time, '\\N') AS time,
                    NULLIF(b.milliseconds, '\\N')::INTEGER AS milliseconds,
                    NULLIF(b.fastestlap, '\\N')::INTEGER AS fastestlap,
                    NULLIF(b.rank, '\\N')::INTEGER AS rank,
                    NULLIF(b.fastestlaptime, '\\N') AS fastestlaptime,
                    NULLIF(b.fastestlapspeed, '\\N')::FLOAT AS fastestlapspeed,
                    b.year::INTEGER,
                    b.round::INTEGER,
                    b.circuitid::INTEGER,
                    b.name_x,
                    NULLIF(b.date, '\\N')::DATE AS date,
                    NULLIF(b.time_races, '\\N')::TIME AS time_races,
                    NULLIF(b.fp1_date, '\\N')::DATE AS fp1_date,
                    NULLIF(b.fp1_time, '\\N')::TIME AS fp1_time,
                    NULLIF(b.fp2_date, '\\N')::DATE AS fp2_date,
                    NULLIF(b.fp2_time, '\\N')::TIME AS fp2_time,
                    NULLIF(b.fp3_date, '\\N')::DATE AS fp3_date,
                    NULLIF(b.fp3_time, '\\N')::TIME AS fp3_time,
                    NULLIF(b.quali_date, '\\N')::DATE AS quali_date,
                    NULLIF(b.quali_time, '\\N')::TIME AS quali_time,
                    NULLIF(b.sprint_date, '\\N')::DATE AS sprint_date,
                    NULLIF(b.sprint_time, '\\N')::TIME AS sprint_time,
                    b.circuitref,
                    b.name_y,
                    b.location,
                    b.country,
                    NULLIF(b.lat, '\\N')::FLOAT AS lat,
                    NULLIF(b.lng, '\\N')::FLOAT AS lng,
                    NULLIF(b.alt, '\\N')::FLOAT AS alt,
                    b.driverref,
                    NULLIF(b.number_drivers, '\\N') AS number_drivers,
                    b.code,
                    b.forename,
                    b.surname,
                    NULLIF(b.dob, '\\N')::DATE AS dob,
                    b.nationality,
                    b.constructorref,
                    b.name,
                    b.nationality_constructors,
                    NULLIF(b.lap, '\\N')::INTEGER AS lap,
                    NULLIF(b.position_laptimes, '\\N')::INTEGER AS position_laptimes,
                    NULLIF(b.time_laptimes, '\\N') AS time_laptimes,
                    NULLIF(b.milliseconds_laptimes, '\\N')::INTEGER AS milliseconds_laptimes,
                    NULLIF(b.stop, '\\N')::INTEGER AS stop,
                    NULLIF(b.lap_pitstops, '\\N')::INTEGER AS lap_pitstops,
                    NULLIF(b.time_pitstops, '\\N') AS time_pitstops,
                    NULLIF(b.duration, '\\N') AS duration,
                    NULLIF(b.milliseconds_pitstops, '\\N')::INTEGER AS milliseconds_pitstops,
                    NULLIF(b.driverstandingsid, '\\N')::INTEGER AS driverstandingsid,
                    NULLIF(b.points_driverstandings, '\\N')::FLOAT AS points_driverstandings,
                    NULLIF(b.position_driverstandings, '\\N')::INTEGER AS position_driverstandings,
                    NULLIF(b.positiontext_driverstandings, '\\N') AS positiontext_driverstandings,
                    NULLIF(b.wins, '\\N')::INTEGER AS wins,
                    NULLIF(b.constructorstandingsid, '\\N')::INTEGER AS constructorstandingsid,
                    NULLIF(b.points_constructorstandings, '\\N')::FLOAT AS points_constructorstandings,
                    NULLIF(b.position_constructorstandings, '\\N')::INTEGER AS position_constructorstandings,
                    NULLIF(b.positiontext_constructorstandings, '\\N') AS positiontext_constructorstandings,
                    NULLIF(b.wins_constructorstandings, '\\N')::INTEGER AS wins_constructorstandings,
                    b.status,
                    
                    -- ============================================================
                    -- SCD2 HASH KOLONE (dodato za change detection)
                    -- ============================================================
                    
                    -- Hash za dim_driver (trackuje: forename, surname, nationality, number, code)
                    MD5(CONCAT(
                        COALESCE(b.forename, ''), '|',
                        COALESCE(b.surname, ''), '|',
                        COALESCE(b.nationality, ''), '|',
                        COALESCE(NULLIF(b.number_drivers, '\\N'), ''), '|',
                        COALESCE(b.code, '')
                    )) AS row_hash_driver,
                    
                    -- Hash za dim_constructor (trackuje: name, nationality_constructors)
                    MD5(CONCAT(
                        COALESCE(b.name, ''), '|',
                        COALESCE(b.nationality_constructors, '')
                    )) AS row_hash_constructor,
                    
                    -- Hash za dim_circuit (trackuje: circuitref, name_y, location, country)
                    MD5(CONCAT(
                        COALESCE(b.circuitref, ''), '|',
                        COALESCE(b.name_y, ''), '|',
                        COALESCE(b.location, ''), '|',
                        COALESCE(b.country, '')
                    )) AS row_hash_circuit,
                    
                    -- Hash za dim_status (trackuje: status)
                    MD5(COALESCE(b.status, '')) AS row_hash_status,
                    
                    -- Hash za dim_date (trackuje: full_date - immutable ali za konzistentnost)
                    MD5(CONCAT(
                        COALESCE(NULLIF(b.date, '\\N')::TEXT, '')
                    )) AS row_hash_date,
                    
                    -- Hash za dim_race (trackuje: race_name, date, time_races)
                    MD5(CONCAT(
                        COALESCE(b.name_x, ''), '|',
                        COALESCE(NULLIF(b.date, '\\N')::TEXT, ''), '|',
                        COALESCE(NULLIF(b.time_races, '\\N')::TEXT, '')
                    )) AS row_hash_race
                    
                FROM bronze.raw_data b;
            """))
        logger.info("Silver load završen sa hash kolonama!")
        
        # Log sample hash vrijednosti za verifikaciju
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    forename, surname, nationality, row_hash_driver,
                    name AS constructor_name, row_hash_constructor,
                    name_y AS circuit_name, row_hash_circuit
                FROM silver.raw_data 
                LIMIT 3;
            """))
            logger.info("Sample hash vrijednosti:")
            for row in result:
                logger.info(f"  Driver: {row.forename} {row.surname} → {row.row_hash_driver[:8]}...")
                logger.info(f"  Constructor: {row.constructor_name} → {row.row_hash_constructor[:8]}...")
                logger.info(f"  Circuit: {row.circuit_name} → {row.row_hash_circuit[:8]}...")
            
    except Exception as e:
        logger.error(f"Greška pri kreiranju silver: {e}")
        raise