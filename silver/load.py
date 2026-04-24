from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def load_to_silver(engine):
    logger.info("Kreiram silver.raw_data tabelu...")
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
                    b.status
                FROM bronze.raw_data b;
            """))
        logger.info("✅ Silver load završen!")
    except Exception as e:
        logger.error(f"Greška pri kreiranju silver: {e}")
        raise