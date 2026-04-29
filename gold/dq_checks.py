from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def run_dq_checks(engine):
    """
    Data Quality checks za gold layer sa SCD2 podrškom.
    
    SCD2 dimenzije (driver, constructor, circuit, status, date, race):
    - Row count provjerava SAMO is_current=TRUE zapise
    - Uniqueness provjerava surrogate key (driver_sk, constructor_sk, etc.)
    - Uniqueness provjerava da postoji samo 1 current zapis po natural key
    
    Fact tabele:
    - Bez promjena (koriste natural keys)
    """
    errors = []
    
    # ============================================================
    # 1. ROW COUNTS (SCD2: samo current zapisi)
    # ============================================================
    logger.info("Provjeravamo row counts (SCD2: is_current=TRUE)...")
    row_count_checks = {
        # SCD2 dimenzije - provjeravamo samo current zapise
        "gold.dim_driver (current)": ("SELECT COUNT(*) FROM gold.dim_driver WHERE is_current = TRUE", 65),
        "gold.dim_constructor (current)": ("SELECT COUNT(*) FROM gold.dim_constructor WHERE is_current = TRUE", 20),
        "gold.dim_circuit (current)": ("SELECT COUNT(*) FROM gold.dim_circuit WHERE is_current = TRUE", 34),
        "gold.dim_race (current)": ("SELECT COUNT(*) FROM gold.dim_race WHERE is_current = TRUE", 232),
        "gold.dim_status (current)": ("SELECT COUNT(*) FROM gold.dim_status WHERE is_current = TRUE", 65),
        "gold.dim_date (current)": ("SELECT COUNT(*) FROM gold.dim_date WHERE is_current = TRUE", 232),
        
        # Fact tabele - bez promjena
        "gold.fact_results": ("SELECT COUNT(*) FROM gold.fact_results", 4502),
        "gold.fact_lap_times": ("SELECT COUNT(*) FROM gold.fact_lap_times", 256836),
        "gold.fact_pit_stops": ("SELECT COUNT(*) FROM gold.fact_pit_stops", 8975),
        "gold.fact_driver_standings": ("SELECT COUNT(*) FROM gold.fact_driver_standings", 4502),
        "gold.fact_constructor_standings": ("SELECT COUNT(*) FROM gold.fact_constructor_standings", 2398),
    }
    
    with engine.connect() as conn:
        for table_name, (query, expected) in row_count_checks.items():
            result = conn.execute(text(query)).scalar()
            if result != expected:
                error_msg = f"ROW COUNT — {table_name}: očekivano {expected}, pronađeno {result}"
                logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
            else:
                logger.info(f" ROW COUNT {table_name}: {result}")
    
    # ============================================================
    # 2. UNIQUENESS CHECKS
    # ============================================================
    logger.info("Provjeravamo uniqueness (SCD2: surrogate keys i current records)...")
    
    uniqueness_checks = {
        # SCD2 dimenzije - surrogate key mora biti unique
        "gold.dim_driver.driver_sk": "SELECT COUNT(*) - COUNT(DISTINCT driver_sk) FROM gold.dim_driver",
        "gold.dim_constructor.constructor_sk": "SELECT COUNT(*) - COUNT(DISTINCT constructor_sk) FROM gold.dim_constructor",
        "gold.dim_circuit.circuit_sk": "SELECT COUNT(*) - COUNT(DISTINCT circuit_sk) FROM gold.dim_circuit",
        "gold.dim_status.status_sk": "SELECT COUNT(*) - COUNT(DISTINCT status_sk) FROM gold.dim_status",
        "gold.dim_date.date_sk": "SELECT COUNT(*) - COUNT(DISTINCT date_sk) FROM gold.dim_date",
        "gold.dim_race.race_sk": "SELECT COUNT(*) - COUNT(DISTINCT race_sk) FROM gold.dim_race",
        
        # SCD2 dimenzije - samo 1 current zapis po natural key
        "gold.dim_driver (current per driverid)": """
            SELECT COUNT(*) FROM (
                SELECT driverid, COUNT(*) as cnt 
                FROM gold.dim_driver 
                WHERE is_current = TRUE 
                GROUP BY driverid 
                HAVING COUNT(*) > 1
            ) duplicates
        """,
        "gold.dim_constructor (current per constructorid)": """
            SELECT COUNT(*) FROM (
                SELECT constructorid, COUNT(*) as cnt 
                FROM gold.dim_constructor 
                WHERE is_current = TRUE 
                GROUP BY constructorid 
                HAVING COUNT(*) > 1
            ) duplicates
        """,
        
        # Fact tabele - PK uniqueness
        "gold.fact_results.resultid": "SELECT COUNT(*) - COUNT(DISTINCT resultid) FROM gold.fact_results",
        "gold.fact_driver_standings.driverstandingsid": "SELECT COUNT(*) - COUNT(DISTINCT driverstandingsid) FROM gold.fact_driver_standings",
        "gold.fact_constructor_standings.constructorstandingsid": "SELECT COUNT(*) - COUNT(DISTINCT constructorstandingsid) FROM gold.fact_constructor_standings",
    }
    
    with engine.connect() as conn:
        for check_name, query in uniqueness_checks.items():
            duplicates = conn.execute(text(query)).scalar()
            if duplicates > 0:
                error_msg = f"UNIQUENESS — {check_name}: pronađeno {duplicates} duplikata!"
                logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
            else:
                logger.info(f"UNIQUENESS {check_name}: nema duplikata")
    
    # ============================================================
    # 3. NULL CHECKS (samo current zapisi za SCD2)
    # ============================================================
    logger.info("Provjeravamo NULL vrijednosti...")
    
    null_checks = {
        # SCD2 dimenzije - current zapisi
        "gold.dim_driver.driverid": "SELECT COUNT(*) FROM gold.dim_driver WHERE driverid IS NULL AND is_current = TRUE",
        "gold.dim_driver.surname": "SELECT COUNT(*) FROM gold.dim_driver WHERE surname IS NULL AND is_current = TRUE",
        "gold.dim_constructor.constructorid": "SELECT COUNT(*) FROM gold.dim_constructor WHERE constructorid IS NULL AND is_current = TRUE",
        "gold.dim_constructor.name": "SELECT COUNT(*) FROM gold.dim_constructor WHERE name IS NULL AND is_current = TRUE",
        "gold.dim_circuit.circuitid": "SELECT COUNT(*) FROM gold.dim_circuit WHERE circuitid IS NULL AND is_current = TRUE",
        "gold.dim_race.raceid": "SELECT COUNT(*) FROM gold.dim_race WHERE raceid IS NULL AND is_current = TRUE",
        "gold.dim_race.date": "SELECT COUNT(*) FROM gold.dim_race WHERE date IS NULL AND is_current = TRUE",
        
        # Fact tabele
        "gold.fact_results.resultid": "SELECT COUNT(*) FROM gold.fact_results WHERE resultid IS NULL",
        "gold.fact_results.raceid": "SELECT COUNT(*) FROM gold.fact_results WHERE raceid IS NULL",
        "gold.fact_results.driverid": "SELECT COUNT(*) FROM gold.fact_results WHERE driverid IS NULL",
        "gold.fact_lap_times.raceid": "SELECT COUNT(*) FROM gold.fact_lap_times WHERE raceid IS NULL",
        "gold.fact_lap_times.driverid": "SELECT COUNT(*) FROM gold.fact_lap_times WHERE driverid IS NULL",
        "gold.fact_lap_times.lap": "SELECT COUNT(*) FROM gold.fact_lap_times WHERE lap IS NULL",
    }
    
    with engine.connect() as conn:
        for column, query in null_checks.items():
            null_count = conn.execute(text(query)).scalar()
            if null_count > 0:
                error_msg = f"NULL CHECK — {column}: pronađeno {null_count} NULL vrijednosti!"
                logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
            else:
                logger.info(f"NULL CHECK {column}: nema NULL-ova")
    
    # ============================================================
    # 4. NEGATIVE CHECKS
    # ============================================================
    logger.info("Provjeravamo negativne vrijednosti...")
    
    negative_checks = {
        "gold.fact_results.laps": "SELECT COUNT(*) FROM gold.fact_results WHERE laps < 0",
        "gold.fact_results.milliseconds": "SELECT COUNT(*) FROM gold.fact_results WHERE milliseconds < 0",
        "gold.fact_results.rank": "SELECT COUNT(*) FROM gold.fact_results WHERE rank < 0",
        "gold.fact_lap_times.lap": "SELECT COUNT(*) FROM gold.fact_lap_times WHERE lap < 0",
        "gold.fact_lap_times.milliseconds_laptimes": "SELECT COUNT(*) FROM gold.fact_lap_times WHERE milliseconds_laptimes < 0",
        "gold.fact_pit_stops.stop": "SELECT COUNT(*) FROM gold.fact_pit_stops WHERE stop < 0",
        "gold.fact_pit_stops.milliseconds_pitstops": "SELECT COUNT(*) FROM gold.fact_pit_stops WHERE milliseconds_pitstops < 0",
        "gold.fact_driver_standings.points_driverstandings": "SELECT COUNT(*) FROM gold.fact_driver_standings WHERE points_driverstandings < 0",
        "gold.fact_driver_standings.wins": "SELECT COUNT(*) FROM gold.fact_driver_standings WHERE wins < 0",
        "gold.fact_constructor_standings.points_constructorstandings": "SELECT COUNT(*) FROM gold.fact_constructor_standings WHERE points_constructorstandings < 0",
        "gold.fact_constructor_standings.wins_constructorstandings": "SELECT COUNT(*) FROM gold.fact_constructor_standings WHERE wins_constructorstandings < 0",
    }
    
    with engine.connect() as conn:
        for column, query in negative_checks.items():
            negative_count = conn.execute(text(query)).scalar()
            if negative_count > 0:
                error_msg = f"NEGATIVE — {column}: pronađeno {negative_count} negativnih vrijednosti!"
                logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
            else:
                logger.info(f"NEGATIVE {column}: nema negativnih vrijednosti")
    
    # ============================================================
    # 5. RANGE CHECKS
    # ============================================================
    logger.info("Provjeravamo range vrijednosti...")
    
    with engine.connect() as conn:
        # Position range (1-25)
        invalid_position = conn.execute(text("""
            SELECT COUNT(*) FROM gold.fact_results 
            WHERE position IS NOT NULL 
            AND (position < 1 OR position > 25)
        """)).scalar()
        if invalid_position > 0:
            error_msg = f"RANGE — fact_results.position: {invalid_position} zapisa van opsega 1-25"
            logger.error(f"❌ {error_msg}")
            errors.append(error_msg)
        else:
            logger.info("RANGE fact_results.position: sve vrijednosti u opsegu 1-25")
        
        # Year range (2012-2023)
        invalid_year = conn.execute(text("""
            SELECT COUNT(*) FROM gold.dim_race 
            WHERE is_current = TRUE 
            AND (year < 2012 OR year > 2023)
        """)).scalar()
        if invalid_year > 0:
            error_msg = f"RANGE — dim_race.year: {invalid_year} zapisa van opsega 2012-2023"
            logger.error(f"❌ {error_msg}")
            errors.append(error_msg)
        else:
            logger.info("RANGE dim_race.year: sve godine u opsegu 2012-2023")
        
        # Round range (1-25)
        invalid_round = conn.execute(text("""
            SELECT COUNT(*) FROM gold.dim_race 
            WHERE is_current = TRUE 
            AND (round < 1 OR round > 25)
        """)).scalar()
        if invalid_round > 0:
            error_msg = f"RANGE — dim_race.round: {invalid_round} zapisa van opsega 1-25"
            logger.error(f"❌ {error_msg}")
            errors.append(error_msg)
        else:
            logger.info("RANGE dim_race.round: sve runde u opsegu 1-25")
    
    # ============================================================
    # 6. REFERENTIAL INTEGRITY (natural keys sa current zapisi)
    # ============================================================
    logger.info("Provjeravamo referencijalnu integritet (SCD2: natural keys)...")
    
    fk_checks = {
        "gold.fact_results.raceid → gold.dim_race": """
            SELECT COUNT(*) FROM gold.fact_results fr
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_race r 
                WHERE r.raceid = fr.raceid AND r.is_current = TRUE
            )
        """,
        "gold.fact_results.driverid → gold.dim_driver": """
            SELECT COUNT(*) FROM gold.fact_results fr
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_driver d 
                WHERE d.driverid = fr.driverid AND d.is_current = TRUE
            )
        """,
        "gold.fact_results.constructorid → gold.dim_constructor": """
            SELECT COUNT(*) FROM gold.fact_results fr
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_constructor c 
                WHERE c.constructorid = fr.constructorid AND c.is_current = TRUE
            )
        """,
        "gold.fact_results.statusid → gold.dim_status": """
            SELECT COUNT(*) FROM gold.fact_results fr
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_status s 
                WHERE s.statusid = fr.statusid AND s.is_current = TRUE
            )
        """,
        "gold.fact_lap_times.raceid → gold.dim_race": """
            SELECT COUNT(*) FROM gold.fact_lap_times flt
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_race r 
                WHERE r.raceid = flt.raceid AND r.is_current = TRUE
            )
        """,
        "gold.fact_lap_times.driverid → gold.dim_driver": """
            SELECT COUNT(*) FROM gold.fact_lap_times flt
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_driver d 
                WHERE d.driverid = flt.driverid AND d.is_current = TRUE
            )
        """,
    }
    
    with engine.connect() as conn:
        for fk_name, query in fk_checks.items():
            orphans = conn.execute(text(query)).scalar()
            if orphans > 0:
                error_msg = f"FK — {fk_name}: pronađeno {orphans} orphan zapisa!"
                logger.error(f"❌ {error_msg}")
                errors.append(error_msg)
            else:
                logger.info(f"FK {fk_name}: integritet ok")
    
    # ============================================================
    # FINAL REPORT
    # ============================================================
    if errors:
        logger.error(f"❌ DQ CHECKS FAILED — pronađeno {len(errors)} problema:")
        for error in errors:
            logger.error(f"  - {error}")
        raise ValueError(f"Data Quality checks failed sa {len(errors)} problema!")
    else:
        logger.info("SVI DQ CHECKS PROŠLI! Warehouse je u odličnom stanju!")