import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

def run_silver_dq_checks(engine):
    errors = []

    with engine.connect() as conn:

        # ============================================================
        # 1. ROW COUNT CHECK
        # ============================================================
        logger.info("Provjeravamo row count silver tabele...")

        result = conn.execute(text("SELECT COUNT(*) FROM silver.raw_data")).scalar()
        if result != 518417:
            errors.append(f"ROW COUNT — silver.raw_data: očekivano 518417, pronađeno {result}")
        else:
            logger.info(f"ROW COUNT silver.raw_data: {result}")

        # ============================================================
        # 2. NULL ARTIFACT CHECKS
        # ============================================================
        logger.info("Provjeravamo da nema \\N artefakata...")

        text_columns = ['number', 'positiontext', 'time', 'fastestlaptime',
                        'circuitref', 'name_x', 'name_y', 'location', 'country',
                        'driverref', 'code', 'forename', 'surname', 'nationality',
                        'constructorref', 'name', 'nationality_constructors',
                        'time_laptimes', 'time_pitstops', 'duration', 'status']

        for col in text_columns:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM silver.raw_data
                WHERE {col} = '\\N'
            """)).scalar()
            if result > 0:
                errors.append(f"NULL ARTIFACT — silver.raw_data.{col}: pronađeno {result} '\\N' artefakata!")
            else:
                logger.info(f"NULL ARTIFACT silver.raw_data.{col}: nema artefakata")

        # ============================================================
        # 3. NULL CHECKS — ključne kolone
        # ============================================================
        logger.info("Provjeravamo NULL vrijednosti u ključnim kolonama...")

        null_checks = [
            'resultid', 'raceid', 'driverid', 'constructorid', 'statusid',
            'year', 'round', 'circuitid'
        ]

        for col in null_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM silver.raw_data WHERE {col} IS NULL
            """)).scalar()
            if result > 0:
                errors.append(f"NULL CHECK — silver.raw_data.{col}: pronađeno {result} NULL vrijednosti!")
            else:
                logger.info(f"NULL CHECK silver.raw_data.{col}: nema NULL-ova")

        # ============================================================
        # 4. NEGATIVE VALUE CHECKS
        # ============================================================
        logger.info("Provjeravamo negativne vrijednosti...")

        negative_checks = [
            'laps', 'milliseconds', 'rank', 'fastestlap',
            'lap', 'milliseconds_laptimes', 'position_laptimes',
            'stop', 'lap_pitstops', 'milliseconds_pitstops',
            'wins', 'wins_constructorstandings'
        ]

        for col in negative_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM silver.raw_data WHERE {col} < 0
            """)).scalar()
            if result > 0:
                errors.append(f"NEGATIVE — silver.raw_data.{col}: pronađeno {result} negativnih vrijednosti!")
            else:
                logger.info(f"NEGATIVE silver.raw_data.{col}: nema negativnih vrijednosti")

        # ============================================================
        # 5. RANGE CHECKS
        # ============================================================
        logger.info("Provjeravamo range vrijednosti...")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM silver.raw_data
            WHERE position IS NOT NULL AND (position < 1 OR position > 25)
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — silver.raw_data.position: {result} vrijednosti van opsega 1-25!")
        else:
            logger.info("RANGE silver.raw_data.position: sve vrijednosti u opsegu 1-25")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM silver.raw_data
            WHERE year < 2012 OR year > 2023
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — silver.raw_data.year: {result} vrijednosti van opsega 2012-2023!")
        else:
            logger.info("RANGE silver.raw_data.year: sve godine u opsegu 2012-2023")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM silver.raw_data
            WHERE round < 1 OR round > 25
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — silver.raw_data.round: {result} vrijednosti van opsega 1-25!")
        else:
            logger.info("RANGE silver.raw_data.round: sve runde u opsegu 1-25")

        # ============================================================
        # 6. DUPLICATE CHECKS
        # ============================================================
        logger.info("Provjeravamo duplikate...")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT resultid, COUNT(*) as cnt
                FROM silver.raw_data
                WHERE resultid IS NOT NULL
                GROUP BY resultid, raceid, driverid, lap, stop
                HAVING COUNT(*) > 1
            ) duplicates
        """)).scalar()
        if result > 0:
            errors.append(f"DUPLICATE — silver.raw_data: pronađeno {result} duplikata!")
        else:
            logger.info("DUPLICATE silver.raw_data: nema duplikata")

    # ============================================================
    # FINALNI REZULTAT
    # ============================================================
    if errors:
        logger.error(f"❌ SILVER DQ CHECKS FAILED — pronađeno {len(errors)} problema:")
        for error in errors:
            logger.error(f"  - {error}")
        raise ValueError(f"Silver Data Quality checks failed sa {len(errors)} problema!")
    else:
        logger.info("SVI SILVER DATA QUALITY CHECKS PROŠLI!")