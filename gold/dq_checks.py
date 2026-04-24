import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

def run_dq_checks(engine):
    errors = []

    with engine.connect() as conn:

        # ============================================================
        # 1. ROW COUNT CHECKS
        # ============================================================
        logger.info("Provjeravamo row counts...")

        counts = {
            'dim_driver': 65,
            'dim_constructor': 20,
            'dim_circuit': 34,
            'dim_race': 232,
            'dim_status': 65,
            'fact_results': 4502,
            'fact_lap_times': 256836,
            'fact_pit_stops': 8975,
            'fact_driver_standings': 4502,
            'fact_constructor_standings': 2398,
        }

        for table, expected in counts.items():
            result = conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()
            if result != expected:
                errors.append(f"ROW COUNT — gold.{table}: očekivano {expected}, pronađeno {result}")
            else:
                logger.info(f"✅ ROW COUNT gold.{table}: {result}")

        # ============================================================
        # 2. UNIQUENESS CHECKS
        # ============================================================
        logger.info("Provjeravamo duplikate u PK kolonama...")

        uniqueness_checks = [
            ("gold.dim_driver", "driverid"),
            ("gold.dim_constructor", "constructorid"),
            ("gold.dim_circuit", "circuitid"),
            ("gold.dim_race", "raceid"),
            ("gold.dim_status", "statusid"),
            ("gold.dim_date", "dateid"),
            ("gold.fact_results", "resultid"),
            ("gold.fact_driver_standings", "driverstandingsid"),
            ("gold.fact_constructor_standings", "constructorstandingsid"),
        ]

        for table, pk in uniqueness_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {pk}
                    HAVING COUNT(*) > 1
                ) duplicates
            """)).scalar()
            if result > 0:
                errors.append(f"UNIQUENESS — {table}.{pk}: pronađeno {result} duplikata!")
            else:
                logger.info(f"✅ UNIQUENESS {table}.{pk}: nema duplikata")

        # ============================================================
        # 3. NULL CHECKS
        # ============================================================
        logger.info("Provjeravamo NULL vrijednosti u ključnim kolonama...")

        null_checks = [
            ("gold.dim_driver", "driverid"),
            ("gold.dim_driver", "surname"),
            ("gold.dim_constructor", "constructorid"),
            ("gold.dim_constructor", "name"),
            ("gold.dim_circuit", "circuitid"),
            ("gold.dim_race", "raceid"),
            ("gold.dim_race", "date"),
            ("gold.fact_results", "resultid"),
            ("gold.fact_results", "raceid"),
            ("gold.fact_results", "driverid"),
            ("gold.fact_lap_times", "raceid"),
            ("gold.fact_lap_times", "driverid"),
            ("gold.fact_lap_times", "lap"),
        ]

        for table, column in null_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
            """)).scalar()
            if result > 0:
                errors.append(f"NULL CHECK — {table}.{column}: pronađeno {result} NULL vrijednosti!")
            else:
                logger.info(f"✅ NULL CHECK {table}.{column}: nema NULL-ova")

        # ============================================================
        # 4. NEGATIVE VALUE CHECKS
        # ============================================================
        logger.info("Provjeravamo negativne vrijednosti...")

        negative_checks = [
            ("gold.fact_results", "laps"),
            ("gold.fact_results", "milliseconds"),
            ("gold.fact_results", "rank"),
            ("gold.fact_lap_times", "lap"),
            ("gold.fact_lap_times", "milliseconds_laptimes"),
            ("gold.fact_pit_stops", "stop"),
            ("gold.fact_pit_stops", "milliseconds_pitstops"),
            ("gold.fact_driver_standings", "points_driverstandings"),
            ("gold.fact_driver_standings", "wins"),
            ("gold.fact_constructor_standings", "points_constructorstandings"),
            ("gold.fact_constructor_standings", "wins_constructorstandings"),
        ]

        for table, column in negative_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table} WHERE {column} < 0
            """)).scalar()
            if result > 0:
                errors.append(f"NEGATIVE — {table}.{column}: pronađeno {result} negativnih vrijednosti!")
            else:
                logger.info(f"✅ NEGATIVE {table}.{column}: nema negativnih vrijednosti")

        # ============================================================
        # 5. RANGE CHECKS
        # ============================================================
        logger.info("Provjeravamo range vrijednosti...")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM gold.fact_results
            WHERE position IS NOT NULL AND (position < 1 OR position > 25)
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — fact_results.position: {result} vrijednosti van opsega 1-25!")
        else:
            logger.info("✅ RANGE fact_results.position: sve vrijednosti u opsegu 1-25")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM gold.dim_race
            WHERE year < 2012 OR year > 2023
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — dim_race.year: {result} vrijednosti van opsega 2012-2023!")
        else:
            logger.info("✅ RANGE dim_race.year: sve godine u opsegu 2012-2023")

        result = conn.execute(text("""
            SELECT COUNT(*) FROM gold.dim_race
            WHERE round < 1 OR round > 25
        """)).scalar()
        if result > 0:
            errors.append(f"RANGE — dim_race.round: {result} vrijednosti van opsega 1-25!")
        else:
            logger.info("✅ RANGE dim_race.round: sve runde u opsegu 1-25")

        # ============================================================
        # 6. REFERENTIAL INTEGRITY CHECKS
        # ============================================================
        logger.info("Provjeravamo referencijalnu integritet...")

        fk_checks = [
            ("gold.fact_results", "raceid", "gold.dim_race", "raceid"),
            ("gold.fact_results", "driverid", "gold.dim_driver", "driverid"),
            ("gold.fact_results", "constructorid", "gold.dim_constructor", "constructorid"),
            ("gold.fact_results", "statusid", "gold.dim_status", "statusid"),
            ("gold.fact_lap_times", "raceid", "gold.dim_race", "raceid"),
            ("gold.fact_lap_times", "driverid", "gold.dim_driver", "driverid"),
            ("gold.fact_pit_stops", "raceid", "gold.dim_race", "raceid"),
            ("gold.fact_pit_stops", "driverid", "gold.dim_driver", "driverid"),
            ("gold.fact_driver_standings", "raceid", "gold.dim_race", "raceid"),
            ("gold.fact_driver_standings", "driverid", "gold.dim_driver", "driverid"),
            ("gold.fact_constructor_standings", "raceid", "gold.dim_race", "raceid"),
            ("gold.fact_constructor_standings", "constructorid", "gold.dim_constructor", "constructorid"),
        ]

        for fact_table, fk_col, dim_table, pk_col in fk_checks:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {fact_table} f
                LEFT JOIN {dim_table} d ON f.{fk_col} = d.{pk_col}
                WHERE d.{pk_col} IS NULL AND f.{fk_col} IS NOT NULL
            """)).scalar()
            if result > 0:
                errors.append(f"FK — {fact_table}.{fk_col} → {dim_table}: {result} orphaned redova!")
            else:
                logger.info(f"✅ FK {fact_table}.{fk_col} → {dim_table}: integritet ok")

    # ============================================================
    # FINALNI REZULTAT
    # ============================================================
    if errors:
        logger.error(f"❌ DQ CHECKS FAILED — pronađeno {len(errors)} problema:")
        for error in errors:
            logger.error(f"  - {error}")
        raise ValueError(f"Data Quality checks failed sa {len(errors)} problema!")
    else:
        logger.info("✅ SVI DATA QUALITY CHECKS PROŠLI!")