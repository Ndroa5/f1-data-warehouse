from sqlalchemy import text

def load_to_gold(engine):
    with engine.begin() as conn:

        print("Brišem stare gold podatke...")
        conn.execute(text("""
            TRUNCATE TABLE 
                gold.fact_constructor_standings,
                gold.fact_driver_standings,
                gold.fact_pit_stops,
                gold.fact_lap_times,
                gold.fact_results,
                gold.dim_race,
                gold.dim_date,
                gold.dim_status,
                gold.dim_circuit,
                gold.dim_constructor,
                gold.dim_driver
            CASCADE;
        """))

        print("Ubacujem gold.dim_driver...")
        conn.execute(text("""
            INSERT INTO gold.dim_driver
            SELECT DISTINCT ON (s.driverid)
                s.driverid, s.driverref, s.code, s.forename, s.surname, s.dob, s.nationality
            FROM silver.raw_data s
            WHERE s.driverid IS NOT NULL
            ORDER BY s.driverid;
        """))

        print("Ubacujem gold.dim_constructor...")
        conn.execute(text("""
            INSERT INTO gold.dim_constructor
            SELECT DISTINCT ON (s.constructorid)
                s.constructorid, s.constructorref, s.name, s.nationality_constructors
            FROM silver.raw_data s
            WHERE s.constructorid IS NOT NULL
            ORDER BY s.constructorid;
        """))

        print("Ubacujem gold.dim_circuit...")
        conn.execute(text("""
            INSERT INTO gold.dim_circuit
            SELECT DISTINCT ON (s.circuitid)
                s.circuitid, s.circuitref, s.name_y AS circuit_name,
                s.location, s.country, s.lat, s.lng, s.alt
            FROM silver.raw_data s
            WHERE s.circuitid IS NOT NULL
            ORDER BY s.circuitid;
        """))

        print("Ubacujem gold.dim_status...")
        conn.execute(text("""
            INSERT INTO gold.dim_status
            SELECT DISTINCT ON (s.statusid)
                s.statusid, s.status
            FROM silver.raw_data s
            WHERE s.statusid IS NOT NULL
            ORDER BY s.statusid;
        """))

        print("Ubacujem gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date
            SELECT
                ROW_NUMBER() OVER (ORDER BY full_date) AS dateid,
                full_date,
                EXTRACT(YEAR FROM full_date)::INTEGER AS year,
                EXTRACT(MONTH FROM full_date)::INTEGER AS month,
                TO_CHAR(full_date, 'Month') AS month_name,
                EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
                EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week,
                EXTRACT(DOW FROM full_date) IN (0, 6) AS is_weekend
            FROM (
                SELECT DISTINCT date AS full_date
                FROM silver.raw_data
                WHERE date IS NOT NULL
            ) dates
            ORDER BY full_date;
        """))

        print("Ubacujem gold.dim_race...")
        conn.execute(text("""
            INSERT INTO gold.dim_race
            SELECT DISTINCT ON (s.raceid)
                s.raceid, s.circuitid, s.year, s.round, s.name_x AS race_name,
                s.date, s.time_races, s.fp1_date, s.fp1_time, s.fp2_date, s.fp2_time,
                s.fp3_date, s.fp3_time, s.quali_date, s.quali_time,
                s.sprint_date, s.sprint_time, NULL AS dateid
            FROM silver.raw_data s
            WHERE s.raceid IS NOT NULL
            ORDER BY s.raceid;
        """))

        conn.execute(text("""
            UPDATE gold.dim_race r
            SET dateid = d.dateid
            FROM gold.dim_date d
            WHERE r.date = d.full_date;
        """))

        print("Ubacujem gold.fact_results...")
        conn.execute(text("""
            INSERT INTO gold.fact_results
            SELECT DISTINCT ON (s.resultid)
                s.resultid, s.raceid, s.driverid, s.constructorid, s.statusid,
                s.grid, s.position, s.laps, s.milliseconds,
                s.fastestlaptime, s.fastestlapspeed, s.rank
            FROM silver.raw_data s
            WHERE s.resultid IS NOT NULL
            ORDER BY s.resultid;
        """))

        print("Ubacujem gold.fact_lap_times...")
        conn.execute(text("""
            INSERT INTO gold.fact_lap_times
            SELECT DISTINCT ON (s.raceid, s.driverid, s.lap)
                s.raceid, s.driverid, s.lap,
                s.position_laptimes, s.time_laptimes, s.milliseconds_laptimes
            FROM silver.raw_data s
            WHERE s.lap IS NOT NULL
            ORDER BY s.raceid, s.driverid, s.lap;
        """))

        print("Ubacujem gold.fact_pit_stops...")
        conn.execute(text("""
            INSERT INTO gold.fact_pit_stops
            SELECT DISTINCT ON (s.raceid, s.driverid, s.stop)
                s.raceid, s.driverid, s.stop,
                s.lap_pitstops, s.time_pitstops, s.duration, s.milliseconds_pitstops
            FROM silver.raw_data s
            WHERE s.stop IS NOT NULL
            ORDER BY s.raceid, s.driverid, s.stop;
        """))

        print("Ubacujem gold.fact_driver_standings...")
        conn.execute(text("""
            INSERT INTO gold.fact_driver_standings
            SELECT DISTINCT ON (s.driverstandingsid)
                s.driverstandingsid, s.raceid, s.driverid,
                s.points_driverstandings, s.position_driverstandings, s.wins
            FROM silver.raw_data s
            WHERE s.driverstandingsid IS NOT NULL
            ORDER BY s.driverstandingsid;
        """))

        print("Ubacujem gold.fact_constructor_standings...")
        conn.execute(text("""
            INSERT INTO gold.fact_constructor_standings
            SELECT DISTINCT ON (s.constructorstandingsid)
                s.constructorstandingsid, s.raceid, s.constructorid,
                s.points_constructorstandings, s.position_constructorstandings,
                s.wins_constructorstandings
            FROM silver.raw_data s
            WHERE s.constructorstandingsid IS NOT NULL
            ORDER BY s.constructorstandingsid;
        """))

    print("Silver → Gold završeno!")