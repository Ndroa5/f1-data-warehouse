from sqlalchemy import text

def load_dim_driver(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_driver...")
        conn.execute(text("""
            INSERT INTO gold.dim_driver (driverid, driverref, code, forename, surname, dob, nationality)
            SELECT DISTINCT ON (s.driverid)
                s.driverid, s.driverref, s.code, s.forename, s.surname, s.dob, s.nationality
            FROM silver.raw_data s
            WHERE s.driverid IS NOT NULL
            ORDER BY s.driverid
            ON CONFLICT (driverid) DO UPDATE SET
                driverref = EXCLUDED.driverref,
                code = EXCLUDED.code,
                forename = EXCLUDED.forename,
                surname = EXCLUDED.surname,
                dob = EXCLUDED.dob,
                nationality = EXCLUDED.nationality;
        """))

def load_dim_constructor(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_constructor...")
        conn.execute(text("""
            INSERT INTO gold.dim_constructor (constructorid, constructorref, name, nationality_constructors)
            SELECT DISTINCT ON (s.constructorid)
                s.constructorid, s.constructorref, s.name, s.nationality_constructors
            FROM silver.raw_data s
            WHERE s.constructorid IS NOT NULL
            ORDER BY s.constructorid
            ON CONFLICT (constructorid) DO UPDATE SET
                constructorref = EXCLUDED.constructorref,
                name = EXCLUDED.name,
                nationality_constructors = EXCLUDED.nationality_constructors;
        """))

def load_dim_circuit(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_circuit...")
        conn.execute(text("""
            INSERT INTO gold.dim_circuit (circuitid, circuitref, circuit_name, location, country, lat, lng, alt)
            SELECT DISTINCT ON (s.circuitid)
                s.circuitid, s.circuitref, s.name_y AS circuit_name,
                s.location, s.country, s.lat, s.lng, s.alt
            FROM silver.raw_data s
            WHERE s.circuitid IS NOT NULL
            ORDER BY s.circuitid
            ON CONFLICT (circuitid) DO UPDATE SET
                circuitref = EXCLUDED.circuitref,
                circuit_name = EXCLUDED.circuit_name,
                location = EXCLUDED.location,
                country = EXCLUDED.country,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                alt = EXCLUDED.alt;
        """))

def load_dim_status(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_status...")
        conn.execute(text("""
            INSERT INTO gold.dim_status (statusid, status)
            SELECT DISTINCT ON (s.statusid)
                s.statusid, s.status
            FROM silver.raw_data s
            WHERE s.statusid IS NOT NULL
            ORDER BY s.statusid
            ON CONFLICT (statusid) DO UPDATE SET
                status = EXCLUDED.status;
        """))

def load_dim_date(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date (dateid, full_date, year, month, month_name, quarter, day_of_week, is_weekend)
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
            ORDER BY full_date
            ON CONFLICT (dateid) DO UPDATE SET
                full_date = EXCLUDED.full_date,
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                month_name = EXCLUDED.month_name,
                quarter = EXCLUDED.quarter,
                day_of_week = EXCLUDED.day_of_week,
                is_weekend = EXCLUDED.is_weekend;
        """))

def load_dim_race(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.dim_race...")
        conn.execute(text("""
            INSERT INTO gold.dim_race (raceid, circuitid, year, round, race_name, date, time_races,
                fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time,
                quali_date, quali_time, sprint_date, sprint_time, dateid)
            SELECT DISTINCT ON (s.raceid)
                s.raceid, s.circuitid, s.year, s.round, s.name_x AS race_name,
                s.date, s.time_races, s.fp1_date, s.fp1_time, s.fp2_date, s.fp2_time,
                s.fp3_date, s.fp3_time, s.quali_date, s.quali_time,
                s.sprint_date, s.sprint_time, NULL AS dateid
            FROM silver.raw_data s
            WHERE s.raceid IS NOT NULL
            ORDER BY s.raceid
            ON CONFLICT (raceid) DO UPDATE SET
                circuitid = EXCLUDED.circuitid,
                year = EXCLUDED.year,
                round = EXCLUDED.round,
                race_name = EXCLUDED.race_name,
                date = EXCLUDED.date,
                time_races = EXCLUDED.time_races,
                fp1_date = EXCLUDED.fp1_date,
                fp1_time = EXCLUDED.fp1_time,
                fp2_date = EXCLUDED.fp2_date,
                fp2_time = EXCLUDED.fp2_time,
                fp3_date = EXCLUDED.fp3_date,
                fp3_time = EXCLUDED.fp3_time,
                quali_date = EXCLUDED.quali_date,
                quali_time = EXCLUDED.quali_time,
                sprint_date = EXCLUDED.sprint_date,
                sprint_time = EXCLUDED.sprint_time;
        """))
        conn.execute(text("""
            UPDATE gold.dim_race r
            SET dateid = d.dateid
            FROM gold.dim_date d
            WHERE r.date = d.full_date;
        """))

def load_fact_results(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.fact_results...")
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

def load_fact_lap_times(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.fact_lap_times...")
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

def load_fact_pit_stops(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.fact_pit_stops...")
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

def load_fact_driver_standings(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.fact_driver_standings...")
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

def load_fact_constructor_standings(engine):
    with engine.begin() as conn:
        print("Ubacujem gold.fact_constructor_standings...")
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