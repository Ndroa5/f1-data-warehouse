from sqlalchemy import text

def load_to_silver(engine):

    with engine.begin() as conn:

        print("Ubacujem silver.drivers...")
        conn.execute(text("""
            INSERT INTO silver.drivers
            SELECT DISTINCT ON (b.driverid)
                b.driverid::INTEGER,
                b.driverref,
                b.code,
                b.forename,
                b.surname,
                NULLIF(b.dob, '\\N')::DATE,
                b.nationality
            FROM bronze.raw_data b
            WHERE b.driverid IS NOT NULL AND b.driverid != '\\N';
        """))

        print("Ubacujem silver.constructors...")
        conn.execute(text("""
            INSERT INTO silver.constructors
            SELECT DISTINCT ON (b.constructorid)
                b.constructorid::INTEGER,
                b.constructorref,
                b.name,
                b.nationality_constructors
            FROM bronze.raw_data b
            WHERE b.constructorid IS NOT NULL AND b.constructorid != '\\N';
        """))

        print("Ubacujem silver.circuits...")
        conn.execute(text("""
            INSERT INTO silver.circuits
            SELECT DISTINCT ON (b.circuitid)
                b.circuitid::INTEGER,
                b.circuitref,
                b.name_y,
                b.location,
                b.country,
                NULLIF(b.lat, '\\N')::FLOAT,
                NULLIF(b.lng, '\\N')::FLOAT,
                NULLIF(b.alt, '\\N')::FLOAT
            FROM bronze.raw_data b
            WHERE b.circuitid IS NOT NULL AND b.circuitid != '\\N';
        """))

        print("Ubacujem silver.status...")
        conn.execute(text("""
            INSERT INTO silver.status
            SELECT DISTINCT ON (b.statusid)
                b.statusid::INTEGER,
                b.status
            FROM bronze.raw_data b
            WHERE b.statusid IS NOT NULL AND b.statusid != '\\N';
        """))

        print("Ubacujem silver.races...")
        conn.execute(text("""
            INSERT INTO silver.races
            SELECT DISTINCT ON (b.raceid)
                b.raceid::INTEGER,
                b.circuitid::INTEGER,
                b.year::INTEGER,
                b.round::INTEGER,
                b.name_x,
                NULLIF(b.date, '\\N')::DATE,
                NULLIF(b.time_races, '\\N')::TIME,
                NULLIF(b.fp1_date, '\\N')::DATE,
                NULLIF(b.fp1_time, '\\N')::TIME,
                NULLIF(b.fp2_date, '\\N')::DATE,
                NULLIF(b.fp2_time, '\\N')::TIME,
                NULLIF(b.fp3_date, '\\N')::DATE,
                NULLIF(b.fp3_time, '\\N')::TIME,
                NULLIF(b.quali_date, '\\N')::DATE,
                NULLIF(b.quali_time, '\\N')::TIME,
                NULLIF(b.sprint_date, '\\N')::DATE,
                NULLIF(b.sprint_time, '\\N')::TIME
            FROM bronze.raw_data b
            WHERE b.raceid IS NOT NULL AND b.raceid != '\\N';
        """))

        print("Ubacujem silver.results...")
        conn.execute(text("""
            INSERT INTO silver.results
            SELECT DISTINCT ON (b.resultid)
                b.resultid::INTEGER,
                b.raceid::INTEGER,
                b.driverid::INTEGER,
                b.constructorid::INTEGER,
                b.statusid::INTEGER,
                NULLIF(b.number, '\\N'),
                NULLIF(b.grid, '\\N')::INTEGER,
                NULLIF(b.position, '\\N')::NUMERIC,
                NULLIF(b.positiontext, '\\N'),
                NULLIF(b.positionorder, '\\N')::INTEGER,
                NULLIF(b.points, '\\N')::FLOAT,
                NULLIF(b.laps, '\\N')::INTEGER,
                NULLIF(b.time, '\\N'),
                NULLIF(b.milliseconds, '\\N')::INTEGER,
                NULLIF(b.fastestlap, '\\N')::INTEGER,
                NULLIF(b.rank, '\\N')::INTEGER,
                NULLIF(b.fastestlaptime, '\\N'),
                NULLIF(b.fastestlapspeed, '\\N')::FLOAT
            FROM bronze.raw_data b
            WHERE b.resultid IS NOT NULL AND b.resultid != '\\N';
        """))

        print("Ubacujem silver.lap_times...")
        conn.execute(text("""
            INSERT INTO silver.lap_times
            SELECT DISTINCT ON (b.raceid, b.driverid, b.lap)
                b.raceid::INTEGER,
                b.driverid::INTEGER,
                NULLIF(b.lap, '\\N')::INTEGER,
                NULLIF(b.position_laptimes, '\\N')::INTEGER,
                NULLIF(b.time_laptimes, '\\N'),
                NULLIF(b.milliseconds_laptimes, '\\N')::INTEGER
            FROM bronze.raw_data b
            WHERE b.lap IS NOT NULL AND b.lap != '\\N';
        """))

        print("Ubacujem silver.pit_stops...")
        conn.execute(text("""
            INSERT INTO silver.pit_stops
            SELECT DISTINCT ON (b.raceid, b.driverid, b.stop)
                b.raceid::INTEGER,
                b.driverid::INTEGER,
                NULLIF(b.stop, '\\N')::INTEGER,
                NULLIF(b.lap_pitstops, '\\N')::INTEGER,
                NULLIF(b.time_pitstops, '\\N'),
                NULLIF(b.duration, '\\N'),
                NULLIF(b.milliseconds_pitstops, '\\N')::INTEGER
            FROM bronze.raw_data b
            WHERE b.stop IS NOT NULL AND b.stop != '\\N';
        """))

        print("Ubacujem silver.driver_standings...")
        conn.execute(text("""
            INSERT INTO silver.driver_standings
            SELECT DISTINCT ON (b.driverstandingsid)
                NULLIF(b.driverstandingsid, '\\N')::INTEGER,
                b.raceid::INTEGER,
                b.driverid::INTEGER,
                NULLIF(b.points_driverstandings, '\\N')::FLOAT,
                NULLIF(b.position_driverstandings, '\\N')::INTEGER,
                NULLIF(b.positiontext_driverstandings, '\\N'),
                NULLIF(b.wins, '\\N')::INTEGER
            FROM bronze.raw_data b
            WHERE b.driverstandingsid IS NOT NULL AND b.driverstandingsid != '\\N';
        """))

        print("Ubacujem silver.constructor_standings...")
        conn.execute(text("""
            INSERT INTO silver.constructor_standings
            SELECT DISTINCT ON (b.constructorstandingsid)
                NULLIF(b.constructorstandingsid, '\\N')::INTEGER,
                b.raceid::INTEGER,
                b.constructorid::INTEGER,
                NULLIF(b.points_constructorstandings, '\\N')::FLOAT,
                NULLIF(b.position_constructorstandings, '\\N')::INTEGER,
                NULLIF(b.positiontext_constructorstandings, '\\N'),
                NULLIF(b.wins_constructorstandings, '\\N')::INTEGER
            FROM bronze.raw_data b
            WHERE b.constructorstandingsid IS NOT NULL AND b.constructorstandingsid != '\\N';
        """))

    print("✅ Bronze → Silver završeno!")