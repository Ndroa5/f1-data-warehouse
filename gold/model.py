from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Float, Date, Time, Numeric, ForeignKey, Boolean

Base = declarative_base()

# ============================================================
# DIM TABELE - SCD TYPE 2
# ============================================================

class DimDriver(Base):
    __tablename__ = 'dim_driver'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    driver_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key - ostaje ali nije vise PK)
    driverid = Column(Integer, nullable=False)
    
    # Business atributi
    driverref = Column(String(100), nullable=True)
    code = Column(String(10), nullable=True)
    forename = Column(String(100), nullable=True)
    surname = Column(String(100), nullable=True)
    dob = Column(Date, nullable=True)
    nationality = Column(String(100), nullable=True)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

class DimConstructor(Base):
    __tablename__ = 'dim_constructor'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    constructor_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key)
    constructorid = Column(Integer, nullable=False)
    
    # Business atributi
    constructorref = Column(String(100), nullable=True)
    name = Column(String(200), nullable=True)
    nationality_constructors = Column(String(100), nullable=True)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

class DimCircuit(Base):
    __tablename__ = 'dim_circuit'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    circuit_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key)
    circuitid = Column(Integer, nullable=False)
    
    # Business atributi
    circuitref = Column(String(100), nullable=True)
    circuit_name = Column(String(200), nullable=True)
    location = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    alt = Column(Float, nullable=True)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

# ============================================================
# DIM TABELE - SCD TYPE 2 (nastavak)
# ============================================================

class DimStatus(Base):
    __tablename__ = 'dim_status'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    status_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key)
    statusid = Column(Integer, nullable=False)
    
    # Business atributi
    status = Column(String(100), nullable=True)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    date_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key)
    dateid = Column(Integer, nullable=False)
    
    # Business atributi
    full_date = Column(Date, nullable=True)
    year = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    month_name = Column(String(20), nullable=True)
    quarter = Column(Integer, nullable=True)
    day_of_week = Column(Integer, nullable=True)
    is_weekend = Column(Boolean, nullable=True)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

class DimRace(Base):
    __tablename__ = 'dim_race'
    __table_args__ = {'schema': 'gold'}

    # SCD2 surrogate key (novi PK)
    race_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (business key)
    raceid = Column(Integer, nullable=False)
    
    # Business atributi (FK constraints uklonjeni - natural key references)
    circuitid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    year = Column(Integer, nullable=True)
    round = Column(Integer, nullable=True)
    race_name = Column(String(200), nullable=True)
    date = Column(Date, nullable=True)
    time_races = Column(Time, nullable=True)
    fp1_date = Column(Date, nullable=True)
    fp1_time = Column(Time, nullable=True)
    fp2_date = Column(Date, nullable=True)
    fp2_time = Column(Time, nullable=True)
    fp3_date = Column(Date, nullable=True)
    fp3_time = Column(Time, nullable=True)
    quali_date = Column(Date, nullable=True)
    quali_time = Column(Time, nullable=True)
    sprint_date = Column(Date, nullable=True)
    sprint_time = Column(Time, nullable=True)
    dateid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    
    # SCD2 tracking kolone
    row_hash = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

# ============================================================
# FACT TABELE
# NOTE: FK constraints uklonjeni jer natural keys više nisu UNIQUE (SCD2)
# Referential integrity se održava kroz ETL logiku i DQ checks
# Point-in-time JOIN: fact → dim_race → dim_driver/constructor (date range)
# ============================================================

class FactResults(Base):
    __tablename__ = 'fact_results'
    __table_args__ = {'schema': 'gold'}

    resultid = Column(Integer, primary_key=True)
    raceid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    driverid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    constructorid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    statusid = Column(Integer, nullable=True)  # Natural key reference (no FK)
    grid = Column(Integer, nullable=True)
    position = Column(Numeric, nullable=True)
    laps = Column(Integer, nullable=True)
    milliseconds = Column(Integer, nullable=True)
    fastestlaptime = Column(String(20), nullable=True)
    fastestlapspeed = Column(Float, nullable=True)
    rank = Column(Integer, nullable=True)

class FactLapTimes(Base):
    __tablename__ = 'fact_lap_times'
    __table_args__ = {'schema': 'gold'}

    raceid = Column(Integer, primary_key=True)  # Natural key (no FK)
    driverid = Column(Integer, primary_key=True)  # Natural key (no FK)
    lap = Column(Integer, primary_key=True)
    position_laptimes = Column(Integer, nullable=True)
    time_laptimes = Column(String(20), nullable=True)
    milliseconds_laptimes = Column(Integer, nullable=True)

class FactPitStops(Base):
    __tablename__ = 'fact_pit_stops'
    __table_args__ = {'schema': 'gold'}

    raceid = Column(Integer, primary_key=True)  # Natural key (no FK)
    driverid = Column(Integer, primary_key=True)  # Natural key (no FK)
    stop = Column(Integer, primary_key=True)
    lap_pitstops = Column(Integer, nullable=True)
    time_pitstops = Column(String(20), nullable=True)
    duration = Column(String(20), nullable=True)
    milliseconds_pitstops = Column(Integer, nullable=True)

class FactDriverStandings(Base):
    __tablename__ = 'fact_driver_standings'
    __table_args__ = {'schema': 'gold'}

    driverstandingsid = Column(Integer, primary_key=True)
    raceid = Column(Integer, nullable=True)  # Natural key (no FK)
    driverid = Column(Integer, nullable=True)  # Natural key (no FK)
    points_driverstandings = Column(Float, nullable=True)
    position_driverstandings = Column(Integer, nullable=True)
    wins = Column(Integer, nullable=True)

class FactConstructorStandings(Base):
    __tablename__ = 'fact_constructor_standings'
    __table_args__ = {'schema': 'gold'}

    constructorstandingsid = Column(Integer, primary_key=True)
    raceid = Column(Integer, nullable=True)  # Natural key (no FK)
    constructorid = Column(Integer, nullable=True)  # Natural key (no FK)
    points_constructorstandings = Column(Float, nullable=True)
    position_constructorstandings = Column(Integer, nullable=True)
    wins_constructorstandings = Column(Integer, nullable=True)