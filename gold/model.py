from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import String, Integer, Float, Date, Time, Numeric, ForeignKey
from typing import Optional

class Base(DeclarativeBase):
    pass

# ============================================================
# DIM TABELE
# ============================================================

class DimDriver(Base):
    __tablename__ = 'dim_driver'
    __table_args__ = {'schema': 'gold'}

    driverid: Mapped[int] = mapped_column(Integer, primary_key=True)
    driverref: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    forename: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    surname: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    dob: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    nationality: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

class DimConstructor(Base):
    __tablename__ = 'dim_constructor'
    __table_args__ = {'schema': 'gold'}

    constructorid: Mapped[int] = mapped_column(Integer, primary_key=True)
    constructorref: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    nationality_constructors: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

class DimCircuit(Base):
    __tablename__ = 'dim_circuit'
    __table_args__ = {'schema': 'gold'}

    circuitid: Mapped[int] = mapped_column(Integer, primary_key=True)
    circuitref: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    circuit_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    location: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    alt: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

class DimStatus(Base):
    __tablename__ = 'dim_status'
    __table_args__ = {'schema': 'gold'}

    statusid: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

class DimRace(Base):
    __tablename__ = 'dim_race'
    __table_args__ = {'schema': 'gold'}

    raceid: Mapped[int] = mapped_column(Integer, primary_key=True)
    circuitid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_circuit.circuitid'), nullable=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    round: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    race_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    time_races: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    fp1_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    fp1_time: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    fp2_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    fp2_time: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    fp3_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    fp3_time: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    quali_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    quali_time: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    sprint_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    sprint_time: Mapped[Optional[Time]] = mapped_column(Time, nullable=True)
    dateid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_date.dateid'), nullable=True)

class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'gold'}

    dateid: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    month_name: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    quarter: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    day_of_week: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_weekend: Mapped[Optional[bool]] = mapped_column(nullable=True)

# ============================================================
# FACT TABELE
# ============================================================

class FactResults(Base):
    __tablename__ = 'fact_results'
    __table_args__ = {'schema': 'gold'}

    resultid: Mapped[int] = mapped_column(Integer, primary_key=True)
    raceid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_race.raceid'), nullable=True)
    driverid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_driver.driverid'), nullable=True)
    constructorid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_constructor.constructorid'), nullable=True)
    statusid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_status.statusid'), nullable=True)
    grid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    position: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    laps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    milliseconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fastestlaptime: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    fastestlapspeed: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class FactLapTimes(Base):
    __tablename__ = 'fact_lap_times'
    __table_args__ = {'schema': 'gold'}

    raceid: Mapped[int] = mapped_column(Integer, ForeignKey('gold.dim_race.raceid'), primary_key=True)
    driverid: Mapped[int] = mapped_column(Integer, ForeignKey('gold.dim_driver.driverid'), primary_key=True)
    lap: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_laptimes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    time_laptimes: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    milliseconds_laptimes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class FactPitStops(Base):
    __tablename__ = 'fact_pit_stops'
    __table_args__ = {'schema': 'gold'}

    raceid: Mapped[int] = mapped_column(Integer, ForeignKey('gold.dim_race.raceid'), primary_key=True)
    driverid: Mapped[int] = mapped_column(Integer, ForeignKey('gold.dim_driver.driverid'), primary_key=True)
    stop: Mapped[int] = mapped_column(Integer, primary_key=True)
    lap_pitstops: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    time_pitstops: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    duration: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    milliseconds_pitstops: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class FactDriverStandings(Base):
    __tablename__ = 'fact_driver_standings'
    __table_args__ = {'schema': 'gold'}

    driverstandingsid: Mapped[int] = mapped_column(Integer, primary_key=True)
    raceid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_race.raceid'), nullable=True)
    driverid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_driver.driverid'), nullable=True)
    points_driverstandings: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    position_driverstandings: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wins: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class FactConstructorStandings(Base):
    __tablename__ = 'fact_constructor_standings'
    __table_args__ = {'schema': 'gold'}

    constructorstandingsid: Mapped[int] = mapped_column(Integer, primary_key=True)
    raceid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_race.raceid'), nullable=True)
    constructorid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('gold.dim_constructor.constructorid'), nullable=True)
    points_constructorstandings: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    position_constructorstandings: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wins_constructorstandings: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)