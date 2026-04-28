import json
import logging
import os
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres123')}"
    f"@{os.getenv('DB_HOST', '172.21.0.1')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'f1_warehouse')}"
)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

def create_kafka_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS kafka_stream;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kafka_stream.lap_times (
                raceid INTEGER,
                driverid INTEGER,
                lap INTEGER,
                position_laptimes INTEGER,
                time_laptimes VARCHAR(20),
                milliseconds_laptimes INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (raceid, driverid, lap)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kafka_stream.pit_stops (
                raceid INTEGER,
                driverid INTEGER,
                stop INTEGER,
                lap_pitstops INTEGER,
                time_pitstops VARCHAR(20),
                duration VARCHAR(20),
                milliseconds_pitstops INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (raceid, driverid, stop)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kafka_stream.results (
                resultid INTEGER PRIMARY KEY,
                raceid INTEGER,
                driverid INTEGER,
                constructorid INTEGER,
                grid INTEGER,
                position NUMERIC,
                laps INTEGER,
                milliseconds INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW()
            );
        """))
    logger.info("✅ Kafka stream tabele kreirane!")

def consume_messages(engine):
    consumer = KafkaConsumer(
        'f1.lap_times',
        'f1.pit_stops',
        'f1.results',
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='f1-consumer-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=60000
    )

    counts = {'f1.lap_times': 0, 'f1.pit_stops': 0, 'f1.results': 0}

    with engine.begin() as conn:
        for message in consumer:
            topic = message.topic
            data = message.value

            if topic == 'f1.lap_times':
                conn.execute(text("""
                    INSERT INTO kafka_stream.lap_times
                        (raceid, driverid, lap, position_laptimes, time_laptimes, milliseconds_laptimes)
                    VALUES
                        (:raceid, :driverid, :lap, :position_laptimes, :time_laptimes, :milliseconds_laptimes)
                    ON CONFLICT (raceid, driverid, lap) DO UPDATE SET
                        position_laptimes = EXCLUDED.position_laptimes,
                        time_laptimes = EXCLUDED.time_laptimes,
                        milliseconds_laptimes = EXCLUDED.milliseconds_laptimes;
                """), data)
                counts['f1.lap_times'] += 1

            elif topic == 'f1.pit_stops':
                conn.execute(text("""
                    INSERT INTO kafka_stream.pit_stops
                        (raceid, driverid, stop, lap_pitstops, time_pitstops, duration, milliseconds_pitstops)
                    VALUES
                        (:raceid, :driverid, :stop, :lap_pitstops, :time_pitstops, :duration, :milliseconds_pitstops)
                    ON CONFLICT (raceid, driverid, stop) DO UPDATE SET
                        lap_pitstops = EXCLUDED.lap_pitstops,
                        time_pitstops = EXCLUDED.time_pitstops,
                        duration = EXCLUDED.duration,
                        milliseconds_pitstops = EXCLUDED.milliseconds_pitstops;
                """), data)
                counts['f1.pit_stops'] += 1

            elif topic == 'f1.results':
                conn.execute(text("""
                    INSERT INTO kafka_stream.results
                        (resultid, raceid, driverid, constructorid, grid, position, laps, milliseconds)
                    VALUES
                        (:resultid, :raceid, :driverid, :constructorid, :grid, :position, :laps, :milliseconds)
                    ON CONFLICT (resultid) DO UPDATE SET
                        raceid = EXCLUDED.raceid,
                        driverid = EXCLUDED.driverid,
                        constructorid = EXCLUDED.constructorid,
                        grid = EXCLUDED.grid,
                        position = EXCLUDED.position,
                        laps = EXCLUDED.laps,
                        milliseconds = EXCLUDED.milliseconds;
                """), data)
                counts['f1.results'] += 1

    logger.info(f"✅ Consumer završen! Primljeno: {counts}")

if __name__ == "__main__":
    engine = create_engine(DB_URL)
    create_kafka_tables(engine)
    consume_messages(engine)