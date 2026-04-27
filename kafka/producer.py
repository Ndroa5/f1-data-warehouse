import json
import time
import logging
import os
from kafka import KafkaProducer
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

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_lap_times(producer, engine):
    logger.info("Streaming lap times...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT raceid, driverid, lap, position_laptimes, time_laptimes, milliseconds_laptimes
            FROM silver.raw_data
            WHERE lap IS NOT NULL
            ORDER BY raceid, driverid, lap
            LIMIT 1000
        """)).fetchall()

    for row in rows:
        message = {
            'raceid': row[0],
            'driverid': row[1],
            'lap': row[2],
            'position_laptimes': row[3],
            'time_laptimes': row[4],
            'milliseconds_laptimes': row[5]
        }
        producer.send('f1.lap_times', message)
        logger.info(f"Sent lap time: race={row[0]}, driver={row[1]}, lap={row[2]}")
        time.sleep(0.01)

def stream_pit_stops(producer, engine):
    logger.info("Streaming pit stops...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT raceid, driverid, stop, lap_pitstops, time_pitstops, duration, milliseconds_pitstops
            FROM silver.raw_data
            WHERE stop IS NOT NULL
            ORDER BY raceid, driverid, stop
            LIMIT 500
        """)).fetchall()

    for row in rows:
        message = {
            'raceid': row[0],
            'driverid': row[1],
            'stop': row[2],
            'lap_pitstops': row[3],
            'time_pitstops': row[4],
            'duration': row[5],
            'milliseconds_pitstops': row[6]
        }
        producer.send('f1.pit_stops', message)
        logger.info(f"Sent pit stop: race={row[0]}, driver={row[1]}, stop={row[2]}")
        time.sleep(0.01)

def stream_results(producer, engine):
    logger.info("Streaming results...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (resultid)
                resultid, raceid, driverid, constructorid, grid, position, laps, milliseconds
            FROM silver.raw_data
            WHERE resultid IS NOT NULL
            ORDER BY resultid
            LIMIT 500
        """)).fetchall()

    for row in rows:
        message = {
            'resultid': row[0],
            'raceid': row[1],
            'driverid': row[2],
            'constructorid': row[3],
            'grid': row[4],
            'position': str(row[5]) if row[5] else None,
            'laps': row[6],
            'milliseconds': row[7]
        }
        producer.send('f1.results', message)
        logger.info(f"Sent result: resultid={row[0]}, race={row[1]}, driver={row[2]}")
        time.sleep(0.01)

if __name__ == "__main__":
    engine = create_engine(DB_URL)
    producer = get_producer()

    logger.info("🏎️ F1 Kafka Producer started!")

    stream_lap_times(producer, engine)
    stream_pit_stops(producer, engine)
    stream_results(producer, engine)

    producer.flush()
    logger.info("✅ Sve poruke poslane!")