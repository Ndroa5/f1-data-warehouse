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

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

def create_kafka_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.car_telemetry (
                session_key     INTEGER,
                driver_number   INTEGER,
                date            VARCHAR(50),
                speed           INTEGER,
                throttle        INTEGER,
                brake           INTEGER,
                drs             INTEGER,
                n_gear          INTEGER,
                rpm             INTEGER,
                ingested_at     TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (session_key, driver_number, date)
            );
        """))
    logger.info("gold.car_telemetry tabela kreirana!")

def consume_messages(engine):
    consumer = KafkaConsumer(
        'f1.car_telemetry',
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='f1-telemetry-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=15000
    )

    count = 0

    with engine.begin() as conn:
        for message in consumer:
            data = message.value
            conn.execute(text("""
                INSERT INTO gold.car_telemetry
                    (session_key, driver_number, date, speed, throttle, brake, drs, n_gear, rpm)
                VALUES
                    (:session_key, :driver_number, :date, :speed, :throttle, :brake, :drs, :n_gear, :rpm)
                ON CONFLICT (session_key, driver_number, date) DO UPDATE SET
                    speed = EXCLUDED.speed,
                    throttle = EXCLUDED.throttle,
                    brake = EXCLUDED.brake,
                    drs = EXCLUDED.drs,
                    n_gear = EXCLUDED.n_gear,
                    rpm = EXCLUDED.rpm;
            """), data)
            count += 1

    logger.info(f"Consumer završen! Primljeno: {count} telemetrijskih zapisa")

if __name__ == "__main__":
    engine = create_engine(DB_URL)
    create_kafka_tables(engine)
    consume_messages(engine)