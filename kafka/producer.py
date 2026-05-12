import json
import time
import logging
import os
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

def stream_car_telemetry(producer):
    logger.info("Dohvatam telemetriju sa OpenF1 API...")

    url = "https://api.openf1.org/v1/car_data"

    driver_numbers = [1, 11, 14, 16, 18, 20, 22, 23, 24, 27, 31, 44, 55, 63, 77]

    total = 0
    for driver in driver_numbers:
        params = {
            "session_key": 9094,
            "driver_number": driver
        }
        response = requests.get(url, params=params)
        data = response.json()

        if not isinstance(data, list):
            logger.error(f"Driver {driver} — API greška: {data}")
            continue

        logger.info(f"Driver {driver}: primljeno {len(data)} zapisa")

        sent = 0
        for record in data:
            if record.get("speed", 0) == 0:
                continue
            if sent >= 300:
                break
            message = {
                "session_key": record.get("session_key"),
                "driver_number": record.get("driver_number"),
                "date": str(record.get("date")),
                "speed": record.get("speed"),
                "throttle": record.get("throttle"),
                "brake": record.get("brake"),
                "drs": record.get("drs"),
                "n_gear": record.get("n_gear"),
                "rpm": record.get("rpm")
            }
            producer.send('f1.car_telemetry', message)
            total += 1
            sent += 1
            time.sleep(0.005)

        logger.info(f"Driver {driver}: poslano {sent} validnih zapisa")

    logger.info(f"✅ Ukupno poslano: {total} telemetrijskih zapisa")

if __name__ == "__main__":
    producer = get_producer()
    logger.info("🏎️ F1 Car Telemetry Producer started!")
    stream_car_telemetry(producer)
    producer.flush()
    logger.info("✅ Sve telemetrijske poruke poslane!")