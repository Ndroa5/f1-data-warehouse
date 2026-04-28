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
    params = {
        "session_key": 9159,
        "driver_number": 1
    }

    response = requests.get(url, params=params)
    data = response.json()

    logger.info(f"Primljeno {len(data)} telemetrijskih zapisa sa API-ja")

    for record in data[:1000]:
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
        logger.info(f"Sent: driver={record.get('driver_number')}, speed={record.get('speed')}, throttle={record.get('throttle')}")
        time.sleep(0.01)

if __name__ == "__main__":
    producer = get_producer()
    logger.info("🏎️ F1 Car Telemetry Producer started!")
    stream_car_telemetry(producer)
    producer.flush()
    logger.info("✅ Sve telemetrijske poruke poslane!")