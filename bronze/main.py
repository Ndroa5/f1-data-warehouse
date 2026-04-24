import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from model import Base, BronzeRawData
from load import load_csv_to_bronze

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def create_tables():
    Base.metadata.create_all(engine)
    logger.info("✅ Bronze tabele kreirane!")

def run():
    create_tables()
    load_csv_to_bronze(engine)

if __name__ == "__main__":
    run()