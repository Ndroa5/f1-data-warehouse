import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from model import Base
from load import load_to_gold

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres123')}"
    f"@{os.getenv('DB_HOST', '172.21.0.1')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'f1_warehouse')}"
)

def create_tables():
    Base.metadata.create_all(engine)
    logger.info("Gold tabele kreirane!")

def run():
    create_tables()
    load_to_gold(engine)

if __name__ == "__main__":
    run()