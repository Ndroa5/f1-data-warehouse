from sqlalchemy import create_engine
from model import Base, BronzeRawData
from load import load_csv_to_bronze

engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')

def create_tables():
    Base.metadata.create_all(engine)
    print("✅ Bronze tabele kreirane!")

def run():
    create_tables()
    load_csv_to_bronze(engine)

if __name__ == "__main__":
    run()