from sqlalchemy import create_engine
from model import Base
from load import load_to_gold

engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')

def create_tables():
    Base.metadata.create_all(engine)
    print("✅ Gold tabele kreirane!")

def run():
    create_tables()
    load_to_gold(engine)

if __name__ == "__main__":
    run()