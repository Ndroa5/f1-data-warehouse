from sqlalchemy import create_engine
from model import Base
from load import load_to_gold

engine = create_engine('postgresql://postgres:postgres123@localhost:5432/f1_warehouse')

def create_tables():
    Base.metadata.create_all(engine)
    print("✅ Gold tabele kreirane!")

def load():
    load_to_gold(engine)

if __name__ == "__main__":
    create_tables()
    load()