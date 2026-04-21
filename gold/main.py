from sqlalchemy import create_engine
from model import Base
from load import load_to_gold

engine = create_engine('postgresql://postgres:postgres123@host.docker.internal:5432/f1_warehouse')

def create_tables():
    Base.metadata.create_all(engine)
    print("✅ Gold tabele kreirane!")

def load():
    load_to_gold(engine)

def run():
    create_tables()
    load()

if __name__ == "__main__":
    run()