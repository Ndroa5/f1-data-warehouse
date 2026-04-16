from sqlalchemy import create_engine, text
from model import CREATE_SCHEMA, CREATE_RAW_DATA

engine = create_engine('postgresql://postgres:postgres123@localhost:5432/f1_warehouse')

def create_tables():
    with engine.begin() as conn:
        conn.execute(text(CREATE_SCHEMA))
        conn.execute(text(CREATE_RAW_DATA))
    print("✅ Bronze tabele kreirane!")

def load():
    from load import load_csv_to_bronze
    load_csv_to_bronze(engine)

if __name__ == "__main__":
    create_tables()
    load()