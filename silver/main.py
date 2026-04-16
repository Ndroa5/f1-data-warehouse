from sqlalchemy import create_engine, text
from model import (
    CREATE_SCHEMA, CREATE_DRIVERS, CREATE_CONSTRUCTORS,
    CREATE_CIRCUITS, CREATE_STATUS, CREATE_RACES, CREATE_RESULTS,
    CREATE_LAP_TIMES, CREATE_PIT_STOPS, CREATE_DRIVER_STANDINGS,
    CREATE_CONSTRUCTOR_STANDINGS
)

engine = create_engine('postgresql://postgres:postgres123@localhost:5432/f1_warehouse')

def create_tables():
    with engine.begin() as conn:
        conn.execute(text(CREATE_SCHEMA))
        conn.execute(text(CREATE_DRIVERS))
        conn.execute(text(CREATE_CONSTRUCTORS))
        conn.execute(text(CREATE_CIRCUITS))
        conn.execute(text(CREATE_STATUS))
        conn.execute(text(CREATE_RACES))
        conn.execute(text(CREATE_RESULTS))
        conn.execute(text(CREATE_LAP_TIMES))
        conn.execute(text(CREATE_PIT_STOPS))
        conn.execute(text(CREATE_DRIVER_STANDINGS))
        conn.execute(text(CREATE_CONSTRUCTOR_STANDINGS))
    print("✅ Silver tabele kreirane!")

def load():
    from load import load_to_silver
    load_to_silver(engine)
if __name__ == "__main__":
    create_tables()
    load()