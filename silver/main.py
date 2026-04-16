from sqlalchemy import create_engine
from load import load_to_silver

engine = create_engine('postgresql://postgres:postgres123@localhost:5432/f1_warehouse')

if __name__ == "__main__":
    load_to_silver(engine)