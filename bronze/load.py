import pandas as pd
from sqlalchemy import create_engine

# konekcija na bazu
engine = create_engine('postgresql://postgres:postgres123@localhost:5432/f1_warehouse')

# citaj CSV
print("Učitavam CSV...")
df = pd.read_csv('DE/dataEngineeringDataset.csv', dtype=str)

# preimenuj unnamed kolonu
df = df.rename(columns={'Unnamed: 0': 'unnamed_0'})

# sve kolone u lowercase
df.columns = [c.lower() for c in df.columns]

# ubaci u bronze
print(f"Ubacujem {len(df)} redova u bronze.raw_data...")
df.to_sql('raw_data', engine, schema='bronze', if_exists='replace', index=False)

print("Gotovo!")