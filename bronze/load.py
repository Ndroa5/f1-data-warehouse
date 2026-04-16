import pandas as pd

def load_csv_to_bronze(engine):
    print("Učitavam CSV...")
    df = pd.read_csv('../DE/dataEngineeringDataset.csv', dtype=str)
    
    df = df.rename(columns={'Unnamed: 0': 'unnamed_0'})
    df.columns = [c.lower() for c in df.columns]
    
    print(f"Ubacujem {len(df)} redova u bronze.raw_data...")
    df.to_sql('raw_data', engine, schema='bronze', if_exists='replace', index=False)
    print("✅ Gotovo!")