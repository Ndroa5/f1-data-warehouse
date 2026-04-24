import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_csv_to_bronze(engine):
    logger.info("Učitavam CSV...")
    try:
        df = pd.read_csv('/opt/airflow/DE/dataEngineeringDataset.csv', dtype=str)
        df = df.rename(columns={'Unnamed: 0': 'unnamed_0'})
        df.columns = [c.lower() for c in df.columns]
        df['unnamed_0'] = df['unnamed_0'].astype(int)
        logger.info(f"Ubacujem {len(df)} redova u bronze.raw_data...")
        df.to_sql('raw_data', engine, schema='bronze', if_exists='replace', index=False)
        logger.info("✅ Bronze load završen!")
    except Exception as e:
        logger.error(f"Greška pri učitavanju bronze: {e}")
        raise