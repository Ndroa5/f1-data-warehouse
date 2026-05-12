-- Driver enrichment tabela
CREATE TABLE IF NOT EXISTS gold.driver_enrichment (
    driverid INT PRIMARY KEY,
    birth_date DATE,
    birth_place TEXT,
    birth_country TEXT,
    height_cm INT,
    weight_kg INT,
    f1_debut_year INT,
    wikidata_qid TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT fk_driver FOREIGN KEY (driverid) 
        REFERENCES gold.dim_driver(driverid)
);

CREATE INDEX IF NOT EXISTS idx_driver_enrichment_qid 
    ON gold.driver_enrichment(wikidata_qid);