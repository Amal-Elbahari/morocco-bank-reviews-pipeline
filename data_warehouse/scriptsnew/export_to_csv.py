import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# Configurer le logger
logging.basicConfig(level=logging.INFO)

def export_tables_to_csv():
    try:
        # Connexion à la base PostgreSQL
        engine = create_engine('postgresql://postgres:2024@localhost:5432/final')

        # Liste des tables à exporter
        tables = ['fact_reviews', 'dim_bank', 'dim_branch', 'dim_location', 'dim_sentiment']

        # Dossier de sortie
        output_dir = '/home/amal/airflow/exports'
        os.makedirs(output_dir, exist_ok=True)

        # Exporter chaque table
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", engine)
            csv_path = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(csv_path, index=False)
            logging.info(f"✅ Exporté {table} vers {csv_path}")

    except Exception as e:
        logging.error(f"❌ Erreur d'export : {e}")
