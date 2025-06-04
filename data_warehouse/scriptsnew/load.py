import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime

# Charger le fichier CSV des avis extraits
df = pd.read_csv("/home/amal/airflow/scriptsnew/bank_reviews.csv", encoding="utf-8")

# Connexion à PostgreSQL
db_url = 'postgresql://postgres:2024@localhost:5432/final'  # Remplacer par tes informations

def check_postgres_connection(db_url):
    try:
        # Essayer de se connecter à la base de données
        engine = create_engine(db_url)
        connection = engine.connect()
        connection.close()
        print("✅ Connexion à PostgreSQL réussie !")
    except OperationalError as e:
        print(f"🚨 Erreur de connexion à PostgreSQL : {e}")
        raise

# Vérifier la connexion à PostgreSQL avant de charger les données
check_postgres_connection(db_url)

# Connexion à PostgreSQL avec SQLAlchemy
engine = create_engine(db_url)

try:
    # Charger les données dans une table nommée 'rev' (ajouter si elle n'existe pas)
    # Utilisation de chunksize pour améliorer les performances avec de grands ensembles de données
    df.to_sql('rev', con=engine, if_exists='append', index=False, chunksize=1000)
    print("✅ Données chargées avec succès dans PostgreSQL.")
except Exception as e:
    print(f"🚨 Erreur lors du chargement des données dans PostgreSQL : {e}")
