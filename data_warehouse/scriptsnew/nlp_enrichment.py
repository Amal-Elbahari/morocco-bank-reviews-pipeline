import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from textblob import TextBlob
from langdetect import detect
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import logging

# Logger pour debug dans Airflow
logging.basicConfig(level=logging.INFO)

def enrich_reviews():
    try:
        # Connexion à PostgreSQL via SQLAlchemy
        engine = create_engine('postgresql://postgres:2024@localhost:5432/final')
        conn = engine.connect()
        logging.info("✅ Connexion PostgreSQL réussie")

        # 1. Charger les données depuis la vue stg_reviews
        df = pd.read_sql("SELECT * FROM stg_reviews", conn)
        logging.info(f"🔹 {len(df)} avis chargés")

        # 2. Détection de la langue
        df['lang'] = df['review_text'].apply(lambda x: detect(x) if pd.notnull(x) and x.strip() else 'unknown')

        # 3. Analyse de sentiment avec TextBlob
        df['sentiment_score'] = df['review_text'].apply(lambda x: TextBlob(x).sentiment.polarity if pd.notnull(x) else 0)
        df['sentiment'] = df['sentiment_score'].apply(
            lambda score: 'Positive' if score > 0.1 else 'Negative' if score < -0.1 else 'Neutral'
        )

        # 4. Extraction des topics avec LDA
        vectorizer = CountVectorizer(stop_words='english')
        X = vectorizer.fit_transform(df['review_text'].fillna(""))

        lda = LatentDirichletAllocation(n_components=5, random_state=42)
        lda.fit(X)
        topics = lda.transform(X)
        df['topic'] = topics.argmax(axis=1)

        # 5. Enregistrement dans PostgreSQL (remplace la table si elle existe)
        df.to_sql('review_enriched', con=engine, if_exists='replace', index=False, schema='public')
        logging.info("✅ Enrichissement NLP terminé et enregistré dans PostgreSQL.")

    except Exception as e:
        logging.error(f"❌ Erreur lors de l'enrichissement : {e}")

if __name__ == "__main__":
    enrich_reviews()
