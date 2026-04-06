import pandas as pd
import duckdb
import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from textblob import TextBlob

def process_batch():
    consumer = KafkaConsumer(
        'raw_news',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    messages = [msg.value for msg in consumer]
    if not messages: return

    df = pd.DataFrame(messages)

    # --- Entity Mapping ---
    def identify_entity(text):
        text = str(text).lower()
        if 'apple' in text: return 'Apple'
        if 'samsung' in text: return 'Samsung'
        if 'google' in text: return 'Google'
        return 'Other'

    df['brand_entity'] = df['title'].apply(identify_entity)

    # --- Sentiment Analysis ---
    df['sentiment_score'] = df['description'].apply(lambda x: TextBlob(str(x)).sentiment.polarity if x else 0)

    # --- Risk Scoring ---
    # A "Crisis" is defined as negative sentiment (-0.2) in a title or description
    df['is_crisis_alert'] = df['sentiment_score'].apply(lambda x: 1 if x < -0.2 else 0)

    # --- DATA WAREHOUSING (Silver Layer) ---
    con = duckdb.connect('analytics_warehouse.db')
    con.execute("DROP TABLE IF EXISTS silver_news") # Add this for a clean slate
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_news (
            brand_entity VARCHAR,
            source_name VARCHAR,
            title VARCHAR,
            sentiment_score DOUBLE,
            is_crisis_alert INTEGER,
            published_at TIMESTAMP
        )
    """)
    
    # Rename publishedAt to published_at in the DataFrame
    df = df.rename(columns={'publishedAt': 'published_at'})

    # Clean and Register
    df['source_name'] = df['source'].apply(lambda x: x.get('name'))
    con.register('df_view', df)
    
    con.execute("""
        INSERT INTO silver_news 
        SELECT brand_entity, source_name, title, sentiment_score, is_crisis_alert, CAST(published_at AS TIMESTAMP)
        FROM df_view
    """)

    # --- EXECUTIVE AGGREGATION (Gold Layer) ---
    # Compare our average sentiment vs competitors to find market outliers
    gold_df = con.execute("""
        SELECT 
            brand_entity,
            AVG(sentiment_score) as brand_reputation_score,
            SUM(is_crisis_alert) as active_crises,
            COUNT(*) as mention_volume,
            (SUM(is_crisis_alert)::FLOAT / COUNT(*)) * 100 as share_of_negativity
        FROM silver_news
        WHERE published_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        GROUP BY brand_entity
    """).df()

    # --- EXPORT TO POSTGRES (Dashboard Backend) ---
    engine = create_engine('postgresql+psycopg2://user:password@postgres_db:5432/warehouse')
    gold_df.to_sql('competitor_brand_health', engine, if_exists='replace', index=False)
    
    print("Executive Brand Health Metrics updated.")
