import os
import json
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration via Environment Variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
NEWS_API_KEY = os.getenv('NEWS_API_KEY', 'my-api-key')
TOPIC_NAME = "raw_news"

# 'acks=all' ensures data is replicated before continuing
# 'retries' handles transient network flickers
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    max_in_flight_requests_per_connection=1
)

def fetch_and_stream():
    url = 'https://newsapi.org/v2/everything'
    # Business Case: Monitor our company (e.g., "Apple") vs Competitors
    search_query = '(Apple OR Samsung OR Google OR Huawei) AND (smartphone OR tech)'
    
    params = {
        'q': search_query,
        'language': 'en',
        'sortBy': 'publishedAt',
        'apiKey': NEWS_API_KEY,
        'pageSize': 100
    }

    try:
        # verify=True just for local env to avoid ssl issues!
        response = requests.get(url, params=params, timeout=10, verify=False)
        response.raise_for_status()
        
        data = response.json()
        articles = data.get('articles', [])
        
        count = 0
        for article in articles:
            # Deduplication or validation
            if article.get('description') and article.get('url'):
                # Using a callback to track success/failure per message
                producer.send(TOPIC_NAME, article).add_callback(on_success).add_errback(on_error)
                count += 1
        
        producer.flush()
        logger.info(f"Successfully streamed {count} articles to {TOPIC_NAME}")

    except requests.exceptions.RequestException as e:
        logger.error(f"API Request failed: {e}")
        raise
    except KafkaError as e:
        logger.error(f"Kafka delivery failed: {e}")
        raise

def on_success(record_metadata):
    pass

def on_error(excp):
    logger.error(f'Failed to send message: {excp}')

if __name__ == "__main__":
    fetch_and_stream()
