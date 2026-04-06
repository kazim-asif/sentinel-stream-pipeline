# Sentinel Stream: A Market Intelligence Pipeline
This project demonstrates a data engineering workflow for real-time brand monitoring and sentiment analysis. It automates the journey from raw API data to high-level executive insights.

## Tech Stack
**Orchestration**: Apache Airflow

**Streaming**: Apache Kafka (KRaft mode)

**Processing**: Python (Pandas, TextBlob)

**Storage** (Medallion): * 
Bronze: Kafka (Raw JSON)
Silver: DuckDB (Cleaned & Scored)
Gold: PostgreSQL (Aggregated Metrics)

**Infrastructure**: Docker & Docker Compose

**Architecture Overview**
Ingestion: A Python producer fetches tech news from NewsAPI and streams it into a Kafka topic with error handling and retries.

Processing (Silver Layer): A consumer retrieves messages, performs entity mapping (Apple vs. Google), and runs NLP sentiment scoring.

Analytics (Gold Layer): Data is moved into DuckDB for high-performance SQL transformations to calculate "Share of Negativity" and "Reputation Scores."

Serving: The final aggregated metrics are pushed to a PostgreSQL warehouse, ready for dashboarding.

## Key Engineering Features
Fault Tolerance: Implements Kafka acks=all and idempotent producer settings.

Containerization: Fully portable environment using a custom Dockerfile and Compose.

Data Quality: Deduplication and validation logic within the processor script.

In-Memory Performance: Utilizes DuckDB for fast intermediate transformations before persisting to the warehouse.

## Quick Start
**Clone the repo**

**Set Environment Variables**:

export NEWS_API_KEY='your_api_key_here'

**Spin up the stack**:

docker-compose up -d

Access Airflow: Navigate to localhost:8080 to trigger the market_intelligence_pipeline DAG.
