# Kafka Streaming Pipeline - Restaurant POS Simulator

An end-to-end streaming pipeline built to simulate real-time restaurant POS analytics. A Python-based simulator generates realistic order, item, and payment events that are serialized using Avro schemas and produced to Kafka topics. Two independent consumer groups process the stream in parallel: one aggregating metrics into PostgreSQL, one watching for notable patterns and generating alerts. A live Streamlit dashboard reads from PostgreSQL and refreshes every minute, visualizing revenue, order activity, server performance, and alerts as they arrive.

The dashboard is deployed at [Live Dashboard](https://restaurant-pos-simulator.streamlit.app/).

---

## Project Overview

### Simulator
A Python-based restaurant environment with 12 tables, 6 servers, and a full menu across food and drink categories. Generates realistic POS events following a daily schedule with configurable load profiles (quiet, steady, busy) mapped to opening hours. Events flow in rounds: drinks and appetizers, mains, then optional additional rounds, before closing with a payment.

### Kafka
A serializing producer registers Avro schemas with Schema Registry and produces events to three topics: `orders`, `order-items`, and `payments`. Two independent consumer groups read from the same stream without any awareness of each other, demonstrating Kafka's decoupled pub/sub model.

### PostgreSQL
The metrics consumer aggregates events into four tables: per-minute order and revenue metrics, item popularity counts, recent payments, and per-server performance. The alerts consumer writes to a separate alerts table when bills, table durations, or revenue milestones cross defined thresholds.

### Streamlit
A live dashboard that queries PostgreSQL every minute and displays KPI metrics, a cumulative revenue line chart, best-selling items, server performance with gradient-shaded tables, and recent alerts. Indicates whether the restaurant is currently open or closed based on the configured schedule.

### Skills Demonstrated
- Event-driven architecture and real-time stream processing
- Apache Kafka producer and consumer group design
- Avro schema design and Schema Registry integration
- Docker Compose multi-container orchestration
- KRaft mode Kafka configuration (no Zookeeper)
- Python-based data pipeline development
- PostgreSQL schema design and upsert patterns
- Streaming aggregations and windowed metrics
- Independent consumer group design (decoupled processing)
- Streamlit dashboard development with live data refresh
- Schedule-aware simulation with configurable load profiles

---

## Architecture

Events flow left to right through the pipeline:

1. The simulator generates POS events based on the current time and load profile
2. The producer serializes events using Avro schemas registered with Schema Registry and publishes them to three Kafka topics
3. Two independent consumer groups read from the topics simultaneously: the metrics consumer aggregates data into PostgreSQL, the alerts consumer watches for notable patterns
4. The Streamlit dashboard queries PostgreSQL every minute and renders live metrics

---

## Hosted Infrastructure

The live version of this project runs on the following hosted services:

- **Apache Kafka**: Confluent Cloud (Basic tier, GCP us-west1)
- **Schema Registry**: Confluent Cloud managed Schema Registry
- **PostgreSQL**: Supabase (free tier)
- **Dashboard**: Streamlit Community Cloud

---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+

### Local Setup

1. Clone the repository:

```bash
git clone https://github.com/alekivetz/kafka-streaming-pipeline.git
cd kafka-streaming-pipeline
```

2. Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Start the infrastructure:

```bash
docker compose up -d
```

This starts Kafka (KRaft mode), Schema Registry, and PostgreSQL, and creates the three Kafka topics automatically.

4. In separate terminals, start the producer and consumers:

```bash
# Terminal 1
python producer/producer.py

# Terminal 2
python consumers/metrics_consumer.py

# Terminal 3
python consumers/alerts_consumer.py
```

5. Launch the dashboard:

```bash
streamlit run dashboard/dashboard.py
```

The dashboard will be available at `http://localhost:8501`.

### Hosted Setup

To connect to the hosted infrastructure (Confluent Cloud + Supabase), create a `.env` file in the project root:

```
KAFKA_BOOTSTRAP_SERVERS=your-confluent-bootstrap-server
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=your-schema-registry-url
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret
DB_HOST=your-supabase-host
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your-supabase-password
```

Then run the producer and consumers as above. 

### Notes
- The simulator follows a daily schedule (open 11am-11pm MT) with load profiles mapped to peak hours. No events will be produced outside opening hours.
- To reset the database and start fresh: `docker compose down -v && docker compose up -d`
- To test outside opening hours, re-enable the demo profile in `producer/simulator.py`

---

## Tools & Technologies

- **Python**: Simulator, producer, consumers, and dashboard
- **Apache Kafka**: Message bus for real-time event streaming (KRaft mode, no Zookeeper)
- **Confluent Schema Registry**: Avro schema registration and enforcement
- **Apache Avro**: Event serialization format with schema evolution support
- **PostgreSQL**: Operational database for aggregated metrics and alerts
- **Streamlit**: Live dashboard with auto-refresh
- **Docker Compose**: Multi-container orchestration for the full stack
- **confluent-kafka**: Python Kafka client with Avro serialization support
- **psycopg2**: Python to PostgreSQL connectivity
- **pandas**: Dataframe handling in the dashboard
- **Confluent Cloud**: Managed Kafka and Schema Registry hosting
- **Supabase**: Managed PostgreSQL hosting

---

## Repository Structure

```
kafka-streaming-pipeline/
|
|-- .streamlit/
|   |-- config.toml              # Streamlit theme configuration
|
|-- consumers/
|   |-- alerts_consumer.py       # Alerts consumer: high bills, long tables, revenue milestones
|   |-- metrics_consumer.py      # Metrics consumer: order counts, revenue, item popularity, server stats
|
|-- dashboard/
|   |-- dashboard.py             # Live Streamlit dashboard
|
|-- db/
|   |-- init.sql                 # PostgreSQL table definitions
|
|-- producer/
|   |-- producer.py              # Kafka producer with Avro serialization
|   |-- simulator.py             # Restaurant simulator with schedule-aware load profiles
|
|-- schemas/
|   |-- order.avsc               # Avro schema for order events
|   |-- order_item.avsc          # Avro schema for order item events
|   |-- payments.avsc            # Avro schema for payment events
|
|-- utils/
|   |-- utils.py                 # Shared utilities: schema loading, schedule management
|
|-- .gitignore
|-- docker-compose.yml           # Kafka, Schema Registry, PostgreSQL orchestration
|-- README.md
```

---
