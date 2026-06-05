# =============================================================================
# Metrics Consumer
# =============================================================================
# Script Purpose:
#     This script is responsible for reading events from all 3 Kafka topics
#     and writing aggregated metrics to a Postgres database.
# =============================================================================

import os
import sys
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.utils import load_schema, connect_to_db, KAFKA_CONFIG,SCHEMA_REGISTRY_CONFIG

def handle_order_event(event):
    """
    Creates a query to insert a new order into the orders table.
    """
    query = """
        INSERT INTO minute_metrics (minute_timestamp, order_count, total_revenue)
        VALUES (date_trunc('minute', %(opened_at)s), 1, 0)
        ON CONFLICT (minute_timestamp) DO UPDATE SET order_count = minute_metrics.order_count + 1
    """
    return query

def handle_order_item_event(event):
    """
    Creates a query to update the item counts table.
    """
    query = """
        INSERT INTO item_counts (item_name, category, count)
        VALUES (%(item_name)s, %(category)s, %(quantity)s)
        ON CONFLICT (item_name) DO UPDATE SET count = item_counts.count + EXCLUDED.count
    """
    return query

def handle_payment_event(event):
    """"
    Creates a query to insert a new payment into the payments table.
    """
    query_payment = """
        INSERT INTO recent_payments (paid_at, payment_method, total_amount, table_number, table_size, table_duration_minutes)
        VALUES (%(paid_at)s, %(payment_method)s, %(total_amount)s, %(table_number)s, %(table_size)s, %(table_duration_minutes)s)
    """

    query_minute = """
        INSERT INTO minute_metrics (minute_timestamp, order_count, total_revenue)
        VALUES (date_trunc('minute', %(paid_at)s), 0, %(total_amount)s)
        ON CONFLICT (minute_timestamp) DO UPDATE SET total_revenue = minute_metrics.total_revenue + EXCLUDED.total_revenue
    """
    query_server = """
        INSERT INTO server_metrics (server_name, table_count, total_covers, total_revenue, total_duration_minutes)
        VALUES (%(server_name)s, 1, %(table_size)s, %(total_amount)s, %(table_duration_minutes)s)
        ON CONFLICT (server_name) DO UPDATE SET 
            table_count = server_metrics.table_count + 1,
            total_covers = server_metrics.total_covers + EXCLUDED.total_covers,
            total_revenue = server_metrics.total_revenue + EXCLUDED.total_revenue, 
            total_duration_minutes = server_metrics.total_duration_minutes + EXCLUDED.total_duration_minutes
    """
    
    return query_payment, query_minute, query_server


def main():
    """
    Main execution function for the metrics consumer.
    Creates a schema registry client, loads the Avro schemas for each topic, and connects to the Postgres database.
    Starts a loop to listen for events from Kafka, deserializes them, and inserts them into the database using the
    correct helper function based on the topic.
    """

    print("\n" + "=" * 20)
    print("Starting metrics consumer...\n")

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    deserializers = {
        "orders": AvroDeserializer(schema_registry_client, load_schema("order.avsc")),
        "order-items": AvroDeserializer(schema_registry_client, load_schema("order_item.avsc")),
        "payments": AvroDeserializer(schema_registry_client, load_schema("payments.avsc"))
    }

    # Connect to the Postgres database
    conn = connect_to_db()

    consumer_config = {**KAFKA_CONFIG, "group.id": "metrics-consumer-2", "auto.offset.reset": "latest"}
    consumer = Consumer(consumer_config)
    consumer.subscribe(["orders", "order-items", "payments"])

    print("=" * 20)
    print("Checking for new events from Kafka...\n")
    # Check for new events from Kafka
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the message   
        topic = msg.topic()
        event = deserializers[topic](msg.value(), SerializationContext(topic, MessageField.VALUE))

        queries = []
        # Route the event to the appropriate handler
        if topic == "orders":
            queries = [handle_order_event(event)]
        elif topic == "order-items":
            queries = [handle_order_item_event(event)]
        elif topic == "payments":
            queries = list(handle_payment_event(event))
        
        # Write the event(s) to the postgres database
        for q in queries:
            cur = conn.cursor()
            cur.execute(q, event)
            conn.commit()
            cur.close()
            print(f"Event processed successfully")

if __name__ == "__main__":
    main()

