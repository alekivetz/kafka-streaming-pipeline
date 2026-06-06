# =============================================================================
# Alerts Consumer
# =============================================================================
# Script Purpose:
#     This script is responsible for reading events from Kafka and watching 
#     for unusual or notable patters, flagging them to a separate alerts table
#     in Postgres.
# =============================================================================

import os
import sys
import uuid
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.utils import load_schema, connect_to_db, KAFKA_CONFIG, SCHEMA_REGISTRY_CONFIG

HIGH_BILL_THRESHOLD = 300
LONG_TABLE_MINUTES = 90
REVENUE_MILESTONE = 1000

def handle_payment_event(event, conn):
    """
    Checks payment events for alert metrics and writes the corresponding SQL query.
    """
    queries = []
    # Check for high bills
    if event["total_amount"] > HIGH_BILL_THRESHOLD:
        query = """
            INSERT INTO alerts (alert_id, alert_type, message, triggered_at)
            VALUES (%(alert_id)s, %(alert_type)s, %(message)s, %(triggered_at)s)
        """
        params = {
            "alert_id": str(uuid.uuid4()),
            "alert_type": "high_bill",
            "message": f"Table {event['table_number']}'s bill exceeded ${HIGH_BILL_THRESHOLD} -- Total amount: ${event['total_amount']:.2f}",
            "triggered_at": event["paid_at"]
            }
        
        queries.append((query, params))
    
    # Check for long table durations
    if event["table_duration_minutes"] > LONG_TABLE_MINUTES:
        query = """
            INSERT INTO alerts (alert_id, alert_type, message, triggered_at)
            VALUES (%(alert_id)s, %(alert_type)s, %(message)s, %(triggered_at)s)
        """
        params = {
            "alert_id": str(uuid.uuid4()),
            "alert_type": "long_table",
            "message": f"Table {event['table_number']} took {event['table_duration_minutes']:.2f} minutes to process",
            "triggered_at": event["paid_at"]
            }
        
        queries.append((query, params))
    
    # Check the total revenue
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(total_amount), 0) FROM recent_payments")
    result = cur.fetchone()
    total_revenue = float(result[0]) if result else 0.0
    cur.close()
 
    if total_revenue // REVENUE_MILESTONE > (total_revenue - float(event["total_amount"])) // REVENUE_MILESTONE and total_revenue > 0:
        query = """
            INSERT INTO alerts (alert_id, alert_type, message, triggered_at)
            VALUES (%(alert_id)s, %(alert_type)s, %(message)s, %(triggered_at)s)
        """
        params = {
            "alert_id": str(uuid.uuid4()),
            "alert_type": "revenue_milestone",
            "message": f"${REVENUE_MILESTONE} revenue milestone reached -- Total revenue: ${total_revenue}",
            "triggered_at": event["paid_at"]
            }
        
        queries.append((query, params))
    
    return queries
    
def main():
    """
    Main execution function for the alerts consumer. Fetches payment events from Kafka, checking
    for high bills, long table durations, and total revenue milestones. Writes the results to the
    alerts table in Postgres.
    """
    print("\n" + "=" * 20)
    print("Starting alerts consumer...\n")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    deserializer = AvroDeserializer(schema_registry_client, load_schema("payments.avsc"))

    # Connect to the Postgres database
    conn = connect_to_db()

    consumer_config = {**KAFKA_CONFIG, "group.id": "alerts-consumer-2", "auto.offset.reset": "latest"}
    consumer = Consumer(consumer_config)
    consumer.subscribe(["payments"])

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
        event = deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))

        queries = handle_payment_event(event, conn)
        
        # Write the event(s) to the postgres database
        for query, params in queries:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            cur.close()
            print(f"Event {event['paid_at']} processed successfully")

if __name__ == "__main__":
    main()

