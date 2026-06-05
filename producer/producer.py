# =============================================================================
# Producer
# =============================================================================
# Script Purpose:
#     This script is acts as the connection point between the restaurant
#     simulator and Kafka. It is responsible for taking events created by
#     the simulator, serializing them using the Avro schemas, and sending
#     them to the correct Kafka topic.
# =============================================================================

import time
import os
import sys
import simulator
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import record_subject_name_strategy
from confluent_kafka.serialization import SerializationContext, MessageField
 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.utils import load_schema, KAFKA_CONFIG, SCHEMA_REGISTRY_CONFIG

def delivery_report(err, msg):
    """
    Callback function for the producer. 
    Called once per message to confirm delivery or report failure.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
def send_event(producer, topic, event, serializer):
    """
    This function is responsible for serializing and sending an event to Kafka using the producer.
    """
    producer.produce(
        topic=topic,
        value=serializer(event, SerializationContext(topic, MessageField.VALUE)),
        on_delivery=delivery_report
    )

def main():
    """
    Main execution function for the producer. 
    Creates a producer, loads the schemas, and starts the simulation,
    sending events to Kafka.
    """
    print("\n" + "=" * 20)
    print("Starting producer...\n")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    producer = Producer(KAFKA_CONFIG)

    serializers = {
        "orders": AvroSerializer(schema_registry_client, load_schema("order.avsc"), conf={'subject.name.strategy': record_subject_name_strategy}),
        "order-items": AvroSerializer(schema_registry_client, load_schema("order_item.avsc"), conf={'subject.name.strategy': record_subject_name_strategy}),
        "payments": AvroSerializer(schema_registry_client, load_schema("payments.avsc"), conf={'subject.name.strategy': record_subject_name_strategy})
    }
    print("Starting simulation...\n")
    print("=" * 20)
    print("Sending events to Kafka...\n")
    sim = simulator.RestaurantSimulator()
    while True:
        events = sim.tick()
        for topic, event in events:
            send_event(producer, topic, event, serializers[topic])
        producer.poll(0)        # check for deliered messages
        time.sleep(1)
    producer.flush()            # drain the buffer before exiting


if __name__ == "__main__":
    main()


    



    