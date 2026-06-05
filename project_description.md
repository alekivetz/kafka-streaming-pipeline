# Kafka POS Streaming Project

## Core Idea

A python producer simulates a busy restaurant (or multiple restaurants) generating realistic POS events. Consumers process those events in real time and feed a live Streamlit dashboard. 

### Topics
- orders: a new order is created (table number, server, timestamp)
- order-items: an order item is added to an order (item name, price, category, order id)
- payments: a payment is made (amount, payment method, timestamp)

## What is Kafka?

Think of Kafka as a messaging bus that sits in the middle of the system and passes messages between producers and consumers. Producers write in, consumers read out, and neither side knows the other exists. Kafka holds the messages in between so if a consumer is slow or down, the message will not be lost. 

### Producers and Consumers
- A producer is any process that writes messages into Kafka. It picks a topic and drops a message in, not knowing or caring who reads it.
- A consumer is any process that reads messages from Kafka. It subscribes to a topic and reads the messages as they come in, not knowing or caring where they came from.

Because they are decoupled, you can add a new consumer at any time without touching the producer or any other consumer. In our project the producer is the restaurant simulator and we have two consumers reading the same stream independently.

A consumer group is how Kafka manages multiple instances of the same consumer. Consumers in the same group split the messages between them so nothing gets processed twice. Consumers in different groups each get their full copy of every message. In our case, our metrics consumer and alerts consumer are in different groups so they both see every event. 

## Topics and Partitions

- A topic is a named mailbox inside Kafka. We have three: orders, order-items, and payments. The producer chooses which topic to write to, and consumers subscribe to the topics they care about.
- A partition is how Kafka splits a topic internally. Think of this like lanes on a highway, where more partitions means more consumers can read in parallel. We sset 3 partitions for each topic as a reasonable default.

## Arvo and Schema Registry

- Arvo is a format for defining what a message looks like, and that definition is called the schema. For example our Order schema says every order event has an order_id (string), table_number (int), server_name (string), and opened_at (timestamp).
- Schema Registry is a service that stores the schemas so both sides share the same rulebook. The producer registers its schema aon startip, ad when consumers receive a message they look up the schema and know exactly what fields to expect. Schemas are stored inside Kafka itself so the Schema Registry depends on Kafka being up first. 

