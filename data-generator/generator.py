#!/usr/bin/env python3
"""
Fake Data Generator for Kafka Streaming
Generates realistic banking transaction data for the lakehouse PoC
"""

import json
import time
import os
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'banking-transactions')
GENERATION_INTERVAL_MS = int(os.getenv('GENERATION_INTERVAL_MS', 1000))

# Initialize Faker
fake = Faker()

# Transaction types and their characteristics
TRANSACTION_TYPES = [
    ('PURCHASE', 0.45, 10, 500),
    ('WITHDRAWAL', 0.20, 20, 1000),
    ('DEPOSIT', 0.15, 50, 5000),
    ('TRANSFER', 0.15, 100, 10000),
    ('PAYMENT', 0.05, 500, 50000),
]

MERCHANTS = [
    'Amazon', 'Walmart', 'Target', 'Costco', 'Best Buy',
    'Apple Store', 'Whole Foods', 'Starbucks', 'McDonald\'s',
    'Shell Gas', 'Uber', 'Netflix', 'Spotify', 'Adobe'
]

LOCATIONS = [
    ('New York', 'NY', 'US'),
    ('Los Angeles', 'CA', 'US'),
    ('Chicago', 'IL', 'US'),
    ('Houston', 'TX', 'US'),
    ('Phoenix', 'AZ', 'US'),
    ('Philadelphia', 'PA', 'US'),
    ('San Antonio', 'TX', 'US'),
    ('San Diego', 'CA', 'US'),
    ('Dallas', 'TX', 'US'),
    ('San Jose', 'CA', 'US'),
]


def create_kafka_producer():
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )


def generate_transaction():
    """Generate a single fake transaction"""
    # Select transaction type based on weights
    tx_type, _, min_amount, max_amount = random.choices(
        TRANSACTION_TYPES,
        weights=[t[1] for t in TRANSACTION_TYPES]
    )[0]

    # Generate transaction details
    account_id = f"ACC{random.randint(1000, 9999)}"
    amount = round(random.uniform(min_amount, max_amount), 2)

    # Add some fraudulent transactions (2% chance)
    is_fraud = random.random() < 0.02
    if is_fraud:
        amount = round(random.uniform(10000, 50000), 2)

    city, state, country = random.choice(LOCATIONS)

    transaction = {
        'transaction_id': fake.uuid4(),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'account_id': account_id,
        'transaction_type': tx_type,
        'amount': amount,
        'currency': 'USD',
        'merchant': random.choice(MERCHANTS) if tx_type in ['PURCHASE', 'PAYMENT'] else None,
        'location': {
            'city': city,
            'state': state,
            'country': country,
            'latitude': round(random.uniform(25.0, 48.0), 6),
            'longitude': round(random.uniform(-125.0, -65.0), 6)
        },
        'customer': {
            'customer_id': f"CUST{random.randint(100, 999)}",
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number()
        },
        'is_fraud': is_fraud,
        'status': 'PENDING',
        'metadata': {
            'device_type': random.choice(['mobile', 'web', 'atm', 'pos']),
            'ip_address': fake.ipv4(),
            'session_id': fake.uuid4()
        }
    }

    return transaction


def main():
    """Main function to generate and send transactions"""
    print(f"Starting data generator...")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Target Topic: {KAFKA_TOPIC}")
    print(f"Generation Interval: {GENERATION_INTERVAL_MS}ms")

    # Wait for Kafka to be ready
    time.sleep(10)

    # Create producer
    producer = create_kafka_producer()
    print("Connected to Kafka successfully!")

    transaction_count = 0

    try:
        while True:
            # Generate transaction
            transaction = generate_transaction()

            # Send to Kafka
            key = transaction['account_id']
            producer.send(KAFKA_TOPIC, key=key, value=transaction)

            transaction_count += 1

            if transaction_count % 10 == 0:
                print(f"Generated {transaction_count} transactions...")

            # Wait before generating next transaction
            time.sleep(GENERATION_INTERVAL_MS / 1000.0)

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"Total transactions generated: {transaction_count}")


if __name__ == '__main__':
    main()
