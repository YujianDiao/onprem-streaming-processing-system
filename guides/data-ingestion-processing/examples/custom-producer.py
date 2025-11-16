#!/usr/bin/env python3
"""
Custom Kafka Producer Example
Demonstrates how to send your own data to the lakehouse platform
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
KAFKA_TOPIC = 'banking.transactions.raw'


def create_producer():
    """Create and configure a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Wait for all replicas to acknowledge
        retries=3,    # Retry up to 3 times on failure
        max_in_flight_requests_per_connection=1  # Ensure ordering
    )


def send_single_transaction(producer):
    """Send a single custom transaction"""
    transaction = {
        'transaction_id': f'custom-{int(time.time())}',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'account_id': 'ACC9999',
        'transaction_type': 'PURCHASE',
        'amount': 299.99,
        'currency': 'USD',
        'merchant': 'Custom Store',
        'location': {
            'city': 'San Francisco',
            'state': 'CA',
            'country': 'US',
            'latitude': 37.7749,
            'longitude': -122.4194
        },
        'customer': {
            'customer_id': 'CUST999',
            'name': 'Custom User',
            'email': 'custom@example.com',
            'phone': '+1-555-9999'
        },
        'is_fraud': False,
        'status': 'PENDING',
        'metadata': {
            'device_type': 'custom_script',
            'ip_address': '192.168.1.100',
            'session_id': 'custom-session-123'
        }
    }

    # Send to Kafka
    key = transaction['account_id']
    future = producer.send(KAFKA_TOPIC, key=key, value=transaction)

    # Wait for confirmation
    try:
        record_metadata = future.get(timeout=10)
        print(f"✓ Message sent successfully!")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partition: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")
        return True
    except Exception as e:
        print(f"✗ Failed to send message: {e}")
        return False


def send_batch_transactions(producer, count=10):
    """Send multiple transactions in batch"""
    print(f"\nSending {count} transactions...")

    successful = 0
    for i in range(count):
        transaction = {
            'transaction_id': f'batch-{int(time.time())}-{i}',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'account_id': f'ACC{1000 + i}',
            'transaction_type': 'PURCHASE',
            'amount': round(50.0 + (i * 10.5), 2),
            'currency': 'USD',
            'merchant': f'Merchant {i}',
            'location': {
                'city': 'New York',
                'state': 'NY',
                'country': 'US',
                'latitude': 40.7128,
                'longitude': -74.0060
            },
            'customer': {
                'customer_id': f'CUST{100 + i}',
                'name': f'User {i}',
                'email': f'user{i}@example.com',
                'phone': f'+1-555-{1000 + i}'
            },
            'is_fraud': False,
            'status': 'PENDING',
            'metadata': {
                'device_type': 'batch_script',
                'batch_id': f'batch-{int(time.time())}'
            }
        }

        try:
            producer.send(KAFKA_TOPIC, key=transaction['account_id'], value=transaction)
            successful += 1
            if (i + 1) % 10 == 0:
                print(f"  Sent {i + 1}/{count} transactions...")
        except Exception as e:
            print(f"  Failed to send transaction {i}: {e}")

    # Flush to ensure all messages are sent
    producer.flush()

    print(f"\n✓ Batch complete: {successful}/{count} transactions sent successfully")


def main():
    """Main function - demonstrates different usage patterns"""
    print("========================================")
    print("Custom Kafka Producer Example")
    print("========================================\n")

    try:
        # Create producer
        print("Connecting to Kafka...")
        producer = create_producer()
        print("✓ Connected to Kafka cluster\n")

        # Example 1: Send a single transaction
        print("Example 1: Sending a single custom transaction")
        print("-" * 40)
        send_single_transaction(producer)

        time.sleep(2)

        # Example 2: Send batch of transactions
        print("\nExample 2: Sending batch of transactions")
        print("-" * 40)
        send_batch_transactions(producer, count=10)

        print("\n========================================")
        print("Examples completed successfully!")
        print("========================================\n")

        print("Your data is now in Kafka and will be processed by Spark.")
        print("Wait ~30 seconds, then query with Trino:")
        print("  make shell-trino")
        print("  > SELECT * FROM iceberg.bronze.transactions")
        print("    WHERE account_id = 'ACC9999';")
        print()

    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Ensure Kafka is running: make status")
        print("  2. Check Kafka connectivity: make test-kafka")
        print("  3. Verify topic exists: docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092")

    finally:
        # Clean up
        if 'producer' in locals():
            producer.close()
            print("Producer closed.")


if __name__ == '__main__':
    main()
