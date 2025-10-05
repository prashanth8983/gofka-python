#!/usr/bin/env python3
"""
Simple Gofka Producer Example

Demonstrates basic message production to a Gofka topic.
"""

import sys
from gofka import Producer

def main():
    # Create producer
    producer = Producer(brokers="localhost:9092")

    try:
        # Connect to broker
        print("Connecting to Gofka broker...")
        producer.connect()
        print("✓ Connected!")

        # Send some messages
        topic = "python-test-topic"
        for i in range(5):
            message = f"Hello from Python producer! Message #{i}"
            offset = producer.send_string(topic, message, partition=0)
            print(f"✓ Sent message {i} at offset {offset}: {message}")

        print(f"\nSuccessfully sent 5 messages to topic '{topic}'")

    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
