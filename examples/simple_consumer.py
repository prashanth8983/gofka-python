#!/usr/bin/env python3
"""
Simple Gofka Consumer Example

Demonstrates consuming messages from a Gofka topic using consumer groups.
"""

import sys
import time
from gofka import Consumer

def main():
    # Create consumer
    consumer = Consumer(
        brokers="localhost:9092",
        group_id="python-test-group",
        topics=["python-test-topic"],
        auto_commit=True
    )

    try:
        # Connect and subscribe
        print("Connecting to Gofka broker...")
        consumer.connect()
        print("✓ Connected!")

        print("Subscribing to topics...")
        consumer.subscribe()
        print("✓ Subscribed!")

        # Poll for messages
        print("\nPolling for messages (will try 10 times)...")
        messages_consumed = 0

        for attempt in range(10):
            messages = consumer.poll(max_messages=5)

            if messages:
                for msg in messages:
                    print(f"✓ Received: topic={msg.topic}, partition={msg.partition}, "
                          f"offset={msg.offset}, value={msg.value.decode()}")
                    messages_consumed += 1
            else:
                print(f"  No messages (attempt {attempt+1}/10)")

            # Send heartbeat to maintain group membership
            consumer.send_heartbeat()

            time.sleep(0.5)

        print(f"\nConsumed {messages_consumed} messages total")

    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
