#!/usr/bin/env python3
"""
Async Gofka Consumer Example

Demonstrates async message consumption with consumer groups.
"""

import asyncio
import sys
from gofka import AsyncConsumer


async def main():
    # Create async consumer
    async with AsyncConsumer(
        brokers="localhost:9092",
        group_id="async-consumer-group",
        topics=["async-test-topic"],
        auto_commit=True,
        max_poll_records=10
    ) as consumer:
        print("✓ Connected to Gofka broker (async)")

        # Subscribe to topics
        await consumer.subscribe()
        print(f"✓ Subscribed to topics with partitions: {consumer.assigned_partitions}")

        # Consume messages using async iteration
        print("\nWaiting for messages...")
        count = 0

        async for message in consumer:
            print(f"✓ Received: {message.value.decode()} at offset {message.offset}")
            count += 1

            if count >= 10:
                break

        print(f"\nConsumed {count} messages")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nConsumer interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
