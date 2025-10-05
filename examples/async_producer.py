#!/usr/bin/env python3
"""
Async Gofka Producer Example

Demonstrates async message production with batching and compression.
"""

import asyncio
import sys
from gofka import AsyncProducer


async def main():
    # Create async producer with compression and batching
    async with AsyncProducer(
        brokers="localhost:9092",
        compression="gzip",  # Enable gzip compression
        batch_size=16384,
        linger_ms=100
    ) as producer:
        print("✓ Connected to Gofka broker (async)")

        # Send messages concurrently
        topic = "async-test-topic"
        tasks = []

        for i in range(10):
            message = f"Async message #{i} with compression"
            task = producer.send_string(topic, message, partition=0)
            tasks.append(task)

        # Wait for all messages to be sent
        offsets = await asyncio.gather(*tasks)

        for i, offset in enumerate(offsets):
            print(f"✓ Sent message {i} at offset {offset}")

        print(f"\nSuccessfully sent {len(offsets)} messages asynchronously")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProducer interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
