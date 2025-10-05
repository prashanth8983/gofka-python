#!/usr/bin/env python3
"""
Consumer Performance Benchmark

Measures throughput and latency for message consumption.
"""

import asyncio
import time
import sys
from gofka import AsyncProducer, AsyncConsumer


async def setup_messages(brokers: str, topic: str, num_messages: int, message_size: int):
    """Produce test messages"""
    print(f"Setting up {num_messages} test messages...")

    message = b'x' * message_size

    async with AsyncProducer(brokers=brokers) as producer:
        tasks = [producer.send(topic, message, partition=0) for _ in range(num_messages)]
        await asyncio.gather(*tasks)

    print(f"✓ Setup complete")


async def benchmark_async_consumer(
    brokers: str,
    topic: str,
    num_messages: int,
    message_size: int
):
    """
    Benchmark async consumer performance

    Args:
        brokers: Broker addresses
        topic: Topic name
        num_messages: Number of messages to consume
        message_size: Size of each message in bytes
    """
    print(f"\n=== Async Consumer Benchmark ===")
    print(f"Messages: {num_messages}")
    print(f"Message size: {message_size} bytes")

    # Setup test messages
    await setup_messages(brokers, topic, num_messages, message_size)

    consumed = 0
    start_time = time.time()

    async with AsyncConsumer(
        brokers=brokers,
        group_id=f"benchmark-group-{int(time.time())}",
        topics=[topic],
        auto_commit=True,
        max_poll_records=100
    ) as consumer:
        await consumer.subscribe()

        # Consume messages
        while consumed < num_messages:
            messages = await consumer.poll(max_messages=100)
            consumed += len(messages)

            if not messages:
                await asyncio.sleep(0.01)

    end_time = time.time()
    duration = end_time - start_time

    # Calculate metrics
    throughput = num_messages / duration

    print(f"\n=== Results ===")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.2f} messages/sec")
    print(f"Throughput: {(throughput * message_size / 1024 / 1024):.2f} MB/sec")

    return {
        "throughput_msg_sec": throughput,
        "throughput_mb_sec": throughput * message_size / 1024 / 1024,
        "duration_sec": duration
    }


async def main():
    brokers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    topic = "benchmark-consumer-topic"

    # Test different configurations
    configs = [
        {"num_messages": 1000, "message_size": 100},
        {"num_messages": 1000, "message_size": 1024},
        {"num_messages": 5000, "message_size": 100},
    ]

    results = []
    for config in configs:
        result = await benchmark_async_consumer(brokers, topic, **config)
        results.append(result)
        await asyncio.sleep(1)

    print("\n=== Summary ===")
    for i, result in enumerate(results):
        print(f"\nConfig {i+1}:")
        print(f"  Throughput: {result['throughput_msg_sec']:.2f} msg/sec, "
              f"{result['throughput_mb_sec']:.2f} MB/sec")
        print(f"  Duration: {result['duration_sec']:.2f} sec")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
