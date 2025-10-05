#!/usr/bin/env python3
"""
Producer Performance Benchmark

Measures throughput and latency for message production.
"""

import asyncio
import time
import sys
from gofka import AsyncProducer


async def benchmark_async_producer(
    brokers: str,
    topic: str,
    num_messages: int,
    message_size: int,
    compression: str = None
):
    """
    Benchmark async producer performance

    Args:
        brokers: Broker addresses
        topic: Topic name
        num_messages: Number of messages to send
        message_size: Size of each message in bytes
        compression: Compression type (None, 'gzip', 'snappy', 'lz4')
    """
    print(f"\n=== Async Producer Benchmark ===")
    print(f"Messages: {num_messages}")
    print(f"Message size: {message_size} bytes")
    print(f"Compression: {compression or 'None'}")

    # Create test message
    message = b'x' * message_size

    latencies = []

    async with AsyncProducer(brokers=brokers, compression=compression) as producer:
        # Warmup
        for _ in range(10):
            await producer.send(topic, message, partition=0)

        # Benchmark
        start_time = time.time()

        tasks = []
        for i in range(num_messages):
            msg_start = time.time()
            task = producer.send(topic, message, partition=0)
            tasks.append((task, msg_start))

        # Wait for all messages
        for task, msg_start in tasks:
            await task
            latencies.append((time.time() - msg_start) * 1000)  # Convert to ms

        end_time = time.time()
        duration = end_time - start_time

    # Calculate metrics
    throughput = num_messages / duration
    avg_latency = sum(latencies) / len(latencies)
    p50 = sorted(latencies)[int(len(latencies) * 0.5)]
    p95 = sorted(latencies)[int(len(latencies) * 0.95)]
    p99 = sorted(latencies)[int(len(latencies) * 0.99)]

    print(f"\n=== Results ===")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.2f} messages/sec")
    print(f"Throughput: {(throughput * message_size / 1024 / 1024):.2f} MB/sec")
    print(f"\nLatency (ms):")
    print(f"  Average: {avg_latency:.2f}")
    print(f"  p50: {p50:.2f}")
    print(f"  p95: {p95:.2f}")
    print(f"  p99: {p99:.2f}")

    return {
        "throughput_msg_sec": throughput,
        "throughput_mb_sec": throughput * message_size / 1024 / 1024,
        "avg_latency_ms": avg_latency,
        "p50_latency_ms": p50,
        "p95_latency_ms": p95,
        "p99_latency_ms": p99
    }


async def main():
    brokers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    topic = "benchmark-topic"

    # Test different configurations
    configs = [
        {"num_messages": 1000, "message_size": 100, "compression": None},
        {"num_messages": 1000, "message_size": 100, "compression": "gzip"},
        {"num_messages": 1000, "message_size": 1024, "compression": None},
        {"num_messages": 1000, "message_size": 1024, "compression": "gzip"},
    ]

    results = []
    for config in configs:
        result = await benchmark_async_producer(brokers, topic, **config)
        results.append(result)
        await asyncio.sleep(1)  # Cool down between tests

    print("\n=== Summary ===")
    for i, result in enumerate(results):
        print(f"\nConfig {i+1}:")
        print(f"  Throughput: {result['throughput_msg_sec']:.2f} msg/sec, "
              f"{result['throughput_mb_sec']:.2f} MB/sec")
        print(f"  Latency p99: {result['p99_latency_ms']:.2f} ms")


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
