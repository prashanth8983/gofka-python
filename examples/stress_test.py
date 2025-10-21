#!/usr/bin/env python3
"""
Stress Test for Gofka
Sends many messages and measures throughput/latency.
"""

import sys
import time
import argparse
from gofka.producer import Producer
from gofka.consumer import Consumer
from gofka.exceptions import GofkaError

def print_progress_bar(iteration, total, prefix='', suffix='', length=50):
    """Print a progress bar"""
    percent = 100 * (iteration / float(total))
    filled_length = int(length * iteration // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent:.1f}% {suffix}', end='')
    if iteration == total:
        print()

def stress_produce(broker_addr, topic, num_messages, message_size):
    """Stress test producer"""
    print(f"\n📤 Producer Stress Test")
    print(f"  Broker: {broker_addr}")
    print(f"  Topic: {topic}")
    print(f"  Messages: {num_messages}")
    print(f"  Message Size: {message_size} bytes")
    print()

    # Create test message
    message = b'x' * message_size

    producer = Producer(brokers=broker_addr)

    latencies = []
    start_time = time.time()

    with producer:
        for i in range(num_messages):
            send_start = time.time()
            try:
                offset = producer.send(topic, message, partition=0)
                latency = (time.time() - send_start) * 1000  # ms
                latencies.append(latency)

                # Progress bar
                if (i + 1) % max(1, num_messages // 100) == 0:
                    print_progress_bar(i + 1, num_messages, prefix='Sending:', suffix=f'msg {i+1}/{num_messages}')

            except GofkaError as e:
                print(f"\n❌ Error at message {i}: {e}")
                break

    elapsed = time.time() - start_time

    # Calculate statistics
    total_bytes = num_messages * message_size
    throughput = num_messages / elapsed if elapsed > 0 else 0
    bandwidth = (total_bytes / elapsed / 1024 / 1024) if elapsed > 0 else 0  # MB/s

    latencies.sort()
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p50_latency = latencies[len(latencies) // 2] if latencies else 0
    p95_latency = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99_latency = latencies[int(len(latencies) * 0.99)] if latencies else 0

    # Print results
    print()
    print("📊 Producer Results:")
    print(f"  Total Time: {elapsed:.2f}s")
    print(f"  Messages Sent: {len(latencies)}")
    print(f"  Total Bytes: {total_bytes:,} ({total_bytes/1024/1024:.2f} MB)")
    print(f"  Throughput: {throughput:.2f} msg/s")
    print(f"  Bandwidth: {bandwidth:.2f} MB/s")
    print()
    print("  Latency (ms):")
    print(f"    Average: {avg_latency:.2f}")
    print(f"    p50: {p50_latency:.2f}")
    print(f"    p95: {p95_latency:.2f}")
    print(f"    p99: {p99_latency:.2f}")
    print()

def stress_consume(broker_addr, topic, expected_messages):
    """Stress test consumer"""
    print(f"\n📥 Consumer Stress Test")
    print(f"  Broker: {broker_addr}")
    print(f"  Topic: {topic}")
    print(f"  Expected Messages: {expected_messages}")
    print()

    consumer = Consumer(
        brokers=broker_addr,
        group_id="stress-test-consumer",
        topics=[topic],
        client_id="stress-consumer"
    )

    messages_received = 0
    total_bytes = 0
    start_time = time.time()

    with consumer:
        consumer.subscribe()

        print("🔄 Consuming messages...")

        while messages_received < expected_messages:
            try:
                messages = consumer.poll(max_messages=10, timeout=1.0)

                if messages:
                    messages_received += len(messages)
                    for msg in messages:
                        total_bytes += len(msg.value)

                    # Progress bar
                    print_progress_bar(messages_received, expected_messages,
                                     prefix='Receiving:',
                                     suffix=f'msg {messages_received}/{expected_messages}')
                else:
                    # No more messages, might be done
                    time.sleep(0.1)

            except GofkaError as e:
                print(f"\n❌ Error: {e}")
                break

    elapsed = time.time() - start_time

    # Calculate statistics
    throughput = messages_received / elapsed if elapsed > 0 else 0
    bandwidth = (total_bytes / elapsed / 1024 / 1024) if elapsed > 0 else 0  # MB/s

    # Print results
    print()
    print("📊 Consumer Results:")
    print(f"  Total Time: {elapsed:.2f}s")
    print(f"  Messages Received: {messages_received}")
    print(f"  Total Bytes: {total_bytes:,} ({total_bytes/1024/1024:.2f} MB)")
    print(f"  Throughput: {throughput:.2f} msg/s")
    print(f"  Bandwidth: {bandwidth:.2f} MB/s")
    print()

def main():
    parser = argparse.ArgumentParser(description='Gofka Stress Test')
    parser.add_argument('--broker', default='localhost:9092', help='Broker address')
    parser.add_argument('--topic', default='stress-test', help='Topic name')
    parser.add_argument('--messages', type=int, default=1000, help='Number of messages')
    parser.add_argument('--size', type=int, default=100, help='Message size in bytes')
    parser.add_argument('--mode', choices=['produce', 'consume', 'both'], default='both',
                       help='Test mode')

    args = parser.parse_args()

    print("=" * 60)
    print("  Gofka Stress Test")
    print("=" * 60)

    try:
        if args.mode in ['produce', 'both']:
            stress_produce(args.broker, args.topic, args.messages, args.size)

        if args.mode in ['consume', 'both']:
            if args.mode == 'both':
                print("\n" + "=" * 60)
            stress_consume(args.broker, args.topic, args.messages)

    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

    print("✅ Stress test completed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
