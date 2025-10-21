#!/usr/bin/env python3
"""
Interactive Gofka Consumer
Polls for new messages and displays them in real-time.
"""

import sys
import time
import signal
from gofka.consumer import Consumer
from gofka.exceptions import GofkaError

# Global flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n\n⚠️  Shutting down gracefully...")
    running = False

def print_banner():
    print("=" * 60)
    print("  Gofka Interactive Consumer")
    print("=" * 60)
    print()

def format_message(msg, index):
    """Format a message for display"""
    try:
        decoded = msg.value.decode('utf-8')
        return f"📩 Message #{index}: {decoded}"
    except:
        return f"📩 Message #{index}: {msg.value.hex()} (binary)"

def main():
    # Configuration
    broker_addr = "localhost:9092"
    topic = "test-topic"
    group_id = "interactive-consumer-group"

    # Parse command line arguments
    if len(sys.argv) > 1:
        broker_addr = sys.argv[1]
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    if len(sys.argv) > 3:
        group_id = sys.argv[3]

    print_banner()
    print(f"📡 Broker: {broker_addr}")
    print(f"📝 Topic: {topic}")
    print(f"👥 Consumer Group: {group_id}")
    print(f"🔢 Partition: 0")
    print(f"⏱️  Poll Interval: 1 second")
    print()
    print("Commands:")
    print("  - Press Ctrl+C to stop gracefully")
    print()
    print("-" * 60)
    print()

    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    # Create consumer
    try:
        consumer = Consumer(
            brokers=broker_addr,
            group_id=group_id,
            topics=[topic],
            client_id=f"interactive-consumer-{int(time.time())}"
        )
        print("✅ Created consumer")
    except Exception as e:
        print(f"❌ Failed to create consumer: {e}")
        return 1

    # Statistics
    messages_consumed = 0
    bytes_consumed = 0
    start_time = time.time()
    last_offset = -1

    print("🔄 Starting to poll for messages...")
    print("   (Waiting for new messages...)")
    print()

    try:
        with consumer:
            # Subscribe to consumer group
            consumer.subscribe()

            # Poll interval
            poll_interval = 1.0  # 1 second
            last_poll_time = time.time()

            while running:
                try:
                    # Poll for messages
                    messages = consumer.poll(max_messages=10, timeout=1.0)

                    if messages:
                        for msg in messages:
                            messages_consumed += 1
                            bytes_consumed += len(msg.value)
                            last_offset = msg.offset

                            # Display message
                            print(format_message(msg, messages_consumed))

                            # Show statistics every 10 messages
                            if messages_consumed % 10 == 0:
                                elapsed = time.time() - start_time
                                rate = messages_consumed / elapsed if elapsed > 0 else 0
                                print(f"   📊 Stats: {messages_consumed} msgs, {rate:.2f} msg/s")

                        # Show polling indicator
                        current_time = time.time()
                        if current_time - last_poll_time >= poll_interval:
                            print(f"   🔄 Polling... (last offset: {last_offset})")
                            last_poll_time = current_time
                    else:
                        # No messages, show waiting indicator periodically
                        current_time = time.time()
                        if current_time - last_poll_time >= poll_interval:
                            print(f"   ⏳ Waiting for new messages... (offset: {last_offset})")
                            last_poll_time = current_time

                    # Small sleep to prevent busy-waiting
                    time.sleep(0.1)

                except GofkaError as e:
                    print(f"   ❌ Error polling: {e}")
                    time.sleep(1)  # Wait before retrying

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

    # Print final statistics
    print()
    print("-" * 60)
    elapsed = time.time() - start_time
    print("📊 Final Statistics:")
    print(f"  Total messages consumed: {messages_consumed}")
    print(f"  Total bytes consumed: {bytes_consumed}")
    print(f"  Total time: {elapsed:.2f}s")
    if elapsed > 0 and messages_consumed > 0:
        print(f"  Average rate: {messages_consumed/elapsed:.2f} msg/s")
    print()

    return 0

if __name__ == "__main__":
    sys.exit(main())
