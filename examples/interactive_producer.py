#!/usr/bin/env python3
"""
Interactive Gofka Producer
Allows user to send messages interactively with real-time feedback.
"""

import sys
import time
from gofka.producer import Producer
from gofka.exceptions import GofkaError

def print_banner():
    print("=" * 60)
    print("  Gofka Interactive Producer")
    print("=" * 60)
    print()

def main():
    # Configuration
    broker_addr = "localhost:9092"
    topic = "test-topic"

    # Parse command line arguments
    if len(sys.argv) > 1:
        broker_addr = sys.argv[1]
    if len(sys.argv) > 2:
        topic = sys.argv[2]

    print_banner()
    print(f"📡 Broker: {broker_addr}")
    print(f"📝 Topic: {topic}")
    print(f"🔢 Partition: 0")
    print()
    print("Commands:")
    print("  - Type a message and press Enter to send")
    print("  - Type 'quit' or 'exit' to stop")
    print("  - Type 'stats' to see statistics")
    print("  - Press Ctrl+C to exit")
    print()
    print("-" * 60)
    print()

    # Create producer
    try:
        producer = Producer(brokers=broker_addr)
        print("✅ Created producer")
        print()
    except Exception as e:
        print(f"❌ Failed to create producer: {e}")
        return 1

    # Statistics
    messages_sent = 0
    bytes_sent = 0
    start_time = time.time()

    try:
        with producer:
            while True:
                try:
                    # Get user input
                    message = input("📤 Enter message: ").strip()

                    # Check for commands
                    if message.lower() in ['quit', 'exit']:
                        print("\n👋 Goodbye!")
                        break

                    if message.lower() == 'stats':
                        elapsed = time.time() - start_time
                        print()
                        print("📊 Statistics:")
                        print(f"  Messages sent: {messages_sent}")
                        print(f"  Bytes sent: {bytes_sent}")
                        print(f"  Elapsed time: {elapsed:.2f}s")
                        if elapsed > 0:
                            print(f"  Avg rate: {messages_sent/elapsed:.2f} msg/s")
                        print()
                        continue

                    if not message:
                        continue

                    # Send message
                    send_start = time.time()
                    offset = producer.send(topic, message.encode('utf-8'), partition=0)
                    send_time = (time.time() - send_start) * 1000  # Convert to ms

                    # Update statistics
                    messages_sent += 1
                    bytes_sent += len(message)

                    # Print confirmation
                    print(f"   ✅ Sent! Offset: {offset}, Latency: {send_time:.2f}ms")
                    print()

                except GofkaError as e:
                    print(f"   ❌ Error sending message: {e}")
                    print()
                except KeyboardInterrupt:
                    print("\n\n⚠️  Interrupted by user")
                    break
                except EOFError:
                    print("\n\n👋 Goodbye!")
                    break

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

    # Print final statistics
    print()
    print("-" * 60)
    elapsed = time.time() - start_time
    print("📊 Final Statistics:")
    print(f"  Total messages sent: {messages_sent}")
    print(f"  Total bytes sent: {bytes_sent}")
    print(f"  Total time: {elapsed:.2f}s")
    if elapsed > 0 and messages_sent > 0:
        print(f"  Average rate: {messages_sent/elapsed:.2f} msg/s")
        print(f"  Average latency: {(elapsed/messages_sent)*1000:.2f}ms")
    print()

    return 0

if __name__ == "__main__":
    sys.exit(main())
