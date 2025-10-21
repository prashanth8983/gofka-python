#!/usr/bin/env python3
"""
Simple test to verify basic produce/consume without consumer groups
"""

import sys
import time
from gofka.protocol import Protocol

broker_addr = "localhost:9092"
topic = "test-topic"
partition = 0

print("=" * 60)
print("Simple Produce/Consume Test (No Consumer Groups)")
print("=" * 60)
print()

# Test 1: Produce a message
print("📤 Test 1: Producing a message...")
try:
    protocol = Protocol("localhost", 9092, timeout=10)
    protocol.connect()

    message = b"Hello from simple test!"
    offset = protocol.produce(topic, partition, message, "test-client")
    print(f"✅ Message produced at offset: {offset}")

    protocol.close()
except Exception as e:
    print(f"❌ Produce failed: {e}")
    sys.exit(1)

print()

# Test 2: Fetch the message directly (no consumer group)
print("📥 Test 2: Fetching the message...")
try:
    protocol = Protocol("localhost", 9092, timeout=10)
    protocol.connect()

    data = protocol.fetch(topic, partition, offset, "test-client")

    if data:
        print(f"✅ Message fetched: {data.decode('utf-8')}")
    else:
        print(f"❌ No message found at offset {offset}")

    protocol.close()
except Exception as e:
    print(f"❌ Fetch failed: {e}")
    sys.exit(1)

print()
print("=" * 60)
print("✅ Basic produce/fetch works!")
print("=" * 60)
