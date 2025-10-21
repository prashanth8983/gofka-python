#!/usr/bin/env python3
"""
Test creating a topic explicitly before producing
"""

import socket
import struct

host = "localhost"
port = 9092
topic = "test-topic"

print("=" * 60)
print("Create Topic Test")
print("=" * 60)
print()

# Connect
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(10)
sock.connect((host, port))
print("✅ Connected")

# Build metadata request to check if topic exists
correlation_id = 1
client_id = "debug-client"

request = bytearray()

# API key (2 bytes) - METADATA = 3
request.extend(struct.pack('>H', 3))
print(f"API Key: 3 (METADATA)")

# API version (2 bytes)
request.extend(struct.pack('>H', 0))

# Correlation ID (4 bytes)
request.extend(struct.pack('>I', correlation_id))

# Client ID (2-byte length + string)
client_id_bytes = client_id.encode('utf-8')
request.extend(struct.pack('>H', len(client_id_bytes)))
request.extend(client_id_bytes)

# Number of topics (4 bytes) - requesting all topics
request.extend(struct.pack('>I', 0))

print()
print(f"Total request payload size: {len(request)} bytes")

# Prepend size
full_request = struct.pack('>I', len(request)) + request

print("Sending metadata request...")
sock.sendall(full_request)
print("✅ Request sent")

print()
print("Reading response...")

# Read correlation ID (4 bytes)
correlation_data = sock.recv(4)
recv_correlation_id = struct.unpack('>I', correlation_data)[0]
print(f"Received correlation ID: {recv_correlation_id}")

# Read number of brokers (4 bytes)
broker_count_data = sock.recv(4)
broker_count = struct.unpack('>I', broker_count_data)[0]
print(f"Number of brokers: {broker_count}")

# Skip broker details for now
for i in range(broker_count):
    # Read broker ID length
    id_len_data = sock.recv(2)
    id_len = struct.unpack('>H', id_len_data)[0]
    # Read broker ID
    broker_id = sock.recv(id_len).decode('utf-8')
    # Read broker addr length
    addr_len_data = sock.recv(2)
    addr_len = struct.unpack('>H', addr_len_data)[0]
    # Read broker addr
    broker_addr = sock.recv(addr_len).decode('utf-8')
    print(f"  Broker {i}: {broker_id} @ {broker_addr}")

# Read number of topics (4 bytes)
topic_count_data = sock.recv(4)
topic_count = struct.unpack('>I', topic_count_data)[0]
print(f"Number of topics: {topic_count}")

for i in range(topic_count):
    # Read topic name length
    topic_len_data = sock.recv(2)
    topic_len = struct.unpack('>H', topic_len_data)[0]
    # Read topic name
    topic_name = sock.recv(topic_len).decode('utf-8')
    print(f"  Topic {i}: {topic_name}")

    # Read partition count
    partition_count_data = sock.recv(4)
    partition_count = struct.unpack('>I', partition_count_data)[0]
    print(f"    Partitions: {partition_count}")

    # Skip partition details
    for j in range(partition_count):
        # Partition ID (4 bytes)
        sock.recv(4)
        # Leader length
        leader_len = struct.unpack('>H', sock.recv(2))[0]
        # Leader
        sock.recv(leader_len)
        # Replica count
        replica_count = struct.unpack('>I', sock.recv(4))[0]
        # Replicas
        for k in range(replica_count):
            replica_len = struct.unpack('>H', sock.recv(2))[0]
            sock.recv(replica_len)

sock.close()
print()
print("=" * 60)
