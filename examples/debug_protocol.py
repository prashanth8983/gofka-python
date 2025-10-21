#!/usr/bin/env python3
"""
Debug protocol to see exactly what bytes are being sent/received
"""

import socket
import struct

host = "localhost"
port = 9092
topic = "test-topic"
partition = 0
message = b"Hello World"

print("=" * 60)
print("Protocol Debug")
print("=" * 60)
print()

# Connect
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(10)
sock.connect((host, port))
print("✅ Connected")

# Build produce request manually
correlation_id = 1
client_id = "debug-client"

request = bytearray()

# API key (2 bytes) - PRODUCE = 0
request.extend(struct.pack('>H', 0))
print(f"API Key: 0 (PRODUCE)")

# API version (2 bytes)
request.extend(struct.pack('>H', 0))
print(f"API Version: 0")

# Correlation ID (4 bytes)
request.extend(struct.pack('>I', correlation_id))
print(f"Correlation ID: {correlation_id}")

# Client ID (2-byte length + string)
client_id_bytes = client_id.encode('utf-8')
request.extend(struct.pack('>H', len(client_id_bytes)))
request.extend(client_id_bytes)
print(f"Client ID: {client_id}")

# Topic (2-byte length + string)
topic_bytes = topic.encode('utf-8')
request.extend(struct.pack('>H', len(topic_bytes)))
request.extend(topic_bytes)
print(f"Topic: {topic}")

# Partition (4 bytes)
request.extend(struct.pack('>I', partition))
print(f"Partition: {partition}")

# Message length (4 bytes)
request.extend(struct.pack('>I', len(message)))
print(f"Message length: {len(message)}")

# Message
request.extend(message)
print(f"Message: {message.decode('utf-8')}")

print()
print(f"Total request payload size: {len(request)} bytes")

# Prepend size
full_request = struct.pack('>I', len(request)) + request
print(f"Total request with size header: {len(full_request)} bytes")

print()
print("Sending request...")
sock.sendall(full_request)
print("✅ Request sent")

print()
print("Reading response...")

# Read correlation ID (4 bytes)
correlation_data = sock.recv(4)
if len(correlation_data) < 4:
    print(f"❌ Only received {len(correlation_data)} bytes for correlation ID")
    sock.close()
    exit(1)

recv_correlation_id = struct.unpack('>I', correlation_data)[0]
print(f"Received correlation ID: {recv_correlation_id}")

# Read next byte - could be error flag (1 byte) or first byte of partition (4 bytes)
first_byte_data = sock.recv(1)
if len(first_byte_data) < 1:
    print(f"❌ No data after correlation ID")
    sock.close()
    exit(1)

first_byte = first_byte_data[0]
print(f"First byte after correlation ID: {first_byte} (hex: {first_byte:02x})")

if first_byte == 0:
    print("⚠️  This is an ERROR RESPONSE (error flag = 0)")

    # Read error message length (2 bytes)
    len_data = sock.recv(2)
    error_len = struct.unpack('>H', len_data)[0]
    print(f"Error message length: {error_len}")
    print(f"Error length bytes (hex): {len_data.hex()}")

    if error_len > 0:
        # Read error message
        error_msg_data = sock.recv(error_len)
        print(f"Error message bytes (first 20): {error_msg_data[:20].hex()}")
        error_msg = error_msg_data.decode('utf-8', errors='replace')
        print(f"❌ ERROR MESSAGE: {error_msg}")
    else:
        print("❌ ERROR MESSAGE: (empty)")
else:
    print("✅ This is a SUCCESS RESPONSE")

    # Read remaining 3 bytes of partition (we already read first byte)
    partition_remaining = sock.recv(3)
    partition_data = first_byte_data + partition_remaining
    recv_partition = struct.unpack('>I', partition_data)[0]
    print(f"Received partition: {recv_partition}")

    # Read offset (8 bytes)
    offset_data = sock.recv(8)
    if len(offset_data) < 8:
        print(f"❌ Only received {len(offset_data)} bytes for offset")
        sock.close()
        exit(1)

    recv_offset = struct.unpack('>Q', offset_data)[0]
    print(f"Received offset: {recv_offset}")

sock.close()
print()
print("=" * 60)
