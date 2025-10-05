"""
Gofka binary wire protocol implementation

Implements the Gofka binary protocol for communication with brokers.
"""

import struct
import socket
from typing import Optional, List, Tuple
from .exceptions import ConnectionError, GofkaError


# API Keys matching the Gofka protocol
class APIKey:
    PRODUCE = 0
    FETCH = 1
    METADATA = 3
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    DESCRIBE_GROUPS = 15
    LIST_GROUPS = 16


class Protocol:
    """Handles low-level Gofka protocol communication"""

    def __init__(self, host: str, port: int, timeout: int = 30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.correlation_id = 0

    def connect(self):
        """Establish connection to Gofka broker"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    def close(self):
        """Close connection to broker"""
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

    def _next_correlation_id(self) -> int:
        """Get next correlation ID"""
        self.correlation_id += 1
        return self.correlation_id

    def _write_string(self, buf: bytearray, s: str):
        """Write a length-prefixed string"""
        encoded = s.encode('utf-8')
        buf.extend(struct.pack('>H', len(encoded)))
        buf.extend(encoded)

    def _read_string(self, data: bytes, offset: int) -> Tuple[str, int]:
        """Read a length-prefixed string"""
        length = struct.unpack_from('>H', data, offset)[0]
        offset += 2
        s = data[offset:offset+length].decode('utf-8')
        offset += length
        return s, offset

    def _build_request_header(self, api_key: int, api_version: int, client_id: str) -> bytearray:
        """Build common request header"""
        correlation_id = self._next_correlation_id()
        buf = bytearray()

        # API key (2 bytes)
        buf.extend(struct.pack('>H', api_key))
        # API version (2 bytes)
        buf.extend(struct.pack('>H', api_version))
        # Correlation ID (4 bytes)
        buf.extend(struct.pack('>I', correlation_id))
        # Client ID
        self._write_string(buf, client_id)

        return buf

    def _send_request(self, api_key: int, api_version: int, client_id: str, payload: bytes) -> bytes:
        """Send request and receive response"""
        if not self.sock:
            raise ConnectionError("Not connected to broker")

        # Build request header
        request = self._build_request_header(api_key, api_version, client_id)
        request.extend(payload)

        # Prepend size (4 bytes)
        size = len(request)
        full_request = struct.pack('>I', size) + request

        # Send request
        try:
            self.sock.sendall(full_request)
        except Exception as e:
            raise ConnectionError(f"Failed to send request: {e}")

        # Read response
        try:
            # Read correlation ID (4 bytes)
            correlation_id_data = self._recv_exact(4)
            correlation_id = struct.unpack('>I', correlation_id_data)[0]

            # For now, return remaining data
            # In production, you'd want to read a size prefix
            # For simplicity, we read available data
            return self.sock.recv(65536)
        except Exception as e:
            raise ConnectionError(f"Failed to receive response: {e}")

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by broker")
            data += chunk
        return data

    def produce(self, topic: str, partition: int, message: bytes, client_id: str = "gofka-python") -> int:
        """
        Produce a message to a topic
        Returns: offset of the produced message
        """
        # Build produce payload
        payload = bytearray()
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>I', len(message)))
        payload.extend(message)

        # Send request
        response = self._send_request(APIKey.PRODUCE, 0, client_id, bytes(payload))

        # Parse response: partition (4 bytes) + offset (8 bytes)
        if len(response) >= 12:
            partition_resp, offset = struct.unpack('>IQ', response[:12])
            return offset
        else:
            raise GofkaError("Invalid produce response")

    def fetch(self, topic: str, partition: int, offset: int, client_id: str = "gofka-python") -> Optional[bytes]:
        """
        Fetch a message from a topic
        Returns: message bytes or None if not found
        """
        # Build fetch payload
        payload = bytearray()
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>Q', offset))

        # Send request
        response = self._send_request(APIKey.FETCH, 0, client_id, bytes(payload))

        # Parse response: message_len (4 bytes) + message
        if len(response) >= 4:
            message_len = struct.unpack('>I', response[:4])[0]
            if len(response) >= 4 + message_len:
                return response[4:4+message_len]

        return None

    def get_metadata(self, topics: List[str], client_id: str = "gofka-python") -> dict:
        """
        Get cluster metadata
        Returns: dict with brokers and topics info
        """
        # Build metadata payload
        payload = bytearray()
        payload.extend(struct.pack('>I', len(topics)))
        for topic in topics:
            self._write_string(payload, topic)

        # Send request
        response = self._send_request(APIKey.METADATA, 0, client_id, bytes(payload))

        # Parse response
        offset = 0

        # Number of brokers
        num_brokers = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        brokers = []
        for _ in range(num_brokers):
            broker_id, offset = self._read_string(response, offset)
            broker_addr, offset = self._read_string(response, offset)
            brokers.append({"id": broker_id, "addr": broker_addr})

        # Number of topics
        num_topics = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        topics_info = []
        for _ in range(num_topics):
            topic_name, offset = self._read_string(response, offset)
            num_partitions = struct.unpack_from('>I', response, offset)[0]
            offset += 4

            partitions = []
            for _ in range(num_partitions):
                partition_id = struct.unpack_from('>I', response, offset)[0]
                offset += 4
                leader, offset = self._read_string(response, offset)
                num_replicas = struct.unpack_from('>I', response, offset)[0]
                offset += 4

                replicas = []
                for _ in range(num_replicas):
                    replica, offset = self._read_string(response, offset)
                    replicas.append(replica)

                partitions.append({
                    "id": partition_id,
                    "leader": leader,
                    "replicas": replicas
                })

            topics_info.append({
                "name": topic_name,
                "partitions": partitions
            })

        return {
            "brokers": brokers,
            "topics": topics_info
        }

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int,
                      metadata: str = "", client_id: str = "gofka-python") -> bool:
        """Commit offset for a consumer group"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>Q', offset))
        self._write_string(payload, metadata)

        response = self._send_request(APIKey.OFFSET_COMMIT, 0, client_id, bytes(payload))

        # Check success flag
        if len(response) >= 1:
            success = response[0]
            return success == 1
        return False

    def fetch_offset(self, group_id: str, topic: str, partition: int,
                     client_id: str = "gofka-python") -> int:
        """Fetch committed offset for a consumer group"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))

        response = self._send_request(APIKey.OFFSET_FETCH, 0, client_id, bytes(payload))

        # Parse offset (8 bytes)
        if len(response) >= 8:
            offset = struct.unpack('>Q', response[:8])[0]
            return offset
        return -1

    def join_group(self, group_id: str, member_id: str, client_id: str,
                   client_host: str, session_timeout: int, subscriptions: List[str]) -> dict:
        """Join a consumer group"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, member_id)
        self._write_string(payload, client_id)
        self._write_string(payload, client_host)
        payload.extend(struct.pack('>I', session_timeout))
        payload.extend(struct.pack('>I', len(subscriptions)))
        for sub in subscriptions:
            self._write_string(payload, sub)

        response = self._send_request(APIKey.JOIN_GROUP, 0, client_id, bytes(payload))

        # Parse response
        offset = 0
        group_id_resp, offset = self._read_string(response, offset)
        generation = struct.unpack_from('>I', response, offset)[0]
        offset += 4
        member_id_resp, offset = self._read_string(response, offset)
        leader_id, offset = self._read_string(response, offset)

        return {
            "group_id": group_id_resp,
            "generation": generation,
            "member_id": member_id_resp,
            "leader_id": leader_id
        }

    def heartbeat(self, group_id: str, member_id: str, generation: int,
                  client_id: str = "gofka-python") -> bool:
        """Send heartbeat for a consumer group member"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, member_id)
        payload.extend(struct.pack('>I', generation))

        response = self._send_request(APIKey.HEARTBEAT, 0, client_id, bytes(payload))

        if len(response) >= 1:
            return response[0] == 1
        return False

    def sync_group(self, group_id: str, member_id: str, generation: int,
                   client_id: str = "gofka-python") -> dict:
        """Sync partition assignments for a consumer group"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, member_id)
        payload.extend(struct.pack('>I', generation))

        response = self._send_request(APIKey.SYNC_GROUP, 0, client_id, bytes(payload))

        # Parse response
        offset = 0
        num_partitions = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        partitions = []
        for _ in range(num_partitions):
            partition_id = struct.unpack_from('>I', response, offset)[0]
            offset += 4
            partitions.append(partition_id)

        generation_resp = struct.unpack_from('>I', response, offset)[0]

        return {
            "partitions": partitions,
            "generation": generation_resp
        }

    def leave_group(self, group_id: str, member_id: str, client_id: str = "gofka-python") -> bool:
        """Leave a consumer group"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, member_id)

        response = self._send_request(APIKey.LEAVE_GROUP, 0, client_id, bytes(payload))

        if len(response) >= 1:
            return response[0] == 1
        return False
