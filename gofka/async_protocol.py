"""
Gofka async binary wire protocol implementation

Implements the Gofka binary protocol for async communication with brokers.
"""

import struct
import asyncio
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


class AsyncProtocol:
    """Handles low-level async Gofka protocol communication"""

    def __init__(self, host: str, port: int, timeout: int = 30, use_ssl: bool = False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.use_ssl = use_ssl
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.correlation_id = 0
        self._lock = asyncio.Lock()

    async def connect(self):
        """Establish async connection to Gofka broker"""
        try:
            if self.use_ssl:
                import ssl
                ssl_context = ssl.create_default_context()
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port, ssl=ssl_context),
                    timeout=self.timeout
                )
            else:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout
                )
        except asyncio.TimeoutError:
            raise ConnectionError(f"Connection timeout to {self.host}:{self.port}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    async def close(self):
        """Close async connection to broker"""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
            self.writer = None
            self.reader = None

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

    async def _send_request(self, api_key: int, api_version: int, client_id: str, payload: bytes) -> bytes:
        """Send request and receive response asynchronously"""
        if not self.writer or not self.reader:
            raise ConnectionError("Not connected to broker")

        async with self._lock:
            # Build request header
            request = self._build_request_header(api_key, api_version, client_id)
            request.extend(payload)

            # Prepend size (4 bytes)
            size = len(request)
            full_request = struct.pack('>I', size) + request

            # Send request
            try:
                self.writer.write(full_request)
                await self.writer.drain()
            except Exception as e:
                raise ConnectionError(f"Failed to send request: {e}")

            # Read response
            try:
                # Read correlation ID (4 bytes)
                correlation_id_data = await asyncio.wait_for(
                    self._recv_exact(4),
                    timeout=self.timeout
                )
                correlation_id = struct.unpack('>I', correlation_id_data)[0]

                # Read response data
                response_data = await asyncio.wait_for(
                    self.reader.read(65536),
                    timeout=self.timeout
                )
                return response_data
            except asyncio.TimeoutError:
                raise ConnectionError("Response timeout")
            except Exception as e:
                raise ConnectionError(f"Failed to receive response: {e}")

    async def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = await self.reader.read(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by broker")
            data += chunk
        return data

    async def produce(self, topic: str, partition: int, message: bytes,
                     client_id: str = "gofka-python", headers: Optional[dict] = None) -> int:
        """
        Produce a message to a topic asynchronously
        Returns: offset of the produced message
        """
        # Build produce payload
        payload = bytearray()
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>I', len(message)))
        payload.extend(message)

        # Send request
        response = await self._send_request(APIKey.PRODUCE, 0, client_id, bytes(payload))

        # Parse response: partition (4 bytes) + offset (8 bytes)
        if len(response) >= 12:
            partition_resp, offset = struct.unpack('>IQ', response[:12])
            return offset
        else:
            raise GofkaError(f"Invalid produce response: {len(response)} bytes")

    async def fetch(self, topic: str, partition: int, offset: int, max_bytes: int = 1048576,
                   client_id: str = "gofka-python") -> List[Tuple[int, bytes]]:
        """
        Fetch messages from a topic asynchronously
        Returns: List of (offset, message) tuples
        """
        # Build fetch payload
        payload = bytearray()
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>Q', offset))
        payload.extend(struct.pack('>I', max_bytes))

        # Send request
        response = await self._send_request(APIKey.FETCH, 0, client_id, bytes(payload))

        # Parse response
        messages = []
        offset_pos = 0

        while offset_pos + 12 <= len(response):
            msg_offset = struct.unpack_from('>Q', response, offset_pos)[0]
            offset_pos += 8
            msg_size = struct.unpack_from('>I', response, offset_pos)[0]
            offset_pos += 4

            if offset_pos + msg_size > len(response):
                break

            msg_data = response[offset_pos:offset_pos + msg_size]
            offset_pos += msg_size
            messages.append((msg_offset, msg_data))

        return messages

    async def metadata(self, topics: Optional[List[str]] = None,
                      client_id: str = "gofka-python") -> dict:
        """Fetch cluster metadata asynchronously"""
        # Build metadata payload
        payload = bytearray()

        if topics:
            payload.extend(struct.pack('>I', len(topics)))
            for topic in topics:
                self._write_string(payload, topic)
        else:
            payload.extend(struct.pack('>I', 0))

        # Send request
        response = await self._send_request(APIKey.METADATA, 0, client_id, bytes(payload))

        # Parse metadata response
        offset = 0

        # Broker count
        broker_count = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        brokers = []
        for _ in range(broker_count):
            broker_id, offset = self._read_string(response, offset)
            broker_addr, offset = self._read_string(response, offset)
            brokers.append({"id": broker_id, "addr": broker_addr})

        # Topic count
        topic_count = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        topics_data = []
        for _ in range(topic_count):
            topic_name, offset = self._read_string(response, offset)
            partition_count = struct.unpack_from('>I', response, offset)[0]
            offset += 4

            partitions = []
            for _ in range(partition_count):
                partition_id = struct.unpack_from('>I', response, offset)[0]
                offset += 4
                leader, offset = self._read_string(response, offset)

                replica_count = struct.unpack_from('>I', response, offset)[0]
                offset += 4
                replicas = []
                for _ in range(replica_count):
                    replica, offset = self._read_string(response, offset)
                    replicas.append(replica)

                partitions.append({
                    "id": partition_id,
                    "leader": leader,
                    "replicas": replicas
                })

            topics_data.append({
                "name": topic_name,
                "partitions": partitions
            })

        return {
            "brokers": brokers,
            "topics": topics_data
        }

    async def commit_offset(self, group_id: str, topic: str, partition: int, offset: int,
                           client_id: str = "gofka-python") -> bool:
        """Commit offset for consumer group asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))
        payload.extend(struct.pack('>Q', offset))

        response = await self._send_request(APIKey.OFFSET_COMMIT, 0, client_id, bytes(payload))

        if len(response) >= 2:
            error_code = struct.unpack('>H', response[:2])[0]
            return error_code == 0
        return False

    async def fetch_offset(self, group_id: str, topic: str, partition: int,
                          client_id: str = "gofka-python") -> int:
        """Fetch committed offset for consumer group asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, topic)
        payload.extend(struct.pack('>I', partition))

        response = await self._send_request(APIKey.OFFSET_FETCH, 0, client_id, bytes(payload))

        if len(response) >= 8:
            offset = struct.unpack('>Q', response[:8])[0]
            return offset
        return 0

    async def join_group(self, group_id: str, member_id: str, topics: List[str],
                        session_timeout: int, client_id: str = "gofka-python") -> Tuple[str, int]:
        """Join consumer group asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        payload.extend(struct.pack('>I', session_timeout))
        self._write_string(payload, member_id)
        payload.extend(struct.pack('>I', len(topics)))
        for topic in topics:
            self._write_string(payload, topic)

        response = await self._send_request(APIKey.JOIN_GROUP, 0, client_id, bytes(payload))

        offset = 0
        error_code = struct.unpack_from('>H', response, offset)[0]
        offset += 2

        if error_code != 0:
            raise GofkaError(f"JoinGroup failed with error code: {error_code}")

        generation = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        assigned_member_id, offset = self._read_string(response, offset)

        return assigned_member_id, generation

    async def sync_group(self, group_id: str, member_id: str, generation: int,
                        client_id: str = "gofka-python") -> List[int]:
        """Sync consumer group and get partition assignment asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        payload.extend(struct.pack('>I', generation))
        self._write_string(payload, member_id)

        response = await self._send_request(APIKey.SYNC_GROUP, 0, client_id, bytes(payload))

        offset = 0
        error_code = struct.unpack_from('>H', response, offset)[0]
        offset += 2

        if error_code != 0:
            raise GofkaError(f"SyncGroup failed with error code: {error_code}")

        partition_count = struct.unpack_from('>I', response, offset)[0]
        offset += 4

        partitions = []
        for _ in range(partition_count):
            partition_id = struct.unpack_from('>I', response, offset)[0]
            offset += 4
            partitions.append(partition_id)

        return partitions

    async def heartbeat(self, group_id: str, member_id: str, generation: int,
                       client_id: str = "gofka-python") -> bool:
        """Send heartbeat to consumer group asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        payload.extend(struct.pack('>I', generation))
        self._write_string(payload, member_id)

        response = await self._send_request(APIKey.HEARTBEAT, 0, client_id, bytes(payload))

        if len(response) >= 2:
            error_code = struct.unpack('>H', response[:2])[0]
            return error_code == 0
        return False

    async def leave_group(self, group_id: str, member_id: str,
                         client_id: str = "gofka-python") -> bool:
        """Leave consumer group asynchronously"""
        payload = bytearray()
        self._write_string(payload, group_id)
        self._write_string(payload, member_id)

        response = await self._send_request(APIKey.LEAVE_GROUP, 0, client_id, bytes(payload))

        if len(response) >= 2:
            error_code = struct.unpack('>H', response[:2])[0]
            return error_code == 0
        return False
