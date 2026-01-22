"""
Gofka Async Producer client
"""

import asyncio
import gzip
import time
import random
from typing import Optional, List, Callable, Tuple
from .async_protocol import AsyncProtocol
from .exceptions import ProduceError


class AsyncProducer:
    """
    Gofka async message producer with batching, compression, and broker failover support

    Example:
        async with AsyncProducer(brokers="localhost:9092,localhost:9093") as producer:
            offset = await producer.send("my-topic", b"Hello, Gofka!", partition=0)
            print(f"Message sent at offset {offset}")
    """

    def __init__(
        self,
        brokers: str,
        client_id: str = "gofka-python-async-producer",
        timeout: int = 30,
        use_ssl: bool = False,
        compression: Optional[str] = None,
        batch_size: int = 16384,
        linger_ms: int = 0,
        max_in_flight: int = 5,
        partitioner: Optional[Callable] = None,
        max_retries: int = 3,
        retry_backoff_ms: int = 100
    ):
        """
        Initialize async producer with failover support

        Args:
            brokers: Comma-separated list of broker addresses (host:port)
            client_id: Client identifier
            timeout: Connection timeout in seconds
            use_ssl: Enable SSL/TLS encryption
            compression: Compression type ('gzip', 'snappy', 'lz4', or None)
            batch_size: Maximum batch size in bytes
            linger_ms: Time to wait for batching (milliseconds)
            max_in_flight: Maximum concurrent requests
            partitioner: Custom partitioner function
            max_retries: Maximum number of retries on failure
            retry_backoff_ms: Base backoff time between retries in milliseconds
        """
        # Parse broker addresses
        self.broker_list: List[Tuple[str, int]] = []
        for broker in brokers.split(','):
            broker = broker.strip()
            if broker:
                try:
                    host, port = broker.split(':')
                    self.broker_list.append((host, int(port)))
                except ValueError:
                    raise ProduceError(f"Invalid broker address: {broker}")

        if not self.broker_list:
            raise ProduceError("No brokers specified")

        self.client_id = client_id
        self.timeout = timeout
        self.use_ssl = use_ssl
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms

        # Current connection state
        self.protocol: Optional[AsyncProtocol] = None
        self.current_broker_index = 0
        self.connected = False

        # Track unhealthy brokers for smart failover
        self._unhealthy_brokers: dict = {}  # broker_index -> unhealthy_until_timestamp
        self._unhealthy_timeout = 30  # seconds to mark broker as unhealthy
        self._connection_lock = asyncio.Lock()

        self.compression = compression
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.max_in_flight = max_in_flight
        self.partitioner = partitioner or self._default_partitioner

        # Batching infrastructure
        self._batches = {}  # (topic, partition) -> list of messages
        self._batch_task = None
        self._semaphore = asyncio.Semaphore(max_in_flight)

        # Validate compression
        if compression and compression not in ('gzip', 'snappy', 'lz4'):
            raise ProduceError(f"Unsupported compression: {compression}")

    def _default_partitioner(self, topic: str, key: Optional[bytes], num_partitions: int) -> int:
        """Default partitioner - round-robin"""
        if key:
            return hash(key) % num_partitions
        return 0  # Default to partition 0 if no key

    def _compress(self, data: bytes) -> bytes:
        """Compress message data"""
        if not self.compression:
            return data

        if self.compression == 'gzip':
            return gzip.compress(data)
        elif self.compression == 'snappy':
            try:
                import snappy
                return snappy.compress(data)
            except ImportError:
                raise ProduceError("snappy-python not installed. Install with: pip install python-snappy")
        elif self.compression == 'lz4':
            try:
                import lz4.frame
                return lz4.frame.compress(data)
            except ImportError:
                raise ProduceError("lz4 not installed. Install with: pip install lz4")

        return data

    def _get_next_healthy_broker_index(self, start_index: int = 0) -> int:
        """Get the next healthy broker index, skipping unhealthy ones"""
        current_time = time.time()
        checked = 0

        while checked < len(self.broker_list):
            index = (start_index + checked) % len(self.broker_list)

            # Check if broker is marked unhealthy
            unhealthy_until = self._unhealthy_brokers.get(index, 0)
            if current_time >= unhealthy_until:
                # Broker is healthy or timeout expired
                if index in self._unhealthy_brokers:
                    del self._unhealthy_brokers[index]
                return index

            checked += 1

        # All brokers unhealthy, try the original one anyway
        return start_index

    def _mark_broker_unhealthy(self, index: int):
        """Mark a broker as unhealthy"""
        self._unhealthy_brokers[index] = time.time() + self._unhealthy_timeout

    async def _connect_to_broker(self, host: str, port: int) -> bool:
        """Attempt to connect to a specific broker"""
        try:
            if self.protocol:
                try:
                    await self.protocol.close()
                except Exception:
                    pass

            self.protocol = AsyncProtocol(host, port, self.timeout, self.use_ssl)
            await self.protocol.connect()
            return True
        except Exception:
            return False

    async def connect(self):
        """Connect to a broker with failover support"""
        async with self._connection_lock:
            last_error = None

            # Try each broker starting from the preferred one
            start_index = self._get_next_healthy_broker_index(self.current_broker_index)

            for i in range(len(self.broker_list)):
                broker_index = (start_index + i) % len(self.broker_list)
                host, port = self.broker_list[broker_index]

                if await self._connect_to_broker(host, port):
                    self.current_broker_index = broker_index
                    self.connected = True

                    # Start batch processing if linger_ms > 0
                    if self.linger_ms > 0 and not self._batch_task:
                        self._batch_task = asyncio.create_task(self._process_batches())
                    return

                self._mark_broker_unhealthy(broker_index)
                last_error = f"Failed to connect to {host}:{port}"

            raise ProduceError(f"Failed to connect to any broker. Last error: {last_error}")

    async def _failover(self) -> bool:
        """Attempt to failover to another broker"""
        async with self._connection_lock:
            # Start from the next broker
            start_index = (self.current_broker_index + 1) % len(self.broker_list)
            start_index = self._get_next_healthy_broker_index(start_index)

            for i in range(len(self.broker_list)):
                broker_index = (start_index + i) % len(self.broker_list)
                if broker_index == self.current_broker_index:
                    continue  # Skip current failed broker

                host, port = self.broker_list[broker_index]
                if await self._connect_to_broker(host, port):
                    self.current_broker_index = broker_index
                    self.connected = True
                    return True

                self._mark_broker_unhealthy(broker_index)

            return False

    async def _send_with_retry(self, topic: str, message: bytes, partition: int, headers: Optional[dict]) -> int:
        """Send a message with retry and failover logic"""
        last_error = None
        backoff = self.retry_backoff_ms / 1000.0  # Convert to seconds

        for attempt in range(self.max_retries + 1):
            try:
                if not self.connected or self.protocol is None:
                    await self.connect()

                offset = await self.protocol.produce(
                    topic, partition, message, self.client_id, headers
                )
                return offset

            except Exception as e:
                last_error = e
                self._mark_broker_unhealthy(self.current_broker_index)
                self.connected = False

                if attempt < self.max_retries:
                    # Try to failover to another broker
                    if await self._failover():
                        # Successfully failed over, retry immediately
                        continue

                    # Couldn't failover, wait with exponential backoff
                    jitter = random.uniform(0, backoff * 0.1)
                    await asyncio.sleep(backoff + jitter)
                    backoff = min(backoff * 2, 30)  # Cap at 30 seconds

                    # Try to reconnect to any broker
                    try:
                        await self.connect()
                    except ProduceError:
                        pass  # Will retry on next iteration

        raise ProduceError(f"Failed to produce message after {self.max_retries + 1} attempts: {last_error}")

    async def send(
        self,
        topic: str,
        message: bytes,
        partition: int = 0,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None
    ) -> int:
        """
        Send a message to a topic asynchronously with automatic failover

        Args:
            topic: Topic name
            message: Message bytes
            partition: Partition ID (default: 0)
            key: Optional message key for partitioning
            headers: Optional message headers

        Returns:
            Offset where the message was stored

        Raises:
            ProduceError: If send fails after all retries
        """
        if not self.connected:
            raise ProduceError("Producer not connected. Call connect() first.")

        # Compress if enabled
        compressed_message = self._compress(message)

        # Use semaphore to limit concurrent requests
        async with self._semaphore:
            return await self._send_with_retry(topic, compressed_message, partition, headers)

    async def send_string(
        self,
        topic: str,
        message: str,
        partition: int = 0,
        key: Optional[str] = None,
        encoding: str = 'utf-8',
        headers: Optional[dict] = None
    ) -> int:
        """
        Send a string message to a topic asynchronously

        Args:
            topic: Topic name
            message: Message string
            partition: Partition ID (default: 0)
            key: Optional message key
            encoding: String encoding (default: utf-8)
            headers: Optional message headers

        Returns:
            Offset where the message was stored
        """
        key_bytes = key.encode(encoding) if key else None
        return await self.send(
            topic, message.encode(encoding), partition, key_bytes, headers
        )

    async def send_batch(self, topic: str, messages: List[bytes], partition: int = 0) -> List[int]:
        """
        Send multiple messages in batch

        Args:
            topic: Topic name
            messages: List of message bytes
            partition: Partition ID

        Returns:
            List of offsets
        """
        offsets = []
        for message in messages:
            offset = await self.send(topic, message, partition)
            offsets.append(offset)
        return offsets

    async def _process_batches(self):
        """Background task to process batched messages"""
        while self.connected:
            await asyncio.sleep(self.linger_ms / 1000.0)

            if not self._batches:
                continue

            # Process all batches
            for (topic, partition), messages in list(self._batches.items()):
                if messages:
                    try:
                        await self.send_batch(topic, messages, partition)
                        self._batches[(topic, partition)] = []
                    except Exception as e:
                        print(f"Error processing batch: {e}")

    async def flush(self):
        """
        Flush any pending messages
        """
        if self._batches:
            for (topic, partition), messages in list(self._batches.items()):
                if messages:
                    await self.send_batch(topic, messages, partition)
                    self._batches[(topic, partition)] = []

    async def close(self):
        """Close connection to broker"""
        if self.connected:
            # Flush pending messages
            await self.flush()

            # Cancel batch task
            if self._batch_task:
                self._batch_task.cancel()
                try:
                    await self._batch_task
                except asyncio.CancelledError:
                    pass
                self._batch_task = None

            await self.protocol.close()
            self.connected = False

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
