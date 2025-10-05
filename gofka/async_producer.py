"""
Gofka Async Producer client
"""

import asyncio
import gzip
import time
from typing import Optional, List, Callable
from .async_protocol import AsyncProtocol
from .exceptions import ProduceError


class AsyncProducer:
    """
    Gofka async message producer with batching and compression support

    Example:
        async with AsyncProducer(brokers="localhost:9092") as producer:
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
        partitioner: Optional[Callable] = None
    ):
        """
        Initialize async producer

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
        """
        # Parse broker addresses
        broker_list = brokers.split(',')
        if not broker_list:
            raise ProduceError("No brokers specified")

        # For now, use first broker
        # TODO: Implement broker failover
        host, port = broker_list[0].strip().split(':')

        self.client_id = client_id
        self.protocol = AsyncProtocol(host, int(port), timeout, use_ssl)
        self.connected = False
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

    async def connect(self):
        """Connect to broker"""
        await self.protocol.connect()
        self.connected = True

        # Start batch processing if linger_ms > 0
        if self.linger_ms > 0 and not self._batch_task:
            self._batch_task = asyncio.create_task(self._process_batches())

    async def send(
        self,
        topic: str,
        message: bytes,
        partition: int = 0,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None
    ) -> int:
        """
        Send a message to a topic asynchronously

        Args:
            topic: Topic name
            message: Message bytes
            partition: Partition ID (default: 0)
            key: Optional message key for partitioning
            headers: Optional message headers

        Returns:
            Offset where the message was stored

        Raises:
            ProduceError: If send fails
        """
        if not self.connected:
            raise ProduceError("Producer not connected. Call connect() first.")

        # Compress if enabled
        compressed_message = self._compress(message)

        # Use semaphore to limit concurrent requests
        async with self._semaphore:
            try:
                offset = await self.protocol.produce(
                    topic, partition, compressed_message, self.client_id, headers
                )
                return offset
            except Exception as e:
                raise ProduceError(f"Failed to produce message: {e}")

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
