"""
Gofka Async Consumer client
"""

import asyncio
import time
from typing import Optional, List, Dict
from .async_protocol import AsyncProtocol
from .exceptions import ConsumeError, OffsetError, GroupCoordinationError


class Message:
    """Represents a consumed message"""

    def __init__(self, topic: str, partition: int, offset: int, value: bytes, headers: Optional[dict] = None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.headers = headers or {}
        self.timestamp = time.time()

    def __repr__(self):
        return f"Message(topic={self.topic}, partition={self.partition}, offset={self.offset}, len={len(self.value)})"


class AsyncConsumer:
    """
    Gofka async message consumer with consumer group support

    Example:
        async with AsyncConsumer(
            brokers="localhost:9092",
            group_id="my-group",
            topics=["my-topic"]
        ) as consumer:
            await consumer.subscribe()

            async for message in consumer:
                print(f"Received: {message.value.decode()}")
                await consumer.commit(message)
    """

    def __init__(
        self,
        brokers: str,
        group_id: str,
        topics: List[str],
        client_id: Optional[str] = None,
        auto_commit: bool = True,
        auto_offset_reset: str = "earliest",
        session_timeout: int = 30000,
        timeout: int = 30,
        use_ssl: bool = False,
        max_poll_records: int = 500
    ):
        """
        Initialize async consumer

        Args:
            brokers: Comma-separated list of broker addresses
            group_id: Consumer group ID
            topics: List of topics to subscribe to
            client_id: Client identifier (auto-generated if None)
            auto_commit: Auto-commit offsets after consuming
            auto_offset_reset: Where to start if no offset committed ("earliest" or "latest")
            session_timeout: Session timeout in milliseconds
            timeout: Connection timeout in seconds
            use_ssl: Enable SSL/TLS encryption
            max_poll_records: Maximum records per poll
        """
        # Parse broker addresses
        broker_list = brokers.split(',')
        if not broker_list:
            raise ConsumeError("No brokers specified")

        host, port = broker_list[0].strip().split(':')

        self.group_id = group_id
        self.topics = topics
        self.client_id = client_id or f"gofka-python-async-consumer-{int(time.time())}"
        self.auto_commit = auto_commit
        self.auto_offset_reset = auto_offset_reset
        self.session_timeout = session_timeout
        self.max_poll_records = max_poll_records

        self.protocol = AsyncProtocol(host, int(port), timeout, use_ssl)
        self.connected = False
        self.subscribed = False

        # Consumer group state
        self.member_id = ""
        self.generation = 0
        self.assigned_partitions: List[int] = []

        # Offset tracking
        self.current_offsets: Dict[int, int] = {}  # partition -> offset

        # Heartbeat task
        self._heartbeat_task = None
        self._running = False

    async def connect(self):
        """Connect to broker"""
        await self.protocol.connect()
        self.connected = True

    async def subscribe(self):
        """
        Subscribe to topics and join consumer group

        Performs consumer group protocol:
        1. JoinGroup - Join the consumer group
        2. SyncGroup - Get partition assignments
        """
        if not self.connected:
            raise ConsumeError("Consumer not connected. Call connect() first.")

        try:
            # Join the consumer group
            self.member_id, self.generation = await self.protocol.join_group(
                self.group_id,
                self.member_id,
                self.topics,
                self.session_timeout,
                self.client_id
            )

            # Sync to get partition assignments
            self.assigned_partitions = await self.protocol.sync_group(
                self.group_id,
                self.member_id,
                self.generation,
                self.client_id
            )

            # Initialize offsets for assigned partitions
            for partition in self.assigned_partitions:
                for topic in self.topics:
                    # Fetch committed offset
                    committed_offset = await self.protocol.fetch_offset(
                        self.group_id,
                        topic,
                        partition,
                        self.client_id
                    )

                    if committed_offset > 0:
                        self.current_offsets[partition] = committed_offset
                    else:
                        # No committed offset, use auto_offset_reset
                        self.current_offsets[partition] = 0 if self.auto_offset_reset == "earliest" else -1

            self.subscribed = True
            self._running = True

            # Start heartbeat task
            if not self._heartbeat_task:
                self._heartbeat_task = asyncio.create_task(self._send_heartbeats())

        except Exception as e:
            raise GroupCoordinationError(f"Failed to subscribe: {e}")

    async def _send_heartbeats(self):
        """Background task to send periodic heartbeats"""
        heartbeat_interval = self.session_timeout / 3  # Send heartbeat every 1/3 of session timeout

        while self._running and self.subscribed:
            try:
                await asyncio.sleep(heartbeat_interval / 1000.0)

                if self.member_id and self.generation > 0:
                    success = await self.protocol.heartbeat(
                        self.group_id,
                        self.member_id,
                        self.generation,
                        self.client_id
                    )

                    if not success:
                        # Heartbeat failed, need to rejoin
                        await self.subscribe()

            except Exception as e:
                print(f"Heartbeat error: {e}")

    async def poll(self, max_messages: int = 1) -> List[Message]:
        """
        Poll for messages asynchronously

        Args:
            max_messages: Maximum number of messages to retrieve

        Returns:
            List of Message objects
        """
        if not self.subscribed:
            raise ConsumeError("Consumer not subscribed. Call subscribe() first.")

        messages = []

        for partition in self.assigned_partitions:
            if len(messages) >= max_messages:
                break

            for topic in self.topics:
                current_offset = self.current_offsets.get(partition, 0)

                try:
                    # Fetch messages
                    fetched = await self.protocol.fetch(
                        topic,
                        partition,
                        current_offset,
                        1048576,  # 1MB max
                        self.client_id
                    )

                    for offset, data in fetched:
                        if len(messages) >= max_messages:
                            break

                        msg = Message(topic, partition, offset, data)
                        messages.append(msg)

                        # Update current offset
                        self.current_offsets[partition] = offset + 1

                        # Auto-commit if enabled
                        if self.auto_commit:
                            await self.commit(msg)

                except Exception as e:
                    raise ConsumeError(f"Failed to poll messages: {e}")

        return messages

    async def commit(self, message: Optional[Message] = None):
        """
        Commit offset asynchronously

        Args:
            message: Message to commit (if None, commits all current offsets)
        """
        if not self.subscribed:
            raise OffsetError("Consumer not subscribed")

        try:
            if message:
                # Commit specific message offset
                success = await self.protocol.commit_offset(
                    self.group_id,
                    message.topic,
                    message.partition,
                    message.offset + 1,  # Commit next offset
                    self.client_id
                )

                if not success:
                    raise OffsetError(f"Failed to commit offset for partition {message.partition}")
            else:
                # Commit all current offsets
                for partition, offset in self.current_offsets.items():
                    for topic in self.topics:
                        await self.protocol.commit_offset(
                            self.group_id,
                            topic,
                            partition,
                            offset,
                            self.client_id
                        )

        except Exception as e:
            raise OffsetError(f"Failed to commit offset: {e}")

    async def close(self):
        """Close connection and leave consumer group"""
        self._running = False

        # Cancel heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self.subscribed and self.member_id:
            try:
                # Leave the consumer group
                await self.protocol.leave_group(
                    self.group_id,
                    self.member_id,
                    self.client_id
                )
            except Exception as e:
                print(f"Error leaving group: {e}")

            self.subscribed = False

        if self.connected:
            await self.protocol.close()
            self.connected = False

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    def __aiter__(self):
        """Make consumer async iterable"""
        return self

    async def __anext__(self):
        """Async iterator to continuously poll for messages"""
        if not self._running:
            raise StopAsyncIteration

        while True:
            messages = await self.poll(max_messages=1)
            if messages:
                return messages[0]
            await asyncio.sleep(0.1)  # Small delay if no messages
