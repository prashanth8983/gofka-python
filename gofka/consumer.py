"""
Gofka Consumer client
"""

import time
import socket
from typing import Optional, List, Dict
from .protocol import Protocol
from .exceptions import ConsumeError, OffsetError, GroupCoordinationError


class Message:
    """Represents a consumed message"""

    def __init__(self, topic: str, partition: int, offset: int, value: bytes):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.timestamp = time.time()

    def __repr__(self):
        return f"Message(topic={self.topic}, partition={self.partition}, offset={self.offset}, len={len(self.value)})"


class Consumer:
    """
    Gofka message consumer with consumer group support

    Example:
        consumer = Consumer(
            brokers="localhost:9092",
            group_id="my-group",
            topics=["my-topic"]
        )
        consumer.connect()
        consumer.subscribe()

        for message in consumer.poll(max_messages=10):
            print(f"Received: {message.value.decode()}")
            consumer.commit(message)

        consumer.close()
    """

    def __init__(self, brokers: str, group_id: str, topics: List[str],
                 client_id: Optional[str] = None,
                 auto_commit: bool = True,
                 auto_offset_reset: str = "earliest",
                 session_timeout: int = 30000,
                 timeout: int = 30):
        """
        Initialize consumer

        Args:
            brokers: Comma-separated list of broker addresses
            group_id: Consumer group ID
            topics: List of topics to subscribe to
            client_id: Client identifier (auto-generated if None)
            auto_commit: Auto-commit offsets after consuming
            auto_offset_reset: Where to start if no offset committed ("earliest" or "latest")
            session_timeout: Session timeout in milliseconds
            timeout: Connection timeout in seconds
        """
        # Parse broker addresses
        broker_list = brokers.split(',')
        if not broker_list:
            raise ConsumeError("No brokers specified")

        host, port = broker_list[0].strip().split(':')

        self.group_id = group_id
        self.topics = topics
        self.client_id = client_id or f"gofka-python-consumer-{int(time.time())}"
        self.auto_commit = auto_commit
        self.auto_offset_reset = auto_offset_reset
        self.session_timeout = session_timeout

        self.protocol = Protocol(host, int(port), timeout)
        self.connected = False
        self.subscribed = False

        # Consumer group state
        self.member_id = ""
        self.generation = 0
        self.assigned_partitions: List[int] = []

        # Offset tracking
        self.current_offsets: Dict[int, int] = {}  # partition -> offset

    def connect(self):
        """Connect to broker"""
        self.protocol.connect()
        self.connected = True

    def subscribe(self):
        """
        Subscribe to topics and join consumer group

        Performs consumer group protocol:
        1. JoinGroup - Join the consumer group
        2. SyncGroup - Get partition assignment
        3. Fetch offsets - Get last committed offsets
        """
        if not self.connected:
            raise ConsumeError("Consumer not connected. Call connect() first.")

        try:
            # Generate member ID if not set
            if not self.member_id:
                self.member_id = f"{self.client_id}-{int(time.time())}"

            # Step 1: Join group
            client_host = socket.gethostname()
            join_response = self.protocol.join_group(
                self.group_id,
                self.member_id,
                self.client_id,
                client_host,
                self.session_timeout,
                self.topics
            )

            self.member_id = join_response["member_id"]
            self.generation = join_response["generation"]
            leader_id = join_response["leader_id"]

            print(f"Joined group '{self.group_id}' as member '{self.member_id}' "
                  f"(generation {self.generation}, leader: {leader_id})")

            # Step 2: Sync group to get partition assignment
            sync_response = self.protocol.sync_group(
                self.group_id,
                self.member_id,
                self.generation,
                self.client_id
            )

            self.assigned_partitions = sync_response["partitions"]
            print(f"Assigned partitions: {self.assigned_partitions}")

            # Step 3: Fetch committed offsets for assigned partitions
            for partition in self.assigned_partitions:
                # For now, assume single topic (first topic in list)
                topic = self.topics[0] if self.topics else ""

                offset = self.protocol.fetch_offset(
                    self.group_id,
                    topic,
                    partition,
                    self.client_id
                )

                if offset == -1 or offset == (2**64 - 1):  # No committed offset
                    # Start from beginning or end based on auto_offset_reset
                    offset = 0 if self.auto_offset_reset == "earliest" else 0

                self.current_offsets[partition] = offset
                print(f"Starting from offset {offset} for partition {partition}")

            self.subscribed = True

        except Exception as e:
            raise GroupCoordinationError(f"Failed to subscribe: {e}")

    def poll(self, max_messages: int = 1, timeout: float = 1.0) -> List[Message]:
        """
        Poll for messages

        Args:
            max_messages: Maximum number of messages to return
            timeout: Timeout in seconds (currently ignored)

        Returns:
            List of Message objects
        """
        if not self.subscribed:
            raise ConsumeError("Consumer not subscribed. Call subscribe() first.")

        messages = []
        topic = self.topics[0] if self.topics else ""

        for partition in self.assigned_partitions:
            if len(messages) >= max_messages:
                break

            offset = self.current_offsets.get(partition, 0)

            try:
                # Fetch message from broker
                data = self.protocol.fetch(topic, partition, offset, self.client_id)

                if data:
                    message = Message(topic, partition, offset, data)
                    messages.append(message)

                    # Update offset
                    self.current_offsets[partition] = offset + 1

                    # Auto-commit if enabled
                    if self.auto_commit:
                        self.commit(message)
            except Exception as e:
                print(f"Error fetching from partition {partition}: {e}")
                continue

        return messages

    def commit(self, message: Optional[Message] = None):
        """
        Commit offset for a message or all current offsets

        Args:
            message: Specific message to commit (if None, commits all current offsets)
        """
        if not self.subscribed:
            raise ConsumeError("Consumer not subscribed")

        try:
            if message:
                # Commit specific message
                self.protocol.commit_offset(
                    self.group_id,
                    message.topic,
                    message.partition,
                    message.offset + 1,  # Commit next offset
                    "",
                    self.client_id
                )
            else:
                # Commit all current offsets
                topic = self.topics[0] if self.topics else ""
                for partition, offset in self.current_offsets.items():
                    self.protocol.commit_offset(
                        self.group_id,
                        topic,
                        partition,
                        offset,
                        "",
                        self.client_id
                    )
        except Exception as e:
            raise OffsetError(f"Failed to commit offset: {e}")

    def send_heartbeat(self):
        """Send heartbeat to maintain group membership"""
        if not self.subscribed:
            return

        try:
            success = self.protocol.heartbeat(
                self.group_id,
                self.member_id,
                self.generation,
                self.client_id
            )
            if not success:
                print("Heartbeat failed - may need to rejoin group")
        except Exception as e:
            print(f"Heartbeat error: {e}")

    def close(self):
        """Close consumer and leave group"""
        if self.subscribed:
            try:
                # Leave group
                self.protocol.leave_group(
                    self.group_id,
                    self.member_id,
                    self.client_id
                )
                print(f"Left group '{self.group_id}'")
            except Exception as e:
                print(f"Error leaving group: {e}")

        if self.connected:
            self.protocol.close()
            self.connected = False

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
