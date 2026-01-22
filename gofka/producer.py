"""
Gofka Producer client
"""

import time
import random
from typing import Optional, List, Tuple
from .protocol import Protocol
from .exceptions import ProduceError


class Producer:
    """
    Gofka message producer with broker failover support

    Example:
        producer = Producer(brokers="localhost:9092,localhost:9093,localhost:9094")
        producer.connect()
        offset = producer.send("my-topic", b"Hello, Gofka!", partition=0)
        print(f"Message sent at offset {offset}")
        producer.close()
    """

    def __init__(
        self,
        brokers: str,
        client_id: str = "gofka-python-producer",
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff_ms: int = 100
    ):
        """
        Initialize producer with failover support

        Args:
            brokers: Comma-separated list of broker addresses (host:port)
            client_id: Client identifier
            timeout: Connection timeout in seconds
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
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms

        # Current connection state
        self.protocol: Optional[Protocol] = None
        self.current_broker_index = 0
        self.connected = False

        # Track unhealthy brokers for smart failover
        self._unhealthy_brokers: dict = {}  # broker_index -> unhealthy_until_timestamp
        self._unhealthy_timeout = 30  # seconds to mark broker as unhealthy

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

    def _connect_to_broker(self, host: str, port: int) -> bool:
        """Attempt to connect to a specific broker"""
        try:
            if self.protocol:
                try:
                    self.protocol.close()
                except Exception:
                    pass

            self.protocol = Protocol(host, port, self.timeout)
            self.protocol.connect()
            return True
        except Exception:
            return False

    def connect(self):
        """Connect to a broker with failover support"""
        last_error = None

        # Try each broker starting from the preferred one
        start_index = self._get_next_healthy_broker_index(self.current_broker_index)

        for i in range(len(self.broker_list)):
            broker_index = (start_index + i) % len(self.broker_list)
            host, port = self.broker_list[broker_index]

            if self._connect_to_broker(host, port):
                self.current_broker_index = broker_index
                self.connected = True
                return

            self._mark_broker_unhealthy(broker_index)
            last_error = f"Failed to connect to {host}:{port}"

        raise ProduceError(f"Failed to connect to any broker. Last error: {last_error}")

    def _failover(self) -> bool:
        """Attempt to failover to another broker"""
        # Start from the next broker
        start_index = (self.current_broker_index + 1) % len(self.broker_list)
        start_index = self._get_next_healthy_broker_index(start_index)

        for i in range(len(self.broker_list)):
            broker_index = (start_index + i) % len(self.broker_list)
            if broker_index == self.current_broker_index:
                continue  # Skip current failed broker

            host, port = self.broker_list[broker_index]
            if self._connect_to_broker(host, port):
                self.current_broker_index = broker_index
                self.connected = True
                return True

            self._mark_broker_unhealthy(broker_index)

        return False

    def _send_with_retry(self, topic: str, message: bytes, partition: int) -> int:
        """Send a message with retry and failover logic"""
        last_error = None
        backoff = self.retry_backoff_ms / 1000.0  # Convert to seconds

        for attempt in range(self.max_retries + 1):
            try:
                if not self.connected or self.protocol is None:
                    self.connect()

                offset = self.protocol.produce(topic, partition, message, self.client_id)
                return offset

            except Exception as e:
                last_error = e
                self._mark_broker_unhealthy(self.current_broker_index)
                self.connected = False

                if attempt < self.max_retries:
                    # Try to failover to another broker
                    if self._failover():
                        # Successfully failed over, retry immediately
                        continue

                    # Couldn't failover, wait with exponential backoff
                    jitter = random.uniform(0, backoff * 0.1)
                    time.sleep(backoff + jitter)
                    backoff = min(backoff * 2, 30)  # Cap at 30 seconds

                    # Try to reconnect to any broker
                    try:
                        self.connect()
                    except ProduceError:
                        pass  # Will retry on next iteration

        raise ProduceError(f"Failed to produce message after {self.max_retries + 1} attempts: {last_error}")

    def send(self, topic: str, message: bytes, partition: int = 0) -> int:
        """
        Send a message to a topic with automatic failover

        Args:
            topic: Topic name
            message: Message bytes
            partition: Partition ID (default: 0)

        Returns:
            Offset where the message was stored

        Raises:
            ProduceError: If send fails after all retries
        """
        if not self.connected:
            raise ProduceError("Producer not connected. Call connect() first.")

        return self._send_with_retry(topic, message, partition)

    def send_string(self, topic: str, message: str, partition: int = 0, encoding: str = 'utf-8') -> int:
        """
        Send a string message to a topic

        Args:
            topic: Topic name
            message: Message string
            partition: Partition ID (default: 0)
            encoding: String encoding (default: utf-8)

        Returns:
            Offset where the message was stored
        """
        return self.send(topic, message.encode(encoding), partition)

    def flush(self):
        """
        Flush any pending messages
        (Currently no-op as we send synchronously)
        """
        pass

    def close(self):
        """Close connection to broker"""
        if self.connected and self.protocol:
            try:
                self.protocol.close()
            except Exception:
                pass  # Ignore close errors
            self.connected = False
            self.protocol = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
