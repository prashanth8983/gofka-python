"""
Gofka Producer client
"""

from typing import Optional
from .protocol import Protocol
from .exceptions import ProduceError


class Producer:
    """
    Gofka message producer

    Example:
        producer = Producer(brokers="localhost:9092")
        producer.connect()
        offset = producer.send("my-topic", b"Hello, Gofka!", partition=0)
        print(f"Message sent at offset {offset}")
        producer.close()
    """

    def __init__(self, brokers: str, client_id: str = "gofka-python-producer", timeout: int = 30):
        """
        Initialize producer

        Args:
            brokers: Comma-separated list of broker addresses (host:port)
            client_id: Client identifier
            timeout: Connection timeout in seconds
        """
        # Parse broker addresses
        broker_list = brokers.split(',')
        if not broker_list:
            raise ProduceError("No brokers specified")

        # For now, use first broker
        # TODO: Implement broker failover
        host, port = broker_list[0].strip().split(':')

        self.client_id = client_id
        self.protocol = Protocol(host, int(port), timeout)
        self.connected = False

    def connect(self):
        """Connect to broker"""
        self.protocol.connect()
        self.connected = True

    def send(self, topic: str, message: bytes, partition: int = 0) -> int:
        """
        Send a message to a topic

        Args:
            topic: Topic name
            message: Message bytes
            partition: Partition ID (default: 0)

        Returns:
            Offset where the message was stored

        Raises:
            ProduceError: If send fails
        """
        if not self.connected:
            raise ProduceError("Producer not connected. Call connect() first.")

        try:
            offset = self.protocol.produce(topic, partition, message, self.client_id)
            return offset
        except Exception as e:
            raise ProduceError(f"Failed to produce message: {e}")

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
