"""
Tests for Gofka Producer
"""
import pytest
from gofka import Producer
from gofka.exceptions import ConnectionError, ProduceError


class TestProducer:
    """Test Producer functionality"""

    def test_producer_init(self):
        """Test producer initialization"""
        producer = Producer(brokers="localhost:9092")
        assert producer.brokers == "localhost:9092"
        assert producer.client_id == "gofka-python-producer"

    def test_producer_custom_client_id(self):
        """Test producer with custom client ID"""
        producer = Producer(brokers="localhost:9092", client_id="custom-producer")
        assert producer.client_id == "custom-producer"

    @pytest.mark.integration
    def test_producer_connect(self, broker_address, require_broker):
        """Test producer connection to broker"""
        producer = Producer(brokers=broker_address)
        producer.connect()
        producer.close()

    @pytest.mark.integration
    def test_producer_send(self, broker_address, test_topic, require_broker):
        """Test sending a message"""
        producer = Producer(brokers=broker_address)
        producer.connect()

        offset = producer.send(test_topic, b"test message", partition=0)
        assert offset >= 0

        producer.close()

    @pytest.mark.integration
    def test_producer_send_string(self, broker_address, test_topic, require_broker):
        """Test sending a string message"""
        producer = Producer(brokers=broker_address)
        producer.connect()

        offset = producer.send_string(test_topic, "test string message", partition=0)
        assert offset >= 0

        producer.close()
