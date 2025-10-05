"""
Tests for Gofka Consumer
"""
import pytest
from gofka import Consumer, Producer


class TestConsumer:
    """Test Consumer functionality"""

    def test_consumer_init(self):
        """Test consumer initialization"""
        consumer = Consumer(
            brokers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )
        assert consumer.brokers == "localhost:9092"
        assert consumer.group_id == "test-group"
        assert consumer.topics == ["test-topic"]

    def test_consumer_auto_commit_default(self):
        """Test auto_commit defaults to True"""
        consumer = Consumer(
            brokers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )
        assert consumer.auto_commit is True

    @pytest.mark.integration
    def test_consumer_connect(self, broker_address, test_group, require_broker):
        """Test consumer connection to broker"""
        consumer = Consumer(
            brokers=broker_address,
            group_id=test_group,
            topics=["test-topic"]
        )
        consumer.connect()
        consumer.close()

    @pytest.mark.integration
    def test_consumer_subscribe(self, broker_address, test_group, test_topic, require_broker):
        """Test consumer subscription"""
        consumer = Consumer(
            brokers=broker_address,
            group_id=test_group,
            topics=[test_topic]
        )
        consumer.connect()
        consumer.subscribe()
        consumer.close()

    @pytest.mark.integration
    def test_produce_and_consume(self, broker_address, test_topic, test_group, require_broker):
        """Test producing and consuming messages"""
        # Produce message
        producer = Producer(brokers=broker_address)
        producer.connect()
        producer.send_string(test_topic, "integration test message", partition=0)
        producer.close()

        # Consume message
        consumer = Consumer(
            brokers=broker_address,
            group_id=test_group,
            topics=[test_topic],
            auto_offset_reset="earliest"
        )
        consumer.connect()
        consumer.subscribe()

        messages = consumer.poll(max_messages=1)
        assert len(messages) >= 1
        assert messages[0].value == b"integration test message"

        consumer.close()
