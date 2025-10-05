"""
Tests for Async Gofka Consumer
"""
import pytest
import asyncio
from gofka import AsyncConsumer, AsyncProducer
from gofka.exceptions import ConsumeError, GroupCoordinationError


class TestAsyncConsumer:
    """Test AsyncConsumer functionality"""

    def test_async_consumer_init(self):
        """Test async consumer initialization"""
        consumer = AsyncConsumer(
            brokers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )
        assert not consumer.connected
        assert not consumer.subscribed
        assert consumer.group_id == "test-group"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_consumer_connect(self, broker_address, require_broker):
        """Test async consumer connection"""
        consumer = AsyncConsumer(
            brokers=broker_address,
            group_id="test-group",
            topics=["test-topic"]
        )
        await consumer.connect()
        assert consumer.connected
        await consumer.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_consumer_context_manager(self, broker_address, require_broker):
        """Test async consumer with context manager"""
        async with AsyncConsumer(
            brokers=broker_address,
            group_id="test-group",
            topics=["test-topic"]
        ) as consumer:
            assert consumer.connected

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_consumer_subscribe(self, broker_address, test_topic, require_broker):
        """Test consumer subscription"""
        # First produce some messages
        async with AsyncProducer(brokers=broker_address) as producer:
            for i in range(5):
                await producer.send_string(test_topic, f"test message {i}", partition=0)

        # Then consume
        async with AsyncConsumer(
            brokers=broker_address,
            group_id="test-subscribe-group",
            topics=[test_topic]
        ) as consumer:
            await consumer.subscribe()
            assert consumer.subscribed
            assert len(consumer.assigned_partitions) > 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_consumer_poll(self, broker_address, test_topic, require_broker):
        """Test message polling"""
        # Produce messages
        async with AsyncProducer(brokers=broker_address) as producer:
            for i in range(5):
                await producer.send_string(test_topic, f"poll test {i}", partition=0)

        # Consume messages
        async with AsyncConsumer(
            brokers=broker_address,
            group_id="test-poll-group",
            topics=[test_topic],
            auto_commit=False
        ) as consumer:
            await consumer.subscribe()
            messages = await consumer.poll(max_messages=5)
            assert len(messages) > 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_consumer_iteration(self, broker_address, test_topic, require_broker):
        """Test async iteration over consumer"""
        # Produce messages
        async with AsyncProducer(brokers=broker_address) as producer:
            for i in range(3):
                await producer.send_string(test_topic, f"iter test {i}", partition=0)

        # Consume using async iteration
        async with AsyncConsumer(
            brokers=broker_address,
            group_id="test-iter-group",
            topics=[test_topic]
        ) as consumer:
            await consumer.subscribe()

            count = 0
            async for message in consumer:
                assert message.value is not None
                count += 1
                if count >= 3:
                    break

            assert count == 3
