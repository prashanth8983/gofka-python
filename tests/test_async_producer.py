"""
Tests for Async Gofka Producer
"""
import pytest
import asyncio
from gofka import AsyncProducer
from gofka.exceptions import ConnectionError, ProduceError


class TestAsyncProducer:
    """Test AsyncProducer functionality"""

    def test_async_producer_init(self):
        """Test async producer initialization"""
        producer = AsyncProducer(brokers="localhost:9092")
        assert not producer.connected
        assert producer.compression is None

    def test_async_producer_with_compression(self):
        """Test async producer with compression"""
        producer = AsyncProducer(brokers="localhost:9092", compression="gzip")
        assert producer.compression == "gzip"

    def test_async_producer_invalid_compression(self):
        """Test async producer with invalid compression"""
        with pytest.raises(ProduceError):
            AsyncProducer(brokers="localhost:9092", compression="invalid")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_connect(self, broker_address, require_broker):
        """Test async producer connection to broker"""
        producer = AsyncProducer(brokers=broker_address)
        await producer.connect()
        assert producer.connected
        await producer.close()
        assert not producer.connected

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_context_manager(self, broker_address, require_broker):
        """Test async producer with context manager"""
        async with AsyncProducer(brokers=broker_address) as producer:
            assert producer.connected

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_send(self, broker_address, test_topic, require_broker):
        """Test sending a message asynchronously"""
        async with AsyncProducer(brokers=broker_address) as producer:
            offset = await producer.send(test_topic, b"async test message", partition=0)
            assert offset >= 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_send_string(self, broker_address, test_topic, require_broker):
        """Test sending a string message asynchronously"""
        async with AsyncProducer(brokers=broker_address) as producer:
            offset = await producer.send_string(test_topic, "async string message", partition=0)
            assert offset >= 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_concurrent_sends(self, broker_address, test_topic, require_broker):
        """Test concurrent message sending"""
        async with AsyncProducer(brokers=broker_address) as producer:
            tasks = [
                producer.send_string(test_topic, f"message {i}", partition=0)
                for i in range(10)
            ]
            offsets = await asyncio.gather(*tasks)
            assert len(offsets) == 10
            assert all(offset >= 0 for offset in offsets)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_producer_with_compression(self, broker_address, test_topic, require_broker):
        """Test sending compressed messages"""
        async with AsyncProducer(brokers=broker_address, compression="gzip") as producer:
            offset = await producer.send_string(test_topic, "compressed message", partition=0)
            assert offset >= 0
