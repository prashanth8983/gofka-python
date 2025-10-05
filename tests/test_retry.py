"""
Tests for retry and failover logic
"""
import pytest
import asyncio
from gofka.retry import RetryPolicy, retry_with_backoff, BrokerFailoverManager
from gofka.exceptions import GofkaError, ConnectionError


class TestRetryPolicy:
    """Test RetryPolicy"""

    def test_retry_policy_init(self):
        """Test retry policy initialization"""
        policy = RetryPolicy(max_retries=5, initial_backoff_ms=100)
        assert policy.max_retries == 5
        assert policy.initial_backoff_ms == 100

    def test_get_backoff_ms(self):
        """Test backoff calculation"""
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=100,
            backoff_multiplier=2.0,
            jitter=False
        )

        backoff_0 = policy.get_backoff_ms(0)
        backoff_1 = policy.get_backoff_ms(1)
        backoff_2 = policy.get_backoff_ms(2)

        assert backoff_0 == 100
        assert backoff_1 == 200
        assert backoff_2 == 400

    def test_max_backoff(self):
        """Test maximum backoff limit"""
        policy = RetryPolicy(
            initial_backoff_ms=100,
            max_backoff_ms=500,
            backoff_multiplier=2.0,
            jitter=False
        )

        backoff_10 = policy.get_backoff_ms(10)
        assert backoff_10 <= 500


class TestRetryWithBackoff:
    """Test retry_with_backoff function"""

    @pytest.mark.asyncio
    async def test_successful_call(self):
        """Test successful call without retries"""
        async def success_func():
            return "success"

        result = await retry_with_backoff(success_func)
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test retry on transient failures"""
        call_count = 0

        async def fail_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient error")
            return "success"

        policy = RetryPolicy(max_retries=3, initial_backoff_ms=10, jitter=False)
        result = await retry_with_backoff(fail_twice, policy=policy)

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self):
        """Test exception when max retries exhausted"""
        async def always_fail():
            raise ConnectionError("Always fails")

        policy = RetryPolicy(max_retries=2, initial_backoff_ms=10, jitter=False)

        with pytest.raises(ConnectionError):
            await retry_with_backoff(always_fail, policy=policy)


class TestBrokerFailoverManager:
    """Test BrokerFailoverManager"""

    def test_init(self):
        """Test failover manager initialization"""
        manager = BrokerFailoverManager(["localhost:9092", "localhost:9093"])
        assert len(manager.brokers) == 2

    def test_get_next_broker(self):
        """Test round-robin broker selection"""
        manager = BrokerFailoverManager(["localhost:9092", "localhost:9093"])

        broker1 = manager.get_next_broker()
        broker2 = manager.get_next_broker()

        assert broker1 != broker2 or len(manager.brokers) == 1

    def test_mark_failed(self):
        """Test marking broker as failed"""
        manager = BrokerFailoverManager(["localhost:9092", "localhost:9093"])

        broker = manager.get_next_broker()
        manager.mark_failed(broker)

        assert broker in manager.failed_brokers

    def test_mark_success(self):
        """Test marking broker as successful"""
        manager = BrokerFailoverManager(["localhost:9092", "localhost:9093"])

        broker = manager.get_next_broker()
        manager.mark_failed(broker)
        assert broker in manager.failed_brokers

        manager.mark_success(broker)
        assert broker not in manager.failed_brokers

    @pytest.mark.asyncio
    async def test_execute_with_failover(self):
        """Test function execution with failover"""
        manager = BrokerFailoverManager(["localhost:9092", "localhost:9093"])
        call_count = 0

        async def test_func(host, port):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Fail first broker")
            return f"{host}:{port}"

        result = await manager.execute_with_failover(test_func)
        assert result is not None
        assert call_count == 2
