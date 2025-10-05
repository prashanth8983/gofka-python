"""
Retry and failover logic for Gofka clients
"""

import asyncio
import random
from typing import List, Callable, TypeVar, Optional
from .exceptions import ConnectionError, GofkaError

T = TypeVar('T')


class RetryPolicy:
    """
    Configurable retry policy with exponential backoff
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_backoff_ms: int = 100,
        max_backoff_ms: int = 30000,
        backoff_multiplier: float = 2.0,
        jitter: bool = True
    ):
        """
        Initialize retry policy

        Args:
            max_retries: Maximum number of retry attempts
            initial_backoff_ms: Initial backoff delay in milliseconds
            max_backoff_ms: Maximum backoff delay in milliseconds
            backoff_multiplier: Multiplier for exponential backoff
            jitter: Add random jitter to backoff delays
        """
        self.max_retries = max_retries
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter

    def get_backoff_ms(self, attempt: int) -> int:
        """
        Calculate backoff delay for given attempt

        Args:
            attempt: Current attempt number (0-based)

        Returns:
            Backoff delay in milliseconds
        """
        backoff = min(
            self.initial_backoff_ms * (self.backoff_multiplier ** attempt),
            self.max_backoff_ms
        )

        if self.jitter:
            # Add random jitter (±25%)
            jitter_range = backoff * 0.25
            backoff = backoff + random.uniform(-jitter_range, jitter_range)

        return int(max(backoff, 0))


async def retry_with_backoff(
    func: Callable[..., T],
    *args,
    policy: Optional[RetryPolicy] = None,
    retriable_exceptions: tuple = (ConnectionError, GofkaError),
    **kwargs
) -> T:
    """
    Execute function with retry and exponential backoff

    Args:
        func: Async function to execute
        *args: Positional arguments for func
        policy: Retry policy (uses default if None)
        retriable_exceptions: Tuple of exceptions to retry on
        **kwargs: Keyword arguments for func

    Returns:
        Result from func

    Raises:
        Last exception if all retries exhausted
    """
    if policy is None:
        policy = RetryPolicy()

    last_exception = None

    for attempt in range(policy.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retriable_exceptions as e:
            last_exception = e

            if attempt < policy.max_retries:
                backoff_ms = policy.get_backoff_ms(attempt)
                await asyncio.sleep(backoff_ms / 1000.0)
            else:
                # Max retries exhausted
                break

    # All retries failed
    raise last_exception


class BrokerFailoverManager:
    """
    Manages broker failover and load balancing
    """

    def __init__(self, brokers: List[str], retry_policy: Optional[RetryPolicy] = None):
        """
        Initialize failover manager

        Args:
            brokers: List of broker addresses in "host:port" format
            retry_policy: Retry policy for connection attempts
        """
        self.brokers = [self._parse_broker(b) for b in brokers]
        self.retry_policy = retry_policy or RetryPolicy()
        self.current_index = 0
        self.failed_brokers = set()

    def _parse_broker(self, broker: str) -> tuple:
        """Parse broker address into (host, port) tuple"""
        host, port = broker.strip().split(':')
        return (host, int(port))

    def get_next_broker(self) -> tuple:
        """
        Get next available broker using round-robin

        Returns:
            (host, port) tuple
        """
        available = [b for b in self.brokers if b not in self.failed_brokers]

        if not available:
            # All brokers failed, reset and try again
            self.failed_brokers.clear()
            available = self.brokers

        # Round-robin selection
        broker = available[self.current_index % len(available)]
        self.current_index = (self.current_index + 1) % len(available)

        return broker

    def mark_failed(self, broker: tuple):
        """Mark a broker as failed"""
        self.failed_brokers.add(broker)

    def mark_success(self, broker: tuple):
        """Mark a broker as successful (remove from failed set)"""
        self.failed_brokers.discard(broker)

    async def execute_with_failover(
        self,
        func: Callable[[str, int], T],
        *args,
        **kwargs
    ) -> T:
        """
        Execute function with automatic broker failover

        Args:
            func: Async function that takes (host, port) as first arguments
            *args: Additional positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result from func

        Raises:
            GofkaError if all brokers fail
        """
        attempts = 0
        max_attempts = len(self.brokers) * (self.retry_policy.max_retries + 1)

        last_exception = None

        while attempts < max_attempts:
            broker = self.get_next_broker()
            host, port = broker

            try:
                result = await retry_with_backoff(
                    func,
                    host,
                    port,
                    *args,
                    policy=self.retry_policy,
                    **kwargs
                )

                # Success - mark broker as healthy
                self.mark_success(broker)
                return result

            except Exception as e:
                last_exception = e
                self.mark_failed(broker)
                attempts += 1

        # All brokers and retries exhausted
        raise GofkaError(f"All brokers failed after {attempts} attempts: {last_exception}")


async def with_timeout(func: Callable[..., T], timeout_seconds: float, *args, **kwargs) -> T:
    """
    Execute async function with timeout

    Args:
        func: Async function to execute
        timeout_seconds: Timeout in seconds
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Result from func

    Raises:
        asyncio.TimeoutError if timeout is exceeded
    """
    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_seconds)
