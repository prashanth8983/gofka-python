"""
Gofka Python Client Library

A native Python client for the Gofka message broker.
"""

# Synchronous clients
from .producer import Producer
from .consumer import Consumer, Message
from .admin import AdminClient

# Asynchronous clients
from .async_producer import AsyncProducer
from .async_consumer import AsyncConsumer
from .async_admin import AsyncAdminClient

# Utilities
from .connection_pool import ConnectionPool
from .retry import RetryPolicy, BrokerFailoverManager

# Exceptions
from .exceptions import (
    GofkaError,
    ConnectionError,
    ProduceError,
    ConsumeError,
    OffsetError,
    GroupCoordinationError,
    MetadataError
)

__version__ = "0.2.0"
__all__ = [
    # Sync clients
    "Producer",
    "Consumer",
    "AdminClient",
    "Message",
    # Async clients
    "AsyncProducer",
    "AsyncConsumer",
    "AsyncAdminClient",
    # Utilities
    "ConnectionPool",
    "RetryPolicy",
    "BrokerFailoverManager",
    # Exceptions
    "GofkaError",
    "ConnectionError",
    "ProduceError",
    "ConsumeError",
    "OffsetError",
    "GroupCoordinationError",
    "MetadataError"
]
