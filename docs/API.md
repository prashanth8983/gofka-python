# Gofka Python Client API Documentation

Complete API reference for the Gofka Python client library.

## Table of Contents

- [Synchronous API](#synchronous-api)
  - [Producer](#producer)
  - [Consumer](#consumer)
  - [AdminClient](#adminclient)
- [Asynchronous API](#asynchronous-api)
  - [AsyncProducer](#asyncproducer)
  - [AsyncConsumer](#asyncconsumer)
  - [AsyncAdminClient](#asyncadminclient)
- [Utilities](#utilities)
  - [ConnectionPool](#connectionpool)
  - [RetryPolicy](#retrypolicy)
  - [BrokerFailoverManager](#brokerfailovermanager)
- [Exceptions](#exceptions)

---

## Synchronous API

### Producer

```python
from gofka import Producer
```

#### Constructor

```python
Producer(
    brokers: str,
    client_id: str = "gofka-python-producer",
    timeout: int = 30
)
```

**Parameters:**
- `brokers`: Comma-separated list of broker addresses (e.g., "localhost:9092,localhost:9093")
- `client_id`: Client identifier string
- `timeout`: Connection timeout in seconds

**Example:**
```python
producer = Producer(brokers="localhost:9092")
producer.connect()
```

#### Methods

##### `connect()`
Connect to the broker.

##### `send(topic: str, message: bytes, partition: int = 0) -> int`
Send a binary message.

**Returns:** Message offset

##### `send_string(topic: str, message: str, partition: int = 0, encoding: str = 'utf-8') -> int`
Send a string message.

**Returns:** Message offset

##### `close()`
Close the connection.

##### Context Manager
```python
with Producer(brokers="localhost:9092") as producer:
    producer.send("topic", b"message")
```

---

### Consumer

```python
from gofka import Consumer
```

#### Constructor

```python
Consumer(
    brokers: str,
    group_id: str,
    topics: List[str],
    client_id: Optional[str] = None,
    auto_commit: bool = True,
    auto_offset_reset: str = "earliest",
    session_timeout: int = 30000,
    timeout: int = 30
)
```

**Parameters:**
- `brokers`: Broker addresses
- `group_id`: Consumer group ID
- `topics`: List of topics to subscribe to
- `client_id`: Client identifier (auto-generated if None)
- `auto_commit`: Automatically commit offsets
- `auto_offset_reset`: "earliest" or "latest"
- `session_timeout`: Session timeout in milliseconds
- `timeout`: Connection timeout in seconds

#### Methods

##### `connect()`
Connect to the broker.

##### `subscribe()`
Subscribe to topics and join consumer group.

##### `poll(max_messages: int = 1) -> List[Message]`
Poll for messages.

**Returns:** List of Message objects

##### `commit(message: Optional[Message] = None)`
Commit offsets.

##### `close()`
Leave group and close connection.

---

### AdminClient

```python
from gofka import AdminClient
```

#### Constructor

```python
AdminClient(
    brokers: str,
    client_id: str = "gofka-python-admin",
    timeout: int = 30
)
```

#### Methods

##### `get_cluster_metadata(topics: List[str] = None) -> Dict`
Get cluster metadata.

##### `list_brokers() -> List[Dict]`
List all brokers.

##### `list_topics() -> List[str]`
List all topics.

##### `describe_topic(topic_name: str) -> Dict`
Get topic details.

---

## Asynchronous API

### AsyncProducer

```python
from gofka import AsyncProducer
```

#### Constructor

```python
AsyncProducer(
    brokers: str,
    client_id: str = "gofka-python-async-producer",
    timeout: int = 30,
    use_ssl: bool = False,
    compression: Optional[str] = None,
    batch_size: int = 16384,
    linger_ms: int = 0,
    max_in_flight: int = 5,
    partitioner: Optional[Callable] = None
)
```

**Parameters:**
- `brokers`: Broker addresses
- `client_id`: Client identifier
- `timeout`: Connection timeout
- `use_ssl`: Enable SSL/TLS
- `compression`: 'gzip', 'snappy', 'lz4', or None
- `batch_size`: Maximum batch size in bytes
- `linger_ms`: Batching delay in milliseconds
- `max_in_flight`: Maximum concurrent requests
- `partitioner`: Custom partitioner function

**Example:**
```python
async with AsyncProducer(
    brokers="localhost:9092",
    compression="gzip",
    linger_ms=100
) as producer:
    offset = await producer.send_string("topic", "message")
```

#### Methods

##### `async connect()`
Connect to broker.

##### `async send(topic: str, message: bytes, partition: int = 0, key: Optional[bytes] = None, headers: Optional[dict] = None) -> int`
Send a message asynchronously.

**Returns:** Message offset

##### `async send_string(topic: str, message: str, partition: int = 0, key: Optional[str] = None, encoding: str = 'utf-8', headers: Optional[dict] = None) -> int`
Send a string message.

##### `async send_batch(topic: str, messages: List[bytes], partition: int = 0) -> List[int]`
Send multiple messages in batch.

##### `async flush()`
Flush pending messages.

##### `async close()`
Close connection.

---

### AsyncConsumer

```python
from gofka import AsyncConsumer
```

#### Constructor

```python
AsyncConsumer(
    brokers: str,
    group_id: str,
    topics: List[str],
    client_id: Optional[str] = None,
    auto_commit: bool = True,
    auto_offset_reset: str = "earliest",
    session_timeout: int = 30000,
    timeout: int = 30,
    use_ssl: bool = False,
    max_poll_records: int = 500
)
```

**Example:**
```python
async with AsyncConsumer(
    brokers="localhost:9092",
    group_id="my-group",
    topics=["my-topic"]
) as consumer:
    await consumer.subscribe()

    async for message in consumer:
        print(message.value.decode())
        await consumer.commit(message)
```

#### Methods

##### `async connect()`
Connect to broker.

##### `async subscribe()`
Subscribe and join consumer group.

##### `async poll(max_messages: int = 1) -> List[Message]`
Poll for messages.

##### `async commit(message: Optional[Message] = None)`
Commit offsets.

##### `async close()`
Leave group and close.

##### Async Iteration
```python
async for message in consumer:
    # Process message
```

---

### AsyncAdminClient

```python
from gofka import AsyncAdminClient
```

#### Constructor

```python
AsyncAdminClient(
    brokers: str,
    client_id: str = "gofka-python-async-admin",
    timeout: int = 30,
    use_ssl: bool = False
)
```

#### Methods

##### `async get_cluster_metadata(topics: Optional[List[str]] = None) -> Dict`
Get cluster metadata.

##### `async list_brokers() -> List[Dict]`
List brokers.

##### `async list_topics() -> List[str]`
List topics.

##### `async describe_topic(topic_name: str) -> Dict`
Get topic details.

##### `async get_partition_count(topic_name: str) -> int`
Get partition count for topic.

##### `async get_topic_leaders(topic_name: str) -> Dict[int, str]`
Get partition leaders.

---

## Utilities

### ConnectionPool

```python
from gofka import ConnectionPool
```

Manages connection pooling for async clients.

```python
pool = ConnectionPool(
    max_connections=10,
    max_idle_time=300,
    connection_timeout=30,
    use_ssl=False
)

connection = await pool.get_connection("localhost", 9092)
await pool.release_connection("localhost", 9092, connection)
await pool.close_all()
```

---

### RetryPolicy

```python
from gofka import RetryPolicy
```

Configures retry behavior with exponential backoff.

```python
policy = RetryPolicy(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=30000,
    backoff_multiplier=2.0,
    jitter=True
)
```

---

### BrokerFailoverManager

```python
from gofka import BrokerFailoverManager
```

Manages broker failover and load balancing.

```python
manager = BrokerFailoverManager(
    brokers=["localhost:9092", "localhost:9093"],
    retry_policy=RetryPolicy()
)

result = await manager.execute_with_failover(my_function)
```

---

## Exceptions

### GofkaError
Base exception for all Gofka errors.

### ConnectionError
Connection-related errors.

### ProduceError
Message production failures.

### ConsumeError
Message consumption failures.

### OffsetError
Offset management issues.

### GroupCoordinationError
Consumer group coordination failures.

### MetadataError
Metadata fetch failures.

**Example:**
```python
from gofka.exceptions import ProduceError, ConnectionError

try:
    producer.send("topic", b"message")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except ProduceError as e:
    print(f"Production failed: {e}")
```

---

## Message Object

```python
class Message:
    topic: str            # Topic name
    partition: int        # Partition ID
    offset: int           # Message offset
    value: bytes          # Message data
    headers: dict         # Message headers (optional)
    timestamp: float      # Receive timestamp
```

---

## Best Practices

### Production Use

1. **Use Async API** for better performance:
   ```python
   async with AsyncProducer(brokers="...") as producer:
       await producer.send(...)
   ```

2. **Enable compression** for large messages:
   ```python
   producer = AsyncProducer(compression="gzip")
   ```

3. **Configure batching** for throughput:
   ```python
   producer = AsyncProducer(batch_size=16384, linger_ms=100)
   ```

4. **Use connection pooling** for multiple clients:
   ```python
   pool = ConnectionPool(max_connections=10)
   ```

5. **Handle exceptions** properly:
   ```python
   try:
       await producer.send(...)
   except GofkaError as e:
       logger.error(f"Failed: {e}")
   ```

### Performance Tips

- Use `AsyncProducer` with `max_in_flight > 1` for concurrent sends
- Enable compression for messages > 1KB
- Use batching with `linger_ms` for higher throughput
- Set appropriate `session_timeout` for consumers
- Monitor consumer lag using admin client

---

## Version Information

**Current Version:** 0.2.0

**Python Compatibility:** Python 3.7+

**Optional Dependencies:**
- `python-snappy` for Snappy compression
- `lz4` for LZ4 compression
