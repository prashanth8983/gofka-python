# Gofka Python Client

A high-performance, feature-rich Python client library for the Gofka message broker.

## Features

### Core Features
- ✅ **Async/Await Support** - Full asyncio integration for high performance
- ✅ **Producer API** - Send messages synchronously or asynchronously
- ✅ **Consumer API** - Consume messages with full consumer group support
- ✅ **Admin API** - Cluster metadata and topic management
- ✅ **Zero Core Dependencies** - Pure Python stdlib implementation

### Production Features (NEW in v0.2.0!)
- ✅ **Compression** - gzip, snappy, and lz4 compression support
- ✅ **Message Batching** - Automatic batching for higher throughput
- ✅ **SSL/TLS** - Secure encrypted connections
- ✅ **Connection Pooling** - Efficient connection reuse
- ✅ **Retry Logic** - Exponential backoff with jitter
- ✅ **Broker Failover** - Automatic broker failover and load balancing
- ✅ **Custom Partitioners** - Flexible message partitioning
- ✅ **Message Headers** - Support for message metadata

### Consumer Group Features
- ✅ **Automatic Partition Assignment** - Range and round-robin strategies
- ✅ **Rebalancing** - Automatic partition rebalancing
- ✅ **Offset Management** - Automatic or manual offset commits
- ✅ **Heartbeat** - Automatic group membership maintenance

## Installation

### From PyPI (when published):
```bash
pip install gofka-python
```

### From source:
```bash
git clone https://github.com/user/gofka-python.git
cd gofka-python
pip install -e .
```

### With optional compression support:
```bash
pip install gofka-python[compression]
# OR
pip install python-snappy lz4
```

## 🎯 Interactive Examples (NEW!)

### Interactive Producer
Send messages interactively with real-time feedback:

```bash
python3 examples/interactive_producer.py [broker_addr] [topic]

# Example:
python3 examples/interactive_producer.py localhost:9092 test-topic
```

**Features:**
- ✅ Type messages and press Enter to send
- ✅ Real-time latency feedback (ms)
- ✅ Live statistics (type `stats`)
- ✅ Graceful shutdown (Ctrl+C or `quit`)

### Interactive Consumer
Poll for messages in real-time:

```bash
python3 examples/interactive_consumer.py [broker_addr] [topic] [group_id]

# Example:
python3 examples/interactive_consumer.py localhost:9092 test-topic my-group
```

**Features:**
- ✅ Continuous polling (1-second intervals)
- ✅ Real-time message display
- ✅ Auto statistics every 10 messages
- ✅ Graceful shutdown (Ctrl+C)

### Stress Test
Performance testing with detailed metrics:

```bash
python3 examples/stress_test.py \
  --broker localhost:9092 \
  --topic stress-test \
  --messages 10000 \
  --size 100 \
  --mode both

# Options:
#   --broker: Broker address (default: localhost:9092)
#   --topic: Topic name (default: stress-test)
#   --messages: Number of messages (default: 1000)
#   --size: Message size in bytes (default: 100)
#   --mode: produce|consume|both (default: both)
```

**Outputs:**
- 📊 Throughput (msg/s)
- 📊 Bandwidth (MB/s)
- 📊 Latency percentiles (p50, p95, p99)
- 📊 Progress bars

## 🚀 Quick Start with Interactive Examples

**Terminal 1 - Start Gofka Broker:**
```bash
cd ../Gofka  # Go to Gofka main project
go run cmd/gofka-broker/main.go --bootstrap
```

**Terminal 2 - Interactive Producer:**
```bash
cd gofka-python
python3 examples/interactive_producer.py
# Type messages and see real-time delivery confirmations!
```

**Terminal 3 - Interactive Consumer:**
```bash
python3 examples/interactive_consumer.py
# Watch messages appear in real-time!
```

**Terminal 4 - Run Stress Test:**
```bash
python3 examples/stress_test.py --messages 1000
# See detailed performance metrics!
```

## Quick Start

### Async Producer (Recommended)

```python
import asyncio
from gofka import AsyncProducer

async def main():
    async with AsyncProducer(
        brokers="localhost:9092",
        compression="gzip",  # Optional compression
        batch_size=16384,    # Batching for performance
        linger_ms=100        # Batch delay
    ) as producer:
        # Send single message
        offset = await producer.send_string("my-topic", "Hello, Gofka!")
        print(f"Message sent at offset {offset}")

        # Send multiple messages concurrently
        tasks = [
            producer.send_string("my-topic", f"Message {i}")
            for i in range(100)
        ]
        offsets = await asyncio.gather(*tasks)
        print(f"Sent {len(offsets)} messages")

asyncio.run(main())
```

### Async Consumer (Recommended)

```python
import asyncio
from gofka import AsyncConsumer

async def main():
    async with AsyncConsumer(
        brokers="localhost:9092",
        group_id="my-consumer-group",
        topics=["my-topic"],
        auto_commit=True
    ) as consumer:
        await consumer.subscribe()

        # Consume messages using async iteration
        async for message in consumer:
            print(f"Received: {message.value.decode()}")
            # Message is auto-committed

asyncio.run(main())
```

### Synchronous Producer

```python
from gofka import Producer

with Producer(brokers="localhost:9092") as producer:
    offset = producer.send_string("my-topic", "Hello, Gofka!")
    print(f"Message sent at offset {offset}")
```

### Synchronous Consumer

```python
from gofka import Consumer

consumer = Consumer(
    brokers="localhost:9092",
    group_id="my-group",
    topics=["my-topic"]
)

consumer.connect()
consumer.subscribe()

for message in consumer.poll(max_messages=10):
    print(f"Received: {message.value.decode()}")
    consumer.commit(message)

consumer.close()
```

## Advanced Features

### Compression

```python
# Automatic compression for all messages
async with AsyncProducer(
    brokers="localhost:9092",
    compression="gzip"  # or "snappy", "lz4"
) as producer:
    await producer.send_string("topic", "compressed message")
```

### SSL/TLS Encryption

```python
async with AsyncProducer(
    brokers="localhost:9092",
    use_ssl=True
) as producer:
    await producer.send_string("topic", "secure message")
```

### Message Batching

```python
async with AsyncProducer(
    brokers="localhost:9092",
    batch_size=16384,      # Max batch size in bytes
    linger_ms=100,         # Wait up to 100ms for batching
    max_in_flight=5        # Up to 5 concurrent requests
) as producer:
    # Messages are automatically batched
    for i in range(1000):
        await producer.send_string("topic", f"message {i}")
```

### Custom Partitioner

```python
def my_partitioner(topic, key, num_partitions):
    # Custom partitioning logic
    if key:
        return hash(key) % num_partitions
    return 0

async with AsyncProducer(
    brokers="localhost:9092",
    partitioner=my_partitioner
) as producer:
    await producer.send_string("topic", "message", key=b"user-123")
```

### Broker Failover

```python
from gofka import BrokerFailoverManager, RetryPolicy

manager = BrokerFailoverManager(
    brokers=["localhost:9092", "localhost:9093", "localhost:9094"],
    retry_policy=RetryPolicy(
        max_retries=3,
        initial_backoff_ms=100,
        max_backoff_ms=30000
    )
)

# Automatically retries and fails over to other brokers
result = await manager.execute_with_failover(my_function)
```

### Connection Pooling

```python
from gofka import ConnectionPool

pool = ConnectionPool(
    max_connections=10,
    max_idle_time=300,
    connection_timeout=30
)

async with pool.get_connection("localhost", 9092) as connection:
    # Use connection
    pass
```

### Message Headers

```python
# Producer
headers = {"user-id": "12345", "request-id": "abc-123"}
await producer.send(
    "topic",
    b"message",
    headers=headers
)

# Consumer
async for message in consumer:
    print(f"Headers: {message.headers}")
    print(f"Value: {message.value}")
```

## Admin Client

### Async Admin

```python
from gofka import AsyncAdminClient

async with AsyncAdminClient(brokers="localhost:9092") as admin:
    # Get cluster metadata
    metadata = await admin.get_cluster_metadata()
    print(f"Brokers: {metadata['brokers']}")
    print(f"Topics: {metadata['topics']}")

    # List topics
    topics = await admin.list_topics()
    print(f"Topics: {topics}")

    # Describe topic
    topic_info = await admin.describe_topic("my-topic")
    print(f"Partitions: {topic_info['partitions']}")

    # Get partition leaders
    leaders = await admin.get_topic_leaders("my-topic")
    print(f"Leaders: {leaders}")
```

## Performance Benchmarks

Run the included benchmarks:

```bash
# Producer benchmark
python benchmarks/benchmark_producer.py localhost:9092

# Consumer benchmark
python benchmarks/benchmark_consumer.py localhost:9092
```

**Typical Performance (single broker, 100-byte messages):**
- Producer: ~5,000-10,000 msg/sec
- Consumer: ~8,000-12,000 msg/sec
- With compression: ~15,000-20,000 msg/sec
- Async batching: ~30,000+ msg/sec

## API Reference

See [API Documentation](docs/API.md) for complete API reference.

### Quick Reference

**Async API:**
- `AsyncProducer` - Async message producer
- `AsyncConsumer` - Async message consumer
- `AsyncAdminClient` - Async admin operations

**Sync API:**
- `Producer` - Sync message producer
- `Consumer` - Sync message consumer
- `AdminClient` - Sync admin operations

**Utilities:**
- `ConnectionPool` - Connection pooling
- `RetryPolicy` - Retry configuration
- `BrokerFailoverManager` - Broker failover

**Exceptions:**
- `GofkaError` - Base exception
- `ConnectionError` - Connection issues
- `ProduceError` - Production failures
- `ConsumeError` - Consumption failures
- `OffsetError` - Offset management issues
- `GroupCoordinationError` - Consumer group issues

## Examples

Check the `examples/` directory:

- `simple_producer.py` - Basic producer
- `simple_consumer.py` - Basic consumer
- `admin_demo.py` - Admin operations
- `async_producer.py` - Async producer with compression
- `async_consumer.py` - Async consumer with groups
- `async_admin.py` - Async admin operations

Run examples:
```bash
cd examples
python3 async_producer.py
python3 async_consumer.py
```

## Testing

### Run all tests:
```bash
pytest
```

### Run with coverage:
```bash
pytest --cov=gofka --cov-report=html
```

### Run only unit tests:
```bash
pytest -m "not integration"
```

### Run integration tests (requires broker):
```bash
pytest -m integration
```

## Development

### Setup development environment:
```bash
pip install -e ".[dev]"
```

### Run linters:
```bash
black gofka/
ruff check gofka/
mypy gofka/
```

### Run benchmarks:
```bash
python benchmarks/benchmark_producer.py
python benchmarks/benchmark_consumer.py
```

## Architecture

```
gofka-python/
├── gofka/
│   ├── __init__.py           # Package exports
│   ├── protocol.py           # Sync protocol implementation
│   ├── async_protocol.py     # Async protocol implementation
│   ├── producer.py           # Sync producer
│   ├── async_producer.py     # Async producer with batching
│   ├── consumer.py           # Sync consumer
│   ├── async_consumer.py     # Async consumer
│   ├── admin.py              # Sync admin client
│   ├── async_admin.py        # Async admin client
│   ├── connection_pool.py    # Connection pooling
│   ├── retry.py              # Retry and failover logic
│   └── exceptions.py         # Exception hierarchy
├── examples/                 # Example scripts
├── tests/                    # Test suite
├── benchmarks/               # Performance benchmarks
└── docs/                     # Documentation
```

## Protocol

The Gofka Python client implements the Gofka binary wire protocol:

**Request Format:**
```
[size:4][api_key:2][api_version:2][correlation_id:4][client_id_len:2][client_id:var][payload:var]
```

**Response Format:**
```
[correlation_id:4][payload:var]
```

**Supported API Keys:**
- 0: Produce
- 1: Fetch
- 3: Metadata
- 8: OffsetCommit
- 9: OffsetFetch
- 11: JoinGroup
- 12: Heartbeat
- 13: LeaveGroup
- 14: SyncGroup

## Comparison with kafka-python

| Feature | gofka-python | kafka-python |
|---------|--------------|--------------|
| Async/await | ✅ Native | ❌ No |
| Compression | ✅ gzip, snappy, lz4 | ✅ Multiple |
| SSL/TLS | ✅ Yes | ✅ Yes |
| Consumer Groups | ✅ Yes | ✅ Yes |
| Transactions | ❌ Planned | ✅ Yes |
| Zero Dependencies | ✅ Yes (core) | ❌ No |
| Performance | ⚡ High | ⚡ High |
| Maturity | 🆕 New | ✅ Mature |

## Roadmap

### v0.3.0 (Planned)
- [ ] SASL authentication
- [ ] Advanced consumer metrics
- [ ] Sticky partition assignment
- [ ] Multi-topic consumer subscription

### v0.4.0 (Planned)
- [ ] Transactional producer
- [ ] Exactly-once semantics
- [ ] Schema registry integration

### v1.0.0 (Future)
- [ ] Production hardening
- [ ] Complete Kafka protocol compatibility
- [ ] Advanced monitoring and metrics

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest`
5. Run linters: `black gofka/ && ruff check gofka/`
6. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Related Projects

- [Gofka](https://github.com/user/gofka) - The Gofka message broker (Go)
- [Gofka Go Client](https://github.com/user/gofka-go) - Official Go client
- [Gofka Node.js Client](https://github.com/user/gofka-nodejs) - Official Node.js client

## Support

- GitHub Issues: https://github.com/user/gofka-python/issues
- Documentation: https://github.com/user/gofka-python/tree/main/docs
- Examples: https://github.com/user/gofka-python/tree/main/examples

---

**Status**: Beta - API is stable. Suitable for development and production use with monitoring.

**Current Version**: 0.2.0

**Python Compatibility**: Python 3.7+
