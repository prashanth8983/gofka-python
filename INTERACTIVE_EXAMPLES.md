# Interactive Examples Guide

This guide shows how to use the interactive examples to test and explore Gofka.

## 📋 Prerequisites

1. **Start Gofka Broker:**
   ```bash
   cd ../Gofka
   go run cmd/gofka-broker/main.go --bootstrap
   ```

2. **Install Python Client** (if not already):
   ```bash
   cd gofka-python
   pip install -e .
   ```

## 🎯 Example 1: Interactive Producer

### Basic Usage

```bash
python3 examples/interactive_producer.py
```

### With Custom Broker and Topic

```bash
python3 examples/interactive_producer.py localhost:9092 my-topic
```

### What You Can Do

1. **Send Messages:**
   ```
   📤 Enter message: Hello Gofka!
      ✅ Sent! Offset: 0, Latency: 12.45ms
   ```

2. **View Statistics:**
   ```
   📤 Enter message: stats

   📊 Statistics:
     Messages sent: 5
     Bytes sent: 127
     Elapsed time: 10.23s
     Avg rate: 0.49 msg/s
   ```

3. **Exit:**
   - Type `quit` or `exit`
   - Press Ctrl+C

## 📥 Example 2: Interactive Consumer

### Basic Usage

```bash
python3 examples/interactive_consumer.py
```

### With Custom Configuration

```bash
python3 examples/interactive_consumer.py localhost:9092 my-topic my-consumer-group
```

### What You'll See

1. **Waiting for Messages:**
   ```
   🔄 Starting to poll for messages...
      (Waiting for new messages...)

   ⏳ Waiting for new messages... (offset: -1)
   ```

2. **Receiving Messages:**
   ```
   📩 Message #1: Hello Gofka!
   📩 Message #2: Another message
   📩 Message #3: Testing replication
      📊 Stats: 3 msgs, 0.30 msg/s
   ```

3. **Auto Statistics:**
   - Shows stats every 10 messages
   - Final stats on exit (Ctrl+C)

## 🔥 Example 3: Stress Test

### Basic Test (1000 messages)

```bash
python3 examples/stress_test.py
```

### Custom Configuration

```bash
python3 examples/stress_test.py \
  --broker localhost:9092 \
  --topic stress-test \
  --messages 10000 \
  --size 200 \
  --mode both
```

### Output Example

```
====================================================================
  Gofka Stress Test
====================================================================

📤 Producer Stress Test
  Broker: localhost:9092
  Topic: stress-test
  Messages: 10000
  Message Size: 200 bytes

Sending: |██████████████████████████████████████████████████| 100.0% msg 10000/10000

📊 Producer Results:
  Total Time: 12.34s
  Messages Sent: 10000
  Total Bytes: 2,000,000 (1.91 MB)
  Throughput: 810.37 msg/s
  Bandwidth: 0.15 MB/s

  Latency (ms):
    Average: 1.23
    p50: 1.10
    p95: 2.45
    p99: 3.87

====================================================================

📥 Consumer Stress Test
  Broker: localhost:9092
  Topic: stress-test
  Expected Messages: 10000

🔄 Consuming messages...
Receiving: |██████████████████████████████████████████████████| 100.0% msg 10000/10000

📊 Consumer Results:
  Total Time: 8.76s
  Messages Received: 10000
  Total Bytes: 2,000,000 (1.91 MB)
  Throughput: 1141.55 msg/s
  Bandwidth: 0.22 MB/s

✅ Stress test completed!
```

## 🌐 Multi-Broker Testing

Test data replication across multiple brokers:

### Step 1: Start 3 Brokers

**Terminal 1 - Broker 1 (Leader):**
```bash
cd ../Gofka
go run cmd/gofka-broker/main.go \
  --node.id=broker-1 \
  --addr=localhost:9092 \
  --raft.addr=localhost:19092 \
  --bootstrap
```

**Terminal 2 - Broker 2 (Follower):**
```bash
go run cmd/gofka-broker/main.go \
  --node.id=broker-2 \
  --addr=localhost:9093 \
  --raft.addr=localhost:19093 \
  --peers=localhost:19092
```

**Terminal 3 - Broker 3 (Follower):**
```bash
go run cmd/gofka-broker/main.go \
  --node.id=broker-3 \
  --addr=localhost:9094 \
  --raft.addr=localhost:19094 \
  --peers=localhost:19092
```

### Step 2: Produce to Broker 1

**Terminal 4:**
```bash
cd gofka-python
python3 examples/interactive_producer.py localhost:9092 cluster-test
```

Type some messages:
```
📤 Enter message: Message 1 - sent to broker-1
   ✅ Sent! Offset: 0, Latency: 15.23ms

📤 Enter message: Message 2 - testing replication
   ✅ Sent! Offset: 1, Latency: 12.87ms
```

### Step 3: Consume from Broker 2

**Terminal 5:**
```bash
python3 examples/interactive_consumer.py localhost:9093 cluster-test cluster-group
```

You'll see the same messages! 🎉
```
📩 Message #1: Message 1 - sent to broker-1
📩 Message #2: Message 2 - testing replication
```

### Step 4: Verify Replication

**Terminal 6 - Consume from Broker 3:**
```bash
python3 examples/interactive_consumer.py localhost:9094 cluster-test cluster-group-2
```

Same messages appear again - proving 3-way replication works! ✅

## 📊 Performance Benchmarking

### Test Different Message Sizes

```bash
# Small messages (100 bytes)
python3 examples/stress_test.py --messages 10000 --size 100

# Medium messages (1KB)
python3 examples/stress_test.py --messages 5000 --size 1024

# Large messages (10KB)
python3 examples/stress_test.py --messages 1000 --size 10240
```

### Test Different Modes

```bash
# Producer only
python3 examples/stress_test.py --mode produce --messages 10000

# Consumer only (make sure messages exist first!)
python3 examples/stress_test.py --mode consume --messages 10000

# Both (default)
python3 examples/stress_test.py --mode both --messages 10000
```

## 🐛 Troubleshooting

### Connection Refused
```
❌ Failed to connect to broker: [Errno 61] Connection refused
```

**Solution:** Make sure broker is running:
```bash
cd ../Gofka
go run cmd/gofka-broker/main.go --bootstrap
```

### No Messages Received
```
⏳ Waiting for new messages... (offset: -1)
```

**Solution:** Send some messages first using interactive producer in another terminal.

### Timeout Errors
```
❌ Error sending message: timeout
```

**Solutions:**
- Check broker is responding
- Increase timeout (edit the example code)
- Check network connectivity

## 💡 Tips and Tricks

1. **Watch Real-Time Replication:**
   - Start consumer first
   - Then send messages from producer
   - Watch messages appear instantly

2. **Measure Latency:**
   - Use interactive producer to see per-message latency
   - Use stress test for percentile analysis

3. **Test Failover:**
   - Start 3 brokers
   - Send messages to broker-1
   - Kill broker-1 (Ctrl+C)
   - Watch brokers elect new leader
   - Messages still available from broker-2 or broker-3!

4. **Consumer Groups:**
   - Start 2 consumers with same group_id
   - Send 10 messages
   - Each consumer gets ~5 messages (load balanced!)

## 📝 Next Steps

- Try the async examples in `examples/async_*.py`
- Explore compression with `gofka.compression`
- Build your own application using the client library
- Read the full API documentation in `docs/`

Happy messaging! 🚀
