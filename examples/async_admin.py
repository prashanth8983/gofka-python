#!/usr/bin/env python3
"""
Async Gofka Admin Example

Demonstrates async cluster metadata operations.
"""

import asyncio
import sys
from gofka import AsyncAdminClient


async def main():
    # Create async admin client
    async with AsyncAdminClient(brokers="localhost:9092") as admin:
        print("✓ Connected to Gofka broker (async)")

        # Get cluster metadata
        metadata = await admin.get_cluster_metadata()

        print("\n=== Cluster Metadata ===")
        print(f"\nBrokers ({len(metadata['brokers'])}):")
        for broker in metadata['brokers']:
            print(f"  - {broker['id']} @ {broker['addr']}")

        print(f"\nTopics ({len(metadata['topics'])}):")
        for topic in metadata['topics']:
            print(f"  - {topic['name']} ({len(topic['partitions'])} partitions)")
            for partition in topic['partitions']:
                print(f"    Partition {partition['id']}: Leader={partition['leader']}, "
                      f"Replicas={partition['replicas']}")

        # List topics
        topics = await admin.list_topics()
        print(f"\n=== Topic List ===")
        for topic in topics:
            print(f"  - {topic}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAdmin client interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
