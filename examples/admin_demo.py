#!/usr/bin/env python3
"""
Gofka Admin Client Example

Demonstrates cluster metadata operations.
"""

import sys
from gofka import AdminClient

def main():
    # Create admin client
    admin = AdminClient(brokers="localhost:9092")

    try:
        # Connect to broker
        print("Connecting to Gofka broker...")
        admin.connect()
        print("✓ Connected!")

        # Get cluster metadata
        print("\n=== Cluster Metadata ===")
        metadata = admin.get_cluster_metadata()

        # List brokers
        print("\nBrokers:")
        for broker in metadata["brokers"]:
            print(f"  - {broker['id']} @ {broker['addr']}")

        # List topics
        print("\nTopics:")
        topics = metadata.get("topics", [])
        if topics:
            for topic in topics:
                print(f"  - {topic['name']} ({len(topic['partitions'])} partitions)")
                for partition in topic['partitions']:
                    print(f"      Partition {partition['id']}: leader={partition['leader']}, "
                          f"replicas={partition['replicas']}")
        else:
            print("  (no topics found)")

        # Try to describe a specific topic
        if topics:
            topic_name = topics[0]["name"]
            print(f"\n=== Topic Details: {topic_name} ===")
            topic_info = admin.describe_topic(topic_name)
            print(f"Name: {topic_info['name']}")
            print(f"Partitions: {len(topic_info['partitions'])}")
            for p in topic_info['partitions']:
                print(f"  Partition {p['id']}: leader={p['leader']}")

    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        admin.close()
        print("\nAdmin client closed.")


if __name__ == "__main__":
    main()
