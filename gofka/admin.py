"""
Gofka Admin client for cluster management
"""

from typing import List, Dict
from .protocol import Protocol
from .exceptions import MetadataError


class AdminClient:
    """
    Admin client for Gofka cluster operations

    Example:
        admin = AdminClient(brokers="localhost:9092")
        admin.connect()

        # Get cluster metadata
        metadata = admin.get_cluster_metadata()
        print(f"Brokers: {metadata['brokers']}")
        print(f"Topics: {metadata['topics']}")

        admin.close()
    """

    def __init__(self, brokers: str, client_id: str = "gofka-python-admin", timeout: int = 30):
        """
        Initialize admin client

        Args:
            brokers: Comma-separated list of broker addresses
            client_id: Client identifier
            timeout: Connection timeout in seconds
        """
        broker_list = brokers.split(',')
        if not broker_list:
            raise MetadataError("No brokers specified")

        host, port = broker_list[0].strip().split(':')

        self.client_id = client_id
        self.protocol = Protocol(host, int(port), timeout)
        self.connected = False

    def connect(self):
        """Connect to broker"""
        self.protocol.connect()
        self.connected = True

    def get_cluster_metadata(self, topics: List[str] = None) -> Dict:
        """
        Get cluster metadata

        Args:
            topics: List of specific topics (None = all topics)

        Returns:
            Dictionary with brokers and topics information
        """
        if not self.connected:
            raise MetadataError("Admin client not connected. Call connect() first.")

        try:
            metadata = self.protocol.get_metadata(topics or [], self.client_id)
            return metadata
        except Exception as e:
            raise MetadataError(f"Failed to get metadata: {e}")

    def list_brokers(self) -> List[Dict]:
        """
        List all brokers in the cluster

        Returns:
            List of broker dictionaries with 'id' and 'addr'
        """
        metadata = self.get_cluster_metadata()
        return metadata.get("brokers", [])

    def list_topics(self) -> List[str]:
        """
        List all topics in the cluster

        Returns:
            List of topic names
        """
        metadata = self.get_cluster_metadata()
        topics = metadata.get("topics", [])
        return [topic["name"] for topic in topics]

    def describe_topic(self, topic_name: str) -> Dict:
        """
        Get detailed information about a topic

        Args:
            topic_name: Topic name

        Returns:
            Topic metadata including partitions
        """
        metadata = self.get_cluster_metadata([topic_name])
        topics = metadata.get("topics", [])

        for topic in topics:
            if topic["name"] == topic_name:
                return topic

        raise MetadataError(f"Topic '{topic_name}' not found")

    def get_topic_partitions(self, topic_name: str) -> List[Dict]:
        """
        Get partition information for a topic

        Args:
            topic_name: Topic name

        Returns:
            List of partition dictionaries
        """
        topic = self.describe_topic(topic_name)
        return topic.get("partitions", [])

    def close(self):
        """Close connection to broker"""
        if self.connected:
            self.protocol.close()
            self.connected = False

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
