"""
Gofka Async Admin client for cluster management
"""

from typing import List, Dict, Optional
from .async_protocol import AsyncProtocol
from .exceptions import MetadataError


class AsyncAdminClient:
    """
    Async admin client for Gofka cluster operations

    Example:
        async with AsyncAdminClient(brokers="localhost:9092") as admin:
            metadata = await admin.get_cluster_metadata()
            print(f"Brokers: {metadata['brokers']}")
            print(f"Topics: {metadata['topics']}")
    """

    def __init__(
        self,
        brokers: str,
        client_id: str = "gofka-python-async-admin",
        timeout: int = 30,
        use_ssl: bool = False
    ):
        """
        Initialize async admin client

        Args:
            brokers: Comma-separated list of broker addresses
            client_id: Client identifier
            timeout: Connection timeout in seconds
            use_ssl: Enable SSL/TLS encryption
        """
        broker_list = brokers.split(',')
        if not broker_list:
            raise MetadataError("No brokers specified")

        host, port = broker_list[0].strip().split(':')

        self.client_id = client_id
        self.protocol = AsyncProtocol(host, int(port), timeout, use_ssl)
        self.connected = False

    async def connect(self):
        """Connect to broker"""
        await self.protocol.connect()
        self.connected = True

    async def get_cluster_metadata(self, topics: Optional[List[str]] = None) -> Dict:
        """
        Get cluster metadata asynchronously

        Args:
            topics: Optional list of topics to get metadata for (None = all topics)

        Returns:
            Dictionary with brokers and topics metadata
        """
        if not self.connected:
            raise MetadataError("Admin client not connected. Call connect() first.")

        try:
            metadata = await self.protocol.metadata(topics, self.client_id)
            return metadata
        except Exception as e:
            raise MetadataError(f"Failed to fetch metadata: {e}")

    async def list_brokers(self) -> List[Dict]:
        """
        List all brokers in the cluster

        Returns:
            List of broker dictionaries with 'id' and 'addr' fields
        """
        metadata = await self.get_cluster_metadata()
        return metadata.get('brokers', [])

    async def list_topics(self) -> List[str]:
        """
        List all topics in the cluster

        Returns:
            List of topic names
        """
        metadata = await self.get_cluster_metadata()
        topics = metadata.get('topics', [])
        return [topic['name'] for topic in topics]

    async def describe_topic(self, topic_name: str) -> Dict:
        """
        Get detailed information about a topic

        Args:
            topic_name: Name of the topic

        Returns:
            Topic metadata dictionary
        """
        metadata = await self.get_cluster_metadata(topics=[topic_name])
        topics = metadata.get('topics', [])

        for topic in topics:
            if topic['name'] == topic_name:
                return topic

        raise MetadataError(f"Topic '{topic_name}' not found")

    async def get_partition_count(self, topic_name: str) -> int:
        """
        Get the number of partitions for a topic

        Args:
            topic_name: Name of the topic

        Returns:
            Number of partitions
        """
        topic = await self.describe_topic(topic_name)
        return len(topic.get('partitions', []))

    async def get_topic_leaders(self, topic_name: str) -> Dict[int, str]:
        """
        Get partition leaders for a topic

        Args:
            topic_name: Name of the topic

        Returns:
            Dictionary mapping partition ID to leader broker ID
        """
        topic = await self.describe_topic(topic_name)
        partitions = topic.get('partitions', [])

        leaders = {}
        for partition in partitions:
            leaders[partition['id']] = partition['leader']

        return leaders

    async def close(self):
        """Close connection to broker"""
        if self.connected:
            await self.protocol.close()
            self.connected = False

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
