"""
Tests for Gofka AdminClient
"""
import pytest
from gofka import AdminClient


class TestAdminClient:
    """Test AdminClient functionality"""

    def test_admin_init(self):
        """Test admin client initialization"""
        admin = AdminClient(brokers="localhost:9092")
        assert admin.brokers == "localhost:9092"
        assert admin.client_id == "gofka-python-admin"

    @pytest.mark.integration
    def test_admin_connect(self, broker_address, require_broker):
        """Test admin client connection"""
        admin = AdminClient(brokers=broker_address)
        admin.connect()
        admin.close()

    @pytest.mark.integration
    def test_get_cluster_metadata(self, broker_address, require_broker):
        """Test getting cluster metadata"""
        admin = AdminClient(brokers=broker_address)
        admin.connect()

        metadata = admin.get_cluster_metadata()
        assert "brokers" in metadata
        assert "topics" in metadata
        assert len(metadata["brokers"]) > 0

        admin.close()

    @pytest.mark.integration
    def test_list_brokers(self, broker_address, require_broker):
        """Test listing brokers"""
        admin = AdminClient(brokers=broker_address)
        admin.connect()

        brokers = admin.list_brokers()
        assert len(brokers) > 0
        assert "id" in brokers[0]
        assert "host" in brokers[0]
        assert "port" in brokers[0]

        admin.close()

    @pytest.mark.integration
    def test_list_topics(self, broker_address, require_broker):
        """Test listing topics"""
        admin = AdminClient(brokers=broker_address)
        admin.connect()

        topics = admin.list_topics()
        assert isinstance(topics, list)

        admin.close()
