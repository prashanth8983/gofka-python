"""
Pytest configuration and fixtures
"""
import pytest
import socket
import time


@pytest.fixture
def broker_address():
    """Default broker address for tests"""
    return "localhost:9092"


@pytest.fixture
def test_topic():
    """Generate a unique test topic name"""
    return f"test-topic-{int(time.time())}"


@pytest.fixture
def test_group():
    """Generate a unique test group name"""
    return f"test-group-{int(time.time())}"


def is_broker_available(host="localhost", port=9092, timeout=1):
    """Check if Gofka broker is running"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except socket.error:
        return False


@pytest.fixture(scope="session")
def require_broker():
    """Skip tests if broker is not available"""
    if not is_broker_available():
        pytest.skip("Gofka broker not running on localhost:9092")
