"""
Gofka client exceptions
"""


class GofkaError(Exception):
    """Base exception for all Gofka errors"""
    pass


class ConnectionError(GofkaError):
    """Connection-related errors"""
    pass


class ProduceError(GofkaError):
    """Error producing messages"""
    pass


class ConsumeError(GofkaError):
    """Error consuming messages"""
    pass


class OffsetError(GofkaError):
    """Error managing offsets"""
    pass


class GroupCoordinationError(GofkaError):
    """Error with consumer group coordination"""
    pass


class MetadataError(GofkaError):
    """Error fetching metadata"""
    pass
