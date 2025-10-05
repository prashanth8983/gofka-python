"""
Connection pooling for Gofka clients
"""

import asyncio
import time
from typing import Dict, Optional, Tuple
from .async_protocol import AsyncProtocol
from .exceptions import ConnectionError


class ConnectionPool:
    """
    Connection pool for managing reusable broker connections

    Provides:
    - Connection reuse to reduce overhead
    - Automatic connection health checks
    - Max connections limit
    - Connection timeout and cleanup
    """

    def __init__(
        self,
        max_connections: int = 10,
        max_idle_time: int = 300,
        connection_timeout: int = 30,
        use_ssl: bool = False
    ):
        """
        Initialize connection pool

        Args:
            max_connections: Maximum number of connections per broker
            max_idle_time: Maximum idle time before closing connection (seconds)
            connection_timeout: Connection timeout (seconds)
            use_ssl: Enable SSL/TLS
        """
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self.connection_timeout = connection_timeout
        self.use_ssl = use_ssl

        # (host, port) -> list of (protocol, last_used_time, in_use)
        self._pools: Dict[Tuple[str, int], list] = {}
        self._lock = asyncio.Lock()

    async def get_connection(self, host: str, port: int) -> AsyncProtocol:
        """
        Get a connection from the pool or create a new one

        Args:
            host: Broker host
            port: Broker port

        Returns:
            AsyncProtocol instance
        """
        key = (host, port)

        async with self._lock:
            # Initialize pool for this broker if needed
            if key not in self._pools:
                self._pools[key] = []

            pool = self._pools[key]

            # Look for an available idle connection
            for i, (protocol, last_used, in_use) in enumerate(pool):
                if not in_use:
                    # Check if connection is still valid
                    idle_time = time.time() - last_used

                    if idle_time < self.max_idle_time:
                        # Mark as in use and return
                        pool[i] = (protocol, time.time(), True)
                        return protocol
                    else:
                        # Connection too old, close it
                        await protocol.close()
                        pool.pop(i)
                        break

            # No available connections, create a new one if under limit
            if len(pool) < self.max_connections:
                protocol = AsyncProtocol(host, port, self.connection_timeout, self.use_ssl)
                await protocol.connect()
                pool.append((protocol, time.time(), True))
                return protocol

            # Pool is full, wait for a connection to become available
            # For simplicity, raise an error (could implement waiting)
            raise ConnectionError(f"Connection pool exhausted for {host}:{port}")

    async def release_connection(self, host: str, port: int, protocol: AsyncProtocol):
        """
        Release a connection back to the pool

        Args:
            host: Broker host
            port: Broker port
            protocol: Protocol instance to release
        """
        key = (host, port)

        async with self._lock:
            if key not in self._pools:
                # Pool doesn't exist, just close the connection
                await protocol.close()
                return

            pool = self._pools[key]

            # Find the connection and mark as not in use
            for i, (p, last_used, in_use) in enumerate(pool):
                if p is protocol:
                    pool[i] = (p, time.time(), False)
                    return

            # Connection not in pool, close it
            await protocol.close()

    async def close_all(self):
        """Close all connections in all pools"""
        async with self._lock:
            for pool in self._pools.values():
                for protocol, _, _ in pool:
                    try:
                        await protocol.close()
                    except:
                        pass

            self._pools.clear()

    async def cleanup_idle(self):
        """Clean up idle connections that have exceeded max_idle_time"""
        async with self._lock:
            for key, pool in list(self._pools.items()):
                to_remove = []

                for i, (protocol, last_used, in_use) in enumerate(pool):
                    if not in_use:
                        idle_time = time.time() - last_used

                        if idle_time >= self.max_idle_time:
                            await protocol.close()
                            to_remove.append(i)

                # Remove closed connections
                for i in reversed(to_remove):
                    pool.pop(i)

                # Remove empty pools
                if not pool:
                    del self._pools[key]


class PooledAsyncProtocol:
    """
    Wrapper for AsyncProtocol with connection pooling support
    """

    def __init__(self, pool: ConnectionPool, host: str, port: int):
        self.pool = pool
        self.host = host
        self.port = port
        self._protocol: Optional[AsyncProtocol] = None

    async def __aenter__(self):
        """Get connection from pool"""
        self._protocol = await self.pool.get_connection(self.host, self.port)
        return self._protocol

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release connection back to pool"""
        if self._protocol:
            await self.pool.release_connection(self.host, self.port, self._protocol)
            self._protocol = None
