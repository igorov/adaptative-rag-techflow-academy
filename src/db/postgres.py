"""PostgreSQL connection pool utilities.

This module provides a process-wide connection pool that can be initialized
once at application startup and reused anywhere (e.g., inside tools).
"""

from __future__ import annotations

import os
from typing import Optional

from psycopg_pool import ConnectionPool

_pool: Optional[ConnectionPool] = None


def init_pool(dsn: Optional[str] = None, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """Initialize a global connection pool if not already created.

    Parameters
    ----------
    dsn: Optional[str]
        PostgreSQL connection string. If not provided, reads from
        environment variables ``POSTGRES_USER``, ``POSTGRES_PASSWORD``, ``POSTGRES_HOST``, ``POSTGRES_PORT``, ``POSTGRES_DB``.
    min_size: int
        Minimum number of pooled connections.
    max_size: int
        Maximum number of pooled connections.

    Returns
    -------
    ConnectionPool
        The initialized global connection pool.
    """
    global _pool
    if _pool is not None:
        return _pool

    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_TECHFLOW_DATABASE')

    postgres_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}?sslmode=require&channel_binding=require"
    effective_dsn = dsn or postgres_url
    if not postgres_user or not postgres_password or not postgres_host or not postgres_port or not postgres_db:
        raise ValueError("POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB no están configurados en .env")

    _pool = ConnectionPool(effective_dsn, min_size=min_size, max_size=max_size)
    return _pool


def get_pool() -> ConnectionPool:
    """Get the global connection pool, initializing it lazily if needed."""
    global _pool
    if _pool is None:
        init_pool()
    assert _pool is not None
    return _pool


def close_pool() -> None:
    """Close the global connection pool if it exists."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


