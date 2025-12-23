"""Replication Reinitialization sub-app factory.

This wrapper lets the main DataSolveX portal mount the replication tool
under a sub-path using DispatcherMiddleware.
"""

from __future__ import annotations


def get_replication_app(secret_key: str):
    """Return the Flask app instance for mounting under /replication-reinit."""
    # Import lazily to avoid side effects during main portal startup
    from .app import app as repl_app
    repl_app.secret_key = secret_key
    return repl_app
