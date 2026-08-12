"""Exclusive encrypted LangGraph Checkpoint PostgreSQL ownership."""

from __future__ import annotations

from contextlib import AbstractContextManager
import os
from typing import Any


class CheckpointError(RuntimeError):
    """Safe checkpoint setup or health failure."""


def build_encrypted_serializer(encryption_key: bytes) -> Any:
    """Build the checkpoint serializer without routing the raw key through env."""

    if len(encryption_key) != 32:
        raise ValueError("checkpoint encryption key must be 32 bytes")
    from langgraph.checkpoint.serde.encrypted import EncryptedSerializer

    return EncryptedSerializer.from_pycryptodome_aes(key=bytes(encryption_key))


class CheckpointRuntime(AbstractContextManager["CheckpointRuntime"]):
    def __init__(self, dsn: str, encryption_key: bytes) -> None:
        if not dsn:
            raise ValueError("checkpoint DSN is required")
        if len(encryption_key) != 32:
            raise ValueError("checkpoint encryption key must be 32 bytes")
        self._dsn = dsn
        self._encryption_key = bytes(encryption_key)
        self._pool: Any = None
        self._checkpointer: Any = None

    @property
    def checkpointer(self) -> Any:
        if self._checkpointer is None:
            raise CheckpointError("checkpoint runtime is not open")
        return self._checkpointer

    def open(self) -> CheckpointRuntime:
        if self._checkpointer is not None:
            return self
        os.environ["LANGGRAPH_STRICT_MSGPACK"] = "true"
        try:
            from langgraph.checkpoint.postgres import PostgresSaver
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool

            pool = ConnectionPool(
                conninfo=self._dsn,
                min_size=1,
                max_size=4,
                timeout=2.0,
                reconnect_timeout=30.0,
                kwargs={
                    "autocommit": True,
                    "prepare_threshold": 0,
                    "row_factory": dict_row,
                },
                open=False,
            )
            self._pool = pool
            pool.open()
            pool.wait(timeout=5.0)
            serializer = build_encrypted_serializer(self._encryption_key)
            checkpointer = PostgresSaver(pool, serde=serializer)
            checkpointer.setup()
            self._checkpointer = checkpointer
        except Exception:
            self.close()
            raise CheckpointError("checkpoint database setup failed") from None
        return self

    def healthy(self) -> bool:
        pool = self._pool
        if pool is None:
            return False
        try:
            pool.check()
            with pool.connection(timeout=1.5) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    row = cursor.fetchone()
            return bool(row)
        except Exception:
            return False

    def close(self) -> None:
        pool = self._pool
        self._checkpointer = None
        self._pool = None
        if pool is not None:
            try:
                pool.close()
            except Exception:
                pass

    def __enter__(self) -> CheckpointRuntime:
        return self.open()

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "CheckpointRuntime[dsn=REDACTED, encryptionKey=REDACTED, open=%s]" % (
            self._checkpointer is not None
        )
