"""Valkey list consumer for the fixed versioned worker queues."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .contracts import QueuedJobReference, WorkerContractViolation


QUEUE_KEY = "axms:coding:jobs:v1"
PROCESSING_QUEUE_KEY = "axms:coding:jobs:v1:processing"
NATURAL_CMS_QUEUE_KEY = "axms:natural-cms:jobs:v1"
ALLOWED_QUEUE_KEYS = frozenset({QUEUE_KEY, NATURAL_CMS_QUEUE_KEY})
MAX_JOB_REFERENCE_BYTES = 128


class QueueError(RuntimeError):
    """Safe Queue connectivity or payload failure."""


@dataclass(frozen=True, slots=True)
class QueueDelivery:
    job: QueuedJobReference
    _raw: bytes = field(repr=False)

    def __repr__(self) -> str:
        return "QueueDelivery[jobId=%s]" % self.job.job_id


class ValkeyJobQueue:
    def __init__(
        self,
        host: str,
        port: int,
        database: int,
        *,
        password: str | None = None,
        queue_key: str = QUEUE_KEY,
        socket_timeout_seconds: float = 10.0,
    ) -> None:
        if queue_key not in ALLOWED_QUEUE_KEYS:
            raise ValueError("queue_key must use a versioned worker queue")
        self._host = host
        self._port = port
        self._database = database
        self._password = password
        self._queue_key = queue_key
        self._processing_queue_key = f"{queue_key}:processing"
        self._socket_timeout_seconds = socket_timeout_seconds
        self._client: Any = None

    def open(self) -> ValkeyJobQueue:
        if self._client is not None:
            return self
        try:
            from redis import Redis

            self._client = Redis(
                host=self._host,
                port=self._port,
                db=self._database,
                password=self._password,
                decode_responses=False,
                socket_connect_timeout=self._socket_timeout_seconds,
                socket_timeout=self._socket_timeout_seconds,
                health_check_interval=15,
            )
            if self._client.ping() is not True:
                raise RuntimeError
            self.recover_stale()
        except Exception:
            self.close()
            raise QueueError("Valkey worker queue is unavailable") from None
        return self

    def healthy(self) -> bool:
        try:
            return self._client is not None and self._client.ping() is True
        except Exception:
            return False

    def pop(self, timeout_seconds: int) -> QueueDelivery | None:
        if self._client is None:
            raise QueueError("Valkey worker queue is not open")
        try:
            raw = self._client.blmove(
                self._queue_key,
                self._processing_queue_key,
                timeout_seconds,
                src="RIGHT",
                dest="LEFT",
            )
        except Exception:
            raise QueueError("Valkey worker queue read failed") from None
        if raw is None:
            return None
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_JOB_REFERENCE_BYTES:
            self._discard_poison(raw)
            raise QueueError("Valkey worker queue job reference size is invalid")
        try:
            job = QueuedJobReference.from_json(raw)
        except WorkerContractViolation:
            self._discard_poison(raw)
            raise QueueError("Valkey worker queue job reference is invalid") from None
        return QueueDelivery(job=job, _raw=raw)

    def ack(self, delivery: QueueDelivery) -> None:
        client = self._require_client()
        try:
            removed = client.lrem(self._processing_queue_key, 1, delivery._raw)
        except Exception:
            raise QueueError("Valkey worker queue acknowledgement failed") from None
        if removed != 1:
            raise QueueError("Valkey worker queue delivery is no longer pending")

    def requeue(self, delivery: QueueDelivery) -> None:
        client = self._require_client()
        try:
            moved = client.lmove(
                self._processing_queue_key,
                self._queue_key,
                src="LEFT",
                dest="RIGHT",
            )
        except Exception:
            raise QueueError("Valkey worker queue requeue failed") from None
        if moved != delivery._raw:
            raise QueueError("Valkey worker queue processing order changed")

    def recover_stale(self) -> int:
        client = self._require_client()
        recovered = 0
        try:
            while True:
                moved = client.lmove(
                    self._processing_queue_key,
                    self._queue_key,
                    src="LEFT",
                    dest="RIGHT",
                )
                if moved is None:
                    return recovered
                recovered += 1
        except Exception:
            raise QueueError("Valkey worker queue recovery failed") from None

    def _discard_poison(self, raw: Any) -> None:
        if not isinstance(raw, bytes):
            return
        try:
            self._require_client().lrem(self._processing_queue_key, 1, raw)
        except Exception:
            pass

    def _require_client(self) -> Any:
        if self._client is None:
            raise QueueError("Valkey worker queue is not open")
        return self._client

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def __enter__(self) -> ValkeyJobQueue:
        return self.open()

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "ValkeyJobQueue[host=%s, port=%d, password=REDACTED]" % (
            self._host,
            self._port,
        )
