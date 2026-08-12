"""Bounded lease heartbeat with fail-closed lease-loss signalling."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import threading
from typing import Callable

from .contracts import WorkerClaim
from .worker_api import WorkerApiClient, WorkerApiError


class LeaseLostError(RuntimeError):
    def __init__(self, code: str = "JOB_STATE_VERSION_CONFLICT") -> None:
        super().__init__("Spring coding worker lease is no longer current.")
        self.code = code
        self.retryable = False


@dataclass(slots=True)
class _LeaseRecord:
    claim: WorkerClaim
    expires_at: datetime
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    lost_code: str | None = None


class LeaseHeartbeatManager:
    def __init__(
        self,
        worker_api: WorkerApiClient,
        interval_seconds: float,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._worker_api = worker_api
        self._interval_seconds = interval_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._records: dict[str, _LeaseRecord] = {}
        self._lock = threading.Lock()

    def start(self, claim: WorkerClaim) -> None:
        self.stop(claim.job_id)
        expires = datetime.fromisoformat(
            claim.to_dict()["leaseExpiresAt"].replace("Z", "+00:00")
        )
        record = _LeaseRecord(claim=claim, expires_at=expires)
        thread = threading.Thread(
            target=self._run,
            args=(record,),
            name="axms-heartbeat-" + claim.job_id[:8],
            daemon=True,
        )
        record.thread = thread
        with self._lock:
            self._records[claim.job_id] = record
        thread.start()

    def ensure_current(self, claim: WorkerClaim) -> None:
        with self._lock:
            record = self._records.get(claim.job_id)
            if record is None or record.claim.lease_id != claim.lease_id:
                raise LeaseLostError()
            lost_code = record.lost_code
            expired = record.expires_at <= self._now()
        if lost_code is not None or expired:
            raise LeaseLostError(lost_code or "JOB_STATE_VERSION_CONFLICT")

    def stop(self, job_id: str) -> None:
        with self._lock:
            record = self._records.pop(job_id, None)
        if record is None:
            return
        record.stop_event.set()
        if record.thread is not None and record.thread is not threading.current_thread():
            record.thread.join(timeout=max(1.0, self._interval_seconds * 2.0))

    def close(self) -> None:
        with self._lock:
            job_ids = list(self._records)
        for job_id in job_ids:
            self.stop(job_id)

    def _run(self, record: _LeaseRecord) -> None:
        sequence = 0
        while not record.stop_event.wait(self._interval_seconds):
            sequence += 1
            identity = "%s|%s|%d|%d" % (
                record.claim.job_id,
                record.claim.lease_id,
                record.claim.state_version,
                sequence,
            )
            key = "heartbeat." + hashlib.sha256(identity.encode("utf-8")).hexdigest()
            try:
                response = self._worker_api.heartbeat(record.claim, key)
                expires = datetime.fromisoformat(
                    response["leaseExpiresAt"].replace("Z", "+00:00")
                )
                with self._lock:
                    current = self._records.get(record.claim.job_id)
                    if current is record:
                        record.expires_at = expires
            except WorkerApiError as failure:
                with self._lock:
                    current = self._records.get(record.claim.job_id)
                    if current is not record:
                        return
                    if not failure.retryable or record.expires_at <= self._now():
                        record.lost_code = failure.code
                        return
            except Exception:
                with self._lock:
                    current = self._records.get(record.claim.job_id)
                    if current is record and record.expires_at <= self._now():
                        record.lost_code = "INTERNAL_TRANSIENT_ERROR"
                        return
