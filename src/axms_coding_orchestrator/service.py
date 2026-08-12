"""Long-running local/full-profile coding runtime service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import signal
import threading
import time
from typing import Any, Callable

from .checkpoint import CheckpointError, CheckpointRuntime
from .config import ConfigurationError, RuntimeSettings
from .contracts import CodingJobRequested, WorkerClaim
from .graph import (
    CodingGraphRunner,
    GraphDependencies,
    GraphExecutionError,
    build_coding_graph,
)
from .heartbeat import LeaseHeartbeatManager, LeaseLostError
from .model_gateway import (
    FileServiceCredentialResolver,
    MODEL_TURN_PATH,
    ModelGatewayClient,
    ModelGatewayRemoteError,
)
from .queue import QueueError, ValkeyJobQueue
from .tool_gateway import ToolGatewayClient, ToolGatewayError
from .worker_api import WorkerApiClient, WorkerApiError


@dataclass(slots=True)
class _Status:
    live: bool = True
    checkpoint: bool = False
    queue: bool = False
    spring: bool = False
    worker: bool = False
    last_error_code: str | None = None


class HealthState:
    def __init__(self) -> None:
        self._status = _Status()
        self._lock = threading.Lock()
        self._checkpoint_probe: Callable[[], bool] | None = None
        self._queue_probe: Callable[[], bool] | None = None
        self._spring_probe: Callable[[], bool] | None = None

    def bind_dependency_probes(
        self,
        *,
        checkpoint: Callable[[], bool],
        queue: Callable[[], bool],
        spring: Callable[[], bool],
    ) -> None:
        with self._lock:
            self._checkpoint_probe = checkpoint
            self._queue_probe = queue
            self._spring_probe = spring

    def update(self, **changes: Any) -> None:
        with self._lock:
            for name, value in changes.items():
                setattr(self._status, name, value)

    def response(self, ready: bool) -> tuple[int, bytes]:
        if ready:
            with self._lock:
                probes = (
                    self._checkpoint_probe,
                    self._queue_probe,
                    self._spring_probe,
                )
            results = tuple(_safe_probe(probe) for probe in probes)
            self.update(
                checkpoint=results[0],
                queue=results[1],
                spring=results[2],
            )
        with self._lock:
            status = _Status(
                live=self._status.live,
                checkpoint=self._status.checkpoint,
                queue=self._status.queue,
                spring=self._status.spring,
                worker=self._status.worker,
                last_error_code=self._status.last_error_code,
            )
        is_ready = (
            status.live
            and status.checkpoint
            and status.queue
            and status.spring
            and status.worker
        )
        success = is_ready if ready else status.live
        payload = {
            "status": "UP" if success else "DOWN",
            "checks": {
                "checkpoint": "UP" if status.checkpoint else "DOWN",
                "queue": "UP" if status.queue else "DOWN",
                "spring": "UP" if status.spring else "DOWN",
                "worker": "UP" if status.worker else "DOWN",
            },
            "errorCode": status.last_error_code,
        }
        return (200 if success else 503), json.dumps(
            payload, separators=(",", ":")
        ).encode("utf-8")


def _safe_probe(probe: Callable[[], bool] | None) -> bool:
    if probe is None:
        return False
    try:
        return probe() is True
    except Exception:
        return False


class _HealthHandler(BaseHTTPRequestHandler):
    state: HealthState

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        if self.path == "/health/live":
            status, body = type(self).state.response(False)
        elif self.path == "/health/ready":
            status, body = type(self).state.response(True)
        else:
            status, body = 404, b'{"status":"NOT_FOUND"}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *args: object) -> None:
        pass


class HealthServer:
    def __init__(self, host: str, port: int, state: HealthState) -> None:
        handler = type("AxmsHealthHandler", (_HealthHandler,), {"state": state})
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="axms-health", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


class WorkerLoop:
    def __init__(
        self,
        queue: ValkeyJobQueue,
        worker_api: WorkerApiClient,
        graph: CodingGraphRunner,
        heartbeat: LeaseHeartbeatManager,
        health: HealthState,
        *,
        queue_block_seconds: int,
        max_attempts: int,
        max_backoff_seconds: int,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._queue = queue
        self._worker_api = worker_api
        self._graph = graph
        self._heartbeat = heartbeat
        self._health = health
        self._queue_block_seconds = queue_block_seconds
        self._max_attempts = max_attempts
        self._max_backoff_seconds = max_backoff_seconds
        self._sleep = sleeper or time.sleep
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        self._health.update(worker=True)
        while not self._stop.is_set():
            try:
                delivery = self._queue.pop(self._queue_block_seconds)
                self._health.update(queue=True)
            except QueueError:
                self._health.update(queue=False, last_error_code="QUEUE_UNAVAILABLE")
                self._stop.wait(min(2, self._queue_block_seconds))
                try:
                    self._queue.recover_stale()
                    self._health.update(queue=True)
                except QueueError:
                    pass
                continue
            if delivery is None:
                continue
            acknowledged = self.process(delivery.event)
            try:
                if acknowledged:
                    self._queue.ack(delivery)
                else:
                    self._queue.requeue(delivery)
                    self._stop.wait(min(2, self._max_backoff_seconds))
            except QueueError:
                self._health.update(queue=False, last_error_code="QUEUE_UNAVAILABLE")
                self._stop.wait(min(2, self._queue_block_seconds))
                try:
                    self._queue.recover_stale()
                    self._health.update(queue=True)
                except QueueError:
                    pass

    def process(self, event: CodingJobRequested) -> bool:
        if self._graph.is_duplicate(event):
            return True
        claim: WorkerClaim | None = None
        try:
            claim = self._claim_with_backoff(event)
            self._heartbeat.start(claim)
            self._graph.invoke(event, claim)
            self._health.update(last_error_code=None)
            return True
        except Exception as failure:
            retryable, code = _classify_failure(failure)
            self._health.update(last_error_code=code)
            if claim is not None:
                return self._report_failure(event, claim, retryable, code)
            return False
        finally:
            if claim is not None:
                self._heartbeat.stop(claim.job_id)

    def _claim_with_backoff(self, event: CodingJobRequested) -> WorkerClaim:
        last: WorkerApiError | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return self._worker_api.claim(event)
            except WorkerApiError as failure:
                last = failure
                if not failure.retryable or attempt == self._max_attempts:
                    raise
                delay = min(2 ** (attempt - 1), self._max_backoff_seconds)
                self._sleep(delay)
        assert last is not None
        raise last

    def _report_failure(
        self,
        event: CodingJobRequested,
        claim: WorkerClaim,
        retryable: bool,
        code: str,
    ) -> bool:
        try:
            self._heartbeat.ensure_current(claim)
        except LeaseLostError:
            return False
        outcome = (
            "RETRYABLE_FAILURE"
            if retryable and event.attempt < self._max_attempts
            else "PERMANENT_FAILURE"
        )
        identity = "%s|%s|%d|%s" % (
            claim.job_id,
            claim.lease_id,
            claim.state_version,
            outcome,
        )
        key = "outcome." + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        try:
            self._worker_api.outcome(claim, outcome, key, error_code=code)
            return True
        except WorkerApiError:
            return False


def _classify_failure(failure: Exception) -> tuple[bool, str]:
    if isinstance(
        failure,
        (
            GraphExecutionError,
            ModelGatewayRemoteError,
            ToolGatewayError,
            WorkerApiError,
            LeaseLostError,
        ),
    ):
        code = getattr(failure, "code", "INTERNAL_TRANSIENT_ERROR")
        retryable = bool(getattr(failure, "retryable", False))
        return retryable, code
    return True, "INTERNAL_TRANSIENT_ERROR"


def main() -> None:
    health = HealthState()
    server: HealthServer | None = None
    checkpoint: CheckpointRuntime | None = None
    queue: ValkeyJobQueue | None = None
    heartbeat: LeaseHeartbeatManager | None = None
    loop: WorkerLoop | None = None
    try:
        settings = RuntimeSettings.from_environment()
        server = HealthServer(settings.health_host, settings.health_port, health)
        server.start()
        checkpoint = CheckpointRuntime(
            settings.checkpoint_dsn(), settings.checkpoint_encryption_key()
        ).open()
        health.update(checkpoint=checkpoint.healthy())
        queue = ValkeyJobQueue(
            settings.valkey_host,
            settings.valkey_port,
            settings.valkey_database,
            password=settings.valkey_password(),
            queue_key=settings.queue_key,
        ).open()
        health.update(queue=queue.healthy())
        credential_resolver = FileServiceCredentialResolver(
            settings.spring_credential_file
        )
        worker_api = WorkerApiClient(settings.spring_origin, credential_resolver)
        health.bind_dependency_probes(
            checkpoint=checkpoint.healthy,
            queue=queue.healthy,
            spring=worker_api.healthy,
        )
        heartbeat = LeaseHeartbeatManager(
            worker_api, settings.heartbeat_seconds
        )
        model_gateway = ModelGatewayClient(
            settings.spring_origin + MODEL_TURN_PATH,
            credential_resolver,
        )
        tool_gateway = ToolGatewayClient(settings.spring_origin, credential_resolver)
        graph = CodingGraphRunner(
            build_coding_graph(
                checkpoint.checkpointer,
                GraphDependencies(
                    model_gateway=model_gateway,
                    tool_gateway=tool_gateway,
                    worker_api=worker_api,
                    lease_guard=heartbeat,
                ),
            )
        )
        loop = WorkerLoop(
            queue,
            worker_api,
            graph,
            heartbeat,
            health,
            queue_block_seconds=settings.queue_block_seconds,
            max_attempts=settings.max_attempts,
            max_backoff_seconds=settings.max_backoff_seconds,
        )

        def stop_handler(_signal: int, _frame: object) -> None:
            if loop is not None:
                loop.stop()

        signal.signal(signal.SIGTERM, stop_handler)
        signal.signal(signal.SIGINT, stop_handler)
        loop.run()
    except (ConfigurationError, CheckpointError, QueueError):
        health.update(live=False, worker=False, last_error_code="STARTUP_FAILED")
        raise SystemExit(1) from None
    finally:
        health.update(worker=False)
        if heartbeat is not None:
            heartbeat.close()
        if queue is not None:
            queue.close()
        if checkpoint is not None:
            checkpoint.close()
        if server is not None:
            server.close()


if __name__ == "__main__":
    main()
