"""Closed, payload-free Langfuse observations for the Snapshot runtime."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import re
import time
from typing import Any, Callable, Iterator, Mapping

from .config import LangfuseSettings


TRACE_NAME = "axms.job"
NODE_NAME = "axms.node"
MODEL_NAME = "axms.model"
TOOL_NAME = "axms.tool"
CHECK_NAME = "axms.check"
OBSERVATION_NAMES = frozenset(
    {TRACE_NAME, NODE_NAME, MODEL_NAME, TOOL_NAME, CHECK_NAME}
)
ALLOWED_METADATA_KEYS = frozenset(
    {
        "jobId",
        "traceId",
        "profileVersionId",
        "nodeId",
        "nodeType",
        "nodeStatus",
        "attempt",
        "provider",
        "model",
        "inputTokens",
        "outputTokens",
        "latencyMs",
        "errorCode",
        "toolStatus",
        "checkStatus",
        "status",
        "timestamp",
    }
)
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")
_FAILED_PORTS = frozenset({"failed", "blocked"})
_OTEL_RESOURCE_ATTRIBUTES = {"service.name": "axms-coding-orchestrator"}


class _JobScope:
    __slots__ = ("_metadata", "status", "error_code")

    def __init__(self, metadata: Mapping[str, Any]) -> None:
        self._metadata = dict(metadata)
        self.status = "COMPLETED"
        self.error_code: str | None = None

    def finish(self, status: str) -> None:
        self.status = status if status in {
            "COMPLETED",
            "WAITING_APPROVAL",
            "FAILED",
        } else "COMPLETED"


class AxmsObservability:
    """Manual SDK bridge that never forwards application payloads or failures."""

    __slots__ = ("_client", "_tracer_provider", "_current_job")

    def __init__(
        self,
        client: Any | None = None,
        tracer_provider: Any | None = None,
    ) -> None:
        self._client = client
        self._tracer_provider = tracer_provider
        self._current_job: ContextVar[Any | None] = ContextVar(
            "axms_langfuse_job", default=None
        )

    @classmethod
    def from_environment(
        cls,
        source: Mapping[str, str] | None = None,
        *,
        client_factory: Callable[..., Any] | None = None,
    ) -> AxmsObservability:
        settings = LangfuseSettings.from_environment(source)
        if settings is None:
            return cls()
        tracer_provider = None
        try:
            tracer_provider = _closed_tracer_provider()
            if client_factory is None:
                from langfuse import Langfuse

                client_factory = Langfuse
            client = client_factory(
                public_key=settings.public_key,
                secret_key=settings.secret_key,
                base_url=settings.base_url,
                environment="local",
                tracer_provider=tracer_provider,
            )
        except Exception:
            _shutdown_provider(tracer_provider)
            return cls()
        return cls(client, tracer_provider)

    @property
    def enabled(self) -> bool:
        return self._client is not None

    @contextmanager
    def job(
        self,
        *,
        job_id: str,
        trace_id: str,
        profile_version_id: str | None,
        attempt: int,
    ) -> Iterator[_JobScope]:
        metadata = _metadata(
            jobId=job_id,
            traceId=trace_id,
            profileVersionId=profile_version_id,
            attempt=attempt,
            status="RUNNING",
            timestamp=_utc_timestamp(),
        )
        scope = _JobScope(metadata)
        root, root_manager = self._start_root(trace_id, metadata)
        token = self._current_job.set(root)
        started = time.perf_counter()
        try:
            yield scope
        except BaseException as failure:
            scope.status = "FAILED"
            scope.error_code = _safe_error_code(failure)
            raise
        finally:
            self._current_job.reset(token)
            final = {
                **metadata,
                "status": scope.status,
                "latencyMs": _latency_ms(started),
            }
            if scope.error_code is not None:
                final["errorCode"] = scope.error_code
            _finish(root, _metadata(**final))
            _exit_manager(root_manager)

    def invoke_node(
        self,
        *,
        node: Any,
        invocation: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        root = self._current_job.get()
        if root is None:
            return handler(invocation)

        base = _metadata(
            jobId=invocation.job_id,
            traceId=invocation.trace_id,
            profileVersionId=invocation.profile_version_id,
            nodeId=invocation.node_id,
            nodeType=node.node_type,
            nodeStatus="RUNNING",
            attempt=invocation.execution_attempt,
            timestamp=_utc_timestamp(),
        )
        node_observation, node_manager = _start_current_child(
            root, NODE_NAME, "span", base
        )
        detail_name, detail_type = _detail_kind(node.node_type)
        detail = None
        detail_base = dict(base)
        detail_base.pop("nodeStatus", None)
        if detail_name is not None:
            detail = _start_child(
                node_observation or root,
                detail_name,
                detail_type,
                _metadata(**detail_base),
            )

        started = time.perf_counter()
        try:
            result = handler(invocation)
        except BaseException as failure:
            error_code = _safe_error_code(failure)
            elapsed = _latency_ms(started)
            failed_values = {
                **base,
                "nodeStatus": "FAILED",
                "latencyMs": elapsed,
                "errorCode": error_code,
            }
            failed = _metadata(**failed_values)
            detail_failed = {
                **detail_base,
                "status": "FAILED",
                "latencyMs": elapsed,
                "errorCode": error_code,
            }
            _finish(detail, _metadata(**detail_failed))
            _close_current(node_observation, node_manager, failed)
            raise

        elapsed = _latency_ms(started)
        port = getattr(result, "port", None)
        completed_values = {
            **base,
            "nodeStatus": "COMPLETED",
            "latencyMs": elapsed,
        }
        completed = _metadata(**completed_values)
        detail_completed = {**detail_base, "status": "COMPLETED", "latencyMs": elapsed}
        if detail_name == TOOL_NAME:
            detail_completed["toolStatus"] = (
                "FAILED" if port in _FAILED_PORTS else "COMPLETED"
            )
        elif detail_name == CHECK_NAME:
            detail_completed["checkStatus"] = (
                "PASSED" if port == "passed" else "FAILED"
            )
        _finish(detail, _metadata(**detail_completed))
        _close_current(node_observation, node_manager, completed)
        return result

    def close(self) -> None:
        client = self._client
        tracer_provider = self._tracer_provider
        self._client = None
        self._tracer_provider = None
        if client is not None:
            try:
                client.shutdown()
            except Exception:
                pass
        _shutdown_provider(tracer_provider)

    def _start_root(
        self, trace_id: str, metadata: Mapping[str, Any]
    ) -> tuple[Any | None, Any | None]:
        if self._client is None:
            return None, None
        manager = None
        try:
            langfuse_trace_id = self._client.create_trace_id(seed=trace_id)
            manager = self._client.start_as_current_observation(
                name=TRACE_NAME,
                as_type="agent",
                trace_context={"trace_id": langfuse_trace_id},
                metadata=dict(metadata),
                end_on_exit=False,
            )
            return manager.__enter__(), manager
        except Exception:
            _exit_manager(manager)
            return None, None


def _detail_kind(node_type: str) -> tuple[str | None, str]:
    if node_type == "tool":
        return TOOL_NAME, "tool"
    if node_type == "check":
        return CHECK_NAME, "span"
    return None, "span"


def _start_child(
    parent: Any,
    name: str,
    as_type: str,
    metadata: Mapping[str, Any],
) -> Any | None:
    try:
        return parent.start_observation(
            name=name,
            as_type=as_type,
            metadata=dict(metadata),
        )
    except Exception:
        return None


def _start_current_child(
    parent: Any,
    name: str,
    as_type: str,
    metadata: Mapping[str, Any],
) -> tuple[Any | None, Any | None]:
    manager = None
    try:
        manager = parent.start_as_current_observation(
            name=name,
            as_type=as_type,
            metadata=dict(metadata),
        )
        return manager.__enter__(), manager
    except Exception:
        _exit_manager(manager)
        return _start_child(parent, name, as_type, metadata), None


def _close_current(
    observation: Any | None,
    manager: Any | None,
    metadata: Mapping[str, Any],
) -> None:
    _update(observation, metadata)
    if manager is None:
        _end(observation)
    else:
        _exit_manager(manager)


def _finish(observation: Any | None, metadata: Mapping[str, Any]) -> None:
    _update(observation, metadata)
    _end(observation)


def _update(observation: Any | None, metadata: Mapping[str, Any]) -> None:
    if observation is None:
        return
    try:
        observation.update(metadata=dict(metadata))
    except Exception:
        pass


def _end(observation: Any | None) -> None:
    if observation is None:
        return
    try:
        observation.end()
    except Exception:
        pass


def _exit_manager(manager: Any | None) -> None:
    if manager is None:
        return
    try:
        manager.__exit__(None, None, None)
    except Exception:
        pass


def _closed_tracer_provider() -> Any:
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

    return TracerProvider(
        resource=Resource(_OTEL_RESOURCE_ATTRIBUTES),
        shutdown_on_exit=False,
    )


def _shutdown_provider(tracer_provider: Any | None) -> None:
    if tracer_provider is None:
        return
    try:
        tracer_provider.shutdown()
    except Exception:
        pass


def _metadata(**values: Any) -> dict[str, Any]:
    return {
        key: value
        for key, value in values.items()
        if key in ALLOWED_METADATA_KEYS and value is not None
    }


def _safe_error_code(failure: BaseException) -> str:
    code = getattr(failure, "code", None)
    if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
        return code
    return "INTERNAL_TRANSIENT_ERROR"


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _latency_ms(started: float) -> int:
    return max(0, round((time.perf_counter() - started) * 1_000))
