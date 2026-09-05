"""Closed, payload-free Langfuse observations for the Snapshot runtime."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
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
_MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
_PROVIDERS = frozenset(
    {"OPENAI", "ANTHROPIC", "GOOGLE_GENAI", "VERTEX_AI_GEMINI"}
)
_FAILED_PORTS = frozenset({"failed", "blocked"})


@dataclass(frozen=True, slots=True)
class ModelObservation:
    """Payload-free facts from one actual successful Backend model response."""

    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: int


def parse_model_observations(value: Any) -> tuple[ModelObservation, ...]:
    """Keep only closed, bounded model facts; malformed telemetry stays fail-open."""

    if not isinstance(value, list) or len(value) > 100:
        return ()
    parsed: list[ModelObservation] = []
    expected = {
        "provider",
        "modelId",
        "inputTokens",
        "outputTokens",
        "latencyMs",
    }
    for item in value:
        if not isinstance(item, Mapping) or set(item) != expected:
            continue
        provider = item.get("provider")
        model = item.get("modelId")
        counts = (
            item.get("inputTokens"),
            item.get("outputTokens"),
            item.get("latencyMs"),
        )
        if (
            provider not in _PROVIDERS
            or not isinstance(model, str)
            or _MODEL_ID.fullmatch(model) is None
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                for count in counts
            )
        ):
            continue
        parsed.append(ModelObservation(provider, model, *counts))
    return tuple(parsed)


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

    __slots__ = ("_client", "_current_job", "_current_node")

    def __init__(self, client: Any | None = None) -> None:
        self._client = client
        self._current_job: ContextVar[Any | None] = ContextVar(
            "axms_langfuse_job", default=None
        )
        self._current_node: ContextVar[Any | None] = ContextVar(
            "axms_langfuse_node", default=None
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
        try:
            if client_factory is None:
                from langfuse import Langfuse

                client_factory = Langfuse
            client = client_factory(
                public_key=settings.public_key,
                secret_key=settings.secret_key,
                base_url=settings.base_url,
                environment="local",
            )
        except Exception:
            return cls()
        return cls(client)

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
        root = self._start_root(trace_id, metadata)
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
        node_observation = _start_child(root, NODE_NAME, "span", base)
        node_token = self._current_node.set(node_observation or root)
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
            _finish(node_observation, failed)
            raise
        finally:
            self._current_node.reset(node_token)

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
        _finish(node_observation, completed)
        return result

    def record_models(self, observations: tuple[ModelObservation, ...]) -> None:
        """Emit actual Backend model facts under the active Node, if one exists."""

        parent = self._current_node.get()
        if parent is None:
            return
        for observation in observations:
            if not isinstance(observation, ModelObservation):
                continue
            metadata = _metadata(
                provider=observation.provider,
                model=observation.model,
                inputTokens=observation.input_tokens,
                outputTokens=observation.output_tokens,
                latencyMs=observation.latency_ms,
            )
            _finish(
                _start_model(parent, observation, metadata),
                metadata,
            )

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.shutdown()
        except Exception:
            pass

    def _start_root(self, trace_id: str, metadata: Mapping[str, Any]) -> Any | None:
        if self._client is None:
            return None
        try:
            langfuse_trace_id = self._client.create_trace_id(seed=trace_id)
            return self._client.start_observation(
                name=TRACE_NAME,
                as_type="agent",
                trace_context={"trace_id": langfuse_trace_id},
                metadata=dict(metadata),
            )
        except Exception:
            return None


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


def _start_model(
    parent: Any,
    observation: ModelObservation,
    metadata: Mapping[str, Any],
) -> Any | None:
    try:
        return parent.start_observation(
            name=MODEL_NAME,
            as_type="generation",
            metadata=dict(metadata),
            model=observation.model,
            usage_details={
                "input": observation.input_tokens,
                "output": observation.output_tokens,
            },
        )
    except Exception:
        return None


def _finish(observation: Any | None, metadata: Mapping[str, Any]) -> None:
    if observation is None:
        return
    try:
        observation.update(metadata=dict(metadata))
    except Exception:
        pass
    try:
        observation.end()
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
