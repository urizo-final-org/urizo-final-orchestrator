"""Common immutable node invocation/result contracts and source registry."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Protocol
from uuid import UUID

from .snapshot import ALLOWED_NODE_TYPES, HANDLER_KEY, NODE_IDENTIFIER, RESULT_PORT


MAX_JSON_DEPTH = 64


class NodeContractViolation(ValueError):
    """Payload-free node invocation or result contract failure."""


class NodeRegistryViolation(ValueError):
    """Source registry definition or lookup failure."""


class _FactoryOnly:
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise TypeError("node contracts must be created through their factory methods")


JsonValue = None | bool | int | float | str | tuple["JsonValue", ...] | Mapping[str, "JsonValue"]


@dataclass(frozen=True, slots=True, init=False)
class NodeInvocation(_FactoryOnly):
    job_id: str
    profile_version_id: str
    node_id: str
    pipeline_attempt: int
    execution_attempt: int
    state_version: int
    trace_id: str
    workspace_id: str | None
    tool_call_id: str | None
    _context: Mapping[str, JsonValue] = field(repr=False)
    _config: Mapping[str, JsonValue] = field(repr=False)

    @classmethod
    def create(
        cls,
        *,
        job_id: str,
        profile_version_id: str,
        node_id: str,
        pipeline_attempt: int,
        execution_attempt: int,
        state_version: int,
        trace_id: str,
        workspace_id: str | None,
        tool_call_id: str | None,
        context: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> NodeInvocation:
        result = object.__new__(cls)
        object.__setattr__(result, "job_id", _uuid(job_id, "invocation.jobId"))
        object.__setattr__(
            result,
            "profile_version_id",
            _uuid(profile_version_id, "invocation.profileVersionId"),
        )
        object.__setattr__(
            result,
            "node_id",
            _matched(node_id, NODE_IDENTIFIER, "invocation.nodeId", 64),
        )
        object.__setattr__(
            result,
            "pipeline_attempt",
            _positive_integer(pipeline_attempt, "invocation.pipelineAttempt"),
        )
        object.__setattr__(
            result,
            "execution_attempt",
            _positive_integer(execution_attempt, "invocation.executionAttempt"),
        )
        object.__setattr__(
            result,
            "state_version",
            _positive_integer(state_version, "invocation.stateVersion"),
        )
        object.__setattr__(result, "trace_id", _uuid(trace_id, "invocation.traceId"))
        object.__setattr__(
            result,
            "workspace_id",
            _optional_uuid(workspace_id, "invocation.workspaceId"),
        )
        object.__setattr__(
            result,
            "tool_call_id",
            _optional_uuid(tool_call_id, "invocation.toolCallId"),
        )
        object.__setattr__(result, "_context", _freeze_object(context, "invocation.context"))
        object.__setattr__(result, "_config", _freeze_object(config, "invocation.config"))
        return result

    @property
    def context(self) -> dict[str, Any]:
        return _thaw_object(self._context)

    @property
    def config(self) -> dict[str, Any]:
        return _thaw_object(self._config)

    def __repr__(self) -> str:
        return (
            "NodeInvocation[jobId=%s, profileVersionId=%s, nodeId=%s, "
            "pipelineAttempt=%d, executionAttempt=%d, stateVersion=%d, "
            "traceId=%s, workspaceId=%s, toolCallId=%s, context=REDACTED]"
            % (
                self.job_id,
                self.profile_version_id,
                self.node_id,
                self.pipeline_attempt,
                self.execution_attempt,
                self.state_version,
                self.trace_id,
                self.workspace_id,
                self.tool_call_id,
            )
        )


@dataclass(frozen=True, slots=True, init=False)
class NodeResult(_FactoryOnly):
    port: str | None
    _updates: Mapping[str, JsonValue] = field(repr=False)

    @classmethod
    def create(
        cls,
        port: str | None,
        updates: Mapping[str, Any] | None = None,
    ) -> NodeResult:
        if port is not None:
            _matched(port, RESULT_PORT, "result.port", 64)
        result = object.__new__(cls)
        object.__setattr__(result, "port", port)
        object.__setattr__(
            result,
            "_updates",
            _freeze_object({} if updates is None else updates, "result.updates"),
        )
        return result

    @property
    def updates(self) -> dict[str, Any]:
        return _thaw_object(self._updates)

    def __repr__(self) -> str:
        return "NodeResult[port=%s, updates=REDACTED]" % (
            self.port if self.port is not None else "TERMINAL"
        )


class NodeHandler(Protocol):
    def __call__(self, invocation: NodeInvocation) -> NodeResult: ...


class NodeConfigValidator(Protocol):
    def __call__(self, config: Mapping[str, Any]) -> str | None: ...


@dataclass(frozen=True, slots=True, init=False)
class NodeHandlerRegistration(_FactoryOnly):
    handler_key: str
    node_types: frozenset[str]
    result_ports: frozenset[str]
    handler: NodeHandler = field(repr=False, compare=False)
    config_validator: NodeConfigValidator = field(repr=False, compare=False)


class NodeRegistry:
    """In-memory registry populated by source code, never by Snapshot JSON."""

    __slots__ = ("_registrations",)

    def __init__(self) -> None:
        self._registrations: dict[str, NodeHandlerRegistration] = {}

    def register(
        self,
        handler_key: str,
        *,
        node_types: Iterable[str],
        result_ports: Iterable[str],
        handler: NodeHandler,
        config_validator: NodeConfigValidator | None = None,
    ) -> NodeRegistry:
        try:
            key = _matched(handler_key, HANDLER_KEY, "registry.handlerKey", 128)
        except NodeContractViolation:
            raise NodeRegistryViolation("registry.handlerKey is invalid") from None
        if key in self._registrations:
            raise NodeRegistryViolation("registry contains a duplicate handlerKey")
        types = _string_set(node_types, "registry.nodeTypes", ALLOWED_NODE_TYPES, NODE_IDENTIFIER)
        ports = _string_set(result_ports, "registry.resultPorts", None, RESULT_PORT)
        if not callable(handler):
            raise NodeRegistryViolation("registry handler must be callable")
        validator = _accept_any_config if config_validator is None else config_validator
        if not callable(validator):
            raise NodeRegistryViolation("registry config validator must be callable")
        registration = object.__new__(NodeHandlerRegistration)
        object.__setattr__(registration, "handler_key", key)
        object.__setattr__(registration, "node_types", types)
        object.__setattr__(registration, "result_ports", ports)
        object.__setattr__(registration, "handler", handler)
        object.__setattr__(registration, "config_validator", validator)
        self._registrations[key] = registration
        return self

    def resolve(self, handler_key: str) -> NodeHandlerRegistration:
        try:
            key = _matched(handler_key, HANDLER_KEY, "registry.handlerKey", 128)
        except NodeContractViolation:
            raise NodeRegistryViolation("registry.handlerKey is invalid") from None
        try:
            return self._registrations[key]
        except KeyError:
            raise NodeRegistryViolation("snapshot references an unregistered handlerKey") from None

    @property
    def registered_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._registrations))

    def __repr__(self) -> str:
        return "NodeRegistry[handlers=%d]" % len(self._registrations)


def _string_set(
    value: Iterable[str],
    field_name: str,
    allowed: frozenset[str] | None,
    pattern: Any,
) -> frozenset[str]:
    if isinstance(value, (str, bytes)):
        raise NodeRegistryViolation(f"{field_name} must be an iterable of strings")
    try:
        items = tuple(value)
    except TypeError:
        raise NodeRegistryViolation(f"{field_name} must be an iterable of strings") from None
    if field_name == "registry.nodeTypes" and not items:
        raise NodeRegistryViolation("registry.nodeTypes must not be empty")
    results: list[str] = []
    for item in items:
        try:
            result = _matched(item, pattern, field_name, 64)
        except NodeContractViolation:
            raise NodeRegistryViolation(f"{field_name} is invalid") from None
        if allowed is not None and result not in allowed:
            raise NodeRegistryViolation(f"{field_name} is unsupported")
        results.append(result)
    if len(results) != len(set(results)):
        raise NodeRegistryViolation(f"{field_name} contains duplicates")
    return frozenset(results)


def _accept_any_config(config: Mapping[str, Any]) -> str | None:
    del config
    return None


def _freeze_object(value: Any, field_name: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise NodeContractViolation(f"{field_name} must be an object")
    if any(_contains_surrogate(key) for key in value):
        raise NodeContractViolation(f"{field_name} contains invalid Unicode")
    return MappingProxyType(
        {key: _freeze_json(item, f"{field_name} value") for key, item in value.items()}
    )


def _freeze_json(value: Any, field_name: str, depth: int = 0) -> JsonValue:
    if depth > MAX_JSON_DEPTH:
        raise NodeContractViolation(f"{field_name} exceeds the JSON depth limit")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        if _contains_surrogate(value):
            raise NodeContractViolation(f"{field_name} contains invalid Unicode")
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NodeContractViolation(f"{field_name} is not JSON-safe")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value) or any(
            _contains_surrogate(key) for key in value
        ):
            raise NodeContractViolation(f"{field_name} is not JSON-safe")
        return MappingProxyType(
            {
                key: _freeze_json(item, field_name, depth + 1)
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item, field_name, depth + 1) for item in value)
    raise NodeContractViolation(f"{field_name} is not JSON-safe")


def _thaw_object(value: Mapping[str, JsonValue]) -> dict[str, Any]:
    return {key: _thaw_json(item) for key, item in value.items()}


def _thaw_json(value: JsonValue) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise NodeContractViolation(f"{field_name} is invalid")
    return value


def _matched(value: Any, pattern: Any, field_name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or _contains_surrogate(value)
        or not 1 <= len(value) <= maximum
        or not pattern.fullmatch(value)
    ):
        raise NodeContractViolation(f"{field_name} is invalid")
    return value


def _uuid(value: Any, field_name: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise NodeContractViolation(f"{field_name} is invalid") from None
    return value


def _optional_uuid(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _uuid(value, field_name)


def _contains_surrogate(value: str) -> bool:
    return any(0xD800 <= ord(character) <= 0xDFFF for character in value)
