"""Immutable Versioned Profile Snapshot contracts.

This module is deliberately separate from ``contracts.ClaimSnapshot``.  The
claim snapshot is the existing Coding worker authorization payload, while the
models below describe the versioned graph definition that a future provider
will resolve from Spring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID


SNAPSHOT_CONTRACT_VERSION = "1.0"
PROFILE_KEYS = frozenset({"LLM_OPS", "NATURAL_CMS"})
ALLOWED_NODE_TYPES = frozenset(
    {"start", "agent", "tool", "approval", "check", "guardrail", "end"}
)
EXECUTION_CONTEXT_FIELDS = frozenset(
    {
        "jobId",
        "pipelineAttempt",
        "executionAttempt",
        "stateVersion",
        "workspaceId",
        "toolCallId",
        "traceId",
    }
)
MAX_JSON_DEPTH = 64
NODE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
NODE_TYPE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
HANDLER_KEY = re.compile(r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)*$")
RESULT_PORT = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
PROFILE_KEY = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
GUARDRAIL_PROFILE_KEY = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")


class SnapshotContractViolation(ValueError):
    """Payload-free Versioned Snapshot validation failure."""


class _FactoryOnly:
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise TypeError("snapshot models must be created through from_dict or from_json")


JsonValue = None | bool | int | float | str | tuple["JsonValue", ...] | Mapping[str, "JsonValue"]


@dataclass(frozen=True, slots=True, init=False)
class SnapshotNode(_FactoryOnly):
    node_id: str
    node_type: str
    handler_key: str
    result_ports: tuple[str, ...]
    _config: Mapping[str, JsonValue] = field(repr=False)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SnapshotNode:
        payload = _object(value, "node")
        _exact_fields(payload, {"id", "type", "handlerKey", "resultPorts", "config"}, "node")
        node_id = _matched(payload["id"], NODE_IDENTIFIER, "node.id", 64)
        node_type = _matched(payload["type"], NODE_TYPE, "node.type", 64)
        if node_type not in ALLOWED_NODE_TYPES:
            raise SnapshotContractViolation("node.type is unsupported")
        handler_key = _matched(payload["handlerKey"], HANDLER_KEY, "node.handlerKey", 128)
        result_ports = tuple(
            _matched(port, RESULT_PORT, f"node.resultPorts[{index}]", 64)
            for index, port in enumerate(_list(payload["resultPorts"], "node.resultPorts"))
        )
        if len(result_ports) != len(set(result_ports)):
            raise SnapshotContractViolation("node.resultPorts contains duplicates")
        config = _freeze_object(payload["config"], "node.config")
        result = object.__new__(cls)
        object.__setattr__(result, "node_id", node_id)
        object.__setattr__(result, "node_type", node_type)
        object.__setattr__(result, "handler_key", handler_key)
        object.__setattr__(result, "result_ports", result_ports)
        object.__setattr__(result, "_config", config)
        return result

    @property
    def config(self) -> dict[str, Any]:
        return _thaw_object(self._config)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "type": self.node_type,
            "handlerKey": self.handler_key,
            "resultPorts": list(self.result_ports),
            "config": self.config,
        }


@dataclass(frozen=True, slots=True, init=False)
class SnapshotEdge(_FactoryOnly):
    source: str
    result_port: str
    target: str

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SnapshotEdge:
        payload = _object(value, "edge")
        _exact_fields(payload, {"from", "resultPort", "to"}, "edge")
        result = object.__new__(cls)
        object.__setattr__(
            result, "source", _matched(payload["from"], NODE_IDENTIFIER, "edge.from", 64)
        )
        object.__setattr__(
            result,
            "result_port",
            _matched(payload["resultPort"], RESULT_PORT, "edge.resultPort", 64),
        )
        object.__setattr__(
            result, "target", _matched(payload["to"], NODE_IDENTIFIER, "edge.to", 64)
        )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {"from": self.source, "resultPort": self.result_port, "to": self.target}


@dataclass(frozen=True, slots=True, init=False)
class SnapshotLoopLimit(_FactoryOnly):
    source: str
    result_port: str
    target: str
    max_iterations: int

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SnapshotLoopLimit:
        payload = _object(value, "config.loopLimits[]")
        _exact_fields(
            payload,
            {"from", "resultPort", "to", "maxIterations"},
            "config.loopLimits[]",
        )
        result = object.__new__(cls)
        object.__setattr__(
            result,
            "source",
            _matched(payload["from"], NODE_IDENTIFIER, "config.loopLimits[].from", 64),
        )
        object.__setattr__(
            result,
            "result_port",
            _matched(
                payload["resultPort"],
                RESULT_PORT,
                "config.loopLimits[].resultPort",
                64,
            ),
        )
        object.__setattr__(
            result,
            "target",
            _matched(payload["to"], NODE_IDENTIFIER, "config.loopLimits[].to", 64),
        )
        object.__setattr__(
            result,
            "max_iterations",
            _positive_integer(
                payload["maxIterations"], "config.loopLimits[].maxIterations"
            ),
        )
        return result

    def route(self) -> tuple[str, str, str]:
        return self.source, self.result_port, self.target

    def to_dict(self) -> dict[str, Any]:
        return {
            "from": self.source,
            "resultPort": self.result_port,
            "to": self.target,
            "maxIterations": self.max_iterations,
        }


@dataclass(frozen=True, slots=True, init=False)
class SnapshotConfig(_FactoryOnly):
    max_nodes: int
    max_attempts: int
    loop_limits: tuple[SnapshotLoopLimit, ...]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SnapshotConfig:
        payload = _object(value, "config")
        _exact_fields(payload, {"maxNodes", "maxAttempts", "loopLimits"}, "config")
        limits = tuple(
            SnapshotLoopLimit.from_dict(limit)
            for limit in _list(payload["loopLimits"], "config.loopLimits")
        )
        routes = [limit.route() for limit in limits]
        if len(routes) != len(set(routes)):
            raise SnapshotContractViolation("config.loopLimits contains duplicate routes")
        result = object.__new__(cls)
        object.__setattr__(
            result, "max_nodes", _positive_integer(payload["maxNodes"], "config.maxNodes")
        )
        object.__setattr__(
            result,
            "max_attempts",
            _positive_integer(payload["maxAttempts"], "config.maxAttempts"),
        )
        object.__setattr__(result, "loop_limits", limits)
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "maxNodes": self.max_nodes,
            "maxAttempts": self.max_attempts,
            "loopLimits": [limit.to_dict() for limit in self.loop_limits],
        }


@dataclass(frozen=True, slots=True, init=False)
class VersionedSnapshot(_FactoryOnly):
    contract_version: str
    profile_version_id: str
    profile_key: str
    profile_version: int
    nodes: tuple[SnapshotNode, ...]
    edges: tuple[SnapshotEdge, ...]
    config: SnapshotConfig
    _model_bindings: Mapping[str, JsonValue] = field(repr=False)
    _tool_policy: Mapping[str, JsonValue] = field(repr=False)
    guardrail_profile_key: str

    @classmethod
    def from_json(cls, raw: bytes | str) -> VersionedSnapshot:
        return cls.from_dict(_decode_json(raw))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> VersionedSnapshot:
        payload = _object(value, "snapshot")
        _exact_fields(
            payload,
            {
                "contractVersion",
                "profileVersionId",
                "profileKey",
                "profileVersion",
                "nodes",
                "edges",
                "config",
                "modelBindings",
                "toolPolicy",
                "guardrailProfileKey",
            },
            "snapshot",
        )
        if payload["contractVersion"] != SNAPSHOT_CONTRACT_VERSION:
            raise SnapshotContractViolation("snapshot.contractVersion is unsupported")
        profile_version_id = _uuid(payload["profileVersionId"], "snapshot.profileVersionId")
        profile_key = _matched(payload["profileKey"], PROFILE_KEY, "snapshot.profileKey", 64)
        if profile_key not in PROFILE_KEYS:
            raise SnapshotContractViolation("snapshot.profileKey is unsupported")
        nodes = tuple(
            SnapshotNode.from_dict(node)
            for node in _nonempty_list(payload["nodes"], "snapshot.nodes")
        )
        edges = tuple(
            SnapshotEdge.from_dict(edge)
            for edge in _list(payload["edges"], "snapshot.edges")
        )
        model_bindings = _freeze_model_bindings(payload["modelBindings"])
        tool_policy = _freeze_tool_policy(payload["toolPolicy"])
        snapshot = object.__new__(cls)
        object.__setattr__(snapshot, "contract_version", SNAPSHOT_CONTRACT_VERSION)
        object.__setattr__(snapshot, "profile_version_id", profile_version_id)
        object.__setattr__(snapshot, "profile_key", profile_key)
        object.__setattr__(
            snapshot,
            "profile_version",
            _positive_integer(payload["profileVersion"], "snapshot.profileVersion"),
        )
        object.__setattr__(snapshot, "nodes", nodes)
        object.__setattr__(snapshot, "edges", edges)
        object.__setattr__(snapshot, "config", SnapshotConfig.from_dict(payload["config"]))
        object.__setattr__(snapshot, "_model_bindings", model_bindings)
        object.__setattr__(snapshot, "_tool_policy", tool_policy)
        object.__setattr__(
            snapshot,
            "guardrail_profile_key",
            _matched(
                payload["guardrailProfileKey"],
                GUARDRAIL_PROFILE_KEY,
                "snapshot.guardrailProfileKey",
                128,
            ),
        )
        validate_snapshot(snapshot)
        return snapshot

    @property
    def model_bindings(self) -> dict[str, Any]:
        return _thaw_object(self._model_bindings)

    @property
    def tool_policy(self) -> dict[str, Any]:
        return _thaw_object(self._tool_policy)

    def to_dict(self) -> dict[str, Any]:
        return {
            "contractVersion": self.contract_version,
            "profileVersionId": self.profile_version_id,
            "profileKey": self.profile_key,
            "profileVersion": self.profile_version,
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
            "config": self.config.to_dict(),
            "modelBindings": self.model_bindings,
            "toolPolicy": self.tool_policy,
            "guardrailProfileKey": self.guardrail_profile_key,
        }

    def to_json(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def __repr__(self) -> str:
        return (
            "VersionedSnapshot[profileVersionId=%s, profileKey=%s, profileVersion=%d, "
            "nodes=%d, edges=%d]"
            % (
                self.profile_version_id,
                self.profile_key,
                self.profile_version,
                len(self.nodes),
                len(self.edges),
            )
        )


def load_snapshot_json(raw: bytes | str) -> VersionedSnapshot:
    """Decode and validate one Versioned Snapshot JSON document."""

    return VersionedSnapshot.from_json(raw)


def validate_snapshot(snapshot: VersionedSnapshot) -> None:
    """Validate schema-local invariants without consulting a Handler Registry."""

    node_ids = [node.node_id for node in snapshot.nodes]
    if len(node_ids) != len(set(node_ids)):
        raise SnapshotContractViolation("snapshot.nodes contains duplicate ids")
    nodes = {node.node_id: node for node in snapshot.nodes}
    routes = [(edge.source, edge.result_port) for edge in snapshot.edges]
    if len(routes) != len(set(routes)):
        raise SnapshotContractViolation("snapshot.edges contains duplicate result routes")
    if len(snapshot.nodes) > snapshot.config.max_nodes:
        raise SnapshotContractViolation("snapshot.nodes exceeds config.maxNodes")

    starts = [node for node in snapshot.nodes if node.node_type == "start"]
    ends = [node for node in snapshot.nodes if node.node_type == "end"]
    if len(starts) != 1:
        raise SnapshotContractViolation("snapshot must contain exactly one start node")
    if len(ends) != 1:
        raise SnapshotContractViolation("snapshot must contain exactly one end node")
    start = starts[0]
    end = ends[0]
    if end.result_ports:
        raise SnapshotContractViolation("snapshot end node must not declare result ports")

    guardrails = [node for node in snapshot.nodes if node.node_type == "guardrail"]
    if not guardrails or any(node.config.get("locked") is not True for node in guardrails):
        raise SnapshotContractViolation("snapshot requires locked guardrail nodes")

    agent_node_ids = {
        node.node_id for node in snapshot.nodes if node.node_type == "agent"
    }
    if set(snapshot._model_bindings) != agent_node_ids:
        raise SnapshotContractViolation(
            "snapshot.modelBindings must match all agent nodes"
        )

    edge_routes: set[tuple[str, str, str]] = set()
    adjacency: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    reverse_adjacency: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    for edge in snapshot.edges:
        source = nodes.get(edge.source)
        target = nodes.get(edge.target)
        if source is None or target is None:
            raise SnapshotContractViolation("snapshot.edges references an unknown node")
        if edge.target == start.node_id:
            raise SnapshotContractViolation("snapshot start node must not have incoming edges")
        if edge.source == end.node_id:
            raise SnapshotContractViolation("snapshot end node must not have outgoing edges")
        if edge.result_port not in source.result_ports:
            raise SnapshotContractViolation(
                "snapshot.edges references an undeclared result port"
            )
        route = (edge.source, edge.result_port, edge.target)
        edge_routes.add(route)
        adjacency[edge.source].add(edge.target)
        reverse_adjacency[edge.target].add(edge.source)

    declared_routes = {
        (node.node_id, result_port)
        for node in snapshot.nodes
        for result_port in node.result_ports
    }
    if set(routes) != declared_routes:
        raise SnapshotContractViolation(
            "snapshot result ports must each have exactly one edge"
        )

    reachable = _reachable_nodes(start.node_id, adjacency)
    if reachable != set(nodes):
        raise SnapshotContractViolation("snapshot contains nodes unreachable from start")
    can_reach_end = _reachable_nodes(end.node_id, reverse_adjacency)
    if can_reach_end != set(nodes):
        raise SnapshotContractViolation("snapshot contains nodes that cannot reach end")

    guardrail_ids = {node.node_id for node in guardrails}
    without_guardrails = {
        node_id: {
            target for target in targets if target not in guardrail_ids
        }
        for node_id, targets in adjacency.items()
        if node_id not in guardrail_ids
    }
    if end.node_id in _reachable_nodes(start.node_id, without_guardrails):
        raise SnapshotContractViolation("snapshot contains a guardrail bypass path")

    limited_routes = {limit.route() for limit in snapshot.config.loop_limits}
    if not limited_routes <= edge_routes:
        raise SnapshotContractViolation(
            "snapshot config.loopLimits references an unknown edge"
        )
    for limit in snapshot.config.loop_limits:
        without_limit = _adjacency_without(snapshot, {limit.route()})
        if limit.source != limit.target and limit.source not in _reachable_nodes(
            limit.target, without_limit
        ):
            raise SnapshotContractViolation(
                "snapshot config.loopLimits must identify a repeating edge"
            )
    bounded_adjacency = _adjacency_without(snapshot, limited_routes)
    if _contains_cycle(bounded_adjacency):
        raise SnapshotContractViolation("snapshot contains an unbounded cycle")


def _adjacency_without(
    snapshot: VersionedSnapshot,
    excluded_routes: set[tuple[str, str, str]],
) -> dict[str, set[str]]:
    adjacency = {node.node_id: set() for node in snapshot.nodes}
    for edge in snapshot.edges:
        route = (edge.source, edge.result_port, edge.target)
        if route not in excluded_routes:
            adjacency[edge.source].add(edge.target)
    return adjacency


def _reachable_nodes(source: str, adjacency: Mapping[str, set[str]]) -> set[str]:
    visited: set[str] = set()
    pending = [source]
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        pending.extend(adjacency.get(current, ()))
    return visited


def _contains_cycle(adjacency: Mapping[str, set[str]]) -> bool:
    indegree = {node_id: 0 for node_id in adjacency}
    for targets in adjacency.values():
        for target in targets:
            indegree[target] += 1
    pending = [node_id for node_id, count in indegree.items() if count == 0]
    visited = 0
    while pending:
        current = pending.pop()
        visited += 1
        for target in adjacency[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                pending.append(target)
    return visited != len(indegree)


def _decode_json(raw: bytes | str) -> Mapping[str, Any]:
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if not isinstance(text, str):
            raise TypeError
        value = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except SnapshotContractViolation:
        raise
    except (ValueError, UnicodeDecodeError, TypeError, RecursionError):
        raise SnapshotContractViolation("snapshot payload is not valid JSON") from None
    return _object(value, "snapshot")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotContractViolation("snapshot payload contains duplicate object fields")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    del value
    raise SnapshotContractViolation("snapshot payload contains a non-finite number")


def _object(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise SnapshotContractViolation(f"{field_name} must be an object")
    return dict(value)


def _freeze_object(value: Any, field_name: str) -> Mapping[str, JsonValue]:
    payload = _object(value, field_name)
    _reject_surrogate_keys(payload, field_name)
    _reject_execution_context_fields(payload)
    return MappingProxyType(
        {key: _freeze_json(item, f"{field_name} value") for key, item in payload.items()}
    )


def _freeze_model_bindings(value: Any) -> Mapping[str, JsonValue]:
    payload = _object(value, "snapshot.modelBindings")
    result: dict[str, JsonValue] = {}
    for node_id, value in payload.items():
        _matched(node_id, NODE_IDENTIFIER, "snapshot.modelBindings key", 64)
        result[node_id] = _freeze_object(
            value, "snapshot.modelBindings value"
        )
    return MappingProxyType(result)


def _freeze_tool_policy(value: Any) -> Mapping[str, JsonValue]:
    return _freeze_object(value, "snapshot.toolPolicy")


def _freeze_json(value: Any, field_name: str, depth: int = 0) -> JsonValue:
    if depth > MAX_JSON_DEPTH:
        raise SnapshotContractViolation(f"{field_name} exceeds the JSON depth limit")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        _reject_surrogate_string(value, field_name)
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SnapshotContractViolation(f"{field_name} is not JSON-safe")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise SnapshotContractViolation(f"{field_name} is not JSON-safe")
        _reject_surrogate_keys(value, field_name)
        _reject_execution_context_fields(value)
        return MappingProxyType(
            {
                key: _freeze_json(item, field_name, depth + 1)
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return tuple(
            _freeze_json(item, field_name, depth + 1) for item in value
        )
    raise SnapshotContractViolation(f"{field_name} is not JSON-safe")


def _reject_execution_context_fields(value: Mapping[str, Any]) -> None:
    if EXECUTION_CONTEXT_FIELDS.intersection(value):
        raise SnapshotContractViolation(
            "snapshot settings must not contain execution context fields"
        )


def _reject_surrogate_keys(value: Mapping[str, Any], field_name: str) -> None:
    if any(_contains_surrogate(key) for key in value):
        raise SnapshotContractViolation(f"{field_name} contains invalid Unicode")


def _reject_surrogate_string(value: str, field_name: str) -> None:
    if _contains_surrogate(value):
        raise SnapshotContractViolation(f"{field_name} contains invalid Unicode")


def _contains_surrogate(value: str) -> bool:
    return any(0xD800 <= ord(character) <= 0xDFFF for character in value)


def _thaw_object(value: Mapping[str, JsonValue]) -> dict[str, Any]:
    return {key: _thaw_json(item) for key, item in value.items()}


def _thaw_json(value: JsonValue) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise SnapshotContractViolation(f"{field_name} must be an array")
    return value


def _nonempty_list(value: Any, field_name: str) -> list[Any]:
    result = _list(value, field_name)
    if not result:
        raise SnapshotContractViolation(f"{field_name} must not be empty")
    return result


def _exact_fields(value: Mapping[str, Any], expected: set[str], field_name: str) -> None:
    if set(value) != expected:
        raise SnapshotContractViolation(f"{field_name} contains missing or unknown fields")


def _positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SnapshotContractViolation(f"{field_name} is invalid")
    return value


def _matched(
    value: Any,
    pattern: re.Pattern[str],
    field_name: str,
    maximum: int,
) -> str:
    if (
        not isinstance(value, str)
        or _contains_surrogate(value)
        or not 1 <= len(value) <= maximum
        or not pattern.fullmatch(value)
    ):
        raise SnapshotContractViolation(f"{field_name} is invalid")
    return value


def _uuid(value: Any, field_name: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise SnapshotContractViolation(f"{field_name} is invalid") from None
    return value
