"""Compile validated Versioned Snapshots with source-registered test handlers."""

from __future__ import annotations

from typing import Any, Callable, Mapping, TypedDict

from langgraph.graph import END, START, StateGraph

from .node_runtime import (
    NodeContractViolation,
    NodeHandler,
    NodeInvocation,
    NodeRegistry,
    NodeRegistryViolation,
    NodeResult,
)
from .snapshot import SnapshotNode, VersionedSnapshot


_COMMON_FAILURE_HANDLERS = frozenset({"common.guardrail", "common.check"})


class SnapshotGraphBuildError(ValueError):
    """A Snapshot cannot be bound to the source registry."""


class SnapshotGraphExecutionError(RuntimeError):
    """A compiled Snapshot graph failed its common execution contract."""


class _SnapshotGraphState(TypedDict, total=False):
    jobId: str
    profileVersionId: str
    pipelineAttempt: int
    executionAttempt: int
    stateVersion: int
    traceId: str
    workspaceId: str | None
    toolCallId: str | None
    context: dict[str, Any]
    _snapshotLoopCounts: dict[str, int]
    _snapshotLastNodeId: str
    _snapshotLastResultPort: str | None
    _snapshotEvent: dict[str, Any]
    _snapshotClaim: dict[str, Any]
    _snapshotLedger: dict[str, Any]
    _snapshotProfileDigest: str


class SnapshotGraphBuilder:
    """Build one in-memory LangGraph without changing the current Coding runner."""

    __slots__ = ("_registry",)

    def __init__(self, registry: NodeRegistry) -> None:
        if not isinstance(registry, NodeRegistry):
            raise TypeError("registry must be a NodeRegistry")
        self._registry = registry

    def compile(self, snapshot: VersionedSnapshot, checkpointer: Any = None) -> Any:
        if not isinstance(snapshot, VersionedSnapshot):
            raise SnapshotGraphBuildError("snapshot must be a validated VersionedSnapshot")

        routes = {
            (edge.source, edge.result_port): edge.target for edge in snapshot.edges
        }
        limits = {
            _route_key(limit.source, limit.result_port, limit.target): limit.max_iterations
            for limit in snapshot.config.loop_limits
        }
        handlers: dict[str, NodeHandler] = {}
        for node in snapshot.nodes:
            try:
                registration = self._registry.resolve(node.handler_key)
            except NodeRegistryViolation:
                raise SnapshotGraphBuildError(
                    f"node '{node.node_id}' references an unregistered handlerKey"
                ) from None
            if node.node_type not in registration.node_types:
                raise SnapshotGraphBuildError(
                    f"node '{node.node_id}' type does not match its registered handler"
                )
            if frozenset(node.result_ports) != registration.result_ports:
                raise SnapshotGraphBuildError(
                    f"node '{node.node_id}' result ports do not match its registered handler"
                )
            handlers[node.node_id] = registration.handler

        graph: StateGraph[_SnapshotGraphState] = StateGraph(_SnapshotGraphState)
        for node in snapshot.nodes:
            graph.add_node(
                node.node_id,
                _node_action(
                    snapshot,
                    node,
                    handlers[node.node_id],
                    routes,
                    limits,
                ),
            )

        start = next(node for node in snapshot.nodes if node.node_type == "start")
        end = next(node for node in snapshot.nodes if node.node_type == "end")
        _validate_common_failure_routes(snapshot, routes, end.node_id)
        graph.add_edge(START, start.node_id)
        for node in snapshot.nodes:
            if node.node_id == end.node_id:
                graph.add_edge(node.node_id, END)
                continue
            port_targets = {
                port: routes[(node.node_id, port)] for port in node.result_ports
            }
            if len(port_targets) == 1:
                graph.add_edge(node.node_id, next(iter(port_targets.values())))
            else:
                graph.add_conditional_edges(
                    node.node_id,
                    _port_router(node.node_id, frozenset(port_targets)),
                    port_targets,
                )
        return graph.compile(checkpointer=checkpointer)


def _node_action(
    snapshot: VersionedSnapshot,
    node: SnapshotNode,
    handler: NodeHandler,
    routes: Mapping[tuple[str, str], str],
    limits: Mapping[str, int],
) -> Callable[[_SnapshotGraphState], dict[str, Any]]:
    def run(state: _SnapshotGraphState) -> dict[str, Any]:
        invocation = _invocation(snapshot, node, state)
        result = handler(invocation)
        if not isinstance(result, NodeResult):
            raise SnapshotGraphExecutionError(
                f"node '{node.node_id}' returned an invalid NodeResult"
            )

        if node.node_type == "end":
            if result.port is not None:
                raise SnapshotGraphExecutionError(
                    f"end node '{node.node_id}' returned a result port"
                )
        elif result.port not in node.result_ports:
            port = "TERMINAL" if result.port is None else result.port
            raise SnapshotGraphExecutionError(
                f"node '{node.node_id}' returned undeclared port '{port}'"
            )

        counts = _loop_counts(state.get("_snapshotLoopCounts"), limits)
        if result.port is not None:
            target = routes[(node.node_id, result.port)]
            route_key = _route_key(node.node_id, result.port, target)
            maximum = limits.get(route_key)
            if maximum is not None:
                count = counts.get(route_key, 0) + 1
                if count > maximum:
                    raise SnapshotGraphExecutionError(
                        f"node '{node.node_id}' exceeded its bounded loop"
                    )
                counts[route_key] = count

        context = invocation.context
        context.update(result.updates)
        return {
            "context": context,
            "_snapshotLoopCounts": counts,
            "_snapshotLastNodeId": node.node_id,
            "_snapshotLastResultPort": result.port,
        }

    return run


def _port_router(
    source: str, ports: frozenset[str]
) -> Callable[[_SnapshotGraphState], str]:
    def route(state: _SnapshotGraphState) -> str:
        port = state.get("_snapshotLastResultPort")
        if state.get("_snapshotLastNodeId") != source or port not in ports:
            raise SnapshotGraphExecutionError(
                f"node '{source}' has no valid result port for routing"
            )
        return port

    return route


def _invocation(
    snapshot: VersionedSnapshot,
    node: SnapshotNode,
    state: Mapping[str, Any],
) -> NodeInvocation:
    try:
        invocation = NodeInvocation.create(
            job_id=state["jobId"],
            profile_version_id=state["profileVersionId"],
            node_id=node.node_id,
            pipeline_attempt=state["pipelineAttempt"],
            execution_attempt=state["executionAttempt"],
            state_version=state["stateVersion"],
            trace_id=state["traceId"],
            workspace_id=state.get("workspaceId"),
            tool_call_id=state.get("toolCallId"),
            context=state.get("context", {}),
            config=node.config,
        )
    except (KeyError, NodeContractViolation, TypeError):
        raise SnapshotGraphExecutionError(
            f"node '{node.node_id}' received an invalid execution context"
        ) from None
    if invocation.profile_version_id != snapshot.profile_version_id:
        raise SnapshotGraphExecutionError(
            f"node '{node.node_id}' received a mismatched profileVersionId"
        )
    return invocation


def _loop_counts(value: Any, limits: Mapping[str, int]) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SnapshotGraphExecutionError("snapshot loop state is invalid")
    counts: dict[str, int] = {}
    for route, count in value.items():
        if (
            not isinstance(route, str)
            or route not in limits
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or count > limits[route]
        ):
            raise SnapshotGraphExecutionError("snapshot loop state is invalid")
        counts[route] = count
    return counts


def _route_key(source: str, port: str, target: str) -> str:
    return f"{source}:{port}:{target}"


def _validate_common_failure_routes(
    snapshot: VersionedSnapshot,
    routes: Mapping[tuple[str, str], str],
    end_node_id: str,
) -> None:
    for node in snapshot.nodes:
        if (
            node.handler_key in _COMMON_FAILURE_HANDLERS
            and routes.get((node.node_id, "failed")) != end_node_id
        ):
            raise SnapshotGraphBuildError(
                f"node '{node.node_id}' failed port must route directly to end"
            )
