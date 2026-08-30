"""Source-owned handlers for the minimal common Snapshot runtime."""

from __future__ import annotations

from langgraph.types import interrupt

from .contracts import SHA256_DIGEST
from .graph_builder import SnapshotGraphExecutionError
from .node_runtime import NodeInvocation, NodeRegistry, NodeResult


def build_common_node_registry() -> NodeRegistry:
    """Build the fixed common registry without feature policy or side effects."""

    return (
        NodeRegistry()
        .register(
            "common.start",
            node_types=["start"],
            result_ports=["next"],
            handler=_start,
        )
        .register(
            "common.guardrail",
            node_types=["guardrail"],
            result_ports=["passed", "failed"],
            handler=_guardrail,
        )
        .register(
            "common.check",
            node_types=["check"],
            result_ports=["passed", "failed"],
            handler=_check,
        )
        .register(
            "common.approval",
            node_types=["approval"],
            result_ports=["approved"],
            handler=_approval,
        )
        .register(
            "common.end",
            node_types=["end"],
            result_ports=[],
            handler=_end,
        )
    )


def _start(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "start")
    return NodeResult.create("next")


def _guardrail(invocation: NodeInvocation) -> NodeResult:
    config = invocation.config
    if config != {"locked": True}:
        raise SnapshotGraphExecutionError(
            "common guardrail config is invalid"
        )
    port = (
        "passed"
        if _is_digest(invocation.context.get("policyHash"))
        else "failed"
    )
    return NodeResult.create(port)


def _check(invocation: NodeInvocation) -> NodeResult:
    config = invocation.config
    if config:
        raise SnapshotGraphExecutionError("common check config is invalid")
    port = (
        "passed"
        if _is_digest(invocation.context.get("contextDigest"))
        else "failed"
    )
    return NodeResult.create(port)


def _approval(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "approval")
    decision = interrupt(
        {
            "schemaVersion": "1.0",
            "jobId": invocation.job_id,
            "profileVersionId": invocation.profile_version_id,
            "nodeId": invocation.node_id,
            "traceId": invocation.trace_id,
            "stateVersion": invocation.state_version,
        }
    )
    if decision is True:
        return NodeResult.create("approved")
    raise SnapshotGraphExecutionError(
        "common approval received an invalid resume decision"
    )


def _end(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "end")
    return NodeResult.create(None)


def _require_empty_config(invocation: NodeInvocation, node_type: str) -> None:
    if invocation.config:
        raise SnapshotGraphExecutionError(
            f"common {node_type} does not accept node config"
        )


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and SHA256_DIGEST.fullmatch(value) is not None
