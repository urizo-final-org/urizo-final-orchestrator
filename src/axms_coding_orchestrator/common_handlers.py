"""Source-owned handlers for the minimal common Snapshot runtime."""

from __future__ import annotations

from typing import Mapping

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
            config_validator=_empty_config_validator("start"),
        )
        .register(
            "common.guardrail",
            node_types=["guardrail"],
            result_ports=["passed", "failed"],
            handler=_guardrail,
            config_validator=_guardrail_config_failure,
        )
        .register(
            "common.check",
            node_types=["check"],
            result_ports=["passed", "failed"],
            handler=_check,
            config_validator=_empty_config_validator("check"),
        )
        .register(
            "common.approval",
            node_types=["approval"],
            result_ports=["approved"],
            handler=_unsupported_approval,
            config_validator=_unsupported_approval_config_failure,
        )
        .register(
            "common.end",
            node_types=["end"],
            result_ports=[],
            handler=_end,
            config_validator=_empty_config_validator("end"),
        )
    )


def _start(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "start")
    return NodeResult.create("next")


def _guardrail(invocation: NodeInvocation) -> NodeResult:
    _require_valid_config(_guardrail_config_failure(invocation.config))
    port = (
        "passed"
        if _is_digest(invocation.context.get("policyHash"))
        else "failed"
    )
    return NodeResult.create(port)


def _check(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "check")
    port = (
        "passed"
        if _is_digest(invocation.context.get("contextDigest"))
        else "failed"
    )
    return NodeResult.create(port)


def _unsupported_approval(invocation: NodeInvocation) -> NodeResult:
    del invocation
    raise SnapshotGraphExecutionError(
        "common.approval is not supported by the production Worker contract"
    )


def _end(invocation: NodeInvocation) -> NodeResult:
    _require_empty_config(invocation, "end")
    return NodeResult.create(None)


def _require_empty_config(invocation: NodeInvocation, node_type: str) -> None:
    _require_valid_config(_empty_config_validator(node_type)(invocation.config))


def _empty_config_validator(node_type: str):
    def validate(config: Mapping[str, object]) -> str | None:
        if config:
            return f"common {node_type} does not accept node config"
        return None

    return validate


def _guardrail_config_failure(config: Mapping[str, object]) -> str | None:
    if config != {"locked": True}:
        return "common guardrail config is invalid"
    return None


def _unsupported_approval_config_failure(config: Mapping[str, object]) -> str | None:
    del config
    return "common.approval is not supported by the production Worker contract"


def _require_valid_config(failure: str | None) -> None:
    if failure is not None:
        raise SnapshotGraphExecutionError(failure)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and SHA256_DIGEST.fullmatch(value) is not None
