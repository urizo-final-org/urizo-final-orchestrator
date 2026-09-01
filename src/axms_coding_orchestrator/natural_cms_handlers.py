"""Natural CMS handlers bound to the existing common Snapshot runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol
from uuid import NAMESPACE_URL, uuid5

from langgraph.types import interrupt

from .graph import GraphExecutionError
from .natural_cms_domain_client import (
    NaturalCmsDomainClient,
    NaturalCmsDomainClientError,
    NaturalCmsJob,
    NaturalCmsStageResult,
)
from .node_runtime import NodeInvocation, NodeRegistry, NodeResult


NATURAL_CMS_HANDLER_CONTRACTS: Mapping[
    str, tuple[frozenset[str], frozenset[str]]
] = {
    "cms.analyze": (frozenset({"agent"}), frozenset({"feasible", "infeasible"})),
    "cms.preview": (frozenset({"agent"}), frozenset({"ready"})),
    "cms.discard": (frozenset({"tool"}), frozenset({"retry", "discarded"})),
    "cms.apply": (frozenset({"tool"}), frozenset({"applied"})),
    "cms.approval": (
        frozenset({"approval"}),
        frozenset({"approved", "rejected"}),
    ),
}


class NaturalCmsStageExecutor(Protocol):
    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        job: NaturalCmsJob,
        result_id: str,
    ) -> NaturalCmsStageResult: ...


@dataclass(frozen=True, slots=True)
class SpringGatewayNaturalCmsStageExecutor:
    domain_client: NaturalCmsDomainClient

    def __post_init__(self) -> None:
        if not callable(getattr(self.domain_client, "execute_stage", None)):
            raise TypeError("domain_client must implement execute_stage")

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        job: NaturalCmsJob,
        result_id: str,
    ) -> NaturalCmsStageResult:
        del job
        return self.domain_client.execute_stage(invocation, handler_key, result_id)


@dataclass(frozen=True, slots=True)
class NaturalCmsHandlerDependencies:
    domain_client: NaturalCmsDomainClient
    executor: NaturalCmsStageExecutor

    def __post_init__(self) -> None:
        if not callable(getattr(self.domain_client, "get_job", None)):
            raise TypeError("domain_client must implement get_job")
        if not callable(getattr(self.executor, "execute", None)):
            raise TypeError("executor must implement execute")


def register_natural_cms_node_handlers(
    registry: NodeRegistry,
    dependencies: NaturalCmsHandlerDependencies,
) -> NodeRegistry:
    """Register only the approved AI05-001-01 CMS handlers."""

    if not isinstance(registry, NodeRegistry):
        raise TypeError("registry must be a NodeRegistry")
    if not isinstance(dependencies, NaturalCmsHandlerDependencies):
        raise TypeError("dependencies must be NaturalCmsHandlerDependencies")
    for handler_key in ("cms.analyze", "cms.preview", "cms.discard", "cms.apply"):
        node_types, ports = NATURAL_CMS_HANDLER_CONTRACTS[handler_key]
        registry.register(
            handler_key,
            node_types=node_types,
            result_ports=ports,
            handler=_stage_handler(handler_key, dependencies),
            config_validator=_empty_config_validator(handler_key),
        )
    registry.register(
        "cms.approval",
        node_types=["approval"],
        result_ports=["approved", "rejected"],
        handler=_approval_handler(dependencies),
        config_validator=_approval_config_failure,
    )
    return registry


def _stage_handler(
    handler_key: str,
    dependencies: NaturalCmsHandlerDependencies,
) -> Any:
    expected_ports = NATURAL_CMS_HANDLER_CONTRACTS[handler_key][1]

    def handle(invocation: NodeInvocation) -> NodeResult:
        _require_valid_config(_empty_config_validator(handler_key)(invocation.config))
        try:
            round_number, rounds = _next_round(invocation)
            result_id = _result_id(invocation, handler_key, round_number)
            job = dependencies.domain_client.get_job(invocation)
            _validate_job(invocation, job)
            outcome = dependencies.executor.execute(
                handler_key, invocation, job, result_id
            )
            if not isinstance(outcome, NaturalCmsStageResult):
                raise ValueError("executor returned an invalid Natural CMS result")
            if outcome.result_id != result_id or outcome.handler_key != handler_key:
                raise ValueError("Natural CMS result identity changed")
            if outcome.result_port not in expected_ports:
                raise ValueError("Natural CMS result port is undeclared")
            if outcome.resource != job.resource:
                raise ValueError("Natural CMS result changed its resource")
            _validate_stage_shape(handler_key, outcome)
        except NaturalCmsDomainClientError as failure:
            raise GraphExecutionError(
                failure.code,
                "The Spring Natural CMS operation failed.",
                retryable=failure.retryable,
            ) from None
        except GraphExecutionError:
            raise
        except (TypeError, ValueError, RecursionError):
            raise _contract_failure(
                "The Natural CMS stage returned an invalid result."
            ) from None

        reference: dict[str, Any] = {
            "resultId": outcome.result_id,
            "handlerKey": outcome.handler_key,
            "resultPort": outcome.result_port,
            "resource": {"type": outcome.resource.type, "id": outcome.resource.id},
        }
        if outcome.preview_id is not None:
            reference["previewId"] = outcome.preview_id
            reference["previewHash"] = outcome.preview_hash
        updates: dict[str, Any] = {
            "naturalCmsStageRounds": rounds,
            "naturalCmsLastResult": reference,
        }
        return NodeResult.create(outcome.result_port, updates)

    return handle


def _approval_handler(dependencies: NaturalCmsHandlerDependencies) -> Any:
    def handle(invocation: NodeInvocation) -> NodeResult:
        _require_valid_config(_approval_config_failure(invocation.config))
        last = invocation.context.get("naturalCmsLastResult")
        if not isinstance(last, Mapping) or last.get("handlerKey") != "cms.preview":
            raise _state_conflict("Natural CMS approval has no current preview.")
        preview_id = last.get("previewId")
        preview_hash = last.get("previewHash")
        if not isinstance(preview_id, str) or not isinstance(preview_hash, str):
            raise _state_conflict("Natural CMS approval preview is invalid.")
        resumed = interrupt(
            {
                "schemaVersion": "1.0",
                "jobId": invocation.job_id,
                "profileVersionId": invocation.profile_version_id,
                "nodeId": invocation.node_id,
                "stage": "PREVIEW",
                "requiredRole": "GENERAL_ADMIN",
                "pipelineAttempt": invocation.pipeline_attempt,
                "traceId": invocation.trace_id,
                "stateVersion": invocation.state_version,
                "previewId": preview_id,
            }
        )
        if resumed is not True:
            raise _contract_failure(
                "Natural CMS approval received an invalid resume decision"
            )
        try:
            job = dependencies.domain_client.get_job(invocation)
            _validate_job(invocation, job)
            if (
                job.status != "WAITING_APPROVAL"
                or not job.preview_valid
                or job.preview_id != preview_id
                or job.preview_hash != preview_hash
                or job.approval_decision not in {"APPROVED", "REJECTED"}
            ):
                raise ValueError("Natural CMS decision does not match its preview")
            port = (
                "approved" if job.approval_decision == "APPROVED" else "rejected"
            )
        except NaturalCmsDomainClientError as failure:
            raise GraphExecutionError(
                failure.code,
                "The Spring Natural CMS approval lookup failed.",
                retryable=failure.retryable,
            ) from None
        except (TypeError, ValueError, RecursionError):
            raise _state_conflict(
                "The recorded Natural CMS approval decision is invalid."
            ) from None
        return NodeResult.create(port)

    return handle


def _empty_config_validator(handler_key: str):
    def validate(config: Mapping[str, Any]) -> str | None:
        if config:
            return f"{handler_key} does not accept node config"
        return None

    return validate


def _approval_config_failure(config: Mapping[str, Any]) -> str | None:
    if config != {"stage": "PREVIEW", "requiredRole": "GENERAL_ADMIN"}:
        return "cms.approval config is invalid"
    return None


def _require_valid_config(failure: str | None) -> None:
    if failure is not None:
        raise _contract_failure(failure)


def _validate_job(invocation: NodeInvocation, job: NaturalCmsJob) -> None:
    if (
        job.job_id != invocation.job_id
        or job.trace_id != invocation.trace_id
        or job.profile_version_id != invocation.profile_version_id
        or job.pipeline_attempt != invocation.pipeline_attempt
        or job.state_version != invocation.state_version
    ):
        raise ValueError("Natural CMS Job does not match the invocation")


def _validate_stage_shape(
    handler_key: str, outcome: NaturalCmsStageResult
) -> None:
    has_preview = outcome.preview_id is not None and outcome.preview_hash is not None
    if handler_key == "cms.analyze":
        if outcome.structured_command is not None or has_preview:
            raise ValueError("Natural CMS analysis leaked a preview subject")
        return
    if outcome.structured_command is None or not has_preview:
        raise ValueError("Natural CMS stage has no preview subject")


def _next_round(invocation: NodeInvocation) -> tuple[int, dict[str, int]]:
    raw = invocation.context.get("naturalCmsStageRounds", {})
    if not isinstance(raw, Mapping):
        raise ValueError("naturalCmsStageRounds is invalid")
    rounds: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("naturalCmsStageRounds is invalid")
        if value < 1 or value > 100:
            raise ValueError("naturalCmsStageRounds is invalid")
        rounds[key] = value
    current = rounds.get(invocation.node_id, 0) + 1
    rounds[invocation.node_id] = current
    return current, rounds


def _result_id(
    invocation: NodeInvocation, handler_key: str, round_number: int
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "axms:natural-cms-result:%s:%d:%s:%s:%d"
            % (
                invocation.job_id,
                invocation.pipeline_attempt,
                invocation.node_id,
                handler_key,
                round_number,
            ),
        )
    )


def _contract_failure(message: str) -> GraphExecutionError:
    return GraphExecutionError(
        "CONTRACT_VALIDATION_FAILED", message, retryable=False
    )


def _state_conflict(message: str) -> GraphExecutionError:
    return GraphExecutionError(
        "JOB_STATE_VERSION_CONFLICT", message, retryable=False
    )
