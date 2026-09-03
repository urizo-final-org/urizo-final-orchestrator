"""AI04 feature handlers bound to the common Snapshot runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol
from uuid import NAMESPACE_URL, uuid5

from langgraph.types import interrupt

from .coding_domain_client import (
    APPROVAL_ROLES,
    APPROVAL_STAGES,
    CodingAttemptAggregate,
    CodingDomainClient,
    CodingDomainClientError,
    CodingResultWrite,
)
from .contracts import GIT_OBJECT_ID, SHA256_DIGEST
from .graph import GraphExecutionError
from .node_runtime import NodeInvocation, NodeRegistry, NodeResult


CODING_HANDLER_CONTRACTS: Mapping[str, tuple[frozenset[str], frozenset[str]]] = {
    "coding.analyze": (frozenset({"agent"}), frozenset({"feasible", "infeasible"})),
    "coding.code": (frozenset({"agent"}), frozenset({"completed"})),
    "coding.review": (
        frozenset({"agent"}),
        frozenset({"passed", "changes_requested"}),
    ),
    "coding.preview": (frozenset({"tool"}), frozenset({"ready"})),
    "coding.approval": (frozenset({"approval"}), frozenset({"approved"})),
    "coding.preview_approval": (
        frozenset({"approval"}),
        frozenset({"approved", "rejected"}),
    ),
    "coding.pr_request": (frozenset({"tool"}), frozenset({"requested"})),
    "coding.pr_complete": (frozenset({"tool"}), frozenset({"completed"})),
    "coding.dev_merge_check": (
        frozenset({"check"}),
        frozenset({"merged", "not_merged", "blocked"}),
    ),
    "coding.deploy_request": (frozenset({"tool"}), frozenset({"recorded"})),
    "coding.deploy": (frozenset({"tool"}), frozenset({"completed", "blocked"})),
    "coding.rework_gate": (
        frozenset({"check"}),
        frozenset({"retry", "handover"}),
    ),
}

_STAGE_RESULT_TYPES = {
    "coding.analyze": "ANALYSIS",
    "coding.code": "CANDIDATE",
    "coding.review": "REVIEW",
    "coding.preview": "DIFF",
    "coding.pr_request": "PULL_REQUEST",
    "coding.pr_complete": "PULL_REQUEST",
    "coding.dev_merge_check": "DEV_MERGE",
    "coding.deploy_request": "DEPLOY_REQUEST",
    "coding.deploy": "DEPLOYMENT",
}
_EMPTY_CONFIG_HANDLERS = frozenset(
    {
        "coding.analyze",
        "coding.code",
        "coding.review",
        "coding.preview",
        "coding.pr_request",
        "coding.pr_complete",
        "coding.dev_merge_check",
        "coding.deploy",
    }
)


class CodingHandlerFailure(RuntimeError):
    """Sanitized, typed feature-handler failure."""

    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class CodingStageOutcome:
    """One current-node result; raw payload is persisted only in Spring."""

    port: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    workspace_id: str | None = None
    candidate_sha: str | None = None
    diff_digest: str | None = None
    validation_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.port, str):
            raise ValueError("Coding stage outcome port is invalid")
        if not isinstance(self.payload, Mapping) or not all(
            isinstance(key, str) for key in self.payload
        ):
            raise ValueError("Coding stage outcome payload is invalid")
        _optional_match(self.candidate_sha, GIT_OBJECT_ID, "candidateSha", 71)
        _optional_match(self.diff_digest, SHA256_DIGEST, "diffDigest", 71)
        _optional_match(self.validation_hash, SHA256_DIGEST, "validationHash", 71)


class CodingStageExecutor(Protocol):
    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        attempt: CodingAttemptAggregate,
        result_id: str,
    ) -> CodingStageOutcome: ...


class PreparedResultCodingStageExecutor:
    """Consume an exact Backend-prepared Model/MCP/request result, or fail closed."""

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        attempt: CodingAttemptAggregate,
        result_id: str,
    ) -> CodingStageOutcome:
        del invocation
        matches = [
            result
            for result in attempt.results
            if result.result_id == result_id and result.handler_key == handler_key
        ]
        if not matches:
            raise CodingHandlerFailure(
                "HANDLER_RESULT_NOT_FOUND",
                "The approved Coding stage has no prepared result.",
                retryable=False,
            )
        if len(matches) != 1:
            raise CodingHandlerFailure(
                "CONTRACT_VALIDATION_FAILED",
                "The prepared Coding stage result is ambiguous.",
                retryable=False,
            )
        result = matches[0]
        if result.result_type != _STAGE_RESULT_TYPES[handler_key]:
            raise CodingHandlerFailure(
                "CONTRACT_VALIDATION_FAILED",
                "The prepared Coding stage result type is invalid.",
                retryable=False,
            )
        return CodingStageOutcome(
            port=result.result_port,
            workspace_id=result.workspace_id,
            candidate_sha=result.candidate_sha,
            diff_digest=result.diff_digest,
            validation_hash=result.validation_hash,
            payload=result.payload,
        )


@dataclass(frozen=True, slots=True)
class SpringGatewayCodingStageExecutor:
    """Execute one AI04 stage through Spring's bounded Model/Tool boundary."""

    domain_client: CodingDomainClient

    def __post_init__(self) -> None:
        if not callable(getattr(self.domain_client, "execute_stage", None)):
            raise TypeError("domain_client must implement execute_stage")

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        attempt: CodingAttemptAggregate,
        result_id: str,
    ) -> CodingStageOutcome:
        del attempt
        result = self.domain_client.execute_stage(invocation, handler_key, result_id)
        return CodingStageOutcome(
            port=result.result_port,
            workspace_id=result.workspace_id,
            candidate_sha=result.candidate_sha,
            diff_digest=result.diff_digest,
            validation_hash=result.validation_hash,
            payload=result.payload,
        )


@dataclass(frozen=True, slots=True)
class CodingHandlerDependencies:
    domain_client: CodingDomainClient
    executor: CodingStageExecutor

    def __post_init__(self) -> None:
        if not callable(getattr(self.domain_client, "get_attempt", None)) or not callable(
            getattr(self.domain_client, "put_result", None)
        ):
            raise TypeError("domain_client must implement the CodingDomainClient contract")
        if not callable(getattr(self.executor, "execute", None)):
            raise TypeError("executor must implement execute")


def register_coding_node_handlers(
    registry: NodeRegistry,
    dependencies: CodingHandlerDependencies,
) -> NodeRegistry:
    """Add only the approved AI04 feature keys to an existing source registry."""

    if not isinstance(registry, NodeRegistry):
        raise TypeError("registry must be a NodeRegistry")
    if not isinstance(dependencies, CodingHandlerDependencies):
        raise TypeError("dependencies must be CodingHandlerDependencies")

    for handler_key in _STAGE_RESULT_TYPES:
        node_types, ports = CODING_HANDLER_CONTRACTS[handler_key]
        registry.register(
            handler_key,
            node_types=node_types,
            result_ports=ports,
            handler=_stage_handler(handler_key, dependencies),
            config_validator=_stage_config_validator(handler_key),
        )
    registry.register(
        "coding.approval",
        node_types=["approval"],
        result_ports=["approved"],
        handler=_approval_handler(dependencies, candidate=False),
        config_validator=_approval_config_validator(candidate=False),
    )
    registry.register(
        "coding.preview_approval",
        node_types=["approval"],
        result_ports=["approved", "rejected"],
        handler=_approval_handler(dependencies, candidate=True),
        config_validator=_approval_config_validator(candidate=True),
    )
    node_types, ports = CODING_HANDLER_CONTRACTS["coding.rework_gate"]
    registry.register(
        "coding.rework_gate",
        node_types=node_types,
        result_ports=ports,
        handler=_rework_gate_handler(),
        config_validator=_rework_gate_config_failure,
    )
    return registry


def _rework_gate_handler() -> Any:
    """Decide whether a rejected review returns to coding or hands over to a human.

    The bounded loop in the common graph builder raises once the retry edge is
    exhausted, which would end the Job as an execution error and leave the
    handover screen with nothing to read. This gate keeps the same round budget
    but turns the final rejection into a declared port, so the Job finishes
    normally with the generated candidates and review reasons already recorded.
    """

    def handle(invocation: NodeInvocation) -> NodeResult:
        config = invocation.config
        _require_valid_config(_rework_gate_config_failure(config))
        maximum = config["maxReworkRounds"]
        try:
            round_number, rounds = _next_round(invocation)
        except (TypeError, ValueError):
            raise _contract_failure("The Coding stage returned an invalid result.") from None
        port = "retry" if round_number < maximum else "handover"
        return NodeResult.create(port, {"codingStageRounds": rounds})

    return handle


def _stage_handler(
    handler_key: str,
    dependencies: CodingHandlerDependencies,
) -> Any:
    expected_ports = CODING_HANDLER_CONTRACTS[handler_key][1]
    result_type = _STAGE_RESULT_TYPES[handler_key]

    def handle(invocation: NodeInvocation) -> NodeResult:
        config = invocation.config
        _require_valid_config(_stage_config_failure(handler_key, config))

        try:
            round_number, rounds = _next_round(invocation)
            result_id = _result_id(invocation, handler_key, round_number)
            aggregate = dependencies.domain_client.get_attempt(invocation)
            _validate_attempt(invocation, aggregate)
            required_subject = _stage_required_subject(handler_key, aggregate)
            required_candidate = _stage_required_candidate(handler_key, aggregate)
            outcome = dependencies.executor.execute(
                handler_key,
                invocation,
                aggregate,
                result_id,
            )
            if not isinstance(outcome, CodingStageOutcome):
                raise ValueError("executor returned an invalid CodingStageOutcome")
            if outcome.port not in expected_ports:
                raise ValueError("executor returned an undeclared Coding result port")
            if required_subject is not None and (
                outcome.candidate_sha,
                outcome.validation_hash,
            ) != required_subject:
                raise ValueError("Coding request result changed the approved subject")
            if (
                required_candidate is not None
                and outcome.candidate_sha != required_candidate
            ):
                raise ValueError("Coding stage result changed the reviewed candidate")
            _validate_stage_outcome_contract(handler_key, aggregate, outcome)
            workspace_id = (
                outcome.workspace_id
                or aggregate.workspace_id
                or invocation.workspace_id
            )
            if invocation.workspace_id is not None and workspace_id != invocation.workspace_id:
                raise ValueError("executor changed the authoritative workspaceId")
            recorded = dependencies.domain_client.put_result(
                invocation,
                CodingResultWrite(
                    result_id=result_id,
                    handler_key=handler_key,
                    result_type=result_type,
                    result_port=outcome.port,
                    workspace_id=workspace_id,
                    candidate_sha=outcome.candidate_sha,
                    diff_digest=outcome.diff_digest,
                    validation_hash=outcome.validation_hash,
                    payload=outcome.payload,
                ),
            )
        except CodingDomainClientError as failure:
            raise GraphExecutionError(
                failure.code,
                "The Spring Coding Domain operation failed.",
                retryable=failure.retryable,
            ) from None
        except CodingHandlerFailure as failure:
            raise GraphExecutionError(
                failure.code,
                "The Coding stage executor failed.",
                retryable=failure.retryable,
            ) from None
        except GraphExecutionError:
            raise
        except (TypeError, ValueError, RecursionError):
            raise _contract_failure("The Coding stage returned an invalid result.") from None

        result_reference: dict[str, Any] = {
            "resultId": recorded.result_id,
            "handlerKey": recorded.handler_key,
            "resultType": recorded.result_type,
            "resultPort": recorded.result_port,
        }
        optional = {
            "workspaceId": recorded.workspace_id,
            "candidateSha": recorded.candidate_sha,
            "diffDigest": recorded.diff_digest,
            "validationHash": recorded.validation_hash,
        }
        result_reference.update(
            {key: value for key, value in optional.items() if value is not None}
        )
        return NodeResult.create(
            outcome.port,
            {
                "codingStageRounds": rounds,
                "codingLastResult": result_reference,
            },
        )

    return handle


def _approval_handler(
    dependencies: CodingHandlerDependencies,
    *,
    candidate: bool,
) -> Any:
    def handle(invocation: NodeInvocation) -> NodeResult:
        config = invocation.config
        _require_valid_config(_approval_config_failure(config, candidate=candidate))
        stage = config["stage"]
        required_role = config["requiredRole"]
        round_number, rounds = _next_round(invocation)
        approval_id = _approval_id(invocation, stage, round_number)
        resumed = interrupt(
            {
                "schemaVersion": "1.0",
                "approvalId": approval_id,
                "jobId": invocation.job_id,
                "profileVersionId": invocation.profile_version_id,
                "nodeId": invocation.node_id,
                "stage": stage,
                "stageRound": round_number,
                "requiredRole": required_role,
                "pipelineAttempt": invocation.pipeline_attempt,
                "traceId": invocation.trace_id,
                "stateVersion": invocation.state_version,
            }
        )
        if resumed is not True:
            raise _contract_failure("coding approval received an invalid resume decision")
        try:
            aggregate = dependencies.domain_client.get_attempt(invocation)
            _validate_attempt(invocation, aggregate)
            matches = [
                decision
                for decision in aggregate.decisions
                if decision.node_id == invocation.node_id
                and decision.stage == stage
                and decision.stage_round == round_number
            ]
            if len(matches) != 1:
                raise ValueError("approval decision is missing or ambiguous")
            decision = matches[0]
            decision_attempt = invocation.pipeline_attempt
            if (
                candidate
                and decision.decision == "REJECTED"
                and decision.next_pipeline_attempt == invocation.pipeline_attempt
            ):
                decision_attempt -= 1
            if (
                decision_attempt < 1
                or decision.approval_id
                != _approval_id(
                    invocation,
                    stage,
                    round_number,
                    pipeline_attempt=decision_attempt,
                )
                or not _role_satisfies(decision.actor_role, required_role)
                or decision.result_state_version > invocation.state_version
            ):
                raise ValueError("approval authority does not match the invocation")
            if candidate:
                _validate_candidate_decision(invocation, aggregate, decision)
                port = "approved" if decision.decision == "APPROVED" else "rejected"
            else:
                if decision.decision != "APPROVED":
                    raise ValueError("terminal approval rejection must not resume")
                if stage in {"GITHUB", "CMS", "DEPLOY"}:
                    _validate_post_preview_decision(aggregate, decision)
                port = "approved"
        except CodingDomainClientError as failure:
            raise GraphExecutionError(
                failure.code,
                "The Spring Coding approval lookup failed.",
                retryable=failure.retryable,
            ) from None
        except (TypeError, ValueError, RecursionError):
            raise _state_conflict("The recorded Coding approval decision is invalid.") from None
        return NodeResult.create(port, {"codingStageRounds": rounds})

    return handle


def _stage_config_validator(handler_key: str):
    def validate(config: Mapping[str, Any]) -> str | None:
        return _stage_config_failure(handler_key, config)

    return validate


def _stage_config_failure(
    handler_key: str, config: Mapping[str, Any]
) -> str | None:
    if handler_key in _EMPTY_CONFIG_HANDLERS and config:
        return f"{handler_key} does not accept node config"
    if handler_key == "coding.deploy_request" and config != {
        "mode": "request_record_only"
    }:
        return "coding.deploy_request config is invalid"
    return None


def _rework_gate_config_failure(config: Mapping[str, Any]) -> str | None:
    if set(config) != {"maxReworkRounds"}:
        return "coding.rework_gate config is invalid"
    maximum = config["maxReworkRounds"]
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
        return "coding.rework_gate config maxReworkRounds is invalid"
    return None


def _approval_config_validator(*, candidate: bool):
    def validate(config: Mapping[str, Any]) -> str | None:
        return _approval_config_failure(config, candidate=candidate)

    return validate


def _approval_config_failure(
    config: Mapping[str, Any], *, candidate: bool
) -> str | None:
    if set(config) != {"stage", "requiredRole"}:
        return "coding approval config is invalid"
    stage = config["stage"]
    required_role = config["requiredRole"]
    expected_stage = "CANDIDATE" if candidate else None
    if (
        not isinstance(stage, str)
        or stage not in APPROVAL_STAGES
        or not isinstance(required_role, str)
        or required_role not in APPROVAL_ROLES
        or (expected_stage is not None) != (stage == "CANDIDATE")
    ):
        return "coding approval config is invalid"
    return None


def _require_valid_config(failure: str | None) -> None:
    if failure is not None:
        raise _contract_failure(failure)


def _validate_candidate_decision(
    invocation: NodeInvocation,
    aggregate: CodingAttemptAggregate,
    decision: Any,
) -> None:
    previews = [
        result
        for result in aggregate.results
        if result.handler_key == "coding.preview"
        and result.result_type == "DIFF"
        and result.result_port == "ready"
    ]
    if (
        decision.candidate_sha is None
        or decision.validation_hash is None
    ):
        raise ValueError("candidate approval is not bound to the preview")
    if previews:
        preview = previews[-1]
        if (
            preview.candidate_sha is None
            or preview.validation_hash is None
            or decision.candidate_sha != preview.candidate_sha
            or decision.validation_hash != preview.validation_hash
        ):
            raise ValueError("candidate approval is not bound to the preview")
    elif not (
        decision.decision == "REJECTED"
        and decision.next_pipeline_attempt == invocation.pipeline_attempt
    ):
        raise ValueError("candidate approval has no ready preview")
    if decision.decision == "REJECTED":
        if decision.next_pipeline_attempt != invocation.pipeline_attempt:
            raise ValueError("candidate rejection did not advance pipelineAttempt")
    elif decision.next_pipeline_attempt is not None:
        raise ValueError("candidate approval contains an unexpected next attempt")


def _validate_post_preview_decision(
    aggregate: CodingAttemptAggregate,
    decision: Any,
) -> None:
    subject = (
        _latest_deploy_request_subject(aggregate)
        if decision.stage == "DEPLOY" and _has_v4_deploy_request(aggregate)
        else _latest_preview_subject(aggregate)
    )
    if _decision_subject(decision) != subject:
        raise ValueError("post-preview approval changed the candidate subject")
    if decision.stage == "GITHUB":
        _require_approved_decision(aggregate, "CANDIDATE", subject)
        requests = [
            result
            for result in aggregate.results
            if result.handler_key in {"coding.pr_request", "coding.pr_complete"}
            and result.result_type == "PULL_REQUEST"
            and result.result_port in {"requested", "completed"}
        ]
        if (
            not requests
            or (requests[-1].candidate_sha, requests[-1].validation_hash) != subject
        ):
            raise ValueError("GitHub approval is not bound to the PR request")


def _stage_required_subject(
    handler_key: str,
    aggregate: CodingAttemptAggregate,
) -> tuple[str, str] | None:
    required_stage = {"coding.pr_request": "CANDIDATE"}.get(handler_key)
    if handler_key == "coding.pr_complete":
        subject = _latest_result_subject(
            aggregate, "coding.pr_request", "PULL_REQUEST", "requested"
        )
        _require_approved_decision(aggregate, "GITHUB", subject)
        return subject
    if handler_key == "coding.dev_merge_check":
        pull_request = _latest_result(
            aggregate, "coding.pr_complete", "PULL_REQUEST", "completed"
        )
        deploy_request = _latest_v4_deploy_request(aggregate)
        _require_matching_pr_identity(pull_request, deploy_request)
        deploy_subject = _result_subject(deploy_request)
        _require_approved_decision(aggregate, "DEPLOY", deploy_subject)
        return _result_subject(pull_request)
    if handler_key == "coding.deploy_request" and _has_completed_pull_request(aggregate):
        pull_request_subject = _latest_result_subject(
            aggregate, "coding.pr_complete", "PULL_REQUEST", "completed"
        )
        _require_approved_decision(aggregate, "GITHUB", pull_request_subject)
        return None
    if handler_key == "coding.deploy_request":
        required_stage = "DEPLOY"
    if handler_key == "coding.deploy":
        deploy_request = _latest_v4_deploy_request(aggregate)
        pull_request = _latest_result(
            aggregate, "coding.pr_complete", "PULL_REQUEST", "completed"
        )
        _require_matching_pr_identity(pull_request, deploy_request)
        subject = _result_subject(deploy_request)
        _require_approved_decision(aggregate, "DEPLOY", subject)
        merge_subject = _latest_result_subject(
            aggregate, "coding.dev_merge_check", "DEV_MERGE", "merged"
        )
        if merge_subject[0] != subject[0]:
            raise ValueError("dev merge changed the deployment candidate")
        return subject
    if required_stage is None:
        return None
    subject = _latest_preview_subject(aggregate)
    decision = _require_approved_decision(aggregate, required_stage, subject)
    if required_stage == "DEPLOY":
        _validate_post_preview_decision(aggregate, decision)
    return subject


def _stage_required_candidate(
    handler_key: str,
    aggregate: CodingAttemptAggregate,
) -> str | None:
    if handler_key == "coding.review":
        return _latest_code_candidate(aggregate)
    if handler_key == "coding.preview":
        code_candidate = _latest_code_candidate(aggregate)
        reviews = [
            result
            for result in aggregate.results
            if result.handler_key == "coding.review"
        ]
        if (
            not reviews
            or reviews[-1].result_type != "REVIEW"
            or reviews[-1].result_port != "passed"
        ):
            raise ValueError("Coding preview has no latest passed review")
        reviewed_candidate = reviews[-1].candidate_sha
        if reviewed_candidate is None or reviewed_candidate != code_candidate:
            raise ValueError("Coding review changed the latest code candidate")
        return reviewed_candidate
    if handler_key == "coding.deploy_request" and _has_completed_pull_request(aggregate):
        return _latest_result_subject(
            aggregate, "coding.pr_complete", "PULL_REQUEST", "completed"
        )[0]
    return None


def _validate_stage_outcome_contract(
    handler_key: str,
    aggregate: CodingAttemptAggregate,
    outcome: CodingStageOutcome,
) -> None:
    payload = outcome.payload
    if handler_key == "coding.pr_complete":
        if (
            payload.get("repository") != "backend"
            or payload.get("base") != "dev"
            or payload.get("candidateSha") != outcome.candidate_sha
            or not isinstance(payload.get("head"), str)
            or not payload["head"].startswith("system/llmops-")
            or not isinstance(payload.get("headSha"), str)
            or GIT_OBJECT_ID.fullmatch(payload["headSha"]) is None
            or isinstance(payload.get("prNumber"), bool)
            or not isinstance(payload.get("prNumber"), int)
            or payload["prNumber"] < 1
            or not isinstance(payload.get("prUrl"), str)
            or not payload["prUrl"]
        ):
            raise ValueError("pull request receipt is invalid")
    elif handler_key == "coding.dev_merge_check":
        expected_status = {
            "merged": "MERGED",
            "not_merged": "NOT_MERGED",
            "blocked": "BLOCKED",
        }[outcome.port]
        if payload.get("status") != expected_status:
            raise ValueError("dev merge receipt status is invalid")
        if (
            payload.get("candidateSha") != outcome.candidate_sha
            or not isinstance(payload.get("head"), str)
            or not isinstance(payload.get("headSha"), str)
            or GIT_OBJECT_ID.fullmatch(payload["headSha"]) is None
        ):
            raise ValueError("dev merge receipt subject is invalid")
        if outcome.port == "merged" and (
            not isinstance(payload.get("mergeSha"), str)
            or GIT_OBJECT_ID.fullmatch(payload["mergeSha"]) is None
        ):
            raise ValueError("dev merge receipt SHA is invalid")
    elif handler_key == "coding.deploy_request" and _has_completed_pull_request(aggregate):
        required = {
            "deploymentRequestId",
            "jobId",
            "pipelineAttempt",
            "repository",
            "prNumber",
            "candidateSha",
            "sourceValidationHash",
            "adapterKey",
            "targetKey",
            "configDigest",
            "status",
        }
        pull_request = _latest_result(
            aggregate, "coding.pr_complete", "PULL_REQUEST", "completed"
        )
        if (
            not required.issubset(payload)
            or payload.get("jobId") != aggregate.job_id
            or payload.get("pipelineAttempt") != aggregate.pipeline_attempt
            or payload.get("candidateSha") != pull_request.candidate_sha
            or payload.get("sourceValidationHash") != pull_request.validation_hash
            or payload.get("repository") != "backend"
            or not isinstance(payload.get("deploymentRequestId"), str)
            or not isinstance(outcome.validation_hash, str)
        ):
            raise ValueError("deployment request subject is invalid")
    elif handler_key == "coding.deploy":
        if (
            not isinstance(payload.get("deploymentRequestId"), str)
            or not isinstance(payload.get("deploymentExecutionId"), str)
            or not isinstance(payload.get("mergeSha"), str)
            or GIT_OBJECT_ID.fullmatch(payload["mergeSha"]) is None
            or "port" in payload
            or "deployedPort" in payload
        ):
            raise ValueError("deployment receipt is invalid")


def _latest_code_candidate(aggregate: CodingAttemptAggregate) -> str:
    candidates = [
        result
        for result in aggregate.results
        if result.handler_key == "coding.code"
    ]
    if (
        not candidates
        or candidates[-1].result_type != "CANDIDATE"
        or candidates[-1].result_port != "completed"
        or candidates[-1].candidate_sha is None
    ):
        raise ValueError("Coding stage has no latest completed candidate")
    return candidates[-1].candidate_sha


def _latest_preview_subject(aggregate: CodingAttemptAggregate) -> tuple[str, str]:
    previews = [
        result
        for result in aggregate.results
        if result.handler_key == "coding.preview"
        and result.result_type == "DIFF"
        and result.result_port == "ready"
    ]
    if not previews:
        raise ValueError("Coding request has no ready preview")
    preview = previews[-1]
    if preview.candidate_sha is None or preview.validation_hash is None:
        raise ValueError("Coding preview has no candidate subject")
    return preview.candidate_sha, preview.validation_hash


def _latest_result(
    aggregate: CodingAttemptAggregate,
    handler_key: str,
    result_type: str,
    result_port: str,
) -> Any:
    matches = [
        result
        for result in aggregate.results
        if result.handler_key == handler_key
        and result.result_type == result_type
        and result.result_port == result_port
    ]
    if not matches:
        raise ValueError(f"Coding prerequisite {handler_key} is missing")
    return matches[-1]


def _latest_result_subject(
    aggregate: CodingAttemptAggregate,
    handler_key: str,
    result_type: str,
    result_port: str,
) -> tuple[str, str]:
    result = _latest_result(aggregate, handler_key, result_type, result_port)
    return _result_subject(result)


def _result_subject(result: Any) -> tuple[str, str]:
    if result.candidate_sha is None or result.validation_hash is None:
        raise ValueError("Coding prerequisite has no subject")
    return result.candidate_sha, result.validation_hash


def _latest_v4_deploy_request(aggregate: CodingAttemptAggregate) -> Any:
    matches = [
        result
        for result in aggregate.results
        if result.handler_key == "coding.deploy_request"
        and result.result_type == "DEPLOY_REQUEST"
        and result.result_port == "recorded"
        and isinstance(result.payload.get("deploymentRequestId"), str)
    ]
    if not matches:
        raise ValueError("Coding deployment request is missing")
    return matches[-1]


def _require_matching_pr_identity(pull_request: Any, deploy_request: Any) -> None:
    if (
        pull_request.candidate_sha != deploy_request.candidate_sha
        or pull_request.payload.get("repository")
        != deploy_request.payload.get("repository")
        or pull_request.payload.get("prNumber") != deploy_request.payload.get("prNumber")
    ):
        raise ValueError("deployment request changed the completed pull request")


def _has_completed_pull_request(aggregate: CodingAttemptAggregate) -> bool:
    return any(
        result.handler_key == "coding.pr_complete"
        and result.result_type == "PULL_REQUEST"
        and result.result_port == "completed"
        for result in aggregate.results
    )


def _has_v4_deploy_request(aggregate: CodingAttemptAggregate) -> bool:
    return any(
        result.handler_key == "coding.deploy_request"
        and result.result_type == "DEPLOY_REQUEST"
        and result.result_port == "recorded"
        and isinstance(result.payload.get("deploymentRequestId"), str)
        for result in aggregate.results
    )


def _latest_deploy_request_subject(
    aggregate: CodingAttemptAggregate,
) -> tuple[str, str]:
    matches = [
        result
        for result in aggregate.results
        if result.handler_key == "coding.deploy_request"
        and result.result_type == "DEPLOY_REQUEST"
        and result.result_port == "recorded"
        and isinstance(result.payload.get("deploymentRequestId"), str)
    ]
    if not matches or matches[-1].candidate_sha is None or matches[-1].validation_hash is None:
        raise ValueError("Coding deployment request has no stable subject")
    return matches[-1].candidate_sha, matches[-1].validation_hash


def _decision_subject(decision: Any) -> tuple[str | None, str | None]:
    return decision.candidate_sha, decision.validation_hash


def _require_approved_decision(
    aggregate: CodingAttemptAggregate,
    stage: str,
    subject: tuple[str, str],
) -> Any:
    decisions = [
        item
        for item in aggregate.decisions
        if item.stage == stage
    ]
    if (
        not decisions
        or decisions[-1].decision != "APPROVED"
        or _decision_subject(decisions[-1]) != subject
    ):
        raise ValueError("Coding request is not bound to its prior approval")
    return decisions[-1]


def _validate_attempt(
    invocation: NodeInvocation, aggregate: CodingAttemptAggregate
) -> None:
    if (
        not isinstance(aggregate, CodingAttemptAggregate)
        or aggregate.job_id != invocation.job_id
        or aggregate.trace_id != invocation.trace_id
        or aggregate.pipeline_attempt != invocation.pipeline_attempt
        or aggregate.status != "ACTIVE"
    ):
        raise ValueError("Coding attempt does not match the current invocation")
    if any(
        result.job_id != invocation.job_id
        or result.trace_id != invocation.trace_id
        or result.pipeline_attempt != invocation.pipeline_attempt
        for result in aggregate.results
    ):
        raise ValueError("Coding result does not match the current attempt")


def _next_round(invocation: NodeInvocation) -> tuple[int, dict[str, int]]:
    value = invocation.context.get("codingStageRounds", {})
    if not isinstance(value, Mapping):
        raise ValueError("codingStageRounds is invalid")
    rounds: dict[str, int] = {}
    for node_id, next_round in value.items():
        if (
            not isinstance(node_id, str)
            or isinstance(next_round, bool)
            or not isinstance(next_round, int)
            or next_round < 1
        ):
            raise ValueError("codingStageRounds is invalid")
        rounds[node_id] = next_round
    current = rounds.get(invocation.node_id, 1)
    rounds[invocation.node_id] = current + 1
    return current, rounds


def _result_id(invocation: NodeInvocation, handler_key: str, round_number: int) -> str:
    identity = (
        f"axms:coding-result:{invocation.job_id}:{invocation.pipeline_attempt}:"
        f"{invocation.node_id}:{handler_key}:{round_number}"
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _approval_id(
    invocation: NodeInvocation,
    stage: str,
    round_number: int,
    *,
    pipeline_attempt: int | None = None,
) -> str:
    attempt = invocation.pipeline_attempt if pipeline_attempt is None else pipeline_attempt
    identity = (
        f"axms:coding-approval:{invocation.job_id}:{attempt}:"
        f"{invocation.node_id}:{stage}:{round_number}"
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _optional_match(value: Any, pattern: Any, field_name: str, maximum: int) -> None:
    if value is None:
        return
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or pattern.fullmatch(value) is None
    ):
        raise ValueError(f"{field_name} is invalid")


def _role_satisfies(actor_role: str, required_role: str) -> bool:
    if required_role == "GENERAL_ADMIN":
        return actor_role in {"GENERAL_ADMIN", "SUPER_ADMIN"}
    return required_role == "SUPER_ADMIN" and actor_role == "SUPER_ADMIN"


def _contract_failure(message: str) -> GraphExecutionError:
    return GraphExecutionError(
        "CONTRACT_VALIDATION_FAILED",
        message,
        retryable=False,
    )


def _state_conflict(message: str) -> GraphExecutionError:
    return GraphExecutionError(
        "JOB_STATE_VERSION_CONFLICT",
        message,
        retryable=False,
    )
