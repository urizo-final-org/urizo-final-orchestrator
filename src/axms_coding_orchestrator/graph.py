"""The single authoritative LangGraph coding workflow."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
from typing import Any, Mapping, Protocol, TypedDict
from uuid import NAMESPACE_URL, uuid5

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from .contracts import CodingJobRequested, WorkerClaim
from .model_gateway import ModelGatewayClient, ModelTurnRequest
from .tool_gateway import (
    READ_FILE_SCHEMA_DIGEST,
    ToolExecutionResult,
    ToolGatewayClient,
    ToolGatewayError,
    build_read_file_request,
)
from .worker_api import WorkerApiClient


READ_FILE_INPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["path"],
    "properties": {"path": {"type": "string"}},
}


class CodingGraphState(TypedDict, total=False):
    event: dict[str, Any]
    claim: dict[str, Any]
    ledger: dict[str, Any]
    modelResponse: dict[str, Any]
    toolResult: dict[str, Any]
    status: str


class LeaseGuard(Protocol):
    def ensure_current(self, claim: WorkerClaim) -> None: ...

    def stop(self, job_id: str) -> None: ...


class NoopLeaseGuard:
    def ensure_current(self, claim: WorkerClaim) -> None:
        del claim

    def stop(self, job_id: str) -> None:
        del job_id


@dataclass(frozen=True, slots=True)
class GraphDependencies:
    model_gateway: ModelGatewayClient
    tool_gateway: ToolGatewayClient
    worker_api: WorkerApiClient
    lease_guard: LeaseGuard = NoopLeaseGuard()


class GraphExecutionError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def build_coding_graph(checkpointer: Any, dependencies: GraphDependencies) -> Any:
    builder: StateGraph[CodingGraphState] = StateGraph(CodingGraphState)

    def model_turn_node(state: CodingGraphState) -> dict[str, Any]:
        event = CodingJobRequested.from_dict(state["event"])
        claim = WorkerClaim.from_dict(state["claim"], event)
        dependencies.lease_guard.ensure_current(claim)
        snapshot = claim.snapshot.to_dict()
        required = {"CHAT", "TOOL_CALLING"}
        if not required <= set(snapshot["allowedCapabilities"]):
            raise GraphExecutionError(
                "MODEL_CAPABILITY_UNSUPPORTED",
                "The approved coding snapshot does not allow the single graph capabilities.",
                retryable=False,
            )
        if "plan" not in snapshot["allowedNodes"]:
            raise GraphExecutionError(
                "SERVICE_AUTHORIZATION_DENIED",
                "The approved coding snapshot does not allow the plan node.",
                retryable=False,
            )
        identity = "%s|%s|%d|%d|plan" % (
            event.job_id,
            event.event_id,
            event.attempt,
            event.expected_state_version,
        )
        turn_id = str(uuid5(NAMESPACE_URL, "axms:model:" + identity))
        idempotency_key = "model." + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        request = ModelTurnRequest.from_dict(
            {
                "schemaVersion": "1.0",
                "turnId": turn_id,
                "jobId": claim.job_id,
                "traceId": claim.trace_id,
                "idempotencyKey": idempotency_key,
                "attempt": event.attempt,
                "expectedStateVersion": claim.state_version,
                "nodeName": "plan",
                "promptVersion": snapshot["promptVersion"],
                "contextDigest": snapshot["contextDigest"],
                "requiredCapabilities": ["CHAT", "TOOL_CALLING"],
                "messages": [
                    {"role": "system", "content": snapshot["systemPrompt"]},
                    {"role": "user", "content": snapshot["userPrompt"]},
                ],
                "toolSchemas": [
                    {
                        "name": "read_file",
                        "description": "Read the single approved repository-relative file.",
                        "inputSchema": deepcopy(READ_FILE_INPUT_SCHEMA),
                        "schemaDigest": READ_FILE_SCHEMA_DIGEST,
                    }
                ],
                "responseFormat": {"type": "TEXT"},
                "deadlineAt": snapshot["deadlineAt"],
            }
        )
        response = dependencies.model_gateway.execute(request).to_dict()
        tool_calls = response["toolCalls"]
        if len(tool_calls) != 1 or tool_calls[0].get("name") != "read_file":
            raise GraphExecutionError(
                "MODEL_RESPONSE_INVALID",
                "The model did not return the single approved read_file candidate.",
                retryable=False,
            )
        arguments = tool_calls[0].get("arguments")
        if not isinstance(arguments, Mapping) or set(arguments) != {"path"} or arguments["path"] != snapshot["toolPath"]:
            raise GraphExecutionError(
                "PATH_POLICY_DENIED",
                "The model candidate did not preserve the approved tool path.",
                retryable=False,
            )
        return {"modelResponse": response, "status": "MODEL_TURN_COMPLETED"}

    def tool_node(state: CodingGraphState) -> dict[str, Any]:
        event = CodingJobRequested.from_dict(state["event"])
        claim = WorkerClaim.from_dict(state["claim"], event)
        dependencies.lease_guard.ensure_current(claim)
        tool_calls = state["modelResponse"]["toolCalls"]
        request = build_read_file_request(event, claim, tool_calls[0])
        try:
            result = dependencies.tool_gateway.execute_read_file(request)
        except ToolGatewayError as failure:
            raise GraphExecutionError(
                failure.code,
                "Spring Tool Gateway did not complete the approved read.",
                retryable=failure.retryable,
            ) from None
        return {"toolResult": _tool_result_dict(result), "status": "TOOL_COMPLETED"}

    def waiting_node(state: CodingGraphState) -> dict[str, Any]:
        event = CodingJobRequested.from_dict(state["event"])
        claim = WorkerClaim.from_dict(state["claim"], event)
        dependencies.lease_guard.ensure_current(claim)
        key = _outcome_key(claim, "waiting")
        dependencies.worker_api.outcome(claim, "WAITING_APPROVAL", key)
        dependencies.lease_guard.stop(claim.job_id)
        return {"status": "WAITING_APPROVAL"}

    def approval_node(state: CodingGraphState) -> dict[str, Any]:
        old_event = CodingJobRequested.from_dict(state["event"])
        old_claim = WorkerClaim.from_dict(state["claim"], old_event)
        snapshot = old_claim.snapshot.to_dict()
        resume_value = interrupt(
            {
                "schemaVersion": "1.0",
                "jobId": old_claim.job_id,
                "traceId": old_claim.trace_id,
                "approvalId": snapshot["approvalId"],
                "candidateSha": snapshot["baseSha"],
                "policyHash": snapshot["policyHash"],
            }
        )
        if not isinstance(resume_value, Mapping) or set(resume_value) != {
            "approved",
            "event",
            "claim",
        }:
            raise GraphExecutionError(
                "CONTRACT_VALIDATION_FAILED",
                "Approval resume payload is invalid.",
                retryable=False,
            )
        if resume_value["approved"] is not True:
            raise GraphExecutionError(
                "TOOL_APPROVAL_DENIED", "Approval was not granted.", retryable=False
            )
        event = CodingJobRequested.from_dict(resume_value["event"])
        claim = WorkerClaim.from_dict(resume_value["claim"], event)
        resumed_snapshot = claim.snapshot.to_dict()
        bound_fields = {
            "actor",
            "project",
            "repository",
            "graphStep",
            "baseSha",
            "contextDigest",
            "policyHash",
            "promptVersion",
            "allowedCapabilities",
            "allowedNodes",
            "systemPrompt",
            "userPrompt",
            "toolPath",
            "approvalId",
        }
        if (
            not claim.resume
            or event.job_id != old_event.job_id
            or event.expected_state_version <= state["ledger"]["maxStateVersion"]
            or any(resumed_snapshot[field] != snapshot[field] for field in bound_fields)
        ):
            raise GraphExecutionError(
                "JOB_STATE_VERSION_CONFLICT",
                "Approval resume does not match the interrupted job.",
                retryable=False,
            )
        ledger = deepcopy(state["ledger"])
        ledger["eventIds"] = [*ledger["eventIds"], event.event_id][-100:]
        ledger["maxStateVersion"] = max(
            ledger["maxStateVersion"], event.expected_state_version
        )
        return {
            "event": event.to_dict(),
            "claim": claim.to_dict(),
            "ledger": ledger,
            "status": "RUNNING",
        }

    def complete_node(state: CodingGraphState) -> dict[str, Any]:
        event = CodingJobRequested.from_dict(state["event"])
        claim = WorkerClaim.from_dict(state["claim"], event)
        dependencies.lease_guard.ensure_current(claim)
        key = _outcome_key(claim, "completed")
        dependencies.worker_api.outcome(claim, "COMPLETED", key)
        dependencies.lease_guard.stop(claim.job_id)
        return {"status": "COMPLETED"}

    builder.add_node("model_turn", model_turn_node)
    builder.add_node("read_file", tool_node)
    builder.add_node("mark_waiting", waiting_node)
    builder.add_node("approval", approval_node)
    builder.add_node("complete", complete_node)
    builder.add_edge(START, "model_turn")
    builder.add_edge("model_turn", "read_file")
    builder.add_edge("read_file", "mark_waiting")
    builder.add_edge("mark_waiting", "approval")
    builder.add_edge("approval", "complete")
    builder.add_edge("complete", END)
    return builder.compile(checkpointer=checkpointer)


class CodingGraphRunner:
    def __init__(self, graph: Any) -> None:
        self._graph = graph

    def is_duplicate(self, event: CodingJobRequested) -> bool:
        snapshot = self._graph.get_state(_config(event.job_id))
        values = getattr(snapshot, "values", None)
        if not isinstance(values, Mapping):
            return False
        ledger = values.get("ledger")
        if not isinstance(ledger, Mapping):
            return False
        event_ids = ledger.get("eventIds", [])
        version = ledger.get("maxStateVersion", 0)
        if not isinstance(event_ids, list) or not isinstance(version, int):
            return False
        if event.expected_state_version < version:
            return True
        if (
            event.expected_state_version != version
            or event.event_id not in event_ids
            or values.get("status") not in {"WAITING_APPROVAL", "COMPLETED"}
        ):
            return False
        recorded_event = values.get("event")
        try:
            return (
                isinstance(recorded_event, Mapping)
                and CodingJobRequested.from_dict(recorded_event).to_dict()
                == event.to_dict()
            )
        except Exception:
            return False

    def invoke(self, event: CodingJobRequested, claim: WorkerClaim) -> Mapping[str, Any]:
        config = _config(event.job_id)
        snapshot = self._graph.get_state(config)
        values = getattr(snapshot, "values", None)
        if isinstance(values, Mapping) and values:
            ledger = values.get("ledger")
            if not isinstance(ledger, Mapping):
                raise GraphExecutionError(
                    "JOB_STATE_VERSION_CONFLICT",
                    "The coding checkpoint has no delivery ledger.",
                    retryable=False,
                )
            maximum = ledger.get("maxStateVersion")
            event_ids = ledger.get("eventIds")
            same_delivery = (
                isinstance(maximum, int)
                and isinstance(event_ids, list)
                and event.expected_state_version == maximum
                and event.event_id in event_ids
            )
            if same_delivery:
                recorded_event = CodingJobRequested.from_dict(values["event"])
                recorded_claim = WorkerClaim.from_dict(values["claim"], recorded_event)
                if recorded_event.to_dict() != event.to_dict():
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The recovered delivery differs from its checkpoint.",
                        retryable=False,
                    )
                previous = recorded_claim.to_dict()
                refreshed = claim.to_dict()
                immutable_claim_fields = {
                    "jobId",
                    "traceId",
                    "leaseId",
                    "stateVersion",
                    "snapshot",
                }
                if any(previous[field] != refreshed[field] for field in immutable_claim_fields):
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The recovered claim differs from its checkpoint authority.",
                        retryable=False,
                    )
                if values.get("status") in {"WAITING_APPROVAL", "COMPLETED"}:
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The recovered delivery is already finalized.",
                        retryable=False,
                    )
                self._graph.update_state(
                    config,
                    {
                        "event": event.to_dict(),
                        "claim": claim.to_dict(),
                    },
                )
                return self._graph.invoke(None, config=config)
        if claim.resume:
            if isinstance(values, Mapping) and values.get("status") == "WAITING_APPROVAL":
                return self._graph.invoke(
                    Command(
                        resume={
                            "approved": True,
                            "event": event.to_dict(),
                            "claim": claim.to_dict(),
                        }
                    ),
                    config=config,
                )
            if isinstance(values, Mapping) and values:
                if values.get("status") == "COMPLETED":
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The coding graph is already complete.",
                        retryable=False,
                    )
                ledger = values.get("ledger")
                if not isinstance(ledger, Mapping):
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The coding retry checkpoint has no delivery ledger.",
                        retryable=False,
                    )
                maximum = ledger.get("maxStateVersion")
                event_ids = ledger.get("eventIds")
                if (
                    not isinstance(maximum, int)
                    or not isinstance(event_ids, list)
                    or event.expected_state_version <= maximum
                ):
                    raise GraphExecutionError(
                        "JOB_STATE_VERSION_CONFLICT",
                        "The coding retry event is stale.",
                        retryable=False,
                    )
                updated_ledger = {
                    "eventIds": [*event_ids, event.event_id][-100:],
                    "maxStateVersion": event.expected_state_version,
                }
                self._graph.update_state(
                    config,
                    {
                        "event": event.to_dict(),
                        "claim": claim.to_dict(),
                        "ledger": updated_ledger,
                        "status": "RUNNING",
                    },
                )
                return self._graph.invoke(None, config=config)
        initial: CodingGraphState = {
            "event": event.to_dict(),
            "claim": claim.to_dict(),
            "ledger": {
                "eventIds": [event.event_id],
                "maxStateVersion": event.expected_state_version,
            },
            "status": "RUNNING",
        }
        return self._graph.invoke(initial, config=config)


def _config(job_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": job_id}}


def _tool_result_dict(result: ToolExecutionResult) -> dict[str, Any]:
    return {
        "executionId": result.execution_id,
        "toolCallId": result.tool_call_id,
        "mediaType": result.media_type,
        "digest": result.digest,
        "sizeBytes": result.size_bytes,
        "content": result.content,
    }


def _outcome_key(claim: WorkerClaim, scope: str) -> str:
    identity = "%s|%s|%d|%s" % (
        claim.job_id,
        claim.lease_id,
        claim.state_version,
        scope,
    )
    return "outcome." + hashlib.sha256(identity.encode("utf-8")).hexdigest()
