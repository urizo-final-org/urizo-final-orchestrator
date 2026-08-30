"""Worker-compatible adapters for current and Versioned Snapshot graphs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
from types import MappingProxyType
from typing import Any, Mapping, Protocol
from uuid import UUID

from langgraph.types import Command

from .contracts import CodingJobRequested, WorkerClaim
from .graph import CodingGraphRunner, GraphExecutionError
from .graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
    SnapshotGraphExecutionError,
)
from .node_runtime import NodeRegistry
from .snapshot import VersionedSnapshot


class WorkerGraphRunner(Protocol):
    """The stable graph surface consumed by the single WorkerLoop."""

    def is_duplicate(self, event: CodingJobRequested) -> bool: ...

    def invoke(
        self, event: CodingJobRequested, claim: WorkerClaim
    ) -> Mapping[str, Any]: ...


class CodingGraphRunnerAdapter:
    """Preserve the current Coding runner behind the common Worker surface."""

    __slots__ = ("_runner",)

    def __init__(self, runner: CodingGraphRunner) -> None:
        if not isinstance(runner, CodingGraphRunner):
            raise TypeError("runner must be a CodingGraphRunner")
        self._runner = runner

    def is_duplicate(self, event: CodingJobRequested) -> bool:
        return self._runner.is_duplicate(event)

    def invoke(
        self, event: CodingJobRequested, claim: WorkerClaim
    ) -> Mapping[str, Any]:
        return self._runner.invoke(event, claim)


class ProfileBoundWorkerGraphRouter:
    """Route only Spring profile-bound Jobs to the Snapshot production path."""

    __slots__ = ("_legacy", "_snapshot")

    def __init__(
        self,
        legacy: WorkerGraphRunner,
        snapshot: WorkerGraphRunner,
    ) -> None:
        for name, runner in (("legacy", legacy), ("snapshot", snapshot)):
            if not callable(getattr(runner, "is_duplicate", None)) or not callable(
                getattr(runner, "invoke", None)
            ):
                raise TypeError(f"{name} must implement the WorkerGraphRunner contract")
        self._legacy = legacy
        self._snapshot = snapshot

    def is_duplicate(self, event: CodingJobRequested) -> bool:
        return self._select(event).is_duplicate(event)

    def invoke(
        self, event: CodingJobRequested, claim: WorkerClaim
    ) -> Mapping[str, Any]:
        return self._select(event).invoke(event, claim)

    def _select(self, event: CodingJobRequested) -> WorkerGraphRunner:
        if not isinstance(event, CodingJobRequested):
            raise TypeError("event must be a CodingJobRequested")
        return self._snapshot if event.is_profile_bound else self._legacy


class SnapshotExecutionProvider(Protocol):
    """Resolve a fixture or future Spring-owned execution by immutable job ID."""

    def resolve(self, event: CodingJobRequested) -> SnapshotExecution: ...


class _FactoryOnly:
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise TypeError("SnapshotExecution must be created through create")


@dataclass(frozen=True, slots=True, init=False)
class SnapshotExecution(_FactoryOnly):
    """Provider-resolved runtime bindings kept outside Snapshot JSON."""

    snapshot: VersionedSnapshot
    pipeline_attempt: int
    execution_attempt: int
    workspace_id: str | None
    tool_call_id: str | None
    _context: Mapping[str, Any] = field(repr=False, compare=False)

    @classmethod
    def create(
        cls,
        snapshot: VersionedSnapshot,
        *,
        pipeline_attempt: int,
        execution_attempt: int,
        context: Mapping[str, Any] | None = None,
        workspace_id: str | None = None,
        tool_call_id: str | None = None,
    ) -> SnapshotExecution:
        if not isinstance(snapshot, VersionedSnapshot):
            raise ValueError("snapshot execution requires a VersionedSnapshot")
        result = object.__new__(cls)
        object.__setattr__(result, "snapshot", snapshot)
        object.__setattr__(
            result,
            "pipeline_attempt",
            _positive_integer(pipeline_attempt, "pipelineAttempt"),
        )
        object.__setattr__(
            result,
            "execution_attempt",
            _positive_integer(execution_attempt, "executionAttempt"),
        )
        object.__setattr__(
            result,
            "workspace_id",
            _optional_uuid(workspace_id, "workspaceId"),
        )
        object.__setattr__(
            result,
            "tool_call_id",
            _optional_uuid(tool_call_id, "toolCallId"),
        )
        source = {} if context is None else context
        if not isinstance(source, Mapping) or not all(
            isinstance(key, str) for key in source
        ):
            raise ValueError("snapshot execution context must be an object")
        object.__setattr__(
            result,
            "_context",
            MappingProxyType(deepcopy(dict(source))),
        )
        return result

    @property
    def context(self) -> dict[str, Any]:
        return deepcopy(dict(self._context))

    def __repr__(self) -> str:
        return (
            "SnapshotExecution[profileVersionId=%s, pipelineAttempt=%d, "
            "executionAttempt=%d, context=REDACTED]"
            % (
                self.snapshot.profile_version_id,
                self.pipeline_attempt,
                self.execution_attempt,
            )
        )


class SnapshotGraphRunner:
    """Run provider-resolved Snapshots through the existing Worker surface."""

    __slots__ = ("_provider", "_registry", "_checkpointer")

    def __init__(
        self,
        provider: SnapshotExecutionProvider,
        registry: NodeRegistry,
        checkpointer: Any,
    ) -> None:
        if not callable(getattr(provider, "resolve", None)):
            raise TypeError("provider must implement resolve(event)")
        if not isinstance(registry, NodeRegistry):
            raise TypeError("registry must be a NodeRegistry")
        if checkpointer is None:
            raise TypeError("SnapshotGraphRunner requires a checkpointer")
        self._provider = provider
        self._registry = registry
        self._checkpointer = checkpointer

    def is_duplicate(self, event: CodingJobRequested) -> bool:
        try:
            execution, graph, config, digest = self._runtime(event)
        except GraphExecutionError as failure:
            if failure.retryable:
                raise
            return False
        checkpoint = graph.get_state(config)
        values = getattr(checkpoint, "values", None)
        if not isinstance(values, Mapping) or not values:
            return False
        ledger = values.get("_snapshotLedger")
        if not isinstance(ledger, Mapping):
            return False
        try:
            validated_ledger = _checkpoint_ledger(values)
        except GraphExecutionError:
            return False
        event_ids = validated_ledger["eventIds"]
        maximum = validated_ledger["maxStateVersion"]
        if event.expected_state_version < maximum:
            return True
        if (
            values.get("profileVersionId") != execution.snapshot.profile_version_id
            or values.get("_snapshotProfileDigest") != digest
            or event.expected_state_version != maximum
            or event.event_id not in event_ids
            or _checkpoint_phase(checkpoint) not in {"WAITING_APPROVAL", "COMPLETED"}
        ):
            return False
        recorded = values.get("_snapshotEvent")
        try:
            return (
                isinstance(recorded, Mapping)
                and CodingJobRequested.from_dict(recorded).to_dict() == event.to_dict()
            )
        except Exception:
            return False

    def invoke(
        self, event: CodingJobRequested, claim: WorkerClaim
    ) -> Mapping[str, Any]:
        execution, graph, config, digest = self._runtime(event)
        checkpoint = graph.get_state(config)
        values = getattr(checkpoint, "values", None)
        if isinstance(values, Mapping) and values:
            _validate_checkpoint_identity(values, event, execution, digest)
            ledger = _checkpoint_ledger(values)
            maximum = ledger["maxStateVersion"]
            event_ids = ledger["eventIds"]
            same_delivery = (
                event.expected_state_version == maximum
                and event.event_id in event_ids
            )
            phase = _checkpoint_phase(checkpoint)
            if same_delivery:
                _validate_recovered_delivery(values, event, claim, execution)
                if phase in {"WAITING_APPROVAL", "COMPLETED"}:
                    return {**dict(values), "status": phase}
                graph.update_state(
                    config,
                    _execution_updates(event, claim, execution, ledger),
                )
                return _invoke_graph(graph, None, config)

            if not claim.resume:
                raise _state_conflict(
                    "The Snapshot checkpoint requires a resume claim."
                )
            if event.expected_state_version <= maximum:
                raise _state_conflict("The Snapshot retry event is stale.")
            if phase == "COMPLETED":
                raise _state_conflict("The Snapshot graph is already complete.")
            updated_ledger = {
                "eventIds": [*event_ids, event.event_id][-100:],
                "maxStateVersion": event.expected_state_version,
            }
            updates = _execution_updates(event, claim, execution, updated_ledger)
            if phase == "WAITING_APPROVAL":
                return _invoke_graph(
                    graph,
                    Command(resume=True, update=updates),
                    config,
                )
            graph.update_state(config, updates)
            return _invoke_graph(graph, None, config)

        if claim.resume:
            raise _state_conflict("The Snapshot resume claim has no checkpoint.")
        initial = {
            **_execution_updates(
                event,
                claim,
                execution,
                {
                    "eventIds": [event.event_id],
                    "maxStateVersion": event.expected_state_version,
                },
            ),
            "context": execution.context,
            "_snapshotProfileDigest": digest,
        }
        return _invoke_graph(graph, initial, config)

    def _runtime(
        self, event: CodingJobRequested
    ) -> tuple[SnapshotExecution, Any, dict[str, Any], str]:
        execution = self._provider.resolve(event)
        if not isinstance(execution, SnapshotExecution):
            raise _contract_failure("Snapshot provider returned an invalid execution.")
        digest = "sha256:" + hashlib.sha256(execution.snapshot.to_json()).hexdigest()
        try:
            graph = SnapshotGraphBuilder(self._registry).compile(
                execution.snapshot,
                checkpointer=self._checkpointer,
            )
        except SnapshotGraphBuildError as failure:
            raise _contract_failure(str(failure)) from None
        config = {
            "configurable": {"thread_id": event.job_id},
            "recursion_limit": _recursion_limit(execution.snapshot),
        }
        return execution, graph, config, digest


def _execution_updates(
    event: CodingJobRequested,
    claim: WorkerClaim,
    execution: SnapshotExecution,
    ledger: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "jobId": event.job_id,
        "profileVersionId": execution.snapshot.profile_version_id,
        "pipelineAttempt": execution.pipeline_attempt,
        "executionAttempt": execution.execution_attempt,
        "stateVersion": claim.state_version,
        "traceId": claim.trace_id,
        "workspaceId": execution.workspace_id,
        "toolCallId": execution.tool_call_id,
        "_snapshotEvent": event.to_dict(),
        "_snapshotClaim": claim.to_dict(),
        "_snapshotLedger": dict(ledger),
    }


def _validate_checkpoint_identity(
    values: Mapping[str, Any],
    event: CodingJobRequested,
    execution: SnapshotExecution,
    digest: str,
) -> None:
    if (
        values.get("jobId") != event.job_id
        or values.get("traceId") != event.trace_id
        or values.get("profileVersionId") != execution.snapshot.profile_version_id
        or values.get("_snapshotProfileDigest") != digest
    ):
        raise _state_conflict("The Snapshot checkpoint identity changed.")


def _validate_recovered_delivery(
    values: Mapping[str, Any],
    event: CodingJobRequested,
    claim: WorkerClaim,
    execution: SnapshotExecution,
) -> None:
    recorded_event = values.get("_snapshotEvent")
    recorded_claim = values.get("_snapshotClaim")
    try:
        if not isinstance(recorded_event, Mapping) or not isinstance(
            recorded_claim, Mapping
        ):
            raise ValueError
        old_event = CodingJobRequested.from_dict(recorded_event)
        old_claim = WorkerClaim.from_dict(recorded_claim, old_event)
    except Exception:
        raise _state_conflict("The Snapshot checkpoint delivery is invalid.") from None
    if old_event.to_dict() != event.to_dict():
        raise _state_conflict("The recovered Snapshot delivery changed.")
    previous = old_claim.to_dict()
    refreshed = claim.to_dict()
    immutable_claim_fields = {
        "jobId",
        "traceId",
        "leaseId",
        "stateVersion",
        "snapshot",
    }
    if any(previous[field] != refreshed[field] for field in immutable_claim_fields):
        raise _state_conflict("The recovered Snapshot claim authority changed.")
    if (
        values.get("pipelineAttempt") != execution.pipeline_attempt
        or values.get("executionAttempt") != execution.execution_attempt
        or values.get("workspaceId") != execution.workspace_id
        or values.get("toolCallId") != execution.tool_call_id
    ):
        raise _state_conflict("The recovered Snapshot execution binding changed.")


def _checkpoint_ledger(values: Mapping[str, Any]) -> dict[str, Any]:
    ledger = values.get("_snapshotLedger")
    if not isinstance(ledger, Mapping):
        raise _state_conflict("The Snapshot checkpoint has no delivery ledger.")
    event_ids = ledger.get("eventIds")
    maximum = ledger.get("maxStateVersion")
    if (
        not isinstance(event_ids, list)
        or not event_ids
        or len(event_ids) > 100
        or not all(isinstance(event_id, str) for event_id in event_ids)
        or isinstance(maximum, bool)
        or not isinstance(maximum, int)
        or maximum < 1
    ):
        raise _state_conflict("The Snapshot checkpoint delivery ledger is invalid.")
    return {"eventIds": list(event_ids), "maxStateVersion": maximum}


def _checkpoint_phase(checkpoint: Any) -> str:
    tasks = getattr(checkpoint, "tasks", ())
    if any(getattr(task, "interrupts", ()) for task in tasks):
        return "WAITING_APPROVAL"
    pending = getattr(checkpoint, "next", ())
    if pending == ():
        return "COMPLETED"
    return "RUNNING"


def _invoke_graph(
    graph: Any, value: Any, config: Mapping[str, Any]
) -> Mapping[str, Any]:
    try:
        result = graph.invoke(value, config=config)
    except SnapshotGraphExecutionError as failure:
        raise _contract_failure(str(failure)) from None
    if not isinstance(result, Mapping):
        raise _contract_failure("Snapshot graph returned an invalid state.")
    checkpoint = graph.get_state(config)
    phase = _checkpoint_phase(checkpoint)
    if phase not in {"WAITING_APPROVAL", "COMPLETED"}:
        raise _state_conflict("Snapshot graph stopped before a terminal boundary.")
    return {**dict(result), "status": phase}


def _recursion_limit(snapshot: VersionedSnapshot) -> int:
    iterations = sum(limit.max_iterations for limit in snapshot.config.loop_limits)
    return len(snapshot.nodes) * (1 + iterations) + 1


def _positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} is invalid")
    return value


def _optional_uuid(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise ValueError(f"{field_name} is invalid") from None
    return value


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
