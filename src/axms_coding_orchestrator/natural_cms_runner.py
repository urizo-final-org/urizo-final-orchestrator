"""Dedicated Natural CMS Snapshot execution from a strict queued job reference."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping, Protocol
from uuid import NAMESPACE_URL, uuid5

from langgraph.types import Command

from .contracts import QueuedJobReference
from .graph import GraphExecutionError
from .graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
    SnapshotGraphExecutionError,
)
from .natural_cms_domain_client import NaturalCmsDomainClient, NaturalCmsJob
from .node_runtime import NodeRegistry
from .snapshot import VersionedSnapshot


class _ProfileVersionReader(Protocol):
    def get(self, profile_version_id: str) -> VersionedSnapshot: ...


class NaturalCmsSnapshotRunner:
    """Run only Spring-owned Natural CMS Jobs on the shared Snapshot checkpointer."""

    __slots__ = ("_jobs", "_profiles", "_registry", "_checkpointer")

    def __init__(
        self,
        jobs: NaturalCmsDomainClient,
        profiles: _ProfileVersionReader,
        registry: NodeRegistry,
        checkpointer: Any,
    ) -> None:
        if not callable(getattr(jobs, "resolve_job", None)):
            raise TypeError("jobs must implement resolve_job(job)")
        if not callable(getattr(profiles, "get", None)):
            raise TypeError("profiles must implement get(profileVersionId)")
        if not isinstance(registry, NodeRegistry):
            raise TypeError("registry must be a NodeRegistry")
        if checkpointer is None:
            raise TypeError("NaturalCmsSnapshotRunner requires a checkpointer")
        self._jobs = jobs
        self._profiles = profiles
        self._registry = registry
        self._checkpointer = checkpointer

    def invoke(self, reference: QueuedJobReference) -> Mapping[str, Any]:
        if not isinstance(reference, QueuedJobReference):
            raise TypeError("reference must be a QueuedJobReference")
        job = self._jobs.resolve_job(reference)
        if job.status in {"COMPLETED", "REJECTED"}:
            return {"jobId": job.job_id, "status": "COMPLETED"}

        snapshot = self._profiles.get(job.profile_version_id)
        if snapshot.profile_key != "NATURAL_CMS":
            raise _contract_failure(
                "Natural CMS Job resolved a non-NATURAL_CMS Snapshot."
            )
        digest = "sha256:" + hashlib.sha256(snapshot.to_json()).hexdigest()
        try:
            graph = SnapshotGraphBuilder(self._registry).compile(
                snapshot,
                checkpointer=self._checkpointer,
            )
        except SnapshotGraphBuildError as failure:
            raise _contract_failure(str(failure)) from None
        config = {
            "configurable": {"thread_id": job.job_id},
            "recursion_limit": _recursion_limit(snapshot),
        }
        checkpoint = graph.get_state(config)
        values = getattr(checkpoint, "values", None)
        if not isinstance(values, Mapping) or not values:
            if job.status == "WAITING_APPROVAL":
                return _recover_spring_preview(
                    self._jobs,
                    reference,
                    graph,
                    config,
                    snapshot,
                    job,
                    digest,
                    None,
                )
            if job.status != "ACTIVE" or job.approval_decision is not None:
                raise _state_conflict(
                    "Natural CMS Job cannot start without an ACTIVE initial state."
                )
            initial = {
                **_execution_updates(job),
                "context": _initial_context(job, digest),
                "_snapshotProfileDigest": digest,
            }
            return _confirm_spring_terminal(
                self._jobs,
                reference,
                _invoke_graph(graph, initial, config),
            )

        _validate_checkpoint_identity(values, job, digest)
        phase = _checkpoint_phase(checkpoint)
        if phase == "COMPLETED":
            return _confirm_spring_terminal(
                self._jobs,
                reference,
                {**dict(values), "status": "COMPLETED"},
            )

        if job.status == "WAITING_APPROVAL" and phase != "WAITING_APPROVAL":
            return _recover_spring_preview(
                self._jobs,
                reference,
                graph,
                config,
                snapshot,
                job,
                digest,
                values,
            )

        previous_state_version = _positive_state(
            values.get("stateVersion"), "checkpoint stateVersion"
        )
        previous_pipeline_attempt = _positive_state(
            values.get("pipelineAttempt"), "checkpoint pipelineAttempt"
        )
        if phase == "WAITING_APPROVAL":
            if job.status != "WAITING_APPROVAL":
                raise _state_conflict(
                    "Natural CMS checkpoint is waiting for a Spring approval."
                )
            if job.approval_decision is None:
                if (
                    job.state_version != previous_state_version
                    or job.pipeline_attempt != previous_pipeline_attempt
                ):
                    raise _state_conflict(
                        "Natural CMS waiting Job changed without a decision."
                    )
                return {**dict(values), "status": "WAITING_APPROVAL"}
            if job.state_version != previous_state_version + 1:
                raise _state_conflict(
                    "Natural CMS approval did not advance stateVersion once."
                )
            expected_pipeline_attempt = previous_pipeline_attempt
            if (
                job.approval_decision == "REJECTED"
                and previous_pipeline_attempt < snapshot.config.max_attempts
            ):
                expected_pipeline_attempt += 1
            if job.pipeline_attempt != expected_pipeline_attempt:
                raise _state_conflict(
                    "Natural CMS approval changed pipelineAttempt unexpectedly."
                )
            return _confirm_spring_terminal(
                self._jobs,
                reference,
                _invoke_graph(
                    graph,
                    Command(resume=True, update=_execution_updates(job)),
                    config,
                ),
            )

        if (
            job.status != "ACTIVE"
            or job.approval_decision is not None
            or job.state_version != previous_state_version
            or job.pipeline_attempt != previous_pipeline_attempt
        ):
            raise _state_conflict(
                "Natural CMS running checkpoint changed outside its retry."
            )
        graph.update_state(config, _execution_updates(job))
        return _confirm_spring_terminal(
            self._jobs,
            reference,
            _invoke_graph(graph, None, config),
        )


def _execution_updates(job: NaturalCmsJob) -> dict[str, Any]:
    return {
        "jobId": job.job_id,
        "profileVersionId": job.profile_version_id,
        "pipelineAttempt": job.pipeline_attempt,
        "executionAttempt": job.state_version,
        "stateVersion": job.state_version,
        "traceId": job.trace_id,
        "workspaceId": None,
        "toolCallId": None,
    }


def _initial_context(job: NaturalCmsJob, digest: str) -> dict[str, Any]:
    return {
        # The digest binds the common syntactic guardrail to the exact validated
        # Snapshot; Spring remains the policy authority.
        "policyHash": digest,
        "requestText": job.request_text,
        "resource": {
            "type": job.resource.type,
            "id": job.resource.id,
        },
    }


def _recover_spring_preview(
    jobs: NaturalCmsDomainClient,
    reference: QueuedJobReference,
    graph: Any,
    config: Mapping[str, Any],
    snapshot: VersionedSnapshot,
    job: NaturalCmsJob,
    digest: str,
    previous: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    if (
        job.status != "WAITING_APPROVAL"
        or not job.preview_valid
        or job.preview_id is None
        or job.preview_hash is None
    ):
        raise _state_conflict(
            "Natural CMS WAITING_APPROVAL Job has no valid Spring preview."
        )
    preview_nodes = [
        node for node in snapshot.nodes if node.handler_key == "cms.preview"
    ]
    if len(preview_nodes) != 1:
        raise _contract_failure(
            "Natural CMS Snapshot must declare exactly one cms.preview node."
        )
    preview = preview_nodes[0]
    context = _initial_context(job, digest)
    if previous is not None:
        previous_context = previous.get("context")
        if isinstance(previous_context, Mapping):
            context.update(previous_context)
    raw_rounds = context.get("naturalCmsStageRounds", {})
    rounds = dict(raw_rounds) if isinstance(raw_rounds, Mapping) else {}
    for node in snapshot.nodes:
        if node.handler_key == "cms.analyze":
            rounds.setdefault(node.node_id, 1)
    previous_preview_round = rounds.get(preview.node_id, 0)
    if (
        isinstance(previous_preview_round, bool)
        or not isinstance(previous_preview_round, int)
        or previous_preview_round < 0
    ):
        raise _state_conflict("Natural CMS preview checkpoint round is invalid.")
    preview_round = previous_preview_round + 1
    rounds[preview.node_id] = preview_round
    context.update(
        {
            "policyHash": digest,
            "requestText": job.request_text,
            "resource": {
                "type": job.resource.type,
                "id": job.resource.id,
            },
            "naturalCmsStageRounds": rounds,
            "naturalCmsLastResult": {
                "resultId": str(
                    uuid5(
                        NAMESPACE_URL,
                        "axms:natural-cms-result:%s:%d:%s:%s:%d"
                        % (
                            job.job_id,
                            job.pipeline_attempt,
                            preview.node_id,
                            "cms.preview",
                            preview_round,
                        ),
                    )
                ),
                "handlerKey": "cms.preview",
                "resultPort": "ready",
                "resource": {
                    "type": job.resource.type,
                    "id": job.resource.id,
                },
                "previewId": job.preview_id,
                "previewHash": job.preview_hash,
            },
        }
    )
    graph.update_state(
        config,
        {
            **_execution_updates(job),
            "context": context,
            "_snapshotProfileDigest": digest,
            "_snapshotLastNodeId": preview.node_id,
            "_snapshotLastResultPort": "ready",
        },
        as_node=preview.node_id,
    )
    waiting = _invoke_graph(graph, None, config)
    if job.approval_decision is None:
        return waiting
    return _confirm_spring_terminal(
        jobs,
        reference,
        _invoke_graph(
            graph,
            Command(resume=True, update=_execution_updates(job)),
            config,
        ),
    )


def _confirm_spring_terminal(
    jobs: NaturalCmsDomainClient,
    reference: QueuedJobReference,
    result: Mapping[str, Any],
) -> Mapping[str, Any]:
    if result.get("status") != "COMPLETED":
        return result
    current = jobs.resolve_job(reference)
    if current.status not in {"COMPLETED", "REJECTED"}:
        raise GraphExecutionError(
            "NATURAL_CMS_TERMINAL_PENDING",
            "The Natural CMS graph completed before Spring reached a terminal state.",
            retryable=True,
        )
    return result


def _validate_checkpoint_identity(
    values: Mapping[str, Any],
    job: NaturalCmsJob,
    digest: str,
) -> None:
    if (
        values.get("jobId") != job.job_id
        or values.get("traceId") != job.trace_id
        or values.get("profileVersionId") != job.profile_version_id
        or values.get("_snapshotProfileDigest") != digest
    ):
        raise _state_conflict("The Natural CMS checkpoint identity changed.")


def _checkpoint_phase(checkpoint: Any) -> str:
    tasks = getattr(checkpoint, "tasks", ())
    if any(getattr(task, "interrupts", ()) for task in tasks):
        return "WAITING_APPROVAL"
    if getattr(checkpoint, "next", ()) == ():
        return "COMPLETED"
    return "RUNNING"


def _invoke_graph(
    graph: Any,
    value: Any,
    config: Mapping[str, Any],
) -> Mapping[str, Any]:
    try:
        result = graph.invoke(value, config=config)
    except SnapshotGraphExecutionError as failure:
        raise _contract_failure(str(failure)) from None
    if not isinstance(result, Mapping):
        raise _contract_failure("Natural CMS Snapshot returned an invalid state.")
    phase = _checkpoint_phase(graph.get_state(config))
    if phase not in {"WAITING_APPROVAL", "COMPLETED"}:
        raise _state_conflict(
            "Natural CMS Snapshot stopped before a terminal boundary."
        )
    return {**dict(result), "status": phase}


def _recursion_limit(snapshot: VersionedSnapshot) -> int:
    iterations = sum(limit.max_iterations for limit in snapshot.config.loop_limits)
    return len(snapshot.nodes) * (1 + iterations) + 1


def _positive_state(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _state_conflict(f"Natural CMS {field} is invalid.")
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
