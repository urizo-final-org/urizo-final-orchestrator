from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any
import unittest
from unittest.mock import Mock

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt

from axms_coding_orchestrator.coding_handlers import (
    CodingHandlerDependencies,
    PreparedResultCodingStageExecutor,
    register_coding_node_handlers,
)
from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.contracts import (
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
)
from axms_coding_orchestrator.graph import CodingGraphRunner, GraphExecutionError
from axms_coding_orchestrator.graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
)
from axms_coding_orchestrator.node_runtime import (
    NodeInvocation,
    NodeRegistry,
    NodeResult,
)
from axms_coding_orchestrator.service import HealthState, WorkerLoop
from axms_coding_orchestrator.snapshot import VersionedSnapshot
from axms_coding_orchestrator.snapshot_runner import (
    CodingGraphRunnerAdapter,
    ProfileBoundWorkerGraphRouter,
    SnapshotExecution,
    SnapshotGraphRunner,
    WorkerGraphRunner,
)
from axms_coding_orchestrator.worker_api import WorkerApiError

from factories import FIXED_NOW, coding_event, worker_claim


JOB_ID = "20202020-2020-4020-8020-202020202020"
OTHER_JOB_ID = "21212121-2121-4121-8121-212121212121"
PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
OTHER_PROFILE_VERSION_ID = "99999999-9999-4999-8999-999999999999"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
OTHER_TRACE_ID = "31313131-3131-4131-8131-313131313131"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
TOOL_CALL_ID = "50505050-5050-4050-8050-505050505050"


Handler = Callable[[NodeInvocation], NodeResult]


def _node(
    node_id: str,
    node_type: str,
    handler_key: str,
    result_ports: Iterable[str],
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": node_type,
        "handlerKey": handler_key,
        "resultPorts": list(result_ports),
        "config": dict(config or {}),
    }


def _edge(source: str, port: str, target: str) -> dict[str, str]:
    return {"from": source, "resultPort": port, "to": target}


def _snapshot(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, str]],
    *,
    profile_version_id: str = PROFILE_VERSION_ID,
    loop_limits: list[dict[str, Any]] | None = None,
    model_bindings: Mapping[str, Any] | None = None,
) -> VersionedSnapshot:
    return VersionedSnapshot.from_dict(
        {
            "contractVersion": "1.0",
            "profileVersionId": profile_version_id,
            "profileKey": "LLM_OPS",
            "profileVersion": 1,
            "nodes": nodes,
            "edges": edges,
            "config": {
                "maxNodes": 12,
                "maxAttempts": 3,
                "loopLimits": list(loop_limits or []),
            },
            "modelBindings": dict(model_bindings or {}),
            "toolPolicy": {},
            "guardrailProfileKey": "fixture.locked",
        }
    )


def _linear_snapshot(
    profile_version_id: str = PROFILE_VERSION_ID,
) -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node("fixture_work", "check", "fixture.work", ["fixture_done"]),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_work"),
            _edge("fixture_work", "fixture_done", "fixture_end"),
        ],
        profile_version_id=profile_version_id,
    )


def _interrupt_snapshot(
    profile_version_id: str = PROFILE_VERSION_ID,
    *,
    work_node_type: str = "check",
    model_bindings: Mapping[str, Any] | None = None,
) -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node("fixture_work", work_node_type, "fixture.work", ["fixture_ready"]),
            _node(
                "fixture_pause",
                "approval",
                "fixture.pause",
                ["fixture_resumed"],
            ),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_work"),
            _edge("fixture_work", "fixture_ready", "fixture_pause"),
            _edge("fixture_pause", "fixture_resumed", "fixture_end"),
        ],
        profile_version_id=profile_version_id,
        model_bindings=model_bindings,
    )


def _common_approval_snapshot() -> VersionedSnapshot:
    return _snapshot(
        [
            _node("start", "start", "common.start", ["next"]),
            _node(
                "guardrail",
                "guardrail",
                "common.guardrail",
                ["passed", "failed"],
                {"locked": True},
            ),
            _node(
                "check",
                "check",
                "common.check",
                ["passed", "failed"],
            ),
            _node(
                "approval",
                "approval",
                "common.approval",
                ["approved"],
            ),
            _node("end", "end", "common.end", []),
        ],
        [
            _edge("start", "next", "guardrail"),
            _edge("guardrail", "passed", "check"),
            _edge("guardrail", "failed", "end"),
            _edge("check", "passed", "approval"),
            _edge("check", "failed", "end"),
            _edge("approval", "approved", "end"),
        ],
    )


def _common_success_snapshot() -> VersionedSnapshot:
    return _snapshot(
        [
            _node("start", "start", "common.start", ["next"]),
            _node(
                "guardrail",
                "guardrail",
                "common.guardrail",
                ["passed", "failed"],
                {"locked": True},
            ),
            _node("check", "check", "common.check", ["passed", "failed"]),
            _node("end", "end", "common.end", []),
        ],
        [
            _edge("start", "next", "guardrail"),
            _edge("guardrail", "passed", "check"),
            _edge("guardrail", "failed", "end"),
            _edge("check", "passed", "end"),
            _edge("check", "failed", "end"),
        ],
    )


def _common_approval_test_registry() -> NodeRegistry:
    production = build_common_node_registry()
    registry = NodeRegistry()
    for handler_key in production.registered_keys:
        if handler_key == "common.approval":
            continue
        registration = production.resolve(handler_key)
        registry.register(
            handler_key,
            node_types=registration.node_types,
            result_ports=registration.result_ports,
            handler=registration.handler,
            config_validator=registration.config_validator,
        )

    def approval(invocation: NodeInvocation) -> NodeResult:
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
        if decision is not True:
            raise AssertionError("fixture approval received an invalid decision")
        return NodeResult.create("approved")

    return registry.register(
        "common.approval",
        node_types=["approval"],
        result_ports=["approved"],
        handler=approval,
        config_validator=lambda config: (
            None if not config else "fixture approval config is invalid"
        ),
    )


def _retry_snapshot() -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node("fixture_stable", "check", "fixture.stable", ["fixture_next"]),
            _node("fixture_flaky", "check", "fixture.flaky", ["fixture_done"]),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_stable"),
            _edge("fixture_stable", "fixture_next", "fixture_flaky"),
            _edge("fixture_flaky", "fixture_done", "fixture_end"),
        ],
    )


def _loop_snapshot(max_iterations: int) -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node(
                "fixture_work",
                "check",
                "fixture.work",
                ["fixture_repeat", "fixture_done"],
            ),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_work"),
            _edge("fixture_work", "fixture_repeat", "fixture_guardrail"),
            _edge("fixture_work", "fixture_done", "fixture_end"),
        ],
        loop_limits=[
            {
                "from": "fixture_work",
                "resultPort": "fixture_repeat",
                "to": "fixture_guardrail",
                "maxIterations": max_iterations,
            }
        ],
    )


def _fixed_handler(
    log: list[tuple[str, NodeInvocation]],
    name: str,
    port: str | None,
    updates: Mapping[str, Any] | None = None,
) -> Handler:
    def run(invocation: NodeInvocation) -> NodeResult:
        log.append((name, invocation))
        return NodeResult.create(port, updates)

    return run


def _fixture_pending_approval(invocation: NodeInvocation) -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "jobId": invocation.job_id,
        "profileVersionId": invocation.profile_version_id,
        "nodeId": invocation.node_id,
        "traceId": invocation.trace_id,
        "stateVersion": invocation.state_version,
    }


def _registry(
    snapshot: VersionedSnapshot, handlers: Mapping[str, Handler]
) -> NodeRegistry:
    registry = NodeRegistry()
    for node in snapshot.nodes:
        registry.register(
            node.handler_key,
            node_types=[node.node_type],
            result_ports=node.result_ports,
            handler=handlers[node.handler_key],
        )
    return registry


def _execution(
    snapshot: VersionedSnapshot,
    *,
    pipeline_attempt: int = 1,
    execution_attempt: int = 1,
    context: Mapping[str, Any] | None = None,
) -> SnapshotExecution:
    return SnapshotExecution.create(
        snapshot=snapshot,
        pipeline_attempt=pipeline_attempt,
        execution_attempt=execution_attempt,
        context={} if context is None else context,
        workspace_id=WORKSPACE_ID,
        tool_call_id=TOOL_CALL_ID,
    )


class _Provider:
    def __init__(
        self,
        default: SnapshotExecution,
        executions: Mapping[str, SnapshotExecution] | None = None,
    ) -> None:
        self.default = default
        self.executions = dict(executions or {})
        self.calls: list[str] = []

    def resolve(self, event: CodingJobRequested) -> SnapshotExecution:
        self.calls.append(event.event_id)
        return self.executions.get(event.event_id, self.default)


def _event(
    *,
    event_id: str = "10101010-1010-4010-8010-101010101010",
    version: int = 4,
    attempt: int = 1,
    job_id: str = JOB_ID,
    trace_id: str = TRACE_ID,
) -> CodingJobRequested:
    payload = coding_event(event_id=event_id, version=version, attempt=attempt)
    payload["jobId"] = job_id
    payload["traceId"] = trace_id
    return CodingJobRequested.from_dict(payload)


def _claim(
    event: CodingJobRequested,
    *,
    resume: bool = False,
    state_version: int | None = None,
    lease_id: str | None = None,
) -> WorkerClaim:
    payload = worker_claim(
        event.to_dict(), resume=resume, state_version=state_version
    )
    if lease_id is not None:
        payload["leaseId"] = lease_id
    return WorkerClaim.from_dict(payload, event, now=FIXED_NOW)


class _UnusedQueue:
    pass


class _OutcomeWorker:
    def __init__(
        self,
        event: CodingJobRequested,
        claim: WorkerClaim,
        *,
        outcome_failures: int = 0,
    ) -> None:
        self.event = event
        self.authoritative_claim = claim
        self.outcome_failures = outcome_failures
        self.claim_calls = 0
        self.outcomes: list[tuple[str, str | None]] = []
        self.outcome_keys: list[str] = []

    def resolve(self, job: QueuedJobReference) -> CodingJobRequested:
        assert job.job_id == self.event.job_id
        return self.event

    def claim(self, event: CodingJobRequested) -> WorkerClaim:
        del event
        self.claim_calls += 1
        return self.authoritative_claim

    def outcome(
        self,
        claim: WorkerClaim,
        outcome: str,
        idempotency_key: str,
        *,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        del claim
        self.outcomes.append((outcome, error_code))
        self.outcome_keys.append(idempotency_key)
        if self.outcome_failures > 0:
            self.outcome_failures -= 1
            raise WorkerApiError(
                "INTERNAL_TRANSIENT_ERROR",
                "safe transient outcome failure",
                retryable=True,
            )
        return {}


class _Heartbeat:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.stopped: list[str] = []

    def start(self, claim: WorkerClaim) -> None:
        self.started.append(claim.job_id)

    def ensure_current(self, claim: WorkerClaim) -> None:
        del claim

    def stop(self, job_id: str) -> None:
        self.stopped.append(job_id)


class SnapshotRunnerCompatibilityTest(unittest.TestCase):
    def test_common_approval_is_rejected_by_the_production_registry(self) -> None:
        with self.assertRaisesRegex(
            SnapshotGraphBuildError, "common.approval is not supported"
        ):
            SnapshotGraphBuilder(build_common_node_registry()).compile(
                _common_approval_snapshot()
            )

    def test_common_handler_configs_are_validated_before_execution(self) -> None:
        invalid_configs = {
            "start": {"unknown": True},
            "guardrail": {"locked": True, "unknown": True},
            "check": {"unknown": True},
            "end": {"unknown": True},
        }

        for node_id, config in invalid_configs.items():
            payload = _common_success_snapshot().to_dict()
            node = next(item for item in payload["nodes"] if item["id"] == node_id)
            node["config"] = config
            snapshot = VersionedSnapshot.from_dict(payload)

            with self.subTest(node_id=node_id), self.assertRaisesRegex(
                SnapshotGraphBuildError, "config"
            ):
                SnapshotGraphBuilder(build_common_node_registry()).compile(snapshot)

    def test_common_success_snapshot_completes_through_the_worker_loop(self) -> None:
        snapshot = _common_success_snapshot()
        event = _event()
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(
            _Provider(_execution(snapshot, context=event.job_payload)),
            build_common_node_registry(),
            checkpointer,
        )
        worker = _OutcomeWorker(event, _claim(event))
        heartbeat = _Heartbeat()
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,  # type: ignore[arg-type]
            runner,
            heartbeat,  # type: ignore[arg-type]
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )

        self.assertTrue(
            loop.process(QueuedJobReference.from_dict({"jobId": event.job_id}))
        )
        self.assertEqual([("COMPLETED", None)], worker.outcomes)
        self.assertEqual([event.job_id], heartbeat.started)
        self.assertEqual([event.job_id], heartbeat.stopped)
        self.assertEqual({event.job_id}, set(checkpointer.storage))
        self.assertTrue(runner.is_duplicate(event))

    def test_common_failure_ports_must_route_directly_to_end(self) -> None:
        original = _common_approval_snapshot()

        for source in ("guardrail", "check"):
            payload = original.to_dict()
            edge = next(
                item
                for item in payload["edges"]
                if item["from"] == source and item["resultPort"] == "failed"
            )
            edge["to"] = "approval"
            snapshot = VersionedSnapshot.from_dict(payload)

            with self.subTest(source=source), self.assertRaisesRegex(
                SnapshotGraphBuildError,
                "failed port must route directly to end",
            ):
                SnapshotGraphBuilder(_common_approval_test_registry()).compile(snapshot)

    def test_common_invalid_or_missing_digest_terminates_without_approval_interrupt(
        self,
    ) -> None:
        snapshot = _common_approval_snapshot()
        event = _event()

        for field, value in (
            ("policyHash", "not-a-digest"),
            ("contextDigest", None),
        ):
            context = event.job_payload
            if value is None:
                context.pop(field)
            else:
                context[field] = value
            runner = SnapshotGraphRunner(
                _Provider(_execution(snapshot, context=context)),
                _common_approval_test_registry(),
                InMemorySaver(),
            )

            with self.subTest(field=field):
                completed = runner.invoke(event, _claim(event))
                self.assertEqual("COMPLETED", completed["status"])
                self.assertNotIn("__interrupt__", completed)

    def test_common_approval_resumes_the_same_checkpoint_as_approved(self) -> None:
        snapshot = _common_approval_snapshot()
        event = _event()
        provider = _Provider(_execution(snapshot, context=event.job_payload))
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(
            provider,
            _common_approval_test_registry(),
            checkpointer,
        )
        waiting = runner.invoke(event, _claim(event))

        self.assertEqual("WAITING_APPROVAL", waiting["status"])
        self.assertIn("__interrupt__", waiting)
        self.assertEqual({event.job_id}, set(checkpointer.storage))

        resume_event = _event(
            event_id="41414141-4141-4141-8141-414141414141",
            version=6,
        )
        restarted = SnapshotGraphRunner(
            provider,
            _common_approval_test_registry(),
            checkpointer,
        )
        completed = restarted.invoke(
            resume_event,
            _claim(resume_event, resume=True, state_version=7),
        )

        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual(event.job_id, completed["jobId"])
        self.assertEqual(PROFILE_VERSION_ID, completed["profileVersionId"])
        self.assertTrue(restarted.is_duplicate(resume_event))

    def test_waiting_technical_retry_cannot_approve_before_same_attempt_resume(
        self,
    ) -> None:
        snapshot = _common_approval_snapshot()
        initial_event = _event()
        retry_event = _event(
            event_id="42424242-4242-4242-8242-424242424242",
            version=6,
            attempt=2,
        )
        same_attempt_without_approval = _event(
            event_id="43434343-4343-4343-8343-434343434343",
            version=8,
            attempt=2,
        )
        legacy_retry_event = _event(
            event_id="47474747-4747-4747-8747-474747474747",
            version=8,
            attempt=3,
        )
        approval_event = _event(
            event_id="48484848-4848-4848-8848-484848484848",
            version=10,
            attempt=3,
        )
        initial_execution = _execution(
            snapshot,
            context=initial_event.job_payload,
        )
        retry_execution = _execution(
            snapshot,
            execution_attempt=2,
            context=initial_event.job_payload,
        )
        third_execution = _execution(
            snapshot,
            execution_attempt=3,
            context=initial_event.job_payload,
        )
        provider = _Provider(
            initial_execution,
            {
                retry_event.event_id: retry_execution,
                same_attempt_without_approval.event_id: retry_execution,
                legacy_retry_event.event_id: third_execution,
                approval_event.event_id: third_execution,
            },
        )
        checkpointer = InMemorySaver()
        registry = _common_approval_test_registry()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        waiting = runner.invoke(initial_event, _claim(initial_event))
        self.assertEqual("WAITING_APPROVAL", waiting["status"])

        technical_retry = SnapshotGraphRunner(
            provider, registry, checkpointer
        ).invoke(
            retry_event,
            _claim(retry_event, resume=False, state_version=7),
        )

        self.assertEqual("WAITING_APPROVAL", technical_retry["status"])
        self.assertEqual(2, technical_retry["executionAttempt"])
        self.assertEqual(
            6,
            technical_retry["_snapshotLedger"]["maxStateVersion"],
        )
        self.assertEqual({initial_event.job_id}, set(checkpointer.storage))

        with self.assertRaisesRegex(
            GraphExecutionError, "waiting Snapshot approval"
        ) as failure:
            runner.invoke(
                same_attempt_without_approval,
                _claim(
                    same_attempt_without_approval,
                    resume=False,
                    state_version=9,
                ),
            )
        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)

        legacy_retry = SnapshotGraphRunner(
            provider, registry, checkpointer
        ).invoke(
            legacy_retry_event,
            _claim(legacy_retry_event, resume=True, state_version=9),
        )
        self.assertEqual("WAITING_APPROVAL", legacy_retry["status"])
        self.assertEqual(3, legacy_retry["executionAttempt"])

        completed = SnapshotGraphRunner(provider, registry, checkpointer).invoke(
            approval_event,
            _claim(approval_event, resume=True, state_version=11),
        )

        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual(initial_event.job_id, completed["jobId"])
        self.assertEqual(PROFILE_VERSION_ID, completed["profileVersionId"])

    def test_completed_checkpoint_recovers_only_a_newer_technical_attempt(
        self,
    ) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_done"),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        initial_event = _event()
        recovery_event = _event(
            event_id="44444444-4444-4444-8444-444444444444",
            version=6,
            attempt=2,
        )
        same_attempt_event = _event(
            event_id="45454545-4545-4545-8545-454545454545",
            version=8,
            attempt=2,
        )
        lower_attempt_event = _event(
            event_id="46464646-4646-4646-8646-464646464646",
            version=8,
        )
        recovery_execution = _execution(snapshot, execution_attempt=2)
        provider = _Provider(
            _execution(snapshot),
            {
                recovery_event.event_id: recovery_execution,
                same_attempt_event.event_id: recovery_execution,
            },
        )
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        first = runner.invoke(initial_event, _claim(initial_event))
        completed_log = list(log)
        self.assertEqual("COMPLETED", first["status"])

        recovered = SnapshotGraphRunner(provider, registry, checkpointer).invoke(
            recovery_event,
            _claim(recovery_event, resume=False, state_version=7),
        )

        self.assertEqual("COMPLETED", recovered["status"])
        self.assertEqual(initial_event.job_id, recovered["jobId"])
        self.assertEqual(PROFILE_VERSION_ID, recovered["profileVersionId"])
        self.assertEqual(2, recovered["executionAttempt"])
        self.assertEqual(6, recovered["_snapshotLedger"]["maxStateVersion"])
        self.assertEqual(completed_log, log)
        self.assertTrue(runner.is_duplicate(recovery_event))

        replayed = runner.invoke(
            recovery_event,
            _claim(recovery_event, resume=False, state_version=7),
        )
        self.assertEqual("COMPLETED", replayed["status"])
        self.assertEqual(completed_log, log)

        for event, execution in (
            (same_attempt_event, recovery_execution),
            (lower_attempt_event, _execution(snapshot)),
        ):
            provider.executions[event.event_id] = execution
            with self.subTest(execution_attempt=execution.execution_attempt):
                with self.assertRaises(GraphExecutionError) as failure:
                    runner.invoke(
                        event,
                        _claim(event, resume=True, state_version=9),
                    )
                self.assertEqual(
                    "JOB_STATE_VERSION_CONFLICT", failure.exception.code
                )
                self.assertEqual(completed_log, log)

    def test_fresh_higher_attempt_starts_only_without_resume(self) -> None:
        snapshot = _linear_snapshot()
        event = _event(attempt=2)
        execution = _execution(snapshot, execution_attempt=2)
        log: list[tuple[str, NodeInvocation]] = []
        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_done"),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        registry = _registry(snapshot, handlers)

        completed = SnapshotGraphRunner(
            _Provider(execution), registry, InMemorySaver()
        ).invoke(event, _claim(event, resume=False))
        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual(2, completed["executionAttempt"])

        with self.assertRaisesRegex(
            GraphExecutionError, "resume claim has no checkpoint"
        ) as failure:
            SnapshotGraphRunner(
                _Provider(execution), registry, InMemorySaver()
            ).invoke(event, _claim(event, resume=True))
        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)

    def test_running_checkpoint_rejects_a_new_same_attempt_delivery(self) -> None:
        snapshot = _retry_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def failing(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.flaky", invocation))
            raise RuntimeError("fixture transient failure")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.stable": _fixed_handler(
                log, "fixture.stable", "fixture_next"
            ),
            "fixture.flaky": failing,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        initial_event = _event()
        different_event = _event(
            event_id="49494949-4949-4949-8949-494949494949",
            version=6,
        )
        provider = _Provider(_execution(snapshot))
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        with self.assertRaisesRegex(RuntimeError, "fixture transient"):
            runner.invoke(initial_event, _claim(initial_event))

        graph = SnapshotGraphBuilder(registry).compile(
            snapshot, checkpointer=checkpointer
        )
        config = {"configurable": {"thread_id": initial_event.job_id}}
        before = graph.get_state(config)
        before_values = dict(before.values)
        before_log = list(log)

        with self.assertRaisesRegex(
            GraphExecutionError, "same attempt"
        ) as failure:
            runner.invoke(
                different_event,
                _claim(different_event, resume=True, state_version=7),
            )

        after = graph.get_state(config)
        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)
        self.assertEqual(before_values, dict(after.values))
        self.assertEqual(before.next, after.next)
        self.assertEqual(before_log, log)

    def test_terminal_delivery_replays_outcome_without_rerunning_handlers(
        self,
    ) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(
                log,
                "fixture.work",
                "fixture_done",
                {"fixture_value": "complete"},
            ),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(
            provider, _registry(snapshot, handlers), checkpointer
        )
        event = _event()

        completed = runner.invoke(event, _claim(event))

        self.assertEqual("complete", completed["context"]["fixture_value"])
        self.assertEqual(
            ["fixture.start", "fixture.guardrail", "fixture.work", "fixture.end"],
            [name for name, _ in log],
        )
        self.assertEqual({event.job_id}, set(checkpointer.storage))
        self.assertTrue(runner.is_duplicate(event))
        stale = _event(
            event_id="12121212-1212-4212-8212-121212121212",
            version=3,
        )
        self.assertTrue(runner.is_duplicate(stale))
        future = _event(
            event_id="13131313-1313-4313-8313-131313131313",
            version=6,
        )
        self.assertFalse(runner.is_duplicate(future))

        worker = _OutcomeWorker(event, _claim(event), outcome_failures=1)
        heartbeat = _Heartbeat()
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,  # type: ignore[arg-type]
            runner,
            heartbeat,  # type: ignore[arg-type]
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )
        job = QueuedJobReference.from_dict({"jobId": event.job_id})

        self.assertFalse(loop.process(job))
        self.assertTrue(loop.process(job))

        self.assertEqual(2, worker.claim_calls)
        self.assertEqual(
            [("COMPLETED", None), ("COMPLETED", None)],
            worker.outcomes,
        )
        self.assertEqual(1, len(set(worker.outcome_keys)))
        self.assertEqual(
            ["fixture.start", "fixture.guardrail", "fixture.work", "fixture.end"],
            [name for name, _ in log],
        )
        self.assertEqual([event.job_id, event.job_id], heartbeat.started)

    def test_permanent_snapshot_contract_failure_is_reported_after_claim(self) -> None:
        snapshot = _linear_snapshot()
        runner = SnapshotGraphRunner(
            _Provider(_execution(snapshot)), NodeRegistry(), InMemorySaver()
        )
        event = _event()
        worker = _OutcomeWorker(event, _claim(event))
        heartbeat = _Heartbeat()
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,  # type: ignore[arg-type]
            runner,
            heartbeat,  # type: ignore[arg-type]
            HealthState(),
            queue_block_seconds=1,
            max_attempts=3,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )

        self.assertTrue(
            loop.process(QueuedJobReference.from_dict({"jobId": event.job_id}))
        )
        self.assertEqual(1, worker.claim_calls)
        self.assertEqual(
            [("PERMANENT_FAILURE", "CONTRACT_VALIDATION_FAILED")],
            worker.outcomes,
        )

    def test_production_registry_rejects_unregistered_handler_after_claim(self) -> None:
        snapshot = _snapshot(
            [
                _node("start", "start", "common.start", ["next"]),
                _node(
                    "guardrail",
                    "guardrail",
                    "common.guardrail",
                    ["passed", "failed"],
                    {"locked": True},
                ),
                _node(
                    "rework_gate",
                    "check",
                    "coding.rework_gate",
                    ["retry", "handover"],
                    {"maxReworkRounds": 3},
                ),
                _node(
                    "unregistered",
                    "check",
                    "coding.unregistered",
                    ["completed"],
                ),
                _node("end", "end", "common.end", []),
            ],
            [
                _edge("start", "next", "guardrail"),
                _edge("guardrail", "passed", "rework_gate"),
                _edge("guardrail", "failed", "end"),
                _edge("rework_gate", "retry", "unregistered"),
                _edge("rework_gate", "handover", "end"),
                _edge("unregistered", "completed", "end"),
            ],
        )
        registry = register_coding_node_handlers(
            build_common_node_registry(),
            CodingHandlerDependencies(Mock(), PreparedResultCodingStageExecutor()),
        )
        runner = SnapshotGraphRunner(
            _Provider(_execution(snapshot)), registry, InMemorySaver()
        )
        event = _event()
        worker = _OutcomeWorker(event, _claim(event))
        heartbeat = _Heartbeat()
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,  # type: ignore[arg-type]
            runner,
            heartbeat,  # type: ignore[arg-type]
            HealthState(),
            queue_block_seconds=1,
            max_attempts=3,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )

        self.assertTrue(
            loop.process(QueuedJobReference.from_dict({"jobId": event.job_id}))
        )
        self.assertEqual(1, worker.claim_calls)
        self.assertEqual(
            [("PERMANENT_FAILURE", "CONTRACT_VALIDATION_FAILED")],
            worker.outcomes,
        )
        self.assertEqual([event.job_id], heartbeat.started)
        self.assertEqual([event.job_id], heartbeat.stopped)

    def test_interrupt_resumes_same_checkpoint_after_runner_restart(self) -> None:
        snapshot = _interrupt_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        pending_approval = {
            "schemaVersion": "1.0",
            "approvalId": "61616161-6161-4161-8161-616161616161",
            "jobId": JOB_ID,
            "profileVersionId": PROFILE_VERSION_ID,
            "nodeId": "fixture_pause",
            "stage": "SCOPE",
            "stageRound": 1,
            "requiredRole": "GENERAL_ADMIN",
            "pipelineAttempt": 1,
            "traceId": TRACE_ID,
            "stateVersion": 5,
        }

        def pause(invocation: NodeInvocation) -> NodeResult:
            resumed = interrupt(pending_approval)
            log.append(("fixture.pause", invocation))
            return NodeResult.create(
                "fixture_resumed", {"fixture_resume_seen": resumed is not None}
            )

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_ready"),
            "fixture.pause": pause,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)
        event = _event()

        waiting = runner.invoke(event, _claim(event))

        self.assertIn("__interrupt__", waiting)
        self.assertEqual(pending_approval, waiting["pendingApproval"])
        self.assertTrue(runner.is_duplicate(event))
        self.assertEqual(
            ["fixture.start", "fixture.guardrail", "fixture.work"],
            [name for name, _ in log],
        )

        restarted = SnapshotGraphRunner(provider, registry, checkpointer)
        resume_event = _event(
            event_id="14141414-1414-4414-8414-141414141414",
            version=6,
        )
        completed = restarted.invoke(
            resume_event,
            _claim(resume_event, resume=True, state_version=7),
        )

        self.assertTrue(completed["context"]["fixture_resume_seen"])
        self.assertEqual(PROFILE_VERSION_ID, completed["profileVersionId"])
        self.assertEqual(
            [
                "fixture.start",
                "fixture.guardrail",
                "fixture.work",
                "fixture.pause",
                "fixture.end",
            ],
            [name for name, _ in log],
        )
        self.assertTrue(restarted.is_duplicate(resume_event))

    def test_partial_coding_approval_authority_fails_at_runner_boundary(
        self,
    ) -> None:
        snapshot = _interrupt_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            payload = _fixture_pending_approval(invocation)
            payload["approvalId"] = "61616161-6161-4161-8161-616161616161"
            interrupt(payload)
            return NodeResult.create("fixture_resumed")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_ready"),
            "fixture.pause": pause,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        event = _event()
        runner = SnapshotGraphRunner(
            _Provider(_execution(snapshot)),
            _registry(snapshot, handlers),
            InMemorySaver(),
        )

        with self.assertRaisesRegex(
            GraphExecutionError,
            "approval interrupt is invalid",
        ) as raised:
            runner.invoke(event, _claim(event))

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)

    def test_retry_continues_failed_node_without_replaying_completed_nodes(
        self,
    ) -> None:
        snapshot = _retry_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        flaky_attempts: list[NodeInvocation] = []

        def flaky(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.flaky", invocation))
            flaky_attempts.append(invocation)
            if len(flaky_attempts) == 1:
                raise RuntimeError("fixture transient failure")
            return NodeResult.create("fixture_done")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.stable": _fixed_handler(
                log, "fixture.stable", "fixture_next"
            ),
            "fixture.flaky": flaky,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        event = _event()
        retry_event = _event(
            event_id="15151515-1515-4515-8515-151515151515",
            version=6,
            attempt=2,
        )
        first_execution = _execution(snapshot)
        retry_execution = _execution(snapshot, execution_attempt=2)
        provider = _Provider(
            first_execution,
            {retry_event.event_id: retry_execution},
        )
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        with self.assertRaisesRegex(RuntimeError, "fixture transient"):
            runner.invoke(event, _claim(event))

        restarted = SnapshotGraphRunner(provider, registry, checkpointer)
        completed = restarted.invoke(
            retry_event,
            _claim(retry_event, resume=True, state_version=7),
        )

        self.assertEqual("fixture_end", completed["_snapshotLastNodeId"])
        names = [name for name, _ in log]
        self.assertEqual(1, names.count("fixture.start"))
        self.assertEqual(1, names.count("fixture.guardrail"))
        self.assertEqual(1, names.count("fixture.stable"))
        self.assertEqual(2, names.count("fixture.flaky"))
        self.assertEqual(1, names.count("fixture.end"))
        self.assertEqual([1, 1], [item.pipeline_attempt for item in flaky_attempts])
        self.assertEqual([1, 2], [item.execution_attempt for item in flaky_attempts])
        self.assertTrue(restarted.is_duplicate(retry_event))

    def test_recovered_same_delivery_continues_its_failed_node(self) -> None:
        snapshot = _retry_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        failures = 1

        def flaky(invocation: NodeInvocation) -> NodeResult:
            nonlocal failures
            log.append(("fixture.flaky", invocation))
            if failures:
                failures -= 1
                raise RuntimeError("fixture transient failure")
            return NodeResult.create("fixture_done")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.stable": _fixed_handler(
                log, "fixture.stable", "fixture_next"
            ),
            "fixture.flaky": flaky,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)
        event = _event()
        initial_claim = _claim(event)

        with self.assertRaisesRegex(RuntimeError, "fixture transient"):
            runner.invoke(event, initial_claim)

        recovered_payload = initial_claim.to_dict()
        recovered_payload["resume"] = True
        recovered_payload["leaseExpiresAt"] = "2026-08-11T10:20:00Z"
        recovered_claim = WorkerClaim.from_dict(
            recovered_payload, event, now=FIXED_NOW
        )
        completed = SnapshotGraphRunner(provider, registry, checkpointer).invoke(
            event, recovered_claim
        )

        self.assertEqual("fixture_end", completed["_snapshotLastNodeId"])
        names = [name for name, _ in log]
        self.assertEqual(1, names.count("fixture.start"))
        self.assertEqual(1, names.count("fixture.guardrail"))
        self.assertEqual(1, names.count("fixture.stable"))
        self.assertEqual(2, names.count("fixture.flaky"))
        self.assertEqual(1, names.count("fixture.end"))

    def test_two_jobs_keep_independent_interrupted_checkpoints(self) -> None:
        snapshot = _interrupt_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            interrupt(_fixture_pending_approval(invocation))
            log.append(("fixture.pause", invocation))
            return NodeResult.create("fixture_resumed")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_ready"),
            "fixture.pause": pause,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)
        first = _event()
        second = _event(
            event_id="16161616-1616-4616-8616-161616161616",
            job_id=OTHER_JOB_ID,
            trace_id=OTHER_TRACE_ID,
        )

        runner.invoke(first, _claim(first))
        runner.invoke(
            second,
            _claim(
                second,
                lease_id="72727272-7272-4272-8272-727272727272",
            ),
        )

        self.assertEqual({JOB_ID, OTHER_JOB_ID}, set(checkpointer.storage))
        self.assertTrue(runner.is_duplicate(first))
        self.assertTrue(runner.is_duplicate(second))
        work_jobs = [
            invocation.job_id for name, invocation in log if name == "fixture.work"
        ]
        self.assertEqual([JOB_ID, OTHER_JOB_ID], work_jobs)

        first_resume = _event(
            event_id="17171717-1717-4717-8717-171717171717",
            version=6,
        )
        runner.invoke(
            first_resume,
            _claim(first_resume, resume=True, state_version=7),
        )
        self.assertTrue(runner.is_duplicate(second))
        self.assertEqual(
            {JOB_ID: 1, OTHER_JOB_ID: 1},
            {
                job_id: sum(
                    1
                    for name, invocation in log
                    if name == "fixture.work" and invocation.job_id == job_id
                )
                for job_id in (JOB_ID, OTHER_JOB_ID)
            },
        )

        second_resume = _event(
            event_id="18181818-1818-4818-8818-181818181818",
            version=6,
            job_id=OTHER_JOB_ID,
            trace_id=OTHER_TRACE_ID,
        )
        runner.invoke(
            second_resume,
            _claim(
                second_resume,
                resume=True,
                state_version=7,
                lease_id="73737373-7373-4373-8373-737373737373",
            ),
        )
        self.assertEqual(
            1,
            sum(
                1
                for name, invocation in log
                if name == "fixture.work" and invocation.job_id == OTHER_JOB_ID
            ),
        )

    def test_declared_thirty_iteration_loop_completes_past_default_recursion(
        self,
    ) -> None:
        maximum = 30
        snapshot = _loop_snapshot(maximum)
        log: list[tuple[str, NodeInvocation]] = []
        work_calls = 0

        def work(invocation: NodeInvocation) -> NodeResult:
            nonlocal work_calls
            work_calls += 1
            log.append(("fixture.work", invocation))
            port = "fixture_repeat" if work_calls <= maximum else "fixture_done"
            return NodeResult.create(port)

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": work,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        runner = SnapshotGraphRunner(
            provider, _registry(snapshot, handlers), InMemorySaver()
        )
        event = _event()

        completed = runner.invoke(event, _claim(event))

        self.assertEqual("fixture_end", completed["_snapshotLastNodeId"])
        self.assertEqual(maximum + 1, work_calls)
        self.assertEqual(
            maximum + 1,
            sum(name == "fixture.guardrail" for name, _ in log),
        )

    def test_resume_rejects_profile_drift_before_resumed_handler(self) -> None:
        original = _interrupt_snapshot()
        changed = _interrupt_snapshot(OTHER_PROFILE_VERSION_ID)
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            interrupt(_fixture_pending_approval(invocation))
            log.append(("fixture.pause", invocation))
            return NodeResult.create("fixture_resumed")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_ready"),
            "fixture.pause": pause,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        event = _event()
        resume_event = _event(
            event_id="19191919-1919-4919-8919-191919191919",
            version=6,
        )
        provider = _Provider(
            _execution(original),
            {resume_event.event_id: _execution(changed)},
        )
        registry = _registry(original, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        runner.invoke(event, _claim(event))
        before_resume = list(log)

        with self.assertRaisesRegex(
            (RuntimeError, ValueError), "checkpoint identity"
        ) as failure:
            runner.invoke(
                resume_event,
                _claim(resume_event, resume=True, state_version=7),
            )

        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)
        self.assertEqual(before_resume, log)

    def test_resume_rejects_same_profile_id_content_drift_by_digest(self) -> None:
        original = _interrupt_snapshot(
            work_node_type="agent",
            model_bindings={
                "fixture_work": {
                    "selections": {
                        "provider": "OPENAI",
                        "model": "gpt-5.6-terra",
                        "inference": {
                            "reasoningIntensity": "medium",
                            "reasoningBudgetTokens": 1024,
                        },
                    }
                }
            },
        )
        changed_payload = original.to_dict()
        changed_payload["modelBindings"] = {
            "fixture_work": {
                "selections": {
                    "provider": "OPENAI",
                    "model": "gpt-5.6-terra",
                    "inference": {
                        "reasoningIntensity": "medium",
                        "reasoningBudgetTokens": 2048,
                    },
                }
            }
        }
        changed = VersionedSnapshot.from_dict(changed_payload)
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            interrupt(_fixture_pending_approval(invocation))
            log.append(("fixture.pause", invocation))
            return NodeResult.create("fixture_resumed")

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_ready"),
            "fixture.pause": pause,
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        event = _event()
        resume_event = _event(
            event_id="29292929-2929-4929-8929-292929292929",
            version=6,
        )
        provider = _Provider(
            _execution(original),
            {resume_event.event_id: _execution(changed)},
        )
        registry = _registry(original, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)

        runner.invoke(event, _claim(event))
        before_resume = list(log)

        with self.assertRaisesRegex(
            GraphExecutionError, "checkpoint identity"
        ) as failure:
            runner.invoke(
                resume_event,
                _claim(resume_event, resume=True, state_version=7),
            )

        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)
        self.assertEqual(before_resume, log)

    def test_malformed_checkpoint_ledger_is_never_treated_as_duplicate(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.work": _fixed_handler(log, "fixture.work", "fixture_done"),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        provider = _Provider(_execution(snapshot))
        registry = _registry(snapshot, handlers)
        checkpointer = InMemorySaver()
        runner = SnapshotGraphRunner(provider, registry, checkpointer)
        event = _event(version=1)
        claim = _claim(event)
        runner.invoke(event, claim)
        graph = SnapshotGraphBuilder(registry).compile(
            snapshot, checkpointer=checkpointer
        )
        graph.update_state(
            {"configurable": {"thread_id": event.job_id}},
            {
                "_snapshotLedger": {
                    "eventIds": [event.event_id],
                    "maxStateVersion": True,
                }
            },
        )

        self.assertFalse(runner.is_duplicate(event))
        with self.assertRaisesRegex(
            GraphExecutionError, "ledger is invalid"
        ) as failure:
            runner.invoke(event, claim)
        self.assertEqual("JOB_STATE_VERSION_CONFLICT", failure.exception.code)

    def test_current_coding_runner_adapter_preserves_worker_shape(self) -> None:
        event = _event()
        claim = _claim(event)
        legacy = Mock(spec=CodingGraphRunner)
        legacy.is_duplicate.return_value = True
        legacy.invoke.return_value = {"status": "fixture"}

        adapter: WorkerGraphRunner = CodingGraphRunnerAdapter(legacy)

        self.assertTrue(adapter.is_duplicate(event))
        self.assertEqual({"status": "fixture"}, adapter.invoke(event, claim))
        legacy.is_duplicate.assert_called_once_with(event)
        legacy.invoke.assert_called_once_with(event, claim)

    def test_profile_bound_router_selects_snapshot_and_preserves_legacy(self) -> None:
        bound = _event()
        legacy_payload = bound.to_dict()
        for field in (
            "profileVersionId",
            "pipelineAttempt",
            "executionAttempt",
            "workspaceId",
            "toolCallId",
        ):
            legacy_payload.pop(field)
        legacy_event = CodingJobRequested.from_dict(legacy_payload)
        claim = _claim(bound)
        legacy = Mock()
        snapshot = Mock()
        legacy.is_duplicate.return_value = False
        snapshot.is_duplicate.return_value = True
        snapshot.invoke.return_value = {"status": "COMPLETED"}
        router = ProfileBoundWorkerGraphRouter(legacy, snapshot)

        self.assertTrue(router.is_duplicate(bound))
        self.assertFalse(router.is_duplicate(legacy_event))
        self.assertEqual(
            {"status": "COMPLETED"},
            router.invoke(bound, claim),
        )
        snapshot.is_duplicate.assert_called_once_with(bound)
        legacy.is_duplicate.assert_called_once_with(legacy_event)
        snapshot.invoke.assert_called_once_with(bound, claim)
        legacy.invoke.assert_not_called()


if __name__ == "__main__":
    unittest.main()
