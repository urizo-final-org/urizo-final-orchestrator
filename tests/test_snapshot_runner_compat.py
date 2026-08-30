from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any
import unittest
from unittest.mock import Mock

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt

from axms_coding_orchestrator.contracts import (
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
)
from axms_coding_orchestrator.graph import CodingGraphRunner, GraphExecutionError
from axms_coding_orchestrator.graph_builder import SnapshotGraphBuilder
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
            "modelBindings": {},
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
            _node("fixture_work", "check", "fixture.work", ["fixture_ready"]),
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

    def test_interrupt_resumes_same_checkpoint_after_runner_restart(self) -> None:
        snapshot = _interrupt_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            resumed = interrupt({"fixture": "pause"})
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
            interrupt({"fixture": "pause", "jobId": invocation.job_id})
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
            interrupt({"fixture": "pause"})
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
        original = _interrupt_snapshot()
        changed_payload = original.to_dict()
        changed_payload["config"]["maxAttempts"] += 1
        changed = VersionedSnapshot.from_dict(changed_payload)
        log: list[tuple[str, NodeInvocation]] = []

        def pause(invocation: NodeInvocation) -> NodeResult:
            interrupt({"fixture": "pause"})
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
