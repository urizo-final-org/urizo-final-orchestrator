from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator
import unittest

from langgraph.checkpoint.memory import InMemorySaver

from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.default_natural_cms_snapshot import (
    DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
    default_natural_cms_snapshot,
)
from axms_coding_orchestrator.graph import GraphExecutionError
from axms_coding_orchestrator.natural_cms_domain_client import (
    NaturalCmsDomainClientError,
    NaturalCmsJob,
    NaturalCmsResource,
    NaturalCmsStageResult,
)
from axms_coding_orchestrator.natural_cms_handlers import (
    NaturalCmsHandlerDependencies,
    SpringGatewayNaturalCmsStageExecutor,
    register_natural_cms_node_handlers,
)
from axms_coding_orchestrator.natural_cms_runner import NaturalCmsSnapshotRunner
from axms_coding_orchestrator.queue import QueueDelivery
from axms_coding_orchestrator.profile_version_client import ProfileVersionClientError
from axms_coding_orchestrator.service import (
    HealthState,
    NaturalCmsWorkerLoop,
)


JOB_ID = "50505050-5050-4050-8050-505050505050"
TRACE_ID = "60606060-6060-4060-8060-606060606060"
PREVIEW_ID = "70707070-7070-4070-8070-707070707070"
PREVIEW_HASH = "sha256:" + ("b" * 64)
RESOURCE = NaturalCmsResource("CONTENT", "7")
COMMAND = {"operation": "UPDATE", "fields": {"title": "New"}}


class _SpringNaturalCms:
    def __init__(self) -> None:
        self.pipeline_attempt = 1
        self.status = "ACTIVE"
        self.state_version = 1
        self.decision: str | None = None
        self.preview_valid = False
        self.preview_attempt: int | None = None
        self.drop_preview_response_once = False
        self.analyze_port = "feasible"
        self.transition_infeasible = True
        self.resolve_count = 0
        self.calls: list[str] = []
        self.call_attempts: list[tuple[str, int]] = []
        self.counts: dict[str, int] = defaultdict(int)

    def _job(self) -> NaturalCmsJob:
        return NaturalCmsJob(
            JOB_ID,
            TRACE_ID,
            DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
            self.pipeline_attempt,
            self.state_version,
            self.status,
            RESOURCE,
            PREVIEW_ID if self.preview_valid or self.decision else None,
            PREVIEW_HASH if self.preview_valid or self.decision else None,
            self.preview_valid,
            self.decision,
            "Update content 7",
        )

    def resolve_job(self, reference: QueuedJobReference) -> NaturalCmsJob:
        self.assert_reference(reference)
        self.resolve_count += 1
        return self._job()

    def get_job(self, invocation) -> NaturalCmsJob:
        return self._job()

    def execute_stage(
        self,
        invocation,
        handler_key: str,
        result_id: str,
    ) -> NaturalCmsStageResult:
        self.calls.append(handler_key)
        self.call_attempts.append((handler_key, invocation.pipeline_attempt))
        self.counts[handler_key] += 1
        if handler_key == "cms.analyze":
            if self.analyze_port == "infeasible" and self.transition_infeasible:
                self.status = "REJECTED"
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                self.analyze_port,
                RESOURCE,
                None,
                None,
                None,
                {},
            )
        if handler_key == "cms.preview":
            self.status = "WAITING_APPROVAL"
            self.preview_valid = True
            self.decision = None
            self.preview_attempt = invocation.pipeline_attempt
            if self.drop_preview_response_once:
                self.drop_preview_response_once = False
                raise NaturalCmsDomainClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "preview response was lost",
                    retryable=True,
                    status=503,
                )
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "ready",
                RESOURCE,
                COMMAND,
                PREVIEW_ID,
                PREVIEW_HASH,
                {"before": {}, "after": {}},
            )
        if handler_key == "cms.discard":
            self.preview_valid = False
            retry = self.preview_attempt != invocation.pipeline_attempt
            self.status = "ACTIVE" if retry else "REJECTED"
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "retry" if retry else "discarded",
                RESOURCE,
                COMMAND,
                PREVIEW_ID,
                PREVIEW_HASH,
                {"discarded": True, "retry": retry},
            )
        if handler_key == "cms.apply":
            self.status = "COMPLETED"
            self.preview_valid = False
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "applied",
                RESOURCE,
                COMMAND,
                PREVIEW_ID,
                PREVIEW_HASH,
                {"status": "APPLIED"},
            )
        raise AssertionError(f"unexpected handler: {handler_key}")

    def approve(self, decision: str = "APPROVED") -> None:
        self.decision = decision
        if decision == "REJECTED" and self.pipeline_attempt < 3:
            self.pipeline_attempt += 1
        self.state_version += 1

    def persist_preview(self) -> None:
        self.status = "WAITING_APPROVAL"
        self.preview_valid = True
        self.preview_attempt = self.pipeline_attempt

    def assert_reference(self, reference: QueuedJobReference) -> None:
        if reference.job_id != JOB_ID:
            raise AssertionError("job reference mismatch")


class _Profiles:
    def get(self, profile_version_id: str):
        if profile_version_id != DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID:
            raise AssertionError("profile mismatch")
        return default_natural_cms_snapshot()


class _FailingRunner:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    def invoke(self, _reference: QueuedJobReference):
        raise self.failure


class _ObservationProbe:
    def __init__(self) -> None:
        self.job_attempts: list[int] = []

    @contextmanager
    def job(self, **values: Any) -> Iterator[SimpleNamespace]:
        self.job_attempts.append(values["attempt"])
        yield SimpleNamespace(finish=lambda _status: None)

    def invoke_node(self, *, invocation: Any, handler: Any, **_values: Any) -> Any:
        return handler(invocation)


class _RunQueue:
    def __init__(self, delivery: QueueDelivery) -> None:
        self.delivery = delivery
        self.loop: NaturalCmsWorkerLoop | None = None
        self.acked: list[QueueDelivery] = []
        self.requeued: list[QueueDelivery] = []

    def pop(self, _timeout: int):
        delivery = self.delivery
        self.delivery = None
        if delivery is None and self.loop is not None:
            self.loop.stop()
        return delivery

    def ack(self, delivery: QueueDelivery) -> None:
        self.acked.append(delivery)
        if self.loop is not None:
            self.loop.stop()

    def requeue(self, delivery: QueueDelivery) -> None:
        self.requeued.append(delivery)
        if self.loop is not None:
            self.loop.stop()


class NaturalCmsWorkerLoopTest(unittest.TestCase):
    @staticmethod
    def _runner(
        spring: _SpringNaturalCms,
        checkpointer: InMemorySaver | None = None,
        observability: _ObservationProbe | None = None,
    ) -> tuple[NaturalCmsSnapshotRunner, InMemorySaver]:
        registry = register_natural_cms_node_handlers(
            build_common_node_registry(),
            NaturalCmsHandlerDependencies(
                spring,
                SpringGatewayNaturalCmsStageExecutor(spring),
            ),
        )
        saver = checkpointer or InMemorySaver()
        return (
            NaturalCmsSnapshotRunner(
                spring,
                _Profiles(),
                registry,
                saver,
                observability,  # type: ignore[arg-type]
            ),
            saver,
        )

    def test_root_observation_uses_pipeline_attempt_not_state_version(self) -> None:
        spring = _SpringNaturalCms()
        spring.pipeline_attempt = 2
        spring.state_version = 7
        observations = _ObservationProbe()
        runner, _checkpointer = self._runner(
            spring, observability=observations
        )

        result = runner.invoke(
            QueuedJobReference.from_dict({"jobId": JOB_ID})
        )

        self.assertEqual("WAITING_APPROVAL", result["status"])
        self.assertEqual([2], observations.job_attempts)

    def test_permanent_poison_is_acked_but_transient_failure_is_requeued(self) -> None:
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        cases = (
            (
                NaturalCmsDomainClientError(
                    "NATURAL_CMS_STATE_CONFLICT",
                    "safe conflict",
                    retryable=False,
                    status=409,
                ),
                True,
            ),
            (
                ProfileVersionClientError(
                    "PROFILE_VERSION_NOT_ACTIVE",
                    "safe conflict",
                    retryable=False,
                    status=409,
                ),
                True,
            ),
            (
                GraphExecutionError(
                    "CONTRACT_VALIDATION_FAILED",
                    "safe contract failure",
                    retryable=False,
                ),
                True,
            ),
            (
                NaturalCmsDomainClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "safe transient",
                    retryable=True,
                    status=503,
                ),
                False,
            ),
            (RuntimeError("unknown transient"), False),
        )
        for failure, acknowledged in cases:
            with self.subTest(failure=type(failure).__name__, acknowledged=acknowledged):
                loop = NaturalCmsWorkerLoop(
                    _RunQueue(
                        QueueDelivery(job=reference, _raw=reference.to_json())
                    ),
                    _FailingRunner(failure),  # type: ignore[arg-type]
                    HealthState(),
                    queue_block_seconds=1,
                    max_backoff_seconds=1,
                )

                self.assertEqual(acknowledged, loop.process(reference))

    def test_waiting_delivery_is_acked_and_same_job_resume_applies_once(self) -> None:
        spring = _SpringNaturalCms()
        runner, checkpointer = self._runner(spring)
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        delivery = QueueDelivery(job=reference, _raw=reference.to_json())

        waiting_queue = _RunQueue(delivery)
        waiting_loop = NaturalCmsWorkerLoop(
            waiting_queue,
            runner,
            HealthState(),
            queue_block_seconds=1,
            max_backoff_seconds=1,
        )
        waiting_queue.loop = waiting_loop
        waiting_loop.run()

        self.assertEqual([delivery], waiting_queue.acked)
        self.assertEqual([], waiting_queue.requeued)
        self.assertEqual("WAITING_APPROVAL", spring.status)
        self.assertEqual(0, spring.counts["cms.apply"])
        self.assertEqual({JOB_ID}, set(checkpointer.storage))

        duplicate_queue = _RunQueue(delivery)
        duplicate_loop = NaturalCmsWorkerLoop(
            duplicate_queue,
            runner,
            HealthState(),
            queue_block_seconds=1,
            max_backoff_seconds=1,
        )
        duplicate_queue.loop = duplicate_loop
        duplicate_loop.run()

        self.assertEqual([delivery], duplicate_queue.acked)
        self.assertEqual(1, spring.counts["cms.preview"])

        spring.approve()
        resume_queue = _RunQueue(delivery)
        resume_loop = NaturalCmsWorkerLoop(
            resume_queue,
            runner,
            HealthState(),
            queue_block_seconds=1,
            max_backoff_seconds=1,
        )
        resume_queue.loop = resume_loop
        resume_loop.run()

        self.assertEqual([delivery], resume_queue.acked)
        self.assertEqual([], resume_queue.requeued)
        self.assertEqual("COMPLETED", spring.status)
        self.assertEqual(1, spring.counts["cms.apply"])
        self.assertEqual(
            ["cms.analyze", "cms.preview", "cms.apply"],
            spring.calls,
        )

    def test_missing_checkpoint_recovers_spring_preview_without_reexecuting_it(self) -> None:
        spring = _SpringNaturalCms()
        spring.persist_preview()
        runner, checkpointer = self._runner(spring)
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})

        waiting = runner.invoke(reference)

        self.assertEqual("WAITING_APPROVAL", waiting["status"])
        self.assertEqual([], spring.calls)
        self.assertEqual({JOB_ID}, set(checkpointer.storage))

    def test_missing_checkpoint_with_approved_preview_applies_in_same_delivery(self) -> None:
        spring = _SpringNaturalCms()
        spring.persist_preview()
        spring.approve()
        runner, _ = self._runner(spring)
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})

        completed = runner.invoke(reference)

        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual("COMPLETED", spring.status)
        self.assertEqual([("cms.apply", 1)], spring.call_attempts)

    def test_missing_checkpoint_with_rejected_preview_uses_current_attempt(self) -> None:
        cases = (
            (1, "WAITING_APPROVAL", [("cms.discard", 2), ("cms.analyze", 2), ("cms.preview", 2)]),
            (3, "COMPLETED", [("cms.discard", 3)]),
        )
        for starting_attempt, expected_status, expected_calls in cases:
            with self.subTest(starting_attempt=starting_attempt):
                spring = _SpringNaturalCms()
                spring.pipeline_attempt = starting_attempt
                spring.persist_preview()
                spring.approve("REJECTED")
                runner, _ = self._runner(spring)
                reference = QueuedJobReference.from_dict({"jobId": JOB_ID})

                result = runner.invoke(reference)

                self.assertEqual(expected_status, result["status"])
                self.assertEqual(expected_calls, spring.call_attempts)
                self.assertNotIn(("cms.discard", starting_attempt - 1), spring.call_attempts)

    def test_pre_preview_checkpoint_recovers_decided_spring_preview(self) -> None:
        spring = _SpringNaturalCms()
        spring.drop_preview_response_once = True
        runner, checkpointer = self._runner(spring)
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})

        with self.assertRaises(GraphExecutionError):
            runner.invoke(reference)

        self.assertEqual("WAITING_APPROVAL", spring.status)
        self.assertEqual({JOB_ID}, set(checkpointer.storage))
        spring.approve()

        completed = runner.invoke(reference)

        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual(1, spring.counts["cms.preview"])
        self.assertEqual(1, spring.counts["cms.apply"])
        self.assertEqual(
            [("cms.analyze", 1), ("cms.preview", 1), ("cms.apply", 1)],
            spring.call_attempts,
        )

    def test_graph_end_requires_spring_terminal_before_ack(self) -> None:
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        delivery = QueueDelivery(job=reference, _raw=reference.to_json())
        cases = ((True, True), (False, False))
        for transition_infeasible, acknowledged in cases:
            with self.subTest(transition_infeasible=transition_infeasible):
                spring = _SpringNaturalCms()
                spring.analyze_port = "infeasible"
                spring.transition_infeasible = transition_infeasible
                runner, _ = self._runner(spring)
                queue = _RunQueue(delivery)
                loop = NaturalCmsWorkerLoop(
                    queue,
                    runner,
                    HealthState(),
                    queue_block_seconds=1,
                    max_backoff_seconds=1,
                )
                queue.loop = loop

                loop.run()

                self.assertEqual([delivery] if acknowledged else [], queue.acked)
                self.assertEqual([] if acknowledged else [delivery], queue.requeued)
                self.assertGreaterEqual(spring.resolve_count, 2)
                self.assertEqual(
                    "REJECTED" if acknowledged else "ACTIVE",
                    spring.status,
                )

    def test_normal_resume_rejects_decision_attempt_mismatch(self) -> None:
        cases = (
            (1, "APPROVED", 2),
            (1, "REJECTED", 1),
            (3, "REJECTED", 4),
        )
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        for starting_attempt, decision, decided_attempt in cases:
            with self.subTest(
                starting_attempt=starting_attempt,
                decision=decision,
                decided_attempt=decided_attempt,
            ):
                spring = _SpringNaturalCms()
                spring.pipeline_attempt = starting_attempt
                runner, _ = self._runner(spring)
                self.assertEqual("WAITING_APPROVAL", runner.invoke(reference)["status"])
                spring.decision = decision
                spring.pipeline_attempt = decided_attempt
                spring.state_version += 1

                with self.assertRaisesRegex(
                    GraphExecutionError,
                    "approval changed pipelineAttempt unexpectedly",
                ):
                    runner.invoke(reference)

                self.assertEqual(0, spring.counts["cms.apply"])
                self.assertEqual(0, spring.counts["cms.discard"])

    def test_normal_rejected_resume_uses_retry_and_terminal_attempt_rules(self) -> None:
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        cases = ((1, "WAITING_APPROVAL"), (3, "COMPLETED"))
        for starting_attempt, expected_status in cases:
            with self.subTest(starting_attempt=starting_attempt):
                spring = _SpringNaturalCms()
                spring.pipeline_attempt = starting_attempt
                runner, _ = self._runner(spring)
                self.assertEqual("WAITING_APPROVAL", runner.invoke(reference)["status"])
                spring.approve("REJECTED")

                result = runner.invoke(reference)

                self.assertEqual(expected_status, result["status"])
                expected_attempt = 2 if starting_attempt == 1 else 3
                self.assertEqual(
                    [("cms.discard", expected_attempt)],
                    [
                        call
                        for call in spring.call_attempts
                        if call[0] == "cms.discard"
                    ],
                )


if __name__ == "__main__":
    unittest.main()
