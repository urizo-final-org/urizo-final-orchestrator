from __future__ import annotations

import unittest

from axms_coding_orchestrator.contracts import (
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
)
from axms_coding_orchestrator.graph import GraphExecutionError
from axms_coding_orchestrator.queue import QueueDelivery
from axms_coding_orchestrator.service import HealthState, WorkerLoop
from axms_coding_orchestrator.worker_api import WorkerApiError

from factories import FIXED_NOW, coding_event, worker_claim


class _UnusedQueue:
    pass


class _RunQueue:
    def __init__(self, delivery) -> None:
        self.delivery = delivery
        self.loop = None
        self.acked = []
        self.requeued = []

    def pop(self, timeout):
        delivery = self.delivery
        self.delivery = None
        if delivery is None:
            self.loop.stop()
        return delivery

    def ack(self, delivery):
        self.acked.append(delivery)
        self.loop.stop()

    def requeue(self, delivery):
        self.requeued.append(delivery)
        self.loop.stop()


class _WorkerApi:
    def __init__(
        self,
        event,
        claim,
        failures: int = 0,
        *,
        resolve_error: WorkerApiError | None = None,
        claim_error: WorkerApiError | None = None,
    ) -> None:
        self.authoritative_event = event
        self.authoritative_claim = claim
        self.failures = failures
        self.resolve_error = resolve_error
        self.claim_error = claim_error
        self.resolve_calls = 0
        self.claim_calls = 0
        self.outcomes = []

    def resolve(self, job):
        self.resolve_calls += 1
        if self.resolve_error is not None:
            raise self.resolve_error
        if job.job_id != self.authoritative_event.job_id:
            raise AssertionError("job reference mismatch")
        return self.authoritative_event

    def claim(self, event):
        self.claim_calls += 1
        if self.claim_error is not None:
            raise self.claim_error
        if self.claim_calls <= self.failures:
            raise WorkerApiError(
                "INTERNAL_TRANSIENT_ERROR", "safe transient", retryable=True
            )
        return self.authoritative_claim

    def outcome(self, claim, outcome, idempotency_key, *, error_code=None):
        self.outcomes.append((outcome, idempotency_key, error_code))
        return {}


class _Heartbeat:
    def __init__(self) -> None:
        self.started = []
        self.stopped = []

    def start(self, claim):
        self.started.append(claim.lease_id)

    def ensure_current(self, claim):
        pass

    def stop(self, job_id):
        self.stopped.append(job_id)


class _Graph:
    def __init__(
        self, *, duplicate: bool = False, failure=None, duplicate_failure=None
    ) -> None:
        self.duplicate = duplicate
        self.failure = failure
        self.duplicate_failure = duplicate_failure
        self.invocations = 0

    def is_duplicate(self, event):
        if self.duplicate_failure is not None:
            raise self.duplicate_failure
        return self.duplicate

    def invoke(self, event, claim):
        self.invocations += 1
        if self.failure is not None:
            raise self.failure
        return {"status": "WAITING_APPROVAL"}


class WorkerLoopTest(unittest.TestCase):
    def build(self, event_payload=None, *, failures=0, graph=None, max_attempts=3):
        event = CodingJobRequested.from_dict(event_payload or coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        worker = _WorkerApi(event, claim, failures)
        heartbeat = _Heartbeat()
        delays = []
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,
            graph or _Graph(),
            heartbeat,
            HealthState(),
            queue_block_seconds=1,
            max_attempts=max_attempts,
            max_backoff_seconds=30,
            sleeper=delays.append,
        )
        return event, worker, heartbeat, delays, loop

    @staticmethod
    def job(event: CodingJobRequested) -> QueuedJobReference:
        return QueuedJobReference.from_dict({"jobId": event.job_id})

    @staticmethod
    def legacy_event_and_claim() -> tuple[CodingJobRequested, WorkerClaim]:
        bound_payload = coding_event()
        legacy_payload = dict(bound_payload)
        for field in (
            "profileVersionId",
            "pipelineAttempt",
            "executionAttempt",
            "workspaceId",
            "toolCallId",
        ):
            legacy_payload.pop(field)
        event = CodingJobRequested.from_dict(legacy_payload)
        claim = WorkerClaim.from_dict(
            worker_claim(bound_payload), event, now=FIXED_NOW
        )
        return event, claim

    def test_claim_retries_with_bounded_exponential_backoff(self) -> None:
        event, worker, heartbeat, delays, loop = self.build(failures=2)

        acknowledged = loop.process(self.job(event))

        self.assertTrue(acknowledged)
        self.assertEqual(3, worker.claim_calls)
        self.assertEqual([1, 2], delays)
        self.assertEqual(1, len(heartbeat.started))
        self.assertEqual([event.job_id], heartbeat.stopped)
        self.assertEqual("WAITING_APPROVAL", worker.outcomes[0][0])

    def test_retryable_graph_failure_reports_retry_or_permanent_by_attempt(self) -> None:
        failure = GraphExecutionError(
            "TOOL_EXECUTOR_UNAVAILABLE", "safe failure", retryable=True
        )
        graph = _Graph(failure=failure)
        event, worker, _, _, loop = self.build(graph=graph)
        self.assertTrue(loop.process(self.job(event)))
        self.assertEqual("RETRYABLE_FAILURE", worker.outcomes[0][0])
        self.assertEqual("TOOL_EXECUTOR_UNAVAILABLE", worker.outcomes[0][2])

        final_payload = coding_event(
            event_id="13131313-1313-4313-8313-131313131313", attempt=3
        )
        final_graph = _Graph(failure=failure)
        final_event, final_worker, _, _, final_loop = self.build(
            final_payload, graph=final_graph
        )
        self.assertTrue(final_loop.process(self.job(final_event)))
        self.assertEqual("PERMANENT_FAILURE", final_worker.outcomes[0][0])

    def test_legacy_checkpoint_duplicate_is_discarded_before_claim(self) -> None:
        event, claim = self.legacy_event_and_claim()
        worker = _WorkerApi(event, claim)
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,
            _Graph(duplicate=True),
            _Heartbeat(),
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )

        self.assertTrue(loop.process(self.job(event)))

        self.assertEqual(1, worker.resolve_calls)
        self.assertEqual(0, worker.claim_calls)

    def test_duplicate_probe_failure_is_requeued_without_claim(self) -> None:
        graph = _Graph(duplicate_failure=RuntimeError("fixture probe failure"))
        event, worker, heartbeat, _, loop = self.build(graph=graph)

        self.assertFalse(loop.process(self.job(event)))
        self.assertEqual(0, worker.claim_calls)
        self.assertEqual([], heartbeat.started)

    def test_run_acks_legacy_duplicate_but_requeues_unclaimed_delivery(self) -> None:
        legacy_event, legacy_claim = self.legacy_event_and_claim()
        job = self.job(legacy_event)
        delivery = QueueDelivery(job=job, _raw=job.to_json())

        duplicate_queue = _RunQueue(delivery)
        duplicate_worker = _WorkerApi(legacy_event, legacy_claim)
        duplicate_loop = WorkerLoop(
            duplicate_queue,
            duplicate_worker,
            _Graph(duplicate=True),
            _Heartbeat(),
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )
        duplicate_queue.loop = duplicate_loop
        duplicate_loop.run()
        self.assertEqual([delivery], duplicate_queue.acked)
        self.assertEqual([], duplicate_queue.requeued)

        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        failed_queue = _RunQueue(delivery)
        failed_worker = _WorkerApi(event, claim, failures=10)
        failed_loop = WorkerLoop(
            failed_queue,
            failed_worker,
            _Graph(),
            _Heartbeat(),
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )
        failed_queue.loop = failed_loop
        failed_loop.run()
        self.assertEqual([], failed_queue.acked)
        self.assertEqual([delivery], failed_queue.requeued)

    def test_legacy_success_does_not_report_a_second_outcome(self) -> None:
        event, claim = self.legacy_event_and_claim()
        worker = _WorkerApi(event, claim)
        graph = _Graph()
        loop = WorkerLoop(
            _UnusedQueue(),
            worker,
            graph,
            _Heartbeat(),
            HealthState(),
            queue_block_seconds=1,
            max_attempts=1,
            max_backoff_seconds=1,
            sleeper=lambda _delay: None,
        )

        self.assertTrue(loop.process(self.job(event)))
        self.assertEqual(1, graph.invocations)
        self.assertEqual([], worker.outcomes)

    def test_unclaimed_worker_errors_ack_only_non_retryable_poison(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        cases = (
            ("resolve", "JOB_NOT_FOUND", 404, False, True),
            ("resolve", "JOB_STATE_VERSION_CONFLICT", 409, False, True),
            ("resolve", "WORKER_RESPONSE_INVALID", None, False, False),
            ("resolve", "CODING_AGENT_NOT_AVAILABLE", None, False, False),
            ("resolve", "JOB_NOT_FOUND", None, False, False),
            ("resolve", "INTERNAL_TRANSIENT_ERROR", 503, True, False),
            ("claim", "JOB_NOT_FOUND", 404, False, False),
            ("claim", "INTERNAL_TRANSIENT_ERROR", 503, True, False),
        )
        for stage, code, status, retryable, acknowledged in cases:
            with self.subTest(stage=stage, code=code, status=status):
                failure = WorkerApiError(
                    code,
                    "safe failure",
                    retryable=retryable,
                    status=status,
                )
                worker = _WorkerApi(
                    event,
                    claim,
                    resolve_error=failure if stage == "resolve" else None,
                    claim_error=failure if stage == "claim" else None,
                )
                heartbeat = _Heartbeat()
                loop = WorkerLoop(
                    _UnusedQueue(),
                    worker,
                    _Graph(),
                    heartbeat,
                    HealthState(),
                    queue_block_seconds=1,
                    max_attempts=1,
                    max_backoff_seconds=1,
                    sleeper=lambda _delay: None,
                )

                self.assertEqual(acknowledged, loop.process(self.job(event)))
                self.assertEqual([], heartbeat.started)


if __name__ == "__main__":
    unittest.main()
