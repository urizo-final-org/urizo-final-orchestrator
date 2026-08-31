from __future__ import annotations

import unittest

from langgraph.checkpoint.memory import InMemorySaver

from axms_coding_orchestrator.contracts import CodingJobRequested, WorkerClaim
from axms_coding_orchestrator.graph import (
    CodingGraphRunner,
    GraphDependencies,
    GraphExecutionError,
    build_coding_graph,
)
from axms_coding_orchestrator.model_gateway import ModelTurnResponse
from axms_coding_orchestrator.tool_gateway import ToolExecutionResult, ToolGatewayError

from factories import FIXED_NOW, coding_event, model_response, worker_claim


class _ModelGateway:
    def __init__(self) -> None:
        self.requests = []

    def execute(self, request):
        self.requests.append(request.to_dict())
        return ModelTurnResponse.from_dict(model_response(request.to_dict()))


class _ToolGateway:
    def __init__(self, failures: int = 0) -> None:
        self.requests = []
        self.failures = failures

    def execute_read_file(self, request):
        self.requests.append(request)
        if self.failures:
            self.failures -= 1
            raise ToolGatewayError(
                "TOOL_EXECUTOR_UNAVAILABLE", "safe transient", retryable=True
            )
        return ToolExecutionResult(
            execution_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            tool_call_id=request["toolCallId"],
            media_type="text/plain",
            digest="sha256:" + ("f" * 64),
            size_bytes=8,
            content="approved",
        )


class _WorkerApi:
    def __init__(self) -> None:
        self.outcomes = []

    def outcome(self, claim, outcome, idempotency_key, *, error_code=None):
        self.outcomes.append((claim.to_dict(), outcome, idempotency_key, error_code))
        status = {
            "WAITING_APPROVAL": "WAITING_APPROVAL",
            "COMPLETED": "COMPLETED",
            "RETRYABLE_FAILURE": "PENDING",
            "PERMANENT_FAILURE": "FAILED",
        }[outcome]
        return {
            "schemaVersion": "1.0",
            "jobId": claim.job_id,
            "traceId": claim.trace_id,
            "stateVersion": claim.state_version + 1,
            "status": status,
        }


class CodingGraphTest(unittest.TestCase):
    def setUp(self) -> None:
        self.model = _ModelGateway()
        self.tool = _ToolGateway()
        self.worker = _WorkerApi()
        self.checkpointer = InMemorySaver()
        self.dependencies = GraphDependencies(
            model_gateway=self.model,
            tool_gateway=self.tool,
            worker_api=self.worker,
        )
        self.runner = self.restart_runner()

    def restart_runner(self) -> CodingGraphRunner:
        return CodingGraphRunner(
            build_coding_graph(self.checkpointer, self.dependencies)
        )

    def test_legacy_single_graph_still_denies_a_snapshot_without_the_plan_node(self) -> None:
        """The 'plan' requirement belongs to this legacy graph alone. It is enforced
        here, not in the shared claim contract that both execution paths use."""

        event = CodingJobRequested.from_dict(coding_event())
        payload = worker_claim(event.to_dict())
        payload["snapshot"]["allowedNodes"] = ["analyze"]
        claim = WorkerClaim.from_dict(payload, event, now=FIXED_NOW)

        with self.assertRaises(GraphExecutionError) as denied:
            self.runner.invoke(event, claim)

        self.assertEqual("SERVICE_AUTHORIZATION_DENIED", denied.exception.code)
        self.assertFalse(denied.exception.retryable)
        self.assertEqual([], self.model.requests)

    def test_single_graph_interrupts_and_resumes_same_job_without_repeating_tools(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )

        interrupted = self.runner.invoke(event, claim)

        self.assertEqual("WAITING_APPROVAL", interrupted["status"])
        self.assertIn("__interrupt__", interrupted)
        self.assertEqual(1, len(self.model.requests))
        self.assertEqual(1, len(self.tool.requests))
        self.assertEqual(["WAITING_APPROVAL"], [item[1] for item in self.worker.outcomes])
        self.assertTrue(self.runner.is_duplicate(event))

        self.runner = self.restart_runner()

        resume_event = CodingJobRequested.from_dict(
            coding_event(
                event_id="11111111-1111-4111-8111-111111111111",
                version=6,
                attempt=1,
            )
        )
        resume_claim = WorkerClaim.from_dict(
            worker_claim(resume_event.to_dict(), resume=True, state_version=7),
            resume_event,
            now=FIXED_NOW,
        )

        self.assertFalse(self.runner.is_duplicate(resume_event))
        completed = self.runner.invoke(resume_event, resume_claim)

        self.assertEqual("COMPLETED", completed["status"])
        self.assertEqual(1, len(self.model.requests))
        self.assertEqual(1, len(self.tool.requests))
        self.assertEqual(
            ["WAITING_APPROVAL", "COMPLETED"],
            [item[1] for item in self.worker.outcomes],
        )
        self.assertTrue(self.runner.is_duplicate(resume_event))

    def test_model_and_tool_side_effect_keys_are_stable_across_worker_leases(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        first_claim_payload = worker_claim(event.to_dict())
        second_claim_payload = worker_claim(event.to_dict())
        second_claim_payload["leaseId"] = "72727272-7272-4272-8272-727272727272"
        first_claim = WorkerClaim.from_dict(
            first_claim_payload, event, now=FIXED_NOW
        )
        second_claim = WorkerClaim.from_dict(
            second_claim_payload, event, now=FIXED_NOW
        )

        first_runner = CodingGraphRunner(
            build_coding_graph(InMemorySaver(), self.dependencies)
        )
        first_runner.invoke(event, first_claim)
        second_runner = CodingGraphRunner(
            build_coding_graph(InMemorySaver(), self.dependencies)
        )
        second_runner.invoke(event, second_claim)

        first_model, second_model = self.model.requests[-2:]
        first_tool, second_tool = self.tool.requests[-2:]
        self.assertEqual(first_model["turnId"], second_model["turnId"])
        self.assertEqual(
            first_model["idempotencyKey"], second_model["idempotencyKey"]
        )
        self.assertEqual(first_tool["requestId"], second_tool["requestId"])
        self.assertEqual(
            first_tool["idempotencyKey"], second_tool["idempotencyKey"]
        )
        self.assertNotEqual(first_tool["leaseId"], second_tool["leaseId"])

    def test_retry_claim_continues_the_checkpoint_without_repeating_model_turn(self) -> None:
        self.tool.failures = 1
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )

        with self.assertRaises(GraphExecutionError) as raised:
            self.runner.invoke(event, claim)
        self.assertTrue(raised.exception.retryable)

        retry_event = CodingJobRequested.from_dict(
            coding_event(
                event_id="14141414-1414-4414-8414-141414141414",
                version=6,
                attempt=2,
            )
        )
        retry_claim = WorkerClaim.from_dict(
            worker_claim(retry_event.to_dict(), resume=True, state_version=7),
            retry_event,
            now=FIXED_NOW,
        )
        interrupted = self.runner.invoke(retry_event, retry_claim)

        self.assertEqual("WAITING_APPROVAL", interrupted["status"])
        self.assertEqual(1, len(self.model.requests))
        self.assertEqual(2, len(self.tool.requests))
        self.assertEqual(["WAITING_APPROVAL"], [item[1] for item in self.worker.outcomes])
        self.assertTrue(self.runner.is_duplicate(retry_event))

    def test_recovered_same_delivery_continues_inflight_checkpoint_before_ack(self) -> None:
        self.tool.failures = 1
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )

        with self.assertRaises(GraphExecutionError):
            self.runner.invoke(event, claim)

        self.assertFalse(self.runner.is_duplicate(event))
        recovered_payload = worker_claim(event.to_dict())
        recovered_payload["resume"] = True
        recovered_payload["leaseExpiresAt"] = "2026-08-11T10:20:00Z"
        recovered_claim = WorkerClaim.from_dict(
            recovered_payload, event, now=FIXED_NOW
        )
        interrupted = self.runner.invoke(event, recovered_claim)

        self.assertEqual("WAITING_APPROVAL", interrupted["status"])
        self.assertEqual(1, len(self.model.requests))
        self.assertEqual(2, len(self.tool.requests))
        self.assertTrue(self.runner.is_duplicate(event))


if __name__ == "__main__":
    unittest.main()
