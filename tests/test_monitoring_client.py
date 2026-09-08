from __future__ import annotations

from contextlib import contextmanager
import json
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.monitoring_client import SpringNodeMonitoringReporter
from axms_coding_orchestrator.node_runtime import NodeInvocation
from axms_coding_orchestrator.snapshot import SnapshotNode


JOB_ID = "11111111-1111-4111-8111-111111111111"
TRACE_ID = "22222222-2222-4222-8222-222222222222"
PROFILE_VERSION_ID = "33333333-3333-4333-8333-333333333333"
ORIGIN = "http://spring.test:8080"


@contextmanager
def _credential():
    yield bytearray(b"service-test-token")


def _node() -> SnapshotNode:
    return SnapshotNode.from_dict(
        {
            "id": "analyze",
            "type": "agent",
            "handlerKey": "coding.analyze",
            "resultPorts": ["completed"],
            "config": {},
        }
    )


def _invocation() -> NodeInvocation:
    return NodeInvocation.create(
        job_id=JOB_ID,
        profile_version_id=PROFILE_VERSION_ID,
        node_id="analyze",
        pipeline_attempt=2,
        execution_attempt=3,
        state_version=4,
        trace_id=TRACE_ID,
        workspace_id=None,
        tool_call_id=None,
        context={"prompt": "FORBIDDEN_PROMPT", "source": "FORBIDDEN_SOURCE"},
        config={},
    )


class MonitoringClientTest(unittest.TestCase):
    def test_reports_only_the_bounded_occurrence_contract(self) -> None:
        captured: dict[str, object] = {}

        def request(
            method: str,
            endpoint: str,
            payload: bytes | None,
            credential: bytearray,
            timeout: float,
        ) -> tuple[int, bytes]:
            captured.update(
                method=method,
                endpoint=endpoint,
                payload=payload,
                credential=bytes(credential),
                timeout=timeout,
            )
            return 200, b"{}"

        reporter = SpringNodeMonitoringReporter(
            ORIGIN, _credential, allowed_origins={ORIGIN}
        )
        with patch(
            "axms_coding_orchestrator.monitoring_client._request_http",
            side_effect=request,
        ):
            applied = reporter.report(
                node=_node(),
                invocation=_invocation(),
                node_sequence=7,
                status="RUNNING",
                observation_trace_id="a" * 32,
            )

        self.assertTrue(applied)
        body = json.loads(bytes(captured["payload"]))
        self.assertEqual(
            {
                "schemaVersion",
                "jobId",
                "traceId",
                "observationTraceId",
                "profileVersionId",
                "pipelineAttempt",
                "executionAttempt",
                "nodeId",
                "nodeSequence",
                "nodeType",
                "handlerKey",
                "status",
                "occurredAt",
                "errorCode",
            },
            set(body),
        )
        self.assertEqual(7, body["nodeSequence"])
        self.assertEqual("a" * 32, body["observationTraceId"])
        self.assertNotIn("FORBIDDEN_PROMPT", repr(body))
        self.assertNotIn("FORBIDDEN_SOURCE", repr(body))
        self.assertEqual(b"service-test-token", captured["credential"])

    def test_transport_and_contract_failures_are_fail_open(self) -> None:
        reporter = SpringNodeMonitoringReporter(
            ORIGIN, _credential, allowed_origins={ORIGIN}
        )
        with patch(
            "axms_coding_orchestrator.monitoring_client._request_http",
            side_effect=TimeoutError("FORBIDDEN_DETAIL"),
        ):
            self.assertFalse(
                reporter.report(
                    node=_node(),
                    invocation=_invocation(),
                    node_sequence=1,
                    status="COMPLETED",
                    observation_trace_id=None,
                )
            )
        self.assertFalse(
            reporter.report(
                node=_node(),
                invocation=_invocation(),
                node_sequence=1,
                status="FAILED",
                observation_trace_id=None,
            )
        )

    def test_failed_report_accepts_only_a_safe_error_code(self) -> None:
        reporter = SpringNodeMonitoringReporter(
            ORIGIN, _credential, allowed_origins={ORIGIN}
        )
        with patch(
            "axms_coding_orchestrator.monitoring_client._request_http",
            return_value=(200, b"{}"),
        ) as request:
            self.assertTrue(
                reporter.report(
                    node=_node(),
                    invocation=_invocation(),
                    node_sequence=2,
                    status="FAILED",
                    observation_trace_id=None,
                    error_code="NODE_EXECUTION_FAILED",
                )
            )
        payload = json.loads(request.call_args.args[2])
        self.assertEqual("NODE_EXECUTION_FAILED", payload["errorCode"])


if __name__ == "__main__":
    unittest.main()
