from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.coding_domain_client import (
    CodingAttemptAggregate,
    CodingDomainClientError,
    CodingResultWrite,
    CodingStageExecutionResult,
    SpringCodingDomainClient,
)
from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.node_runtime import NodeInvocation


JOB_ID = "20202020-2020-4020-8020-202020202020"
PROFILE_VERSION_ID = "d3d41f73-9a07-51e5-9ec8-4ed8aca7f7cb"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
RESULT_ID = "50505050-5050-4050-8050-505050505050"
ACTOR_ID = "60606060-6060-4060-8060-606060606060"
APPROVAL_ID = "70707070-7070-4070-8070-707070707070"
NOW = "2026-08-30T01:02:03Z"
DIGEST = "sha256:" + ("a" * 64)
SHA = "sha1:" + ("a" * 40)


class _CodingHandler(BaseHTTPRequestHandler):
    response_status = 200
    response_body = b"{}"
    observed_path: str | None = None
    observed_authorization: str | None = None
    observed_trace_id: str | None = None
    observed_body: dict[str, object] | None = None

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        length = int(self.headers.get("Content-Length", "0"))
        type(self).observed_path = self.path
        type(self).observed_authorization = self.headers.get("Authorization")
        type(self).observed_trace_id = self.headers.get("X-Trace-Id")
        type(self).observed_body = json.loads(self.rfile.read(length))
        self._respond()

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        self.do_PUT()

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        type(self).observed_path = self.path
        type(self).observed_authorization = self.headers.get("Authorization")
        type(self).observed_trace_id = self.headers.get("X-Trace-Id")
        type(self).observed_body = None
        self._respond()

    def _respond(self) -> None:
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(type(self).response_body)))
        self.end_headers()
        self.wfile.write(type(self).response_body)

    def log_message(self, _format: str, *args: object) -> None:
        del args


@contextmanager
def _coding_server(response: dict[str, object], *, status: int = 200):
    _CodingHandler.response_status = status
    _CodingHandler.response_body = json.dumps(response).encode("utf-8")
    _CodingHandler.observed_path = None
    _CodingHandler.observed_authorization = None
    _CodingHandler.observed_trace_id = None
    _CodingHandler.observed_body = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CodingHandler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def _invocation(*, pipeline_attempt: int = 2) -> NodeInvocation:
    return NodeInvocation.create(
        job_id=JOB_ID,
        profile_version_id=PROFILE_VERSION_ID,
        node_id="preview",
        pipeline_attempt=pipeline_attempt,
        execution_attempt=3,
        state_version=7,
        trace_id=TRACE_ID,
        workspace_id=WORKSPACE_ID,
        tool_call_id=None,
        context={},
        config={},
    )


def _result_response() -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "resultId": RESULT_ID,
        "jobId": JOB_ID,
        "traceId": TRACE_ID,
        "pipelineAttempt": 2,
        "handlerKey": "coding.preview",
        "resultType": "DIFF",
        "resultPort": "ready",
        "workspaceId": WORKSPACE_ID,
        "candidateSha": SHA,
        "diffDigest": DIGEST,
        "validationHash": DIGEST,
        "payload": {"artifactRef": "candidate/preview"},
        "recordedAt": NOW,
    }


def _aggregate_response() -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "jobId": JOB_ID,
        "traceId": TRACE_ID,
        "pipelineAttempt": 2,
        "status": "ACTIVE",
        "requestText": "sensitive coding request",
        "results": [_result_response()],
        "pendingApprovals": [
            {
                "approvalId": APPROVAL_ID,
                "nodeId": "preview_approval",
                "stage": "CANDIDATE",
                "stageRound": 1,
                "requiredRole": "GENERAL_ADMIN",
            }
        ],
        "decisions": [
            {
                "approvalId": APPROVAL_ID,
                "nodeId": "preview_approval",
                "stage": "CANDIDATE",
                "stageRound": 1,
                "decision": "APPROVED",
                "candidateSha": SHA,
                "validationHash": DIGEST,
                "actorId": ACTOR_ID,
                "actorRole": "SUPER_ADMIN",
                "resultStateVersion": 7,
                "decidedAt": NOW,
            }
        ],
        "createdAt": NOW,
    }


def _stage_execution_response() -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "resultId": RESULT_ID,
        "handlerKey": "coding.preview",
        "resultPort": "ready",
        "workspaceId": WORKSPACE_ID,
        "candidateSha": SHA,
        "diffDigest": DIGEST,
        "validationHash": DIGEST,
        "payload": {"status": "READY"},
    }


class SpringCodingDomainClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.leases: list[ServiceCredentialLease] = []

    def _credential(self) -> ServiceCredentialLease:
        lease = ServiceCredentialLease(b"spring-service-test-token")
        self.leases.append(lease)
        return lease

    def _client(self) -> SpringCodingDomainClient:
        return SpringCodingDomainClient(
            "http://127.0.0.1:18080",
            self._credential,
            allowed_origins={"http://127.0.0.1:18080"},
        )

    def test_attempt_accepts_backend_omitted_nullable_fields_and_exact_approvals(self) -> None:
        payload = _aggregate_response()
        with patch(
            "axms_coding_orchestrator.coding_domain_client._request_coding_http",
            return_value=(200, json.dumps(payload).encode("utf-8")),
        ) as request:
            result = self._client().get_attempt(_invocation())

        self.assertIsInstance(result, CodingAttemptAggregate)
        self.assertIsNone(result.workspace_id)
        self.assertIsNone(result.finished_at)
        self.assertEqual("preview_approval", result.pending_approvals[0].node_id)
        self.assertEqual("SUPER_ADMIN", result.decisions[0].actor_role)
        self.assertNotIn("sensitive coding request", repr(result))
        self.assertEqual(
            (
                "GET",
                f"http://127.0.0.1:18080/internal/coding/worker/jobs/{JOB_ID}/attempts/2",
                None,
            ),
            request.call_args.args[:3],
        )
        self.assertEqual(TRACE_ID, request.call_args.args[5])
        self.assertTrue(self.leases[0]._closed)
        self.assertEqual({0}, set(self.leases[0]._credential))

    def test_put_preserves_exact_feature_port_and_omits_absent_optionals(self) -> None:
        response = _result_response()
        response.pop("candidateSha")
        response.pop("diffDigest")
        response.pop("validationHash")
        response.pop("workspaceId")
        response["resultPort"] = "feasible"
        response["handlerKey"] = "coding.analyze"
        response["resultType"] = "ANALYSIS"
        response["payload"] = {}
        observed: dict[str, object] = {}

        def request_http(
            method: str,
            url: str,
            body: bytes | None,
            credential: bytearray,
            timeout_seconds: float,
            trace_id: str,
        ) -> tuple[int, bytes]:
            observed.update(
                method=method,
                url=url,
                body=json.loads(body or b"{}"),
                credential=bytes(credential),
                timeout=timeout_seconds,
                trace_id=trace_id,
            )
            return 200, json.dumps(response).encode("utf-8")

        write = CodingResultWrite(
            result_id=RESULT_ID,
            handler_key="coding.analyze",
            result_type="ANALYSIS",
            result_port="feasible",
            payload={},
        )
        with patch(
            "axms_coding_orchestrator.coding_domain_client._request_coding_http",
            side_effect=request_http,
        ):
            recorded = self._client().put_result(_invocation(), write)

        self.assertEqual("feasible", recorded.result_port)
        self.assertEqual(
            {
                "schemaVersion": "1.0",
                "traceId": TRACE_ID,
                "expectedStateVersion": 7,
                "handlerKey": "coding.analyze",
                "resultType": "ANALYSIS",
                "resultPort": "feasible",
                "payload": {},
            },
            observed["body"],
        )
        self.assertNotIn("workspaceId", observed["body"])
        self.assertEqual(b"spring-service-test-token", observed["credential"])
        self.assertEqual(TRACE_ID, observed["trace_id"])

    def test_put_transport_supports_the_exact_backend_method_and_path(self) -> None:
        response = _result_response()
        write = CodingResultWrite(
            result_id=RESULT_ID,
            handler_key="coding.preview",
            result_type="DIFF",
            result_port="ready",
            workspace_id=WORKSPACE_ID,
            candidate_sha=SHA,
            diff_digest=DIGEST,
            validation_hash=DIGEST,
            payload={"artifactRef": "candidate/preview"},
        )
        with _coding_server(response) as origin:
            client = SpringCodingDomainClient(
                origin, self._credential, allowed_origins={origin}
            )
            recorded = client.put_result(_invocation(), write)

        self.assertEqual(RESULT_ID, recorded.result_id)
        self.assertEqual(
            f"/internal/coding/worker/jobs/{JOB_ID}/attempts/2/results/{RESULT_ID}",
            _CodingHandler.observed_path,
        )
        self.assertEqual(
            "Bearer spring-service-test-token", _CodingHandler.observed_authorization
        )
        self.assertEqual(TRACE_ID, _CodingHandler.observed_trace_id)
        self.assertEqual("ready", _CodingHandler.observed_body["resultPort"])

    def test_execute_stage_posts_the_exact_authoritative_invocation(self) -> None:
        with _coding_server(_stage_execution_response()) as origin:
            client = SpringCodingDomainClient(
                origin, self._credential, allowed_origins={origin}
            )
            result = client.execute_stage(
                _invocation(), "coding.preview", RESULT_ID
            )

        self.assertIsInstance(result, CodingStageExecutionResult)
        self.assertEqual("ready", result.result_port)
        self.assertEqual(
            f"/internal/coding/worker/jobs/{JOB_ID}/attempts/2/stages/"
            f"coding.preview/executions/{RESULT_ID}",
            _CodingHandler.observed_path,
        )
        self.assertEqual(
            {
                "schemaVersion": "1.0",
                "traceId": TRACE_ID,
                "expectedStateVersion": 7,
                "executionAttempt": 3,
                "nodeId": "preview",
                "handlerKey": "coding.preview",
                "resultId": RESULT_ID,
            },
            _CodingHandler.observed_body,
        )
        self.assertEqual(
            "Bearer spring-service-test-token", _CodingHandler.observed_authorization
        )
        self.assertEqual(TRACE_ID, _CodingHandler.observed_trace_id)

    def test_real_get_maps_correlated_error_envelope_and_sends_trace_header(self) -> None:
        response = {
            "schemaVersion": "1.0",
            "traceId": TRACE_ID,
            "jobId": JOB_ID,
            "idempotencyKey": "attempt.2",
            "error": {
                "code": "JOB_STATE_VERSION_CONFLICT",
                "message": "remote-sensitive-state-detail",
                "retryable": False,
                "retryAfterMs": None,
            },
        }
        with _coding_server(response, status=409) as origin:
            client = SpringCodingDomainClient(
                origin, self._credential, allowed_origins={origin}
            )
            with self.assertRaises(CodingDomainClientError) as raised:
                client.get_attempt(_invocation())

        self.assertEqual("JOB_STATE_VERSION_CONFLICT", raised.exception.code)
        self.assertEqual(409, raised.exception.status)
        self.assertFalse(raised.exception.retryable)
        self.assertNotIn("remote-sensitive-state-detail", str(raised.exception))
        self.assertEqual(TRACE_ID, _CodingHandler.observed_trace_id)
        self.assertEqual(
            f"/internal/coding/worker/jobs/{JOB_ID}/attempts/2",
            _CodingHandler.observed_path,
        )

    def test_attempt_rejects_nested_result_correlation_mismatch(self) -> None:
        cases = {
            "jobId": "21212121-2121-4121-8121-212121212121",
            "traceId": "31313131-3131-4131-8131-313131313131",
            "pipelineAttempt": 1,
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                payload = _aggregate_response()
                payload["results"][0][field] = value
                with patch(
                    "axms_coding_orchestrator.coding_domain_client._request_coding_http",
                    return_value=(200, json.dumps(payload).encode("utf-8")),
                ):
                    with self.assertRaises(CodingDomainClientError) as raised:
                        self._client().get_attempt(_invocation())

                self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

    def test_response_correlation_mismatch_and_unknown_fields_fail_closed(self) -> None:
        response = _result_response()
        response["resultPort"] = "approved"
        with patch(
            "axms_coding_orchestrator.coding_domain_client._request_coding_http",
            return_value=(200, json.dumps(response).encode("utf-8")),
        ):
            with self.assertRaises(CodingDomainClientError) as raised:
                self._client().put_result(
                    _invocation(),
                    CodingResultWrite(
                        result_id=RESULT_ID,
                        handler_key="coding.preview",
                        result_type="DIFF",
                        result_port="ready",
                        workspace_id=WORKSPACE_ID,
                        candidate_sha=SHA,
                        diff_digest=DIGEST,
                        validation_hash=DIGEST,
                        payload={"artifactRef": "candidate/preview"},
                    ),
                )

        self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

        aggregate = _aggregate_response()
        aggregate["unexpected"] = "sensitive"
        with self.assertRaises(ValueError):
            CodingAttemptAggregate.from_dict(aggregate)


if __name__ == "__main__":
    unittest.main()
