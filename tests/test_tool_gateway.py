from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest

from axms_coding_orchestrator.contracts import CodingJobRequested, WorkerClaim
from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.tool_gateway import (
    ToolGatewayClient,
    ToolGatewayError,
    build_read_file_request,
)

from factories import FIXED_NOW, coding_event, result_reference, worker_claim


class _Handler(BaseHTTPRequestHandler):
    responses: dict[tuple[str, str], tuple[int, dict[str, object]]] = {}
    observed_request: dict[str, object] | None = None

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        type(self).observed_request = json.loads(self.rfile.read(length))
        self._respond("POST")

    def do_GET(self) -> None:  # noqa: N802
        self._respond("GET")

    def _respond(self, method: str) -> None:
        status, payload = type(self).responses[(method, self.path)]
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *args: object) -> None:
        pass


@contextmanager
def tool_server(responses: dict[tuple[str, str], tuple[int, dict[str, object]]]):
    _Handler.responses = responses
    _Handler.observed_request = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class ToolGatewayClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.event = CodingJobRequested.from_dict(coding_event())
        self.claim = WorkerClaim.from_dict(
            worker_claim(self.event.to_dict()), self.event, now=FIXED_NOW
        )
        self.tool_call = {
            "toolCallId": "90909090-9090-4090-8090-909090909090",
            "name": "read_file",
            "arguments": {"path": "contracts/README.md"},
        }
        self.request = build_read_file_request(self.event, self.claim, self.tool_call)

    def resolver(self) -> ServiceCredentialLease:
        return ServiceCredentialLease(b"spring-tool-test-token")

    def client(self, origin: str) -> ToolGatewayClient:
        return ToolGatewayClient(
            origin,
            self.resolver,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            allowed_origins={origin},
        )

    def responses(self, *, content: str = "Approved contract content.") -> dict[tuple[str, str], tuple[int, dict[str, object]]]:
        request = self.request
        execution_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        reference = result_reference(content, execution_id)
        common = {
            "schemaVersion": "1.0",
            "requestId": request["requestId"],
            "toolCallId": request["toolCallId"],
            "jobId": request["jobId"],
            "traceId": request["traceId"],
            "idempotencyKey": request["idempotencyKey"],
            "executionId": execution_id,
        }
        accepted = {
            **common,
            "messageType": "TOOL_ACCEPTED",
            "status": "ACCEPTED",
            "statusUrl": f"/internal/coding/tool-executions/{execution_id}",
            "pollAfterMs": 1,
            "acceptedAt": "2026-08-11T10:00:01Z",
        }
        succeeded = {
            **common,
            "messageType": "TOOL_RESULT",
            "status": "SUCCEEDED",
            "result": reference,
            "completedAt": "2026-08-11T10:00:02Z",
        }
        result = {
            **common,
            "mediaType": reference["mediaType"],
            "sizeBytes": reference["sizeBytes"],
            "digest": reference["digest"],
            "content": content,
        }
        return {
            ("POST", "/internal/coding/tool-requests"): (202, accepted),
            ("GET", accepted["statusUrl"]): (200, succeeded),
            ("GET", reference["resultRef"]): (200, result),
        }

    def test_async_read_file_is_polled_and_digest_bound(self) -> None:
        with tool_server(self.responses()) as origin:
            result = self.client(origin).execute_read_file(self.request)

        self.assertEqual("Approved contract content.", result.content)
        self.assertEqual(self.claim.lease_id, _Handler.observed_request["leaseId"])
        self.assertEqual(self.claim.state_version, _Handler.observed_request["expectedStateVersion"])

    def test_result_content_digest_mismatch_is_rejected(self) -> None:
        responses = self.responses()
        result_path = next(path for method, path in responses if path.endswith("/result"))
        responses[("GET", result_path)][1]["content"] = "tampered"
        with tool_server(responses) as origin:
            with self.assertRaises(ToolGatewayError) as raised:
                self.client(origin).execute_read_file(self.request)

        self.assertEqual("TOOL_RESPONSE_INVALID", raised.exception.code)

    def test_model_cannot_expand_the_approved_path(self) -> None:
        candidate = dict(self.tool_call)
        candidate["arguments"] = {"path": "../outside.txt"}

        with self.assertRaises(ToolGatewayError) as raised:
            build_read_file_request(self.event, self.claim, candidate)

        self.assertEqual("PATH_POLICY_DENIED", raised.exception.code)

    def test_job_scoped_remote_error_must_match_request_correlation(self) -> None:
        error = {
            "schemaVersion": "1.0",
            "traceId": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "jobId": self.request["jobId"],
            "idempotencyKey": self.request["idempotencyKey"],
            "error": {
                "code": "TOOL_EXECUTOR_UNAVAILABLE",
                "message": "safe transient",
                "retryable": True,
                "retryAfterMs": 1_000,
            },
        }
        responses = {
            ("POST", "/internal/coding/tool-requests"): (503, error),
        }

        with tool_server(responses) as origin:
            with self.assertRaises(ToolGatewayError) as raised:
                self.client(origin).execute_read_file(self.request)

        self.assertEqual("TOOL_RESPONSE_INVALID", raised.exception.code)


if __name__ == "__main__":
    unittest.main()
