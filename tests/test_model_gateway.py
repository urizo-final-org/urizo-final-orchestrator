from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from urllib.parse import urlsplit

from axms_coding_orchestrator.model_gateway import (
    ContractViolation,
    FileServiceCredentialResolver,
    ModelGatewayClient,
    ModelGatewayRemoteError,
    ModelTurnRequest,
    ModelTurnResponse,
    ServiceCredentialLease,
)


FIXTURES = Path(__file__).parent / "fixtures"
FIXED_NOW = datetime(2026, 8, 10, 9, 10, 0, tzinfo=timezone.utc)


def fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class _Handler(BaseHTTPRequestHandler):
    response_status = 200
    response_body = b"{}"
    observed_authorization: str | None = None
    observed_request: dict[str, object] | None = None

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        length = int(self.headers.get("Content-Length", "0"))
        type(self).observed_authorization = self.headers.get("Authorization")
        type(self).observed_request = json.loads(self.rfile.read(length))
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(type(self).response_body)))
        self.end_headers()
        self.wfile.write(type(self).response_body)

    def log_message(self, _format: str, *args: object) -> None:
        pass


@contextmanager
def gateway_server(status: int, payload: dict[str, object]):
    _Handler.response_status = status
    _Handler.response_body = json.dumps(payload).encode("utf-8")
    _Handler.observed_authorization = None
    _Handler.observed_request = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/internal/coding/model-turns"
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class ModelTurnContractTest(unittest.TestCase):
    def test_backend_golden_request_round_trips(self) -> None:
        payload = fixture("model-turn.request.valid.json")

        parsed = ModelTurnRequest.from_dict(payload)

        self.assertEqual(payload, parsed.to_dict())
        self.assertNotIn("Plan within", repr(parsed))

    def test_backend_golden_response_round_trips(self) -> None:
        payload = fixture("model-turn.response.valid.json")

        parsed = ModelTurnResponse.from_dict(payload)

        self.assertEqual(payload, parsed.to_dict())
        self.assertNotIn("inspect the approved", repr(parsed))

    def test_unknown_request_field_and_version_are_rejected(self) -> None:
        unknown = fixture("model-turn.request.valid.json")
        unknown["provider"] = "OPENAI"
        with self.assertRaisesRegex(ContractViolation, "unknown fields"):
            ModelTurnRequest.from_dict(unknown)

        unsupported = fixture("model-turn.request.valid.json")
        unsupported["schemaVersion"] = "1.1"
        with self.assertRaisesRegex(ContractViolation, "unsupported"):
            ModelTurnRequest.from_dict(unsupported)

    def test_request_and_response_are_defensive_copies(self) -> None:
        source = fixture("model-turn.request.valid.json")
        parsed = ModelTurnRequest.from_dict(source)
        source["messages"] = []

        returned = parsed.to_dict()
        returned["messages"] = []

        self.assertEqual(2, len(parsed.to_dict()["messages"]))


class ModelGatewayClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.request = ModelTurnRequest.from_dict(fixture("model-turn.request.valid.json"))
        self.secret = b"spring-service-test-token"
        self.leases: list[ServiceCredentialLease] = []

    def resolver(self) -> ServiceCredentialLease:
        lease = ServiceCredentialLease(self.secret)
        self.leases.append(lease)
        return lease

    def client(self, endpoint: str) -> ModelGatewayClient:
        parsed = urlsplit(endpoint)
        origin = f"http://{parsed.hostname}:{parsed.port}"
        return ModelGatewayClient(
            endpoint,
            self.resolver,
            now=lambda: FIXED_NOW,
            allowed_origins={origin},
        )

    def test_valid_response_preserves_correlation_and_closes_credential(self) -> None:
        response = fixture("model-turn.response.valid.json")
        with gateway_server(200, response) as endpoint:
            result = self.client(endpoint).execute(self.request)

        self.assertEqual(self.request.correlation(), result.correlation())
        self.assertEqual("Bearer spring-service-test-token", _Handler.observed_authorization)
        self.assertEqual(self.request.to_dict(), _Handler.observed_request)
        self.assertEqual("ServiceCredentialLease[credential=REDACTED, closed=True]", repr(self.leases[0]))
        self.assertEqual({0}, set(self.leases[0]._credential))

    def test_mismatched_response_correlation_is_rejected(self) -> None:
        response = fixture("model-turn.response.mismatched-turn.valid.json")
        with gateway_server(200, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("CONTRACT_CORRELATION_MISMATCH", raised.exception.code)
        self.assertFalse(raised.exception.retryable)

    def test_contract_error_envelope_is_sanitized(self) -> None:
        response = fixture("model-turn.validation-error.valid.json")
        response["idempotencyKey"] = self.request.to_dict()["idempotencyKey"]
        with gateway_server(422, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
        self.assertEqual(422, raised.exception.status)
        self.assertFalse(raised.exception.retryable)
        self.assertNotIn(self.secret.decode("ascii"), str(raised.exception))

    def test_invalid_error_envelope_does_not_echo_remote_payload(self) -> None:
        response = {"unexpected": "remote-sensitive-detail"}
        with gateway_server(500, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("MODEL_RESPONSE_INVALID", raised.exception.code)
        self.assertNotIn("remote-sensitive-detail", str(raised.exception))

    def test_pre_context_authentication_error_is_accepted_and_sanitized(self) -> None:
        response = {
            "schemaVersion": "1.0",
            "requestId": "77777777-7777-4777-8777-777777777777",
            "traceId": "66666666-6666-4666-8666-666666666666",
            "error": {
                "code": "SERVICE_AUTHENTICATION_FAILED",
                "message": "Service authentication failed.",
                "retryable": False,
            },
        }
        with gateway_server(401, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("SERVICE_AUTHENTICATION_FAILED", raised.exception.code)
        self.assertEqual(401, raised.exception.status)
        self.assertFalse(raised.exception.retryable)

    def test_elapsed_deadline_fails_before_resolving_credential(self) -> None:
        calls = 0

        def resolver() -> ServiceCredentialLease:
            nonlocal calls
            calls += 1
            return ServiceCredentialLease(self.secret)

        client = ModelGatewayClient(
            "http://127.0.0.1:1/internal/coding/model-turns",
            resolver,
            now=lambda: datetime(2026, 8, 10, 9, 12, 0, tzinfo=timezone.utc),
            allowed_origins={"http://127.0.0.1:1"},
        )

        with self.assertRaises(ModelGatewayRemoteError) as raised:
            client.execute(self.request)

        self.assertEqual("MODEL_TIMEOUT", raised.exception.code)
        self.assertEqual(0, calls)

    def test_service_credential_rejects_header_injection_bytes(self) -> None:
        with self.assertRaisesRegex(ValueError, "visible ASCII"):
            ServiceCredentialLease(b"unsafe\r\nX-Injected: true")

    def test_file_credential_resolver_does_not_cache_or_display_plaintext(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "service-token"
            path.write_bytes(self.secret)
            resolver = FileServiceCredentialResolver(path)

            with resolver() as lease:
                copied = lease.copy()
            try:
                self.assertEqual(self.secret, bytes(copied))
                self.assertNotIn(self.secret.decode("ascii"), repr(resolver))
            finally:
                for index in range(len(copied)):
                    copied[index] = 0

    def test_response_format_must_be_bound_to_request(self) -> None:
        response = fixture("model-turn.response.valid.json")
        response["responseFormat"] = {
            "type": "JSON_SCHEMA",
            "schemaDigest": "sha256:" + ("a" * 64),
            "structuredOutput": {"ok": True},
        }
        with gateway_server(200, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("MODEL_RESPONSE_INVALID", raised.exception.code)

    def test_unrequested_tool_candidate_is_rejected(self) -> None:
        request = fixture("model-turn.request.valid.json")
        request["requiredCapabilities"] = ["CHAT"]
        request["toolSchemas"] = []
        parsed = ModelTurnRequest.from_dict(request)
        response = fixture("model-turn.response.valid.json")
        with gateway_server(200, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(parsed)

        self.assertEqual("MODEL_RESPONSE_INVALID", raised.exception.code)

    def test_unknown_remote_error_code_is_rejected(self) -> None:
        response = fixture("model-turn.validation-error.valid.json")
        response["idempotencyKey"] = self.request.to_dict()["idempotencyKey"]
        response["error"]["code"] = "UNKNOWN_REMOTE_CODE"
        with gateway_server(422, response) as endpoint:
            with self.assertRaises(ModelGatewayRemoteError) as raised:
                self.client(endpoint).execute(self.request)

        self.assertEqual("MODEL_RESPONSE_INVALID", raised.exception.code)

    def test_default_endpoint_is_pinned_to_spring_private_origin(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowlisted"):
            ModelGatewayClient(
                "http://127.0.0.1:8080/internal/coding/model-turns",
                self.resolver,
            )


if __name__ == "__main__":
    unittest.main()
