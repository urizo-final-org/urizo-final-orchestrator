from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.profile_version_client import (
    PROFILE_VERSION_PATH,
    ProfileVersionClient,
    ProfileVersionClientError,
)


PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
FIXTURE = Path(__file__).parent / "fixtures" / "versioned-snapshot.valid.json"


class _Handler(BaseHTTPRequestHandler):
    responses: dict[str, tuple[int, bytes]] = {}
    observed: list[tuple[str, str | None]] = []

    def do_GET(self) -> None:  # noqa: N802
        type(self).observed.append((self.path, self.headers.get("Authorization")))
        status, raw = type(self).responses[self.path]
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *args: object) -> None:
        del args


@contextmanager
def profile_server(responses: dict[str, tuple[int, bytes]]):
    _Handler.responses = responses
    _Handler.observed = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _error(code: str, *, retryable: bool, retry_after_ms: int | None = None) -> bytes:
    detail: dict[str, object] = {
        "code": code,
        "message": "remote detail must stay private",
        "retryable": retryable,
        "executionState": "NOT_STARTED",
    }
    if retry_after_ms is not None:
        detail["retryAfterMs"] = retry_after_ms
    return json.dumps(
        {
            "schemaVersion": "1.0",
            "requestId": "12121212-1212-4212-8212-121212121212",
            "traceId": "13131313-1313-4313-8313-131313131313",
            "error": detail,
        }
    ).encode("utf-8")


class ProfileVersionClientTest(unittest.TestCase):
    def resolver(self) -> ServiceCredentialLease:
        return ServiceCredentialLease(b"spring-profile-test-token")

    def client(self, origin: str) -> ProfileVersionClient:
        return ProfileVersionClient(
            origin,
            self.resolver,
            allowed_origins={origin},
        )

    def test_get_uses_exact_path_auth_and_raw_snapshot_contract(self) -> None:
        path = f"{PROFILE_VERSION_PATH}/{PROFILE_VERSION_ID}"
        with profile_server({path: (200, FIXTURE.read_bytes())}) as origin:
            snapshot = self.client(origin).get(PROFILE_VERSION_ID)

        self.assertEqual(PROFILE_VERSION_ID, snapshot.profile_version_id)
        self.assertEqual(
            [(path, "Bearer spring-profile-test-token")],
            _Handler.observed,
        )

    def test_origin_uuid_and_timeout_are_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowlisted"):
            ProfileVersionClient("http://127.0.0.1:8080", self.resolver)
        with self.assertRaisesRegex(ValueError, "timeout"):
            ProfileVersionClient(
                "http://spring-app:8080",
                self.resolver,
                timeout_seconds=float("inf"),
            )
        client = ProfileVersionClient("http://spring-app:8080", self.resolver)
        with self.assertRaisesRegex(ValueError, "profileVersionId"):
            client.get("../snapshot")

    def test_invalid_json_and_profile_id_mismatch_are_sanitized(self) -> None:
        path = f"{PROFILE_VERSION_PATH}/{PROFILE_VERSION_ID}"
        changed = json.loads(FIXTURE.read_bytes())
        changed["profileVersionId"] = "14141414-1414-4414-8414-141414141414"
        cases = (
            b'{"not":"a snapshot"}',
            json.dumps(changed).encode("utf-8"),
        )
        for raw in cases:
            with self.subTest(raw=raw[:24]):
                with profile_server({path: (200, raw)}) as origin:
                    with self.assertRaises(ProfileVersionClientError) as raised:
                        self.client(origin).get(PROFILE_VERSION_ID)
                self.assertEqual("PROFILE_VERSION_RESPONSE_INVALID", raised.exception.code)
                self.assertFalse(raised.exception.retryable)
                self.assertNotIn("not a snapshot", str(raised.exception))

    def test_supported_remote_errors_are_exact_and_sanitized(self) -> None:
        path = f"{PROFILE_VERSION_PATH}/{PROFILE_VERSION_ID}"
        cases = (
            (401, "SERVICE_AUTHENTICATION_FAILED", False, None),
            (404, "PROFILE_VERSION_NOT_FOUND", False, None),
            (409, "PROFILE_VERSION_NOT_ACTIVE", False, None),
            (503, "INTERNAL_TRANSIENT_ERROR", True, 250),
        )
        for status, code, retryable, retry_after in cases:
            with self.subTest(status=status):
                body = _error(
                    code,
                    retryable=retryable,
                    retry_after_ms=retry_after,
                )
                with profile_server({path: (status, body)}) as origin:
                    with self.assertRaises(ProfileVersionClientError) as raised:
                        self.client(origin).get(PROFILE_VERSION_ID)
                self.assertEqual(code, raised.exception.code)
                self.assertEqual(retryable, raised.exception.retryable)
                self.assertEqual(retry_after, raised.exception.retry_after_ms)
                self.assertNotIn("remote detail", str(raised.exception))

    def test_malformed_error_envelope_is_not_trusted(self) -> None:
        path = f"{PROFILE_VERSION_PATH}/{PROFILE_VERSION_ID}"
        envelope = json.loads(_error("PROFILE_VERSION_NOT_FOUND", retryable=False))
        envelope["profileVersionId"] = PROFILE_VERSION_ID
        wrong_state = json.loads(
            _error("PROFILE_VERSION_NOT_FOUND", retryable=False)
        )
        wrong_state["error"]["executionState"] = "COMPLETED"
        for value in (envelope, wrong_state):
            with self.subTest(fields=set(value)):
                with profile_server(
                    {path: (404, json.dumps(value).encode("utf-8"))}
                ) as origin:
                    with self.assertRaises(ProfileVersionClientError) as raised:
                        self.client(origin).get(PROFILE_VERSION_ID)

                self.assertEqual(
                    "PROFILE_VERSION_RESPONSE_INVALID",
                    raised.exception.code,
                )

    def test_malformed_5xx_body_is_still_a_sanitized_transient_failure(self) -> None:
        path = f"{PROFILE_VERSION_PATH}/{PROFILE_VERSION_ID}"
        with profile_server({path: (502, b"proxy detail must stay private")}) as origin:
            with self.assertRaises(ProfileVersionClientError) as raised:
                self.client(origin).get(PROFILE_VERSION_ID)

        self.assertEqual("INTERNAL_TRANSIENT_ERROR", raised.exception.code)
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(502, raised.exception.status)
        self.assertNotIn("proxy detail", str(raised.exception))

    def test_network_failure_is_retryable_and_payload_free(self) -> None:
        client = ProfileVersionClient("http://spring-app:8080", self.resolver)
        with patch(
            "axms_coding_orchestrator.profile_version_client._request_http",
            side_effect=OSError("host secret"),
        ):
            with self.assertRaises(ProfileVersionClientError) as raised:
                client.get(PROFILE_VERSION_ID)

        self.assertEqual("INTERNAL_TRANSIENT_ERROR", raised.exception.code)
        self.assertTrue(raised.exception.retryable)
        self.assertNotIn("host secret", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
