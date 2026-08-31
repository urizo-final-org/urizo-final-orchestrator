from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest

from axms_coding_orchestrator.contracts import (
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
)
from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.worker_api import WorkerApiClient, WorkerApiError

from factories import FIXED_NOW, coding_event, worker_claim


class _Handler(BaseHTTPRequestHandler):
    responses: dict[str, tuple[int, dict[str, object]]] = {}
    observed: list[tuple[str, dict[str, object], str | None]] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        type(self).observed.append((self.path, payload, self.headers.get("Authorization")))
        self._respond()

    def do_GET(self) -> None:  # noqa: N802
        type(self).observed.append((self.path, {}, self.headers.get("Authorization")))
        self._respond()

    def _respond(self) -> None:
        status, response = type(self).responses[self.path]
        raw = json.dumps(response).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *args: object) -> None:
        pass


@contextmanager
def worker_server(responses: dict[str, tuple[int, dict[str, object]]]):
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


def pending_approval(
    event: CodingJobRequested,
    claim: WorkerClaim,
) -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "approvalId": "61616161-6161-4161-8161-616161616161",
        "jobId": event.job_id,
        "profileVersionId": event.profile_version_id,
        "nodeId": "scope_approval",
        "stage": "SCOPE",
        "stageRound": 1,
        "requiredRole": "GENERAL_ADMIN",
        "pipelineAttempt": event.pipeline_attempt,
        "traceId": event.trace_id,
        "stateVersion": claim.state_version,
    }


def pending_approval_projection(
    value: dict[str, object],
) -> dict[str, object]:
    return {
        field: value[field]
        for field in (
            "approvalId",
            "nodeId",
            "stage",
            "stageRound",
            "requiredRole",
        )
    }


class WorkerApiClientTest(unittest.TestCase):
    def resolver(self) -> ServiceCredentialLease:
        return ServiceCredentialLease(b"spring-worker-test-token")

    def client(self, origin: str) -> WorkerApiClient:
        return WorkerApiClient(
            origin,
            self.resolver,
            now=lambda: FIXED_NOW,
            allowed_origins={origin},
        )

    def test_claim_heartbeat_and_outcome_preserve_lease_context(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim_payload = worker_claim(event.to_dict())
        expected_pending = pending_approval(
            event,
            WorkerClaim.from_dict(claim_payload, event, now=FIXED_NOW),
        )
        job_id = event.job_id
        responses = {
            f"/internal/coding/worker/jobs/{job_id}/claim-context": (
                200,
                event.to_dict(),
            ),
            f"/internal/coding/worker/jobs/{job_id}/claim": (200, claim_payload),
            f"/internal/coding/worker/jobs/{job_id}/heartbeat": (
                200,
                {
                    "schemaVersion": "1.0",
                    "jobId": job_id,
                    "traceId": event.trace_id,
                    "leaseId": claim_payload["leaseId"],
                    "leaseExpiresAt": "2026-08-11T10:15:00Z",
                    "stateVersion": 5,
                },
            ),
            f"/internal/coding/worker/jobs/{job_id}/outcomes": (
                200,
                {
                    "schemaVersion": "1.0",
                    "jobId": job_id,
                    "traceId": event.trace_id,
                    "stateVersion": 6,
                    "status": "WAITING_APPROVAL",
                    "pendingApproval": pending_approval_projection(
                        expected_pending
                    ),
                },
            ),
        }
        with worker_server(responses) as origin:
            client = self.client(origin)
            resolved = client.resolve(
                QueuedJobReference.from_dict({"jobId": job_id})
            )
            claim = client.claim(resolved)
            heartbeat = client.heartbeat(claim, "heartbeat.test.0001")
            receipt = client.outcome(
                claim,
                "WAITING_APPROVAL",
                "outcome.test.0001",
                pending_approval=expected_pending,
            )

        self.assertEqual(event.to_dict(), resolved.to_dict())
        self.assertEqual(claim.lease_id, heartbeat["leaseId"])
        self.assertEqual("WAITING_APPROVAL", receipt["status"])
        self.assertTrue(all(item[2] == "Bearer spring-worker-test-token" for item in _Handler.observed))
        self.assertEqual(claim.lease_id, _Handler.observed[2][1]["leaseId"])
        self.assertEqual(claim.state_version, _Handler.observed[3][1]["expectedStateVersion"])

    def test_waiting_outcome_preserves_exact_pending_approval(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim_payload = worker_claim(event.to_dict())
        claim = WorkerClaim.from_dict(claim_payload, event, now=FIXED_NOW)
        expected = pending_approval(event, claim)
        projected = pending_approval_projection(expected)
        path = f"/internal/coding/worker/jobs/{event.job_id}/outcomes"
        responses = {
            path: (
                200,
                {
                    "schemaVersion": "1.0",
                    "jobId": event.job_id,
                    "traceId": event.trace_id,
                    "stateVersion": claim.state_version + 1,
                    "status": "WAITING_APPROVAL",
                    "pendingApproval": projected,
                },
            )
        }

        with worker_server(responses) as origin:
            receipt = self.client(origin).outcome(
                claim,
                "WAITING_APPROVAL",
                "outcome.test.pending-approval",
                pending_approval=expected,
            )

        self.assertEqual(projected, receipt["pendingApproval"])
        self.assertEqual(projected, _Handler.observed[0][1]["pendingApproval"])

    def test_waiting_outcome_rejects_mismatched_approval_receipt(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        expected = pending_approval(event, claim)
        mismatched = pending_approval_projection(expected)
        mismatched["stageRound"] = 2
        path = f"/internal/coding/worker/jobs/{event.job_id}/outcomes"
        responses = {
            path: (
                200,
                {
                    "schemaVersion": "1.0",
                    "jobId": event.job_id,
                    "traceId": event.trace_id,
                    "stateVersion": claim.state_version + 1,
                    "status": "WAITING_APPROVAL",
                    "pendingApproval": mismatched,
                },
            )
        }

        with worker_server(responses) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).outcome(
                    claim,
                    "WAITING_APPROVAL",
                    "outcome.test.mismatched-approval",
                    pending_approval=expected,
                )

        self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

    def test_waiting_outcome_rejects_partial_approval_authority(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        complete = pending_approval(event, claim)
        client = self.client("http://127.0.0.1:1")

        for field in (
            "approvalId",
            "nodeId",
            "stage",
            "stageRound",
            "requiredRole",
        ):
            partial = dict(complete)
            partial.pop(field)
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError,
                "pending_approval is invalid",
            ):
                client.outcome(
                    claim,
                    "WAITING_APPROVAL",
                    f"outcome.test.partial.{field}",
                    pending_approval=partial,
                )

    def test_resolve_rejects_unbound_or_mismatched_job_context(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        path = f"/internal/coding/worker/jobs/{job.job_id}/claim-context"
        unbound = coding_event()
        for field in (
            "profileVersionId",
            "pipelineAttempt",
            "executionAttempt",
            "workspaceId",
            "toolCallId",
        ):
            unbound.pop(field)
        mismatched = coding_event()
        mismatched["jobId"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        for payload in (unbound, mismatched):
            with self.subTest(fields=set(payload)):
                with worker_server({path: (200, payload)}) as origin:
                    with self.assertRaises(WorkerApiError) as raised:
                        self.client(origin).resolve(job)
                self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

    def test_claim_rejects_scope_mismatch(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        payload = worker_claim(event.to_dict())
        payload["snapshot"]["contextDigest"] = "sha256:" + ("f" * 64)
        path = f"/internal/coding/worker/jobs/{event.job_id}/claim"
        with worker_server({path: (200, payload)}) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).claim(event)

        self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

    def test_worker_error_envelope_is_exact_and_sanitized(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        path = f"/internal/coding/worker/jobs/{event.job_id}/claim"
        envelope = {
            "schemaVersion": "1.0",
            "traceId": event.trace_id,
            "jobId": event.job_id,
            "idempotencyKey": event.idempotency_key,
            "error": {
                "code": "JOB_STATE_VERSION_CONFLICT",
                "message": "safe conflict",
                "retryable": False,
            },
        }
        with worker_server({path: (409, envelope)}) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).claim(event)
        self.assertEqual("JOB_STATE_VERSION_CONFLICT", raised.exception.code)
        self.assertNotIn("safe conflict", str(raised.exception))

        envelope["jobId"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        with worker_server({path: (409, envelope)}) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).claim(event)
        self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)
        envelope["jobId"] = event.job_id

        envelope["providerBody"] = "must-not-be-accepted"
        with worker_server({path: (409, envelope)}) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).claim(event)
        self.assertEqual("WORKER_RESPONSE_INVALID", raised.exception.code)

    def test_pre_context_error_is_exact_but_not_body_correlated(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        path = f"/internal/coding/worker/jobs/{event.job_id}/claim"
        envelope = {
            "schemaVersion": "1.0",
            "requestId": "17171717-1717-4717-8717-171717171717",
            "traceId": "18181818-1818-4818-8818-181818181818",
            "error": {
                "code": "SERVICE_AUTHENTICATION_FAILED",
                "message": "safe authentication failure",
                "retryable": False,
            },
        }
        with worker_server({path: (401, envelope)}) as origin:
            with self.assertRaises(WorkerApiError) as raised:
                self.client(origin).claim(event)

        self.assertEqual("SERVICE_AUTHENTICATION_FAILED", raised.exception.code)

    def test_default_origin_rejects_non_spring_host(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowlisted"):
            WorkerApiClient("http://127.0.0.1:8080", self.resolver)

    def test_health_probe_requires_the_exact_spring_liveness_contract(self) -> None:
        healthy = {
            "schemaVersion": "1.0",
            "traceId": "abababab-abab-4bab-8bab-abababababab",
            "status": "UP",
            "checkedAt": "2026-08-11T10:00:00Z",
        }
        with worker_server({"/api/health": (200, healthy)}) as origin:
            self.assertTrue(self.client(origin).healthy())
            self.assertIsNone(_Handler.observed[0][2])

        healthy["provider"] = "LOCAL"
        with worker_server({"/api/health": (200, healthy)}) as origin:
            self.assertFalse(self.client(origin).healthy())


if __name__ == "__main__":
    unittest.main()
