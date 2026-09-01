from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.natural_cms_domain_client import (
    NaturalCmsJob,
    NaturalCmsStageResult,
    SpringNaturalCmsDomainClient,
)
from axms_coding_orchestrator.node_runtime import NodeInvocation


JOB_ID = "11111111-1111-4111-8111-111111111111"
PROFILE_VERSION_ID = "22222222-2222-4222-8222-222222222222"
TRACE_ID = "33333333-3333-4333-8333-333333333333"
RESULT_ID = "44444444-4444-4444-8444-444444444444"


class SpringNaturalCmsDomainClientTest(unittest.TestCase):
    def test_resolve_job_uses_the_job_id_only_spring_boundary(self) -> None:
        response = {
            "schemaVersion": "1.0",
            "jobId": JOB_ID,
            "traceId": TRACE_ID,
            "profileVersionId": PROFILE_VERSION_ID,
            "pipelineAttempt": 2,
            "stateVersion": 7,
            "status": "WAITING_APPROVAL",
            "requestText": "Update content 7",
            "resource": {"type": "CONTENT", "id": "7"},
            "structuredCommand": {"operation": "UPDATE"},
            "previewId": "55555555-5555-4555-8555-555555555555",
            "previewHash": "sha256:" + ("a" * 64),
            "previewValid": True,
            "approvalDecision": "APPROVED",
            "approvalFeedback": None,
            "createdAt": "2026-09-01T00:00:00Z",
            "updatedAt": "2026-09-01T00:01:00Z",
        }
        observed: dict[str, object] = {}

        def request_http(
            method: str,
            url: str,
            body: bytes | None,
            credential: bytearray,
            timeout_seconds: float,
        ) -> tuple[int, bytes]:
            observed.update(
                method=method,
                url=url,
                body=body,
                credential=bytes(credential),
                timeout=timeout_seconds,
            )
            return 200, json.dumps(response).encode("utf-8")

        client = SpringNaturalCmsDomainClient(
            "http://127.0.0.1:18080",
            lambda: ServiceCredentialLease(b"spring-service-test-token"),
            allowed_origins={"http://127.0.0.1:18080"},
        )
        reference = QueuedJobReference.from_dict({"jobId": JOB_ID})
        with patch(
            "axms_coding_orchestrator.natural_cms_domain_client._request_http",
            side_effect=request_http,
        ):
            job = client.resolve_job(reference)

        self.assertIsInstance(job, NaturalCmsJob)
        self.assertEqual(JOB_ID, job.job_id)
        self.assertEqual("Update content 7", job.request_text)
        self.assertEqual("GET", observed["method"])
        self.assertEqual(
            f"http://127.0.0.1:18080/internal/natural-cms/jobs/{JOB_ID}",
            observed["url"],
        )
        self.assertIsNone(observed["body"])
        self.assertEqual(b"spring-service-test-token", observed["credential"])

    def test_execute_stage_passes_the_frozen_profile_and_current_node(self) -> None:
        invocation = NodeInvocation.create(
            job_id=JOB_ID,
            profile_version_id=PROFILE_VERSION_ID,
            node_id="preview",
            pipeline_attempt=2,
            execution_attempt=3,
            state_version=7,
            trace_id=TRACE_ID,
            workspace_id=None,
            tool_call_id=None,
            context={},
            config={},
        )
        response = {
            "schemaVersion": "1.0",
            "resultId": RESULT_ID,
            "handlerKey": "cms.preview",
            "resultPort": "ready",
            "resource": {"type": "CONTENT", "id": "7"},
            "payload": {"status": "READY"},
        }
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

        client = SpringNaturalCmsDomainClient(
            "http://127.0.0.1:18080",
            lambda: ServiceCredentialLease(b"spring-service-test-token"),
            allowed_origins={"http://127.0.0.1:18080"},
        )
        with patch(
            "axms_coding_orchestrator.natural_cms_domain_client._request_coding_http",
            side_effect=request_http,
        ):
            result = client.execute_stage(invocation, "cms.preview", RESULT_ID)

        self.assertIsInstance(result, NaturalCmsStageResult)
        self.assertEqual(
            {
                "schemaVersion": "1.0",
                "traceId": TRACE_ID,
                "profileVersionId": PROFILE_VERSION_ID,
                "expectedStateVersion": 7,
                "executionAttempt": 3,
                "nodeId": "preview",
                "handlerKey": "cms.preview",
                "resultId": RESULT_ID,
            },
            observed["body"],
        )
        self.assertEqual(b"spring-service-test-token", observed["credential"])
        self.assertEqual(TRACE_ID, observed["trace_id"])


if __name__ == "__main__":
    unittest.main()
