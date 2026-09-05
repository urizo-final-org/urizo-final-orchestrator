from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.natural_cms_domain_client import (
    NaturalCmsJob,
    NaturalCmsResource,
    NaturalCmsStageResult,
    SpringNaturalCmsDomainClient,
)
from axms_coding_orchestrator.node_runtime import NodeInvocation


JOB_ID = "11111111-1111-4111-8111-111111111111"
PROFILE_VERSION_ID = "22222222-2222-4222-8222-222222222222"
TRACE_ID = "33333333-3333-4333-8333-333333333333"
RESULT_ID = "44444444-4444-4444-8444-444444444444"


class SpringNaturalCmsDomainClientTest(unittest.TestCase):
    def test_resource_accepts_supported_types_and_preserves_type(self) -> None:
        for resource_type in ("MENU", "BOARD", "CONTENT", "TEMPLATE"):
            with self.subTest(resource_type=resource_type):
                resource = NaturalCmsResource.from_dict({"type": resource_type, "id": "7"})

                self.assertEqual(resource_type, resource.type)
                self.assertEqual("7", resource.id)

    def test_resource_rejects_unsupported_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "resource is invalid"):
            NaturalCmsResource.from_dict({"type": "CMS_COMPOSITE", "id": "7"})

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
            "modelObservations": [
                {
                    "provider": "GOOGLE_GENAI",
                    "modelId": "gemini-final",
                    "inputTokens": 87,
                    "outputTokens": 16,
                    "latencyMs": 320,
                }
            ],
        }
        observed: dict[str, object] = {}
        observed_models: list[object] = []

        class ObservationSink:
            def record_models(self, values: object) -> None:
                observed_models.extend(values)  # type: ignore[arg-type]

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
            observability=ObservationSink(),  # type: ignore[arg-type]
        )
        with patch(
            "axms_coding_orchestrator.natural_cms_domain_client._request_coding_http",
            side_effect=request_http,
        ):
            result = client.execute_stage(invocation, "cms.preview", RESULT_ID)

        self.assertIsInstance(result, NaturalCmsStageResult)
        self.assertEqual(1, len(result.model_observations))
        self.assertEqual("GOOGLE_GENAI", result.model_observations[0].provider)
        self.assertEqual("gemini-final", result.model_observations[0].model)
        self.assertEqual(87, result.model_observations[0].input_tokens)
        self.assertEqual(16, result.model_observations[0].output_tokens)
        self.assertEqual(320, result.model_observations[0].latency_ms)
        self.assertEqual(list(result.model_observations), observed_models)
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

    def test_replay_or_legacy_stage_without_model_turns_has_no_observation(self) -> None:
        base = {
            "schemaVersion": "1.0",
            "resultId": RESULT_ID,
            "handlerKey": "cms.preview",
            "resultPort": "ready",
            "resource": {"type": "CONTENT", "id": "7"},
            "payload": {"status": "READY"},
        }
        self.assertEqual((), NaturalCmsStageResult.from_dict(base).model_observations)
        base["modelObservations"] = []
        self.assertEqual((), NaturalCmsStageResult.from_dict(base).model_observations)


if __name__ == "__main__":
    unittest.main()
