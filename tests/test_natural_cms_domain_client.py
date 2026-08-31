from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from axms_coding_orchestrator.model_gateway import ServiceCredentialLease
from axms_coding_orchestrator.natural_cms_domain_client import (
    NaturalCmsStageResult,
    SpringNaturalCmsDomainClient,
)
from axms_coding_orchestrator.node_runtime import NodeInvocation


JOB_ID = "11111111-1111-4111-8111-111111111111"
PROFILE_VERSION_ID = "22222222-2222-4222-8222-222222222222"
TRACE_ID = "33333333-3333-4333-8333-333333333333"
RESULT_ID = "44444444-4444-4444-8444-444444444444"


class SpringNaturalCmsDomainClientTest(unittest.TestCase):
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
