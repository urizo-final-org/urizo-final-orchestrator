from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from axms_coding_orchestrator.model_gateway import (
    FileServiceCredentialResolver,
    ModelGatewayClient,
    ModelTurnRequest,
)


CONTEXT_DIGEST = "sha256:" + ("c" * 64)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the local Spring Model Turn bridge without a Provider call."
    )
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--credential-file", required=True, type=Path)
    parser.add_argument("--job-id", required=True, type=UUID)
    parser.add_argument("--expected-state-version", required=True, type=int)
    parser.add_argument("--trace-id", required=True, type=UUID)
    return parser.parse_args()


def request_payload(arguments: argparse.Namespace) -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "turnId": str(uuid4()),
        "jobId": str(arguments.job_id),
        "traceId": str(arguments.trace_id),
        "idempotencyKey": f"local.smoke.{uuid4().hex}",
        "attempt": 1,
        "expectedStateVersion": arguments.expected_state_version,
        "nodeName": "plan",
        "promptVersion": "local-smoke-v1",
        "contextDigest": CONTEXT_DIGEST,
        "requiredCapabilities": ["CHAT"],
        "messages": [
            {
                "role": "user",
                "content": "Verify the approved local mock Model Turn boundary.",
            }
        ],
        "toolSchemas": [],
        "responseFormat": {"type": "TEXT"},
        "deadlineAt": (
            datetime.now(timezone.utc) + timedelta(seconds=45)
        ).isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    arguments = parse_args()
    request = ModelTurnRequest.from_dict(request_payload(arguments))
    parsed_endpoint = urlsplit(arguments.endpoint)
    local_origin = f"http://{parsed_endpoint.hostname}:{parsed_endpoint.port or 80}"
    client = ModelGatewayClient(
        arguments.endpoint,
        FileServiceCredentialResolver(arguments.credential_file),
        max_timeout_seconds=30.0,
        allowed_origins={local_origin},
    )

    first = client.execute(request)
    replay = client.execute(request)
    if first.correlation() != request.correlation():
        raise RuntimeError("First response correlation did not match the request.")
    if replay.to_dict() != first.to_dict():
        raise RuntimeError("Idempotent replay did not return the stored response.")
    if first.to_dict()["assistant"] != {
        "role": "assistant",
        "content": "LOCAL_MOCK_MODEL_TURN_OK",
    }:
        raise RuntimeError("Local mock response was not deterministic.")

    print("Local Spring-Orchestrator Model Turn round trip and idempotent replay PASS.")


if __name__ == "__main__":
    main()
