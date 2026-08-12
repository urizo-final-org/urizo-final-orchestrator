from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from typing import Any


FIXED_NOW = datetime(2026, 8, 11, 10, 0, 0, tzinfo=timezone.utc)


def coding_event(*, event_id: str = "10101010-1010-4010-8010-101010101010", version: int = 4, attempt: int = 1) -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "eventId": event_id,
        "eventType": "CODING_JOB_REQUESTED",
        "jobId": "20202020-2020-4020-8020-202020202020",
        "traceId": "30303030-3030-4030-8030-303030303030",
        "idempotencyKey": f"coding.event.{event_id}",
        "attempt": attempt,
        "expectedStateVersion": version,
        "occurredAt": "2026-08-11T10:00:00Z",
        "payload": {
            "actorId": "40404040-4040-4040-8040-404040404040",
            "projectId": "50505050-5050-4050-8050-505050505050",
            "repositoryId": "60606060-6060-4060-8060-606060606060",
            "graphStep": "inspect",
            "baseSha": "sha1:" + ("1" * 40),
            "contextDigest": "sha256:" + ("2" * 64),
            "policyHash": "sha256:" + ("3" * 64),
            "expiresAt": "2026-08-11T10:30:00Z",
        },
    }


def worker_claim(event: dict[str, Any], *, resume: bool = False, state_version: int | None = None) -> dict[str, Any]:
    source = event["payload"]
    return {
        "schemaVersion": "1.0",
        "jobId": event["jobId"],
        "traceId": event["traceId"],
        "leaseId": "70707070-7070-4070-8070-707070707070" if not resume else "71717171-7171-4171-8171-717171717171",
        "leaseExpiresAt": "2026-08-11T10:10:00Z" if not resume else "2026-08-11T10:20:00Z",
        "stateVersion": state_version if state_version is not None else event["expectedStateVersion"] + 1,
        "resume": resume,
        "snapshot": {
            "actor": {"actorId": source["actorId"], "role": "DEVELOPER"},
            "project": {"projectId": source["projectId"]},
            "repository": {"repositoryId": source["repositoryId"]},
            "graphStep": source["graphStep"],
            "baseSha": source["baseSha"],
            "contextDigest": source["contextDigest"],
            "policyHash": source["policyHash"],
            "promptVersion": "coding-inspect-v1",
            "allowedCapabilities": ["CHAT", "TOOL_CALLING"],
            "allowedNodes": ["plan"],
            "deadlineAt": "2026-08-11T10:25:00Z",
            "systemPrompt": "Inspect only the approved repository path.",
            "userPrompt": "Read the approved contract file.",
            "toolPath": "contracts/README.md",
            "approvalId": "80808080-8080-4080-8080-808080808080",
        },
    }


def model_response(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "turnId": request["turnId"],
        "jobId": request["jobId"],
        "traceId": request["traceId"],
        "idempotencyKey": request["idempotencyKey"],
        "assistant": {"role": "assistant", "content": "I will inspect the approved file."},
        "toolCalls": [
            {
                "toolCallId": "90909090-9090-4090-8090-909090909090",
                "name": "read_file",
                "arguments": {"path": "contracts/README.md"},
            }
        ],
        "responseFormat": {"type": "TEXT"},
        "selectedModel": {"provider": "LOCAL", "modelId": "local-mock"},
        "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        "latencyMs": 5,
        "finishReason": "TOOL_CALLS",
        "completedAt": "2026-08-11T10:00:01Z",
    }


def result_reference(content: str, execution_id: str) -> dict[str, Any]:
    raw = content.encode("utf-8")
    return {
        "mediaType": "text/plain",
        "resultRef": f"/internal/coding/tool-executions/{execution_id}/result",
        "sizeBytes": len(raw),
        "digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
    }
