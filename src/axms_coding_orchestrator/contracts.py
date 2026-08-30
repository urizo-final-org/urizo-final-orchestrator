"""Strict worker-side contracts derived from Backend-owned schemas."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import json
import re
from typing import Any, Mapping
from uuid import UUID


SCHEMA_VERSION = "1.0"
IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
GIT_OBJECT_ID = re.compile(r"^(sha1:[0-9a-f]{40}|sha256:[0-9a-f]{64})$")
GRAPH_STEP = re.compile(r"^[a-z][a-z0-9_-]*$")
PROMPT_VERSION = re.compile(r"^[A-Za-z0-9._-]+$")
CAPABILITIES = frozenset({"CHAT", "STRUCTURED_OUTPUT", "TOOL_CALLING"})
ROLES = frozenset({"SUPER_ADMIN", "PROJECT_ADMIN", "REVIEWER", "DEVELOPER"})
OUTCOMES = frozenset(
    {"WAITING_APPROVAL", "COMPLETED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE"}
)


class WorkerContractViolation(ValueError):
    """Payload-free contract validation failure."""


@dataclass(frozen=True, slots=True)
class QueuedJobReference:
    """The only payload allowed on the private coding queue."""

    job_id: str

    @classmethod
    def from_json(cls, raw: bytes | str) -> QueuedJobReference:
        return cls.from_dict(_decode_json(raw))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> QueuedJobReference:
        payload = _object(value, "queuedJob")
        _exact_fields(payload, {"jobId"}, "queuedJob")
        return cls(_uuid(payload["jobId"], "jobId"))

    def to_dict(self) -> dict[str, str]:
        return {"jobId": self.job_id}

    def to_json(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    def __repr__(self) -> str:
        return "QueuedJobReference[jobId=%s]" % self.job_id


@dataclass(frozen=True, slots=True)
class CodingJobRequested:
    _payload: dict[str, Any]

    @classmethod
    def from_json(cls, raw: bytes | str) -> CodingJobRequested:
        return cls.from_dict(_decode_json(raw))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CodingJobRequested:
        payload = _object(value, "event")
        base_fields = {
            "schemaVersion",
            "eventId",
            "eventType",
            "jobId",
            "traceId",
            "idempotencyKey",
            "attempt",
            "expectedStateVersion",
            "occurredAt",
            "payload",
        }
        binding_fields = {
            "profileVersionId",
            "pipelineAttempt",
            "executionAttempt",
            "workspaceId",
            "toolCallId",
        }
        actual_fields = frozenset(payload)
        if actual_fields not in {
            frozenset(base_fields),
            frozenset(base_fields | binding_fields),
        }:
            raise WorkerContractViolation("event contains missing or unknown fields")
        _schema_version(payload["schemaVersion"])
        if payload["eventType"] != "CODING_JOB_REQUESTED":
            raise WorkerContractViolation("eventType is unsupported by the coding queue")
        for field in ("eventId", "jobId", "traceId"):
            _uuid(payload[field], field)
        _matched(payload["idempotencyKey"], IDEMPOTENCY_KEY, "idempotencyKey", 128)
        _positive_integer(payload["attempt"], "attempt")
        _positive_integer(payload["expectedStateVersion"], "expectedStateVersion")
        _timestamp(payload["occurredAt"], "occurredAt")
        if binding_fields <= set(payload):
            _uuid(payload["profileVersionId"], "profileVersionId")
            _positive_integer(payload["pipelineAttempt"], "pipelineAttempt")
            execution_attempt = _positive_integer(
                payload["executionAttempt"], "executionAttempt"
            )
            if execution_attempt != payload["attempt"]:
                raise WorkerContractViolation(
                    "executionAttempt does not match the authoritative attempt"
                )
            _optional_uuid(payload["workspaceId"], "workspaceId")
            _optional_uuid(payload["toolCallId"], "toolCallId")
        job = _object(payload["payload"], "payload")
        _exact_fields(
            job,
            {
                "actorId",
                "projectId",
                "repositoryId",
                "graphStep",
                "baseSha",
                "contextDigest",
                "policyHash",
                "expiresAt",
            },
            "payload",
        )
        for field in ("actorId", "projectId", "repositoryId"):
            _uuid(job[field], f"payload.{field}")
        _matched(job["graphStep"], GRAPH_STEP, "payload.graphStep", 120)
        _matched(job["baseSha"], GIT_OBJECT_ID, "payload.baseSha", 71)
        for field in ("contextDigest", "policyHash"):
            _matched(job[field], SHA256_DIGEST, f"payload.{field}", 71)
        _timestamp(job["expiresAt"], "payload.expiresAt")
        return cls(deepcopy(payload))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    def to_json(self) -> bytes:
        return canonical_json_bytes(self._payload)

    @property
    def event_id(self) -> str:
        return self._payload["eventId"]

    @property
    def job_id(self) -> str:
        return self._payload["jobId"]

    @property
    def trace_id(self) -> str:
        return self._payload["traceId"]

    @property
    def attempt(self) -> int:
        return self._payload["attempt"]

    @property
    def expected_state_version(self) -> int:
        return self._payload["expectedStateVersion"]

    @property
    def idempotency_key(self) -> str:
        return self._payload["idempotencyKey"]

    @property
    def is_profile_bound(self) -> bool:
        return "profileVersionId" in self._payload

    @property
    def profile_version_id(self) -> str | None:
        return self._payload.get("profileVersionId")

    @property
    def pipeline_attempt(self) -> int | None:
        return self._payload.get("pipelineAttempt")

    @property
    def execution_attempt(self) -> int | None:
        return self._payload.get("executionAttempt")

    @property
    def workspace_id(self) -> str | None:
        return self._payload.get("workspaceId")

    @property
    def tool_call_id(self) -> str | None:
        return self._payload.get("toolCallId")

    @property
    def job_payload(self) -> dict[str, Any]:
        return deepcopy(self._payload["payload"])

    def ledger_key(self) -> tuple[str, int]:
        return self.job_id, self.expected_state_version

    def __repr__(self) -> str:
        return "CodingJobRequested[eventId=%s, jobId=%s]" % (self.event_id, self.job_id)


@dataclass(frozen=True, slots=True)
class ClaimSnapshot:
    _payload: dict[str, Any]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ClaimSnapshot:
        payload = _object(value, "snapshot")
        _exact_fields(
            payload,
            {
                "actor",
                "project",
                "repository",
                "graphStep",
                "baseSha",
                "contextDigest",
                "policyHash",
                "promptVersion",
                "allowedCapabilities",
                "allowedNodes",
                "deadlineAt",
                "systemPrompt",
                "userPrompt",
                "toolPath",
                "approvalId",
            },
            "snapshot",
        )
        actor = _object(payload["actor"], "snapshot.actor")
        _exact_fields(actor, {"actorId", "role"}, "snapshot.actor")
        _uuid(actor["actorId"], "snapshot.actor.actorId")
        if actor["role"] not in ROLES:
            raise WorkerContractViolation("snapshot.actor.role is invalid")
        project = _object(payload["project"], "snapshot.project")
        _exact_fields(project, {"projectId"}, "snapshot.project")
        _uuid(project["projectId"], "snapshot.project.projectId")
        repository = _object(payload["repository"], "snapshot.repository")
        _exact_fields(repository, {"repositoryId"}, "snapshot.repository")
        _uuid(repository["repositoryId"], "snapshot.repository.repositoryId")
        _matched(payload["graphStep"], GRAPH_STEP, "snapshot.graphStep", 120)
        _matched(payload["baseSha"], GIT_OBJECT_ID, "snapshot.baseSha", 71)
        for field in ("contextDigest", "policyHash"):
            _matched(payload[field], SHA256_DIGEST, f"snapshot.{field}", 71)
        _matched(payload["promptVersion"], PROMPT_VERSION, "snapshot.promptVersion", 120)
        capabilities = _string_list(
            payload["allowedCapabilities"], "snapshot.allowedCapabilities", 1, 3, 120
        )
        if not set(capabilities) <= CAPABILITIES or "CHAT" not in capabilities:
            raise WorkerContractViolation("snapshot.allowedCapabilities is invalid")
        nodes = _string_list(payload["allowedNodes"], "snapshot.allowedNodes", 1, 50, 120)
        if any(not GRAPH_STEP.fullmatch(node) for node in nodes):
            raise WorkerContractViolation("snapshot.allowedNodes is invalid")
        if "plan" not in nodes:
            raise WorkerContractViolation("snapshot does not allow the plan node")
        _timestamp(payload["deadlineAt"], "snapshot.deadlineAt")
        _bounded_string(payload["systemPrompt"], "snapshot.systemPrompt", 1, 200_000)
        _bounded_string(payload["userPrompt"], "snapshot.userPrompt", 1, 200_000)
        _relative_path(payload["toolPath"], "snapshot.toolPath")
        _uuid(payload["approvalId"], "snapshot.approvalId")
        return cls(deepcopy(payload))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    def __getitem__(self, key: str) -> Any:
        return deepcopy(self._payload[key])

    def __repr__(self) -> str:
        return "ClaimSnapshot[repository=%s, prompts=REDACTED]" % self._payload[
            "repository"
        ]["repositoryId"]


@dataclass(frozen=True, slots=True)
class WorkerClaim:
    _payload: dict[str, Any]

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        event: CodingJobRequested,
        *,
        now: datetime | None = None,
    ) -> WorkerClaim:
        payload = _object(value, "claim")
        _exact_fields(
            payload,
            {
                "schemaVersion",
                "jobId",
                "traceId",
                "leaseId",
                "leaseExpiresAt",
                "stateVersion",
                "resume",
                "profileVersionId",
                "snapshot",
            },
            "claim",
        )
        _schema_version(payload["schemaVersion"])
        for field in ("jobId", "traceId", "leaseId"):
            _uuid(payload[field], field)
        if payload["jobId"] != event.job_id or payload["traceId"] != event.trace_id:
            raise WorkerContractViolation("claim correlation does not match the event")
        expires = _timestamp(payload["leaseExpiresAt"], "leaseExpiresAt")
        if now is not None and expires <= now:
            raise WorkerContractViolation("claim lease is already expired")
        _positive_integer(payload["stateVersion"], "stateVersion")
        if payload["stateVersion"] <= event.expected_state_version:
            raise WorkerContractViolation("claim stateVersion did not advance")
        if not isinstance(payload["resume"], bool):
            raise WorkerContractViolation("claim.resume is invalid")
        _uuid(payload["profileVersionId"], "profileVersionId")
        if event.profile_version_id is not None and (
            payload["profileVersionId"] != event.profile_version_id
        ):
            raise WorkerContractViolation(
                "claim profileVersionId does not match the authoritative job"
            )
        snapshot = ClaimSnapshot.from_dict(payload["snapshot"])
        source = event.job_payload
        actual = snapshot.to_dict()
        correlations = {
            "actorId": actual["actor"]["actorId"],
            "projectId": actual["project"]["projectId"],
            "repositoryId": actual["repository"]["repositoryId"],
            "graphStep": actual["graphStep"],
            "baseSha": actual["baseSha"],
            "contextDigest": actual["contextDigest"],
            "policyHash": actual["policyHash"],
        }
        if any(source[field] != result for field, result in correlations.items()):
            raise WorkerContractViolation("claim snapshot does not match the event scope")
        return cls(deepcopy(payload))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    @property
    def job_id(self) -> str:
        return self._payload["jobId"]

    @property
    def trace_id(self) -> str:
        return self._payload["traceId"]

    @property
    def lease_id(self) -> str:
        return self._payload["leaseId"]

    @property
    def state_version(self) -> int:
        return self._payload["stateVersion"]

    @property
    def resume(self) -> bool:
        return self._payload["resume"]

    @property
    def profile_version_id(self) -> str:
        return self._payload["profileVersionId"]

    @property
    def snapshot(self) -> ClaimSnapshot:
        return ClaimSnapshot.from_dict(self._payload["snapshot"])

    def __repr__(self) -> str:
        return "WorkerClaim[jobId=%s, leaseId=%s, snapshot=REDACTED]" % (
            self.job_id,
            self.lease_id,
        )


def validate_lease_response(
    value: Mapping[str, Any],
    claim: WorkerClaim,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = _object(value, "leaseResponse")
    _exact_fields(
        payload,
        {"schemaVersion", "jobId", "traceId", "leaseId", "leaseExpiresAt", "stateVersion"},
        "leaseResponse",
    )
    _schema_version(payload["schemaVersion"])
    for field in ("jobId", "traceId", "leaseId"):
        _uuid(payload[field], field)
    if (
        payload["jobId"] != claim.job_id
        or payload["traceId"] != claim.trace_id
        or payload["leaseId"] != claim.lease_id
        or payload["stateVersion"] != claim.state_version
    ):
        raise WorkerContractViolation("lease response correlation is invalid")
    expires = _timestamp(payload["leaseExpiresAt"], "leaseExpiresAt")
    if now is not None and expires <= now:
        raise WorkerContractViolation("lease response is already expired")
    return deepcopy(payload)


def validate_outcome_receipt(
    value: Mapping[str, Any], claim: WorkerClaim, outcome: str
) -> dict[str, Any]:
    payload = _object(value, "outcomeReceipt")
    _exact_fields(
        payload,
        {"schemaVersion", "jobId", "traceId", "stateVersion", "status"},
        "outcomeReceipt",
    )
    _schema_version(payload["schemaVersion"])
    _uuid(payload["jobId"], "jobId")
    _uuid(payload["traceId"], "traceId")
    _positive_integer(payload["stateVersion"], "stateVersion")
    if (
        payload["jobId"] != claim.job_id
        or payload["traceId"] != claim.trace_id
        or payload["stateVersion"] != claim.state_version + 1
    ):
        raise WorkerContractViolation("outcome receipt correlation is invalid")
    expected_status = {
        "WAITING_APPROVAL": "WAITING_APPROVAL",
        "COMPLETED": "COMPLETED",
        "RETRYABLE_FAILURE": "PENDING",
        "PERMANENT_FAILURE": "FAILED",
    }.get(outcome)
    if payload["status"] != expected_status:
        raise WorkerContractViolation("outcome receipt status is invalid")
    return deepcopy(payload)


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


def _decode_json(raw: bytes | str) -> Mapping[str, Any]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        raise WorkerContractViolation("payload is not valid JSON") from None
    return _object(value, "payload")


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise WorkerContractViolation(f"{field} must be an object")
    return dict(value)


def _exact_fields(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise WorkerContractViolation(f"{field} contains missing or unknown fields")


def _schema_version(value: Any) -> None:
    if value != SCHEMA_VERSION:
        raise WorkerContractViolation("schemaVersion is unsupported")


def _uuid(value: Any, field: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value.lower():
            raise ValueError
    except (ValueError, AttributeError):
        raise WorkerContractViolation(f"{field} is invalid") from None
    return value


def _optional_uuid(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _uuid(value, field)


def _timestamp(value: Any, field: str) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed
    except ValueError:
        raise WorkerContractViolation(f"{field} is invalid") from None


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WorkerContractViolation(f"{field} is invalid")
    return value


def _bounded_string(value: Any, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum:
        raise WorkerContractViolation(f"{field} is invalid")
    return value


def _matched(
    value: Any, pattern: re.Pattern[str], field: str, maximum: int
) -> str:
    result = _bounded_string(value, field, 1, maximum)
    if not pattern.fullmatch(result):
        raise WorkerContractViolation(f"{field} is invalid")
    return result


def _string_list(
    value: Any, field: str, minimum: int, maximum: int, item_maximum: int
) -> list[str]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise WorkerContractViolation(f"{field} has an invalid item count")
    result = [
        _bounded_string(item, f"{field}[{index}]", 1, item_maximum)
        for index, item in enumerate(value)
    ]
    if len(result) != len(set(result)):
        raise WorkerContractViolation(f"{field} contains duplicates")
    return result


def _relative_path(value: Any, field: str) -> str:
    path = _bounded_string(value, field, 1, 1000)
    if (
        path.startswith("/")
        or re.match(r"^[A-Za-z]:", path)
        or "\\" in path
        or ":" in path
        or re.search(r"%[0-9A-Fa-f]{2}", path)
        or any(segment == ".." for segment in path.split("/"))
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in path)
    ):
        raise WorkerContractViolation(f"{field} is invalid")
    return path
