"""Strict Spring client for AI04 Coding Handler results and attempt state."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import json
import math
import re
import socket
from typing import Any, Mapping, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from .contracts import GIT_OBJECT_ID, SHA256_DIGEST, canonical_json_bytes
from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    MAX_RESPONSE_BYTES,
    ModelGatewayRemoteError,
    SPRING_PRIVATE_ORIGIN,
    _check_body_size,
    _read_chunked,
)
from .node_runtime import NodeInvocation
from .snapshot import HANDLER_KEY, NODE_IDENTIFIER


RESULT_TYPES = frozenset(
    {
        "ANALYSIS",
        "CANDIDATE",
        "DIFF",
        "CHECK",
        "REVIEW",
        "PULL_REQUEST",
        "DEPLOY_REQUEST",
    }
)
RESULT_PORTS = frozenset(
    {
        "feasible",
        "infeasible",
        "completed",
        "passed",
        "failed",
        "changes_requested",
        "ready",
        "approved",
        "rejected",
        "requested",
        "recorded",
    }
)
ATTEMPT_STATUSES = frozenset({"ACTIVE", "REJECTED", "COMPLETED", "FAILED"})
APPROVAL_STAGES = frozenset({"SCOPE", "CANDIDATE", "GITHUB", "CMS", "DEPLOY"})
APPROVAL_ROLES = frozenset({"GENERAL_ADMIN", "SUPER_ADMIN"})
APPROVAL_DECISIONS = frozenset({"APPROVED", "REJECTED"})
SAFE_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")
MAX_JSON_DEPTH = 64


class CodingDomainClientError(RuntimeError):
    """Payload-free failure at the Spring-owned Coding Domain boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        status: int | None = None,
        retry_after_ms: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status = status
        self.retry_after_ms = retry_after_ms


@dataclass(frozen=True, slots=True)
class CodingResultWrite:
    result_id: str
    handler_key: str
    result_type: str
    result_port: str
    payload: Mapping[str, Any]
    workspace_id: str | None = None
    candidate_sha: str | None = None
    diff_digest: str | None = None
    validation_hash: str | None = None

    def __post_init__(self) -> None:
        _uuid(self.result_id, "result.resultId")
        _matched(self.handler_key, HANDLER_KEY, "result.handlerKey", 128)
        if self.result_type not in RESULT_TYPES:
            raise ValueError("result.resultType is invalid")
        if self.result_port not in RESULT_PORTS:
            raise ValueError("result.resultPort is invalid")
        _json_object(self.payload, "result.payload")
        _optional_uuid(self.workspace_id, "result.workspaceId")
        _optional_match(self.candidate_sha, GIT_OBJECT_ID, "result.candidateSha", 71)
        _optional_match(self.diff_digest, SHA256_DIGEST, "result.diffDigest", 71)
        _optional_match(
            self.validation_hash,
            SHA256_DIGEST,
            "result.validationHash",
            71,
        )

    def body(self, invocation: NodeInvocation) -> dict[str, Any]:
        body: dict[str, Any] = {
            "schemaVersion": "1.0",
            "traceId": invocation.trace_id,
            "expectedStateVersion": invocation.state_version,
            "handlerKey": self.handler_key,
            "resultType": self.result_type,
            "resultPort": self.result_port,
            "payload": deepcopy(dict(self.payload)),
        }
        optional = {
            "workspaceId": self.workspace_id,
            "candidateSha": self.candidate_sha,
            "diffDigest": self.diff_digest,
            "validationHash": self.validation_hash,
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        return body


@dataclass(frozen=True, slots=True)
class CodingResultRecord:
    result_id: str
    job_id: str
    trace_id: str
    pipeline_attempt: int
    handler_key: str
    result_type: str
    result_port: str
    payload: Mapping[str, Any]
    recorded_at: str
    workspace_id: str | None = None
    candidate_sha: str | None = None
    diff_digest: str | None = None
    validation_hash: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CodingResultRecord:
        payload = _object(value, "result")
        required = {
            "schemaVersion",
            "resultId",
            "jobId",
            "traceId",
            "pipelineAttempt",
            "handlerKey",
            "resultType",
            "resultPort",
            "payload",
            "recordedAt",
        }
        optional = {"workspaceId", "candidateSha", "diffDigest", "validationHash"}
        _fields(payload, required, optional, "result")
        if payload["schemaVersion"] != "1.0":
            raise ValueError("result.schemaVersion is invalid")
        result = cls(
            result_id=_uuid(payload["resultId"], "result.resultId"),
            job_id=_uuid(payload["jobId"], "result.jobId"),
            trace_id=_uuid(payload["traceId"], "result.traceId"),
            pipeline_attempt=_positive(payload["pipelineAttempt"], "result.pipelineAttempt"),
            handler_key=_matched(payload["handlerKey"], HANDLER_KEY, "result.handlerKey", 128),
            result_type=_one_of(payload["resultType"], RESULT_TYPES, "result.resultType"),
            result_port=_one_of(payload["resultPort"], RESULT_PORTS, "result.resultPort"),
            payload=_json_object(payload["payload"], "result.payload"),
            recorded_at=_timestamp(payload["recordedAt"], "result.recordedAt"),
            workspace_id=_optional_uuid(payload.get("workspaceId"), "result.workspaceId"),
            candidate_sha=_optional_match(
                payload.get("candidateSha"), GIT_OBJECT_ID, "result.candidateSha", 71
            ),
            diff_digest=_optional_match(
                payload.get("diffDigest"), SHA256_DIGEST, "result.diffDigest", 71
            ),
            validation_hash=_optional_match(
                payload.get("validationHash"),
                SHA256_DIGEST,
                "result.validationHash",
                71,
            ),
        )
        return result

    def __repr__(self) -> str:
        return (
            "CodingResultRecord[resultId=%s, handlerKey=%s, resultPort=%s, "
            "payload=REDACTED]"
            % (self.result_id, self.handler_key, self.result_port)
        )


@dataclass(frozen=True, slots=True)
class CodingApprovalDecision:
    approval_id: str
    node_id: str
    stage: str
    stage_round: int
    decision: str
    candidate_sha: str | None
    validation_hash: str | None
    feedback: str | None
    actor_id: str
    actor_role: str
    result_state_version: int
    next_pipeline_attempt: int | None
    decided_at: str

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CodingApprovalDecision:
        payload = _object(value, "decision")
        required = {
            "approvalId",
            "nodeId",
            "stage",
            "stageRound",
            "decision",
            "actorId",
            "actorRole",
            "resultStateVersion",
            "decidedAt",
        }
        optional = {
            "candidateSha",
            "validationHash",
            "feedback",
            "nextPipelineAttempt",
        }
        _fields(payload, required, optional, "decision")
        feedback = payload.get("feedback")
        if feedback is not None and (
            not isinstance(feedback, str) or len(feedback) > 20_000
        ):
            raise ValueError("decision.feedback is invalid")
        stage = _one_of(payload["stage"], APPROVAL_STAGES, "decision.stage")
        candidate_sha = _optional_match(
            payload.get("candidateSha"),
            GIT_OBJECT_ID,
            "decision.candidateSha",
            71,
        )
        validation_hash = _optional_match(
            payload.get("validationHash"),
            SHA256_DIGEST,
            "decision.validationHash",
            71,
        )
        if stage == "SCOPE":
            if candidate_sha is not None or validation_hash is not None:
                raise ValueError("scope decision has an unexpected candidate subject")
        elif candidate_sha is None or validation_hash is None:
            raise ValueError("post-preview decision has no candidate subject")
        return cls(
            approval_id=_uuid(payload["approvalId"], "decision.approvalId"),
            node_id=_matched(
                payload["nodeId"], NODE_IDENTIFIER, "decision.nodeId", 64
            ),
            stage=stage,
            stage_round=_positive(payload["stageRound"], "decision.stageRound"),
            decision=_one_of(
                payload["decision"], APPROVAL_DECISIONS, "decision.decision"
            ),
            candidate_sha=candidate_sha,
            validation_hash=validation_hash,
            feedback=feedback,
            actor_id=_uuid(payload["actorId"], "decision.actorId"),
            actor_role=_one_of(
                payload["actorRole"], APPROVAL_ROLES, "decision.actorRole"
            ),
            result_state_version=_positive(
                payload["resultStateVersion"], "decision.resultStateVersion"
            ),
            next_pipeline_attempt=(
                None
                if "nextPipelineAttempt" not in payload
                else _positive(
                    payload["nextPipelineAttempt"], "decision.nextPipelineAttempt"
                )
            ),
            decided_at=_timestamp(payload["decidedAt"], "decision.decidedAt"),
        )


@dataclass(frozen=True, slots=True)
class CodingPendingApproval:
    approval_id: str
    node_id: str
    stage: str
    stage_round: int
    required_role: str

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CodingPendingApproval:
        payload = _object(value, "pendingApproval")
        if set(payload) != {
            "approvalId",
            "nodeId",
            "stage",
            "stageRound",
            "requiredRole",
        }:
            raise ValueError("pendingApproval fields are invalid")
        return cls(
            approval_id=_uuid(payload["approvalId"], "pendingApproval.approvalId"),
            node_id=_matched(
                payload["nodeId"], NODE_IDENTIFIER, "pendingApproval.nodeId", 64
            ),
            stage=_one_of(
                payload["stage"], APPROVAL_STAGES, "pendingApproval.stage"
            ),
            stage_round=_positive(
                payload["stageRound"], "pendingApproval.stageRound"
            ),
            required_role=_one_of(
                payload["requiredRole"],
                APPROVAL_ROLES,
                "pendingApproval.requiredRole",
            ),
        )


@dataclass(frozen=True, slots=True)
class CodingAttemptAggregate:
    job_id: str
    trace_id: str
    pipeline_attempt: int
    workspace_id: str | None
    status: str
    request_text: str
    results: tuple[CodingResultRecord, ...]
    pending_approvals: tuple[CodingPendingApproval, ...]
    decisions: tuple[CodingApprovalDecision, ...]
    created_at: str
    finished_at: str | None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CodingAttemptAggregate:
        payload = _object(value, "attempt")
        required = {
            "schemaVersion",
            "jobId",
            "traceId",
            "pipelineAttempt",
            "status",
            "requestText",
            "results",
            "pendingApprovals",
            "decisions",
            "createdAt",
        }
        _fields(payload, required, {"workspaceId", "finishedAt"}, "attempt")
        if payload["schemaVersion"] != "1.0":
            raise ValueError("attempt.schemaVersion is invalid")
        request_text = payload["requestText"]
        if not isinstance(request_text, str) or not 1 <= len(request_text) <= 200_000:
            raise ValueError("attempt.requestText is invalid")
        raw_results = payload["results"]
        if not isinstance(raw_results, list) or len(raw_results) > 10_000:
            raise ValueError("attempt.results is invalid")
        raw_decisions = payload["decisions"]
        if not isinstance(raw_decisions, list) or len(raw_decisions) > 1_000:
            raise ValueError("attempt.decisions is invalid")
        raw_pending = payload["pendingApprovals"]
        if not isinstance(raw_pending, list) or len(raw_pending) > 100:
            raise ValueError("attempt.pendingApprovals is invalid")
        finished_at = payload.get("finishedAt")
        return cls(
            job_id=_uuid(payload["jobId"], "attempt.jobId"),
            trace_id=_uuid(payload["traceId"], "attempt.traceId"),
            pipeline_attempt=_positive(
                payload["pipelineAttempt"], "attempt.pipelineAttempt"
            ),
            workspace_id=_optional_uuid(payload.get("workspaceId"), "attempt.workspaceId"),
            status=_one_of(payload["status"], ATTEMPT_STATUSES, "attempt.status"),
            request_text=request_text,
            results=tuple(CodingResultRecord.from_dict(item) for item in raw_results),
            pending_approvals=tuple(
                CodingPendingApproval.from_dict(item) for item in raw_pending
            ),
            decisions=tuple(
                CodingApprovalDecision.from_dict(item) for item in raw_decisions
            ),
            created_at=_timestamp(payload["createdAt"], "attempt.createdAt"),
            finished_at=(
                None
                if finished_at is None
                else _timestamp(finished_at, "attempt.finishedAt")
            ),
        )

    def __repr__(self) -> str:
        return (
            "CodingAttemptAggregate[jobId=%s, pipelineAttempt=%d, status=%s, "
            "requestText=REDACTED, results=%d]"
            % (self.job_id, self.pipeline_attempt, self.status, len(self.results))
        )


class CodingDomainClient(Protocol):
    def get_attempt(self, invocation: NodeInvocation) -> CodingAttemptAggregate: ...

    def put_result(
        self, invocation: NodeInvocation, result: CodingResultWrite
    ) -> CodingResultRecord: ...


class SpringCodingDomainClient:
    """Read and atomically record only the current Spring Coding attempt."""

    __slots__ = ("_origin", "_credential_resolver", "_timeout_seconds")

    def __init__(
        self,
        spring_origin: str,
        credential_resolver: CredentialResolver,
        *,
        timeout_seconds: float = 10.0,
        allowed_origins: set[str] | frozenset[str] | None = None,
    ) -> None:
        origins = frozenset(
            {SPRING_PRIVATE_ORIGIN} if allowed_origins is None else allowed_origins
        )
        if spring_origin not in origins or not _canonical_origin(spring_origin):
            raise ValueError("Spring Coding Domain origin is not allowlisted")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or float(timeout_seconds) <= 0
        ):
            raise ValueError("timeout_seconds must be positive and finite")
        if not callable(credential_resolver):
            raise TypeError("credential_resolver must be callable")
        self._origin = spring_origin
        self._credential_resolver = credential_resolver
        self._timeout_seconds = float(timeout_seconds)

    def get_attempt(self, invocation: NodeInvocation) -> CodingAttemptAggregate:
        _invocation(invocation)
        path = (
            f"/internal/coding/worker/jobs/{invocation.job_id}/attempts/"
            f"{invocation.pipeline_attempt}"
        )
        response = self._call("GET", path, None, invocation)
        try:
            aggregate = CodingAttemptAggregate.from_dict(response)
        except (TypeError, ValueError, RecursionError):
            raise _invalid_response() from None
        if (
            aggregate.job_id != invocation.job_id
            or aggregate.trace_id != invocation.trace_id
            or aggregate.pipeline_attempt != invocation.pipeline_attempt
            or any(
                result.job_id != invocation.job_id
                or result.trace_id != invocation.trace_id
                or result.pipeline_attempt != invocation.pipeline_attempt
                for result in aggregate.results
            )
        ):
            raise _invalid_response()
        return aggregate

    def put_result(
        self, invocation: NodeInvocation, result: CodingResultWrite
    ) -> CodingResultRecord:
        _invocation(invocation)
        if not isinstance(result, CodingResultWrite):
            raise TypeError("result must be a CodingResultWrite")
        path = (
            f"/internal/coding/worker/jobs/{invocation.job_id}/attempts/"
            f"{invocation.pipeline_attempt}/results/{result.result_id}"
        )
        body = result.body(invocation)
        response = self._call("PUT", path, canonical_json_bytes(body), invocation)
        try:
            recorded = CodingResultRecord.from_dict(response)
        except (TypeError, ValueError, RecursionError):
            raise _invalid_response() from None
        expected_workspace = result.workspace_id
        if (
            recorded.result_id != result.result_id
            or recorded.job_id != invocation.job_id
            or recorded.trace_id != invocation.trace_id
            or recorded.pipeline_attempt != invocation.pipeline_attempt
            or recorded.handler_key != result.handler_key
            or recorded.result_type != result.result_type
            or recorded.result_port != result.result_port
            or recorded.workspace_id != expected_workspace
            or recorded.candidate_sha != result.candidate_sha
            or recorded.diff_digest != result.diff_digest
            or recorded.validation_hash != result.validation_hash
            or dict(recorded.payload) != dict(result.payload)
        ):
            raise _invalid_response()
        return recorded

    def _call(
        self,
        method: str,
        path: str,
        body: bytes | None,
        invocation: NodeInvocation,
    ) -> Mapping[str, Any]:
        credential = bytearray()
        try:
            try:
                with self._credential_resolver() as lease:
                    credential = lease.copy()
            except ModelGatewayRemoteError:
                raise CodingDomainClientError(
                    "SERVICE_AUTHENTICATION_FAILED",
                    "Spring Coding Domain credential is unavailable.",
                    retryable=False,
                ) from None
            try:
                status, raw = _request_coding_http(
                    method,
                    self._origin + path,
                    body,
                    credential,
                    self._timeout_seconds,
                    invocation.trace_id,
                )
            except (TimeoutError, socket.timeout, OSError):
                raise CodingDomainClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "Spring Coding Domain API is unavailable.",
                    retryable=True,
                ) from None
            except (ContractViolation, ModelGatewayRemoteError):
                raise _invalid_response() from None
        finally:
            for index in range(len(credential)):
                credential[index] = 0
        if not 200 <= status < 300:
            raise _remote_error(status, raw, invocation)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise _invalid_response(status=status) from None
        if not isinstance(value, Mapping):
            raise _invalid_response(status=status)
        return value


def _remote_error(
    status: int, raw: bytes, invocation: NodeInvocation
) -> CodingDomainClientError:
    try:
        envelope = json.loads(raw)
        if not isinstance(envelope, Mapping) or set(envelope) != {
            "schemaVersion",
            "traceId",
            "jobId",
            "idempotencyKey",
            "error",
        }:
            raise ValueError
        if envelope["schemaVersion"] != "1.0":
            raise ValueError
        _uuid(envelope["traceId"], "error.traceId")
        _uuid(envelope["jobId"], "error.jobId")
        if (
            envelope["traceId"] != invocation.trace_id
            or envelope["jobId"] != invocation.job_id
            or not isinstance(envelope["idempotencyKey"], str)
            or not 8 <= len(envelope["idempotencyKey"]) <= 128
        ):
            raise ValueError
        error = _object(envelope["error"], "error")
        if set(error) != {"code", "message", "retryable", "retryAfterMs"}:
            raise ValueError
        code = error["code"]
        message = error["message"]
        retryable = error["retryable"]
        retry_after_ms = error["retryAfterMs"]
        if (
            not isinstance(code, str)
            or not SAFE_ERROR_CODE.fullmatch(code)
            or not isinstance(message, str)
            or not 1 <= len(message) <= 1_000
            or not isinstance(retryable, bool)
        ):
            raise ValueError
        if retryable:
            if (
                isinstance(retry_after_ms, bool)
                or not isinstance(retry_after_ms, int)
                or not 1 <= retry_after_ms <= 3_600_000
            ):
                raise ValueError
        elif retry_after_ms is not None:
            raise ValueError
        return CodingDomainClientError(
            code,
            "Spring Coding Domain request was rejected.",
            retryable=retryable,
            status=status,
            retry_after_ms=retry_after_ms,
        )
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return _invalid_response(status=status)


def _request_coding_http(
    method: str,
    endpoint: str,
    body: bytes | None,
    credential: bytearray,
    timeout: float,
    trace_id: str,
) -> tuple[int, bytes]:
    """Keep traced GET/PUT support local to the Coding result contract."""

    if method not in {"GET", "PUT"} or (method == "GET") != (body is None):
        raise ContractViolation("Coding Domain HTTP method is invalid")
    try:
        _uuid(trace_id, "traceId")
    except ValueError:
        raise ContractViolation("Coding Domain traceId is invalid") from None
    parsed = urlsplit(endpoint)
    host = parsed.hostname
    if host is None:
        raise ContractViolation("Coding Domain endpoint host is required")
    port = parsed.port or 80
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    host_header = host if port == 80 else f"{host}:{port}"
    wire = bytearray()
    try:
        wire.extend(f"{method} {target} HTTP/1.1\r\n".encode("ascii"))
        wire.extend(f"Host: {host_header}\r\n".encode("ascii"))
        wire.extend(b"Accept: application/json\r\n")
        wire.extend(f"X-Trace-Id: {trace_id}\r\n".encode("ascii"))
        wire.extend(b"Authorization: Bearer ")
        wire.extend(credential)
        wire.extend(b"\r\n")
        if body is not None:
            wire.extend(b"Content-Type: application/json\r\n")
            wire.extend(f"Content-Length: {len(body)}\r\n".encode("ascii"))
        wire.extend(b"Connection: close\r\n\r\n")
        if body is not None:
            wire.extend(body)
        with socket.create_connection((host, port), timeout=timeout) as connection:
            connection.settimeout(timeout)
            connection.sendall(wire)
            with connection.makefile("rb") as stream:
                status_line = stream.readline(8_193)
                if len(status_line) > 8_192 or not status_line.startswith(b"HTTP/1."):
                    raise ContractViolation("HTTP status line is invalid")
                try:
                    status = int(status_line.split(b" ", 2)[1])
                except (IndexError, ValueError):
                    raise ContractViolation("HTTP status line is invalid") from None
                headers: dict[bytes, bytes] = {}
                header_bytes = len(status_line)
                while True:
                    line = stream.readline(8_193)
                    header_bytes += len(line)
                    if header_bytes > 65_536 or len(line) > 8_192:
                        raise ContractViolation("HTTP headers are too large")
                    if line in {b"\r\n", b"\n"}:
                        break
                    if b":" not in line:
                        raise ContractViolation("HTTP header is invalid")
                    name, value = line.split(b":", 1)
                    headers[name.strip().lower()] = value.strip().lower()
                if headers.get(b"transfer-encoding") == b"chunked":
                    response_body = _read_chunked(stream)
                elif b"content-length" in headers:
                    try:
                        length = int(headers[b"content-length"])
                    except ValueError:
                        raise ContractViolation("HTTP Content-Length is invalid") from None
                    if length < 0 or length > MAX_RESPONSE_BYTES:
                        raise ContractViolation("HTTP response body is too large")
                    response_body = stream.read(length)
                    if len(response_body) != length:
                        raise ContractViolation("HTTP response body is incomplete")
                else:
                    response_body = stream.read(MAX_RESPONSE_BYTES + 1)
                    _check_body_size(response_body)
                return status, response_body
    finally:
        for index in range(len(wire)):
            wire[index] = 0


def _invalid_response(*, status: int | None = None) -> CodingDomainClientError:
    return CodingDomainClientError(
        "WORKER_RESPONSE_INVALID",
        "Spring Coding Domain API returned an invalid response.",
        retryable=False,
        status=status,
    )


def _invocation(value: Any) -> NodeInvocation:
    if not isinstance(value, NodeInvocation):
        raise TypeError("invocation must be a NodeInvocation")
    return value


def _object(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be an object")
    return dict(value)


def _fields(
    value: Mapping[str, Any],
    required: set[str],
    optional: set[str],
    field_name: str,
) -> None:
    if not required <= set(value) or set(value) - required - optional:
        raise ValueError(f"{field_name} fields are invalid")


def _json_object(value: Any, field_name: str) -> dict[str, Any]:
    source = _object(value, field_name)
    try:
        encoded = json.dumps(source, ensure_ascii=False, allow_nan=False)
        decoded = json.loads(encoded)
    except (TypeError, ValueError, RecursionError):
        raise ValueError(f"{field_name} is not JSON-safe") from None
    if _json_depth(decoded) > MAX_JSON_DEPTH:
        raise ValueError(f"{field_name} exceeds the JSON depth limit")
    return decoded


def _json_depth(value: Any) -> int:
    if isinstance(value, Mapping):
        return 1 + max((_json_depth(item) for item in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_json_depth(item) for item in value), default=0)
    return 0


def _matched(value: Any, pattern: Any, field_name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or pattern.fullmatch(value) is None
    ):
        raise ValueError(f"{field_name} is invalid")
    return value


def _optional_match(
    value: Any, pattern: Any, field_name: str, maximum: int
) -> str | None:
    if value is None:
        return None
    return _matched(value, pattern, field_name, maximum)


def _one_of(value: Any, allowed: frozenset[str], field_name: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{field_name} is invalid")
    return value


def _positive(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} is invalid")
    return value


def _uuid(value: Any, field_name: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise ValueError(f"{field_name} is invalid") from None
    return value


def _optional_uuid(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _uuid(value, field_name)


def _timestamp(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{field_name} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{field_name} is invalid") from None
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _canonical_origin(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        return False
    return (
        parsed.scheme == "http"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.path == ""
        and parsed.query == ""
        and parsed.fragment == ""
        and port is not None
    )
