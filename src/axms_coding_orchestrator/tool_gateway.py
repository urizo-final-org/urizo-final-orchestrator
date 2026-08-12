"""Strict Spring Tool Gateway client; Python never executes repository tools."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
import socket
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
from uuid import UUID, NAMESPACE_URL, uuid5

from .contracts import CodingJobRequested, WorkerClaim, canonical_json_bytes
from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    ModelGatewayRemoteError,
    NON_RETRYABLE_ERROR_CODES,
    RETRYABLE_ERROR_CODES,
    SPRING_PRIVATE_ORIGIN,
    _request_http,
)


READ_FILE_SCHEMA_DIGEST = (
    "sha256:39b714704935190561ed407980480b9a4a0b346b97346e0bff71fb9ace820194"
)
SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
GIT_OBJECT_ID = re.compile(r"^(sha1:[0-9a-f]{40}|sha256:[0-9a-f]{64})$")
IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
GRAPH_STEP = re.compile(r"^[a-z][a-z0-9_-]*$")
ATTEMPT_SCOPE = re.compile(r"^[A-Za-z0-9._:-]+$")
ROLES = frozenset({"SUPER_ADMIN", "PROJECT_ADMIN", "REVIEWER", "DEVELOPER"})
DENIED_TOOL_ERROR_CODES = frozenset(
    {
        "TOOL_ARGUMENTS_INVALID",
        "TOOL_NOT_ALLOWED",
        "PATH_POLICY_DENIED",
        "REPOSITORY_SCOPE_DENIED",
        "CANDIDATE_SHA_MISMATCH",
        "CONTEXT_DIGEST_MISMATCH",
        "TOOL_APPROVAL_REQUIRED",
        "TOOL_APPROVAL_DENIED",
        "TOOL_APPROVAL_EXPIRED",
    }
)
TOOL_ERROR_CODES = frozenset(
    {
        "TOOL_ARGUMENTS_INVALID",
        "TOOL_NOT_ALLOWED",
        "PATH_POLICY_DENIED",
        "REPOSITORY_SCOPE_DENIED",
        "CANDIDATE_SHA_MISMATCH",
        "CONTEXT_DIGEST_MISMATCH",
        "TOOL_APPROVAL_REQUIRED",
        "TOOL_APPROVAL_DENIED",
        "TOOL_APPROVAL_EXPIRED",
        "TOOL_EXECUTION_FAILED",
        "TOOL_EXECUTION_TIMEOUT",
        "TOOL_EXECUTION_NOT_FOUND",
        "TOOL_EXECUTOR_UNAVAILABLE",
        "TOOL_RESULT_NOT_READY",
    }
)


class ToolGatewayError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        status: int | None = None,
        execution_state: str | None = None,
        execution_id: str | None = None,
        retry_after_ms: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status = status
        self.execution_state = execution_state
        self.execution_id = execution_id
        self.retry_after_ms = retry_after_ms


@dataclass(frozen=True, slots=True)
class ToolExecutionResult:
    execution_id: str
    tool_call_id: str
    media_type: str
    digest: str
    size_bytes: int
    content: str

    def as_tool_message(self) -> dict[str, Any]:
        return {
            "role": "tool",
            "toolCallId": self.tool_call_id,
            "executionId": self.execution_id,
            "result": {
                "mediaType": self.media_type,
                "sizeBytes": self.size_bytes,
                "digest": self.digest,
            },
            "content": self.content,
        }

    def __repr__(self) -> str:
        return "ToolExecutionResult[executionId=%s, content=REDACTED]" % self.execution_id


def build_read_file_request(
    event: CodingJobRequested,
    claim: WorkerClaim,
    tool_call: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot = claim.snapshot.to_dict()
    if _timestamp(snapshot["deadlineAt"], "snapshot.deadlineAt") > _timestamp(
        event.job_payload["expiresAt"], "event.payload.expiresAt"
    ):
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID",
            "The approved tool deadline exceeds the authoritative job expiry.",
            retryable=False,
        )
    candidate = _object(tool_call, "toolCall")
    _exact(candidate, {"toolCallId", "name", "arguments"}, "toolCall")
    _uuid(candidate["toolCallId"], "toolCall.toolCallId")
    if candidate["name"] != "read_file":
        raise ToolGatewayError(
            "TOOL_NOT_ALLOWED",
            "The graph accepts only the approved read_file candidate.",
            retryable=False,
        )
    arguments = _object(candidate["arguments"], "toolCall.arguments")
    _exact(arguments, {"path"}, "toolCall.arguments")
    if arguments["path"] != snapshot["toolPath"]:
        raise ToolGatewayError(
            "PATH_POLICY_DENIED",
            "The model candidate is outside the approved tool path.",
            retryable=False,
        )
    attempt_scope = f"attempt-{event.attempt}.inspect-approved-path"
    identity = "|".join(
        (
            claim.job_id,
            snapshot["graphStep"],
            snapshot["baseSha"],
            "read_file",
            attempt_scope,
        )
    )
    idempotency_key = "tool." + hashlib.sha256(identity.encode("utf-8")).hexdigest()
    request_id = str(uuid5(NAMESPACE_URL, "axms:" + idempotency_key))
    return {
        "schemaVersion": "1.0",
        "messageType": "TOOL_REQUEST",
        "requestId": request_id,
        "toolCallId": candidate["toolCallId"],
        "jobId": claim.job_id,
        "traceId": claim.trace_id,
        "leaseId": claim.lease_id,
        "idempotencyKey": idempotency_key,
        "attempt": event.attempt,
        "graphStep": snapshot["graphStep"],
        "attemptScope": attempt_scope,
        "expectedStateVersion": claim.state_version,
        "deadlineAt": snapshot["deadlineAt"],
        "actor": {
            "actorId": snapshot["actor"]["actorId"],
            "projectId": snapshot["project"]["projectId"],
            "role": snapshot["actor"]["role"],
        },
        "jobState": "RUNNING",
        "repository": {
            "repositoryId": snapshot["repository"]["repositoryId"],
            "baseSha": snapshot["baseSha"],
            "candidateSha": snapshot["baseSha"],
        },
        "contextDigest": snapshot["contextDigest"],
        "policyHash": snapshot["policyHash"],
        "argumentSchemaDigest": READ_FILE_SCHEMA_DIGEST,
        "requestedPaths": [snapshot["toolPath"]],
        "tool": {"name": "read_file", "arguments": {"path": snapshot["toolPath"]}},
        "approval": {
            "approvalId": snapshot["approvalId"],
            "scopeDigest": snapshot["policyHash"],
            "expiresAt": snapshot["deadlineAt"],
        },
    }


class ToolGatewayClient:
    def __init__(
        self,
        spring_origin: str,
        credential_resolver: CredentialResolver,
        *,
        max_timeout_seconds: float = 30.0,
        now: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
        allowed_origins: set[str] | frozenset[str] | None = None,
    ) -> None:
        origins = frozenset(allowed_origins or {SPRING_PRIVATE_ORIGIN})
        if spring_origin not in origins or not _canonical_origin(spring_origin):
            raise ValueError("Spring Tool Gateway origin is not allowlisted")
        if max_timeout_seconds <= 0:
            raise ValueError("max_timeout_seconds must be positive")
        self._origin = spring_origin
        self._credential_resolver = credential_resolver
        self._max_timeout_seconds = max_timeout_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._sleep = sleeper or time.sleep

    def execute_read_file(self, request: Mapping[str, Any]) -> ToolExecutionResult:
        request_payload = _validate_request(request)
        deadline = _timestamp(request_payload["deadlineAt"], "deadlineAt")
        status, response = self._call(
            "POST",
            "/internal/coding/tool-requests",
            canonical_json_bytes(request_payload),
            deadline,
            error_context=request_payload,
        )
        terminal: dict[str, Any]
        if status == 202:
            accepted = _validate_accepted(response, request_payload)
            while True:
                remaining = (deadline - self._now()).total_seconds()
                if remaining <= 0:
                    raise ToolGatewayError(
                        "MODEL_TIMEOUT",
                        "Tool execution exceeded the graph deadline.",
                        retryable=True,
                        execution_state="UNKNOWN",
                        execution_id=accepted["executionId"],
                    )
                self._sleep(min(accepted["pollAfterMs"] / 1000.0, remaining))
                poll_status, polled = self._call(
                    "GET",
                    accepted["statusUrl"],
                    None,
                    deadline,
                    error_context=request_payload,
                    execution_id=accepted["executionId"],
                )
                if poll_status != 200:
                    raise ToolGatewayError(
                        "TOOL_EXECUTOR_UNAVAILABLE",
                        "Tool execution status was unavailable.",
                        retryable=True,
                    )
                if polled.get("messageType") == "TOOL_ACCEPTED":
                    accepted = _validate_accepted(polled, request_payload, accepted["executionId"])
                    continue
                terminal = _validate_terminal(polled, request_payload, accepted["executionId"])
                break
        elif status == 200:
            terminal = _validate_terminal(response, request_payload)
        else:
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Tool Gateway returned an unexpected success status.",
                retryable=False,
                status=status,
            )
        if terminal["status"] != "SUCCEEDED":
            error = terminal["error"]
            raise ToolGatewayError(
                error["code"],
                "Spring Tool Gateway rejected or failed the candidate.",
                retryable=error["retryable"],
                execution_state=error["executionState"],
                execution_id=terminal["executionId"],
            )
        result_reference = terminal["result"]
        result_status, content = self._call(
            "GET",
            result_reference["resultRef"],
            None,
            deadline,
            error_context=request_payload,
            execution_id=terminal["executionId"],
        )
        if result_status != 200:
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Tool result endpoint returned an unexpected success status.",
                retryable=False,
                status=result_status,
            )
        return _validate_result_content(content, request_payload, terminal)

    def _call(
        self,
        method: str,
        path: str,
        body: bytes | None,
        deadline: datetime,
        *,
        error_context: Mapping[str, Any],
        execution_id: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        remaining = (deadline - self._now()).total_seconds()
        if remaining <= 0:
            raise ToolGatewayError(
                "MODEL_TIMEOUT", "Tool Gateway deadline has elapsed.", retryable=True
            )
        credential = bytearray()
        try:
            try:
                with self._credential_resolver() as lease:
                    credential = lease.copy()
            except ModelGatewayRemoteError:
                raise ToolGatewayError(
                    "CODING_AGENT_NOT_AVAILABLE",
                    "Spring Tool Gateway credential is unavailable.",
                    retryable=False,
                ) from None
            try:
                status, raw = _request_http(
                    method,
                    self._origin + path,
                    body,
                    credential,
                    min(remaining, self._max_timeout_seconds),
                )
            except (TimeoutError, socket.timeout, OSError):
                raise ToolGatewayError(
                    "TOOL_EXECUTOR_UNAVAILABLE",
                    "Spring Tool Gateway is unavailable.",
                    retryable=True,
                ) from None
            except (ContractViolation, ModelGatewayRemoteError):
                raise ToolGatewayError(
                    "TOOL_RESPONSE_INVALID",
                    "Spring Tool Gateway returned an invalid HTTP response.",
                    retryable=False,
                ) from None
        finally:
            for index in range(len(credential)):
                credential[index] = 0
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Spring Tool Gateway returned invalid JSON.",
                retryable=False,
                status=status,
            ) from None
        if not isinstance(payload, Mapping):
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Spring Tool Gateway returned an invalid response.",
                retryable=False,
                status=status,
            )
        if not 200 <= status < 300:
            raise _remote_error(
                status,
                payload,
                error_context,
                execution_id=execution_id,
            )
        return status, dict(payload)


def _validate_request(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = _object(value, "toolRequest")
    required = {
        "schemaVersion",
        "messageType",
        "requestId",
        "toolCallId",
        "jobId",
        "traceId",
        "leaseId",
        "idempotencyKey",
        "attempt",
        "graphStep",
        "attemptScope",
        "expectedStateVersion",
        "deadlineAt",
        "actor",
        "jobState",
        "repository",
        "contextDigest",
        "policyHash",
        "argumentSchemaDigest",
        "requestedPaths",
        "tool",
        "approval",
    }
    _exact(payload, required, "toolRequest")
    if payload["schemaVersion"] != "1.0" or payload["messageType"] != "TOOL_REQUEST":
        raise ToolGatewayError("TOOL_ARGUMENTS_INVALID", "Tool request version is invalid.", retryable=False)
    for field in ("requestId", "toolCallId", "jobId", "traceId", "leaseId"):
        _uuid(payload[field], field)
    if not isinstance(payload["idempotencyKey"], str) or not IDEMPOTENCY_KEY.fullmatch(
        payload["idempotencyKey"]
    ):
        raise ToolGatewayError("TOOL_ARGUMENTS_INVALID", "Tool request key is invalid.", retryable=False)
    _positive(payload["attempt"], "attempt")
    _positive(payload["expectedStateVersion"], "expectedStateVersion")
    _matched(payload["graphStep"], GRAPH_STEP, "graphStep", 120)
    _matched(payload["attemptScope"], ATTEMPT_SCOPE, "attemptScope", 120)
    if payload["jobState"] != "RUNNING":
        raise ToolGatewayError("TOOL_ARGUMENTS_INVALID", "Tool request state is invalid.", retryable=False)
    deadline = _timestamp(payload["deadlineAt"], "deadlineAt")

    actor = _object(payload["actor"], "actor")
    _exact(actor, {"actorId", "projectId", "role"}, "actor")
    _uuid(actor["actorId"], "actor.actorId")
    _uuid(actor["projectId"], "actor.projectId")
    if actor["role"] not in ROLES:
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID", "Tool actor role is invalid.", retryable=False
        )

    repository = _object(payload["repository"], "repository")
    _exact(repository, {"repositoryId", "baseSha", "candidateSha"}, "repository")
    _uuid(repository["repositoryId"], "repository.repositoryId")
    for field in ("baseSha", "candidateSha"):
        if not isinstance(repository[field], str) or not GIT_OBJECT_ID.fullmatch(
            repository[field]
        ):
            raise ToolGatewayError(
                "TOOL_ARGUMENTS_INVALID",
                "Tool repository object ID is invalid.",
                retryable=False,
            )
    if repository["candidateSha"] != repository["baseSha"]:
        raise ToolGatewayError(
            "CANDIDATE_SHA_MISMATCH",
            "read_file must remain bound to the approved base revision.",
            retryable=False,
        )

    _digest(payload["contextDigest"], "contextDigest")
    _digest(payload["policyHash"], "policyHash")
    if payload["argumentSchemaDigest"] != READ_FILE_SCHEMA_DIGEST:
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID",
            "read_file argument schema digest is invalid.",
            retryable=False,
        )

    requested_paths = payload["requestedPaths"]
    if (
        not isinstance(requested_paths, list)
        or not 1 <= len(requested_paths) <= 100
    ):
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID", "requestedPaths is invalid.", retryable=False
        )
    validated_paths = [
        _relative_path(path, f"requestedPaths[{index}]")
        for index, path in enumerate(requested_paths)
    ]
    if len(validated_paths) != len(set(validated_paths)):
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID",
            "requestedPaths contains duplicates.",
            retryable=False,
        )

    tool = _object(payload["tool"], "tool")
    _exact(tool, {"name", "arguments"}, "tool")
    if tool["name"] != "read_file":
        raise ToolGatewayError(
            "TOOL_NOT_ALLOWED", "Only read_file is enabled in this graph.", retryable=False
        )
    arguments = _object(tool["arguments"], "tool.arguments")
    _exact(arguments, {"path"}, "tool.arguments")
    tool_path = _relative_path(arguments["path"], "tool.arguments.path")
    if validated_paths != [tool_path]:
        raise ToolGatewayError(
            "PATH_POLICY_DENIED",
            "read_file path must exactly match requestedPaths.",
            retryable=False,
        )

    approval = _object(payload["approval"], "approval")
    _exact(approval, {"approvalId", "scopeDigest", "expiresAt"}, "approval")
    _uuid(approval["approvalId"], "approval.approvalId")
    _digest(approval["scopeDigest"], "approval.scopeDigest")
    if approval["scopeDigest"] != payload["policyHash"]:
        raise ToolGatewayError(
            "TOOL_APPROVAL_DENIED",
            "Tool approval scope is not bound to the policy.",
            retryable=False,
        )
    approval_expiry = _timestamp(approval["expiresAt"], "approval.expiresAt")
    if approval_expiry != deadline:
        raise ToolGatewayError(
            "TOOL_APPROVAL_EXPIRED",
            "Tool approval expiry is not bound to the request deadline.",
            retryable=False,
        )
    return deepcopy(payload)


def _validate_accepted(
    value: Mapping[str, Any], request: Mapping[str, Any], execution_id: str | None = None
) -> dict[str, Any]:
    payload = _object(value, "toolAccepted")
    _exact(
        payload,
        {
            "schemaVersion",
            "messageType",
            "requestId",
            "toolCallId",
            "jobId",
            "traceId",
            "idempotencyKey",
            "executionId",
            "status",
            "statusUrl",
            "pollAfterMs",
            "acceptedAt",
        },
        "toolAccepted",
    )
    if payload["schemaVersion"] != "1.0" or payload["messageType"] != "TOOL_ACCEPTED" or payload["status"] != "ACCEPTED":
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool acceptance is invalid.", retryable=False)
    _correlate(payload, request)
    _uuid(payload["executionId"], "executionId")
    if execution_id is not None and payload["executionId"] != execution_id:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool execution correlation changed.", retryable=False)
    expected_url = f"/internal/coding/tool-executions/{payload['executionId']}"
    if payload["statusUrl"] != expected_url:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool status URL is invalid.", retryable=False)
    if isinstance(payload["pollAfterMs"], bool) or not isinstance(payload["pollAfterMs"], int) or not 1 <= payload["pollAfterMs"] <= 60_000:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool polling delay is invalid.", retryable=False)
    _timestamp(payload["acceptedAt"], "acceptedAt")
    return payload


def _validate_terminal(
    value: Mapping[str, Any], request: Mapping[str, Any], execution_id: str | None = None
) -> dict[str, Any]:
    payload = _object(value, "toolResult")
    common = {
        "schemaVersion",
        "messageType",
        "requestId",
        "toolCallId",
        "jobId",
        "traceId",
        "idempotencyKey",
        "executionId",
        "status",
        "completedAt",
    }
    status = payload.get("status")
    expected = common | ({"result"} if status == "SUCCEEDED" else {"error"})
    if status == "SUCCEEDED" and "candidateSha" in payload:
        expected.add("candidateSha")
    _exact(payload, expected, "toolResult")
    if payload["schemaVersion"] != "1.0" or payload["messageType"] != "TOOL_RESULT":
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result is invalid.", retryable=False)
    _correlate(payload, request)
    _uuid(payload["executionId"], "executionId")
    if execution_id is not None and payload["executionId"] != execution_id:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool execution correlation changed.", retryable=False)
    _timestamp(payload["completedAt"], "completedAt")
    if status == "SUCCEEDED":
        result = _object(payload["result"], "result")
        _exact(result, {"mediaType", "resultRef", "sizeBytes", "digest"}, "result")
        if result["resultRef"] != f"/internal/coding/tool-executions/{payload['executionId']}/result":
            raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result URL is invalid.", retryable=False)
        if result["mediaType"] not in {"application/json", "text/plain", "text/x-diff"}:
            raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result media type is invalid.", retryable=False)
        _digest(result["digest"], "result.digest")
        _size(result["sizeBytes"], "result.sizeBytes")
        if "candidateSha" in payload and (
            not isinstance(payload["candidateSha"], str)
            or not GIT_OBJECT_ID.fullmatch(payload["candidateSha"])
            or payload["candidateSha"] != request["repository"]["candidateSha"]
        ):
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Tool candidate SHA is invalid.",
                retryable=False,
            )
    elif status in {"DENIED", "FAILED", "TIMED_OUT"}:
        error = _object(payload["error"], "error")
        required_error = {"code", "message", "retryable", "executionState"}
        if status == "TIMED_OUT":
            required_error.add("executionId")
        error_fields = frozenset(error)
        if error_fields not in {
            frozenset(required_error),
            frozenset(required_error | {"violations"}),
        }:
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID", "Tool error is invalid.", retryable=False
            )
        if not isinstance(error["message"], str) or not 1 <= len(error["message"]) <= 1_000:
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID", "Tool error is invalid.", retryable=False
            )
        expected = {
            "DENIED": (DENIED_TOOL_ERROR_CODES, False, "NOT_STARTED"),
            "FAILED": (frozenset({"TOOL_EXECUTION_FAILED"}), False, "COMPLETED"),
            "TIMED_OUT": (frozenset({"TOOL_EXECUTION_TIMEOUT"}), False, "UNKNOWN"),
        }[status]
        if (
            error["code"] not in expected[0]
            or error["retryable"] is not expected[1]
            or error["executionState"] != expected[2]
        ):
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID", "Tool error is invalid.", retryable=False
            )
        if status == "TIMED_OUT" and error["executionId"] != payload["executionId"]:
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID", "Tool timeout execution is invalid.", retryable=False
            )
        _validate_violations(error.get("violations"))
    else:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result status is invalid.", retryable=False)
    return payload


def _validate_result_content(
    value: Mapping[str, Any], request: Mapping[str, Any], terminal: Mapping[str, Any]
) -> ToolExecutionResult:
    payload = _object(value, "toolResultContent")
    _exact(
        payload,
        {
            "schemaVersion",
            "requestId",
            "toolCallId",
            "jobId",
            "traceId",
            "idempotencyKey",
            "executionId",
            "mediaType",
            "sizeBytes",
            "digest",
            "content",
        },
        "toolResultContent",
    )
    if payload["schemaVersion"] != "1.0":
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result version is invalid.", retryable=False)
    _correlate(payload, request)
    if payload["executionId"] != terminal["executionId"]:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result execution is invalid.", retryable=False)
    reference = terminal["result"]
    content = payload["content"]
    if not isinstance(content, str) or len(content) > 200_000:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result content is invalid.", retryable=False)
    raw = content.encode("utf-8")
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    if (
        payload["mediaType"] != reference["mediaType"]
        or payload["sizeBytes"] != reference["sizeBytes"]
        or payload["digest"] != reference["digest"]
        or payload["sizeBytes"] != len(raw)
        or payload["digest"] != digest
    ):
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool result binding is invalid.", retryable=False)
    return ToolExecutionResult(
        execution_id=payload["executionId"],
        tool_call_id=payload["toolCallId"],
        media_type=payload["mediaType"],
        digest=payload["digest"],
        size_bytes=payload["sizeBytes"],
        content=content,
    )


def _remote_error(
    status: int,
    payload: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    execution_id: str | None,
) -> ToolGatewayError:
    try:
        envelope = _object(payload, "errorEnvelope")
        fields = frozenset(envelope)
        valid_envelopes = {
            frozenset({"schemaVersion", "requestId", "traceId", "error"}),
            frozenset(
                {"schemaVersion", "traceId", "jobId", "idempotencyKey", "error"}
            ),
            frozenset(
                {
                    "schemaVersion",
                    "requestId",
                    "traceId",
                    "jobId",
                    "idempotencyKey",
                    "error",
                }
            ),
            frozenset(
                {"schemaVersion", "requestId", "traceId", "executionId", "error"}
            ),
        }
        if fields not in valid_envelopes or envelope["schemaVersion"] != "1.0":
            raise ValueError
        for field in ("requestId", "traceId", "jobId", "executionId"):
            if field in envelope:
                _uuid(envelope[field], field)
        if "idempotencyKey" in envelope and (
            not isinstance(envelope["idempotencyKey"], str)
            or not IDEMPOTENCY_KEY.fullmatch(envelope["idempotencyKey"])
        ):
            raise ValueError
        job_envelope = fields == frozenset(
            {"schemaVersion", "traceId", "jobId", "idempotencyKey", "error"}
        )
        execution_envelope = fields == frozenset(
            {"schemaVersion", "requestId", "traceId", "executionId", "error"}
        )
        if execution_id is None:
            if not job_envelope or any(
                envelope[field] != request[field]
                for field in ("traceId", "jobId", "idempotencyKey")
            ):
                raise ValueError
        elif not execution_envelope or envelope["executionId"] != execution_id:
            raise ValueError
        error = _object(envelope["error"], "error")
        code = error["code"]
        retryable = error["retryable"]
        if not isinstance(code, str) or not isinstance(retryable, bool):
            raise ValueError
        if (
            not isinstance(error.get("message"), str)
            or not 1 <= len(error["message"]) <= 1_000
        ):
            raise ValueError
        if code == "TOOL_EXECUTION_TIMEOUT":
            required = {"code", "message", "retryable", "executionState", "executionId"}
            if frozenset(error) not in {
                frozenset(required),
                frozenset(required | {"violations"}),
            }:
                raise ValueError
            if retryable or error["executionState"] != "UNKNOWN":
                raise ValueError
            _uuid(error["executionId"], "error.executionId")
        elif retryable:
            allowed = {
                "code",
                "message",
                "retryable",
                "retryAfterMs",
                "executionState",
                "violations",
            }
            if (
                code not in RETRYABLE_ERROR_CODES
                or set(error) - allowed
                or not {"code", "message", "retryable", "retryAfterMs"} <= set(error)
                or isinstance(error["retryAfterMs"], bool)
                or not isinstance(error["retryAfterMs"], int)
                or not 1 <= error["retryAfterMs"] <= 3_600_000
                or error.get("executionState") not in {None, "NOT_STARTED", "IN_PROGRESS"}
            ):
                raise ValueError
        else:
            allowed = {
                "code",
                "message",
                "retryable",
                "executionState",
                "violations",
            }
            if (
                code not in NON_RETRYABLE_ERROR_CODES
                or set(error) - allowed
                or not {"code", "message", "retryable"} <= set(error)
                or error.get("executionState")
                not in {None, "NOT_STARTED", "COMPLETED"}
            ):
                raise ValueError
        _validate_violations(error.get("violations"))
        return ToolGatewayError(
            code,
            "Spring Tool Gateway rejected the request.",
            retryable=retryable,
            status=status,
            retry_after_ms=error.get("retryAfterMs"),
            execution_state=error.get("executionState"),
            execution_id=error.get("executionId"),
        )
    except (KeyError, TypeError, ValueError, ToolGatewayError):
        return ToolGatewayError(
            "TOOL_RESPONSE_INVALID",
            "Spring Tool Gateway returned an invalid error response.",
            retryable=False,
            status=status,
        )


def _correlate(response: Mapping[str, Any], request: Mapping[str, Any]) -> None:
    for field in ("requestId", "toolCallId", "jobId", "traceId", "idempotencyKey"):
        if response.get(field) != request[field]:
            raise ToolGatewayError("TOOL_RESPONSE_INVALID", "Tool response correlation is invalid.", retryable=False)


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} is invalid.", retryable=False)
    return dict(value)


def _exact(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} fields are invalid.", retryable=False)


def _uuid(value: Any, field: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value.lower():
            raise ValueError
    except (ValueError, AttributeError):
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} is invalid.", retryable=False) from None
    return value


def _timestamp(value: Any, field: str) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed
    except ValueError:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} is invalid.", retryable=False) from None


def _digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SHA256_DIGEST.fullmatch(value):
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} is invalid.", retryable=False)
    return value


def _size(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 200_000:
        raise ToolGatewayError("TOOL_RESPONSE_INVALID", f"{field} is invalid.", retryable=False)
    return value


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID", f"{field} is invalid.", retryable=False
        )
    return value


def _matched(
    value: Any, pattern: re.Pattern[str], field: str, maximum: int
) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or not pattern.fullmatch(value)
    ):
        raise ToolGatewayError(
            "TOOL_ARGUMENTS_INVALID", f"{field} is invalid.", retryable=False
        )
    return value


def _relative_path(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 1_000
        or value.startswith("/")
        or re.match(r"^[A-Za-z]:", value)
        or "\\" in value
        or ":" in value
        or re.search(r"%[0-9A-Fa-f]{2}", value)
        or any(segment == ".." for segment in value.split("/"))
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ToolGatewayError(
            "PATH_POLICY_DENIED", f"{field} is invalid.", retryable=False
        )
    return value


def _validate_violations(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, list) or len(value) > 100:
        raise ToolGatewayError(
            "TOOL_RESPONSE_INVALID", "Tool violations are invalid.", retryable=False
        )
    for index, item in enumerate(value):
        violation = _object(item, f"violations[{index}]")
        _exact(violation, {"field", "reason"}, f"violations[{index}]")
        if (
            not isinstance(violation["field"], str)
            or not violation["field"].startswith("/")
            or len(violation["field"]) > 500
            or not isinstance(violation["reason"], str)
            or not 1 <= len(violation["reason"]) <= 1_000
        ):
            raise ToolGatewayError(
                "TOOL_RESPONSE_INVALID",
                "Tool violation is invalid.",
                retryable=False,
            )


def _canonical_origin(value: str) -> bool:
    parsed = urlsplit(value)
    return (
        parsed.scheme == "http"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.path == ""
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.port is not None
    )
