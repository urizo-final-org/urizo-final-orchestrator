"""Strict Spring client for the Natural CMS Job and Stage boundary."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import re
import socket
from typing import Any, Mapping, Protocol
from uuid import UUID

from .coding_domain_client import _canonical_origin, _request_coding_http
from .contracts import SHA256_DIGEST, canonical_json_bytes
from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    ModelGatewayRemoteError,
    SPRING_PRIVATE_ORIGIN,
)
from .node_runtime import NodeInvocation
from .snapshot import HANDLER_KEY


CMS_JOB_STATUSES = frozenset({"ACTIVE", "WAITING_APPROVAL", "COMPLETED", "REJECTED"})
CMS_DECISIONS = frozenset({"APPROVED", "REJECTED"})
CMS_PORTS = frozenset(
    {"feasible", "infeasible", "ready", "approved", "rejected", "retry", "discarded", "applied"}
)
RESOURCE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SAFE_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")


class NaturalCmsDomainClientError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class NaturalCmsResource:
    type: str
    id: str

    @classmethod
    def from_dict(cls, value: Any) -> "NaturalCmsResource":
        data = _object(value, "resource")
        if set(data) != {"type", "id"} or data.get("type") != "CONTENT":
            raise ValueError("resource is invalid")
        identifier = data.get("id")
        if not isinstance(identifier, str) or RESOURCE_ID.fullmatch(identifier) is None:
            raise ValueError("resource.id is invalid")
        return cls("CONTENT", identifier)


@dataclass(frozen=True, slots=True)
class NaturalCmsJob:
    job_id: str
    trace_id: str
    profile_version_id: str
    pipeline_attempt: int
    state_version: int
    status: str
    resource: NaturalCmsResource
    preview_id: str | None
    preview_hash: str | None
    preview_valid: bool
    approval_decision: str | None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NaturalCmsJob":
        data = _object(value, "job")
        required = {
            "schemaVersion", "jobId", "traceId", "profileVersionId",
            "pipelineAttempt", "stateVersion", "status", "resource", "previewValid",
        }
        allowed = required | {
            "requestText", "structuredCommand", "previewId", "previewHash",
            "approvalDecision", "approvalFeedback", "createdAt", "updatedAt",
        }
        if (
            not required.issubset(data)
            or not set(data).issubset(allowed)
            or data.get("schemaVersion") != "1.0"
        ):
            raise ValueError("job fields are invalid")
        preview_id = data.get("previewId")
        preview_hash = data.get("previewHash")
        decision = data.get("approvalDecision")
        if preview_id is not None:
            preview_id = _uuid(preview_id, "job.previewId")
        if preview_hash is not None and (
            not isinstance(preview_hash, str) or SHA256_DIGEST.fullmatch(preview_hash) is None
        ):
            raise ValueError("job.previewHash is invalid")
        if (preview_id is None) != (preview_hash is None):
            raise ValueError("job preview is invalid")
        if decision is not None and decision not in CMS_DECISIONS:
            raise ValueError("job approvalDecision is invalid")
        return cls(
            _uuid(data["jobId"], "job.jobId"),
            _uuid(data["traceId"], "job.traceId"),
            _uuid(data["profileVersionId"], "job.profileVersionId"),
            _positive(data["pipelineAttempt"], "job.pipelineAttempt"),
            _positive(data["stateVersion"], "job.stateVersion"),
            _one_of(data["status"], CMS_JOB_STATUSES, "job.status"),
            NaturalCmsResource.from_dict(data["resource"]),
            preview_id,
            preview_hash,
            _boolean(data["previewValid"], "job.previewValid"),
            decision,
        )


@dataclass(frozen=True, slots=True)
class NaturalCmsStageResult:
    result_id: str
    handler_key: str
    result_port: str
    resource: NaturalCmsResource
    structured_command: Mapping[str, Any] | None
    preview_id: str | None
    preview_hash: str | None
    payload: Mapping[str, Any]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NaturalCmsStageResult":
        data = _object(value, "stage")
        required = {
            "schemaVersion", "resultId", "handlerKey", "resultPort", "resource", "payload"
        }
        allowed = required | {
            "structuredCommand", "previewId", "previewHash"
        }
        if (
            not required.issubset(data)
            or not set(data).issubset(allowed)
            or data.get("schemaVersion") != "1.0"
        ):
            raise ValueError("stage fields are invalid")
        command = data.get("structuredCommand")
        if command is not None:
            command = _object(command, "stage.structuredCommand")
        preview_id = data.get("previewId")
        preview_hash = data.get("previewHash")
        if preview_id is not None:
            preview_id = _uuid(preview_id, "stage.previewId")
        if preview_hash is not None and (
            not isinstance(preview_hash, str) or SHA256_DIGEST.fullmatch(preview_hash) is None
        ):
            raise ValueError("stage.previewHash is invalid")
        if (preview_id is None) != (preview_hash is None):
            raise ValueError("stage preview is invalid")
        handler_key = data.get("handlerKey")
        if not isinstance(handler_key, str) or HANDLER_KEY.fullmatch(handler_key) is None:
            raise ValueError("stage.handlerKey is invalid")
        return cls(
            _uuid(data["resultId"], "stage.resultId"),
            handler_key,
            _one_of(data["resultPort"], CMS_PORTS, "stage.resultPort"),
            NaturalCmsResource.from_dict(data["resource"]),
            command,
            preview_id,
            preview_hash,
            _object(data["payload"], "stage.payload"),
        )


class NaturalCmsDomainClient(Protocol):
    def get_job(self, invocation: NodeInvocation) -> NaturalCmsJob: ...

    def execute_stage(
        self, invocation: NodeInvocation, handler_key: str, result_id: str
    ) -> NaturalCmsStageResult: ...


class SpringNaturalCmsDomainClient:
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
            raise ValueError("Spring Natural CMS origin is not allowlisted")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive and finite")
        if not callable(credential_resolver):
            raise TypeError("credential_resolver must be callable")
        self._origin = spring_origin
        self._credential_resolver = credential_resolver
        self._timeout_seconds = float(timeout_seconds)

    def get_job(self, invocation: NodeInvocation) -> NaturalCmsJob:
        response = self._call(
            "GET",
            f"/internal/natural-cms/jobs/{invocation.job_id}/attempts/"
            f"{invocation.pipeline_attempt}",
            None,
            invocation,
        )
        try:
            job = NaturalCmsJob.from_dict(response)
        except (TypeError, ValueError, RecursionError):
            raise _invalid_response() from None
        _match_invocation(job, invocation)
        return job

    def execute_stage(
        self, invocation: NodeInvocation, handler_key: str, result_id: str
    ) -> NaturalCmsStageResult:
        body = canonical_json_bytes(
            {
                "schemaVersion": "1.0",
                "traceId": invocation.trace_id,
                "profileVersionId": invocation.profile_version_id,
                "expectedStateVersion": invocation.state_version,
                "executionAttempt": invocation.execution_attempt,
                "nodeId": invocation.node_id,
                "handlerKey": handler_key,
                "resultId": result_id,
            }
        )
        response = self._call(
            "POST",
            f"/internal/natural-cms/jobs/{invocation.job_id}/attempts/"
            f"{invocation.pipeline_attempt}/stages/{handler_key}/executions/{result_id}",
            body,
            invocation,
        )
        try:
            result = NaturalCmsStageResult.from_dict(response)
        except (TypeError, ValueError, RecursionError):
            raise _invalid_response() from None
        if result.result_id != result_id or result.handler_key != handler_key:
            raise _invalid_response()
        return result

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
                raise NaturalCmsDomainClientError(
                    "SERVICE_AUTHENTICATION_FAILED",
                    "Spring Natural CMS credential is unavailable.",
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
                raise NaturalCmsDomainClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "Spring Natural CMS API is unavailable.",
                    retryable=True,
                ) from None
            except (ContractViolation, ModelGatewayRemoteError):
                raise _invalid_response() from None
        finally:
            for index in range(len(credential)):
                credential[index] = 0
        if not 200 <= status < 300:
            raise _remote_error(status, raw)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise _invalid_response() from None
        if not isinstance(value, Mapping):
            raise _invalid_response()
        return value


def _match_invocation(job: NaturalCmsJob, invocation: NodeInvocation) -> None:
    if (
        job.job_id != invocation.job_id
        or job.trace_id != invocation.trace_id
        or job.profile_version_id != invocation.profile_version_id
        or job.pipeline_attempt != invocation.pipeline_attempt
    ):
        raise _invalid_response()


def _remote_error(status: int, raw: bytes) -> NaturalCmsDomainClientError:
    try:
        value = json.loads(raw)
        if not isinstance(value, Mapping):
            raise ValueError
        if set(value) != {"code", "message", "retryable"}:
            raise ValueError
        code = value.get("code")
        retryable = value.get("retryable")
        if (
            not isinstance(code, str)
            or SAFE_ERROR_CODE.fullmatch(code) is None
            or not isinstance(value.get("message"), str)
            or not isinstance(retryable, bool)
        ):
            raise ValueError
        return NaturalCmsDomainClientError(
            code, "Spring Natural CMS request was rejected.", retryable=retryable
        )
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return _invalid_response()


def _invalid_response() -> NaturalCmsDomainClientError:
    return NaturalCmsDomainClientError(
        "WORKER_RESPONSE_INVALID",
        "Spring Natural CMS API returned an invalid response.",
        retryable=False,
    )


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} is invalid")
    return dict(value)


def _uuid(value: Any, field: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise ValueError(f"{field} is invalid") from None
    return value


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} is invalid")
    return value


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    return value


def _one_of(value: Any, allowed: frozenset[str], field: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{field} is invalid")
    return value
