"""Spring-owned coding worker claim, heartbeat, and outcome client."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import socket
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
from uuid import UUID

from .contracts import (
    CodingJobRequested,
    IDEMPOTENCY_KEY,
    OUTCOMES,
    WorkerClaim,
    WorkerContractViolation,
    canonical_json_bytes,
    validate_lease_response,
    validate_outcome_receipt,
)
from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    ModelGatewayRemoteError,
    NON_RETRYABLE_ERROR_CODES,
    RETRYABLE_ERROR_CODES,
    SPRING_PRIVATE_ORIGIN,
    _request_http,
)


SAFE_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")


class WorkerApiError(RuntimeError):
    """Safe worker API failure that never retains the remote payload."""

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


class WorkerApiClient:
    def __init__(
        self,
        spring_origin: str,
        credential_resolver: CredentialResolver,
        *,
        timeout_seconds: float = 10.0,
        now: Callable[[], datetime] | None = None,
        allowed_origins: set[str] | frozenset[str] | None = None,
    ) -> None:
        origins = frozenset(allowed_origins or {SPRING_PRIVATE_ORIGIN})
        if spring_origin not in origins or not _canonical_origin(spring_origin):
            raise ValueError("Spring worker origin is not allowlisted")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._origin = spring_origin
        self._credential_resolver = credential_resolver
        self._timeout_seconds = timeout_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))

    def claim(self, event: CodingJobRequested) -> WorkerClaim:
        body = {
            "schemaVersion": "1.0",
            "eventId": event.event_id,
            "jobId": event.job_id,
            "traceId": event.trace_id,
            "idempotencyKey": event.idempotency_key,
            "attempt": event.attempt,
            "expectedStateVersion": event.expected_state_version,
        }
        response = self._call(
            "POST",
            f"/internal/coding/worker/jobs/{event.job_id}/claim",
            canonical_json_bytes(body),
            error_context=body,
        )
        try:
            return WorkerClaim.from_dict(response, event, now=self._now())
        except WorkerContractViolation:
            raise WorkerApiError(
                "WORKER_RESPONSE_INVALID",
                "Spring worker claim response is invalid.",
                retryable=False,
            ) from None

    def heartbeat(self, claim: WorkerClaim, idempotency_key: str) -> dict[str, Any]:
        body = {
            "schemaVersion": "1.0",
            "jobId": claim.job_id,
            "traceId": claim.trace_id,
            "leaseId": claim.lease_id,
            "idempotencyKey": idempotency_key,
            "expectedStateVersion": claim.state_version,
        }
        response = self._call(
            "POST",
            f"/internal/coding/worker/jobs/{claim.job_id}/heartbeat",
            canonical_json_bytes(body),
            error_context=body,
        )
        try:
            return validate_lease_response(response, claim, now=self._now())
        except WorkerContractViolation:
            raise WorkerApiError(
                "WORKER_RESPONSE_INVALID",
                "Spring worker heartbeat response is invalid.",
                retryable=False,
            ) from None

    def outcome(
        self,
        claim: WorkerClaim,
        outcome: str,
        idempotency_key: str,
        *,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        if outcome not in OUTCOMES:
            raise ValueError("outcome is invalid")
        failure = outcome in {"RETRYABLE_FAILURE", "PERMANENT_FAILURE"}
        if failure != (error_code is not None):
            raise ValueError("error_code must be present only for failure outcomes")
        if error_code is not None and not SAFE_ERROR_CODE.fullmatch(error_code):
            raise ValueError("error_code is invalid")
        body = {
            "schemaVersion": "1.0",
            "jobId": claim.job_id,
            "traceId": claim.trace_id,
            "leaseId": claim.lease_id,
            "idempotencyKey": idempotency_key,
            "expectedStateVersion": claim.state_version,
            "outcome": outcome,
            "errorCode": error_code,
        }
        response = self._call(
            "POST",
            f"/internal/coding/worker/jobs/{claim.job_id}/outcomes",
            canonical_json_bytes(body),
            error_context=body,
        )
        try:
            return validate_outcome_receipt(response, claim, outcome)
        except WorkerContractViolation:
            raise WorkerApiError(
                "WORKER_RESPONSE_INVALID",
                "Spring worker outcome response is invalid.",
                retryable=False,
            ) from None

    def healthy(self) -> bool:
        """Probe Spring liveness without treating it as worker authorization state."""

        try:
            response = self._call(
                "GET",
                "/api/health",
                None,
                timeout_seconds=min(2.0, self._timeout_seconds),
                authenticated=False,
            )
            if set(response) != {"schemaVersion", "traceId", "status", "checkedAt"}:
                return False
            if response["schemaVersion"] != "1.0" or response["status"] != "UP":
                return False
            if not isinstance(response["traceId"], str):
                return False
            UUID(response["traceId"])
            checked_at = response["checkedAt"]
            if not isinstance(checked_at, str):
                return False
            timestamp = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            return timestamp.tzinfo is not None
        except (WorkerApiError, ValueError, TypeError, KeyError):
            return False

    def _call(
        self,
        method: str,
        path: str,
        body: bytes | None,
        *,
        timeout_seconds: float | None = None,
        error_context: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Mapping[str, Any]:
        credential = bytearray()
        try:
            if authenticated:
                try:
                    with self._credential_resolver() as lease:
                        credential = lease.copy()
                except ModelGatewayRemoteError:
                    raise WorkerApiError(
                        "CODING_AGENT_NOT_AVAILABLE",
                        "Spring worker credential is unavailable.",
                        retryable=False,
                    ) from None
            try:
                status, raw = _request_http(
                    method,
                    self._origin + path,
                    body,
                    credential if authenticated else None,
                    self._timeout_seconds
                    if timeout_seconds is None
                    else timeout_seconds,
                )
            except (TimeoutError, socket.timeout, OSError):
                raise WorkerApiError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "Spring worker API is unavailable.",
                    retryable=True,
                ) from None
            except (ContractViolation, ModelGatewayRemoteError):
                raise WorkerApiError(
                    "WORKER_RESPONSE_INVALID",
                    "Spring worker API returned an invalid HTTP response.",
                    retryable=False,
                ) from None
        finally:
            for index in range(len(credential)):
                credential[index] = 0
        if not 200 <= status < 300:
            raise _safe_remote_error(status, raw, error_context)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise WorkerApiError(
                "WORKER_RESPONSE_INVALID",
                "Spring worker API returned invalid JSON.",
                retryable=False,
            ) from None
        if not isinstance(value, Mapping):
            raise WorkerApiError(
                "WORKER_RESPONSE_INVALID",
                "Spring worker API returned an invalid response.",
                retryable=False,
            )
        return value


def _safe_remote_error(
    status: int,
    raw: bytes,
    context: Mapping[str, Any] | None,
) -> WorkerApiError:
    try:
        envelope = json.loads(raw)
        if not isinstance(envelope, Mapping) or envelope.get("schemaVersion") != "1.0":
            raise ValueError
        fields = frozenset(envelope)
        job_scoped = fields == frozenset(
            {"schemaVersion", "traceId", "jobId", "idempotencyKey", "error"}
        )
        pre_context = fields == frozenset(
            {"schemaVersion", "requestId", "traceId", "error"}
        )
        if not (job_scoped ^ pre_context):
            raise ValueError
        UUID(envelope["traceId"])
        if pre_context:
            UUID(envelope["requestId"])
        else:
            UUID(envelope["jobId"])
            if (
                context is None
                or envelope["traceId"] != context.get("traceId")
                or envelope["jobId"] != context.get("jobId")
                or envelope["idempotencyKey"] != context.get("idempotencyKey")
                or not isinstance(envelope["idempotencyKey"], str)
                or not IDEMPOTENCY_KEY.fullmatch(envelope["idempotencyKey"])
            ):
                raise ValueError
        error = envelope["error"]
        if not isinstance(error, Mapping):
            raise ValueError
        allowed = {
            "code",
            "message",
            "retryable",
            "retryAfterMs",
            "executionState",
            "violations",
        }
        required = {"code", "message", "retryable"}
        if set(error) - allowed or not required <= set(error):
            raise ValueError
        code = error.get("code")
        retryable = error.get("retryable")
        retry_after = error.get("retryAfterMs")
        if not isinstance(code, str) or not SAFE_ERROR_CODE.fullmatch(code):
            raise ValueError
        if not isinstance(error.get("message"), str) or not 1 <= len(error["message"]) <= 1_000:
            raise ValueError
        if not isinstance(retryable, bool):
            raise ValueError
        canonical = RETRYABLE_ERROR_CODES if retryable else NON_RETRYABLE_ERROR_CODES
        if code not in canonical:
            raise ValueError
        if retryable != (retry_after is not None):
            raise ValueError
        if retry_after is not None and (
            isinstance(retry_after, bool)
            or not isinstance(retry_after, int)
            or not 1 <= retry_after <= 3_600_000
        ):
            raise ValueError
        execution_state = error.get("executionState")
        expected_states = (
            {None, "NOT_STARTED", "IN_PROGRESS"}
            if retryable
            else {None, "NOT_STARTED", "COMPLETED"}
        )
        if execution_state not in expected_states:
            raise ValueError
        _validate_violations(error.get("violations"))
        return WorkerApiError(
            code,
            "Spring worker request was rejected.",
            retryable=retryable,
            status=status,
            retry_after_ms=retry_after,
        )
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return WorkerApiError(
            "WORKER_RESPONSE_INVALID",
            "Spring worker API returned an invalid error response.",
            retryable=False,
            status=status,
        )


def _validate_violations(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError
    for violation in value:
        if (
            not isinstance(violation, Mapping)
            or set(violation) != {"field", "reason"}
            or not isinstance(violation["field"], str)
            or not violation["field"].startswith("/")
            or len(violation["field"]) > 500
            or not isinstance(violation["reason"], str)
            or not 1 <= len(violation["reason"]) <= 1_000
        ):
            raise ValueError


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
