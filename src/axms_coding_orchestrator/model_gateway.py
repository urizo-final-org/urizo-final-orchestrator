"""Strict, secret-safe client for Spring's internal Model Turn boundary.

Python owns graph execution only. Provider selection, provider credentials and
model invocation remain behind the Spring endpoint represented by this client.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import socket
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlsplit
from uuid import UUID

SCHEMA_VERSION = "1.0"
MAX_RESPONSE_BYTES = 1_048_576
SPRING_PRIVATE_ORIGIN = "http://spring-app:8080"
MODEL_TURN_PATH = "/internal/coding/model-turns"
IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
NODE_NAME = re.compile(r"^[a-z][a-z0-9_-]*$")
PROMPT_VERSION = re.compile(r"^[A-Za-z0-9._-]+$")
NON_RETRYABLE_ERROR_CODES = frozenset(
    {
        "CONTRACT_VALIDATION_FAILED",
        "UNSUPPORTED_SCHEMA_VERSION",
        "UNKNOWN_FIELD",
        "SERVICE_AUTHENTICATION_FAILED",
        "SERVICE_AUTHORIZATION_DENIED",
        "IDEMPOTENCY_KEY_REUSED",
        "JOB_NOT_FOUND",
        "JOB_EXPIRED",
        "JOB_STATE_VERSION_CONFLICT",
        "MODEL_NOT_CONFIGURED",
        "MODEL_CAPABILITY_UNSUPPORTED",
        "MODEL_RESPONSE_INVALID",
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
        "TOOL_EXECUTION_NOT_FOUND",
        "CODING_AGENT_NOT_AVAILABLE",
    }
)
RETRYABLE_ERROR_CODES = frozenset(
    {
        "IDEMPOTENCY_IN_PROGRESS",
        "MODEL_RATE_LIMITED",
        "MODEL_TIMEOUT",
        "MODEL_PROVIDER_UNAVAILABLE",
        "TOOL_RESULT_NOT_READY",
        "TOOL_EXECUTOR_UNAVAILABLE",
        "INTERNAL_TRANSIENT_ERROR",
    }
)

REQUEST_FIELDS = frozenset(
    {
        "schemaVersion",
        "turnId",
        "jobId",
        "traceId",
        "idempotencyKey",
        "attempt",
        "expectedStateVersion",
        "nodeName",
        "promptVersion",
        "contextDigest",
        "requiredCapabilities",
        "messages",
        "toolSchemas",
        "responseFormat",
        "deadlineAt",
    }
)
RESPONSE_FIELDS = frozenset(
    {
        "schemaVersion",
        "turnId",
        "jobId",
        "traceId",
        "idempotencyKey",
        "assistant",
        "toolCalls",
        "responseFormat",
        "selectedModel",
        "usage",
        "latencyMs",
        "finishReason",
        "completedAt",
    }
)


class ContractViolation(ValueError):
    """A safe, payload-free contract validation failure."""


class ModelGatewayRemoteError(RuntimeError):
    """A sanitized error returned by or generated while calling Spring."""

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


class ServiceCredentialLease:
    """Short-lived credential buffer that erases its owned bytes on close."""

    def __init__(self, credential: bytes | bytearray) -> None:
        if not credential:
            raise ValueError("service credential cannot be empty")
        if any(value < 0x21 or value > 0x7E for value in credential):
            raise ValueError("service credential must contain visible ASCII only")
        self._credential = bytearray(credential)
        self._closed = False

    def copy(self) -> bytearray:
        if self._closed:
            raise RuntimeError("service credential lease is closed")
        return bytearray(self._credential)

    def close(self) -> None:
        if not self._closed:
            for index in range(len(self._credential)):
                self._credential[index] = 0
            self._closed = True

    def __enter__(self) -> ServiceCredentialLease:
        if self._closed:
            raise RuntimeError("service credential lease is closed")
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "ServiceCredentialLease[credential=REDACTED, closed=%s]" % self._closed


class CredentialResolver(Protocol):
    def __call__(self) -> ServiceCredentialLease: ...


class FileServiceCredentialResolver:
    """Reads one local Spring service credential without caching its plaintext."""

    def __init__(self, credential_file: str | Path) -> None:
        self._credential_file = Path(credential_file)

    def __call__(self) -> ServiceCredentialLease:
        try:
            with self._credential_file.open("rb") as stream:
                credential = bytearray(stream.read(513))
        except OSError:
            raise ModelGatewayRemoteError(
                "CODING_AGENT_NOT_AVAILABLE",
                "Spring service credential is unavailable.",
                retryable=False,
            ) from None
        try:
            if len(credential) > 512:
                raise ValueError("service credential exceeds the size limit")
            return ServiceCredentialLease(credential)
        finally:
            for index in range(len(credential)):
                credential[index] = 0

    def __repr__(self) -> str:
        return "FileServiceCredentialResolver[credential=REDACTED]"


@dataclass(frozen=True, slots=True)
class ModelTurnRequest:
    _payload: dict[str, Any]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ModelTurnRequest:
        payload = _object(value, "request")
        _exact_fields(payload, REQUEST_FIELDS, "request")
        _schema_version(payload["schemaVersion"])
        for field in ("turnId", "jobId", "traceId"):
            _uuid(payload[field], field)
        _matched_string(payload["idempotencyKey"], IDEMPOTENCY_KEY, "idempotencyKey", 128)
        _positive_integer(payload["attempt"], "attempt")
        _positive_integer(payload["expectedStateVersion"], "expectedStateVersion")
        _matched_string(payload["nodeName"], NODE_NAME, "nodeName", 120)
        _matched_string(payload["promptVersion"], PROMPT_VERSION, "promptVersion", 120)
        _matched_string(payload["contextDigest"], SHA256_DIGEST, "contextDigest", 71)

        capabilities = _list(payload["requiredCapabilities"], "requiredCapabilities", 1, 3)
        if len(capabilities) != len(set(capabilities)) or not set(capabilities) <= {
            "CHAT",
            "STRUCTURED_OUTPUT",
            "TOOL_CALLING",
        }:
            raise ContractViolation("requiredCapabilities is invalid")
        if "CHAT" not in capabilities:
            raise ContractViolation("requiredCapabilities must include CHAT")
        if {"STRUCTURED_OUTPUT", "TOOL_CALLING"} <= set(capabilities):
            raise ContractViolation("structured output and tool calling cannot be combined")

        messages = _list(payload["messages"], "messages", 1, 200)
        for index, message in enumerate(messages):
            _validate_message(message, f"messages[{index}]")
        tool_schemas = _list(payload["toolSchemas"], "toolSchemas", 0, 50)
        for index, tool_schema in enumerate(tool_schemas):
            _validate_tool_schema(tool_schema, f"toolSchemas[{index}]")
        tool_names = [tool_schema["name"] for tool_schema in tool_schemas]
        if len(tool_names) != len(set(tool_names)):
            raise ContractViolation("toolSchemas contains duplicate names")
        if ("TOOL_CALLING" in capabilities) != bool(tool_schemas):
            raise ContractViolation("toolSchemas does not match TOOL_CALLING capability")

        response_format = _object(payload["responseFormat"], "responseFormat")
        _validate_response_format_request(response_format)
        structured = response_format["type"] == "JSON_SCHEMA"
        if structured != ("STRUCTURED_OUTPUT" in capabilities):
            raise ContractViolation("responseFormat does not match STRUCTURED_OUTPUT capability")
        _timestamp(payload["deadlineAt"], "deadlineAt")
        return cls(deepcopy(payload))

    @classmethod
    def from_json(cls, raw: bytes | str) -> ModelTurnRequest:
        return cls.from_dict(_decode_json(raw))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    def to_json(self) -> bytes:
        return json.dumps(self._payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @property
    def deadline_at(self) -> datetime:
        return _timestamp(self._payload["deadlineAt"], "deadlineAt")

    def correlation(self) -> tuple[str, str, str, str]:
        return tuple(self._payload[key] for key in ("turnId", "jobId", "traceId", "idempotencyKey"))  # type: ignore[return-value]

    def __repr__(self) -> str:
        return "ModelTurnRequest[turnId=%s, jobId=%s, messages=REDACTED]" % (
            self._payload["turnId"],
            self._payload["jobId"],
        )


@dataclass(frozen=True, slots=True)
class ModelTurnResponse:
    _payload: dict[str, Any]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ModelTurnResponse:
        payload = _object(value, "response")
        _exact_fields(payload, RESPONSE_FIELDS, "response")
        _schema_version(payload["schemaVersion"])
        for field in ("turnId", "jobId", "traceId"):
            _uuid(payload[field], field)
        _matched_string(payload["idempotencyKey"], IDEMPOTENCY_KEY, "idempotencyKey", 128)

        assistant = _object(payload["assistant"], "assistant")
        _exact_fields(assistant, {"role", "content"}, "assistant")
        if assistant["role"] != "assistant" or not isinstance(assistant["content"], str) or len(assistant["content"]) > 200_000:
            raise ContractViolation("assistant is invalid")
        tool_calls = _list(payload["toolCalls"], "toolCalls", 0, 50)
        for index, tool_call in enumerate(tool_calls):
            _validate_tool_call(tool_call, f"toolCalls[{index}]")
        _validate_response_format_result(_object(payload["responseFormat"], "responseFormat"))

        selected_model = _object(payload["selectedModel"], "selectedModel")
        _exact_fields(selected_model, {"provider", "modelId"}, "selectedModel")
        if selected_model["provider"] not in {"OPENAI", "ANTHROPIC", "GOOGLE", "LOCAL"}:
            raise ContractViolation("selectedModel.provider is invalid")
        _bounded_string(selected_model["modelId"], "selectedModel.modelId", 1, 200)

        usage = _object(payload["usage"], "usage")
        _exact_fields(usage, {"inputTokens", "outputTokens", "totalTokens"}, "usage")
        for field in ("inputTokens", "outputTokens", "totalTokens"):
            _nonnegative_integer(usage[field], f"usage.{field}")
        if usage["totalTokens"] != usage["inputTokens"] + usage["outputTokens"]:
            raise ContractViolation("usage.totalTokens is inconsistent")
        _nonnegative_integer(payload["latencyMs"], "latencyMs")
        if payload["finishReason"] not in {"STOP", "TOOL_CALLS", "LENGTH", "CONTENT_FILTER"}:
            raise ContractViolation("finishReason is invalid")
        if (payload["finishReason"] == "TOOL_CALLS") != bool(tool_calls):
            raise ContractViolation("toolCalls does not match finishReason")
        _timestamp(payload["completedAt"], "completedAt")
        return cls(deepcopy(payload))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    def correlation(self) -> tuple[str, str, str, str]:
        return tuple(self._payload[key] for key in ("turnId", "jobId", "traceId", "idempotencyKey"))  # type: ignore[return-value]

    def __repr__(self) -> str:
        return "ModelTurnResponse[turnId=%s, jobId=%s, assistant=REDACTED]" % (
            self._payload["turnId"],
            self._payload["jobId"],
        )


class ModelGatewayClient:
    """Calls the Spring boundary without owning a provider credential."""

    def __init__(
        self,
        endpoint: str,
        credential_resolver: CredentialResolver,
        *,
        max_timeout_seconds: float = 30.0,
        now: Callable[[], datetime] | None = None,
        allowed_origins: set[str] | frozenset[str] | None = None,
    ) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "http"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or parsed.query
            or parsed.path != MODEL_TURN_PATH
        ):
            raise ValueError("endpoint must be the exact credential-free Spring Model Turn URL")
        origins = frozenset(allowed_origins or {SPRING_PRIVATE_ORIGIN})
        if not origins or any(_parse_origin(origin) != origin for origin in origins):
            raise ValueError("allowed_origins must contain canonical HTTP origins")
        if _endpoint_origin(parsed) not in origins:
            raise ValueError("endpoint origin is not allowlisted")
        if max_timeout_seconds <= 0:
            raise ValueError("max_timeout_seconds must be positive")
        self._endpoint = endpoint
        self._credential_resolver = credential_resolver
        self._max_timeout_seconds = max_timeout_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))

    def execute(self, request: ModelTurnRequest) -> ModelTurnResponse:
        remaining = (request.deadline_at - self._now()).total_seconds()
        if remaining <= 0:
            raise ModelGatewayRemoteError(
                "MODEL_TIMEOUT",
                "Model turn deadline has elapsed.",
                retryable=True,
            )
        timeout = min(remaining, self._max_timeout_seconds)
        credential = bytearray()
        try:
            with self._credential_resolver() as lease:
                credential = lease.copy()
            try:
                status, raw = _post_http(self._endpoint, request.to_json(), credential, timeout)
            except ContractViolation:
                raise ModelGatewayRemoteError(
                    "MODEL_RESPONSE_INVALID",
                    "Spring Model Gateway returned an invalid HTTP response.",
                    retryable=False,
                ) from None
            except (TimeoutError, socket.timeout, OSError):
                raise ModelGatewayRemoteError(
                    "MODEL_PROVIDER_UNAVAILABLE",
                    "Spring Model Gateway is unavailable.",
                    retryable=True,
                ) from None
            if not 200 <= status < 300:
                raise _remote_error(status, raw, request)
        finally:
            for index in range(len(credential)):
                credential[index] = 0

        try:
            result = ModelTurnResponse.from_dict(_decode_json(raw))
        except ContractViolation:
            raise ModelGatewayRemoteError(
                "MODEL_RESPONSE_INVALID",
                "Spring Model Gateway returned an invalid response.",
                retryable=False,
            ) from None
        if result.correlation() != request.correlation():
            raise ModelGatewayRemoteError(
                "CONTRACT_CORRELATION_MISMATCH",
                "Model turn response correlation does not match the request.",
                retryable=False,
            )
        _validate_response_binding(request, result)
        return result


def _remote_error(
    status: int,
    raw: bytes,
    request: ModelTurnRequest,
) -> ModelGatewayRemoteError:
    try:
        payload = _object(_decode_json(raw), "errorEnvelope")
        fields = set(payload)
        job_fields = {"schemaVersion", "traceId", "jobId", "idempotencyKey", "error"}
        pre_context_fields = {"schemaVersion", "requestId", "traceId", "error"}
        job_scoped = fields == job_fields
        pre_context = fields == pre_context_fields
        if not (job_scoped ^ pre_context):
            raise ContractViolation("errorEnvelope shape is invalid")
        _schema_version(payload["schemaVersion"])
        _uuid(payload["traceId"], "traceId")
        if "requestId" in payload:
            _uuid(payload["requestId"], "requestId")
        if "jobId" in payload:
            _uuid(payload["jobId"], "jobId")
        if "idempotencyKey" in payload:
            _matched_string(payload["idempotencyKey"], IDEMPOTENCY_KEY, "idempotencyKey", 128)
        if job_scoped:
            expected = request.to_dict()
            if (
                payload["traceId"] != expected["traceId"]
                or payload["jobId"] != expected["jobId"]
                or payload["idempotencyKey"] != expected["idempotencyKey"]
            ):
                raise ContractViolation("errorEnvelope correlation is invalid")
        error = _object(payload["error"], "error")
        allowed_error = {
            "code",
            "message",
            "retryable",
            "retryAfterMs",
            "executionState",
            "executionId",
            "violations",
        }
        if set(error) - allowed_error or not {"code", "message", "retryable"} <= set(error):
            raise ContractViolation("error is invalid")
        code = _bounded_string(error["code"], "error.code", 1, 120)
        message = _bounded_string(error["message"], "error.message", 1, 1_000)
        if not isinstance(error["retryable"], bool):
            raise ContractViolation("error.retryable is invalid")
        expected_codes = RETRYABLE_ERROR_CODES if error["retryable"] else NON_RETRYABLE_ERROR_CODES
        if code not in expected_codes:
            raise ContractViolation("error.code is not canonical for retryability")
        retry_after_ms = error.get("retryAfterMs")
        if error["retryable"]:
            _positive_integer(retry_after_ms, "error.retryAfterMs")
            if retry_after_ms > 3_600_000:
                raise ContractViolation("error.retryAfterMs is invalid")
        elif retry_after_ms is not None:
            raise ContractViolation("non-retryable error cannot contain retryAfterMs")
        execution_state = error.get("executionState")
        if execution_state is not None and execution_state not in {
            "NOT_STARTED",
            "COMPLETED",
            "IN_PROGRESS",
            "UNKNOWN",
        }:
            raise ContractViolation("error.executionState is invalid")
        if "executionId" in error:
            _uuid(error["executionId"], "error.executionId")
        if "violations" in error:
            violations = _list(error["violations"], "error.violations", 0, 100)
            for index, violation_value in enumerate(violations):
                violation = _object(violation_value, f"error.violations[{index}]")
                _exact_fields(violation, {"field", "reason"}, f"error.violations[{index}]")
                field = _bounded_string(violation["field"], "violation.field", 1, 500)
                if not field.startswith("/"):
                    raise ContractViolation("violation.field is invalid")
                _bounded_string(violation["reason"], "violation.reason", 1, 1_000)
        return ModelGatewayRemoteError(
            code,
            message,
            retryable=error["retryable"],
            status=status,
            retry_after_ms=retry_after_ms,
        )
    except ContractViolation:
        return ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Spring Model Gateway returned an invalid error response.",
            retryable=False,
            status=status,
        )


def _post_http(endpoint: str, body: bytes, credential: bytearray, timeout: float) -> tuple[int, bytes]:
    return _request_http("POST", endpoint, body, credential, timeout)


def _request_http(
    method: str,
    endpoint: str,
    body: bytes | None,
    credential: bytearray | None,
    timeout: float,
) -> tuple[int, bytes]:
    """Minimal HTTP/1.1 transport for the local Spring service boundary.

    Deliberately supports HTTP only. TLS and service-identity infrastructure are
    a later activation concern; this local foundation must not silently invent
    certificate or credential policy.
    """

    parsed = urlsplit(endpoint)
    host = parsed.hostname
    if host is None:
        raise ValueError("endpoint host is required")
    port = parsed.port or 80
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    host_header = host if port == 80 else f"{host}:{port}"
    wire = bytearray()
    try:
        if method not in {"GET", "POST"}:
            raise ValueError("unsupported HTTP method")
        wire.extend(f"{method} {target} HTTP/1.1\r\n".encode("ascii"))
        wire.extend(f"Host: {host_header}\r\n".encode("ascii"))
        wire.extend(b"Accept: application/json\r\n")
        if credential is not None:
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
                        raise ModelGatewayRemoteError(
                            "MODEL_RESPONSE_INVALID",
                            "Spring Model Gateway response exceeded the size limit.",
                            retryable=False,
                        )
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


def _read_chunked(stream: Any) -> bytes:
    result = bytearray()
    while True:
        size_line = stream.readline(128)
        try:
            size = int(size_line.split(b";", 1)[0].strip(), 16)
        except ValueError:
            raise ContractViolation("HTTP chunk size is invalid") from None
        if size == 0:
            while stream.readline(8_193) not in {b"\r\n", b"\n", b""}:
                pass
            break
        if len(result) + size > MAX_RESPONSE_BYTES:
            raise ModelGatewayRemoteError(
                "MODEL_RESPONSE_INVALID",
                "Spring Model Gateway response exceeded the size limit.",
                retryable=False,
            )
        chunk = stream.read(size)
        if len(chunk) != size or stream.read(2) != b"\r\n":
            raise ContractViolation("HTTP chunk is incomplete")
        result.extend(chunk)
    return bytes(result)


def _check_body_size(raw: bytes) -> None:
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Spring Model Gateway response exceeded the size limit.",
            retryable=False,
        )


def _decode_json(raw: bytes | str) -> Mapping[str, Any]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        raise ContractViolation("payload is not valid JSON") from None
    return _object(value, "payload")


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ContractViolation(f"{field} must be an object")
    return dict(value)


def _list(value: Any, field: str, minimum: int, maximum: int) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ContractViolation(f"{field} has an invalid item count")
    return value


def _exact_fields(value: Mapping[str, Any], expected: set[str] | frozenset[str], field: str) -> None:
    if set(value) != set(expected):
        raise ContractViolation(f"{field} contains missing or unknown fields")


def _schema_version(value: Any) -> None:
    if value != SCHEMA_VERSION:
        raise ContractViolation("schemaVersion is unsupported")


def _uuid(value: Any, field: str) -> None:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value.lower():
            raise ValueError
    except (ValueError, AttributeError):
        raise ContractViolation(f"{field} is invalid") from None


def _timestamp(value: Any, field: str) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed
    except ValueError:
        raise ContractViolation(f"{field} is invalid") from None


def _bounded_string(value: Any, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum:
        raise ContractViolation(f"{field} is invalid")
    return value


def _matched_string(value: Any, pattern: re.Pattern[str], field: str, maximum: int) -> str:
    result = _bounded_string(value, field, 1, maximum)
    if not pattern.fullmatch(result):
        raise ContractViolation(f"{field} is invalid")
    return result


def _positive_integer(value: Any, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContractViolation(f"{field} is invalid")


def _nonnegative_integer(value: Any, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field} is invalid")


def _validate_message(value: Any, field: str) -> None:
    message = _object(value, field)
    role = message.get("role")
    if role in {"system", "user", "assistant"} and set(message) == {"role", "content"}:
        _bounded_string(message["content"], f"{field}.content", 1, 200_000)
        return
    if role == "assistant" and set(message) == {"role", "content", "toolCalls"}:
        _bounded_string(message["content"], f"{field}.content", 0, 200_000)
        for index, tool_call in enumerate(_list(message["toolCalls"], f"{field}.toolCalls", 1, 50)):
            _validate_tool_call(tool_call, f"{field}.toolCalls[{index}]")
        return
    if role == "tool" and set(message) == {"role", "toolCallId", "executionId", "result", "content"}:
        _uuid(message["toolCallId"], f"{field}.toolCallId")
        _uuid(message["executionId"], f"{field}.executionId")
        _object(message["result"], f"{field}.result")
        _bounded_string(message["content"], f"{field}.content", 0, 200_000)
        return
    raise ContractViolation(f"{field} is invalid")


def _validate_tool_call(value: Any, field: str) -> None:
    tool_call = _object(value, field)
    _exact_fields(tool_call, {"toolCallId", "name", "arguments"}, field)
    _uuid(tool_call["toolCallId"], f"{field}.toolCallId")
    _matched_string(tool_call["name"], re.compile(r"^[a-z][a-z0-9_]*$"), f"{field}.name", 120)
    _object(tool_call["arguments"], f"{field}.arguments")


def _validate_tool_schema(value: Any, field: str) -> None:
    tool_schema = _object(value, field)
    _exact_fields(tool_schema, {"name", "description", "inputSchema", "schemaDigest"}, field)
    _matched_string(tool_schema["name"], re.compile(r"^[a-z][a-z0-9_]*$"), f"{field}.name", 120)
    _bounded_string(tool_schema["description"], f"{field}.description", 1, 2_000)
    _object(tool_schema["inputSchema"], f"{field}.inputSchema")
    _matched_string(tool_schema["schemaDigest"], SHA256_DIGEST, f"{field}.schemaDigest", 71)


def _validate_response_format_request(value: Mapping[str, Any]) -> None:
    if value.get("type") == "TEXT":
        _exact_fields(value, {"type"}, "responseFormat")
    elif value.get("type") == "JSON_SCHEMA":
        _exact_fields(value, {"type", "schemaDigest", "outputSchema"}, "responseFormat")
        _matched_string(value["schemaDigest"], SHA256_DIGEST, "responseFormat.schemaDigest", 71)
        _object(value["outputSchema"], "responseFormat.outputSchema")
    else:
        raise ContractViolation("responseFormat is invalid")


def _validate_response_format_result(value: Mapping[str, Any]) -> None:
    if value.get("type") == "TEXT":
        _exact_fields(value, {"type"}, "responseFormat")
    elif value.get("type") == "JSON_SCHEMA":
        _exact_fields(value, {"type", "schemaDigest", "structuredOutput"}, "responseFormat")
        _matched_string(value["schemaDigest"], SHA256_DIGEST, "responseFormat.schemaDigest", 71)
        _object(value["structuredOutput"], "responseFormat.structuredOutput")
    else:
        raise ContractViolation("responseFormat is invalid")


def _validate_response_binding(request: ModelTurnRequest, response: ModelTurnResponse) -> None:
    request_payload = request.to_dict()
    response_payload = response.to_dict()
    request_format = request_payload["responseFormat"]
    response_format = response_payload["responseFormat"]
    if request_format["type"] != response_format["type"]:
        raise ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Model turn response format does not match the request.",
            retryable=False,
        )
    if request_format["type"] == "JSON_SCHEMA" and (
        request_format["schemaDigest"] != response_format["schemaDigest"]
    ):
        raise ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Model turn response schema does not match the request.",
            retryable=False,
        )
    tool_calls = response_payload["toolCalls"]
    capabilities = set(request_payload["requiredCapabilities"])
    if "TOOL_CALLING" not in capabilities and tool_calls:
        raise ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Model turn returned an unrequested tool candidate.",
            retryable=False,
        )
    allowed_tools = {tool_schema["name"] for tool_schema in request_payload["toolSchemas"]}
    if any(tool_call["name"] not in allowed_tools for tool_call in tool_calls):
        raise ModelGatewayRemoteError(
            "MODEL_RESPONSE_INVALID",
            "Model turn returned an unknown tool candidate.",
            retryable=False,
        )


def _parse_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("origin is invalid")
    return _endpoint_origin(parsed)


def _endpoint_origin(parsed: Any) -> str:
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    port = parsed.port or 80
    return f"http://{host}:{port}"
