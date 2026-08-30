"""Strict, credential-safe reader for Spring-owned Profile Version Snapshots."""

from __future__ import annotations

import json
import math
import re
import socket
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import UUID

from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    ModelGatewayRemoteError,
    SPRING_PRIVATE_ORIGIN,
    _request_http,
)
from .snapshot import SnapshotContractViolation, VersionedSnapshot, load_snapshot_json


PROFILE_VERSION_PATH = "/internal/ai/profile-versions"
_SAFE_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")
_PERMANENT_ERRORS = {
    401: "SERVICE_AUTHENTICATION_FAILED",
    404: "PROFILE_VERSION_NOT_FOUND",
    409: "PROFILE_VERSION_NOT_ACTIVE",
}


class ProfileVersionClientError(RuntimeError):
    """Payload-free failure raised at the Spring Profile Version boundary."""

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


class ProfileVersionClient:
    """Read one executable immutable Snapshot from Spring by Profile Version ID."""

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
            raise ValueError("Spring Profile Version origin is not allowlisted")
        if not callable(credential_resolver):
            raise TypeError("credential_resolver must be callable")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or float(timeout_seconds) <= 0
        ):
            raise ValueError("timeout_seconds must be positive and finite")
        self._origin = spring_origin
        self._credential_resolver = credential_resolver
        self._timeout_seconds = float(timeout_seconds)

    def get(self, profile_version_id: str) -> VersionedSnapshot:
        requested_id = _canonical_uuid(profile_version_id, "profileVersionId")
        credential = bytearray()
        try:
            try:
                with self._credential_resolver() as lease:
                    credential = lease.copy()
            except ModelGatewayRemoteError:
                raise ProfileVersionClientError(
                    "SERVICE_AUTHENTICATION_FAILED",
                    "Spring Profile Version credential is unavailable.",
                    retryable=False,
                ) from None
            try:
                status, raw = _request_http(
                    "GET",
                    f"{self._origin}{PROFILE_VERSION_PATH}/{requested_id}",
                    None,
                    credential,
                    self._timeout_seconds,
                )
            except (TimeoutError, socket.timeout, OSError):
                raise ProfileVersionClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "Spring Profile Version API is unavailable.",
                    retryable=True,
                ) from None
            except (ContractViolation, ModelGatewayRemoteError):
                raise _invalid_response() from None
        finally:
            for index in range(len(credential)):
                credential[index] = 0

        if status != 200:
            raise _safe_remote_error(status, raw)
        try:
            snapshot = load_snapshot_json(raw)
        except (SnapshotContractViolation, RecursionError, ValueError):
            raise _invalid_response(status=200) from None
        if snapshot.profile_version_id != requested_id:
            raise _invalid_response(status=200)
        return snapshot


def _safe_remote_error(status: int, raw: bytes) -> ProfileVersionClientError:
    try:
        envelope = _decode_exact_json(raw)
        if set(envelope) != {"schemaVersion", "requestId", "traceId", "error"}:
            raise ValueError
        if envelope["schemaVersion"] != "1.0":
            raise ValueError
        _canonical_uuid(envelope["requestId"], "requestId")
        _canonical_uuid(envelope["traceId"], "traceId")
        error = envelope["error"]
        if not isinstance(error, Mapping):
            raise ValueError
        code = error.get("code")
        message = error.get("message")
        retryable = error.get("retryable")
        execution_state = error.get("executionState")
        if (
            not isinstance(code, str)
            or not _SAFE_ERROR_CODE.fullmatch(code)
            or not isinstance(message, str)
            or not 1 <= len(message) <= 1_000
            or not isinstance(retryable, bool)
            or execution_state != "NOT_STARTED"
        ):
            raise ValueError

        expected_code = _PERMANENT_ERRORS.get(status)
        expected_retryable = False
        if 500 <= status <= 599:
            expected_code = "INTERNAL_TRANSIENT_ERROR"
            expected_retryable = True
        if expected_code is None or code != expected_code or retryable != expected_retryable:
            raise ValueError

        expected_fields = {"code", "message", "retryable", "executionState"}
        retry_after = None
        if retryable:
            expected_fields.add("retryAfterMs")
            retry_after = error.get("retryAfterMs")
            if (
                isinstance(retry_after, bool)
                or not isinstance(retry_after, int)
                or not 1 <= retry_after <= 3_600_000
            ):
                raise ValueError
        if set(error) != expected_fields:
            raise ValueError
        return ProfileVersionClientError(
            code,
            "Spring Profile Version request was rejected.",
            retryable=retryable,
            status=status,
            retry_after_ms=retry_after,
        )
    except (
        ValueError,
        TypeError,
        RecursionError,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ):
        if 500 <= status <= 599:
            return ProfileVersionClientError(
                "INTERNAL_TRANSIENT_ERROR",
                "Spring Profile Version API is unavailable.",
                retryable=True,
                status=status,
            )
        return _invalid_response(status=status)


def _invalid_response(*, status: int | None = None) -> ProfileVersionClientError:
    return ProfileVersionClientError(
        "PROFILE_VERSION_RESPONSE_INVALID",
        "Spring Profile Version API returned an invalid response.",
        retryable=False,
        status=status,
    )


def _decode_exact_json(raw: bytes) -> Mapping[str, Any]:
    def exact_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    value = json.loads(
        raw,
        object_pairs_hook=exact_object,
        parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
    )
    if not isinstance(value, Mapping):
        raise ValueError("error envelope must be an object")
    return value


def _canonical_uuid(value: Any, field_name: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError):
        raise ValueError(f"{field_name} is invalid") from None
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
