"""Fail-open, payload-free node occurrence reporting to Spring."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
import socket
from typing import Any, Protocol
from urllib.parse import urlsplit

from .model_gateway import (
    ContractViolation,
    CredentialResolver,
    ModelGatewayRemoteError,
    SPRING_PRIVATE_ORIGIN,
    _request_http,
)
from .node_runtime import NodeInvocation
from .snapshot import SnapshotNode


MONITORING_PATH = "/internal/ai/monitoring/node-occurrences"
_STATUSES = frozenset({"RUNNING", "WAITING_APPROVAL", "COMPLETED", "FAILED"})
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{2,119}$")
_OBSERVATION_TRACE_ID = re.compile(r"^[0-9a-f]{32}$")


class NodeMonitoringReporter(Protocol):
    def report(
        self,
        *,
        node: SnapshotNode,
        invocation: NodeInvocation,
        node_sequence: int,
        status: str,
        observation_trace_id: str | None,
        error_code: str | None = None,
    ) -> bool: ...


class SpringNodeMonitoringReporter:
    """Send one bounded report; every transport or response failure stays fail-open."""

    __slots__ = ("_origin", "_credential_resolver", "_timeout_seconds")

    def __init__(
        self,
        spring_origin: str,
        credential_resolver: CredentialResolver,
        *,
        timeout_seconds: float = 1.0,
        allowed_origins: set[str] | frozenset[str] | None = None,
    ) -> None:
        origins = frozenset(
            {SPRING_PRIVATE_ORIGIN} if allowed_origins is None else allowed_origins
        )
        if spring_origin not in origins or not _canonical_origin(spring_origin):
            raise ValueError("Spring monitoring origin is not allowlisted")
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

    def report(
        self,
        *,
        node: SnapshotNode,
        invocation: NodeInvocation,
        node_sequence: int,
        status: str,
        observation_trace_id: str | None,
        error_code: str | None = None,
    ) -> bool:
        try:
            payload = _payload(
                node=node,
                invocation=invocation,
                node_sequence=node_sequence,
                status=status,
                observation_trace_id=observation_trace_id,
                error_code=error_code,
            )
        except (TypeError, ValueError):
            return False

        credential = bytearray()
        try:
            try:
                with self._credential_resolver() as lease:
                    credential = lease.copy()
                response_status, _raw = _request_http(
                    "POST",
                    self._origin + MONITORING_PATH,
                    payload,
                    credential,
                    self._timeout_seconds,
                )
                return response_status == 200
            except (
                ContractViolation,
                ModelGatewayRemoteError,
                TimeoutError,
                socket.timeout,
                OSError,
            ):
                return False
        finally:
            for index in range(len(credential)):
                credential[index] = 0


def _payload(
    *,
    node: SnapshotNode,
    invocation: NodeInvocation,
    node_sequence: int,
    status: str,
    observation_trace_id: str | None,
    error_code: str | None,
) -> bytes:
    if not isinstance(node, SnapshotNode) or not isinstance(invocation, NodeInvocation):
        raise TypeError("monitoring identity is invalid")
    if isinstance(node_sequence, bool) or not isinstance(node_sequence, int) or node_sequence < 1:
        raise ValueError("node_sequence is invalid")
    if status not in _STATUSES:
        raise ValueError("status is invalid")
    if observation_trace_id is not None and not _OBSERVATION_TRACE_ID.fullmatch(
        observation_trace_id
    ):
        raise ValueError("observation_trace_id is invalid")
    if (status == "FAILED") != (
        isinstance(error_code, str) and _ERROR_CODE.fullmatch(error_code) is not None
    ):
        raise ValueError("error_code does not match status")
    body: dict[str, Any] = {
        "schemaVersion": "1.0",
        "jobId": invocation.job_id,
        "traceId": invocation.trace_id,
        "observationTraceId": observation_trace_id,
        "profileVersionId": invocation.profile_version_id,
        "pipelineAttempt": invocation.pipeline_attempt,
        "executionAttempt": invocation.execution_attempt,
        "nodeId": invocation.node_id,
        "nodeSequence": node_sequence,
        "nodeType": node.node_type,
        "handlerKey": node.handler_key,
        "status": status,
        "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "errorCode": error_code,
    }
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


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
