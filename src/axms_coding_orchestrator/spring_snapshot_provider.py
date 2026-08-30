"""Spring-owned Job binding to immutable Profile Version Snapshot execution."""

from __future__ import annotations

from typing import Protocol

from .contracts import CodingJobRequested
from .graph import GraphExecutionError
from .profile_version_client import ProfileVersionClientError
from .snapshot import VersionedSnapshot
from .snapshot_runner import SnapshotExecution


class _ProfileVersionReader(Protocol):
    def get(self, profile_version_id: str) -> VersionedSnapshot: ...


class SpringSnapshotExecutionProvider:
    """Resolve one profile-bound Spring Job into an executable Snapshot."""

    __slots__ = ("_client",)

    def __init__(self, client: _ProfileVersionReader) -> None:
        if not callable(getattr(client, "get", None)):
            raise TypeError("client must implement get(profileVersionId)")
        self._client = client

    def resolve(self, event: CodingJobRequested) -> SnapshotExecution:
        profile_version_id = event.profile_version_id
        pipeline_attempt = event.pipeline_attempt
        execution_attempt = event.execution_attempt
        if (
            profile_version_id is None
            or pipeline_attempt is None
            or execution_attempt is None
        ):
            raise _contract_failure("Spring Job has no Snapshot execution binding.")
        try:
            snapshot = self._client.get(profile_version_id)
        except ProfileVersionClientError as failure:
            raise _provider_failure(failure) from None
        if not isinstance(snapshot, VersionedSnapshot):
            raise _contract_failure("Spring Profile Version client returned an invalid Snapshot.")
        try:
            return SnapshotExecution.create(
                snapshot,
                pipeline_attempt=pipeline_attempt,
                execution_attempt=execution_attempt,
                context=event.job_payload,
                workspace_id=event.workspace_id,
                tool_call_id=event.tool_call_id,
            )
        except (TypeError, ValueError):
            raise _contract_failure("Snapshot execution binding is invalid.") from None


def _provider_failure(failure: ProfileVersionClientError) -> GraphExecutionError:
    if failure.retryable:
        return GraphExecutionError(
            "INTERNAL_TRANSIENT_ERROR",
            "Spring Profile Version API is unavailable.",
            retryable=True,
        )
    if failure.code in {
        "SERVICE_AUTHENTICATION_FAILED",
        "SERVICE_AUTHORIZATION_DENIED",
    }:
        return GraphExecutionError(
            failure.code,
            "Spring Profile Version authorization failed.",
            retryable=False,
        )
    return _contract_failure("Spring Profile Version could not be resolved.")


def _contract_failure(message: str) -> GraphExecutionError:
    return GraphExecutionError(
        "CONTRACT_VALIDATION_FAILED",
        message,
        retryable=False,
    )
