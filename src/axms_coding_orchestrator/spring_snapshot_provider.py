"""Non-production adapter from an injected execution binding to Spring JSON."""

from __future__ import annotations

from typing import Protocol

from .contracts import CodingJobRequested
from .graph import GraphExecutionError
from .profile_version_client import ProfileVersionClientError
from .snapshot import VersionedSnapshot
from .snapshot_runner import SnapshotExecution, SnapshotExecutionProvider


class _ProfileVersionReader(Protocol):
    def get(self, profile_version_id: str) -> VersionedSnapshot: ...


class SpringSnapshotExecutionProvider:
    """Replace only an injected execution's Snapshot with Spring's immutable JSON."""

    __slots__ = ("_bindings", "_client")

    def __init__(
        self,
        bindings: SnapshotExecutionProvider,
        client: _ProfileVersionReader,
    ) -> None:
        if not callable(getattr(bindings, "resolve", None)):
            raise TypeError("bindings must implement resolve(event)")
        if not callable(getattr(client, "get", None)):
            raise TypeError("client must implement get(profileVersionId)")
        self._bindings = bindings
        self._client = client

    def resolve(self, event: CodingJobRequested) -> SnapshotExecution:
        binding = self._bindings.resolve(event)
        if not isinstance(binding, SnapshotExecution):
            raise _contract_failure("Snapshot execution binding is invalid.")
        try:
            snapshot = self._client.get(binding.snapshot.profile_version_id)
        except ProfileVersionClientError as failure:
            raise _provider_failure(failure) from None
        if not isinstance(snapshot, VersionedSnapshot):
            raise _contract_failure("Spring Profile Version client returned an invalid Snapshot.")
        try:
            return SnapshotExecution.create(
                snapshot,
                pipeline_attempt=binding.pipeline_attempt,
                execution_attempt=binding.execution_attempt,
                context=binding.context,
                workspace_id=binding.workspace_id,
                tool_call_id=binding.tool_call_id,
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
