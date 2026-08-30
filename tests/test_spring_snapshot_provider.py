from __future__ import annotations

import json
from pathlib import Path
import unittest

from axms_coding_orchestrator.contracts import CodingJobRequested
from axms_coding_orchestrator.graph import GraphExecutionError
from axms_coding_orchestrator.profile_version_client import ProfileVersionClientError
from axms_coding_orchestrator.snapshot import VersionedSnapshot
from axms_coding_orchestrator.snapshot_runner import SnapshotExecution
from axms_coding_orchestrator.spring_snapshot_provider import (
    SpringSnapshotExecutionProvider,
)

from factories import coding_event


PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
WORKSPACE_ID = "15151515-1515-4515-8515-151515151515"
TOOL_CALL_ID = "16161616-1616-4616-8616-161616161616"
FIXTURE = Path(__file__).parent / "fixtures" / "versioned-snapshot.valid.json"


def _snapshot(*, version: int) -> VersionedSnapshot:
    payload = json.loads(FIXTURE.read_bytes())
    payload["profileVersion"] = version
    return VersionedSnapshot.from_dict(payload)


def _execution(snapshot: VersionedSnapshot) -> SnapshotExecution:
    return SnapshotExecution.create(
        snapshot,
        pipeline_attempt=2,
        execution_attempt=3,
        context={"approved": {"paths": ["src"]}},
        workspace_id=WORKSPACE_ID,
        tool_call_id=TOOL_CALL_ID,
    )


class _Bindings:
    def __init__(self, value: object) -> None:
        self.value = value
        self.events: list[CodingJobRequested] = []

    def resolve(self, event: CodingJobRequested) -> object:
        self.events.append(event)
        return self.value


class _Client:
    def __init__(self, value: object) -> None:
        self.value = value
        self.profile_ids: list[str] = []

    def get(self, profile_version_id: str) -> object:
        self.profile_ids.append(profile_version_id)
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


class SpringSnapshotExecutionProviderTest(unittest.TestCase):
    def test_replaces_only_snapshot_and_preserves_execution_binding(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        binding = _execution(_snapshot(version=1))
        remote = _snapshot(version=2)
        bindings = _Bindings(binding)
        client = _Client(remote)

        resolved = SpringSnapshotExecutionProvider(bindings, client).resolve(event)

        self.assertIs(remote, resolved.snapshot)
        self.assertEqual(2, resolved.pipeline_attempt)
        self.assertEqual(3, resolved.execution_attempt)
        self.assertEqual(WORKSPACE_ID, resolved.workspace_id)
        self.assertEqual(TOOL_CALL_ID, resolved.tool_call_id)
        self.assertEqual(binding.context, resolved.context)
        self.assertEqual([event], bindings.events)
        self.assertEqual([PROFILE_VERSION_ID], client.profile_ids)

    def test_client_failures_map_to_existing_worker_error_contract(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        binding = _execution(_snapshot(version=1))
        cases = (
            (
                ProfileVersionClientError(
                    "PROFILE_VERSION_NOT_FOUND",
                    "private",
                    retryable=False,
                    status=404,
                ),
                "CONTRACT_VALIDATION_FAILED",
                False,
            ),
            (
                ProfileVersionClientError(
                    "PROFILE_VERSION_NOT_ACTIVE",
                    "private",
                    retryable=False,
                    status=409,
                ),
                "CONTRACT_VALIDATION_FAILED",
                False,
            ),
            (
                ProfileVersionClientError(
                    "SERVICE_AUTHENTICATION_FAILED",
                    "private",
                    retryable=False,
                    status=401,
                ),
                "SERVICE_AUTHENTICATION_FAILED",
                False,
            ),
            (
                ProfileVersionClientError(
                    "INTERNAL_TRANSIENT_ERROR",
                    "private",
                    retryable=True,
                    status=503,
                    retry_after_ms=250,
                ),
                "INTERNAL_TRANSIENT_ERROR",
                True,
            ),
        )
        for source, code, retryable in cases:
            with self.subTest(source=source.code):
                provider = SpringSnapshotExecutionProvider(
                    _Bindings(binding),
                    _Client(source),
                )
                with self.assertRaises(GraphExecutionError) as raised:
                    provider.resolve(event)
                self.assertEqual(code, raised.exception.code)
                self.assertEqual(retryable, raised.exception.retryable)
                self.assertNotIn("private", str(raised.exception))

    def test_invalid_injected_binding_and_client_result_fail_closed(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        cases = (
            (_Bindings(object()), _Client(_snapshot(version=1))),
            (_Bindings(_execution(_snapshot(version=1))), _Client(object())),
        )
        for bindings, client in cases:
            with self.subTest(value=type(bindings.value).__name__):
                with self.assertRaises(GraphExecutionError) as raised:
                    SpringSnapshotExecutionProvider(bindings, client).resolve(event)
                self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
                self.assertFalse(raised.exception.retryable)


if __name__ == "__main__":
    unittest.main()
