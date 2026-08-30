from __future__ import annotations

from copy import deepcopy
import unittest

from axms_coding_orchestrator.contracts import (
    ClaimSnapshot,
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
    WorkerContractViolation,
)

from factories import FIXED_NOW, coding_event, worker_claim


class CodingJobRequestedContractTest(unittest.TestCase):
    def test_valid_event_and_claim_are_strict_and_correlated(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(worker_claim(event.to_dict()), event, now=FIXED_NOW)

        self.assertEqual((event.job_id, 4), event.ledger_key())
        self.assertEqual(
            "11111111-1111-4111-8111-111111111111",
            event.profile_version_id,
        )
        self.assertEqual(1, event.pipeline_attempt)
        self.assertEqual(1, event.execution_attempt)
        self.assertEqual(5, claim.state_version)
        self.assertEqual(event.profile_version_id, claim.profile_version_id)
        self.assertFalse(claim.resume)
        self.assertNotIn("Inspect only", repr(claim.snapshot))

    def test_unknown_event_field_is_rejected(self) -> None:
        payload = coding_event()
        payload["provider"] = "OPENAI"

        with self.assertRaisesRegex(WorkerContractViolation, "unknown fields"):
            CodingJobRequested.from_dict(payload)

    def test_non_job_event_is_rejected_from_coding_queue(self) -> None:
        payload = coding_event()
        payload["eventType"] = "APPROVAL_RECORDED"

        with self.assertRaisesRegex(WorkerContractViolation, "unsupported"):
            CodingJobRequested.from_dict(payload)

    def test_queue_reference_contains_only_a_canonical_job_id(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )

        self.assertEqual({"jobId": job.job_id}, job.to_dict())
        for payload in (
            {"jobId": job.job_id, "profileVersionId": "hidden"},
            {"jobId": "not-a-uuid"},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(WorkerContractViolation):
                    QueuedJobReference.from_dict(payload)

    def test_claim_scope_mismatch_and_nonadvancing_version_are_rejected(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        mismatch = worker_claim(event.to_dict())
        mismatch["snapshot"]["repository"]["repositoryId"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        with self.assertRaisesRegex(WorkerContractViolation, "scope"):
            WorkerClaim.from_dict(mismatch, event, now=FIXED_NOW)

        stale = worker_claim(event.to_dict(), state_version=event.expected_state_version)
        with self.assertRaisesRegex(WorkerContractViolation, "advance"):
            WorkerClaim.from_dict(stale, event, now=FIXED_NOW)

        changed_profile = worker_claim(event.to_dict())
        changed_profile["profileVersionId"] = (
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
        with self.assertRaisesRegex(WorkerContractViolation, "profileVersionId"):
            WorkerClaim.from_dict(changed_profile, event, now=FIXED_NOW)

    def test_snapshot_rejects_repository_escape_path(self) -> None:
        payload = worker_claim(coding_event())["snapshot"]
        payload["toolPath"] = "../outside.txt"

        with self.assertRaisesRegex(WorkerContractViolation, "toolPath"):
            ClaimSnapshot.from_dict(payload)

    def test_contract_models_are_defensive_copies(self) -> None:
        source = coding_event()
        event = CodingJobRequested.from_dict(source)
        source["payload"]["graphStep"] = "changed"
        returned = event.to_dict()
        returned["payload"]["graphStep"] = "changed"

        self.assertEqual("inspect", event.job_payload["graphStep"])


if __name__ == "__main__":
    unittest.main()
