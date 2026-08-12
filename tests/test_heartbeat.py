from __future__ import annotations

import threading
import time
import unittest

from axms_coding_orchestrator.contracts import CodingJobRequested, WorkerClaim
from axms_coding_orchestrator.heartbeat import LeaseHeartbeatManager, LeaseLostError
from axms_coding_orchestrator.worker_api import WorkerApiError

from factories import FIXED_NOW, coding_event, worker_claim


class _LeaseRejectingWorker:
    def __init__(self) -> None:
        self.called = threading.Event()

    def heartbeat(self, claim, idempotency_key):
        self.called.set()
        raise WorkerApiError(
            "JOB_STATE_VERSION_CONFLICT",
            "safe test rejection",
            retryable=False,
            status=409,
        )


class LeaseHeartbeatManagerTest(unittest.TestCase):
    def test_nonretryable_heartbeat_rejection_marks_lease_lost(self) -> None:
        event = CodingJobRequested.from_dict(coding_event())
        claim = WorkerClaim.from_dict(
            worker_claim(event.to_dict()), event, now=FIXED_NOW
        )
        worker = _LeaseRejectingWorker()
        manager = LeaseHeartbeatManager(worker, 0.01, now=lambda: FIXED_NOW)
        try:
            manager.start(claim)
            self.assertTrue(worker.called.wait(1.0))
            time.sleep(0.02)

            with self.assertRaises(LeaseLostError) as raised:
                manager.ensure_current(claim)
            self.assertEqual("JOB_STATE_VERSION_CONFLICT", raised.exception.code)
        finally:
            manager.close()


if __name__ == "__main__":
    unittest.main()
