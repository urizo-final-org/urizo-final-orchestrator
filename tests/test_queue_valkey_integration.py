from __future__ import annotations

import json
import os
import unittest

from redis import Redis

from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.queue import (
    DEAD_LETTER_QUEUE_SUFFIX,
    DELIVERY_ATTEMPTS_SUFFIX,
    FAILURE_HISTORY_SUFFIX,
    MAX_DELIVERY_ATTEMPTS,
    PROCESSING_QUEUE_KEY,
    QUEUE_KEY,
    ValkeyJobQueue,
)


VALKEY_PORT = os.environ.get("AXMS_TEST_VALKEY_PORT")


@unittest.skipUnless(VALKEY_PORT, "AXMS_TEST_VALKEY_PORT is not configured")
class ValkeyJobQueueIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        assert VALKEY_PORT is not None
        self.client = Redis(
            host="127.0.0.1",
            port=int(VALKEY_PORT),
            db=0,
            decode_responses=False,
        )
        self.keys = (
            QUEUE_KEY,
            PROCESSING_QUEUE_KEY,
            f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}",
            f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}",
            f"{QUEUE_KEY}{FAILURE_HISTORY_SUFFIX}",
        )
        self.client.delete(*self.keys)
        self.queue = ValkeyJobQueue(
            "127.0.0.1",
            int(VALKEY_PORT),
            0,
        ).open()

    def tearDown(self) -> None:
        self.queue.close()
        self.client.delete(*self.keys)
        self.client.close()

    def test_fair_retry_then_third_failure_moves_job_to_dlq(self) -> None:
        first = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        second = QueuedJobReference.from_dict(
            {"jobId": "21212121-2121-4121-8121-212121212121"}
        )
        self.client.rpush(QUEUE_KEY, second.to_json(), first.to_json())

        failed = self.queue.pop(1)
        self.queue.requeue(failed)
        next_delivery = self.queue.pop(1)
        self.queue.ack(next_delivery)

        self.assertEqual(first.job_id, failed.job.job_id)
        self.assertEqual(second.job_id, next_delivery.job.job_id)

        attempts = [failed.attempt]
        for _ in range(1, MAX_DELIVERY_ATTEMPTS):
            failed = self.queue.pop(1)
            attempts.append(failed.attempt)
            self.queue.requeue(failed)

        history_raw = self.client.lindex(
            f"{QUEUE_KEY}{FAILURE_HISTORY_SUFFIX}",
            0,
        )
        assert isinstance(history_raw, bytes)
        history = json.loads(history_raw)

        self.assertEqual([1, 2, 3], attempts)
        self.assertEqual([], self.client.lrange(QUEUE_KEY, 0, -1))
        self.assertEqual([], self.client.lrange(PROCESSING_QUEUE_KEY, 0, -1))
        self.assertEqual(
            [first.to_json()],
            self.client.lrange(
                f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}",
                0,
                -1,
            ),
        )
        self.assertEqual(first.job_id, history["jobId"])
        self.assertEqual(
            "MAX_DELIVERY_ATTEMPTS_EXCEEDED",
            history["failureCode"],
        )
        self.assertEqual(MAX_DELIVERY_ATTEMPTS, history["deliveryAttempts"])
        self.assertEqual(
            {},
            self.client.hgetall(f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}"),
        )

    def test_recovery_quarantines_a_processing_job_at_the_delivery_limit(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "22222222-2222-4222-8222-222222222222"}
        )
        self.client.rpush(PROCESSING_QUEUE_KEY, job.to_json())
        self.client.hset(
            f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}",
            job.job_id,
            MAX_DELIVERY_ATTEMPTS,
        )

        recovered = self.queue.recover_stale()

        self.assertEqual(0, recovered)
        self.assertEqual([], self.client.lrange(QUEUE_KEY, 0, -1))
        self.assertEqual([], self.client.lrange(PROCESSING_QUEUE_KEY, 0, -1))
        self.assertEqual(
            [job.to_json()],
            self.client.lrange(
                f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}",
                0,
                -1,
            ),
        )


if __name__ == "__main__":
    unittest.main()
