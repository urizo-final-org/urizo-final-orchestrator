from __future__ import annotations

import json
import unittest

from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.queue import (
    DEAD_LETTER_QUEUE_SUFFIX,
    DELIVERY_ATTEMPTS_SUFFIX,
    FAILURE_HISTORY_SUFFIX,
    MAX_DELIVERY_ATTEMPTS,
    PROCESSING_QUEUE_KEY,
    QUEUE_KEY,
    QueueError,
    ValkeyJobQueue,
)

class _FakeRedis:
    def __init__(self, source=None, processing=None, *, healthy: bool = True) -> None:
        self.lists = {
            QUEUE_KEY: list(source or []),
            PROCESSING_QUEUE_KEY: list(processing or []),
        }
        self.hashes = {}
        self.is_healthy = healthy
        self.observed = None

    def ping(self):
        return self.is_healthy

    def blmove(self, first, second, timeout, src="LEFT", dest="RIGHT"):
        self.observed = (first, second, timeout, src, dest)
        return self.lmove(first, second, src=src, dest=dest)

    def lmove(self, first, second, src="LEFT", dest="RIGHT"):
        source = self.lists.setdefault(first, [])
        if not source:
            return None
        value = source.pop(0 if src == "LEFT" else -1)
        target = self.lists.setdefault(second, [])
        if dest == "LEFT":
            target.insert(0, value)
        else:
            target.append(value)
        return value

    def lrem(self, name, count, value):
        values = self.lists.setdefault(name, [])
        removed = 0
        index = 0
        while index < len(values) and removed < count:
            if values[index] == value:
                values.pop(index)
                removed += 1
            else:
                index += 1
        return removed

    def lindex(self, name, index):
        values = self.lists.setdefault(name, [])
        if not values:
            return None
        return values[index]

    def hincrby(self, name, key, amount):
        values = self.hashes.setdefault(name, {})
        values[key] = values.get(key, 0) + amount
        return values[key]

    def hdel(self, name, key):
        values = self.hashes.setdefault(name, {})
        return int(values.pop(key, None) is not None)

    def hget(self, name, key):
        return self.hashes.setdefault(name, {}).get(key)

    def rpush(self, name, value):
        values = self.lists.setdefault(name, [])
        values.append(value)
        return len(values)

    def eval(self, _script, numkeys, *args):
        keys = args[:numkeys]
        values = args[numkeys:]
        if numkeys == 2:
            processing_key, attempts_key = keys
            raw, job_id = values
            removed = self.lrem(processing_key, 1, raw)
            if removed == 1:
                self.hdel(attempts_key, job_id)
            return removed
        if numkeys == 4:
            processing_key, dlq_key, history_key, attempts_key = keys
            raw, failure, job_id = values
            removed = self.lrem(processing_key, 1, raw)
            if removed != 1:
                return 0
            self.rpush(dlq_key, raw)
            self.rpush(history_key, failure)
            self.hdel(attempts_key, job_id)
            return 1
        raise AssertionError("unexpected script")

    def close(self):
        pass


class ValkeyJobQueueTest(unittest.TestCase):
    def queue(self, client: _FakeRedis) -> ValkeyJobQueue:
        queue = ValkeyJobQueue("valkey", 6379, 0, password="test-only-password")
        queue._client = client
        return queue

    def test_blmove_then_ack_consumes_only_the_strict_job_reference(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        client = _FakeRedis([job.to_json()])
        queue = self.queue(client)

        delivery = queue.pop(5)

        self.assertEqual(job.job_id, delivery.job.job_id)
        self.assertEqual(
            (QUEUE_KEY, PROCESSING_QUEUE_KEY, 5, "RIGHT", "LEFT"),
            client.observed,
        )
        self.assertEqual([job.to_json()], client.lists[PROCESSING_QUEUE_KEY])
        self.assertEqual(1, delivery.attempt)
        queue.ack(delivery)
        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])
        self.assertEqual({}, client.hashes[f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}"])
        self.assertTrue(queue.healthy())
        self.assertNotIn("test-only-password", repr(queue))
        self.assertNotIn("profileVersionId", repr(delivery))

    def test_natural_cms_lane_consumes_the_same_strict_job_reference(self) -> None:
        natural_cms_queue_key = "axms:natural-cms:jobs:v1"
        natural_cms_processing_key = f"{natural_cms_queue_key}:processing"
        job = QueuedJobReference.from_dict(
            {"jobId": "23232323-2323-4232-8232-232323232323"}
        )
        client = _FakeRedis()
        client.lists[natural_cms_queue_key] = [job.to_json()]
        client.lists[natural_cms_processing_key] = []
        queue = ValkeyJobQueue(
            "valkey",
            6379,
            0,
            password="test-only-password",
            queue_key=natural_cms_queue_key,
        )
        queue._client = client

        delivery = queue.pop(5)

        self.assertEqual(job.job_id, delivery.job.job_id)
        self.assertEqual(
            (
                natural_cms_queue_key,
                natural_cms_processing_key,
                5,
                "RIGHT",
                "LEFT",
            ),
            client.observed,
        )
        self.assertEqual([job.to_json()], client.lists[natural_cms_processing_key])
        queue.ack(delivery)
        self.assertEqual([], client.lists[natural_cms_processing_key])

    def test_natural_cms_requeue_and_recovery_never_touch_coding_processing(self) -> None:
        natural_cms_queue_key = "axms:natural-cms:jobs:v1"
        natural_cms_processing_key = f"{natural_cms_queue_key}:processing"
        job = QueuedJobReference.from_dict(
            {"jobId": "24242424-2424-4242-8242-242424242424"}
        )
        coding_pending = b'{"jobId":"25252525-2525-4252-8252-252525252525"}'
        client = _FakeRedis(processing=[coding_pending])
        client.lists[natural_cms_queue_key] = [job.to_json()]
        client.lists[natural_cms_processing_key] = []
        queue = ValkeyJobQueue(
            "valkey",
            6379,
            0,
            queue_key=natural_cms_queue_key,
        )
        queue._client = client

        delivery = queue.pop(1)
        queue.requeue(delivery)
        delivery = queue.pop(1)
        self.assertEqual(1, queue.recover_stale())

        self.assertEqual([job.to_json()], client.lists[natural_cms_queue_key])
        self.assertEqual([], client.lists[natural_cms_processing_key])
        self.assertEqual([coding_pending], client.lists[PROCESSING_QUEUE_KEY])

    def test_unknown_queue_envelope_fails_closed(self) -> None:
        client = _FakeRedis([b'{"providerKey":"hidden"}'])
        queue = self.queue(client)

        with self.assertRaisesRegex(QueueError, "reference") as raised:
            queue.pop(1)

        self.assertNotIn("providerKey", str(raised.exception))
        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])

    def test_startup_recovery_preserves_oldest_first_delivery_order(self) -> None:
        oldest = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        ).to_json()
        newer = QueuedJobReference.from_dict(
            {"jobId": "21212121-2121-4121-8121-212121212121"}
        ).to_json()
        new_source = QueuedJobReference.from_dict(
            {"jobId": "22222222-2222-4222-8222-222222222222"}
        ).to_json()
        client = _FakeRedis(
            source=[new_source],
            processing=[newer, oldest],
        )
        queue = self.queue(client)

        self.assertEqual(2, queue.recover_stale())
        recovered = [queue.pop(1), queue.pop(1), queue.pop(1)]

        self.assertEqual(
            [oldest, newer, new_source],
            [delivery._raw for delivery in recovered],
        )

    def test_startup_recovery_quarantines_an_exhausted_delivery(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        client = _FakeRedis(processing=[job.to_json()])
        attempts_key = f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}"
        client.hashes[attempts_key] = {
            job.job_id: MAX_DELIVERY_ATTEMPTS,
        }
        queue = self.queue(client)

        recovered = queue.recover_stale()

        self.assertEqual(0, recovered)
        self.assertEqual([], client.lists[QUEUE_KEY])
        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])
        self.assertEqual(
            [job.to_json()],
            client.lists[f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}"],
        )

    def test_failed_delivery_can_be_atomically_requeued(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        client = _FakeRedis([job.to_json()])
        queue = self.queue(client)
        delivery = queue.pop(1)

        queue.requeue(delivery)

        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])
        self.assertEqual([job.to_json()], client.lists[QUEUE_KEY])

    def test_failed_delivery_yields_to_the_next_waiting_job(self) -> None:
        first = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        second = QueuedJobReference.from_dict(
            {"jobId": "21212121-2121-4121-8121-212121212121"}
        )
        client = _FakeRedis([second.to_json(), first.to_json()])
        queue = self.queue(client)

        failed = queue.pop(1)
        queue.requeue(failed)
        next_delivery = queue.pop(1)

        self.assertEqual(first.job_id, failed.job.job_id)
        self.assertEqual(second.job_id, next_delivery.job.job_id)
        self.assertEqual([first.to_json()], client.lists[QUEUE_KEY])

    def test_third_failed_delivery_is_quarantined_with_common_history(self) -> None:
        job = QueuedJobReference.from_dict(
            {"jobId": "20202020-2020-4020-8020-202020202020"}
        )
        client = _FakeRedis([job.to_json()])
        queue = self.queue(client)

        attempts = []
        for _ in range(MAX_DELIVERY_ATTEMPTS):
            delivery = queue.pop(1)
            attempts.append(delivery.attempt)
            queue.requeue(delivery)

        dlq_key = f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}"
        history_key = f"{QUEUE_KEY}{FAILURE_HISTORY_SUFFIX}"
        attempts_key = f"{QUEUE_KEY}{DELIVERY_ATTEMPTS_SUFFIX}"
        history = json.loads(client.lists[history_key][0])

        self.assertEqual([1, 2, 3], attempts)
        self.assertEqual([], client.lists[QUEUE_KEY])
        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])
        self.assertEqual([job.to_json()], client.lists[dlq_key])
        self.assertEqual("1.0", history["schemaVersion"])
        self.assertEqual(job.job_id, history["jobId"])
        self.assertEqual(
            "MAX_DELIVERY_ATTEMPTS_EXCEEDED",
            history["failureCode"],
        )
        self.assertEqual(MAX_DELIVERY_ATTEMPTS, history["deliveryAttempts"])
        self.assertRegex(history["failedAt"], r"^\d{4}-\d{2}-\d{2}T.*Z$")
        self.assertEqual({}, client.hashes[attempts_key])

    def test_natural_cms_quarantine_is_isolated_from_coding_lane(self) -> None:
        natural_queue_key = "axms:natural-cms:jobs:v1"
        job = QueuedJobReference.from_dict(
            {"jobId": "24242424-2424-4242-8242-242424242424"}
        )
        client = _FakeRedis()
        client.lists[natural_queue_key] = [job.to_json()]
        client.lists[f"{natural_queue_key}:processing"] = []
        queue = ValkeyJobQueue("valkey", 6379, 0, queue_key=natural_queue_key)
        queue._client = client

        for _ in range(MAX_DELIVERY_ATTEMPTS):
            delivery = queue.pop(1)
            queue.requeue(delivery)

        self.assertEqual(
            [job.to_json()],
            client.lists[f"{natural_queue_key}{DEAD_LETTER_QUEUE_SUFFIX}"],
        )
        self.assertNotIn(f"{QUEUE_KEY}{DEAD_LETTER_QUEUE_SUFFIX}", client.lists)


if __name__ == "__main__":
    unittest.main()
