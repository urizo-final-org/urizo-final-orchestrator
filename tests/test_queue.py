from __future__ import annotations

import unittest

from axms_coding_orchestrator.contracts import QueuedJobReference
from axms_coding_orchestrator.queue import (
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
        queue.ack(delivery)
        self.assertEqual([], client.lists[PROCESSING_QUEUE_KEY])
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


if __name__ == "__main__":
    unittest.main()
