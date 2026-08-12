from __future__ import annotations

import json
import unittest

from axms_coding_orchestrator.service import HealthState


class HealthStateTest(unittest.TestCase):
    def test_readiness_rechecks_each_required_dependency(self) -> None:
        available = {"checkpoint": True, "queue": True, "spring": True}
        state = HealthState()
        state.bind_dependency_probes(
            checkpoint=lambda: available["checkpoint"],
            queue=lambda: available["queue"],
            spring=lambda: available["spring"],
        )
        state.update(worker=True)

        status, raw = state.response(True)
        self.assertEqual(200, status)
        self.assertEqual("UP", json.loads(raw)["status"])

        available["checkpoint"] = False
        status, raw = state.response(True)
        payload = json.loads(raw)
        self.assertEqual(503, status)
        self.assertEqual("DOWN", payload["checks"]["checkpoint"])
        self.assertEqual("UP", payload["checks"]["queue"])
        self.assertEqual("UP", payload["checks"]["spring"])

        live_status, _ = state.response(False)
        self.assertEqual(200, live_status)

    def test_probe_exception_fails_closed(self) -> None:
        def fail() -> bool:
            raise RuntimeError("dependency detail must not escape")

        state = HealthState()
        state.bind_dependency_probes(
            checkpoint=fail,
            queue=lambda: True,
            spring=lambda: True,
        )
        state.update(worker=True)

        status, raw = state.response(True)
        self.assertEqual(503, status)
        self.assertNotIn("dependency detail", raw.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
