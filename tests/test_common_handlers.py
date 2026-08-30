from __future__ import annotations

import unittest
from unittest.mock import patch

from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.graph_builder import SnapshotGraphExecutionError
from axms_coding_orchestrator.node_runtime import NodeInvocation


JOB_ID = "20202020-2020-4020-8020-202020202020"
PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
CONTEXT_DIGEST = "sha256:" + ("2" * 64)
POLICY_HASH = "sha256:" + ("3" * 64)


def _invocation(
    node_id: str,
    config: dict[str, object],
    context: dict[str, object] | None = None,
) -> NodeInvocation:
    return NodeInvocation.create(
        job_id=JOB_ID,
        profile_version_id=PROFILE_VERSION_ID,
        node_id=node_id,
        pipeline_attempt=1,
        execution_attempt=1,
        state_version=5,
        trace_id=TRACE_ID,
        workspace_id=None,
        tool_call_id=None,
        context={} if context is None else context,
        config=config,
    )


class CommonNodeHandlersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = build_common_node_registry()

    def test_registry_contains_only_the_fixed_common_contracts(self) -> None:
        expected = {
            "common.start": ({"start"}, {"next"}),
            "common.guardrail": ({"guardrail"}, {"passed", "failed"}),
            "common.check": ({"check"}, {"passed", "failed"}),
            "common.approval": ({"approval"}, {"approved"}),
            "common.end": ({"end"}, set()),
        }

        self.assertEqual(tuple(sorted(expected)), self.registry.registered_keys)
        for key, (node_types, result_ports) in expected.items():
            with self.subTest(handler_key=key):
                registration = self.registry.resolve(key)
                self.assertEqual(frozenset(node_types), registration.node_types)
                self.assertEqual(frozenset(result_ports), registration.result_ports)

    def test_structural_handlers_match_spring_digests_and_fail_closed(self) -> None:
        start = self.registry.resolve("common.start").handler
        guardrail = self.registry.resolve("common.guardrail").handler
        check = self.registry.resolve("common.check").handler
        end = self.registry.resolve("common.end").handler

        self.assertEqual("next", start(_invocation("start", {})).port)
        self.assertEqual(
            "passed",
            guardrail(
                _invocation(
                    "guardrail",
                    {"locked": True},
                    {"policyHash": POLICY_HASH},
                )
            ).port,
        )
        self.assertEqual(
            "failed",
            guardrail(
                _invocation(
                    "guardrail",
                    {"locked": True},
                    {"policyHash": "not-a-digest"},
                )
            ).port,
        )
        self.assertEqual(
            "passed",
            check(
                _invocation(
                    "check",
                    {},
                    {"contextDigest": CONTEXT_DIGEST},
                )
            ).port,
        )
        self.assertEqual(
            "failed",
            check(
                _invocation(
                    "check",
                    {},
                    {},
                )
            ).port,
        )
        self.assertIsNone(end(_invocation("end", {})).port)

        malformed = (
            (guardrail, "guardrail", {}),
            (
                guardrail,
                "guardrail",
                {"locked": True, "unknown": True},
            ),
            (check, "check", {"unknown": True}),
        )
        for handler, node_id, config in malformed:
            with self.subTest(node_id=node_id, config=config), self.assertRaises(
                SnapshotGraphExecutionError
            ):
                handler(_invocation(node_id, config))

    def test_non_policy_common_nodes_reject_unknown_config(self) -> None:
        for key in ("common.start", "common.approval", "common.end"):
            with self.subTest(handler_key=key), self.assertRaises(
                SnapshotGraphExecutionError
            ):
                self.registry.resolve(key).handler(
                    _invocation(key.removeprefix("common."), {"unknown": True})
                )

    def test_approval_accepts_only_the_true_interrupt_decision(
        self,
    ) -> None:
        approval = self.registry.resolve("common.approval").handler
        invocation = _invocation("approval", {})

        with patch(
            "axms_coding_orchestrator.common_handlers.interrupt",
            return_value=True,
        ):
            self.assertEqual("approved", approval(invocation).port)
        with patch(
            "axms_coding_orchestrator.common_handlers.interrupt",
            return_value=False,
        ), self.assertRaises(SnapshotGraphExecutionError):
            approval(invocation)
        with patch(
            "axms_coding_orchestrator.common_handlers.interrupt",
            return_value="approved",
        ), self.assertRaises(SnapshotGraphExecutionError):
            approval(invocation)


if __name__ == "__main__":
    unittest.main()
