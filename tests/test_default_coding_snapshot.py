from __future__ import annotations

from pathlib import Path
import unittest

from axms_coding_orchestrator.coding_handlers import (
    CODING_HANDLER_CONTRACTS,
    CodingHandlerDependencies,
    PreparedResultCodingStageExecutor,
    register_coding_node_handlers,
)
from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.default_coding_snapshot import (
    CODING_TOOL_NAMES,
    DEFAULT_CODING_PROFILE_VERSION,
    DEFAULT_CODING_PROFILE_VERSION_ID,
    default_coding_snapshot,
    default_coding_snapshot_dict,
)
from axms_coding_orchestrator.graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
)
from axms_coding_orchestrator.snapshot import VersionedSnapshot, load_snapshot_json


FIXTURE = Path(__file__).parent / "fixtures" / "llm-ops-coding-handler.snapshot.valid.json"


class _Domain:
    def get_attempt(self, invocation: object) -> object:
        del invocation
        raise AssertionError("fixture compilation must not execute handlers")

    def put_result(self, invocation: object, result: object) -> object:
        del invocation, result
        raise AssertionError("fixture compilation must not execute handlers")


class DefaultCodingSnapshotTest(unittest.TestCase):
    def test_source_builder_and_backend_seed_fixture_are_semantically_identical(self) -> None:
        fixture = load_snapshot_json(FIXTURE.read_bytes())

        self.assertEqual(DEFAULT_CODING_PROFILE_VERSION_ID, fixture.profile_version_id)
        self.assertEqual(DEFAULT_CODING_PROFILE_VERSION, fixture.profile_version)
        self.assertEqual(default_coding_snapshot_dict(), fixture.to_dict())
        self.assertEqual(default_coding_snapshot().to_json(), fixture.to_json())

    def test_fixture_has_only_the_approved_handlers_models_tools_and_bounds(self) -> None:
        snapshot = default_coding_snapshot()
        feature_nodes = {
            node.handler_key: frozenset(node.result_ports)
            for node in snapshot.nodes
            if node.handler_key.startswith("coding.")
        }
        for handler_key, (_, ports) in CODING_HANDLER_CONTRACTS.items():
            self.assertEqual(ports, feature_nodes[handler_key])

        self.assertEqual({"analyze", "code", "review"}, set(snapshot.model_bindings))
        self.assertEqual(list(CODING_TOOL_NAMES), snapshot.tool_policy["allowedTools"])
        limits = {limit.route(): limit.max_iterations for limit in snapshot.config.loop_limits}
        self.assertEqual(2, limits[("rework_gate", "retry", "code")])
        self.assertEqual(2, limits[("preview_approval", "rejected", "analyze")])

    def test_fixture_compiles_against_common_plus_coding_registry(self) -> None:
        registry = register_coding_node_handlers(
            build_common_node_registry(),
            CodingHandlerDependencies(_Domain(), PreparedResultCodingStageExecutor()),
        )

        graph = SnapshotGraphBuilder(registry).compile(default_coding_snapshot())

        self.assertIsNotNone(graph)
        self.assertEqual(
            tuple(sorted({
                "common.start",
                "common.guardrail",
                "common.check",
                "common.approval",
                "common.end",
                *CODING_HANDLER_CONTRACTS,
            })),
            registry.registered_keys,
        )

    def test_feature_handler_configs_are_validated_before_execution(self) -> None:
        dependencies = CodingHandlerDependencies(
            _Domain(), PreparedResultCodingStageExecutor()
        )
        invalid_configs = {
            "analyze": {"unknown": True},
            "deploy_request": {"mode": "execute"},
            "scope_approval": {"stage": "CANDIDATE", "requiredRole": "GENERAL_ADMIN"},
            "rework_gate": {"maxReworkRounds": 0},
        }

        for node_id, config in invalid_configs.items():
            payload = default_coding_snapshot_dict()
            node = next(item for item in payload["nodes"] if item["id"] == node_id)
            node["config"] = config
            snapshot = VersionedSnapshot.from_dict(payload)
            registry = register_coding_node_handlers(
                build_common_node_registry(), dependencies
            )

            with self.subTest(node_id=node_id), self.assertRaisesRegex(
                SnapshotGraphBuildError, "config"
            ):
                SnapshotGraphBuilder(registry).compile(snapshot)


if __name__ == "__main__":
    unittest.main()
