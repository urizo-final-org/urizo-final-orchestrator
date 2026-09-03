from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError
import hashlib
import json
from pathlib import Path
import unittest

from axms_coding_orchestrator.snapshot import (
    SnapshotContractViolation,
    SnapshotNode,
    VersionedSnapshot,
    load_snapshot_json,
)


FIXTURE = Path(__file__).parent / "fixtures" / "versioned-snapshot.valid.json"


def valid_snapshot() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


class VersionedSnapshotLoaderTest(unittest.TestCase):
    def test_valid_fixture_round_trips_from_text_and_bytes(self) -> None:
        raw = FIXTURE.read_bytes()

        from_bytes = load_snapshot_json(raw)
        from_text = VersionedSnapshot.from_json(raw.decode("utf-8"))

        self.assertEqual(from_bytes, from_text)
        self.assertEqual(valid_snapshot(), from_bytes.to_dict())
        self.assertEqual(from_bytes, load_snapshot_json(from_bytes.to_json()))
        self.assertEqual("LLM_OPS", from_bytes.profile_key)
        self.assertEqual(("next",), from_bytes.nodes[0].result_ports)

    def test_nested_source_and_returned_values_are_defensive(self) -> None:
        source = valid_snapshot()
        snapshot = VersionedSnapshot.from_dict(source)

        source["nodes"][2]["config"]["promptKey"] = "changed"  # type: ignore[index]
        source["modelBindings"]["analyze"]["fallback"].append(  # type: ignore[index,union-attr]
            "changed"
        )
        returned = snapshot.to_dict()
        returned["nodes"][2]["config"]["promptKey"] = "changed"
        returned["toolPolicy"]["allowedTools"].append("changed")

        self.assertEqual("fixture.analysis", snapshot.nodes[2].config["promptKey"])
        self.assertEqual([], snapshot.model_bindings["analyze"]["fallback"])
        self.assertEqual([], snapshot.tool_policy["allowedTools"])
        with self.assertRaises(FrozenInstanceError):
            snapshot.profile_version = 2  # type: ignore[misc]

    def test_invalid_json_utf8_top_level_and_duplicate_keys_are_rejected(self) -> None:
        invalid_documents = (
            "{",
            b"\xff",
            "[]",
            "null",
            '{"contractVersion":"1.0","contractVersion":"2.0"}',
            '{"value":NaN}',
        )

        for raw in invalid_documents:
            with self.subTest(raw=repr(raw)), self.assertRaises(SnapshotContractViolation):
                load_snapshot_json(raw)

    def test_top_level_contract_is_exact_and_excludes_job_context(self) -> None:
        for field in (
            "jobId",
            "pipelineAttempt",
            "executionAttempt",
            "stateVersion",
            "workspaceId",
            "toolCallId",
            "traceId",
        ):
            payload = valid_snapshot()
            payload[field] = "forbidden"
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotContractViolation, "unknown fields"
            ):
                VersionedSnapshot.from_dict(payload)

        missing = valid_snapshot()
        del missing["toolPolicy"]
        with self.assertRaisesRegex(SnapshotContractViolation, "missing or unknown"):
            VersionedSnapshot.from_dict(missing)

    def test_nested_settings_exclude_job_execution_context(self) -> None:
        for field in (
            "jobId",
            "pipelineAttempt",
            "executionAttempt",
            "stateVersion",
            "workspaceId",
            "toolCallId",
            "traceId",
        ):
            payload = valid_snapshot()
            payload["nodes"][2]["config"] = {  # type: ignore[index]
                "nested": {field: "forbidden"}
            }
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotContractViolation, "execution context"
            ):
                VersionedSnapshot.from_dict(payload)

    def test_profile_identity_and_limits_are_strict(self) -> None:
        mutations = (
            ("contractVersion", "2.0"),
            ("profileVersionId", "not-a-uuid"),
            ("profileKey", "OTHER"),
            ("profileVersion", 0),
            ("profileVersion", True),
        )
        for field, value in mutations:
            payload = valid_snapshot()
            payload[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(
                SnapshotContractViolation
            ):
                VersionedSnapshot.from_dict(payload)

        payload = valid_snapshot()
        payload["config"]["maxNodes"] = 3  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "maxNodes"):
            VersionedSnapshot.from_dict(payload)

        for max_attempts in (1, 2, 4):
            payload = valid_snapshot()
            payload["config"]["maxAttempts"] = max_attempts  # type: ignore[index]
            with self.subTest(max_attempts=max_attempts), self.assertRaisesRegex(
                SnapshotContractViolation, "maxAttempts"
            ):
                VersionedSnapshot.from_dict(payload)

    def test_identifiers_and_json_safe_settings_fail_closed(self) -> None:
        payload = valid_snapshot()
        payload["nodes"][0]["handlerKey"] = "Invalid Handler"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "handlerKey"):
            VersionedSnapshot.from_dict(payload)

        payload = valid_snapshot()
        payload["nodes"][0]["resultPorts"] = ["not valid"]  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "resultPorts"):
            VersionedSnapshot.from_dict(payload)

        payload = valid_snapshot()
        payload["nodes"][2]["type"] = "plugin"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "node.type"):
            VersionedSnapshot.from_dict(payload)

        payload = valid_snapshot()
        payload["toolPolicy"] = {"opaquePolicy": {"not-json-safe"}}
        with self.assertRaisesRegex(SnapshotContractViolation, "JSON-safe"):
            VersionedSnapshot.from_dict(payload)

        safe = VersionedSnapshot.from_dict(deepcopy(valid_snapshot()))
        self.assertNotIn("fixture.analysis", repr(safe))

    def test_deep_json_is_rejected_with_the_contract_error(self) -> None:
        payload = valid_snapshot()
        nested: dict[str, object] = {}
        payload["nodes"][2]["config"] = nested  # type: ignore[index]
        for _ in range(70):
            child: dict[str, object] = {}
            nested["child"] = child
            nested = child

        with self.assertRaises(SnapshotContractViolation):
            VersionedSnapshot.from_dict(payload)

        deeply_nested_json = '{"value":' * 1100 + "null" + "}" * 1100
        with self.assertRaises(SnapshotContractViolation):
            load_snapshot_json(deeply_nested_json)

        huge_integer_json = FIXTURE.read_text(encoding="utf-8").replace(
            '"profileVersion": 1', '"profileVersion": ' + ("9" * 5000)
        )
        with self.assertRaises(SnapshotContractViolation):
            load_snapshot_json(huge_integer_json)

        surrogate_json = FIXTURE.read_text(encoding="utf-8").replace(
            '"fixture-model"', '"\\ud800"'
        )
        with self.assertRaises(SnapshotContractViolation):
            load_snapshot_json(surrogate_json)

        payload = valid_snapshot()
        payload["nodes"][2]["config"] = {"value": "\ud800"}  # type: ignore[index]
        with self.assertRaises(SnapshotContractViolation):
            VersionedSnapshot.from_dict(payload)

        payload = valid_snapshot()
        payload["nodes"][2]["config"] = {  # type: ignore[index]
            "do-not-leak-config-key": {"not-json-safe"}
        }
        with self.assertRaises(SnapshotContractViolation) as failure:
            VersionedSnapshot.from_dict(payload)
        self.assertNotIn("do-not-leak-config-key", str(failure.exception))

    def test_supported_natural_cms_profile_and_direct_constructor_boundary(self) -> None:
        payload = valid_snapshot()
        payload["profileKey"] = "NATURAL_CMS"
        payload["toolPolicy"] = {"allowedTools": ["resolve_cms_target"]}

        snapshot = VersionedSnapshot.from_dict(payload)

        self.assertEqual("NATURAL_CMS", snapshot.profile_key)
        self.assertFalse(hasattr(snapshot, "__dict__"))
        with self.assertRaises(TypeError):
            SnapshotNode(  # type: ignore[call-arg]
                "start", "start", "fixture.start", ("next",), {}
            )
        with self.assertRaises(TypeError):
            VersionedSnapshot()  # type: ignore[call-arg]

    def test_opaque_model_and_tool_settings_round_trip_without_new_inner_schema(self) -> None:
        payload = valid_snapshot()
        payload["modelBindings"] = {
            "analyze": {
                "providerPreference": {"tier": "quality"},
                "candidates": ["fixture-a", "fixture-b"],
            }
        }
        payload["toolPolicy"] = {
            "catalogRef": "fixture-catalog",
            "rules": [{"scope": "coding", "names": ["read_file"]}],
        }

        snapshot = VersionedSnapshot.from_dict(payload)

        self.assertEqual(payload["modelBindings"], snapshot.model_bindings)
        self.assertEqual(payload["toolPolicy"], snapshot.tool_policy)
        self.assertEqual(payload, load_snapshot_json(snapshot.to_json()).to_dict())

    def test_optional_tool_bindings_round_trip_immutably_without_semantic_revalidation(
        self,
    ) -> None:
        payload = valid_snapshot()
        payload["toolBindings"] = {
            "analyze": {
                "read_file": "MODEL_OPTIONAL",
                "run_check": "SYSTEM_REQUIRED",
            }
        }

        snapshot = VersionedSnapshot.from_dict(payload)
        payload["toolBindings"]["analyze"]["read_file"] = "changed"  # type: ignore[index]
        returned = snapshot.to_dict()
        returned["toolBindings"]["analyze"]["run_check"] = "changed"  # type: ignore[index]

        self.assertEqual(
            {"analyze": {"read_file": "MODEL_OPTIONAL", "run_check": "SYSTEM_REQUIRED"}},
            snapshot.tool_bindings,
        )
        self.assertEqual(snapshot, load_snapshot_json(snapshot.to_json()))
        changed = snapshot.to_dict()
        changed["toolBindings"]["analyze"]["read_file"] = "SYSTEM_REQUIRED"  # type: ignore[index]
        self.assertNotEqual(
            hashlib.sha256(snapshot.to_json()).hexdigest(),
            hashlib.sha256(VersionedSnapshot.from_dict(changed).to_json()).hexdigest(),
        )

        legacy = VersionedSnapshot.from_dict(valid_snapshot())
        self.assertIsNone(legacy.tool_bindings)
        self.assertNotIn("toolBindings", legacy.to_dict())

    def test_optional_model_selection_metadata_round_trips_with_stable_digest(self) -> None:
        metadata = {
            "provider": "OPENAI",
            "model": "gpt-5.6-terra",
            "inference": {
                "reasoningIntensity": "medium",
                "reasoningBudgetTokens": 2048,
            },
        }
        first_payload = valid_snapshot()
        first_payload["modelBindings"] = {"analyze": {"selections": metadata}}
        second_payload = valid_snapshot()
        second_payload["modelBindings"] = {
            "analyze": {
                "selections": {
                    "inference": {
                        "reasoningBudgetTokens": 2048,
                        "reasoningIntensity": "medium",
                    },
                    "model": "gpt-5.6-terra",
                    "provider": "OPENAI",
                }
            }
        }

        first = VersionedSnapshot.from_dict(first_payload)
        second = VersionedSnapshot.from_dict(second_payload)
        changed_payload = deepcopy(first_payload)
        changed_payload["modelBindings"]["analyze"]["selections"]["inference"][  # type: ignore[index]
            "reasoningBudgetTokens"
        ] = 4096
        changed = VersionedSnapshot.from_dict(changed_payload)

        self.assertEqual(first_payload["modelBindings"], first.model_bindings)
        self.assertEqual(first.to_json(), second.to_json())
        self.assertEqual(
            hashlib.sha256(first.to_json()).hexdigest(),
            hashlib.sha256(second.to_json()).hexdigest(),
        )
        self.assertNotEqual(first.to_json(), changed.to_json())
        self.assertNotEqual(
            hashlib.sha256(first.to_json()).hexdigest(),
            hashlib.sha256(changed.to_json()).hexdigest(),
        )

    def test_documented_node_types_are_accepted_structurally(self) -> None:
        for node_type in (
            "start",
            "agent",
            "tool",
            "approval",
            "check",
            "guardrail",
            "end",
        ):
            node = SnapshotNode.from_dict(
                {
                    "id": f"fixture-{node_type}",
                    "type": node_type,
                    "handlerKey": f"fixture.{node_type}",
                    "resultPorts": [],
                    "config": {"locked": True} if node_type == "guardrail" else {},
                }
            )
            with self.subTest(node_type=node_type):
                self.assertEqual(node_type, node.node_type)


class VersionedSnapshotValidatorTest(unittest.TestCase):
    def test_declared_limited_loop_is_accepted_but_unbounded_cycle_is_rejected(self) -> None:
        payload = valid_snapshot()
        payload["nodes"][2]["resultPorts"] = ["completed", "retry"]  # type: ignore[index]
        payload["edges"].append(  # type: ignore[union-attr]
            {"from": "analyze", "resultPort": "retry", "to": "guardrail"}
        )
        payload["config"]["loopLimits"] = [  # type: ignore[index]
            {
                "from": "analyze",
                "resultPort": "retry",
                "to": "guardrail",
                "maxIterations": 2,
            }
        ]

        snapshot = VersionedSnapshot.from_dict(payload)
        self.assertEqual(2, snapshot.config.loop_limits[0].max_iterations)

        payload["config"]["loopLimits"] = []  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "unbounded cycle"):
            VersionedSnapshot.from_dict(payload)

    def test_duplicate_ids_routes_and_unknown_references_are_rejected(self) -> None:
        duplicate_node = valid_snapshot()
        duplicate_node["nodes"][1]["id"] = "start"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "duplicate ids"):
            VersionedSnapshot.from_dict(duplicate_node)

        duplicate_route = valid_snapshot()
        duplicate_route["edges"].append(  # type: ignore[union-attr]
            {"from": "start", "resultPort": "next", "to": "analyze"}
        )
        with self.assertRaisesRegex(SnapshotContractViolation, "duplicate result routes"):
            VersionedSnapshot.from_dict(duplicate_route)

        unknown_node = valid_snapshot()
        unknown_node["edges"][0]["to"] = "missing"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "unknown node"):
            VersionedSnapshot.from_dict(unknown_node)

    def test_ports_and_edge_directions_are_total_and_strict(self) -> None:
        undeclared = valid_snapshot()
        undeclared["edges"][0]["resultPort"] = "other"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "undeclared result port"):
            VersionedSnapshot.from_dict(undeclared)

        dangling = valid_snapshot()
        dangling["nodes"][2]["resultPorts"].append("failed")  # type: ignore[index,union-attr]
        with self.assertRaisesRegex(SnapshotContractViolation, "exactly one edge"):
            VersionedSnapshot.from_dict(dangling)

        incoming_start = valid_snapshot()
        incoming_start["nodes"][2]["resultPorts"].append(  # type: ignore[index,union-attr]
            "restart"
        )
        incoming_start["edges"].append(  # type: ignore[union-attr]
            {"from": "analyze", "resultPort": "restart", "to": "start"}
        )
        with self.assertRaisesRegex(SnapshotContractViolation, "start node"):
            VersionedSnapshot.from_dict(incoming_start)

        outgoing_end = valid_snapshot()
        outgoing_end["nodes"][3]["resultPorts"] = ["again"]  # type: ignore[index]
        outgoing_end["edges"].append(  # type: ignore[union-attr]
            {"from": "end", "resultPort": "again", "to": "analyze"}
        )
        with self.assertRaisesRegex(SnapshotContractViolation, "end node"):
            VersionedSnapshot.from_dict(outgoing_end)

    def test_exact_start_end_and_locked_guardrail_are_required(self) -> None:
        no_start = valid_snapshot()
        no_start["nodes"][0]["type"] = "agent"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "exactly one start"):
            VersionedSnapshot.from_dict(no_start)

        second_end = valid_snapshot()
        second_end["nodes"][2]["type"] = "end"  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "exactly one end"):
            VersionedSnapshot.from_dict(second_end)

        unlocked = valid_snapshot()
        unlocked["nodes"][1]["config"]["locked"] = False  # type: ignore[index]
        with self.assertRaisesRegex(SnapshotContractViolation, "locked guardrail"):
            VersionedSnapshot.from_dict(unlocked)

        bypass = valid_snapshot()
        bypass["nodes"][0]["resultPorts"] = ["safe", "bypass"]  # type: ignore[index]
        bypass["edges"][0]["resultPort"] = "safe"  # type: ignore[index]
        bypass["edges"].append(  # type: ignore[union-attr]
            {"from": "start", "resultPort": "bypass", "to": "analyze"}
        )
        with self.assertRaisesRegex(SnapshotContractViolation, "bypass"):
            VersionedSnapshot.from_dict(bypass)

    def test_all_nodes_are_reachable_and_can_reach_end(self) -> None:
        unreachable = valid_snapshot()
        unreachable["nodes"].append(  # type: ignore[union-attr]
            {
                "id": "orphan",
                "type": "check",
                "handlerKey": "fixture.orphan",
                "resultPorts": ["passed"],
                "config": {},
            }
        )
        unreachable["edges"].append(  # type: ignore[union-attr]
            {"from": "orphan", "resultPort": "passed", "to": "end"}
        )
        with self.assertRaisesRegex(SnapshotContractViolation, "unreachable from start"):
            VersionedSnapshot.from_dict(unreachable)

        dead_end = valid_snapshot()
        dead_end["nodes"].append(  # type: ignore[union-attr]
            {
                "id": "dead",
                "type": "check",
                "handlerKey": "fixture.dead",
                "resultPorts": ["retry"],
                "config": {},
            }
        )
        dead_end["nodes"][2]["resultPorts"].append("failed")  # type: ignore[index,union-attr]
        dead_end["edges"].extend(  # type: ignore[union-attr]
            [
                {"from": "analyze", "resultPort": "failed", "to": "dead"},
                {"from": "dead", "resultPort": "retry", "to": "dead"},
            ]
        )
        dead_end["config"]["loopLimits"] = [  # type: ignore[index]
            {
                "from": "dead",
                "resultPort": "retry",
                "to": "dead",
                "maxIterations": 1,
            }
        ]
        with self.assertRaisesRegex(SnapshotContractViolation, "cannot reach end"):
            VersionedSnapshot.from_dict(dead_end)

    def test_loop_limits_and_model_bindings_must_reference_the_snapshot(self) -> None:
        unknown_loop = valid_snapshot()
        unknown_loop["config"]["loopLimits"] = [  # type: ignore[index]
            {
                "from": "analyze",
                "resultPort": "retry",
                "to": "guardrail",
                "maxIterations": 2,
            }
        ]
        with self.assertRaisesRegex(SnapshotContractViolation, "unknown edge"):
            VersionedSnapshot.from_dict(unknown_loop)

        not_a_loop = valid_snapshot()
        not_a_loop["config"]["loopLimits"] = [  # type: ignore[index]
            {
                "from": "start",
                "resultPort": "next",
                "to": "guardrail",
                "maxIterations": 2,
            }
        ]
        with self.assertRaisesRegex(SnapshotContractViolation, "repeating edge"):
            VersionedSnapshot.from_dict(not_a_loop)

        missing_binding = valid_snapshot()
        missing_binding["modelBindings"] = {}
        with self.assertRaisesRegex(SnapshotContractViolation, "all agent nodes"):
            VersionedSnapshot.from_dict(missing_binding)

        non_agent_binding = valid_snapshot()
        non_agent_binding["modelBindings"] = {
            "guardrail": {"primary": "fixture-model", "fallback": []},
            "analyze": {"primary": "fixture-model", "fallback": []},
        }
        with self.assertRaisesRegex(SnapshotContractViolation, "all agent nodes"):
            VersionedSnapshot.from_dict(non_agent_binding)

        non_object_binding = valid_snapshot()
        non_object_binding["modelBindings"] = {"analyze": "opaque"}
        with self.assertRaisesRegex(SnapshotContractViolation, "must be an object"):
            VersionedSnapshot.from_dict(non_object_binding)


if __name__ == "__main__":
    unittest.main()
