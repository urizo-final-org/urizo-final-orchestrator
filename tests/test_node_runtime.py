from __future__ import annotations

from dataclasses import FrozenInstanceError
import unittest

from axms_coding_orchestrator.node_runtime import (
    NodeContractViolation,
    NodeInvocation,
    NodeRegistry,
    NodeRegistryViolation,
    NodeResult,
)


JOB_ID = "20202020-2020-4020-8020-202020202020"
PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
TOOL_CALL_ID = "50505050-5050-4050-8050-505050505050"


class NodeContractTest(unittest.TestCase):
    def test_invocation_is_deeply_immutable_and_redacted(self) -> None:
        context = {"nested": {"items": ["original"]}}
        config = {"promptRef": "fixture.prompt"}
        invocation = NodeInvocation.create(
            job_id=JOB_ID,
            profile_version_id=PROFILE_VERSION_ID,
            node_id="analyze",
            pipeline_attempt=2,
            execution_attempt=3,
            state_version=4,
            trace_id=TRACE_ID,
            workspace_id=WORKSPACE_ID,
            tool_call_id=TOOL_CALL_ID,
            context=context,
            config=config,
        )

        context["nested"]["items"].append("changed")  # type: ignore[index,union-attr]
        config["promptRef"] = "changed"
        returned = invocation.context
        returned["nested"]["items"].append("changed")

        self.assertEqual(["original"], invocation.context["nested"]["items"])
        self.assertEqual("fixture.prompt", invocation.config["promptRef"])
        self.assertEqual(4, invocation.state_version)
        self.assertEqual(WORKSPACE_ID, invocation.workspace_id)
        self.assertEqual(TOOL_CALL_ID, invocation.tool_call_id)
        self.assertFalse(hasattr(invocation, "__dict__"))
        self.assertNotIn("fixture.prompt", repr(invocation))
        with self.assertRaises(FrozenInstanceError):
            invocation.node_id = "changed"  # type: ignore[misc]

    def test_result_is_deeply_immutable_and_supports_terminal_nodes(self) -> None:
        updates = {"nested": {"values": [1]}}
        result = NodeResult.create("fixture_done", updates)
        terminal = NodeResult.create(None)

        updates["nested"]["values"].append(2)  # type: ignore[index,union-attr]
        returned = result.updates
        returned["nested"]["values"].append(3)

        self.assertEqual([1], result.updates["nested"]["values"])
        self.assertIsNone(terminal.port)
        self.assertFalse(hasattr(result, "__dict__"))
        self.assertNotIn("values", repr(result))

    def test_contract_factories_fail_closed(self) -> None:
        invalid_invocations = (
            {"job_id": "bad"},
            {"profile_version_id": "bad"},
            {"node_id": "Invalid Node"},
            {"pipeline_attempt": 0},
            {"execution_attempt": True},
            {"state_version": 0},
            {"trace_id": "bad"},
            {"workspace_id": "bad"},
            {"tool_call_id": "bad"},
            {"context": {"value": {"not-json"}}},
        )
        base = {
            "job_id": JOB_ID,
            "profile_version_id": PROFILE_VERSION_ID,
            "node_id": "analyze",
            "pipeline_attempt": 1,
            "execution_attempt": 1,
            "state_version": 1,
            "trace_id": TRACE_ID,
            "workspace_id": None,
            "tool_call_id": None,
            "context": {},
            "config": {},
        }
        for mutation in invalid_invocations:
            with self.subTest(mutation=mutation), self.assertRaises(NodeContractViolation):
                NodeInvocation.create(**{**base, **mutation})

        with self.assertRaises(NodeContractViolation):
            NodeResult.create("Invalid Port")
        with self.assertRaises(NodeContractViolation):
            NodeResult.create("done", {"value": float("nan")})
        with self.assertRaises(TypeError):
            NodeInvocation()  # type: ignore[call-arg]


class NodeRegistryTest(unittest.TestCase):
    def test_registers_and_resolves_one_source_handler_contract(self) -> None:
        handler = lambda invocation: NodeResult.create("done", invocation.context)
        config_validator = lambda config: None
        registry = NodeRegistry().register(
            "fixture.analyze",
            node_types=["agent"],
            result_ports=["done", "retry"],
            handler=handler,
            config_validator=config_validator,
        )

        registration = registry.resolve("fixture.analyze")

        self.assertIs(handler, registration.handler)
        self.assertIs(config_validator, registration.config_validator)
        self.assertEqual(frozenset({"agent"}), registration.node_types)
        self.assertEqual(frozenset({"done", "retry"}), registration.result_ports)
        self.assertEqual(("fixture.analyze",), registry.registered_keys)
        self.assertFalse(hasattr(registration, "__dict__"))

    def test_duplicate_registration_never_overwrites_the_first_handler(self) -> None:
        first = lambda invocation: NodeResult.create("done")
        second = lambda invocation: NodeResult.create("done")
        registry = NodeRegistry().register(
            "fixture.analyze",
            node_types=["agent"],
            result_ports=["done"],
            handler=first,
        )

        with self.assertRaisesRegex(NodeRegistryViolation, "duplicate"):
            registry.register(
                "fixture.analyze",
                node_types=["agent"],
                result_ports=["done"],
                handler=second,
            )

        self.assertIs(first, registry.resolve("fixture.analyze").handler)

    def test_registry_definition_and_lookup_are_strict(self) -> None:
        handler = lambda invocation: NodeResult.create(None)
        invalid_definitions = (
            {"handler_key": "Invalid Handler", "node_types": ["end"], "result_ports": []},
            {"handler_key": "fixture.end", "node_types": [], "result_ports": []},
            {"handler_key": "fixture.end", "node_types": ["plugin"], "result_ports": []},
            {"handler_key": "fixture.end", "node_types": ["end", "end"], "result_ports": []},
            {"handler_key": "fixture.end", "node_types": ["end"], "result_ports": ["bad port"]},
            {"handler_key": "fixture.end", "node_types": ["end"], "result_ports": ["done", "done"]},
        )
        for definition in invalid_definitions:
            registry = NodeRegistry()
            with self.subTest(definition=definition), self.assertRaises(NodeRegistryViolation):
                registry.register(**definition, handler=handler)

        with self.assertRaisesRegex(NodeRegistryViolation, "unregistered"):
            NodeRegistry().resolve("fixture.missing")

        with self.assertRaisesRegex(NodeRegistryViolation, "config validator"):
            NodeRegistry().register(
                "fixture.end",
                node_types=["end"],
                result_ports=[],
                handler=handler,
                config_validator=object(),  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
