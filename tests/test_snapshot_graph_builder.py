from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any
import unittest

from axms_coding_orchestrator.graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
    SnapshotGraphExecutionError,
)
from axms_coding_orchestrator.node_runtime import (
    NodeInvocation,
    NodeRegistry,
    NodeResult,
)
from axms_coding_orchestrator.snapshot import VersionedSnapshot


JOB_ID = "20202020-2020-4020-8020-202020202020"
PROFILE_VERSION_ID = "11111111-1111-4111-8111-111111111111"
OTHER_PROFILE_VERSION_ID = "99999999-9999-4999-8999-999999999999"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
TOOL_CALL_ID = "50505050-5050-4050-8050-505050505050"


Handler = Callable[[NodeInvocation], NodeResult]


def _node(
    node_id: str,
    node_type: str,
    handler_key: str,
    result_ports: Iterable[str],
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": node_type,
        "handlerKey": handler_key,
        "resultPorts": list(result_ports),
        "config": dict(config or {}),
    }


def _edge(source: str, port: str, target: str) -> dict[str, str]:
    return {"from": source, "resultPort": port, "to": target}


def _snapshot(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, str]],
    *,
    loop_limits: list[dict[str, Any]] | None = None,
) -> VersionedSnapshot:
    return VersionedSnapshot.from_dict(
        {
            "contractVersion": "1.0",
            "profileVersionId": PROFILE_VERSION_ID,
            "profileKey": "LLM_OPS",
            "profileVersion": 1,
            "nodes": nodes,
            "edges": edges,
            "config": {
                "maxNodes": 12,
                "maxAttempts": 3,
                "loopLimits": list(loop_limits or []),
            },
            "modelBindings": {},
            "toolPolicy": {},
            "guardrailProfileKey": "fixture.locked",
        }
    )


def _linear_snapshot() -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node(
                "fixture_work",
                "check",
                "fixture.work",
                ["fixture_done"],
                {"fixtureMode": "linear"},
            ),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_work"),
            _edge("fixture_work", "fixture_done", "fixture_end"),
        ],
    )


def _branch_snapshot() -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node(
                "fixture_branch",
                "check",
                "fixture.branch",
                ["fixture_left", "fixture_right"],
            ),
            _node(
                "fixture_left_node",
                "check",
                "fixture.left",
                ["fixture_done"],
            ),
            _node(
                "fixture_right_node",
                "check",
                "fixture.right",
                ["fixture_done"],
            ),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_branch"),
            _edge("fixture_branch", "fixture_left", "fixture_left_node"),
            _edge("fixture_branch", "fixture_right", "fixture_right_node"),
            _edge("fixture_left_node", "fixture_done", "fixture_end"),
            _edge("fixture_right_node", "fixture_done", "fixture_end"),
        ],
    )


def _loop_snapshot(max_iterations: int = 2) -> VersionedSnapshot:
    return _snapshot(
        [
            _node("fixture_start", "start", "fixture.start", ["fixture_next"]),
            _node(
                "fixture_guardrail",
                "guardrail",
                "fixture.guardrail",
                ["fixture_passed"],
                {"locked": True},
            ),
            _node(
                "fixture_work",
                "check",
                "fixture.work",
                ["fixture_repeat", "fixture_done"],
            ),
            _node("fixture_end", "end", "fixture.end", []),
        ],
        [
            _edge("fixture_start", "fixture_next", "fixture_guardrail"),
            _edge("fixture_guardrail", "fixture_passed", "fixture_work"),
            _edge("fixture_work", "fixture_repeat", "fixture_guardrail"),
            _edge("fixture_work", "fixture_done", "fixture_end"),
        ],
        loop_limits=[
            {
                "from": "fixture_work",
                "resultPort": "fixture_repeat",
                "to": "fixture_guardrail",
                "maxIterations": max_iterations,
            }
        ],
    )


def _state(
    *,
    profile_version_id: str = PROFILE_VERSION_ID,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "jobId": JOB_ID,
        "profileVersionId": profile_version_id,
        "pipelineAttempt": 2,
        "executionAttempt": 3,
        "stateVersion": 4,
        "traceId": TRACE_ID,
        "workspaceId": WORKSPACE_ID,
        "toolCallId": TOOL_CALL_ID,
        "context": dict(context or {}),
    }


def _fixed_handler(
    log: list[tuple[str, NodeInvocation]],
    name: str,
    port: str | None,
    updates: Mapping[str, Any] | None = None,
) -> Handler:
    def run(invocation: NodeInvocation) -> NodeResult:
        log.append((name, invocation))
        return NodeResult.create(port, updates)

    return run


def _registry(
    snapshot: VersionedSnapshot,
    handlers: Mapping[str, Handler],
    *,
    skip: frozenset[str] = frozenset(),
    node_type_overrides: Mapping[str, Iterable[str]] | None = None,
    port_overrides: Mapping[str, Iterable[str]] | None = None,
) -> NodeRegistry:
    node_type_overrides = node_type_overrides or {}
    port_overrides = port_overrides or {}
    registry = NodeRegistry()
    for node in snapshot.nodes:
        if node.handler_key in skip:
            continue
        registry.register(
            node.handler_key,
            node_types=node_type_overrides.get(node.handler_key, [node.node_type]),
            result_ports=port_overrides.get(node.handler_key, node.result_ports),
            handler=handlers[node.handler_key],
        )
    return registry


def _linear_handlers(
    log: list[tuple[str, NodeInvocation]],
    *,
    work: Handler | None = None,
) -> dict[str, Handler]:
    return {
        "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
        "fixture.guardrail": _fixed_handler(
            log, "fixture.guardrail", "fixture_passed"
        ),
        "fixture.work": work
        or _fixed_handler(
            log,
            "fixture.work",
            "fixture_done",
            {"fixture_value": "complete"},
        ),
        "fixture.end": _fixed_handler(log, "fixture.end", None),
    }


class SnapshotGraphBuilderTest(unittest.TestCase):
    def test_compiles_and_executes_a_linear_fixture_graph(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = _linear_handlers(log)
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        completed = graph.invoke(_state(context={"fixture_seed": "original"}))

        self.assertEqual(
            ["fixture.start", "fixture.guardrail", "fixture.work", "fixture.end"],
            [name for name, _ in log],
        )
        work_invocation = log[2][1]
        self.assertEqual(JOB_ID, work_invocation.job_id)
        self.assertEqual(PROFILE_VERSION_ID, work_invocation.profile_version_id)
        self.assertEqual("fixture_work", work_invocation.node_id)
        self.assertEqual(2, work_invocation.pipeline_attempt)
        self.assertEqual(3, work_invocation.execution_attempt)
        self.assertEqual(4, work_invocation.state_version)
        self.assertEqual(TRACE_ID, work_invocation.trace_id)
        self.assertEqual(WORKSPACE_ID, work_invocation.workspace_id)
        self.assertEqual(TOOL_CALL_ID, work_invocation.tool_call_id)
        self.assertEqual({"fixtureMode": "linear"}, work_invocation.config)
        self.assertEqual("original", work_invocation.context["fixture_seed"])
        self.assertEqual("complete", completed["context"]["fixture_value"])

    def test_executes_only_the_selected_fixture_branch(self) -> None:
        snapshot = _branch_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def branch(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.branch", invocation))
            return NodeResult.create(invocation.context["fixture_choice"])

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.branch": branch,
            "fixture.left": _fixed_handler(log, "fixture.left", "fixture_done"),
            "fixture.right": _fixed_handler(log, "fixture.right", "fixture_done"),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        graph.invoke(_state(context={"fixture_choice": "fixture_left"}))
        self.assertEqual(
            [
                "fixture.start",
                "fixture.guardrail",
                "fixture.branch",
                "fixture.left",
                "fixture.end",
            ],
            [name for name, _ in log],
        )

        log.clear()
        graph.invoke(_state(context={"fixture_choice": "fixture_right"}))
        self.assertEqual(
            [
                "fixture.start",
                "fixture.guardrail",
                "fixture.branch",
                "fixture.right",
                "fixture.end",
            ],
            [name for name, _ in log],
        )

    def test_bounded_fixture_repeat_completes_at_the_declared_limit(self) -> None:
        snapshot = _loop_snapshot(max_iterations=2)
        log: list[tuple[str, NodeInvocation]] = []
        ports = iter(["fixture_repeat", "fixture_repeat", "fixture_done"])

        def work(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.work", invocation))
            return NodeResult.create(next(ports))

        handlers = _linear_handlers(log, work=work)
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        graph.invoke(_state())

        names = [name for name, _ in log]
        self.assertEqual(3, names.count("fixture.work"))
        self.assertEqual(3, names.count("fixture.guardrail"))
        self.assertEqual("fixture.end", names[-1])

    def test_bounded_fixture_repeat_rejects_the_next_transition(self) -> None:
        snapshot = _loop_snapshot(max_iterations=2)
        log: list[tuple[str, NodeInvocation]] = []

        def work(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.work", invocation))
            return NodeResult.create("fixture_repeat")

        handlers = _linear_handlers(log, work=work)
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        with self.assertRaisesRegex(SnapshotGraphExecutionError, "bounded loop"):
            graph.invoke(_state())

        names = [name for name, _ in log]
        self.assertEqual(3, names.count("fixture.work"))
        self.assertNotIn("fixture.end", names)

    def test_build_rejects_an_unregistered_fixture_handler(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = _linear_handlers(log)
        registry = _registry(snapshot, handlers, skip=frozenset({"fixture.work"}))

        with self.assertRaisesRegex(SnapshotGraphBuildError, "unregistered"):
            SnapshotGraphBuilder(registry).compile(snapshot)

        self.assertEqual([], log)

    def test_build_rejects_a_registered_fixture_node_type_mismatch(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = _linear_handlers(log)
        registry = _registry(
            snapshot,
            handlers,
            node_type_overrides={"fixture.work": ["tool"]},
        )

        with self.assertRaisesRegex(SnapshotGraphBuildError, "type"):
            SnapshotGraphBuilder(registry).compile(snapshot)

    def test_build_requires_the_exact_registered_fixture_port_set(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = _linear_handlers(log)
        incompatible_port_sets = (
            ["fixture_done", "fixture_extra"],
            ["fixture_other"],
        )

        for ports in incompatible_port_sets:
            with self.subTest(ports=ports):
                registry = _registry(
                    snapshot,
                    handlers,
                    port_overrides={"fixture.work": ports},
                )
                with self.assertRaisesRegex(SnapshotGraphBuildError, "result ports"):
                    SnapshotGraphBuilder(registry).compile(snapshot)

    def test_execution_rejects_an_undeclared_fixture_runtime_port(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def work(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.work", invocation))
            return NodeResult.create("fixture_unexpected")

        handlers = _linear_handlers(log, work=work)
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        with self.assertRaisesRegex(SnapshotGraphExecutionError, "undeclared port"):
            graph.invoke(_state())

        self.assertEqual(
            ["fixture.start", "fixture.guardrail", "fixture.work"],
            [name for name, _ in log],
        )

    def test_execution_rejects_a_mismatched_profile_version_before_handlers(self) -> None:
        snapshot = _linear_snapshot()
        log: list[tuple[str, NodeInvocation]] = []
        handlers = _linear_handlers(log)
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        with self.assertRaisesRegex(
            SnapshotGraphExecutionError, "mismatched profileVersionId"
        ):
            graph.invoke(_state(profile_version_id=OTHER_PROFILE_VERSION_ID))

        self.assertEqual([], log)

    def test_handler_updates_cannot_replace_internal_fixture_routing_state(self) -> None:
        snapshot = _branch_snapshot()
        log: list[tuple[str, NodeInvocation]] = []

        def branch(invocation: NodeInvocation) -> NodeResult:
            log.append(("fixture.branch", invocation))
            return NodeResult.create(
                "fixture_left",
                {
                    "_snapshotLastNodeId": "fixture_poison",
                    "_snapshotLastResultPort": "fixture_right",
                    "_snapshotLoopCounts": {"fixture_poison": 999},
                    "profileVersionId": OTHER_PROFILE_VERSION_ID,
                },
            )

        handlers = {
            "fixture.start": _fixed_handler(log, "fixture.start", "fixture_next"),
            "fixture.guardrail": _fixed_handler(
                log, "fixture.guardrail", "fixture_passed"
            ),
            "fixture.branch": branch,
            "fixture.left": _fixed_handler(log, "fixture.left", "fixture_done"),
            "fixture.right": _fixed_handler(log, "fixture.right", "fixture_done"),
            "fixture.end": _fixed_handler(log, "fixture.end", None),
        }
        graph = SnapshotGraphBuilder(_registry(snapshot, handlers)).compile(snapshot)

        completed = graph.invoke(_state())

        self.assertEqual(
            [
                "fixture.start",
                "fixture.guardrail",
                "fixture.branch",
                "fixture.left",
                "fixture.end",
            ],
            [name for name, _ in log],
        )
        left_invocation = next(
            invocation for name, invocation in log if name == "fixture.left"
        )
        self.assertEqual(PROFILE_VERSION_ID, left_invocation.profile_version_id)
        self.assertEqual(PROFILE_VERSION_ID, completed["profileVersionId"])
        self.assertEqual("fixture_end", completed["_snapshotLastNodeId"])
        self.assertIsNone(completed["_snapshotLastResultPort"])
        self.assertEqual(
            "fixture_right", completed["context"]["_snapshotLastResultPort"]
        )


if __name__ == "__main__":
    unittest.main()
