from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any
import unittest
from uuid import uuid5, NAMESPACE_URL

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.default_natural_cms_snapshot import (
    CMS_TOOL_NAMES,
    DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
    default_natural_cms_snapshot,
    default_natural_cms_snapshot_dict,
)
from axms_coding_orchestrator.graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
)
from axms_coding_orchestrator.natural_cms_domain_client import (
    NaturalCmsJob,
    NaturalCmsResource,
    NaturalCmsStageResult,
)
from axms_coding_orchestrator.natural_cms_handlers import (
    NATURAL_CMS_HANDLER_CONTRACTS,
    NaturalCmsHandlerDependencies,
    register_natural_cms_node_handlers,
)
from axms_coding_orchestrator.node_runtime import NodeInvocation
from axms_coding_orchestrator.snapshot import VersionedSnapshot, load_snapshot_json


FIXTURE = Path(__file__).parent / "fixtures" / "natural-cms-handler.snapshot.valid.json"
JOB_ID = "50505050-5050-4050-8050-505050505050"
TRACE_ID = "60606060-6060-4060-8060-606060606060"
POLICY_HASH = "sha256:" + ("a" * 64)
PREVIEW_HASH = "sha256:" + ("b" * 64)
RESOURCE = NaturalCmsResource("CONTENT", "7")
COMMAND = {"operation": "UPDATE", "fields": {"title": "New", "body": "Body"}}


class _Domain:
    def __init__(self) -> None:
        self.pipeline_attempt = 1
        self.state_version = 1
        self.status = "ACTIVE"
        self.preview_id: str | None = None
        self.preview_hash: str | None = None
        self.preview_valid = False
        self.decision: str | None = None
        self.preview_attempt: int | None = None

    def get_job(self, invocation: NodeInvocation) -> NaturalCmsJob:
        return NaturalCmsJob(
            JOB_ID,
            TRACE_ID,
            DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
            self.pipeline_attempt,
            self.state_version,
            self.status,
            RESOURCE,
            self.preview_id,
            self.preview_hash,
            self.preview_valid,
            self.decision,
            "Update content 7",
        )

    def decide(self, decision: str) -> int:
        self.decision = decision
        if decision == "REJECTED" and self.pipeline_attempt < 3:
            self.pipeline_attempt += 1
        self.state_version += 1
        return self.state_version


class _Executor:
    def __init__(self, domain: _Domain) -> None:
        self.domain = domain
        self.calls: list[tuple[str, int]] = []
        self.counts: dict[str, int] = defaultdict(int)

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        job: NaturalCmsJob,
        result_id: str,
    ) -> NaturalCmsStageResult:
        del job
        self.calls.append((handler_key, invocation.pipeline_attempt))
        self.counts[handler_key] += 1
        if handler_key == "cms.analyze":
            return NaturalCmsStageResult(
                result_id, handler_key, "feasible", RESOURCE, None, None, None, {}
            )
        if handler_key == "cms.preview":
            preview_id = str(
                uuid5(NAMESPACE_URL, f"natural-cms-preview:{invocation.pipeline_attempt}")
            )
            self.domain.status = "WAITING_APPROVAL"
            self.domain.preview_id = preview_id
            self.domain.preview_hash = PREVIEW_HASH
            self.domain.preview_valid = True
            self.domain.decision = None
            self.domain.preview_attempt = invocation.pipeline_attempt
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "ready",
                RESOURCE,
                COMMAND,
                preview_id,
                PREVIEW_HASH,
                {"before": {}, "after": {}},
            )
        if handler_key == "cms.discard":
            self.domain.preview_valid = False
            retry = self.domain.preview_attempt != invocation.pipeline_attempt
            self.domain.status = "ACTIVE" if retry else "REJECTED"
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "retry" if retry else "discarded",
                RESOURCE,
                COMMAND,
                self.domain.preview_id,
                PREVIEW_HASH,
                {"discarded": True, "retry": retry},
            )
        if handler_key == "cms.apply":
            self.domain.status = "COMPLETED"
            self.domain.preview_valid = False
            return NaturalCmsStageResult(
                result_id,
                handler_key,
                "applied",
                RESOURCE,
                COMMAND,
                self.domain.preview_id,
                PREVIEW_HASH,
                {"status": "APPLIED"},
            )
        raise AssertionError(f"unexpected handler: {handler_key}")


def _state() -> dict[str, Any]:
    return {
        "jobId": JOB_ID,
        "profileVersionId": DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
        "pipelineAttempt": 1,
        "executionAttempt": 1,
        "stateVersion": 1,
        "traceId": TRACE_ID,
        "workspaceId": None,
        "toolCallId": None,
        "context": {"policyHash": POLICY_HASH},
    }


class DefaultNaturalCmsSnapshotTest(unittest.TestCase):
    def test_stage_contract_rejects_coding_only_fields(self) -> None:
        with self.assertRaises(ValueError):
            NaturalCmsStageResult.from_dict(
                {
                    "schemaVersion": "1.0",
                    "resultId": "77777777-7777-4777-8777-777777777777",
                    "handlerKey": "cms.analyze",
                    "resultPort": "feasible",
                    "resource": {"type": "CONTENT", "id": "7"},
                    "payload": {},
                    "workspaceId": "88888888-8888-4888-8888-888888888888",
                }
            )

    def test_fixture_matches_source_and_declares_only_reserved_tools(self) -> None:
        fixture = load_snapshot_json(FIXTURE.read_bytes())

        self.assertEqual(default_natural_cms_snapshot_dict(), fixture.to_dict())
        self.assertEqual(default_natural_cms_snapshot().to_json(), fixture.to_json())
        self.assertEqual(
            list(CMS_TOOL_NAMES), fixture.tool_policy["allowedTools"]
        )
        self.assertNotIn("candidateSha", FIXTURE.read_text(encoding="utf-8"))
        self.assertNotIn("workspaceId", FIXTURE.read_text(encoding="utf-8"))

    def test_snapshot_compiles_with_common_registry_and_cms_handlers(self) -> None:
        domain = _Domain()
        registry = register_natural_cms_node_handlers(
            build_common_node_registry(),
            NaturalCmsHandlerDependencies(domain, _Executor(domain)),
        )

        graph = SnapshotGraphBuilder(registry).compile(default_natural_cms_snapshot())

        self.assertIsNotNone(graph)
        self.assertTrue(set(NATURAL_CMS_HANDLER_CONTRACTS).issubset(registry.registered_keys))

    def test_feature_handler_configs_are_validated_before_execution(self) -> None:
        invalid_configs = {
            "analyze": {"unknown": True},
            "approval": {"stage": "PREVIEW", "requiredRole": "SUPER_ADMIN"},
        }

        for node_id, config in invalid_configs.items():
            payload = default_natural_cms_snapshot_dict()
            node = next(item for item in payload["nodes"] if item["id"] == node_id)
            node["config"] = config
            snapshot = VersionedSnapshot.from_dict(payload)
            domain = _Domain()
            registry = register_natural_cms_node_handlers(
                build_common_node_registry(),
                NaturalCmsHandlerDependencies(domain, _Executor(domain)),
            )

            with self.subTest(node_id=node_id), self.assertRaisesRegex(
                SnapshotGraphBuildError, "config"
            ):
                SnapshotGraphBuilder(registry).compile(snapshot)

    def test_rejection_discards_then_retries_same_job_before_apply(self) -> None:
        domain = _Domain()
        executor = _Executor(domain)
        registry = register_natural_cms_node_handlers(
            build_common_node_registry(),
            NaturalCmsHandlerDependencies(domain, executor),
        )
        graph = SnapshotGraphBuilder(registry).compile(
            default_natural_cms_snapshot(), checkpointer=InMemorySaver()
        )
        config: dict[str, Any] = {
            "configurable": {"thread_id": JOB_ID},
            "recursion_limit": 50,
        }

        waiting = graph.invoke(_state(), config=config)
        self.assertIn("__interrupt__", waiting)
        self.assertEqual(1, executor.counts["cms.preview"])

        rejected_version = domain.decide("REJECTED")
        waiting = graph.invoke(
            Command(
                resume=True,
                update={
                    "pipelineAttempt": domain.pipeline_attempt,
                    "stateVersion": rejected_version,
                },
            ),
            config=config,
        )
        self.assertIn("__interrupt__", waiting)
        self.assertEqual(2, waiting["pipelineAttempt"])
        self.assertEqual(1, executor.counts["cms.discard"])
        self.assertEqual(2, executor.counts["cms.analyze"])
        self.assertEqual(2, executor.counts["cms.preview"])

        approved_version = domain.decide("APPROVED")
        completed = graph.invoke(
            Command(resume=True, update={"stateVersion": approved_version}),
            config=config,
        )

        self.assertNotIn("__interrupt__", completed)
        self.assertEqual("end", completed["_snapshotLastNodeId"])
        self.assertEqual("COMPLETED", domain.status)
        self.assertEqual(1, executor.counts["cms.apply"])
        self.assertEqual(
            [
                ("cms.analyze", 1),
                ("cms.preview", 1),
                ("cms.discard", 2),
                ("cms.analyze", 2),
                ("cms.preview", 2),
                ("cms.apply", 2),
            ],
            executor.calls,
        )


if __name__ == "__main__":
    unittest.main()
