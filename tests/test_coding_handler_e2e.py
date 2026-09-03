from __future__ import annotations

from collections import defaultdict
import unittest
from uuid import NAMESPACE_URL, uuid5

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from axms_coding_orchestrator.coding_domain_client import (
    CodingApprovalDecision,
    CodingAttemptAggregate,
    CodingResultRecord,
    CodingResultWrite,
)
from axms_coding_orchestrator.coding_handlers import (
    CodingHandlerDependencies,
    CodingStageOutcome,
    register_coding_node_handlers,
)
from axms_coding_orchestrator.common_handlers import build_common_node_registry
from axms_coding_orchestrator.default_coding_snapshot import (
    DEFAULT_CODING_PROFILE_VERSION_ID,
    default_coding_snapshot,
    default_coding_snapshot_dict,
)
from axms_coding_orchestrator.graph_builder import (
    SnapshotGraphBuilder,
    SnapshotGraphExecutionError,
)
from axms_coding_orchestrator.node_runtime import NodeInvocation
from axms_coding_orchestrator.snapshot import VersionedSnapshot


JOB_ID = "20202020-2020-4020-8020-202020202020"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
ACTOR_ID = "60606060-6060-4060-8060-606060606060"
NOW = "2026-08-30T01:02:03Z"
POLICY_HASH = "sha256:" + ("f" * 64)


def _state() -> dict[str, object]:
    return {
        "jobId": JOB_ID,
        "profileVersionId": DEFAULT_CODING_PROFILE_VERSION_ID,
        "pipelineAttempt": 1,
        "executionAttempt": 1,
        "stateVersion": 5,
        "traceId": TRACE_ID,
        "workspaceId": WORKSPACE_ID,
        "toolCallId": None,
        "context": {"policyHash": POLICY_HASH},
    }


def _approval_id(
    *, pipeline_attempt: int, node_id: str, stage: str, stage_round: int
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"axms:coding-approval:{JOB_ID}:{pipeline_attempt}:"
            f"{node_id}:{stage}:{stage_round}",
        )
    )


class _Domain:
    def __init__(self) -> None:
        self.results: dict[int, dict[str, CodingResultRecord]] = defaultdict(dict)
        self.decisions: list[CodingApprovalDecision] = []

    def get_attempt(self, invocation: NodeInvocation) -> CodingAttemptAggregate:
        return CodingAttemptAggregate(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            pipeline_attempt=invocation.pipeline_attempt,
            workspace_id=WORKSPACE_ID,
            status="ACTIVE",
            request_text="sensitive coding request",
            results=tuple(self.results[invocation.pipeline_attempt].values()),
            pending_approvals=(),
            decisions=tuple(self.decisions),
            created_at=NOW,
            finished_at=None,
        )

    def put_result(
        self, invocation: NodeInvocation, result: CodingResultWrite
    ) -> CodingResultRecord:
        recorded = CodingResultRecord(
            result_id=result.result_id,
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            pipeline_attempt=invocation.pipeline_attempt,
            handler_key=result.handler_key,
            result_type=result.result_type,
            result_port=result.result_port,
            workspace_id=result.workspace_id,
            candidate_sha=result.candidate_sha,
            diff_digest=result.diff_digest,
            validation_hash=result.validation_hash,
            payload=dict(result.payload),
            recorded_at=NOW,
        )
        prior = self.results[invocation.pipeline_attempt].get(result.result_id)
        if prior is not None:
            if prior != recorded:
                raise AssertionError("result replay changed")
            return prior
        self.results[invocation.pipeline_attempt][result.result_id] = recorded
        return recorded

    def approve(
        self,
        *,
        node_id: str,
        stage: str,
        stage_round: int,
        pipeline_attempt: int,
        state_version: int,
        decision: str = "APPROVED",
        next_pipeline_attempt: int | None = None,
    ) -> None:
        preview = next(
            (
                result
                for result in reversed(
                    tuple(self.results[pipeline_attempt].values())
                )
                if result.handler_key == "coding.preview"
            ),
            None,
        )
        self.decisions.append(
            CodingApprovalDecision(
                approval_id=_approval_id(
                    pipeline_attempt=pipeline_attempt,
                    node_id=node_id,
                    stage=stage,
                    stage_round=stage_round,
                ),
                node_id=node_id,
                stage=stage,
                stage_round=stage_round,
                decision=decision,
                candidate_sha=(preview.candidate_sha if stage != "SCOPE" and preview else None),
                validation_hash=(
                    preview.validation_hash if stage != "SCOPE" and preview else None
                ),
                feedback="Please revise." if decision == "REJECTED" else None,
                actor_id=ACTOR_ID,
                actor_role="SUPER_ADMIN",
                result_state_version=state_version,
                next_pipeline_attempt=next_pipeline_attempt,
                decided_at=NOW,
            )
        )


class _ScriptedExecutor:
    def __init__(
        self,
        review_ports: list[str] | None = None,
        merge_ports: list[str] | None = None,
    ) -> None:
        self.review_ports = list(review_ports or ["passed"])
        self.merge_ports = list(merge_ports or ["merged"])
        self.calls: list[tuple[str, int, str]] = []
        self.counts: dict[str, int] = defaultdict(int)

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        attempt: CodingAttemptAggregate,
        result_id: str,
    ) -> CodingStageOutcome:
        self.calls.append((handler_key, invocation.pipeline_attempt, invocation.node_id))
        index = self.counts[handler_key]
        self.counts[handler_key] += 1
        if handler_key == "coding.analyze":
            return CodingStageOutcome("feasible", {"analysisRef": result_id})
        if handler_key == "coding.code":
            digit = format((index + 1) % 16, "x")
            return CodingStageOutcome(
                "completed",
                {"candidateRef": result_id},
                candidate_sha="sha1:" + (digit * 40),
            )
        if handler_key == "coding.review":
            candidate = next(
                result.candidate_sha
                for result in reversed(attempt.results)
                if result.handler_key == "coding.code"
                and result.result_type == "CANDIDATE"
                and result.result_port == "completed"
            )
            port = self.review_ports[min(index, len(self.review_ports) - 1)]
            return CodingStageOutcome(
                port,
                {"reviewRef": result_id},
                candidate_sha=candidate,
            )
        if handler_key == "coding.preview":
            review = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.review"
                and result.result_type == "REVIEW"
            )
            if review.result_port != "passed" or review.candidate_sha is None:
                raise AssertionError("preview must follow the latest passed review")
            digit = review.candidate_sha.removeprefix("sha1:")[0]
            return CodingStageOutcome(
                "ready",
                {"previewRef": result_id},
                candidate_sha=review.candidate_sha,
                diff_digest="sha256:" + (digit * 64),
                validation_hash="sha256:" + (digit * 64),
            )
        if handler_key == "coding.pr_request":
            preview = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.preview"
                and result.result_type == "DIFF"
                and result.result_port == "ready"
            )
            return CodingStageOutcome(
                "requested",
                {"requestRef": result_id},
                candidate_sha=preview.candidate_sha,
                validation_hash=preview.validation_hash,
            )
        if handler_key == "coding.pr_complete":
            request = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.pr_request"
                and result.result_port == "requested"
            )
            return CodingStageOutcome(
                "completed",
                {
                    "repository": "backend",
                    "base": "dev",
                    "head": "system/llmops-" + ("a" * 32),
                    "headSha": "sha1:" + ("a" * 40),
                    "candidateSha": request.candidate_sha,
                    "prNumber": 42,
                    "prUrl": "https://github.example/pr/42",
                },
                candidate_sha=request.candidate_sha,
                validation_hash=request.validation_hash,
            )
        if handler_key == "coding.deploy_request":
            pull_request = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.pr_complete"
                and result.result_port == "completed"
            )
            return CodingStageOutcome(
                "recorded",
                {
                    "deploymentRequestId": f"deploy-request-{index + 1}",
                    "jobId": JOB_ID,
                    "pipelineAttempt": invocation.pipeline_attempt,
                    "repository": "backend",
                    "prNumber": pull_request.payload["prNumber"],
                    "candidateSha": pull_request.candidate_sha,
                    "sourceValidationHash": pull_request.validation_hash,
                    "adapterKey": "local-docker-compose",
                    "targetKey": "full:backend:spring-app",
                    "configDigest": pull_request.validation_hash,
                    "status": "DEPLOY_REQUEST_RECORDED",
                },
                candidate_sha=pull_request.candidate_sha,
                validation_hash=pull_request.validation_hash,
            )
        if handler_key == "coding.dev_merge_check":
            deploy_request = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.deploy_request"
                and result.result_port == "recorded"
            )
            port = self.merge_ports[min(index, len(self.merge_ports) - 1)]
            payload = {
                "status": {
                    "merged": "MERGED",
                    "not_merged": "NOT_MERGED",
                    "blocked": "BLOCKED",
                }[port],
                "candidateSha": deploy_request.candidate_sha,
                "head": "system/llmops-" + ("a" * 32),
                "headSha": "sha1:" + ("a" * 40),
            }
            if port == "merged":
                payload["mergeSha"] = "sha1:" + ("b" * 40)
            return CodingStageOutcome(
                port,
                payload,
                candidate_sha=deploy_request.candidate_sha,
                validation_hash=deploy_request.validation_hash,
            )
        if handler_key == "coding.deploy":
            deploy_request = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.deploy_request"
                and result.result_port == "recorded"
            )
            merge = next(
                result
                for result in reversed(attempt.results)
                if result.handler_key == "coding.dev_merge_check"
                and result.result_port == "merged"
            )
            return CodingStageOutcome(
                "completed",
                {
                    "deploymentRequestId": deploy_request.payload["deploymentRequestId"],
                    "deploymentExecutionId": f"deploy-execution-{index + 1}",
                    "mergeSha": merge.payload["mergeSha"],
                    "status": "COMPLETED",
                },
                candidate_sha=deploy_request.candidate_sha,
                validation_hash=deploy_request.validation_hash,
            )
        raise AssertionError(f"unexpected handler {handler_key}")


class CodingHandlerGraphContractTest(unittest.TestCase):
    def _runtime(
        self,
        *,
        review_ports: list[str] | None = None,
        merge_ports: list[str] | None = None,
    ) -> tuple[object, _Domain, _ScriptedExecutor, dict[str, object]]:
        domain = _Domain()
        executor = _ScriptedExecutor(review_ports, merge_ports)
        registry = register_coding_node_handlers(
            build_common_node_registry(),
            CodingHandlerDependencies(domain, executor),
        )
        graph = SnapshotGraphBuilder(registry).compile(
            default_coding_snapshot(), checkpointer=InMemorySaver()
        )
        config: dict[str, object] = {
            "configurable": {"thread_id": JOB_ID},
            "recursion_limit": 100,
        }
        return graph, domain, executor, config

    def test_graph_contract_resumes_approvals_and_finishes_at_review_limit(self) -> None:
        graph, domain, executor, config = self._runtime(
            review_ports=[
                "changes_requested",
                "changes_requested",
                "passed",
            ],
            merge_ports=["not_merged", "merged"],
        )

        waiting = graph.invoke(_state(), config=config)  # type: ignore[attr-defined]
        self.assertIn("__interrupt__", waiting)
        domain.approve(
            node_id="scope_approval",
            stage="SCOPE",
            stage_round=1,
            pipeline_attempt=1,
            state_version=6,
        )
        waiting = graph.invoke(  # type: ignore[attr-defined]
            Command(resume=True, update={"stateVersion": 6}), config=config
        )
        self.assertIn("__interrupt__", waiting)
        self.assertEqual(3, executor.counts["coding.code"])
        self.assertEqual(3, executor.counts["coding.review"])

        approvals = (
            ("preview_approval", "CANDIDATE", 7),
            ("github_approval", "GITHUB", 8),
            ("deploy_approval", "DEPLOY", 9),
        )
        for node_id, stage, state_version in approvals:
            domain.approve(
                node_id=node_id,
                stage=stage,
                stage_round=1,
                pipeline_attempt=1,
                state_version=state_version,
            )
            waiting = graph.invoke(  # type: ignore[attr-defined]
                Command(resume=True, update={"stateVersion": state_version}),
                config=config,
            )

        self.assertIn("__interrupt__", waiting)
        domain.approve(
            node_id="deploy_approval",
            stage="DEPLOY",
            stage_round=2,
            pipeline_attempt=1,
            state_version=10,
        )
        waiting = graph.invoke(  # type: ignore[attr-defined]
            Command(resume=True, update={"stateVersion": 10}), config=config
        )

        self.assertNotIn("__interrupt__", waiting)
        self.assertEqual("end", waiting["_snapshotLastNodeId"])
        self.assertEqual(1, executor.counts["coding.pr_request"])
        self.assertEqual(1, executor.counts["coding.pr_complete"])
        self.assertEqual(2, executor.counts["coding.deploy_request"])
        self.assertEqual(2, executor.counts["coding.dev_merge_check"])
        self.assertEqual(1, executor.counts["coding.deploy"])
        self.assertEqual(
            "completed", waiting["context"]["codingLastResult"]["resultPort"]
        )

    def test_v4_snapshot_edges_control_approval_interrupts_across_restarts(
        self,
    ) -> None:
        payload = default_coding_snapshot_dict()
        snapshot = VersionedSnapshot.from_dict(payload)

        domain = _Domain()
        executor = _ScriptedExecutor()
        registry = register_coding_node_handlers(
            build_common_node_registry(),
            CodingHandlerDependencies(domain, executor),
        )
        checkpointer = InMemorySaver()
        config: dict[str, object] = {
            "configurable": {"thread_id": JOB_ID},
            "recursion_limit": 100,
        }

        def invoke(value: object) -> dict[str, object]:
            graph = SnapshotGraphBuilder(registry).compile(
                snapshot, checkpointer=checkpointer
            )
            return graph.invoke(value, config=config)  # type: ignore[no-any-return]

        def interrupted_at(result: dict[str, object]) -> tuple[str, str]:
            interruptions = result["__interrupt__"]
            self.assertEqual(1, len(interruptions))  # type: ignore[arg-type]
            value = interruptions[0].value  # type: ignore[index,union-attr]
            return value["nodeId"], value["stage"]

        waiting = invoke(_state())
        self.assertEqual(("scope_approval", "SCOPE"), interrupted_at(waiting))
        self.assertEqual({JOB_ID}, set(checkpointer.storage))

        approvals = (
            ("scope_approval", "SCOPE", 6, "preview_approval", "CANDIDATE"),
            ("preview_approval", "CANDIDATE", 7, "github_approval", "GITHUB"),
            ("github_approval", "GITHUB", 8, "deploy_approval", "DEPLOY"),
        )
        for node_id, stage, state_version, next_node_id, next_stage in approvals:
            domain.approve(
                node_id=node_id,
                stage=stage,
                stage_round=1,
                pipeline_attempt=1,
                state_version=state_version,
            )
            waiting = invoke(
                Command(resume=True, update={"stateVersion": state_version})
            )
            self.assertEqual(
                (next_node_id, next_stage),
                interrupted_at(waiting),
            )

        domain.approve(
            node_id="deploy_approval",
            stage="DEPLOY",
            stage_round=1,
            pipeline_attempt=1,
            state_version=9,
        )
        completed = invoke(Command(resume=True, update={"stateVersion": 9}))

        self.assertNotIn("__interrupt__", completed)
        self.assertEqual("end", completed["_snapshotLastNodeId"])
        self.assertEqual(
            {
                "coding.analyze": 1,
                "coding.code": 1,
                "coding.review": 1,
                "coding.preview": 1,
                "coding.pr_request": 1,
                "coding.pr_complete": 1,
                "coding.deploy_request": 1,
                "coding.dev_merge_check": 1,
                "coding.deploy": 1,
            },
            {handler: executor.counts[handler] for handler in executor.counts},
        )

    def test_candidate_rejection_resumes_new_attempt_and_routes_back_to_analyze(self) -> None:
        graph, domain, executor, config = self._runtime()
        graph.invoke(_state(), config=config)  # type: ignore[attr-defined]
        domain.approve(
            node_id="scope_approval",
            stage="SCOPE",
            stage_round=1,
            pipeline_attempt=1,
            state_version=6,
        )
        graph.invoke(  # type: ignore[attr-defined]
            Command(resume=True, update={"stateVersion": 6}), config=config
        )
        domain.approve(
            node_id="preview_approval",
            stage="CANDIDATE",
            stage_round=1,
            pipeline_attempt=1,
            state_version=7,
            decision="REJECTED",
            next_pipeline_attempt=2,
        )

        waiting = graph.invoke(  # type: ignore[attr-defined]
            Command(
                resume=True,
                update={"pipelineAttempt": 2, "stateVersion": 7},
            ),
            config=config,
        )

        analyze_attempts = [
            attempt
            for handler, attempt, _ in executor.calls
            if handler == "coding.analyze"
        ]
        self.assertEqual([1, 2], analyze_attempts)
        self.assertIn("__interrupt__", waiting)
        self.assertEqual("analyze", waiting["_snapshotLastNodeId"])
        self.assertEqual(2, waiting["pipelineAttempt"])

    def test_third_review_rework_hands_over_instead_of_failing(self) -> None:
        graph, domain, executor, config = self._runtime(
            review_ports=["changes_requested"]
        )
        graph.invoke(_state(), config=config)  # type: ignore[attr-defined]
        domain.approve(
            node_id="scope_approval",
            stage="SCOPE",
            stage_round=1,
            pipeline_attempt=1,
            state_version=6,
        )

        final = graph.invoke(  # type: ignore[attr-defined]
            Command(resume=True, update={"stateVersion": 6}), config=config
        )

        # The Job must finish, not raise: the handover screen reads the recorded
        # candidates and review reasons, and an execution error records neither.
        self.assertNotIn("__interrupt__", final)
        self.assertEqual("end", final["_snapshotLastNodeId"])
        self.assertEqual(3, executor.counts["coding.code"])
        self.assertEqual(3, executor.counts["coding.review"])
        self.assertEqual(3, final["context"]["codingStageRounds"]["rework_gate"] - 1)
        self.assertEqual({"rework_gate:retry:code": 2}, final["_snapshotLoopCounts"])
        self.assertNotIn("coding.preview", executor.counts)


if __name__ == "__main__":
    unittest.main()
