from __future__ import annotations

import unittest
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid5

from axms_coding_orchestrator.coding_domain_client import (
    CodingApprovalDecision,
    CodingAttemptAggregate,
    CodingResultRecord,
    CodingResultWrite,
    CodingStageExecutionResult,
)
from axms_coding_orchestrator.coding_handlers import (
    CodingHandlerDependencies,
    CodingHandlerFailure,
    CodingStageOutcome,
    PreparedResultCodingStageExecutor,
    SpringGatewayCodingStageExecutor,
    register_coding_node_handlers,
)
from axms_coding_orchestrator.graph import GraphExecutionError
from axms_coding_orchestrator.node_runtime import NodeInvocation, NodeRegistry


JOB_ID = "20202020-2020-4020-8020-202020202020"
PROFILE_VERSION_ID = "d3d41f73-9a07-51e5-9ec8-4ed8aca7f7cb"
TRACE_ID = "30303030-3030-4030-8030-303030303030"
WORKSPACE_ID = "40404040-4040-4040-8040-404040404040"
ACTOR_ID = "60606060-6060-4060-8060-606060606060"
NOW = "2026-08-30T01:02:03Z"
DIGEST = "sha256:" + ("a" * 64)
SHA = "sha1:" + ("a" * 40)
SHA_B = "sha1:" + ("b" * 40)
MERGE_SHA = "sha1:" + ("c" * 40)
DEPLOY_DIGEST = "sha256:" + ("d" * 64)


def _invocation(
    node_id: str,
    *,
    pipeline_attempt: int = 1,
    execution_attempt: int = 2,
    state_version: int = 7,
    config: dict[str, object] | None = None,
    context: dict[str, object] | None = None,
) -> NodeInvocation:
    return NodeInvocation.create(
        job_id=JOB_ID,
        profile_version_id=PROFILE_VERSION_ID,
        node_id=node_id,
        pipeline_attempt=pipeline_attempt,
        execution_attempt=execution_attempt,
        state_version=state_version,
        trace_id=TRACE_ID,
        workspace_id=WORKSPACE_ID,
        tool_call_id=None,
        context=context or {},
        config=config or {},
    )


def _record(
    *,
    result_id: str,
    handler_key: str,
    result_type: str,
    result_port: str,
    pipeline_attempt: int = 1,
    payload: dict[str, object] | None = None,
    candidate_sha: str | None = None,
    diff_digest: str | None = None,
    validation_hash: str | None = None,
) -> CodingResultRecord:
    return CodingResultRecord(
        result_id=result_id,
        job_id=JOB_ID,
        trace_id=TRACE_ID,
        pipeline_attempt=pipeline_attempt,
        handler_key=handler_key,
        result_type=result_type,
        result_port=result_port,
        workspace_id=WORKSPACE_ID,
        candidate_sha=candidate_sha,
        diff_digest=diff_digest,
        validation_hash=validation_hash,
        payload=payload or {},
        recorded_at=NOW,
    )


def _aggregate(
    *,
    pipeline_attempt: int = 1,
    results: tuple[CodingResultRecord, ...] = (),
    decisions: tuple[CodingApprovalDecision, ...] = (),
) -> CodingAttemptAggregate:
    return CodingAttemptAggregate(
        job_id=JOB_ID,
        trace_id=TRACE_ID,
        pipeline_attempt=pipeline_attempt,
        workspace_id=WORKSPACE_ID,
        status="ACTIVE",
        request_text="sensitive request",
        results=results,
        pending_approvals=(),
        decisions=decisions,
        created_at=NOW,
        finished_at=None,
    )


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


def _decision(stage: str, node_id: str) -> CodingApprovalDecision:
    return CodingApprovalDecision(
        approval_id=str(uuid5(NAMESPACE_URL, f"test-approval-{stage}")),
        node_id=node_id,
        stage=stage,
        stage_round=1,
        decision="APPROVED",
        candidate_sha=SHA,
        validation_hash=DIGEST,
        feedback=None,
        actor_id=ACTOR_ID,
        actor_role="SUPER_ADMIN",
        result_state_version=7,
        next_pipeline_attempt=None,
        decided_at=NOW,
    )


def _subject_aggregate(*, include_deploy: bool = True) -> CodingAttemptAggregate:
    preview = _record(
        result_id=str(uuid5(NAMESPACE_URL, "test-preview")),
        handler_key="coding.preview",
        result_type="DIFF",
        result_port="ready",
        candidate_sha=SHA,
        diff_digest=DIGEST,
        validation_hash=DIGEST,
    )
    pull_request = _record(
        result_id=str(uuid5(NAMESPACE_URL, "test-pr")),
        handler_key="coding.pr_request",
        result_type="PULL_REQUEST",
        result_port="requested",
        candidate_sha=SHA,
        validation_hash=DIGEST,
    )
    decisions = [
        _decision("CANDIDATE", "preview_approval"),
        _decision("GITHUB", "github_approval"),
        _decision("CMS", "cms_approval"),
    ]
    if include_deploy:
        decisions.append(_decision("DEPLOY", "deploy_approval"))
    return _aggregate(results=(preview, pull_request), decisions=tuple(decisions))


def _reviewed_aggregate(
    *,
    code_sha: str = SHA,
    review_sha: str = SHA,
    review_port: str = "passed",
) -> CodingAttemptAggregate:
    code = _record(
        result_id=str(uuid5(NAMESPACE_URL, "test-code")),
        handler_key="coding.code",
        result_type="CANDIDATE",
        result_port="completed",
        candidate_sha=code_sha,
    )
    review = _record(
        result_id=str(uuid5(NAMESPACE_URL, "test-review")),
        handler_key="coding.review",
        result_type="REVIEW",
        result_port=review_port,
        candidate_sha=review_sha,
    )
    return _aggregate(results=(code, review))


class _Domain:
    def __init__(self, attempt: CodingAttemptAggregate) -> None:
        self.attempt = attempt
        self.writes: list[CodingResultWrite] = []

    def get_attempt(self, invocation: NodeInvocation) -> CodingAttemptAggregate:
        del invocation
        return self.attempt

    def put_result(
        self, invocation: NodeInvocation, result: CodingResultWrite
    ) -> CodingResultRecord:
        self.writes.append(result)
        return _record(
            result_id=result.result_id,
            handler_key=result.handler_key,
            result_type=result.result_type,
            result_port=result.result_port,
            pipeline_attempt=invocation.pipeline_attempt,
            payload=dict(result.payload),
            candidate_sha=result.candidate_sha,
            diff_digest=result.diff_digest,
            validation_hash=result.validation_hash,
        )


class _FixedExecutor:
    def __init__(self, outcome: CodingStageOutcome) -> None:
        self.outcome = outcome
        self.result_ids: list[str] = []

    def execute(
        self,
        handler_key: str,
        invocation: NodeInvocation,
        attempt: CodingAttemptAggregate,
        result_id: str,
    ) -> CodingStageOutcome:
        del handler_key, invocation, attempt
        self.result_ids.append(result_id)
        return self.outcome


class _GatewayDomain(_Domain):
    def __init__(self, attempt: CodingAttemptAggregate) -> None:
        super().__init__(attempt)
        self.stage_calls: list[tuple[str, str]] = []

    def execute_stage(
        self, invocation: NodeInvocation, handler_key: str, result_id: str
    ) -> CodingStageExecutionResult:
        del invocation
        self.stage_calls.append((handler_key, result_id))
        return CodingStageExecutionResult(
            result_id=result_id,
            handler_key=handler_key,
            result_port="feasible",
            workspace_id=WORKSPACE_ID,
            payload={"summary": "bounded model/tool stage completed"},
        )


class CodingStageHandlerTest(unittest.TestCase):
    def test_external_side_effects_stop_before_executor_without_latest_approval(self) -> None:
        base = _subject_aggregate(include_deploy=False)
        pull_complete = _record(
            result_id=str(uuid5(NAMESPACE_URL, "approval-preflight-pr")),
            handler_key="coding.pr_complete",
            result_type="PULL_REQUEST",
            result_port="completed",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            payload={"repository": "backend", "prNumber": 42},
        )
        deploy_request = _record(
            result_id=str(uuid5(NAMESPACE_URL, "approval-preflight-deploy-request")),
            handler_key="coding.deploy_request",
            result_type="DEPLOY_REQUEST",
            result_port="recorded",
            candidate_sha=SHA,
            validation_hash=DEPLOY_DIGEST,
            payload={
                "deploymentRequestId": "81818181-8181-4181-8181-818181818181",
                "repository": "backend",
                "prNumber": 42,
            },
        )
        merged = _record(
            result_id=str(uuid5(NAMESPACE_URL, "approval-preflight-merge")),
            handler_key="coding.dev_merge_check",
            result_type="DEV_MERGE",
            result_port="merged",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            payload={"mergeSha": MERGE_SHA},
        )
        no_github = _aggregate(
            results=base.results,
            decisions=tuple(item for item in base.decisions if item.stage != "GITHUB"),
        )
        rejected_github = CodingApprovalDecision(
            approval_id=str(uuid5(NAMESPACE_URL, "latest-rejected-github")),
            node_id="github_approval",
            stage="GITHUB",
            stage_round=2,
            decision="REJECTED",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            feedback="do not create the PR",
            actor_id=ACTOR_ID,
            actor_role="SUPER_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=None,
            decided_at=NOW,
        )
        superseded_github = _aggregate(
            results=base.results,
            decisions=base.decisions + (rejected_github,),
        )
        cases = (
            ("coding.pr_complete", "pr_complete", {}, no_github),
            ("coding.pr_complete", "pr_complete", {}, superseded_github),
            (
                "coding.dev_merge_check",
                "dev_merge_check",
                {},
                _aggregate(
                    results=base.results + (pull_complete, deploy_request),
                    decisions=base.decisions,
                ),
            ),
            (
                "coding.deploy",
                "deploy",
                {},
                _aggregate(
                    results=base.results + (pull_complete, deploy_request, merged),
                    decisions=base.decisions,
                ),
            ),
        )
        for handler_key, node_id, config, aggregate in cases:
            with self.subTest(handler_key=handler_key):
                domain = _Domain(aggregate)
                executor = _FixedExecutor(CodingStageOutcome("completed"))
                handler = register_coding_node_handlers(
                    NodeRegistry(), CodingHandlerDependencies(domain, executor)
                ).resolve(handler_key).handler

                with self.assertRaises(GraphExecutionError) as raised:
                    handler(_invocation(node_id, config=config))

                self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
                self.assertEqual([], executor.result_ids)
                self.assertEqual([], domain.writes)

    def test_v4_deploy_request_subject_precedes_merge_and_deploy(self) -> None:
        base = _subject_aggregate(include_deploy=False)
        pull_complete = _record(
            result_id=str(uuid5(NAMESPACE_URL, "test-pr-complete")),
            handler_key="coding.pr_complete",
            result_type="PULL_REQUEST",
            result_port="completed",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            payload={
                "repository": "backend",
                "base": "dev",
                "head": "system/llmops-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "headSha": SHA_B,
                "candidateSha": SHA,
                "prNumber": 42,
                "prUrl": "https://github.example/pr/42",
            },
        )
        request_payload = {
            "deploymentRequestId": "81818181-8181-4181-8181-818181818181",
            "jobId": JOB_ID,
            "pipelineAttempt": 1,
            "repository": "backend",
            "prNumber": 42,
            "candidateSha": SHA,
            "sourceValidationHash": DIGEST,
            "adapterKey": "local-docker-compose",
            "targetKey": "full:backend:spring-app",
            "configDigest": DIGEST,
            "status": "DEPLOY_REQUEST_RECORDED",
        }
        request_domain = _Domain(
            _aggregate(
                results=base.results + (pull_complete,), decisions=base.decisions
            )
        )
        request_handler = register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(
                request_domain,
                _FixedExecutor(
                    CodingStageOutcome(
                        "recorded",
                        request_payload,
                        candidate_sha=SHA,
                        validation_hash=DEPLOY_DIGEST,
                    )
                ),
            ),
        ).resolve("coding.deploy_request").handler

        request_result = request_handler(
            _invocation("deploy_request", config={"mode": "request_record_only"})
        )

        self.assertEqual("recorded", request_result.port)
        self.assertEqual("DEPLOY_REQUEST", request_domain.writes[0].result_type)

        deploy_request = _record(
            result_id=request_domain.writes[0].result_id,
            handler_key="coding.deploy_request",
            result_type="DEPLOY_REQUEST",
            result_port="recorded",
            candidate_sha=SHA,
            validation_hash=DEPLOY_DIGEST,
            payload=request_payload,
        )
        deploy_decision = CodingApprovalDecision(
            approval_id=str(uuid5(NAMESPACE_URL, "test-deploy-approval-v4")),
            node_id="deploy_approval",
            stage="DEPLOY",
            stage_round=1,
            decision="APPROVED",
            candidate_sha=SHA,
            validation_hash=DEPLOY_DIGEST,
            feedback=None,
            actor_id=ACTOR_ID,
            actor_role="SUPER_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=None,
            decided_at=NOW,
        )
        merge_payload = {
            "repository": "backend",
            "base": "dev",
            "head": pull_complete.payload["head"],
            "headSha": pull_complete.payload["headSha"],
            "candidateSha": SHA,
            "prNumber": 42,
            "status": "MERGED",
            "mergeSha": MERGE_SHA,
        }
        merge_domain = _Domain(
            _aggregate(
                results=base.results + (pull_complete, deploy_request),
                decisions=base.decisions + (deploy_decision,),
            )
        )
        merge_handler = register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(
                merge_domain,
                _FixedExecutor(
                    CodingStageOutcome(
                        "merged",
                        merge_payload,
                        candidate_sha=SHA,
                        validation_hash=DIGEST,
                    )
                ),
            ),
        ).resolve("coding.dev_merge_check").handler

        merged = merge_handler(_invocation("dev_merge_check"))
        merge_write = merge_domain.writes[0]
        merge = _record(
            result_id=merge_write.result_id,
            handler_key=merge_write.handler_key,
            result_type=merge_write.result_type,
            result_port=merge_write.result_port,
            candidate_sha=merge_write.candidate_sha,
            validation_hash=merge_write.validation_hash,
            payload=dict(merge_write.payload),
        )
        self.assertEqual("merged", merged.port)
        self.assertEqual("DEV_MERGE", merge_write.result_type)

        deploy_domain = _Domain(
            _aggregate(
                results=base.results + (pull_complete, deploy_request, merge),
                decisions=base.decisions + (deploy_decision,),
            )
        )
        deploy_payload = {
            "deploymentRequestId": request_payload["deploymentRequestId"],
            "deploymentExecutionId": "91919191-9191-4191-8191-919191919191",
            "mergeSha": MERGE_SHA,
            "status": "COMPLETED",
        }
        deploy_handler = register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(
                deploy_domain,
                _FixedExecutor(
                    CodingStageOutcome(
                        "completed",
                        deploy_payload,
                        candidate_sha=SHA,
                        validation_hash=DEPLOY_DIGEST,
                    )
                ),
            ),
        ).resolve("coding.deploy").handler

        deployed = deploy_handler(_invocation("deploy"))

        self.assertEqual("completed", deployed.port)
        self.assertEqual("DEPLOYMENT", deploy_domain.writes[0].result_type)
        self.assertNotIn("deployedPort", deploy_domain.writes[0].payload)

    def test_spring_gateway_stage_is_recorded_through_existing_result_api(self) -> None:
        domain = _GatewayDomain(_aggregate())
        handler = register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(
                domain,
                SpringGatewayCodingStageExecutor(domain),
            ),
        ).resolve("coding.analyze").handler

        result = handler(_invocation("analyze"))

        self.assertEqual("feasible", result.port)
        self.assertEqual(1, len(domain.stage_calls))
        self.assertEqual("coding.analyze", domain.stage_calls[0][0])
        self.assertEqual(domain.stage_calls[0][1], domain.writes[0].result_id)
        self.assertEqual("ANALYSIS", domain.writes[0].result_type)
        self.assertEqual("feasible", domain.writes[0].result_port)

    def test_result_id_is_stable_across_technical_attempt_and_changes_by_round(
        self,
    ) -> None:
        domain = _Domain(_reviewed_aggregate())
        executor = _FixedExecutor(
            CodingStageOutcome(
                "ready",
                {"artifactRef": "candidate/preview"},
                workspace_id=WORKSPACE_ID,
                candidate_sha=SHA,
                diff_digest=DIGEST,
                validation_hash=DIGEST,
            )
        )
        registry = register_coding_node_handlers(
            NodeRegistry(), CodingHandlerDependencies(domain, executor)
        )
        handler = registry.resolve("coding.preview").handler
        invocation = _invocation("preview")

        first = handler(invocation)
        replay = handler(
            _invocation("preview", execution_attempt=3, state_version=8)
        )
        next_round = handler(
            _invocation("preview", context={"codingStageRounds": {"preview": 2}})
        )

        self.assertEqual("ready", first.port)
        self.assertEqual(first.updates["codingLastResult"], replay.updates["codingLastResult"])
        self.assertEqual(executor.result_ids[0], executor.result_ids[1])
        self.assertNotEqual(executor.result_ids[0], executor.result_ids[2])
        self.assertEqual("DIFF", domain.writes[0].result_type)
        self.assertEqual("ready", domain.writes[0].result_port)
        self.assertNotIn("artifactRef", first.updates["codingLastResult"])

    def test_review_result_must_match_latest_completed_code_candidate(self) -> None:
        source = _reviewed_aggregate()
        domain = _Domain(_aggregate(results=(source.results[0],)))
        executor = _FixedExecutor(
            CodingStageOutcome("passed", candidate_sha=SHA_B)
        )
        handler = register_coding_node_handlers(
            NodeRegistry(), CodingHandlerDependencies(domain, executor)
        ).resolve("coding.review").handler

        with self.assertRaises(GraphExecutionError) as raised:
            handler(_invocation("review"))

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
        self.assertEqual(1, len(executor.result_ids))
        self.assertEqual([], domain.writes)

    def test_preview_requires_latest_passed_review_of_latest_code(self) -> None:
        passed_different_candidate = _reviewed_aggregate(review_sha=SHA_B)
        changes_requested = _reviewed_aggregate(review_port="changes_requested")
        reviewed_then_reworked = _reviewed_aggregate()
        newer_code = _record(
            result_id=str(uuid5(NAMESPACE_URL, "test-newer-code")),
            handler_key="coding.code",
            result_type="CANDIDATE",
            result_port="completed",
            candidate_sha=SHA_B,
        )
        cases = (
            passed_different_candidate,
            changes_requested,
            _aggregate(results=reviewed_then_reworked.results + (newer_code,)),
        )
        for aggregate in cases:
            with self.subTest(results=len(aggregate.results)):
                domain = _Domain(aggregate)
                executor = _FixedExecutor(
                    CodingStageOutcome(
                        "ready",
                        candidate_sha=SHA,
                        diff_digest=DIGEST,
                        validation_hash=DIGEST,
                    )
                )
                handler = register_coding_node_handlers(
                    NodeRegistry(), CodingHandlerDependencies(domain, executor)
                ).resolve("coding.preview").handler

                with self.assertRaises(GraphExecutionError) as raised:
                    handler(_invocation("preview"))

                self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
                self.assertEqual([], executor.result_ids)
                self.assertEqual([], domain.writes)

    def test_preview_result_must_match_reviewed_candidate(self) -> None:
        domain = _Domain(_reviewed_aggregate())
        executor = _FixedExecutor(
            CodingStageOutcome(
                "ready",
                candidate_sha=SHA_B,
                diff_digest=DIGEST,
                validation_hash=DIGEST,
            )
        )
        handler = register_coding_node_handlers(
            NodeRegistry(), CodingHandlerDependencies(domain, executor)
        ).resolve("coding.preview").handler

        with self.assertRaises(GraphExecutionError) as raised:
            handler(_invocation("preview"))

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
        self.assertEqual(1, len(executor.result_ids))
        self.assertEqual([], domain.writes)

    def test_pr_and_deploy_require_approved_subject_before_executor(self) -> None:
        preview_only = _subject_aggregate()
        preview_only = _aggregate(results=(preview_only.results[0],))
        without_deploy = _subject_aggregate(include_deploy=False)
        cases = (
            (
                "coding.pr_request",
                "pr_request",
                {},
                preview_only,
                CodingStageOutcome(
                    "requested", candidate_sha=SHA, validation_hash=DIGEST
                ),
            ),
            (
                "coding.deploy_request",
                "deploy_request",
                {"mode": "request_record_only"},
                without_deploy,
                CodingStageOutcome(
                    "recorded", candidate_sha=SHA, validation_hash=DIGEST
                ),
            ),
        )
        for handler_key, node_id, config, aggregate, outcome in cases:
            with self.subTest(handler_key=handler_key):
                domain = _Domain(aggregate)
                executor = _FixedExecutor(outcome)
                handler = register_coding_node_handlers(
                    NodeRegistry(), CodingHandlerDependencies(domain, executor)
                ).resolve(handler_key).handler

                with self.assertRaises(GraphExecutionError) as raised:
                    handler(_invocation(node_id, config=config))

                self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
                self.assertEqual([], executor.result_ids)
                self.assertEqual([], domain.writes)

    def test_pr_and_deploy_reject_outcome_subject_drift_before_put(self) -> None:
        cases = (
            (
                "coding.pr_request",
                "pr_request",
                {},
                _subject_aggregate(),
                "requested",
            ),
            (
                "coding.deploy_request",
                "deploy_request",
                {"mode": "request_record_only"},
                _subject_aggregate(),
                "recorded",
            ),
        )
        for handler_key, node_id, config, aggregate, port in cases:
            with self.subTest(handler_key=handler_key):
                domain = _Domain(aggregate)
                executor = _FixedExecutor(
                    CodingStageOutcome(
                        port,
                        candidate_sha="sha1:" + ("b" * 40),
                        validation_hash=DIGEST,
                    )
                )
                handler = register_coding_node_handlers(
                    NodeRegistry(), CodingHandlerDependencies(domain, executor)
                ).resolve(handler_key).handler

                with self.assertRaises(GraphExecutionError) as raised:
                    handler(_invocation(node_id, config=config))

                self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
                self.assertEqual([], domain.writes)

    def test_prepared_result_executor_requires_exact_id_handler_and_type(self) -> None:
        invocation = _invocation("preview")
        result_id = str(uuid5(NAMESPACE_URL, "prepared-result"))
        executor = PreparedResultCodingStageExecutor()
        prepared = _record(
            result_id=result_id,
            handler_key="coding.preview",
            result_type="DIFF",
            result_port="ready",
            candidate_sha=SHA,
            diff_digest=DIGEST,
            validation_hash=DIGEST,
        )

        outcome = executor.execute(
            "coding.preview", invocation, _aggregate(results=(prepared,)), result_id
        )
        self.assertEqual("ready", outcome.port)

        wrong_type = _record(
            result_id=result_id,
            handler_key="coding.preview",
            result_type="CHECK",
            result_port="ready",
        )
        with self.assertRaises(CodingHandlerFailure) as raised:
            executor.execute(
                "coding.preview", invocation, _aggregate(results=(wrong_type,)), result_id
            )
        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)

        with self.assertRaises(CodingHandlerFailure) as missing:
            executor.execute("coding.preview", invocation, _aggregate(), result_id)
        self.assertEqual("HANDLER_RESULT_NOT_FOUND", missing.exception.code)

    def test_undeclared_executor_port_fails_closed(self) -> None:
        domain = _Domain(_aggregate())
        registry = register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(domain, _FixedExecutor(CodingStageOutcome("failed"))),
        )

        with self.assertRaises(GraphExecutionError) as raised:
            registry.resolve("coding.code").handler(_invocation("code"))

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
        self.assertEqual([], domain.writes)

    def test_handler_rejects_nested_result_from_another_attempt(self) -> None:
        foreign = _record(
            result_id=str(uuid5(NAMESPACE_URL, "foreign-result")),
            handler_key="coding.preview",
            result_type="DIFF",
            result_port="ready",
            pipeline_attempt=2,
            candidate_sha=SHA,
            diff_digest=DIGEST,
            validation_hash=DIGEST,
        )
        domain = _Domain(_aggregate(results=(foreign,)))
        executor = _FixedExecutor(CodingStageOutcome("ready"))
        handler = register_coding_node_handlers(
            NodeRegistry(), CodingHandlerDependencies(domain, executor)
        ).resolve("coding.preview").handler

        with self.assertRaises(GraphExecutionError) as raised:
            handler(_invocation("preview"))

        self.assertEqual("CONTRACT_VALIDATION_FAILED", raised.exception.code)
        self.assertEqual([], executor.result_ids)
        self.assertEqual([], domain.writes)


class CodingApprovalHandlerTest(unittest.TestCase):
    def _handler(
        self,
        handler_key: str,
        attempt: CodingAttemptAggregate,
    ) -> object:
        return register_coding_node_handlers(
            NodeRegistry(),
            CodingHandlerDependencies(
                _Domain(attempt), _FixedExecutor(CodingStageOutcome("completed"))
            ),
        ).resolve(handler_key).handler

    def test_general_admin_stage_accepts_super_admin_and_uses_exact_uuid(self) -> None:
        approval_id = _approval_id(
            pipeline_attempt=1, node_id="scope_approval", stage="SCOPE", stage_round=1
        )
        decision = CodingApprovalDecision(
            approval_id=approval_id,
            node_id="scope_approval",
            stage="SCOPE",
            stage_round=1,
            decision="APPROVED",
            candidate_sha=None,
            validation_hash=None,
            feedback=None,
            actor_id=ACTOR_ID,
            actor_role="SUPER_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=None,
            decided_at=NOW,
        )
        handler = self._handler("coding.approval", _aggregate(decisions=(decision,)))
        invocation = _invocation(
            "scope_approval",
            config={"stage": "SCOPE", "requiredRole": "GENERAL_ADMIN"},
        )
        interrupt_payloads: list[dict[str, object]] = []

        with patch(
            "axms_coding_orchestrator.coding_handlers.interrupt",
            side_effect=lambda payload: interrupt_payloads.append(payload) or True,
        ):
            result = handler(invocation)  # type: ignore[operator]

        self.assertEqual("approved", result.port)
        self.assertEqual(approval_id, interrupt_payloads[0]["approvalId"])
        self.assertEqual("SCOPE", interrupt_payloads[0]["stage"])
        self.assertEqual(1, interrupt_payloads[0]["stageRound"])
        self.assertEqual("GENERAL_ADMIN", interrupt_payloads[0]["requiredRole"])

    def test_super_admin_stage_rejects_general_admin_decision(self) -> None:
        decision = CodingApprovalDecision(
            approval_id=_approval_id(
                pipeline_attempt=1,
                node_id="github_approval",
                stage="GITHUB",
                stage_round=1,
            ),
            node_id="github_approval",
            stage="GITHUB",
            stage_round=1,
            decision="APPROVED",
            candidate_sha=None,
            validation_hash=None,
            feedback=None,
            actor_id=ACTOR_ID,
            actor_role="GENERAL_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=None,
            decided_at=NOW,
        )
        handler = self._handler("coding.approval", _aggregate(decisions=(decision,)))

        with patch("axms_coding_orchestrator.coding_handlers.interrupt", return_value=True):
            with self.assertRaises(GraphExecutionError) as raised:
                handler(  # type: ignore[operator]
                    _invocation(
                        "github_approval",
                        config={"stage": "GITHUB", "requiredRole": "SUPER_ADMIN"},
                    )
                )

        self.assertEqual("JOB_STATE_VERSION_CONFLICT", raised.exception.code)

    def test_snapshot_required_role_is_authority_for_every_stage(self) -> None:
        stages = (
            ("coding.approval", "scope_approval", "SCOPE"),
            ("coding.preview_approval", "preview_approval", "CANDIDATE"),
            ("coding.approval", "github_approval", "GITHUB"),
            ("coding.approval", "cms_approval", "CMS"),
            ("coding.approval", "deploy_approval", "DEPLOY"),
        )
        role_cases = (
            ("GENERAL_ADMIN", "GENERAL_ADMIN", True),
            ("GENERAL_ADMIN", "SUPER_ADMIN", True),
            ("SUPER_ADMIN", "SUPER_ADMIN", True),
            ("SUPER_ADMIN", "GENERAL_ADMIN", False),
        )
        source = _subject_aggregate()
        for handler_key, node_id, stage in stages:
            for required_role, actor_role, accepted in role_cases:
                decision = CodingApprovalDecision(
                    approval_id=_approval_id(
                        pipeline_attempt=1,
                        node_id=node_id,
                        stage=stage,
                        stage_round=1,
                    ),
                    node_id=node_id,
                    stage=stage,
                    stage_round=1,
                    decision="APPROVED",
                    candidate_sha=SHA,
                    validation_hash=DIGEST,
                    feedback=None,
                    actor_id=ACTOR_ID,
                    actor_role=actor_role,
                    result_state_version=7,
                    next_pipeline_attempt=None,
                    decided_at=NOW,
                )
                prior = tuple(
                    item for item in source.decisions if item.stage != stage
                )
                aggregate = _aggregate(
                    results=source.results,
                    decisions=(*prior, decision),
                )
                handler = self._handler(handler_key, aggregate)
                invocation = _invocation(
                    node_id,
                    config={"stage": stage, "requiredRole": required_role},
                )

                with self.subTest(
                    stage=stage,
                    required_role=required_role,
                    actor_role=actor_role,
                ), patch(
                    "axms_coding_orchestrator.coding_handlers.interrupt",
                    return_value=True,
                ):
                    if accepted:
                        self.assertEqual(
                            "approved",
                            handler(invocation).port,  # type: ignore[operator]
                        )
                    else:
                        with self.assertRaises(GraphExecutionError) as raised:
                            handler(invocation)  # type: ignore[operator]
                        self.assertEqual(
                            "JOB_STATE_VERSION_CONFLICT",
                            raised.exception.code,
                        )

    def test_github_approval_requires_prior_candidate_approval_for_same_subject(
        self,
    ) -> None:
        invocation = _invocation(
            "github_approval",
            config={"stage": "GITHUB", "requiredRole": "SUPER_ADMIN"},
        )
        github = CodingApprovalDecision(
            approval_id=_approval_id(
                pipeline_attempt=1,
                node_id="github_approval",
                stage="GITHUB",
                stage_round=1,
            ),
            node_id="github_approval",
            stage="GITHUB",
            stage_round=1,
            decision="APPROVED",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            feedback=None,
            actor_id=ACTOR_ID,
            actor_role="SUPER_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=None,
            decided_at=NOW,
        )
        source = _subject_aggregate()
        aggregate = _aggregate(results=source.results, decisions=(github,))
        handler = self._handler("coding.approval", aggregate)

        with patch("axms_coding_orchestrator.coding_handlers.interrupt", return_value=True):
            with self.assertRaises(GraphExecutionError) as raised:
                handler(invocation)  # type: ignore[operator]

        self.assertEqual("JOB_STATE_VERSION_CONFLICT", raised.exception.code)

    def test_candidate_rejection_matches_previous_attempt_uuid_and_advances(self) -> None:
        attempt_one_id = _approval_id(
            pipeline_attempt=1,
            node_id="preview_approval",
            stage="CANDIDATE",
            stage_round=1,
        )
        attempt_two_id = _approval_id(
            pipeline_attempt=2,
            node_id="preview_approval",
            stage="CANDIDATE",
            stage_round=1,
        )
        self.assertNotEqual(attempt_one_id, attempt_two_id)
        decision = CodingApprovalDecision(
            approval_id=attempt_one_id,
            node_id="preview_approval",
            stage="CANDIDATE",
            stage_round=1,
            decision="REJECTED",
            candidate_sha=SHA,
            validation_hash=DIGEST,
            feedback="Please revise.",
            actor_id=ACTOR_ID,
            actor_role="GENERAL_ADMIN",
            result_state_version=7,
            next_pipeline_attempt=2,
            decided_at=NOW,
        )
        handler = self._handler(
            "coding.preview_approval",
            _aggregate(pipeline_attempt=2, decisions=(decision,)),
        )

        with patch("axms_coding_orchestrator.coding_handlers.interrupt", return_value=True):
            result = handler(  # type: ignore[operator]
                _invocation(
                    "preview_approval",
                    pipeline_attempt=2,
                    config={
                        "stage": "CANDIDATE",
                        "requiredRole": "GENERAL_ADMIN",
                    },
                )
            )

        self.assertEqual("rejected", result.port)


if __name__ == "__main__":
    unittest.main()
