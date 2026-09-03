"""Approved AI04-002 default LLM_OPS Snapshot proposal."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .snapshot import VersionedSnapshot


# A new graph is a new Profile Version. The Backend seed keys on
# profile_version_id with ON CONFLICT DO NOTHING and then requires the stored
# snapshot_json to equal the seeded text, so reusing an id with changed
# content breaks every environment that already seeded the old one.
# uuid5(NAMESPACE_URL, "axms:LLM_OPS:profile-version:4:pr-deploy-profile")
DEFAULT_CODING_PROFILE_VERSION_ID = "dc495130-0146-5f01-8e97-3c3272ad62b0"
DEFAULT_CODING_PROFILE_VERSION = 4
CODING_TOOL_NAMES = (
    "read_file",
    "search_code",
    "read_diff",
    "apply_patch",
    "run_check",
    "check_package_allowlist",
    "scan_changed_files",
)


_DEFAULT_CODING_SNAPSHOT: dict[str, Any] = {
    "contractVersion": "1.0",
    "profileVersionId": DEFAULT_CODING_PROFILE_VERSION_ID,
    "profileKey": "LLM_OPS",
    "profileVersion": DEFAULT_CODING_PROFILE_VERSION,
    "nodes": [
        {
            "id": "start",
            "type": "start",
            "handlerKey": "common.start",
            "resultPorts": ["next"],
            "config": {},
        },
        {
            "id": "guardrail",
            "type": "guardrail",
            "handlerKey": "common.guardrail",
            "resultPorts": ["passed", "failed"],
            "config": {"locked": True},
        },
        {
            "id": "analyze",
            "type": "agent",
            "handlerKey": "coding.analyze",
            "resultPorts": ["feasible", "infeasible"],
            "config": {},
        },
        {
            "id": "scope_approval",
            "type": "approval",
            "handlerKey": "coding.approval",
            "resultPorts": ["approved"],
            "config": {"stage": "SCOPE", "requiredRole": "GENERAL_ADMIN"},
        },
        {
            "id": "code",
            "type": "agent",
            "handlerKey": "coding.code",
            "resultPorts": ["completed"],
            "config": {},
        },
        {
            "id": "review",
            "type": "agent",
            "handlerKey": "coding.review",
            "resultPorts": ["passed", "changes_requested"],
            "config": {},
        },
        {
            "id": "rework_gate",
            "type": "check",
            "handlerKey": "coding.rework_gate",
            "resultPorts": ["retry", "handover"],
            "config": {"maxReworkRounds": 3},
        },
        {
            "id": "preview",
            "type": "tool",
            "handlerKey": "coding.preview",
            "resultPorts": ["ready"],
            "config": {},
        },
        {
            "id": "preview_approval",
            "type": "approval",
            "handlerKey": "coding.preview_approval",
            "resultPorts": ["approved", "rejected"],
            "config": {"stage": "CANDIDATE", "requiredRole": "GENERAL_ADMIN"},
        },
        {
            "id": "pr_request",
            "type": "tool",
            "handlerKey": "coding.pr_request",
            "resultPorts": ["requested"],
            "config": {},
        },
        {
            "id": "github_approval",
            "type": "approval",
            "handlerKey": "coding.approval",
            "resultPorts": ["approved"],
            "config": {"stage": "GITHUB", "requiredRole": "SUPER_ADMIN"},
        },
        {
            "id": "pr_complete",
            "type": "tool",
            "handlerKey": "coding.pr_complete",
            "resultPorts": ["completed"],
            "config": {},
        },
        {
            "id": "deploy_request",
            "type": "tool",
            "handlerKey": "coding.deploy_request",
            "resultPorts": ["recorded"],
            "config": {"mode": "request_record_only"},
        },
        {
            "id": "deploy_approval",
            "type": "approval",
            "handlerKey": "coding.approval",
            "resultPorts": ["approved"],
            "config": {"stage": "DEPLOY", "requiredRole": "SUPER_ADMIN"},
        },
        {
            "id": "dev_merge_check",
            "type": "check",
            "handlerKey": "coding.dev_merge_check",
            "resultPorts": ["merged", "not_merged", "blocked"],
            "config": {},
        },
        {
            "id": "deploy",
            "type": "tool",
            "handlerKey": "coding.deploy",
            "resultPorts": ["completed", "blocked"],
            "config": {},
        },
        {
            "id": "end",
            "type": "end",
            "handlerKey": "common.end",
            "resultPorts": [],
            "config": {},
        },
    ],
    "edges": [
        {"from": "start", "resultPort": "next", "to": "guardrail"},
        {"from": "guardrail", "resultPort": "passed", "to": "analyze"},
        {"from": "guardrail", "resultPort": "failed", "to": "end"},
        {"from": "analyze", "resultPort": "feasible", "to": "scope_approval"},
        {"from": "analyze", "resultPort": "infeasible", "to": "end"},
        {"from": "scope_approval", "resultPort": "approved", "to": "code"},
        {"from": "code", "resultPort": "completed", "to": "review"},
        {"from": "review", "resultPort": "passed", "to": "preview"},
        {
            "from": "review",
            "resultPort": "changes_requested",
            "to": "rework_gate",
        },
        {"from": "rework_gate", "resultPort": "retry", "to": "code"},
        {"from": "rework_gate", "resultPort": "handover", "to": "end"},
        {"from": "preview", "resultPort": "ready", "to": "preview_approval"},
        {
            "from": "preview_approval",
            "resultPort": "approved",
            "to": "pr_request",
        },
        {
            "from": "preview_approval",
            "resultPort": "rejected",
            "to": "analyze",
        },
        {"from": "pr_request", "resultPort": "requested", "to": "github_approval"},
        {"from": "github_approval", "resultPort": "approved", "to": "pr_complete"},
        {"from": "pr_complete", "resultPort": "completed", "to": "deploy_request"},
        {
            "from": "deploy_request",
            "resultPort": "recorded",
            "to": "deploy_approval",
        },
        {
            "from": "deploy_approval",
            "resultPort": "approved",
            "to": "dev_merge_check",
        },
        {"from": "dev_merge_check", "resultPort": "not_merged", "to": "deploy_request"},
        {"from": "dev_merge_check", "resultPort": "merged", "to": "deploy"},
        {"from": "dev_merge_check", "resultPort": "blocked", "to": "end"},
        {"from": "deploy", "resultPort": "completed", "to": "end"},
        {"from": "deploy", "resultPort": "blocked", "to": "end"},
    ],
    "config": {
        "maxNodes": 17,
        "maxAttempts": 3,
        "loopLimits": [
            {
                "from": "rework_gate",
                "resultPort": "retry",
                "to": "code",
                "maxIterations": 2,
            },
            {
                "from": "preview_approval",
                "resultPort": "rejected",
                "to": "analyze",
                "maxIterations": 2,
            },
            {
                "from": "dev_merge_check",
                "resultPort": "not_merged",
                "to": "deploy_request",
                "maxIterations": 2,
            },
        ],
    },
    "modelBindings": {
        "analyze": {"primary": "llm-ops-analyze", "fallback": []},
        "code": {"primary": "llm-ops-code", "fallback": []},
        "review": {"primary": "llm-ops-review", "fallback": []},
    },
    "toolBindings": {
        "code": {
            "read_file": "MODEL_OPTIONAL",
            "search_code": "MODEL_OPTIONAL",
            "read_diff": "MODEL_OPTIONAL",
            "apply_patch": "MODEL_OPTIONAL",
            "run_check": "MODEL_OPTIONAL",
            "check_package_allowlist": "MODEL_OPTIONAL",
            "scan_changed_files": "MODEL_OPTIONAL",
        },
        "review": {
            "read_file": "MODEL_OPTIONAL",
            "search_code": "MODEL_OPTIONAL",
            "read_diff": "MODEL_OPTIONAL",
            "run_check": "MODEL_OPTIONAL",
            "check_package_allowlist": "MODEL_OPTIONAL",
            "scan_changed_files": "MODEL_OPTIONAL",
        },
        "preview": {
            "read_diff": "SYSTEM_REQUIRED",
            "run_check": "SYSTEM_REQUIRED",
            "check_package_allowlist": "SYSTEM_REQUIRED",
            "scan_changed_files": "SYSTEM_REQUIRED",
        },
    },
    "toolPolicy": {"allowedTools": list(CODING_TOOL_NAMES)},
    "guardrailProfileKey": "central.default",
}


def default_coding_snapshot_dict() -> dict[str, Any]:
    """Return the exact Backend-seedable proposal without shared mutable state."""

    return deepcopy(_DEFAULT_CODING_SNAPSHOT)


def default_coding_snapshot() -> VersionedSnapshot:
    return VersionedSnapshot.from_dict(default_coding_snapshot_dict())


def default_coding_snapshot_json() -> bytes:
    return default_coding_snapshot().to_json()
