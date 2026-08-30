"""Approved AI04-002 default LLM_OPS Snapshot proposal."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .snapshot import VersionedSnapshot


DEFAULT_CODING_PROFILE_VERSION_ID = "d3d41f73-9a07-51e5-9ec8-4ed8aca7f7cb"
DEFAULT_CODING_PROFILE_VERSION = 2
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
            "id": "cms_approval",
            "type": "approval",
            "handlerKey": "coding.approval",
            "resultPorts": ["approved"],
            "config": {"stage": "CMS", "requiredRole": "GENERAL_ADMIN"},
        },
        {
            "id": "deploy_approval",
            "type": "approval",
            "handlerKey": "coding.approval",
            "resultPorts": ["approved"],
            "config": {"stage": "DEPLOY", "requiredRole": "SUPER_ADMIN"},
        },
        {
            "id": "deploy_request",
            "type": "tool",
            "handlerKey": "coding.deploy_request",
            "resultPorts": ["recorded"],
            "config": {"mode": "request_record_only"},
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
            "to": "code",
        },
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
        {"from": "github_approval", "resultPort": "approved", "to": "cms_approval"},
        {"from": "cms_approval", "resultPort": "approved", "to": "deploy_approval"},
        {
            "from": "deploy_approval",
            "resultPort": "approved",
            "to": "deploy_request",
        },
        {"from": "deploy_request", "resultPort": "recorded", "to": "end"},
    ],
    "config": {
        "maxNodes": 14,
        "maxAttempts": 3,
        "loopLimits": [
            {
                "from": "review",
                "resultPort": "changes_requested",
                "to": "code",
                "maxIterations": 2,
            },
            {
                "from": "preview_approval",
                "resultPort": "rejected",
                "to": "analyze",
                "maxIterations": 2,
            },
        ],
    },
    "modelBindings": {
        "analyze": {"primary": "llm-ops-analyze", "fallback": []},
        "code": {"primary": "llm-ops-code", "fallback": []},
        "review": {"primary": "llm-ops-review", "fallback": []},
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
