"""Approved minimum NATURAL_CMS Snapshot for AI05-001-01."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .snapshot import VersionedSnapshot


DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID = "a5050010-1001-5001-8001-000000000001"
CMS_TOOL_NAMES = (
    "resolve_cms_target",
    "validate_cms_command",
    "create_cms_preview",
    "discard_cms_preview",
    "revalidate_cms_preview",
    "apply_cms_preview",
)


_DEFAULT_NATURAL_CMS_SNAPSHOT: dict[str, Any] = {
    "contractVersion": "1.0",
    "profileVersionId": DEFAULT_NATURAL_CMS_PROFILE_VERSION_ID,
    "profileKey": "NATURAL_CMS",
    "profileVersion": 1,
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
            "handlerKey": "cms.analyze",
            "resultPorts": ["feasible", "infeasible"],
            "config": {},
        },
        {
            "id": "preview",
            "type": "agent",
            "handlerKey": "cms.preview",
            "resultPorts": ["ready"],
            "config": {},
        },
        {
            "id": "approval",
            "type": "approval",
            "handlerKey": "cms.approval",
            "resultPorts": ["approved", "rejected"],
            "config": {"stage": "PREVIEW", "requiredRole": "GENERAL_ADMIN"},
        },
        {
            "id": "discard",
            "type": "tool",
            "handlerKey": "cms.discard",
            "resultPorts": ["retry", "discarded"],
            "config": {},
        },
        {
            "id": "apply",
            "type": "tool",
            "handlerKey": "cms.apply",
            "resultPorts": ["applied"],
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
        {"from": "analyze", "resultPort": "feasible", "to": "preview"},
        {"from": "analyze", "resultPort": "infeasible", "to": "end"},
        {"from": "preview", "resultPort": "ready", "to": "approval"},
        {"from": "approval", "resultPort": "approved", "to": "apply"},
        {"from": "approval", "resultPort": "rejected", "to": "discard"},
        {"from": "discard", "resultPort": "retry", "to": "analyze"},
        {"from": "discard", "resultPort": "discarded", "to": "end"},
        {"from": "apply", "resultPort": "applied", "to": "end"},
    ],
    "config": {
        "maxNodes": 8,
        "maxAttempts": 3,
        "loopLimits": [
            {
                "from": "discard",
                "resultPort": "retry",
                "to": "analyze",
                "maxIterations": 2,
            }
        ],
    },
    "modelBindings": {
        "analyze": {"primary": "natural-cms-analyze", "fallback": []},
        "preview": {"primary": "natural-cms-command", "fallback": []},
    },
    "toolPolicy": {"allowedTools": list(CMS_TOOL_NAMES)},
    "guardrailProfileKey": "central.default",
}


def default_natural_cms_snapshot_dict() -> dict[str, Any]:
    return deepcopy(_DEFAULT_NATURAL_CMS_SNAPSHOT)


def default_natural_cms_snapshot() -> VersionedSnapshot:
    return VersionedSnapshot.from_dict(default_natural_cms_snapshot_dict())


def default_natural_cms_snapshot_json() -> bytes:
    return default_natural_cms_snapshot().to_json()
