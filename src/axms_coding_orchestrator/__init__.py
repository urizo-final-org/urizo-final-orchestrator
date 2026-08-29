"""AX Module Studio Spring-authorized LangGraph coding runtime."""

import os

os.environ["LANGGRAPH_STRICT_MSGPACK"] = "true"

from .contracts import CodingJobRequested, ClaimSnapshot, WorkerClaim
from .graph import CodingGraphRunner, GraphDependencies, build_coding_graph

from .model_gateway import (
    ContractViolation,
    FileServiceCredentialResolver,
    ModelGatewayClient,
    ModelGatewayRemoteError,
    ModelTurnRequest,
    ModelTurnResponse,
    ServiceCredentialLease,
)
from .snapshot import (
    SnapshotConfig,
    SnapshotContractViolation,
    SnapshotEdge,
    SnapshotLoopLimit,
    SnapshotNode,
    VersionedSnapshot,
    load_snapshot_json,
    validate_snapshot,
)
from .node_runtime import (
    NodeContractViolation,
    NodeHandler,
    NodeHandlerRegistration,
    NodeInvocation,
    NodeRegistry,
    NodeRegistryViolation,
    NodeResult,
)
from .graph_builder import (
    SnapshotGraphBuildError,
    SnapshotGraphBuilder,
    SnapshotGraphExecutionError,
)
from .snapshot_runner import (
    CodingGraphRunnerAdapter,
    SnapshotExecution,
    SnapshotExecutionProvider,
    SnapshotGraphRunner,
    WorkerGraphRunner,
)
from .tool_gateway import ToolExecutionResult, ToolGatewayClient
from .worker_api import WorkerApiClient

__all__ = [
    "ContractViolation",
    "CodingGraphRunner",
    "CodingGraphRunnerAdapter",
    "CodingJobRequested",
    "ClaimSnapshot",
    "FileServiceCredentialResolver",
    "GraphDependencies",
    "ModelGatewayClient",
    "ModelGatewayRemoteError",
    "ModelTurnRequest",
    "ModelTurnResponse",
    "NodeContractViolation",
    "NodeHandler",
    "NodeHandlerRegistration",
    "NodeInvocation",
    "NodeRegistry",
    "NodeRegistryViolation",
    "NodeResult",
    "SnapshotGraphBuildError",
    "SnapshotGraphBuilder",
    "SnapshotGraphExecutionError",
    "SnapshotExecution",
    "SnapshotExecutionProvider",
    "SnapshotGraphRunner",
    "ServiceCredentialLease",
    "SnapshotConfig",
    "SnapshotContractViolation",
    "SnapshotEdge",
    "SnapshotLoopLimit",
    "SnapshotNode",
    "ToolExecutionResult",
    "ToolGatewayClient",
    "VersionedSnapshot",
    "WorkerApiClient",
    "WorkerClaim",
    "WorkerGraphRunner",
    "build_coding_graph",
    "load_snapshot_json",
    "validate_snapshot",
]
