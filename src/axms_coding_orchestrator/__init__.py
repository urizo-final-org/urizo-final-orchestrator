"""AX Module Studio Spring-authorized LangGraph coding runtime."""

import os

os.environ["LANGGRAPH_STRICT_MSGPACK"] = "true"

from .contracts import (
    ClaimSnapshot,
    CodingJobRequested,
    QueuedJobReference,
    WorkerClaim,
)
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
from .common_handlers import build_common_node_registry
from .coding_domain_client import (
    CodingApprovalDecision,
    CodingAttemptAggregate,
    CodingDomainClientError,
    CodingPendingApproval,
    CodingResultRecord,
    CodingResultWrite,
    SpringCodingDomainClient,
)
from .coding_handlers import (
    CODING_HANDLER_CONTRACTS,
    CodingHandlerDependencies,
    CodingHandlerFailure,
    CodingStageOutcome,
    PreparedResultCodingStageExecutor,
    register_coding_node_handlers,
)
from .default_coding_snapshot import (
    CODING_TOOL_NAMES,
    DEFAULT_CODING_PROFILE_VERSION,
    DEFAULT_CODING_PROFILE_VERSION_ID,
    default_coding_snapshot,
    default_coding_snapshot_dict,
    default_coding_snapshot_json,
)
from .snapshot_runner import (
    CodingGraphRunnerAdapter,
    ProfileBoundWorkerGraphRouter,
    SnapshotExecution,
    SnapshotExecutionProvider,
    SnapshotGraphRunner,
    WorkerGraphRunner,
)
from .profile_version_client import ProfileVersionClient, ProfileVersionClientError
from .spring_snapshot_provider import SpringSnapshotExecutionProvider
from .tool_gateway import ToolExecutionResult, ToolGatewayClient
from .worker_api import WorkerApiClient

__all__ = [
    "ContractViolation",
    "CODING_HANDLER_CONTRACTS",
    "CODING_TOOL_NAMES",
    "CodingApprovalDecision",
    "CodingAttemptAggregate",
    "CodingDomainClientError",
    "CodingGraphRunner",
    "CodingGraphRunnerAdapter",
    "CodingJobRequested",
    "CodingHandlerDependencies",
    "CodingHandlerFailure",
    "CodingPendingApproval",
    "CodingResultRecord",
    "CodingResultWrite",
    "CodingStageOutcome",
    "ClaimSnapshot",
    "FileServiceCredentialResolver",
    "DEFAULT_CODING_PROFILE_VERSION",
    "DEFAULT_CODING_PROFILE_VERSION_ID",
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
    "ProfileVersionClient",
    "ProfileVersionClientError",
    "PreparedResultCodingStageExecutor",
    "ProfileBoundWorkerGraphRouter",
    "QueuedJobReference",
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
    "SpringSnapshotExecutionProvider",
    "SpringCodingDomainClient",
    "ToolExecutionResult",
    "ToolGatewayClient",
    "VersionedSnapshot",
    "WorkerApiClient",
    "WorkerClaim",
    "WorkerGraphRunner",
    "build_coding_graph",
    "build_common_node_registry",
    "default_coding_snapshot",
    "default_coding_snapshot_dict",
    "default_coding_snapshot_json",
    "load_snapshot_json",
    "validate_snapshot",
    "register_coding_node_handlers",
]
