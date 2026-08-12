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
from .tool_gateway import ToolExecutionResult, ToolGatewayClient
from .worker_api import WorkerApiClient

__all__ = [
    "ContractViolation",
    "CodingGraphRunner",
    "CodingJobRequested",
    "ClaimSnapshot",
    "FileServiceCredentialResolver",
    "GraphDependencies",
    "ModelGatewayClient",
    "ModelGatewayRemoteError",
    "ModelTurnRequest",
    "ModelTurnResponse",
    "ServiceCredentialLease",
    "ToolExecutionResult",
    "ToolGatewayClient",
    "WorkerApiClient",
    "WorkerClaim",
    "build_coding_graph",
]
