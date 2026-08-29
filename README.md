# AX Module Studio Orchestrator

Python LangGraph coding-agent runtime repository for AX Module Studio.

## Current state

The local/full-profile runtime has one persistent LangGraph:

1. Atomically `BLMOVE RIGHT -> LEFT` from `axms:coding:jobs:v1` to its
   versioned processing list and strictly validate the Backend-owned
   `CODING_JOB_REQUESTED` envelope.
2. Claim the authoritative Spring Job lease and start bounded heartbeats.
3. Ask Spring Model Turn for one `read_file` candidate.
4. Submit that candidate to the Spring Tool Gateway; Python never opens the
   repository or executes a Tool.
5. Persist an encrypted checkpoint, report `WAITING_APPROVAL`, and interrupt.
6. Resume the same `thread_id == jobId` from a new Spring-approved claim and
   report `COMPLETED`.

Spring/Core PostgreSQL remains authoritative for actors, projects,
repositories, Job state/version, Provider selection and credentials, Tool
allowlisting/PathPolicy, execution, approval, and idempotency. This process has
no Core DB configuration, Provider SDK/key, Provider selection, or local Tool
implementation. Configuration rejects known Provider/Core-DB authority
variables and pins Spring, Valkey, and the exclusive Checkpoint PostgreSQL
service identities.

Checkpoint values use authenticated AES encryption with a raw 32-byte local
secret and strict LangGraph msgpack deserialization. Model and Tool side-effect
IDs remain stable across worker-lease replacement. Event IDs and
`jobId + expectedStateVersion` are kept in the checkpoint ledger so duplicate
queue delivery does not repeat completed graph work.

The processing-list entry is acknowledged only after `WAITING_APPROVAL`,
`COMPLETED`, or a failure outcome is accepted by Spring. Unclaimed work is
atomically requeued, and startup moves stale processing entries back to the
source list in oldest-first order. The queue payload is never rewritten, so the
strict Backend event remains the sole delivery contract.

An immutable Versioned Profile Snapshot contract now validates `nodes`,
`edges`, `config`, `handlerKey`, declared Result Ports, mandatory locked
Guardrail passage, and bounded loop declarations. A source-only Node Registry,
immutable Node Invocation/Result contracts, and an in-memory Graph Builder can
compile those validated Snapshots with exact Node Type/Port binding and bounded
loop enforcement. An injected execution provider and Worker-compatible Snapshot
Runner preserve `thread_id == jobId`, checkpoint idempotency, failed-node retry,
and approval interrupt/resume for fixture handlers. The production service still
uses the current Coding runner through its compatibility Adapter; Spring Profile
lookup and live Snapshot selection are not connected yet.

`/health/live` reports process liveness. `/health/ready` dynamically probes the
Checkpoint DB, Valkey, and Spring on every request and returns `503` if any
required dependency is unavailable.

## Container contract

- service: `coding-runtime`
- command: `python -m axms_coding_orchestrator.service`
- internal port: `8090`
- health endpoints: `/health/live`, `/health/ready`
- Python: `3.12.13`
- dependency lock: `uv.lock`

Required local/full environment and secret-file paths:

```text
AXMS_SPRING_ORIGIN=http://spring-app:8080
AXMS_SPRING_CREDENTIAL_FILE=/run/secrets/coding_model_bridge_service_token
AXMS_CHECKPOINT_HOST=checkpoint_database
AXMS_CHECKPOINT_PORT=5432
AXMS_CHECKPOINT_DATABASE=axms_langgraph
AXMS_CHECKPOINT_USER=axms_checkpoint
AXMS_CHECKPOINT_PASSWORD_FILE=/run/secrets/checkpoint_postgres_password
AXMS_CHECKPOINT_ENCRYPTION_KEY_FILE=/run/secrets/checkpoint_encryption_key
AXMS_VALKEY_HOST=valkey
AXMS_VALKEY_PORT=6379
AXMS_VALKEY_DATABASE=0
AXMS_VALKEY_PASSWORD_FILE=/run/secrets/valkey_password
AXMS_HEALTH_HOST=0.0.0.0
AXMS_HEALTH_PORT=8090
LANGGRAPH_STRICT_MSGPACK=true
```

The Spring service token is read for every request and held in an erasable
short-lived buffer. Secret contents, prompts, raw Tool results, remote error
bodies, and credential digests are never logged.

## Local verification

Run the repository-owned Python gates from this Orchestrator repository:

```powershell
uv sync --frozen --python 3.12.13
$env:PYTHONPATH = 'src'
uv run --frozen python -B -m unittest discover -s tests -v
```

Tests cover immutable Versioned Snapshot loading and validation, Registry and
Graph Builder linear/branch/bounded-loop contracts, Snapshot Runner checkpoint
compatibility, Backend golden Model Turn payloads, exact-origin and credential
handling, queue/claim/lease contracts, Tool request/result binding, encrypted
serialization, persistent interrupt/resume, duplicate suppression,
retry/backoff, lease loss, and dynamic dependency readiness.

The Backend repository owns the primary local/full-profile acceptance. Run
these commands from the sibling `urizo-final-backend` repository after the
approved local secrets are present:

```powershell
.\scripts\bootstrap-dev.ps1 -Profile full
.\scripts\verify-full-local-e2e.ps1 -Profile full
.\scripts\verify-full-local-restart.ps1 -ConfirmRestart
.\scripts\verify-full-local-failure-gates.ps1 -ConfirmFailureInjection
```

Those gates own the seven-service Compose topology, Flyway one-shot execution,
Frontend-through-Nginx routing, Spring/Core DB/Valkey integration, the coding
Job interrupt/resume lifecycle, preserved-volume restart/idempotency, and
bounded dependency failure/recovery checks.

Latest verified Orchestrator evidence:

- Python contract/runtime suite: 96 of 96 tests passed.
- Syntax gate: 34 Python files parsed successfully.
- The frozen `uv.lock` image built with Python 3.12.13 and ran as non-root UID
  10001.
- Full Compose `coding-runtime` returned HTTP 200 from both `/health/live` and
  `/health/ready`; Checkpoint PostgreSQL, Valkey queue, Spring, and the worker
  loop all reported `UP`.

The Backend script `scripts/verify-local-model-turn-roundtrip.ps1` is a
supplementary Model Turn contract smoke, not the primary full-profile gate. It
starts a separate loopback Spring process with the
`dev,coding-job-local-fixture,coding-model-turn-local-mock` profiles, creates
and starts an authoritative Job, and calls
`tests/verify_local_model_turn_roundtrip.py` with the returned state version.
It verifies a real HTTP request and DB-backed idempotent replay without any
Provider call.

## Team policy authority

Cross-repository workflow, current Wave/Slice state, assignments, and Git/PR policy are owned by the
sibling Master repository. Start from the canonical parent workspace and follow
`../urizo-final-master/AGENTS.md`; this README contains only Orchestrator runtime and verification facts.
