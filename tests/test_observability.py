from __future__ import annotations

from types import SimpleNamespace
import unittest

from axms_coding_orchestrator.coding_domain_client import _current_traceparent
from axms_coding_orchestrator.observability import (
    ALLOWED_METADATA_KEYS,
    AxmsObservability,
)


JOB_ID = "11111111-1111-4111-8111-111111111111"
TRACE_ID = "22222222-2222-4222-8222-222222222222"
PROFILE_VERSION_ID = "33333333-3333-4333-8333-333333333333"


class _FakeManager:
    def __init__(self, observation: _FakeObservation, *, auto_end: bool) -> None:
        self.observation = observation
        self.auto_end = auto_end

    def __enter__(self) -> _FakeObservation:
        return self.observation

    def __exit__(self, *_args: object) -> None:
        if self.auto_end:
            self.observation.end()


class _FakeObservation:
    def __init__(self, client: _FakeClient, name: str, metadata: dict[str, object]) -> None:
        self.client = client
        self.name = name
        self.metadata = dict(metadata)
        self.ended = False

    def start_observation(self, **kwargs: object) -> _FakeObservation:
        return self.client._start(**kwargs)

    def start_as_current_observation(self, **kwargs: object) -> _FakeManager:
        return _FakeManager(self.client._start(**kwargs), auto_end=True)

    def update(self, **kwargs: object) -> None:
        if self.client.fail_update:
            raise RuntimeError("export detail must stay fail-open")
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict):
            self.metadata = dict(metadata)

    def end(self) -> None:
        self.ended = True
        if self.client.fail_end:
            raise RuntimeError("export close must stay fail-open")


class _FakeClient:
    def __init__(
        self,
        *,
        fail_start: bool = False,
        fail_update: bool = False,
        fail_end: bool = False,
        fail_shutdown: bool = False,
    ) -> None:
        self.fail_start = fail_start
        self.fail_update = fail_update
        self.fail_end = fail_end
        self.fail_shutdown = fail_shutdown
        self.observations: list[_FakeObservation] = []
        self.start_arguments: list[dict[str, object]] = []
        self.shutdown_called = False

    def create_trace_id(self, *, seed: str) -> str:
        self.seed = seed
        return "a" * 32

    def start_as_current_observation(self, **kwargs: object) -> _FakeManager:
        end_on_exit = bool(kwargs.pop("end_on_exit", True))
        return _FakeManager(self._start(**kwargs), auto_end=end_on_exit)

    def _start(self, **kwargs: object) -> _FakeObservation:
        if self.fail_start:
            raise RuntimeError("start must stay fail-open")
        self.start_arguments.append(dict(kwargs))
        metadata = kwargs.get("metadata")
        observation = _FakeObservation(
            self,
            str(kwargs["name"]),
            dict(metadata) if isinstance(metadata, dict) else {},
        )
        self.observations.append(observation)
        return observation

    def shutdown(self) -> None:
        self.shutdown_called = True
        if self.fail_shutdown:
            raise RuntimeError("shutdown must stay fail-open")


def _invocation(node_id: str = "analyze") -> SimpleNamespace:
    return SimpleNamespace(
        job_id=JOB_ID,
        trace_id=TRACE_ID,
        profile_version_id=PROFILE_VERSION_ID,
        node_id=node_id,
        execution_attempt=2,
        context={
            "prompt": "FORBIDDEN_PROMPT",
            "source": "FORBIDDEN_SOURCE",
            "toolOutput": "FORBIDDEN_TOOL_OUTPUT",
        },
    )


class ObservabilityTest(unittest.TestCase):
    def test_environment_activation_is_exact_and_initialization_is_fail_open(self) -> None:
        calls: list[dict[str, object]] = []

        def factory(**kwargs: object) -> _FakeClient:
            calls.append(dict(kwargs))
            return _FakeClient()

        disabled = AxmsObservability.from_environment({}, client_factory=factory)
        partial = AxmsObservability.from_environment(
            {"LANGFUSE_PUBLIC_KEY": "public-test"}, client_factory=factory
        )
        invalid_host = AxmsObservability.from_environment(
            {
                "LANGFUSE_PUBLIC_KEY": "public-test",
                "LANGFUSE_SECRET_KEY": "secret-test",
                "LANGFUSE_BASE_URL": "https://cloud.langfuse.com",
            },
            client_factory=factory,
        )
        enabled = AxmsObservability.from_environment(
            {
                "LANGFUSE_PUBLIC_KEY": "public-test",
                "LANGFUSE_SECRET_KEY": "secret-test",
                "LANGFUSE_BASE_URL": "https://jp.cloud.langfuse.com",
            },
            client_factory=factory,
        )
        failed = AxmsObservability.from_environment(
            {
                "LANGFUSE_PUBLIC_KEY": "public-test",
                "LANGFUSE_SECRET_KEY": "secret-test",
                "LANGFUSE_BASE_URL": "https://jp.cloud.langfuse.com",
            },
            client_factory=lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("initialization failure")
            ),
        )

        self.assertFalse(disabled.enabled)
        self.assertFalse(partial.enabled)
        self.assertFalse(invalid_host.enabled)
        self.assertTrue(enabled.enabled)
        self.assertFalse(failed.enabled)
        self.assertEqual(
            [{
                "public_key": "public-test",
                "secret_key": "secret-test",
                "base_url": "https://jp.cloud.langfuse.com",
                "environment": "local",
            }],
            calls,
        )

    def test_python_emits_payload_free_job_node_tool_and_check_only(self) -> None:
        client = _FakeClient()
        observability = AxmsObservability(client)
        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=2,
        ) as job:
            observability.invoke_node(
                node=SimpleNamespace(node_type="tool"),
                invocation=_invocation("preview"),
                handler=lambda _invocation: SimpleNamespace(port="blocked"),
            )
            observability.invoke_node(
                node=SimpleNamespace(node_type="check"),
                invocation=_invocation("check"),
                handler=lambda _invocation: SimpleNamespace(port="passed"),
            )
            job.finish("COMPLETED")

        self.assertEqual(
            ["axms.job", "axms.node", "axms.tool", "axms.node", "axms.check"],
            [observation.name for observation in client.observations],
        )
        self.assertEqual("FAILED", client.observations[2].metadata["toolStatus"])
        self.assertEqual("PASSED", client.observations[4].metadata["checkStatus"])
        for arguments in client.start_arguments:
            self.assertNotIn("input", arguments)
            self.assertNotIn("output", arguments)
            self.assertNotIn("prompt", arguments)
        for observation in client.observations:
            self.assertLessEqual(set(observation.metadata), ALLOWED_METADATA_KEYS)
        serialized = repr(client.start_arguments) + repr(
            [observation.metadata for observation in client.observations]
        )
        for forbidden in (
            "FORBIDDEN_PROMPT",
            "FORBIDDEN_SOURCE",
            "FORBIDDEN_TOOL_OUTPUT",
            "axms.model",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_raw_failure_is_not_exported_and_original_failure_is_preserved(self) -> None:
        class ExpectedFailure(RuntimeError):
            code = "MODEL_TIMEOUT"

        client = _FakeClient()
        observability = AxmsObservability(client)
        with self.assertRaisesRegex(ExpectedFailure, "FORBIDDEN_RAW_ERROR"):
            with observability.job(
                job_id=JOB_ID,
                trace_id=TRACE_ID,
                profile_version_id=PROFILE_VERSION_ID,
                attempt=1,
            ):
                observability.invoke_node(
                    node=SimpleNamespace(node_type="agent"),
                    invocation=_invocation(),
                    handler=lambda _invocation: (_ for _ in ()).throw(
                        ExpectedFailure("FORBIDDEN_RAW_ERROR")
                    ),
                )

        serialized = repr(
            [observation.metadata for observation in client.observations]
        )
        self.assertNotIn("FORBIDDEN_RAW_ERROR", serialized)
        self.assertIn("MODEL_TIMEOUT", serialized)

    def test_sdk_start_update_end_and_shutdown_failures_are_fail_open(self) -> None:
        for client in (
            _FakeClient(fail_start=True),
            _FakeClient(fail_update=True),
            _FakeClient(fail_end=True),
        ):
            with self.subTest(client=client):
                observability = AxmsObservability(client)
                marker = object()
                with observability.job(
                    job_id=JOB_ID,
                    trace_id=TRACE_ID,
                    profile_version_id=PROFILE_VERSION_ID,
                    attempt=1,
                ):
                    result = observability.invoke_node(
                        node=SimpleNamespace(node_type="tool"),
                        invocation=_invocation("tool"),
                        handler=lambda _invocation: marker,
                    )
                self.assertIs(marker, result)

        shutdown_client = _FakeClient(fail_shutdown=True)
        AxmsObservability(shutdown_client).close()
        self.assertTrue(shutdown_client.shutdown_called)

    def test_actual_sdk_exports_closed_spans_and_active_node_traceparent(self) -> None:
        from langfuse import Langfuse
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        client = Langfuse(
            public_key="pk-lf-traceparent-test",
            secret_key="test-private-traceparent-value",
            base_url="https://jp.cloud.langfuse.com",
            environment="local",
            span_exporter=exporter,
        )
        observability = AxmsObservability(client)
        captured: dict[str, str | None] = {}
        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=1,
        ):
            observability.invoke_node(
                node=SimpleNamespace(node_type="tool"),
                invocation=_invocation("tool"),
                handler=lambda _invocation: (
                    captured.update(traceparent=_current_traceparent()),
                    SimpleNamespace(port="completed"),
                )[1],
            )
            observability.invoke_node(
                node=SimpleNamespace(node_type="check"),
                invocation=_invocation("check"),
                handler=lambda _invocation: SimpleNamespace(port="passed"),
            )
        client.flush()
        spans = exporter.get_finished_spans()

        self.assertEqual(
            {"axms.job", "axms.node", "axms.tool", "axms.check"},
            {span.name for span in spans},
        )
        self.assertNotIn("axms.model", {span.name for span in spans})
        traceparent = captured["traceparent"]
        self.assertIsInstance(traceparent, str)
        parts = str(traceparent).split("-")
        tool_node = next(
            span
            for span in spans
            if span.name == "axms.node"
            and f"{span.get_span_context().span_id:016x}" == parts[2]
        )
        context = tool_node.get_span_context()
        self.assertEqual(f"{context.trace_id:032x}", parts[1])
        self.assertEqual(f"{context.span_id:016x}", parts[2])

        serialized = repr(
            [{
                "attributes": span.attributes,
                "events": span.events,
                "status": span.status.description,
                "resource": span.resource.attributes,
            } for span in spans]
        )
        for forbidden in (
            "FORBIDDEN_PROMPT",
            "FORBIDDEN_SOURCE",
            "FORBIDDEN_TOOL_OUTPUT",
            "langfuse.observation.input",
            "langfuse.observation.output",
            "langfuse.observation.prompt",
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertTrue(all(not span.events for span in spans))
        observability.close()

    def test_actual_sdk_does_not_record_raw_exception_event_or_status(self) -> None:
        from langfuse import Langfuse
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        client = Langfuse(
            public_key="pk-lf-exception-test",
            secret_key="test-private-exception-value",
            base_url="https://jp.cloud.langfuse.com",
            environment="local",
            span_exporter=exporter,
        )
        observability = AxmsObservability(client)
        with self.assertRaisesRegex(RuntimeError, "FORBIDDEN_RAW_ERROR"):
            with observability.job(
                job_id=JOB_ID,
                trace_id=TRACE_ID,
                profile_version_id=PROFILE_VERSION_ID,
                attempt=1,
            ):
                observability.invoke_node(
                    node=SimpleNamespace(node_type="agent"),
                    invocation=_invocation(),
                    handler=lambda _invocation: (_ for _ in ()).throw(
                        RuntimeError("FORBIDDEN_RAW_ERROR")
                    ),
                )
        client.flush()
        spans = exporter.get_finished_spans()
        serialized = repr(
            [(span.attributes, span.events, span.status.description) for span in spans]
        )
        self.assertNotIn("FORBIDDEN_RAW_ERROR", serialized)
        self.assertTrue(all(not span.events for span in spans))
        self.assertTrue(all(span.status.description is None for span in spans))
        observability.close()


if __name__ == "__main__":
    unittest.main()
