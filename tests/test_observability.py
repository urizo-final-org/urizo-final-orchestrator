from __future__ import annotations

from types import SimpleNamespace
import unittest

from axms_coding_orchestrator.observability import (
    ALLOWED_METADATA_KEYS,
    AxmsObservability,
    ModelObservation,
    parse_model_observations,
)


JOB_ID = "11111111-1111-4111-8111-111111111111"
TRACE_ID = "22222222-2222-4222-8222-222222222222"
PROFILE_VERSION_ID = "33333333-3333-4333-8333-333333333333"


class _FakeObservation:
    def __init__(self, client: _FakeClient, name: str, metadata: dict[str, object]) -> None:
        self.client = client
        self.name = name
        self.metadata = dict(metadata)
        self.ended = False

    def start_observation(self, **kwargs: object) -> _FakeObservation:
        return self.client._start(**kwargs)

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

    def start_observation(self, **kwargs: object) -> _FakeObservation:
        return self._start(**kwargs)

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
    def test_model_provider_allowlist_matches_backend_enum_exactly(self) -> None:
        def value(provider: str) -> dict[str, object]:
            return {
                "provider": provider,
                "modelId": "model-final",
                "inputTokens": 1,
                "outputTokens": 2,
                "latencyMs": 3,
            }

        accepted = parse_model_observations(
            [value("GOOGLE_GENAI"), value("VERTEX_AI_GEMINI")]
        )
        rejected = parse_model_observations(
            [value("GOOGLE"), value("LOCAL"), value("UNKNOWN")]
        )

        self.assertEqual(
            ["GOOGLE_GENAI", "VERTEX_AI_GEMINI"],
            [observation.provider for observation in accepted],
        )
        self.assertEqual((), rejected)

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
            [
                {
                    "public_key": "public-test",
                    "secret_key": "secret-test",
                    "base_url": "https://jp.cloud.langfuse.com",
                    "environment": "local",
                }
            ],
            calls,
        )

    def test_configured_model_binding_is_not_reported_as_actual_execution(self) -> None:
        client = _FakeClient()
        observability = AxmsObservability(client)
        node = SimpleNamespace(node_type="agent")
        result = SimpleNamespace(port="completed")

        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=2,
        ) as job:
            actual = observability.invoke_node(
                node=node,
                invocation=_invocation(),
                handler=lambda _invocation: result,
            )
            job.finish("COMPLETED")

        self.assertIs(result, actual)
        self.assertEqual(
            ["axms.job", "axms.node"],
            [observation.name for observation in client.observations],
        )
        self.assertEqual(TRACE_ID, client.seed)
        self.assertNotIn("provider", client.observations[1].metadata)
        self.assertNotIn("model", client.observations[1].metadata)
        for arguments in client.start_arguments:
            self.assertNotIn("input", arguments)
            self.assertNotIn("output", arguments)
        for observation in client.observations:
            self.assertLessEqual(set(observation.metadata), ALLOWED_METADATA_KEYS)
        serialized = repr(client.start_arguments) + repr(
            [observation.metadata for observation in client.observations]
        )
        for forbidden in (
            "FORBIDDEN_PROMPT",
            "FORBIDDEN_SOURCE",
            "FORBIDDEN_TOOL_OUTPUT",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_actual_model_values_emit_one_observation_per_backend_turn(self) -> None:
        client = _FakeClient()
        observability = AxmsObservability(client)
        turns = (
            ModelObservation("OPENAI", "gpt-final", 101, 29, 450),
            ModelObservation("ANTHROPIC", "claude-final", 203, 41, 780),
        )

        def handler(_invocation: object) -> SimpleNamespace:
            observability.record_models(turns)
            return SimpleNamespace(port="completed")

        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=1,
        ):
            observability.invoke_node(
                node=SimpleNamespace(node_type="agent"),
                invocation=_invocation(),
                handler=handler,
            )

        self.assertEqual(
            ["axms.job", "axms.node", "axms.model", "axms.model"],
            [observation.name for observation in client.observations],
        )
        self.assertEqual(
            {
                "provider": "OPENAI",
                "model": "gpt-final",
                "inputTokens": 101,
                "outputTokens": 29,
                "latencyMs": 450,
            },
            client.observations[2].metadata,
        )
        self.assertEqual(
            {
                "provider": "ANTHROPIC",
                "model": "claude-final",
                "inputTokens": 203,
                "outputTokens": 41,
                "latencyMs": 780,
            },
            client.observations[3].metadata,
        )
        model_starts = [
            arguments
            for arguments in client.start_arguments
            if arguments.get("name") == "axms.model"
        ]
        self.assertEqual(
            [
                {
                    "model": "gpt-final",
                    "usage_details": {"input": 101, "output": 29},
                },
                {
                    "model": "claude-final",
                    "usage_details": {"input": 203, "output": 41},
                },
            ],
            [
                {
                    "model": arguments.get("model"),
                    "usage_details": arguments.get("usage_details"),
                }
                for arguments in model_starts
            ],
        )
        for arguments in model_starts:
            self.assertNotIn("input", arguments)
            self.assertNotIn("output", arguments)
            self.assertNotIn("prompt", arguments)

    def test_tool_and_check_statuses_use_only_fixed_observation_names(self) -> None:
        client = _FakeClient()
        observability = AxmsObservability(client)
        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=1,
        ):
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

        self.assertEqual(
            ["axms.job", "axms.node", "axms.tool", "axms.node", "axms.check"],
            [observation.name for observation in client.observations],
        )
        self.assertEqual("FAILED", client.observations[2].metadata["toolStatus"])
        self.assertEqual("PASSED", client.observations[4].metadata["checkStatus"])

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

    def test_actual_sdk_exports_only_fixed_names_without_application_io(self) -> None:
        from langfuse import Langfuse
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        client = Langfuse(
            public_key="pk-lf-unit-test",
            secret_key="sk-lf-unit-test",
            base_url="https://jp.cloud.langfuse.com",
            environment="local",
            span_exporter=exporter,
        )
        observability = AxmsObservability(client)
        with observability.job(
            job_id=JOB_ID,
            trace_id=TRACE_ID,
            profile_version_id=PROFILE_VERSION_ID,
            attempt=1,
        ):
            observability.invoke_node(
                node=SimpleNamespace(node_type="check"),
                invocation=_invocation("check"),
                handler=lambda _invocation: SimpleNamespace(port="passed"),
            )
            observability.invoke_node(
                node=SimpleNamespace(node_type="agent"),
                invocation=_invocation("analyze"),
                handler=lambda _invocation: (
                    observability.record_models(
                        (ModelObservation("OPENAI", "gpt-final", 101, 29, 450),)
                    ),
                    SimpleNamespace(port="completed"),
                )[1],
            )
        client.flush()
        spans = exporter.get_finished_spans()
        serialized_attributes = repr([span.attributes for span in spans])

        self.assertEqual(
            {"axms.job", "axms.node", "axms.model", "axms.check"},
            {span.name for span in spans},
        )
        model_span = next(span for span in spans if span.name == "axms.model")
        self.assertEqual(
            "gpt-final",
            model_span.attributes["langfuse.observation.model.name"],
        )
        self.assertEqual(
            '{"input": 101, "output": 29}',
            model_span.attributes["langfuse.observation.usage_details"],
        )
        self.assertNotIn("FORBIDDEN_PROMPT", serialized_attributes)
        self.assertNotIn("FORBIDDEN_SOURCE", serialized_attributes)
        self.assertNotIn("FORBIDDEN_TOOL_OUTPUT", serialized_attributes)
        self.assertNotIn("langfuse.observation.input", serialized_attributes)
        self.assertNotIn("langfuse.observation.output", serialized_attributes)
        self.assertNotIn("langfuse.observation.prompt", serialized_attributes)
        observability.close()


if __name__ == "__main__":
    unittest.main()
