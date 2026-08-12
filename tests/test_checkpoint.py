from __future__ import annotations

import unittest

from axms_coding_orchestrator.checkpoint import (
    CheckpointRuntime,
    build_encrypted_serializer,
)


class CheckpointEncryptionTest(unittest.TestCase):
    def test_raw_32_byte_key_encrypts_and_authenticates_checkpoint_payload(self) -> None:
        key = bytes(range(32))
        serializer = build_encrypted_serializer(key)
        encoded = serializer.dumps_typed({"prompt": "checkpoint-secret-test"})

        self.assertNotIn(b"checkpoint-secret-test", encoded[1])
        self.assertEqual(
            {"prompt": "checkpoint-secret-test"}, serializer.loads_typed(encoded)
        )

        wrong = build_encrypted_serializer(bytes(reversed(range(32))))
        with self.assertRaises(ValueError):
            wrong.loads_typed(encoded)

    def test_runtime_repr_redacts_database_and_key_material(self) -> None:
        runtime = CheckpointRuntime(
            "postgresql://axms_checkpoint:test-password@checkpoint_database/axms_langgraph",
            b"x" * 32,
        )

        rendered = repr(runtime)
        self.assertNotIn("test-password", rendered)
        self.assertNotIn("xxxxxxxx", rendered)
        self.assertIn("REDACTED", rendered)


if __name__ == "__main__":
    unittest.main()
