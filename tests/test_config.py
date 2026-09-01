from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from axms_coding_orchestrator.config import ConfigurationError, RuntimeSettings


class RuntimeSettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.spring_token = root / "spring-token"
        self.checkpoint_password = root / "checkpoint-password"
        self.encryption_key = root / "checkpoint-key"
        self.valkey_password = root / "valkey-password"
        self.spring_token.write_bytes(b"test-spring-token")
        self.checkpoint_password.write_bytes(b"test-checkpoint-password\n")
        self.encryption_key.write_bytes(bytes(range(32)))
        self.valkey_password.write_bytes(b"test-valkey-password\r\n")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def environment(self) -> dict[str, str]:
        return {
            "AXMS_SPRING_ORIGIN": "http://spring-app:8080",
            "AXMS_SPRING_CREDENTIAL_FILE": str(self.spring_token),
            "AXMS_CHECKPOINT_HOST": "checkpoint_database",
            "AXMS_CHECKPOINT_DATABASE": "axms_langgraph",
            "AXMS_CHECKPOINT_USER": "axms_checkpoint",
            "AXMS_CHECKPOINT_PASSWORD_FILE": str(self.checkpoint_password),
            "AXMS_CHECKPOINT_ENCRYPTION_KEY_FILE": str(self.encryption_key),
            "AXMS_VALKEY_HOST": "valkey",
            "AXMS_VALKEY_PASSWORD_FILE": str(self.valkey_password),
            "AXMS_QUEUE_KEY": "axms:coding:jobs:v1",
            "AXMS_HEALTH_HOST": "0.0.0.0",
        }

    def test_resolves_raw_32_byte_key_and_text_password_files(self) -> None:
        settings = RuntimeSettings.from_environment(self.environment())

        self.assertEqual(bytes(range(32)), settings.checkpoint_encryption_key())
        self.assertEqual("test-valkey-password", settings.valkey_password())
        self.assertEqual(
            "axms:natural-cms:jobs:v1", settings.natural_cms_queue_key
        )
        self.assertIn("checkpoint_database:5432/axms_langgraph", settings.checkpoint_dsn())

    def test_natural_cms_queue_lane_is_fixed_and_distinct(self) -> None:
        environment = self.environment()
        environment["AXMS_NATURAL_CMS_QUEUE_KEY"] = "axms:coding:jobs:v1"

        with self.assertRaisesRegex(ConfigurationError, "Natural CMS queue"):
            RuntimeSettings.from_environment(environment)

    def test_provider_or_core_database_authority_is_rejected(self) -> None:
        for forbidden in ("OPENAI_API_KEY", "AXMS_CORE_DB_PASSWORD_FILE"):
            with self.subTest(forbidden=forbidden):
                environment = self.environment()
                environment[forbidden] = "must-not-enter-runtime"
                with self.assertRaisesRegex(ConfigurationError, "authority"):
                    RuntimeSettings.from_environment(environment)

    def test_checkpoint_service_identity_and_key_size_are_fail_closed(self) -> None:
        environment = self.environment()
        environment["AXMS_CHECKPOINT_HOST"] = "database"
        with self.assertRaisesRegex(ConfigurationError, "exclusive checkpoint"):
            RuntimeSettings.from_environment(environment)

        self.encryption_key.write_bytes(b"x" * 31)
        settings = RuntimeSettings.from_environment(self.environment())
        with self.assertRaisesRegex(ConfigurationError, "exactly 32"):
            settings.checkpoint_encryption_key()


if __name__ == "__main__":
    unittest.main()
