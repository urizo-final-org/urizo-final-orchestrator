"""Fail-closed runtime configuration without Provider or Core DB authority."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping
from urllib.parse import quote

from .model_gateway import SPRING_PRIVATE_ORIGIN


class ConfigurationError(RuntimeError):
    """A stable, value-free configuration failure."""


FORBIDDEN_AUTHORITY_ENV = frozenset(
    {
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "AZURE_OPENAI_API_KEY",
        "AXMS_CORE_DB_JDBC_URL",
        "AXMS_CORE_DB_PASSWORD_FILE",
    }
)


def _integer(
    source: Mapping[str, str],
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = source.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} is outside the supported range")
    return value


def _path(source: Mapping[str, str], name: str, default: str | None = None) -> Path:
    raw = source.get(name, default)
    if not raw:
        raise ConfigurationError(f"{name} is required")
    return Path(raw)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    spring_origin: str
    spring_credential_file: Path
    checkpoint_host: str
    checkpoint_port: int
    checkpoint_database: str
    checkpoint_user: str
    checkpoint_password_file: Path
    checkpoint_encryption_key_file: Path
    valkey_host: str
    valkey_port: int
    valkey_database: int
    valkey_password_file: Path | None
    queue_key: str
    health_host: str
    health_port: int
    queue_block_seconds: int
    heartbeat_seconds: int
    max_attempts: int
    max_backoff_seconds: int

    @classmethod
    def from_environment(cls, source: Mapping[str, str] | None = None) -> RuntimeSettings:
        values = os.environ if source is None else source
        if FORBIDDEN_AUTHORITY_ENV.intersection(values):
            raise ConfigurationError(
                "Provider and Core DB authority must not enter the coding runtime"
            )
        spring_origin = values.get("AXMS_SPRING_ORIGIN", SPRING_PRIVATE_ORIGIN)
        if spring_origin != SPRING_PRIVATE_ORIGIN:
            raise ConfigurationError("AXMS_SPRING_ORIGIN must use the private Spring origin")
        checkpoint_host = values.get("AXMS_CHECKPOINT_HOST", "checkpoint_database")
        valkey_host = values.get("AXMS_VALKEY_HOST", "valkey")
        health_host = values.get("AXMS_HEALTH_HOST", "0.0.0.0")
        if checkpoint_host != "checkpoint_database":
            raise ConfigurationError(
                "AXMS_CHECKPOINT_HOST must use the exclusive checkpoint service"
            )
        if valkey_host != "valkey":
            raise ConfigurationError("AXMS_VALKEY_HOST must use the private Valkey service")
        if health_host != "0.0.0.0":
            raise ConfigurationError("AXMS_HEALTH_HOST must use the container listener")
        checkpoint_database = values.get("AXMS_CHECKPOINT_DATABASE", "axms_langgraph")
        checkpoint_user = values.get("AXMS_CHECKPOINT_USER", "axms_checkpoint")
        if checkpoint_database != "axms_langgraph" or checkpoint_user != "axms_checkpoint":
            raise ConfigurationError(
                "checkpoint database identity must use the exclusive runtime role"
            )
        queue_key = values.get("AXMS_QUEUE_KEY", "axms:coding:jobs:v1")
        if queue_key != "axms:coding:jobs:v1":
            raise ConfigurationError("AXMS_QUEUE_KEY must use the versioned coding queue")
        password_file = values.get("AXMS_VALKEY_PASSWORD_FILE")
        return cls(
            spring_origin=spring_origin,
            spring_credential_file=_path(
                values,
                "AXMS_SPRING_CREDENTIAL_FILE",
                "/run/secrets/coding_model_bridge_service_token",
            ),
            checkpoint_host=checkpoint_host,
            checkpoint_port=_integer(values, "AXMS_CHECKPOINT_PORT", 5432, 1, 65535),
            checkpoint_database=checkpoint_database,
            checkpoint_user=checkpoint_user,
            checkpoint_password_file=_path(
                values,
                "AXMS_CHECKPOINT_PASSWORD_FILE",
                "/run/secrets/checkpoint_postgres_password",
            ),
            checkpoint_encryption_key_file=_path(
                values,
                "AXMS_CHECKPOINT_ENCRYPTION_KEY_FILE",
                "/run/secrets/checkpoint_encryption_key",
            ),
            valkey_host=valkey_host,
            valkey_port=_integer(values, "AXMS_VALKEY_PORT", 6379, 1, 65535),
            valkey_database=_integer(values, "AXMS_VALKEY_DATABASE", 0, 0, 15),
            valkey_password_file=Path(password_file) if password_file else None,
            queue_key=queue_key,
            health_host=health_host,
            health_port=_integer(values, "AXMS_HEALTH_PORT", 8090, 1, 65535),
            queue_block_seconds=_integer(values, "AXMS_QUEUE_BLOCK_SECONDS", 5, 1, 60),
            heartbeat_seconds=_integer(values, "AXMS_HEARTBEAT_SECONDS", 10, 1, 60),
            max_attempts=_integer(values, "AXMS_MAX_ATTEMPTS", 3, 1, 10),
            max_backoff_seconds=_integer(values, "AXMS_MAX_BACKOFF_SECONDS", 30, 1, 300),
        )

    def checkpoint_dsn(self) -> str:
        password = _read_text_secret(self.checkpoint_password_file, 512)
        return "postgresql://%s:%s@%s:%d/%s" % (
            quote(self.checkpoint_user, safe=""),
            quote(password, safe=""),
            self.checkpoint_host,
            self.checkpoint_port,
            quote(self.checkpoint_database, safe=""),
        )

    def checkpoint_encryption_key(self) -> bytes:
        key = _read_binary_secret(self.checkpoint_encryption_key_file, 32)
        if len(key) != 32:
            raise ConfigurationError("checkpoint encryption key must contain exactly 32 bytes")
        return key

    def valkey_password(self) -> str | None:
        if self.valkey_password_file is None:
            return None
        return _read_text_secret(self.valkey_password_file, 512)


def _read_binary_secret(path: Path, maximum: int) -> bytes:
    try:
        with path.open("rb") as stream:
            value = stream.read(maximum + 1)
    except OSError:
        raise ConfigurationError("required secret file is unavailable") from None
    if not value or len(value) > maximum:
        raise ConfigurationError("required secret file has an invalid size")
    return value


def _read_text_secret(path: Path, maximum: int) -> str:
    owned = bytearray(_read_binary_secret(path, maximum))
    try:
        while owned and owned[-1] in {10, 13}:
            owned.pop()
        if not owned or any(value < 0x21 or value > 0x7E for value in owned):
            raise ConfigurationError("required text secret has an invalid format")
        return owned.decode("ascii")
    finally:
        for index in range(len(owned)):
            owned[index] = 0
