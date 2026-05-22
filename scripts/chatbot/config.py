"""
Configuration Management

Loads and validates all environment variables for the chatbot service.
"""

import os
from typing import List

from pydantic_settings import BaseSettings
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Application settings from environment variables."""

    # API Configuration
    API_HOST: str = Field(default="0.0.0.0", description="API host")
    API_PORT: int = Field(default=5002, description="API port")
    DEBUG: bool = Field(default=False, description="Debug mode")
    CORS_ORIGINS: List[str] = Field(
        default=["*"],
        description="CORS allowed origins"
    )

    # LLM Configuration
    GEMINI_API_KEY: str = Field(
        ...,
        description="Google Gemini API key"
    )
    GEMINI_MODEL: str = Field(
        default="gemini-2.5-flash",
        description="Gemini model name"
    )

    # Trino Configuration
    TRINO_HOST: str = Field(
        default="localhost",
        description="Trino coordinator hostname"
    )
    TRINO_PORT: int = Field(
        default=8080,
        description="Trino port"
    )
    TRINO_USER: str = Field(
        default="trino",
        description="Trino username"
    )
    TRINO_CATALOG: str = Field(
        default="iceberg",
        description="Default Trino catalog"
    )
    TRINO_SCHEMA: str = Field(
        default="security",
        description="Default Trino schema"
    )

    # MinIO/S3 Configuration
    S3_ENDPOINT: str = Field(
        default="http://minio:9000",
        description="MinIO S3 endpoint"
    )
    S3_BUCKET: str = Field(
        default="evidence-frames",
        description="S3 bucket for evidence frames"
    )
    MINIO_ROOT_USER: str = Field(
        default="minio",
        description="MinIO access key"
    )
    MINIO_ROOT_PASSWORD: str = Field(
        default="mypassword",
        description="MinIO secret key"
    )

    # Flink SQL Gateway Configuration (HOT layer Fluss queries)
    FLINK_GATEWAY_HOST: str = Field(
        default="jobmanager",
        description="Flink SQL Gateway hostname"
    )
    FLINK_GATEWAY_PORT: int = Field(
        default=8083,
        description="Flink SQL Gateway port"
    )

    # Agent Configuration
    MAX_RETRIES: int = Field(
        default=3,
        description="Maximum query retry attempts"
    )
    QUERY_TIMEOUT_SECONDS: int = Field(
        default=30,
        description="Query execution timeout"
    )
    RESPONSE_TIMEOUT_SECONDS: int = Field(
        default=60,
        description="Total response timeout"
    )

    # Logging Configuration
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    LOG_FORMAT: str = Field(
        default="json",
        description="Log format (json or text)"
    )

    class Config:
        """Pydantic settings config."""
        env_file = "/app/.env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    @field_validator("API_PORT", "TRINO_PORT")
    @classmethod
    def validate_positive_int(cls, v):
        """Ensure positive integers."""
        if v <= 0:
            raise ValueError("Must be positive")
        return v

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()


# Load settings
try:
    settings = Settings()
except Exception as e:
    print(f"⚠️ Failed to load settings: {e}")
    print("Using default values where possible")
    settings = Settings(
        GEMINI_API_KEY="PLACEHOLDER",  # Will fail later if used
    )


# ============================================================================
# Validation Functions
# ============================================================================

def validate_config() -> None:
    """
    Validate configuration on application startup.

    Raises:
        ValueError: If critical configuration is missing or invalid
    """
    errors = []

    # Check critical env vars
    if settings.GEMINI_API_KEY == "PLACEHOLDER" or not settings.GEMINI_API_KEY:
        errors.append("GEMINI_API_KEY is required but not set")

    if not settings.TRINO_HOST:
        errors.append("TRINO_HOST is required but not set")

    if not settings.S3_ENDPOINT:
        errors.append("S3_ENDPOINT is required but not set")

    if not settings.MINIO_ROOT_USER or not settings.MINIO_ROOT_PASSWORD:
        errors.append("MinIO credentials (MINIO_ROOT_USER, MINIO_ROOT_PASSWORD) are required")

    # Raise all errors at once
    if errors:
        raise ValueError("Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

    print("✓ Configuration validated successfully")


def print_config(redact_secrets: bool = True) -> None:
    """
    Print current configuration (useful for debugging).

    Args:
        redact_secrets: If True, mask sensitive values
    """
    print("\n" + "=" * 70)
    print("CHATBOT CONFIGURATION")
    print("=" * 70)

    config_dict = settings.model_dump()

    for key, value in sorted(config_dict.items()):
        if redact_secrets and any(secret in key.upper() for secret in ["KEY", "PASSWORD", "SECRET"]):
            display_value = "***REDACTED***"
        else:
            display_value = value

        print(f"  {key:<30} = {display_value}")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    """Print configuration when run directly."""
    validate_config()
    print_config()
