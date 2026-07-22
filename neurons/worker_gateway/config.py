"""Worker gateway configuration."""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_DIR = Path(__file__).resolve().parent
_ENV_FILE = _PACKAGE_DIR / ".env"


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE) if _ENV_FILE.is_file() else None,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="0.0.0.0", env="GATEWAY_HOST")
    port: int = Field(default=8001, env="GATEWAY_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # No hardcoded default: leaving this unset lets main.py's
    # _resolve_control_secret() generate and print a fresh one at startup.
    control_secret: str = Field(default="", env="WORKER_GATEWAY_CONTROL_SECRET")
    require_worker_api_key: bool = Field(default=True, env="GATEWAY_REQUIRE_API_KEY")
    public_url: Optional[str] = Field(default=None, env="WORKER_GATEWAY_PUBLIC_URL")

    # JSON file for worker stats (total_tasks, bytes_relayed, trust_score, …). Survives restarts.
    metrics_path: Optional[str] = Field(default=None, env="WORKER_GATEWAY_METRICS_PATH")

    def normalized_log_level(self) -> str:
        return self.log_level.upper()

    def resolved_metrics_path(self) -> Path:
        custom = (self.metrics_path or "").strip()
        if custom:
            return Path(custom)
        return _PACKAGE_DIR / "data" / "worker_metrics.json"


@lru_cache
def get_settings() -> GatewaySettings:
    return GatewaySettings()
