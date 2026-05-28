"""Worker gateway configuration."""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class GatewaySettings(BaseSettings):
    host: str = Field(default="0.0.0.0", env="GATEWAY_HOST")
    port: int = Field(default=8001, env="GATEWAY_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Shared secret for orchestrator control WebSocket (/control)
    control_secret: str = Field(default="", env="WORKER_GATEWAY_CONTROL_SECRET")

    # Require ?api_key= on worker /ws/{worker_id} connections
    require_worker_api_key: bool = Field(default=True, env="GATEWAY_REQUIRE_API_KEY")

    # Public base URL advertised to BeamCore (https://gateway.example.com)
    public_url: Optional[str] = Field(default=None, env="WORKER_GATEWAY_PUBLIC_URL")

    class Config:
        env_file = ".env"
        extra = "ignore"

    def model_post_init(self, __context) -> None:
        object.__setattr__(self, "log_level", self.log_level.upper())


@lru_cache
def get_settings() -> GatewaySettings:
    return GatewaySettings()
