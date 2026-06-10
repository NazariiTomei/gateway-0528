"""
Run the orchestrator-owned worker gateway.

  cd neurons/worker_gateway
  python main.py

Workers:  wss://<host>/ws/{worker_id}?api_key=...
Orch:     wss://<host>/control  (header x-control-secret)
"""

from __future__ import annotations

import logging
import os
import secrets
import sys
from pathlib import Path

import uvicorn

# Ensure this package directory is importable when started from any cwd.
_GATEWAY_DIR = Path(__file__).resolve().parent
if str(_GATEWAY_DIR) not in sys.path:
    sys.path.insert(0, str(_GATEWAY_DIR))

from config import get_settings  # noqa: E402
from server import create_app  # noqa: E402

LOG_DIR = os.environ.get("LOG_DIR", "/tmp/beam_worker_gateway_logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(f"{LOG_DIR}/worker_gateway.log")
file_handler.setFormatter(logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)


def _resolve_control_secret(settings) -> str:
    secret = (settings.control_secret or os.environ.get("WORKER_GATEWAY_CONTROL_SECRET") or "").strip()
    if secret:
        return secret
    generated = secrets.token_urlsafe(32)
    os.environ["WORKER_GATEWAY_CONTROL_SECRET"] = generated
    logger.warning(
        "WORKER_GATEWAY_CONTROL_SECRET was not set — generated one for this process. "
        "Copy it to the orchestrator .env."
    )
    print(f"\n*** Set on orchestrator: WORKER_GATEWAY_CONTROL_SECRET={generated}\n")
    return generated


def main() -> None:
    settings = get_settings()
    log_level = settings.normalized_log_level()
    logging.getLogger().setLevel(log_level)

    control_secret = _resolve_control_secret(settings)
    app = create_app(settings, control_secret=control_secret)

    print(
        f"""
╔═══════════════════════════════════════════════════╗
║           BEAM WORKER GATEWAY                     ║
║  Listen:   {settings.host}:{settings.port}
║  Workers:  /ws/{{worker_id}}?api_key=...
║  Control:  /control  (x-control-secret)
║  Workers:  /get-firefox-workers  (x-control-secret)
║  Public:   {settings.public_url or "(set WORKER_GATEWAY_PUBLIC_URL)"}
╚═══════════════════════════════════════════════════╝
"""
    )

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=log_level.lower(),
        # Required when running behind Caddy / nginx TLS termination
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


if __name__ == "__main__":
    main()
