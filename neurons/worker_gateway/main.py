"""
Run the orchestrator-owned worker gateway.

  cd neurons/worker_gateway
  python main.py

Workers connect to wss://<public-host>/ws/{worker_id}?api_key=...
Orchestrator connects to wss://<host>/control with header x-control-secret.
"""

import logging
import os
import secrets
import sys

import uvicorn

from config import get_settings
from server import create_app

LOG_DIR = os.environ.get("LOG_DIR", "/tmp/beam_worker_gateway_logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(f"{LOG_DIR}/worker_gateway.log")
file_handler.setFormatter(logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    logging.getLogger().setLevel(settings.log_level)

    if not (settings.control_secret or "").strip():
        generated = secrets.token_urlsafe(32)
        logger.warning(
            "WORKER_GATEWAY_CONTROL_SECRET is not set — generated ephemeral secret for this "
            "process only. Set the same value on the orchestrator."
        )
        settings.control_secret = generated
        print(f"\n*** Set on orchestrator: WORKER_GATEWAY_CONTROL_SECRET={generated}\n")

    app = create_app(settings)

    print(
        f"""
╔═══════════════════════════════════════════════════╗
║           BEAM WORKER GATEWAY                     ║
║  Workers:  /ws/{{worker_id}}?api_key=...           ║
║  Control:  /control  (x-control-secret)           ║
║  Public:   {settings.public_url or '(set WORKER_GATEWAY_PUBLIC_URL)'}
╚═══════════════════════════════════════════════════╝
"""
    )

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
