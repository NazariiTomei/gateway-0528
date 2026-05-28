"""
Dedicated worker gateway: worker data plane + orchestrator control plane.

Workers:  GET /health, WebSocket /ws/{worker_id}?api_key=
Orch:     WebSocket /control  (header x-control-secret)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, Header, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import GatewaySettings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class WorkerSession:
    worker_id: str
    websocket: WebSocket
    api_key: str = ""
    bandwidth_mbps: float = 100.0
    trust_score: float = 0.5
    max_concurrent_tasks: int = 4
    active_tasks: int = 0
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class ControlSession:
    websocket: WebSocket
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class GatewayState:
    """In-memory session registry for one gateway process."""

    def __init__(self) -> None:
        self.workers: Dict[str, WorkerSession] = {}
        self.control: Optional[ControlSession] = None

    def list_worker_records(self) -> List[dict]:
        now = time.time()
        records = []
        for worker_id, session in self.workers.items():
            records.append(
                {
                    "worker_id": worker_id,
                    "bandwidth_mbps": session.bandwidth_mbps,
                    "trust_score": session.trust_score,
                    "capacity": max(0, session.max_concurrent_tasks - session.active_tasks),
                    "active_tasks": session.active_tasks,
                    "connected_seconds": int(now - session.connected_at),
                }
            )
        return records

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        session = self.workers.get(worker_id)
        if not session:
            logger.warning("task_offer for offline worker %s", worker_id)
            return False
        try:
            async with session.send_lock:
                await session.websocket.send_json(payload)
            if payload.get("type") == "task_offer":
                logger.info(
                    "task_offer delivered: worker=%s task=%s offer=%s",
                    worker_id,
                    (payload.get("task_id") or "")[:16],
                    (payload.get("offer_id") or "")[:16],
                )
            return True
        except Exception as exc:
            logger.error("Failed to send to worker %s: %s", worker_id, exc)
            return False

    async def send_to_control(self, payload: dict) -> bool:
        if not self.control:
            logger.debug("No control session; drop %s", payload.get("type"))
            return False
        try:
            async with self.control.send_lock:
                await self.control.websocket.send_json(payload)
            return True
        except Exception as exc:
            logger.error("Failed to send to control plane: %s", exc)
            return False


gateway_state = GatewayState()


async def _close_worker_socket(
    websocket: WebSocket,
    *,
    code: int,
    reason: str,
    error_message: Optional[str] = None,
) -> None:
    """Close a worker socket after accept() — never close before accept (breaks proxies)."""
    if error_message:
        try:
            await websocket.send_json({"type": "error", "message": error_message})
        except Exception:
            pass
    try:
        await websocket.close(code=code, reason=reason)
    except Exception:
        pass


def create_app(
    settings: Optional[GatewaySettings] = None,
    *,
    control_secret: Optional[str] = None,
) -> FastAPI:
    settings = settings or get_settings()
    resolved_secret = (control_secret or settings.control_secret or "").strip()
    app = FastAPI(title="BEAM Worker Gateway", version="0.1.0")

    @app.get("/")
    async def root() -> JSONResponse:
        return JSONResponse(
            {
                "service": "BEAM Worker Gateway",
                "health": "/health",
                "worker_ws": "/ws/{worker_id}",
                "control_ws": "/control",
            }
        )

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "workers_connected": len(gateway_state.workers),
                "control_connected": gateway_state.control is not None,
                "public_url": settings.public_url,
                "require_api_key": settings.require_worker_api_key,
            }
        )

    @app.get("/workers")
    async def workers() -> JSONResponse:
        """
        List workers currently connected to this gateway.

        This is an operator/debug endpoint (read-only) and does not include any secrets.
        """
        return JSONResponse(
            {
                "count": len(gateway_state.workers),
                "workers": gateway_state.list_worker_records(),
            }
        )

    @app.websocket("/ws/{worker_id}")
    async def worker_ws(
        websocket: WebSocket,
        worker_id: str,
        api_key: Optional[str] = Query(default=None),
    ) -> None:
        # Always accept first — closing before accept causes bad HTTP status / 502 behind Caddy.
        await websocket.accept()

        if settings.require_worker_api_key and not (api_key or "").strip():
            logger.warning("Worker %s rejected: missing api_key query param", worker_id)
            await _close_worker_socket(
                websocket,
                code=4401,
                reason="api_key required",
                error_message="api_key required",
            )
            return

        if worker_id in gateway_state.workers:
            old = gateway_state.workers.pop(worker_id)
            await _close_worker_socket(old.websocket, code=4000, reason="replaced")

        session = WorkerSession(
            worker_id=worker_id,
            websocket=websocket,
            api_key=api_key or "",
        )
        gateway_state.workers[worker_id] = session
        logger.info("Worker connected: %s", worker_id)

        await websocket.send_json({"type": "connected", "worker_id": worker_id})
        await gateway_state.send_to_control(
            {
                "type": "worker_connected",
                "worker_id": worker_id,
                "capacity": session.max_concurrent_tasks,
                "bandwidth_mbps": session.bandwidth_mbps,
            }
        )

        try:
            while True:
                raw = await websocket.receive_text()
                session.last_seen = time.time()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    await websocket.send_json({"type": "error", "message": "invalid json"})
                    continue

                msg_type = data.get("type")
                if msg_type == "stats_snapshot":
                    bw = data.get("bandwidth_mbps")
                    if bw is not None:
                        try:
                            session.bandwidth_mbps = float(bw)
                        except (TypeError, ValueError):
                            pass
                    active = data.get("tasks_active")
                    if active is not None:
                        try:
                            session.active_tasks = int(active)
                        except (TypeError, ValueError):
                            pass
                    await websocket.send_json({"type": "stats_snapshot_ack"})
                    await gateway_state.send_to_control(
                        {
                            "type": "worker_capacity_update",
                            "worker_id": worker_id,
                            "bandwidth_mbps": session.bandwidth_mbps,
                            "active_tasks": session.active_tasks,
                        }
                    )
                    continue

                if msg_type == "task_accept":
                    await _relay_worker_response(session, data, "task_accept")
                    continue

                if msg_type == "task_reject":
                    reason = data.get("reason", "")
                    await _relay_worker_response(session, data, "task_reject", reason=reason)
                    continue

                if msg_type == "task_result_summary":
                    logger.info(
                        "task_result_summary from worker: worker=%s task=%s offer=%s success=%s bytes=%s mbps=%.1f",
                        worker_id,
                        (data.get("task_id") or "")[:16],
                        (data.get("offer_id") or data.get("task_id") or "")[:16],
                        bool(data.get("success", False)),
                        int(data.get("bytes_transferred", 0) or 0),
                        float(data.get("bandwidth_mbps", 0.0) or 0.0),
                    )
                    await gateway_state.send_to_control({**data, "worker_id": worker_id})
                    continue

                if msg_type == "task_transfer_progress":
                    await gateway_state.send_to_control({**data, "worker_id": worker_id})
                    continue

                if msg_type == "capacity_update":
                    try:
                        session.max_concurrent_tasks = int(
                            data.get("max_concurrent_tasks", session.max_concurrent_tasks)
                        )
                    except (TypeError, ValueError):
                        pass
                    await gateway_state.send_to_control(
                        {
                            "type": "worker_capacity_update",
                            "worker_id": worker_id,
                            "capacity": session.max_concurrent_tasks,
                        }
                    )
                    continue

                logger.debug("Worker %s sent unhandled type %s", worker_id, msg_type)

        except WebSocketDisconnect:
            logger.info("Worker disconnected: %s", worker_id)
        except Exception as exc:
            logger.exception("Worker session error %s: %s", worker_id, exc)
        finally:
            gateway_state.workers.pop(worker_id, None)
            await gateway_state.send_to_control(
                {"type": "worker_disconnected", "worker_id": worker_id}
            )

    @app.websocket("/control")
    async def control_ws(
        websocket: WebSocket,
        x_control_secret: Optional[str] = Header(default=None, alias="x-control-secret"),
    ) -> None:
        await websocket.accept()

        if not resolved_secret:
            logger.error("Control connection rejected: WORKER_GATEWAY_CONTROL_SECRET not set")
            await _close_worker_socket(
                websocket,
                code=1011,
                reason="control secret not configured",
                error_message="control secret not configured",
            )
            return

        if (x_control_secret or "").strip() != resolved_secret:
            logger.warning("Control connection rejected: invalid x-control-secret")
            await _close_worker_socket(
                websocket,
                code=4403,
                reason="invalid control secret",
                error_message="invalid control secret",
            )
            return

        if gateway_state.control:
            await _close_worker_socket(gateway_state.control.websocket, code=4000, reason="replaced")

        gateway_state.control = ControlSession(websocket=websocket)
        logger.info("Orchestrator control connected")

        await websocket.send_json(
            {
                "type": "control_connected",
                "workers": gateway_state.list_worker_records(),
            }
        )

        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    await websocket.send_json({"type": "error", "message": "invalid json"})
                    continue

                msg_type = data.get("type")

                if msg_type == "list_workers":
                    await websocket.send_json(
                        {
                            "type": "list_workers",
                            "workers": gateway_state.list_worker_records(),
                            "request_id": data.get("request_id"),
                        }
                    )
                    continue

                if msg_type == "task_offer":
                    worker_id = data.get("worker_id")
                    offer = data.get("offer") or {}
                    if not worker_id:
                        await websocket.send_json(
                            {
                                "type": "task_offer_result",
                                "success": False,
                                "reason": "missing worker_id",
                                "request_id": data.get("request_id"),
                            }
                        )
                        continue
                    payload = {**offer, "type": "task_offer"}
                    delivered = await gateway_state.send_to_worker(worker_id, payload)
                    await websocket.send_json(
                        {
                            "type": "task_offer_result",
                            "success": delivered,
                            "worker_id": worker_id,
                            "task_id": offer.get("task_id"),
                            "offer_id": offer.get("offer_id"),
                            "request_id": data.get("request_id"),
                        }
                    )
                    continue

                if msg_type in ("task_accept_ack", "task_result_summary_ack"):
                    worker_id = data.get("worker_id")
                    if worker_id:
                        await gateway_state.send_to_worker(worker_id, data)
                    continue

                if msg_type == "ping":
                    await websocket.send_json({"type": "pong"})
                    continue

                logger.debug("Control sent unhandled type %s", msg_type)

        except WebSocketDisconnect:
            logger.info("Orchestrator control disconnected")
        except Exception as exc:
            logger.exception("Control session error: %s", exc)
        finally:
            if gateway_state.control and gateway_state.control.websocket is websocket:
                gateway_state.control = None

    return app


async def _relay_worker_response(
    session: WorkerSession,
    data: dict,
    decision: str,
    reason: str = "",
) -> None:
    """Forward worker accept/reject to orchestrator; BeamCore acks return via control plane."""
    task_id = data.get("task_id")
    offer_id = data.get("offer_id") or task_id
    payload: dict[str, Any] = {
        "type": "worker_response",
        "task_id": task_id,
        "offer_id": offer_id,
        "worker_id": session.worker_id,
        "decision": decision,
    }
    if reason:
        payload["reason"] = reason
    logger.info(
        "worker_response from worker: worker=%s task=%s offer=%s decision=%s",
        session.worker_id,
        (task_id or "")[:16],
        (offer_id or "")[:16],
        decision,
    )
    await gateway_state.send_to_control(payload)

    if decision == "task_reject":
        await session.websocket.send_json(
            {
                "type": "task_accept_ack",
                "task_id": task_id,
                "offer_id": offer_id,
                "accepted": False,
                "reason": reason or "rejected",
            }
        )


def get_app() -> FastAPI:
    return create_app()
