"""
Client for an orchestrator-owned worker gateway control WebSocket (/control).
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


def _http_to_ws_url(base_url: str, path: str = "") -> str:
    base = base_url.rstrip("/")
    if base.startswith("https://"):
        ws = "wss://" + base[8:]
    elif base.startswith("http://"):
        ws = "ws://" + base[7:]
    elif base.startswith("wss://") or base.startswith("ws://"):
        ws = base
    else:
        ws = "ws://" + base
    suffix = path if path.startswith("/") else f"/{path}" if path else ""
    return ws + suffix


class WorkerGatewayClient:
    """Maintains control-plane session to a dedicated worker gateway."""

    def __init__(
        self,
        control_url: str,
        control_secret: str,
        *,
        open_timeout: float = 30.0,
        ping_interval: float = 25.0,
        ping_timeout: float = 45.0,
    ) -> None:
        self._control_url = _http_to_ws_url(control_url, "/control")
        self._control_secret = control_secret
        self._open_timeout = open_timeout
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        self._reconnect_delay = 5.0

        self._pending: Dict[str, asyncio.Future] = {}
        self._local_workers: Dict[str, dict] = {}

        self._on_worker_response: Optional[Callable[[dict], Any]] = None
        self._on_task_result_summary: Optional[Callable[[dict], Any]] = None
        self._on_worker_stats_update: Optional[Callable[[dict], Any]] = None

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    def set_worker_response_handler(self, handler: Callable[[dict], Any]) -> None:
        self._on_worker_response = handler

    def set_task_result_summary_handler(self, handler: Callable[[dict], Any]) -> None:
        self._on_task_result_summary = handler

    def set_worker_stats_handler(self, handler: Callable[[dict], Any]) -> None:
        """Called when gateway pushes worker_stats_update (e.g. after task_result_summary)."""
        self._on_worker_stats_update = handler

    async def _dispatch_worker_stats(self, row: dict) -> None:
        if not self._on_worker_stats_update:
            return
        result = self._on_worker_stats_update(row)
        if asyncio.iscoroutine(result):
            await result

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._connection_loop())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        self._ws = None
        self._connected = False

    async def _connection_loop(self) -> None:
        while self._running:
            try:
                async with websockets.connect(
                    self._control_url,
                    additional_headers={"x-control-secret": self._control_secret},
                    open_timeout=self._open_timeout,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    logger.info("Connected to worker gateway control at %s", self._control_url)
                    await self._recv_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Worker gateway control disconnected: %s", exc)
            finally:
                self._connected = False
                self._ws = None
                for fut in list(self._pending.values()):
                    if not fut.done():
                        fut.set_exception(RuntimeError("gateway control disconnected"))
                self._pending.clear()

            if self._running:
                await asyncio.sleep(self._reconnect_delay)

    async def _recv_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await self._handle_message(data)

    async def _handle_message(self, data: dict) -> None:
        request_id = data.get("request_id")
        if request_id and request_id in self._pending:
            fut = self._pending.pop(request_id)
            if not fut.done():
                fut.set_result(data)
            return

        msg_type = data.get("type")

        if msg_type == "control_connected":
            for worker in data.get("workers", []):
                wid = worker.get("worker_id")
                if wid:
                    self._local_workers[wid] = worker
            logger.info(
                "Worker gateway control ready (%s workers connected)",
                len(self._local_workers),
            )

        elif msg_type == "worker_connected":
            wid = data.get("worker_id")
            if wid:
                row = data.get("worker")
                if isinstance(row, dict) and row.get("worker_id"):
                    self._local_workers[wid] = row
                else:
                    self._local_workers[wid] = {
                        "worker_id": wid,
                        "region": "unknown",
                        "status": "active",
                        "bandwidth_mbps": float(data.get("bandwidth_mbps", 100.0)),
                        "trust_score": 0.5,
                        "success_rate": 1.0,
                        "total_tasks": 0,
                        "bytes_relayed": 0,
                        "bytes_relayed_total": 0,
                        "load_factor": 0.0,
                        "capacity": int(data.get("capacity", 4)),
                    }
                    row = self._local_workers[wid]
                await self._dispatch_worker_stats(row)
            logger.info("Gateway worker connected: %s", wid)

        elif msg_type == "worker_disconnected":
            wid = data.get("worker_id")
            self._local_workers.pop(wid, None)
            logger.info("Gateway worker disconnected: %s", wid)

        elif msg_type == "worker_stats_update":
            row = data.get("worker")
            if isinstance(row, dict) and row.get("worker_id"):
                self._local_workers[row["worker_id"]] = row
                await self._dispatch_worker_stats(row)

        elif msg_type == "worker_capacity_update":
            wid = data.get("worker_id")
            if wid and wid in self._local_workers:
                rec = self._local_workers[wid]
                if data.get("bandwidth_mbps") is not None:
                    rec["bandwidth_mbps"] = float(data["bandwidth_mbps"])
                if data.get("capacity") is not None:
                    rec["capacity"] = int(data["capacity"])

        elif msg_type == "worker_response":
            logger.info(
                "gateway->orch worker_response: worker=%s task=%s offer=%s decision=%s",
                (data.get("worker_id") or "")[:36],
                (data.get("task_id") or "")[:16],
                (data.get("offer_id") or data.get("task_id") or "")[:16],
                data.get("decision"),
            )
            if self._on_worker_response:
                result = self._on_worker_response(data)
                if asyncio.iscoroutine(result):
                    await result

        elif msg_type == "task_result_summary":
            logger.info(
                "gateway->orch task_result_summary: worker=%s task=%s offer=%s success=%s bytes=%s",
                (data.get("worker_id") or "")[:36],
                (data.get("task_id") or "")[:16],
                (data.get("offer_id") or data.get("task_id") or "")[:16],
                bool(data.get("success", False)),
                int(data.get("bytes_transferred", 0) or 0),
            )
            if self._on_task_result_summary:
                result = self._on_task_result_summary(data)
                if asyncio.iscoroutine(result):
                    await result

    async def _send(self, message: dict, timeout: float = 15.0) -> dict:
        if not self._ws or not self._connected:
            raise RuntimeError("worker gateway control is not connected")

        request_id = message.get("request_id") or uuid.uuid4().hex
        message = {**message, "request_id": request_id}
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._ws.send(json.dumps(message))
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def list_workers(self, timeout: float = 10.0) -> List[dict]:
        if not self.connected:
            return list(self._local_workers.values())
        try:
            response = await self._send({"type": "list_workers"}, timeout=timeout)
            workers = response.get("workers", [])
            for worker in workers:
                wid = worker.get("worker_id")
                if wid:
                    self._local_workers[wid] = worker
            return workers
        except Exception as exc:
            logger.warning("list_workers via gateway failed: %s", exc)
            return list(self._local_workers.values())

    async def send_task_offer(self, worker_id: str, offer: dict) -> bool:
        response = await self._send(
            {"type": "task_offer", "worker_id": worker_id, "offer": offer},
            timeout=30.0,
        )
        success = bool(response.get("success"))
        if success:
            logger.info(
                "orch->gateway task_offer ok: worker=%s task=%s offer=%s",
                worker_id,
                (offer.get("task_id") or "")[:16],
                (offer.get("offer_id") or "")[:16],
            )
        else:
            logger.warning(
                "orch->gateway task_offer failed: worker=%s task=%s reason=%s",
                worker_id,
                (offer.get("task_id") or "")[:16],
                response.get("reason") or "unknown",
            )
        return success

    async def send_task_accept_ack(
        self,
        worker_id: str,
        task_id: str,
        offer_id: str,
        accepted: bool,
        reason: str = "",
    ) -> None:
        if not self._ws or not self._connected:
            return
        payload = {
            "type": "task_accept_ack",
            "worker_id": worker_id,
            "task_id": task_id,
            "offer_id": offer_id,
            "accepted": accepted,
        }
        if reason:
            payload["reason"] = reason
        logger.info(
            "orch->gateway task_accept_ack: worker=%s task=%s accepted=%s",
            worker_id,
            (task_id or "")[:16],
            accepted,
        )
        await self._ws.send(json.dumps(payload))

    async def send_task_result_summary_ack(
        self,
        worker_id: str,
        task_id: str,
        offer_id: str,
        received: bool,
        reason: str = "",
    ) -> None:
        if not self._ws or not self._connected:
            return
        payload = {
            "type": "task_result_summary_ack",
            "worker_id": worker_id,
            "task_id": task_id,
            "offer_id": offer_id,
            "received": received,
            "completed": received,
        }
        if reason:
            payload["reason"] = reason
        logger.info(
            "orch->gateway task_result_summary_ack: worker=%s task=%s received=%s",
            worker_id,
            (task_id or "")[:16],
            received,
        )
        await self._ws.send(json.dumps(payload))
