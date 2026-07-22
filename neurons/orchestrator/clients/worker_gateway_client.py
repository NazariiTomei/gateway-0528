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

_RESULT_FORWARD_RETRY_BASE_SECONDS = 0.25
_RESULT_FORWARD_RETRY_MAX_SECONDS = 2.0
_RESULT_TERMINAL_STATUSES = {
    "owned_processing",
    "completed",
    "failed",
    "late_superseded",
    "late_expired",
    "rejected",
}


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
        self._result_forward_tasks: Dict[str, asyncio.Task] = {}

        self._on_worker_stats_update: Optional[Callable[[dict], Any]] = None
        self._upstream: Optional[Any] = None

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    def set_upstream(self, upstream: Any) -> None:
        self._upstream = upstream

    def set_worker_stats_handler(self, handler: Callable[[dict], Any]) -> None:
        """Called when gateway pushes worker_stats_update."""
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

        if self._result_forward_tasks:
            pending = list(self._result_forward_tasks.values())
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self._result_forward_tasks.clear()

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

        elif msg_type == "task_result":
            asyncio.create_task(self._relay_task_result(data))

    async def _send_worker_payload(self, payload: dict) -> None:
        if not self._ws or not self._connected:
            return
        await self._ws.send(json.dumps(payload))

    async def _relay_task_result(self, data: dict) -> None:
        """Forward a worker's task_result upstream, retrying until BeamCore
        reaches a terminal status, then relay the ack back to the worker.

        Mirrors neurons/orchestrator/core/worker_gateway.py's
        _forward_task_result_to_beamcore for the in-process gateway path —
        BeamCore acks are not always terminal on the first attempt, so a
        single-shot relay (the old behavior here) can silently drop results.
        """
        worker_id = data.get("worker_id")
        task_id = data.get("task_id")
        offer_id = data.get("offer_id") or task_id
        if not worker_id:
            return
        if not task_id or not offer_id:
            await self._send_worker_payload(
                {
                    "type": "task_result_ack",
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": False,
                    "status": "rejected",
                    "reason": "missing_task_or_offer_id",
                }
            )
            return

        result_key = f"{offer_id}:{worker_id}"
        if result_key in self._result_forward_tasks:
            return

        payload = {
            "type": "task_result",
            "task_id": task_id,
            "offer_id": offer_id,
            "worker_id": worker_id,
            "success": bool(data.get("success")),
        }
        for key in ("etag", "chunk_hash", "error"):
            if data.get(key) is not None:
                payload[key] = data[key]

        task = asyncio.create_task(self._forward_task_result_to_upstream(worker_id, payload))
        self._result_forward_tasks[result_key] = task

        def _done(done_task: asyncio.Task) -> None:
            if self._result_forward_tasks.get(result_key) is done_task:
                self._result_forward_tasks.pop(result_key, None)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None:
                logger.error("task_result forward crashed: %s", exc)

        task.add_done_callback(_done)

    async def _forward_task_result_to_upstream(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        offer_id = payload.get("offer_id") or task_id
        attempt = 0
        while True:
            attempt += 1
            try:
                if self._upstream is None:
                    raise RuntimeError("beamcore_unavailable")
                sender = getattr(self._upstream, "send_task_result_strict", None) or self._upstream.send_task_result
                ack = await sender(payload)
                if not isinstance(ack, dict):
                    raise RuntimeError("invalid_beamcore_ack")
                status = str(ack.get("status") or "")
                if status in _RESULT_TERMINAL_STATUSES:
                    await self._send_worker_payload(
                        {
                            **ack,
                            "type": "task_result_ack",
                            "worker_id": worker_id,
                            "task_id": task_id,
                            "offer_id": offer_id,
                        }
                    )
                    logger.info(
                        "task_result relay terminal: task=%s offer=%s worker=%s status=%s",
                        task_id,
                        offer_id,
                        worker_id,
                        status,
                    )
                    return
                retry_reason = ack.get("reason") or status or "invalid_ack_status"
            except Exception as exc:
                retry_reason = type(exc).__name__

            delay = min(
                _RESULT_FORWARD_RETRY_MAX_SECONDS,
                _RESULT_FORWARD_RETRY_BASE_SECONDS * (2 ** min(attempt - 1, 16)),
            )
            if attempt == 1 or attempt & (attempt - 1) == 0:
                logger.info(
                    "task_result relay retry: task=%s offer=%s worker=%s attempt=%s delay_s=%.3f reason=%s",
                    task_id,
                    offer_id,
                    worker_id,
                    attempt,
                    delay,
                    retry_reason,
                )
            await asyncio.sleep(delay)

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

    def workers_snapshot(self) -> List[dict]:
        """Return the latest connected-worker cache without a control-plane round trip."""
        return list(self._local_workers.values())

    async def push_task_offer(self, worker_id: str, offer: dict) -> bool:
        """Low-latency dedicated path: push offer without waiting for gateway ack."""
        if not self._ws or not self._connected:
            return False
        if worker_id not in self._local_workers:
            return False
        try:
            await self._ws.send(
                json.dumps(
                    {
                        "type": "task_offer",
                        "worker_id": worker_id,
                        "offer": offer,
                    }
                )
            )
            return True
        except Exception as exc:
            logger.warning("push_task_offer failed: worker=%s error=%s", worker_id, exc)
            return False

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
            timeout=10.0,
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

