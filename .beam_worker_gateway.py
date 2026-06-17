"""
In-process worker gateway.

Workers connect via WebSocket to /ws/{worker_id}?api_key=...
The orchestrator forwards task offer batch items as task_offer messages,
and relays task_accept / task_reject / task_result upstream.
"""

import asyncio
import json
import logging
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

MAX_WORKERS = 10


class WorkerGateway:
    """Manages WebSocket sessions for locally-connected workers."""

    def __init__(
        self,
        on_ready_change: Optional[Callable[[bool], None]] = None,
    ) -> None:
        self._sessions: Dict[str, object] = {}  # worker_id → WebSocket
        self._cursor = 0
        self._on_ready_change = on_ready_change
        self._upstream: Optional[object] = None  # SubnetCoreClient ref

    def set_upstream(self, upstream: object) -> None:
        self._upstream = upstream

    @property
    def connected_count(self) -> int:
        return len(self._sessions)

    @property
    def worker_ids(self) -> list:
        return list(self._sessions.keys())

    def is_full(self) -> bool:
        return len(self._sessions) >= MAX_WORKERS

    def connect(self, worker_id: str, ws: object) -> bool:
        if self.is_full() and worker_id not in self._sessions:
            logger.warning("Worker cap reached (%d); rejecting %s", MAX_WORKERS, worker_id)
            return False
        was_empty = len(self._sessions) == 0
        self._sessions[worker_id] = ws
        logger.info("Worker connected: %s (%d/%d)", worker_id, len(self._sessions), MAX_WORKERS)
        if was_empty and self._on_ready_change:
            self._on_ready_change(True)
        return True

    def disconnect(self, worker_id: str) -> None:
        self._sessions.pop(worker_id, None)
        logger.info("Worker disconnected: %s (%d/%d)", worker_id, len(self._sessions), MAX_WORKERS)
        if len(self._sessions) == 0 and self._on_ready_change:
            self._on_ready_change(False)

    async def deliver_task_offer(self, worker_id: str, offer: dict) -> bool:
        ws = self._sessions.get(worker_id)
        if ws is None:
            logger.warning("deliver_task_offer: worker %s not connected", worker_id)
            return False
        try:
            await ws.send_text(json.dumps({"type": "task_offer", **offer}))
            return True
        except Exception as exc:
            logger.warning("deliver_task_offer send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)
            return False

    def get_workers_round_robin(self, n: int = 1) -> list:
        """Return up to n worker_ids in round-robin order."""
        ids = list(self._sessions.keys())
        if not ids:
            return []
        selected = []
        for _ in range(min(n, len(ids))):
            selected.append(ids[self._cursor % len(ids)])
            self._cursor += 1
        return selected

    async def handle_worker_message(self, worker_id: str, raw: str) -> None:
        """Process an inbound message from a connected worker."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Non-JSON from worker %s", worker_id)
            return

        msg_type = msg.get("type")
        if msg_type in ("task_accept", "task_reject"):
            await self._relay_task_decision(worker_id, msg)
        elif msg_type == "task_result":
            await self._relay_task_result(worker_id, msg)
        else:
            logger.debug("Unhandled worker message type %s from %s", msg_type, worker_id)

    async def _send_to_worker(self, worker_id: str, payload: dict) -> None:
        ws = self._sessions.get(worker_id)
        if ws is None:
            return
        try:
            await ws.send_text(json.dumps(payload))
        except Exception as exc:
            logger.warning("worker ack send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)

    async def _relay_task_decision(self, worker_id: str, msg: dict) -> None:
        ack_type = "task_accept_ack" if msg.get("type") == "task_accept" else "task_reject_ack"
        if self._upstream is None:
            await self._send_to_worker(
                worker_id,
                {"type": ack_type, "task_id": msg.get("task_id"), "offer_id": msg.get("offer_id"), "accepted": False, "reason": "beamcore_unavailable"},
            )
            return
        task_id = msg.get("task_id") or msg.get("offer_id")
        offer_id = msg.get("offer_id") or task_id
        reason = msg.get("reason")
        try:
            if msg.get("type") == "task_accept":
                ack = await self._upstream.send_task_accept(
                    task_id=task_id,
                    worker_id=worker_id,
                    offer_id=offer_id,
                    worker_version=msg.get("worker_version"),
                )
            else:
                ack = await self._upstream.send_task_reject(
                    task_id=task_id,
                    worker_id=worker_id,
                    offer_id=offer_id,
                    reason=reason,
                )
        except Exception as exc:
            logger.warning("relay task decision failed: %s", exc)
            ack = {
                "type": ack_type,
                "task_id": task_id,
                "offer_id": offer_id,
                "accepted": False,
                "reason": "beamcore_decision_forward_failed",
            }
        ack_payload = {
            **(ack if isinstance(ack, dict) else {}),
            "type": ack_type,
            "task_id": task_id,
            "offer_id": offer_id,
        }
        await self._send_to_worker(worker_id, ack_payload)

    async def _relay_task_result(self, worker_id: str, msg: dict) -> None:
        if self._upstream is None:
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": msg.get("task_id"),
                    "offer_id": msg.get("offer_id"),
                    "received": False,
                    "completed": False,
                    "reason": "beamcore_unavailable",
                },
            )
            return
        try:
            task_id = msg.get("task_id")
            offer_id = msg.get("offer_id") or task_id
            if not task_id or not offer_id:
                logger.warning("dropping task_result missing task_id/offer_id from worker=%s", worker_id)
                await self._send_to_worker(
                    worker_id,
                    {
                        "type": "task_result_ack",
                        "task_id": task_id,
                        "offer_id": offer_id,
                        "received": False,
                        "completed": False,
                        "reason": "missing_task_or_offer_id",
                    },
                )
                return
            payload = {
                "type": "task_result",
                "task_id": task_id,
                "offer_id": offer_id,
                "worker_id": worker_id,
                "success": bool(msg.get("success")),
            }
            for key in ("etag", "chunk_hash", "error"):
                if msg.get(key) is not None:
                    payload[key] = msg[key]
            ack = await self._upstream.send_task_result(payload)
        except Exception as exc:
            logger.warning("relay task_result failed: %s", exc)
            ack = {
                "type": "task_result_ack",
                "task_id": msg.get("task_id"),
                "offer_id": msg.get("offer_id") or msg.get("task_id"),
                "received": False,
                "completed": False,
                "reason": "beamcore_result_forward_failed",
            }
        ack_payload = {
            **(ack if isinstance(ack, dict) else {}),
            "type": "task_result_ack",
            "task_id": msg.get("task_id"),
            "offer_id": msg.get("offer_id") or msg.get("task_id"),
        }
        await self._send_to_worker(worker_id, ack_payload)
