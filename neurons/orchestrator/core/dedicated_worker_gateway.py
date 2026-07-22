"""
External worker gateway adapter for SubnetCoreClient.set_worker_gateway().

Uses WorkerGatewayClient (control WebSocket to neurons/worker_gateway) while
matching the upstream in-process WorkerGateway interface.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _coerce_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class DedicatedWorkerGateway:
    """Route BeamCore offer batches to workers on a dedicated external gateway."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._upstream: Optional[Any] = None
        self._batch_load: dict[str, float] = {}

    def set_upstream(self, upstream: object) -> None:
        self._upstream = upstream
        if hasattr(self._client, "set_upstream"):
            self._client.set_upstream(upstream)

    @property
    def connected_count(self) -> int:
        return len(self.workers_snapshot())

    async def stop(self) -> None:
        if hasattr(self._client, "stop"):
            await self._client.stop()

    def reset_batch_load(self) -> None:
        self._batch_load.clear()

    def workers_snapshot(self) -> list[dict]:
        if hasattr(self._client, "workers_snapshot"):
            return self._client.workers_snapshot()
        return []

    def _pick_worker_id(self) -> Optional[str]:
        workers = self.workers_snapshot()
        assignable: list[dict] = []
        for worker in workers:
            worker_id = worker.get("worker_id")
            if not worker_id:
                continue
            if _coerce_float(worker.get("load_factor"), 0.0) >= 1.0:
                continue
            capacity = worker.get("capacity")
            if capacity is not None:
                try:
                    if int(capacity) <= 0:
                        continue
                except (TypeError, ValueError):
                    pass
            assignable.append(worker)

        if not assignable:
            return None

        def score(worker: dict) -> float:
            worker_id = worker["worker_id"]
            bandwidth = _coerce_float(worker.get("bandwidth_mbps"), 100.0)
            return bandwidth / (1.0 + self._batch_load.get(worker_id, 0.0))

        best = max(assignable, key=score)
        worker_id = str(best["worker_id"])
        self._batch_load[worker_id] = self._batch_load.get(worker_id, 0.0) + 1.0
        return worker_id

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        """Pick highest-bandwidth workers with light load bias (dedicated pool)."""
        selected: list[str] = []
        for _ in range(max(0, n)):
            worker_id = self._pick_worker_id()
            if not worker_id:
                break
            selected.append(worker_id)
        return selected

    async def deliver_task_offer(self, worker_id: str, offer: dict) -> bool:
        if hasattr(self._client, "push_task_offer"):
            delivered = await self._client.push_task_offer(worker_id, offer)
            if delivered:
                return True
        if hasattr(self._client, "send_task_offer"):
            return await self._client.send_task_offer(worker_id, offer)
        return False
