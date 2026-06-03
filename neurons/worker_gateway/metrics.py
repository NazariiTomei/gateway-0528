"""Persistent worker metrics for dedicated gateway (BeamCore-compatible shape)."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

TRUST_SUCCESS_DELTA = 0.001
TRUST_FAILURE_DELTA = 0.01
TRUST_MIN = 0.0
TRUST_MAX = 1.0
DEFAULT_TRUST = 0.5


@dataclass
class WorkerRecord:
    """Aggregated worker stats (survives disconnect; used for list_workers / orch scoring)."""

    worker_id: str
    region: str = "unknown"
    status: str = "offline"
    trust_score: float = DEFAULT_TRUST
    bandwidth_mbps: float = 100.0
    success_rate: float = 1.0
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    bytes_relayed: int = 0
    active_tasks: int = 0
    max_concurrent_tasks: int = 4
    last_seen: float = field(default_factory=time.time)
    first_seen: float = field(default_factory=time.time)

    @property
    def load_factor(self) -> float:
        if self.max_concurrent_tasks <= 0:
            return 1.0
        return min(1.0, self.active_tasks / self.max_concurrent_tasks)

    def recompute_success_rate(self) -> None:
        if self.total_tasks <= 0:
            self.success_rate = 1.0
            return
        self.success_rate = round(self.successful_tasks / self.total_tasks, 4)

    def to_beamcore_dict(self, *, connected: bool) -> dict[str, Any]:
        """Shape aligned with public worker-gateway / BeamCore worker listings."""
        status = "active" if connected else self.status
        return {
            "worker_id": self.worker_id,
            "region": self.region,
            "status": status,
            "trust_score": round(self.trust_score, 4),
            "bandwidth_mbps": round(self.bandwidth_mbps, 2),
            "success_rate": self.success_rate,
            "total_tasks": self.total_tasks,
            "bytes_relayed": self.bytes_relayed,
            "bytes_relayed_total": self.bytes_relayed,
            "load_factor": round(self.load_factor, 4),
            "active_tasks": self.active_tasks,
            "capacity": max(0, self.max_concurrent_tasks - self.active_tasks),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "connected": connected,
            "last_seen": self.last_seen,
        }


class WorkerMetricsStore:
    """In-memory worker metrics with optional JSON persistence."""

    def __init__(self, persist_path: Optional[Path] = None) -> None:
        self._records: Dict[str, WorkerRecord] = {}
        self._persist_path = persist_path
        if persist_path:
            self._load()

    @property
    def persist_path(self) -> Optional[Path]:
        return self._persist_path

    @property
    def worker_count(self) -> int:
        return len(self._records)

    def get(self, worker_id: str) -> WorkerRecord:
        if worker_id not in self._records:
            self._records[worker_id] = WorkerRecord(worker_id=worker_id)
        return self._records[worker_id]

    def touch_connected(self, worker_id: str, *, region: Optional[str] = None) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.status = "active"
        rec.last_seen = time.time()
        if region and region.strip():
            rec.region = region.strip()
        self._save()
        return rec

    def touch_disconnected(self, worker_id: str) -> None:
        rec = self._records.get(worker_id)
        if not rec:
            return
        rec.status = "offline"
        rec.active_tasks = 0
        rec.last_seen = time.time()
        self._save()

    def apply_stats_snapshot(
        self,
        worker_id: str,
        *,
        bandwidth_mbps: Optional[float] = None,
        active_tasks: Optional[int] = None,
        bytes_relayed_delta: Optional[int] = None,
        region: Optional[str] = None,
        max_concurrent_tasks: Optional[int] = None,
    ) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.status = "active"
        rec.last_seen = time.time()
        if bandwidth_mbps is not None and bandwidth_mbps > 0:
            rec.bandwidth_mbps = float(bandwidth_mbps)
        if active_tasks is not None:
            rec.active_tasks = max(0, int(active_tasks))
        if bytes_relayed_delta is not None and bytes_relayed_delta > 0:
            rec.bytes_relayed += int(bytes_relayed_delta)
        if region and region.strip():
            rec.region = region.strip()
        if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
            rec.max_concurrent_tasks = int(max_concurrent_tasks)
        self._save()
        return rec

    def on_task_accept(self, worker_id: str) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.active_tasks += 1
        rec.last_seen = time.time()
        self._save()
        return rec

    def on_task_reject(self, worker_id: str) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.total_tasks += 1
        rec.failed_tasks += 1
        rec.recompute_success_rate()
        rec.trust_score = max(TRUST_MIN, rec.trust_score - TRUST_FAILURE_DELTA)
        rec.last_seen = time.time()
        self._save()
        return rec

    def on_task_result(
        self,
        worker_id: str,
        *,
        success: bool,
        bytes_transferred: int = 0,
        bandwidth_mbps: Optional[float] = None,
    ) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.total_tasks += 1
        if success:
            rec.successful_tasks += 1
            rec.trust_score = min(TRUST_MAX, rec.trust_score + TRUST_SUCCESS_DELTA)
        else:
            rec.failed_tasks += 1
            rec.trust_score = max(TRUST_MIN, rec.trust_score - TRUST_FAILURE_DELTA)
        rec.recompute_success_rate()
        if bytes_transferred > 0:
            rec.bytes_relayed += int(bytes_transferred)
        if bandwidth_mbps is not None and bandwidth_mbps > 0:
            rec.bandwidth_mbps = float(bandwidth_mbps)
        rec.active_tasks = max(0, rec.active_tasks - 1)
        rec.last_seen = time.time()
        self._save()
        return rec

    def list_connected_records(
        self, connected_ids: Dict[str, Any]
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for worker_id in connected_ids:
            rec = self.get(worker_id)
            out.append(rec.to_beamcore_dict(connected=True))
        return out

    def _load(self) -> None:
        if not self._persist_path or not self._persist_path.is_file():
            return
        try:
            raw = json.loads(self._persist_path.read_text(encoding="utf-8"))
            workers = raw.get("workers") if isinstance(raw, dict) else raw
            if not isinstance(workers, dict):
                return
            for worker_id, data in workers.items():
                if not isinstance(data, dict):
                    continue
                rec = WorkerRecord(worker_id=str(worker_id))
                for key in (
                    "region",
                    "status",
                    "trust_score",
                    "bandwidth_mbps",
                    "success_rate",
                    "total_tasks",
                    "successful_tasks",
                    "failed_tasks",
                    "bytes_relayed",
                    "active_tasks",
                    "max_concurrent_tasks",
                    "last_seen",
                    "first_seen",
                ):
                    if key in data and data[key] is not None:
                        setattr(rec, key, data[key])
                rec.recompute_success_rate()
                self._records[str(worker_id)] = rec
            logger.info(
                "Loaded worker metrics for %s workers from %s",
                len(self._records),
                self._persist_path,
            )
        except Exception as exc:
            logger.warning("Failed to load worker metrics from %s: %s", self._persist_path, exc)

    def _save(self) -> None:
        if not self._persist_path:
            return
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "workers": {
                    wid: asdict(rec) for wid, rec in self._records.items()
                },
                "updated_at": time.time(),
            }
            tmp = self._persist_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self._persist_path)
        except Exception as exc:
            logger.warning("Failed to persist worker metrics: %s", exc)
