"""Persistent worker metrics for dedicated gateway (BeamCore-compatible shape)."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

TRUST_SUCCESS_DELTA = 0.001
TRUST_FAILURE_DELTA = 0.01
TRUST_MIN = 0.0
TRUST_MAX = 1.0
DEFAULT_TRUST = 0.5


def _running_average_bandwidth(
    current_avg: float, prior_task_count: int, new_sample_mbps: float
) -> float:
    """
    Update cumulative average bandwidth after a completed task.

    (avg * prior_task_count + new_sample) / (prior_task_count + 1)
    """
    if new_sample_mbps <= 0:
        return current_avg
    if prior_task_count <= 0:
        return float(new_sample_mbps)
    return (current_avg * prior_task_count + float(new_sample_mbps)) / (
        prior_task_count + 1
    )


def _coerce_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def format_worker_stats_row(
    raw: dict[str, Any],
    *,
    worker_id: Optional[str] = None,
    connected: bool = False,
) -> dict[str, Any]:
    """
    Normalize one worker dict to the BeamCore / public worker-gateway shape.

    Accepts legacy keys (e.g. bytes_relayed only) and fills derived fields.
    """
    wid = (worker_id or raw.get("worker_id") or "").strip()
    if not wid:
        raise ValueError("worker_id is required")

    total_tasks = _coerce_int(raw.get("total_tasks"), 0)
    successful_tasks = _coerce_int(
        raw.get("successful_tasks"),
        _coerce_int(raw.get("successful_task_count"), 0),
    )
    failed_tasks = _coerce_int(raw.get("failed_tasks"), 0)
    if total_tasks <= 0 and (successful_tasks or failed_tasks):
        total_tasks = successful_tasks + failed_tasks

    if successful_tasks + failed_tasks > total_tasks:
        total_tasks = successful_tasks + failed_tasks

    bytes_total = _coerce_int(
        raw.get("bytes_relayed_total"),
        _coerce_int(raw.get("bytes_relayed"), 0),
    )
    max_concurrent = max(1, _coerce_int(raw.get("max_concurrent_tasks"), 4))
    active_tasks = max(0, min(_coerce_int(raw.get("active_tasks"), 0), max_concurrent))

    if raw.get("success_rate") is not None:
        success_rate = round(_coerce_float(raw.get("success_rate"), 1.0), 4)
    elif total_tasks > 0:
        success_rate = round(successful_tasks / total_tasks, 4)
    else:
        success_rate = 1.0

    load_factor = raw.get("load_factor")
    if load_factor is not None:
        load_factor_val = round(min(1.0, max(0.0, _coerce_float(load_factor, 0.0))), 4)
    else:
        load_factor_val = round(min(1.0, active_tasks / max_concurrent), 4)

    status = (raw.get("status") or "offline").strip().lower()
    if connected:
        status = "active"

    return {
        "worker_id": wid,
        "region": (raw.get("region") or "unknown").strip() or "unknown",
        "status": status,
        "trust_score": round(
            min(TRUST_MAX, max(TRUST_MIN, _coerce_float(raw.get("trust_score"), DEFAULT_TRUST))),
            4,
        ),
        "bandwidth_mbps": round(_coerce_float(raw.get("bandwidth_mbps"), 100.0), 2),
        "success_rate": success_rate,
        "total_tasks": total_tasks,
        "successful_tasks": successful_tasks,
        "failed_tasks": failed_tasks,
        "bytes_relayed": bytes_total,
        "bytes_relayed_total": bytes_total,
        "load_factor": load_factor_val,
        "active_tasks": active_tasks,
        "capacity": max(0, max_concurrent - active_tasks),
        "max_concurrent_tasks": max_concurrent,
        "connected": connected,
        "last_seen": _coerce_float(raw.get("last_seen"), time.time()),
    }


def record_from_formatted_row(row: dict[str, Any]) -> WorkerRecord:
    """Build a WorkerRecord from a formatted worker stats row."""
    wid = str(row["worker_id"])
    rec = WorkerRecord(worker_id=wid)
    rec.region = row.get("region") or "unknown"
    rec.status = row.get("status") or "offline"
    rec.trust_score = _coerce_float(row.get("trust_score"), DEFAULT_TRUST)
    rec.bandwidth_mbps = _coerce_float(row.get("bandwidth_mbps"), 100.0)
    rec.total_tasks = _coerce_int(row.get("total_tasks"), 0)
    rec.successful_tasks = _coerce_int(row.get("successful_tasks"), 0)
    rec.failed_tasks = _coerce_int(row.get("failed_tasks"), 0)
    rec.success_rate = _coerce_float(row.get("success_rate"), 1.0)
    rec.bytes_relayed_total = _coerce_int(
        row.get("bytes_relayed_total"), _coerce_int(row.get("bytes_relayed"), 0)
    )
    rec.active_tasks = _coerce_int(row.get("active_tasks"), 0)
    rec.max_concurrent_tasks = max(1, _coerce_int(row.get("max_concurrent_tasks"), 4))
    rec.last_seen = _coerce_float(row.get("last_seen"), time.time())
    rec.recompute_success_rate()
    return rec


def parse_workers_input(payload: Any) -> Dict[str, dict[str, Any]]:
    """Accept {workers: {...}}, {workers: [...]}, or a bare map/list."""
    if payload is None:
        return {}

    if isinstance(payload, list):
        out: Dict[str, dict[str, Any]] = {}
        for item in payload:
            if isinstance(item, dict) and item.get("worker_id"):
                out[str(item["worker_id"])] = item
        return out

    if not isinstance(payload, dict):
        return {}

    workers = payload.get("workers", payload)
    if isinstance(workers, list):
        return parse_workers_input(workers)
    if isinstance(workers, dict):
        return {str(k): v for k, v in workers.items() if isinstance(v, dict)}
    return {}


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
    bytes_relayed_total: int = 0
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
            "bytes_relayed": self.bytes_relayed_total,
            "bytes_relayed_total": self.bytes_relayed_total,
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
        active_tasks: Optional[int] = None,
        region: Optional[str] = None,
        max_concurrent_tasks: Optional[int] = None,
    ) -> WorkerRecord:
        rec = self.get(worker_id)
        rec.status = "active"
        rec.last_seen = time.time()
        # Live telemetry only — cumulative bytes are updated on task_result_summary.
        if active_tasks is not None:
            rec.active_tasks = max(0, int(active_tasks))
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
        prior_total_tasks = rec.total_tasks
        rec.total_tasks += 1
        if success:
            rec.successful_tasks += 1
            rec.trust_score = min(TRUST_MAX, rec.trust_score + TRUST_SUCCESS_DELTA)
        else:
            rec.failed_tasks += 1
            rec.trust_score = max(TRUST_MIN, rec.trust_score - TRUST_FAILURE_DELTA)
        rec.recompute_success_rate()
        if bytes_transferred > 0:
            rec.bytes_relayed_total += int(bytes_transferred)
        if bandwidth_mbps is not None and bandwidth_mbps > 0:
            rec.bandwidth_mbps = _running_average_bandwidth(
                rec.bandwidth_mbps,
                prior_total_tasks,
                float(bandwidth_mbps),
            )
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

    def list_all_formatted(
        self, connected_ids: Optional[Dict[str, Any]] = None
    ) -> List[dict[str, Any]]:
        """All persisted workers in BeamCore shape (connected flag when online)."""
        connected_ids = connected_ids or {}
        rows: List[dict[str, Any]] = []
        for worker_id in sorted(self._records.keys()):
            connected = worker_id in connected_ids
            rows.append(
                format_worker_stats_row(
                    asdict(self._records[worker_id]),
                    worker_id=worker_id,
                    connected=connected,
                )
            )
        return rows

    def format_and_cache_initial(
        self,
        payload: Optional[Union[dict[str, Any], list[Any]]] = None,
        *,
        reload_file: bool = True,
    ) -> dict[str, Any]:
        """
        Normalize worker stats, refresh in-memory cache, and write formatted JSON.

        - With no payload: reload from metrics file (if present), then normalize.
        - With payload: merge/replace workers from POST body, then normalize.
        """
        incoming = parse_workers_input(payload)

        if incoming:
            # POST import replaces the in-memory cache with the provided workers.
            self._records.clear()
            for worker_id, raw in incoming.items():
                self._records[worker_id] = record_from_formatted_row(
                    format_worker_stats_row(raw, worker_id=worker_id, connected=False)
                )
        elif reload_file:
            self._records.clear()
            self._load()

        # Re-normalize every cached row (fixes legacy / inconsistent JSON).
        normalized: Dict[str, WorkerRecord] = {}
        for worker_id, rec in self._records.items():
            row = format_worker_stats_row(asdict(rec), worker_id=worker_id, connected=False)
            normalized[worker_id] = record_from_formatted_row(row)
        self._records = normalized
        self._save()

        formatted_rows = self.list_all_formatted()
        return {
            "count": len(formatted_rows),
            "workers": formatted_rows,
            "metrics_file": str(self._persist_path) if self._persist_path else None,
            "updated_at": time.time(),
        }

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
                    "active_tasks",
                    "max_concurrent_tasks",
                    "last_seen",
                    "first_seen",
                ):
                    if key in data and data[key] is not None:
                        setattr(rec, key, data[key])
                if data.get("bytes_relayed_total") is not None:
                    rec.bytes_relayed_total = int(data["bytes_relayed_total"])
                elif data.get("bytes_relayed") is not None:
                    rec.bytes_relayed_total = int(data["bytes_relayed"])
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
