"""Cluster-wide health scoring.

The evaluator turns the same raw signals the Nodes / Metrics / Fault pages
already show into one stable 0-100 score plus per-dimension and per-node
detail, so a glance at the overview page answers "is the cluster healthy?".

Dimensions (fixed weights, exposed in the report so the UI can show them):

* **availability (0.30)** — alive/dead plus stale-but-not-yet-reaped workers.
  A single node going silent therefore costs a predictable
  ``0.30 * 100 / N`` points (plus a small bounded worst-node tail term, so one
  dead node stays visible even in large clusters);
* **heartbeat stability (0.20)** — heartbeat freshness (age vs. the observed
  median interval) and interval jitter per alive node;
* **resource load (0.25)** — window-averaged CPU / memory / load-per-core,
  only penalising values close to saturation (busy-but-healthy stays high);
* **task quality (0.25)** — failed vs. succeeded task *events* inside a
  sliding window.  Failures are the same ``task_failed`` fault documents the
  Fault page lists; successes are the same task metric samples the Metrics
  page charts.  The rate is shrunk when the window holds few tasks so a
  single failure on an idle cluster cannot crash the score.

Missing data never produces fake numbers: a dimension a node cannot provide
is excluded for that node, the remaining dimensions are re-normalised by
their weights, and a small bounded "uncertainty" deduction (max 5 points)
reflects the missing coverage.  Newly registered workers get a grace period
before their missing samples count against coverage.

Stability: every raw score passes through a **time-aware asymmetric EWMA**
(``alpha = 1 - exp(-dt/tau)``; deterioration moves roughly twice as fast as
recovery).  The smoothed state is persisted under ``health/state.json`` so
restarts do not make the score jump, and ``dt`` (not call count) drives the
filter so the cadence of polling cannot change the result.
"""

from __future__ import annotations

import math
import os
import statistics
import threading
from dataclasses import dataclass
from typing import Optional

from backend.common.jsonutil import now_ms
from backend.common.storage import Storage, list_files, read_json

# ---------------------------------------------------------------------------
# Weights / thresholds (single vocabulary shared with the frontend via report)
# ---------------------------------------------------------------------------
DIMENSION_KEYS = ("availability", "heartbeat", "load", "tasks")
DIMENSION_WEIGHTS = {
    "availability": 0.30,
    "heartbeat": 0.20,
    "load": 0.25,
    "tasks": 0.25,
}
# Within a single node, availability carries a sharp tail risk (its tasks all
# fail), so the cluster availability score blends the mean with the worst node.
AVAILABILITY_WORST_BLEND = 0.20

# Heartbeat freshness 65% / interval stability 35%.
HB_FRESHNESS_WEIGHT = 0.65

# Resource load sub-weights.
LOAD_CPU_WEIGHT = 0.45
LOAD_MEM_WEIGHT = 0.35
LOAD_RATIO_WEIGHT = 0.20

# Below this many task events in the window, the failure rate is shrunk toward
# 0 ("not enough evidence") instead of being taken at face value.
TASK_MIN_EVENTS = 10

# Coverage gap costs at most this many points of the final score.
UNCERTAINTY_MAX_PENALTY = 5.0

# Downgrades propagate ~2x faster than recoveries (pain now, trust slowly).
EWMA_DOWN_FACTOR = 2.0

# A state file older than this is treated as cold (e.g. cluster was shut over
# the weekend) and re-initialised instead of being EWMA-blended.
REINIT_GAP_MS = 15 * 60 * 1000

HISTORY_LIMIT = 90

GRADE_GOOD = "good"
GRADE_WARN = "warn"
GRADE_BAD = "bad"
GRADE_UNKNOWN = "unknown"
GRADE_THRESHOLDS = ((80.0, GRADE_GOOD), (60.0, GRADE_WARN), (-1.0, GRADE_BAD))


def grade_for(score: Optional[float]) -> str:
    if score is None:
        return GRADE_UNKNOWN
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return GRADE_BAD


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _ramp(value: float, full_score_below: float, zero_at: float) -> float:
    """100 while ``value <= full_score_below``, linear to 0 at ``zero_at``."""
    if value <= full_score_below:
        return 100.0
    if value >= zero_at:
        return 0.0
    return 100.0 * (zero_at - value) / (zero_at - full_score_below)


def _round(x: Optional[float], ndigits: int = 1) -> Optional[float]:
    return None if x is None else round(x, ndigits)


# ---------------------------------------------------------------------------
# Per-node raw evaluation
# ---------------------------------------------------------------------------
@dataclass
class _NodeEval:
    worker_id: str
    name: str
    status: str
    alive: bool
    availability: float
    heartbeat: Optional[float]
    load: Optional[float]
    hb_age_ms: Optional[int]
    expected_interval_ms: int
    jitter_cv: Optional[float]
    cpu_avg: Optional[float]
    mem_avg: Optional[float]
    load1_avg: Optional[float]
    load_ratio: Optional[float]
    sample_count: int
    in_grace: bool = False
    tasks_ok: int = 0
    tasks_failed: int = 0
    missing: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "name": self.name,
            "status": self.status,
            "alive": self.alive,
            "score": _round(self.node_score()),
            "availability": _round(self.availability),
            "heartbeat": _round(self.heartbeat),
            "load": _round(self.load),
            "hb_age_ms": self.hb_age_ms,
            "expected_interval_ms": self.expected_interval_ms,
            "jitter_cv": _round(self.jitter_cv, 3),
            "cpu_avg": _round(self.cpu_avg),
            "mem_avg": _round(self.mem_avg),
            "load1_avg": _round(self.load1_avg, 2),
            "load_ratio": _round(self.load_ratio, 2),
            "sample_count": self.sample_count,
            "in_grace": self.in_grace,
            "tasks_ok_window": self.tasks_ok,
            "tasks_failed_window": self.tasks_failed,
            "missing": list(self.missing),
        }

    def node_score(self) -> Optional[float]:
        """Composite for the per-node table; cluster-level task dim excluded."""
        parts = [(DIMENSION_WEIGHTS["availability"], self.availability)]
        if self.heartbeat is not None:
            parts.append((DIMENSION_WEIGHTS["heartbeat"], self.heartbeat))
        if self.load is not None:
            parts.append((DIMENSION_WEIGHTS["load"], self.load))
        total_w = sum(w for w, _ in parts)
        return sum(w * s for w, s in parts) / total_w if total_w else None


class HealthEvaluator:
    def __init__(self, storage: Storage, registry, config) -> None:
        self.storage = storage
        self.registry = registry
        self.config = config
        self._lock = threading.RLock()
        self._state: Optional[dict] = None
        self._last_report: Optional[dict] = None
        self._load()

    # ------------------------------------------------------------------
    # Persistence (only the smoothed filter state + history, never raw data)
    # ------------------------------------------------------------------
    def _load(self) -> None:
        doc = self.storage.read("health", "state.json", default=None)
        if isinstance(doc, dict) and isinstance(doc.get("smoothed"), dict):
            self._state = doc

    def _save(self) -> None:
        self.storage.write(self._state, "health", "state.json")

    # ------------------------------------------------------------------
    # Raw input collection (same files the other pages read)
    # ------------------------------------------------------------------
    def _worker_samples(self, worker_id: str, cutoff_ms: int) -> list[dict]:
        path_parts = ("metrics", "workers", f"{worker_id}.jsonl")
        try:
            lines = self.storage.read_lines(*path_parts)
        except OSError:
            return []
        return [s for s in lines if isinstance(s, dict) and s.get("ts_ms", 0) >= cutoff_ms]

    def _task_events(self, task_cutoff_ms: int) -> tuple[list[dict], list[dict]]:
        """(success samples, task_failed fault docs) inside the task window."""
        successes: list[dict] = []
        for path in list_files(self.storage.path("metrics", "jobs"), suffix=".jsonl"):
            for line in self.storage.read_lines("metrics", "jobs", os.path.basename(path)):
                if isinstance(line, dict) and line.get("ts_ms", 0) >= task_cutoff_ms:
                    successes.append(line)

        failures: list[dict] = []
        for job_dir in self.storage.subdirs("jobs"):
            job_id = os.path.basename(job_dir)
            for path in list_files(self.storage.path("jobs", job_id, "faults"), suffix=".json"):
                doc = read_json(path)
                if (
                    isinstance(doc, dict)
                    and doc.get("kind") == "task_failed"
                    and doc.get("created_ms", 0) >= task_cutoff_ms
                ):
                    failures.append(doc)
        return successes, failures

    # ------------------------------------------------------------------
    # Per-node dimensions
    # ------------------------------------------------------------------
    def _evaluate_node(self, worker, samples: list[dict], now: int,
                       window_ms: int, timeout_ms: int,
                       fallback_interval_ms: int) -> _NodeEval:
        age = max(0, now - int(worker.last_heartbeat_ms or 0))
        missing: list[str] = []

        # -- availability -------------------------------------------------
        if not worker.is_alive:
            availability = 0.0
        elif age <= int(fallback_interval_ms * 1.5):
            availability = 100.0
        else:
            # Officially alive but heartbeats are lagging: decay to a 40 floor
            # at the reap timeout (death itself is scored as exactly 0).
            availability = _clamp(
                100.0 - 60.0 * (age - 1.5 * fallback_interval_ms)
                / max(1.0, timeout_ms - 1.5 * fallback_interval_ms),
                lo=40.0,
            )

        # -- heartbeat stability -----------------------------------------
        ts_list = sorted(int(s["ts_ms"]) for s in samples if s.get("ts_ms"))
        heartbeat: Optional[float] = None
        jitter_cv: Optional[float] = None
        expected = fallback_interval_ms
        if len(ts_list) >= 2:
            gaps = [b - a for a, b in zip(ts_list, ts_list[1:]) if b > a]
            if gaps:
                expected = max(1, int(statistics.median(gaps)))
                mean_gap = statistics.mean(gaps)
                if mean_gap > 0 and len(gaps) >= 2:
                    jitter_cv = statistics.pstdev(gaps) / mean_gap
        freshness = _ramp(age, expected * 1.5, timeout_ms)
        if jitter_cv is not None:
            stability = 100.0 * max(0.0, 1.0 - _clamp(jitter_cv, 0.0, 1.0))
            heartbeat = HB_FRESHNESS_WEIGHT * freshness + (1 - HB_FRESHNESS_WEIGHT) * stability
        else:
            heartbeat = freshness

        grace_ms = max(window_ms, 10 * fallback_interval_ms)
        in_grace = worker.is_alive and (now - int(worker.registered_ms or now)) < grace_ms
        if not samples:
            if in_grace:
                # New node that has not produced a full window yet: neutral.
                heartbeat = None
            else:
                missing.append("heartbeat")

        # -- resource load ------------------------------------------------
        cpu_avg = mem_avg = load1_avg = load_ratio = None
        load_score: Optional[float] = None
        if samples:
            cpu_vals = [float(s["cpu_percent"]) for s in samples if s.get("cpu_percent") is not None]
            mem_vals = [float(s["mem_percent"]) for s in samples if s.get("mem_percent") is not None]
            load_vals = [float(s["load1"]) for s in samples if s.get("load1") is not None]
            if cpu_vals:
                cpu_avg = statistics.mean(cpu_vals)
            if mem_vals:
                mem_avg = statistics.mean(mem_vals)
            if load_vals:
                load1_avg = statistics.mean(load_vals)
                cores = max(1, int(worker.cpu_cores or 1))
                load_ratio = _clamp(load1_avg / cores, 0.0, 4.0)

            parts = []
            if cpu_avg is not None:
                parts.append((LOAD_CPU_WEIGHT, _ramp(cpu_avg, 60.0, 95.0)))
            if mem_avg is not None:
                parts.append((LOAD_MEM_WEIGHT, _ramp(mem_avg, 70.0, 97.0)))
            if load_ratio is not None:
                # Healthy up to 0.7 per core, saturated at 2 per core.
                parts.append((LOAD_RATIO_WEIGHT, _ramp(load_ratio, 0.7, 2.0)))
            total_w = sum(w for w, _ in parts)
            if total_w:
                load_score = sum(w * s for w, s in parts) / total_w
        else:
            # Grace-period nodes simply have not reported long enough; only a
            # long-lived node with no samples counts as a real coverage gap.
            if not in_grace:
                missing.append("load")

        if not worker.is_alive:
            # A dead node cannot report; absence is explained by availability.
            heartbeat = None
            load_score = None
            in_grace = False
            missing = [m for m in missing if m in ("heartbeat", "load")]

        return _NodeEval(
            worker_id=worker.worker_id,
            name=worker.name,
            status=worker.status,
            alive=worker.is_alive,
            availability=availability,
            heartbeat=heartbeat,
            load=load_score,
            hb_age_ms=age if worker.last_heartbeat_ms else None,
            expected_interval_ms=expected,
            jitter_cv=jitter_cv,
            cpu_avg=cpu_avg,
            mem_avg=mem_avg,
            load1_avg=load1_avg,
            load_ratio=load_ratio,
            sample_count=len(samples),
            in_grace=in_grace,
            missing=tuple(missing),
        )

    # ------------------------------------------------------------------
    # Full raw report
    # ------------------------------------------------------------------
    def _compute(self, now: int) -> Optional[dict]:
        cfg = self.config
        window_ms = int(cfg.health_window_sec * 1000)
        task_window_ms = int(cfg.health_task_window_sec * 1000)
        cutoff = now - window_ms
        task_cutoff = now - task_window_ms
        timeout_ms = int(cfg.heartbeat_timeout_sec * 1000)
        fallback_interval_ms = max(1, int(cfg.heartbeat_interval_sec * 1000))

        workers = self.registry.all()
        if not workers:
            return {
                "ts_ms": now,
                "score": None,
                "grade": GRADE_UNKNOWN,
                "window_sec": cfg.health_window_sec,
                "task_window_sec": cfg.health_task_window_sec,
                "smoothing_sec": cfg.health_smoothing_sec,
                "weights": dict(DIMENSION_WEIGHTS),
                "coverage": 1.0,
                "missing_workers": [],
                "dimensions": {},
                "nodes": [],
            }

        nodes: list[_NodeEval] = []
        for worker in workers:
            samples = self._worker_samples(worker.worker_id, cutoff) if worker.is_alive else []
            nodes.append(self._evaluate_node(
                worker, samples, now, window_ms, timeout_ms, fallback_interval_ms,
            ))

        # -- task-level events (cluster-wide, same docs as Fault/Metrics) --
        successes, failures = self._task_events(task_cutoff)
        tasks_ok = len(successes)
        tasks_failed = len(failures)
        task_events = tasks_ok + tasks_failed
        failure_rate = (tasks_failed / task_events) if task_events else None
        low_sample = bool(task_events and task_events < TASK_MIN_EVENTS)
        if task_events:
            confidence = min(1.0, task_events / TASK_MIN_EVENTS)
            task_score = 100.0 * (1.0 - confidence * failure_rate)
        else:
            task_score = None

        # Attribute window events to nodes for the detail table.
        per_node_ok: dict[str, int] = {}
        per_node_failed: dict[str, int] = {}
        for ev in successes:
            wid = ev.get("worker_id")
            if wid:
                per_node_ok[wid] = per_node_ok.get(wid, 0) + 1
        for ev in failures:
            wid = ev.get("worker_id")
            if wid:
                per_node_failed[wid] = per_node_failed.get(wid, 0) + 1
        by_id = {n.worker_id: n for n in nodes}
        for wid, count in per_node_ok.items():
            if wid in by_id:
                by_id[wid].tasks_ok = count
        for wid, count in per_node_failed.items():
            if wid in by_id:
                by_id[wid].tasks_failed = count

        # -- dimension aggregates -----------------------------------------
        alive_nodes = [n for n in nodes if n.alive]

        avail_scores = [n.availability for n in nodes]
        avail_mean = statistics.mean(avail_scores)
        availability = (
            (1 - AVAILABILITY_WORST_BLEND) * avail_mean
            + AVAILABILITY_WORST_BLEND * min(avail_scores)
        )

        hb_scored = [n.heartbeat for n in alive_nodes if n.heartbeat is not None]
        hb_missing = sum(1 for n in alive_nodes
                         if "heartbeat" in n.missing and not n.in_grace)
        hb_denominator = sum(1 for n in alive_nodes if not n.in_grace)
        hb_coverage = ((hb_denominator - hb_missing) / hb_denominator) if hb_denominator else 1.0
        heartbeat = statistics.mean(hb_scored) if hb_scored else None
        hb_ages = [n.hb_age_ms for n in alive_nodes if n.hb_age_ms is not None]
        hb_cvs = [n.jitter_cv for n in alive_nodes if n.jitter_cv is not None]

        load_scored = [n.load for n in alive_nodes if n.load is not None]
        load_missing = sum(1 for n in alive_nodes
                           if "load" in n.missing and not n.in_grace)
        load_coverage = ((hb_denominator - load_missing) / hb_denominator) if hb_denominator else 1.0
        load = statistics.mean(load_scored) if load_scored else None
        cpu_vals = [n.cpu_avg for n in alive_nodes if n.cpu_avg is not None]
        mem_vals = [n.mem_avg for n in alive_nodes if n.mem_avg is not None]
        ratio_vals = [n.load_ratio for n in alive_nodes if n.load_ratio is not None]

        raw_dimensions = {
            "availability": availability,
            "heartbeat": heartbeat,
            "load": load,
            "tasks": task_score,
        }
        coverage_by_dim = {
            "availability": 1.0,
            "heartbeat": hb_coverage,
            "load": load_coverage,
            "tasks": 1.0 if task_events else 0.0,
        }
        present = [k for k in DIMENSION_KEYS if raw_dimensions[k] is not None]
        weight_total = sum(DIMENSION_WEIGHTS[k] for k in present)
        base_score = (
            sum(DIMENSION_WEIGHTS[k] * raw_dimensions[k] for k in present) / weight_total
            if weight_total else None
        )
        coverage = (
            sum(DIMENSION_WEIGHTS[k] * coverage_by_dim[k] for k in present) / weight_total
            if weight_total else 1.0
        )
        if base_score is not None:
            score = base_score * (1.0 - (UNCERTAINTY_MAX_PENALTY / 100.0) * (1.0 - coverage))
        else:
            score = None

        dead = [n for n in nodes if not n.alive]
        stale = [
            n for n in alive_nodes
            if n.hb_age_ms is not None and n.hb_age_ms > fallback_interval_ms * 1.5
        ]

        def dim_dict(key: str, raw: Optional[float], detail: dict) -> dict:
            return {
                "key": key,
                "weight": DIMENSION_WEIGHTS[key],
                "raw_score": _round(raw),
                "score": _round(raw),      # replaced by smoothed value below
                "coverage": round(coverage_by_dim[key], 3),
                "available": raw is not None,
                "detail": detail,
            }

        dimensions = {
            "availability": dim_dict("availability", availability, {
                "workers_total": len(nodes),
                "alive": len(alive_nodes),
                "dead": len(dead),
                "stale": len(stale),
                "mean_score": round(avail_mean, 1),
                "worst_score": round(min(avail_scores), 1),
            }),
            "heartbeat": dim_dict("heartbeat", heartbeat, {
                "expected_interval_ms": fallback_interval_ms,
                "avg_age_ms": round(statistics.mean(hb_ages)) if hb_ages else None,
                "avg_jitter_cv": round(statistics.mean(hb_cvs), 3) if hb_cvs else None,
                "reporting_workers": len(hb_scored),
            }),
            "load": dim_dict("load", load, {
                "avg_cpu": round(statistics.mean(cpu_vals), 1) if cpu_vals else None,
                "avg_mem": round(statistics.mean(mem_vals), 1) if mem_vals else None,
                "avg_load_ratio": round(statistics.mean(ratio_vals), 2) if ratio_vals else None,
                "reporting_workers": len(load_scored),
            }),
            "tasks": dim_dict("tasks", task_score, {
                "window_sec": cfg.health_task_window_sec,
                "succeeded": tasks_ok,
                "failed": tasks_failed,
                "failure_rate": round(failure_rate, 4) if failure_rate is not None else None,
                "low_sample": low_sample,
            }),
        }

        missing_workers = [
            {"worker_id": n.worker_id, "name": n.name, "missing": list(n.missing)}
            for n in nodes if n.missing
        ]

        raw_report = {
            "ts_ms": now,
            "score": _round(score),
            "grade": grade_for(score),
            "window_sec": cfg.health_window_sec,
            "task_window_sec": cfg.health_task_window_sec,
            "smoothing_sec": cfg.health_smoothing_sec,
            "weights": dict(DIMENSION_WEIGHTS),
            "coverage": round(coverage, 3),
            "missing_workers": missing_workers,
            "dimensions": dimensions,
            "nodes": [n.to_dict() for n in sorted(nodes, key=lambda x: (x.alive, x.node_score() if x.node_score() is not None else -1))],
        }
        return raw_report

    # ------------------------------------------------------------------
    # Smoothing
    # ------------------------------------------------------------------
    def _ewma(self, previous: Optional[float], target: float, dt_ms: int,
              worsening: bool) -> float:
        if previous is None:
            return target
        if dt_ms >= REINIT_GAP_MS:
            return target
        tau_ms = max(1.0, float(self.config.health_smoothing_sec) * 1000.0)
        alpha = 1.0 - math.exp(-dt_ms / tau_ms)
        if worsening:
            alpha = min(1.0, alpha * EWMA_DOWN_FACTOR)
        return previous + alpha * (target - previous)

    def _smooth(self, report: dict) -> dict:
        now = report["ts_ms"]
        score = report["score"]

        previous = (self._state or {}).get("smoothed") or {}
        prev_ts = (self._state or {}).get("ts_ms")
        dt_ms = max(0, now - prev_ts) if prev_ts else 0

        if score is None:
            smoothed_score = None
        elif "score" not in previous or previous.get("score") is None or dt_ms >= REINIT_GAP_MS:
            smoothed_score = score
        else:
            smoothed_score = self._ewma(
                previous["score"], score, dt_ms, worsening=score < previous["score"],
            )

        dims = report["dimensions"]
        for key, dim in dims.items():
            raw = dim["raw_score"]
            prev = previous.get("dimensions", {}).get(key)
            if raw is None:
                dim["score"] = None
            elif prev is None or dt_ms >= REINIT_GAP_MS:
                dim["score"] = _round(raw)
            else:
                dim["score"] = _round(self._ewma(prev, raw, dt_ms, worsening=raw < prev))

        report["score"] = _round(smoothed_score)
        report["grade"] = grade_for(smoothed_score)
        report["raw_score"] = _round_raw(report)

        history = list((self._state or {}).get("history", []))
        if smoothed_score is not None:
            history.append({"ts_ms": now, "score": report["score"]})
        report["history"] = history[-HISTORY_LIMIT:]

        self._state = {
            "ts_ms": now,
            "smoothed": {
                "score": smoothed_score,
                "dimensions": {k: d["score"] for k, d in dims.items()},
            },
            "history": report["history"],
        }
        return report

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def evaluate(self, now_ms_override: Optional[int] = None) -> Optional[dict]:
        """Recompute raw signals, advance the EWMA and persist. Returns report."""
        with self._lock:
            now = int(now_ms_override if now_ms_override is not None else now_ms())
            raw = self._compute(now)
            assert raw is not None
            report = self._smooth(raw)
            self._last_report = report
            self._save()
            return report

    def snapshot(self, max_age_ms: int = 4000) -> Optional[dict]:
        """Last evaluated report; force one on cold start / stale state."""
        with self._lock:
            fresh = self._state is not None and (now_ms() - self._state.get("ts_ms", 0)) <= max_age_ms
            if not fresh or self._last_report is None:
                return self.evaluate()
            # The scheduler re-evaluates every tick, so the cached report is at
            # most one tick old; the API route reads this without doing IO.
            return self._last_report


def _round_raw(report: dict) -> Optional[float]:
    """Weighted mean of raw dimension scores (before uncertainty penalty)."""
    total_w = 0.0
    acc = 0.0
    for key, dim in report["dimensions"].items():
        raw = dim["raw_score"]
        if raw is not None:
            total_w += DIMENSION_WEIGHTS[key]
            acc += DIMENSION_WEIGHTS[key] * raw
    return _round(acc / total_w) if total_w else None
