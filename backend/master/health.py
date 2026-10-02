"""Cluster health scoring.

One number (0-100) summarising whether the cluster is healthy, plus the full
per-dimension and per-node breakdown so the number can always be traced back to
the raw values shown on the nodes and metrics pages.

Design goals (matching the operational requirements):

* **Explicit weighting** — four dimensions are combined with fixed, documented
  weights: node availability, heartbeat stability, node load, and task
  reliability. Dimensions with no data are dropped and the remaining weights
  are renormalised, so missing telemetry can neither inflate nor crash the
  score.
* **Bounded blast radius of one bad node** — node-level scores are combined
  with capacity weights, then a small "short-board" penalty lets the worst node
  drag the total down without ever dominating it (capped at 15 points).
* **Smoothness** — every dimension and the total are filtered with a
  time-based exponential moving average that reacts fast to deterioration and
  recovers slowly, so a momentary metric spike cannot make the score yo-yo.
  The filter state is persisted (``metrics/health_snapshot.json``) so a Master
  restart does not reset the baseline.
* **Traceability** — the response carries both the raw inputs (heartbeat age,
  cpu/mem/load, failure counts) and the derived sub-scores for every node.

Task outcomes (success/failure per worker) are appended to a small JSONL event
log; reliability is measured over a sliding window and falls back to
cumulative counters when the window has too few samples.
"""

from __future__ import annotations

import math
import threading
from typing import Optional

from backend.common.jsonutil import now_ms
from backend.common.storage import Storage, read_jsonl

# ---------------------------------------------------------------------------
# Tunables (kept here on purpose: they define the scoring contract)
# ---------------------------------------------------------------------------
WEIGHTS = {
    "availability": 0.25,   # share of nodes alive
    "heartbeat": 0.25,      # heartbeat freshness + cadence stability
    "load": 0.30,           # cpu / mem / load-average saturation
    "reliability": 0.20,    # 1 - task failure rate
}

# Time constants of the asymmetric EWMA (milliseconds).  Degradation is tracked
# quickly, recovery is earned slowly — this is what keeps the score stable.
TAU_DOWN_MS = 10_000.0
TAU_UP_MS = 40_000.0

RELIABILITY_WINDOW_MS = 10 * 60_000     # sliding window for failure rate
RELIABILITY_MIN_SAMPLES = 5             # below this, fall back to cumulative
FAIL_RATE_FULL_PENALTY = 0.20           # 20% failures -> reliability score 0

HB_FRESH_RATIO = 0.33                   # age <= 1/3 timeout: perfectly fresh
HB_STABLE_JITTER = 0.20                 # <=20% interval jitter costs nothing
HB_STABILITY_SHARE = 0.4                # weight of cadence stability vs age

LOAD_CPU_SHARE = 0.5
LOAD_MEM_SHARE = 0.3
LOAD_LOAD_SHARE = 0.2
LOAD_SOFT_CAP = 0.60                    # <=60% saturated: still full score
LOAD_HARD_CAP = 1.00

SHORT_BOARD_PENALTY = 0.15              # max 15 points from the worst node
NO_DATA_COVERAGE = 0.5                  # <50% nodes reporting: dimension dropped

EVENT_LOG_PARTS = ("metrics", "task_outcomes.jsonl")
SNAPSHOT_PARTS = ("metrics", "health_snapshot.json")
EVENT_LOG_MAX = 2000                    # prune beyond this many outcomes

DIMENSION_LABELS = {
    "availability": ("节点可用 Availability", "存活节点占比"),
    "heartbeat": ("心跳稳定 Heartbeat", "心跳新鲜度与节奏稳定性"),
    "load": ("节点负载 Load", "CPU / 内存 / 负载均值饱和度"),
    "reliability": ("任务可靠 Reliability", "滑动窗口任务失败率"),
}


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _read_jsonl_tail(path: str, max_bytes: int = 16_384) -> list[dict]:
    """Read only the trailing ``max_bytes`` of a JSONL file.

    Heartbeat metric files grow forever, but cadence only needs the most recent
    ~60 samples, so a tail read keeps a health poll O(1) in cluster uptime.
    """
    import os
    from backend.common import jsonutil
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
                f.readline()  # discard the likely-partial first line
            data = f.read()
    except OSError:
        return []
    out: list[dict] = []
    for line in data.decode("utf-8", errors="replace").splitlines():
        rec = jsonutil.parse_line(line)
        if isinstance(rec, dict):
            out.append(rec)
    return out


def _lerp(x: float, x0: float, y0: float, x1: float, y1: float) -> float:
    if x1 == x0:
        return y0
    return y0 + (y1 - y0) * (x - x0) / (x1 - x0)


class HealthScorer:
    def __init__(self, storage: Storage, config) -> None:
        self.storage = storage
        self.config = config
        self._lock = threading.RLock()
        self._smoothed: dict[str, float] = {}   # dim key -> score, "__total__" -> total
        self._last_compute_ms = 0
        self._last_prune_ms = 0
        self._last_save_ms = 0
        self._load_snapshot()

    # ------------------------------------------------------------------
    # Event recording (called from the scheduler on task completion)
    # ------------------------------------------------------------------
    def record_task_outcome(self, worker_id: str, success: bool,
                            ts_ms: Optional[int] = None) -> None:
        if not worker_id:
            return
        self.storage.append(
            {"ts_ms": ts_ms or now_ms(), "worker_id": worker_id,
             "success": bool(success)},
            *EVENT_LOG_PARTS,
        )

    def _outcomes(self) -> list[dict]:
        return read_jsonl(self.storage.path(*EVENT_LOG_PARTS))

    def _maybe_prune(self, outcomes: list[dict], now: int) -> None:
        if len(outcomes) <= EVENT_LOG_MAX or now - self._last_prune_ms < 60_000:
            return
        self._last_prune_ms = now
        kept = outcomes[-EVENT_LOG_MAX:]
        from backend.common import jsonutil
        path = self.storage.path(*EVENT_LOG_PARTS)
        # Rewrite under the storage's per-path lock so concurrent appends stay safe.
        with self.storage.store._lock_for(path):  # noqa: SLF001
            with open(path, "w", encoding="utf-8") as f:
                for rec in kept:
                    f.write(jsonutil.dumps_line(rec) + "\n")

    # ------------------------------------------------------------------
    # Snapshot persistence (keeps the EWMA stable across Master restarts)
    # ------------------------------------------------------------------
    def _load_snapshot(self) -> None:
        doc = self.storage.read(*SNAPSHOT_PARTS, default=None)
        if isinstance(doc, dict):
            self._smoothed = {k: float(v) for k, v in (doc.get("smoothed") or {}).items()}
            self._last_compute_ms = int(doc.get("ts_ms", 0))

    def _save_snapshot(self, now: int, force: bool = False) -> None:
        if not force and now - self._last_save_ms < 5_000:
            return
        self._last_save_ms = now
        self.storage.write(
            {"ts_ms": now, "smoothed": self._smoothed}, *SNAPSHOT_PARTS,
        )

    # ------------------------------------------------------------------
    # Per-node raw sub-scores
    # ------------------------------------------------------------------
    def _heartbeat_node(self, worker, now: int) -> tuple[float, dict, bool]:
        """Return (score, raw, has_data) for one alive worker."""
        timeout_ms = max(1.0, self.config.heartbeat_timeout_sec * 1000.0)
        age_ms = max(0, now - (worker.last_heartbeat_ms or now))
        ratio = age_ms / timeout_ms
        if ratio <= HB_FRESH_RATIO:
            age_score = 100.0
        else:
            age_score = _clamp(_lerp(ratio, HB_FRESH_RATIO, 100.0, 1.0, 0.0))

        raw = {"heartbeat_age_s": round(age_ms / 1000.0, 1),
               "heartbeat_timeout_s": round(timeout_ms / 1000.0, 1),
               "age_ratio": round(ratio, 3),
               "age_score": round(age_score, 1)}

        # Cadence stability: coefficient of variation of recent sample intervals.
        intervals = self._heartbeat_intervals(worker.worker_id, now)
        if len(intervals) >= 3:
            mean = sum(intervals) / len(intervals)
            if mean > 0:
                var = sum((x - mean) ** 2 for x in intervals) / len(intervals)
                cv = math.sqrt(var) / mean
                stability_score = _clamp(
                    100.0 * (1.0 - max(0.0, cv - HB_STABLE_JITTER) /
                             (1.0 - HB_STABLE_JITTER))
                )
                score = (1.0 - HB_STABILITY_SHARE) * age_score + HB_STABILITY_SHARE * stability_score
                raw.update({"interval_cv": round(cv, 3),
                            "stability_score": round(stability_score, 1)})
                return round(score, 2), raw, True

        raw["interval_cv"] = None
        return round(age_score, 2), raw, False  # age alone is still usable

    def _heartbeat_intervals(self, worker_id: str, now: int) -> list[float]:
        samples = _read_jsonl_tail(
            self.storage.path("metrics", "workers", f"{worker_id}.jsonl"),
            max_bytes=16_384)
        window_start = now - 5 * 60_000
        stamps = [s.get("ts_ms", 0) for s in samples[-61:]
                  if s.get("ts_ms", 0) >= window_start]
        return [b - a for a, b in zip(stamps, stamps[1:]) if b > a]

    def _load_node(self, worker) -> tuple[float, dict, bool]:
        # load1 is reported as the OS 1-minute load average; normalise by cores.
        cores = max(1, int(worker.cpu_cores or 1))
        cpu = _clamp(float(worker.cpu_percent or 0.0))
        mem = _clamp(float(worker.mem_percent or 0.0))
        load = _clamp(float(worker.load1 or 0.0) / cores * 100.0)
        saturation = (LOAD_CPU_SHARE * cpu + LOAD_MEM_SHARE * mem
                      + LOAD_LOAD_SHARE * load) / 100.0
        if saturation <= LOAD_SOFT_CAP:
            score = 100.0
        else:
            score = _clamp(_lerp(saturation, LOAD_SOFT_CAP, 100.0,
                                 LOAD_HARD_CAP, 0.0))
        raw = {"cpu_percent": round(cpu, 1), "mem_percent": round(mem, 1),
               "load1": round(float(worker.load1 or 0.0), 2),
               "cpu_cores": cores, "saturation": round(saturation, 3)}
        # A worker registered but never heard from carries no real load data.
        has_data = bool(worker.last_heartbeat_ms) and (
            cpu > 0 or mem > 0 or float(worker.load1 or 0.0) > 0)
        return round(score, 2), raw, has_data

    # ------------------------------------------------------------------
    # Reliability dimension (sliding-window failure rate)
    # ------------------------------------------------------------------
    def _reliability(self, workers: list, outcomes: list[dict],
                     now: int) -> tuple[float, dict, str, dict[str, dict]]:
        window_start = now - RELIABILITY_WINDOW_MS
        window = [o for o in outcomes if o.get("ts_ms", 0) >= window_start]
        per_worker: dict[str, dict] = {}

        def tally(events: list[dict]) -> tuple[int, int]:
            ok = sum(1 for o in events if o.get("success"))
            return ok, len(events) - ok

        for w in workers:
            ok_w, fail_w = tally([o for o in window
                                  if o.get("worker_id") == w.worker_id])
            per_worker[w.worker_id] = {
                "window_success": ok_w, "window_failed": fail_w}

        ok, failed = tally(window)
        total = ok + failed
        source = "window"
        if total < RELIABILITY_MIN_SAMPLES:
            # Window too sparse: use cumulative worker counters which cover the
            # whole process lifetime and match the nodes page exactly.
            ok = sum(int(getattr(w, "total_tasks_completed", 0) or 0) for w in workers)
            failed = sum(int(getattr(w, "total_tasks_failed", 0) or 0) for w in workers)
            total = ok + failed
            source = "cumulative"
            for w in workers:
                per_worker.setdefault(w.worker_id, {}).update({
                    "cumulative_success": int(w.total_tasks_completed or 0),
                    "cumulative_failed": int(w.total_tasks_failed or 0),
                })

        if total == 0:
            return 100.0, {"window_success": 0, "window_failed": 0,
                           "failure_rate": 0.0, "samples": 0}, "none", per_worker

        rate = failed / total
        score = _clamp(100.0 * (1.0 - rate / FAIL_RATE_FULL_PENALTY))
        raw = {"window_success": ok if source == "window" else
               sum(1 for o in window if o.get("success")),
               "window_failed": len(window) - sum(1 for o in window if o.get("success")),
               "failure_rate": round(rate, 4), "samples": total}
        return round(score, 2), raw, source, per_worker

    # ------------------------------------------------------------------
    # Full evaluation
    # ------------------------------------------------------------------
    def evaluate(self, workers: list, now: Optional[int] = None) -> dict:
        with self._lock:
            now = now or now_ms()
            dt_ms = max(0.0, float(now - self._last_compute_ms)) if self._last_compute_ms else 0.0
            outcomes = self._outcomes()
            self._maybe_prune(outcomes, now)

            total_nodes = len(workers)
            alive = [w for w in workers if w.is_alive]
            dead = [w for w in workers if not w.is_alive]

            dims: dict[str, dict] = {}
            node_rows: dict[str, dict] = {}
            for w in workers:
                node_rows[w.worker_id] = {
                    "worker_id": w.worker_id, "name": w.name,
                    "status": w.status, "cpu_cores": w.cpu_cores,
                    "dims": {}, "weights": {},
                }

            # -- availability ------------------------------------------------
            if total_nodes == 0:
                dims["availability"] = {"score": None, "status": "no_data",
                                        "raw": {"alive": 0, "total": 0},
                                        "coverage": 0.0}
            else:
                av = 100.0 * len(alive) / total_nodes
                dims["availability"] = {
                    "score": round(av, 2), "status": "ok",
                    "raw": {"alive": len(alive), "dead": len(dead),
                            "total": total_nodes},
                    "coverage": 1.0,
                }
                for w in workers:
                    node_rows[w.worker_id]["dims"]["availability"] = 100.0 if w.is_alive else 0.0
                    node_rows[w.worker_id]["weights"]["availability"] = WEIGHTS["availability"]

            # -- heartbeat + load (alive nodes only) ------------------------
            # Freshness (heartbeat age) is available for every alive node from
            # the registry; interval cadence is a bonus when samples exist.
            hb_scores, hb_weights = [], []
            load_scores, load_weights, load_reporting = [], [], 0
            for w in alive:
                cap = max(1.0, float(w.cpu_cores or 1))
                hb_score, hb_raw, _hb_has = self._heartbeat_node(w, now)
                hb_scores.append(hb_score)
                hb_weights.append(cap)
                node_rows[w.worker_id]["dims"]["heartbeat"] = hb_score
                node_rows[w.worker_id]["raw_heartbeat"] = hb_raw
                node_rows[w.worker_id]["weights"]["heartbeat"] = WEIGHTS["heartbeat"]

                ld_score, ld_raw, ld_has = self._load_node(w)
                node_rows[w.worker_id]["raw_load"] = ld_raw
                if ld_has:
                    load_scores.append(ld_score)
                    load_weights.append(cap)
                    load_reporting += 1
                    node_rows[w.worker_id]["dims"]["load"] = ld_score
                    node_rows[w.worker_id]["weights"]["load"] = WEIGHTS["load"]
                else:
                    node_rows[w.worker_id]["dims"]["load"] = None
                    node_rows[w.worker_id]["weights"]["load"] = 0.0
                    node_rows[w.worker_id]["load_missing"] = True

            dims["heartbeat"] = self._aggregate_dim(
                hb_scores, hb_weights, len(alive), len(alive),
                {"reporting": len(alive)})
            dims["load"] = self._aggregate_dim(
                load_scores, load_weights, len(alive), load_reporting,
                {"reporting": load_reporting})

            # -- reliability -------------------------------------------------
            rel_score, rel_raw, rel_source, rel_per_worker = self._reliability(
                workers, outcomes, now)
            for w in workers:
                info = rel_per_worker.get(w.worker_id, {})
                node_rows[w.worker_id]["raw_reliability"] = info
                # Per-node reliability only where the node has outcomes.
                samples = info.get("window_success", 0) + info.get("window_failed", 0)
                if samples:
                    rate = info["window_failed"] / samples
                    score = round(_clamp(100.0 * (1.0 - rate / FAIL_RATE_FULL_PENALTY)), 2)
                    node_rows[w.worker_id]["dims"]["reliability"] = score
                    node_rows[w.worker_id]["weights"]["reliability"] = WEIGHTS["reliability"]
                else:
                    node_rows[w.worker_id]["dims"]["reliability"] = None
                    node_rows[w.worker_id]["weights"]["reliability"] = 0.0
            dims["reliability"] = {
                "score": rel_score if rel_source != "none" else None,
                "status": "ok" if rel_source != "none" else "no_data",
                "raw": {**rel_raw, "source": rel_source,
                        "window_sec": RELIABILITY_WINDOW_MS // 1000},
                "coverage": 1.0 if rel_source != "none" else 0.0,
            }

            # -- weighted total + short-board penalty -----------------------
            raw_total, used_weights = self._weighted_total(dims)
            worst_node = self._worst_node(node_rows)
            penalty = 0.0
            if raw_total is not None and worst_node is not None:
                penalty = SHORT_BOARD_PENALTY * max(0.0, raw_total - worst_node)
                raw_total = _clamp(raw_total - penalty)

            smooth_total = self._smooth("__total__", raw_total, dt_ms)
            if raw_total is None:
                # Empty cluster / no usable dimension: surface "no data", not a
                # stale historical number.
                smooth_total = None
                self._smoothed.pop("__total__", None)
            for key, dim in dims.items():
                label, desc = DIMENSION_LABELS[key]
                dim["label"] = label
                dim["description"] = desc
                dim["weight"] = used_weights.get(key, 0.0)
                dim["raw_score"] = dim["score"]
                dim["score"] = self._smooth(f"dim:{key}", dim["score"], dt_ms)
            self._last_compute_ms = now

            # -- node composite scores --------------------------------------
            nodes_out = []
            for w in workers:
                row = node_rows[w.worker_id]
                row["score"] = self._node_score(row)
                nodes_out.append(row)
            nodes_out.sort(key=lambda r: (r["score"] if r["score"] is not None else -1))

            self._save_snapshot(now)

            return {
                "score": round(smooth_total, 1) if smooth_total is not None else None,
                "raw_score": round(raw_total, 1) if raw_total is not None else None,
                "grade": self._grade(smooth_total, total_nodes),
                "ts_ms": now,
                "weights": WEIGHTS,
                "used_weights": used_weights,
                "short_board_penalty": round(penalty, 2),
                "worst_node": worst_node,
                "smoothing": {"tau_down_ms": TAU_DOWN_MS, "tau_up_ms": TAU_UP_MS},
                "coverage": {
                    "nodes_total": total_nodes,
                    "nodes_alive": len(alive),
                    "nodes_dead": len(dead),
                    "load_reporting": load_reporting,
                    "heartbeat_reporting": len(alive),
                    "reliability_source": rel_source,
                },
                "dimensions": dims,
                "nodes": nodes_out,
            }

    def _aggregate_dim(self, scores: list[float], weights: list[float],
                       population: int, reporting: int, extra: dict) -> dict:
        coverage = (reporting / population) if population else 0.0
        if not scores or coverage < NO_DATA_COVERAGE:
            return {"score": None, "status": "no_data",
                    "raw": {**extra, "coverage": round(coverage, 2)},
                    "coverage": coverage}
        ws = sum(weights)
        avg = sum(s * w for s, w in zip(scores, weights)) / ws if ws else None
        return {"score": round(avg, 2), "status": "ok",
                "raw": {**extra, "min": round(min(scores), 1),
                        "coverage": round(coverage, 2)},
                "coverage": coverage}

    def _weighted_total(self, dims: dict[str, dict]) -> tuple[Optional[float], dict]:
        used: dict[str, float] = {}
        num = 0.0
        for key, weight in WEIGHTS.items():
            score = dims.get(key, {}).get("score")
            if score is None:
                continue
            used[key] = weight
            num += weight * score
        wsum = sum(used.values())
        if wsum == 0:
            return None, {}
        total = round(num / wsum, 2)
        normalized = {k: round(v / wsum, 4) for k, v in used.items()}
        return total, normalized

    def _worst_node(self, node_rows: dict[str, dict]) -> Optional[float]:
        worst: Optional[float] = None
        for row in node_rows.values():
            score = self._node_score(row)
            if score is not None and (worst is None or score < worst):
                worst = score
        return round(worst, 1) if worst is not None else None

    @staticmethod
    def _node_score(row: dict) -> Optional[float]:
        num = 0.0
        wsum = 0.0
        for key, weight in row.get("weights", {}).items():
            score = row["dims"].get(key)
            if score is None or not weight:
                continue
            num += weight * score
            wsum += weight
        return round(num / wsum, 1) if wsum else None

    # ------------------------------------------------------------------
    # Asymmetric time-based EWMA
    # ------------------------------------------------------------------
    def _smooth(self, key: str, raw: Optional[float], dt_ms: float) -> Optional[float]:
        if raw is None:
            # Keep history on a transient data gap so the score does not jump.
            return self._smoothed.get(key)
        prev = self._smoothed.get(key)
        if prev is None or dt_ms <= 0:
            self._smoothed[key] = raw
            return raw
        tau = TAU_DOWN_MS if raw < prev else TAU_UP_MS
        alpha = 1.0 - math.exp(-dt_ms / tau)
        value = prev + alpha * (raw - prev)
        self._smoothed[key] = value
        return value

    @staticmethod
    def _grade(score: Optional[float], total_nodes: int) -> str:
        if score is None:
            return "unknown" if total_nodes == 0 else "no_data"
        if score >= 85:
            return "good"
        if score >= 60:
            return "warn"
        return "critical"
