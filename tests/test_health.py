"""Tests for the cluster health evaluator: weighting, smoothing, gaps."""

import os
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.models import new_worker
from backend.common.storage import Storage
from backend.master.health import (
    DIMENSION_WEIGHTS,
    GRADE_BAD,
    GRADE_GOOD,
    GRADE_UNKNOWN,
    GRADE_WARN,
    HealthEvaluator,
    grade_for,
)
from backend.master.registry import WorkerRegistry

NOW = 1_000_000_000_000
INTERVAL_MS = 2000


class HealthTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.registry = WorkerRegistry(self.storage, self.config)
        self.health = HealthEvaluator(self.storage, self.registry, self.config)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers -------------------------------------------------------
    def add_worker(self, wid, name="", status="alive", cores=4,
                   hb_age_ms=1500, registered_age_ms=600_000,
                   cpu=20.0, mem=30.0, load1=0.5, samples=40):
        w = new_worker(wid, name or wid, "127.0.0.1", 9000 + len(self.registry.all()),
                       cpu_cores=cores, mem_total_mb=8000)
        w.status = status
        w.registered_ms = NOW - registered_age_ms
        w.last_heartbeat_ms = NOW - hb_age_ms
        w.cpu_percent = cpu
        w.mem_percent = mem
        w.load1 = load1
        self.registry._workers[wid] = w
        if samples and status == "alive":
            for i in range(samples):
                ts = NOW - (samples - i) * INTERVAL_MS
                self.storage.append(
                    {"ts_ms": ts, "worker_id": wid, "cpu_percent": cpu,
                     "mem_percent": mem, "load1": load1},
                    "metrics", "workers", f"{wid}.jsonl",
                )
        return w

    def add_worker_samples(self, wid, cpu, mem, load1, count=45):
        path = self.storage.path("metrics", "workers", f"{wid}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for i in range(count):
                import json
                f.write(json.dumps({
                    "ts_ms": NOW - (count - i) * INTERVAL_MS,
                    "worker_id": wid, "cpu_percent": cpu,
                    "mem_percent": mem, "load1": load1,
                }) + "\n")

    def add_task_events(self, job_id="j1", ok=20, failed=0, wid="worker-a"):
        for i in range(ok):
            self.storage.append(
                {"ts_ms": NOW - i * 100, "job_id": job_id, "worker_id": wid,
                 "records_per_sec": 10.0},
                "metrics", "jobs", f"{job_id}.jsonl",
            )
        for i in range(failed):
            self.storage.write(
                {"fault_id": f"{job_id}-f{i}", "job_id": job_id, "kind": "task_failed",
                 "worker_id": wid, "created_ms": NOW - i * 200 - 50},
                "jobs", job_id, "faults", f"f{i}.json",
            )


class TestGrading(unittest.TestCase):
    def test_grade_bands(self):
        self.assertEqual(grade_for(None), GRADE_UNKNOWN)
        self.assertEqual(grade_for(100), GRADE_GOOD)
        self.assertEqual(grade_for(80.0), GRADE_GOOD)
        self.assertEqual(grade_for(70), GRADE_WARN)
        self.assertEqual(grade_for(10), GRADE_BAD)


class TestEmptyCluster(HealthTestBase):
    def test_no_workers_is_unknown_not_zero(self):
        report = self.health.evaluate(now_ms_override=NOW)
        self.assertIsNone(report["score"])
        self.assertEqual(report["grade"], GRADE_UNKNOWN)
        self.assertEqual(report["nodes"], [])


class TestHealthyCluster(HealthTestBase):
    def test_healthy_cluster_scores_near_hundred(self):
        self.add_worker("worker-a", "a", cpu=25, mem=40, load1=0.6)
        self.add_worker("worker-b", "b", cpu=30, mem=35, load1=0.4)
        self.add_worker("worker-c", "c", cpu=20, mem=45, load1=0.3)
        self.add_task_events(ok=30)
        report = self.health.evaluate(now_ms_override=NOW)
        self.assertGreaterEqual(report["score"], 95.0)
        self.assertEqual(report["grade"], GRADE_GOOD)
        for key, dim in report["dimensions"].items():
            self.assertGreaterEqual(dim["raw_score"], 90.0, key)
        self.assertEqual(report["coverage"], 1.0)
        self.assertEqual(report["missing_workers"], [])

    def test_node_breakdown_tracks_raw_inputs(self):
        self.add_worker("worker-a", "a", cpu=42.0, mem=55.0, load1=1.0)
        report = self.health.evaluate(now_ms_override=NOW)
        node = next(n for n in report["nodes"] if n["worker_id"] == "worker-a")
        self.assertAlmostEqual(node["cpu_avg"], 42.0, places=1)
        self.assertAlmostEqual(node["mem_avg"], 55.0, places=1)
        self.assertAlmostEqual(node["load1_avg"], 1.0, places=1)
        self.assertAlmostEqual(node["load_ratio"], 0.25, places=2)
        self.assertEqual(node["tasks_ok_window"], 0)
        self.assertEqual(node["missing"], [])


class TestSingleNodeImpact(HealthTestBase):
    def test_one_dead_node_costs_proportional_share(self):
        n = 5
        for i in range(n):
            self.add_worker(f"worker-{i}", str(i))
        self.registry._workers["worker-4"].status = "dead"
        report = self.health.evaluate(now_ms_override=NOW)
        avail = report["dimensions"]["availability"]
        # Mean 80, worst 0 -> 0.8*80 + 0.2*0 = 64.
        self.assertAlmostEqual(avail["raw_score"], 64.0, places=1)
        self.assertEqual(avail["detail"]["dead"], 1)
        self.assertEqual(avail["detail"]["alive"], 4)
        # Only the availability dimension (weight 0.30) is affected; total drop
        # is bounded to roughly 0.30 * 36 = 10.8 points.
        self.assertGreater(report["raw_score"], 85.0)
        dead_node = next(x for x in report["nodes"] if x["worker_id"] == "worker-4")
        self.assertEqual(dead_node["availability"], 0.0)
        self.assertEqual(dead_node["score"], 0.0)
        self.assertIn("heartbeat", dead_node["missing"])
        self.assertIn("load", dead_node["missing"])

    def test_larger_cluster_dilutes_single_death(self):
        for i in range(10):
            self.add_worker(f"worker-{i}", str(i))
        self.registry._workers["worker-9"].status = "dead"
        report = self.health.evaluate(now_ms_override=NOW)
        # Availability 0.8*90 + 0.2*0 = 72 (weight 0.30); the other present
        # dimensions (heartbeat/load, 0.45 of the total) stay perfect and the
        # idle task dim is excluded, so total ~88.6 — a clear but bounded hit.
        self.assertAlmostEqual(report["dimensions"]["availability"]["raw_score"], 72.0, places=1)
        self.assertGreater(report["raw_score"], 88.0)


class TestResourceLoad(HealthTestBase):
    def test_sustained_overload_lowers_load_dimension(self):
        self.add_worker("worker-a", "a", cpu=97.0, mem=96.0, load1=7.5)
        report = self.health.evaluate(now_ms_override=NOW)
        load = report["dimensions"]["load"]
        self.assertLess(load["raw_score"], 15.0)
        self.assertGreater(load["detail"]["avg_load_ratio"], 1.5)

    def test_single_spike_is_smoothed_by_window(self):
        self.add_worker("worker-a", "a", cpu=20.0, mem=30.0, load1=0.4, samples=44)
        # One recent spike inside the 90s window.
        self.storage.append(
            {"ts_ms": NOW - 1000, "worker_id": "worker-a", "cpu_percent": 99.0,
             "mem_percent": 99.0, "load1": 8.0},
            "metrics", "workers", "worker-a.jsonl",
        )
        report = self.health.evaluate(now_ms_override=NOW)
        # Window average keeps the load dimension comfortably above critical.
        self.assertGreater(report["dimensions"]["load"]["raw_score"], 70.0)


class TestTaskQuality(HealthTestBase):
    def test_failure_rate_lowers_task_dimension(self):
        self.add_worker("worker-a", "a")
        self.add_task_events(ok=80, failed=20)  # 20% failure
        report = self.health.evaluate(now_ms_override=NOW)
        tasks = report["dimensions"]["tasks"]
        self.assertEqual(tasks["detail"]["succeeded"], 80)
        self.assertEqual(tasks["detail"]["failed"], 20)
        self.assertAlmostEqual(tasks["detail"]["failure_rate"], 0.2, places=2)
        self.assertAlmostEqual(tasks["raw_score"], 80.0, places=1)
        node = report["nodes"][0]
        self.assertEqual(node["tasks_ok_window"], 80)
        self.assertEqual(node["tasks_failed_window"], 20)

    def test_one_failure_on_idle_cluster_is_shrunk(self):
        self.add_worker("worker-a", "a")
        self.add_task_events(ok=1, failed=1)
        report = self.health.evaluate(now_ms_override=NOW)
        tasks = report["dimensions"]["tasks"]
        # 50% raw rate, confidence 2/10 -> effective 10% -> score ~90.
        self.assertGreater(tasks["raw_score"], 85.0)
        self.assertTrue(tasks["detail"]["low_sample"])

    def test_no_task_events_drops_dimension_not_zero(self):
        self.add_worker("worker-a", "a")
        report = self.health.evaluate(now_ms_override=NOW)
        self.assertFalse(report["dimensions"]["tasks"]["available"])
        # Remaining three dimensions re-normalise; score still near 100.
        self.assertGreaterEqual(report["score"], 99.0)

    def test_failures_outside_window_are_ignored(self):
        self.add_worker("worker-a", "a")
        self.add_task_events(ok=10, failed=0)
        old = NOW - int(self.config.health_task_window_sec * 1000) - 5000
        self.storage.write(
            {"fault_id": "old", "job_id": "j1", "kind": "task_failed",
             "worker_id": "worker-a", "created_ms": old},
            "jobs", "j1", "faults", "old.json",
        )
        report = self.health.evaluate(now_ms_override=NOW)
        self.assertEqual(report["dimensions"]["tasks"]["detail"]["failed"], 0)


class TestMissingData(HealthTestBase):
    def test_worker_without_samples_is_reported_missing(self):
        self.add_worker("worker-a", "a", cpu=20.0)
        self.add_worker("worker-b", "b", samples=0)  # registered long ago, no samples
        report = self.health.evaluate(now_ms_override=NOW)
        missing = {m["worker_id"]: m["missing"] for m in report["missing_workers"]}
        self.assertIn("worker-b", missing)
        self.assertIn("heartbeat", missing["worker-b"])
        self.assertIn("load", missing["worker-b"])
        self.assertLess(report["coverage"], 1.0)
        # Coverage penalty is bounded.
        self.assertGreaterEqual(report["score"], report["raw_score"] - 5.2)

    def test_new_worker_grace_period_not_penalised(self):
        self.add_worker("worker-a", "a")
        self.add_worker("worker-b", "b", samples=0, registered_age_ms=3000)
        report = self.health.evaluate(now_ms_override=NOW)
        node_b = next(n for n in report["nodes"] if n["worker_id"] == "worker-b")
        # New node still inside its grace window: absent samples do not reduce
        # coverage and the node is flagged as in-grace rather than missing.
        self.assertTrue(node_b["in_grace"])
        self.assertEqual(report["dimensions"]["heartbeat"]["coverage"], 1.0)
        self.assertEqual(report["dimensions"]["load"]["coverage"], 1.0)
        self.assertEqual(report["missing_workers"], [])


class TestSmoothing(HealthTestBase):
    def _stable_score(self, healthy=True):
        self.add_worker(
            "worker-a", "a",
            cpu=20.0 if healthy else 98.0,
            mem=30.0 if healthy else 97.0,
            load1=0.4 if healthy else 7.5,
        )
        if healthy:
            self.add_task_events(ok=30, failed=0)
        else:
            self.add_task_events(ok=10, failed=90)

    def test_score_does_not_jump_in_one_tick(self):
        self._stable_score(healthy=True)
        first = self.health.evaluate(now_ms_override=NOW - INTERVAL_MS * 60)
        self.health.evaluate(now_ms_override=NOW - INTERVAL_MS * 30)
        before = self.health.evaluate(now_ms_override=NOW - INTERVAL_MS)
        # Cluster suddenly critical: rewrite the window with saturated samples.
        w = self.registry._workers["worker-a"]
        w.cpu_percent, w.mem_percent, w.load1 = 98.0, 97.0, 7.5
        self.add_worker_samples("worker-a", 98.0, 97.0, 7.5)
        # Recent task failures too: 10 ok / 40 failed.
        for i in range(40):
            self.storage.write(
                {"fault_id": f"nf{i}", "job_id": "j1", "kind": "task_failed",
                 "worker_id": "worker-a", "created_ms": NOW - i * 200},
                "jobs", "j1", "faults", f"nf{i}.json",
            )
        after = self.health.evaluate(now_ms_override=NOW)
        raw_drop = after["raw_score"] - before["raw_score"]
        # Load dimension saturated; weighted overall raw score drops sharply.
        self.assertLess(after["dimensions"]["load"]["raw_score"], 5.0)
        self.assertLess(after["raw_score"], 78.0)
        # But the smoothed score moves only a fraction of the raw drop in one tick.
        self.assertGreater(after["score"], before["score"] + raw_drop * 0.5 + 5)
        self.assertGreater(after["score"], 60.0)

    def test_stable_input_converges_and_then_holds(self):
        self._stable_score(healthy=True)
        scores = []
        for step in range(60):
            r = self.health.evaluate(now_ms_override=NOW - (60 - step) * INTERVAL_MS)
            scores.append(r["score"])
        self.assertGreaterEqual(scores[-1], 99.0)
        # Last few identical-input evaluations differ by less than a point.
        self.assertLessEqual(max(scores[-5:]) - min(scores[-5:]), 1.0)

    def test_downgrade_propagates_faster_than_recovery(self):
        # Prime a healthy steady state.
        self._stable_score(healthy=True)
        for step in range(40):
            self.health.evaluate(now_ms_override=NOW - (80 - step) * INTERVAL_MS)
        healthy_score = self.health._state["smoothed"]["score"]

        # One sharp deterioration: capture smoothed score after 10s.
        self._flip_to_bad()
        down = self.health.evaluate(now_ms_override=NOW + 5 * INTERVAL_MS)["score"]

        # Rebuild a fresh evaluator that recovered at the same instant and
        # compare its 10s movement upward from the low steady state.
        self._prime_bad_steady(NOW)
        self._flip_to_good()
        up_report = self.health.evaluate(now_ms_override=NOW + 5 * INTERVAL_MS)
        down_movement = healthy_score - down
        up_movement = up_report["score"] - up_report["raw_score"]  # raw recovers fully
        self.assertGreater(down_movement, up_movement + 5)

    def _flip_to_bad(self):
        w = self.registry._workers["worker-a"]
        w.cpu_percent, w.mem_percent, w.load1 = 98.0, 97.0, 7.5
        for i in range(45):
            self.storage.append(
                {"ts_ms": NOW + i * 100, "worker_id": "worker-a", "cpu_percent": 98.0,
                 "mem_percent": 97.0, "load1": 7.5},
                "metrics", "workers", "worker-a.jsonl",
            )

    def _flip_to_good(self):
        w = self.registry._workers["worker-a"]
        w.cpu_percent, w.mem_percent, w.load1 = 20.0, 30.0, 0.4

    def _prime_bad_steady(self, start):
        # Replace evaluator state directly with a low smoothed baseline.
        self.health._state = {
            "ts_ms": start,
            "smoothed": {"score": 10.0, "dimensions": {
                "availability": 100.0, "heartbeat": 100.0, "load": 5.0, "tasks": 10.0}},
            "history": [],
        }

    def test_state_survives_restart(self):
        self._stable_score(healthy=True)
        self.health.evaluate(now_ms_override=NOW - INTERVAL_MS * 40)
        mid = self.health.evaluate(now_ms_override=NOW - INTERVAL_MS)["score"]

        revived = HealthEvaluator(self.storage, self.registry, self.config)
        after = revived.evaluate(now_ms_override=NOW)["score"]
        # No jump: continued convergence, not re-initialisation.
        self.assertLess(abs(after - mid), 3.0)
        self.assertTrue(os.path.exists(self.storage.path("health", "state.json")))

    def test_ancient_state_is_reinitialized(self):
        self.add_worker("worker-a", "a", cpu=20.0)
        self.health._state = {
            "ts_ms": NOW - 60 * 60 * 1000,
            "smoothed": {"score": 5.0, "dimensions": {}},
            "history": [],
        }
        report = self.health.evaluate(now_ms_override=NOW)
        # Healthy cluster -> jumps straight back near 100 instead of EWMA from 5.
        self.assertGreater(report["score"], 95.0)


class TestWeights(unittest.TestCase):
    def test_weights_sum_to_one(self):
        self.assertAlmostEqual(sum(DIMENSION_WEIGHTS.values()), 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
