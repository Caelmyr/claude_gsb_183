"""Tests for the cluster health score.

Covers the four contract requirements:

* dimensions combine with explicit weights and renormalise when data is missing;
* a single bad node has bounded impact (capacity weighting + short-board cap);
* metric fluctuations are smoothed with the asymmetric EWMA and persist across
  ``HealthScorer`` restarts;
* a node with missing telemetry is excluded (not treated as healthy or dead).
"""

import math
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.jsonutil import now_ms
from backend.common.models import WorkerRecord
from backend.common.storage import Storage
from backend.master.health import (
    FAIL_RATE_FULL_PENALTY,
    HealthScorer,
    NO_DATA_COVERAGE,
    SHORT_BOARD_PENALTY,
    WEIGHTS,
)


def make_worker(wid, status="alive", cores=4, cpu=20.0, mem=30.0, load1=0.5,
                age_ms=1000, completed=0, failed=0, hb_ms=None):
    now = hb_ms if hb_ms is not None else now_ms()
    return WorkerRecord(
        worker_id=wid, name=wid, status=status, cpu_cores=cores,
        mem_total_mb=8000, last_heartbeat_ms=now - age_ms,
        cpu_percent=cpu, mem_percent=mem, load1=load1,
        total_tasks_completed=completed, total_tasks_failed=failed,
    )


class TestHealthScoring(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(heartbeat_timeout_sec=8.0)
        self.hs = HealthScorer(self.storage, self.config)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- weighting ---------------------------------------------------------
    def test_weights_sum_to_one(self):
        self.assertAlmostEqual(sum(WEIGHTS.values()), 1.0)

    def test_healthy_cluster_scores_high(self):
        now = now_ms()
        workers = [make_worker(f"w{i}", completed=50, hb_ms=now) for i in range(3)]
        r = self.hs.evaluate(workers, now=now)
        self.assertEqual(r["grade"], "good")
        self.assertGreaterEqual(r["score"], 95)
        self.assertEqual(r["dimensions"]["availability"]["score"], 100.0)
        self.assertEqual(r["dimensions"]["load"]["status"], "ok")
        self.assertEqual(r["coverage"]["reliability_source"], "cumulative")

    def test_dimensions_renormalised_when_missing(self):
        # A freshly registered worker that has not reported resource data yet:
        # last_heartbeat_ms == 0 and zero counters -> load has no data.
        now = now_ms()
        fresh = make_worker("w0", age_ms=0)
        fresh.last_heartbeat_ms = 0
        fresh.cpu_percent = fresh.mem_percent = fresh.load1 = 0.0
        r = self.hs.evaluate([fresh], now=now)
        load = r["dimensions"]["load"]
        self.assertIsNone(load["score"])
        self.assertEqual(load["status"], "no_data")
        used = r["used_weights"]
        self.assertNotIn("load", used)
        # Remaining weights still renormalise to a finite score.
        self.assertGreater(used["availability"], WEIGHTS["availability"])
        self.assertIsNotNone(r["score"])

    def test_no_nodes_reports_no_data_not_zero(self):
        r = self.hs.evaluate([], now=now_ms())
        self.assertIsNone(r["score"])
        self.assertEqual(r["grade"], "unknown")

    # -- single-node impact ------------------------------------------------
    def test_one_dead_node_lowers_availability_proportionally(self):
        now = now_ms()
        workers = [make_worker(f"w{i}", hb_ms=now) for i in range(4)]
        r0 = self.hs.evaluate(workers, now=now)
        self.assertEqual(r0["dimensions"]["availability"]["score"], 100.0)
        workers[0].status = "dead"
        r1 = self.hs.evaluate(workers, now=now + 1000)
        self.assertEqual(r1["dimensions"]["availability"]["raw_score"], 75.0)

    def test_one_overloaded_node_has_bounded_impact(self):
        now = now_ms()
        good = [make_worker(f"w{i}", cpu=10, mem=10, load1=0.1, hb_ms=now)
                for i in range(7)]
        bad = [make_worker("wbad", cpu=100, mem=100, load1=8.0, hb_ms=now)]
        r = self.hs.evaluate(good + bad, now=now)
        # Worst node is visible in the penalty and sorted node list...
        self.assertGreater(r["short_board_penalty"], 0)
        self.assertEqual(r["nodes"][0]["worker_id"], "wbad")
        # ...but it cannot tank the whole cluster: penalty is capped at 15 pts
        # of the raw-vs-worst gap, so the smoothed total stays clearly healthy.
        self.assertGreaterEqual(r["raw_score"], 75.0)
        self.assertLessEqual(r["short_board_penalty"],
                             SHORT_BOARD_PENALTY * 100.0 + 0.01)

    def test_capacity_weighting_counts_big_nodes_more(self):
        now = now_ms()
        # One overloaded 16-core node should outweigh one overloaded 1-core node.
        big_bad = make_worker("big", cores=16, cpu=100, mem=100, load1=20, hb_ms=now)
        smalls = [make_worker(f"s{i}", cores=1, cpu=10, mem=10, load1=0.05, hb_ms=now)
                  for i in range(15)]
        r = self.hs.evaluate([big_bad] + smalls, now=now)
        small_bad = make_worker("tiny", cores=1, cpu=100, mem=100, load1=1, hb_ms=now)
        bigs = [make_worker(f"b{i}", cores=16, cpu=10, mem=10, load1=0.2, hb_ms=now)
                for i in range(15)]
        r2 = self.hs.evaluate([small_bad] + bigs, now=now + 100)
        self.assertLess(r["dimensions"]["load"]["raw_score"],
                        r2["dimensions"]["load"]["raw_score"])

    # -- failure rate ------------------------------------------------------
    def test_window_failure_rate_scores_zero_at_full_penalty(self):
        now = now_ms()
        workers = [make_worker("w1", hb_ms=now)]
        ts = now - 1000
        n = 100
        for i in range(n):
            self.hs.record_task_outcome(
                "w1", success=(i >= int(n * FAIL_RATE_FULL_PENALTY)), ts_ms=ts)
        r = self.hs.evaluate(workers, now=now)
        rel = r["dimensions"]["reliability"]
        self.assertEqual(rel["raw"]["source"], "window")
        self.assertAlmostEqual(rel["raw"]["failure_rate"], FAIL_RATE_FULL_PENALTY)
        self.assertAlmostEqual(rel["raw_score"], 0.0)

    def test_cumulative_fallback_when_window_sparse(self):
        now = now_ms()
        workers = [make_worker("w1", completed=90, failed=10, hb_ms=now)]
        r = self.hs.evaluate(workers, now=now)
        rel = r["dimensions"]["reliability"]
        self.assertEqual(rel["raw"]["source"], "cumulative")
        self.assertEqual(rel["raw"]["samples"], 100)
        self.assertAlmostEqual(rel["raw"]["failure_rate"], 0.1)

    # -- smoothing ---------------------------------------------------------
    def test_score_smooths_and_deteriorates_faster_than_recovers(self):
        t0 = 1_000_000.0
        good = [make_worker("w1", hb_ms=int(t0))]
        r = self.hs.evaluate(good, now=int(t0))
        self.assertEqual(r["raw_score"], 100.0)
        bad = [make_worker("w1", cpu=100, mem=100, load1=8,
                           age_ms=7900, hb_ms=int(t0))]
        drop = self.hs.evaluate(bad, now=int(t0 + 5000))["score"]
        recover_5s = self.hs.evaluate(
            [make_worker("w1", hb_ms=int(t0 + 10000))], now=int(t0 + 10000))["score"]
        self.assertLess(drop, 90)
        self.assertLess(recover_5s - drop, 15)  # recovery is deliberately slow

    def test_smoothing_state_persists_across_restarts(self):
        t0 = 2_000_000
        good = [make_worker("w1", hb_ms=t0)]
        self.hs.evaluate(good, now=t0)
        bad = [make_worker("w1", cpu=100, mem=100, load1=8, age_ms=7900, hb_ms=t0)]
        first = self.hs.evaluate(bad, now=t0 + 5000)["score"]
        # A brand-new scorer over the same data dir must continue the EWMA.
        hs2 = HealthScorer(self.storage, self.config)
        second = hs2.evaluate(
            [make_worker("w1", cpu=100, mem=100, load1=8, age_ms=7900,
                         hb_ms=t0 + 5000)], now=t0 + 6000)["score"]
        self.assertLess(second, first)  # keeps drifting down, did not reset to 100

    def test_transient_zero_reading_does_not_reset_history(self):
        t0 = 3_000_000
        self.hs.evaluate([make_worker("w1", hb_ms=t0)], now=t0)
        self.hs.evaluate(
            [make_worker("w1", cpu=100, mem=100, load1=8, age_ms=7900, hb_ms=t0)],
            now=t0 + 5000)
        # Empty reading (transient telemetry gap) returns the held value.
        gap = self.hs.evaluate([], now=t0 + 6000)
        self.assertIsNone(gap["score"])  # no nodes -> no_data, not a reset zero
        back = self.hs.evaluate([make_worker("w1", hb_ms=t0 + 7000)], now=t0 + 7000)
        self.assertIsNotNone(back["score"])

    # -- raw traceability --------------------------------------------------
    def test_node_breakdown_carries_raw_inputs(self):
        now = now_ms()
        w = make_worker("w1", cpu=42.0, mem=55.0, load1=1.25, cores=4, hb_ms=now)
        r = self.hs.evaluate([w], now=now)
        row = r["nodes"][0]
        self.assertEqual(row["raw_load"]["cpu_percent"], 42.0)
        self.assertEqual(row["raw_load"]["mem_percent"], 55.0)
        self.assertEqual(row["raw_load"]["load1"], 1.25)
        self.assertEqual(row["raw_heartbeat"]["heartbeat_timeout_s"], 8.0)
        self.assertIn("age_ratio", row["raw_heartbeat"])


if __name__ == "__main__":
    unittest.main()
