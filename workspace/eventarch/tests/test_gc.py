"""Acceptance tests for capacity-eviction rehearsal and reader protection.

Scenarios (>= 8), per the design:
  1. preview is read-only: file digests + manifest identical before/after
  2. a snapshot reference alone keeps an eligible item out of the selection
  3. an in-flight repair alone keeps an eligible item out of the selection
  4. an unexpired hold alone excludes an item; renew/release affect only
     plans created afterwards (older plans answer 409)
  5. stamp/reference drift injected between preview and apply -> 409, zero
     reclamation, disk untouched
  6. two applies of one plan produce exactly one gc_job (202 then 200)
  7. process exit injected at each of the three publish phases -> startup
     reconciliation leaves no orphan directories; audit/job trackable
  8. after eviction: new writes, reads from gaps (410 + cursor), frozen
     history reads all correct; remaining positions/keys/boundaries unchanged
"""

import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import segments as segmod
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore, Gone, PlanConflict


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8, janitor_interval_sec=10,
        repair_workers=2, repair_retry_backoff_sec=0.01,
        gc_hold_default_ttl_sec=900, gc_hold_max_ttl_sec=86400,
    )
    base.update(kw)
    return Config(**base)


def ev(device, seq, event_id=None, ts=None, payload=None):
    return {
        "device_id": device,
        "event_id": event_id or f"{device}-{seq}",
        "seq": seq,
        "device_ts": fmt_ts(ts or utcnow()),
        "payload": payload if payload is not None else {"seq": seq},
    }


def open_store(tmp, **kw):
    s = ArchiveStore(make_cfg(tmp, **kw))
    s.open()
    return s


def seg_ids(s):
    return [m["id"] for m in s.list_segments()["segments"]]


def digest_tree(root):
    """Stable per-file digest of every regular file under root."""
    out = {}
    for dirpath, _dirs, names in os.walk(root):
        for name in names:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            with open(full, "rb") as fh:
                out[rel] = hashlib.sha256(fh.read()).hexdigest()
    return out


def wait_gc(s, jid, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        j = s.get_gc_job(jid)
        if j["status"] in ("succeeded", "failed"):
            return j
        time.sleep(0.005)
    raise AssertionError(f"gc job {jid} never finished: {s.get_gc_job(jid)}")


def wait_repair(s, jid, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        j = s.get_repair(jid)
        if j["status"] in ("succeeded", "failed"):
            return j
        time.sleep(0.005)
    raise AssertionError(f"repair {jid} never finished: {s.get_repair(jid)}")


class GCTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def seed(self, n=15, **kw):
        """n records with seg size 5 -> ceil(n/5) sealed segments + tail."""
        s = open_store(self.tmp, **kw)
        s.ingest([ev("d1", i) for i in range(n)])
        return s


class Scenario1PreviewReadOnlyTest(GCTestBase):
    def test_preview_changes_nothing_on_disk(self):
        s = self.seed(15)  # seg [0..4],[5..9],[10..14], settled state
        before = digest_tree(self.tmp)
        manifest_before = json.dumps(s.manifest, sort_keys=True)

        # Pure rehearsal: only the fixed fields come back.
        p = s.create_plan(5)
        self.assertEqual(set(p), {"plan_id", "cut", "stamp", "items",
                                  "size", "created_at"})
        self.assertTrue(p["plan_id"].startswith("gcp-"))
        self.assertEqual([i["seg_id"] for i in p["items"]], [seg_ids(s)[0]])
        self.assertEqual(sum(i["size"] for i in p["items"]), p["size"])
        self.assertEqual(len(p["stamp"]), 64)

        # A second, wider rehearsal selects two items and still writes nothing.
        p2 = s.create_plan(10)
        self.assertEqual([i["seg_id"] for i in p2["items"]],
                         [seg_ids(s)[0], seg_ids(s)[1]])

        after = digest_tree(self.tmp)
        self.assertEqual(before, after)
        self.assertEqual(manifest_before, json.dumps(s.manifest, sort_keys=True))
        # Previews persist neither an accepted order nor an audit.  (An empty
        # gc_jobs.json may exist from store initialization; its emptiness is
        # asserted separately below.)
        self.assertFalse(os.path.exists(
            os.path.join(self.tmp, "state", "gc_plans.json")))
        self.assertFalse(os.path.exists(
            os.path.join(self.tmp, "state", "gc_audit.json")))
        self.assertEqual(s.list_gc_jobs(50), [])
        s.close()


class Scenario2SnapshotProtectionTest(GCTestBase):
    def test_snapshot_reference_alone_excludes_item(self):
        # Build one snapshot referencing ONLY the first segment: freeze right
        # after seg-0 seals, then add two more sealed segments.
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(5)])   # seg-0
        frz = s.freeze()                            # pins seg-0
        s.ingest([ev("d1", i) for i in range(5, 15)])  # seg-5, seg-10
        ids = seg_ids(s)
        self.assertEqual(frz["segments"], [ids[0]])

        # cut beyond everything: without the snapshot both seg-0 and seg-5
        # and seg-10 would be reclaimed; the snapshot reference alone saves
        # seg-0 (14 < 15, so both later segments are below cut).
        p = s.create_plan(15)
        chosen = [i["seg_id"] for i in p["items"]]
        self.assertNotIn(ids[0], chosen)   # snapshot-pinned
        self.assertIn(ids[1], chosen)      # referenced by no snapshot
        self.assertIn(ids[2], chosen)
        s.close()


class Scenario3RepairProtectionTest(GCTestBase):
    def test_active_repair_alone_excludes_item(self):
        s = self.seed(15)
        ids = seg_ids(s)
        victim = ids[0]
        s.quarantine(victim, "test corruption")

        release = threading.Event()
        entered = threading.Event()

        def hook(job, phase):
            if phase == "planned" and job["seg_id"] == victim:
                entered.set()
                release.wait(5)

        s._repair_phase_hook = hook
        rjob, created = s.start_repair(victim)
        self.assertTrue(created)
        self.assertTrue(entered.wait(2), "repair never parked")

        # The repair set excludes the victim even though no snapshot/hold
        # covers it and its last offset is below cut.
        p = s.create_plan(100)
        chosen = [i["seg_id"] for i in p["items"]]
        self.assertNotIn(victim, chosen)
        self.assertIn(ids[1], chosen)
        self.assertIn(ids[2], chosen)

        release.set()
        done = wait_repair(s, rjob["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        s.close()


class Scenario4HoldProtectionTest(GCTestBase):
    def test_valid_hold_excludes_and_renew_release_affect_only_new_plans(self):
        s = self.seed(15)
        ids = seg_ids(s)  # [0..4],[5..9],[10..14]

        # hold pos=0 protects ALL segments (last_offset >= 0)
        h1, created = s.put_hold("reader-A", 0, 600)
        self.assertTrue(created)
        self.assertEqual(s.create_plan(100)["items"], [])

        # Renewal is idempotent (same id, same pos) -> 200.
        h1b, created_b = s.put_hold("reader-A", 0, 900)
        self.assertFalse(created_b)
        self.assertNotEqual(h1b["expires_at"], h1["expires_at"])

        # Different pos on an existing id is a conflict.
        with self.assertRaises(PlanConflict):
            s.put_hold("reader-A", 10, 300)

        # Hold at pos=10 protects the item containing 10 (seg-10) and all
        # larger items. seg-0/seg-5 end below 10, so they stay reclaimable.
        s.release_hold("reader-A")
        s.put_hold("reader-B", 10, 600)
        chosen = [i["seg_id"] for i in s.create_plan(100)["items"]]
        self.assertEqual(chosen, [ids[0], ids[1]])
        # Hold at pos=12 still protects seg-10 (last 14 >= 12).
        s.release_hold("reader-B")
        s.put_hold("reader-C", 12, 600)
        chosen = [i["seg_id"] for i in s.create_plan(100)["items"]]
        self.assertEqual(chosen, [ids[0], ids[1]])

        # Releasing makes a *new* plan select the items again.
        s.release_hold("reader-C")
        chosen = [i["seg_id"] for i in s.create_plan(100)["items"]]
        self.assertEqual(chosen, [ids[0], ids[1], ids[2]])

    def test_plan_made_before_renew_is_rejected_at_apply(self):
        s = self.seed(15)
        plan = s.create_plan(100)
        # A hold created AFTER the preview changes the protection set -> the
        # already-minted plan must 409.
        s.put_hold("late-reader", 0, 600)
        with self.assertRaises(PlanConflict):
            s.apply_plan(plan["plan_id"])
        # And a new plan made under the hold selects nothing.
        self.assertEqual(s.create_plan(100)["items"], [])
        s.close()

    def test_plan_made_then_hold_released_still_applies(self):
        # Releasing a hold that protected nothing in the order is still a
        # protection-set change -> the conservative contract is 409; a fresh
        # plan then applies.  This pins the "only later plans" wording.
        s = self.seed(15)
        s.put_hold("h", 100, 600)  # protects nothing below any sensible cut
        plan = s.create_plan(20)
        s.release_hold("h")
        with self.assertRaises(PlanConflict):
            s.apply_plan(plan["plan_id"])
        job, status = s.apply_plan(s.create_plan(20)["plan_id"])
        self.assertEqual(status, 202)
        self.assertEqual(wait_gc(s, job["id"])["status"], "succeeded")
        s.close()

    def test_expired_hold_protects_nothing(self):
        s = self.seed(10)
        s.put_hold("short", 0, 0.01)
        time.sleep(0.05)
        p = s.create_plan(100)
        self.assertEqual([i["seg_id"] for i in p["items"]], seg_ids(s))
        # expired hold is lazily purged
        self.assertEqual(s.list_holds(), [])
        s.close()


class Scenario5ConflictAbortTest(GCTestBase):
    def _apply_and_expect_409(self, s, plan_id):
        # Drift already present at acceptance -> the whole order is rejected
        # synchronously with 409 and no gc_job/audit/disk effect.
        with self.assertRaises(PlanConflict) as ctx:
            s.apply_plan(plan_id)
        return ctx.exception

    def test_stamp_change_between_preview_and_apply_aborts_entire_order(self):
        s = self.seed(15)
        ids = seg_ids(s)
        plan = s.create_plan(100)  # would evict seg-0, seg-1, seg-2
        self.assertEqual(len(plan["items"]), 3)

        # Mutate one chosen item's stamp: rebuild bumps the meta version.
        s.quarantine(ids[1], "injected")
        rj, _ = s.start_repair(ids[1])
        self.assertEqual(wait_repair(s, rj["id"])["status"], "succeeded")

        done = self._apply_and_expect_409(s, plan["plan_id"])
        self.assertTrue(any("stamp" in c for c in done.changed))

        # Zero reclamation: all dirs present, all sealed, no audit.
        for sid in ids:
            self.assertTrue(os.path.isdir(segmod.seg_dir(s.seg_root, sid)))
        self.assertEqual(
            [m["status"] for m in s.list_segments()["segments"]],
            ["sealed"] * 3)
        self.assertEqual(s.gc_audit(), [])
        # Repeated apply of the same plan -> same 409 conclusion.
        with self.assertRaises(PlanConflict):
            s.apply_plan(plan["plan_id"])
        s.close()

        # The rejection survives a restart and keeps answering 409.
        s2 = open_store(self.tmp)
        with self.assertRaises(PlanConflict):
            s2.apply_plan(plan["plan_id"])
        s2.close()

    def test_reference_change_aborts(self):
        s = self.seed(15)
        plan = s.create_plan(100)
        s.freeze()  # snapshot now references the formerly free segments
        self._apply_and_expect_409(s, plan["plan_id"])
        self.assertEqual(s.gc_audit(), [])
        s.close()

    def test_conflict_injected_after_move_restores_and_aborts(self):
        # Drift injected AFTER acceptance but BEFORE the manifest publish:
        # the worker already moved dirs into the grave, detects the drift at
        # the publish re-validation, restores the old layout and fails 409 —
        # zero reclamation, disk bytes intact.
        s = self.seed(15)
        ids = seg_ids(s)
        plan = s.create_plan(100)
        original_sha = {sid: next(m for m in s.list_segments()["segments"]
                                  if m["id"] == sid)["sha256"] for sid in ids}
        injected = threading.Event()

        def hook(job, phase):
            if phase == "moved" and not injected.is_set():
                injected.set()
                # Create a snapshot between move and publish: protection set
                # now references segments the order is reclaiming.
                s.freeze()

        s._gc_phase_hook = hook
        job, status = s.apply_plan(plan["plan_id"])
        self.assertEqual(status, 202)
        self.assertTrue(injected.wait(3))
        done = wait_gc(s, job["id"])
        self.assertEqual(done["status"], "failed")
        self.assertEqual(done["error"]["type"], "conflict")
        # Old layout restored; every directory present with identical bytes.
        for sid in ids:
            self.assertTrue(os.path.isdir(segmod.seg_dir(s.seg_root, sid)))
            m = next(x for x in s.list_segments()["segments"] if x["id"] == sid)
            self.assertEqual(m["status"], "sealed")
            self.assertEqual(m["sha256"], original_sha[sid])
        self.assertEqual(s.gc_audit(), [])
        self.assertFalse([n for n in os.listdir(s.seg_root)
                          if n.startswith("gcgrave-")])
        # Repeated apply gives the same deterministic 409.
        with self.assertRaises(PlanConflict):
            s.apply_plan(plan["plan_id"])
        s.close()

    def test_cut_is_part_of_order_but_plan_cut_is_fixed(self):
        # A plan always keeps its own cut; drift is expressed via the
        # protection/stamp sets.  Confirm a brand new plan with a different
        # cut is simply a different order with a different id.
        s = self.seed(15)
        p1 = s.create_plan(5)
        p2 = s.create_plan(10)
        self.assertNotEqual(p1["plan_id"], p2["plan_id"])
        self.assertNotEqual(p1["stamp"], p2["stamp"])
        s.close()


class Scenario6IdempotentApplyTest(GCTestBase):
    def test_two_applies_one_job_202_then_200(self):
        s = self.seed(15)
        plan = s.create_plan(100)
        job1, code1 = s.apply_plan(plan["plan_id"])
        self.assertEqual(code1, 202)
        done = wait_gc(s, job1["id"])
        self.assertEqual(done["status"], "succeeded")
        self.assertEqual(done["evicted"], 3)

        job2, code2 = s.apply_plan(plan["plan_id"])
        self.assertEqual(code2, 200)
        self.assertEqual(job2["id"], job1["id"])
        job3, code3 = s.apply_plan(plan["plan_id"])
        self.assertEqual(code3, 200)
        self.assertEqual(job3["id"], job1["id"])

        # Exactly one gc_job exists for the plan; audit has exactly 3 entries.
        jobs = [j for j in s.list_gc_jobs(50)
                if j["plan_id"] == plan["plan_id"]]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(s.gc_audit()), 3)
        s.close()


class Scenario7CrashReconciliationTest(GCTestBase):
    GRAVE_PHASES = ("moved", "published", "audited")

    def _run_with_crash_at(self, phase):
        s = self.seed(15)
        ids = seg_ids(s)
        plan = s.create_plan(100)
        reached = threading.Event()

        def hook(job, ph):
            if ph == phase:
                reached.set()
                # Simulate hard process exit mid-phase: kill workers by
                # raising, leaving the durable/journal+fs state exactly as it
                # was at that instant (the test then opens a fresh store).
                raise SystemExit("simulated crash")

        s._gc_phase_hook = hook
        job, code = s.apply_plan(plan["plan_id"])
        self.assertEqual(code, 202)
        self.assertTrue(reached.wait(3), f"never reached phase {phase}")
        # Give the worker a moment to die on the exception, then abandon this
        # process image without close() (no graceful cleanup).
        time.sleep(0.15)
        del s
        return plan, job, ids

    def _assert_no_orphan_dirs(self, root):
        names = os.listdir(root)
        self.assertFalse(
            [n for n in names if n.startswith(("stage-", "bak-", "gcgrave-"))],
            f"orphan dirs present after reconciliation: {names}")

    def test_crash_after_move_publishes_or_rolls_back(self):
        plan, job, ids = self._run_with_crash_at("moved")
        seg_root = os.path.join(self.tmp, "segments")
        # On restart the journaled non-terminal job is reconciled: the move is
        # rolled back then the order replayed to completion (stamps still
        # match, nothing else changed).
        s = open_store(self.tmp)
        done = wait_gc(s, job["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        self.assertEqual(done["evicted"], 3)
        self._assert_no_orphan_dirs(seg_root)
        self.assertEqual(len(s.gc_audit()), 3)
        self.assertTrue(all(e["gc_job_id"] == job["id"]
                            for e in s.gc_audit()))
        # Restarting again neither duplicates work nor leaves artifacts.
        s.close()
        s = open_store(self.tmp)
        self._assert_no_orphan_dirs(seg_root)
        self.assertEqual(
            [j["status"] for j in s.list_gc_jobs() if j["id"] == job["id"]],
            ["succeeded"])
        s.close()

    def test_crash_after_manifest_publish_finishes_audit(self):
        plan, job, ids = self._run_with_crash_at("published")
        seg_root = os.path.join(self.tmp, "segments")
        s = open_store(self.tmp)
        # Startup adopts the durable publish: same job id, audit backfilled,
        # graves removed — no orphan dirs.
        j = s.get_gc_job(job["id"])
        self.assertEqual(j["status"], "succeeded")
        self._assert_no_orphan_dirs(seg_root)
        self.assertEqual(len(s.gc_audit()), 3)
        statuses = [m["status"] for m in s.list_segments()["segments"]]
        self.assertEqual(statuses, ["evicted"] * 3)
        s.close()

    def test_crash_after_audit_is_fully_done(self):
        plan, job, ids = self._run_with_crash_at("audited")
        seg_root = os.path.join(self.tmp, "segments")
        time.sleep(0.1)  # let grave removal land
        s = open_store(self.tmp)
        self.assertEqual(s.get_gc_job(job["id"])["status"], "succeeded")
        self._assert_no_orphan_dirs(seg_root)
        self.assertEqual(len(s.gc_audit()), 3)
        # idempotent across another restart
        s.close()
        s2 = open_store(self.tmp)
        self._assert_no_orphan_dirs(seg_root)
        self.assertEqual(len(s2.gc_audit()), 3)
        s2.close()


class Scenario8PostEvictionSemanticsTest(GCTestBase):
    def test_410_cursor_and_surviving_reads(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(15)])  # seg-0, seg-5, seg-10
        ids = seg_ids(s)
        # Take a snapshot whose boundary we will verify stays unchanged; it
        # references all three segments, so do the eviction FIRST in this test
        # and snapshot afterwards.  Plan evicts only seg-0 (cut=10).
        plan = s.create_plan(5)
        self.assertEqual([i["seg_id"] for i in plan["items"]], [ids[0]])
        job, code = s.apply_plan(plan["plan_id"])
        self.assertEqual(code, 202)
        self.assertEqual(wait_gc(s, job["id"])["status"], "succeeded")

        # New head writes beyond the gap still succeed.
        r = s.ingest([ev("d1", i) for i in range(15, 20)])  # offsets 15..19
        self.assertTrue(all(x["status"] == "stored" for x in r))

        # 410 with the exact resume cursor for every offset inside the run.
        with self.assertRaises(Gone) as ctx:
            s.replay(from_offset=0, limit=100)
        self.assertEqual(ctx.exception.cursor, 5)
        for off in range(0, 5):
            with self.assertRaises(Gone) as c:
                s.replay(from_offset=off, limit=100)
            self.assertEqual(c.exception.cursor, 5, off)

        # Surviving positions keep their exact offsets/keys/order.
        got = s.replay(from_offset=5, limit=100)
        self.assertEqual([e["offset"] for e in got["events"]],
                         list(range(5, 20)))
        dev = s.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in dev["events"]],
                         list(range(5, 20)))

        # Segment-oriented read of the reclaimed id -> 410 with cursor.
        with self.assertRaises(Gone) as c:
            s.segment_events(ids[0])
        self.assertEqual(c.exception.cursor, 5)

        # Another new write is readable right after the head.
        r = s.ingest([ev("d1", 100, event_id="d1-100")])
        self.assertEqual(r[0]["offset"], 20)
        self.assertEqual(
            [e["offset"] for e in s.replay(from_offset=20, limit=10)["events"]],
            [20])

        # A snapshot taken AFTER eviction references only survivors and its
        # boundary/history values are exact.  freeze() seals the open tail
        # (offsets 15..20) into a new segment; seg-0's tombstone is excluded.
        frz_after = s.freeze()
        surviving_sealed = [m["id"] for m in s.list_segments()["segments"]
                            if m["status"] != "evicted"]
        self.assertEqual(frz_after["segments"], surviving_sealed)
        self.assertNotIn(ids[0], frz_after["segments"])
        hist = s.replay(freeze_id=frz_after["id"], from_offset=5, limit=1000)
        self.assertEqual(hist["end_offset"], 21)
        self.assertEqual([e["offset"] for e in hist["events"]],
                         list(range(5, 21)))
        # reading the frozen history at the evicted run still yields 410
        with self.assertRaises(Gone) as c:
            s.replay(freeze_id=frz_after["id"], from_offset=0, limit=10)
        self.assertEqual(c.exception.cursor, 5)

        # Audit lists the one successful item.
        audit = s.gc_audit()
        self.assertEqual([a["seg_id"] for a in audit], [ids[0]])
        self.assertEqual(audit[0]["first_offset"], 0)
        self.assertEqual(audit[0]["last_offset"], 4)
        self.assertEqual(audit[0]["count"], 5)
        self.assertTrue(audit[0]["gc_job_id"])
        s.close()

        # Restart: tombstone + audit persist; still 410 with the same cursor.
        s2 = open_store(self.tmp)
        with self.assertRaises(Gone) as c:
            s2.replay(from_offset=2, limit=100)
        self.assertEqual(c.exception.cursor, 5)
        self.assertEqual(len(s2.gc_audit()), 1)
        statuses = {m["id"]: m["status"]
                    for m in s2.list_segments()["segments"]}
        self.assertEqual(statuses[ids[0]], "evicted")
        self.assertEqual(statuses[ids[1]], "sealed")
        s2.close()


class GCOverlappingOrdersTest(GCTestBase):
    def test_concurrent_orders_never_claim_the_same_item(self):
        s = self.seed(15)  # seg-0, seg-5, seg-10
        ids = seg_ids(s)
        entered = threading.Event()
        release = threading.Event()

        def hook(job, phase):
            if phase == "moved":
                entered.set()
                release.wait(5)

        s._gc_phase_hook = hook
        # First order takes all three segments and parks after moving them.
        p1 = s.create_plan(100)
        j1, c1 = s.apply_plan(p1["plan_id"])
        self.assertEqual(c1, 202)
        self.assertTrue(entered.wait(2))

        # While the first order is in flight, a new preview sees the items as
        # reserved and selects none of them.
        self.assertEqual(s.create_plan(100)["items"], [])
        release.set()
        self.assertEqual(wait_gc(s, j1["id"])["status"], "succeeded")
        self.assertEqual(len(s.gc_audit()), 3)
        s.close()

    def test_second_order_reclaims_what_first_left(self):
        # A hold at preview pins the tail for order 1; order 2 (after release)
        # reclaims the remainder with its own job and its own audit entries.
        s = self.seed(15)
        ids = seg_ids(s)
        s.put_hold("tail", 10, 600)  # pins seg-10
        p1 = s.create_plan(100)
        self.assertEqual([i["seg_id"] for i in p1["items"]], [ids[0], ids[1]])
        j1, _ = s.apply_plan(p1["plan_id"])
        self.assertEqual(wait_gc(s, j1["id"])["status"], "succeeded")
        s.release_hold("tail")
        p2 = s.create_plan(100)
        self.assertEqual([i["seg_id"] for i in p2["items"]], [ids[2]])
        j2, code = s.apply_plan(p2["plan_id"])
        self.assertEqual(code, 202)
        self.assertNotEqual(j2["id"], j1["id"])
        self.assertEqual(wait_gc(s, j2["id"])["status"], "succeeded")
        self.assertEqual([a["seg_id"] for a in s.gc_audit()],
                         [ids[0], ids[1], ids[2]])
        s.close()


class GCForegroundNotBlockedTest(GCTestBase):
    def test_large_eviction_does_not_block_foreground(self):
        s = self.seed(50)  # 10 sealed segments
        plan = s.create_plan(100)
        self.assertEqual(len(plan["items"]), 10)

        entered = threading.Event()
        release = threading.Event()

        def hook(job, phase):
            if phase == "moved":
                entered.set()
                release.wait(5)

        s._gc_phase_hook = hook
        job, code = s.apply_plan(plan["plan_id"])
        self.assertEqual(code, 202)
        self.assertTrue(entered.wait(2))
        # Heavy directory work parked: key reads and writes answer at once,
        # reading even the segments currently held in the grave.
        t0 = time.monotonic()
        r = s.ingest([ev("d1", 200, event_id="d1-200")])
        self.assertEqual(r[0]["status"], "stored")
        got = s.device_events("d1", limit=50)
        self.assertTrue(any(e["offset"] == 0 for e in got["events"]),
                        "key reads of moved segments must still succeed")
        self.assertLess(time.monotonic() - t0, 1.0)
        release.set()
        done = wait_gc(s, job["id"])
        # No freeze was injected, so the parked order commits cleanly even
        # though reads used the grave copies concurrently.
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        s.close()


if __name__ == "__main__":
    unittest.main()
