"""HTTP contract tests for the capacity-eviction API.

Exercises the exact wire shapes from the spec over a real ThreadingHTTPServer:
  POST /v1/gc/plans            -> {plan_id, stamp, items, size}
  POST /v1/gc/plans/{id}/apply -> 202 + gc_job, repeat 200 + same id; 409 drift
  GET  /v1/gc/jobs/{id}         -> progress
  GET  /v1/gc/audit             -> successful items
  POST /v1/holds (idempotent renew), DELETE /v1/holds/{id}
  reads of evicted positions    -> 410 + accurate cursor
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.api import Handler
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore


def ev(device, seq):
    return {"device_id": device, "event_id": f"{device}-{seq}", "seq": seq,
            "device_ts": fmt_ts(utcnow()), "payload": {"seq": seq}}


class ApiClient:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def wait_gc(self, jid, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code, body = self.call("GET", f"/v1/gc/jobs/{jid}")
            j = body["gc_job"]
            if j["status"] in ("succeeded", "failed"):
                return j
            time.sleep(0.01)
        raise AssertionError("gc job never finished")


class GcApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        cfg = Config(data_dir=self.tmp, segment_max_records=5,
                     segment_max_age_sec=3600, wal_retain_segments=8,
                     janitor_interval_sec=60)
        self.store = ArchiveStore(cfg)
        self.store.open()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.store = self.store
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.api = ApiClient(f"http://127.0.0.1:{self.port}")
        self.t = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.t.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.store.close()
        shutil.rmtree(self.tmp, True)

    def _ingest(self, n):
        self.api.call("POST", "/v1/ingest",
                      {"events": [ev("d1", i) for i in range(n)]})

    def test_plan_apply_job_audit_and_410_cursor(self):
        self._ingest(15)  # three sealed segments [0..4],[5..9],[10..14]
        code, p = self.api.call("POST", "/v1/gc/plans", {"cut": 5})
        self.assertEqual(code, 200)
        self.assertEqual(set(p), {"plan_id", "cut", "stamp", "items",
                                  "size", "created_at"})
        self.assertEqual(len(p["items"]), 1)
        plan_id = p["plan_id"]

        code, body = self.api.call("POST", f"/v1/gc/plans/{plan_id}/apply")
        self.assertEqual(code, 202)
        jid = body["gc_job"]["id"]
        self.assertTrue(jid.startswith("gcj-"))

        done = self.api.wait_gc(jid)
        self.assertEqual(done["status"], "succeeded")
        self.assertEqual(done["evicted"], 1)

        # repeat apply -> 200 + same job id
        code, body = self.api.call("POST", f"/v1/gc/plans/{plan_id}/apply")
        self.assertEqual(code, 200)
        self.assertEqual(body["gc_job"]["id"], jid)

        # audit
        code, body = self.api.call("GET", "/v1/gc/audit")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["audit"]), 1)
        self.assertEqual(body["audit"][0]["last_offset"], 4)

        # 410 with accurate cursor on the evicted range
        code, body = self.api.call("GET", "/v1/replay?from_offset=0&limit=100")
        self.assertEqual(code, 410)
        self.assertEqual(body["cursor"], 5)
        # resume from the cursor returns surviving data
        code, body = self.api.call("GET", "/v1/replay?from_offset=5&limit=100")
        self.assertEqual(code, 200)
        self.assertEqual([e["offset"] for e in body["events"]],
                         list(range(5, 15)))

    def test_holds_create_renew_delete_and_protection(self):
        self._ingest(10)  # seg-0, seg-5
        code, body = self.api.call("POST", "/v1/holds",
                                   {"hold_id": "r1", "pos": 0, "ttl_seconds": 600})
        self.assertEqual(code, 201)
        self.assertEqual(body["hold"]["hold_id"], "r1")

        # idempotent renewal -> 200
        code, body = self.api.call("POST", "/v1/holds",
                                   {"hold_id": "r1", "pos": 0, "ttl_seconds": 900})
        self.assertEqual(code, 200)

        code, p = self.api.call("POST", "/v1/gc/plans", {"cut": 100})
        self.assertEqual(p["items"], [])  # hold protects everything

        code, _ = self.api.call("DELETE", "/v1/holds/r1")
        self.assertEqual(code, 200)
        code, _ = self.api.call("DELETE", "/v1/holds/r1")
        self.assertEqual(code, 404)  # already released

        code, p = self.api.call("POST", "/v1/gc/plans", {"cut": 100})
        self.assertEqual(len(p["items"]), 2)

    def test_apply_unknown_plan_404_and_drift_409(self):
        self._ingest(5)
        code, _ = self.api.call("POST", "/v1/gc/plans/nope/apply")
        self.assertEqual(code, 404)

        code, p = self.api.call("POST", "/v1/gc/plans", {"cut": 100})
        # inject a snapshot reference after preview
        self.api.call("POST", "/v1/freeze", {"note": "late"})
        code, body = self.api.call("POST", f"/v1/gc/plans/{p['plan_id']}/apply")
        self.assertEqual(code, 409)
        self.assertIn("error", body)
        # repeat stays 409
        code, _ = self.api.call("POST", f"/v1/gc/plans/{p['plan_id']}/apply")
        self.assertEqual(code, 409)

    def test_invalid_inputs_are_400(self):
        code, _ = self.api.call("POST", "/v1/gc/plans", {"cut": -3})
        self.assertEqual(code, 400)
        code, _ = self.api.call("POST", "/v1/gc/plans", {})
        self.assertEqual(code, 400)
        code, _ = self.api.call("POST", "/v1/holds",
                                {"hold_id": "x", "ttl_seconds": 10})
        self.assertEqual(code, 400)


if __name__ == "__main__":
    unittest.main()
