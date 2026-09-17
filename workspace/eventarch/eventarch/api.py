"""HTTP API (stdlib only).  See README.md for the full endpoint reference."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from . import store as storemod

log = logging.getLogger("eventarch.api")

MAX_BODY = 16 << 20  # 16 MiB


def _clamp_limit(raw, default, ceiling):
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return default
    return max(1, min(n, ceiling))


class Handler(BaseHTTPRequestHandler):
    server_version = "eventarch/0.1"
    protocol_version = "HTTP/1.1"

    # -- helpers -------------------------------------------------------- #

    @property
    def store(self) -> storemod.ArchiveStore:
        return self.server.store  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # route through logging
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, msg, **extra):
        self._send_json({"error": msg, **extra}, status=status)

    def _body_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        if n > MAX_BODY:
            raise ValueError("request body too large")
        return json.loads(self.rfile.read(n))

    def _dispatch(self, method):
        try:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            return self._route(method, parts, q)
        except storemod.NotFound as exc:
            self._error(404, str(exc))
        except storemod.PlanConflict as exc:
            self._error(409, str(exc), changed=exc.changed)
        except storemod.Gone as exc:
            self._error(410, str(exc), cursor=exc.cursor)
        except storemod.Quarantined as exc:
            self._error(410, str(exc), segment=exc.seg_id,
                        resume_offset=exc.resume_offset)
        except storemod.WalCoverageGone as exc:
            self._error(410, str(exc), segment=exc.seg_id,
                        resume_offset=exc.resume_offset)
        except storemod.RepairTimeout as exc:
            self._error(503, str(exc), job_id=exc.job_id)
        except (ValueError, json.JSONDecodeError) as exc:
            self._error(400, str(exc))
        except BrokenPipeError:
            pass
        except Exception:
            log.exception("unhandled error")
            self._error(500, "internal error")

    # -- routing -------------------------------------------------------- #

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _route(self, method, parts, q):
        s = self.store

        if method == "GET" and parts == ["v1", "healthz"]:
            return self._send_json({"status": "ok"})

        if method == "GET" and parts == ["v1", "stats"]:
            return self._send_json(s.stats())

        if method == "POST" and parts == ["v1", "ingest"]:
            body = self._body_json()
            events = body.get("events") if isinstance(body, dict) else body
            if isinstance(body, dict) and "events" not in body and "event_id" in body:
                events = [body]  # single-event convenience form
            return self._send_json({"results": s.ingest(events)})

        if method == "GET" and parts == ["v1", "devices"]:
            return self._send_json({"devices": s.list_devices()})

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "devices"] \
                and parts[3] == "events":
            from_seq = q.get("from_seq")
            return self._send_json(s.device_events(
                parts[2],
                from_seq=int(from_seq) if from_seq is not None else None,
                from_offset=int(q.get("from_offset", 0)),
                limit=_clamp_limit(q.get("limit"), 100, 1000),
            ))

        if method == "GET" and parts == ["v1", "segments"]:
            return self._send_json(s.list_segments())

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "segments"] \
                and parts[3] == "events":
            return self._send_json(s.segment_events(
                parts[2],
                from_offset=int(q.get("from_offset", 0)),
                limit=_clamp_limit(q.get("limit"), 500, 5000),
            ))

        if method == "POST" and len(parts) == 4 and parts[:2] == ["v1", "segments"] \
                and parts[3] == "rebuild":
            # Maintenance is asynchronous: enqueue a background repair job
            # and return immediately so foreground traffic is never blocked.
            # Same segment while a job is active -> the existing job (200).
            job, created = s.start_repair(parts[2])
            return self._send_json({"job": job}, status=202 if created else 200)

        if method == "GET" and parts == ["v1", "repairs"]:
            limit = _clamp_limit(q.get("limit"), 100, 1000)
            return self._send_json({"jobs": s.list_repairs(limit=limit)})

        if method == "GET" and len(parts) == 3 and parts[:2] == ["v1", "repairs"]:
            return self._send_json({"job": s.get_repair(parts[2])})

        if method == "POST" and len(parts) == 3 and parts[:2] == ["v1", "repairs"]:
            body = self._body_json()
            timeout = body.get("timeout", 60.0) if isinstance(body, dict) else 60.0
            try:
                timeout = float(timeout)
            except (TypeError, ValueError):
                timeout = 60.0
            timeout = max(0.0, min(timeout, 3600.0))
            return self._send_json({"job": s.wait_repair(parts[2], timeout=timeout)})

        if method == "POST" and parts == ["v1", "freeze"]:
            body = self._body_json()
            note = body.get("note", "") if isinstance(body, dict) else ""
            return self._send_json(s.freeze(note))

        if method == "GET" and parts == ["v1", "freezes"]:
            return self._send_json({"freezes": s.list_freezes()})

        if method == "GET" and parts == ["v1", "replay"]:
            return self._send_json(s.replay(
                freeze_id=q.get("freeze_id"),
                from_offset=int(q.get("from_offset", 0)),
                device_id=q.get("device_id"),
                limit=_clamp_limit(q.get("limit"), 500, 5000),
            ))

        # ---- capacity eviction: previews, holds, jobs, audit ----------- #

        if method == "POST" and parts == ["v1", "gc", "plans"]:
            # Pure preview: only plan_id/stamp/items/size (+cut metadata) are
            # returned; no archive file or manifest is modified.
            body = self._body_json()
            if not isinstance(body, dict) or "cut" not in body:
                raise ValueError("body must contain an integer 'cut'")
            return self._send_json(s.create_plan(int(body["cut"])))

        if method == "POST" and len(parts) == 5 and parts[:2] == ["v1", "gc"] \
                and parts[2] == "plans" and parts[4] == "apply":
            # First acceptance -> 202 + gc_job; repeat of the same plan ->
            # 200 + the same job id.  A drifted order -> 409 (disk untouched).
            job, status = s.apply_plan(parts[3])
            return self._send_json({"gc_job": job}, status=status)

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "gc"] \
                and parts[2] == "plans":
            return self._send_json(s.get_plan(parts[3]))

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "gc"] \
                and parts[2] == "jobs":
            return self._send_json({"gc_job": s.get_gc_job(parts[3])})

        if method == "POST" and len(parts) == 4 and parts[:2] == ["v1", "gc"] \
                and parts[2] == "jobs":
            body = self._body_json()
            timeout = body.get("timeout", 60.0) if isinstance(body, dict) else 60.0
            try:
                timeout = float(timeout)
            except (TypeError, ValueError):
                timeout = 60.0
            timeout = max(0.0, min(timeout, 3600.0))
            return self._send_json({"gc_job": s.wait_gc_job(parts[3], timeout)})

        if method == "GET" and parts == ["v1", "gc", "jobs"]:
            return self._send_json(
                {"jobs": s.list_gc_jobs(limit=_clamp_limit(q.get("limit"), 100, 1000))})

        if method == "GET" and parts == ["v1", "gc", "audit"]:
            return self._send_json(
                {"audit": s.gc_audit(limit=_clamp_limit(q.get("limit"), 100, 10000))})

        if method == "POST" and parts == ["v1", "holds"]:
            body = self._body_json()
            if not isinstance(body, dict) or "hold_id" not in body \
                    or "pos" not in body:
                raise ValueError("body must contain hold_id, pos, ttl_seconds")
            hold, created = s.put_hold(
                body["hold_id"], int(body["pos"]),
                float(body["ttl_seconds"]) if body.get("ttl_seconds") is not None
                else None)
            return self._send_json({"hold": hold}, status=201 if created else 200)

        if method == "GET" and parts == ["v1", "holds"]:
            return self._send_json({"holds": s.list_holds()})

        if method == "DELETE" and len(parts) == 3 and parts[:2] == ["v1", "holds"]:
            return self._send_json({"hold": s.release_hold(parts[2])})

        return self._error(404, "not found")
