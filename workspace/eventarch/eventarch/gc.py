"""Capacity eviction (GC) plans, stamps, holds and evicted-range geometry.

Pure helper logic (no store state, no threads) used by ArchiveStore:

  * selection: which sealed segments a ``cut`` would reclaim, after avoiding
    snapshot references, the active repair set and unexpired hold zones;
  * stamps:   exact content fingerprints the apply step re-validates so any
    stamp / reference / repair-state / hold-set / cut change aborts the whole
    order with 409 and leaves the disk untouched;
  * ranges:   merged evicted offset runs; a read landing on one yields 410
    with the precise cursor (next surviving offset).

Protection semantics of a hold at ``pos``: the item containing ``pos`` and
every item at a *larger* position are protected, i.e. a segment whose
``last_offset >= pos`` is never reclaimed while the hold is unexpired.
"""

from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from typing import Iterable, List, Optional, Tuple

from .models import fmt_ts
from .util import dumps

# GC job lifecycle (mirrors the repair-job conventions).
GC_TERMINAL = ("succeeded", "failed")

# Persistent manifest marker for segments that a committed GC order removed.
EVICTED = "evicted"


class PlanConflict(Exception):
    """An apply order no longer matches the current archive (HTTP 409)."""

    def __init__(self, reason: str, changed: Optional[List[str]] = None):
        super().__init__(reason)
        self.reason = reason
        self.changed = changed or []


class Gone(Exception):
    """Read of an evicted position (HTTP 410); ``cursor`` resumes the scan."""

    def __init__(self, message: str, cursor: int):
        super().__init__(message)
        self.cursor = cursor


def hold_expired(hold: dict, now) -> bool:
    return fmt_ts(now) >= hold["expires_at"]


def active_holds(holds: Iterable[dict], now) -> List[dict]:
    return [h for h in holds if not hold_expired(h, now)]


def make_hold(hold_id: str, pos: int, ttl_seconds: float, now) -> dict:
    return {
        "hold_id": hold_id,
        "pos": pos,
        "created_at": fmt_ts(now),
        "renewed_at": fmt_ts(now),
        "ttl_seconds": float(ttl_seconds),
        "expires_at": fmt_ts(now + timedelta(seconds=ttl_seconds)),
    }


def renew_hold(hold: dict, ttl_seconds: float, now) -> dict:
    hold = dict(hold)
    hold["ttl_seconds"] = float(ttl_seconds)
    hold["renewed_at"] = fmt_ts(now)
    hold["expires_at"] = fmt_ts(now + timedelta(seconds=ttl_seconds))
    return hold


# ---------------------------------------------------------------------- #
# selection                                                              #
# ---------------------------------------------------------------------- #

def select_items(cut: int, segments: List[dict], snapshot_seg_ids: set,
                 repairing_ids: set, holds: List[dict],
                 evicting_ids: Optional[set] = None) -> List[dict]:
    """Candidates strictly below ``cut`` (item *last* offset < cut) that are
    sealed and avoided by every protection set.  Sorted by first_offset."""
    evicting_ids = evicting_ids or set()
    items: List[dict] = []
    for meta in sorted(segments, key=lambda m: m["first_offset"]):
        if meta["status"] != "sealed":
            continue  # quarantined or already evicted: never an eviction item
        if meta["last_offset"] >= cut:
            continue  # cut is an exclusive high-water position
        if meta["id"] in snapshot_seg_ids:
            continue  # referenced by at least one snapshot (freeze)
        if meta["id"] in repairing_ids:
            continue  # currently under repair
        if meta["id"] in evicting_ids:
            continue  # reserved by an in-flight eviction order
        if any(meta["last_offset"] >= h["pos"] for h in holds):
            continue  # inside an unexpired hold protection zone
        items.append(meta)
    return items


def segment_bytes(seg_root: str, seg_id: str) -> int:
    total = 0
    d = os.path.join(seg_root, seg_id)
    for name in ("events.log", "index.json", "meta.json"):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            total += os.path.getsize(p)
    return total


def item_fingerprint(meta: dict, size: int) -> dict:
    return {
        "seg_id": meta["id"],
        "first_offset": meta["first_offset"],
        "last_offset": meta["last_offset"],
        "count": meta["count"],
        "sha256": meta.get("sha256"),
        "version": meta.get("version", 1),
        "status": meta["status"],
        "size": size,
    }


def plan_stamp(cut: int, fingerprints: List[dict]) -> str:
    """Whole-order fingerprint over cut and the exact selected items.

    The protection sets (snapshots / repairs / holds) are captured separately
    by protection_signature() at preview time; apply requires both the item
    stamp and the protection signature to match, so any intervening change —
    including a hold that was renewed/released — aborts the order.
    """
    payload = {
        "cut": cut,
        "items": sorted(fingerprints, key=lambda f: f["seg_id"]),
    }
    return hashlib.sha256(dumps(payload)).hexdigest()


def protection_signature(snapshot_seg_ids, repairing_ids, holds,
                         evicting_ids=None) -> str:
    payload = {
        "snapshots": sorted(snapshot_seg_ids),
        "repairing": sorted(repairing_ids),
        "evicting": sorted(evicting_ids or []),
        "holds": sorted(
            ({"hold_id": h["hold_id"], "pos": h["pos"],
              "expires_at": h["expires_at"]} for h in holds),
            key=lambda h: h["hold_id"]),
    }
    return hashlib.sha256(dumps(payload)).hexdigest()


# ---------------------------------------------------------------------- #
# evicted ranges / 410 cursors                                           #
# ---------------------------------------------------------------------- #

def merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Merge inclusive [first, last] offset runs (also joins adjacency)."""
    if not ranges:
        return []
    merged: List[Tuple[int, int]] = []
    for first, last in sorted(ranges):
        if not merged or first > merged[-1][1] + 1:
            merged.append((first, last))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], last))
    return merged


def evicted_ranges(segments: List[dict]) -> List[Tuple[int, int]]:
    return merge_ranges([
        (m["first_offset"], m["last_offset"])
        for m in segments if m.get("status") == EVICTED
    ])


def find_run(ranges: List[Tuple[int, int]], off: int) -> Optional[Tuple[int, int]]:
    for first, last in ranges:
        if first <= off <= last:
            return first, last
        if off < first:
            break
    return None


def next_surviving_offset(ranges: List[Tuple[int, int]], off: int,
                           head: int) -> int:
    """Resume cursor for a read starting at (inside) an evicted run.

    Returns the first offset >= ``off`` that is not evicted, i.e. the run's
    last+1 (capped at the current head when the whole tail is gone)."""
    run = find_run(ranges, off)
    if run is None:
        return off
    return min(run[1] + 1, head)
