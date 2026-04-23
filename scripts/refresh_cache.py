#!/usr/bin/env python3
"""Refresh the static data cache for the BAL dashboard.

Fetches every test_stats row and runs metadata for the configured suite
from the benchmarkoor API, writes a compact snapshot to data/cache.json.

Rows are stored in a minimal per-client shape: {i, r, t, m} for
(id, run_id, test_name, test_mgas_s). The front-end expands them into
full field names on load.

A max_test_stats_id is included so the front-end can issue a short,
targeted "?id=gt.<max>" query for whatever has been added since the
cache was built.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_KEY = os.environ.get("BENCHMARKOOR_API_KEY")
if not API_KEY:
    print("ERROR: BENCHMARKOOR_API_KEY env var required", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://benchmarkoor-api.core.ethpandaops.io/api/v1/index/query"
SUITE_HASH = "bc398819d1ebc628"
CLIENTS = ["besu", "geth", "nethermind", "erigon", "reth"]
PAGE_SIZE = 1000
OUT = Path("data") / "cache.json"


def fetch(path: str, params: dict) -> list[dict]:
    params = dict(params)
    params["limit"] = PAGE_SIZE
    offset = 0
    rows: list[dict] = []
    while True:
        params["offset"] = offset
        q = urllib.parse.urlencode(params)
        url = f"{BASE_URL}/{path}?{q}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "User-Agent": "bal-dashboard-refresh/1.0",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            print(
                f"HTTPError {exc.code} for {url}: {exc.read()[:200]!r}",
                file=sys.stderr,
            )
            raise
        data = body.get("data") or []
        rows.extend(data)
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def minimize_run(row: dict) -> dict:
    return {
        "run_id": row.get("run_id"),
        "client": row.get("client"),
        "instance_id": row.get("instance_id"),
        "image": row.get("image"),
        "indexed_at": row.get("indexed_at"),
        "timestamp": row.get("timestamp"),
        "status": row.get("status"),
    }


def main() -> int:
    generated_at = int(time.time())

    # Intern run_ids and test names so the same strings aren't repeated
    # tens of thousands of times in the output. The front-end expands them.
    run_ids: list[str] = []
    run_id_to_idx: dict[str, int] = {}
    test_names: list[str] = []
    test_name_to_idx: dict[str, int] = {}

    def rid_idx(s: str) -> int:
        i = run_id_to_idx.get(s)
        if i is None:
            i = len(run_ids)
            run_id_to_idx[s] = i
            run_ids.append(s)
        return i

    def tname_idx(s: str) -> int:
        i = test_name_to_idx.get(s)
        if i is None:
            i = len(test_names)
            test_name_to_idx[s] = i
            test_names.append(s)
        return i

    test_stats: dict[str, list[list]] = {}
    max_id = 0
    for client in CLIENTS:
        print(f"Fetching test_stats for {client}...", file=sys.stderr, flush=True)
        rows = fetch(
            "test_stats",
            {
                "client": f"eq.{client}",
                "suite_hash": f"eq.{SUITE_HASH}",
                "select": "id,run_id,test_name,test_mgas_s",
            },
        )
        rows.sort(key=lambda r: r["id"])
        compact = [
            [r["id"], rid_idx(r["run_id"]), tname_idx(r["test_name"]), r["test_mgas_s"]]
            for r in rows
        ]
        test_stats[client] = compact
        client_max = rows[-1]["id"] if rows else 0
        if client_max > max_id:
            max_id = client_max
        print(f"  {len(rows)} rows (max id {client_max})", file=sys.stderr)

    print("Fetching runs...", file=sys.stderr, flush=True)
    runs_rows = fetch(
        "runs",
        {
            "suite_hash": f"eq.{SUITE_HASH}",
            "select": "run_id,client,instance_id,image,indexed_at,timestamp,status",
        },
    )
    runs = sorted(
        (minimize_run(r) for r in runs_rows),
        key=lambda x: (x.get("client") or "", x.get("run_id") or ""),
    )
    print(f"  {len(runs)} runs", file=sys.stderr)

    cache = {
        "version": 1,
        "generated_at": generated_at,
        "max_test_stats_id": max_id,
        "suite_hash": SUITE_HASH,
        "clients": CLIENTS,
        "run_ids": run_ids,
        "test_names": test_names,
        "test_stats": test_stats,
        "runs": runs,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(cache, separators=(",", ":")) + "\n")
    size = OUT.stat().st_size
    print(
        f"Wrote {OUT} ({size:,} bytes) with max_id={max_id}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
