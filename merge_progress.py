#!/usr/bin/env python3
"""Merge per-worker progress files into a single combined progress.json for dashboard display.

Reads:
  pro_scraped_output/{series_slug}/progress_worker_1.json
  pro_scraped_output/{series_slug}/progress_worker_2.json
  pro_scraped_output/{series_slug}/progress_worker_3.json

Writes:
  pro_scraped_output/{series_slug}/progress.json  (merged, same format as free scraper)
  pro_scraped_output/COMBINED_STATS.json           (overall summary)

This script is idempotent — safe to run multiple times.
"""
import json, os, sys
from datetime import datetime, timezone


def merge_one(combined: dict, worker_data: dict):
    """Merge one worker's progress into the combined dict (in-place)."""
    for series_slug, sd in worker_data.items():
        if series_slug not in combined:
            combined[series_slug] = {"scraped": {}, "failed": {}, "pro": {}}
        tgt = combined[series_slug]
        # Scraped: take any new test IDs (worker files don't overlap on test IDs)
        for tid, info in sd.get("scraped", {}).items():
            if tid not in tgt["scraped"]:
                tgt["scraped"][tid] = info
        # Failed: merge attempts
        for tid, info in sd.get("failed", {}).items():
            if tid in tgt["failed"]:
                tgt["failed"][tid]["attempts"] = (
                    tgt["failed"][tid].get("attempts", 1) +
                    info.get("attempts", 1)
                )
                tgt["failed"][tid]["last_reason"] = info.get("last_reason",
                                                              info.get("reason", ""))
            else:
                tgt["failed"][tid] = dict(info)
        # PRO: merge
        for tid, info in sd.get("pro", {}).items():
            tgt["pro"][tid] = info


def main():
    output_dir = "pro_scraped_output"
    if not os.path.isdir(output_dir):
        print(f"No {output_dir}/ directory found")
        return

    total_scraped = 0
    total_failed = 0
    series_count = 0
    worker_summaries = []

    for series_slug in sorted(os.listdir(output_dir)):
        series_path = os.path.join(output_dir, series_slug)
        if not os.path.isdir(series_path):
            continue

        combined = {"scraped": {}, "failed": {}, "pro": {}}
        found_any = False
        for wid in [1, 2, 3]:
            wp = os.path.join(series_path, f"progress_worker_{wid}.json")
            if not os.path.exists(wp):
                continue
            try:
                with open(wp) as f:
                    worker_data = json.load(f)
                merge_one(combined, worker_data)
                found_any = True
            except Exception as e:
                print(f"  ⚠️ error reading {wp}: {e}")

        if not found_any:
            continue

        # Write combined progress.json for this series
        out_path = os.path.join(series_path, "progress.json")
        tmp = out_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(combined, f, indent=2)
        os.rename(tmp, out_path)

        sc = len(combined["scraped"])
        fl = len(combined["failed"])
        total_scraped += sc
        total_failed += fl
        series_count += 1
        print(f"  {series_slug:35s}: {sc:5d} scraped, {fl:5d} failed")

    summary = {
        "total_scraped": total_scraped,
        "total_failed": total_failed,
        "series_count": series_count,
        "target_total": 9915,
        "remaining": max(0, 9915 - total_scraped),
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }
    out = os.path.join(output_dir, "COMBINED_STATS.json")
    with open(out, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Combined {series_count} series → {total_scraped} scraped, "
          f"{total_failed} failed")
    print(f"Remaining: {summary['remaining']}/{summary['target_total']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
