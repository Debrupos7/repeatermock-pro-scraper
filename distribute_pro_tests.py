#!/usr/bin/env python3
"""Distribute PRO_TESTS.json across 3 workers (round-robin per series).

Output:
  pro_chunks/worker_1.json  — tests assigned to worker 1 (account 1)
  pro_chunks/worker_2.json  — tests assigned to worker 2 (account 2)
  pro_chunks/worker_3.json  — tests assigned to worker 3 (account 3)
  pro_chunks/all_chunks.json — combined (for stats)

Each worker file has the same structure:
  {
    "worker_id": 1,
    "account_email": "akarakesh7@gmail.com",
    "chunks": [
      {"job_name": "SSC-CGL-2026", "series_slug": "ssc-cgl", "tests": [...]},
      ...
    ],
    "total_tests": 3305
  }

The folder structure for scraped output is EXACTLY the same as the free
scraper:
    pro_scraped_output/{series_slug}/ai_export/{section}/{subsection}/{title}_{test_id}.json
    pro_scraped_output/{series_slug}/html_export/{section}/{subsection}/{title}_{test_id}.html

But each worker writes its own progress file:
    pro_scraped_output/{series_slug}/progress_worker_1.json
    pro_scraped_output/{series_slug}/progress_worker_2.json
    pro_scraped_output/{series_slug}/progress_worker_3.json

SSC series come before RRB series. Within each series, tests are distributed
round-robin across workers (worker 1 gets tests 0, 3, 6, ...; worker 2 gets
1, 4, 7, ...; worker 3 gets 2, 5, 8, ...). This ensures balanced load per
worker and that every worker handles tests from every series, so the
scraped output folder is filled in parallel.
"""
import json, os, sys, urllib.request

REPO = "sujitbhai7710/repeatermock-mass-scraper"
MAX_TESTS = int(os.environ.get("MAX_TESTS", "0"))  # 0 = unlimited
NUM_WORKERS = 3

ACCOUNTS = [
    {"id": 1, "email": "akarakesh7@gmail.com"},
    {"id": 2, "email": "spandanrathore@gmail.com"},
    {"id": 3, "email": "tulikup2@gmail.com"},
]

# Series slug → job_name mapping (same as free scraper)
SERIES_TO_JOB = {
    "ssc-cgl": "SSC-CGL-2026", "ssc-chsl": "SSC-CHSL-2026", "ssc-mts": "SSC-MTS-2026",
    "ssc-gd-constable": "SSC-GD-2026", "ssc-cpo": "SSC-CPO-2026", "ssc-stenographer": "SSC-Steno-2026",
    "ssc-selection-post": "SSC-SelPost-2026", "ssc-chsl-previous": "SSC-CHSL-2025",
    "ssc-cpo-previous": "SSC-CPO-2025", "ssc-mts-previous": "SSC-MTS-2025",
    "ssc-maths-previous-year-questions": "Maths-PYP", "ssc-reasoning-previous-year-questions": "Reasoning-PYP",
    "ssc-english-previous-year-questions": "English-PYP", "ssc-gk-previous-year-questions": "GK-PYP",
    "rrb-group-d": "RRB-Group-D", "rrb-ntpc-ug": "RRB-NTPC-UG", "rrb-ntpc": "RRB-NTPC-Grad",
    "rrb-alp": "RRB-ALP-2026", "rrb-technician": "RRB-Tech-2026",
    "rrb-technician-previous": "RRB-Tech-Prev", "rrb-alp-previous": "RRB-ALP-Prev",
    "rrb-technician-grade-1": "RRB-Tech-Gr1",
}

SSC_ORDER = [k for k in SERIES_TO_JOB if k.startswith("ssc")]
RRB_ORDER = [k for k in SERIES_TO_JOB if k.startswith("rrb")]
ORDERED_SERIES = SSC_ORDER + RRB_ORDER


def fetch_pro_tests():
    url = f"https://raw.githubusercontent.com/{REPO}/main/scraped_output/PRO_TESTS.json"
    print(f"Fetching PRO_TESTS.json from {url}...")
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data.get("pro_tests", [])


def main():
    tests = fetch_pro_tests()
    print(f"Total PRO tests: {len(tests)}")

    if MAX_TESTS > 0:
        tests = tests[:MAX_TESTS]
        print(f"TEST MODE: {len(tests)} tests")

    # Sort: SSC first, then RRB, then any other
    ssc_tests = [t for t in tests if t.get("series_slug", "").startswith("ssc")]
    rrb_tests = [t for t in tests if t.get("series_slug", "").startswith("rrb")]
    other_tests = [t for t in tests
                   if not t.get("series_slug", "").startswith("ssc")
                   and not t.get("series_slug", "").startswith("rrb")]
    sorted_tests = ssc_tests + rrb_tests + other_tests
    print(f"Sorted: {len(ssc_tests)} SSC + {len(rrb_tests)} RRB + {len(other_tests)} other = {len(sorted_tests)} total")

    # Group by series_slug, preserving SSC-first ordering
    by_series = {}
    for t in sorted_tests:
        slug = t.get("series_slug", "unknown")
        by_series.setdefault(slug, []).append(t)

    # Build chunks per worker (round-robin within each series)
    # Worker N gets tests at indices N, N+3, N+6, ... within each series
    worker_chunks = {i + 1: [] for i in range(NUM_WORKERS)}
    for slug in ORDERED_SERIES:
        if slug not in by_series:
            continue
        series_tests = by_series[slug]
        job_name = SERIES_TO_JOB.get(slug, slug)
        for idx, test in enumerate(series_tests):
            wid = (idx % NUM_WORKERS) + 1
            worker_chunks[wid].append({
                "job_name": job_name,
                "series_slug": slug,
                "test": test,
            })

    # Now organize per worker: group their tests into chunks per series
    os.makedirs("pro_chunks", exist_ok=True)

    all_chunks = []
    for wid in range(1, NUM_WORKERS + 1):
        # Group worker's tests by series_slug
        worker_by_series = {}
        for item in worker_chunks[wid]:
            slug = item["series_slug"]
            worker_by_series.setdefault(slug, {
                "job_name": item["job_name"],
                "series_slug": slug,
                "tests": [],
            })["tests"].append(item["test"])

        chunks = list(worker_by_series.values())
        # Re-sort chunks per SSC-first order
        order_map = {slug: i for i, slug in enumerate(ORDERED_SERIES)}
        chunks.sort(key=lambda c: order_map.get(c["series_slug"], 999))

        worker_data = {
            "worker_id": wid,
            "account_email": ACCOUNTS[wid - 1]["email"],
            "chunks": chunks,
            "total_tests": sum(len(c["tests"]) for c in chunks),
            "num_series": len(chunks),
        }

        path = f"pro_chunks/worker_{wid}.json"
        with open(path, "w") as f:
            json.dump(worker_data, f, ensure_ascii=False, indent=2)

        all_chunks.append(worker_data)
        print(f"\nWorker {wid} ({ACCOUNTS[wid - 1]['email']}): "
              f"{worker_data['total_tests']} tests across {worker_data['num_series']} series")
        for c in chunks:
            print(f"  {c['job_name']:25s} ({c['series_slug']:30s}): {len(c['tests'])} tests")

    with open("pro_chunks/all_chunks.json", "w") as f:
        json.dump({"workers": all_chunks, "total_tests": len(sorted_tests)}, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"Total: {len(sorted_tests)} tests split across {NUM_WORKERS} workers")
    print(f"Expected per-worker runtime: ~{(len(sorted_tests) / NUM_WORKERS) * 6 / 60:.1f} minutes "
          f"({(len(sorted_tests) / NUM_WORKERS) * 6 / 3600:.1f} hours) at 6s/test")
    print(f"All workers run in parallel — total wall time: ~{(len(sorted_tests) / NUM_WORKERS) * 6 / 3600:.1f} hours")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
