#!/usr/bin/env python3
"""Distribute PRO_TESTS.json equally across 20 jobs.

Reads scraped_output/PRO_TESTS.json from the free scraper repo,
splits into 20 equal chunks, and writes each chunk as a separate JSON file.

For testing: limits to 200 tests (10 per job).
For production: uses all PRO tests.
"""
import json
import os
import sys
import urllib.request

REPO = "sujitbhai7710/repeatermock-mass-scraper"
NUM_JOBS = 20
MAX_TESTS_FOR_TESTING = 0  # Set to 0 for production (all tests)

def fetch_pro_tests():
    """Fetch PRO_TESTS.json from GitHub."""
    url = f"https://raw.githubusercontent.com/{REPO}/main/scraped_output/PRO_TESTS.json"
    print(f"Fetching PRO_TESTS.json from {url}...")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data.get("pro_tests", [])

def distribute_tests(tests, num_jobs):
    """Split tests into num_jobs equal chunks."""
    if MAX_TESTS_FOR_TESTING > 0:
        tests = tests[:MAX_TESTS_FOR_TESTING]
        print(f"TEST MODE: limiting to {len(tests)} tests ({MAX_TESTS_FOR_TESTING} max)")

    chunk_size = len(tests) // num_jobs
    remainder = len(tests) % num_jobs

    chunks = []
    start = 0
    for i in range(num_jobs):
        # Distribute remainder across first N chunks
        size = chunk_size + (1 if i < remainder else 0)
        chunks.append(tests[start:start + size])
        start += size

    print(f"Distributed {len(tests)} tests across {num_jobs} jobs:")
    for i, chunk in enumerate(chunks):
        print(f"  Job {i+1:02d}: {len(chunk)} tests")
    return chunks

def save_chunks(chunks, output_dir="pro_chunks"):
    """Save each chunk as a separate JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    for i, chunk in enumerate(chunks):
        path = os.path.join(output_dir, f"chunk_{i+1:02d}.json")
        with open(path, "w") as f:
            json.dump({
                "job_number": i + 1,
                "total_tests": len(chunk),
                "tests": chunk,
            }, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {path} ({len(chunk)} tests)")
    return output_dir

def main():
    tests = fetch_pro_tests()
    print(f"Total PRO tests: {len(tests)}")

    chunks = distribute_tests(tests, NUM_JOBS)
    output_dir = save_chunks(chunks)

    # Also save a summary
    summary = {
        "total_pro_tests": len(tests),
        "num_jobs": NUM_JOBS,
        "tests_per_job": len(tests) // NUM_JOBS,
        "test_mode": MAX_TESTS_FOR_TESTING > 0,
        "max_tests": MAX_TESTS_FOR_TESTING if MAX_TESTS_FOR_TESTING > 0 else "unlimited",
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
    }
    with open(os.path.join(output_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {output_dir}/summary.json")
    print(f"✅ Distribution complete: {len(tests)} tests → {NUM_JOBS} jobs")

if __name__ == "__main__":
    main()
