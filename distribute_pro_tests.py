#!/usr/bin/env python3
"""Distribute PRO_TESTS.json — SSC first, then RRB. Same series folder structure as free scraper."""
import json, os, sys, urllib.request

REPO = "sujitbhai7710/repeatermock-mass-scraper"
MAX_TESTS = int(os.environ.get("MAX_TESTS", "0"))  # 0 = unlimited

def fetch_pro_tests():
    url = f"https://raw.githubusercontent.com/{REPO}/main/scraped_output/PRO_TESTS.json"
    print(f"Fetching PRO_TESTS.json...")
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data.get("pro_tests", [])

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

SSC_FIRST = [k for k in SERIES_TO_JOB if k.startswith("ssc")]
RRB_AFTER = [k for k in SERIES_TO_JOB if k.startswith("rrb")]

def main():
    tests = fetch_pro_tests()
    print(f"Total PRO tests: {len(tests)}")
    
    if MAX_TESTS > 0:
        tests = tests[:MAX_TESTS]
        print(f"TEST MODE: {len(tests)} tests")
    
    # Sort: SSC first, then RRB
    ssc_tests = [t for t in tests if t.get("series_slug","").startswith("ssc")]
    rrb_tests = [t for t in tests if t.get("series_slug","").startswith("rrb")]
    other_tests = [t for t in tests if not t.get("series_slug","").startswith("ssc") and not t.get("series_slug","").startswith("rrb")]
    
    sorted_tests = ssc_tests + rrb_tests + other_tests
    print(f"Sorted: {len(ssc_tests)} SSC + {len(rrb_tests)} RRB + {len(other_tests)} other = {len(sorted_tests)} total")
    
    # Group by series_slug (same as free scraper folder structure)
    by_series = {}
    for t in sorted_tests:
        slug = t.get("series_slug", "unknown")
        if slug not in by_series:
            by_series[slug] = []
        by_series[slug].append(t)
    
    # Write one chunk file per series (same folder structure as free scraper)
    os.makedirs("pro_chunks", exist_ok=True)
    chunks = []
    for slug in list(SSC_FIRST) + list(RRB_AFTER):
        if slug in by_series:
            job_name = SERIES_TO_JOB.get(slug, slug)
            chunk = {"job_name": job_name, "series_slug": slug, "tests": by_series[slug]}
            chunks.append(chunk)
    
    # Write chunks as a single file (scraper processes them sequentially)
    with open("pro_chunks/all_chunks.json", "w") as f:
        json.dump({"chunks": chunks, "total_tests": len(sorted_tests)}, f, ensure_ascii=False, indent=2)
    
    # Also write individual chunk files
    for i, chunk in enumerate(chunks):
        path = f"pro_chunks/chunk_{i+1:02d}_{chunk['series_slug']}.json"
        with open(path, "w") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)
    
    print(f"\nCreated {len(chunks)} chunks (one per series):")
    for c in chunks:
        print(f"  {c['job_name']:25s} ({c['series_slug']:30s}): {len(c['tests'])} tests")
    print(f"\nTotal: {len(sorted_tests)} tests across {len(chunks)} series")

if __name__ == "__main__":
    main()
