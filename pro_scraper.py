#!/usr/bin/env python3
"""RepeaterMock PRO Test Scraper — scrapes PRO tests using paid account auth.

Reads a chunk of PRO test IDs, logs in via pro_auth.py, then scrapes each test
using the same logic as the free scraper (DOM-based HTML extraction + image download).

Usage:
    python3 pro_scraper.py --chunk pro_chunks/chunk_01.json --output-dir pro_scraped_output/PRO-Job-01
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import random
import hashlib
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

API_BASE = "https://api.repeatermock.com"
WEB_BASE = "https://repeatermock.com"

# Import auth manager
sys.path.insert(0, os.path.dirname(__file__))
from pro_auth import PROAuthManager


async def scrape_pro_test(auth: PROAuthManager, test_info: dict, output_dir: str, worker_id: int) -> Optional[dict]:
    """Scrape a single PRO test using auth tokens."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series_slug = test_info.get("series_slug", "")
    section = test_info.get("section", "")
    subsection = test_info.get("subsection", "")

    # Get valid access token
    access_token = await auth.get_valid_access_token()
    if not access_token:
        print(f"  [worker {worker_id}] ❌ no access token — stopping")
        return None

    if auth.should_stop():
        print(f"  [worker {worker_id}] ⛔ auth stop signal — skipping all remaining")
        return "STOP"

    # 1. Start attempt
    cookie_header = auth.get_cookie_header()
    print(f"  [worker {worker_id}] starting: {title[:50]}... (id={tid})")
    try:
        body = json.dumps({}).encode()
        req = urllib.request.Request(f"{API_BASE}/api/v1/attempts/{tid}/start", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", cookie_header)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        with urllib.request.urlopen(req, timeout=30) as r:
            start_data = json.loads(r.read().decode())
        if r.status != 200:
            print(f"  [worker {worker_id}] start failed: status={r.status}")
            return None
    except urllib.error.HTTPError as e:
        if e.code == 402:
            print(f"  [worker {worker_id}] 💰 still PRO (402) — skipping: {tid}")
            return "PRO"
        print(f"  [worker {worker_id}] start failed: HTTP {e.code}")
        return None
    except Exception as e:
        print(f"  [worker {worker_id}] start error: {e}")
        return None

    # 2. Submit empty answers
    try:
        body = json.dumps({"answers": [], "timeTaken": 1, "language": "en", "interface": "classic"}).encode()
        req = urllib.request.Request(f"{API_BASE}/api/v1/attempts/{tid}/submit", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", cookie_header)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        with urllib.request.urlopen(req, timeout=30) as r:
            submit_data = json.loads(r.read().decode())
        if r.status != 200:
            print(f"  [worker {worker_id}] submit failed: status={r.status}")
            return None
    except Exception as e:
        print(f"  [worker {worker_id}] submit error: {e}")
        return None

    # 3. Fetch solution page using Playwright (DOM-based extraction)
    from playwright.async_api import async_playwright
    solution_url = f"{WEB_BASE}/tb/test-series/{series_slug}/test/{tid}/solution"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        # Set auth cookies
        await context.add_cookies([
            {"name": "accessToken", "value": auth.access_token, "domain": ".repeatermock.com", "path": "/"},
            {"name": "refreshToken", "value": auth.refresh_token, "domain": ".repeatermock.com", "path": "/"},
        ])
        # Anti-self-destruct init script
        from pro_auth import INIT_SCRIPT
        await context.add_init_script(INIT_SCRIPT)
        page = await context.new_page()

        # Navigate to solution page
        await page.goto(solution_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)

        # Get HTML via DOM (not fetch!) — same fix as free scraper
        js_fetch = """
        (function(){
            var domHTML = document.documentElement.outerHTML;
            window.__HTML__ = domHTML;
            return {stored: true, len: domHTML.length, hasTestData: domHTML.indexOf('testData') >= 0, hasAnswersData: domHTML.indexOf('answersData') >= 0, source: 'dom'};
        })()
        """
        result = await page.evaluate(js_fetch)
        if not result or not result.get("hasTestData"):
            # Fallback: fetch()
            result = await page.evaluate("""
                (function(){
                    return fetch(window.location.href, {credentials: 'include'})
                        .then(r => r.text())
                        .then(t => { window.__HTML__ = t; return {stored: true, len: t.length, hasTestData: t.indexOf('testData') >= 0, hasAnswersData: t.indexOf('answersData') >= 0, source: 'fetch'}; })
                        .catch(e => ({stored: false, err: String(e).slice(0, 200)}));
                })()
            """)

        if not result or not result.get("stored"):
            print(f"  [worker {worker_id}] failed to fetch solution HTML: {result}")
            await browser.close()
            return None

        # Extract HTML in chunks (200KB — Playwright's safe limit)
        total_len = await page.evaluate("(function(){ return (window.__HTML__ || '').length; })()")
        chunks = []
        chunk_size = 200000
        num_chunks = min((total_len // chunk_size) + 1, 100)
        for i in range(num_chunks):
            start = i * chunk_size
            if start >= total_len:
                break
            chunk = await page.evaluate(f"(function(){{var h = window.__HTML__ || ''; if ({start} >= h.length) return null; return h.slice({start}, {start + chunk_size});}})()")
            if chunk is None:
                break
            chunks.append(chunk)
        html = "".join(chunks)
        await browser.close()

    if not html or "testData" not in html or "answersData" not in html:
        print(f"  [worker {worker_id}] HTML missing testData/answersData (len={len(html)})")
        return None

    # 4. Parse flight data (reuse free scraper's parsing)
    # For now, save the raw HTML + metadata
    # The full parsing will be done by the database_maker_script later

    # Save to output dir
    safe_title = re.sub(r'[^a-zA-Z0-9\-_]+', '_', title)[:80] or "test"
    ai_dir = os.path.join(output_dir, "ai_export", re.sub(r'[^a-zA-Z0-9\-_]+', '_', section or "Uncategorized"), re.sub(r'[^a-zA-Z0-9\-_]+', '_', subsection or "Default"))
    os.makedirs(ai_dir, exist_ok=True)
    ai_path = os.path.join(ai_dir, f"{safe_title}_{tid}.json")

    # Save test data
    test_data = {
        "test_id": tid,
        "title": title,
        "series_slug": series_slug,
        "section": section,
        "subsection": subsection,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "html_length": len(html),
        "raw_html": html,  # Store raw HTML for later parsing
    }
    with open(ai_path, "w") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)

    print(f"  [worker {worker_id}] ✅ saved: {title[:50]}... ({len(html):,}B)")
    return test_data


async def run_scraper(chunk_file: str, output_dir: str, workers: int = 4):
    """Run the PRO scraper for a chunk of tests."""
    # Load chunk
    with open(chunk_file) as f:
        chunk_data = json.load(f)
    tests = chunk_data.get("tests", [])
    job_number = chunk_data.get("job_number", 1)
    print(f"\n{'='*60}")
    print(f"PRO Scraper — Job {job_number}")
    print(f"Tests to scrape: {len(tests)}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")

    os.makedirs(output_dir, exist_ok=True)

    # Initialize auth manager
    email = os.environ.get("RM_EMAIL", "")
    password = os.environ.get("RM_PASSWORD", "")
    scrapfly_key = os.environ.get("SCRAPFLY_API_KEY", "")
    scrapingbee_key = os.environ.get("SCRAPINGBEE_API_KEY", "")

    if not email or not password:
        print("❌ RM_EMAIL and RM_PASSWORD must be set")
        return

    auth = PROAuthManager(email, password, scrapfly_key, scrapingbee_key)

    # Try to get valid access token
    access_token = await auth.get_valid_access_token()
    if not access_token:
        print("❌ Failed to get access token — login failed")
        return

    # Scrape tests
    completed = 0
    failed = 0
    pro_still = 0

    queue = asyncio.Queue()
    for t in tests:
        await queue.put(t)

    async def worker_loop(worker_id: int):
        nonlocal completed, failed, pro_still
        while not queue.empty():
            if auth.should_stop():
                print(f"  [worker {worker_id}] ⛔ stopping (auth failure limit)")
                break
            try:
                test_info = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            result = await scrape_pro_test(auth, test_info, output_dir, worker_id)
            if result == "STOP":
                break
            elif result == "PRO":
                pro_still += 1
            elif result:
                completed += 1
            else:
                failed += 1
            await asyncio.sleep(2.0 + random.uniform(0, 1.0))

    # Spawn workers
    worker_tasks = [asyncio.create_task(worker_loop(i + 1)) for i in range(min(workers, 4))]
    await asyncio.gather(*worker_tasks)

    # Save progress
    progress = {
        "job_number": job_number,
        "total_tests": len(tests),
        "scraped": completed,
        "failed": failed,
        "still_pro": pro_still,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    progress_path = os.path.join(output_dir, "pro_progress.json")
    with open(progress_path, "w") as f:
        json.dump(progress, f, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ Job {job_number} complete!")
    print(f"   Scraped: {completed}")
    print(f"   Failed: {failed}")
    print(f"   Still PRO: {pro_still}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="RepeaterMock PRO Test Scraper")
    parser.add_argument("--chunk", required=True, help="Path to chunk JSON file")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    args = parser.parse_args()
    asyncio.run(run_scraper(args.chunk, args.output_dir, args.workers))


if __name__ == "__main__":
    main()
