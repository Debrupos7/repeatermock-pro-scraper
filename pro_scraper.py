#!/usr/bin/env python3
"""RepeaterMock PRO Test Scraper — scrapes PRO tests using paid account cookies.

Reads a chunk of PRO test IDs, uses PRO_COOKIES (from GitHub secrets),
calls /auth/refresh to get fresh accessToken, then scrapes each test.

Auth flow:
1. Read PRO_COOKIES from environment (contains refreshToken + accessToken)
2. Call /auth/refresh with refreshToken → get new accessToken (15 min)
3. Use accessToken for /attempts/{tid}/start + /submit
4. When accessToken expires → call /auth/refresh again
5. If /auth/refresh fails 5 times → stop all workers
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import random
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional, Dict, Any

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

API_BASE = "https://api.repeatermock.com"
WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 5  # Stop after 5 auth failures


class PROAuthManager:
    """Manages PRO auth using refreshToken → accessToken refresh cycle."""

    def __init__(self, cookies: str = "", refresh_token: str = ""):
        self.cookie_str = cookies or os.environ.get("PRO_COOKIES", "")
        self.refresh_token = refresh_token or os.environ.get("PRO_REFRESH_TOKEN", "")
        self.access_token = ""
        self.access_token_expires_at = 0
        self.auth_failures = 0
        
        # Extract accessToken from cookie string
        if self.cookie_str:
            m = re.search(r'accessToken=([^;]+)', self.cookie_str)
            if m:
                self.access_token = m.group(1)
                self.access_token_expires_at = time.time() + 900  # Assume 15 min
                print(f"  [auth] loaded accessToken from PRO_COOKIES (len={len(self.access_token)})")
        
        # Extract refreshToken if not provided separately
        if not self.refresh_token and self.cookie_str:
            m = re.search(r'refreshToken=([^;]+)', self.cookie_str)
            if m:
                self.refresh_token = m.group(1)
                print(f"  [auth] loaded refreshToken from PRO_COOKIES (len={len(self.refresh_token)})")

    def is_access_token_valid(self) -> bool:
        return bool(self.access_token) and time.time() < (self.access_token_expires_at - 60)

    def refresh_access_token(self) -> bool:
        """Call /auth/refresh to get new accessToken."""
        if self.auth_failures >= MAX_AUTH_FAILURES:
            print(f"  [auth] ⛔ MAX_AUTH_FAILURES ({MAX_AUTH_FAILURES}) reached — STOPPING")
            return False
        
        if not self.refresh_token:
            print("  [auth] no refreshToken — can't refresh")
            return False
        
        print("  [auth] refreshing accessToken...")
        try:
            body = json.dumps({}).encode()
            req = urllib.request.Request(f"{API_BASE}/auth/refresh", data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Cookie", f"refreshToken={self.refresh_token}")
            req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            req.add_header("Origin", "https://repeatermock.com")
            req.add_header("Referer", "https://repeatermock.com/")
            
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            
            if data.get("success") and data.get("accessToken"):
                self.access_token = data["accessToken"]
                self.access_token_expires_at = time.time() + 900  # 15 min
                self.auth_failures = 0  # Reset on success
                print(f"  [auth] ✅ accessToken refreshed (valid 15 min)")
                return True
            else:
                self.auth_failures += 1
                print(f"  [auth] refresh failed: {data.get('message','?')} (failures: {self.auth_failures}/{MAX_AUTH_FAILURES})")
                return False
        except urllib.error.HTTPError as e:
            self.auth_failures += 1
            err = e.read().decode()[:100]
            print(f"  [auth] refresh error: HTTP {e.code} (failures: {self.auth_failures}/{MAX_AUTH_FAILURES})")
            return False
        except Exception as e:
            self.auth_failures += 1
            print(f"  [auth] refresh error: {e} (failures: {self.auth_failures}/{MAX_AUTH_FAILURES})")
            return False

    def get_access_token(self) -> Optional[str]:
        """Get valid accessToken. Refreshes if needed."""
        if self.is_access_token_valid():
            return self.access_token
        if self.refresh_access_token():
            return self.access_token
        # Refresh failed but we might still have a valid accessToken from cookies
        if self.access_token:
            print("  [auth] using existing accessToken (refresh failed but token might still work)")
            return self.access_token
        return None

    def get_cookie_header(self) -> str:
        """Get full cookie header for API requests."""
        parts = []
        if self.access_token:
            parts.append(f"accessToken={self.access_token}")
        if self.refresh_token:
            parts.append(f"refreshToken={self.refresh_token}")
        parts.append("totpVerified=1")
        return "; ".join(parts)

    def should_stop(self) -> bool:
        return self.auth_failures >= MAX_AUTH_FAILURES


async def scrape_pro_test(auth: PROAuthManager, test_info: dict, output_dir: str, worker_id: int) -> Optional[str]:
    """Scrape a single PRO test."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series_slug = test_info.get("series_slug", "")
    section = test_info.get("section", "")
    subsection = test_info.get("subsection", "")

    if auth.should_stop():
        return "STOP"

    access_token = auth.get_access_token()
    if not access_token:
        print(f"  [worker {worker_id}] ❌ no access token")
        return None

    cookie_header = auth.get_cookie_header()
    print(f"  [worker {worker_id}] starting: {title[:50]}... (id={tid})")

    # 1. Start attempt
    try:
        body = json.dumps({}).encode()
        req = urllib.request.Request(f"{API_BASE}/api/v1/attempts/{tid}/start", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", cookie_header)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        req.add_header("Origin", "https://repeatermock.com")
        with urllib.request.urlopen(req, timeout=30) as r:
            start_data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 402:
            print(f"  [worker {worker_id}] 💰 still PRO (402): {tid}")
            return "PRO"
        elif e.code == 401:
            print(f"  [worker {worker_id}] 🔑 401 — refreshing token...")
            auth.refresh_access_token()
            return None
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
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=30) as r:
            submit_data = json.loads(r.read().decode())
    except Exception as e:
        print(f"  [worker {worker_id}] submit error: {e}")
        return None

    # 3. Fetch solution page via Playwright (DOM extraction)
    from playwright.async_api import async_playwright
    solution_url = f"{WEB_BASE}/tb/test-series/{series_slug}/test/{tid}/solution"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        # Set auth cookies
        await context.add_cookies([
            {"name": "accessToken", "value": auth.access_token, "domain": ".repeatermock.com", "path": "/"},
            {"name": "refreshToken", "value": auth.refresh_token, "domain": ".repeatermock.com", "path": "/"},
            {"name": "totpVerified", "value": "1", "domain": ".repeatermock.com", "path": "/"},
        ])
        # Anti-self-destruct
        INIT_SCRIPT = r"""
        (function(){window.close=function(){};console.clear=function(){};window.stop=function(){};
        try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}
        try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}
        const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};
        window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);
        console.log=function(){};console.table=function(){};console.dir=function(){};console.debug=function(){};console.info=function(){};console.trace=function(){};})();
        """
        await context.add_init_script(INIT_SCRIPT)
        page = await context.new_page()

        await page.goto(solution_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)

        # Get HTML via DOM
        result = await page.evaluate("""
            (function(){
                var h = document.documentElement.outerHTML;
                window.__HTML__ = h;
                return {stored: true, len: h.length, hasTestData: h.indexOf('testData') >= 0, hasAnswersData: h.indexOf('answersData') >= 0};
            })()
        """)

        if not result or not result.get("hasTestData"):
            # Fallback: fetch
            result = await page.evaluate("""
                (function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{stored:true,len:t.length,hasTestData:t.indexOf('testData')>=0,hasAnswersData:t.indexOf('answersData')>=0}}).catch(e=>({stored:false,err:String(e).slice(0,200)}))})()
            """)

        # Extract in 200KB chunks
        total_len = await page.evaluate("(function(){return (window.__HTML__||'').length})()")
        chunks = []
        chunk_size = 200000
        num_chunks = min((total_len // chunk_size) + 1, 100)
        for i in range(num_chunks):
            start = i * chunk_size
            if start >= total_len: break
            chunk = await page.evaluate(f"(function(){{var h=window.__HTML__||'';if({start}>=h.length)return null;return h.slice({start},{start+chunk_size})}})()")
            if chunk is None: break
            chunks.append(chunk)
        html = "".join(chunks)
        await browser.close()

    if not html or "testData" not in html or "answersData" not in html:
        print(f"  [worker {worker_id}] HTML missing testData (len={len(html)})")
        return None

    # Save
    safe_title = re.sub(r'[^a-zA-Z0-9\-_]+', '_', title)[:80] or "test"
    ai_dir = os.path.join(output_dir, "ai_export",
                          re.sub(r'[^a-zA-Z0-9\-_]+', '_', section or "Uncategorized"),
                          re.sub(r'[^a-zA-Z0-9\-_]+', '_', subsection or "Default"))
    os.makedirs(ai_dir, exist_ok=True)
    ai_path = os.path.join(ai_dir, f"{safe_title}_{tid}.json")

    test_data = {
        "test_id": tid, "title": title, "series_slug": series_slug,
        "section": section, "subsection": subsection,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "html_length": len(html), "raw_html": html,
    }
    tmp = ai_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(test_data, f, ensure_ascii=False)
    os.rename(tmp, ai_path)

    print(f"  [worker {worker_id}] ✅ saved: {title[:50]}... ({len(html):,}B)")
    return "OK"


async def run_scraper(chunk_file: str, output_dir: str, workers: int = 4):
    with open(chunk_file) as f:
        chunk_data = json.load(f)
    tests = chunk_data.get("tests", [])
    job_number = chunk_data.get("job_number", 1)
    print(f"\n{'='*60}")
    print(f"PRO Scraper — Job {job_number}")
    print(f"Tests: {len(tests)} | Output: {output_dir}")
    print(f"{'='*60}\n")

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuthManager()

    # Verify auth works
    token = auth.get_access_token()
    if not token:
        print("❌ No valid access token — stopping")
        return

    completed = failed = pro_still = 0
    queue = asyncio.Queue()
    for t in tests:
        await queue.put(t)

    async def worker_loop(wid: int):
        nonlocal completed, failed, pro_still
        while not queue.empty():
            if auth.should_stop():
                print(f"  [worker {wid}] ⛔ stopping (auth failures)")
                break
            try:
                test_info = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            result = await scrape_pro_test(auth, test_info, output_dir, wid)
            if result == "STOP": break
            elif result == "PRO": pro_still += 1
            elif result == "OK": completed += 1
            else: failed += 1
            await asyncio.sleep(2.0 + random.uniform(0, 1.0))

    worker_tasks = [asyncio.create_task(worker_loop(i+1)) for i in range(min(workers, 4))]
    await asyncio.gather(*worker_tasks)

    progress = {"job_number": job_number, "total_tests": len(tests),
                "scraped": completed, "failed": failed, "still_pro": pro_still,
                "completed_at": datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir, "pro_progress.json"), "w") as f:
        json.dump(progress, f, indent=2)
    print(f"\n{'='*60}")
    print(f"✅ Job {job_number}: {completed} scraped, {failed} failed, {pro_still} still PRO")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    asyncio.run(run_scraper(args.chunk, args.output_dir, args.workers))

if __name__ == "__main__":
    main()
