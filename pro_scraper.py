#!/usr/bin/env python3
"""RepeaterMock PRO Test Scraper — uses provided cookies + 429 retry logic."""
import argparse, asyncio, json, os, re, sys, time, random, urllib.request, urllib.error
from datetime import datetime, timezone
from typing import Optional

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

API_BASE = "https://api.repeatermock.com"
WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 5
WORKER_429_LIMIT = 5


class PROAuthManager:
    def __init__(self):
        self.cookie_str = os.environ.get("PRO_COOKIES", "")
        self.access_token = ""
        self.refresh_token = os.environ.get("PRO_REFRESH_TOKEN", "")
        self.auth_failures = 0
        
        if self.cookie_str:
            m = re.search(r'accessToken=([^;]+)', self.cookie_str)
            if m: self.access_token = m.group(1)
            m = re.search(r'refreshToken=([^;]+)', self.cookie_str)
            if m: self.refresh_token = m.group(1)
        
        print(f"  [auth] accessToken: {'✅' if self.access_token else '❌'} (len={len(self.access_token)})")
        print(f"  [auth] refreshToken: {'✅' if self.refresh_token else '❌'} (len={len(self.refresh_token)})")

    def get_cookie_header(self):
        parts = []
        if self.access_token: parts.append(f"accessToken={self.access_token}")
        if self.refresh_token: parts.append(f"refreshToken={self.refresh_token}")
        parts.append("totpVerified=1")
        return "; ".join(parts)

    def should_stop(self):
        return self.auth_failures >= MAX_AUTH_FAILURES


def api_call(url, method="POST", cookie_header="", body_data=None, max_retries=3):
    """Make API call with 429 retry + exponential backoff."""
    for attempt in range(max_retries):
        req = urllib.request.Request(url, data=body_data, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", cookie_header)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        req.add_header("Origin", "https://repeatermock.com")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = (2 ** attempt) + random.uniform(1, 3)
                print(f"    429 rate limited — waiting {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            try:
                err_body = json.loads(e.read().decode())
            except:
                err_body = {"error": str(e)}
            return e.code, err_body
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return 0, {"error": str(e)}
    return 429, {"error": "max retries exceeded"}


async def scrape_pro_test(auth, test_info, output_dir, worker_id, worker_429_count):
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series_slug = test_info.get("series_slug", "")
    section = test_info.get("section", "")
    subsection = test_info.get("subsection", "")

    if auth.should_stop():
        return "STOP"

    if worker_429_count[0] >= WORKER_429_LIMIT:
        return "STOP_429"

    cookie_header = auth.get_cookie_header()
    print(f"  [worker {worker_id}] starting: {title[:50]}... (id={tid})")

    # 1. Start attempt
    body = json.dumps({}).encode()
    status, data = api_call(f"{API_BASE}/api/v1/attempts/{tid}/start", "POST", cookie_header, body)
    
    if status == 429:
        worker_429_count[0] += 1
        print(f"  [worker {worker_id}] 429 (count={worker_429_count[0]}/{WORKER_429_LIMIT})")
        return None
    if status == 402:
        print(f"  [worker {worker_id}] 💰 still PRO: {tid}")
        return "PRO"
    if status == 401:
        auth.auth_failures += 1
        print(f"  [worker {worker_id}] 401 — auth failure ({auth.auth_failures}/{MAX_AUTH_FAILURES})")
        return None
    if status != 200:
        print(f"  [worker {worker_id}] start failed: {status}")
        return None

    # 2. Submit
    submit_body = json.dumps({"answers": [], "timeTaken": 1, "language": "en", "interface": "classic"}).encode()
    status, data = api_call(f"{API_BASE}/api/v1/attempts/{tid}/submit", "POST", cookie_header, submit_body)
    if status != 200:
        print(f"  [worker {worker_id}] submit failed: {status}")
        return None

    # 3. Fetch solution page via Playwright
    from playwright.async_api import async_playwright
    solution_url = f"{WEB_BASE}/tb/test-series/{series_slug}/test/{tid}/solution"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        await context.add_cookies([
            {"name": "accessToken", "value": auth.access_token, "domain": ".repeatermock.com", "path": "/"},
            {"name": "refreshToken", "value": auth.refresh_token, "domain": ".repeatermock.com", "path": "/"},
            {"name": "totpVerified", "value": "1", "domain": ".repeatermock.com", "path": "/"},
        ])
        INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""
        await context.add_init_script(INIT_SCRIPT)
        page = await context.new_page()

        await page.goto(solution_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)

        result = await page.evaluate("(function(){var h=document.documentElement.outerHTML;window.__HTML__=h;return{stored:true,len:h.length,hasTestData:h.indexOf('testData')>=0,hasAnswersData:h.indexOf('answersData')>=0}})()")
        if not result or not result.get("hasTestData"):
            result = await page.evaluate("(function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{stored:true,len:t.length,hasTestData:t.indexOf('testData')>=0,hasAnswersData:t.indexOf('answersData')>=0}}).catch(e=>({stored:false,err:String(e).slice(0,200)}))})()")

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

    safe_title = re.sub(r'[^a-zA-Z0-9\-_]+', '_', title)[:80] or "test"
    ai_dir = os.path.join(output_dir, "ai_export", re.sub(r'[^a-zA-Z0-9\-_]+', '_', section or "Uncategorized"), re.sub(r'[^a-zA-Z0-9\-_]+', '_', subsection or "Default"))
    os.makedirs(ai_dir, exist_ok=True)
    ai_path = os.path.join(ai_dir, f"{safe_title}_{tid}.json")
    test_data = {"test_id": tid, "title": title, "series_slug": series_slug, "section": section, "subsection": subsection, "scraped_at": datetime.now(timezone.utc).isoformat(), "html_length": len(html), "raw_html": html}
    tmp = ai_path + ".tmp"
    with open(tmp, "w") as f: json.dump(test_data, f, ensure_ascii=False)
    os.rename(tmp, ai_path)
    print(f"  [worker {worker_id}] ✅ saved: {title[:50]}... ({len(html):,}B)")
    return "OK"


async def run_scraper(chunk_file, output_dir, workers=2):
    with open(chunk_file) as f:
        chunk_data = json.load(f)
    tests = chunk_data.get("tests", [])
    job_number = chunk_data.get("job_number", 1)
    print(f"\n{'='*60}\nPRO Scraper — Job {job_number}\nTests: {len(tests)} | Workers: {workers}\n{'='*60}\n")

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuthManager()

    completed = failed = pro_still = 0
    queue = asyncio.Queue()
    for t in tests: await queue.put(t)

    async def worker_loop(wid):
        nonlocal completed, failed, pro_still
        worker_429_count = [0]  # Mutable counter for this worker
        while not queue.empty():
            if auth.should_stop():
                print(f"  [worker {wid}] ⛔ auth stop")
                break
            if worker_429_count[0] >= WORKER_429_LIMIT:
                print(f"  [worker {wid}] ⛔ 429 limit ({worker_429_count[0]})")
                break
            try: test_info = queue.get_nowait()
            except asyncio.QueueEmpty: break
            result = await scrape_pro_test(auth, test_info, output_dir, wid, worker_429_count)
            if result == "STOP" or result == "STOP_429": break
            elif result == "PRO": pro_still += 1
            elif result == "OK": completed += 1
            else: failed += 1
            await asyncio.sleep(3.0 + random.uniform(0, 2.0))  # 3-5s delay between tests

    worker_tasks = [asyncio.create_task(worker_loop(i+1)) for i in range(min(workers, 2))]
    await asyncio.gather(*worker_tasks)

    progress = {"job_number": job_number, "total_tests": len(tests), "scraped": completed, "failed": failed, "still_pro": pro_still, "completed_at": datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir, "pro_progress.json"), "w") as f: json.dump(progress, f, indent=2)
    print(f"\n{'='*60}\n✅ Job {job_number}: {completed} scraped, {failed} failed, {pro_still} still PRO\n{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    asyncio.run(run_scraper(args.chunk, args.output_dir, args.workers))

if __name__ == "__main__":
    main()
