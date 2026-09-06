#!/usr/bin/env python3
"""RepeaterMock PRO Scraper — API calls + solution page + same format as free scraper.

Flow per test:
1. POST /api/v1/attempts/{tid}/start (creates attempt — required for solution page)
2. POST /api/v1/attempts/{tid}/submit (submits empty answers — unlocks solution)
3. Navigate to solution page with cookies → get HTML with testData
4. Parse flight data → render AI export + HTML export (same as free scraper)

Rate limiting:
- 1 worker per job
- 5s delay between tests
- 429 retry: 10s, 20s, 30s backoff
- 5 × 429 = stop that worker
- max-parallel: 5 in workflow (5 concurrent jobs = 5 concurrent API calls)
"""
import argparse, asyncio, json, os, re, sys, time, random, urllib.request, urllib.error
from datetime import datetime, timezone
from typing import Optional

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

sys.path.insert(0, os.path.dirname(__file__))
from free_scraper_module import (
    parse_test_data, render_ai_export, render_test_html,
    build_ai_export_path, build_html_output_path,
    find_props_in_flight, build_text_refs, TestRef
)

API_BASE = "https://api.repeatermock.com"
WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 3
WORKER_429_LIMIT = 5

INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""


class PROAuth:
    def __init__(self):
        cookies = os.environ.get("PRO_COOKIES", "")
        self.access_token = ""
        self.refresh_token = os.environ.get("PRO_REFRESH_TOKEN", "")
        self.failures = 0
        if cookies:
            m = re.search(r'accessToken=([^;]+)', cookies)
            if m: self.access_token = m.group(1)
            m = re.search(r'refreshToken=([^;]+)', cookies)
            if m: self.refresh_token = m.group(1)
        print(f"  [auth] accessToken: {'✅' if self.access_token else '❌'} | refreshToken: {'✅' if self.refresh_token else '❌'}")

    def cookie_header(self):
        return f"accessToken={self.access_token}; refreshToken={self.refresh_token}; totpVerified=1"

    def stop(self):
        return self.failures >= MAX_AUTH_FAILURES


def api_post(url, cookie_header, body="{}", max_retries=3):
    """POST API call with 429 retry."""
    for attempt in range(max_retries):
        req = urllib.request.Request(url, data=body.encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", cookie_header)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        req.add_header("Origin", "https://repeatermock.com")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 10 * (attempt + 1) + random.uniform(1, 5)
                print(f"    429 — waiting {wait:.0f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            try: err = json.loads(e.read().decode())
            except: err = {"error": str(e)}
            return e.code, err
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return 0, {"error": str(e)}
    return 429, {"error": "max retries"}


async def scrape_test(browser, auth, test_info, output_dir, wid, w429):
    """Scrape one PRO test: API start/submit + solution page."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series = test_info.get("series_slug", "")
    section = test_info.get("section", "Uncategorized")
    subsection = test_info.get("subsection", "Default")

    if auth.stop() or w429[0] >= WORKER_429_LIMIT:
        return "STOP"

    ch = auth.cookie_header()
    print(f"  [w{wid}] {title[:45]}... (id={tid[:12]})")

    # 1. Start attempt
    status, data = api_post(f"{API_BASE}/api/v1/attempts/{tid}/start", ch)
    if status == 429:
        w429[0] += 1
        print(f"  [w{wid}] 429 ({w429[0]}/{WORKER_429_LIMIT})")
        return None
    if status == 402:
        print(f"  [w{wid}] 💰 still PRO")
        return "PRO"
    if status == 401:
        auth.failures += 1
        print(f"  [w{wid}] 401 expired ({auth.failures}/{MAX_AUTH_FAILURES})")
        return None
    if status != 200:
        print(f"  [w{wid}] start failed: {status}")
        return None

    # 2. Submit
    status, _ = api_post(f"{API_BASE}/api/v1/attempts/{tid}/submit", ch,
                        '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if status != 200:
        print(f"  [w{wid}] submit failed: {status}")
        return None

    # 3. Solution page
    test_ref = TestRef(
        test_id=tid, title=title, series_slug=series, series_name=test_info.get("series_name", series),
        section_id="", section_name=section, sub_section_id="", sub_section_name=subsection,
        is_free=False, duration=0, question_count=0, total_mark=0,
    )

    from playwright.async_api import async_playwright
    # We reuse the browser passed in
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        viewport={"width": 1366, "height": 900},
    )
    await context.add_cookies([
        {"name": "accessToken", "value": auth.access_token, "domain": ".repeatermock.com", "path": "/"},
        {"name": "refreshToken", "value": auth.refresh_token, "domain": ".repeatermock.com", "path": "/"},
        {"name": "totpVerified", "value": "1", "domain": ".repeatermock.com", "path": "/"},
    ])
    await context.add_init_script(INIT_SCRIPT)
    page = await context.new_page()

    try:
        await page.goto(f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)

        if "login" in page.url or "about:blank" in page.url:
            auth.failures += 1
            print(f"  [w{wid}] ❌ redirected to login")
            return None

        # Get HTML via DOM
        result = await page.evaluate("(function(){var h=document.documentElement.outerHTML;window.__HTML__=h;return{len:h.length,hasTD:h.indexOf('testData')>=0,hasAD:h.indexOf('answersData')>=0}})()")
        if not result or not result.get("hasTD"):
            result = await page.evaluate("(function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{len:t.length,hasTD:t.indexOf('testData')>=0,hasAD:t.indexOf('answersData')>=0}}).catch(e=>({err:String(e).slice(0,100)}))})()")

        total = await page.evaluate("(function(){return(window.__HTML__||'').length})()")
        chunks = []
        cs = 200000
        nc = min((total // cs) + 1, 100)
        for i in range(nc):
            s = i * cs
            if s >= total: break
            c = await page.evaluate(f"(function(){{var h=window.__HTML__||'';if({s}>=h.length)return null;return h.slice({s},{s+cs})}})()")
            if c is None: break
            chunks.append(c)
        html = "".join(chunks)

        if not html or "testData" not in html or "answersData" not in html:
            print(f"  [w{wid}] ❌ no testData (len={len(html)})")
            return None

        # Parse + render (same as free scraper)
        props = find_props_in_flight(html)
        if not props:
            print(f"  [w{wid}] ❌ no flight props")
            return None
        text_refs = build_text_refs(html)
        test_data = parse_test_data(props, tid, series, text_refs)
        if test_data.title: test_ref.title = test_data.title

        ai_path = build_ai_export_path(output_dir, test_ref)
        ai_export = render_ai_export(test_data, test_ref)
        tmp = ai_path + ".tmp"
        with open(tmp, "w") as f: json.dump(ai_export, f, ensure_ascii=False, indent=2)
        os.rename(tmp, ai_path)

        html_path = build_html_output_path(output_dir, test_ref)
        rendered = render_test_html(test_data)
        tmp2 = html_path + ".tmp"
        with open(tmp2, "w") as f: f.write(rendered)
        os.rename(tmp2, html_path)

        print(f"  [w{wid}] ✅ saved HTML ({len(rendered):,}B) + AI ({len(json.dumps(ai_export)):,}B): {title[:45]}")
        return "OK"
    except Exception as e:
        print(f"  [w{wid}] ❌ {e}")
        return None
    finally:
        await context.close()


async def run_scraper(chunk_file, output_dir, workers=1):
    with open(chunk_file) as f:
        chunk = json.load(f)
    tests = chunk.get("tests", [])
    jn = chunk.get("job_number", 1)
    print(f"\n{'='*60}\nPRO Scraper Job {jn} | Tests: {len(tests)} | Workers: {workers}\n{'='*60}\n")

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth()

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        done = fail = 0
        q = asyncio.Queue()
        for t in tests: await q.put(t)

        async def loop(wid):
            nonlocal done, fail
            w429 = [0]
            while not q.empty():
                if auth.stop():
                    print(f"  [w{wid}] ⛔ stop")
                    break
                if w429[0] >= WORKER_429_LIMIT:
                    print(f"  [w{wid}] ⛔ 429 limit")
                    break
                try: ti = q.get_nowait()
                except: break
                r = await scrape_test(browser, auth, ti, output_dir, wid, w429)
                if r == "STOP": break
                elif r == "OK": done += 1
                else: fail += 1
                await asyncio.sleep(5.0 + random.uniform(0, 2.0))

        await asyncio.gather(*[asyncio.create_task(loop(i+1)) for i in range(min(workers, 1))])
        await browser.close()

    prog = {"job": jn, "total": len(tests), "scraped": done, "failed": fail, "at": datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir, "pro_progress.json"), "w") as f: json.dump(prog, f, indent=2)
    print(f"\n{'='*60}\n✅ Job {jn}: {done} scraped, {fail} failed\n{'='*60}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chunk", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--workers", type=int, default=1)
    a = p.parse_args()
    asyncio.run(run_scraper(a.chunk, a.output_dir, a.workers))

if __name__ == "__main__":
    main()
