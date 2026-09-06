#!/usr/bin/env python3
"""RepeaterMock PRO Scraper v2 — same folder structure as free scraper + ZenRows auto-login.

Folder structure (EXACT same as free scraper):
  pro_scraped_output/{JOB_NAME}/
    ├── progress.json          # Resume tracking
    ├── ai_export/             # AI JSON (same format as free)
    │   └── {section}/{subsection}/{title}_{test_id}.json
    └── html_export/          # Interactive HTML
        └── {section}/{subsection}/{title}_{test_id}.html

Flow:
  1. ZenRows login → accessToken + refreshToken
  2. For each series (SSC first, then RRB):
     a. For each test in series:
        - Skip if already scraped (progress.json)
        - Browser fetch /attempts/start + /attempts/submit
        - Navigate to /solution → extract HTML → parse → save
        - Wait 5s
     b. Commit results after each series
  3. Auto-refresh token every 14 min (ZenRows re-login if needed)
  4. Auto-trigger next run when done (or 5.5h limit reached)
"""
import argparse, asyncio, json, os, re, sys, time, random, urllib.request, urllib.error
from datetime import datetime, timezone

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from free_scraper_module import (
    parse_test_data, render_ai_export, render_test_html,
    build_ai_export_path, build_html_output_path,
    find_props_in_flight, build_text_refs, TestRef
)

API_BASE = "https://api.repeatermock.com"
WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 3

INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""


class PROAuth:
    def __init__(self):
        self.access_token = ""
        self.refresh_token = ""
        self.token_expires = 0
        self.failures = 0
        self.email = os.environ.get("RM_EMAIL", "")
        self.password = os.environ.get("RM_PASSWORD", "")
        self.zenrows_key = os.environ.get("ZENROWS_API_KEY", "")

    def is_valid(self):
        return bool(self.access_token) and time.time() < (self.token_expires - 60)

    def login_zenrows(self):
        if not self.zenrows_key: return False
        print("  [auth] ZenRows login...", flush=True)
        import httpx
        for attempt in range(3):
            print(f"  [auth] attempt {attempt+1}/3...", flush=True)
            try:
                with httpx.Client(timeout=180.0) as cli:
                    r = cli.get("https://api.zenrows.com/v1/", params={
                        "apikey": self.zenrows_key, "url": "https://repeatermock.com/login",
                        "js_render": "true", "premium_proxy": "true", "wait": "25000",
                    }, timeout=180.0)
                    content = r.text
                    print(f"  [auth] ZenRows: {r.status_code}, {len(content):,}B", flush=True)
                    for pat in [r'name="cf-turnstile-response"[^>]*value="([^"]+)"']:
                        m = re.search(pat, content)
                        if m and len(m.group(1)) > 20:
                            token = m.group(1)
                            print(f"  [auth] ✅ Turnstile! len={len(token)}", flush=True)
                            r2 = cli.post("https://api.repeatermock.com/auth/login", json={
                                "email": self.email, "password": self.password, "turnstileToken": token
                            }, headers={"Content-Type":"application/json","Origin":"https://repeatermock.com","User-Agent":"Mozilla/5.0"}, timeout=30.0)
                            data = r2.json()
                            sc = r2.headers.get_list("set-cookie") if hasattr(r2.headers,"get_list") else []
                            if data.get("success"):
                                for c in sc:
                                    p = c.split(";")[0].split("=",1)
                                    if len(p)==2:
                                        if 'access' in p[0].lower(): self.access_token = p[1].strip()
                                        elif 'refresh' in p[0].lower(): self.refresh_token = p[1].strip()
                                self.token_expires = time.time() + 900
                                self.failures = 0
                                print(f"  [auth] ✅ Login! {data.get('user',{}).get('name','?')}", flush=True)
                                return True
                            else:
                                print(f"  [auth] Login failed: {data}", flush=True)
                                return False
                    print(f"  [auth] No token", flush=True)
            except Exception as e:
                print(f"  [auth] Error: {e}", flush=True)
            time.sleep(5)
        return False

    def refresh(self):
        if not self.refresh_token: return False
        try:
            body = json.dumps({}).encode()
            req = urllib.request.Request("https://api.repeatermock.com/auth/refresh", data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Cookie", f"refreshToken={self.refresh_token}")
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            if data.get("accessToken"):
                self.access_token = data["accessToken"]
                self.token_expires = time.time() + 900
                self.failures = 0
                print(f"  [auth] ✅ Refreshed!", flush=True)
                return True
        except: pass
        return False

    def get_token(self):
        if self.is_valid(): return self.access_token
        if self.refresh(): return self.access_token
        if self.failures < MAX_AUTH_FAILURES and self.login_zenrows(): return self.access_token
        self.failures += 1
        return None

    def stop(self): return self.failures >= MAX_AUTH_FAILURES


class ProgressTracker:
    """Same format as free scraper's progress.json."""
    def __init__(self, output_dir):
        self.path = os.path.join(output_dir, "progress.json")
        self.data = {}
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    self.data = json.load(f)
            except: pass

    def is_scraped(self, series_slug, test_id):
        return test_id in self.data.get(series_slug, {}).get("scraped", {})

    async def mark_scraped(self, series_slug, test_id, filepath):
        if series_slug not in self.data:
            self.data[series_slug] = {"scraped": {}, "failed": {}, "pro": {}}
        self.data[series_slug].setdefault("scraped", {})[test_id] = {
            "filepath": filepath, "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self._save()

    async def mark_failed(self, series_slug, test_id, reason):
        if series_slug not in self.data:
            self.data[series_slug] = {"scraped": {}, "failed": {}, "pro": {}}
        fd = self.data[series_slug].setdefault("failed", {})
        if test_id in fd:
            fd[test_id]["attempts"] = fd[test_id].get("attempts", 1) + 1
            fd[test_id]["last_reason"] = reason
        else:
            fd[test_id] = {"reason": reason, "attempts": 1}
        self._save()

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2)
        os.rename(tmp, self.path)

    def stats(self):
        s = sum(len(v.get("scraped",{})) for v in self.data.values())
        f = sum(len(v.get("failed",{})) for v in self.data.values())
        return {"scraped": s, "failed": f}


async def browser_api(page, url, body="{}", max_retries=2):
    """Browser fetch API call with retryAfter handling."""
    for attempt in range(max_retries):
        result = await page.evaluate(f"""
            (async function(){{
                try {{
                    const r = await fetch('{url}', {{
                        method: 'POST', credentials: 'include',
                        headers: {{'Content-Type': 'application/json'}}, body: '{body}'
                    }});
                    const t = await r.text();
                    const ra = r.headers.get('retry-after') || r.headers.get('Retry-After') || '';
                    return {{status: r.status, body: t.slice(0, 500), retryAfter: ra}};
                }} catch(e) {{ return {{status: 0, error: String(e).slice(0, 200)}}; }}
            }})()
        """)
        status = result.get("status", 0)
        if status == 429:
            # Read retryAfter from response body
            try:
                err = json.loads(result.get("body", "{}"))
                ra = err.get("retryAfter", 0)
            except:
                ra = 0
            if ra and ra > 0:
                wait = min(ra, 300)
                print(f"    429 retryAfter={ra}s → wait {wait}s ({attempt+1}/{max_retries})", flush=True)
                await asyncio.sleep(wait)
                continue
            wait = 60
            print(f"    429 → wait {wait}s ({attempt+1}/{max_retries})", flush=True)
            await asyncio.sleep(wait)
            continue
        return status, result
    return 429, {"body": "max retries"}


async def scrape_test(page, auth, progress, test_info, output_dir, wid):
    """Scrape one PRO test — same format as free scraper."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series = test_info.get("series_slug", "")
    series_name = test_info.get("series_name", series)
    section = test_info.get("section", "Section")
    subsection = test_info.get("subsection", "Subsection")

    # Skip if already scraped
    if progress.is_scraped(series, tid):
        return "SKIP"

    if auth.stop(): return "STOP"

    # Refresh token if needed
    token = auth.get_token()
    if not token:
        print(f"  [w{wid}] ❌ no token", flush=True)
        return None

    # Update cookies
    await page.context.add_cookies([
        {"name":"accessToken","value":auth.access_token,"domain":".repeatermock.com","path":"/"},
        {"name":"refreshToken","value":auth.refresh_token,"domain":".repeatermock.com","path":"/"},
    ])

    print(f"  [w{wid}] {title[:50]}... ({tid[:8]})", flush=True)

    # API calls via browser fetch
    s, r = await browser_api(page, f"{API_BASE}/api/v1/attempts/{tid}/start")
    if s == 429: print(f"  [w{wid}] 429", flush=True); return None
    if s == 402: print(f"  [w{wid}] 💰 PRO", flush=True); await progress.mark_failed(series, tid, "PRO 402"); return "PRO"
    if s == 401: auth.failures += 1; print(f"  [w{wid}] 401 ({auth.failures}/{MAX_AUTH_FAILURES})", flush=True); return None
    if s != 200: print(f"  [w{wid}] start: {s}", flush=True); return None

    s, r = await browser_api(page, f"{API_BASE}/api/v1/attempts/{tid}/submit",
        '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if s != 200: print(f"  [w{wid}] submit: {s}", flush=True); return None

    # Solution page
    tr = TestRef(test_id=tid, title=title, series_slug=series, series_name=series_name,
        section_id="", section_name=section, sub_section_id="", sub_section_name=subsection,
        is_free=False, duration=0, question_count=0, total_mark=0)

    await page.goto(f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution", wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(6000)
    if "login" in page.url: auth.failures += 1; print(f"  [w{wid}] ❌ login redirect", flush=True); return None

    # Get HTML via DOM (same as free scraper)
    await page.evaluate("(function(){window.__HTML__=document.documentElement.outerHTML})()")
    total = await page.evaluate("(window.__HTML__||'').length")
    chunks, cs = [], 200000
    for i in range(min((total//cs)+1, 100)):
        s2 = i*cs
        if s2 >= total: break
        c = await page.evaluate(f"(window.__HTML__||'').slice({s2},{s2+cs})")
        if not c: break
        chunks.append(c)
    html = "".join(chunks)
    if not html or "testData" not in html or "answersData" not in html:
        print(f"  [w{wid}] ❌ no testData (len={len(html)})", flush=True)
        await progress.mark_failed(series, tid, "no testData")
        return None

    # Parse + render (same as free scraper)
    props = find_props_in_flight(html)
    if not props: print(f"  [w{wid}] ❌ no props", flush=True); return None
    text_refs = build_text_refs(html)
    td = parse_test_data(props, tid, series, text_refs)
    if td.title: tr.title = td.title

    # Save AI export (same path structure as free scraper)
    ai_path = build_ai_export_path(output_dir, tr)
    ai_export = render_ai_export(td, tr)
    tmp = ai_path + ".tmp"
    with open(tmp, "w") as f: json.dump(ai_export, f, ensure_ascii=False, indent=2)
    os.rename(tmp, ai_path)

    # Save HTML export (same path structure as free scraper)
    html_path = build_html_output_path(output_dir, tr)
    rendered = render_test_html(td)
    tmp2 = html_path + ".tmp"
    with open(tmp2, "w") as f: f.write(rendered)
    os.rename(tmp2, html_path)

    # Mark scraped in progress.json
    await progress.mark_scraped(series, tid, html_path)

    print(f"  [w{wid}] ✅ HTML({len(rendered):,}B)+AI({len(json.dumps(ai_export)):,}B): {title[:45]}", flush=True)
    return "OK"


async def run_scraper(chunks_file, output_dir, max_runtime=330):
    with open(chunks_file) as f:
        data = json.load(f)
    chunks = data.get("chunks", [])
    total_tests = data.get("total_tests", 0)

    print(f"\n{'='*60}", flush=True)
    print(f"PRO Scraper v2 | {total_tests} tests | {len(chunks)} series", flush=True)
    print(f"Output: {output_dir}", flush=True)
    print(f"Structure: {output_dir}/{{JOB_NAME}}/ai_export/...", flush=True)
    print(f"{'='*60}\n", flush=True)

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth()
    progress = ProgressTracker(output_dir)

    if not auth.is_valid():
        if not auth.login_zenrows():
            print("❌ Login failed — stopping", flush=True)
            return

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        await ctx.add_cookies([
            {"name":"accessToken","value":auth.access_token,"domain":".repeatermock.com","path":"/"},
            {"name":"refreshToken","value":auth.refresh_token,"domain":".repeatermock.com","path":"/"},
            {"name":"totpVerified","value":"1","domain":".repeatermock.com","path":"/"},
        ])
        await ctx.add_init_script(INIT_SCRIPT)
        page = await ctx.new_page()

        # Establish session (get cf_clearance)
        await page.goto("https://repeatermock.com/tb/test-series/ssc-gd-constable", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        done = fail = skip = 0
        start_time = time.time()

        for ci, chunk in enumerate(chunks):
            job_name = chunk.get("job_name", f"Series-{ci+1}")
            series_slug = chunk.get("series_slug", "")
            tests = chunk.get("tests", [])
            series_dir = os.path.join(output_dir, job_name)
            os.makedirs(series_dir, exist_ok=True)

            print(f"\n--- Series {ci+1}/{len(chunks)}: {job_name} ({series_slug}) — {len(tests)} tests ---", flush=True)

            for i, ti in enumerate(tests):
                # Check runtime
                elapsed = (time.time() - start_time) / 60
                if elapsed >= max_runtime:
                    print(f"\n⏰ Max runtime {max_runtime}min reached — stopping", flush=True)
                    break
                if auth.stop():
                    print(f"\n⛔ Auth stop — stopping", flush=True)
                    break

                result = await scrape_test(page, auth, progress, ti, series_dir, 1)
                if result == "STOP": break
                elif result == "OK": done += 1
                elif result == "SKIP": skip += 1
                elif result == "PRO": fail += 1
                else: fail += 1

                # 5s delay between tests
                if i < len(tests) - 1:
                    await asyncio.sleep(5 + random.uniform(0, 2))

                # Progress report every 10 tests
                if (done + fail + skip) % 10 == 0:
                    stats = progress.stats()
                    print(f"📊 [{elapsed:.1f}min] Done:{done} Fail:{fail} Skip:{skip} | Progress: {stats['scraped']} scraped, {stats['failed']} failed", flush=True)

            # Commit after each series
            if os.environ.get("GITHUB_ACTIONS"):
                import subprocess
                try:
                    subprocess.run(["git", "add", series_dir + "/"], capture_output=True, timeout=10)
                    subprocess.run(["git", "commit", "-m", f"pro-scrape: {job_name} ({done} total scraped)"], capture_output=True, timeout=10)
                    subprocess.run(["git", "push", "origin", "HEAD"], capture_output=True, timeout=15)
                    print(f"  💾 Committed {job_name}", flush=True)
                except: pass

            if auth.stop() or elapsed >= max_runtime:
                break

        await browser.close()

    # Save progress
    stats = progress.stats()
    summary = {
        "total_tests": total_tests, "scraped": done, "failed": fail, "skipped": skip,
        "progress_scraped": stats["scraped"], "progress_failed": stats["failed"],
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "runtime_min": (time.time() - start_time) / 60,
    }
    with open(os.path.join(output_dir, "PRO_STATS.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}", flush=True)
    print(f"✅ Done! Scraped: {done} | Failed: {fail} | Skipped: {skip}", flush=True)
    print(f"   Runtime: {summary['runtime_min']:.1f} min", flush=True)
    print(f"{'='*60}", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chunks", required=True, help="Path to all_chunks.json")
    p.add_argument("--output-dir", required=True, help="Output directory (e.g. pro_scraped_output)")
    p.add_argument("--max-runtime", type=int, default=330, help="Max runtime in minutes (default: 330 = 5.5h)")
    a = p.parse_args()
    asyncio.run(run_scraper(a.chunks, a.output_dir, a.max_runtime))

if __name__ == "__main__": main()
