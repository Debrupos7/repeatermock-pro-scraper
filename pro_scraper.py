#!/usr/bin/env python3
"""RepeaterMock PRO Scraper v3 — multi-account + ZenRows key rotation.

Improvements over v2:
- **3 PRO accounts**: Each worker uses its own account (RM_EMAIL_N / RM_PASSWORD_N).
  Account selected via WORKER_ID env var (1, 2, or 3).
- **4 ZenRows keys rotation**: Passed via ZENROWS_API_KEYS env var (comma-separated).
  Tries each key in order until one works. When a key returns 402/403/no credits,
  marks it as exhausted and tries the next one.
- **Smart token refresh**: Refresh accessToken only when it expires (~14 min).
  No repeated refresh calls. After 3 consecutive refresh failures, attempt
  full ZenRows re-login.
- **5-error stop**: After 5 consecutive scrape failures (not rate-limit 429s),
  stop the worker and save progress. The workflow will auto-trigger the next
  round to retry failed tests.
- **5.5h max runtime** per worker.
- **Same folder structure as free scraper**:
  pro_scraped_output/{series_slug}/ai_export/{section}/{subsection}/{title}_{test_id}.json
  pro_scraped_output/{series_slug}/html_export/{section}/{subsection}/{title}_{test_id}.html
- **Per-worker progress**: Each worker writes its own progress file:
  pro_scraped_output/{series_slug}/progress_worker_{N}.json
  This avoids concurrent write conflicts when 3 workers run in parallel.
- **5s delay between tests** (with ±2s jitter) — keeps well under rate limit
  when each account handles only ~3,300 tests.
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

# Tunables
MAX_CONSECUTIVE_ERRORS = 5      # stop after 5 consecutive non-rate-limit errors
MAX_AUTH_FAILURES = 3          # max login failures before giving up
TOKEN_REFRESH_INTERVAL = 14 * 60  # 14 minutes (access token expires at 15)
DELAY_BETWEEN_TESTS = 5.0      # seconds (with ±2s jitter)
DEFAULT_MAX_RUNTIME = 330      # 5.5 hours in minutes

# Anti-self-destruct INIT_SCRIPT (neutralizes repeatermock's anti-devtools trap)
INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""


class PROAuth:
    """Manages one PRO account per worker. Rotates ZenRows keys on exhaustion."""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.email = os.environ.get(f"RM_EMAIL_{worker_id}", os.environ.get("RM_EMAIL", ""))
        self.password = os.environ.get(f"RM_PASSWORD_{worker_id}", os.environ.get("RM_PASSWORD", ""))
        # ZenRows keys (comma-separated). First is the original (uses remaining credits first).
        zenrows_keys_str = os.environ.get("ZENROWS_API_KEYS", os.environ.get("ZENROWS_API_KEY", ""))
        self.zenrows_keys = [k.strip() for k in zenrows_keys_str.split(",") if k.strip()]
        self.zenrows_key_idx = 0  # which key we're currently trying
        self.exhausted_keys = set()  # keys that returned 402/403/no-credits

        self.access_token = ""
        self.refresh_token = ""
        self.token_expires = 0  # unix timestamp when accessToken expires
        self.last_refresh_attempt = 0  # throttle refresh calls
        self.failures = 0  # consecutive auth failures (login or refresh)
        self.consecutive_scrape_errors = 0  # consecutive non-rate-limit scrape failures

        print(f"  [auth/w{worker_id}] account: {self.email}", flush=True)
        print(f"  [auth/w{worker_id}] zenrows keys: {len(self.zenrows_keys)} available", flush=True)

    def is_valid(self):
        return bool(self.access_token) and time.time() < (self.token_expires - 60)

    def _next_zenrows_key(self):
        """Rotate to next ZenRows key. Returns the key or empty string if all exhausted."""
        for i, k in enumerate(self.zenrows_keys):
            if i not in self.exhausted_keys:
                self.zenrows_key_idx = i
                return k
        return ""

    def _mark_key_exhausted(self, key: str):
        """Mark a ZenRows key as exhausted (out of credits)."""
        for i, k in enumerate(self.zenrows_keys):
            if k == key:
                self.exhausted_keys.add(i)
                print(f"  [auth/w{self.worker_id}] ⛔ ZenRows key #{i+1} marked exhausted "
                      f"({len(self.zenrows_keys) - len(self.exhausted_keys)} keys remaining)", flush=True)
                return

    def login_zenrows(self):
        """Login via ZenRows — fetches login page with JS render to solve Turnstile, then POSTs to /auth/login."""
        key = self._next_zenrows_key()
        if not key:
            print(f"  [auth/w{self.worker_id}] ❌ all ZenRows keys exhausted", flush=True)
            return False

        print(f"  [auth/w{self.worker_id}] ZenRows login with key #{self.zenrows_key_idx+1}...", flush=True)
        import httpx
        for attempt in range(3):
            print(f"  [auth/w{self.worker_id}] attempt {attempt+1}/3...", flush=True)
            try:
                with httpx.Client(timeout=180.0) as cli:
                    r = cli.get("https://api.zenrows.com/v1/", params={
                        "apikey": key, "url": "https://repeatermock.com/login",
                        "js_render": "true", "premium_proxy": "true", "wait": "25000",
                    }, timeout=180.0)
                    content = r.text
                    print(f"  [auth/w{self.worker_id}] ZenRows: HTTP {r.status_code}, {len(content):,}B",
                          flush=True)

                    # Check for ZenRows credit exhaustion signals
                    if r.status_code == 402 or r.status_code == 403:
                        self._mark_key_exhausted(key)
                        # Try next key
                        new_key = self._next_zenrows_key()
                        if new_key and new_key != key:
                            print(f"  [auth/w{self.worker_id}] trying next key...", flush=True)
                            return self.login_zenrows()
                        return False
                    try:
                        jr = r.json()
                        if jr.get("code") in (ERR_OUT_OF_CREDITS := "AUTH_001"):
                            self._mark_key_exhausted(key)
                            return self.login_zenrows()
                    except Exception:
                        pass

                    # Extract Turnstile token
                    m = re.search(r'name="cf-turnstile-response"[^>]*value="([^"]+)"', content)
                    if not m or len(m.group(1)) < 20:
                        print(f"  [auth/w{self.worker_id}] no Turnstile token in response", flush=True)
                        # Sometimes ZenRows returns HTML but Turnstile didn't solve. Try next key.
                        time.sleep(5)
                        continue

                    token = m.group(1)
                    print(f"  [auth/w{self.worker_id}] ✅ Turnstile token (len={len(token)})", flush=True)

                    # Now POST login
                    r2 = cli.post("https://api.repeatermock.com/auth/login", json={
                        "email": self.email, "password": self.password, "turnstileToken": token
                    }, headers={
                        "Content-Type": "application/json",
                        "Origin": "https://repeatermock.com",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=30.0)
                    data = r2.json()
                    set_cookies = r2.headers.get_list("set-cookie") if hasattr(r2.headers, "get_list") else []

                    if data.get("success"):
                        for c in set_cookies:
                            p = c.split(";")[0].split("=", 1)
                            if len(p) == 2:
                                if 'access' in p[0].lower():
                                    self.access_token = p[1].strip()
                                elif 'refresh' in p[0].lower():
                                    self.refresh_token = p[1].strip()
                        self.token_expires = time.time() + 900  # 15 min
                        self.failures = 0
                        print(f"  [auth/w{self.worker_id}] ✅ Login success! "
                              f"User: {data.get('user', {}).get('name', '?')}", flush=True)
                        return True
                    else:
                        print(f"  [auth/w{self.worker_id}] login API failed: {data}", flush=True)
                        return False
            except Exception as e:
                print(f"  [auth/w{self.worker_id}] ZenRows error: {e}", flush=True)
            time.sleep(5)
        return False

    def refresh(self):
        """Refresh accessToken via /auth/refresh. Throttled: max 1 call per 60s."""
        if not self.refresh_token:
            return False
        now = time.time()
        if now - self.last_refresh_attempt < 60:
            # Throttled — don't hammer the refresh endpoint
            return self.is_valid()
        self.last_refresh_attempt = now
        try:
            body = json.dumps({}).encode()
            req = urllib.request.Request("https://api.repeatermock.com/auth/refresh",
                                         data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Cookie", f"refreshToken={self.refresh_token}")
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            if data.get("accessToken"):
                self.access_token = data["accessToken"]
                self.token_expires = time.time() + 900  # 15 min
                self.failures = 0
                print(f"  [auth/w{self.worker_id}] ✅ Token refreshed (valid 15 min)", flush=True)
                return True
            else:
                print(f"  [auth/w{self.worker_id}] refresh returned no token: {data}", flush=True)
        except Exception as e:
            print(f"  [auth/w{self.worker_id}] refresh error: {e}", flush=True)
        return False

    def get_token(self):
        """Get a valid accessToken. Refresh if expired (every 14 min). Re-login if refresh fails."""
        if self.is_valid():
            return self.access_token

        # Try refresh first (only if we have a refresh_token)
        if self.refresh_token:
            if self.refresh():
                return self.access_token
            # Refresh failed — refresh_token may be invalid. Try ZenRows re-login.

        if self.failures < MAX_AUTH_FAILURES:
            if self.login_zenrows():
                return self.access_token

        self.failures += 1
        return None

    def should_stop(self):
        """Stop if too many auth failures OR too many consecutive scrape errors."""
        return (self.failures >= MAX_AUTH_FAILURES
                or self.consecutive_scrape_errors >= MAX_CONSECUTIVE_ERRORS)

    def reset_scrape_errors(self):
        self.consecutive_scrape_errors = 0

    def note_scrape_error(self):
        self.consecutive_scrape_errors += 1

    def note_scrape_success(self):
        self.consecutive_scrape_errors = 0
        self.failures = 0  # successful scrape means auth is fine


class ProgressTracker:
    """Per-worker progress.json — same format as free scraper's progress.json
    but writes to progress_worker_{N}.json to avoid concurrent write conflicts.
    """
    def __init__(self, output_dir, worker_id: int):
        self.worker_id = worker_id
        self.path = os.path.join(output_dir, f"progress_worker_{worker_id}.json")
        self.data = {}
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    self.data = json.load(f)
            except Exception:
                pass

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
        s = sum(len(v.get("scraped", {})) for v in self.data.values())
        f = sum(len(v.get("failed", {})) for v in self.data.values())
        return {"scraped": s, "failed": f}


async def browser_api(page, url, body="{}", max_retries=2):
    """Browser fetch API call with retryAfter handling for 429."""
    for attempt in range(max_retries):
        # Escape body for safe interpolation into JS
        body_escaped = body.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
        result = await page.evaluate(f"""
            (async function(){{
                try {{
                    const r = await fetch('{url}', {{
                        method: 'POST', credentials: 'include',
                        headers: {{'Content-Type': 'application/json'}}, body: '{body_escaped}'
                    }});
                    const t = await r.text();
                    const ra = r.headers.get('retry-after') || r.headers.get('Retry-After') || '';
                    return {{status: r.status, body: t.slice(0, 500), retryAfter: ra}};
                }} catch(e) {{ return {{status: 0, error: String(e).slice(0, 200)}}; }}
            }})()
        """)
        status = result.get("status", 0)
        if status == 429:
            try:
                err = json.loads(result.get("body", "{}"))
                ra = err.get("retryAfter", 0)
            except Exception:
                ra = 0
            if ra and ra > 0:
                wait = min(int(ra), 300)
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
    """Scrape one PRO test. Returns 'OK', 'SKIP', 'PRO', 'STOP', or None on error.

    output_dir here is the per-series directory: pro_scraped_output/{series_slug}/
    AI/HTML files go to:
      {output_dir}/ai_export/Single_Tests/Default/{title}_{test_id}.{json,html}
    """
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series = test_info.get("series_slug", "")
    series_name = test_info.get("series_name", series)
    # PRO_TESTS.json has placeholder "Section"/"Subsection" values — replace with
    # "Single_Tests"/"Default" so files end up in a sensible per-series subfolder
    # rather than literally named "Section/Subsection".
    raw_section = test_info.get("section", "")
    raw_subsection = test_info.get("subsection", "")
    section = raw_section if raw_section and raw_section != "Section" else "Single_Tests"
    subsection = raw_subsection if raw_subsection and raw_subsection != "Subsection" else "Default"

    if progress.is_scraped(series, tid):
        return "SKIP"

    if auth.should_stop():
        return "STOP"

    token = auth.get_token()
    if not token:
        print(f"  [w{wid}] ❌ no token (failures={auth.failures})", flush=True)
        auth.note_scrape_error()
        return None

    # Inject auth cookies into browser context
    await page.context.add_cookies([
        {"name": "accessToken", "value": auth.access_token,
         "domain": ".repeatermock.com", "path": "/"},
        {"name": "refreshToken", "value": auth.refresh_token,
         "domain": ".repeatermock.com", "path": "/"},
    ])

    print(f"  [w{wid}] {title[:50]}... ({tid[:8]})", flush=True)

    # 1) POST /attempts/{tid}/start
    s, r = await browser_api(page, f"{API_BASE}/api/v1/attempts/{tid}/start")
    if s == 429:
        print(f"  [w{wid}] 429 on start", flush=True)
        auth.note_scrape_error()
        return None
    if s == 402:
        print(f"  [w{wid}] 💰 PRO-only (402)", flush=True)
        await progress.mark_failed(series, tid, "PRO 402")
        return "PRO"
    if s == 401:
        auth.failures += 1
        print(f"  [w{wid}] 401 (auth failures={auth.failures}/{MAX_AUTH_FAILURES})", flush=True)
        auth.note_scrape_error()
        return None
    if s != 200:
        print(f"  [w{wid}] start returned {s}", flush=True)
        auth.note_scrape_error()
        return None

    # 2) POST /attempts/{tid}/submit (with empty answers — just to create the attempt record)
    s, r = await browser_api(page, f"{API_BASE}/api/v1/attempts/{tid}/submit",
                             '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if s != 200:
        print(f"  [w{wid}] submit returned {s}", flush=True)
        auth.note_scrape_error()
        return None

    # 3) Navigate to /solution page and extract HTML
    tr = TestRef(test_id=tid, title=title, series_slug=series, series_name=series_name,
                 section_id="", section_name=section, sub_section_id="",
                 sub_section_name=subsection, is_free=False,
                 duration=0, question_count=0, total_mark=0)

    try:
        await page.goto(f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution",
                        wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"  [w{wid}] goto solution failed: {e}", flush=True)
        auth.note_scrape_error()
        return None

    await page.wait_for_timeout(6000)

    if "login" in page.url:
        auth.failures += 1
        print(f"  [w{wid}] ❌ login redirect on solution page", flush=True)
        auth.note_scrape_error()
        return None

    # 4) Extract HTML from DOM (chunked — 200KB at a time)
    await page.evaluate("(function(){window.__HTML__=document.documentElement.outerHTML})()")
    total = await page.evaluate("(window.__HTML__||'').length")
    chunks, cs = [], 200000
    for i in range(min((total // cs) + 1, 100)):
        s2 = i * cs
        if s2 >= total:
            break
        c = await page.evaluate(f"(window.__HTML__||'').slice({s2},{s2+cs})")
        if not c:
            break
        chunks.append(c)
    html = "".join(chunks)
    if not html or "testData" not in html or "answersData" not in html:
        print(f"  [w{wid}] ❌ no testData in HTML (len={len(html)})", flush=True)
        await progress.mark_failed(series, tid, "no testData")
        auth.note_scrape_error()
        return None

    # 5) Parse + render (same as free scraper)
    props = find_props_in_flight(html)
    if not props:
        print(f"  [w{wid}] ❌ no props found in flight data", flush=True)
        auth.note_scrape_error()
        return None
    text_refs = build_text_refs(html)
    td = parse_test_data(props, tid, series, text_refs)
    if td.title:
        tr.title = td.title

    # 6) Save AI export
    ai_path = build_ai_export_path(output_dir, tr)
    ai_export = render_ai_export(td, tr)
    tmp = ai_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(ai_export, f, ensure_ascii=False, indent=2)
    os.rename(tmp, ai_path)

    # 7) Save HTML export
    html_path = build_html_output_path(output_dir, tr)
    rendered = render_test_html(td)
    tmp2 = html_path + ".tmp"
    with open(tmp2, "w") as f:
        f.write(rendered)
    os.rename(tmp2, html_path)

    # 8) Mark scraped
    await progress.mark_scraped(series, tid, html_path)
    auth.note_scrape_success()

    print(f"  [w{wid}] ✅ HTML({len(rendered):,}B)+AI({len(json.dumps(ai_export)):,}B): "
          f"{title[:45]}", flush=True)
    return "OK"


async def run_scraper(worker_file, output_dir, max_runtime=DEFAULT_MAX_RUNTIME):
    """Run the scraper for one worker. Reads its assigned tests from worker_file."""
    with open(worker_file) as f:
        data = json.load(f)
    wid = data.get("worker_id", 1)
    chunks = data.get("chunks", [])
    total_tests = data.get("total_tests", 0)
    account_email = data.get("account_email", "?")

    print(f"\n{'='*60}", flush=True)
    print(f"PRO Scraper v3 | Worker {wid} | Account: {account_email}", flush=True)
    print(f"Total tests: {total_tests} across {len(chunks)} series", flush=True)
    print(f"Output: {output_dir}/{{series_slug}}/ai_export/...", flush=True)
    print(f"Max runtime: {max_runtime} min ({max_runtime/60:.1f}h)", flush=True)
    print(f"Delay between tests: {DELAY_BETWEEN_TESTS}s (±2s jitter)", flush=True)
    print(f"Stop conditions: {MAX_CONSECUTIVE_ERRORS} consecutive errors OR "
          f"{MAX_AUTH_FAILURES} auth failures", flush=True)
    print(f"{'='*60}\n", flush=True)

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth(worker_id=wid)
    progress = ProgressTracker(output_dir, wid)

    # Initial login
    if not auth.is_valid():
        if not auth.login_zenrows():
            print(f"❌ Worker {wid}: initial login failed — aborting", flush=True)
            return {"worker_id": wid, "scraped": 0, "failed": 0, "skipped": 0,
                    "reason": "initial_login_failed"}

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )
        await ctx.add_cookies([
            {"name": "accessToken", "value": auth.access_token,
             "domain": ".repeatermock.com", "path": "/"},
            {"name": "refreshToken", "value": auth.refresh_token,
             "domain": ".repeatermock.com", "path": "/"},
            {"name": "totpVerified", "value": "1",
             "domain": ".repeatermock.com", "path": "/"},
        ])
        await ctx.add_init_script(INIT_SCRIPT)
        page = await ctx.new_page()

        # Establish Cloudflare clearance by visiting a benign page
        try:
            await page.goto("https://repeatermock.com/tb/test-series/ssc-gd-constable",
                            wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"  [w{wid}] warning: initial goto failed: {e}", flush=True)

        done = fail = skip = pro = 0
        start_time = time.time()
        stop_reason = ""

        for ci, chunk in enumerate(chunks):
            job_name = chunk.get("job_name", f"Series-{ci+1}")
            series_slug = chunk.get("series_slug", "")
            tests = chunk.get("tests", [])

            # Per-series output directory: pro_scraped_output/{series_slug}/
            # This matches the free scraper's folder structure.
            series_dir = os.path.join(output_dir, series_slug) if series_slug else output_dir
            os.makedirs(series_dir, exist_ok=True)

            print(f"\n--- [w{wid}] Series {ci+1}/{len(chunks)}: {job_name} "
                  f"({series_slug}) — {len(tests)} tests ---", flush=True)
            print(f"    Output dir: {series_dir}", flush=True)

            series_done = 0
            for i, ti in enumerate(tests):
                # Check runtime limit
                elapsed = (time.time() - start_time) / 60
                if elapsed >= max_runtime:
                    stop_reason = f"max_runtime {max_runtime}min"
                    print(f"\n⏰ [w{wid}] Max runtime reached — stopping", flush=True)
                    break
                if auth.should_stop():
                    stop_reason = (f"stop condition met "
                                   f"(auth_failures={auth.failures}, "
                                   f"scrape_errors={auth.consecutive_scrape_errors})")
                    print(f"\n⛔ [w{wid}] Stop condition — stopping", flush=True)
                    break

                # Pass the per-series directory so AI/HTML files land in the right place
                result = await scrape_test(page, auth, progress, ti, series_dir, wid)
                if result == "STOP":
                    stop_reason = "stop condition met"
                    break
                elif result == "OK":
                    done += 1
                    series_done += 1
                elif result == "SKIP":
                    skip += 1
                elif result == "PRO":
                    pro += 1
                else:
                    fail += 1

                # 5s delay (±2s jitter) between tests
                if i < len(tests) - 1:
                    await asyncio.sleep(DELAY_BETWEEN_TESTS + random.uniform(0, 2))

                # Progress report every 10 tests
                if (done + fail + skip + pro) % 10 == 0:
                    stats = progress.stats()
                    print(f"📊 [w{wid}] [{elapsed:.1f}min] "
                          f"Done:{done} Fail:{fail} Skip:{skip} PRO:{pro} | "
                          f"Progress: {stats['scraped']} scraped, "
                          f"{stats['failed']} failed", flush=True)

            # Commit progress + scraped files after each series
            if os.environ.get("GITHUB_ACTIONS"):
                import subprocess
                try:
                    subprocess.run(["git", "add", "pro_scraped_output/"],
                                   capture_output=True, timeout=30)
                    subprocess.run(["git", "commit", "-m",
                                    f"pro-scrape w{wid}: {job_name} +{series_done} "
                                    f"(total scraped: {done})"],
                                   capture_output=True, timeout=15)
                    # Try push with retries (other workers may be pushing concurrently)
                    for push_attempt in range(3):
                        r = subprocess.run(["git", "pull", "--rebase",
                                            "origin", "main"],
                                          capture_output=True, timeout=20)
                        r2 = subprocess.run(["git", "push", "origin", "HEAD"],
                                            capture_output=True, timeout=20)
                        if r2.returncode == 0:
                            print(f"  💾 [w{wid}] committed + pushed {job_name}", flush=True)
                            break
                        time.sleep(push_attempt * 3 + 2)
                    else:
                        print(f"  ⚠️ [w{wid}] push failed for {job_name} "
                              f"(will retry later)", flush=True)
                except Exception as e:
                    print(f"  ⚠️ [w{wid}] git error: {e}", flush=True)

            if stop_reason:
                break

        await browser.close()

    # Save worker summary
    stats = progress.stats()
    summary = {
        "worker_id": wid,
        "account_email": account_email,
        "total_tests": total_tests,
        "scraped": done,
        "failed": fail,
        "skipped": skip,
        "pro": pro,
        "progress_scraped": stats["scraped"],
        "progress_failed": stats["failed"],
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "runtime_min": (time.time() - start_time) / 60,
        "stop_reason": stop_reason or "completed_all_tests",
    }
    with open(os.path.join(output_dir, f"WORKER_{wid}_STATS.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}", flush=True)
    print(f"✅ [w{wid}] Done! Scraped: {done} | Failed: {fail} | "
          f"Skipped: {skip} | PRO: {pro}", flush=True)
    print(f"   Runtime: {summary['runtime_min']:.1f} min", flush=True)
    if stop_reason:
        print(f"   Stop reason: {stop_reason}", flush=True)
    print(f"{'='*60}", flush=True)

    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--worker-file", required=True,
                   help="Path to worker_N.json (from distribute_pro_tests.py)")
    p.add_argument("--output-dir", required=True,
                   help="Output directory (e.g. pro_scraped_output)")
    p.add_argument("--max-runtime", type=int, default=DEFAULT_MAX_RUNTIME,
                   help=f"Max runtime in minutes (default: {DEFAULT_MAX_RUNTIME} = 5.5h)")
    a = p.parse_args()
    asyncio.run(run_scraper(a.worker_file, a.output_dir, a.max_runtime))


if __name__ == "__main__":
    main()
