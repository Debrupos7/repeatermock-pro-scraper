#!/usr/bin/env python3
"""RepeaterMock PRO Scraper — ZenRows auto-login + browser fetch API calls.

Key insight: The free scraper uses page.evaluate(fetch(...)) for API calls
(browser-initiated, with CF clearance + all cookies). Our PRO scraper was
using urllib.request (direct API calls) which gets rate-limited differently.

FIX: Use the SAME approach as the free scraper — make API calls from the
browser context via page.evaluate(fetch(...)).

Flow:
1. ZenRows auto-login → get accessToken + refreshToken
2. Navigate to repeatermock.com (get cf_clearance)
3. For each test: browser fetch /attempts/start → /attempts/submit → solution page
4. 15s delay between tests (same as free scraper)
5. Auto-refresh token every 14 min
6. Auto re-login via ZenRows if refresh fails
"""
import argparse, asyncio, json, os, re, sys, time, random, urllib.request, urllib.error, urllib.parse
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
        cookies = os.environ.get("PRO_COOKIES", "")
        if cookies:
            m = re.search(r'accessToken=([^;]+)', cookies)
            if m: self.access_token = m.group(1)
            m = re.search(r'refreshToken=([^;]+)', cookies)
            if m: self.refresh_token = m.group(1)
            self.token_expires = time.time() + 900

    def is_token_valid(self):
        return bool(self.access_token) and time.time() < (self.token_expires - 60)

    def login_via_zenrows(self):
        if not self.zenrows_key: return False
        print("  [auth] ZenRows login...")
        import subprocess
        # Use httpx (same as working login repo) — install if needed
        try:
            import httpx
        except ImportError:
            subprocess.run([sys.executable, "-m", "pip", "install", "httpx", "-q"], capture_output=True, timeout=30)
            import httpx
        
        for attempt in range(3):
            print(f"  [auth] ZenRows attempt {attempt+1}/3...")
            try:
                async def zenrows_login():
                    async with httpx.AsyncClient(timeout=180.0) as cli:
                        params = {
                            "apikey": self.zenrows_key,
                            "url": "https://repeatermock.com/login",
                            "js_render": "true",
                            "premium_proxy": "true",
                            "wait": "25000",
                        }
                        r = await cli.get("https://api.zenrows.com/v1/", params=params, timeout=180.0)
                        content = r.text
                        print(f"  [auth] ZenRows response: {r.status_code}, {len(content):,} bytes")
                        
                        for pat in [r'name="cf-turnstile-response"[^>]*value="([^"]+)"']:
                            m = re.search(pat, content)
                            if m and len(m.group(1)) > 20:
                                token = m.group(1)
                                print(f"  [auth] ✅ Turnstile solved! len={len(token)}")
                                # Login
                                r2 = await cli.post("https://api.repeatermock.com/auth/login", json={
                                    "email": self.email, "password": self.password, "turnstileToken": token
                                }, headers={
                                    "Content-Type": "application/json",
                                    "Origin": "https://repeatermock.com",
                                    "Referer": "https://repeatermock.com/login",
                                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                                }, timeout=30.0)
                                data = r2.json()
                                set_cookies = r2.headers.get_list("set-cookie") if hasattr(r2.headers, "get_list") else []
                                if data.get("success"):
                                    for sc in set_cookies:
                                        parts = sc.split(";")[0].split("=", 1)
                                        if len(parts) == 2:
                                            name = parts[0].strip()
                                            value = parts[1].strip()
                                            if 'access' in name.lower(): self.access_token = value
                                            elif 'refresh' in name.lower(): self.refresh_token = value
                                    self.token_expires = time.time() + 900
                                    self.failures = 0
                                    user = data.get("user", {})
                                    print(f"  [auth] ✅ Login! {user.get('name','?')} | Plan: {user.get('plan','?')}")
                                    print(f"  [auth] accessToken: {len(self.access_token)} | refreshToken: {len(self.refresh_token)}")
                                    return True
                                else:
                                    print(f"  [auth] Login failed: {data}")
                                    return False
                        print(f"  [auth] No Turnstile token found")
                        return False
                
                result = asyncio.run(zenrows_login())
                if result:
                    return True
            except Exception as e:
                print(f"  [auth] ZenRows error: {e}")
            time.sleep(5)
        return False

    def refresh_access_token(self):
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
                print(f"  [auth] ✅ Token refreshed!")
                return True
        except: pass
        return False

    def get_valid_token(self):
        if self.is_token_valid(): return self.access_token
        if self.refresh_access_token(): return self.access_token
        if self.failures < MAX_AUTH_FAILURES and self.login_via_zenrows(): return self.access_token
        self.failures += 1
        return None

    def stop(self): return self.failures >= MAX_AUTH_FAILURES


async def browser_api_call(page, url, body="{}", max_retries=3):
    """Make API call from browser context (same as free scraper)."""
    for attempt in range(max_retries):
        result = await page.evaluate(f"""
            (async function(){{
                try {{
                    const r = await fetch('{url}', {{
                        method: 'POST', credentials: 'include',
                        headers: {{'Content-Type': 'application/json'}}, body: '{body}'
                    }});
                    const t = await r.text();
                    return {{status: r.status, body: t.slice(0, 500)}};
                }} catch(e) {{ return {{status: 0, error: String(e).slice(0, 200)}}; }}
            }})()
        """)
        status = result.get("status", 0)
        if status == 429:
            wait = 30 * (attempt + 1)
            print(f"    429 — wait {wait}s ({attempt+1}/{max_retries})")
            await asyncio.sleep(wait)
            continue
        return status, result
    return 429, {"body": "max retries"}


async def scrape_test(page, auth, ti, out, wid):
    tid = ti.get("test_id",""); title = ti.get("title", tid); series = ti.get("series_slug","")
    section = ti.get("section","Uncategorized"); subsection = ti.get("subsection","Default")
    if auth.stop(): return "STOP"
    
    token = auth.get_valid_token()
    if not token: return None

    # Update cookies in the page context
    await page.context.add_cookies([
        {"name":"accessToken","value":auth.access_token,"domain":".repeatermock.com","path":"/"},
        {"name":"refreshToken","value":auth.refresh_token,"domain":".repeatermock.com","path":"/"},
    ])

    print(f"  [w{wid}] {title[:45]}... ({tid[:8]})")

    # API calls from browser (same as free scraper)
    s, r = await browser_api_call(page, f"{API_BASE}/api/v1/attempts/{tid}/start")
    if s == 429: print(f"  [w{wid}] 429"); return None
    if s == 402: print(f"  [w{wid}] 💰 PRO"); return "PRO"
    if s == 401: auth.failures += 1; print(f"  [w{wid}] 401 ({auth.failures}/{MAX_AUTH_FAILURES})"); return None
    if s != 200: print(f"  [w{wid}] start: {s}"); return None

    s, r = await browser_api_call(page, f"{API_BASE}/api/v1/attempts/{tid}/submit",
        '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if s != 200: print(f"  [w{wid}] submit: {s}"); return None

    # Solution page
    tr = TestRef(test_id=tid,title=title,series_slug=series,series_name=ti.get("series_name",series),
        section_id="",section_name=section,sub_section_id="",sub_section_name=subsection,
        is_free=False,duration=0,question_count=0,total_mark=0)

    await page.goto(f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution", wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(6000)
    if "login" in page.url: auth.failures += 1; return None

    # Get HTML via DOM + chunks (same as free scraper)
    await page.evaluate("(function(){window.__HTML__=document.documentElement.outerHTML})()")
    total = await page.evaluate("(window.__HTML__||'').length")
    chunks, cs = [], 200000
    for i in range(min((total//cs)+1, 100)):
        s2=i*cs
        if s2>=total: break
        c = await page.evaluate(f"(window.__HTML__||'').slice({s2},{s2+cs})")
        if not c: break
        chunks.append(c)
    html = "".join(chunks)
    if not html or "testData" not in html or "answersData" not in html:
        print(f"  [w{wid}] ❌ no testData (len={len(html)})"); return None

    props = find_props_in_flight(html)
    if not props: return None
    tr_ref = build_text_refs(html)
    td = parse_test_data(props, tid, series, tr_ref)
    if td.title: tr.title = td.title

    ap = build_ai_export_path(out, tr)
    ae = render_ai_export(td, tr)
    tmp=ap+".tmp"
    with open(tmp,"w") as f: json.dump(ae,f,ensure_ascii=False,indent=2)
    os.rename(tmp,ap)
    hp = build_html_output_path(out, tr)
    rh = render_test_html(td)
    tmp2=hp+".tmp"
    with open(tmp2,"w") as f: f.write(rh)
    os.rename(tmp2,hp)
    print(f"  [w{wid}] ✅ HTML({len(rh):,}B)+AI({len(json.dumps(ae)):,}B): {title[:45]}")
    return "OK"


async def run_scraper(chunk_file, output_dir, workers=1):
    with open(chunk_file) as f: chunk = json.load(f)
    tests = chunk.get("tests",[]); jn = chunk.get("job_number",1)
    print(f"\n{'='*60}\nPRO Job {jn} | Tests:{len(tests)} | Browser fetch + ZenRows\n{'='*60}\n")
    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth()
    if not auth.is_token_valid():
        if not auth.login_via_zenrows():
            print("❌ Login failed"); return

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36", viewport={"width":1366,"height":900})
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

        done=fail=0
        for i, ti in enumerate(tests):
            if auth.stop(): break
            r = await scrape_test(page, auth, ti, output_dir, 1)
            if r == "STOP": break
            elif r == "OK": done += 1
            else: fail += 1
            if i < len(tests) - 1:
                await asyncio.sleep(20 + random.uniform(0, 5))
        await browser.close()

    prog = {"job":jn,"total":len(tests),"scraped":done,"failed":fail,"at":datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir,"pro_progress.json"),"w") as f: json.dump(prog,f,indent=2)
    print(f"\n{'='*60}\n✅ Job {jn}: {done} scraped, {fail} failed\n{'='*60}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chunk", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--workers", type=int, default=1)
    a = p.parse_args()
    asyncio.run(run_scraper(a.chunk, a.output_dir, a.workers))
if __name__ == "__main__": main()
