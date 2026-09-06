#!/usr/bin/env python3
"""RepeaterMock PRO Scraper — auto-login via ZenRows + sequential API calls."""
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
LOGIN_URL = "https://repeatermock.com/login"
LOGIN_API = "https://api.repeatermock.com/auth/login"
REFRESH_API = "https://api.repeatermock.com/auth/refresh"
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
            print(f"  [auth] from PRO_COOKIES: access={'✅' if self.access_token else '❌'} refresh={'✅' if self.refresh_token else '❌'}")

    def is_token_valid(self):
        return bool(self.access_token) and time.time() < (self.token_expires - 60)

    def login_via_zenrows(self):
        if not self.zenrows_key or not self.email or not self.password:
            print("  [auth] missing ZenRows/credentials"); return False
        print("  [auth] logging in via ZenRows...")
        import urllib.parse
        for attempt in range(3):
            params = urllib.parse.urlencode({"apikey": self.zenrows_key, "url": LOGIN_URL, "js_render": "true", "premium_proxy": "true", "wait": "25000"})
            try:
                req = urllib.request.Request(f"https://api.zenrows.com/v1/?{params}")
                with urllib.request.urlopen(req, timeout=120) as r:
                    content = r.read().decode()
                print(f"  [auth] ZenRows: {len(content):,} bytes")
            except Exception as e:
                print(f"  [auth] ZenRows error: {e}"); continue
            for pat in [r'name="cf-turnstile-response"[^>]*value="([^"]+)"', r'value="([^"]+)"[^>]*name="cf-turnstile-response"']:
                m = re.search(pat, content)
                if m and len(m.group(1)) > 20:
                    token = m.group(1)
                    print(f"  [auth] ✅ Turnstile solved! len={len(token)}")
                    body = json.dumps({"email": self.email, "password": self.password, "turnstileToken": token}).encode()
                    req = urllib.request.Request(LOGIN_API, data=body, method="POST")
                    req.add_header("Content-Type", "application/json")
                    req.add_header("Origin", "https://repeatermock.com")
                    req.add_header("Referer", "https://repeatermock.com/login")
                    req.add_header("User-Agent", "Mozilla/5.0")
                    try:
                        with urllib.request.urlopen(req, timeout=30) as r:
                            data = json.loads(r.read().decode())
                            sc = r.headers.get_all("Set-Cookie") or []
                        if data.get("success"):
                            for c in sc:
                                p = c.split(";")[0].split("=", 1)
                                if len(p) == 2:
                                    if 'access' in p[0].lower(): self.access_token = p[1].strip()
                                    elif 'refresh' in p[0].lower(): self.refresh_token = p[1].strip()
                            self.token_expires = time.time() + 900
                            self.failures = 0
                            u = data.get("user", {})
                            print(f"  [auth] ✅ Login! {u.get('name','?')} | Plan: {u.get('plan','?')}")
                            return True
                    except urllib.error.HTTPError as e:
                        print(f"  [auth] login failed: {e.code}"); return False
            print(f"  [auth] no token (attempt {attempt+1}/3)"); time.sleep(5)
        return False

    def refresh_access_token(self):
        if not self.refresh_token: return False
        print("  [auth] refreshing...")
        try:
            body = json.dumps({}).encode()
            req = urllib.request.Request(REFRESH_API, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Cookie", f"refreshToken={self.refresh_token}")
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            if data.get("success") and data.get("accessToken"):
                self.access_token = data["accessToken"]
                self.token_expires = time.time() + 900
                self.failures = 0
                print(f"  [auth] ✅ Refreshed!")
                return True
        except: pass
        return False

    def get_valid_token(self):
        if self.is_token_valid(): return self.access_token
        if self.refresh_access_token(): return self.access_token
        if self.failures < MAX_AUTH_FAILURES and self.login_via_zenrows(): return self.access_token
        self.failures += 1
        return None

    def cookie_header(self):
        return f"accessToken={self.access_token}; refreshToken={self.refresh_token}; totpVerified=1"

    def stop(self): return self.failures >= MAX_AUTH_FAILURES


def api_post(url, ch, body="{}", max_retries=3):
    for attempt in range(max_retries):
        req = urllib.request.Request(url, data=body.encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Cookie", ch)
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        req.add_header("Origin", "https://repeatermock.com")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 30 * (attempt + 1)
                print(f"    429 — wait {wait}s ({attempt+1}/{max_retries})")
                time.sleep(wait); continue
            try: err = json.loads(e.read().decode())
            except: err = {"error": str(e)}
            return e.code, err
        except Exception as e:
            if attempt < max_retries - 1: time.sleep(10); continue
            return 0, {"error": str(e)}
    return 429, {"error": "max retries"}


async def scrape_test(browser, auth, ti, out, wid):
    tid = ti.get("test_id",""); title = ti.get("title", tid); series = ti.get("series_slug","")
    section = ti.get("section","Uncategorized"); subsection = ti.get("subsection","Default")
    if auth.stop(): return "STOP"
    token = auth.get_valid_token()
    if not token: print(f"  [w{wid}] ❌ no token"); return None
    ch = auth.cookie_header()
    print(f"  [w{wid}] {title[:45]}... ({tid[:8]})")

    s, d = api_post(f"{API_BASE}/api/v1/attempts/{tid}/start", ch)
    if s == 429: print(f"  [w{wid}] 429"); return None
    if s == 402: print(f"  [w{wid}] 💰 PRO"); return "PRO"
    if s == 401: auth.failures += 1; print(f"  [w{wid}] 401 ({auth.failures}/{MAX_AUTH_FAILURES})"); return None
    if s != 200: print(f"  [w{wid}] start: {s}"); return None

    s, d = api_post(f"{API_BASE}/api/v1/attempts/{tid}/submit", ch, '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if s != 200: print(f"  [w{wid}] submit: {s}"); return None

    tr = TestRef(test_id=tid,title=title,series_slug=series,series_name=ti.get("series_name",series),
        section_id="",section_name=section,sub_section_id="",sub_section_name=subsection,
        is_free=False,duration=0,question_count=0,total_mark=0)
    ctx = await browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",viewport={"width":1366,"height":900})
    await ctx.add_cookies([{"name":"accessToken","value":auth.access_token,"domain":".repeatermock.com","path":"/"},{"name":"refreshToken","value":auth.refresh_token,"domain":".repeatermock.com","path":"/"},{"name":"totpVerified","value":"1","domain":".repeatermock.com","path":"/"}])
    await ctx.add_init_script(INIT_SCRIPT)
    page = await ctx.new_page()
    try:
        await page.goto(f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)
        if "login" in page.url: auth.failures += 1; print(f"  [w{wid}] ❌ login redirect"); return None
        r = await page.evaluate("(function(){var h=document.documentElement.outerHTML;window.__HTML__=h;return{len:h.length,td:h.indexOf('testData')>=0,ad:h.indexOf('answersData')>=0}})()")
        if not r or not r.get("td"):
            r = await page.evaluate("(function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{len:t.length,td:t.indexOf('testData')>=0,ad:t.indexOf('answersData')>=0}}).catch(e=>({err:String(e).slice(0,100)}))})()")
        tl = await page.evaluate("(function(){return(window.__HTML__||'').length})()")
        chunks,cs=[],200000
        nc=min((tl//cs)+1,100)
        for i in range(nc):
            s2=i*cs
            if s2>=tl:break
            c=await page.evaluate(f"(function(){{var h=window.__HTML__||'';if({s2}>=h.length)return null;return h.slice({s2},{s2+cs})}})()")
            if c is None:break
            chunks.append(c)
        html="".join(chunks)
        if not html or "testData" not in html or "answersData" not in html:
            print(f"  [w{wid}] ❌ no testData (len={len(html)})"); return None
        props = find_props_in_flight(html)
        if not props: print(f"  [w{wid}] ❌ no props"); return None
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
    except Exception as e:
        print(f"  [w{wid}] ❌ {e}"); return None
    finally:
        await ctx.close()


async def run_scraper(chunk_file, output_dir, workers=1):
    with open(chunk_file) as f: chunk = json.load(f)
    tests = chunk.get("tests",[]); jn = chunk.get("job_number",1)
    print(f"\n{'='*60}\nPRO Job {jn} | Tests:{len(tests)} | Workers:{workers} | ZenRows auto-login\n{'='*60}\n")
    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth()
    if not auth.is_token_valid():
        if not auth.login_via_zenrows():
            print("❌ Login failed — stopping"); return
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        done=fail=0
        for i, ti in enumerate(tests):
            if auth.stop(): print(f"  ⛔ stop"); break
            r = await scrape_test(browser, auth, ti, output_dir, 1)
            if r == "STOP": break
            elif r == "OK": done += 1
            else: fail += 1
            if i < len(tests) - 1:
                await asyncio.sleep(10 + random.uniform(0, 3))
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
