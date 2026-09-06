#!/usr/bin/env python3
"""RepeaterMock PRO Scraper — NO API calls, direct solution page navigation.

No /attempts/start or /attempts/submit → NO 429 rate limiting.
Just navigate to solution page with auth cookies → get HTML with testData.
"""
import argparse, asyncio, json, os, re, sys, time, random
from datetime import datetime, timezone

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

sys.path.insert(0, os.path.dirname(__file__))
from free_scraper_module import (
    parse_test_data, render_ai_export, render_test_html,
    build_ai_export_path, build_html_output_path,
    find_props_in_flight, build_text_refs, TestRef
)

WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 3

INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""


class PROAuth:
    def __init__(self):
        c = os.environ.get("PRO_COOKIES", "")
        self.access_token = ""
        self.refresh_token = os.environ.get("PRO_REFRESH_TOKEN", "")
        self.failures = 0
        if c:
            m = re.search(r'accessToken=([^;]+)', c)
            if m: self.access_token = m.group(1)
            m = re.search(r'refreshToken=([^;]+)', c)
            if m: self.refresh_token = m.group(1)
        print(f"  [auth] accessToken: {'✅' if self.access_token else '❌'} | refreshToken: {'✅' if self.refresh_token else '❌'}")
    def stop(self):
        return self.failures >= MAX_AUTH_FAILURES


async def scrape_test(browser, auth, ti, out, wid):
    tid = ti.get("test_id","")
    title = ti.get("title", tid)
    series = ti.get("series_slug","")
    section = ti.get("section","Uncategorized")
    subsection = ti.get("subsection","Default")
    if auth.stop(): return "STOP"

    url = f"{WEB_BASE}/tb/test-series/{series}/test/{tid}/solution"
    print(f"  [w{wid}] {title[:45]}... ({tid[:8]})")

    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        viewport={"width":1366,"height":900},
    )
    await ctx.add_cookies([
        {"name":"accessToken","value":auth.access_token,"domain":".repeatermock.com","path":"/"},
        {"name":"refreshToken","value":auth.refresh_token,"domain":".repeatermock.com","path":"/"},
        {"name":"totpVerified","value":"1","domain":".repeatermock.com","path":"/"},
    ])
    await ctx.add_init_script(INIT_SCRIPT)
    page = await ctx.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)
        if "login" in page.url or "about:blank" in page.url:
            auth.failures += 1
            print(f"  [w{wid}] ❌ login redirect ({auth.failures}/{MAX_AUTH_FAILURES})")
            return None

        r = await page.evaluate("(function(){var h=document.documentElement.outerHTML;window.__HTML__=h;return{len:h.length,td:h.indexOf('testData')>=0,ad:h.indexOf('answersData')>=0}})()")
        if not r or not r.get("td"):
            r = await page.evaluate("(function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{len:t.length,td:t.indexOf('testData')>=0,ad:t.indexOf('answersData')>=0}}).catch(e=>({err:String(e).slice(0,100)}))})()")
        tl = await page.evaluate("(function(){return(window.__HTML__||'').length})()")
        chunks, cs = [], 200000
        nc = min((tl//cs)+1, 100)
        for i in range(nc):
            s=i*cs
            if s>=tl: break
            c=await page.evaluate(f"(function(){{var h=window.__HTML__||'';if({s}>=h.length)return null;return h.slice({s},{s+cs})}})()")
            if c is None: break
            chunks.append(c)
        html="".join(chunks)
        if not html or "testData" not in html or "answersData" not in html:
            print(f"  [w{wid}] ❌ no testData (len={len(html)})")
            return None

        tr = TestRef(test_id=tid,title=title,series_slug=series,series_name=ti.get("series_name",series),
            section_id="",section_name=section,sub_section_id="",sub_section_name=subsection,
            is_free=False,duration=0,question_count=0,total_mark=0)
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
        print(f"  [w{wid}] ❌ {e}")
        return None
    finally:
        await ctx.close()


async def run_scraper(chunk_file, output_dir, workers=2):
    with open(chunk_file) as f: chunk = json.load(f)
    tests = chunk.get("tests",[])
    jn = chunk.get("job_number",1)
    print(f"\n{'='*60}\nPRO Job {jn} | Tests:{len(tests)} | Workers:{workers} | NO API\n{'='*60}\n")
    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth()
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        done=fail=0
        q=asyncio.Queue()
        for t in tests: await q.put(t)
        async def loop(wid):
            nonlocal done,fail
            while not q.empty():
                if auth.stop(): print(f"  [w{wid}] ⛔ stop"); break
                try: ti=q.get_nowait()
                except: break
                r=await scrape_test(browser,auth,ti,output_dir,wid)
                if r=="STOP": break
                elif r=="OK": done+=1
                else: fail+=1
                await asyncio.sleep(2.0+random.uniform(0,1.0))
        await asyncio.gather(*[asyncio.create_task(loop(i+1)) for i in range(min(workers,2))])
        await browser.close()
    prog={"job":jn,"total":len(tests),"scraped":done,"failed":fail,"at":datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir,"pro_progress.json"),"w") as f: json.dump(prog,f,indent=2)
    print(f"\n{'='*60}\n✅ Job {jn}: {done} scraped, {fail} failed\n{'='*60}")


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--chunk",required=True)
    p.add_argument("--output-dir",required=True)
    p.add_argument("--workers",type=int,default=2)
    a=p.parse_args()
    asyncio.run(run_scraper(a.chunk,a.output_dir,a.workers))
if __name__=="__main__": main()
