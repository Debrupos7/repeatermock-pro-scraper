#!/usr/bin/env python3
"""RepeaterMock PRO Test Scraper — same format as free scraper.

Uses the free scraper's parsing + rendering functions for identical output.
Only difference: navigates to solution pages with PRO auth cookies (no API calls).
"""
import argparse, asyncio, json, os, re, sys, time, random
from datetime import datetime, timezone
from typing import Optional

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Import free scraper's functions
sys.path.insert(0, os.path.dirname(__file__))
from free_scraper_module import (
    parse_test_data, render_ai_export, render_test_html,
    build_ai_export_path, build_html_output_path,
    find_props_in_flight, build_text_refs, extract_images_from_html,
    TestRef, sanitize_filename
)

WEB_BASE = "https://repeatermock.com"
MAX_AUTH_FAILURES = 5

INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};const origReplaceState=history.replaceState;history.replaceState=function(state,title,url){if(typeof url==='string'&&url.indexOf('about:blank')===0)return;return origReplaceState.call(this,state,title,url)};const origPushState=history.pushState;history.pushState=function(state,title,url){if(typeof url==='string'&&url.indexOf('about:blank')===0)return;return origPushState.call(this,state,title,url)};const origWrite=document.write.bind(document);document.write=function(html){if(typeof html==='string'&&html.length<500)return;return origWrite(html)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};console.debug=function(){};console.info=function(){};console.trace=function(){};console.group=function(){};console.groupEnd=function(){};console.groupCollapsed=function(){};const origEval=window.eval;window.eval=function(code){if(typeof code==='string'&&code.indexOf('debugger')>=0){code=code.replace(/\bdebugger\b/g,'void 0')}return origEval.call(this,code)};const origSetTimeout=window.setTimeout;window.setTimeout=function(fn,delay,...args){if(typeof fn==='string'&&fn.indexOf('debugger')>=0){fn=fn.replace(/\bdebugger\b/g,'void 0')}return origSetTimeout.call(this,fn,delay,...args)};const origSetInterval=window.setInterval;window.setInterval=function(fn,delay,...args){if(typeof fn==='string'&&fn.indexOf('debugger')>=0){fn=fn.replace(/\bdebugger\b/g,'void 0')}return origSetInterval.call(this,fn,delay,...args)};Object.defineProperty(navigator,"webdriver",{get:()=>undefined});})();"""


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

    def should_stop(self):
        return self.auth_failures >= MAX_AUTH_FAILURES


async def scrape_pro_test(browser, auth, test_info, output_dir, worker_id):
    """Scrape a PRO test — same format as free scraper."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series_slug = test_info.get("series_slug", "")
    series_name = test_info.get("series_name", series_slug)
    section = test_info.get("section", "")
    subsection = test_info.get("subsection", "")

    if auth.should_stop():
        return "STOP"

    # Build TestRef (same as free scraper)
    test_ref = TestRef(
        test_id=tid,
        title=title,
        series_slug=series_slug,
        series_name=series_name,
        section_id="",
        section_name=section or "Uncategorized",
        sub_section_id="",
        sub_section_name=subsection or "Default",
        is_free=False,
        duration=0,
        question_count=0,
        total_mark=0,
    )

    solution_url = f"{WEB_BASE}/tb/test-series/{series_slug}/test/{tid}/solution"
    print(f"  [worker {worker_id}] loading: {title[:50]}... (id={tid})")

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
        await page.goto(solution_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(6000)

        if "about:blank" in page.url or "login" in page.url:
            print(f"  [worker {worker_id}] ❌ redirected — token expired")
            auth.auth_failures += 1
            return None

        # Get HTML via DOM (same as free scraper v4)
        result = await page.evaluate("""(function(){var h=document.documentElement.outerHTML;window.__HTML__=h;return{len:h.length,hasTestData:h.indexOf('testData')>=0,hasAnswersData:h.indexOf('answersData')>=0}})()""")
        if not result or not result.get("hasTestData"):
            result = await page.evaluate("(function(){return fetch(window.location.href,{credentials:'include'}).then(r=>r.text()).then(t=>{window.__HTML__=t;return{len:t.length,hasTestData:t.indexOf('testData')>=0,hasAnswersData:t.indexOf('answersData')>=0}}).catch(e=>({err:String(e).slice(0,200)}))})()")

        # Extract in 200KB chunks (same as free scraper)
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

        if not html or "testData" not in html or "answersData" not in html:
            print(f"  [worker {worker_id}] ❌ no testData (len={len(html)})")
            return None

        # Parse flight data (SAME as free scraper)
        props = find_props_in_flight(html)
        if not props:
            print(f"  [worker {worker_id}] ❌ couldn't find props in flight data")
            return None
        text_refs = build_text_refs(html)
        test_data = parse_test_data(props, tid, series_slug, text_refs)
        
        # Update title from scraped data (more accurate)
        if test_data.title:
            test_ref.title = test_data.title

        # Render + save AI export (SAME format as free scraper)
        ai_path = build_ai_export_path(output_dir, test_ref)
        ai_export = render_ai_export(test_data, test_ref)
        tmp_ai = ai_path + ".tmp"
        with open(tmp_ai, "w") as f:
            json.dump(ai_export, f, ensure_ascii=False, indent=2)
        os.rename(tmp_ai, ai_path)

        # Render + save HTML export (SAME format as free scraper — interactive mock test)
        html_path = build_html_output_path(output_dir, test_ref)
        rendered_html = render_test_html(test_data)
        tmp_html = html_path + ".tmp"
        with open(tmp_html, "w") as f:
            f.write(rendered_html)
        os.rename(tmp_html, html_path)

        print(f"  [worker {worker_id}] ✅ saved HTML ({len(rendered_html):,}B) + AI JSON ({len(json.dumps(ai_export)):,}B): {title[:50]}")
        return "OK"
    except Exception as e:
        print(f"  [worker {worker_id}] ❌ error: {e}")
        return None
    finally:
        await context.close()


async def run_scraper(chunk_file, output_dir, workers=2):
    with open(chunk_file) as f:
        chunk_data = json.load(f)
    tests = chunk_data.get("tests", [])
    job_number = chunk_data.get("job_number", 1)
    print(f"\n{'='*60}\nPRO Scraper — Job {job_number}\nTests: {len(tests)} | Workers: {workers}\nSame format as free scraper\n{'='*60}\n")

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuthManager()

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])

        completed = failed = 0
        queue = asyncio.Queue()
        for t in tests: await queue.put(t)

        async def worker_loop(wid):
            nonlocal completed, failed
            while not queue.empty():
                if auth.should_stop():
                    print(f"  [worker {wid}] ⛔ auth stop")
                    break
                try: test_info = queue.get_nowait()
                except asyncio.QueueEmpty: break
                result = await scrape_pro_test(browser, auth, test_info, output_dir, wid)
                if result == "STOP": break
                elif result == "OK": completed += 1
                else: failed += 1
                await asyncio.sleep(2.0 + random.uniform(0, 1.0))

        worker_tasks = [asyncio.create_task(worker_loop(i+1)) for i in range(min(workers, 2))]
        await asyncio.gather(*worker_tasks)
        await browser.close()

    progress = {"job_number": job_number, "total_tests": len(tests), "scraped": completed, "failed": failed, "completed_at": datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(output_dir, "pro_progress.json"), "w") as f: json.dump(progress, f, indent=2)
    print(f"\n{'='*60}\n✅ Job {job_number}: {completed} scraped, {failed} failed\n{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    asyncio.run(run_scraper(args.chunk, args.output_dir, args.workers))

if __name__ == "__main__":
    main()
