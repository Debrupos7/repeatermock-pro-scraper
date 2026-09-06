#!/usr/bin/env python3
"""PRO Auth Manager — handles login, token refresh, and re-login.

Login flow:
1. Try Playwright (headless Chrome + anti-self-destruct INIT_SCRIPT)
2. Fallback: Scrapfly (renders with real Chrome via API)
3. Fallback: ScrapingBee (renders with stealth proxy)

Token refresh:
- accessToken expires every 15 minutes
- Call /auth/refresh with refreshToken cookie → get new accessToken
- refreshToken expires every 30 days → triggers re-login

Re-login with 7-fail stop:
- If login fails 7 times consecutively, stop ALL workers
- Commits auth state to pro_auth.json in repo
"""
import asyncio
import base64
import json
import os
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

LOGIN_URL = "https://repeatermock.com/login"
LOGIN_API = "https://api.repeatermock.com/auth/login"
ME_API = "https://api.repeatermock.com/auth/me"
REFRESH_API = "https://api.repeatermock.com/auth/refresh"

# Anti-self-destruct INIT_SCRIPT (same as free scraper)
INIT_SCRIPT = r"""
(function(){
  window.close = function() {}; console.clear = function() {}; window.stop = function() {};
  try {
    const origReplace = window.location.replace.bind(window.location);
    window.location.replace = function(u) { if (u && String(u).indexOf('about:blank') === 0) return; return origReplace(u); };
    const origAssign = window.location.assign.bind(window.location);
    window.location.assign = function(u) { if (u && String(u).indexOf('about:blank') === 0) return; return origAssign(u); };
  } catch(e){}
  try {
    const locDesc = Object.getOwnPropertyDescriptor(window.Location.prototype, 'href');
    if (locDesc && locDesc.set) {
      const origSetter = locDesc.set;
      Object.defineProperty(window.Location.prototype, 'href', {
        get: locDesc.get, set: function(v) { if (typeof v === 'string' && v.indexOf('about:blank') === 0) return; return origSetter.call(this, v); }, configurable: true,
      });
    }
  } catch(e){}
  const origOpen = window.open;
  window.open = function(u, ...rest) { if (typeof u === 'string' && (u.indexOf('about:blank') === 0 || u === '')) return null; return origOpen.call(this, u, ...rest); };
  const origReplaceState = history.replaceState;
  history.replaceState = function(state, title, url) { if (typeof url === 'string' && url.indexOf('about:blank') === 0) return; return origReplaceState.call(this, state, title, url); };
  const origPushState = history.pushState;
  history.pushState = function(state, title, url) { if (typeof url === 'string' && url.indexOf('about:blank') === 0) return; return origPushState.call(this, state, title, url); };
  const origWrite = document.write.bind(document);
  document.write = function(html) { if (typeof html === 'string' && html.length < 500) return; return origWrite(html); };
  window.addEventListener('beforeunload', function(e) { e.stopImmediatePropagation(); e.preventDefault(); e.returnValue = ''; return ''; }, true);
  console.log = function() {}; console.table = function() {}; console.dir = function() {}; console.debug = function() {}; console.info = function() {}; console.trace = function() {}; console.group = function() {}; console.groupEnd = function() {}; console.groupCollapsed = function() {};
  const origEval = window.eval;
  window.eval = function(code) { if (typeof code === 'string' && code.indexOf('debugger') >= 0) { code = code.replace(/\bdebugger\b/g, 'void 0'); } return origEval.call(this, code); };
  const origSetTimeout = window.setTimeout;
  window.setTimeout = function(fn, delay, ...args) { if (typeof fn === 'string' && fn.indexOf('debugger') >= 0) { fn = fn.replace(/\bdebugger\b/g, 'void 0'); } return origSetTimeout.call(this, fn, delay, ...args); };
  const origSetInterval = window.setInterval;
  window.setInterval = function(fn, delay, ...args) { if (typeof fn === 'string' && fn.indexOf('debugger') >= 0) { fn = fn.replace(/\bdebugger\b/g, 'void 0'); } return origSetInterval.call(this, fn, delay, ...args); };
  Object.defineProperty(navigator, "webdriver", {get: () => undefined});
})();
"""

MAX_LOGIN_FAILURES = 7  # Stop all workers after 7 consecutive login failures
login_failure_count = 0


class PROAuthManager:
    """Manages PRO account authentication: login, refresh, re-login."""

    def __init__(self, email: str, password: str, scrapfly_key: str = "", scrapingbee_key: str = ""):
        self.email = email
        self.password = password
        self.scrapfly_key = scrapfly_key
        self.scrapingbee_key = scrapingbee_key
        self.refresh_token = ""
        self.access_token = ""
        self.access_token_expires_at = 0  # Unix timestamp
        self.auth_file = "pro_auth.json"
        self._load_auth()

    def _load_auth(self):
        """Load existing auth from pro_auth.json."""
        if os.path.exists(self.auth_file):
            try:
                with open(self.auth_file) as f:
                    data = json.load(f)
                self.refresh_token = data.get("refreshToken", "")
                self.access_token = data.get("accessToken", "")
                self.access_token_expires_at = data.get("accessToken_expires_at", 0)
                print(f"  [auth] loaded existing auth: refreshToken={'✅' if self.refresh_token else '❌'}, accessToken={'✅' if self.access_token else '❌'}")
            except Exception as e:
                print(f"  [auth] WARNING: couldn't load {self.auth_file}: {e}")

    def _save_auth(self):
        """Save auth to pro_auth.json."""
        data = {
            "email": self.email,
            "refreshToken": self.refresh_token,
            "accessToken": self.access_token,
            "accessToken_expires_at": self.access_token_expires_at,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        tmp = self.auth_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.rename(tmp, self.auth_file)
        print(f"  [auth] saved auth to {self.auth_file}")

    def is_access_token_valid(self) -> bool:
        """Check if accessToken is still valid (expires in >60 seconds)."""
        if not self.access_token:
            return False
        return time.time() < (self.access_token_expires_at - 60)  # 60s safety margin

    async def refresh_access_token(self) -> bool:
        """Refresh accessToken using refreshToken. Returns True if successful."""
        if not self.refresh_token:
            print("  [auth] no refreshToken — need full login")
            return False
        print("  [auth] refreshing accessToken...")
        try:
            body = json.dumps({}).encode()
            req = urllib.request.Request(REFRESH_API, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Cookie", f"refreshToken={self.refresh_token}")
            req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            if data.get("success") and data.get("accessToken"):
                self.access_token = data["accessToken"]
                self.access_token_expires_at = time.time() + (15 * 60)  # 15 minutes
                self._save_auth()
                print(f"  [auth] ✅ accessToken refreshed (valid for 15 min)")
                return True
            else:
                print(f"  [auth] refresh failed: {data.get('message', '?')}")
                return False
        except Exception as e:
            print(f"  [auth] refresh error: {e}")
            return False

    async def get_valid_access_token(self) -> Optional[str]:
        """Get a valid accessToken. Refreshes if expired. Returns None if all fails."""
        if self.is_access_token_valid():
            return self.access_token
        # Try refresh
        if await self.refresh_access_token():
            return self.access_token
        # Refresh failed — need full re-login
        if await self.login():
            return self.access_token
        return None

    async def login_via_scrapfly(self) -> Optional[str]:
        """Login via Scrapfly API — renders login page with real Chrome + extracts Turnstile token."""
        if not self.scrapfly_key:
            return None
        print("  [auth] trying Scrapfly login...")
        try:
            # Use Scrapfly to render the login page + extract Turnstile token via js_scenario
            js_scenario = json.dumps({
                "instructions": [
                    {"wait": 10000},
                    {"evaluate": "document.querySelector('[name=cf-turnstile-response]') ? document.querySelector('[name=cf-turnstile-response]').value : ''"}
                ]
            })
            js_scenario_b64 = base64.b64encode(js_scenario.encode()).decode()
            params = urllib.parse.urlencode({
                "key": self.scrapfly_key,
                "url": LOGIN_URL,
                "render_js": "true",
                "asp": "true",
                "rendering_wait": "25000",
            })
            url = f"https://api.scrapfly.io/scrape?{params}&js_scenario={js_scenario_b64}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=120) as r:
                data = json.loads(r.read().decode())
            content = data.get("result", {}).get("content", "")
            # Check JS eval result
            js_eval = data.get("result", {}).get("browser_data", {}).get("javascript_evaluation_result")
            if js_eval and isinstance(js_eval, str) and len(js_eval) > 20:
                print(f"  [auth] Scrapfly: Turnstile token from JS eval (len={len(js_eval)})")
                return js_eval
            # Check HTML
            for pat in [r'name="cf-turnstile-response"[^>]*value="([^"]+)"',
                        r'value="([^"]+)"[^>]*name="cf-turnstile-response"']:
                m = re.search(pat, content)
                if m and len(m.group(1)) > 20:
                    print(f"  [auth] Scrapfly: Turnstile token from HTML (len={len(m.group(1))})")
                    return m.group(1)
            print("  [auth] Scrapfly: no Turnstile token found")
            return None
        except Exception as e:
            print(f"  [auth] Scrapfly error: {e}")
            return None

    async def login_via_playwright(self) -> Optional[str]:
        """Login via Playwright — opens real browser, solves Turnstile, extracts tokens."""
        from playwright.async_api import async_playwright
        print("  [auth] trying Playwright login...")
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"]
                )
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                    viewport={"width": 1366, "height": 900},
                    locale="en-US",
                )
                await context.add_init_script(INIT_SCRIPT)
                page = await context.new_page()
                await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(10000)  # Wait for Turnstile

                # Wait for Turnstile token (up to 60s)
                token = None
                for attempt in range(20):
                    await page.wait_for_timeout(3000)
                    token = await page.evaluate("""
                        () => {
                            const input = document.querySelector('[name="cf-turnstile-response"]');
                            return (input && input.value && input.value.length > 20) ? input.value : null;
                        }
                    """)
                    if token:
                        print(f"  [auth] Playwright: Turnstile solved after {(attempt+1)*3}s!")
                        break
                    if attempt % 5 == 0:
                        print(f"  [auth] Playwright: waiting for Turnstile... ({(attempt+1)*3}s)")

                if not token:
                    print("  [auth] Playwright: Turnstile didn't solve")
                    await browser.close()
                    return None

                # Login via API (from browser context — same cookies)
                result = await page.evaluate(f"""
                    (async function() {{
                        const r = await fetch('{LOGIN_API}', {{
                            method: 'POST', credentials: 'include',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{email: '{self.email}', password: '{self.password}', turnstileToken: '{token}'}})
                        }});
                        return await r.json();
                    }})()
                """)

                if result.get("success"):
                    # Get cookies (including HttpOnly ones)
                    cookies = await context.cookies()
                    for c in cookies:
                        if 'refresh' in c['name'].lower():
                            self.refresh_token = c['value']
                        if 'access' in c['name'].lower():
                            self.access_token = c['value']
                    self.access_token_expires_at = time.time() + (15 * 60)
                    self._save_auth()
                    print(f"  [auth] ✅ Playwright login successful! refreshToken={'✅' if self.refresh_token else '❌'}, accessToken={'✅' if self.access_token else '❌'}")
                    await browser.close()
                    return self.access_token
                else:
                    print(f"  [auth] Playwright login failed: {result.get('message', '?')}")
                    await browser.close()
                    return None
        except Exception as e:
            print(f"  [auth] Playwright error: {e}")
            return None

    async def login(self) -> bool:
        """Full login flow. Tries Playwright first, then Scrapfly. Returns True if successful."""
        global login_failure_count
        print(f"  [auth] starting login flow (failure count: {login_failure_count}/{MAX_LOGIN_FAILURES})...")

        if login_failure_count >= MAX_LOGIN_FAILURES:
            print(f"  [auth] ⛔ MAX_LOGIN_FAILURES ({MAX_LOGIN_FAILURES}) reached — STOPPING all workers")
            return False

        # Try Playwright first
        token = await self.login_via_playwright()
        if not token:
            # Fallback: Scrapfly
            token = await self.login_via_scrapfly()
            if token:
                # Use Scrapfly's token to login via API
                token = await self._login_with_token(token)

        if token:
            login_failure_count = 0  # Reset on success
            return True
        else:
            login_failure_count += 1
            print(f"  [auth] login failed (count: {login_failure_count}/{MAX_LOGIN_FAILURES})")
            if login_failure_count >= MAX_LOGIN_FAILURES:
                print(f"  [auth] ⛔ MAX_LOGIN_FAILURES reached — STOPPING")
            return False

    async def _login_with_token(self, turnstile_token: str) -> Optional[str]:
        """Login using a Turnstile token (from Scrapfly or other source)."""
        try:
            body = json.dumps({
                "email": self.email,
                "password": self.password,
                "turnstileToken": turnstile_token
            }).encode()
            req = urllib.request.Request(LOGIN_API, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Origin", "https://repeatermock.com")
            req.add_header("Referer", "https://repeatermock.com/login")
            req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode())
                set_cookies = r.headers.get_all("Set-Cookie") or []
            if data.get("success"):
                for sc in set_cookies:
                    parts = sc.split(";")[0].split("=", 1)
                    if len(parts) == 2:
                        name = parts[0].strip()
                        value = parts[1].strip()
                        if 'refresh' in name.lower():
                            self.refresh_token = value
                        if 'access' in name.lower():
                            self.access_token = value
                self.access_token_expires_at = time.time() + (15 * 60)
                self._save_auth()
                print(f"  [auth] ✅ token-based login successful!")
                return self.access_token
            else:
                print(f"  [auth] token login failed: {data.get('message', '?')}")
                return None
        except Exception as e:
            print(f"  [auth] token login error: {e}")
            return None

    def get_cookie_header(self) -> str:
        """Get Cookie header for API requests."""
        cookies = []
        if self.access_token:
            cookies.append(f"accessToken={self.access_token}")
        if self.refresh_token:
            cookies.append(f"refreshToken={self.refresh_token}")
        return "; ".join(cookies)

    def should_stop(self) -> bool:
        """Check if we should stop all workers (max login failures reached)."""
        return login_failure_count >= MAX_LOGIN_FAILURES
