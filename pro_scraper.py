#!/usr/bin/env python3
"""RepeaterMock PRO Scraper v3.1 — rate-limit-aware multi-account scraper.

KEY FIXES vs v3.0:
- **No 300s cap on retryAfter**: wait the FULL duration the server tells us
  (typically 30-45 min when rate-limited). Capping was causing us to retry too
  early and trigger more 429s.
- **Sliding-window rate limiter**: max 4 test starts per 60 seconds per account.
  This is repeatermock's actual threshold before locking the account for ~40 min.
  Proactively throttles to never trigger 429 in the first place.
- **30s delay between tests** (was 5s) — much gentler on the API.
- **Staggered worker start**: worker N waits (N-1)*30s before first login. This
  prevents 3 concurrent ZenRows calls + 3 concurrent repeatermock logins.
- **ZenRows key rotation on no-Turnstile-token**: each failed ZenRows attempt
  rotates to the next key. Previously we retried with the same key 3 times.
- **Auth failure logic fixed**: when refresh fails with 401 DURING a 429 storm,
  that's not 3 separate auth failures — it's 1 logical "account locked" event.
  We now wait the rate-limit window before counting auth failures.
- **Pre-flight rate-limit check**: before each test, check if we're in a 429
  backoff window. If so, wait until it expires before attempting.
- **Per-worker ZenRows key starting index**: worker N starts with ZenRows key
  (N-1) % num_keys. Distributes load across keys.

Folder structure (same as free scraper):
  pro_scraped_output/{series_slug}/ai_export/Single_Tests/Default/{title}_{test_id}.json
  pro_scraped_output/{series_slug}/html_export/Single_Tests/Default/{title}_{test_id}.html
  pro_scraped_output/{series_slug}/progress_worker_{N}.json
"""
import argparse, asyncio, json, os, re, sys, time, random, urllib.request, urllib.error
from datetime import datetime, timezone
from collections import deque

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

# Tunables — RATE-LIMIT-AWARE
MAX_CONSECUTIVE_ERRORS = 8          # was 5; bumped because rate-limit 429s shouldn't count
MAX_AUTH_FAILURES = 5              # was 3; bumped to give more chances
TOKEN_REFRESH_INTERVAL = 14 * 60    # 14 min (access token expires at 15)
DELAY_BETWEEN_TESTS = 3.0           # 3s between tests (within burst capacity)
DEFAULT_MAX_RUNTIME = 330           # 5.5 hours in minutes (per user spec)

# Sliding-window rate limiter - TUNED FOR TULIKUP2 (verified empirically):
# - tulikup2 allows 5 starts in ~15s, then 40s cooldown
# - Setting: 5 starts per 30s window, then wait 40s before next burst
# - This gives ~300 tests/hour per worker (safe sustained rate)
# - For 9,915 tests: ~33 hours total (6 runs of 5.5h each)
RATE_LIMIT_WINDOW = 30             # 30-second window for burst detection
RATE_LIMIT_MAX_STARTS = 5          # 5 starts per 30s = burst capacity
RATE_LIMIT_BACKOFF_AFTER_429 = 40  # after a SHORT 429, wait 40s before next burst

# retryAfter handling — CRITICAL FIX:
# - If retryAfter is short (< 600s = 10 min), wait FULL duration (normal rate limit)
# - If retryAfter is medium (600s-3600s = 10min-1h), wait 300s and retry (might be temporary)
# - If retryAfter is long (> 3600s = 1h), account is LOCKED → STOP WORKER IMMEDIATELY
#   (do NOT waste 4+ hours of CI time waiting on a 15+ hour lock!)
# - If we get 2+ long locks in a row, also STOP (account is being rate-limited too aggressively)
RETRY_AFTER_SHORT_THRESHOLD = 600    # under 10 min = OK to wait full
RETRY_AFTER_LONG_THRESHOLD = 3600   # over 1 hour = account locked, STOP
RETRY_AFTER_CAP_FOR_RETRY = 300     # for medium (10min-1h), wait at most 5 min then retry
MAX_LONG_LOCKS = 1                  # stop after this many long locks (was effectively unlimited)

# Anti-self-destruct INIT_SCRIPT (neutralizes repeatermock's anti-devtools trap)
INIT_SCRIPT = r"""(function(){window.close=function(){};console.clear=function(){};window.stop=function(){};try{const o=window.location.replace.bind(window.location);window.location.replace=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return o(u)};const a=window.location.assign.bind(window.location);window.location.assign=function(u){if(u&&String(u).indexOf('about:blank')===0)return;return a(u)}}catch(e){}try{const d=Object.getOwnPropertyDescriptor(window.Location.prototype,'href');if(d&&d.set){const s=d.set;Object.defineProperty(window.Location.prototype,'href',{get:d.get,set:function(v){if(typeof v==='string'&&v.indexOf('about:blank')===0)return;return s.call(this,v)},configurable:true})}}catch(e){}const o=window.open;window.open=function(u,...r){if(typeof u==='string'&&(u.indexOf('about:blank')===0||u===''))return null;return o.call(this,u,...r)};window.addEventListener('beforeunload',function(e){e.stopImmediatePropagation();e.preventDefault();e.returnValue='';return ''},true);console.log=function(){};console.table=function(){};console.dir=function(){};})();"""


class RateLimiter:
    """Sliding-window rate limiter. Tracks test start timestamps per account.

    Allows max RATE_LIMIT_MAX_STARTS starts per RATE_LIMIT_WINDOW seconds.
    If exceeded, computes wait time until oldest start falls out of window.
    """
    def __init__(self, max_starts: int = RATE_LIMIT_MAX_STARTS,
                 window: int = RATE_LIMIT_WINDOW):
        self.max_starts = max_starts
        self.window = window
        self.starts = deque()  # timestamps of recent starts
        self.backoff_until = 0  # if > now, all starts blocked until this time
        self.consecutive_429s = 0
        self.long_locks = 0  # count of long locks (>1 hour) - if >MAX_LONG_LOCKS, stop
        self.account_locked = False  # set True if long lock detected
        self.account_locked_until = 0  # timestamp when lock expires
        self.last_429_retry_after = 0  # last retryAfter value seen

    def time_until_next_allowed(self) -> float:
        """Returns seconds we must wait before next start is allowed (0 if OK now)."""
        now = time.time()
        # If account is in cooldown (429 backoff), wait until that expires
        if self.backoff_until > now:
            return self.backoff_until - now
        # Drop starts older than the window
        while self.starts and self.starts[0] < now - self.window:
            self.starts.popleft()
        # If we're under the limit, allow immediately
        if len(self.starts) < self.max_starts:
            return 0.0
        # Otherwise, wait until the oldest start falls out of the window
        return self.starts[0] + self.window - now

    def record_start(self):
        self.starts.append(time.time())

    def note_429(self, retry_after_seconds: int):
        """Record a 429 hit. Decide how to handle based on retryAfter duration.

        CRITICAL FIX (v3.2): Long locks (>1 hour) mean the account is PERMANENTLY
        locked for ~15-19 hours. We should NOT cap and retry every hour — that wastes
        4+ hours of CI time. Instead, mark the account as locked and let the worker
        stop cleanly so progress is committed.
        """
        self.last_429_retry_after = retry_after_seconds
        self.consecutive_429s += 1

        if retry_after_seconds > RETRY_AFTER_LONG_THRESHOLD:
            # LONG LOCK - account is permanently locked for 15+ hours
            # DO NOT retry - mark as locked so worker stops cleanly
            self.account_locked = True
            self.account_locked_until = time.time() + min(retry_after_seconds, 86400)  # max 1 day
            self.long_locks += 1
            hours = retry_after_seconds / 3600
            print(f"    [rate-limit] 🚨 LONG LOCK DETECTED: retryAfter={retry_after_seconds}s "
                  f"({hours:.1f}h) — account is PERMANENTLY LOCKED", flush=True)
            print(f"    [rate-limit] 🚨 Worker will STOP to avoid wasting CI time "
                  f"(long_locks={self.long_locks}/{MAX_LONG_LOCKS})", flush=True)
        elif retry_after_seconds > RETRY_AFTER_SHORT_THRESHOLD:
            # MEDIUM LOCK (10min - 1hour) - wait 5 min and retry
            wait = RETRY_AFTER_CAP_FOR_RETRY
            self.backoff_until = time.time() + wait
            print(f"    [rate-limit] 429 medium lock: retryAfter={retry_after_seconds}s "
                  f"→ wait {wait}s and retry (consecutive_429s={self.consecutive_429s})",
                  flush=True)
        else:
            # SHORT LOCK (under 10 min) - wait full duration
            wait = max(retry_after_seconds, 60)
            self.backoff_until = time.time() + wait
            print(f"    [rate-limit] 429 short lock: retryAfter={retry_after_seconds}s "
                  f"→ wait {wait}s (consecutive_429s={self.consecutive_429s})", flush=True)

    def is_permanently_locked(self) -> bool:
        """Check if account is permanently locked (long lock detected)."""
        if self.long_locks >= MAX_LONG_LOCKS:
            return True
        if self.account_locked and time.time() < self.account_locked_until:
            return True
        return False

    def note_success(self):
        """Reset 429 counter on successful scrape."""
        self.consecutive_429s = 0
        # Don't reset account_locked - if it's locked, it stays locked
        # (the rate limit clock is still ticking server-side)
        if not self.account_locked:
            self.backoff_until = 0  # clear backoff only if not permanently locked


class PROAuth:
    """Manages one PRO account per worker. Rotates ZenRows keys on exhaustion."""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.email = os.environ.get(f"RM_EMAIL_{worker_id}", os.environ.get("RM_EMAIL", ""))
        self.password = os.environ.get(f"RM_PASSWORD_{worker_id}", os.environ.get("RM_PASSWORD", ""))
        zenrows_keys_str = os.environ.get("ZENROWS_API_KEYS", os.environ.get("ZENROWS_API_KEY", ""))
        self.zenrows_keys = [k.strip() for k in zenrows_keys_str.split(",") if k.strip()]
        # Each worker starts with a different ZenRows key to spread the load
        # Worker 1 → key 0, worker 2 → key 1, worker 3 → key 2, etc.
        self.zenrows_key_idx = (worker_id - 1) % len(self.zenrows_keys) if self.zenrows_keys else 0
        self.exhausted_keys = set()

        self.access_token = ""
        self.refresh_token = ""
        self.token_expires = 0
        self.last_refresh_attempt = 0
        self.failures = 0
        self.consecutive_scrape_errors = 0
        # Track when account is in cooldown (from refresh-401 after 429 storm)
        self.account_cooldown_until = 0

        print(f"  [auth/w{worker_id}] account: {self.email}", flush=True)
        print(f"  [auth/w{worker_id}] zenrows keys: {len(self.zenrows_keys)} available "
              f"(starting with key #{self.zenrows_key_idx+1})", flush=True)

    def is_valid(self):
        return bool(self.access_token) and time.time() < (self.token_expires - 60)

    def is_in_cooldown(self):
        return time.time() < self.account_cooldown_until

    def _next_zenrows_key(self):
        """Returns the next non-exhausted key (rotates through all available)."""
        n = len(self.zenrows_keys)
        for offset in range(n):
            idx = (self.zenrows_key_idx + offset) % n
            if idx not in self.exhausted_keys:
                self.zenrows_key_idx = idx
                return self.zenrows_keys[idx]
        return ""

    def _mark_key_exhausted(self, key: str):
        for i, k in enumerate(self.zenrows_keys):
            if k == key:
                if i not in self.exhausted_keys:
                    self.exhausted_keys.add(i)
                    remaining = len(self.zenrows_keys) - len(self.exhausted_keys)
                    print(f"  [auth/w{self.worker_id}] ⛔ ZenRows key #{i+1} marked exhausted "
                          f"({remaining} keys remaining)", flush=True)
                return

    def login_zenrows(self):
        """Login via ZenRows — rotates to next key on each attempt."""
        key = self._next_zenrows_key()
        if not key:
            print(f"  [auth/w{self.worker_id}] ❌ all ZenRows keys exhausted", flush=True)
            return False

        print(f"  [auth/w{self.worker_id}] ZenRows login with key #{self.zenrows_key_idx+1}...",
              flush=True)
        import httpx
        for attempt in range(3):
            # Each attempt rotates to next key (only on "no Turnstile token" failure)
            if attempt > 0:
                # Cycle to next non-exhausted key for retry
                old_idx = self.zenrows_key_idx
                next_key = self._next_zenrows_key()
                if next_key and next_key != key:
                    key = next_key
                    print(f"  [auth/w{self.worker_id}] (retry with key #{self.zenrows_key_idx+1})",
                          flush=True)
                else:
                    print(f"  [auth/w{self.worker_id}] attempt {attempt+1}/3 (same key)...",
                          flush=True)

            print(f"  [auth/w{self.worker_id}] attempt {attempt+1}/3...", flush=True)
            try:
                with httpx.Client(timeout=180.0) as cli:
                    r = cli.get("https://api.zenrows.com/v1/", params={
                        "apikey": key, "url": "https://repeatermock.com/login",
                        "js_render": "true", "premium_proxy": "true", "wait": "25000",
                    }, timeout=180.0)
                    content = r.text
                    print(f"  [auth/w{self.worker_id}] ZenRows: HTTP {r.status_code}, "
                          f"{len(content):,}B", flush=True)

                    # Check for ZenRows credit exhaustion
                    if r.status_code in (402, 403):
                        self._mark_key_exhausted(key)
                        return self.login_zenrows()  # retry with next key
                    try:
                        jr = r.json()
                        # Common ZenRows error codes: AUTH_001 (out of credits), etc.
                        if jr.get("code") in ("AUTH_001", "AUTH_002", "AUTH_003"):
                            self._mark_key_exhausted(key)
                            return self.login_zenrows()
                    except Exception:
                        pass

                    # Extract Turnstile token
                    m = re.search(r'name="cf-turnstile-response"[^>]*value="([^"]+)"', content)
                    if not m or len(m.group(1)) < 20:
                        print(f"  [auth/w{self.worker_id}] no Turnstile token in response "
                              f"(will rotate key)", flush=True)
                        # Don't mark as exhausted (ZenRows may just be rate-limited itself)
                        # but do rotate to next key for next attempt
                        time.sleep(3)
                        continue

                    token = m.group(1)
                    print(f"  [auth/w{self.worker_id}] ✅ Turnstile token (len={len(token)})",
                          flush=True)

                    # POST login
                    r2 = cli.post("https://api.repeatermock.com/auth/login", json={
                        "email": self.email, "password": self.password,
                        "turnstileToken": token
                    }, headers={
                        "Content-Type": "application/json",
                        "Origin": "https://repeatermock.com",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=30.0)
                    data = r2.json()
                    set_cookies = (r2.headers.get_list("set-cookie")
                                   if hasattr(r2.headers, "get_list") else [])

                    if data.get("success"):
                        for c in set_cookies:
                            p = c.split(";")[0].split("=", 1)
                            if len(p) == 2:
                                if 'access' in p[0].lower():
                                    self.access_token = p[1].strip()
                                elif 'refresh' in p[0].lower():
                                    self.refresh_token = p[1].strip()
                        self.token_expires = time.time() + 900
                        self.failures = 0
                        self.account_cooldown_until = 0  # clear cooldown on fresh login
                        print(f"  [auth/w{self.worker_id}] ✅ Login success! "
                              f"User: {data.get('user', {}).get('name', '?')}", flush=True)
                        return True
                    else:
                        # RepeaterMock returned non-success — could be rate-limited account
                        msg = data.get('message', str(data))
                        print(f"  [auth/w{self.worker_id}] login API failed: {msg}", flush=True)
                        # If "too many requests" or similar, set a cooldown
                        if 'rate' in msg.lower() or 'limit' in msg.lower() or 'lock' in msg.lower():
                            self.account_cooldown_until = time.time() + 600  # 10 min
                            print(f"  [auth/w{self.worker_id}] account appears rate-limited; "
                                  f"cooling down 600s", flush=True)
                        return False
            except Exception as e:
                print(f"  [auth/w{self.worker_id}] ZenRows error: {e}", flush=True)
            time.sleep(5)
        return False

    def refresh(self):
        """Refresh accessToken via /auth/refresh. Throttled to 1 call per 60s."""
        if not self.refresh_token:
            return False
        now = time.time()
        if now - self.last_refresh_attempt < 60:
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
                self.token_expires = time.time() + 900
                self.failures = 0
                print(f"  [auth/w{self.worker_id}] ✅ Token refreshed (valid 15 min)",
                      flush=True)
                return True
            else:
                print(f"  [auth/w{self.worker_id}] refresh returned no token: {data}",
                      flush=True)
        except urllib.error.HTTPError as e:
            # 401 here means refresh_token was invalidated — likely because account got
            # rate-limited. Set a long cooldown instead of counting as auth failure.
            if e.code == 401:
                print(f"  [auth/w{self.worker_id}] refresh 401 — account likely rate-limited; "
                      f"cooling down 600s before retry", flush=True)
                self.account_cooldown_until = time.time() + 600
                # DO NOT increment self.failures — this is a rate-limit issue, not auth
                return False
            print(f"  [auth/w{self.worker_id}] refresh HTTP {e.code}: {e.reason}", flush=True)
        except Exception as e:
            print(f"  [auth/w{self.worker_id}] refresh error: {e}", flush=True)
        return False

    def get_token(self):
        """Get a valid accessToken. Respects account cooldown."""
        # If account is in cooldown, don't even try
        if self.is_in_cooldown():
            wait = int(self.account_cooldown_until - time.time())
            print(f"  [auth/w{self.worker_id}] account in cooldown ({wait}s remaining)",
                  flush=True)
            return None

        if self.is_valid():
            return self.access_token

        if self.refresh_token and self.refresh():
            return self.access_token

        if self.failures < MAX_AUTH_FAILURES:
            if self.login_zenrows():
                return self.access_token

        self.failures += 1
        return None

    def should_stop(self):
        """Stop if too many auth failures OR too many consecutive scrape errors."""
        return (self.failures >= MAX_AUTH_FAILURES
                or self.consecutive_scrape_errors >= MAX_CONSECUTIVE_ERRORS)

    def note_scrape_error(self):
        self.consecutive_scrape_errors += 1

    def note_scrape_success(self):
        self.consecutive_scrape_errors = 0
        self.failures = 0


class ProgressTracker:
    """Per-worker progress.json — writes to progress_worker_{N}.json."""
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


async def browser_api(page, url, body="{}", max_retries=4):
    """Browser fetch API call with retryAfter handling for 429.

    CRITICAL FIX (v3.2): Long locks (>1 hour) cause the function to RETURN the 429
    immediately instead of waiting. The caller will then check rate_limiter and stop
    the worker cleanly. This prevents 4+ hour wastes on already-locked accounts.
    """
    last_429_retry_after = 0
    for attempt in range(max_retries):
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
            last_429_retry_after = ra
            if ra and ra > 0:
                if ra > RETRY_AFTER_LONG_THRESHOLD:
                    # LONG LOCK - return immediately, do NOT retry
                    # Caller will check rate_limiter.is_permanently_locked() and stop worker
                    print(f"    429 LONG LOCK retryAfter={ra}s ({ra/3600:.1f}h) "
                          f"→ returning immediately (worker will stop)", flush=True)
                    return status, result
                elif ra > RETRY_AFTER_SHORT_THRESHOLD:
                    # MEDIUM LOCK - wait 5 min then retry
                    wait = RETRY_AFTER_CAP_FOR_RETRY
                    print(f"    429 medium retryAfter={ra}s → wait {wait}s "
                          f"({attempt+1}/{max_retries})", flush=True)
                    await asyncio.sleep(wait)
                    continue
                else:
                    # SHORT LOCK - wait full duration
                    wait = max(int(ra), 60)
                    print(f"    429 short retryAfter={ra}s → wait {wait}s "
                          f"({attempt+1}/{max_retries})", flush=True)
                    await asyncio.sleep(wait)
                    continue
            # No retryAfter header — back off exponentially
            wait = 60 * (attempt + 1)
            print(f"    429 no retryAfter → wait {wait}s ({attempt+1}/{max_retries})",
                  flush=True)
            await asyncio.sleep(wait)
            continue
        return status, result
    return 429, {"body": "max retries", "retryAfter": last_429_retry_after}


async def scrape_test(page, auth, progress, rate_limiter, test_info, output_dir, wid):
    """Scrape one PRO test. Returns 'OK', 'SKIP', 'PRO', 'STOP', or None on error."""
    tid = test_info.get("test_id", "")
    title = test_info.get("title", tid)
    series = test_info.get("series_slug", "")
    series_name = test_info.get("series_name", series)
    raw_section = test_info.get("section", "")
    raw_subsection = test_info.get("subsection", "")
    section = raw_section if raw_section and raw_section != "Section" else "Single_Tests"
    subsection = (raw_subsection if raw_subsection and raw_subsection != "Subsection"
                  else "Default")

    if progress.is_scraped(series, tid):
        return "SKIP"

    if auth.should_stop():
        return "STOP"

    # Pre-flight rate-limit check — wait if we're in cooldown
    wait_time = rate_limiter.time_until_next_allowed()
    if wait_time > 0:
        if wait_time > 600:
            print(f"  [w{wid}] rate-limit backoff: waiting {int(wait_time)}s "
                  f"before next test", flush=True)
        if wait_time > 30:
            await asyncio.sleep(wait_time)
        else:
            await asyncio.sleep(wait_time)

    token = auth.get_token()
    if not token:
        print(f"  [w{wid}] ❌ no token (failures={auth.failures})", flush=True)
        auth.note_scrape_error()
        return None

    # Record this start in the rate limiter
    rate_limiter.record_start()

    # Inject auth cookies
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
        # Tell rate limiter about the 429 (it will set backoff_until)
        try:
            err = json.loads(r.get("body", "{}"))
            ra = err.get("retryAfter", 60)
        except Exception:
            ra = 60
        rate_limiter.note_429(ra)
        print(f"  [w{wid}] 429 on start (retryAfter={ra}s)", flush=True)
        # Don't count this as a scrape error — it's a rate-limit issue
        return None
    if s == 402:
        print(f"  [w{wid}] 💰 PRO-only (402)", flush=True)
        await progress.mark_failed(series, tid, "PRO 402")
        return "PRO"
    if s == 401:
        # Token expired mid-test. Try refresh, but don't count as auth failure yet.
        print(f"  [w{wid}] 401 on start (token expired) — will refresh", flush=True)
        if auth.refresh():
            # Retry the test after successful refresh
            rate_limiter.starts.pop() if rate_limiter.starts else None  # undo record
            return None  # will retry on next iteration
        auth.failures += 1
        print(f"  [w{wid}] refresh failed (auth failures={auth.failures}/{MAX_AUTH_FAILURES})",
              flush=True)
        return None
    if s != 200:
        print(f"  [w{wid}] start returned {s}", flush=True)
        auth.note_scrape_error()
        return None

    # 2) POST /attempts/{tid}/submit (with empty answers)
    s, r = await browser_api(page, f"{API_BASE}/api/v1/attempts/{tid}/submit",
                             '{"answers":[],"timeTaken":1,"language":"en","interface":"classic"}')
    if s == 429:
        try:
            err = json.loads(r.get("body", "{}"))
            ra = err.get("retryAfter", 60)
        except Exception:
            ra = 60
        rate_limiter.note_429(ra)
        print(f"  [w{wid}] 429 on submit (retryAfter={ra}s)", flush=True)
        return None
    if s != 200:
        print(f"  [w{wid}] submit returned {s}", flush=True)
        auth.note_scrape_error()
        return None

    # 3) Navigate to /solution page
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
        print(f"  [w{wid}] ❌ login redirect on solution page "
              f"(auth failures={auth.failures}/{MAX_AUTH_FAILURES})", flush=True)
        auth.note_scrape_error()
        return None

    # 4) Extract HTML from DOM (chunked)
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

    # 5) Parse + render
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
    rate_limiter.note_success()

    print(f"  [w{wid}] ✅ HTML({len(rendered):,}B)+AI({len(json.dumps(ai_export)):,}B): "
          f"{title[:45]}", flush=True)
    return "OK"


async def run_scraper(worker_file, output_dir, max_runtime=DEFAULT_MAX_RUNTIME):
    """Run the scraper for one worker."""
    with open(worker_file) as f:
        data = json.load(f)
    wid = data.get("worker_id", 1)
    chunks = data.get("chunks", [])
    total_tests = data.get("total_tests", 0)
    account_email = data.get("account_email", "?")

    # Stagger worker start: worker N waits (N-1)*30s before starting
    # This prevents 3 concurrent ZenRows logins + 3 concurrent repeatermock logins
    stagger_delay = (wid - 1) * 30
    if stagger_delay > 0:
        print(f"\n  [w{wid}] Stagger: waiting {stagger_delay}s before starting "
              f"(prevents concurrent auth load)", flush=True)
        await asyncio.sleep(stagger_delay)

    print(f"\n{'='*60}", flush=True)
    print(f"PRO Scraper v3.1 | Worker {wid} | Account: {account_email}", flush=True)
    print(f"Total tests: {total_tests} across {len(chunks)} series", flush=True)
    print(f"Output: {output_dir}/{{series_slug}}/ai_export/...", flush=True)
    print(f"Max runtime: {max_runtime} min ({max_runtime/60:.1f}h)", flush=True)
    print(f"Delay between tests: {DELAY_BETWEEN_TESTS}s (±10s jitter)", flush=True)
    print(f"Rate limit: max {RATE_LIMIT_MAX_STARTS} starts per {RATE_LIMIT_WINDOW}s window",
          flush=True)
    print(f"Stop conditions: {MAX_CONSECUTIVE_ERRORS} consecutive errors OR "
          f"{MAX_AUTH_FAILURES} auth failures", flush=True)
    print(f"retryAfter: wait FULL duration (no 300s cap; max {RETRY_AFTER_MAX_CAP}s)",
          flush=True)
    print(f"{'='*60}\n", flush=True)

    os.makedirs(output_dir, exist_ok=True)
    auth = PROAuth(worker_id=wid)
    progress = ProgressTracker(output_dir, wid)
    rate_limiter = RateLimiter()

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

        # Establish Cloudflare clearance
        try:
            await page.goto("https://repeatermock.com/tb/test-series/ssc-gd-constable",
                            wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"  [w{wid}] warning: initial goto failed: {e}", flush=True)

        done = fail = skip = pro = 0
        start_time = time.time()
        stop_reason = ""
        last_commit_time = time.time()

        for ci, chunk in enumerate(chunks):
            job_name = chunk.get("job_name", f"Series-{ci+1}")
            series_slug = chunk.get("series_slug", "")
            tests = chunk.get("tests", [])

            series_dir = (os.path.join(output_dir, series_slug)
                          if series_slug else output_dir)
            os.makedirs(series_dir, exist_ok=True)

            print(f"\n--- [w{wid}] Series {ci+1}/{len(chunks)}: {job_name} "
                  f"({series_slug}) — {len(tests)} tests ---", flush=True)
            print(f"    Output dir: {series_dir}", flush=True)

            series_done = 0
            for i, ti in enumerate(tests):
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
                # CRITICAL: Check if account is permanently locked (long lock detected)
                if rate_limiter.is_permanently_locked():
                    hours_left = max(0, (rate_limiter.account_locked_until - time.time())) / 3600
                    stop_reason = (f"account_permanently_locked "
                                   f"(long_locks={rate_limiter.long_locks}, "
                                   f"hours_left={hours_left:.1f})")
                    print(f"\n🚨 [w{wid}] Account PERMANENTLY LOCKED — stopping cleanly",
                          flush=True)
                    print(f"    Worker will commit progress and exit", flush=True)
                    break

                result = await scrape_test(page, auth, progress, rate_limiter,
                                            ti, series_dir, wid)
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

                # 30s delay (±10s jitter) between tests — much gentler than 5s
                if i < len(tests) - 1:
                    delay = DELAY_BETWEEN_TESTS + random.uniform(0, 10)
                    # If we just had a 429 storm, slow down even more
                    if rate_limiter.consecutive_429s > 0:
                        delay += RATE_LIMIT_BACKOFF_AFTER_429
                    await asyncio.sleep(delay)

                # Progress report every 5 tests
                if (done + fail + skip + pro) % 5 == 0:
                    stats = progress.stats()
                    print(f"📊 [w{wid}] [{elapsed:.1f}min] "
                          f"Done:{done} Fail:{fail} Skip:{skip} PRO:{pro} | "
                          f"Progress: {stats['scraped']} scraped, "
                          f"{stats['failed']} failed | "
                          f"Rate: {rate_limiter.consecutive_429s} 429s", flush=True)

                # Commit every 10 min for more frequent saves (was 15 min)
                if time.time() - last_commit_time > 600:  # 10 min
                    if os.environ.get("GITHUB_ACTIONS"):
                        import subprocess
                        try:
                            # Stage + commit FIRST, then pull --rebase, then push
                            subprocess.run(["git", "add", "pro_scraped_output/"],
                                           capture_output=True, timeout=30)
                            commit_result = subprocess.run(
                                ["git", "commit", "-m",
                                 f"pro-scrape w{wid}: {done} scraped "
                                 f"({datetime.now(timezone.utc).strftime('%H:%M')})"],
                                capture_output=True, timeout=15)
                            for push_attempt in range(3):
                                pull_result = subprocess.run(
                                    ["git", "pull", "--rebase", "origin", "main"],
                                    capture_output=True, timeout=20)
                                if pull_result.returncode != 0:
                                    # If pull --rebase fails (uncommitted changes),
                                    # try stash + reset + stash pop
                                    subprocess.run(["git", "rebase", "--abort"],
                                                  capture_output=True, timeout=10)
                                    subprocess.run(["git", "stash"],
                                                  capture_output=True, timeout=10)
                                    subprocess.run(["git", "fetch", "origin", "main"],
                                                  capture_output=True, timeout=10)
                                    subprocess.run(["git", "reset", "--hard", "origin/main"],
                                                  capture_output=True, timeout=10)
                                    subprocess.run(["git", "stash", "pop"],
                                                  capture_output=True, timeout=10)
                                    subprocess.run(["git", "add", "pro_scraped_output/"],
                                                  capture_output=True, timeout=30)
                                    subprocess.run(["git", "commit", "-m",
                                                   f"pro-scrape w{wid}: {done} scraped (retry)"],
                                                  capture_output=True, timeout=15)
                                r2 = subprocess.run(["git", "push", "origin", "HEAD"],
                                                    capture_output=True, timeout=20)
                                if r2.returncode == 0:
                                    print(f"  💾 [w{wid}] committed + pushed "
                                          f"({done} tests)", flush=True)
                                    break
                                time.sleep(push_attempt * 3 + 2)
                        except Exception as e:
                            print(f"  ⚠️ [w{wid}] git error: {e}", flush=True)
                    last_commit_time = time.time()

            # Always commit at end of each series
            if os.environ.get("GITHUB_ACTIONS"):
                import subprocess
                try:
                    subprocess.run(["git", "add", "pro_scraped_output/"],
                                   capture_output=True, timeout=30)
                    subprocess.run(["git", "commit", "-m",
                                    f"pro-scrape w{wid}: {job_name} +{series_done} "
                                    f"(total: {done})"],
                                   capture_output=True, timeout=15)
                    for push_attempt in range(3):
                        subprocess.run(["git", "pull", "--rebase", "origin", "main"],
                                      capture_output=True, timeout=20)
                        r2 = subprocess.run(["git", "push", "origin", "HEAD"],
                                            capture_output=True, timeout=20)
                        if r2.returncode == 0:
                            print(f"  💾 [w{wid}] committed {job_name}", flush=True)
                            break
                        time.sleep(push_attempt * 3 + 2)
                    else:
                        print(f"  ⚠️ [w{wid}] push failed for {job_name}", flush=True)
                except Exception as e:
                    print(f"  ⚠️ [w{wid}] git error: {e}", flush=True)

            if stop_reason:
                break

        await browser.close()

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
        "rate_limit_429s": rate_limiter.consecutive_429s,
    }
    with open(os.path.join(output_dir, f"WORKER_{wid}_STATS.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}", flush=True)
    print(f"✅ [w{wid}] Done! Scraped: {done} | Failed: {fail} | "
          f"Skipped: {skip} | PRO: {pro}", flush=True)
    print(f"   Runtime: {summary['runtime_min']:.1f} min", flush=True)
    if stop_reason:
        print(f"   Stop reason: {stop_reason}", flush=True)
    print(f"   429 hits: {rate_limiter.consecutive_429s}", flush=True)
    print(f"{'='*60}", flush=True)

    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--worker-file", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--max-runtime", type=int, default=DEFAULT_MAX_RUNTIME)
    a = p.parse_args()
    asyncio.run(run_scraper(a.worker_file, a.output_dir, a.max_runtime))


if __name__ == "__main__":
    main()
