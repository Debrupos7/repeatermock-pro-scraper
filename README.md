# RepeaterMock PRO Scraper

Scrapes PRO (paid) tests from RepeaterMock using a paid account.

## How it works

1. **Distribute**: Reads PRO_TESTS.json from the free scraper repo, splits into 20 equal chunks
2. **Login**: Each job logs in via Playwright (or Scrapfly fallback) to get auth tokens
3. **Scrape**: Each job scrapes its chunk of PRO tests using the auth tokens
4. **Merge**: Combines all results into PRO_STATS.json

## Auth flow

- **accessToken**: 15-minute expiry, refreshed via /auth/refresh
- **refreshToken**: 30-day expiry, used to get new accessTokens
- **Re-login**: When refreshToken expires, one job re-logs in + commits pro_auth.json to repo
- **7-fail stop**: If login fails 7 times consecutively, all workers stop

## Setup

1. Add repository secrets:
   - `RM_EMAIL` — RepeaterMock PRO account email
   - `RM_PASSWORD` — RepeaterMock PRO account password
   - `SCRAPFLY_API_KEY` — Scrapfly API key (for Turnstile bypass fallback)

2. Trigger workflow:
   - Go to Actions -> "Scrape RepeaterMock PRO Tests" -> Run workflow
   - For test mode: set max_tests to 200 (10 tests per job)
   - For production: leave empty (all ~10,000 PRO tests)

## Files

- `pro_auth.py` — Auth manager (login, refresh, re-login with 7-fail stop)
- `pro_scraper.py` — PRO test scraper (uses auth tokens + DOM extraction)
- `distribute_pro_tests.py` — Splits PRO_TESTS.json into 20 chunks
- `.github/workflows/scrape-pro-matrix.yml` — 20-job matrix workflow
