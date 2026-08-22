# New x402 Listings Feed

Feed of x402/L402 services newly listed on 402index.io within a caller-specified recency window. NEXUS
candidate #8 -- **manual build, not FORGE-generated**, same manual-Cloud-Run-asset pattern as candidates #3
(`agent-verification-api`), #4 (`url-metadata-api`) and #6 (`document-conversion-api`).

- `POST /new-x402-listings {"window_hours": 24, "protocol": null, "category": null, "payment_network": null}`
  -- services registered on 402index.io within the window (1-168h, default 24). **$0.01/call.**
- MCP tool `get_new_x402_listings` at `/mcp`, same params -- **currently free**, see "Known limitations".
- `GET /health`, `GET /.well-known/agent-card.json`, `GET /openapi.json` (has `x-payment-info`),
  `GET /.well-known/402index-verify.txt` (402index claim verification file).

## What this is (and isn't)

**This is not exclusive data.** The registration data underneath this asset is 402index.io's own free,
public directory (`https://402index.io/api-docs`, no auth needed, 100 req/min free tier). Anyone can query
it directly for nothing. This asset's value is entirely in the packaging: polling and paginating the ~96k+
entry catalog so a buyer doesn't have to, merging in 402index's own recency feed for freshness, deduping,
and filtering to a caller's window/protocol/category/payment-network. This disclosure is not just in this
README -- it's baked into the product itself: every response includes a `note` field stating it plainly, and
the agent-card's `protocol_note` repeats it, so a buyer never has to dig for it.

## Feasibility research (2026-08-23, before writing any code)

The task brief's premise cited a prior-session figure of "~7,595 total services" on 402index.io. A real
`curl https://402index.io/api/v1/services?limit=5` at the start of this session showed `"total": 96093` --
the catalog grew roughly 12x since that check. `registered_at` is real and populated on every entry sampled.
Two upstream mechanisms were verified live, each with a real limitation neither the task brief nor
402index's own docs fully surfaced:

1. **`GET /api/v1/services`** (paginated, `limit`/`offset`, max 200/page) has NO sort-by-date or date-range
   filter -- `sort` only accepts `name`/`price`/`latency`/`uptime`/`reliability` (checked `/api-docs`
   directly). A window query can only be answered by walking the *entire* catalog and filtering
   client-side -- there is no cheaper server-side path. At the real current size that's ~481 pages, not the
   ~40 pages the ~7,595 figure would have implied.
2. **`GET /feed.xml?type=new`** is real, RSS 2.0, and IS already recency-sorted (`pubDate`) -- and is on
   402index's own documented rate-limit-exempt list. But it's capped to a fixed item count: a live fetch
   during this session returned exactly 90 items spanning only ~3 hours, despite the docs describing
   `type=new` as "services added in the last 7 days". At current registration velocity it does not cover a
   7-day (or even a 24h, at peak velocity) window by itself.

Neither source alone honestly satisfies the product's advertised window range. **Decision: proceed, using
both.** See the `main.py` module docstring and "Architecture" below for how they're combined. This is the
kind of real, current-data discrepancy CLAUDE.md SS3 asks to surface rather than build past silently -- so it's
recorded here rather than only in a chat transcript.

## Architecture

- A background `asyncio` task (started in FastAPI's `lifespan`, not blocking startup) walks the full
  `/api/v1/services` catalog every `NEXUS_CATALOG_REFRESH_SECONDS` (default 600s/10min), paced at
  0.65s/request (~92 req/min sustained, safety margin under 402index's 100 req/min free-tier cap). Results
  are cached in memory (`_catalog_cache`), keyed by service id. A full walk at the current catalog size is
  ~481 pages / ~5.2 minutes -- entirely a background-task cost, **never** inline in a buyer's request.
- Every buyer request additionally fetches `/feed.xml?type=new` live (rate-limit-exempt, ~1 request, cheap)
  and merges it into whatever the background walk has cached, catching anything registered after the last
  completed walk. Cache entries win on id conflict (richer fields); feed entries only fill gaps.
- Response is filtered/deduped/sorted (newest first) from that merge, capped at 500 results.

### Why not a literal TTL cache (deviation from the task brief's suggested shape)

The brief suggested "a short in-memory cache (5-10 min TTL)". Implemented instead: a continuously-running
background loop that re-walks every 10 minutes and replaces the cache, with buyer requests always reading
whatever is currently cached (never triggering a walk themselves). A literal on-demand-when-stale TTL would
mean whichever buyer request happens to arrive right after expiry pays $0.01 and then blocks for up to ~5
minutes waiting on a fresh 481-page walk -- unacceptable buyer experience found during design, not
retroactively. The background-loop shape gets the same "don't hammer 402index.io" goal without ever making a
paying caller wait on the walk.

### Cold start (Cloud Run scale-to-zero)

`min-instances=0` means a fresh container starts with an empty cache. The first request(s) after any period
of inactivity get `catalog_walk.status: "cold_fallback_feed_only"` -- results are limited to whatever
`/feed.xml?type=new` currently holds (recently observed to span only a few hours), not the full requested
window. This is disclosed in the response's own `note` field, not hidden. Once the background task's first
walk completes (~5 minutes after container start), subsequent requests get `status: "warm"` with full
catalog-walk coverage. Genuinely mitigated, not solved -- see "Known limitations".

## Pricing: $0.01/call (low tier)

Speculative niche per the product owner (explicit direction, not a data-driven conclusion) -- same low tier
as `url-metadata-api`/`document-conversion-api`'s $0.01-$0.02, not `agent-verification-api`'s $0.35 signal
tier. No paid third-party API cost, pure CPU/memory + one free upstream fetch per call.

## Pre-deploy quality gate (2026-08-23, from design not retroactive)

Reviewed across 4 lenses before first deploy, tested against the real live 402index.io API (not mocked) at
each step. Real findings and fixes:

- **Security**: caller input (`window_hours`/`protocol`/`category`/`payment_network`) is never interpolated
  into the upstream 402index.io request -- the catalog walk and feed fetch use fixed, hardcoded query params
  only (`limit`/`offset`/`sort`/`order` and `type=new` respectively); caller filters apply exclusively to
  the already-fetched, normalized in-memory result. Verified upstream JSON/XML is never trusted blindly:
  every field read via `.get()` with type checks (`_normalize_catalog_item` returns `None` on a malformed
  entry rather than raising), malformed XML from `/feed.xml` is caught (`ET.ParseError`) rather than
  crashing the request, and a single malformed `<item>` in the feed doesn't drop the rest of it. All
  confirmed with real malformed-input tests (`None`, missing id, wrong types, garbage timestamp, truncated
  XML) during this session, not just read as correct.
- **Functional correctness (real gap, fixed)**: the initial design cached `complete: bool` internally but
  never surfaced it in the response -- a caller had no way to tell a background walk that hit the
  `_WALK_MAX_WALL_SECONDS` wall-clock cap (bailing out with a partial result) from one that finished cleanly,
  even though both report `status: "warm"`. Fixed: added `catalog_walk.walk_complete` to the response.
  Pagination itself (offset advance, stop-at-total, wall-cap bailout) was verified against the real live API
  with a bounded test walk (8 real pages, offset advancing correctly, graceful bailout logged) -- no silent
  truncation, no infinite loop.
- **Code quality**: no dead code or unused imports found in review; matches sibling assets' structure
  (Supabase telemetry helpers, x402 wiring, discovery routes) ported by hand, consistent with how
  `document-conversion-api`/`url-metadata-api` are built.
- **Buyer experience (the "raw data is free elsewhere" disclosure lens)**: the fact that 402index.io's data
  is itself free was verified present in 3 places, not just the README: the agent-card `protocol_note`, and
  -- more directly, so no buyer has to fetch the agent-card at all -- every single API response's own `note`
  field. Cold-cache degraded coverage (`cold_fallback_feed_only`) is also disclosed in that same `note`
  field with plain language about what it means for the requested window, not just a status enum a caller
  has to already know how to interpret.

## Known limitations (left as documented tradeoffs, not silently)

- **MCP tool calls are not charged.** Same in-process-call pattern as the sibling manual assets.
- **No per-caller rate limiting.** Fine for a 7-day disposable measurement window.
- **Cold-cache window under-coverage is mitigated, not solved.** A request landing in the first ~5 minutes
  after a Cloud Run cold start gets feed-only coverage (recently observed ~3h span for a 90-item feed at
  current 402index velocity), not the full requested window, even though it's charged the same $0.01. This
  is disclosed live in the response, but a buyer who calls once, right after a cold start, and doesn't read
  `catalog_walk.status`/`note` could reasonably read a short result list as "nothing new" rather than
  "cache still warming up". A future fix could hold `min-instances=1` to eliminate cold starts entirely, at
  a real always-on Cloud Run cost -- not done for a 7-day probation candidate.
- **`registered_at` is assumed UTC.** 402index's `/api/v1/services` returns naive timestamps
  (`"2026-08-22 22:13:15"`, no offset) -- treated as UTC based on consistency with `last_checked` values
  observed live during this session, not a documented guarantee from 402index.io itself.
- **Full-catalog walk pacing assumes today's catalog size.** At ~96k entries a full walk is ~481
  pages/~5.2min, safely inside the 10-minute refresh interval. If 402index's catalog keeps growing at the
  ~12x-since-last-check rate observed this session, a future walk could approach or exceed the 10-minute
  refresh interval (it would then just take longer between the visible "last_full_walk_at" updates, not
  break -- `_WALK_MAX_WALL_SECONDS` bails a single cycle out at 900s and `walk_complete` reports it
  honestly) -- not re-engineered preemptively per CLAUDE.md SS3 (no gate without evidence it's needed yet).

## NEXUS_X402_FREE_MODE

Default `false` (charges from day 1, no freemium window) -- same convention as `document-conversion-api`.
Set `true` locally for testing without a real facilitator round-trip.

## Deploy target: Cloud Run, not Railway

Same pipeline as candidates #3/#4/#6 -- see `skills/infra-deploy-ops`. No PDF/Office parsing here, so the
shared script's default 512Mi is used (not bumped to 1Gi like `document-conversion-api`).

```bash
# 1. First deploy -- PUBLIC_DOMAIN not known yet, every real request 421s until step 2.
./scripts/deploy_cloud_run.sh new-x402-listings-feed manual_assets/new-x402-listings-feed \
    manual_assets/new-x402-listings-feed/env-vars.deploy.yaml

# 2. Grab the printed *.run.app URL, then:
gcloud run services update new-x402-listings-feed --region us-central1 --project nexus-505016 \
    --update-env-vars PUBLIC_DOMAIN=<the-real-domain>
```

## Measurement (candidate #8, 7-day window)

7-day window from first real deploy (2026-08-23 -> decision point 2026-08-30). Source of truth:
`traffic_events`/`revenue_events`/`mcp_call_events` tables (`asset_name = 'new-x402-listings-feed'`), not
Cloud Run logs. Day 7: if zero real traffic (filtering crawlers), pause/delete the Cloud Run service, same
decision rule as candidates #3/#4/#6. This is explicitly the most speculative of the 4 manual candidates
(product owner's own framing) -- a niche discovery feed for a still-nascent x402 ecosystem, not a proven
recurring-demand shape like document conversion or link previews.
