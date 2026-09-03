"""
new-x402-listings-feed -- NEXUS candidate #8 (manual build, no FORGE).

A recency feed of x402/L402 services newly listed on 402index.io (the
existing free, public paid-API directory this repo already has a proven
registration relationship with -- 3 sibling manual assets already did real
POST /api/v1/claim + verify-file registrations against it). Given a caller
window (default 24h, max 7d), returns the services registered_at within that
window, deduped and packaged so a buyer doesn't have to paginate ~96k raw
catalog entries themselves.

**Honesty disclosure baked into the product, not just the docs**: the
underlying registration data is 402index.io's own free public API
(https://402index.io/api-docs, no auth needed, 100 req/min free tier). This
asset does not have exclusive access to anything -- its value is (a)
periodic polling/pagination of the full catalog so a buyer gets one fast
call instead of walking ~480 pages themselves, (b) merging that with
402index's own recency feed for freshness between polls, (c) dedup +
window/protocol/category/payment_network filtering. See README "What this
is (and isn't)".

**Why two upstream sources, not one** (found during feasibility research,
2026-08-23):
1. `GET /api/v1/services` supports pagination (limit/offset, max 200/page)
   but NO sort-by-date or date-range filter -- `sort` only accepts
   name/price/latency/uptime/reliability. At the real catalog size observed
   this session (total=96,093, not the ~7,595 last-checked figure the task
   brief cited -- catalog grew ~12x since that check), a full walk is
   ~481 pages. That is real, current data (registered_at populated on every
   entry checked), just NOT date-sorted -- a caller-window query can only be
   answered by walking the WHOLE catalog and filtering client-side, there is
   no cheaper server-side path.
2. `GET /feed.xml?type=new` is real and IS already recency-sorted (RSS
   pubDate), and is explicitly exempt from 402index's own rate limit
   (see their docs' "Exempt from rate limiting" list) -- but it is capped to
   a fixed item count. Observed live: 90 items spanning only ~3 hours at
   current registration velocity, despite the docs describing it as "last 7
   days". It under-covers a 7-day (or even 24h, at peak velocity) window
   alone.

Neither source alone satisfies the product's advertised window range
honestly. This asset combines them: a background task walks the full
catalog periodically (paced well under the 100 req/min free-tier limit,
cached in memory) as the primary, complete-as-of-last-walk source, and
`/feed.xml?type=new` is fetched on every buyer request (free of the rate
limit, ~1 request) as a cheap top-up for anything registered after the last
completed walk. See README "Known limitations" for what this does NOT fully
solve (cold-cache-on-deploy coverage, walk-vs-feed staleness window).
"""

import asyncio
import base64
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Annotated, Literal, Optional
from xml.etree import ElementTree as ET

import httpx
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi as _fastapi_get_openapi
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field

from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer

_NEXUS_ASSET_NAME = "new-x402-listings-feed"

# ---------------------------------------------------------------
# 402index.io upstream config
# ---------------------------------------------------------------
_402INDEX_BASE = "https://402index.io"
_UPSTREAM_TIMEOUT = 10.0
_SERVICES_PAGE_LIMIT = 200
# Paces the full-catalog walk well under 402index's documented 100 req/min
# free tier (0.65s/request -> ~92 req/min sustained, safety margin below
# the hard limit). A full walk at total~96k is ~481 pages -> ~5.2 minutes;
# this ONLY runs in the background task below, never inline in a buyer
# request (see _get_new_listings).
_WALK_PAGE_DELAY_SECONDS = 0.65
_WALK_MAX_WALL_SECONDS = 900  # hard cap per walk cycle; if the catalog has
                                # grown further and one cycle runs long, bail
                                # out with whatever was collected rather than
                                # blocking the next scheduled cycle forever.
_WALK_REFRESH_INTERVAL_SECONDS = int(os.getenv("NEXUS_CATALOG_REFRESH_SECONDS", "600"))  # 10 min
_MAX_WINDOW_HOURS = 168  # 7 days -- matches feed.xml's own "type=new" window
_MAX_RESULTS = 500  # response size cap, defensive


def _parse_catalog_timestamp(raw) -> Optional[datetime]:
    """402index /api/v1/services `registered_at` is observed as
    'YYYY-MM-DD HH:MM:SS' with no timezone offset -- treated as UTC (their
    `last_checked`/`registered_at` fields are consistently naive-UTC across
    every entry sampled during feasibility research). Never raises -- a
    malformed/missing timestamp on one upstream record must not break the
    whole response."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _parse_rfc2822(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _normalize_catalog_item(svc: dict) -> Optional[dict]:
    """Defensive by construction: every field read via .get(), no assumption
    that upstream JSON has a fixed shape (functional-correctness lens of the
    pre-deploy quality gate -- upstream is a third party, its schema is not
    a contract we control)."""
    if not isinstance(svc, dict):
        return None
    sid = svc.get("id")
    if not sid:
        return None
    return {
        "id": str(sid),
        "name": str(svc.get("name") or svc.get("url") or "")[:300],
        "url": svc.get("url"),
        "category": svc.get("category"),
        "protocol": svc.get("protocol"),
        "price_usd": svc.get("price_usd"),
        "price_sats": svc.get("price_sats"),
        "payment_asset": svc.get("payment_asset"),
        "payment_network": svc.get("payment_network"),
        "provider": svc.get("provider"),
        "source_402index": svc.get("source"),
        "registered_at": _parse_catalog_timestamp(svc.get("registered_at")),
        "detail_url": f"https://402index.io/service/{sid}",
    }


def _parse_feed_xml(xml_text: str) -> tuple[list[dict], Optional[float]]:
    """Parses /feed.xml?type=new. Wrapped by the caller in try/except --
    ET.fromstring raises ET.ParseError on malformed XML, which must not
    crash the request (upstream is a third party we don't control)."""
    ns = {"l402": "https://402index.io/ns/l402"}
    root = ET.fromstring(xml_text)
    items = []
    dates = []
    for item_el in root.findall("./channel/item"):
        try:
            guid = item_el.findtext("guid") or item_el.findtext("link") or ""
            sid = guid.rstrip("/").rsplit("/", 1)[-1] if guid else None
            if not sid:
                continue
            registered_at = _parse_rfc2822(item_el.findtext("pubDate"))
            endpoint_el = item_el.find("l402:endpoint", ns)
            protocol_el = item_el.find("l402:protocol", ns)
            price_el = item_el.find("l402:price", ns)
            url = endpoint_el.get("url") if endpoint_el is not None else None
            protocol = protocol_el.get("type") if protocol_el is not None else None
            price_usd_raw = price_el.get("usd") if price_el is not None else None
            try:
                price_usd = float(price_usd_raw) if price_usd_raw not in (None, "") else None
            except (TypeError, ValueError):
                price_usd = None
            items.append({
                "id": str(sid),
                "name": str(item_el.findtext("title") or url or "")[:300],
                "url": url,
                "category": item_el.findtext("category"),
                "protocol": protocol,
                "price_usd": price_usd,
                "price_sats": None,
                "payment_asset": None,
                "payment_network": None,
                "provider": None,
                "source_402index": "feed",
                "registered_at": registered_at,
                "detail_url": item_el.findtext("link"),
            })
            if registered_at:
                dates.append(registered_at)
        except Exception:
            continue  # one malformed <item> must not drop the rest of the feed
    span_hours = (max(dates) - min(dates)).total_seconds() / 3600.0 if len(dates) >= 2 else None
    return items, span_hours


# ---------------------------------------------------------------
# Background catalog cache -- populated by a periodic full-catalog walk,
# never blocking a buyer request. See module docstring point 1/2.
# ---------------------------------------------------------------
_catalog_cache: dict = {
    "items": {},           # id -> normalized item
    "fetched_at": None,    # datetime | None -- None until the first walk completes
    "pages_walked": None,
    "catalog_total": None,
    "complete": False,
}
_catalog_refresh_task: Optional[asyncio.Task] = None


async def _walk_full_catalog() -> dict:
    items: dict = {}
    offset = 0
    pages = 0
    total = None
    start = time.monotonic()
    async with httpx.AsyncClient(timeout=_UPSTREAM_TIMEOUT) as client:
        while True:
            if time.monotonic() - start > _WALK_MAX_WALL_SECONDS:
                print(f"[WARN] catalog walk hit {_WALK_MAX_WALL_SECONDS}s wall cap at "
                      f"offset={offset}, stopping this cycle with a partial result", file=sys.stderr)
                break
            try:
                resp = await client.get(
                    f"{_402INDEX_BASE}/api/v1/services",
                    params={"limit": _SERVICES_PAGE_LIMIT, "offset": offset, "sort": "name", "order": "asc"},
                )
                resp.raise_for_status()
                data = resp.json()
            except (httpx.HTTPError, ValueError) as e:
                print(f"[WARN] catalog walk page offset={offset} failed: {e}", file=sys.stderr)
                break
            batch = data.get("services") if isinstance(data, dict) else None
            if not batch:
                break
            total = data.get("total", total)
            for svc in batch:
                norm = _normalize_catalog_item(svc)
                if norm:
                    items[norm["id"]] = norm
            pages += 1
            offset += _SERVICES_PAGE_LIMIT
            if isinstance(total, int) and offset >= total:
                break
            await asyncio.sleep(_WALK_PAGE_DELAY_SECONDS)
    complete = isinstance(total, int) and offset >= total
    return {"items": items, "pages": pages, "total": total, "complete": complete}


async def _catalog_refresh_loop():
    while True:
        try:
            result = await _walk_full_catalog()
            _catalog_cache["items"] = result["items"]
            _catalog_cache["fetched_at"] = datetime.now(timezone.utc)
            _catalog_cache["pages_walked"] = result["pages"]
            _catalog_cache["catalog_total"] = result["total"]
            _catalog_cache["complete"] = result["complete"]
            print(f"[INFO] catalog walk done: {result['pages']} pages, "
                  f"{len(result['items'])} items cached, complete={result['complete']}", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] catalog refresh loop error: {e}", file=sys.stderr)
        await asyncio.sleep(_WALK_REFRESH_INTERVAL_SECONDS)


async def _fetch_feed_topup() -> tuple[list[dict], Optional[float]]:
    """/feed.xml is on 402index's own rate-limit-exempt list, so this is
    safe to call on every buyer request (unlike the paginated walk)."""
    try:
        async with httpx.AsyncClient(timeout=_UPSTREAM_TIMEOUT) as client:
            resp = await client.get(f"{_402INDEX_BASE}/feed.xml", params={"type": "new"})
            resp.raise_for_status()
    except httpx.HTTPError as e:
        print(f"[WARN] feed topup fetch failed: {e}", file=sys.stderr)
        return [], None
    try:
        return _parse_feed_xml(resp.text)
    except ET.ParseError as e:
        print(f"[WARN] feed topup parse failed: {e}", file=sys.stderr)
        return [], None


async def _get_new_listings(window_hours: int, protocol: Optional[str],
                             category: Optional[str], payment_network: Optional[str]) -> dict:
    """Core logic, called directly (no HTTP re-entry) by both the REST
    route and the MCP tool. Never triggers the expensive full walk inline
    -- reads whatever the background task has cached (possibly empty on a
    cold Cloud Run instance right after deploy/scale-from-zero) and tops it
    up with the cheap, rate-limit-exempt feed fetch. See README "Known
    limitations" for what a cold cache means for a caller's window."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=window_hours)

    cache_items = _catalog_cache["items"] or {}
    cache_warm = bool(cache_items)
    feed_items, feed_span_hours = await _fetch_feed_topup()

    merged = dict(cache_items)
    for it in feed_items:
        merged.setdefault(it["id"], it)  # cache entries (richer fields) win on conflict

    results = []
    proto_filter = protocol.lower() if protocol else None
    cat_filter = category.lower() if category else None
    net_filter = payment_network.lower() if payment_network else None
    for it in merged.values():
        reg = it.get("registered_at")
        if reg is None or reg < cutoff:
            continue
        if proto_filter and (it.get("protocol") or "").lower() != proto_filter:
            continue
        if cat_filter and not (it.get("category") or "").lower().startswith(cat_filter):
            continue
        if net_filter and (it.get("payment_network") or "").lower() != net_filter:
            continue
        results.append(it)

    results.sort(key=lambda x: x["registered_at"], reverse=True)
    truncated = len(results) > _MAX_RESULTS
    results = results[:_MAX_RESULTS]

    out_results = []
    for it in results:
        r = dict(it)
        r["registered_at"] = it["registered_at"].isoformat() if it["registered_at"] else None
        out_results.append(r)

    return {
        "window_hours": window_hours,
        "as_of": now.isoformat(),
        "count": len(out_results),
        "truncated": truncated,
        "results": out_results,
        "catalog_walk": {
            "status": "warm" if cache_warm else "cold_fallback_feed_only",
            "last_full_walk_at": _catalog_cache["fetched_at"].isoformat() if _catalog_cache["fetched_at"] else None,
            "pages_walked": _catalog_cache["pages_walked"],
            "catalog_total_at_last_walk": _catalog_cache["catalog_total"],
            # False if the walk hit _WALK_MAX_WALL_SECONDS before reaching the end of the
            # catalog (bails out with a partial result rather than blocking the next cycle
            # forever) -- distinct from "cold_fallback_feed_only" above, which means no walk
            # has ever finished at all. A caller relying on completeness for a wide window
            # should check this, not just `status`.
            "walk_complete": _catalog_cache["complete"],
        },
        "feed_topup": {
            "items_fetched": len(feed_items),
            "observed_span_hours": feed_span_hours,
        },
        "note": (
            "Source data is 402index.io's own free public directory (https://402index.io/api-docs, "
            "no auth needed) -- this endpoint does not have exclusive access to anything. Its value is "
            "polling the ~96k+ entry catalog so you don't have to, merging in their recency feed for "
            "freshness between polls, deduping, and filtering to your window. If catalog_walk.status is "
            "'cold_fallback_feed_only', the background full-catalog walk has not completed yet on this "
            "instance (fresh Cloud Run cold start) -- results are limited to what 402index's own recency "
            "feed currently holds (recently observed to cover roughly the most recent few hours, not the "
            "full requested window); retry after a few minutes for full coverage."
        ),
    }


# ---------------------------------------------------------------
# Supabase telemetry -- ported by hand, same pattern as the 3 sibling
# manual assets (document-conversion-api / agent-verification-api /
# url-metadata-api). anon key only, RLS is INSERT-only for anon.
# ---------------------------------------------------------------
_NEXUS_SUPABASE_URL = os.getenv("SUPABASE_URL")
_NEXUS_SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

_nexus_bg_tasks: set = set()


def _nexus_fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _nexus_bg_tasks.add(task)
    task.add_done_callback(_nexus_bg_tasks.discard)
    return task


def _nexus_truncate_ip(raw_ip: Optional[str]) -> Optional[str]:
    if not raw_ip:
        return None
    if ":" in raw_ip and "." not in raw_ip:
        segments = [s for s in raw_ip.split(":") if s]
        head = segments[:4] if len(segments) >= 4 else segments
        return (":".join(head) + "::/64") if head else None
    octets = raw_ip.split(".")
    if len(octets) == 4 and all(o.isdigit() for o in octets):
        return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
    return None


async def _nexus_supabase_insert(table: str, payload: dict) -> None:
    if not _NEXUS_SUPABASE_URL or not _NEXUS_SUPABASE_ANON_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(
                f"{_NEXUS_SUPABASE_URL}/rest/v1/{table}",
                json=payload,
                headers={
                    "apikey": _NEXUS_SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {_NEXUS_SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
            )
    except Exception as e:
        print(f"[WARN] Supabase insert to {table!r} failed: {e}", file=sys.stderr)


async def _nexus_log_mcp_call_event(tool_id: str, success: bool, latency_ms: int) -> None:
    await _nexus_supabase_insert("mcp_call_events", {
        "tool_id": tool_id,
        "asset_name": _NEXUS_ASSET_NAME,
        "success": success,
        "latency_ms": latency_ms,
        "agent_framework": "mcp",
        "price_charged": 0,  # MCP path is free today -- see README "Known limitations"
    })


# ---------------------------------------------------------------
# MCP server (Streamable HTTP)
# ---------------------------------------------------------------
_public_domain = os.getenv("PUBLIC_DOMAIN", "*")
if _public_domain == "*":
    print(
        "[WARN] PUBLIC_DOMAIN is unset -- every real request will get 421'd once "
        "deployed publicly (fails closed). Set it to the real Cloud Run domain and "
        "redeploy.", file=sys.stderr,
    )

mcp = FastMCP(
    "new-x402-listings-feed",
    stateless_http=True,
    host="0.0.0.0",
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["localhost:*", "127.0.0.1:*", _public_domain, _public_domain + ":*"],
        allowed_origins=["http://localhost:*", "http://127.0.0.1:*", "https://" + _public_domain],
    ),
)


@mcp.tool()
async def get_new_x402_listings(
    window_hours: int = 24,
    protocol: Optional[str] = None,
    category: Optional[str] = None,
    payment_network: Optional[str] = None,
    ctx: Context = None,
) -> dict:
    """Return x402/L402 services newly registered on 402index.io within
    the last `window_hours` (1-168, default 24). Optional filters:
    `protocol` ('x402' or 'l402'), `category` (prefix match), `payment_network`
    (exact match, e.g. 'Base', 'Lightning'). Source data is 402index.io's own
    free public directory -- this tool's value is polling/dedup/filtering
    convenience, not exclusive data access."""
    start = time.monotonic()
    success = False
    try:
        window_hours = max(1, min(int(window_hours), _MAX_WINDOW_HOURS))
        result = await _get_new_listings(window_hours, protocol, category, payment_network)
        success = True
        return result
    finally:
        latency_ms = int((time.monotonic() - start) * 1000)
        _nexus_fire_and_forget(_nexus_log_mcp_call_event("get_new_x402_listings", success, latency_ms))


# ---------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _catalog_refresh_task
    _catalog_refresh_task = asyncio.create_task(_catalog_refresh_loop())
    async with mcp.session_manager.run():
        try:
            yield
        finally:
            _catalog_refresh_task.cancel()


app = FastAPI(
    title="New x402 Listings Feed",
    description=(
        "Feed of x402/L402 services newly listed on 402index.io within a caller-specified recency "
        "window (default 24h, max 7d). Source data is 402index.io's own free public directory -- this "
        "asset's value is polling/dedup/filtering convenience, not exclusive data access."
    ),
    version="1.0.0",
    contact={"email": "dasaanrod@gmail.com"},
    lifespan=lifespan,
)


# ---------------------------------------------------------------
# MCP listen-connection timeout -- fixes the Cloud Run cost spike confirmed
# 2026-09-03 (same pattern as erc8004-agent-liveness, see that repo's
# patch_mcp_listen_timeout.py for the full root-cause writeup). Third-party
# MCP-monitoring bots open GET /mcp/ "listen" connections (per the
# Streamable HTTP transport spec) and this asset -- which never pushes
# server-initiated notifications -- never closes them, so every one rode
# Cloud Run's full 300s request timeout and was billed for the whole
# duration. This applies ONLY to GET requests under /mcp -- the paid POST
# tool-call path and every other route are completely untouched.
# ---------------------------------------------------------------
_NEXUS_MCP_LISTEN_TIMEOUT_SECONDS = 25


class _NexusMcpListenTimeoutMiddleware:
    """Pure ASGI middleware. Enforces a short idle timeout on GET /mcp
    listen connections only. On timeout, cancels the inner call and lets
    the ASGI server close the underlying connection -- equivalent to a
    normal disconnect from the client's point of view, which any
    SSE-based MCP client already has to handle and reconnect from."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] == "http"
            and scope.get("method") == "GET"
            and scope.get("path", "").rstrip("/").startswith("/mcp")
        ):
            try:
                await asyncio.wait_for(
                    self.app(scope, receive, send),
                    timeout=_NEXUS_MCP_LISTEN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                # Connection closes here -- no further ASGI messages are
                # sent, which is the correct way to end a still-open SSE
                # response early. Intentionally swallowed: this is an
                # expected, routine cutoff, not an error condition.
                return
        else:
            await self.app(scope, receive, send)


app.add_middleware(_NexusMcpListenTimeoutMiddleware)


# --- x402: pay-per-call in USDC, Base Sepolia testnet -- same wallet,
#     facilitator and self-payment-bug fix as the sibling manual assets
#     (skills/x402-payments). Speculative niche per the product owner ->
#     low tier, matching url-metadata-api/document-conversion-api's
#     $0.01-$0.02 tier, not agent-verification-api's $0.35 signal tier. ---
_NEXUS_X402_FREE_MODE = os.getenv("NEXUS_X402_FREE_MODE", "false").strip().lower() == "true"

_X402_EVM_ADDRESS = os.getenv("X402_WALLET_ADDRESS", "0xYOUR_WALLET_ADDRESS_HERE")
_X402_NETWORK: Network = "eip155:84532"  # Base Sepolia testnet
_X402_PRICE = os.getenv("X402_PRICE", "$0.01")

if not _X402_PRICE or not _X402_PRICE.startswith("$"):
    print(
        f"[WARN] X402_PRICE ({_X402_PRICE!r}) doesn't look like a price string "
        "(expected something like '$0.01'). Falling back to '$0.01' so the "
        "server can still boot -- fix X402_PRICE before deploying.",
        file=sys.stderr,
    )
    _X402_PRICE = "$0.01"

_looks_like_evm_address = (
    _X402_EVM_ADDRESS.startswith("0x")
    and len(_X402_EVM_ADDRESS) == 42
    and all(c in "0123456789abcdefABCDEF" for c in _X402_EVM_ADDRESS[2:])
)
if not _NEXUS_X402_FREE_MODE and not _looks_like_evm_address:
    print(
        f"[WARN] X402_WALLET_ADDRESS ({_X402_EVM_ADDRESS!r}) doesn't look like a "
        "real EVM address (expected '0x' + 40 hex chars). The server will still "
        "boot and issue 402 challenges, but payments will have nowhere real to "
        "settle. See README.md.",
        file=sys.stderr,
    )

_x402_facilitator = HTTPFacilitatorClient(FacilitatorConfig(url="https://x402.org/facilitator"))
_x402_server = x402ResourceServer(_x402_facilitator)
_x402_server.register(_X402_NETWORK, ExactEvmServerScheme())


async def _nexus_log_x402_revenue_event(ctx) -> None:
    try:
        result = getattr(ctx, "result", None)
        requirements = getattr(ctx, "requirements", None)
        if result is None or requirements is None or not getattr(result, "success", False):
            return
        raw_amount = getattr(requirements, "amount", None)
        amount_eur = int(raw_amount) / 1_000_000 if raw_amount is not None else None
        await _nexus_supabase_insert("revenue_events", {
            "asset_name": _NEXUS_ASSET_NAME,
            "amount_eur": amount_eur,
            "pricing_model": "x402",
            "stripe_event_id": None,
            "customer_id": getattr(result, "payer", None),
        })
    except Exception:
        pass


_x402_server.on_after_settle(_nexus_log_x402_revenue_event)

_X402_ROUTES: dict[str, RouteConfig] = {
    "POST /new-x402-listings": RouteConfig(
        accepts=[PaymentOption(scheme="exact", pay_to=_X402_EVM_ADDRESS, price=_X402_PRICE, network=_X402_NETWORK)],
        mime_type="application/json",
        description="Feed of x402/L402 services newly listed on 402index.io within a recency window",
    ),
}
if not _NEXUS_X402_FREE_MODE:
    app.add_middleware(PaymentMiddlewareASGI, routes=_X402_ROUTES, server=_x402_server)

# --- PATCH traffic_log_middleware_order_v1 ---
@app.middleware("http")
async def _nexus_traffic_log(request, call_next):
    response = await call_next(request)
    ip = request.client.host if request.client else None
    _nexus_fire_and_forget(_nexus_supabase_insert("traffic_events", {
        "asset_name": _NEXUS_ASSET_NAME,
        "ip_range": _nexus_truncate_ip(ip),
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
    }))
    return response



class NewListingsRequest(BaseModel):
    window_hours: Annotated[int, Field(24, ge=1, le=_MAX_WINDOW_HOURS,
        description="Recency window in hours, 1-168 (7 days). Default 24.")]
    protocol: Annotated[Optional[Literal["x402", "l402", "X402", "L402"]], Field(
        None, description="Filter by protocol.")] = None
    category: Annotated[Optional[str], Field(
        None, max_length=100, description="Filter by category, prefix match (e.g. 'crypto' matches 'crypto/nft').")] = None
    payment_network: Annotated[Optional[str], Field(
        None, max_length=100, description="Filter by payment network, exact match (e.g. 'Base', 'Lightning').")] = None


@app.post(
    "/new-x402-listings",
    responses={
        422: {"description": "invalid request body (window_hours out of range, bad protocol value)"},
    },
)
async def new_x402_listings_endpoint(payload: NewListingsRequest) -> dict:
    """Return x402/L402 services newly registered on 402index.io within the
    requested window. Payment (x402, $0.01) settles BEFORE this handler
    runs, at the ASGI middleware layer ahead of request validation -- a
    call is charged even if it 422s. Caller input (window_hours/protocol/
    category/payment_network) is never interpolated into the upstream
    402index.io request URL -- the upstream catalog walk and feed fetch
    use fixed, hardcoded query params only; caller filters are applied
    exclusively to the already-fetched, normalized in-memory result."""
    return await _get_new_listings(
        payload.window_hours, payload.protocol, payload.category, payload.payment_network
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


_FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(content=_FAVICON_PNG, media_type="image/png")


# --- 402index.io domain claim verification (POST /api/v1/claim -- filled in
#     after the real Cloud Run domain is known, see README "Deploy") ---
_NEXUS_402INDEX_VERIFY_HASH = os.getenv("NEXUS_402INDEX_VERIFY_HASH", "")


@app.get("/.well-known/402index-verify.txt", include_in_schema=False)
async def _nexus_402index_verify():
    from fastapi.responses import PlainTextResponse
    if not _NEXUS_402INDEX_VERIFY_HASH:
        raise HTTPException(404, "no 402index claim on file")
    return PlainTextResponse(content=_NEXUS_402INDEX_VERIFY_HASH)


@app.get("/.well-known/agent-card.json", include_in_schema=False)
async def agent_card() -> dict:
    base = f"https://{_public_domain}" if _public_domain != "*" else "https://new-x402-listings-feed.example"
    return {
        "name": "New x402 Listings Feed",
        "description": "Feed of x402/L402 services newly listed on 402index.io within a caller-specified "
                        "recency window. Source data is 402index.io's own free public directory.",
        "url": base,
        "version": "1.0.0",
        "documentationUrl": f"{base}/docs",
        "provider": {"organization": "nexus-mcp-infra", "url": "https://github.com/nexus-mcp-infra"},
        "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": False},
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "additionalInterfaces": [{"url": f"{base}/mcp", "transport": "MCP"}],
        "skills": [
            {
                "id": "nexus_new_x402_listings_feed_get_new_x402_listings",
                "name": "Get New x402 Listings",
                "description": "Services newly registered on 402index.io within a recency window (default 24h, max 7d).",
                "tags": ["x402", "l402", "discovery", "402index", "feed"],
            },
        ],
        "metadata": {
            "protocol_note": (
                "This service implements the Model Context Protocol (MCP) at /mcp, not A2A's own task "
                "methods. POST /new-x402-listings is charged $0.01 via x402 (Base Sepolia TESTNET, not "
                "real funds); the MCP tool is currently free -- see README. Payment settles BEFORE this "
                "handler runs: a call that 422s (invalid request body) is still charged, same as a 200. "
                "Underlying registration data is 402index.io's own free public API "
                "(https://402index.io/api-docs) -- this service does not have exclusive access to "
                "anything; its value is polling/dedup/filtering convenience over 402index's ~96k+ entry "
                "catalog, not raw data access."
            ),
        },
    }


_NEXUS_X402_OPENAPI_OPERATIONS = [("post", "/new-x402-listings")]


def _nexus_openapi_with_payment_info():
    if app.openapi_schema:
        return app.openapi_schema
    schema = _fastapi_get_openapi(title=app.title, version=app.version, description=app.description, routes=app.routes)
    if not _NEXUS_X402_FREE_MODE:
        for method, path in _NEXUS_X402_OPENAPI_OPERATIONS:
            op = schema.get("paths", {}).get(path, {}).get(method)
            if op is None:
                continue
            op["x-payment-info"] = {
                "price": {"mode": "fixed", "currency": "USD", "amount": _X402_PRICE.lstrip("$")},
                "protocols": [{"x402": {}}],
            }
            op.setdefault("responses", {})["402"] = {"description": "Payment Required"}
    schema.setdefault("info", {})["contact"] = {"email": "dasaanrod@gmail.com"}
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = _nexus_openapi_with_payment_info

app.mount("/mcp", mcp.streamable_http_app())
