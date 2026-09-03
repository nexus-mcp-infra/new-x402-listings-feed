#!/usr/bin/env python3
"""
patch_mcp_listen_timeout.py

Standalone patch for new-x402-listings-feed/main.py.

ROOT CAUSE (same pattern already confirmed and fixed on erc8004-agent-liveness
2026-09-03, see that repo's patch_mcp_listen_timeout.py for the full
root-cause writeup; confirmed here via Cloud Logging export the same day):
third-party "agent trust"/MCP-monitoring bots issue a GET to /mcp/, which
the official MCP Python SDK's Streamable HTTP transport treats as opening a
server-to-client SSE "listen" stream for server-initiated push
notifications. This asset never sends any (it's a plain request/response
tool), and the code never closes an idle listen connection on its own -- so
every bot that opens one rides Cloud Run's outer 300s request timeout
before being cut off, and Cloud Run bills the full held-open duration as
active instance time. Evidence for this asset: 422 requests, GET /mcp/,
status 200, latency clustered at ~301s.

FIX: a narrow ASGI middleware that applies a short server-side timeout
(default 25s) ONLY to GET requests whose path starts with /mcp. Every
other path (including the paid POST tool-call path and the x402 payment
flow) is untouched. A real MCP client that needs to keep listening simply
reconnects -- cheap -- instead of holding a phantom connection open for
five minutes -- expensive. Lossless for this asset specifically because it
has no server-initiated notifications today.

Usage:
    python patch_mcp_listen_timeout.py [path/to/main.py]

Defaults to ./main.py if no path given.
"""

import ast
import shutil
import sys
from pathlib import Path

IDEMPOTENCY_MARKER = "_NEXUS_MCP_LISTEN_TIMEOUT_SECONDS"

MIDDLEWARE_CLASS_SRC = '''

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
'''


def main():
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("main.py")
    if not target.exists():
        print(f"[ABORT] {target} not found.", file=sys.stderr)
        sys.exit(1)

    original_src = target.read_text(encoding="utf-8")

    if IDEMPOTENCY_MARKER in original_src:
        print(f"[SKIP] {target} already patched (found marker "
              f"'{IDEMPOTENCY_MARKER}'). No changes made.")
        sys.exit(0)

    try:
        ast.parse(original_src)
    except SyntaxError as exc:
        print(f"[ABORT] {target} does not parse as valid Python BEFORE "
              f"patching: {exc}", file=sys.stderr)
        sys.exit(1)

    anchor = 'app = FastAPI(\n    title="New x402 Listings Feed",'
    if anchor not in original_src:
        print("[ABORT] Expected anchor text (start of `app = FastAPI(...)`) "
              "not found -- refusing to guess an insertion point.",
              file=sys.stderr)
        sys.exit(1)

    # Insert right after the FastAPI(...) constructor call closes, i.e.
    # after the matching ")" + blank lines, before the x402 comment block.
    fastapi_close_marker = "    lifespan=lifespan,\n)\n"
    if fastapi_close_marker not in original_src:
        print("[ABORT] Could not find the end of the FastAPI(...) "
              "constructor call -- refusing to guess an insertion point.",
              file=sys.stderr)
        sys.exit(1)

    patched_src = original_src.replace(
        fastapi_close_marker,
        fastapi_close_marker + MIDDLEWARE_CLASS_SRC,
        1,
    )

    try:
        ast.parse(patched_src)
    except SyntaxError as exc:
        print(f"[ABORT] Patched source does not parse as valid Python -- "
              f"not writing anything: {exc}", file=sys.stderr)
        sys.exit(1)

    backup = target.with_suffix(target.suffix + ".bak")
    shutil.copy2(target, backup)
    target.write_text(patched_src, encoding="utf-8")

    print(f"[OK] Patched {target} (backup at {backup}).")
    print(f"[OK] Idempotency marker '{IDEMPOTENCY_MARKER}' present.")
    print("[NEXT] Verify with: grep -n 'NexusMcpListenTimeoutMiddleware' "
          f"{target}")
    print("[NEXT] git add (explicit pathspec, not -A), commit, push to "
          "main to trigger the connected Cloud Build deploy.")


if __name__ == "__main__":
    main()
