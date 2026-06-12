"""
FINRA Short Interest MCP Connector
==================================

A Model Context Protocol server that exposes FINRA's Consolidated Short Interest
to Claude. It serves the S1 input for SURGE:

  * short_interest      -> recent SI reports for a ticker, newest first, each
                           with days-to-cover and a freshness verdict
  * short_interest_asof -> the report for a specific settlement date (PIT)
  * check_si_freshness  -> just the age-gate decision SURGE depends on

THE FRAMEWORK'S CORE TENSION (built into every result):
  FINRA publishes official short interest twice a month (15th + month-end) with
  an 8-12 day publish lag. SURGE's rule: SI report age <=10d -> score S1
  normally; >18d -> the number is too stale, switch to the borrow-rate proxy
  (Fintel/S3). Every reading here therefore carries report_age_days and a
  freshness_status so that gate can be applied automatically.

  Note: FINRA reports short SHARES and days-to-cover, not short interest as a
  % of float (float isn't in FINRA data). For SI%float, combine
  current_short_shares here with shares-outstanding/float from your EDGAR
  connector. days_to_cover IS provided directly.

Data access (per the framework data blueprint, SRC-FINRA-SI):
  * FREE, PIT_FULL, LIC_OPEN. Official, append-only by ticker+settlement_date.
  * The supported programmatic path is FINRA's Query API, which needs a FREE
    "Public Credential" (Client ID + Secret) from the FINRA API Console. This
    server performs the OAuth2 client-credentials token exchange for you.
      Get credentials: https://developer.finra.org  (Console -> Public Credential)
    Set them via FINRA_API_CLIENT_ID and FINRA_API_CLIENT_SECRET.

Configurable (defaults match FINRA's current catalog; override if FINRA renames):
  FINRA_SI_GROUP         (default "otcMarket")
  FINRA_SI_DATASET       (default "consolidatedShortInterest")
  FINRA_SI_SYMBOL_FIELD  (default "issueSymbolIdentifier")

Transports (same pattern as your other connectors):
  * stdio            (default)  -> local testing in Claude Desktop
  * streamable-http  (MCP_TRANSPORT=http) -> hosted URL for a claude.ai project,
                                            served at /mcp (PORT, default 8000)
"""

from __future__ import annotations

import asyncio
import base64
import os
import time
from datetime import date, datetime, timezone
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

CLIENT_ID = os.environ.get("FINRA_API_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("FINRA_API_CLIENT_SECRET", "").strip()

FIP_TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
                 "access_token?grant_type=client_credentials")
DATA_BASE = "https://api.finra.org/data/group"
SI_GROUP = os.environ.get("FINRA_SI_GROUP", "otcMarket")
SI_DATASET = os.environ.get("FINRA_SI_DATASET", "EquityShortInterest")
SYMBOL_FIELD = os.environ.get("FINRA_SI_SYMBOL_FIELD", "issueSymbolIdentifier")
MAX_RPS = float(os.environ.get("FINRA_MAX_RPS", "3"))

# SURGE S1 age gate (calendar days since settlement date).
FRESH_MAX = 10     # <=10d : score normally
AGING_MAX = 18     # 11-18d: usable, verify near the edge;  >18d: use proxy

# Host-header fix (same as your FRED/ApeWisdom connectors): hosted behind a
# proxy, requests carry a public Host header. Disable the MCP SDK's DNS-
# rebinding protection so onrender.com etc. isn't rejected (421). Read-only
# public data, reached by Claude's backend -> the check adds no security here.
_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
mcp = FastMCP("finra-short-interest", transport_security=_security)


# ----------------------------------------------------------------------------
# Rate limiter + OAuth2 token manager
# ----------------------------------------------------------------------------

class _RateLimiter:
    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / rps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            delta = time.monotonic() - self._last
            if delta < self._min_interval:
                await asyncio.sleep(self._min_interval - delta)
            self._last = time.monotonic()


_limiter = _RateLimiter(MAX_RPS)
_token: str | None = None
_token_exp: float = 0.0
_token_lock = asyncio.Lock()


def _require_creds() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "FINRA_API_CLIENT_ID / FINRA_API_CLIENT_SECRET are not set. Create a "
            "FREE Public Credential in the FINRA API Console "
            "(https://developer.finra.org) and set both values."
        )


async def _get_token() -> str:
    """OAuth2 client-credentials: exchange ID+Secret for a short-lived bearer."""
    global _token, _token_exp
    async with _token_lock:
        if _token and time.monotonic() < _token_exp - 60:
            return _token
        _require_creds()
        basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(FIP_TOKEN_URL,
                                  headers={"Authorization": f"Basic {basic}"})
            r.raise_for_status()
            data = r.json()
        _token = data["access_token"]
        _token_exp = time.monotonic() + float(data.get("expires_in", 1800))
        return _token


async def _query(compare: list[dict] | None = None,
                 date_range: list[dict] | None = None,
                 limit: int = 100) -> list[dict]:
    """POST a filtered query to the FINRA data endpoint; return list of records."""
    await _limiter.wait()
    body: dict[str, Any] = {"limit": limit}
    if compare:
        body["compareFilters"] = compare
    if date_range:
        body["dateRangeFilters"] = date_range
    url = f"{DATA_BASE}/{SI_GROUP}/name/{SI_DATASET}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    async def _do(token: str) -> httpx.Response:
        headers["Authorization"] = f"Bearer {token}"
        async with httpx.AsyncClient(timeout=45.0) as client:
            return await client.post(url, json=body, headers=headers)

    global _token
    token = await _get_token()
    r = await _do(token)
    if r.status_code == 401:                      # token expired/invalid -> refresh once
        _token = None
        r = await _do(await _get_token())
    txt = (r.text or "").strip()
    if r.status_code >= 400:
        raise RuntimeError(f"FINRA data API {r.status_code}: "
                           f"{txt[:300] or '(empty body)'}")
    if not txt:                                   # HTTP 200 with empty body = no rows
        return []
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"FINRA returned non-JSON (HTTP {r.status_code}): "
                           f"{txt[:300]}")
    if isinstance(data, dict):                    # some FINRA datasets wrap in a key
        for k in ("data", "results", "records"):
            if isinstance(data.get(k), list):
                return data[k]
        return []
    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------------
# Parsing / framework logic
# ----------------------------------------------------------------------------

def _pick(rec: dict, *keys: str) -> Any:
    for k in keys:
        if k in rec and rec[k] not in (None, ""):
            return rec[k]
    return None


def _num(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _age_days(settlement_date: str | None) -> int | None:
    if not settlement_date:
        return None
    try:
        d = datetime.strptime(settlement_date[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    return (date.today() - d).days


def _freshness(age: int | None) -> dict:
    """SURGE S1 age gate."""
    if age is None:
        return {"status": "UNKNOWN", "recommendation": "No settlement date parsed."}
    if age <= FRESH_MAX:
        return {"status": "FRESH",
                "recommendation": "Score S1 normally (report age within 10 days)."}
    if age <= AGING_MAX:
        return {"status": "AGING",
                "recommendation": "Usable, but verify against the borrow-rate "
                                  "proxy (Fintel/S3) before acting near a threshold."}
    return {"status": "STALE",
            "recommendation": "Do NOT score S1 on this report (age >18d). SURGE "
                              "rule: switch to the borrow-rate/utilization proxy."}


def _shape(rec: dict) -> dict:
    """Extract the framework-relevant fields; keep the raw record too."""
    settlement = _pick(rec, "settlementDate", "settlement_date")
    cur = _num(_pick(rec, "currentShortPositionQuantity",
                     "currentShortShareNumber", "shortInterestCurrentQuantity"))
    prev = _num(_pick(rec, "previousShortPositionQuantity",
                      "previousShortShareNumber"))
    dtc = _num(_pick(rec, "daysToCoverQuantity", "daysToCover",
                     "averageDaysToCoverQuantity"))
    adv = _num(_pick(rec, "averageDailyVolumeQuantity", "averageDailyVolume"))
    chg_pct = _num(_pick(rec, "changePercent", "shortInterestChangePercent",
                         "percentageChange"))
    age = _age_days(settlement)
    return {
        "symbol": _pick(rec, SYMBOL_FIELD, "issueSymbolIdentifier", "symbolCode"),
        "name": _pick(rec, "issueName", "securityName", "companyName"),
        "settlement_date": settlement,
        "current_short_shares": cur,
        "previous_short_shares": prev,
        "short_shares_change": (cur - prev) if (cur is not None and prev is not None) else None,
        "change_percent": chg_pct,
        "days_to_cover": dtc,
        "avg_daily_volume": adv,
        "report_age_days": age,
        "freshness": _freshness(age),
        "raw": rec,
    }


# ----------------------------------------------------------------------------
# Tools
# ----------------------------------------------------------------------------

@mcp.tool()
async def short_interest(ticker: str, limit: int = 4) -> dict:
    """Recent FINRA short-interest reports for a ticker, newest first.

    The SURGE S1 workhorse. Each report includes current/previous short shares,
    days-to-cover, the % change, and — crucially — report_age_days plus a
    freshness verdict so the S1 age gate is applied automatically.

    Args:
        ticker: Symbol, e.g. "GME" (case-insensitive).
        limit: How many recent reports to return (default 4 = ~2 months).
    Returns:
        Identity plus a list of reports (newest first) with derived fields, and
        the freshness of the latest report surfaced at the top.
    """
    sym = ticker.strip().upper()
    # Symbol-only query (no date-range filter — that was returning empty for some
    # symbols). A single symbol has only a few hundred biweekly reports, so a high
    # limit returns the full history; we sort newest-first and trim client-side.
    rows = await _query(
        compare=[{"compareType": "EQUAL", "fieldName": SYMBOL_FIELD, "compareValue": sym}],
        limit=1000,
    )
    shaped = [_shape(r) for r in rows]
    shaped.sort(key=lambda x: x.get("settlement_date") or "", reverse=True)
    shaped = shaped[:max(1, limit)]
    if not shaped:
        return {"ticker": sym, "found": False,
                "note": "No reports returned for this symbol. It may have no "
                        "reported short position, or the dataset/field names need "
                        "adjusting (see FINRA_SI_* settings)."}
    return {
        "ticker": sym,
        "found": True,
        "latest_freshness": shaped[0]["freshness"],
        "latest_settlement_date": shaped[0]["settlement_date"],
        "report_count": len(shaped),
        "reports": shaped,
        "publish_lag_note": "FINRA publishes ~8-12 days after the settlement date, "
                            "so even the newest report is already several days old.",
    }


@mcp.tool()
async def short_interest_asof(ticker: str, settlement_date: str) -> dict:
    """The short-interest report for a SPECIFIC settlement date (point-in-time).

    FINRA SI is append-only by ticker+settlement_date, so this is the backtest-
    safe read: pull exactly what was reported for that period.

    Args:
        ticker: Symbol, e.g. "GME".
        settlement_date: ISO date "YYYY-MM-DD" of the settlement period.
    Returns:
        The matching report (with derived fields) or a not-found marker.
    """
    sym = ticker.strip().upper()
    rows = await _query(compare=[
        {"compareType": "EQUAL", "fieldName": SYMBOL_FIELD, "compareValue": sym},
        {"compareType": "EQUAL", "fieldName": "settlementDate", "compareValue": settlement_date},
    ], limit=10)
    if not rows:
        return {"ticker": sym, "settlement_date": settlement_date, "found": False,
                "note": "No report for that exact settlement date. Use "
                        "short_interest() to see available dates."}
    return {"ticker": sym, "found": True, "report": _shape(rows[0])}


@mcp.tool()
async def check_si_freshness(ticker: str) -> dict:
    """Just the SURGE age-gate decision for a ticker's latest SI report.

    SURGE makes a dependency call on this: if the official SI is fresh, score S1
    on it; if stale (>18d), the borrow-rate proxy becomes primary instead. This
    returns that decision directly without the full report payload.

    Args:
        ticker: Symbol, e.g. "GME".
    Returns:
        latest settlement date, report_age_days, freshness status, and the
        score-vs-proxy recommendation.
    """
    sym = ticker.strip().upper()
    rows = await _query(
        compare=[{"compareType": "EQUAL", "fieldName": SYMBOL_FIELD, "compareValue": sym}],
        limit=1000,
    )
    shaped = [_shape(r) for r in rows]
    shaped.sort(key=lambda x: x.get("settlement_date") or "", reverse=True)
    if not shaped:
        return {"ticker": sym, "found": False,
                "note": "No report found for this symbol to assess freshness."}
    latest = shaped[0]
    return {
        "ticker": sym, "found": True,
        "latest_settlement_date": latest["settlement_date"],
        "report_age_days": latest["report_age_days"],
        "freshness_status": latest["freshness"]["status"],
        "recommendation": latest["freshness"]["recommendation"],
        "days_to_cover": latest["days_to_cover"],
    }


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print(f"[finra-short-interest] build-4-comparevalue | dns_rebinding_protection="
          f"{_security.enable_dns_rebinding_protection} | "
          f"dataset={SI_GROUP}/{SI_DATASET} | "
          f"transport={os.environ.get('MCP_TRANSPORT', 'stdio')}",
          file=sys.stderr, flush=True)
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable-http", "streamable_http"):
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")
