"""
Buy-the-Dip - Bloomberg Data Layer (with local Parquet cache)
"""

import blpapi
import pandas as pd
import numpy as np
import os
import hashlib
import time
from datetime import datetime, timedelta
from config import (CACHE_DIR, CACHE_STALE_HRS, BATCH_SIZE, TIMEOUT_MS,
                    US_EXCHANGE_CODES, BEST_FPERIOD)

_session = None


def get_session():
    global _session
    if _session is None:
        opts = blpapi.SessionOptions()
        opts.setServerHost("localhost")
        opts.setServerPort(8194)
        _session = blpapi.Session(opts)
        if not _session.start():
            raise RuntimeError("Cannot connect to Bloomberg")
        _session.openService("//blp/refdata")
    return _session


def close_session():
    global _session
    if _session:
        _session.stop()
        _session = None


def _collect(session, timeout_ms=TIMEOUT_MS):
    msgs = []
    while True:
        ev = session.nextEvent(timeout_ms)
        for msg in ev:
            msgs.append(msg)
        if ev.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.TIMEOUT):
            break
    return msgs


# ==================== Cache ====================

def _cache_path(prefix, *args):
    raw = f"{prefix}_{'_'.join(str(a) for a in args)}"
    h = hashlib.md5(raw.encode()).hexdigest()[:12]
    safe = prefix.replace(" ", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe}_{h}.parquet")


def _cache_get(path):
    if not os.path.exists(path):
        return None
    age_h = (time.time() - os.path.getmtime(path)) / 3600
    if age_h > CACHE_STALE_HRS:
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _cache_put(path, df):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as e:
        print(f"    [WARN] Cache write failed: {e}")


# ==================== Ticker Conversion ====================

def convert_tickers(raw_tickers):
    """Convert INDX_MEMBERS exchange codes to US composite.
    'AAPL UW' -> 'AAPL US Equity'
    """
    out = []
    for t in raw_tickers:
        parts = t.strip().split()
        if len(parts) == 2 and parts[1] in US_EXCHANGE_CODES:
            out.append(f"{parts[0]} US Equity")
        elif "Equity" not in t:
            out.append(t + " Equity")
        else:
            out.append(t)
    return list(dict.fromkeys(out))


# ==================== BDS: Index Members ====================

def get_index_members(index_ticker):
    cp = _cache_path("members", index_ticker)
    cached = _cache_get(cp)
    if cached is not None:
        tickers = cached["ticker"].tolist()
        print(f"  [CACHE] {index_ticker} members: {len(tickers)}")
        return tickers

    print(f"  [BBG] Fetching {index_ticker} members...")
    session = get_session()
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(index_ticker)
    req.getElement("fields").appendValue("INDX_MEMBERS")
    session.sendRequest(req)

    raw = []
    for msg in _collect(session):
        if not msg.hasElement("securityData"):
            continue
        arr = msg.getElement("securityData")
        for i in range(arr.numValues()):
            sec = arr.getValueAsElement(i)
            if not sec.hasElement("fieldData"):
                continue
            fd = sec.getElement("fieldData")
            if not fd.hasElement("INDX_MEMBERS"):
                continue
            bulk = fd.getElement("INDX_MEMBERS")
            for j in range(bulk.numValues()):
                row = bulk.getValueAsElement(j)
                if row.hasElement("Member Ticker and Exchange Code"):
                    raw.append(row.getElementAsString(
                        "Member Ticker and Exchange Code"))

    tickers = convert_tickers(raw)
    print(f"  [BBG] Got {len(tickers)} members (raw {len(raw)})")
    _cache_put(cp, pd.DataFrame({"ticker": tickers}))
    return tickers


# ==================== BDH: Daily Prices ====================

def get_daily_prices(ticker, start_date, end_date, fields=None):
    if fields is None:
        fields = ["PX_LAST"]
    cp = _cache_path("daily", ticker, start_date, end_date, ",".join(fields))
    cached = _cache_get(cp)
    if cached is not None:
        return cached

    session = get_session()
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue(ticker)
    for f in fields:
        req.getElement("fields").appendValue(f)
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", start_date)
    req.set("endDate", end_date)
    session.sendRequest(req)

    rows = []
    for msg in _collect(session):
        if not msg.hasElement("securityData"):
            continue
        sec = msg.getElement("securityData")
        if sec.hasElement("securityError"):
            return pd.DataFrame()
        if not sec.hasElement("fieldData"):
            continue
        fd_arr = sec.getElement("fieldData")
        for i in range(fd_arr.numValues()):
            el = fd_arr.getValueAsElement(i)
            row = {}
            if el.hasElement("date"):
                d = el.getElementAsDatetime("date")
                row["date"] = f"{d.year}-{d.month:02d}-{d.day:02d}"
            for f in fields:
                if el.hasElement(f):
                    try:
                        row[f] = el.getElementAsFloat(f)
                    except Exception:
                        row[f] = np.nan
                else:
                    row[f] = np.nan
            rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        _cache_put(cp, df)
    return df


# ==================== BDH Batch: Multi-security Single Date ====================

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_snapshot_bdh(tickers, date_str, fields):
    """BDH multi-security single-date snapshot.
    Returns dict: {ticker: {field: value, ...}}
    CUR_MKT_CAP is in millions USD.
    """
    cp = _cache_path("snap", date_str, ",".join(sorted(fields)),
                     str(len(tickers)))
    cached = _cache_get(cp)
    if cached is not None:
        result = {}
        for _, row in cached.iterrows():
            t = row["ticker"]
            result[t] = {f: row[f] if pd.notna(row[f]) else None
                         for f in fields}
        print(f"    [CACHE] snap {date_str}: {len(result)} tickers")
        return result

    session = get_session()
    svc = session.getService("//blp/refdata")
    result = {}
    batches = list(_chunks(tickers, BATCH_SIZE))

    for idx, batch in enumerate(batches):
        if (idx + 1) % 5 == 0 or idx == 0:
            print(f"    [BBG] snap {date_str}: batch {idx+1}/{len(batches)}")

        req = svc.createRequest("HistoricalDataRequest")
        for t in batch:
            req.getElement("securities").appendValue(t)
        for f in fields:
            req.getElement("fields").appendValue(f)
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", date_str)
        req.set("endDate", date_str)
        session.sendRequest(req)

        for msg in _collect(session):
            if not msg.hasElement("securityData"):
                continue
            sec = msg.getElement("securityData")
            tk = sec.getElementAsString("security")
            if sec.hasElement("securityError"):
                continue
            if not sec.hasElement("fieldData"):
                continue
            fd_arr = sec.getElement("fieldData")
            if fd_arr.numValues() == 0:
                continue
            row = fd_arr.getValueAsElement(0)
            vals = {}
            for f in fields:
                if row.hasElement(f):
                    try:
                        vals[f] = row.getElementAsFloat(f)
                    except Exception:
                        vals[f] = None
                else:
                    vals[f] = None
            result[tk] = vals

    if result:
        recs = [{"ticker": t, **v} for t, v in result.items()]
        _cache_put(cp, pd.DataFrame(recs))
    return result


def get_consensus_bdh(tickers, date_str, consensus_field):
    """BDH consensus with BEST_FPERIOD_OVERRIDE (mandatory).
    Returns dict: {ticker: float_or_None}
    """
    cp = _cache_path("cons", date_str, consensus_field, str(len(tickers)))
    cached = _cache_get(cp)
    if cached is not None:
        result = {}
        for _, row in cached.iterrows():
            v = row[consensus_field] if pd.notna(row[consensus_field]) else None
            result[row["ticker"]] = v
        print(f"    [CACHE] consensus {date_str}: {len(result)} tickers")
        return result

    session = get_session()
    svc = session.getService("//blp/refdata")
    result = {}
    batches = list(_chunks(tickers, BATCH_SIZE))

    for idx, batch in enumerate(batches):
        if (idx + 1) % 5 == 0 or idx == 0:
            print(f"    [BBG] consensus {date_str}: "
                  f"batch {idx+1}/{len(batches)}")

        req = svc.createRequest("HistoricalDataRequest")
        for t in batch:
            req.getElement("securities").appendValue(t)
        req.getElement("fields").appendValue(consensus_field)
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", date_str)
        req.set("endDate", date_str)
        overrides = req.getElement("overrides")
        ov = overrides.appendElement()
        ov.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
        ov.setElement("value", BEST_FPERIOD)
        session.sendRequest(req)

        for msg in _collect(session):
            if not msg.hasElement("securityData"):
                continue
            sec = msg.getElement("securityData")
            tk = sec.getElementAsString("security")
            if sec.hasElement("securityError"):
                continue
            if not sec.hasElement("fieldData"):
                continue
            fd_arr = sec.getElement("fieldData")
            if fd_arr.numValues() == 0:
                continue
            row = fd_arr.getValueAsElement(0)
            if row.hasElement(consensus_field):
                try:
                    result[tk] = row.getElementAsFloat(consensus_field)
                except Exception:
                    result[tk] = None
            else:
                result[tk] = None

    if result:
        recs = [{"ticker": t, consensus_field: v} for t, v in result.items()]
        _cache_put(cp, pd.DataFrame(recs))
    return result
