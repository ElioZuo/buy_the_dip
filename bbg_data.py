"""
错杀抄底 — Bloomberg 数据层 (带本地 Parquet 缓存)
"""

import blpapi
import pandas as pd
import numpy as np
import os
import hashlib
import time
from datetime import datetime, timedelta
from config import (CACHE_DIR, CACHE_STALE_HOURS, BATCH_SIZE, TIMEOUT_MS,
                    US_EXCHANGE_CODES, BEST_FPERIOD)


# ==================== Session 管理 ====================

_session = None


def get_session():
    global _session
    if _session is None:
        opts = blpapi.SessionOptions()
        opts.setServerHost("localhost")
        opts.setServerPort(8194)
        _session = blpapi.Session(opts)
        if not _session.start():
            raise RuntimeError("无法连接 Bloomberg")
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


# ==================== 缓存 ====================

def _cache_key(prefix, *args):
    """生成缓存文件路径"""
    raw = f"{prefix}_{'_'.join(str(a) for a in args)}"
    h = hashlib.md5(raw.encode()).hexdigest()[:10]
    safe_prefix = prefix.replace(" ", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe_prefix}_{h}.parquet")


def _cache_get(path):
    """从缓存读取, 如果文件存在且未过期"""
    if not os.path.exists(path):
        return None
    age_hours = (time.time() - os.path.getmtime(path)) / 3600
    if age_hours > CACHE_STALE_HOURS:
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _cache_put(path, df):
    """写入缓存"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as e:
        print(f"    [WARN] 缓存写入失败: {e}")


# ==================== Ticker 转换 ====================

def convert_tickers_to_composite(raw_tickers):
    """
    INDX_MEMBERS 返回交易所专属代码 (UW/UN/UA...),
    需转换为美国复合代码 US。
    'AAPL UW' → 'AAPL US Equity'
    """
    result = []
    for t in raw_tickers:
        parts = t.strip().split()
        if len(parts) == 2 and parts[1] in US_EXCHANGE_CODES:
            result.append(f"{parts[0]} US Equity")
        elif "Equity" not in t:
            result.append(t + " Equity")
        else:
            result.append(t)
    return list(dict.fromkeys(result))  # 去重保序


# ==================== BDS: 指数成分股 ====================

def get_index_members(member_index):
    """获取指数成分股列表, 返回 list[str] (US Equity 格式)"""
    cache_path = _cache_key("members", member_index)
    cached = _cache_get(cache_path)
    if cached is not None:
        print(f"  [缓存] {member_index} 成分股: {len(cached)} 只")
        return cached["ticker"].tolist()

    print(f"  [BBG] 获取 {member_index} 成分股...")
    session = get_session()
    svc = session.getService("//blp/refdata")

    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(member_index)
    req.getElement("fields").appendValue("INDX_MEMBERS")
    session.sendRequest(req)
    msgs = _collect(session)

    raw = []
    for msg in msgs:
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

    tickers = convert_tickers_to_composite(raw)
    print(f"  [BBG] 获取到 {len(tickers)} 只 (原始 {len(raw)} 只)")

    _cache_put(cache_path, pd.DataFrame({"ticker": tickers}))
    return tickers


# ==================== BDH: 历史价格 ====================

def get_daily_prices(ticker, start_date, end_date, fields=None):
    """
    获取单只证券日频数据, 返回 DataFrame (date, PX_LAST, ...)
    """
    if fields is None:
        fields = ["PX_LAST"]
    cache_path = _cache_key("daily", ticker, start_date, end_date,
                            ",".join(fields))
    cached = _cache_get(cache_path)
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
    msgs = _collect(session)

    rows = []
    for msg in msgs:
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
        _cache_put(cache_path, df)
    return df


# ==================== BDH 批量: 多证券单日 ====================

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_snapshot_bdh(tickers, date_str, fields):
    """
    BDH 获取多只证券在某一天的数据
    返回 dict: {ticker: {field: value, ...}}
    ⚠️ CUR_MKT_CAP 返回百万美元
    ⚠️ BDH 一次只返回一只证券的 HistoricalDataResponse
    """
    cache_path = _cache_key("snap", date_str, ",".join(fields),
                            str(len(tickers)))
    cached = _cache_get(cache_path)
    if cached is not None:
        result = {}
        for _, row in cached.iterrows():
            t = row["ticker"]
            result[t] = {f: row[f] if pd.notna(row[f]) else None
                         for f in fields}
        print(f"    [缓存] {date_str} snapshot: {len(result)} 只")
        return result

    session = get_session()
    svc = session.getService("//blp/refdata")
    result = {}
    batches = list(_chunks(tickers, BATCH_SIZE))

    for idx, batch in enumerate(batches):
        if (idx + 1) % 5 == 0 or idx == 0:
            print(f"    [BBG] snapshot {date_str}: "
                  f"批次 {idx+1}/{len(batches)}")

        req = svc.createRequest("HistoricalDataRequest")
        for t in batch:
            req.getElement("securities").appendValue(t)
        for f in fields:
            req.getElement("fields").appendValue(f)
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", date_str)
        req.set("endDate", date_str)
        session.sendRequest(req)
        msgs = _collect(session)

        for msg in msgs:
            if not msg.hasElement("securityData"):
                continue
            sec = msg.getElement("securityData")
            ticker = sec.getElementAsString("security")
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
            result[ticker] = vals

    # 缓存
    if result:
        rows = []
        for t, vals in result.items():
            r = {"ticker": t}
            r.update(vals)
            rows.append(r)
        _cache_put(cache_path, pd.DataFrame(rows))

    return result


def get_consensus_bdh(tickers, date_str, consensus_field):
    """
    BDH 获取一致预期字段 (必须带 BEST_FPERIOD_OVERRIDE)
    返回 dict: {ticker: value}
    """
    cache_path = _cache_key("consensus", date_str, consensus_field,
                            str(len(tickers)))
    cached = _cache_get(cache_path)
    if cached is not None:
        result = {}
        for _, row in cached.iterrows():
            v = row[consensus_field] if pd.notna(row[consensus_field]) else None
            result[row["ticker"]] = v
        print(f"    [缓存] {date_str} consensus {consensus_field}: "
              f"{len(result)} 只")
        return result

    session = get_session()
    svc = session.getService("//blp/refdata")
    result = {}
    batches = list(_chunks(tickers, BATCH_SIZE))

    for idx, batch in enumerate(batches):
        if (idx + 1) % 5 == 0 or idx == 0:
            print(f"    [BBG] consensus {date_str}: "
                  f"批次 {idx+1}/{len(batches)}")

        req = svc.createRequest("HistoricalDataRequest")
        for t in batch:
            req.getElement("securities").appendValue(t)
        req.getElement("fields").appendValue(consensus_field)
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", date_str)
        req.set("endDate", date_str)

        # ⚠️ 必须设置, 否则返回空
        overrides = req.getElement("overrides")
        ov = overrides.appendElement()
        ov.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
        ov.setElement("value", BEST_FPERIOD)

        session.sendRequest(req)
        msgs = _collect(session)

        for msg in msgs:
            if not msg.hasElement("securityData"):
                continue
            sec = msg.getElement("securityData")
            ticker = sec.getElementAsString("security")
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
                    result[ticker] = row.getElementAsFloat(consensus_field)
                except Exception:
                    result[ticker] = None
            else:
                result[ticker] = None

    # 缓存
    if result:
        rows = [{"ticker": t, consensus_field: v} for t, v in result.items()]
        _cache_put(cache_path, pd.DataFrame(rows))

    return result
