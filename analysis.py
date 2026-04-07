"""
Buy-the-Dip - Drawdown Detection + Factor Computation

FACTOR DEFINITIONS (all cross-sectional):

1. OVERSOLD FACTOR (higher = more oversold, positive direction):
   - Prerequisite: stock price declined during drawdown (px_chg_dd < 0)
   - Value: est_chg_dd - px_chg_dd
   - Example: EPS -5%, price -30% -> factor = -0.05 - (-0.30) = +0.25
   - Meaning: consensus estimate held up better than price

2. LOW RECOVERY FACTOR (lower = less recovered, negative direction):
   - Value: px_chg_recov = (P_T2 - P_T1) / P_T1
   - Stocks that haven't bounced back score LOWER

3. OVERSOLD + LOW RECOVERY COMPOSITE (positive direction):
   - zscore(oversold_factor) * 0.62 - zscore(px_chg_recov) * 0.38
   - Subtracting recovery: stocks with LOW recovery get a BOOST

4. PERSISTENT OVERSOLD FACTOR (positive direction):
   - ongoing_oversold = est_chg_recov - px_chg_recov (during T1->T2)
   - zscore(oversold_factor) * 0.62 + zscore(ongoing_oversold) * 0.38
   - Stocks that remain fundamentally undervalued after the trough
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import (DRAWDOWN_THRESHOLD, LOOKBACK_YEARS, MKTCAP_FLOOR_MUSD,
                    CONSENSUS_FIELD_MAP, TOP_N,
                    W_OVERSOLD, W_LOWRECOV, W_PERSIST_OS, W_PERSIST_OG)
import bbg_data as bbg


# ==================== Safe Math ====================

def safe_pct_change(new, old):
    """(new - old) / |old|, returns NaN for zero/NaN inputs."""
    new = np.asarray(new, dtype=float)
    old = np.asarray(old, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(
            (np.abs(old) < 1e-12) | np.isnan(new) | np.isnan(old),
            np.nan,
            (new - old) / np.abs(old)
        )
    return result


def cross_sectional_zscore(series):
    """Z-score across the cross-section, NaN-tolerant."""
    s = series.copy()
    valid = s.dropna()
    if len(valid) < 3 or valid.std() < 1e-12:
        return pd.Series(0.0, index=series.index)
    return (s - valid.mean()) / valid.std()


# ==================== Drawdown Detection ====================

def detect_drawdowns(prices_df, threshold=DRAWDOWN_THRESHOLD):
    """
    Detect drawdown events from daily prices.
    Input: DataFrame with [date, PX_LAST]
    Returns: list of dicts sorted by trough_date descending (most recent first)
    """
    df = prices_df.dropna(subset=["PX_LAST"]).sort_values("date").reset_index(drop=True)
    if len(df) < 10:
        return []

    px = df["PX_LAST"].values
    dates = df["date"].values
    cummax = np.maximum.accumulate(px)
    dd = (px - cummax) / cummax

    in_dd = dd < threshold
    events = []
    i = 0
    while i < len(dd):
        if in_dd[i]:
            start_i = i
            while i < len(dd) and in_dd[i]:
                i += 1
            end_i = i

            # Trough = lowest point in this drawdown segment
            seg = dd[start_i:end_i]
            trough_i = start_i + np.argmin(seg)

            # Peak = the actual high before this drawdown
            peak_price = cummax[start_i]
            candidates = np.where(px[:start_i + 1] >= peak_price * 0.9999)[0]
            peak_i = candidates[-1] if len(candidates) > 0 else start_i

            events.append({
                "peak_date":    pd.Timestamp(dates[peak_i]),
                "peak_price":   float(px[peak_i]),
                "trough_date":  pd.Timestamp(dates[trough_i]),
                "trough_price": float(px[trough_i]),
                "drawdown_pct": float(dd[trough_i]),
            })
        else:
            i += 1

    events.sort(key=lambda x: x["trough_date"], reverse=True)
    return events


def find_two_drawdowns(index_ticker, ref_date,
                       lookback_years=LOOKBACK_YEARS,
                       threshold=DRAWDOWN_THRESHOLD):
    """Find the two most recent drawdowns. Returns (current, previous, prices_df)."""
    end = ref_date.strftime("%Y%m%d")
    start = (ref_date - timedelta(days=lookback_years * 365)).strftime("%Y%m%d")

    print(f"\n  Fetching {index_ticker} daily ({start} -> {end})...")
    prices_df = bbg.get_daily_prices(index_ticker, start, end)
    if prices_df.empty:
        print("  [ERROR] No price data")
        return None, None, None

    print(f"  {len(prices_df)} trading days loaded")
    events = detect_drawdowns(prices_df, threshold)

    if not events:
        print(f"  [WARN] No drawdowns > {abs(threshold)*100:.0f}% detected")
        return None, None, prices_df

    print(f"  Found {len(events)} drawdown(s):")
    for i, ev in enumerate(events[:5]):
        print(f"    [{i+1}] {ev['peak_date'].strftime('%Y-%m-%d')} -> "
              f"{ev['trough_date'].strftime('%Y-%m-%d')}  "
              f"{ev['drawdown_pct']*100:+.1f}%")

    current = events[0]
    previous = events[1] if len(events) > 1 else None
    return current, previous, prices_df


# ==================== Core Factor Computation ====================

def compute_factors(member_index, ref_date, peak_date, trough_date,
                    consensus_key="EPS", mktcap_floor=MKTCAP_FLOOR_MUSD):
    """
    Main screening and factor computation.

    T0 = peak_date (drawdown start)
    T1 = trough_date (drawdown end)
    T2 = ref_date (observation date / today)

    Returns: DataFrame with all factor columns, sorted by persistent_oversold desc.
    """
    consensus_field = CONSENSUS_FIELD_MAP[consensus_key]
    T0 = peak_date.strftime("%Y%m%d")
    T1 = trough_date.strftime("%Y%m%d")
    T2 = ref_date.strftime("%Y%m%d")

    print(f"\n{'='*60}")
    print(f"  Factor Computation")
    print(f"  T0 (peak)   = {T0}")
    print(f"  T1 (trough) = {T1}")
    print(f"  T2 (today)  = {T2}")
    print(f"  Consensus   = {consensus_key} ({consensus_field})")
    print(f"{'='*60}")

    # ---- 1. Get universe ----
    print(f"\n  [1/5] Fetching universe...")
    members = bbg.get_index_members(member_index)
    if not members:
        print("  [ERROR] No members found")
        return pd.DataFrame()

    # ---- 2. Market cap filter at T2 ----
    print(f"\n  [2/5] Market cap filter at T2 (>= ${mktcap_floor/1000:.0f}B)...")
    cap_data = bbg.get_snapshot_bdh(members, T2, ["CUR_MKT_CAP"])
    large_caps = {}
    for t, v in cap_data.items():
        c = v.get("CUR_MKT_CAP")
        if c is not None and c >= mktcap_floor:
            large_caps[t] = c
    print(f"  {len(large_caps)} stocks pass filter")
    tickers = list(large_caps.keys())
    if not tickers:
        return pd.DataFrame()

    # ---- 3. Prices at T0, T1, T2 ----
    print(f"\n  [3/5] Fetching prices at 3 dates...")
    px_T0 = bbg.get_snapshot_bdh(tickers, T0, ["PX_LAST"])
    px_T1 = bbg.get_snapshot_bdh(tickers, T1, ["PX_LAST"])
    px_T2 = bbg.get_snapshot_bdh(tickers, T2, ["PX_LAST"])

    # ---- 4. Consensus at T0, T1, T2 ----
    print(f"\n  [4/5] Fetching consensus ({consensus_key}) at 3 dates...")
    est_T0 = bbg.get_consensus_bdh(tickers, T0, consensus_field)
    est_T1 = bbg.get_consensus_bdh(tickers, T1, consensus_field)
    est_T2 = bbg.get_consensus_bdh(tickers, T2, consensus_field)

    # ---- 5. Build DataFrame + compute factors ----
    print(f"\n  [5/5] Computing factors...")
    rows = []
    for t in tickers:
        rows.append({
            "ticker":  t,
            "mktcap_B": large_caps[t] / 1000,  # millions -> billions
            "px_T0":   (px_T0.get(t) or {}).get("PX_LAST"),
            "px_T1":   (px_T1.get(t) or {}).get("PX_LAST"),
            "px_T2":   (px_T2.get(t) or {}).get("PX_LAST"),
            "est_T0":  est_T0.get(t),
            "est_T1":  est_T1.get(t),
            "est_T2":  est_T2.get(t),
        })
    df = pd.DataFrame(rows)

    # -- Price changes --
    df["px_chg_dd"]    = safe_pct_change(df["px_T1"].values, df["px_T0"].values)
    df["px_chg_recov"] = safe_pct_change(df["px_T2"].values, df["px_T1"].values)

    # -- Estimate changes --
    df["est_chg_dd"]    = safe_pct_change(df["est_T1"].values, df["est_T0"].values)
    df["est_chg_recov"] = safe_pct_change(df["est_T2"].values, df["est_T1"].values)

    # ========== FACTOR 1: Oversold Factor ==========
    # Prerequisite: stock actually declined (px_chg_dd < 0)
    # Value: est_chg_dd - px_chg_dd  (positive = estimate held up better than price)
    raw_oversold = df["est_chg_dd"] - df["px_chg_dd"]
    # Mask out stocks that didn't decline
    df["oversold_factor"] = np.where(
        df["px_chg_dd"] < 0,
        raw_oversold,
        np.nan
    )

    # ========== FACTOR 2: Low Recovery Factor ==========
    # Simply px_chg_recov (negative direction: lower = less recovered)
    df["low_recovery_factor"] = df["px_chg_recov"]

    # ========== FACTOR 3: Oversold + Low Recovery Composite ==========
    # zscore(oversold) * 0.62 - zscore(recovery) * 0.38
    z_os = cross_sectional_zscore(df["oversold_factor"])
    z_recov = cross_sectional_zscore(df["px_chg_recov"])
    df["composite_factor"] = W_OVERSOLD * z_os - W_LOWRECOV * z_recov

    # ========== FACTOR 4: Persistent Oversold Factor ==========
    # ongoing_oversold = est_chg_recov - px_chg_recov (during T1->T2)
    df["ongoing_oversold"] = df["est_chg_recov"] - df["px_chg_recov"]
    z_ongoing = cross_sectional_zscore(df["ongoing_oversold"])
    df["persistent_oversold"] = W_PERSIST_OS * z_os + W_PERSIST_OG * z_ongoing

    # ---- Drop rows missing critical fields ----
    required = ["px_T0", "px_T1", "px_T2", "px_chg_dd", "oversold_factor"]
    before_n = len(df)
    df = df.dropna(subset=required)
    if len(df) < before_n:
        print(f"  Dropped {before_n - len(df)} rows with missing critical data, "
              f"{len(df)} remain")

    # Sort by primary factor (persistent oversold)
    df = df.sort_values("persistent_oversold", ascending=False).reset_index(drop=True)
    df.index += 1
    df.index.name = "rank"

    # ---- Summary stats ----
    if len(df) > 0:
        print(f"\n  Factor summary ({len(df)} stocks):")
        for col in ["px_chg_dd", "px_chg_recov", "est_chg_dd", "est_chg_recov",
                     "oversold_factor", "ongoing_oversold",
                     "composite_factor", "persistent_oversold"]:
            s = df[col].dropna()
            if len(s) > 0:
                print(f"    {col:>22s}: mean={s.mean():+.4f}  "
                      f"med={s.median():+.4f}  "
                      f"[{s.min():+.4f}, {s.max():+.4f}]")

    return df


# ==================== Strategy Selection ====================

def select_top(df, factor_col, n=TOP_N, ascending=False):
    """Select top N by a factor column."""
    valid = df.dropna(subset=[factor_col])
    return valid.nlargest(n, factor_col) if not ascending else valid.nsmallest(n, factor_col)


def apply_all_strategies(df, consensus_key="EPS", top_n=TOP_N):
    """
    Apply all 4 factor strategies. Returns ordered dict of {name: DataFrame}.
    """
    strategies = {}

    # Primary: Persistent Oversold
    strategies["Persistent Oversold"] = select_top(df, "persistent_oversold", top_n)

    # Oversold Factor
    strategies["Oversold Factor"] = select_top(df, "oversold_factor", top_n)

    # Composite (Oversold + Low Recovery)
    strategies["Oversold + Low Recovery"] = select_top(df, "composite_factor", top_n)

    # Low Recovery only (ascending=False because we negate inside composite,
    # but here we want LEAST recovered -> smallest px_chg_recov)
    strategies["Low Recovery"] = select_top(df, "low_recovery_factor", top_n, ascending=True)

    for name, sdf in strategies.items():
        n = len(sdf)
        if n > 0:
            cols = ["ticker", "mktcap_B", "px_chg_dd", "px_chg_recov",
                    "oversold_factor", "persistent_oversold"]
            cols = [c for c in cols if c in sdf.columns]
            print(f"\n  Strategy [{name}]: {n} stocks")
            print(sdf[cols].head(10).to_string(
                index=False,
                float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:,.1f}"))

    return strategies


# ==================== Backtest ====================

def backtest_previous(member_index, prev_event, current_event,
                      consensus_key="EPS"):
    """Backtest using previous drawdown: screen at prev trough, measure to current peak."""
    if prev_event is None:
        print("\n  [INFO] No previous drawdown available, skip backtest")
        return None

    prev_T0 = prev_event["peak_date"]
    prev_T1 = prev_event["trough_date"]
    prev_T2 = current_event["peak_date"]  # measure recovery until next peak

    print(f"\n{'='*60}")
    print(f"  BACKTEST: Previous Drawdown")
    print(f"  Peak:    {prev_T0.strftime('%Y-%m-%d')}")
    print(f"  Trough:  {prev_T1.strftime('%Y-%m-%d')}")
    print(f"  Measure: {prev_T2.strftime('%Y-%m-%d')} (current peak)")
    print(f"{'='*60}")

    df = compute_factors(member_index, prev_T2, prev_T0, prev_T1, consensus_key)
    if df.empty:
        return None

    strategies = apply_all_strategies(df, consensus_key)

    print(f"\n  Backtest returns ({prev_T1.strftime('%Y-%m-%d')} -> "
          f"{prev_T2.strftime('%Y-%m-%d')}):")
    for name, sdf in strategies.items():
        if len(sdf) > 0 and "px_chg_recov" in sdf.columns:
            s = sdf["px_chg_recov"].dropna()
            if len(s) > 0:
                print(f"    {name:30s}: mean={s.mean():+.1%}  "
                      f"median={s.median():+.1%}  n={len(s)}")

    return {"df": df, "strategies": strategies,
            "T0": prev_T0, "T1": prev_T1, "T2": prev_T2}
