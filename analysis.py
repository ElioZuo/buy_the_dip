"""
错杀抄底 — 回撤检测 + 因子计算
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import (DRAWDOWN_THRESHOLD, LOOKBACK_YEARS, MKTCAP_FLOOR_MUSD,
                    CONSENSUS_FIELD_MAP, TOP_N)
import bbg_data as bbg


# ==================== 回撤检测 ====================

def detect_drawdowns(prices_df, threshold=DRAWDOWN_THRESHOLD):
    """
    在日频价格序列上检测所有回撤事件
    输入: DataFrame with columns [date, PX_LAST]
    返回: list of dict, 每个 dict 包含:
      - peak_date, peak_price
      - trough_date, trough_price
      - drawdown_pct (负数)
    按 trough_date 降序排列 (最近的在前)
    """
    df = prices_df.dropna(subset=["PX_LAST"]).copy()
    if len(df) < 10:
        return []

    df = df.sort_values("date").reset_index(drop=True)
    px = df["PX_LAST"].values
    dates = df["date"].values

    # 计算累计最高价和回撤
    cummax = np.maximum.accumulate(px)
    dd = (px - cummax) / cummax  # 负数

    # 找回撤事件: 连续低于阈值的区间
    in_dd = dd < threshold
    events = []
    i = 0
    while i < len(dd):
        if in_dd[i]:
            # 回撤开始 → 找此段的最低点
            start_i = i
            while i < len(dd) and in_dd[i]:
                i += 1
            end_i = i  # end_i 是回撤结束后的第一个点 (或序列末尾)

            # 此段最低点 = trough
            segment_dd = dd[start_i:end_i]
            trough_offset = np.argmin(segment_dd)
            trough_i = start_i + trough_offset

            # peak = 回撤段开始前的累计最高点
            # 找到 cummax 在 start_i 处对应的日期
            peak_price = cummax[start_i]
            # 向回找到 peak_price 首次达到的位置
            peak_candidates = np.where(px[:start_i + 1] >= peak_price * 0.9999)[0]
            peak_i = peak_candidates[-1] if len(peak_candidates) > 0 else start_i

            events.append({
                "peak_date": pd.Timestamp(dates[peak_i]),
                "peak_price": float(px[peak_i]),
                "trough_date": pd.Timestamp(dates[trough_i]),
                "trough_price": float(px[trough_i]),
                "drawdown_pct": float(dd[trough_i]),
            })
        else:
            i += 1

    # 按 trough_date 降序
    events.sort(key=lambda x: x["trough_date"], reverse=True)
    return events


def find_recent_drawdown(index_ticker, ref_date, lookback_years=LOOKBACK_YEARS,
                         threshold=DRAWDOWN_THRESHOLD):
    """
    找到距离 ref_date 最近的回撤事件
    返回 (drawdown_event, prices_df) 或 (None, None)
    """
    end = ref_date.strftime("%Y%m%d")
    start = (ref_date - timedelta(days=lookback_years * 365)).strftime("%Y%m%d")

    print(f"\n  获取 {index_ticker} 日频数据 ({start} → {end})...")
    prices_df = bbg.get_daily_prices(index_ticker, start, end)

    if prices_df.empty:
        print(f"  [ERROR] 无法获取 {index_ticker} 价格数据")
        return None, None

    print(f"  共 {len(prices_df)} 个交易日")

    events = detect_drawdowns(prices_df, threshold)
    if not events:
        print(f"  [WARN] 未检测到 >{abs(threshold)*100:.0f}% 的回撤")
        return None, None

    print(f"  检测到 {len(events)} 个回撤事件:")
    for i, ev in enumerate(events[:5]):
        print(f"    [{i+1}] {ev['peak_date'].strftime('%Y-%m-%d')} → "
              f"{ev['trough_date'].strftime('%Y-%m-%d')}  "
              f"{ev['drawdown_pct']*100:+.1f}%")

    return events[0], prices_df


def find_two_drawdowns(index_ticker, ref_date, lookback_years=LOOKBACK_YEARS,
                       threshold=DRAWDOWN_THRESHOLD):
    """
    找最近两个回撤 (当前回撤 + 上一个回撤用于回测)
    返回 (current_event, previous_event, prices_df)
    """
    end = ref_date.strftime("%Y%m%d")
    start = (ref_date - timedelta(days=lookback_years * 365)).strftime("%Y%m%d")

    print(f"\n  获取 {index_ticker} 日频数据 ({start} → {end})...")
    prices_df = bbg.get_daily_prices(index_ticker, start, end)
    if prices_df.empty:
        return None, None, None

    print(f"  共 {len(prices_df)} 个交易日")
    events = detect_drawdowns(prices_df, threshold)

    if not events:
        return None, None, prices_df

    current = events[0]
    previous = events[1] if len(events) > 1 else None

    print(f"  检测到 {len(events)} 个回撤事件")
    print(f"  当前回撤: {current['peak_date'].strftime('%Y-%m-%d')} → "
          f"{current['trough_date'].strftime('%Y-%m-%d')}  "
          f"{current['drawdown_pct']*100:+.1f}%")
    if previous:
        print(f"  上一回撤: {previous['peak_date'].strftime('%Y-%m-%d')} → "
              f"{previous['trough_date'].strftime('%Y-%m-%d')}  "
              f"{previous['drawdown_pct']*100:+.1f}%")

    return current, previous, prices_df


# ==================== 选股 + 因子计算 ====================

def _nearest_trading_day(prices_df, target_date, window=10):
    """找到最近的交易日"""
    dates = prices_df["date"].values
    target = pd.Timestamp(target_date)
    diffs = np.abs(dates - target)
    idx = np.argmin(diffs)
    return pd.Timestamp(dates[idx])


def screen_and_compute(member_index, ref_date, peak_date, trough_date,
                       consensus_key="EPS", mktcap_floor=MKTCAP_FLOOR_MUSD):
    """
    主筛选和因子计算流程

    输入:
      member_index: 成分股指数 ticker (如 "RIY Index")
      ref_date: 观察日 (T2)
      peak_date: 回撤开始日 (T0)
      trough_date: 回撤结束日 (T1)
      consensus_key: "EPS" / "SALES" / "GROSS_MARGIN" / "OPER_MARGIN"

    返回: DataFrame, 包含所有因子列
    """
    consensus_field = CONSENSUS_FIELD_MAP[consensus_key]
    T0 = peak_date.strftime("%Y%m%d")
    T1 = trough_date.strftime("%Y%m%d")
    T2 = ref_date.strftime("%Y%m%d")

    print(f"\n{'='*60}")
    print(f"  因子计算: T0={T0}, T1={T1}, T2={T2}")
    print(f"  一致预期: {consensus_key} ({consensus_field})")
    print(f"{'='*60}")

    # ---- 1. 获取成分股 ----
    print(f"\n  [1/5] 获取成分股...")
    members = bbg.get_index_members(member_index)
    if not members:
        print("  [ERROR] 无成分股数据")
        return pd.DataFrame()

    # ---- 2. 获取 T2 市值, 筛选大市值 ----
    print(f"\n  [2/5] 获取 T2={T2} 市值并筛选 > {mktcap_floor/1000:.0f}B...")
    mktcap_data = bbg.get_snapshot_bdh(members, T2, ["CUR_MKT_CAP"])
    # ⚠️ CUR_MKT_CAP 单位是百万美元
    large_caps = {t: v["CUR_MKT_CAP"]
                  for t, v in mktcap_data.items()
                  if v.get("CUR_MKT_CAP") and v["CUR_MKT_CAP"] >= mktcap_floor}
    print(f"  筛选后: {len(large_caps)} 只 (>= {mktcap_floor/1000:.0f}B)")
    tickers = list(large_caps.keys())
    if not tickers:
        return pd.DataFrame()

    # ---- 3. 获取三个时点的价格 ----
    print(f"\n  [3/5] 获取三个时点的价格 (PX_LAST)...")
    px_T0 = bbg.get_snapshot_bdh(tickers, T0, ["PX_LAST"])
    px_T1 = bbg.get_snapshot_bdh(tickers, T1, ["PX_LAST"])
    px_T2 = bbg.get_snapshot_bdh(tickers, T2, ["PX_LAST"])

    # ---- 4. 获取三个时点的一致预期 ----
    print(f"\n  [4/5] 获取三个时点的 {consensus_key}...")
    est_T0 = bbg.get_consensus_bdh(tickers, T0, consensus_field)
    est_T1 = bbg.get_consensus_bdh(tickers, T1, consensus_field)
    est_T2 = bbg.get_consensus_bdh(tickers, T2, consensus_field)

    # ---- 5. 组装 DataFrame ----
    print(f"\n  [5/5] 计算因子...")
    rows = []
    for t in tickers:
        p0 = (px_T0.get(t, {}) or {}).get("PX_LAST")
        p1 = (px_T1.get(t, {}) or {}).get("PX_LAST")
        p2 = (px_T2.get(t, {}) or {}).get("PX_LAST")
        e0 = est_T0.get(t)
        e1 = est_T1.get(t)
        e2 = est_T2.get(t)
        cap = large_caps.get(t)

        rows.append({
            "ticker": t,
            "mktcap_B": cap / 1000 if cap else np.nan,
            "px_T0": p0, "px_T1": p1, "px_T2": p2,
            f"{consensus_key}_T0": e0,
            f"{consensus_key}_T1": e1,
            f"{consensus_key}_T2": e2,
        })

    df = pd.DataFrame(rows)

    # ---- 计算变动百分比 (容忍 NaN) ----
    def safe_pct(a, b):
        """(a - b) / |b|, 处理 0 和 NaN"""
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.where((b == 0) | np.isnan(a) | np.isnan(b),
                         np.nan,
                         (a - b) / np.abs(b))
        return r

    # 价格变动
    df["px_drawdown"] = safe_pct(df["px_T1"].values, df["px_T0"].values)
    df["px_recovery"] = safe_pct(df["px_T2"].values, df["px_T1"].values)

    # 回撤修复幅度: (P2 - P1) / (P0 - P1), P0 > P1 时分母为正
    denom = df["px_T0"].values - df["px_T1"].values
    numer = df["px_T2"].values - df["px_T1"].values
    with np.errstate(divide="ignore", invalid="ignore"):
        df["px_recovery_ratio"] = np.where(
            (denom == 0) | np.isnan(denom) | np.isnan(numer),
            np.nan,
            numer / denom
        )

    # 一致预期变动
    e0_col = f"{consensus_key}_T0"
    e1_col = f"{consensus_key}_T1"
    e2_col = f"{consensus_key}_T2"
    df["est_drawdown"] = safe_pct(df[e1_col].values, df[e0_col].values)
    df["est_recovery"] = safe_pct(df[e2_col].values, df[e1_col].values)

    est_denom = df[e0_col].values - df[e1_col].values
    est_numer = df[e2_col].values - df[e1_col].values
    with np.errstate(divide="ignore", invalid="ignore"):
        df["est_recovery_ratio"] = np.where(
            (np.abs(est_denom) < 1e-9) | np.isnan(est_denom) | np.isnan(est_numer),
            np.nan,
            est_numer / est_denom
        )

    # ---- 错杀因子 ----
    # 错杀 = 股价跌幅 > 预期跌幅 → est_drawdown - px_drawdown > 0 表示错杀
    # (EPS 跌 5%, 股价跌 30% → -0.05 - (-0.30) = 0.25, 正值=错杀)
    df["oversold_factor"] = df["est_drawdown"] - df["px_drawdown"]

    # 修复落差 = 预期修复 - 价格修复 (正值 = 预期恢复了但价格没跟上)
    df["recovery_gap"] = df["est_recovery_ratio"] - df["px_recovery_ratio"]

    # ---- 清理: 移除关键字段缺失的行 ----
    required = ["px_T0", "px_T1", "px_T2", "px_drawdown", "oversold_factor"]
    before = len(df)
    df = df.dropna(subset=required)
    dropped = before - len(df)
    if dropped > 0:
        print(f"  [INFO] 因关键字段缺失移除 {dropped} 只, 剩余 {len(df)} 只")

    # 排序
    df = df.sort_values("oversold_factor", ascending=False).reset_index(drop=True)
    df.index += 1

    # 统计
    n = len(df)
    if n > 0:
        print(f"\n  因子统计 ({n} 只):")
        for col in ["px_drawdown", "px_recovery", "px_recovery_ratio",
                     "est_drawdown", "oversold_factor"]:
            s = df[col].dropna()
            if len(s) > 0:
                print(f"    {col:>22s}: "
                      f"mean={s.mean():+.3f}  "
                      f"med={s.median():+.3f}  "
                      f"[{s.min():+.3f}, {s.max():+.3f}]")

    return df


# ==================== 策略筛选 ====================

def apply_strategies(df, consensus_key="EPS", top_n=TOP_N):
    """
    应用多个选股策略, 返回 dict of DataFrames
    """
    e2_col = f"{consensus_key}_T2"
    e1_col = f"{consensus_key}_T1"

    strategies = {}

    # 策略 1: 错杀最多 (oversold_factor 最大)
    s1 = df.nlargest(top_n, "oversold_factor")
    strategies["错杀最多"] = s1

    # 策略 2: 错杀多 + 修复少
    # 定义: oversold_factor > 中位数 且 px_recovery_ratio < 中位数
    med_os = df["oversold_factor"].median()
    med_rr = df["px_recovery_ratio"].median()
    mask2 = (df["oversold_factor"] > med_os) & (df["px_recovery_ratio"] < med_rr)
    s2 = df[mask2].nlargest(top_n, "oversold_factor")
    strategies["错杀多+修复少"] = s2

    # 策略 3: 错杀多 + EPS 上升 + 修复少
    if e2_col in df.columns and e1_col in df.columns:
        eps_up = df[e2_col] > df[e1_col]
        mask3 = mask2 & eps_up
        s3 = df[mask3].nlargest(top_n, "oversold_factor")
        strategies["错杀多+预期上升+修复少"] = s3

    # 策略 4: 综合评分
    # 标准化各指标后加权
    df_scored = df.copy()
    for col in ["oversold_factor", "recovery_gap"]:
        s = df_scored[col].dropna()
        if s.std() > 1e-9:
            df_scored[f"{col}_z"] = (df_scored[col] - s.mean()) / s.std()
        else:
            df_scored[f"{col}_z"] = 0
    # 价格修复越少分越高 → 取负
    s = df_scored["px_recovery_ratio"].dropna()
    if s.std() > 1e-9:
        df_scored["low_recovery_z"] = -(df_scored["px_recovery_ratio"] - s.mean()) / s.std()
    else:
        df_scored["low_recovery_z"] = 0

    df_scored["composite_score"] = (
        df_scored["oversold_factor_z"].fillna(0) * 0.5 +
        df_scored["low_recovery_z"].fillna(0) * 0.3 +
        df_scored["recovery_gap_z"].fillna(0) * 0.2
    )
    s4 = df_scored.nlargest(top_n, "composite_score")
    strategies["综合评分"] = s4

    for name, sdf in strategies.items():
        print(f"\n  策略 [{name}]: {len(sdf)} 只")
        if len(sdf) > 0:
            cols = ["ticker", "mktcap_B", "px_drawdown", "px_recovery_ratio",
                    "est_drawdown", "oversold_factor"]
            cols = [c for c in cols if c in sdf.columns]
            print(sdf[cols].head(10).to_string(index=False,
                  float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:,.1f}"))

    return strategies


# ==================== 回测 ====================

def backtest_previous(member_index, prev_event, current_event,
                      consensus_key="EPS"):
    """
    用上一个回撤的数据回测: 在上一个 trough 选的错杀股, 到 current peak 的表现
    """
    if prev_event is None:
        print("\n  [INFO] 无上一个回撤, 跳过回测")
        return None

    prev_T0 = prev_event["peak_date"]
    prev_T1 = prev_event["trough_date"]
    # 回测期终点 = 当前回撤的 peak (上一个回撤结束到当前回撤开始之间的高点)
    prev_T2 = current_event["peak_date"]

    print(f"\n{'='*60}")
    print(f"  回测: 上一轮回撤 ({prev_T0.strftime('%Y-%m-%d')} → "
          f"{prev_T1.strftime('%Y-%m-%d')})")
    print(f"  回测观察日: {prev_T2.strftime('%Y-%m-%d')} (当前回撤的 peak)")
    print(f"{'='*60}")

    df = screen_and_compute(member_index, prev_T2, prev_T0, prev_T1,
                            consensus_key)
    if df.empty:
        return None

    strategies = apply_strategies(df, consensus_key)

    # 计算各策略平均收益
    print(f"\n  回测结果 (持有期: {prev_T1.strftime('%Y-%m-%d')} → "
          f"{prev_T2.strftime('%Y-%m-%d')}):")
    for name, sdf in strategies.items():
        if len(sdf) > 0 and "px_recovery" in sdf.columns:
            mean_ret = sdf["px_recovery"].mean()
            med_ret = sdf["px_recovery"].median()
            print(f"    {name:20s}: 平均={mean_ret:+.1%}  中位数={med_ret:+.1%}  "
                  f"(n={len(sdf)})")

    return {"df": df, "strategies": strategies,
            "T0": prev_T0, "T1": prev_T1, "T2": prev_T2}
