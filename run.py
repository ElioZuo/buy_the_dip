#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Buy-the-Dip Oversold Screener - Main Entry

Usage:
  python run.py                         # Default: SP500, latest date, EPS
  python run.py --index NASDAQ
  python run.py --date 20260331
  python run.py --consensus SALES
  python run.py --mktcap 50000          # $50B threshold (in millions)
  python run.py --threshold -0.10       # 10% drawdown threshold
  python run.py --skip-test
  python run.py --no-backtest
"""

import argparse
import sys
import os
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(description="Buy-the-Dip Oversold Screener")
    parser.add_argument("--index", default="SP500",
                        choices=["SP500", "NASDAQ", "DOW"])
    parser.add_argument("--date", default=None, help="Observation date YYYYMMDD")
    parser.add_argument("--consensus", default="EPS",
                        choices=["EPS", "SALES", "GROSS_MARGIN", "OPER_MARGIN"])
    parser.add_argument("--mktcap", type=float, default=None,
                        help="Market cap floor in millions USD (10000=$10B)")
    parser.add_argument("--threshold", type=float, default=None,
                        help="Drawdown threshold (negative, e.g. -0.05)")
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--skip-test", action="store_true")
    parser.add_argument("--no-backtest", action="store_true")
    args = parser.parse_args()

    # Override config
    import config
    if args.mktcap:
        config.MKTCAP_FLOOR_MUSD = args.mktcap
    if args.threshold:
        config.DRAWDOWN_THRESHOLD = args.threshold
    if args.top_n:
        config.TOP_N = args.top_n

    from config import INDEX_MAP, OUTPUT_DIR, CACHE_DIR, TOP_N

    t0 = time.time()
    price_index, member_index = INDEX_MAP[args.index]

    print("=" * 60)
    print("  Buy-the-Dip Oversold Screener")
    print("=" * 60)
    print(f"  Index:      {args.index} ({price_index} / {member_index})")
    print(f"  Consensus:  {args.consensus}")
    print(f"  MktCap:     >= ${config.MKTCAP_FLOOR_MUSD/1000:,.0f}B")
    print(f"  Drawdown:   {config.DRAWDOWN_THRESHOLD:.0%}")
    print(f"  Top N:      {TOP_N}")
    print("=" * 60)

    # ---- Step 0: API Test ----
    if not args.skip_test:
        print("\n[Step 0] API Smoke Test")
        print("-" * 40)
        from test_api import run_all_tests
        if not run_all_tests():
            print("\n  FAILED. Fix issues or use --skip-test.")
            sys.exit(1)
    else:
        print("\n[Step 0] Skipped (--skip-test)")

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    import analysis
    import bbg_data as bbg

    # ---- Determine ref_date ----
    if args.date:
        ref_date = datetime.strptime(args.date, "%Y%m%d")
    else:
        end = datetime.now()
        start = end - timedelta(days=10)
        recent = bbg.get_daily_prices(price_index,
                                      start.strftime("%Y%m%d"),
                                      end.strftime("%Y%m%d"))
        if recent.empty:
            print("  [ERROR] Cannot fetch recent price data. Exiting.")
            sys.exit(1)
        ref_date = recent["date"].max().to_pydatetime()
        print(f"\n  Latest trading day: {ref_date.strftime('%Y-%m-%d')}")

    # ---- Step 1: Detect Drawdowns ----
    print(f"\n[Step 1] Detect Drawdowns in {price_index}")
    print("-" * 40)

    current_ev, prev_ev, prices_df = analysis.find_two_drawdowns(
        price_index, ref_date, threshold=config.DRAWDOWN_THRESHOLD)

    if current_ev is None:
        print("\n  [ERROR] No drawdown detected. Try --threshold -0.03")
        bbg.close_session()
        sys.exit(1)

    T0 = current_ev["peak_date"]
    T1 = current_ev["trough_date"]
    T2 = ref_date

    print(f"\n  Key Dates:")
    print(f"    T0 (Peak):    {T0.strftime('%Y-%m-%d')}  price={current_ev['peak_price']:.2f}")
    print(f"    T1 (Trough):  {T1.strftime('%Y-%m-%d')}  price={current_ev['trough_price']:.2f}")
    print(f"    T2 (Today):   {T2.strftime('%Y-%m-%d')}")
    print(f"    Drawdown:     {current_ev['drawdown_pct']*100:+.1f}%")

    # ---- Step 2: Screen + Compute Factors ----
    print(f"\n[Step 2] Screen Stocks & Compute Factors")
    print("-" * 40)

    factor_df = analysis.compute_factors(
        member_index, T2, T0, T1,
        consensus_key=args.consensus,
        mktcap_floor=config.MKTCAP_FLOOR_MUSD)

    if factor_df.empty:
        print("\n  [ERROR] No valid factor data. Exiting.")
        bbg.close_session()
        sys.exit(1)

    factor_path = os.path.join(OUTPUT_DIR, f"factors_{T2.strftime('%Y%m%d')}.csv")
    factor_df.to_csv(factor_path, index=True)
    print(f"\n  Factors saved: {factor_path}")

    # ---- Step 3: Strategy Selection ----
    print(f"\n[Step 3] Strategy Selection (top {TOP_N})")
    print("-" * 40)

    strategies = analysis.apply_all_strategies(factor_df, args.consensus, TOP_N)

    for name, sdf in strategies.items():
        safe = name.replace(" ", "_").replace("+", "")
        path = os.path.join(OUTPUT_DIR, f"strat_{safe}_{T2.strftime('%Y%m%d')}.csv")
        sdf.to_csv(path, index=True)

    # ---- Step 4: Backtest ----
    bt_result = None
    if not args.no_backtest and prev_ev is not None:
        print(f"\n[Step 4] Backtest Previous Drawdown")
        print("-" * 40)
        bt_result = analysis.backtest_previous(
            member_index, prev_ev, current_ev, args.consensus)
    else:
        print(f"\n[Step 4] Backtest skipped")

    # ---- Step 5: Report ----
    print(f"\n[Step 5] Generate HTML Report")
    print("-" * 40)

    import report as rpt
    report_path = rpt.generate_report(
        prices_df, current_ev, T2, factor_df, strategies,
        bt_result, consensus_key=args.consensus, index_name=price_index)

    # ---- Done ----
    bbg.close_session()
    elapsed = time.time() - t0

    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"{'='*60}")
    print(f"  Time:    {elapsed:.1f}s")
    print(f"  Report:  {report_path}")
    print(f"  Factors: {factor_path}")
    print(f"  Cache:   {CACHE_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
