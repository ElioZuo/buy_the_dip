#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
错杀抄底 — 主程序入口
=====================
用法:
  python run.py                         # 默认: S&P500, 最近交易日, EPS
  python run.py --index NASDAQ          # 纳指
  python run.py --date 20260331         # 指定日期
  python run.py --consensus SALES       # 用一致预期收入
  python run.py --mktcap 50000          # 市值门槛 500亿美元 (百万美元单位)
  python run.py --threshold -0.10       # 回撤阈值 10%
  python run.py --skip-test             # 跳过 API 测试
  python run.py --no-backtest           # 不做上轮回撤回测

流程:
  1. API 连通性测试
  2. 检测指数最近回撤 → T0 (peak), T1 (trough)
  3. 筛选大市值股票
  4. 获取三个时点 (T0, T1, T2) 的价格和一致预期
  5. 计算错杀因子
  6. 多策略选股
  7. 上轮回撤回测
  8. 生成 HTML 可视化报告
"""

import argparse
import sys
import os
import time
from datetime import datetime, timedelta

# 确保当前目录在 path 中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(description="错杀抄底分析")
    parser.add_argument("--index", default="SP500",
                        choices=["SP500", "NASDAQ", "DOW"],
                        help="分析指数 (default: SP500)")
    parser.add_argument("--date", default=None,
                        help="观察日 YYYYMMDD (default: 最近交易日)")
    parser.add_argument("--consensus", default="EPS",
                        choices=["EPS", "SALES", "GROSS_MARGIN", "OPER_MARGIN"],
                        help="一致预期指标 (default: EPS)")
    parser.add_argument("--mktcap", type=float, default=None,
                        help="市值门槛(百万美元), default=10000 (=100亿美元)")
    parser.add_argument("--threshold", type=float, default=None,
                        help="回撤阈值 (负数), default=-0.05")
    parser.add_argument("--skip-test", action="store_true",
                        help="跳过 API 测试")
    parser.add_argument("--no-backtest", action="store_true",
                        help="不做上轮回撤回测")
    parser.add_argument("--top-n", type=int, default=None,
                        help="每个策略选多少只 (default: 30)")
    args = parser.parse_args()

    # ---- 导入配置 (允许覆盖) ----
    import config
    if args.mktcap:
        config.MKTCAP_FLOOR_MUSD = args.mktcap
    if args.threshold:
        config.DRAWDOWN_THRESHOLD = args.threshold
    if args.top_n:
        config.TOP_N = args.top_n

    from config import INDEX_MAP, CHART_PAD_DAYS, OUTPUT_DIR, CACHE_DIR

    t0 = time.time()
    index_key = args.index
    price_index, member_index = INDEX_MAP[index_key]
    consensus_key = args.consensus

    print("=" * 60)
    print("  错杀抄底分析系统")
    print("=" * 60)
    print(f"  指数:     {index_key} ({price_index} / {member_index})")
    print(f"  一致预期: {consensus_key}")
    print(f"  市值门槛: {config.MKTCAP_FLOOR_MUSD/1000:,.0f}B "
          f"({config.MKTCAP_FLOOR_MUSD:,.0f}M)")
    print(f"  回撤阈值: {config.DRAWDOWN_THRESHOLD:.0%}")
    print(f"  缓存目录: {CACHE_DIR}")
    print(f"  输出目录: {OUTPUT_DIR}")
    print("=" * 60)

    # ---- Step 0: API 测试 ----
    if not args.skip_test:
        print("\n[Step 0] API 连通性测试")
        print("-" * 40)
        from test_api import run_all_tests
        if not run_all_tests():
            print("\n  ✗ API 测试失败, 退出。")
            print("  可用 --skip-test 跳过测试 (不推荐)")
            sys.exit(1)
    else:
        print("\n[Step 0] 跳过 API 测试 (--skip-test)")

    # ---- 确保目录存在 ----
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---- Step 1: 检测回撤 ----
    print(f"\n\n[Step 1] 检测 {price_index} 的回撤")
    print("-" * 40)

    import analysis
    import bbg_data as bbg

    if args.date:
        ref_date = datetime.strptime(args.date, "%Y%m%d")
    else:
        # 用最近的交易日: 先取最近 5 天的数据, 取最后一个有数据的日期
        end = datetime.now()
        start = end - timedelta(days=10)
        recent = bbg.get_daily_prices(price_index,
                                      start.strftime("%Y%m%d"),
                                      end.strftime("%Y%m%d"))
        if recent.empty:
            print("  [ERROR] 无法获取最近价格数据, 退出")
            sys.exit(1)
        ref_date = recent["date"].max().to_pydatetime()
        print(f"  最近交易日: {ref_date.strftime('%Y-%m-%d')}")

    if args.no_backtest:
        event, prices_df = analysis.find_recent_drawdown(
            price_index, ref_date,
            threshold=config.DRAWDOWN_THRESHOLD)
        prev_event = None
    else:
        event, prev_event, prices_df = analysis.find_two_drawdowns(
            price_index, ref_date,
            threshold=config.DRAWDOWN_THRESHOLD)

    if event is None:
        print("\n  [ERROR] 未检测到有效回撤, 退出。")
        print("  可尝试降低阈值: --threshold -0.03")
        bbg.close_session()
        sys.exit(1)

    T0 = event["peak_date"]
    T1 = event["trough_date"]
    T2 = ref_date

    print(f"\n  确定三个关键时点:")
    print(f"    T0 (回撤开始): {T0.strftime('%Y-%m-%d')}  "
          f"指数={event['peak_price']:.2f}")
    print(f"    T1 (回撤结束): {T1.strftime('%Y-%m-%d')}  "
          f"指数={event['trough_price']:.2f}")
    print(f"    T2 (观察日):   {T2.strftime('%Y-%m-%d')}")
    print(f"    回撤幅度:      {event['drawdown_pct']*100:+.1f}%")

    # ---- Step 2: 选股 + 因子计算 ----
    print(f"\n\n[Step 2] 选股与因子计算")
    print("-" * 40)

    factor_df = analysis.screen_and_compute(
        member_index, T2, T0, T1,
        consensus_key=consensus_key,
        mktcap_floor=config.MKTCAP_FLOOR_MUSD
    )

    if factor_df.empty:
        print("\n  [ERROR] 无有效因子数据, 退出")
        bbg.close_session()
        sys.exit(1)

    # 保存因子数据
    factor_path = os.path.join(OUTPUT_DIR,
                               f"factors_{T2.strftime('%Y%m%d')}.csv")
    factor_df.to_csv(factor_path, index=True, index_label="rank")
    print(f"\n  ✓ 因子数据已保存: {factor_path}")

    # ---- Step 3: 策略选股 ----
    print(f"\n\n[Step 3] 策略选股")
    print("-" * 40)

    strategies = analysis.apply_strategies(factor_df, consensus_key,
                                           top_n=config.TOP_N)

    # 保存每个策略
    for name, sdf in strategies.items():
        safe_name = name.replace("+", "_").replace(" ", "")
        path = os.path.join(OUTPUT_DIR,
                            f"strategy_{safe_name}_{T2.strftime('%Y%m%d')}.csv")
        sdf.to_csv(path, index=True, index_label="rank")

    # ---- Step 4: 回测 ----
    backtest_result = None
    if not args.no_backtest and prev_event is not None:
        print(f"\n\n[Step 4] 上轮回撤回测")
        print("-" * 40)
        backtest_result = analysis.backtest_previous(
            member_index, prev_event, event,
            consensus_key=consensus_key
        )
    else:
        print(f"\n\n[Step 4] 跳过回测")

    # ---- Step 5: 生成报告 ----
    print(f"\n\n[Step 5] 生成可视化报告")
    print("-" * 40)

    import report
    report_path = report.generate_report(
        prices_df, event, T2, factor_df, strategies,
        backtest_result, consensus_key=consensus_key,
        index_name=price_index
    )

    # ---- 完成 ----
    bbg.close_session()
    elapsed = time.time() - t0

    print(f"\n{'='*60}")
    print(f"  分析完成!")
    print(f"{'='*60}")
    print(f"  耗时: {elapsed:.1f} 秒")
    print(f"  报告: {report_path}")
    print(f"  因子: {factor_path}")
    print(f"  缓存: {CACHE_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
