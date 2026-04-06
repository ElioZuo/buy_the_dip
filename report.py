"""
错杀抄底 — HTML 可视化报告
"""

import pandas as pd
import numpy as np
import os
import base64
from io import BytesIO
from datetime import datetime
from config import OUTPUT_DIR

# matplotlib 延迟导入 (可能需要安装)
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("  [WARN] matplotlib 未安装, 图表将被跳过")
    print("  运行: pip install matplotlib --break-system-packages")


def _fig_to_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight",
                facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def plot_drawdown(prices_df, event, ref_date, title=""):
    """绘制指数价格 + 回撤阴影区域"""
    if not HAS_MPL:
        return ""

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8),
                                    gridspec_kw={"height_ratios": [3, 1]},
                                    sharex=True)
    fig.suptitle(title or "指数回撤可视化", fontsize=14, fontweight="bold")

    df = prices_df.copy()
    df = df.sort_values("date")

    # 上图: 价格
    ax1.plot(df["date"], df["PX_LAST"], color="#2563eb", linewidth=1.2)
    ax1.axvline(event["peak_date"], color="#16a34a", linestyle="--",
                alpha=0.8, label=f'Peak: {event["peak_date"].strftime("%Y-%m-%d")}')
    ax1.axvline(event["trough_date"], color="#dc2626", linestyle="--",
                alpha=0.8, label=f'Trough: {event["trough_date"].strftime("%Y-%m-%d")}')
    ax1.axvline(pd.Timestamp(ref_date), color="#9333ea", linestyle="--",
                alpha=0.8, label=f'T2: {ref_date.strftime("%Y-%m-%d")}')
    ax1.axhspan(event["trough_price"], event["peak_price"],
                xmin=0, xmax=1, alpha=0.05, color="red")
    ax1.fill_between(df["date"], df["PX_LAST"],
                     where=(df["date"] >= event["peak_date"]) &
                           (df["date"] <= event["trough_date"]),
                     alpha=0.15, color="red", label="回撤区间")
    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True, alpha=0.3)

    # 下图: 回撤幅度
    cummax = np.maximum.accumulate(df["PX_LAST"].values)
    dd = (df["PX_LAST"].values - cummax) / cummax
    ax2.fill_between(df["date"], dd, 0, color="#dc2626", alpha=0.4)
    ax2.axhline(0, color="black", linewidth=0.5)
    ax2.set_ylabel("Drawdown %")
    ax2.set_xlabel("Date")
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax2.grid(True, alpha=0.3)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()

    return _fig_to_base64(fig)


def plot_scatter(df, consensus_key="EPS"):
    """散点图: 价格回撤 vs 预期回撤, 颜色=错杀因子"""
    if not HAS_MPL or df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(12, 8))

    valid = df.dropna(subset=["px_drawdown", "est_drawdown", "oversold_factor"])
    if valid.empty:
        plt.close(fig)
        return ""

    sc = ax.scatter(valid["est_drawdown"] * 100, valid["px_drawdown"] * 100,
                    c=valid["oversold_factor"] * 100,
                    cmap="RdYlGn", s=40, alpha=0.7, edgecolors="gray",
                    linewidth=0.5)

    # 45 度线 (价格跌幅 = 预期跌幅)
    lim = max(abs(valid["px_drawdown"].min()), abs(valid["est_drawdown"].min())) * 100 + 5
    ax.plot([-lim, 5], [-lim, 5], "k--", alpha=0.4, label="价格跌幅=预期跌幅")
    ax.fill_between([-lim, 5], [-lim, 5], [-lim - 50, -45],
                    alpha=0.05, color="green", label="错杀区 (价格跌>预期跌)")

    # 标注 top 10 错杀
    top10 = valid.nlargest(10, "oversold_factor")
    for _, row in top10.iterrows():
        name = row["ticker"].replace(" US Equity", "")
        ax.annotate(name, (row["est_drawdown"] * 100, row["px_drawdown"] * 100),
                    fontsize=7, alpha=0.8,
                    xytext=(5, 5), textcoords="offset points")

    ax.set_xlabel(f"{consensus_key} 变动 (%)", fontsize=11)
    ax.set_ylabel("价格变动 (%)", fontsize=11)
    ax.set_title(f"错杀散点图: 股价回撤 vs {consensus_key}预期变动", fontsize=13)
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(True, alpha=0.3)

    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("错杀因子 (%)")

    fig.tight_layout()
    return _fig_to_base64(fig)


def plot_recovery(df, consensus_key="EPS"):
    """修复幅度散点: 价格修复 vs 错杀因子"""
    if not HAS_MPL or df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(12, 7))

    valid = df.dropna(subset=["oversold_factor", "px_recovery_ratio"])
    if valid.empty:
        plt.close(fig)
        return ""

    colors = np.where(valid["px_recovery_ratio"] < 0, "#dc2626", "#16a34a")
    ax.scatter(valid["oversold_factor"] * 100, valid["px_recovery_ratio"] * 100,
               c=colors, s=40, alpha=0.6, edgecolors="gray", linewidth=0.5)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(100, color="green", linewidth=0.5, linestyle=":",
               alpha=0.5, label="100% 完全修复")
    ax.axvline(0, color="black", linewidth=0.8)

    # 标注
    top = valid.nlargest(8, "oversold_factor")
    for _, row in top.iterrows():
        name = row["ticker"].replace(" US Equity", "")
        ax.annotate(name,
                    (row["oversold_factor"] * 100, row["px_recovery_ratio"] * 100),
                    fontsize=7, alpha=0.8,
                    xytext=(5, 3), textcoords="offset points")

    ax.set_xlabel("错杀因子 (%)", fontsize=11)
    ax.set_ylabel("价格修复幅度 (%)", fontsize=11)
    ax.set_title("错杀因子 vs 价格修复进度", fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _df_to_html_table(df, max_rows=50, highlight_col=None):
    """DataFrame → 带样式的 HTML 表格"""
    display_df = df.head(max_rows).copy()

    # 格式化数值
    for col in display_df.columns:
        if display_df[col].dtype in [np.float64, np.float32]:
            if any(k in col for k in ["pct", "drawdown", "recovery", "factor",
                                       "gap", "score", "_z"]):
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:+.2%}" if pd.notna(x) else "—")
            elif "mktcap" in col.lower():
                display_df[col] = display_df[col].apply(
                    lambda x: f"${x:,.1f}B" if pd.notna(x) else "—")
            elif "px_" in col and "drawdown" not in col:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else "—")
            else:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:,.4f}" if pd.notna(x) else "—")

    # 简化 ticker
    if "ticker" in display_df.columns:
        display_df["ticker"] = display_df["ticker"].str.replace(" US Equity", "")

    html = display_df.to_html(index=True, classes="data-table", border=0,
                               escape=False)
    return html


def generate_report(prices_df, event, ref_date, factor_df, strategies,
                    backtest_result, consensus_key="EPS",
                    index_name="SPX Index"):
    """生成完整 HTML 报告"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    T0 = event["peak_date"].strftime("%Y-%m-%d")
    T1 = event["trough_date"].strftime("%Y-%m-%d")
    T2 = ref_date.strftime("%Y-%m-%d")

    # ---- 生成图表 ----
    img_dd = plot_drawdown(prices_df, event, ref_date,
                           f"{index_name} 回撤: {T0} → {T1}")
    img_scatter = plot_scatter(factor_df, consensus_key)
    img_recovery = plot_recovery(factor_df, consensus_key)

    # ---- 策略表格 ----
    strategy_html = ""
    for name, sdf in strategies.items():
        display_cols = ["ticker", "mktcap_B", "px_drawdown", "px_recovery",
                        "px_recovery_ratio", "est_drawdown", "est_recovery",
                        "oversold_factor"]
        display_cols = [c for c in display_cols if c in sdf.columns]
        strategy_html += f'<h3>策略: {name} ({len(sdf)} 只)</h3>\n'
        strategy_html += _df_to_html_table(sdf[display_cols], 30)

    # ---- 回测 ----
    backtest_html = ""
    if backtest_result:
        bt = backtest_result
        backtest_html = f"""
        <h2>回测: 上一轮回撤</h2>
        <p>回撤区间: {bt['T0'].strftime('%Y-%m-%d')} → {bt['T1'].strftime('%Y-%m-%d')}<br>
        观察日: {bt['T2'].strftime('%Y-%m-%d')}</p>
        """
        for name, sdf in bt["strategies"].items():
            if len(sdf) > 0 and "px_recovery" in sdf.columns:
                mean_r = sdf["px_recovery"].mean()
                med_r = sdf["px_recovery"].median()
                backtest_html += (
                    f'<p><b>{name}</b>: '
                    f'平均收益={mean_r:+.1%}, 中位数={med_r:+.1%}, '
                    f'n={len(sdf)}</p>\n')
            display_cols = ["ticker", "mktcap_B", "px_drawdown",
                            "px_recovery", "oversold_factor"]
            display_cols = [c for c in display_cols if c in sdf.columns]
            backtest_html += _df_to_html_table(sdf[display_cols], 15)

    # ---- 组装 HTML ----
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>错杀抄底分析报告 — {T2}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
         max-width: 1400px; margin: 0 auto; padding: 20px; background: #fafafa; color: #1a1a1a; }}
  h1 {{ color: #1e40af; border-bottom: 3px solid #1e40af; padding-bottom: 10px; }}
  h2 {{ color: #1e3a5f; margin-top: 40px; border-bottom: 1px solid #cbd5e1; padding-bottom: 8px; }}
  h3 {{ color: #374151; }}
  .summary {{ background: #f0f9ff; border-left: 4px solid #2563eb; padding: 15px;
              margin: 20px 0; border-radius: 4px; }}
  .summary b {{ color: #1e40af; }}
  .metric {{ display: inline-block; margin: 5px 15px; text-align: center; }}
  .metric .val {{ font-size: 28px; font-weight: bold; color: #1e40af; }}
  .metric .lbl {{ font-size: 12px; color: #64748b; }}
  img {{ max-width: 100%; border: 1px solid #e2e8f0; border-radius: 8px;
         margin: 15px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
  .data-table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin: 10px 0; }}
  .data-table th {{ background: #1e40af; color: white; padding: 8px 12px; text-align: left;
                    position: sticky; top: 0; }}
  .data-table td {{ padding: 6px 12px; border-bottom: 1px solid #e2e8f0; }}
  .data-table tr:nth-child(even) {{ background: #f8fafc; }}
  .data-table tr:hover {{ background: #e0f2fe; }}
  .green {{ color: #16a34a; }} .red {{ color: #dc2626; }}
  .footer {{ margin-top: 40px; padding-top: 15px; border-top: 1px solid #e2e8f0;
             font-size: 12px; color: #94a3b8; }}
</style>
</head>
<body>
<h1>错杀抄底分析报告</h1>

<div class="summary">
  <div class="metric"><div class="val">{index_name}</div><div class="lbl">分析指数</div></div>
  <div class="metric"><div class="val">{T0}</div><div class="lbl">回撤开始 (T0)</div></div>
  <div class="metric"><div class="val">{T1}</div><div class="lbl">回撤结束 (T1)</div></div>
  <div class="metric"><div class="val">{T2}</div><div class="lbl">观察日 (T2)</div></div>
  <div class="metric"><div class="val">{event['drawdown_pct']*100:+.1f}%</div><div class="lbl">指数回撤幅度</div></div>
  <div class="metric"><div class="val">{len(factor_df)}</div><div class="lbl">分析股票数</div></div>
  <div class="metric"><div class="val">{consensus_key}</div><div class="lbl">一致预期指标</div></div>
</div>

<h2>1. 指数回撤可视化</h2>
{'<img src="data:image/png;base64,' + img_dd + '"/>' if img_dd else '<p>图表不可用</p>'}

<h2>2. 错杀散点图</h2>
<p>横轴: {consensus_key}预期变动, 纵轴: 股价变动。对角线下方 = 股价跌幅超过预期跌幅 (被错杀)。</p>
{'<img src="data:image/png;base64,' + img_scatter + '"/>' if img_scatter else '<p>图表不可用</p>'}

<h2>3. 修复进度图</h2>
<p>横轴: 错杀因子, 纵轴: 价格修复幅度 (100%=完全修复, &lt;0=继续跌)。右下角 = 错杀严重+尚未修复。</p>
{'<img src="data:image/png;base64,' + img_recovery + '"/>' if img_recovery else '<p>图表不可用</p>'}

<h2>4. 选股策略</h2>
{strategy_html}

<h2>5. 完整因子数据 (Top 50)</h2>
{_df_to_html_table(factor_df, 50)}

{backtest_html}

<div class="footer">
  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
  数据来源: Bloomberg Desktop API<br>
  注: 本报告仅供参考, 不构成投资建议。
</div>
</body>
</html>"""

    path = os.path.join(OUTPUT_DIR, f"oversold_report_{T2}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  ✓ 报告已保存: {path}")
    return path
