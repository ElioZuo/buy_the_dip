"""
Buy-the-Dip - HTML Report Generation (All English, Enhanced Charts)
"""

import pandas as pd
import numpy as np
import os
import base64
from io import BytesIO
from datetime import datetime
from config import OUTPUT_DIR, TOP_N

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.patches import FancyBboxPatch
    import matplotlib.ticker as mticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("  [WARN] matplotlib not installed. Run: pip install matplotlib")

COLORS = {
    "blue": "#2563eb", "red": "#dc2626", "green": "#16a34a",
    "purple": "#9333ea", "orange": "#ea580c", "gray": "#64748b",
    "lightblue": "#dbeafe", "lightred": "#fee2e2", "lightgreen": "#dcfce7",
}


def _to_b64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


# ==================== Chart 1: Index Drawdown ====================

def chart_drawdown(prices_df, event, ref_date, title=""):
    if not HAS_MPL:
        return ""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 8),
                                    gridspec_kw={"height_ratios": [3, 1]}, sharex=True)
    fig.suptitle(title or "Index Drawdown", fontsize=14, fontweight="bold")
    df = prices_df.sort_values("date")

    ax1.plot(df["date"], df["PX_LAST"], color=COLORS["blue"], lw=1.3, label="Price")
    ax1.axvline(event["peak_date"], color=COLORS["green"], ls="--", alpha=.8,
                label=f'Peak (T0): {event["peak_date"].strftime("%Y-%m-%d")}')
    ax1.axvline(event["trough_date"], color=COLORS["red"], ls="--", alpha=.8,
                label=f'Trough (T1): {event["trough_date"].strftime("%Y-%m-%d")}')
    ax1.axvline(pd.Timestamp(ref_date), color=COLORS["purple"], ls="--", alpha=.8,
                label=f'Today (T2): {ref_date.strftime("%Y-%m-%d")}')
    mask = (df["date"] >= event["peak_date"]) & (df["date"] <= event["trough_date"])
    ax1.fill_between(df["date"], df["PX_LAST"], where=mask, alpha=.12, color="red")
    ax1.set_ylabel("Index Level", fontsize=11)
    ax1.legend(loc="upper left", fontsize=9, framealpha=.9)
    ax1.grid(True, alpha=.25)

    cummax = np.maximum.accumulate(df["PX_LAST"].values)
    dd = (df["PX_LAST"].values - cummax) / cummax
    ax2.fill_between(df["date"], dd, 0, color=COLORS["red"], alpha=.35)
    ax2.axhline(0, color="k", lw=.5)
    ax2.set_ylabel("Drawdown", fontsize=11)
    ax2.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax2.grid(True, alpha=.25)
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return _to_b64(fig)


# ==================== Chart 2: Oversold Scatter ====================

def chart_scatter_oversold(df, ck="EPS"):
    if not HAS_MPL or df.empty:
        return ""
    fig, ax = plt.subplots(figsize=(13, 9))
    v = df.dropna(subset=["px_chg_dd", "est_chg_dd", "oversold_factor"])
    if v.empty:
        plt.close(fig)
        return ""

    sc = ax.scatter(v["est_chg_dd"]*100, v["px_chg_dd"]*100,
                    c=v["oversold_factor"]*100, cmap="RdYlGn", s=45, alpha=.7,
                    edgecolors="gray", lw=.5)
    lim = max(abs(v["px_chg_dd"].min()), abs(v["est_chg_dd"].min()))*100 + 5
    ax.plot([-lim, 10], [-lim, 10], "k--", alpha=.35, label="Price chg = Estimate chg")
    ax.fill_between([-lim, 10], [-lim, 10], [-lim-60, -50], alpha=.04, color="green",
                    label="Oversold zone (price fell more)")
    top = v.nlargest(12, "oversold_factor")
    for _, r in top.iterrows():
        nm = r["ticker"].replace(" US Equity", "")
        ax.annotate(nm, (r["est_chg_dd"]*100, r["px_chg_dd"]*100),
                    fontsize=7.5, alpha=.85, xytext=(5, 5), textcoords="offset points")
    ax.set_xlabel(f"{ck} Estimate Change (%)", fontsize=12)
    ax.set_ylabel("Price Change (%)", fontsize=12)
    ax.set_title(f"Oversold Scatter: Price vs {ck} Estimate (T0 -> T1)", fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=.25)
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("Oversold Factor (%)")
    fig.tight_layout()
    return _to_b64(fig)


# ==================== Chart 3: Recovery vs Oversold ====================

def chart_recovery_vs_oversold(df):
    if not HAS_MPL or df.empty:
        return ""
    fig, ax = plt.subplots(figsize=(13, 8))
    v = df.dropna(subset=["oversold_factor", "px_chg_recov"])
    if v.empty:
        plt.close(fig)
        return ""

    colors = np.where(v["px_chg_recov"] < 0, COLORS["red"], COLORS["green"])
    ax.scatter(v["oversold_factor"]*100, v["px_chg_recov"]*100,
               c=colors, s=45, alpha=.6, edgecolors="gray", lw=.5)
    ax.axhline(0, color="k", lw=.8)
    ax.axvline(0, color="k", lw=.8)

    # Highlight quadrant: oversold + not recovered (top-right opportunity)
    ax.axhspan(-999, 0, xmin=0.5, xmax=1.0, alpha=.03, color="blue")
    ax.annotate("OPPORTUNITY\n(Oversold + Not Recovered)", xy=(0.85, 0.15),
                xycoords="axes fraction", fontsize=10, alpha=.4, ha="center",
                fontweight="bold", color=COLORS["blue"])

    top = v.nlargest(10, "oversold_factor")
    for _, r in top.iterrows():
        nm = r["ticker"].replace(" US Equity", "")
        ax.annotate(nm, (r["oversold_factor"]*100, r["px_chg_recov"]*100),
                    fontsize=7.5, alpha=.85, xytext=(5, 3), textcoords="offset points")
    ax.set_xlabel("Oversold Factor (%)", fontsize=12)
    ax.set_ylabel("Price Recovery T1->T2 (%)", fontsize=12)
    ax.set_title("Oversold Factor vs Recovery Progress", fontsize=13)
    ax.grid(True, alpha=.25)
    fig.tight_layout()
    return _to_b64(fig)


# ==================== Chart 4: Persistent Oversold Bar ====================

def chart_persistent_bar(df, top_n=30):
    if not HAS_MPL or df.empty:
        return ""
    fig, ax = plt.subplots(figsize=(14, max(8, top_n * 0.32)))
    top = df.nlargest(top_n, "persistent_oversold").sort_values("persistent_oversold")
    names = top["ticker"].str.replace(" US Equity", "")
    vals = top["persistent_oversold"].values
    colors = [COLORS["blue"] if v > 0 else COLORS["red"] for v in vals]
    ax.barh(range(len(names)), vals, color=colors, alpha=.8, height=.7)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel("Persistent Oversold Score", fontsize=11)
    ax.set_title(f"Top {top_n} Persistent Oversold Stocks", fontsize=13)
    ax.axvline(0, color="k", lw=.5)
    ax.grid(True, alpha=.2, axis="x")
    fig.tight_layout()
    return _to_b64(fig)


# ==================== Chart 5: Factor Distribution ====================

def chart_factor_distributions(df):
    if not HAS_MPL or df.empty:
        return ""
    factors = ["oversold_factor", "low_recovery_factor",
               "composite_factor", "persistent_oversold"]
    titles = ["Oversold Factor", "Low Recovery Factor",
              "Composite Factor", "Persistent Oversold"]
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    for ax, col, title in zip(axes.flat, factors, titles):
        s = df[col].dropna()
        if len(s) < 3:
            ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center")
            ax.set_title(title)
            continue
        ax.hist(s, bins=40, color=COLORS["blue"], alpha=.6, edgecolor="white")
        ax.axvline(s.mean(), color=COLORS["red"], ls="--", label=f"Mean: {s.mean():.3f}")
        ax.axvline(s.median(), color=COLORS["green"], ls="--", label=f"Median: {s.median():.3f}")
        ax.set_title(title, fontsize=12)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=.2)
    fig.suptitle("Factor Distributions (Cross-Section)", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return _to_b64(fig)


# ==================== Chart 6: Ongoing Oversold ====================

def chart_ongoing_scatter(df, ck="EPS"):
    if not HAS_MPL or df.empty:
        return ""
    fig, ax = plt.subplots(figsize=(13, 8))
    v = df.dropna(subset=["est_chg_recov", "px_chg_recov", "ongoing_oversold"])
    if v.empty:
        plt.close(fig)
        return ""
    sc = ax.scatter(v["est_chg_recov"]*100, v["px_chg_recov"]*100,
                    c=v["ongoing_oversold"]*100, cmap="RdYlGn", s=45, alpha=.7,
                    edgecolors="gray", lw=.5)
    lim = max(abs(v["px_chg_recov"]).max(), abs(v["est_chg_recov"]).max())*100 + 5
    ax.plot([-lim, lim], [-lim, lim], "k--", alpha=.35)
    top = v.nlargest(10, "ongoing_oversold")
    for _, r in top.iterrows():
        nm = r["ticker"].replace(" US Equity", "")
        ax.annotate(nm, (r["est_chg_recov"]*100, r["px_chg_recov"]*100),
                    fontsize=7.5, alpha=.85, xytext=(5, 5), textcoords="offset points")
    ax.set_xlabel(f"{ck} Estimate Change T1->T2 (%)", fontsize=12)
    ax.set_ylabel("Price Change T1->T2 (%)", fontsize=12)
    ax.set_title(f"Ongoing Oversold: Price vs {ck} Recovery (T1 -> T2)", fontsize=13)
    ax.grid(True, alpha=.25)
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("Ongoing Oversold (%)")
    fig.tight_layout()
    return _to_b64(fig)


# ==================== HTML Table ====================

def _df_to_html(df, max_rows=50):
    d = df.head(max_rows).copy()
    for col in d.columns:
        if d[col].dtype in [np.float64, np.float32]:
            if any(k in col for k in ["chg", "factor", "oversold", "recovery",
                                       "ongoing", "composite", "persistent"]):
                d[col] = d[col].apply(lambda x: f"{x:+.2%}" if pd.notna(x) else "—")
            elif "mktcap" in col.lower():
                d[col] = d[col].apply(lambda x: f"${x:,.1f}B" if pd.notna(x) else "—")
            elif col.startswith("px_T") or col.startswith("est_"):
                d[col] = d[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "—")
            else:
                d[col] = d[col].apply(lambda x: f"{x:,.4f}" if pd.notna(x) else "—")
    if "ticker" in d.columns:
        d["ticker"] = d["ticker"].str.replace(" US Equity", "")
    return d.to_html(index=True, classes="tbl", border=0, escape=False)


# ==================== Main Report ====================

def generate_report(prices_df, event, ref_date, factor_df, strategies,
                    backtest_result, consensus_key="EPS", index_name="SPX Index"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    T0 = event["peak_date"].strftime("%Y-%m-%d")
    T1 = event["trough_date"].strftime("%Y-%m-%d")
    T2 = ref_date.strftime("%Y-%m-%d")

    # Generate all charts
    img = {}
    img["dd"]       = chart_drawdown(prices_df, event, ref_date,
                                     f"{index_name} Drawdown: {T0} -> {T1}")
    img["scatter"]  = chart_scatter_oversold(factor_df, consensus_key)
    img["recov"]    = chart_recovery_vs_oversold(factor_df)
    img["persist"]  = chart_persistent_bar(factor_df, min(TOP_N, 30))
    img["dist"]     = chart_factor_distributions(factor_df)
    img["ongoing"]  = chart_ongoing_scatter(factor_df, consensus_key)

    def _img_tag(key):
        return (f'<img src="data:image/png;base64,{img[key]}"/>'
                if img.get(key) else '<p>Chart not available</p>')

    # Strategy tables with dropdown
    strat_names = list(strategies.keys())
    display_cols = ["ticker", "mktcap_B", "px_chg_dd", "px_chg_recov",
                    "est_chg_dd", "est_chg_recov",
                    "oversold_factor", "ongoing_oversold",
                    "composite_factor", "persistent_oversold"]

    strat_divs = ""
    strat_options = ""
    for i, (name, sdf) in enumerate(strategies.items()):
        safe_id = name.replace(" ", "_").replace("+", "")
        vis = "block" if i == 0 else "none"
        cols = [c for c in display_cols if c in sdf.columns]
        strat_divs += (f'<div class="strat-panel" id="strat_{safe_id}" '
                       f'style="display:{vis}">\n'
                       f'<h3>{name} ({len(sdf)} stocks)</h3>\n'
                       f'{_df_to_html(sdf[cols])}\n</div>\n')
        sel = "selected" if i == 0 else ""
        strat_options += f'<option value="strat_{safe_id}" {sel}>{name}</option>\n'

    # Backtest section
    bt_html = ""
    if backtest_result:
        bt = backtest_result
        bt_html = f"""
        <h2>7. Backtest: Previous Drawdown</h2>
        <div class="card">
          <p>Previous drawdown: {bt['T0'].strftime('%Y-%m-%d')} -> {bt['T1'].strftime('%Y-%m-%d')}<br>
          Observation date: {bt['T2'].strftime('%Y-%m-%d')}</p>
        """
        for name, sdf in bt["strategies"].items():
            if len(sdf) > 0 and "px_chg_recov" in sdf.columns:
                s = sdf["px_chg_recov"].dropna()
                if len(s) > 0:
                    bt_html += (f'<p><b>{name}</b>: mean={s.mean():+.1%}, '
                                f'median={s.median():+.1%}, n={len(s)}</p>\n')
            cols = [c for c in ["ticker", "mktcap_B", "px_chg_dd", "px_chg_recov",
                                "oversold_factor", "persistent_oversold"]
                    if c in sdf.columns]
            bt_html += _df_to_html(sdf[cols], 20)
        bt_html += "</div>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Oversold Screener Report - {T2}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
         max-width: 1500px; margin: 0 auto; padding: 24px; background: #f8fafc; color: #1a1a2e; }}
  h1 {{ color: #1e3a5f; font-size: 28px; border-bottom: 3px solid #2563eb; padding-bottom: 12px; }}
  h2 {{ color: #1e3a5f; margin-top: 48px; font-size: 22px; border-bottom: 1px solid #cbd5e1; padding-bottom: 8px; }}
  h3 {{ color: #374151; margin-top: 20px; }}
  .card {{ background: white; border-radius: 12px; padding: 24px; margin: 20px 0;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); border: 1px solid #e2e8f0; }}
  .metrics {{ display: flex; flex-wrap: wrap; gap: 16px; margin: 20px 0; }}
  .metric {{ background: white; border-radius: 10px; padding: 16px 24px; text-align: center;
             box-shadow: 0 1px 3px rgba(0,0,0,.06); border: 1px solid #e2e8f0; flex: 1; min-width: 140px; }}
  .metric .val {{ font-size: 26px; font-weight: 700; color: #1e40af; }}
  .metric .lbl {{ font-size: 12px; color: #64748b; margin-top: 4px; }}
  img {{ max-width: 100%; border-radius: 10px; margin: 16px 0;
         box-shadow: 0 2px 12px rgba(0,0,0,.08); }}
  .tbl {{ border-collapse: collapse; width: 100%; font-size: 12.5px; margin: 12px 0; }}
  .tbl th {{ background: #1e3a5f; color: white; padding: 8px 10px; text-align: left;
             position: sticky; top: 0; font-weight: 600; }}
  .tbl td {{ padding: 5px 10px; border-bottom: 1px solid #e2e8f0; white-space: nowrap; }}
  .tbl tr:nth-child(even) {{ background: #f8fafc; }}
  .tbl tr:hover {{ background: #e0f2fe; }}
  select {{ font-size: 14px; padding: 8px 16px; border-radius: 8px; border: 1px solid #cbd5e1;
            background: white; cursor: pointer; }}
  .strat-panel {{ margin-top: 12px; }}
  .note {{ background: #fffbeb; border-left: 4px solid #f59e0b; padding: 12px 16px;
           border-radius: 4px; margin: 16px 0; font-size: 13px; }}
  .footer {{ margin-top: 48px; padding-top: 16px; border-top: 1px solid #e2e8f0;
             font-size: 12px; color: #94a3b8; }}
</style>
</head>
<body>

<h1>Buy-the-Dip: Oversold Screener Report</h1>

<div class="metrics">
  <div class="metric"><div class="val">{index_name}</div><div class="lbl">Index</div></div>
  <div class="metric"><div class="val">{T0}</div><div class="lbl">Peak (T0)</div></div>
  <div class="metric"><div class="val">{T1}</div><div class="lbl">Trough (T1)</div></div>
  <div class="metric"><div class="val">{T2}</div><div class="lbl">Today (T2)</div></div>
  <div class="metric"><div class="val">{event['drawdown_pct']*100:+.1f}%</div><div class="lbl">Index Drawdown</div></div>
  <div class="metric"><div class="val">{len(factor_df)}</div><div class="lbl">Stocks Analyzed</div></div>
  <div class="metric"><div class="val">{consensus_key}</div><div class="lbl">Consensus Metric</div></div>
</div>

<h2>1. Index Drawdown</h2>
<div class="card">{_img_tag("dd")}</div>

<h2>2. Oversold Scatter (T0 -> T1)</h2>
<div class="card">
  <p>X-axis: {consensus_key} estimate change during drawdown. Y-axis: price change.
  Stocks below the diagonal were <b>oversold</b> (price fell more than fundamentals).</p>
  {_img_tag("scatter")}
</div>

<h2>3. Recovery vs Oversold</h2>
<div class="card">
  <p>X-axis: oversold factor. Y-axis: price recovery (T1 -> T2).
  Bottom-right = oversold stocks that <b>have not yet recovered</b>.</p>
  {_img_tag("recov")}
</div>

<h2>4. Ongoing Oversold (T1 -> T2)</h2>
<div class="card">
  <p>Stocks where {consensus_key} estimates continued to outperform price since the trough.
  Above the diagonal = estimate recovery &gt; price recovery.</p>
  {_img_tag("ongoing")}
</div>

<h2>5. Top Persistent Oversold Stocks</h2>
<div class="card">{_img_tag("persist")}</div>

<h2>6. Factor Distributions</h2>
<div class="card">{_img_tag("dist")}</div>

<h2>7. Strategy Selection</h2>
<div class="card">
  <div class="note">
    <b>Factors:</b><br>
    1. <b>Oversold Factor</b> = est_chg - px_chg during drawdown (higher = more oversold)<br>
    2. <b>Low Recovery</b> = px change T1->T2 (lower = less recovered)<br>
    3. <b>Composite</b> = 0.62 * z(oversold) - 0.38 * z(recovery)<br>
    4. <b>Persistent Oversold</b> = 0.62 * z(oversold) + 0.38 * z(ongoing_oversold) [PRIMARY]
  </div>
  <label for="strat-select"><b>Select Strategy:</b></label>
  <select id="strat-select" onchange="switchStrategy(this.value)">
    {strat_options}
  </select>
  {strat_divs}
</div>

<h2>8. Full Factor Data (Top 50)</h2>
<div class="card" style="overflow-x:auto;">
  {_df_to_html(factor_df, 50)}
</div>

{bt_html}

<div class="footer">
  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |
  Data source: Bloomberg Desktop API |
  This report is for informational purposes only and does not constitute investment advice.
</div>

<script>
function switchStrategy(id) {{
  document.querySelectorAll('.strat-panel').forEach(el => el.style.display = 'none');
  document.getElementById(id).style.display = 'block';
}}
</script>
</body>
</html>"""

    path = os.path.join(OUTPUT_DIR, f"oversold_report_{T2}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  Report saved: {path}")
    return path
