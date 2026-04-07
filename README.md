# Buy-the-Dip: Oversold Screener

## Overview

During index drawdowns, identify stocks whose price declined **more than their fundamentals** (consensus estimates) — i.e., "oversold" stocks — and filter for those that **haven't yet recovered**.

## Factor Definitions

```
T0 (Peak)         T1 (Trough)         T2 (Today)
   |--- Drawdown ----|--- Recovery ------|
```

For each stock, at three dates T0, T1, T2 we collect price (P) and consensus estimate (E):

### Factor 1: Oversold Factor *(positive = more oversold)*
```
Prerequisite: stock declined during drawdown (P_T1 < P_T0)
Value: (E_T1 - E_T0)/|E_T0| - (P_T1 - P_T0)/|P_T0|
       = est_chg_dd - px_chg_dd

Example: EPS fell 5%, price fell 30%
  -> (-0.05) - (-0.30) = +0.25 (heavily oversold)
```

### Factor 2: Low Recovery Factor *(negative direction)*
```
Value: (P_T2 - P_T1) / P_T1
Lower = less recovered = more opportunity remaining
```

### Factor 3: Oversold + Low Recovery Composite
```
0.62 * zscore(oversold_factor) - 0.38 * zscore(px_chg_recov)
Subtracting recovery: low-recovery stocks get a boost
```

### Factor 4: Persistent Oversold *(PRIMARY factor)*
```
ongoing_oversold = (E_T2 - E_T1)/|E_T1| - (P_T2 - P_T1)/|P_T1|
                 = est_chg_recov - px_chg_recov

persistent_oversold = 0.62 * zscore(oversold_factor)
                    + 0.38 * zscore(ongoing_oversold)

Stocks that were oversold AND remain undervalued post-trough.
```

## File Structure

```
buy_the_dip/
├── run.py           Main entry (CLI arguments)
├── config.py        All parameters
├── test_api.py      Bloomberg API smoke test (10 checks)
├── bbg_data.py      Data layer + Parquet cache
├── analysis.py      Drawdown detection + factor engine
├── report.py        HTML report (6 charts, strategy dropdown)
├── cache/           Auto-created Parquet cache
└── output/          Reports + CSV
```

## Quick Start

```bash
conda activate bbg_311
pip install matplotlib --break-system-packages  # first time

python run.py                                   # default
python run.py --index NASDAQ --consensus SALES
python run.py --date 20260331 --threshold -0.10
```

## Bloomberg API Notes

- BDH `CUR_MKT_CAP` returns **millions USD** (not dollars)
- BDH `BEST_*` fields **require** `BEST_FPERIOD_OVERRIDE` or returns empty
- `INDX_MEMBERS` returns exchange codes (UW/UN) → must convert to US
