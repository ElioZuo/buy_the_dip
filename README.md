# 错杀抄底分析系统

## 概述

在指数回撤期间，找出"股价下跌幅度远超基本面恶化幅度"的股票（即被错杀的股票），并筛选出尚未充分修复的投资机会。

## 原理

```
        Peak (T0)
           /\
          /  \          Current (T2)
         /    \         /
        /      \       /
       /        \_____/
                Trough (T1)
```

对每只股票计算：
- **价格回撤** = (P_T1 - P_T0) / P_T0
- **预期回撤** = (EPS_T1 - EPS_T0) / EPS_T0
- **错杀因子** = 预期回撤 - 价格回撤（正值 = 股价跌幅 > 基本面跌幅 = 被错杀）
- **修复幅度** = (P_T2 - P_T1) / (P_T0 - P_T1)（100% = 完全修复, <0 = 继续跌）

## 文件结构

```
buy_the_dip/
├── run.py           # 主入口 (含命令行参数)
├── config.py        # 全局配置 (阈值/字段/路径)
├── test_api.py      # Bloomberg API 连通性测试
├── bbg_data.py      # Bloomberg 数据层 + Parquet 缓存
├── analysis.py      # 回撤检测 + 因子计算 + 回测
├── report.py        # HTML 可视化报告
├── cache/           # 自动创建, Parquet 缓存
└── output/          # 自动创建, CSV + HTML 报告
```

## 运行

```bash
conda activate bbg_311

# 确保 matplotlib 已安装 (首次运行)
pip install matplotlib --break-system-packages

# 默认运行: S&P500, 最近交易日, EPS
python run.py

# 自定义
python run.py --index NASDAQ --consensus SALES --mktcap 50000
python run.py --date 20260331 --threshold -0.10 --top-n 20
python run.py --skip-test --no-backtest
```

## 参数

| 参数 | 默认值 | 说明 |
|------|-------|------|
| `--index` | SP500 | 指数: SP500 / NASDAQ / DOW |
| `--date` | 最近交易日 | 观察日 YYYYMMDD |
| `--consensus` | EPS | 一致预期: EPS / SALES / GROSS_MARGIN / OPER_MARGIN |
| `--mktcap` | 10000 | 市值门槛(百万美元), 10000=100亿 |
| `--threshold` | -0.05 | 回撤阈值, -0.05=5% |
| `--top-n` | 30 | 每个策略选多少只 |
| `--skip-test` | False | 跳过 API 测试 |
| `--no-backtest` | False | 不做上轮回撤回测 |

## 四个策略

1. **错杀最多** — oversold_factor 最大的 N 只
2. **错杀多+修复少** — 错杀 > 中位数 且 修复 < 中位数
3. **错杀多+预期上升+修复少** — 在 2 的基础上, T2 的 EPS > T1 的 EPS
4. **综合评分** — 标准化加权: 错杀因子×0.5 + 低修复×0.3 + 修复落差×0.2

## 缓存

- 数据自动缓存为 Parquet 文件到 `cache/` 目录
- 默认 12 小时过期 (可在 config.py 修改 CACHE_STALE_HOURS)
- 重新运行时自动使用缓存, 大幅减少 Bloomberg 请求

## 输出

| 文件 | 内容 |
|------|------|
| `output/oversold_report_YYYY-MM-DD.html` | 完整 HTML 报告 (浏览器打开) |
| `output/factors_YYYYMMDD.csv` | 全部股票的因子数据 |
| `output/strategy_*.csv` | 各策略选股列表 |

## Bloomberg API 注意事项

- BDH `CUR_MKT_CAP` 单位是**百万美元** (不是美元)
- BDH `BEST_*` 字段**必须**带 `BEST_FPERIOD_OVERRIDE` 否则返回空
- `INDX_MEMBERS` 返回交易所代码 (UW/UN), 需转换为 US 复合代码
