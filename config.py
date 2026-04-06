"""
错杀抄底 — 全局配置
"""

# ========================= 指数选择 =========================
INDEX_MAP = {
    "SP500":  ("SPX Index",  "RIY Index"),    # (价格指数, 成分股指数)
    "NASDAQ": ("CCMP Index", "NDX Index"),
    "DOW":    ("INDU Index", "INDU Index"),
}
DEFAULT_INDEX = "SP500"

# ========================= 回撤检测 =========================
DRAWDOWN_THRESHOLD  = -0.05    # 回撤阈值: -5% 才算一次有效回撤
LOOKBACK_YEARS      = 3        # 往回看多少年检测回撤
CHART_PAD_DAYS      = 60       # 可视化前后多取的天数

# ========================= 选股 =========================
# ⚠️ BDH CUR_MKT_CAP 单位是百万美元
MKTCAP_FLOOR_MUSD   = 10_000   # 100 亿美元 = 10,000 百万美元
BATCH_SIZE          = 40       # Bloomberg 每批证券数
TIMEOUT_MS          = 30000

# ========================= 一致预期 =========================
# BDH 模式下 BEST_* 必须带 BEST_FPERIOD_OVERRIDE, 否则返回空
CONSENSUS_FIELD_MAP = {
    "EPS":      "BEST_EPS",
    "SALES":    "BEST_SALES",
    "GROSS_MARGIN": "BEST_GROSS_MARGIN",
    "OPER_MARGIN":  "BEST_OPR_MARGIN",
}
DEFAULT_CONSENSUS   = "EPS"
BEST_FPERIOD        = "1BF"

# ========================= INDX_MEMBERS Ticker 转换 =========================
US_EXCHANGE_CODES = {"UW", "UN", "UA", "UQ", "UP", "UV", "UR", "UC"}

# ========================= 缓存 =========================
CACHE_DIR           = "./cache"
CACHE_STALE_HOURS   = 12       # 缓存过期时间

# ========================= 因子策略 =========================
TOP_N               = 30       # 每个策略选多少只

# ========================= 输出 =========================
OUTPUT_DIR          = "./output"
