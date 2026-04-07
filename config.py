"""
Buy-the-Dip Oversold Screener - Global Configuration
"""

# ========================= Index Selection =========================
INDEX_MAP = {
    "SP500":  ("SPX Index",  "RIY Index"),
    "NASDAQ": ("CCMP Index", "NDX Index"),
    "DOW":    ("INDU Index", "INDU Index"),
}
DEFAULT_INDEX = "SP500"

# ========================= Drawdown Detection =========================
DRAWDOWN_THRESHOLD  = -0.05
LOOKBACK_YEARS      = 3
CHART_PAD_DAYS      = 60

# ========================= Stock Screening =========================
# BDH CUR_MKT_CAP unit = millions USD (NOT dollars)
MKTCAP_FLOOR_MUSD   = 10_000   # $10B = 10,000M
BATCH_SIZE           = 40
TIMEOUT_MS           = 30000

# ========================= Consensus Estimates =========================
# BDH BEST_* REQUIRES BEST_FPERIOD_OVERRIDE else returns empty
CONSENSUS_FIELD_MAP = {
    "EPS":          "BEST_EPS",
    "SALES":        "BEST_SALES",
    "GROSS_MARGIN": "BEST_GROSS_MARGIN",
    "OPER_MARGIN":  "BEST_OPR_MARGIN",
}
DEFAULT_CONSENSUS   = "EPS"
BEST_FPERIOD        = "1BF"

# ========================= Ticker Conversion =========================
US_EXCHANGE_CODES = {"UW", "UN", "UA", "UQ", "UP", "UV", "UR", "UC"}

# ========================= Factor Weights =========================
W_OVERSOLD   = 0.62
W_LOWRECOV   = 0.38
W_PERSIST_OS = 0.62
W_PERSIST_OG = 0.38

# ========================= Strategy =========================
TOP_N = 50

# ========================= Cache =========================
CACHE_DIR        = "./cache"
CACHE_STALE_HRS  = 12

# ========================= Output =========================
OUTPUT_DIR = "./output"
