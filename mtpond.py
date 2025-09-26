# ============================================================
# 1. Imports
# ============================================================
import os
import asyncio
import uuid
import hashlib
import urllib.parse
import time
import math
import json
from typing import Optional, Dict, Any, List, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
import dotenv
import httpx
import jwt
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, false

# ============================================================
# 2. 환경 로드
# ============================================================
dotenv.load_dotenv()
# ============================================================
# 3. DB & 기본 상수
# ============================================================
engine = create_async_engine(os.getenv("dburl"), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
API_BASE = os.getenv("API_BASE", "").rstrip("/")
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "15"))
BASE_UNIT = "KRW"
# ============================================================
# 3b. 주기 리셋 설정
# ============================================================
PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "1800"))  # 30분
class ResetRequested(Exception):
    pass
# ============================================================
# 4. STATIC CONFIG (재시작 필요 / 거의 고정)
# ============================================================
# 지속 하락 판단 (최근 N개 1분봉 중 하락봉 개수와 총 낙폭)
FALLING_LOOKBACK = int(os.getenv("FALLING_LOOKBACK", "6"))
FALLING_MIN_DROPS = int(os.getenv("FALLING_MIN_DROPS", "4"))
FALLING_MIN_TOTAL_DROP_PCT = Decimal(os.getenv("FALLING_MIN_TOTAL_DROP_PCT", "-0.6"))

WATCH_MIN_PNL = Decimal("-10")
ARM_PNL = Decimal("0.25")
HARD_TP = Decimal("0.6")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET
TRAIL_DROP = Decimal("0.15")
TRAIL_START_PNL = Decimal("0.45")
ENABLE_STOP_TRAIL = os.getenv("ENABLE_STOP_TRAIL", "1") == "1"
STOP_SELL_PORTION = Decimal(os.getenv("STOP_SELL_PORTION", "0.10"))
STOP_MAX_SELLS = int(os.getenv("STOP_MAX_SELLS", "6"))
STOP_DISABLE_NEW_BUYS = os.getenv("STOP_DISABLE_NEW_BUYS", "1") == "1"
STOP_REBOUND_COOLDOWN_SEC = int(os.getenv("STOP_REBOUND_COOLDOWN_SEC", "30"))
STOP_COOLDOWN_BATCH_SELL = os.getenv("STOP_COOLDOWN_BATCH_SELL", "1") == "1"
STOP_COOLDOWN_MAX_BATCH = int(os.getenv("STOP_COOLDOWN_MAX_BATCH", "5"))
STOP_RECOVERY_REQUIRE_COOLDOWN = os.getenv("STOP_RECOVERY_REQUIRE_COOLDOWN", "1") == "1"
STOP_RECOVERY_LOG = os.getenv("STOP_RECOVERY_LOG", "1") == "1"
STOP_SIMPLE_MODE = os.getenv("STOP_SIMPLE_MODE", "1") == "1"
# 새 1회성 손절
ENABLE_NEW_STOP = os.getenv("ENABLE_NEW_STOP", "1") == "1"
STOP_LOSS_FIRST_PORTION = Decimal(os.getenv("STOP_LOSS_FIRST_PORTION", "0.5"))
STOP_LOSS_MIN_REMAIN_KRW = Decimal(os.getenv("STOP_LOSS_MIN_REMAIN_KRW", "5000"))
STOP_SECOND_EXIT_EXTRA_PNL = Decimal(os.getenv("STOP_SECOND_EXIT_EXTRA_PNL", "0.3"))
USE_LIMIT_SELL_ON_TRAIL = os.getenv("USE_LIMIT_SELL_ON_TRAIL", "1") == "1"
LIMIT_SELL_REPRICE_INTERVAL_SEC = int(os.getenv("LIMIT_SELL_REPRICE_INTERVAL_SEC", "5"))
LIMIT_SELL_UNFILLED_TIMEOUT_SEC = int(os.getenv("LIMIT_SELL_UNFILLED_TIMEOUT_SEC", "25"))
LIMIT_SELL_FALLBACK_TO_MARKET = os.getenv("LIMIT_SELL_FALLBACK_TO_MARKET", "1") == "1"
LIMIT_SELL_PRICE_MODE = os.getenv("LIMIT_SELL_PRICE_MODE", "bid")
LIMIT_SELL_BID_OFFSET_TICKS = int(os.getenv("LIMIT_SELL_BID_OFFSET_TICKS", "0"))
SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "1.0"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "1.0"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))
INTERVAL_SECONDS = 5
FIVE_MIN_SECONDS = 300
MIN_NOTIONAL_KRW = Decimal("50")
MAX_BACKOFF = 120
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
WHITELIST_MARKETS: List[str] = []
ENFORCE_WHITELIST = False
INTERSECTION_USE_CACHE_ON_EMPTY = os.getenv("INTERSECTION_USE_CACHE_ON_EMPTY", "1") == "1"
INTERSECTION_CACHE_TTL_SEC = int(os.getenv("INTERSECTION_CACHE_TTL_SEC", "180"))
INTERSECTION_MAX_EMPTY_WARN = int(os.getenv("INTERSECTION_MAX_EMPTY_WARN", "5"))
INTERSECTION_BUY_ENABLED = os.getenv("INTERSECTION_BUY_ENABLED", "1") == "1"
INTERSECTION_MIN_SCORE = Decimal(os.getenv("INTERSECTION_MIN_SCORE", "0.6"))
INTERSECTION_MAX_BUY_PER_CYCLE = int(os.getenv("INTERSECTION_MAX_BUY_PER_CYCLE", "1"))
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "180"))
ENABLE_RANGE_BUY = os.getenv("ENABLE_RANGE_BUY", "1") == "1"
MAX_BUY_PER_WINDOW = int(os.getenv("MAX_BUY_PER_WINDOW", "20"))
SKIP_BUY_IF_RECENT_SELL = True
ENABLE_DYNAMIC_MOMENTUM_TP = os.getenv("ENABLE_DYNAMIC_MOMENTUM_TP", "1") == "1"
MOMENTUM_TIER1_SEC = int(os.getenv("MOMENTUM_TIER1_SEC", "60"))
MOMENTUM_TIER1_TP_OFFSET = Decimal(os.getenv("MOMENTUM_TIER1_TP_OFFSET", "0.8"))
MOMENTUM_TIER1_TRAIL_EXTRA = Decimal(os.getenv("MOMENTUM_TIER1_TRAIL_EXTRA", "0.30"))
MOMENTUM_TIER2_SEC = int(os.getenv("MOMENTUM_TIER2_SEC", "180"))
MOMENTUM_TIER2_TP_OFFSET = Decimal(os.getenv("MOMENTUM_TIER2_TP_OFFSET", "0.5"))
MOMENTUM_TIER2_TRAIL_EXTRA = Decimal(os.getenv("MOMENTUM_TIER2_TRAIL_EXTRA", "0.20"))
MOMENTUM_TIER3_SEC = int(os.getenv("MOMENTUM_TIER3_SEC", "300"))
MOMENTUM_TIER3_TP_OFFSET = Decimal(os.getenv("MOMENTUM_TIER3_TP_OFFSET", "0.3"))
MOMENTUM_TIER3_TRAIL_EXTRA = Decimal(os.getenv("MOMENTUM_TIER3_TRAIL_EXTRA", "0.10"))
MOMENTUM_MAX_EXTRA_CAP = Decimal(os.getenv("MOMENTUM_MAX_EXTRA_CAP", "1.2"))
INTERSECTION_TICK_FILTER_ENABLED = os.getenv("INTERSECTION_TICK_FILTER_ENABLED", "1") == "1"
INTERSECTION_TARGET_PROFIT_PCT = Decimal(os.getenv("INTERSECTION_TARGET_PROFIT_PCT", "0.4"))
INTERSECTION_MAX_TICKS = int(os.getenv("INTERSECTION_MAX_TICKS", "10"))
UPRISES_LAST_NONEMPTY: List[dict] = []
UPRISES_LAST_TS: float | None = None
UPRISES_EMPTY_STREAK: int = 0
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"
USE_INITAMT_FOR_INTERSECTION = os.getenv("USE_INITAMT_FOR_INTERSECTION", "1") == "1"
NORMALIZE_MARGIN_PERCENT = os.getenv("NORMALIZE_MARGIN_PERCENT", "0") == "1"
MARGIN_PERCENT_DIVISOR = Decimal(os.getenv("MARGIN_PERCENT_DIVISOR", "1"))
ALLOW_ADDITIONAL_BUY_WHEN_FULL = os.getenv("ALLOW_ADDITIONAL_BUY_WHEN_FULL","1")=="1"
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS","5"))
# Avg Down
AVG_DOWN_ENABLED = 1
AVG_DOWN_FACTOR = 2
AVG_DOWN_BOLL_PERIOD = 20
AVG_DOWN_BOLL_MULT = 2
AVG_DOWN_LOWER_TOUCH_TOL = Decimal(os.getenv("AVG_DOWN_LOWER_TOUCH_TOL", "0.002"))
AVG_DOWN_REBOUND_PCT = Decimal(os.getenv("AVG_DOWN_REBOUND_PCT", "1.0"))
AVG_DOWN_MIN_PNL = -2.0
AVG_DOWN_GLOBAL_COOLDOWN_SEC = 60
AVG_DOWN_MARKET_COOLDOWN_SEC = 999999
AVG_DOWN_TIMEFRAME_MIN = 1
AVG_DOWN_CANDLE_COUNT = 120
AVG_DOWN_REQUIRE_MIN_VOL_KRW = 10000
AVG_DOWN_TP_ENABLED = os.getenv("AVG_DOWN_TP_ENABLED", "1") == "1"
AVG_DOWN_TP_ARM_PNL = Decimal(os.getenv("AVG_DOWN_TP_ARM_PNL", "0.20"))
AVG_DOWN_TP_HARD_PNL = Decimal(os.getenv("AVG_DOWN_TP_HARD_PNL", "0.40"))
AVG_DOWN_TP_TRAIL_START = Decimal(os.getenv("AVG_DOWN_TP_TRAIL_START", "0.40"))
AVG_DOWN_TP_TRAIL_DROP = Decimal(os.getenv("AVG_DOWN_TP_TRAIL_DROP", "0.15"))
AVG_DOWN_TP_SELL_PORTION = Decimal(os.getenv("AVG_DOWN_TP_SELL_PORTION", "1.0"))
AVG_DOWN_TP_RESET_PEAK = os.getenv("AVG_DOWN_TP_RESET_PEAK", "1") == "1"
# 선지정 TP
PREPLACE_HARD_TP = os.getenv("PREPLACE_HARD_TP", "1") == "1"
PREPLACE_TP_PORTION = os.getenv("PREPLACE_TP_PORTION")  # 없으면 HARD_TP_SELL_PORTION 사용
PREPLACE_TP_TIMEOUT_SEC = int(os.getenv("PREPLACE_TP_TIMEOUT_SEC", "0"))  # 0이면 무제한
PREPLACE_ALLOW_CANCEL_FOR_TRAIL = False
PREPLACE_CANCEL_ON_STOP = os.getenv("PREPLACE_CANCEL_ON_STOP", "1") == "1"
PREPLACE_REPRICE_ON_AVG_DOWN = os.getenv("PREPLACE_REPRICE_ON_AVG_DOWN", "1") == "1"
PREPLACE_MODE = os.getenv("PREPLACE_MODE", "HARD_TP1")
PREPLACE_ON_START = os.getenv("PREPLACE_ON_START", "0") == "1"
AVG_DOWN_ACTIVE = None  # {"market": "...", "ts": float}
# ------------------------------------------------------------
# 4b. FULL LIMIT SELL (전체 수량 단일 지정가 유지) 추가 설정
# ------------------------------------------------------------
FULL_LIMIT_SELL_ENABLED = os.getenv("FULL_LIMIT_SELL_ENABLED", "0") == "1"
FULL_LIMIT_SELL_MODE = os.getenv("FULL_LIMIT_SELL_MODE", "percent")  # percent | fixed
FULL_LIMIT_SELL_PERCENT = Decimal(os.getenv("FULL_LIMIT_SELL_PERCENT", "1.0"))
FULL_LIMIT_SELL_PRICE_BASIS = os.getenv("FULL_LIMIT_SELL_PRICE_BASIS", "current")  # current | avg
FULL_LIMIT_SELL_FIXED_PRICE = Decimal(os.getenv("FULL_LIMIT_SELL_FIXED_PRICE", "0"))
FULL_LIMIT_SELL_REPRICE_DIFF_TOL_PCT = Decimal(os.getenv("FULL_LIMIT_SELL_REPRICE_DIFF_TOL_PCT", "0.1"))
FULL_LIMIT_SELL_AMOUNT_TOL = Decimal(os.getenv("FULL_LIMIT_SELL_AMOUNT_TOL", "0.00000001"))
FULL_LIMIT_SELL_MIN_NOTIONAL = Decimal(os.getenv("FULL_LIMIT_SELL_MIN_NOTIONAL", "5500"))
FULL_LIMIT_SELL_REPRICE_INTERVAL_SEC = int(os.getenv("FULL_LIMIT_SELL_REPRICE_INTERVAL_SEC", "300"))
# 항상 재배치(보유코인 있으면 무조건 취소→재주문)
FULL_LIMIT_SELL_FORCE_REPLACE_ALWAYS = os.getenv("FULL_LIMIT_SELL_FORCE_REPLACE_ALWAYS", "0") == "1"
PASSIVE_FORCE_REPLACE_ALWAYS = os.getenv("PASSIVE_FORCE_REPLACE_ALWAYS", "0") == "1"
# 과도한 취소/재주문 방지 최소 간격(초)
FORCE_REPLACE_MIN_INTERVAL_SEC = int(os.getenv("FORCE_REPLACE_MIN_INTERVAL_SEC", "10"))


# ------------------------------------------------------------
# 4c. PASSIVE (Idle holdings) LIMIT SELL 설정
# ------------------------------------------------------------
PASSIVE_LIMIT_SELL_ENABLED = os.getenv("PASSIVE_LIMIT_SELL_ENABLED", "0") == "1"
PASSIVE_LIMIT_SELL_MODE = os.getenv("PASSIVE_LIMIT_SELL_MODE", "percent")  # percent | fixed
PASSIVE_LIMIT_SELL_PERCENT = Decimal(os.getenv("PASSIVE_LIMIT_SELL_PERCENT", "0.6"))
PASSIVE_LIMIT_SELL_PRICE_BASIS = os.getenv("PASSIVE_LIMIT_SELL_PRICE_BASIS", "avg")  # current | avg
PASSIVE_LIMIT_SELL_FIXED_PRICE = Decimal(os.getenv("PASSIVE_LIMIT_SELL_FIXED_PRICE", "0"))
PASSIVE_LIMIT_SELL_REPRICE_DIFF_TOL_PCT = Decimal(os.getenv("PASSIVE_LIMIT_SELL_REPRICE_DIFF_TOL_PCT", "0.25"))
PASSIVE_LIMIT_SELL_REPRICE_INTERVAL_SEC = int(os.getenv("PASSIVE_LIMIT_SELL_REPRICE_INTERVAL_SEC", "600"))
PASSIVE_LIMIT_SELL_MIN_NOTIONAL = Decimal(os.getenv("PASSIVE_LIMIT_SELL_MIN_NOTIONAL", "5500"))
PASSIVE_LIMIT_SELL_AMOUNT_TOL = Decimal(os.getenv("PASSIVE_LIMIT_SELL_AMOUNT_TOL", "0.00000001"))
PASSIVE_LIMIT_SELL_DEBUG = os.getenv("PASSIVE_LIMIT_SELL_DEBUG", "0") == "1"

FULL_LIMIT_SELL_FORCE_REPLACE_ON_INCREASE = os.getenv("FULL_LIMIT_SELL_FORCE_REPLACE_ON_INCREASE", "1") == "1"
FULL_LIMIT_SELL_FORCE_INCREASE_TOL = Decimal(os.getenv("FULL_LIMIT_SELL_FORCE_INCREASE_TOL", "0"))  # 0이면 증가폭 > 0 즉시

PASSIVE_FORCE_REPLACE_ON_INCREASE = os.getenv("PASSIVE_FORCE_REPLACE_ON_INCREASE","1") == "1"
PASSIVE_FORCE_INCREASE_TOL = Decimal(os.getenv("PASSIVE_FORCE_INCREASE_TOL","0"))

FULL_LIMIT_SELL_ADOPT = os.getenv("FULL_LIMIT_SELL_ADOPT", "1") == "1"
FULL_LIMIT_SELL_DEBUG = os.getenv("FULL_LIMIT_SELL_DEBUG", "0") == "1"

MIN_ORDER_NOTIONAL_KRW = Decimal(os.getenv("MIN_ORDER_NOTIONAL_KRW", "5500"))
ORDER_NOTIONAL_BUFFER_PCT = Decimal(os.getenv("ORDER_NOTIONAL_BUFFER_PCT", "0.01"))
DUST_ABS_VOLUME_THRESHOLD = Decimal(os.getenv("DUST_ABS_VOLUME_THRESHOLD", "0.000001"))
DUST_CLEANUP_ENABLED = os.getenv("DUST_CLEANUP_ENABLED","1") == "1"
DUST_LOG_INTERVAL_SEC = int(os.getenv("DUST_LOG_INTERVAL_SEC","600"))

DUST_LAST_LOG: dict[str,float] = {}

BBTREND_API_URL = os.getenv("BBTREND_API_URL", "http://ywydpapa.iptime.org:8000/api/bbtrend30")
BBTREND_MIN_EXPECTED_PCT = float(os.getenv("BBTREND_MIN_EXPECTED_PCT", "0.005"))  # 0.005 = 0.5%
BBTREND_MIN_NOTIONAL_3M = float(os.getenv("BBTREND_MIN_NOTIONAL_3M", "20000000"))
BBTREND_FETCH_INTERVAL_SEC = int(os.getenv("BBTREND_FETCH_INTERVAL_SEC", "30"))
BBTREND_TIMEFRAMES = ("3m", "5m", "15m", "30m")
MAX_MARTIN = int(os.getenv("MAX_MARTIN", "1"))
# 디버그 스위치
DEBUG_INTX = os.getenv("DEBUG_INTX","0") == "1"

# ============================================================
# 5. RUNTIME CONFIG
# ============================================================
_CONFIG_INITIALIZED = False
def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT, LOSS_CUT_RATE
    global ADDITIONAL_BUY_KRW, USE_TICK_RATE, TICK_RATE
    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
    INTERSECTION_BUY_KRW = Decimal(os.getenv("INTERSECTION_BUY_KRW", "200000"))
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))
    BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-0.2"))
    BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.15"))
    STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
    LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))
    STOP_PEAK_INCREMENT = Decimal(os.getenv("STOP_PEAK_INCREMENT", "0.1"))
    ADDITIONAL_BUY_KRW = Decimal("0")
    USE_TICK_RATE = False
    TICK_RATE = Decimal("0")
    _CONFIG_INITIALIZED = True
    print("[INIT] Runtime config 초기화 완료")

def apply_dynamic_config(cfg: dict):
    required_globals = [
        "MAX_ACTIVE_MARKETS","RANGE_BUY_KRW","INTERSECTION_BUY_KRW",
        "MAX_TOTAL_INVEST_PER_MARKET","BUY_RANGE_LOW","BUY_RANGE_HIGH",
        "STOP_TRIGGER_PNL","STOP_PEAK_INCREMENT","ADDITIONAL_BUY_KRW",
        "USE_TICK_RATE","TICK_RATE"
    ]
    miss = [k for k in required_globals if k not in globals()]
    if miss:
        print(f"[CFG] 전역 미초기화 감지:{miss} → init_config() 필요")
        return
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT, ADDITIONAL_BUY_KRW
    global USE_TICK_RATE, TICK_RATE, LOSS_CUT_RATE
    def to_decimal_safe(v, name):
        try:
            if v is None or str(v).strip() == "":
                return None
            return Decimal(str(v))
        except Exception:
            print(f"[CFG] {name} 변환 실패 value={v}")
            return None
    changes = []
    try:
        if "maxCoincnt" in cfg:
            try:
                nv = int(cfg["maxCoincnt"])
                if nv > 0 and nv != MAX_ACTIVE_MARKETS:
                    changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {nv}")
                    MAX_ACTIVE_MARKETS = nv
            except:
                print(f"[CFG] maxCoincnt 변환 실패:{cfg.get('maxCoincnt')}")
        if "initAmt" in cfg:
            val = to_decimal_safe(cfg.get("initAmt"), "initAmt")
            if val and val > 0:
                if val != RANGE_BUY_KRW:
                    changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW} -> {val}")
                    RANGE_BUY_KRW = val
                if USE_INITAMT_FOR_INTERSECTION and val != INTERSECTION_BUY_KRW:
                    changes.append(f"INTERSECTION_BUY_KRW {INTERSECTION_BUY_KRW} -> {val}")
                    INTERSECTION_BUY_KRW = val
        if "addAmt" in cfg:
            val = to_decimal_safe(cfg.get("addAmt"), "addAmt")
            if val and val > 0 and val != ADDITIONAL_BUY_KRW:
                changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW} -> {val}")
                ADDITIONAL_BUY_KRW = val
        if "limitAmt" in cfg:
            val = to_decimal_safe(cfg.get("limitAmt"), "limitAmt")
            if val and val > 0 and val != MAX_TOTAL_INVEST_PER_MARKET:
                changes.append(f"MAX_TOTAL_INVEST_PER_MARKET {MAX_TOTAL_INVEST_PER_MARKET} -> {val}")
                MAX_TOTAL_INVEST_PER_MARKET = val
        if "minMargin" in cfg:
            raw = to_decimal_safe(cfg.get("minMargin"), "minMargin")
            if raw is not None:
                v = raw / MARGIN_PERCENT_DIVISOR if NORMALIZE_MARGIN_PERCENT else raw
                if v != BUY_RANGE_LOW:
                    changes.append(f"BUY_RANGE_LOW {BUY_RANGE_LOW} -> {v}")
                    BUY_RANGE_LOW = v
        if "maxMargin" in cfg:
            raw = to_decimal_safe(cfg.get("maxMargin"), "maxMargin")
            if raw is not None:
                v = raw / MARGIN_PERCENT_DIVISOR if NORMALIZE_MARGIN_PERCENT else raw
                if v != BUY_RANGE_HIGH:
                    changes.append(f"BUY_RANGE_HIGH {BUY_RANGE_HIGH} -> {v}")
                    BUY_RANGE_HIGH = v
        if "lcRate" in cfg:
            val = to_decimal_safe(cfg.get("lcRate"), "lcRate")
            if val is not None:
                mn, mx = Decimal("-15"), Decimal("-0.1")
                if mn <= val <= mx:
                    if val != LOSS_CUT_RATE:
                        changes.append(f"LOSS_CUT_RATE {LOSS_CUT_RATE} -> {val}")
                        LOSS_CUT_RATE = val
                else:
                    print(f"[CFG] lcRate 범위초과 {val} (허용 {mn} ~ {mx})")
        if "lcGap" in cfg:
            g = to_decimal_safe(cfg.get("lcGap"), "lcGap")
            if g is not None:
                if g < 0:
                    print(f"[CFG] lcGap 음수 {g} -> abs 처리")
                    g = abs(g)
                if g != STOP_PEAK_INCREMENT:
                    changes.append(f"STOP_PEAK_INCREMENT {STOP_PEAK_INCREMENT} -> {g}")
                    STOP_PEAK_INCREMENT = g
        if "tickRate" in cfg:
            t = to_decimal_safe(cfg.get("tickRate"), "tickRate")
            if t and t > 0 and t != TICK_RATE:
                changes.append(f"TICK_RATE {TICK_RATE} -> {t}")
                TICK_RATE = t
        if "tickYN" in cfg:
            flag = (str(cfg.get("tickYN")).upper() == "Y")
            if flag != USE_TICK_RATE:
                changes.append(f"USE_TICK_RATE {USE_TICK_RATE} -> {flag}")
                USE_TICK_RATE = flag
    except Exception as e:
        print(f"[CFG] 적용 예외: {e}")
        return
    if BUY_RANGE_LOW > BUY_RANGE_HIGH:
        print(f"[CFG] 경고: BUY_RANGE_LOW({BUY_RANGE_LOW}) > BUY_RANGE_HIGH({BUY_RANGE_HIGH}) → SWAP")
        BUY_RANGE_LOW, BUY_RANGE_HIGH = BUY_RANGE_HIGH, BUY_RANGE_LOW
        changes.append("SWAP BUY_RANGE_LOW/HIGH")
    if changes:
        print("[CFG] 업데이트:\n  " + "\n  ".join(changes))
    else:
        print("[CFG] 변경 없음")
def get_preplace_portion():
    if PREPLACE_TP_PORTION:
        try:
            v = Decimal(PREPLACE_TP_PORTION)
            if v > 0:
                return min(v, Decimal("1"))
        except:
            pass
    return HARD_TP_SELL_PORTION
# ============================================================
# 6. Exclude Markets 파싱
# ============================================================
def parse_exclude_markets() -> set:
    raw = os.getenv("EXCLUDE_MARKETS", "").strip()
    if not raw:
        return set()
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            return {str(x).strip() for x in arr if isinstance(x, str)}
        except Exception:
            pass
    return {p.strip() for p in raw.split(",") if p.strip()}
EXCLUDED_MARKETS = parse_exclude_markets()
# ============================================================
# 7. Upbit API Helper
# ============================================================
async def upbit_request(method: str,
                        url: str,
                        access_key: str,
                        secret_key: str,
                        params: dict | None = None,
                        max_retry: int = 3,
                        expect_json: bool = True) -> dict:
    params = params or {}
    backoff = 1.5
    attempt = 0
    while True:
        attempt += 1
        if method.upper() == "GET":
            token = build_upbit_jwt_with_params(access_key, secret_key, params) if params else build_upbit_jwt_simple(access_key, secret_key)
        else:
            token = build_upbit_jwt_with_params(access_key, secret_key, params)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                if method.upper() == "GET":
                    resp = await client.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    resp = await client.post(url, headers=headers, params=params)
                elif method.upper() == "DELETE":
                    resp = await client.delete(url, headers=headers, params=params)
                else:
                    raise RuntimeError(f"Unsupported method {method}")
            if resp.status_code == 401 and "nonce_used" in resp.text:
                if attempt < max_retry:
                    print(f"[UPBIT][WARN] nonce_used 재시도 attempt={attempt}")
                    await asyncio.sleep(0.3)
                    continue
                raise RuntimeError(f"Upbit nonce 재사용 감지(최대시도 초과) body={resp.text}")
            if resp.status_code == 429:
                if attempt < max_retry:
                    print(f"[UPBIT][RATE] 429 재시도 attempt={attempt}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 8)
                    continue
                raise RuntimeError(f"Too many requests 429 body={resp.text}")
            if resp.status_code >= 400:
                # 400 중 재시도 무의미한 insufficient_funds_* 즉시 반환
               if resp.status_code == 400:
                  try:
                     j = resp.json()
                     err_name = (j.get("error") or {}).get("name")
                     if err_name and err_name.startswith("insufficient_funds_"):
                       # 재시도하지 않고 바로 예외
                         raise RuntimeError(f"HTTP 400 body={resp.text}")
                  except Exception:
                     pass
               raise RuntimeError(f"HTTP {resp.status_code} body={resp.text}")
            if not expect_json:
                return {"raw": resp.text}
            try:
                return resp.json()
            except Exception as je:
                raise RuntimeError(f"JSON 파싱 실패: {je} body={resp.text[:200]}")
        except Exception as e:
            if attempt >= max_retry:
                raise
            print(f"[UPBIT][RETRY] attempt={attempt} err={e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8)
def build_upbit_jwt_simple(access_key: str, secret_key: str) -> str:
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    return jwt.encode(payload, secret_key, algorithm="HS256")
def build_upbit_jwt_with_params(access_key: str, secret_key: str, params: Dict[str, Any]) -> str:
    filtered = {k: v for k, v in params.items() if v is not None}
    query_string = urllib.parse.urlencode(filtered)
    query_hash = hashlib.sha512(query_string.encode()).hexdigest()
    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "query_hash": query_hash,
        "query_hash_alg": "SHA512",
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")
async def http_get_json(url: str, headers=None, params=None, timeout=10.0, max_retry=5):
    backoff = 2
    for attempt in range(1, max_retry + 1):
        try:
            to = httpx.Timeout(timeout, connect=5.0)
            async with httpx.AsyncClient(timeout=to) as client:
                resp = await client.get(url, headers=headers, params=params)
            if resp.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"GET 실패 status={resp.status_code} body={resp.text}")
            return resp.json()
        except Exception:
            if attempt == max_retry:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
async def http_post_json(url: str, headers=None, params=None, timeout=10.0, max_retry=3):
    backoff = 2
    for attempt in range(1, max_retry + 1):
        try:
            to = httpx.Timeout(timeout, connect=5.0)
            async with httpx.AsyncClient(timeout=to) as client:
                resp = await client.post(url, headers=headers, params=params)
            if resp.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"POST 실패 status={resp.status_code} body={resp.text}")
            return resp.json()
        except Exception:
            if attempt == max_retry:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
async def fetch_upbit_accounts(access_key: str, secret_key: str) -> List[Dict[str, Any]]:
    url = "https://api.upbit.com/v1/accounts"
    token = build_upbit_jwt_simple(access_key, secret_key)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return await http_get_json(url, headers=headers)
async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets:
        return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(markets)}
    raw = await http_get_json(url, params=params)
    out = {}
    for item in raw:
        m = item.get("market")
        p = item.get("trade_price")
        if m and p is not None:
            try:
                out[m] = Decimal(str(p))
            except:
                pass
    return out

def parse_decimal(x) -> Decimal:
    return Decimal(str(x))

def quantize_volume(vol: Decimal) -> Decimal:
    # 업비트 다수 코인 8자리 허용. 필요하다면 마켓별 precision 맵 도입
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

def is_dust_volume(volume: Decimal,
                   price: Decimal,
                   min_notional: Decimal = MIN_ORDER_NOTIONAL_KRW,
                   buffer_pct: Decimal = ORDER_NOTIONAL_BUFFER_PCT,
                   abs_threshold: Decimal = DUST_ABS_VOLUME_THRESHOLD) -> tuple[bool,str]:
    """
    반환: (is_dust, reason)
    """
    if volume <= 0:
        return True, "zero_or_negative"
    if volume < abs_threshold:
        return True, f"abs_lt_{abs_threshold}"
    notional = volume * price
    min_req = min_notional * (Decimal("1") + buffer_pct)
    if notional < min_req:
        return True, f"notional_lt_{min_req}"
    # 양쪽 조건 통과 → 최종 반올림 후 0 이 되는지 체크
    vol_q = quantize_volume(volume)
    if vol_q <= 0:
        return True, "quantized_zero"
    return False, ""

def log_dust_once(market: str, volume: Decimal, price: Decimal, reason: str):
    now = time.time()
    prev = DUST_LAST_LOG.get(market, 0)
    if now - prev >= DUST_LOG_INTERVAL_SEC:
        print(f"[DUST][SKIP] {market} vol={volume} price={price} notional={(volume*price):.8f} reason={reason}")
        DUST_LAST_LOG[market] = now


# ============================================================
# 8. DB / 계정 키
# ============================================================
async def get_keys(user_no: int, server_no: int) -> Optional[tuple]:
    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        return result.fetchone()
# ============================================================
# 9. 시장 / 가격 유틸
# ============================================================
async def get_orderbook_top(market: str):
    url = "https://api.upbit.com/v1/orderbook"
    params = {"markets": market}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"orderbook status={r.status_code} body={r.text}")
    data = r.json()
    if not data:
        raise RuntimeError("empty orderbook data")
    ob = data[0]
    bids = ob.get("orderbook_units", [])
    if not bids:
        raise RuntimeError("no orderbook units")
    top = bids[0]
    best_bid = Decimal(str(top["bid_price"]))
    best_ask = Decimal(str(top["ask_price"]))
    return best_bid, best_ask

def to_decimal(x):
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


# 환경변수로 디버깅 on/off
TICK_DEBUG = os.getenv("TICK_DEBUG", "0") == "1"

def get_tick_unit(price: Decimal) -> Decimal:
    p = float(price)
    if p >= 2_000_000: return Decimal("1000")
    if p >= 1_000_000: return Decimal("1000")
    if p >= 500_000:   return Decimal("500")
    if p >= 100_000:   return Decimal("100")
    if p >= 50_000:    return Decimal("50")
    if p >= 10_000:    return Decimal("10")
    if p >= 5_000:     return Decimal("5")
    if p >= 1_000:     return Decimal("1")
    if p >= 100:       return Decimal("1")
    if p >= 10:        return Decimal("0.1")
    if p >= 1:         return Decimal("0.01")
    if p >= 0.1:       return Decimal("0.001")
    if p >= 0.01:      return Decimal("0.0001")
    if p >= 0.001:     return Decimal("0.00001")
    if p >= 0.0001:    return Decimal("0.000001")
    if p >= 0.00001:   return Decimal("0.0000001")
    return Decimal("0.00000001")

def adjust_price_to_tick(raw_price,
                         side: str | None = None,
                         mode: str = "auto") -> Decimal:
    price = to_decimal(raw_price)
    if price <= 0:
        return Decimal("0")
    unit = get_tick_unit(price)

    if mode == "auto":
        if side == "ask":
            mode_eff = "ceil"
        elif side == "bid":
            mode_eff = "floor"
        else:
            mode_eff = "floor"
    else:
        mode_eff = mode

    steps = price / unit
    if mode_eff == "floor":
        steps_i = steps.to_integral_value(rounding=ROUND_FLOOR)
    elif mode_eff == "ceil":
        steps_i = steps.to_integral_value(rounding=ROUND_CEILING)
    elif mode_eff == "round":
        # 표준 반올림 (0.5 up)
        steps_i = (steps + Decimal("0.5")).to_integral_value(rounding=ROUND_FLOOR)
    else:
        steps_i = steps.to_integral_value(rounding=ROUND_FLOOR)

    adjusted = steps_i * unit

    # 정밀도 quantize
    if unit >= 1:
        adjusted_q = adjusted.quantize(Decimal("1"))
    else:
        adjusted_q = adjusted.quantize(unit)

    if TICK_DEBUG:
        print(f"[TICK] raw={price} unit={unit} side={side} mode={mode} -> {adjusted_q}")
    return adjusted_q

def format_display_volume(vol: Decimal) -> str:
    # 8자리 이하에서는 그대로, 다만 abs < 0.000001 이면 "~0 (<1e-6)" 표기
    if vol == 0:
        return "0"
    if abs(vol) < Decimal("0.000001"):
        return f"~0 ({vol})"
    return f"{vol.normalize()}"

# ============================================================
# 10. 캔들 & 볼린저
# ============================================================
async def fetch_minute_candles(market: str, unit: int = 1, count: int = 120):
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    params = {"market": market, "count": count}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"candles status={r.status_code} body={r.text[:120]}")
    data = r.json()
    if not data:
        return []
    data.reverse()
    return data
def compute_bollinger_from_candles(candles: list, period: int, mult: Decimal):
    if len(candles) < period:
        return None
    closes = [Decimal(str(c["trade_price"])) for c in candles]
    window = closes[-period:]
    sma = sum(window) / Decimal(period)
    mean = sma
    var = sum((c - mean) * (c - mean) for c in window) / Decimal(period)
    try:
        std = var.sqrt()
    except:
        std = Decimal("0")
    upper = sma + mult * std
    lower = sma - mult * std
    last_close = closes[-1]
    return (lower, sma, upper, last_close)
# ============================================================
# 11. 시세 조회 캐시
# ============================================================
TRADABLE_MARKETS_CACHE = set()
TRADABLE_MARKETS_CACHE_TS = 0
TRADABLE_CACHE_TTL_SEC = 3600
async def load_tradable_markets():
    global TRADABLE_MARKETS_CACHE, TRADABLE_MARKETS_CACHE_TS
    now = time.time()
    if TRADABLE_MARKETS_CACHE and (now - TRADABLE_MARKETS_CACHE_TS) < TRADABLE_CACHE_TTL_SEC:
        return TRADABLE_MARKETS_CACHE
    try:
        async with httpx.AsyncClient(timeout=5) as cli:
            r = await cli.get("https://api.upbit.com/v1/market/all")
            r.raise_for_status()
            data = r.json()
        TRADABLE_MARKETS_CACHE = {d["market"] for d in data}
        TRADABLE_MARKETS_CACHE_TS = now
    except Exception as e:
        print(f"[WARN] 시장목록 갱신 실패: {e} (이전 캐시 사용)")
    return TRADABLE_MARKETS_CACHE
async def safe_fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets:
        return {}
    try:
        return await fetch_current_prices(markets)
    except Exception as e:
        err_txt = str(e)
        if "404" not in err_txt:
            print(f"[WARN] 일괄시세 조회 실패: {e} → 개별 재시도")
        price_map = {}
        invalid = []
        for m in markets:
            try:
                one = await fetch_current_prices([m])
                price_map.update(one)
            except Exception as ie:
                if "404" in str(ie):
                    invalid.append(m)
                else:
                    print(f"[WARN] 단일티커 실패 {m}: {ie}")
        if invalid:
            print(f"[INIT] 거래불가 제외: {invalid}")
        return price_map
# ============================================================
# 12. PNL 계산
# ============================================================
def calc_pnl_percent(account: dict, current_price) -> Decimal:
    try:
        avg_buy_price = Decimal(str(account.get("avg_buy_price", "0")))
        balance = Decimal(str(account.get("balance", "0")))
        locked = Decimal(str(account.get("locked", "0")))
        qty = balance + locked
        if qty <= 0 or avg_buy_price <= 0:
            return Decimal("0")
        cur = Decimal(str(current_price))
        pnl = (cur - avg_buy_price) / avg_buy_price * Decimal("100")
        return pnl.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError):
        return Decimal("0")
# ============================================================
# 13. 선지정 HARD_TP 주문 함수
# ============================================================
async def place_preplaced_hard_tp(access_key: str, secret_key: str, market: str,
                                  state: dict, balance: Decimal,
                                  avg_buy_price: Decimal):
    if FULL_LIMIT_SELL_ENABLED:
        # 전체지정가 모드에서는 선지정 TP 비활성
        return
    print(f"[PRE_HARD_TP][ENTER] {market} bal={balance} avg={avg_buy_price}")
    if avg_buy_price is None or avg_buy_price <= 0:
        print(f"[PRE_HARD_TP][SKIP] avg_buy_price invalid {avg_buy_price}")
        return
    if balance is None or balance <= 0:
        print("[PRE_HARD_TP][SKIP] balance <= 0")
        return
    if state.get("active_limit_uuid") and state.get("limit_pending_category") == "PRE_HARD_TP1":
        print("[PRE_HARD_TP][SKIP] already has pending PRE_HARD_TP1")
        return
    portion = get_preplace_portion()
    volume = safe_calc_volume(balance, portion)
    print(f"[PRE_HARD_TP][CALC] portion={portion} volume={volume}")
    if volume <= 0:
        print("[PRE_HARD_TP][SKIP] volume<=0 (min size?)")
        return
    target_price_raw = avg_buy_price * (Decimal("1") + HARD_TP / Decimal("100"))
    try:
        best_bid, best_ask = await get_orderbook_top(market)
        base_price = target_price_raw
        if base_price < best_ask:
            base_price = best_ask
        limit_price = adjust_price_to_tick(base_price)
        print(f"[PRE_HARD_TP][PRICE] target_raw={target_price_raw} best_ask={best_ask} final_limit={limit_price}")
    except Exception as e:
        print(f"[PRE_HARD_TP][ERR] orderbook {market}: {e}")
        return
    notional_est = Decimal(str(limit_price)) * volume
    if notional_est < MIN_NOTIONAL_KRW:
        print(f"[PRE_HARD_TP][SKIP] notional {notional_est} < MIN_NOTIONAL_KRW {MIN_NOTIONAL_KRW}")
        return
    params = {
        "market": market,
        "side": "ask",
        "volume": str(volume),
        "price": str(limit_price),
        "ord_type": "limit",
    }
    try:
        resp = await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)
        uid = resp.get("uuid")
        if not uid:
            print("[PRE_HARD_TP][FAIL] uuid not returned")
            return
        state["active_limit_uuid"] = uid
        state["limit_pending_category"] = "PRE_HARD_TP1"
        state["limit_pending_volume"] = str(volume)
        state["limit_submit_ts"] = time.time()
        state["pre_tp_uuid"] = uid
        state["pre_tp_price"] = Decimal(str(limit_price))
        state["pre_tp_volume"] = volume
        state["pre_tp_partial_filled"] = False
        state["pre_tp_source"] = state.get("pre_tp_source") or "initial_or_buy"
        print(f"[PRE_HARD_TP][OK] placed uid={uid}")
    except Exception as e:
        print(f"[PRE_HARD_TP][ERR] post order {market}: {e}")
# ============================================================
# 14. Intersection 데이터
# ============================================================
def _is_effectively_empty(cands):
    return not cands or not any(
        (c.get("market") and c.get("avg_score") is not None)
        for c in cands if isinstance(c, dict)
    )
def _normalize_uprises(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        out = []
        for r in raw:
            if isinstance(r, dict):
                m = r.get("market"); s = r.get("avg_score")
                if m and s is not None:
                    out.append({"market": m, "avg_score": s})
        return out
    if isinstance(raw, str):
        t = raw.strip()
        if not t:
            return []
        try:
            js = json.loads(t)
        except:
            return []
        return _normalize_uprises(js)
    if isinstance(raw, dict):
        if "multi_tf_intersection" in raw:
            return _normalize_uprises(raw.get("multi_tf_intersection", {}).get("items", []))
        if "items" in raw and isinstance(raw["items"], list):
            return _normalize_uprises(raw["items"])
        if raw.get("market") and raw.get("avg_score") is not None:
            return [{"market": raw["market"], "avg_score": raw["avg_score"]}]
        return []
    return []

async def get_intersection_candidates_safe():
    global UPRISES_LAST_NONEMPTY, UPRISES_LAST_TS, UPRISES_EMPTY_STREAK
    meta = {
        "source": None,
        "empty_streak": UPRISES_EMPTY_STREAK,
        "fresh_ts": None,
        "cache_age": None,
        "logic": "bbtrend30"
    }
    now = time.time()
    try:
        cands = await fetch_bbtrend_candidates()
    except Exception as e:
        print(f"[BBTREND][WARN] 예외 발생: {e}")
        cands = []
    meta["fresh_ts"] = now
    if not cands:
        UPRISES_EMPTY_STREAK += 1
        meta["empty_streak"] = UPRISES_EMPTY_STREAK
        use_cache = False
        if INTERSECTION_USE_CACHE_ON_EMPTY and UPRISES_LAST_NONEMPTY and UPRISES_LAST_TS:
            age = now - UPRISES_LAST_TS
            meta["cache_age"] = age
            if age <= INTERSECTION_CACHE_TTL_SEC:
                use_cache = True
        if use_cache:
            meta["source"] = "cache"
            return UPRISES_LAST_NONEMPTY, meta
        else:
            meta["source"] = "empty"
            return [], meta
    # 변환: expected_move_pct (%) → avg_score
    transformed = []
    for c in cands:
        market = c["market"]
        avg_score = Decimal(str(c["expected_move_pct"]))  # 퍼센트 그대로
        transformed.append({"market": market, "avg_score": avg_score})
    UPRISES_EMPTY_STREAK = 0
    UPRISES_LAST_NONEMPTY = transformed
    UPRISES_LAST_TS = now
    meta["source"] = "fresh"
    meta["empty_streak"] = 0
    meta["cache_age"] = 0
    return transformed, meta

# ============================================================
# 15. 포지션 상태 클래스
# ============================================================
class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}
        self.last_buy_window: Dict[str, float] = {}
        self.intersection_last_buy_time: Dict[str, float] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}
    def update_or_init(self, market, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,
                "min_pnl": pnl,
                "max_drawdown": Decimal("0"),
                "last_drawdown": Decimal("0"),
                "max_runup": Decimal("0"),
                "prev_pnl": pnl,
                "armed": False,
                "avg_buy_price": avg_price,
                "hard_tp_taken": False,
                "hard_tp2_taken": False,
                "dynamic_hard_tp2": None,
                "trail_drop_dynamic": None,
                "entry_ts": now,
                "htp1_time": None,
                "momentum_tag": None,
                "last_update_ts": now,
                "entry_source": self.data.get(market, {}).get("entry_source"),
                "stop_triggered": False,
                "stop_last_peak": None,
                "stop_sells_done": 0,
                "worst_pnl": pnl,
                "stop_last_sell_ts": None,
                "stop_cooldown_flag": False,
                "stop_pending_peak": None,
                "stop_cooldown_start_ts": None,
                "avg_down_done": False,
                "avg_down_candidate": False,
                "avg_down_touch_ts": None,
                "avg_down_touch_price": None,
                "avg_down_buy_uuid": None,
                "first_buy_amount": None,
                "avg_down_tp_mode": False,
                "avg_down_tp_completed": False,
                "avg_down_tp_baseline_pnl": None,
                "avg_down_tp_peak_offset": Decimal("0"),
                "avg_down_tp_armed": False,
                "pre_tp_uuid": None,
                "pre_tp_price": None,
                "pre_tp_volume": None,
                "pre_tp_partial_filled": False,
                "pre_tp_source": None,
                "full_limit_uuid": None,
                "full_limit_price": None,
                "full_limit_volume": None,
                "full_limit_ts": None,
                "passive_limit_uuid": None,
                "passive_limit_price": None,
                "passive_limit_volume": None,
                "passive_limit_ts": None,
                # 추가된 손절/마틴 상태
                "stop_mode_active": False,
                "martingale_count": 0,
                "last_martin_ts": None,
            }
            return self.data[market]
        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl
        try:
            drawdown = st["peak_pnl"] - pnl if pnl < st["peak_pnl"] else Decimal("0")
        except Exception:
            drawdown = Decimal("0")
        st["last_drawdown"] = drawdown
        if drawdown > st.get("max_drawdown", Decimal("0")):
            st["max_drawdown"] = drawdown
        try:
            runup = pnl - st["min_pnl"] if pnl > st["min_pnl"] else Decimal("0")
        except Exception:
            runup = Decimal("0")
        if runup > st.get("max_runup", Decimal("0")):
            st["max_runup"] = runup
        if st.get("stop_triggered") and pnl < st.get("worst_pnl", pnl):
            st["worst_pnl"] = pnl
        st["prev_pnl"] = pnl
        st["last_update_ts"] = now
        return st
    def ensure_stop_fields(self, market: str):
        st = self.data.setdefault(market, {})
        st.setdefault("stop_mode_active", False)
        st.setdefault("martingale_count", 0)
    def remove(self, market):
        self.data.pop(market, None)
    def mark_sold(self, market):
        self.last_sell_time[market] = time.time()
        self.remove(market)
        self.buy_info.pop(market, None)
    def reduce_invested_after_sell(self, market, portion: Decimal):
        if portion <= 0:
            return
        info = self.buy_info.get(market)
        if not info or portion >= 1:
            return
        try:
            remain = (Decimal("1") - portion)
            info["total_invested"] = (info["total_invested"] * remain).quantize(Decimal("0.0001"))
        except:
            pass
    def recently_sold(self, market):
        ts = self.last_sell_time.get(market)
        return bool(ts and (time.time() - ts) < 10)
    def record_buy_window(self, market, w):
        self.last_buy_window[market] = w
    def bought_this_window(self, market, w):
        return self.last_buy_window.get(market) == w
    def mark_intersection_buy(self, market):
        self.intersection_last_buy_time[market] = time.time()
    def recently_bought_intersection(self, market, cooldown):
        ts = self.intersection_last_buy_time.get(market)
        return bool(ts and (time.time() - ts) < cooldown)
    def record_buy(self, market, krw_amount: Decimal):
        info = self.buy_info.get(market)
        if not info:
            self.buy_info[market] = {"total_buys": 1, "total_invested": krw_amount}
        else:
            info["total_buys"] += 1
            info["total_invested"] += krw_amount
        if market in self.buy_info and "first_buy_amount" not in self.data.get(market, {}):
            st = self.data.setdefault(market, {})
            if st.get("first_buy_amount") is None:
                st["first_buy_amount"] = krw_amount
    def get_buy_stats(self, market):
        info = self.buy_info.get(market)
        if not info:
            return 0, Decimal("0")
        return info["total_buys"], info["total_invested"]
    def can_additional_buy(self, market, next_amount: Decimal, max_additional_buys: int, max_total_invest: Decimal):
        st = self.data.get(market)
        if STOP_DISABLE_NEW_BUYS and st and st.get("stop_triggered"):
            return False, f"[SKIP] {market} 손절모드(추가매수금지)"
        total_buys, total_inv = self.get_buy_stats(market)
        if total_buys == 0:
            if max_total_invest > 0 and next_amount >= max_total_invest:
                return False, f"[SKIP] {market} 초기매수 {next_amount} >= 한도 {max_total_invest}"
            return True, "INIT_OK"
        add_done = total_buys - 1
        if add_done >= max_additional_buys:
            return False, f"[SKIP] {market} 추가매수 한도초과 (이미 {add_done}회)"
        if max_total_invest > 0 and (total_inv + next_amount) >= max_total_invest:
            return False, f"[SKIP] {market} 누적 {total_inv}+{next_amount} >= {max_total_invest}"
        return True, "OK"
# ============================================================
# 16. 익절/트레일 판단
# ============================================================
def get_state_decimal(state: dict, key: str, default: Decimal):
    v = state.get(key)
    if isinstance(v, Decimal):
        return v
    try:
        if v is not None:
            return Decimal(str(v))
    except:
        pass
    return default
def decide_avg_down_tp(pnl: Decimal, state: dict):
    if not state.get("avg_down_tp_mode") or state.get("avg_down_tp_completed"):
        return False, "", None
    try:
        baseline = Decimal(str(state.get("avg_down_tp_baseline_pnl")))
    except:
        baseline = pnl
    offset = pnl - baseline
    prev_peak = state.get("avg_down_tp_peak_offset", Decimal("0"))
    if offset > prev_peak:
        state["avg_down_tp_peak_offset"] = offset
    armed = state.get("avg_down_tp_armed", False)
    if offset >= AVG_DOWN_TP_HARD_PNL:
        return True, f"AD_TP_HARD offset={offset}% >= {AVG_DOWN_TP_HARD_PNL}%", "AD_TP_HARD"
    if (not armed) and offset >= AVG_DOWN_TP_ARM_PNL:
        state["avg_down_tp_armed"] = True
        return False, f"AD_TP_ARM offset={offset}% >= {AVG_DOWN_TP_ARM_PNL}%", None
    if armed and prev_peak >= AVG_DOWN_TP_TRAIL_START:
        drop = prev_peak - offset
        if drop >= AVG_DOWN_TP_TRAIL_DROP:
            return True, f"AD_TP_TRAIL drop={drop}% >= {AVG_DOWN_TP_TRAIL_DROP}%(peakOff={prev_peak} nowOff={offset})", "AD_TP_TRAIL"
    return False, "", None
def decide_sell(market, pnl: Decimal, state: dict):
    if FULL_LIMIT_SELL_ENABLED:
        return False, "", None
    if state.get("limit_pending_category") == "PRE_HARD_TP1":
        return False, "", None
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)
    hard_tp2_target = get_state_decimal(state, "dynamic_hard_tp2", HARD_TP2_BASE)
    if hard_tp_taken and (not hard_tp2_taken) and pnl >= hard_tp2_target:
        return True, f"HARD_TP2 {pnl}% >= {hard_tp2_target}%", "HARD_TP2"
    if (not hard_tp_taken) and pnl >= HARD_TP:
        return True, f"HARD_TP1 {pnl}% >= {HARD_TP}%", "HARD_TP1"
    if (not armed) and pnl >= ARM_PNL:
        state["armed"] = True
        return False, f"ARMED {pnl}% (>= {ARM_PNL}%)", None
    trail_drop_used = get_state_decimal(state, "trail_drop_dynamic", TRAIL_DROP)
    if state["armed"] and peak >= TRAIL_START_PNL:
        drop = peak - pnl
        if drop >= trail_drop_used:
            return True, f"TRAIL_DROP {drop}% >= {trail_drop_used}% (peak={peak}% now={pnl}%)", "TRAIL"
    return False, "", None
# ============================================================
# 17. 공용 유틸
# ============================================================
async def is_persistently_falling(market: str,
                                  lookback: int = FALLING_LOOKBACK,
                                  min_drops: int = FALLING_MIN_DROPS,
                                  min_total_drop_pct: Decimal = FALLING_MIN_TOTAL_DROP_PCT) -> bool:
    try:
        candles = await fetch_minute_candles(market, unit=1, count=lookback + 1)
        if len(candles) < lookback + 1:
            return False
        closes = [Decimal(str(c["trade_price"])) for c in candles]
        drops = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])
        total_drop_pct = ((closes[-1] - closes[0]) / closes[0]) * Decimal("100")
        return (drops >= min_drops) and (total_drop_pct <= min_total_drop_pct)
    except Exception:
        return False


def safe_calc_volume(balance: Decimal, portion: Decimal):
    portion = min(portion, Decimal("1"))
    vol = balance if portion >= 1 else balance * portion
    vol = vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    return vol if vol > 0 else Decimal("0")
async def align_to_half_minute():
    now = time.time()
    rem = now % INTERVAL_SECONDS
    if rem > 0.01:
        await asyncio.sleep(INTERVAL_SECONDS - rem)
def is_five_minute_boundary(ts: float):
    window_start = ts - (ts % FIVE_MIN_SECONDS)
    lt = time.localtime(ts)
    return (lt.tm_min % 5 == 0 and lt.tm_sec == 0), window_start
async def sleep_until_next_boundary():
    now = time.time()
    nxt = math.floor(now / INTERVAL_SECONDS) * INTERVAL_SECONDS + INTERVAL_SECONDS
    await asyncio.sleep(max(0, nxt - now))
async def dynamic_sleep():
    if 'USE_TICK_RATE' in globals() and USE_TICK_RATE and TICK_RATE > 0:
        await asyncio.sleep(float(TICK_RATE))
    else:
        await sleep_until_next_boundary()
# ============================================================
# 18. 활성 포지션 판별
# ============================================================
def get_active_markets(enriched: List[dict]) -> List[str]:
    active = []
    for it in enriched:
        m = it.get("market")
        bal = it.get("balance")
        price = it.get("current_price")
        if not m or bal is None or price is None:
            continue
        try:
            notional = bal * price
            if notional >= MIN_NOTIONAL_KRW:
                active.append(m)
        except:
            pass
    return active
# ============================================================
# 19. 원격 설정 fetch
# ============================================================
async def fetch_mtpond_setup(user_no: int) -> dict | None:
    if not API_BASE:
        print("[WARN] API_BASE 미설정")
        return None
    url = f"{API_BASE}/api/mtpondsetup/{user_no}"
    try:
        to = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=to) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            print(f"[WARN] mtpondsetup status={resp.status_code} body={resp.text[:200]}")
            return None
        try:
            data = resp.json()
        except Exception as je:
            print(f"[WARN] JSON 파싱 실패: {je}")
            return None
        if isinstance(data, list):
            if not data or not isinstance(data[0], dict):
                return None
            return data[0]
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        print(f"[WARN] mtpondsetup 네트워크 오류: {e}")
        return None

# ============================================================
# 20. 주문 함수
# ============================================================
async def order_market_sell(access_key, secret_key, market, volume: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "ord_type": "market"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_market_buy_price(access_key, secret_key, market, krw_amount: Decimal):
    params = {"market": market, "side": "bid", "price": str(krw_amount), "ord_type": "price"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_limit_sell(access_key: str, secret_key: str, market: str, volume: Decimal, price: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "price": str(price), "ord_type": "limit"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_limit_buy(access_key: str, secret_key: str, market: str, volume: Decimal, price: Decimal):
    params = {"market": market, "side": "bid", "volume": str(volume), "price": str(price), "ord_type": "limit"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def get_order(access_key, secret_key, uuid_):
    params = {"uuid": uuid_}
    return await upbit_request("GET", "https://api.upbit.com/v1/order", access_key, secret_key, params=params, max_retry=3)

async def cancel_order(access_key, secret_key, uuid_: str):
    params = {"uuid": uuid_}
    try:
        resp = await upbit_request("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, params=params, max_retry=2)
        print(f"[CANCEL] 성공 uuid={uuid_}")
        return resp
    except Exception as e:
        print(f"[CANCEL] 실패 uuid={uuid_} err={e}")

# ---------------- Upbit 오픈주문 조회/취소/언락 대기 ----------------

async def fetch_open_orders_market(access_key: str, secret_key: str, market: str,
                                   state: str = "wait", page: int = 1, limit: int = 100) -> List[Dict[str, Any]]:
    params = {
        "market": market,
        "state": state,
        "page": page,
        "limit": limit,
        "order_by": "desc",
    }
    try:
        rows = await upbit_request("GET", "https://api.upbit.com/v1/orders",
                                   access_key, secret_key, params=params)
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"[ORDERS][WARN] fetch_open_orders_market fail {market}: {e}")
        return []

async def cancel_all_open_orders_for_market(access_key: str, secret_key: str, market: str,
                                            max_rounds: int = 3, sleep_between: float = 0.3) -> bool:
    for _ in range(max_rounds):
        orders = await fetch_open_orders_market(access_key, secret_key, market, state="wait")
        if not orders:
            return True
        any_cancel = False
        for od in orders:
            uid = od.get("uuid")
            if not uid:
                continue
            try:
                await cancel_order(access_key, secret_key, uid)
                any_cancel = True
            except Exception as e:
                print(f"[CANCEL][WARN] {market} uuid={uid} err={e}")
        if not any_cancel:
            break
        await asyncio.sleep(sleep_between)
    remain = await fetch_open_orders_market(access_key, secret_key, market, state="wait")
    return len(remain) == 0

async def wait_unlock_and_get_balance(access_key: str, secret_key: str, market: str,
                                      timeout: float = 2.0, poll: float = 0.25) -> tuple[Decimal, Decimal]:
    base = market.split("-")[1]
    t0 = time.time()
    last_bal, last_locked = Decimal("0"), Decimal("0")
    while time.time() - t0 < timeout:
        acc = await refetch_single_account(access_key, secret_key, base)
        if acc:
            try:
                bal = Decimal(str(acc.get("balance", "0")))
                locked = Decimal(str(acc.get("locked", "0")))
                last_bal, last_locked = bal, locked
                if locked <= 0:
                    return bal, locked
            except:
                pass
        await asyncio.sleep(poll)
    return last_bal, last_locked

def detect_state_tags_for_market(market: str,
                                 ps: "PositionState",
                                 raw_accounts: List[Dict[str, Any]]) -> set[str]:
    tags: set[str] = set()
    st = ps.data.get(market) or {}
    strategy_managed = bool(st.get("avg_buy_price"))
    base = market.split("-")[1]
    acc = next((a for a in raw_accounts
                if a.get("currency") == base and a.get("unit_currency") == BASE_UNIT), None)
    try:
        locked = Decimal(str(acc.get("locked", "0"))) if acc else Decimal("0")
        bal = Decimal(str(acc.get("balance", "0"))) if acc else Decimal("0")
    except:
        locked, bal = Decimal("0"), Decimal("0")
    if locked > 0:
        tags.add("HOLD-LOCKED")
    if not strategy_managed:
        tags.add("PASSIVE")
    return tags

# ============================================================
# 21. 지정가 TP 주문 생성
# ============================================================
async def refetch_single_account(access_key: str, secret_key: str, currency: str) -> dict | None:
    try:
        accounts = await fetch_upbit_accounts(access_key, secret_key)
        for a in accounts:
            if a.get("currency") == currency and a.get("unit_currency") == BASE_UNIT:
                return a
    except Exception as e:
        print(f"[RECHECK] account refetch fail {currency}: {e}")
    return None

async def place_limit_tp_order(access_key: str, secret_key: str, market: str, volume: Decimal, category: str, state: dict):
    if FULL_LIMIT_SELL_ENABLED:
        return
    try:
        best_bid, best_ask = await get_orderbook_top(market)
    except Exception as e:
        print(f"[TP-LIMIT] 호가조회 실패 → 시장가 대체 market={market} e={e}")
        return await order_market_sell(access_key, secret_key, market, volume)
    limit_price = adjust_price_to_tick(best_bid)
    if limit_price <= 0:
        print(f"[TP-LIMIT] limit_price 비정상 → 시장가 대체 market={market}")
        return await order_market_sell(access_key, secret_key, market, volume)
    try:
        resp = await order_limit_sell(access_key, secret_key, market, volume, Decimal(str(limit_price)))
        uid = resp.get("uuid")
        if uid:
            state["active_limit_uuid"] = uid
            state["limit_submit_ts"] = time.time()
            state["limit_pending_category"] = category
            state["limit_pending_volume"] = str(volume)
            print(f"[TP-LIMIT] 지정가 제출 {market} cat={category} vol={volume} price={limit_price} uuid={uid}")
        else:
            print(f"[TP-LIMIT] uuid 없음 → 시장가 대체 market={market} resp={resp}")
            return await order_market_sell(access_key, secret_key, market, volume)
        return resp
    except Exception as e:
        print(f"[TP-LIMIT] 지정가 실패 → 시장가 대체 market={market} e={e}")
        return await order_market_sell(access_key, secret_key, market, volume)

# ============================================================
# 21b. 전체 수량 단일 지정가 매도 유지 로직
# ============================================================
async def manage_full_limit_sells(access_key: str, secret_key: str,
                                  raw_accounts: List[Dict[str, Any]],
                                  price_map: Dict[str, Decimal],
                                  ps: "PositionState"):
    now = time.time()
    for acc in raw_accounts:
        currency = acc.get("currency")
        unit_cur = acc.get("unit_currency")
        if not currency or unit_cur != BASE_UNIT or currency == BASE_UNIT:
            continue
        try:
            bal = Decimal(str(acc.get("balance","0")))
            locked = Decimal(str(acc.get("locked","0")))
            avg_raw = acc.get("avg_buy_price")
            avg_price = Decimal(str(avg_raw)) if avg_raw not in (None,"","0") else Decimal("0")
        except:
            continue

        total_qty = bal + locked
        if total_qty <= 0:
            continue

        market = f"{BASE_UNIT}-{currency}"
        cur_price = price_map.get(market)
        if cur_price is None:
            continue
        notional = total_qty * cur_price
        if notional < FULL_LIMIT_SELL_MIN_NOTIONAL:
            continue

        if FULL_LIMIT_SELL_MODE == "percent":
            base_p = avg_price if (FULL_LIMIT_SELL_PRICE_BASIS == "avg" and avg_price > 0) else cur_price
            target_price = base_p * (Decimal("1") + FULL_LIMIT_SELL_PERCENT / Decimal("100"))
        elif FULL_LIMIT_SELL_MODE == "fixed":
            if FULL_LIMIT_SELL_FIXED_PRICE <= 0:
                continue
            target_price = FULL_LIMIT_SELL_FIXED_PRICE
        else:
            continue

        adj_price = adjust_price_to_tick(target_price, side="ask")
        st = ps.data.setdefault(market, {})

        fl_uuid = st.get("full_limit_uuid")
        fl_ts = st.get("full_limit_ts")
        need_place = False
        need_cancel = False
        reason = ""

        if fl_uuid:
            try:
                od = await get_order(access_key, secret_key, fl_uuid)
                state_val = od.get("state")
                price_str = od.get("price")
                vol_str = od.get("volume")
                rem_str = od.get("remaining_volume")
                exec_str = od.get("executed_volume")
                try:
                    od_price = Decimal(str(price_str)) if price_str is not None else None
                    od_total_vol = Decimal(str(vol_str)) if vol_str is not None else None
                    od_rem = Decimal(str(rem_str)) if rem_str is not None else Decimal("0")
                    od_exec = Decimal(str(exec_str)) if exec_str is not None else Decimal("0")
                except:
                    od_price = None; od_total_vol = None; od_rem = Decimal("0"); od_exec = Decimal("0")

                if state_val != "wait":
                    for k in ("full_limit_uuid","full_limit_price","full_limit_volume","full_limit_ts"):
                        st.pop(k, None)
                    need_place = True
                    reason = f"prev_state={state_val}"
                else:
                    if FULL_LIMIT_SELL_FORCE_REPLACE_ALWAYS and (fl_ts is None or (now - fl_ts) >= FORCE_REPLACE_MIN_INTERVAL_SEC):
                        need_cancel = True
                        reason = "force_replace_always"
                    elif od_exec > 0 and od_rem > 0:
                        need_cancel = True
                        reason = "partial_fill"
                    else:
                        if od_total_vol is None:
                            need_cancel = True; reason = "order_volume_none"
                        else:
                            diff_amt = (od_total_vol - total_qty).copy_abs()
                            if diff_amt > FULL_LIMIT_SELL_AMOUNT_TOL:
                                need_cancel = True; reason = f"qty_mismatch orig={od_total_vol} now={total_qty}"
                            else:
                                if od_price is None:
                                    need_cancel = True; reason = "no_price_in_order"
                                else:
                                    price_diff_pct = ((adj_price - od_price) / od_price * Decimal("100")).copy_abs()
                                    reprice_time_ok = (fl_ts is None) or ((now - fl_ts) >= FULL_LIMIT_SELL_REPRICE_INTERVAL_SEC)
                                    if price_diff_pct > FULL_LIMIT_SELL_REPRICE_DIFF_TOL_PCT and reprice_time_ok:
                                        need_cancel = True; reason = f"price_diff {price_diff_pct:.5f}%>tol {FULL_LIMIT_SELL_REPRICE_DIFF_TOL_PCT}%"
            except Exception as e:
                print(f"[FLS][WARN] get_order 실패 {market} err={e} → 재배치")
                for k in ("full_limit_uuid","full_limit_price","full_limit_volume","full_limit_ts"):
                    st.pop(k, None)
                need_place = True
                reason = "fetch_fail"
        else:
            need_place = True
            reason = "no_existing_order"

        if need_cancel and st.get("full_limit_uuid"):
            old_uuid = st["full_limit_uuid"]
            try:
                await cancel_order(access_key, secret_key, old_uuid)
            except Exception as ce:
                print(f"[FLS] 취소 실패 {market} err={ce}")
            finally:
                for k in ("full_limit_uuid","full_limit_price","full_limit_volume","full_limit_ts"):
                    st.pop(k, None)
                await asyncio.sleep(0.25)
                ref = await refetch_single_account(access_key, secret_key, market.split("-")[1])
                if ref:
                    try:
                        bal = Decimal(str(ref.get("balance", "0")))
                        locked = Decimal(str(ref.get("locked", "0")))
                    except:
                        bal = Decimal("0"); locked = Decimal("0")
                need_place = True

        if need_place:
            usable_qty = quantize_volume(bal)
            if usable_qty <= 0:
                continue
            notional_check = usable_qty * adj_price
            if notional_check < FULL_LIMIT_SELL_MIN_NOTIONAL:
                continue
            try:
                resp = await order_limit_sell(access_key, secret_key, market, usable_qty, adj_price)
                uuid_new = resp.get("uuid")
                if uuid_new:
                    st["full_limit_uuid"] = uuid_new
                    st["full_limit_price"] = adj_price
                    st["full_limit_volume"] = str(usable_qty)
                    st["full_limit_ts"] = time.time()
                    print(f"[FLS][PLACE] {market} vol={usable_qty} price={adj_price} reason={reason} uuid={uuid_new}")
                else:
                    print(f"[FLS][FAIL] uuid 없음 {market} resp={resp}")
            except Exception as pe:
                print(f"[FLS][ERR] 주문 실패 {market} err={pe}")

# ------------------------------------------------------------
# 21c. PASSIVE (Idle) 전체 지정가 유지 로직
# ------------------------------------------------------------
async def manage_passive_limit_sells(access_key: str, secret_key: str,
                                     raw_accounts: List[Dict[str, Any]],
                                     price_map: Dict[str, Decimal],
                                     ps: "PositionState"):
    if FULL_LIMIT_SELL_ENABLED:
        return
    now = time.time()
    for acc in raw_accounts:
        currency = acc.get("currency")
        unit_cur = acc.get("unit_currency")
        if not currency or unit_cur != BASE_UNIT or currency == BASE_UNIT:
            continue
        try:
            bal = Decimal(str(acc.get("balance","0")))
            locked = Decimal(str(acc.get("locked","0")))
            avg_raw = acc.get("avg_buy_price")
            avg_price = Decimal(str(avg_raw)) if avg_raw not in (None,"","0") else Decimal("0")
        except:
            continue

        total_qty = bal + locked
        if (bal <= 0) and (locked <= 0):
            continue

        market = f"{BASE_UNIT}-{currency}"
        cur_price = price_map.get(market)
        if cur_price is None:
            continue
        notional = total_qty * cur_price
        if notional < PASSIVE_LIMIT_SELL_MIN_NOTIONAL:
            if PASSIVE_LIMIT_SELL_DEBUG:
                print(f"[PASSIVE][SKIP] {market} notional<{PASSIVE_LIMIT_SELL_MIN_NOTIONAL}")
            continue

        st = ps.data.setdefault(market, {})
        if st.get("avg_buy_price"):
            if st.get("passive_limit_uuid"):
                try:
                    await cancel_order(access_key, secret_key, st["passive_limit_uuid"])
                except Exception as ce:
                    print(f"[PASSIVE][WARN] cancel fail {market} takeover err={ce}")
                finally:
                    for k in ("passive_limit_uuid","passive_limit_price","passive_limit_volume","passive_limit_ts"):
                        st.pop(k, None)
            continue

        if PASSIVE_LIMIT_SELL_MODE == "percent":
            base_p = avg_price if (PASSIVE_LIMIT_SELL_PRICE_BASIS == "avg" and avg_price > 0) else cur_price
            target_price = base_p * (Decimal("1") + PASSIVE_LIMIT_SELL_PERCENT / Decimal("100"))
        elif PASSIVE_LIMIT_SELL_MODE == "fixed":
            if PASSIVE_LIMIT_SELL_FIXED_PRICE <= 0:
                continue
            target_price = PASSIVE_LIMIT_SELL_FIXED_PRICE
        else:
            continue

        adj_price = adjust_price_to_tick(target_price)
        order_qty = quantize_volume(bal)
        if order_qty <= 0:
            if PASSIVE_LIMIT_SELL_DEBUG and locked > 0:
                print(f"[PASSIVE][HOLD-LOCKED] {market} balance=0 locked={locked}")
            continue

        dust_flag, dust_reason = is_dust_volume(order_qty, adj_price,
                                                min_notional=PASSIVE_LIMIT_SELL_MIN_NOTIONAL,
                                                buffer_pct=ORDER_NOTIONAL_BUFFER_PCT)
        if dust_flag:
            if PASSIVE_LIMIT_SELL_DEBUG:
                print(f"[PASSIVE][DUST] {market} reason={dust_reason} qty={order_qty} price={adj_price}")
            continue

        pl_uuid = st.get("passive_limit_uuid")
        pl_ts = st.get("passive_limit_ts")
        need_place = False
        need_cancel = False
        reason = ""

        if pl_uuid:
            try:
                od = await get_order(access_key, secret_key, pl_uuid)
                state_val = od.get("state")
                price_str = od.get("price")
                vol_str = od.get("volume")
                rem_str = od.get("remaining_volume")
                exec_str = od.get("executed_volume")
                try:
                    od_price = Decimal(str(price_str)) if price_str is not None else None
                    od_total_vol = Decimal(str(vol_str)) if vol_str is not None else None
                    od_rem = Decimal(str(rem_str)) if rem_str is not None else Decimal("0")
                    od_exec = Decimal(str(exec_str)) if exec_str is not None else Decimal("0")
                except:
                    od_price = None; od_total_vol = None; od_rem = Decimal("0"); od_exec = Decimal("0")

                if state_val != "wait":
                    for k in ("passive_limit_uuid","passive_limit_price","passive_limit_volume","passive_limit_ts"):
                        st.pop(k, None)
                    need_place = True
                    reason = f"prev_state={state_val}"
                else:
                    if PASSIVE_FORCE_REPLACE_ALWAYS and (pl_ts is None or (now - pl_ts) >= FORCE_REPLACE_MIN_INTERVAL_SEC):
                        need_cancel = True; reason = "force_replace_always"
                    elif od_exec > 0 and od_rem > 0:
                        need_cancel = True; reason = "partial_fill"
                    else:
                        if od_total_vol is None:
                            need_cancel = True; reason = "order_volume_none"
                        else:
                            diff_amt = (od_total_vol - total_qty).copy_abs()
                            if PASSIVE_FORCE_REPLACE_ON_INCREASE:
                                extra_added = total_qty - od_total_vol
                                if extra_added > PASSIVE_FORCE_INCREASE_TOL:
                                    need_cancel = True; reason = f"qty_increase +{extra_added}"
                            if (not need_cancel) and diff_amt > PASSIVE_LIMIT_SELL_AMOUNT_TOL:
                                need_cancel = True; reason = f"qty_mismatch orig={od_total_vol} now={total_qty}"
                            if (not need_cancel):
                                if od_price is None:
                                    need_cancel = True; reason = "no_price_in_order"
                                else:
                                    price_diff_pct = ((adj_price - od_price) / od_price * Decimal("100")).copy_abs()
                                    reprice_time_ok = (pl_ts is None) or ((now - pl_ts) >= PASSIVE_LIMIT_SELL_REPRICE_INTERVAL_SEC)
                                    if price_diff_pct > PASSIVE_LIMIT_SELL_REPRICE_DIFF_TOL_PCT and reprice_time_ok:
                                        need_cancel = True; reason = f"price_diff {price_diff_pct:.5f}%>tol {PASSIVE_LIMIT_SELL_REPRICE_DIFF_TOL_PCT}%"
            except Exception as e:
                print(f"[PASSIVE][WARN] get_order 실패 {market} err={e} → 재배치")
                for k in ("passive_limit_uuid","passive_limit_price","passive_limit_volume","passive_limit_ts"):
                    st.pop(k, None)
                need_place = True
                reason = "fetch_fail"
        else:
            need_place = True
            reason = "no_existing_order"

        if need_cancel and st.get("passive_limit_uuid"):
            try:
                await cancel_order(access_key, secret_key, st["passive_limit_uuid"])
            except Exception as ce:
                print(f"[PASSIVE] 취소 실패 {market} err={ce}")
            finally:
                for k in ("passive_limit_uuid","passive_limit_price","passive_limit_volume","passive_limit_ts"):
                    st.pop(k, None)
                await asyncio.sleep(0.25)
                ref = await refetch_single_account(access_key, secret_key, market.split("-")[1])
                if ref:
                    try:
                        bal = Decimal(str(ref.get("balance", "0")))
                        locked = Decimal(str(ref.get("locked", "0")))
                    except:
                        bal = Decimal("0"); locked = Decimal("0")
                if locked > 0:
                    if PASSIVE_LIMIT_SELL_DEBUG:
                        print(f"[PASSIVE][WAIT_UNLOCK] {market} locked={locked} → next loop")
                    continue
                order_qty = quantize_volume(bal)
                if order_qty <= 0:
                    continue
                need_place = True

        if need_place:
            order_qty = quantize_volume(bal)
            if order_qty <= 0:
                continue
            notional_check = order_qty * adj_price
            if notional_check < PASSIVE_LIMIT_SELL_MIN_NOTIONAL:
                if PASSIVE_LIMIT_SELL_DEBUG:
                    print(f"[PASSIVE][SKIP] {market} notional {notional_check} < min {PASSIVE_LIMIT_SELL_MIN_NOTIONAL}")
                continue
            try:
                resp = await order_limit_sell(access_key, secret_key, market, order_qty, adj_price)
                uuid_new = resp.get("uuid")
                if uuid_new:
                    st["passive_limit_uuid"] = uuid_new
                    st["passive_limit_price"] = adj_price
                    st["passive_limit_volume"] = str(order_qty)
                    st["passive_limit_ts"] = time.time()
                    print(f"[PASSIVE][PLACE] {market} vol={order_qty} price={adj_price} reason={reason} uuid={uuid_new}")
                else:
                    print(f"[PASSIVE][FAIL] uuid 없음 {market} resp={resp}")
            except Exception as pe:
                print(f"[PASSIVE][ERR] 주문 실패 {market} err={pe}")

# ============================================================
# 22. 잔고/가격 보조
# ============================================================
def build_market_list_from_accounts(accounts, base_unit="KRW") -> List[str]:
    res = []
    for acc in accounts:
        c = acc.get("currency")
        u = acc.get("unit_currency")
        if not c or not u or u != base_unit or c == base_unit:
            continue
        try:
            bal = Decimal(str(acc.get("balance", "0")))
            locked = Decimal(str(acc.get("locked", "0")))
        except:
            bal = Decimal("0"); locked = Decimal("0")
        if bal == 0 and locked == 0:
            continue
        m = f"{base_unit}-{c}"
        if ENFORCE_WHITELIST and WHITELIST_MARKETS and m not in WHITELIST_MARKETS:
            continue
        res.append(m)
    out, seen = [], set()
    for m in res:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out

def get_available_krw(raw_accounts: List[Dict[str, Any]]) -> Decimal:
    for acc in raw_accounts:
        if acc.get("currency") == "KRW":
            try:
                bal = Decimal(str(acc.get("balance", "0")))
                locked = Decimal(str(acc.get("locked", "0")))
                return bal - locked
            except:
                return Decimal("0")
    return Decimal("0")

def enrich_accounts_with_prices(accounts: List[dict], price_map: Dict[str, Decimal], base_unit="KRW"):
    out = []
    for acc in accounts:
        c = acc.get("currency")
        u = acc.get("unit_currency")
        avg_raw = acc.get("avg_buy_price")
        try:
            avg = Decimal(str(avg_raw)) if avg_raw not in (None, "", "0") else Decimal("0")
        except:
            avg = Decimal("0")
        market = None
        cur = None
        pnl = None
        ratio = None
        if c and u == base_unit and c != base_unit:
            market = f"{base_unit}-{c}"
            cur = price_map.get(market)
            if cur and avg > 0:
                try:
                    pnl = ((cur - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                    ratio = (cur / avg).quantize(Decimal("0.0001"))
                except:
                    pass
        def to_d(v):
            try:
                return Decimal(str(v))
            except:
                return Decimal("0")
        out.append({
            "currency": c,
            "market": market,
            "unit_currency": u,
            "balance": to_d(acc.get("balance")),
            "locked": to_d(acc.get("locked")),
            "avg_buy_price": avg if avg > 0 else None,
            "current_price": cur,
            "pnl_percent": pnl,
            "ratio_cur_over_avg": ratio
        })
    return out

# ============================================================
# 23. 보조: 시장가 매수 후 선지정 TP
# ============================================================
async def after_market_buy_place_pre_tp(access_key, secret_key, market: str, ps: "PositionState", sleep_sec=0.4):
    if FULL_LIMIT_SELL_ENABLED:
        return
    if not PREPLACE_HARD_TP:
        return
    await asyncio.sleep(sleep_sec)
    try:
        accounts = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[PRE_HARD_TP][AFTER_BUY] 계정조회 실패 {market}: {e}")
        return
    base_cur = market.split("-")[1]
    for acc in accounts:
        if acc.get("currency") == base_cur:
            try:
                bal = Decimal(str(acc.get("balance","0"))) + Decimal(str(acc.get("locked","0")))
                avg = Decimal(str(acc.get("avg_buy_price","0")))
            except:
                continue
            st = ps.data.setdefault(market, {})
            await place_preplaced_hard_tp(access_key, secret_key, market, st, bal, avg)
            break

# ============================================================
# 23b. 교차(추천) 매수 실행 함수
# ============================================================
async def process_intersection_buys(access_key: str,
                                    secret_key: str,
                                    ps: "PositionState",
                                    active_set: set,
                                    available_krw: Decimal) -> Decimal:
    if not INTERSECTION_BUY_ENABLED:
        if DEBUG_INTX:
            print("[INTX][SKIP] INTERSECTION_BUY_ENABLED=False")
        return Decimal("0")

    if DEBUG_INTX:
        print(f"[INTX][STATE] active={len(active_set)}/{MAX_ACTIVE_MARKETS} "
              f"availKRW={available_krw} buyKRW={INTERSECTION_BUY_KRW} "
              f"minScore={INTERSECTION_MIN_SCORE} cooldown={INTERSECTION_BUY_COOLDOWN_SEC}s")

    if len(active_set) >= MAX_ACTIVE_MARKETS:
        if DEBUG_INTX:
            print("[INTX][SKIP] MAX_ACTIVE_MARKETS 도달")
        return Decimal("0")

    try:
        cands, meta = await get_intersection_candidates_safe()
    except Exception as e:
        print(f"[INTX][ERR] 후보 조회 실패: {e}")
        return Decimal("0")

    if not cands:
        print(f"[INTX] 후보 0개 source={meta.get('source')} empty_streak={meta.get('empty_streak')}")
        return Decimal("0")

    print(f"[INTX] raw 후보 {len(cands)}개 (minScore={INTERSECTION_MIN_SCORE}) source={meta.get('source')}")
    for c in cands[:10]:
        print(f"   - {c['market']} score={c['avg_score']}")

    ordered = sorted(cands, key=lambda x: x.get("avg_score", Decimal("0")), reverse=True)

    buys_done = 0
    krw_used_total = Decimal("0")

    for item in ordered:
        if buys_done >= INTERSECTION_MAX_BUY_PER_CYCLE:
            break
        market = item.get("market")
        if not market:
            continue
        raw_score = item.get("avg_score")
        try:
            score = Decimal(str(raw_score))
        except:
            if DEBUG_INTX:
                print(f"[INTX][SKIP] {market} score 변환 실패 raw={raw_score}")
            continue

        skip_reasons = []

        if score < INTERSECTION_MIN_SCORE:
            skip_reasons.append(f"score<{INTERSECTION_MIN_SCORE}")
        if market in EXCLUDED_MARKETS:
            skip_reasons.append("excluded_market")
        in_active = market in active_set
        if in_active and not ALLOW_ADDITIONAL_BUY_WHEN_FULL:
            skip_reasons.append("already_active")

        total_buys, total_inv = ps.get_buy_stats(market)
        if total_buys > 0 and ALLOW_ADDITIONAL_BUY_WHEN_FULL:
            ok, msg = ps.can_additional_buy(market,
                                            INTERSECTION_BUY_KRW,
                                            MAX_ADDITIONAL_BUYS,
                                            MAX_TOTAL_INVEST_PER_MARKET)
            if not ok:
                skip_reasons.append(msg)

        if ps.recently_bought_intersection(market, INTERSECTION_BUY_COOLDOWN_SEC):
            skip_reasons.append("cooldown")

        if available_krw < INTERSECTION_BUY_KRW:
            skip_reasons.append("insufficient_krw")

        if skip_reasons:
            if DEBUG_INTX:
                print(f"[INTX][CAND][SKIP] {market} score={score} -> {','.join(skip_reasons)}")
            continue

        if len(active_set) >= MAX_ACTIVE_MARKETS:
            if DEBUG_INTX:
                print("[INTX][STOP] 한도 도달 재확인")
            break

        krw_to_use = INTERSECTION_BUY_KRW
        if not LIVE_TRADING:
            print(f"[DRY_BUY][INTX] {market} score={score} krw={krw_to_use}")
        else:
            try:
                resp = await order_market_buy_price(access_key, secret_key, market, krw_to_use)
                uid = resp.get("uuid")
                print(f"[ORDER][INTX-BUY] {market} score={score} krw={krw_to_use} uuid={uid}")
            except Exception as e:
                print(f"[INTX][ERR] 매수 실패 {market}: {e}")
                continue

        ps.record_buy(market, krw_to_use)
        ps.mark_intersection_buy(market)
        st = ps.data.setdefault(market, {})
        st.setdefault("entry_source", "intersection")

        if PREPLACE_HARD_TP and LIVE_TRADING:
            asyncio.create_task(after_market_buy_place_pre_tp(access_key, secret_key, market, ps))

        available_krw -= krw_to_use
        krw_used_total += krw_to_use
        buys_done += 1

    if buys_done > 0:
        print(f"[INTX] 이번 사이클 매수 {buys_done}건 사용KRW={krw_used_total}")
    else:
        if DEBUG_INTX:
            print("[INTX] 이번 사이클 체결 없음")
    return krw_used_total

# ============================================================
# 24. 메인 모니터 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()  # 주기 리셋 기준 시각
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("[ERR] API 키 없음 → 종료")
        return
    access_key, secret_key = keys

    # PositionState에 stop/martin 필드 확장
    class _PS(PositionState):
        def ensure_stop_fields(self, market: str):
            st = self.data.setdefault(market, {})
            st.setdefault("stop_mode_active", False)
            st.setdefault("martingale_count", 0)
            st.setdefault("last_martin_ts", None)
            return st

    ps = _PS()

    print("=== 모니터 시작 ===")
    if EXCLUDED_MARKETS:
        print(f"[INFO] 제외 목록: {sorted(EXCLUDED_MARKETS)}")
    if FULL_LIMIT_SELL_ENABLED:
        print("[INFO] FULL_LIMIT_SELL_ENABLED=1 → 기존 TP/Trail/Stop 로직 비활성, 단일 전체 지정가 유지 모드")
        print(f"[INFO] MODE={FULL_LIMIT_SELL_MODE} BASIS={FULL_LIMIT_SELL_PRICE_BASIS} PCT={FULL_LIMIT_SELL_PERCENT} FIXED={FULL_LIMIT_SELL_FIXED_PRICE}")
    else:
        if ENABLE_NEW_STOP:
            print(f"[INFO] NEW_STOP 활성화 trigger={STOP_TRIGGER_PNL}% first_portion={STOP_LOSS_FIRST_PORTION*100:.1f}% minRemain={STOP_LOSS_MIN_REMAIN_KRW}")
        elif ENABLE_STOP_TRAIL:
            print(f"[INFO] (LEGACY STOP) trigger={STOP_TRIGGER_PNL}% simple={STOP_SIMPLE_MODE}")
        print(f"[INFO] MAX_ACTIVE_MARKETS={MAX_ACTIVE_MARKETS} ALLOW_ADDITIONAL_BUY_WHEN_FULL={ALLOW_ADDITIONAL_BUY_WHEN_FULL}")
        print(f"[INFO] PREPLACE_HARD_TP={PREPLACE_HARD_TP} PREPLACE_TP_TIMEOUT_SEC={PREPLACE_TP_TIMEOUT_SEC} PREPLACE_ON_START={PREPLACE_ON_START}")

    # 초기 포지션 스캔
    try:
        init_accounts = await fetch_upbit_accounts(access_key, secret_key)
        tradable = await load_tradable_markets()
        raw_markets = build_market_list_from_accounts(init_accounts, BASE_UNIT)
        tradable_markets = [m for m in raw_markets if m in tradable]
        orphan = [m for m in raw_markets if m not in tradable]
        if orphan:
            print(f"[INIT] 거래지원종료(orphan) 제외: {orphan}")
        init_prices = await safe_fetch_current_prices(tradable_markets)
        init_enriched = []
        for acc in init_accounts:
            c = acc.get("currency")
            u = acc.get("unit_currency")
            if c and u == BASE_UNIT and c != BASE_UNIT:
                market = f"{BASE_UNIT}-{c}"
                price = init_prices.get(market)
                if price is None:
                    continue
                avg_raw = acc.get("avg_buy_price")
                try:
                    avg = Decimal(str(avg_raw)) if avg_raw not in (None, "", "0") else Decimal("0")
                except:
                    avg = Decimal("0")
                try:
                    bal = Decimal(str(acc.get("balance", "0")))
                except:
                    bal = Decimal("0")
                if avg > 0 and bal > 0:
                    pnl = ((price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                else:
                    pnl = None
                init_enriched.append({
                    "market": market,
                    "balance": bal,
                    "avg_buy_price": avg if avg > 0 else None,
                    "current_price": price,
                    "pnl_percent": pnl
                })
        for it in init_enriched:
            mkt = it["market"]
            bal = it["balance"]
            avg = it["avg_buy_price"]
            if not avg or bal <= 0:
                continue
            est = (bal * avg).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": est}
            st = ps.data.setdefault(mkt, {})
            st.setdefault("entry_source", "pre_existing")
            st.setdefault("first_buy_amount", est)
            # 손절/마틴 필드 기본값
            st.setdefault("stop_mode_active", False)
            st.setdefault("martingale_count", 0)
            st.setdefault("last_martin_ts", None)

            for k, v in {
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0,
                "worst_pnl": Decimal("0"), "stop_last_sell_ts": None,
                "stop_cooldown_flag": False, "stop_pending_peak": None, "stop_cooldown_start_ts": None
            }.items():
                st.setdefault(k, v)
            if (not FULL_LIMIT_SELL_ENABLED) and PREPLACE_HARD_TP and PREPLACE_ON_START:
                try:
                    locked = Decimal("0")
                    for a2 in init_accounts:
                        if a2.get("currency") == mkt.split("-")[1]:
                            locked = Decimal(str(a2.get("locked","0")))
                            break
                    total_bal = bal + locked
                except:
                    total_bal = bal
                await place_preplaced_hard_tp(access_key, secret_key, mkt, st, total_bal, avg)
        print(f"[INFO] 초기 추적 {len(ps.buy_info)}개 (orphan={len(orphan)})")
    except Exception as e:
        print(f"[WARN] 초기화 부분 실패 (진행): {e}")

    try:
        await restart_reseed_after_cancellation(access_key, secret_key, ps, init_accounts, init_prices)
        print("[INIT] 재시작 초기화(일괄 취소→재배치) 완료")
    except Exception as e:
        print(f"[INIT][WARN] 재시작 초기화 실패 (다음 루프에서 보정): {e}")

    await align_to_half_minute()
    prev_active_count = None

    while True:
        try:
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print(f"[ERR] 잔고 조회 실패: {e}")
            await dynamic_sleep()
            continue

        tradable = await load_tradable_markets()
        markets_raw = build_market_list_from_accounts(raw_accounts, BASE_UNIT)
        invalid = [m for m in markets_raw if m not in tradable]
        if invalid:
            print(f"[LOOP] 거래불가 제외: {invalid}")
        markets = [m for m in markets_raw if m in tradable]

        try:
            price_map = await safe_fetch_current_prices(markets)
        except Exception as e:
            print(f"[WARN] 가격 조회 실패: {e}")
            price_map = {}

        # FULL LIMIT SELL 전용
        if FULL_LIMIT_SELL_ENABLED:
            try:
                await manage_full_limit_sells(access_key, secret_key, raw_accounts, price_map, ps)
            except Exception as fle:
                print(f"[FLS][ERR] 관리루프 예외: {fle}")
            await dynamic_sleep()
            continue

        if (not FULL_LIMIT_SELL_ENABLED) and PASSIVE_LIMIT_SELL_ENABLED:
            try:
                await manage_passive_limit_sells(access_key, secret_key, raw_accounts, price_map, ps)
            except Exception as ple:
                print(f"[PASSIVE][ERR] 관리루프 예외: {ple}")

        enriched_all = enrich_accounts_with_prices(raw_accounts, price_map, BASE_UNIT)
        enriched = [r for r in enriched_all if r.get("current_price") is not None]
        available_krw = get_available_krw(raw_accounts)

        managed_set = set(ps.buy_info.keys())
        active_by_notional = set(get_active_markets(enriched))
        active_set = managed_set
        active_count = len(active_by_notional)
        if prev_active_count is None or prev_active_count != active_count:
            print(f"[PORTFOLIO] 활성 {active_count}개 (한도 {MAX_ACTIVE_MARKETS})")
            prev_active_count = active_count

        actions = []
        sell_orders = []

        # --------------------- 포지션 루프 ---------------------
        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            avg = it.get("avg_buy_price")
            bal = it.get("balance")
            cur_price = it.get("current_price")
            if not market or pnl is None or avg is None or bal is None or cur_price is None:
                continue
            if pnl < WATCH_MIN_PNL:
                ps.remove(market)
                continue
            if ps.recently_sold(market):
                continue
            if bal * cur_price < MIN_NOTIONAL_KRW:
                continue

            st = ps.update_or_init(market, pnl, avg)
            ps.ensure_stop_fields(market)

            # 대기 중인 지정가 체크
            if st.get("active_limit_uuid"):
                uid = st["active_limit_uuid"]
                cat_pending = st.get("limit_pending_category", "")
                submit_ts = st.get("limit_submit_ts")
                elapsed = (time.time() - submit_ts) if submit_ts else 0
                effective_timeout = LIMIT_SELL_UNFILLED_TIMEOUT_SEC
                if cat_pending == "PRE_HARD_TP1":
                    if PREPLACE_TP_TIMEOUT_SEC <= 0:
                        effective_timeout = None
                    else:
                        effective_timeout = PREPLACE_TP_TIMEOUT_SEC
                try:
                    od = await get_order(access_key, secret_key, uid)
                    od_state = od.get("state")
                    rem_vol_raw = od.get("remaining_volume")
                    exec_vol_raw = od.get("executed_volume")
                    try:
                        rem_vol = Decimal(str(rem_vol_raw)) if rem_vol_raw is not None else Decimal("0")
                        exec_vol = Decimal(str(exec_vol_raw)) if exec_vol_raw is not None else Decimal("0")
                    except:
                        rem_vol = Decimal("0"); exec_vol = Decimal("0")
                    total_req = None
                    try:
                        total_req = Decimal(st.get("limit_pending_volume", "0"))
                    except:
                        pass
                    if total_req and exec_vol > 0 and exec_vol < total_req:
                        st["pre_tp_partial_filled"] = True
                    if od_state == "done" or rem_vol <= 0:
                        cat = cat_pending
                        print(f"[LIMIT-FILL] market={market} cat={cat} uuid={uid} filled={exec_vol}")
                        if cat == "PRE_HARD_TP1":
                            st["hard_tp_taken"] = True
                            portion_used = get_preplace_portion()
                            if portion_used < 1:
                                ps.reduce_invested_after_sell(market, portion_used)
                            else:
                                ps.mark_sold(market)
                        elif cat == "HARD_TP1":
                            st["hard_tp_taken"] = True
                            if HARD_TP_SELL_PORTION < 1:
                                ps.reduce_invested_after_sell(market, HARD_TP_SELL_PORTION)
                            else:
                                ps.mark_sold(market)
                        elif cat == "HARD_TP2":
                            st["hard_tp2_taken"] = True
                            if HARD_TP2_SELL_PORTION < 1:
                                ps.reduce_invested_after_sell(market, HARD_TP2_SELL_PORTION)
                            else:
                                ps.mark_sold(market)
                        elif cat == "TRAIL":
                            ps.mark_sold(market)
                        for k in ("active_limit_uuid", "limit_submit_ts", "limit_pending_category",
                                  "limit_pending_volume"):
                            st.pop(k, None)
                        continue
                    if effective_timeout and elapsed > effective_timeout:
                        try:
                            await cancel_order(access_key, secret_key, uid)
                            print(f"[LIMIT-CANCEL] timeout market={market} cat={cat_pending}")
                        except Exception as ce:
                            print(f"[LIMIT-CANCEL] 실패 market={market}: {ce}")
                        for k in ("active_limit_uuid", "limit_submit_ts", "limit_pending_category",
                                  "limit_pending_volume"):
                            st.pop(k, None)
                    continue
                except Exception as e:
                    print(f"[LIMIT] 조회 실패 market={market} uuid={uid} e={e}")
                    continue

            # 익절/트레일 판단
            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market,"pnl": pnl,"peak": st["peak_pnl"],
                "drawdown": st.get("last_drawdown"),"max_dd": st.get("max_drawdown"),
                "min_pnl": st.get("min_pnl"),"armed": st["armed"],
                "sell": sell,"cat": category,"reason": reason
            })
            if sell:
                if category == "HARD_TP1":
                    portion = HARD_TP_SELL_PORTION
                elif category == "HARD_TP2":
                    portion = HARD_TP2_SELL_PORTION
                else:
                    portion = SELL_PORTION
                volume = safe_calc_volume(bal, portion)
                if volume <= 0:
                    if category in ("TRAIL", "HARD_TP2") and portion >= 1:
                        ps.mark_sold(market)
                    continue
                sell_orders.append({
                    "market": market,"volume": volume,"pnl": pnl,
                    "category": category,"reason": reason,
                    "state_ref": st,"portion": portion
                })

        # --------------------- 포지션 루프 끝 ---------------------

        # stop_mode 토글 및 즉시 손절 후보 구축
        immediate_sells = []
        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            bal = it.get("balance")
            cur_price = it.get("current_price")
            if not market or pnl is None or bal is None or cur_price is None:
                continue
            st = ps.data.setdefault(market, {})
            ps.ensure_stop_fields(market)
            # stop_mode 토글
            if pnl <= STOP_TRIGGER_PNL:
                if not st["stop_mode_active"]:
                    print(f"[STOP-MODE][ON] {market} pnl={pnl}% <= trigger {STOP_TRIGGER_PNL}%")
                st["stop_mode_active"] = True
            else:
                if st["stop_mode_active"]:
                    print(f"[STOP-MODE][OFF] {market} pnl={pnl}% > trigger {STOP_TRIGGER_PNL}%")
                st["stop_mode_active"] = False

            # 즉시 손절 단순화: pnl ≤ LOSS_CUT_RATE이면 무조건 손절
            if pnl <= LOSS_CUT_RATE:
                immediate_sells.append({
                    "market": market,
                    "pnl": pnl,
                    "balance": bal,
                    "current_price": cur_price
                })

        # 즉시 손절 먼저 처리 (모든 대기 주문 취소 → 언락 → 시장가 전량)
        for item in immediate_sells:
            mkt = item["market"]
            st = ps.data.get(mkt) or {}

            # 활성/수동 지정가/선지정 TP 등 취소
            for k in ("active_limit_uuid", "passive_limit_uuid", "full_limit_uuid", "pre_tp_uuid"):
                uid = st.get(k)
                if uid:
                    try:
                        await cancel_order(access_key, secret_key, uid)
                        if k != "pre_tp_uuid":
                            st[k] = None
                    except Exception as ce:
                        print(f"[LOSS-CUT][WARN] cancel {k} {mkt} err={ce}")

            await cancel_all_open_orders_for_market(access_key, secret_key, mkt, max_rounds=4, sleep_between=0.3)
            bal_now, locked_now = await wait_unlock_and_get_balance(access_key, secret_key, mkt, timeout=2.0, poll=0.25)
            vol = safe_calc_volume(bal_now, Decimal("1"))
            if vol <= 0:
                print(f"[LOSS-CUT][SKIP] {mkt} unlocked but no balance (bal={bal_now}, locked={locked_now})")
                continue

            try:
                best_bid, _best_ask = await get_orderbook_top(mkt)
                notional = vol * best_bid
                if notional < MIN_ORDER_NOTIONAL_KRW:
                    print(f"[LOSS-CUT][SKIP] {mkt} notional {notional} < MIN_ORDER_NOTIONAL_KRW {MIN_ORDER_NOTIONAL_KRW}")
                    continue
            except Exception:
                pass

            try:
                if not LIVE_TRADING:
                    print(f"[LOSS-CUT][DRY] {mkt} pnl={item['pnl']}% (<= {LOSS_CUT_RATE}%) → SELL ALL vol={vol}")
                else:
                    resp = await order_market_sell(access_key, secret_key, mkt, vol)
                    print(f"[LOSS-CUT] {mkt} pnl={item['pnl']}% → SELL ALL uuid={resp.get('uuid')} vol={vol}")
                # 손절 후 상태 초기화
                st["stop_mode_active"] = False
                st["martingale_count"] = 0
                st["last_martin_ts"] = None
                ps.mark_sold(mkt)
            except Exception as e:
                print(f"[LOSS-CUT][ERR] {mkt} market sell fail: {e}")

        # 마틴 추가매수: stop_mode_active이고 martingale_count < MAX_MARTIN일 때 누적×2 매수 (마켓당 1건)
        martin_used_krw_total = Decimal("0")
        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            cur_price = it.get("current_price")
            if not market or pnl is None or cur_price is None:
                continue
            st = ps.data.get(market) or {}

            if not st.get("stop_mode_active"):
                continue
            cur_martin = int(st.get("martingale_count", 0))
            if cur_martin >= MAX_MARTIN:
                continue

            # 현재까지의 총 매수금
            _, total_inv = ps.get_buy_stats(market)
            if total_inv <= 0:
                continue

            # 직전 마틴 추가금
            last_martin_amt = st.get("last_martin_amount")
            try:
                last_martin_amt = Decimal(str(last_martin_amt)) if last_martin_amt is not None else None
            except:
                last_martin_amt = None

            # 마틴 추가금 계산:
            # - 첫 마틴: buy_amt = total_inv * 2
            # - 이후 마틴: buy_amt = last_martin_amt * 2
            if cur_martin == 0:
                buy_amt = (total_inv * Decimal("2")).quantize(Decimal("0.0001"))
            else:
                if last_martin_amt is None or last_martin_amt <= 0:
                    # 안전장치: 기록이 없다면 total_inv 기준으로 재시작
                    buy_amt = (total_inv * Decimal("2")).quantize(Decimal("0.0001"))
                else:
                    buy_amt = (last_martin_amt * Decimal("2")).quantize(Decimal("0.0001"))

            # 최소주문/가용 현금 체크
            if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                print(f"[MARTIN][SKIP] {market} 추가금 {buy_amt} < 최소주문 {MIN_ORDER_NOTIONAL_KRW}")
                continue
            if available_krw < buy_amt:
                print(f"[MARTIN][SKIP] {market} KRW 부족 ({available_krw} < {buy_amt})")
                continue

            # 일반 누적 한도는 마틴에 적용하지 않음. (요구사항대로 별도 트랙)
            try:
                if not LIVE_TRADING:
                    print(
                        f"[MARTIN][DRY-BUY] {market} m#{cur_martin + 1} pnl={pnl}% buyKRW={buy_amt} (total_inv={total_inv}, last={last_martin_amt})")
                else:
                    resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                    print(f"[MARTIN][BUY] {market} m#{cur_martin + 1} pnl={pnl}% 금액={buy_amt} uuid={resp.get('uuid')}")
                ps.record_buy(market, buy_amt)
                st["martingale_count"] = cur_martin + 1
                st["last_martin_ts"] = time.time()
                st["last_martin_amount"] = buy_amt
                st.setdefault("entry_source", "martin")
                martin_used_krw_total += buy_amt

                # 선지정 TP
                if PREPLACE_HARD_TP and LIVE_TRADING:
                    asyncio.create_task(after_market_buy_place_pre_tp(access_key, secret_key, market, ps))
            except Exception as e:
                print(f"[MARTIN][ERR] {market} buy fail: {e}")

        if martin_used_krw_total > 0:
            available_krw -= martin_used_krw_total

        # 매도 실행(익절/트레일)
        realized = Decimal("0")
        for so in sell_orders:
            market = so["market"]
            volume = so["volume"]
            category = so["category"]
            pnl = so["pnl"]
            reason = so["reason"]
            st = so["state_ref"]
            portion = so["portion"]
            if volume <= 0:
                continue
            cur_price = None
            for _it in enriched:
                if _it.get("market") == market:
                    cur_price = _it.get("current_price")
                    break
            if not LIVE_TRADING:
                print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                    if portion < 1:
                        ps.reduce_invested_after_sell(market, portion)
                    else:
                        ps.mark_sold(market)
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                elif category == "TRAIL":
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                else:
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
                continue
            try:
                resp = await order_market_sell(access_key, secret_key, market, volume)
                uid = resp.get("uuid")
                print(f"[ORDER] SELL {market} cat={category} vol={volume} pnl={pnl}% uuid={uid} reason={reason}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                    if portion < 1:
                        ps.reduce_invested_after_sell(market, portion)
                    else:
                        ps.mark_sold(market)
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                elif category == "TRAIL":
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                else:
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
            except Exception as e:
                print(f"[ERR] 매도 실패 {market}: {e}")
                continue

        if realized > 0:
            available_krw += realized

        # 교차(추천) 매수 실행
        try:
            used = await process_intersection_buys(access_key, secret_key, ps, active_set, available_krw)
            if used > 0:
                available_krw -= used
        except Exception as e:
            print(f"[INTX][ERR] buy block exception: {e}")

        # 상태 요약
        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 상태 요약:")
            for a in actions:
                cat = a["cat"]
                status = "HOLD"
                if a["sell"]:
                    status = "SELL"
                elif a["reason"].startswith("ARMED"):
                    status = "ARM"
                print(
                    f"  {a['market']} pnl={a['pnl']} peak={a['peak']} "
                    f"min={a['min_pnl']} dd={a['drawdown']} maxDD={a['max_dd']} "
                    f"armed={a['armed']} cat={cat} -> {status} {a['reason']}"
                )
        # --- 주기 리셋 체크 (루프 말미, 모든 작업 처리 후) ---
        if PERIODIC_RESET_SEC > 0 and (time.time() - start_ts) >= PERIODIC_RESET_SEC:
            print(f"[RESET] periodic reset requested after {PERIODIC_RESET_SEC}s")
            raise ResetRequested()
        await dynamic_sleep()


# ============================================================
# 25. Controller
# ============================================================
_last_cfg_signature = None
async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    global _last_cfg_signature
    print(f"[CTRL] Controller 시작 user={user_no} server={server_no} poll={CONTROLLER_POLL_SEC}s")
    while True:
        try:
            cfg = await fetch_mtpond_setup(user_no)
            if cfg:
                active_flag = str(cfg.get("activeYN", "")).upper() == "Y"
                sig_items = []
                for k, v in cfg.items():
                    if k == "activeYN":
                        continue
                    sig_items.append((k, str(v)))
                sig = tuple(sorted(sig_items))
                if sig != _last_cfg_signature:
                    apply_dynamic_config(cfg)
                    _last_cfg_signature = sig
            else:
                active_flag = None
            now = time.strftime("%H:%M:%S")
            if active_flag is True:
                if task is None or task.done():
                    print(f"[CTRL {now}] activeYN=Y → 모니터 시작")
                    task = asyncio.create_task(monitor_positions(user_no, server_no))
            elif active_flag is False:
                if task and not task.done():
                    print(f"[CTRL {now}] activeYN=N → 모니터 중지")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        print("[CTRL] monitorPositions 취소됨")
                    except Exception as e:
                        print(f"[CTRL] monitorPositions 예외 종료: {e}")
                    task = None
            else:
                print(f"[CTRL {now}] activeYN 조회 실패(None) → 상태 유지")
            if active_flag is True and task and task.done():
                print(f"[CTRL {now}] 모니터 태스크 종료 감지 → 재시작")
                task = asyncio.create_task(monitor_positions(user_no, server_no))
        except ResetRequested:
            # 모니터 태스크가 주기 리셋을 요청 → 즉시 재기동
            print("[CTRL] ResetRequested caught → restarting monitor task")
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print(f"[CTRL] cancel on ResetRequested: {e}")
            task = asyncio.create_task(monitor_positions(user_no, server_no))
        except Exception as e:
            print(f"[CTRL] 루프 예외: {e}")
        await asyncio.sleep(CONTROLLER_POLL_SEC)



# ============================================================
# 새 추천 함수 (BBTrend 후보 계산)
# ============================================================

_bbtrend_cache = {
    "updated": None,
    "fetched_ts": 0.0,
    "candidates_raw": [],
}

def _bb_safe_float(v, default: float = None) -> Optional[float]:
    try:
        if v is None:
            return default
        return float(v)
    except:
        return default

def _bb_index_by_market(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for r in rows:
        m = r.get("market")
        if m:
            out[m] = r
    return out

def _bb_collect_per_market(data: Dict[str, Any], timeframes=BBTREND_TIMEFRAMES) -> Dict[str, Dict[str, Dict[str, Any]]]:
    tf_map = data.get("timeframes", {})
    market_union = set()
    per_tf = {}
    for tf in timeframes:
        rows = tf_map.get(tf, [])
        idx = _bb_index_by_market(rows)
        per_tf[tf] = idx
        market_union |= set(idx.keys())
    merged = {}
    for m in market_union:
        merged[m] = {}
        for tf in timeframes:
            row = per_tf[tf].get(m)
            if row:
                merged[m][tf] = row
    return merged

_BB_VOLUME_KEYS = ["volume", "vol", "trade_volume", "base_volume"]
def _bb_extract_volume(row: Dict[str, Any]) -> Optional[float]:
    for k in _BB_VOLUME_KEYS:
        if k in row:
            return _bb_safe_float(row.get(k))
    return None

def _bb_score_momentum(rows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    max_score = 8
    score = 0
    reasons = []
    r3 = rows.get("3m"); r5 = rows.get("5m"); r15 = rows.get("15m"); r30 = rows.get("30m")
    if not r3 or not r5:
        return {"score": 0, "max_score": max_score, "est_pct": 0.0, "reasons": ["need_3m_5m"]}
    bb3 = _bb_safe_float(r3.get("BB_Pos"))
    bb5 = _bb_safe_float(r5.get("BB_Pos"))
    macd_hist_3 = _bb_safe_float(r3.get("MACD_Hist"))
    macd_hist_5 = _bb_safe_float(r5.get("MACD_Hist"))
    macd3 = _bb_safe_float(r3.get("MACD"))
    macd5 = _bb_safe_float(r5.get("MACD"))
    macd_sig3 = _bb_safe_float(r3.get("MACD_Signal"))
    macd_sig5 = _bb_safe_float(r5.get("MACD_Signal"))
    rsi3 = _bb_safe_float(r3.get("RSI"))
    rsi5 = _bb_safe_float(r5.get("RSI"))
    rsi15 = _bb_safe_float(r15.get("RSI")) if r15 else None
    bb30 = _bb_safe_float(r30.get("BB_Pos")) if r30 else None

    if bb3 is not None and -20 <= bb3 <= 70: score += 1; reasons.append("3m_bb_mid")
    if bb5 is not None and -20 <= bb5 <= 60: score += 1; reasons.append("5m_bb_mid")
    if macd_hist_3 is not None and macd_hist_3 >= 0: score += 1; reasons.append("3m_macd_hist_pos")
    if macd_hist_5 is not None and macd_hist_5 >= 0: score += 1; reasons.append("5m_macd_hist_pos")
    if macd3 is not None and macd_sig3 is not None and macd3 > macd_sig3: score += 1; reasons.append("3m_macd_cross_up")
    if macd5 is not None and macd_sig5 is not None and macd5 > macd_sig5: score += 1; reasons.append("5m_macd_cross_up")
    if (rsi3 is not None and 40 <= rsi3 <= 65) and (rsi5 is not None and 45 <= rsi5 <= 68):
        score += 1; reasons.append("rsi_alignment")
    if ((rsi15 is not None and rsi15 < 70) or rsi15 is None) and (bb30 is not None and bb30 < 90):
        score += 1; reasons.append("higher_tf_not_overbought")
    est_pct = (score / max_score) * 0.01  # 최대 1% (0.01)
    return {"score": score, "max_score": max_score, "est_pct": est_pct, "reasons": reasons}

def _bb_estimate_mean_reversion(rows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    r3 = rows.get("3m"); r5 = rows.get("5m")
    if not r3 or not r5:
        return {"est_pct": 0.0, "qualifies": False, "reasons": ["need_3m_5m"]}
    bb3 = _bb_safe_float(r3.get("BB_Pos"))
    bb5 = _bb_safe_float(r5.get("BB_Pos"))
    rsi3 = _bb_safe_float(r3.get("RSI"))
    macd_hist_3 = _bb_safe_float(r3.get("MACD_Hist"))
    atr3 = _bb_safe_float(r3.get("ATR"))
    bandwidth3 = _bb_safe_float(r3.get("BandWidth"))
    reasons = []
    qualifies = True
    if bb3 is None or bb3 > -50: qualifies = False; reasons.append("bb3_not_low")
    else: reasons.append("bb3_low")
    if bb5 is None or bb5 > -40: qualifies = False; reasons.append("bb5_not_low")
    else: reasons.append("bb5_low")
    if rsi3 is None or not (25 <= rsi3 <= 45): qualifies = False; reasons.append("rsi3_not_rebound")
    else: reasons.append("rsi3_rebound_zone")
    if macd_hist_3 is not None and atr3 and atr3 > 0:
        ratio = abs(macd_hist_3) / atr3
        if ratio > 4: qualifies = False; reasons.append(f"macd_hist_ratio_high={ratio:.2f}")
        else: reasons.append(f"macd_hist_ratio_ok={ratio:.2f}")
    else:
        reasons.append("macd_hist_ratio_skip")
    if not qualifies:
        return {"est_pct": 0.0, "qualifies": False, "reasons": reasons}
    if bb3 is not None and bandwidth3 is not None:
        bw_adj = bandwidth3 / (1 + math.log10(1 + bandwidth3)) if bandwidth3 and bandwidth3 > 0 else 0
        raw_move = ((0 - bb3) / 100.0) * bw_adj
        est_pct = max(0.0, min(raw_move, 0.02))
    else:
        est_pct = 0.0
    return {"est_pct": est_pct, "qualifies": True, "reasons": reasons}

def _bb_liquidity_pass(rows: Dict[str, Dict[str, Any]],
                       min_notional: float = BBTREND_MIN_NOTIONAL_3M) -> Tuple[bool, Dict[str, Any]]:
    r3 = rows.get("3m")
    if not r3:
        return False, {"reason": "no_3m_row"}
    close = _bb_safe_float(r3.get("close"))
    if close is None or close <= 0:
        return False, {"reason": "invalid_close"}
    volume = _bb_extract_volume(r3)
    if volume is None or volume <= 0:
        return False, {"reason": "no_or_zero_volume"}
    notional = close * volume
    if notional < min_notional:
        return False, {"reason": f"notional_lt {notional:.2f} < {min_notional}"}
    return True, {"notional": notional, "volume": volume, "close": close}

def _bb_select_candidates(api_json: Dict[str, Any],
                          min_expected_pct: float = BBTREND_MIN_EXPECTED_PCT,
                          min_notional_3m: float = BBTREND_MIN_NOTIONAL_3M,
                          include_debug: bool = False) -> List[Dict[str, Any]]:
    merged = _bb_collect_per_market(api_json)
    results = []
    for market, rows in merged.items():
        liq_ok, liq_info = _bb_liquidity_pass(rows, min_notional_3m)
        if not liq_ok:
            continue
        close = None
        for tf in ("3m","5m","15m","30m"):
            if tf in rows:
                v = _bb_safe_float(rows[tf].get("close"))
                if v and v > 0:
                    close = v
                    break
        if not close:
            continue
        if close < 100:
            continue  # 100원 미만 코인은 제외
        mom = _bb_score_momentum(rows)
        rev = _bb_estimate_mean_reversion(rows)
        est_pct = max(mom["est_pct"], rev["est_pct"])  # 0.0 ~ 0.02 (이론상)
        tags = []
        if mom["est_pct"] >= rev["est_pct"] and mom["est_pct"] > 0: tags.append("momentum")
        if rev["est_pct"] > mom["est_pct"]: tags.append("mean_reversion")
        if mom["est_pct"] > 0 and rev["est_pct"] > 0: tags.append("both")
        if est_pct >= min_expected_pct:
            item = {
                "market": market,
                "close": close,
                "expected_move_pct": est_pct * 100,
                "target_price": round(close * (1 + est_pct), 8),
                "scenario": tags or ["unknown"],
                "notional_3m": liq_info.get("notional"),
                "volume_3m": liq_info.get("volume")
            }
            if include_debug:
                item["momentum_detail"] = mom
                item["mean_reversion_detail"] = rev
            results.append(item)
    results.sort(key=lambda x: x["expected_move_pct"], reverse=True)
    return results

async def _bb_fetch_raw() -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(BBTREND_API_URL)
        r.raise_for_status()
        return r.json()

async def fetch_bbtrend_candidates(force: bool = False,
                                   include_debug: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if (not force) and _bbtrend_cache["updated"] and (now - _bbtrend_cache["fetched_ts"] < BBTREND_FETCH_INTERVAL_SEC):
        return _bbtrend_cache["candidates_raw"]
    try:
        raw = await _bb_fetch_raw()
    except Exception as e:
        print(f"[BBTREND][ERR] fetch 실패: {e}")
        return _bbtrend_cache["candidates_raw"]
    updated = raw.get("updated")
    if (not force) and updated == _bbtrend_cache["updated"]:
        _bbtrend_cache["fetched_ts"] = now
        return _bbtrend_cache["candidates_raw"]
    cands = _bb_select_candidates(
        raw,
        min_expected_pct=BBTREND_MIN_EXPECTED_PCT,
        min_notional_3m=BBTREND_MIN_NOTIONAL_3M,
        include_debug=include_debug
    )
    _bbtrend_cache["updated"] = updated
    _bbtrend_cache["fetched_ts"] = now
    _bbtrend_cache["candidates_raw"] = cands
    print(f"[BBTREND] updated={updated} 후보 {len(cands)}개 (min_expected_pct={BBTREND_MIN_EXPECTED_PCT*100:.3f}%)")
    return cands

async def debug_show_reject_reasons():
    raw = await _bb_fetch_raw()
    merged = _bb_collect_per_market(raw)
    cnt_total=0; cnt_liq_pass=0; cnt_score_pass=0
    for m, rows in merged.items():
        cnt_total += 1
        liq_ok, liq_info = _bb_liquidity_pass(rows, BBTREND_MIN_NOTIONAL_3M)
        if not liq_ok:
            continue
        cnt_liq_pass += 1
        mom = _bb_score_momentum(rows)
        rev = _bb_estimate_mean_reversion(rows)
        est_pct = max(mom["est_pct"], rev["est_pct"])
        if est_pct >= BBTREND_MIN_EXPECTED_PCT:
            cnt_score_pass += 1
        else:
            print(f"[DBG][DROP_SCORE] {m} est_pct={est_pct*100:.3f}% (< {BBTREND_MIN_EXPECTED_PCT*100:.3f}%) mom={mom['est_pct']*100:.2f}% rev={rev['est_pct']*100:.2f}% score={mom['score']}")
    print(f"[DBG] 전체={cnt_total} 유동성통과={cnt_liq_pass} 최종통과={cnt_score_pass}")

# ============================================================
# 주문 취소후 손절 프로세스 (참고/유틸)
# ============================================================
import time as _time_alias
from decimal import Decimal as _DecAlias, ROUND_DOWN as _RD_ALIAS

def to_amount_precision(exchange, symbol, amount):
    try:
        return float(exchange.amount_to_precision(symbol, amount))
    except Exception:
        return float(amount)

def cancel_open_limit_orders(exchange, symbol, max_wait_sec=5):
    open_orders = exchange.fetch_open_orders(symbol)
    limit_orders = [o for o in open_orders if str(o.get('type', '')).lower().startswith('limit')]
    for o in limit_orders:
        try:
            exchange.cancel_order(o['id'], symbol)
        except Exception as e:
            print(f"[{symbol}] cancel_order 실패: {e}")
    t0 = _time_alias.time()
    while _time_alias.time() - t0 < max_wait_sec:
        remain = exchange.fetch_open_orders(symbol)
        remain_limits = [o for o in remain if str(o.get('type', '')).lower().startswith('limit')]
        if not remain_limits:
            return True
        _time_alias.sleep(0.3)
    return False

def market_flat_spot(exchange, symbol, min_notional=5.0):
    market = exchange.market(symbol)
    base = market['base']
    bal = exchange.fetch_balance()
    free_base = bal.get(base, {}).get('free', 0.0) or 0.0
    if free_base <= 0:
        return {"status": "NO_BALANCE", "amount": 0}
    ticker = exchange.fetch_ticker(symbol)
    price = float(ticker['last'] or ticker['close'])
    if free_base * price < min_notional:
        return {"status": "TOO_SMALL", "amount": free_base}
    amt = to_amount_precision(exchange, symbol, free_base)
    if amt <= 0:
        return {"status": "ROUND_TO_ZERO", "amount": free_base}
    try:
        order = exchange.create_market_sell_order(symbol, amt)
        return {"status": "OK", "order": order, "amount": amt}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

TARGET_STATES = {"PASSIVE", "HOLD-LOCKED"}

def _collect_states(x) -> set:
    states = set()
    for key in ("state", "status", "mode"):
        v = x.get(key)
        if isinstance(v, str):
            states.add(v.upper())
        elif isinstance(v, (list, tuple, set)):
            states |= {str(s).upper() for s in v}
    tags = x.get("tags") or x.get("status_tags")
    if isinstance(tags, str):
        states.add(tags.upper())
    elif isinstance(tags, (list, tuple, set)):
        states |= {str(s).upper() for s in tags}
    return states

def has_any_state(x, targets=TARGET_STATES) -> bool:
    return len(_collect_states(x) & set(targets)) > 0

def is_immediate_cut(x, loss_cut_rate) -> bool:
    try:
        pnl = float(x.get("pnl", 0))
    except Exception:
        return False
    falling = bool(x.get("falling", False))
    recommended = bool(x.get("recommended", True))
    in_locked_states = has_any_state(x, TARGET_STATES)
    return (pnl <= float(loss_cut_rate)) and ((not recommended) or falling or in_locked_states)

def cancel_open_price_based_orders(exchange, symbol, include_stops=True, max_wait_sec=5):
    def is_price_based(o):
        t = str(o.get('type', '')).lower()
        if t.startswith('limit'):
            return True
        if include_stops and ('stop' in t or 'tp' in t or 'take' in t):
            return True
        return False
    open_orders = exchange.fetch_open_orders(symbol)
    targets = [o for o in open_orders if is_price_based(o)]
    for o in targets:
        try:
            exchange.cancel_order(o['id'], symbol)
        except Exception as e:
            print(f"[{symbol}] cancel_order 실패: {e}")
    t0 = _time_alias.time()
    while _time_alias.time() - t0 < max_wait_sec:
        remain = exchange.fetch_open_orders(symbol)
        remain_targets = [o for o in remain if is_price_based(o)]
        if not remain_targets:
            return True
        _time_alias.sleep(0.3)
    return False

def process_immediate_sells(exchange, annotated, LOSS_CUT_RATE, mode="spot", min_notional=5.0):
    immediate_sells = [x for x in annotated if is_immediate_cut(x, LOSS_CUT_RATE)]
    results = []
    for row in immediate_sells:
        symbol = row.get("symbol") or row.get("sym") or row.get("ticker")
        if not symbol:
            results.append({"symbol": None, "status": "NO_SYMBOL"})
            continue
        cancel_open_price_based_orders(exchange, symbol, include_stops=True, max_wait_sec=7)
        if mode == "spot":
            res = market_flat_spot(exchange, symbol, min_notional=min_notional)
        else:
            res = {"status": "UNSUPPORTED_MODE"}
        res["symbol"] = symbol
        res["states"] = list(_collect_states(row))
        results.append(res)
    return results

# ============================================================
# X. 재시작 초기화: 전체 지정가 매도 취소 후 재배치
# ============================================================
async def cancel_all_waiting_orders_for_markets(access_key: str, secret_key: str, markets: List[str]):
    # 모든 대기(wait) 주문 일괄 취소 (종류 가리지 않고), 마켓별로 반복
    for m in markets:
        try:
            ok = await cancel_all_open_orders_for_market(access_key, secret_key, m, max_rounds=4, sleep_between=0.3)
            if not ok:
                print(f"[RESTART][WARN] 일부 주문이 남아있음 market={m}")
        except Exception as e:
            print(f"[RESTART][ERR] cancel all orders fail market={m}: {e}")

async def restart_reseed_after_cancellation(access_key: str,
                                            secret_key: str,
                                            ps: "PositionState",
                                            raw_accounts: List[Dict[str, Any]],
                                            price_map: Dict[str, Decimal]):
    # 잔고 기준 마켓 목록
    markets = build_market_list_from_accounts(raw_accounts, BASE_UNIT)
    # 1) 대기 주문 모두 취소
    await cancel_all_waiting_orders_for_markets(access_key, secret_key, markets)
    # 2) 잠깐 대기 후 언락 보장
    await asyncio.sleep(0.4)
    # 3) 잔고 재조회
    try:
        acc2 = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[RESTART][ERR] accounts reload fail: {e}")
        acc2 = raw_accounts

    # 4) 모드별 재배치
    if FULL_LIMIT_SELL_ENABLED:
        try:
            await manage_full_limit_sells(access_key, secret_key, acc2, price_map, ps)
            print("[RESTART] FULL_LIMIT_SELL 재배치 완료")
        except Exception as e:
            print(f"[RESTART][ERR] FLS 재배치 실패: {e}")
        return

    # FULL_LIMIT_SELL 비활성 시
    # 4-a) PASSIVE 모드 재배치
    if PASSIVE_LIMIT_SELL_ENABLED:
        try:
            await manage_passive_limit_sells(access_key, secret_key, acc2, price_map, ps)
            print("[RESTART] PASSIVE_LIMIT_SELL 재배치 완료")
        except Exception as e:
            print(f"[RESTART][ERR] PASSIVE 재배치 실패: {e}")

    # 4-b) PREPLACE_ON_START가 켜져 있으면 각 포지션에 선지정 TP 재배치
    if PREPLACE_HARD_TP and PREPLACE_ON_START:
        for acc in acc2:
            c = acc.get("currency"); u = acc.get("unit_currency")
            if not c or u != BASE_UNIT or c == BASE_UNIT:
                continue
            market = f"{BASE_UNIT}-{c}"
            price = price_map.get(market)
            if price is None:
                continue
            try:
                bal = Decimal(str(acc.get("balance", "0")))
                locked = Decimal(str(acc.get("locked", "0")))
                avg_raw = acc.get("avg_buy_price")
                avg = Decimal(str(avg_raw)) if avg_raw not in (None, "", "0") else Decimal("0")
            except:
                bal = Decimal("0"); locked = Decimal("0"); avg = Decimal("0")

            total_qty = bal + locked
            if avg <= 0 or total_qty <= 0:
                continue

            st = ps.data.setdefault(market, {})
            # 선지정 TP 중복 방지 필드 제거
            for k in ("active_limit_uuid","limit_submit_ts","limit_pending_category","limit_pending_volume","pre_tp_uuid"):
                st.pop(k, None)
            # 재배치
            try:
                await place_preplaced_hard_tp(access_key, secret_key, market, st, total_qty, avg)
            except Exception as e:
                print(f"[RESTART][WARN] preplace TP 실패 {market}: {e}")
        print("[RESTART] PREPLACE_HARD_TP 재배치 완료")


# ============================================================
# 26. main
# ============================================================
async def main():
    init_config()
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청(Ctrl+C)")

