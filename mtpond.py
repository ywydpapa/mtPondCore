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
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
import dotenv
import httpx
import jwt
import topuprise
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

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
# 4. STATIC CONFIG (재시작 필요 / 거의 고정)
# ============================================================
WATCH_MIN_PNL = Decimal("-10")      # 너무 큰 손실 포지션은 추적 제외
ARM_PNL = Decimal("0.25")           # 트레일링 시작 준비 (Arming)
HARD_TP = Decimal("1.5")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET
TRAIL_DROP = Decimal("0.15")
TRAIL_START_PNL = Decimal("0.45")
ENABLE_STOP_TRAIL = os.getenv("ENABLE_STOP_TRAIL", "1") == "1"   # (구 StopTrail 플래그, 신규 손절 사용시 실질적 비활성화)
STOP_SELL_PORTION = Decimal(os.getenv("STOP_SELL_PORTION", "0.10"))
STOP_MAX_SELLS = int(os.getenv("STOP_MAX_SELLS", "6"))
STOP_DISABLE_NEW_BUYS = os.getenv("STOP_DISABLE_NEW_BUYS", "1") == "1"
STOP_REBOUND_COOLDOWN_SEC = int(os.getenv("STOP_REBOUND_COOLDOWN_SEC", "30"))
STOP_COOLDOWN_BATCH_SELL = os.getenv("STOP_COOLDOWN_BATCH_SELL", "1") == "1"
STOP_COOLDOWN_MAX_BATCH = int(os.getenv("STOP_COOLDOWN_MAX_BATCH", "5"))
STOP_RECOVERY_REQUIRE_COOLDOWN = os.getenv("STOP_RECOVERY_REQUIRE_COOLDOWN", "1") == "1"
STOP_RECOVERY_LOG = os.getenv("STOP_RECOVERY_LOG", "1") == "1"
STOP_SIMPLE_MODE = os.getenv("STOP_SIMPLE_MODE", "1") == "1"     # (구 단순 Stop 모드)
STOP_INITIAL_SELL_PORTION = Decimal(os.getenv("STOP_INITIAL_SELL_PORTION", "0.5"))
STOP_FINAL_MOVE_THRESHOLD = Decimal(os.getenv("STOP_FINAL_MOVE_THRESHOLD", "0.1"))
# === 새 1회성 손절 로직 관련 ===
ENABLE_NEW_STOP = os.getenv("ENABLE_NEW_STOP", "1") == "1"        # 새 손절 로직 스위치
STOP_LOSS_FIRST_PORTION = Decimal(os.getenv("STOP_LOSS_FIRST_PORTION", "0.5"))     # 최초 손절 비율 (기본 50%)
STOP_LOSS_MIN_REMAIN_KRW = Decimal(os.getenv("STOP_LOSS_MIN_REMAIN_KRW", "5000"))  # 50% 손절 후 남기는 최소 평가금 기준
STOP_SECOND_EXIT_EXTRA_PNL = Decimal(os.getenv("STOP_SECOND_EXIT_EXTRA_PNL", "0.3"))  # (구 로직 잔존 변수, 미사용)
USE_LIMIT_SELL_ON_TRAIL = os.getenv("USE_LIMIT_SELL_ON_TRAIL", "1") == "1"
LIMIT_SELL_REPRICE_INTERVAL_SEC = int(os.getenv("LIMIT_SELL_REPRICE_INTERVAL_SEC", "5"))  # (현재 재호가 미사용)
LIMIT_SELL_UNFILLED_TIMEOUT_SEC = int(os.getenv("LIMIT_SELL_UNFILLED_TIMEOUT_SEC", "25"))
LIMIT_SELL_FALLBACK_TO_MARKET = os.getenv("LIMIT_SELL_FALLBACK_TO_MARKET", "1") == "1"
LIMIT_SELL_PRICE_MODE = os.getenv("LIMIT_SELL_PRICE_MODE", "bid")  # 'bid' or 'mid'
LIMIT_SELL_BID_OFFSET_TICKS = int(os.getenv("LIMIT_SELL_BID_OFFSET_TICKS", "0"))   # (현재 구현 offset 적용 X)
SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "1.0"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "0.7"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))
INTERVAL_SECONDS = 5
FIVE_MIN_SECONDS = 300
MIN_NOTIONAL_KRW = Decimal("5500")
MAX_BACKOFF = 120
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
WHITELIST_MARKETS: List[str] = []
ENFORCE_WHITELIST = False
INTERSECTION_USE_CACHE_ON_EMPTY = os.getenv("INTERSECTION_USE_CACHE_ON_EMPTY", "1") == "1"
INTERSECTION_CACHE_TTL_SEC = int(os.getenv("INTERSECTION_CACHE_TTL_SEC", "180"))
INTERSECTION_MAX_EMPTY_WARN = int(os.getenv("INTERSECTION_MAX_EMPTY_WARN", "5"))
INTERSECTION_BUY_ENABLED = os.getenv("INTERSECTION_BUY_ENABLED", "1") == "1"
INTERSECTION_MIN_SCORE = Decimal(os.getenv("INTERSECTION_MIN_SCORE", "10"))
INTERSECTION_MAX_BUY_PER_CYCLE = int(os.getenv("INTERSECTION_MAX_BUY_PER_CYCLE", "1"))
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "999999"))
ENABLE_RANGE_BUY = os.getenv("ENABLE_RANGE_BUY", "1") == "1"
MAX_BUY_PER_WINDOW = int(os.getenv("MAX_BUY_PER_WINDOW", "999"))
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

# 설정 해석 옵션
USE_INITAMT_FOR_INTERSECTION = os.getenv("USE_INITAMT_FOR_INTERSECTION", "1") == "1"
NORMALIZE_MARGIN_PERCENT = os.getenv("NORMALIZE_MARGIN_PERCENT", "0") == "1"
MARGIN_PERCENT_DIVISOR = Decimal(os.getenv("MARGIN_PERCENT_DIVISOR", "1"))
ALLOW_ADDITIONAL_BUY_WHEN_FULL = os.getenv("ALLOW_ADDITIONAL_BUY_WHEN_FULL","1")=="1"
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS","5"))

AVG_DOWN_ENABLED=1
AVG_DOWN_FACTOR=2          # 초기 진입(첫 매수) 금액의 배수
AVG_DOWN_BOLL_PERIOD=20
AVG_DOWN_BOLL_MULT=2
AVG_DOWN_LOWER_TOUCH_TOL=0.002   # 0.2% 여유 허용
AVG_DOWN_REBOUND_PCT=1.0         # 하단터치 저점 대비 +1% 이상 반등 시
AVG_DOWN_MIN_PNL=-2.0            # 최소 손익률(예: -2% 이하)에서만 전략 고려 (필요시)
AVG_DOWN_GLOBAL_COOLDOWN_SEC=60  # 한 종목 수행 후 다른 종목 대기
AVG_DOWN_MARKET_COOLDOWN_SEC=999999  # 동일 종목 2회 제한(사실상 1회)
AVG_DOWN_TIMEFRAME_MIN=1         # 캔들 분 단위 (1분봉 권장)
AVG_DOWN_CANDLE_COUNT=120
AVG_DOWN_REQUIRE_MIN_VOL_KRW=10000  # 최소 추가매수 금액

AVG_DOWN_TP_ENABLED = os.getenv("AVG_DOWN_TP_ENABLED", "1") == "1"
AVG_DOWN_TP_ARM_PNL = Decimal(os.getenv("AVG_DOWN_TP_ARM_PNL", "0.20"))
AVG_DOWN_TP_HARD_PNL = Decimal(os.getenv("AVG_DOWN_TP_HARD_PNL", "0.40"))
AVG_DOWN_TP_TRAIL_START = Decimal(os.getenv("AVG_DOWN_TP_TRAIL_START", "0.40"))
AVG_DOWN_TP_TRAIL_DROP = Decimal(os.getenv("AVG_DOWN_TP_TRAIL_DROP", "0.15"))
AVG_DOWN_TP_SELL_PORTION = Decimal(os.getenv("AVG_DOWN_TP_SELL_PORTION", "1.0"))
AVG_DOWN_TP_RESET_PEAK = os.getenv("AVG_DOWN_TP_RESET_PEAK", "1") == "1"


AVG_DOWN_ACTIVE = None  # {"market": "...", "ts": float}

# ============================================================
# 5. RUNTIME CONFIG
# ============================================================
_CONFIG_INITIALIZED = False

def init_config(force: bool = False):
    """전역 런타임 설정 초기화"""
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT
    global ADDITIONAL_BUY_KRW, USE_TICK_RATE, TICK_RATE

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
    INTERSECTION_BUY_KRW = Decimal(os.getenv("INTERSECTION_BUY_KRW", "200000"))
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))
    BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-0.2"))
    BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.15"))
    STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.7"))
    STOP_PEAK_INCREMENT = Decimal(os.getenv("STOP_PEAK_INCREMENT", "0.1"))
    ADDITIONAL_BUY_KRW = Decimal("0")
    USE_TICK_RATE = False
    TICK_RATE = Decimal("0")

    _CONFIG_INITIALIZED = True
    print("[INIT] Runtime config 초기화 완료")

def apply_dynamic_config(cfg: dict):
    required_globals = [
        "MAX_ACTIVE_MARKETS", "RANGE_BUY_KRW", "INTERSECTION_BUY_KRW",
        "MAX_TOTAL_INVEST_PER_MARKET", "BUY_RANGE_LOW", "BUY_RANGE_HIGH",
        "STOP_TRIGGER_PNL", "STOP_PEAK_INCREMENT", "ADDITIONAL_BUY_KRW",
        "USE_TICK_RATE", "TICK_RATE"
    ]
    miss = [k for k in required_globals if k not in globals()]
    if miss:
        print(f"[CFG] 전역 미초기화 감지:{miss} → init_config() 필요")
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT, ADDITIONAL_BUY_KRW
    global USE_TICK_RATE, TICK_RATE

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
                    if val != STOP_TRIGGER_PNL:
                        changes.append(f"STOP_TRIGGER_PNL {STOP_TRIGGER_PNL} -> {val}")
                        STOP_TRIGGER_PNL = val
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
    best_bid = float(top["bid_price"])
    best_ask = float(top["ask_price"])
    return best_bid, best_ask

async def order_limit_sell(access_key, secret_key, market: str, volume: Decimal, price: Decimal):
    params = {
        "market": market,
        "side": "ask",
        "volume": str(volume),
        "price": str(price),
        "ord_type": "limit",
    }
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def cancel_order(access_key, secret_key, uuid_: str):
    url = "https://api.upbit.com/v1/order"
    params = {"uuid": uuid_}
    token = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.delete(url, headers=headers, params=params)
    if r.status_code != 200:
        print(f"[CANCEL] 실패 uuid={uuid_} status={r.status_code} body={r.text}")
    else:
        print(f"[CANCEL] 성공 uuid={uuid_}")

def adjust_price_to_tick(price: float) -> float:
    if price >= 2000000: unit = 1000
    elif price >= 1000000: unit = 500
    elif price >= 500000: unit = 100
    elif price >= 100000: unit = 50
    elif price >= 10000: unit = 10
    elif price >= 1000: unit = 1
    elif price >= 100: unit = 0.1
    else: unit = 0.01
    return math.floor(price / unit) * unit

# ===================== (B) 캔들 & 볼린저 계산 함수 추가 =====================
async def fetch_minute_candles(market: str, unit: int = 1, count: int = 120):
    """
    Upbit 분봉 캔들 (최신순 반환) → 시간 오름차순으로 리턴
    """
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    params = {"market": market, "count": count}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"candles status={r.status_code} body={r.text[:120]}")
    data = r.json()
    if not data:
        return []
    # 최신이 앞 → 뒤집기
    data.reverse()
    # 각 항목: opening_price, high_price, low_price, trade_price(종가), timestamp 등
    return data

def compute_bollinger_from_candles(candles: list, period: int, mult: Decimal):
    """
    candles: 시간 오름차순 list, 각 item['trade_price'] 사용
    return: (lower, middle, upper, last_close)
    """
    if len(candles) < period:
        return None
    closes = [Decimal(str(c["trade_price"])) for c in candles]
    window = closes[-period:]
    sma = sum(window) / Decimal(period)
    # 표준편차
    # (population)
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
# 10. 시세 조회 보조
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
# 11. PNL 계산
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
# 12. Intersection 데이터
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
    meta = {"source": None, "empty_streak": UPRISES_EMPTY_STREAK, "fresh_ts": None, "cache_age": None}
    try:
        raw = topuprise.uprises()
    except Exception as e:
        raw = None
        print(f"[WARN] uprises() 예외:{e}")
    cands = _normalize_uprises(raw)
    now = time.time()
    meta["fresh_ts"] = now
    if _is_effectively_empty(cands):
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
            cands = UPRISES_LAST_NONEMPTY
        else:
            meta["source"] = "empty"
        return cands, meta
    else:
        UPRISES_EMPTY_STREAK = 0
        UPRISES_LAST_NONEMPTY = cands
        UPRISES_LAST_TS = now
        meta["source"] = "fresh"
        meta["empty_streak"] = 0
        meta["cache_age"] = 0
        return cands, meta

# ============================================================
# 13. 포지션 상태 클래스
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

        # 신규 초기화
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,          # 진입 후 최고 PNL
                "min_pnl": pnl,           # 진입 후 최저 PNL (추가)
                "max_drawdown": Decimal("0"),  # 관측된 최대 드로다운 (추가)
                "last_drawdown": Decimal("0"), # 직전 갱신 시점의 드로다운 (추가)
                "max_runup": Decimal("0"),     # 최저점 이후 최대 반등폭 (추가)
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
                "avg_down_tp_armed": False
            }
            return self.data[market]

        # --- 기존 peak 갱신 ---
        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl

        # --- 신규: min_pnl (최저 PNL) 갱신 ---
        # (peak 갱신 이후에 처리해도 문제 없음. 독립적인 최소값 추적)
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl

        # --- 드로다운 계산 (peak 대비 현재 하락폭) ---
        # peak_pnl >= pnl 일 때만 유효
        try:
            drawdown = st["peak_pnl"] - pnl if pnl < st["peak_pnl"] else Decimal("0")
        except Exception:
            drawdown = Decimal("0")

        st["last_drawdown"] = drawdown

        # --- 최대 드로다운 갱신 ---
        if drawdown > st.get("max_drawdown", Decimal("0")):
            st["max_drawdown"] = drawdown

        # --- 런업(run-up): 최저점 대비 현재 반등폭 ---
        # min_pnl 은 손익률 자체이므로 (현재 pnl - min_pnl)가 반등폭
        try:
            runup = pnl - st["min_pnl"] if pnl > st["min_pnl"] else Decimal("0")
        except Exception:
            runup = Decimal("0")
        if runup > st.get("max_runup", Decimal("0")):
            st["max_runup"] = runup

        # --- 기존 worst_pnl 로직 (stop_triggered 시에만) 유지 ---
        if st.get("stop_triggered") and pnl < st.get("worst_pnl", pnl):
            st["worst_pnl"] = pnl

        st["prev_pnl"] = pnl
        st["last_update_ts"] = now
        return st

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
# 14. 매도 판단 로직 (익절/트레일)
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
    """
    Avg-Down 이후 전용 TP 판단.
    pnl: 전체 포지션 현재 PNL%
    state: 포지션 상태 dict
    반환: (sell_bool, reason, category) 또는 (False,"",None)
    """
    if not state.get("avg_down_tp_mode") or state.get("avg_down_tp_completed"):
        return False, "", None
    try:
        baseline = Decimal(str(state.get("avg_down_tp_baseline_pnl")))
    except:
        baseline = pnl
    offset = pnl - baseline  # Offset PNL
    # 최고 Offset 갱신
    prev_peak = state.get("avg_down_tp_peak_offset", Decimal("0"))
    if offset > prev_peak:
        state["avg_down_tp_peak_offset"] = offset

    armed = state.get("avg_down_tp_armed", False)

    # 1) 하드 TP (즉시 매도)
    if offset >= AVG_DOWN_TP_HARD_PNL:
        return True, f"AD_TP_HARD offset={offset}% >= {AVG_DOWN_TP_HARD_PNL}%", "AD_TP_HARD"

    # 2) ARM 조건
    if (not armed) and offset >= AVG_DOWN_TP_ARM_PNL:
        state["avg_down_tp_armed"] = True
        return False, f"AD_TP_ARM offset={offset}% >= {AVG_DOWN_TP_ARM_PNL}%", None

    # 3) 트레일 (armed 상태)
    if armed and prev_peak >= AVG_DOWN_TP_TRAIL_START:
        drop = prev_peak - offset
        if drop >= AVG_DOWN_TP_TRAIL_DROP:
            return True, f"AD_TP_TRAIL drop={drop}% >= {AVG_DOWN_TP_TRAIL_DROP}% (peakOff={prev_peak} nowOff={offset})", "AD_TP_TRAIL"

    return False, "", None

def decide_sell(market, pnl: Decimal, state: dict):
    if state.get("stop_triggered") and not ENABLE_NEW_STOP:
        return False, "", None
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)
    hard_tp2_target = get_state_decimal(state, "dynamic_hard_tp2", HARD_TP2_BASE)

    if hard_tp_taken and (not hard_tp2_taken) and pnl >= hard_tp2_target:
        label = "HARD_TP2"
        if state.get("dynamic_hard_tp2") and hard_tp2_target != HARD_TP2_BASE:
            label += "(dyn)"
        return True, f"{label} {pnl}% >= {hard_tp2_target}%", "HARD_TP2"

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
# 15. 공용 유틸
# ============================================================
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
    if USE_TICK_RATE and TICK_RATE > 0:
        await asyncio.sleep(float(TICK_RATE))
    else:
        await sleep_until_next_boundary()

# ============================================================
# 16. 활성 포지션 판별
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
# 17. 원격 설정 fetch
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
# 18. 주문 함수
# ============================================================
async def order_market_sell(access_key, secret_key, market, volume: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "ord_type": "market"}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def order_market_buy_price(access_key, secret_key, market, krw_amount: Decimal):
    params = {"market": market, "side": "bid", "price": str(krw_amount), "ord_type": "price"}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def order_limit_buy(access_key: str,secret_key: str,market: str,volume: Decimal,price: Decimal):
    params = { "market": market, "side": "bid", "volume": str(volume), "price": str(price), "ord_type": "limit",}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = { "Authorization": f"Bearer {jwt_t}", "Accept": "application/json" }
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)


async def order_limit_sell( access_key: str, secret_key: str, market: str, volume: Decimal, price: Decimal):
    params = { "market": market, "side": "ask", "volume": str(volume), "price": str(price), "ord_type": "limit", }
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = { "Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def get_order(access_key, secret_key, uuid_):
    params = {"uuid": uuid_}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_get_json("https://api.upbit.com/v1/order", headers=headers, params=params)

# ============================================================
# 19. 지정가 TP 주문 생성 함수 (새 트레일/익절용)
# ============================================================
async def place_limit_tp_order(access_key: str, secret_key: str, market: str, volume: Decimal, category: str, state: dict):
    """
    HARD_TP1 / HARD_TP2 / TRAIL 익절 시 지정가 주문 발행.
    최우선 bid 가격 사용, 틱 단위 내림.
    """
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
# 20. 잔고/가격 보조
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
# 21. 메인 모니터 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("[ERR] API 키 없음 → 종료")
        return

    access_key, secret_key = keys
    ps = PositionState()
    print("=== 모니터 시작 ===")
    if EXCLUDED_MARKETS:
        print(f"[INFO] 제외 목록: {sorted(EXCLUDED_MARKETS)}")
    if ENABLE_NEW_STOP:
        print(f"[INFO] NEW_STOP 활성화 trigger={STOP_TRIGGER_PNL}% first_portion={STOP_LOSS_FIRST_PORTION*100:.1f}% minRemain={STOP_LOSS_MIN_REMAIN_KRW}")
    elif ENABLE_STOP_TRAIL:
        print(f"[INFO] (LEGACY STOP) trigger={STOP_TRIGGER_PNL}% simple={STOP_SIMPLE_MODE}")
    print(f"[INFO] MAX_ACTIVE_MARKETS={MAX_ACTIVE_MARKETS} ALLOW_ADDITIONAL_BUY_WHEN_FULL={ALLOW_ADDITIONAL_BUY_WHEN_FULL}")

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
            for k, v in {
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0,
                "worst_pnl": Decimal("0"), "stop_last_sell_ts": None,
                "stop_cooldown_flag": False, "stop_pending_peak": None, "stop_cooldown_start_ts": None
            }.items():
                st.setdefault(k, v)

        print(f"[INFO] 초기 추적 {len(ps.buy_info)}개 (orphan={len(orphan)})")

    except Exception as e:
        print(f"[WARN] 초기화 부분 실패 (진행): {e}")

    await align_to_half_minute()
    prev_active_count = None

    while True:
        try:
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print(f"[ERR] 잔고 조회 실패: {e}")
            await dynamic_sleep()
            continue

            # (계속)
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

        enriched_all = enrich_accounts_with_prices(raw_accounts, price_map, BASE_UNIT)
        enriched = [r for r in enriched_all if r.get("current_price") is not None]
        available_krw = get_available_krw(raw_accounts)

        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)
        if prev_active_count is None or prev_active_count != active_count:
            print(f"[PORTFOLIO] 활성 {active_count}개 (한도 {MAX_ACTIVE_MARKETS})")
            prev_active_count = active_count

        actions = []
        sell_orders = []

        # --------------------- 포지션별 처리 (개선된 블록) ---------------------
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

            # (A) 지정가 TP 주문 진행중이면 상태 관리
            if st.get("active_limit_uuid"):
                uid = st["active_limit_uuid"]
                submit_ts = st.get("limit_submit_ts")
                elapsed = (time.time() - submit_ts) if submit_ts else 0
                timeout_sec = LIMIT_SELL_UNFILLED_TIMEOUT_SEC
                try:
                    od = await get_order(access_key, secret_key, uid)
                    od_state = od.get("state")
                    rem_vol_raw = od.get("remaining_volume")
                    try:
                        rem_vol = Decimal(str(rem_vol_raw)) if rem_vol_raw is not None else Decimal("0")
                    except:
                        rem_vol = Decimal("0")

                    if od_state == "done" or rem_vol <= 0:
                        cat = st.get("limit_pending_category")
                        try:
                            orig_vol = Decimal(st.get("limit_pending_volume", "0"))
                        except:
                            orig_vol = Decimal("0")
                        print(f"[TP-LIMIT] 체결완료 market={market} uuid={uid} cat={cat} rem={rem_vol}")
                        if cat == "HARD_TP1":
                            st["hard_tp_taken"] = True
                            if HARD_TP_SELL_PORTION < 1:
                                ps.reduce_invested_after_sell(market, HARD_TP_SELL_PORTION)
                        elif cat == "HARD_TP2":
                            st["hard_tp2_taken"] = True
                            if HARD_TP2_SELL_PORTION >= 1:
                                ps.mark_sold(market)
                            else:
                                ps.reduce_invested_after_sell(market, HARD_TP2_SELL_PORTION)
                        elif cat == "TRAIL":
                            ps.mark_sold(market)
                        for k in ("active_limit_uuid","limit_submit_ts","limit_pending_category","limit_pending_volume"):
                            st.pop(k, None)
                        continue
                    else:
                        if elapsed > timeout_sec:
                            print(f"[TP-LIMIT] 타임아웃 → cancel+fallback market={market} uuid={uid}")
                            try:
                                await cancel_order(access_key, secret_key, uid)
                            except Exception as ce:
                                print(f"[TP-LIMIT] cancel 실패 market={market} e={ce}")
                            if LIMIT_SELL_FALLBACK_TO_MARKET:
                                if rem_vol > 0:
                                    try:
                                        await order_market_sell(access_key, secret_key, market, rem_vol)
                                        print(f"[TP-LIMIT] Fallback 시장가 매도 market={market} vol={rem_vol}")
                                    except Exception as fe:
                                        print(f"[TP-LIMIT] fallback 실패 market={market} e={fe}")
                                cat = st.get("limit_pending_category")
                                if cat == "HARD_TP1":
                                    st["hard_tp_taken"] = True
                                    if HARD_TP_SELL_PORTION < 1:
                                        ps.reduce_invested_after_sell(market, HARD_TP_SELL_PORTION)
                                elif cat == "HARD_TP2":
                                    st["hard_tp2_taken"] = True
                                    if HARD_TP2_SELL_PORTION >= 1:
                                        ps.mark_sold(market)
                                    else:
                                        ps.reduce_invested_after_sell(market, HARD_TP2_SELL_PORTION)
                                elif cat == "TRAIL":
                                    ps.mark_sold(market)
                            for k in ("active_limit_uuid","limit_submit_ts","limit_pending_category","limit_pending_volume"):
                                st.pop(k, None)
                            continue
                        else:
                            continue
                except Exception as e:
                    print(f"[TP-LIMIT] 주문조회 실패 market={market} uuid={uid} e={e}")
                    continue

            if AVG_DOWN_ENABLED:
                # 안전한 setdefault (초기화 안 되어 있을 가능성 대비)
                for k, v in (
                    ("avg_down_done", False),
                    ("avg_down_candidate", False),
                    ("avg_down_touch_ts", None),
                    ("avg_down_touch_price", None),
                    ("avg_down_buy_uuid", None),
                    ("first_buy_amount", None),
                    ("avg_down_tp_mode", False),
                    ("avg_down_tp_completed", False),
                    ("avg_down_tp_baseline_pnl", None),
                    ("avg_down_tp_peak_offset", Decimal("0")),
                    ("avg_down_tp_armed", False),):
                    st.setdefault(k, v)

                # 최초 진입 추정 (first_buy_amount 없으면 추정)
                if st.get("first_buy_amount") is None:
                    # buy_info 에서 첫 진입 금액 추적 실패 시 현재 잔고 * 평균단가 추정
                    total_buys, total_inv = ps.get_buy_stats(market)
                    if total_buys >= 1:
                        # 추정: 첫 매수 금액 = total_inv / total_buys (단순 평균) 보다
                        # 더 정확히 하려면 별도 first 기록 필요. 여기서는 첫 번째 기록이 없으므로 근사치 사용.
                        st["first_buy_amount"] = (total_inv / total_buys).quantize(
                            Decimal("0.0001")) if total_buys > 0 else None
                    if st["first_buy_amount"] is None and avg and bal:
                        try:
                            est = (bal * avg)
                            st["first_buy_amount"] = est.quantize(Decimal("0.0001"))
                        except:
                            pass

                if not st["avg_down_done"]:
                    # PNL 조건 (예: -2% 이하에서만)
                    if pnl <= AVG_DOWN_MIN_PNL:
                        new_min_hit = (pnl == st.get("min_pnl"))  # update_or_init 후 peak/min 갱신됨
                        global AVG_DOWN_ACTIVE
                        now_ts = time.time()
                        # 글로벌 락 만료 여부
                        lock_busy = False
                        if AVG_DOWN_ACTIVE:
                            # 다른 종목이 실행 중
                            if AVG_DOWN_ACTIVE["market"] != market:
                                # 만료?
                                if (now_ts - AVG_DOWN_ACTIVE["ts"]) < AVG_DOWN_GLOBAL_COOLDOWN_SEC:
                                    lock_busy = True
                                else:
                                    # 락 만료
                                    AVG_DOWN_ACTIVE = None
                        if not lock_busy:
                            try:
                                # (1) 하단 터치 감시
                                if new_min_hit and not st["avg_down_candidate"]:
                                    # 캔들 조회 & 밴드
                                    try:
                                        candles = await fetch_minute_candles(market, AVG_DOWN_TIMEFRAME_MIN,
                                                                             AVG_DOWN_CANDLE_COUNT)
                                        bb = compute_bollinger_from_candles(candles, AVG_DOWN_BOLL_PERIOD,
                                                                            AVG_DOWN_BOLL_MULT)
                                    except Exception as ce:
                                        bb = None
                                        if str(ce):
                                            print(f"[AVG_DOWN] 캔들/BB 실패 {market}: {ce}")
                                    if bb:
                                        lower, mid, upper, last_close = bb
                                        # 하단 터치 판단
                                        if last_close <= lower * (Decimal("1") + AVG_DOWN_LOWER_TOUCH_TOL):
                                            st["avg_down_touch_ts"] = now_ts
                                            st["avg_down_touch_price"] = last_close
                                            st["avg_down_candidate"] = True
                                            # 글로벌 소유권 예약(잠정) - 아직 매수는 아니므로 필요 없으면 주석 처리 가능
                                            # AVG_DOWN_ACTIVE = {"market": market, "ts": now_ts}
                                            print(
                                                f"[AVG_DOWN] LOWER TOUCH 감지 {market} price={last_close} lower={lower}")
                                # (2) 반전 감시 & 실행
                                elif st["avg_down_candidate"]:
                                    touch_price = st.get("avg_down_touch_price")
                                    if touch_price:
                                        try:
                                            candles = await fetch_minute_candles(market, AVG_DOWN_TIMEFRAME_MIN,
                                                                                 AVG_DOWN_CANDLE_COUNT)
                                            bb = compute_bollinger_from_candles(candles, AVG_DOWN_BOLL_PERIOD,
                                                                                AVG_DOWN_BOLL_MULT)
                                        except Exception as ce2:
                                            bb = None
                                            print(f"[AVG_DOWN] 재계산 실패 {market}: {ce2}")
                                        if bb:
                                            lower, mid, upper, last_close = bb
                                            # 반등 조건
                                            condA = last_close >= mid
                                            condB = (last_close - touch_price) / touch_price * Decimal(
                                                "100") >= AVG_DOWN_REBOUND_PCT
                                            if condA or condB:
                                                # 실행 준비
                                                # 글로벌 락 다시 점검 / 확보
                                                if AVG_DOWN_ACTIVE and AVG_DOWN_ACTIVE["market"] not in (None, market):
                                                    # 다른 종목이 이미 확정 실행 중
                                                    pass
                                                else:
                                                    # 금액 계산
                                                    total_buys, total_inv = ps.get_buy_stats(market)
                                                    first_amt = st.get("first_buy_amount") or (
                                                        total_inv / total_buys if total_buys else None)
                                                    if first_amt is None:
                                                        # 추정 불가시 스킵
                                                        print(f"[AVG_DOWN] first_amt 추정불가 skip {market}")
                                                    else:
                                                        target_amount = (first_amt * AVG_DOWN_FACTOR).quantize(
                                                            Decimal("0.0001"))
                                                        # 한도/가용 KRW 적용
                                                        remaining_cap = MAX_TOTAL_INVEST_PER_MARKET - total_inv if MAX_TOTAL_INVEST_PER_MARKET > 0 else target_amount
                                                        buy_amt = min(target_amount, remaining_cap, available_krw)
                                                        if buy_amt < AVG_DOWN_REQUIRE_MIN_VOL_KRW:
                                                            print(
                                                                f"[AVG_DOWN] 금액부족 skip {market} need>={AVG_DOWN_REQUIRE_MIN_VOL_KRW} got={buy_amt}")
                                                        else:
                                                            # 실행
                                                            if not LIVE_TRADING:
                                                                print(
                                                                    f"[DRY_AVG_DOWN] {market} buy_amt={buy_amt} rebound condA={condA} condB={condB} from={touch_price} now={last_close}")

                                                                ps.record_buy(market, buy_amt)
                                                                st["avg_down_done"] = True
                                                                st["avg_down_candidate"] = False
                                                                AVG_DOWN_ACTIVE = {"market": market, "ts": now_ts}
                                                                if AVG_DOWN_TP_ENABLED and not st.get("avg_down_tp_mode") and not st.get("avg_down_tp_completed"):
                                                                    st["avg_down_tp_mode"] = True
                                                                    st["avg_down_tp_completed"] = False
                                                                    st["avg_down_tp_baseline_pnl"] = pnl
                                                                    st["avg_down_tp_peak_offset"] = Decimal("0")
                                                                    st["avg_down_tp_armed"] = False
                                                                    if AVG_DOWN_TP_RESET_PEAK:
                                                                        st["peak_pnl"] = pnl
                                                                        st["min_pnl"] = pnl
                                                                    print(f"[AD_TP] 모드 활성화 {market} baseline={pnl}%")
                                                                if AVG_DOWN_TP_ENABLED and not st.get("avg_down_tp_mode") and not st.get("avg_down_tp_completed"):
                                                                    st["avg_down_tp_mode"] = True
                                                                    st["avg_down_tp_completed"] = False
                                                                    st["avg_down_tp_baseline_pnl"] = pnl
                                                                    st["avg_down_tp_peak_offset"] = Decimal("0")
                                                                    st["avg_down_tp_armed"] = False
                                                                    if AVG_DOWN_TP_RESET_PEAK:
                                                                        st["peak_pnl"] = pnl
                                                                        st["min_pnl"] = pnl
                                                                print(f"[AD_TP] 모드 활성화 {market} baseline={pnl}% (DRY)")

                                                            else:
                                                                try:
                                                                    resp = await order_market_buy_price(access_key,
                                                                                                        secret_key,
                                                                                                        market, buy_amt)
                                                                    uid = resp.get("uuid")
                                                                    print(
                                                                        f"[ORDER] AVG_DOWN BUY {market} amt={buy_amt} uid={uid} rebound condA={condA} condB={condB} from={touch_price} now={last_close}")
                                                                    st["avg_down_buy_uuid"] = uid
                                                                    ps.record_buy(market, buy_amt)
                                                                    st["avg_down_done"] = True
                                                                    st["avg_down_candidate"] = False
                                                                    AVG_DOWN_ACTIVE = {"market": market, "ts": now_ts}
                                                                except Exception as eod:
                                                                    print(f"[ERR] AVG_DOWN 주문실패 {market}: {eod}")
                            except Exception as eavg:
                                print(f"[AVG_DOWN] 로직 예외 {market}: {eavg}")
                    else:
                        # PNL 조건 벗어나면 candidate 초기화(선택적)
                        # st["avg_down_candidate"] = False
                        pass

                # 동일 종목 후속 재실행 제한
                if st.get("avg_down_done"):
                    # 시장별 쿨다운 (사실상 1회 제한, 시간 지나도 안 풀고 싶으면 조건 유지)
                    pass

            # (B) 새 손절 로직
            if ENABLE_NEW_STOP:
                new_stage = st.get("new_stop_stage")
                if pnl <= STOP_TRIGGER_PNL and new_stage not in ("done",):
                    if new_stage is None:
                        total_notional = bal * cur_price
                        half_volume = safe_calc_volume(bal, STOP_LOSS_FIRST_PORTION)
                        remaining_after_half = (bal - half_volume) * cur_price
                        if remaining_after_half < STOP_LOSS_MIN_REMAIN_KRW:
                            volume = safe_calc_volume(bal, Decimal("1"))
                            if volume > 0:
                                sell_orders.append({
                                    "market": market,
                                    "volume": volume,
                                    "pnl": pnl,
                                    "category": "NEW_STOP_FULL",
                                    "reason": f"NEW_STOP full pnl={pnl}% <= {STOP_TRIGGER_PNL}%",
                                    "state_ref": st,
                                    "portion": Decimal("1")
                                })
                                st["new_stop_stage"] = "done"
                                continue
                        else:
                            if half_volume > 0:
                                portion_used = STOP_LOSS_FIRST_PORTION
                                sell_orders.append({
                                    "market": market,
                                    "volume": half_volume,
                                    "pnl": pnl,
                                    "category": "NEW_STOP_HALF",
                                    "reason": f"NEW_STOP half({portion_used*100:.1f}%) pnl={pnl}% <= {STOP_TRIGGER_PNL}%",
                                    "state_ref": st,
                                    "portion": portion_used
                                })
                                st["new_stop_stage"] = "half_done"
                                continue
                # half_done 이후 추가 손절 없음
            else:
                # (구) 기존 손절 로직 유지가 필요하면 여기에 삽입 (현재 비활성 컨셉)
                pass

            ad_tp_sell = False
            ad_tp_reason = ""
            ad_tp_category = None
            if AVG_DOWN_TP_ENABLED and st.get("avg_down_tp_mode") and not st.get("avg_down_tp_completed"):
                ad_tp_sell, ad_tp_reason, ad_tp_category = decide_avg_down_tp(pnl, st)
                if ad_tp_sell:
                    # 별도 sell_orders 큐에 추가
                    portion = AVG_DOWN_TP_SELL_PORTION
                    volume = safe_calc_volume(bal, portion)
                    if volume > 0:
                        sell_orders.append({
                            "market": market,
                            "volume": volume,
                            "pnl": pnl,
                            "category": ad_tp_category,
                            "reason": ad_tp_reason,
                            "state_ref": st,
                            "portion": portion
                        })
                # 상태 출력 리스트(actions)에 넣어 추적 (평가만 했을 경우)
                actions.append({
                    "market": market,
                    "pnl": pnl,
                    "peak": st["peak_pnl"],
                    "drawdown": st.get("last_drawdown"),
                    "max_dd": st.get("max_drawdown"),
                    "min_pnl": st.get("min_pnl"),
                    "armed": st.get("avg_down_tp_armed") if st.get("avg_down_tp_mode") else st["armed"],
                    "sell": ad_tp_sell,
                    "cat": ad_tp_category,
                    "reason": ad_tp_reason if ad_tp_reason else ("AD_TP_MODE" if st.get("avg_down_tp_mode") else "")
                })
                # 전용 TP가 발동되었으면 일반 decide_sell은 skip
                if ad_tp_sell:
                    continue
                # 전용 모드 활성 중이고 아직 미체결 → 일반 로직과 병행하지 않으려면 여기서 continue
                # 일반 로직도 병행하여 고차 조건을 허용하려면 제거
                # 여기서는 병행 억제:
                continue
            # (C) 일반 익절/트레일
            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market,
                "pnl": pnl,
                "peak": st["peak_pnl"],
                "drawdown": st.get("last_drawdown"),
                "max_dd": st.get("max_drawdown"),
                "min_pnl": st.get("min_pnl"),
                "armed": st["armed"],
                "sell": sell,
                "cat": category,
                "reason": reason
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
                    "market": market, "volume": volume, "pnl": pnl,
                    "category": category, "reason": reason,
                    "state_ref": st, "portion": portion
                })
        # --------------------- 포지션별 처리 끝 ---------------------

        # (D) 매도 실행
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

            # DRY 모드
            if not LIVE_TRADING:
                if category in ("NEW_STOP_FULL","NEW_STOP_HALF"):
                    print(f"[DRY_NEW_STOP] {market} {category} vol={volume} pnl={pnl}% {reason}")
                    if category == "NEW_STOP_FULL":
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    if cur_price:
                        try: realized += cur_price * volume
                        except: pass
                    continue

                limit_used = False
                if category in ("HARD_TP1","HARD_TP2","TRAIL") and USE_LIMIT_SELL_ON_TRAIL:
                    peak_for_tp = st.get("peak_pnl")
                    try:
                        peak_ok = (peak_for_tp is not None and Decimal(str(peak_for_tp)) >= TRAIL_START_PNL)
                    except:
                        peak_ok = False
                    if peak_ok:
                        print(f"[DRY_TP_LIMIT] {market} cat={category} vol={volume} (limit pretend) reason={reason}")
                        if category == "HARD_TP1":
                            st["hard_tp_taken"] = True
                            if portion < 1:
                                ps.reduce_invested_after_sell(market, portion)
                        elif category == "HARD_TP2":
                            st["hard_tp2_taken"] = True
                            if portion >= 1:
                                ps.mark_sold(market)
                            else:
                                ps.reduce_invested_after_sell(market, portion)
                        else:
                            if portion >= 1:
                                ps.mark_sold(market)
                            else:
                                ps.reduce_invested_after_sell(market, portion)
                        limit_used = True
                        if cur_price:
                            try: realized += cur_price * volume
                            except: pass
                if category in ("AD_TP_HARD", "AD_TP_TRAIL"):
                    print(f"[DRY_AD_TP] {market} {category} vol={volume} pnl={pnl}% {reason}")
                    st["avg_down_tp_mode"] = False
                    st["avg_down_tp_completed"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    if cur_price:
                        try:
                            realized += cur_price * volume
                        except:
                            pass
                    continue

                if not limit_used:
                    print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
                    if category == "HARD_TP1":
                        st["hard_tp_taken"] = True
                        if portion < 1:
                            ps.reduce_invested_after_sell(market, portion)
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
                            st["peak_pnl"] = pnl
                            st["armed"] = False
                            ps.reduce_invested_after_sell(market, portion)
                    else:
                        if portion >= 1:
                            ps.mark_sold(market)
                        else:
                            st["peak_pnl"] = pnl
                            st["armed"] = False
                            ps.reduce_invested_after_sell(market, portion)
                    if cur_price and portion > 0:
                        try: realized += cur_price * volume
                        except: pass
                continue

            # LIVE 모드
            try:
                if category in ("NEW_STOP_FULL","NEW_STOP_HALF"):
                    resp = await order_market_sell(access_key, secret_key, market, volume)
                    uid = resp.get("uuid")
                    print(f"[ORDER] NEW_STOP {market} {category} vol={volume} pnl={pnl}% uuid={uid} reason={reason}")
                    if category == "NEW_STOP_FULL":
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    if cur_price:
                        try: realized += cur_price * volume
                        except: pass
                    continue

                use_limit = False
                if category in ("HARD_TP1","HARD_TP2","TRAIL") and USE_LIMIT_SELL_ON_TRAIL:
                    peak_for_tp = st.get("peak_pnl")
                    try:
                        peak_ok = (peak_for_tp is not None and Decimal(str(peak_for_tp)) >= TRAIL_START_PNL)
                    except:
                        peak_ok = False
                    if peak_ok:
                        await place_limit_tp_order(access_key, secret_key, market, volume, category, st)
                        use_limit = True
                if category in ("AD_TP_HARD", "AD_TP_TRAIL"):
                    # 공통 처리: 전용모드 종료
                    st["avg_down_tp_mode"] = False
                    st["avg_down_tp_completed"] = True
                    # 이후 전액매도면 mark_sold, 부분매도면 peak 리셋
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    # 추가로 일반 peak 초기화 (선택)
                    # st["peak_pnl"] = pnl
                    # st["armed"] = False
                    # 이미 처리 후 continue
                    if cur_price and portion > 0:
                        try:
                            realized += cur_price * volume
                        except:
                            pass
                    continue

                if use_limit:
                    continue
                else:
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

        # (E) 매도 후 활성 재집계
        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)

        # (F) 교집합 매수
        if INTERSECTION_BUY_ENABLED:
            try:
                inter_cands, meta = await get_intersection_candidates_safe()
            except Exception as e:
                inter_cands = []
                meta = {"source": "error", "empty_streak": -1}
                print(f"[WARN] 교집합 실패: {e}")

            if meta.get("source") != "empty":
                inter_cands.sort(key=lambda x: x.get("avg_score", 0), reverse=True)
                buys_done = 0
                for row in inter_cands:
                    if buys_done >= INTERSECTION_MAX_BUY_PER_CYCLE:
                        break
                    mkt = row.get("market")
                    sc = row.get("avg_score")
                    if not mkt or sc is None:
                        continue
                    if mkt in EXCLUDED_MARKETS:
                        continue
                    st = ps.data.get(mkt)
                    if STOP_DISABLE_NEW_BUYS and st and st.get("stop_triggered"):
                        continue
                    try:
                        scd = Decimal(str(sc))
                    except:
                        continue
                    if scd < INTERSECTION_MIN_SCORE:
                        continue
                    is_existing = (mkt in active_set)
                    if active_count >= MAX_ACTIVE_MARKETS and ((not is_existing) or (not ALLOW_ADDITIONAL_BUY_WHEN_FULL)):
                        print(f"[CAP] 교집합 매수 제한: {active_count}/{MAX_ACTIVE_MARKETS} mkt={mkt} exist={is_existing}")
                        continue
                    can, msg = ps.can_additional_buy(mkt, INTERSECTION_BUY_KRW,
                                                     MAX_ADDITIONAL_BUYS, MAX_TOTAL_INVEST_PER_MARKET)
                    if not can:
                        print(msg)
                        continue
                    if SKIP_BUY_IF_RECENT_SELL and ps.recently_sold(mkt):
                        continue
                    if ps.recently_bought_intersection(mkt, INTERSECTION_BUY_COOLDOWN_SEC):
                        continue
                    if INTERSECTION_BUY_KRW < MIN_NOTIONAL_KRW:
                        break
                    if available_krw < INTERSECTION_BUY_KRW:
                        break

                    if not LIVE_TRADING:
                        print(f"[DRY_INTERSECTION_BUY] {mkt} score={scd}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        ps.record_buy(mkt, INTERSECTION_BUY_KRW)
                        available_krw -= INTERSECTION_BUY_KRW
                        buys_done += 1
                        if not is_existing:
                            active_count += 1
                            active_set.add(mkt)
                        continue

                    try:
                        resp = await order_market_buy_price(access_key, secret_key, mkt, INTERSECTION_BUY_KRW)
                        uid = resp.get("uuid")
                        print(f"[ORDER] INTERSECTION BUY {mkt} score={scd} KRW={INTERSECTION_BUY_KRW} uuid={uid}")
                        if uid:
                            await asyncio.sleep(1)
                            try:
                                od = await get_order(access_key, secret_key, uid)
                                print(f"[ORDER-CHK] INT BUY {mkt} state={od.get('state')} fee={od.get('paid_fee')}")
                            except Exception as oe:
                                print(f"[WARN] 주문조회 실패 {mkt}: {oe}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        ps.record_buy(mkt, INTERSECTION_BUY_KRW)
                        available_krw -= INTERSECTION_BUY_KRW
                        buys_done += 1
                        if not is_existing:
                            active_count += 1
                            active_set.add(mkt)
                    except Exception as e:
                        print(f"[ERR] 교집합 매수 실패 {mkt}: {e}")

        # (G) 범위 매수
        now_ts = time.time()
        is5m, window_start = is_five_minute_boundary(now_ts)
        if ENABLE_RANGE_BUY and is5m:
            buys_exe = 0
            candidates = []
            for it2 in enriched:
                m = it2.get("market")
                pnl = it2.get("pnl_percent")
                cur = it2.get("current_price")
                if not m or pnl is None or cur is None:
                    continue
                if m in EXCLUDED_MARKETS:
                    continue
                st = ps.data.get(m)
                if STOP_DISABLE_NEW_BUYS and st and st.get("stop_triggered"):
                    continue
                if ps.recently_sold(m):
                    continue
                if ps.bought_this_window(m, window_start):
                    continue
                if BUY_RANGE_LOW <= pnl <= BUY_RANGE_HIGH:
                    candidates.append((pnl, m))

            candidates.sort(key=lambda x: x[0])

            for pnl_val, m in candidates:
                if buys_exe >= MAX_BUY_PER_WINDOW:
                    break
                if available_krw < RANGE_BUY_KRW:
                    break
                if RANGE_BUY_KRW < MIN_NOTIONAL_KRW:
                    break
                is_existing = (m in active_set)
                if active_count >= MAX_ACTIVE_MARKETS and ((not is_existing) or (not ALLOW_ADDITIONAL_BUY_WHEN_FULL)):
                    print(f"[CAP] 범위매수 제한: {active_count}/{MAX_ACTIVE_MARKETS} m={m} exist={is_existing}")
                    continue
                can, msg = ps.can_additional_buy(m, RANGE_BUY_KRW, MAX_ADDITIONAL_BUYS, MAX_TOTAL_INVEST_PER_MARKET)
                if not can:
                    print(msg)
                    continue
                if not LIVE_TRADING:
                    print(f"[DRY_BUY] {m} pnl={pnl_val}% KRW={RANGE_BUY_KRW}")
                    ps.record_buy_window(m, window_start)
                    ps.data.setdefault(m, {})["entry_source"] = "range"
                    ps.record_buy(m, RANGE_BUY_KRW)
                    available_krw -= RANGE_BUY_KRW
                    buys_exe += 1
                    if not is_existing:
                        active_count += 1
                        active_set.add(m)
                    continue
                try:
                    resp = await order_market_buy_price(access_key, secret_key, m, RANGE_BUY_KRW)
                    uid = resp.get("uuid")
                    print(f"[ORDER] RANGE BUY {m} pnl={pnl_val}% KRW={RANGE_BUY_KRW} uuid={uid}")
                    ps.record_buy_window(m, window_start)
                    ps.data.setdefault(m, {})["entry_source"] = "range"
                    ps.record_buy(m, RANGE_BUY_KRW)
                    available_krw -= RANGE_BUY_KRW
                    buys_exe += 1
                    if not is_existing:
                        active_count += 1
                        active_set.add(m)
                except Exception as e:
                    print(f"[ERR] RANGE 매수 실패 {m}: {e}")

        # (H) 액션 요약 로그
        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 상태 요약:")
            for a in actions:
                cat = a["cat"]
                status = "HOLD"
                if a["sell"]:
                    status = "SELL"
                elif a["reason"].startswith("ARMED"):
                    status = "ARM"
                # drawdown / max_dd 출력 추가
                print(
                    f"  {a['market']} pnl={a['pnl']} peak={a['peak']} "
                    f"min={a['min_pnl']} dd={a['drawdown']} maxDD={a['max_dd']} "
                    f"armed={a['armed']} cat={cat} -> {status} {a['reason']}"
                )

        await dynamic_sleep()

# ============================================================
# 22. Controller
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

        except Exception as e:
            print(f"[CTRL] 루프 예외: {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)

# ============================================================
# 23. main
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