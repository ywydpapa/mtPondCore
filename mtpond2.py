# ============================================================
# 1. Imports
# ============================================================
import os
import asyncio
import redis.asyncio as aioredis
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

# Redis 연결 설정
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "56379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

if REDIS_HOST:
    redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        retry_on_timeout=True
    )
else:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_client = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        retry_on_timeout=True
    )

# 주기 리셋 설정
PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "300"))  # 5분
class ResetRequested(Exception):
    pass

# ============================================================
# 4. STATIC CONFIG (설정 및 신규 조건)
# ============================================================
# [조건 2 반영] BB-Patterns 수집 API 및 진입 필터 조건
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))           # 종합 점수 +3 이상
BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "120"))    # 10분당 체결 120건 이상
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "30"))                 # API 조회 캐시 (30초)

# [조건 5 반영] 매도 후 재매수 쿨다운 시간 (초)
COOLDOWN_AFTER_SELL_SEC = int(os.getenv("COOLDOWN_AFTER_SELL_SEC", "600"))   # 10분

# 익절 및 트레일링 설정
ARM_PNL = Decimal("0.25")
HARD_TP = Decimal("0.6")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET
TRAIL_DROP = Decimal("0.15")
TRAIL_START_PNL = Decimal("0.45")

SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "1.0"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "1.0"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))

INTERVAL_SECONDS = 5
MIN_NOTIONAL_KRW = Decimal("5000")
MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
ORDER_NOTIONAL_BUFFER_PCT = Decimal("0.01")
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

# 선지정 TP
PREPLACE_HARD_TP = os.getenv("PREPLACE_HARD_TP", "1") == "1"
PREPLACE_TP_PORTION = os.getenv("PREPLACE_TP_PORTION")
PREPLACE_TP_TIMEOUT_SEC = int(os.getenv("PREPLACE_TP_TIMEOUT_SEC", "0"))
PREPLACE_ON_START = os.getenv("PREPLACE_ON_START", "0") == "1"

# 동시 최대 추가 매수 횟수
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "5"))

# ============================================================
# 5. RUNTIME CONFIG (동적 설정)
# ============================================================
ENABLE_STOP = (os.getenv("ENABLE_STOP", "1").upper() in ("1", "Y", "TRUE"))
STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))

_CONFIG_INITIALIZED = False

class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,
                "min_pnl": pnl,
                "last_drawdown": Decimal("0"),
                "max_drawdown": Decimal("0"),
                "armed": False,
                "avg_buy_price": avg_price,
                "hard_tp_taken": False,
                "hard_tp2_taken": False,
                "last_update_ts": now,
                "active_limit_uuid": None,
                "limit_pending_category": None,
                "limit_submit_ts": None,
                # 추가 매수 제어용 필드
                "last_additional_buy_ts": 0.0,       # 직전 추가매수 시각 (epoch)
                "last_additional_buy_price": None,   # 직전 추가매수 당시 가격
                "last_additional_buy_pnl": None      # 직전 추가매수 당시 PNL
            }
            return self.data[market]

        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl

        drawdown = st["peak_pnl"] - pnl if pnl < st["peak_pnl"] else Decimal("0")
        st["last_drawdown"] = drawdown
        if drawdown > st.get("max_drawdown", Decimal("0")):
            st["max_drawdown"] = drawdown

        st["last_update_ts"] = now
        return st

def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))              # 초기 매수 금액
    ADDITIONAL_BUY_KRW = Decimal(os.getenv("ADDITIONAL_BUY_KRW", "40000"))    # 추가 매수 금액
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))
    BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-2.2"))              # 추가 매수 하락 기준선
    BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.45"))
    STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
    LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))
    ENABLE_STOP = (str(os.getenv("ENABLE_STOP", "1")).upper() in ("1", "Y", "TRUE"))

    _CONFIG_INITIALIZED = True
    print("[INIT] Runtime config 초기화 완료")

def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP

    changes = []

    def to_decimal_safe(v, name):
        try:
            if v is None or str(v).strip() == "":
                return None
            return Decimal(str(v))
        except Exception:
            return None

    if "maxCoincnt" in cfg:
        try:
            nv = int(cfg["maxCoincnt"])
            if nv > 0 and nv != MAX_ACTIVE_MARKETS:
                changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {nv}")
                MAX_ACTIVE_MARKETS = nv
        except:
            pass

    if "initAmt" in cfg:
        val = to_decimal_safe(cfg.get("initAmt"), "initAmt")
        if val and val > 0 and val != RANGE_BUY_KRW:
            changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW} -> {val}")
            RANGE_BUY_KRW = val

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
        if raw is not None and raw != BUY_RANGE_LOW:
            changes.append(f"BUY_RANGE_LOW {BUY_RANGE_LOW} -> {raw}")
            BUY_RANGE_LOW = raw

    if "maxMargin" in cfg:
        raw = to_decimal_safe(cfg.get("maxMargin"), "maxMargin")
        if raw is not None and raw != BUY_RANGE_HIGH:
            changes.append(f"BUY_RANGE_HIGH {BUY_RANGE_HIGH} -> {raw}")
            BUY_RANGE_HIGH = raw

    if "lcRate" in cfg:
        val = to_decimal_safe(cfg.get("lcRate"), "lcRate")
        if val is not None and val != LOSS_CUT_RATE:
            changes.append(f"LOSS_CUT_RATE {LOSS_CUT_RATE} -> {val}")
            LOSS_CUT_RATE = val

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

    if changes:
        print("[CFG] 동적 설정 업데이트:\n  " + "\n  ".join(changes))

# ============================================================
# 6. Exclude Markets (거래 제외 코인 철저 관리)
# ============================================================
async def get_excoins(user_no: int) -> list[tuple] | None:
    async with SessionLocal() as session:
        sql = text("""SELECT DISTINCT market FROM exCoinlist WHERE userNo = :u AND attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"u": user_no, "attr": "%XXX%"})
        return result.fetchall()

def _env_excluded_set() -> set[str]:
    raw = os.getenv("EXCLUDE_MARKETS", "") or ""
    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    return set(parts)

async def parse_exclude_markets() -> set[str]:
    try:
        uno = int(os.getenv("USER_NO", "0"))
    except Exception:
        uno = 0

    try:
        redis_key = f"mtpond:excoins:{uno}"
        cached_coins = await redis_client.smembers(redis_key)
        if cached_coins:
            return set(cached_coins)
    except Exception as e:
        pass

    try:
        rows = await get_excoins(uno)
        if rows:
            ex_set = {str(r[0]).strip() for r in rows if r and r[0]}
            try:
                if ex_set:
                    await redis_client.sadd(f"mtpond:excoins:{uno}", *ex_set)
                    await redis_client.expire(f"mtpond:excoins:{uno}", 600)
            except Exception:
                pass
            return ex_set
    except Exception as e:
        pass

    return _env_excluded_set()

EXCLUDED_MARKETS: set[str] = set()
_excluded_last_update: float = 0
EXCLUDED_CACHE_TTL_SEC = 60

async def get_excluded_markets(force_reload: bool = False) -> set[str]:
    global EXCLUDED_MARKETS, _excluded_last_update
    now = time.time()
    if (not force_reload) and EXCLUDED_MARKETS and (now - _excluded_last_update) < EXCLUDED_CACHE_TTL_SEC:
        return EXCLUDED_MARKETS
    new_excluded = await parse_exclude_markets()
    if EXCLUDED_MARKETS != new_excluded:
        print(f"[EXC] 거래 제외 목록 갱신 완료 ({len(new_excluded)}개): {sorted(new_excluded)}")
    EXCLUDED_MARKETS = new_excluded
    _excluded_last_update = now
    return EXCLUDED_MARKETS

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

async def upbit_request(method: str, url: str, access_key: str, secret_key: str, params: dict = None, max_retry: int = 3) -> dict:
    params = params or {}
    backoff = 1.0
    for attempt in range(1, max_retry + 1):
        token = build_upbit_jwt_with_params(access_key, secret_key, params) if (params or method.upper() != "GET") else build_upbit_jwt_simple(access_key, secret_key)
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

            if resp.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} body={resp.text}")
            return resp.json()
        except Exception as e:
            if attempt >= max_retry:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8)

async def fetch_upbit_accounts(access_key: str, secret_key: str) -> List[Dict[str, Any]]:
    url = "https://api.upbit.com/v1/accounts"
    token = build_upbit_jwt_simple(access_key, secret_key)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets:
        return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(markets)}
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
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

def quantize_volume(vol: Decimal) -> Decimal:
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

# ============================================================
# 8. 호가 및 틱 단위 보정
# ============================================================
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
    return Decimal("0.00000001")

def adjust_price_to_tick(raw_price: Decimal, side: str = "ask") -> Decimal:
    price = Decimal(str(raw_price))
    if price <= 0:
        return Decimal("0")
    unit = get_tick_unit(price)
    steps = price / unit
    mode = ROUND_CEILING if side == "ask" else ROUND_FLOOR
    steps_i = steps.to_integral_value(rounding=mode)
    adjusted = steps_i * unit
    return adjusted.quantize(Decimal("1") if unit >= 1 else unit)

async def get_orderbook_top(market: str):
    url = "https://api.upbit.com/v1/orderbook"
    params = {"markets": market}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    bids = data[0].get("orderbook_units", [])
    return Decimal(str(bids[0]["bid_price"])), Decimal(str(bids[0]["ask_price"]))

# 거래 가능 마켓 캐시 (1시간 주기 갱신)
_TRADABLE_MARKETS_SET: set[str] = set()
_TRADABLE_MARKETS_TS: float = 0.0

async def get_upbit_tradable_markets(force: bool = False) -> set[str]:
    """업비트에서 현재 실제 거래 지원 중인 전체 마켓 코드 조회"""
    global _TRADABLE_MARKETS_SET, _TRADABLE_MARKETS_TS
    now = time.time()
    if (not force) and _TRADABLE_MARKETS_SET and (now - _TRADABLE_MARKETS_TS < 3600):
        return _TRADABLE_MARKETS_SET

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("https://api.upbit.com/v1/market/all?isDetails=false")
            if resp.status_code == 200:
                data = resp.json()
                _TRADABLE_MARKETS_SET = {m["market"] for m in data if "market" in m}
                _TRADABLE_MARKETS_TS = now
    except Exception as e:
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패 (기존 캐시 유지): {e}")

    return _TRADABLE_MARKETS_SET

async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    """
    업비트 시세 조회 함수
    - 존재하지 않는 마켓으로 인한 404 원천 차단
    - 실패 시 개별 조회를 통해 유효한 코인 시세만 복구
    """
    if not markets:
        return {}

    # 1. 실제 업비트에서 거래 지원 중인 마켓만 1차 필터링
    tradable = await get_upbit_tradable_markets()
    if tradable:
        valid_markets = [m for m in markets if m in tradable]
        orphan_markets = [m for m in markets if m not in tradable]
        if orphan_markets:
            print(f"[TICKER][SKIP] 거래 불가/상폐 마켓 제외: {orphan_markets}")
    else:
        valid_markets = markets

    if not valid_markets:
        return {}

    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(valid_markets)}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                raw = r.json()
                out = {}
                for item in raw:
                    m = item.get("market")
                    p = item.get("trade_price")
                    if m and p is not None:
                        try:
                            out[m] = Decimal(str(p))
                        except Exception:
                            pass
                return out
            else:
                raise RuntimeError(f"HTTP {r.status_code}")
    except Exception:
        # 2. 404 등 일괄 조회 실패 시 개별 호출로 유효 코인만 건져냄 (Fallback)
        price_map = {}
        async with httpx.AsyncClient(timeout=3.0) as fallback_client:
            for m in valid_markets:
                try:
                    res = await fallback_client.get(url, params={"markets": m})
                    if res.status_code == 200:
                        items = res.json()
                        if items and items[0].get("trade_price") is not None:
                            price_map[m] = Decimal(str(items[0]["trade_price"]))
                    elif res.status_code == 404:
                        print(f"[TICKER][404] 업비트에 존재하지 않는 심볼 확인 및 영구 제외: {m}")
                        # 404 확인된 심볼은 런타임 캐시에서 제거
                        if m in _TRADABLE_MARKETS_SET:
                            _TRADABLE_MARKETS_SET.discard(m)
                except Exception:
                    pass
        return price_map

# ============================================================
# 9. [조건 2 반영] BB-Patterns 기반 신규 매수 후보 발굴 엔진
# ============================================================
BB_CACHE_TTL_SEC = 600  # 10분 (600초)으로 변경하여 백엔드 갱신 주기와 동기화

_bb_patterns_cache: Dict[str, Any] = {
    "fetched_ts": 0.0,
    "last_server_updated": None,
    "candidates": []
}


async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    """
    becog.iptime.org:8088/api/bb-patterns 호출 (10분 주기 갱신)
    - 백엔드 수집 중(status == 'processing') 대응
    - 점수(>=3), 10분 체결 건수(>=120건), 제외코인 필터링
    """
    now = time.time()

    # 10분(600초)이 지나지 않았고 강제 갱신이 아니면 기존 캐시 반환
    if (not force) and (now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC) and _bb_patterns_cache["candidates"]:
        return _bb_patterns_cache["candidates"]

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(BB_PATTERNS_API_URL)
            if resp.status_code == 202:
                print("[BB-PATTERNS] 백엔드 서버가 캔들 지표를 수집/계산 중입니다 (202). 기존 캐시를 유지합니다.")
                return _bb_patterns_cache["candidates"]
            elif resp.status_code != 200:
                print(f"[BB-PATTERNS][WARN] 상태코드 이상: {resp.status_code}")
                return _bb_patterns_cache["candidates"]

            data = resp.json()
    except Exception as e:
        print(f"[BB-PATTERNS][ERR] API 호출 실패: {e}")
        return _bb_patterns_cache["candidates"]

    # 서버 상태가 ok가 아니거나 초기 상태인 경우 방어
    server_status = data.get("status")
    if server_status == "processing":
        print("[BB-PATTERNS] 서버 계산 중. 이전 후보 유지.")
        return _bb_patterns_cache["candidates"]

    coins = data.get("coins", [])
    if not isinstance(coins, list) or len(coins) == 0:
        print(f"[BB-PATTERNS] 응답에 코인 목록이 비어있습니다. (server_status: {server_status})")
        return _bb_patterns_cache["candidates"]

    server_updated = data.get("updated")
    excluded = await get_excluded_markets()
    qualified = []

    # 디버깅 카운터
    cnt_total = len(coins)
    cnt_excluded = 0
    cnt_score_drop = 0
    cnt_trade_drop = 0
    cnt_price_drop = 0

    for c in coins:
        market = c.get("market", "")
        if not market.startswith("KRW-"):
            continue

        # 1. 제외 코인 필터
        if market in excluded:
            cnt_excluded += 1
            continue

        # 2. 가격 검사 (50원 미만 동전주 제외 방어)
        try:
            price = float(c.get("price", 0))
        except:
            price = 0
        if price < 50:
            cnt_price_drop += 1
            continue

        # 3. 종합 점수 검증 (문자열/숫자 안전 파싱)
        try:
            overall_score = int(c.get("overall_score", 0))
        except:
            overall_score = 0
        if overall_score < BB_MIN_OVERALL_SCORE:
            cnt_score_drop += 1
            continue

        # 4. 10분 체결 건수 검증 (문자열/숫자 안전 파싱)
        act = c.get("trade_activity") or {}
        try:
            count_10m = int(act.get("trade_count_10m", 0))
        except:
            count_10m = 0

        if count_10m < BB_MIN_TRADE_COUNT_10M:
            cnt_trade_drop += 1
            continue

        qualified.append({
            "market": market,
            "price": Decimal(str(price)),
            "overall_score": overall_score,
            "trade_count_10m": count_10m,
            "trade_count_1h": act.get("trade_count_1h", 0),
            "recommendation": c.get("recommendation")
        })

    # 종합 점수 내림차순(1순위), 10분 체결 건수 내림차순(2순위) 정렬
    qualified.sort(key=lambda x: (x["overall_score"], x["trade_count_10m"]), reverse=True)

    _bb_patterns_cache["fetched_ts"] = now
    _bb_patterns_cache["last_server_updated"] = server_updated
    _bb_patterns_cache["candidates"] = qualified

    # 후보 선정 결과 및 탈락 원인 상세 로그 출력
    print(
        f"\n[BB-PATTERNS 갱신] 서버기준 시각: {server_updated}\n"
        f"  - 전체 수신: {cnt_total}개\n"
        f"  - 제외 코인 탈락: {cnt_excluded}개\n"
        f"  - 100원 미만 탈락: {cnt_price_drop}개\n"
        f"  - 점수 미달(<{BB_MIN_OVERALL_SCORE}): {cnt_score_drop}개\n"
        f"  - 체결 미달(<{BB_MIN_TRADE_COUNT_10M}건): {cnt_trade_drop}개\n"
        f"  ▶ 최종 적합 후보: {len(qualified)}개 선택됨"
    )
    if qualified:
        for q in qualified[:5]:
            print(
                f"    * {q['market']} | 점수: {q['overall_score']}점 | 10분체결: {q['trade_count_10m']}건 | 현재가: {q['price']}원")
    print()

    return qualified

# ============================================================
# 10. 포지션 상태 관리 클래스 (쿨다운 및 매수 이력 보강)
# ============================================================
class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}   # [조건 5] 매도 시각 추적
        self.buy_info: Dict[str, Dict[str, Any]] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,
                "min_pnl": pnl,
                "last_drawdown": Decimal("0"),
                "max_drawdown": Decimal("0"),
                "armed": False,
                "avg_buy_price": avg_price,
                "hard_tp_taken": False,
                "hard_tp2_taken": False,
                "last_update_ts": now,
                "active_limit_uuid": None,
                "limit_pending_category": None,
                "limit_submit_ts": None,
                "last_additional_buy_pnl": None  # 직전 추가매수 당시 PNL
            }
            return self.data[market]

        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl

        drawdown = st["peak_pnl"] - pnl if pnl < st["peak_pnl"] else Decimal("0")
        st["last_drawdown"] = drawdown
        if drawdown > st.get("max_drawdown", Decimal("0")):
            st["max_drawdown"] = drawdown

        st["last_update_ts"] = now
        return st

    def mark_sold(self, market: str):
        """[조건 5 반영] 매도 후 재매수 쿨다운 타이머 시작"""
        self.last_sell_time[market] = time.time()
        self.data.pop(market, None)
        self.buy_info.pop(market, None)
        print(f"[COOLDOWN] {market} 매도 완료 → 재매수 금지 쿨다운 시작 ({COOLDOWN_AFTER_SELL_SEC}초)")

    def recently_sold(self, market: str, cooldown_sec: int = COOLDOWN_AFTER_SELL_SEC) -> bool:
        """[조건 5 반영] 매도 후 10분 이내 여부 확인"""
        ts = self.last_sell_time.get(market)
        if not ts:
            return False
        elapsed = time.time() - ts
        return elapsed < cooldown_sec

    def record_buy(self, market: str, krw_amount: Decimal):
        info = self.buy_info.setdefault(market, {"total_buys": 0, "total_invested": Decimal("0")})
        info["total_buys"] += 1
        info["total_invested"] += krw_amount

    def get_buy_stats(self, market: str) -> Tuple[int, Decimal]:
        info = self.buy_info.get(market)
        if not info:
            return 0, Decimal("0")
        return info["total_buys"], info["total_invested"]

    def reduce_invested_after_sell(self, market: str, portion: Decimal):
        info = self.buy_info.get(market)
        if not info or portion <= 0 or portion >= 1:
            return
        info["total_invested"] = (info["total_invested"] * (Decimal("1") - portion)).quantize(Decimal("0.0001"))

# ============================================================
# 11. 주문 실행 도우미
# ============================================================
async def order_market_buy_price(access_key: str, secret_key: str, market: str, krw_amount: Decimal):
    params = {"market": market, "side": "bid", "price": str(krw_amount), "ord_type": "price"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_market_sell(access_key: str, secret_key: str, market: str, volume: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "ord_type": "market"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_limit_sell(access_key: str, secret_key: str, market: str, volume: Decimal, price: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "price": str(price), "ord_type": "limit"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def cancel_order(access_key: str, secret_key: str, uuid_: str):
    params = {"uuid": uuid_}
    try:
        return await upbit_request("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, params=params, max_retry=2)
    except Exception as e:
        print(f"[CANCEL] 주문 취소 실패 ({uuid_}): {e}")

async def cancel_all_open_orders_for_market(access_key: str, secret_key: str, market: str):
    try:
        orders = await upbit_request("GET", "https://api.upbit.com/v1/orders", access_key, secret_key, params={"market": market, "state": "wait"})
        if isinstance(orders, list):
            for od in orders:
                uid = od.get("uuid")
                if uid:
                    await cancel_order(access_key, secret_key, uid)
    except Exception as e:
        print(f"[CANCEL-ALL][ERR] {market} 주문 전체 취소 실패: {e}")

# ============================================================
# 12. 익절 및 손절 판단 엔진
# ============================================================
def decide_sell(market: str, pnl: Decimal, state: dict) -> Tuple[bool, str, Optional[str]]:
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)

    # 1. 2차 하드 익절 (+1.1%)
    if hard_tp_taken and (not hard_tp2_taken) and pnl >= HARD_TP2_BASE:
        return True, f"HARD_TP2 {pnl}% >= {HARD_TP2_BASE}%", "HARD_TP2"

    # 2. 1차 하드 익절 (+0.6%)
    if (not hard_tp_taken) and pnl >= HARD_TP:
        return True, f"HARD_TP1 {pnl}% >= {HARD_TP}%", "HARD_TP1"

    # 3. 트레일링 스탑 암(Arming)
    if (not armed) and pnl >= ARM_PNL:
        state["armed"] = True
        return False, f"ARMED {pnl}% (>= {ARM_PNL}%)", None

    # 4. 트레일링 하락 청산
    if state["armed"] and peak >= TRAIL_START_PNL:
        drop = peak - pnl
        if drop >= TRAIL_DROP:
            return True, f"TRAIL_DROP {drop}% >= {TRAIL_DROP}% (peak={peak}% now={pnl}%)", "TRAIL"

    return False, "", None

# ============================================================
# 13. [조건 3 & 4 반영] 추가 매수 프로세스
# ============================================================
ADDITIONAL_BUY_COOLDOWN_SEC = int(os.getenv("ADDITIONAL_BUY_COOLDOWN_SEC", "120"))  # 최소 2분(120초)


async def process_additional_buys(
        access_key: str,
        secret_key: str,
        enriched_positions: List[dict],
        ps: PositionState,
        available_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if available_krw < MIN_ORDER_NOTIONAL_KRW or ADDITIONAL_BUY_KRW <= 0:
        return used_krw

    now = time.time()

    try:
        max_cap = Decimal(str(MAX_TOTAL_INVEST_PER_MARKET))
    except Exception:
        max_cap = Decimal("0")

    # 기준 하락폭 안전 변환 (기본값 -2.2)
    try:
        drop_target = Decimal(str(BUY_RANGE_LOW))
        if drop_target > 0:
            drop_target = -abs(drop_target)  # 양수로 잘못 입력된 경우 음수로 강제 변환
    except Exception:
        drop_target = Decimal("-2.2")

    excluded = await get_excluded_markets()

    for pos in enriched_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        cur_price = pos.get("current_price")

        # 1. 제외 코인 및 유효성 검사
        if market in excluded or pnl is None or cur_price is None:
            continue

        # ★ [가드 1] 하락폭 검증 강화 (양수 수익권 및 기준 미달 즉시 탈락)
        # pnl이 drop_target(-2.2%)보다 크면 무조건 스킵
        if pnl >= Decimal("0") or pnl > drop_target:
            continue

        total_buys, total_invested = ps.get_buy_stats(market)

        # 지갑 잔고 기준 투자금 보정
        if total_invested <= 0:
            bal_eff = (pos["balance"] + pos["locked"])
            total_invested = (bal_eff * pos["avg_buy_price"]).quantize(Decimal("0.0001"))
            ps.buy_info[market] = {"total_buys": max(1, total_buys), "total_invested": total_invested}

        # 2. 최대 구매한도 초과 방지
        if max_cap > 0 and total_invested >= max_cap:
            continue

        # 3. 최대 추가 매수 횟수 초과 방지
        if total_buys - 1 >= MAX_ADDITIONAL_BUYS:
            continue

        st = ps.data.get(market, {})
        last_add_ts = st.get("last_additional_buy_ts")
        last_add_price = st.get("last_additional_buy_price")
        last_add_pnl = st.get("last_additional_buy_pnl")

        # ★ [가드 2] 프로그램 시작 직후 혹은 직전 매수 후 최소 2분(120초) 쿨다운
        if last_add_ts is None:
            st["last_additional_buy_ts"] = now
            continue

        elapsed_sec = now - last_add_ts
        if elapsed_sec < ADDITIONAL_BUY_COOLDOWN_SEC:
            # 2분이 지나지 않았으면 절대 매수하지 않음
            continue

        # ★ [가드 3] 가격 변화 검증 (동일 가격 매수 차단)
        if last_add_price is not None:
            if cur_price == last_add_price:
                continue

            tick_unit = get_tick_unit(cur_price)
            if abs(cur_price - last_add_price) < tick_unit:
                continue

        # ★ [가드 4] 직전 추가매수 PNL 대비 추가 낙폭(-0.8% 이상) 검증
        if last_add_pnl is not None and (pnl - last_add_pnl) > Decimal("-0.8"):
            continue

        # 4. 잔여 한도 계산
        cap_remain = (max_cap - total_invested) if max_cap > 0 else ADDITIONAL_BUY_KRW
        if cap_remain < MIN_ORDER_NOTIONAL_KRW:
            continue

        buy_amt = min(ADDITIONAL_BUY_KRW, cap_remain, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        if max_cap > 0 and (total_invested + buy_amt) > max_cap:
            buy_amt = max_cap - total_invested
            if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                continue

        # 5. 추가 매수 실행
        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(
                    f"[BUY-ADD] {market} 추가 매수 완료! pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원, 경과:{int(elapsed_sec)}초) amt={buy_amt} | 누적: {total_invested + buy_amt}/{max_cap}원 (uuid={resp.get('uuid')})")
            else:
                print(
                    f"[BUY-ADD][DRY] {market} 추가 매수: pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원) amt={buy_amt}")

            # 매수 상태 갱신
            ps.record_buy(market, buy_amt)
            st["last_additional_buy_ts"] = now
            st["last_additional_buy_price"] = cur_price
            st["last_additional_buy_pnl"] = pnl

            available_krw -= buy_amt
            used_krw += buy_amt

            if PREPLACE_HARD_TP and LIVE_TRADING:
                asyncio.create_task(after_buy_preplace_tp(access_key, secret_key, market, ps))

        except Exception as e:
            print(f"[BUY-ADD][ERR] {market} 추가 매수 실패: {e}")

    return used_krw

# ============================================================
# 14. [조건 2, 3, 5 반영] 신규 종목 매수 프로세스
# ============================================================
async def process_new_candidate_buys(
    access_key: str,
    secret_key: str,
    ps: PositionState,
    remaining_slots: int,
    available_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if remaining_slots <= 0 or available_krw < MIN_ORDER_NOTIONAL_KRW or RANGE_BUY_KRW <= 0:
        return used_krw

    # 최대 한도 설정값 안전 변환
    try:
        max_cap = Decimal(str(MAX_TOTAL_INVEST_PER_MARKET))
    except Exception:
        max_cap = Decimal("0")

    candidates = await fetch_bb_patterns_candidates()
    if not candidates:
        return used_krw

    excluded = await get_excluded_markets()
    buys_done = 0

    for item in candidates:
        if buys_done >= remaining_slots:
            break

        market = item["market"]

        # 제외 코인 원천 배제
        if market in excluded:
            continue

        # 이미 보유 중인 종목은 신규 매수 루프에서 건너뜀
        if ps.buy_info.get(market):
            continue

        # 10분 매도 쿨다운 확인
        if ps.recently_sold(market):
            continue

        # ★ 초기 매수금액도 MAX_TOTAL_INVEST_PER_MARKET을 초과할 수 없도록 방어
        target_amt = RANGE_BUY_KRW
        if max_cap > 0 and target_amt > max_cap:
            target_amt = max_cap

        buy_amt = min(target_amt, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(f"[BUY-NEW] {market} 신규 진입! 점수={item['overall_score']} 10분체결={item['trade_count_10m']}건 amt={buy_amt} (uuid={resp.get('uuid')})")
            else:
                print(f"[BUY-NEW][DRY] {market} 신규 진입: 점수={item['overall_score']} amt={buy_amt}")

            ps.record_buy(market, buy_amt)
            available_krw -= buy_amt
            used_krw += buy_amt
            buys_done += 1

            if PREPLACE_HARD_TP and LIVE_TRADING:
                asyncio.create_task(after_buy_preplace_tp(access_key, secret_key, market, ps))

        except Exception as e:
            print(f"[BUY-NEW][ERR] {market} 신규 매수 실패: {e}")

    return used_krw

# ============================================================
# 15. 매수 후 선지정 지정가 익절 주문 보조
# ============================================================
async def after_buy_preplace_tp(access_key: str, secret_key: str, market: str, ps: PositionState):
    await asyncio.sleep(0.5)
    try:
        accounts = await fetch_upbit_accounts(access_key, secret_key)
        base = market.split("-")[1]
        for acc in accounts:
            if acc.get("currency") == base:
                bal = Decimal(str(acc.get("balance", "0")))
                avg = Decimal(str(acc.get("avg_buy_price", "0")))
                if bal <= 0 or avg <= 0:
                    return

                # 기존 미체결 매도 취소
                await cancel_all_open_orders_for_market(access_key, secret_key, market)

                # 목표가 계산 (+0.6%)
                target_price = avg * (Decimal("1") + (HARD_TP / Decimal("100")))
                best_bid, best_ask = await get_orderbook_top(market)
                limit_price = adjust_price_to_tick(max(target_price, best_ask), side="ask")

                vol = quantize_volume(bal)
                if vol * limit_price >= MIN_ORDER_NOTIONAL_KRW:
                    resp = await order_limit_sell(access_key, secret_key, market, vol, limit_price)
                    st = ps.data.setdefault(market, {})
                    st["active_limit_uuid"] = resp.get("uuid")
                    st["limit_pending_category"] = "PRE_HARD_TP1"
                    st["limit_submit_ts"] = time.time()
                    print(f"[PRE_TP] {market} 선지정 익절 주문 완료: vol={vol} price={limit_price}")
                break
    except Exception as e:
        print(f"[PRE_TP][ERR] {market} 선지정 주문 실패: {e}")

# ============================================================
# 16. 메인 모니터링 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()

    # DB API 키 획득
    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        row = result.fetchone()
        if not row:
            print("[ERR] API Key를 찾을 수 없습니다.")
            return
        access_key, secret_key = row[0], row[1]

    ps = PositionState()
    print("=== 자동매매 모니터링 엔진 가동 시작 ===")

    # 1. 초기 제외 코인 로드
    excluded = await get_excluded_markets(force_reload=True)

    # 2. 초기 보유 잔고 동기화 (제외 코인 원천 배제)
    try:
        init_accs = await fetch_upbit_accounts(access_key, secret_key)
        boot_now = time.time()
        for acc in init_accs:
            cur = acc.get("currency")
            if not cur or cur == "KRW":
                continue
            market = f"KRW-{cur}"

            if market in excluded:
                continue

            bal = Decimal(str(acc.get("balance", "0"))) + Decimal(str(acc.get("locked", "0")))
            avg = Decimal(str(acc.get("avg_buy_price", "0")))
            if bal > 0 and avg > 0:
                est = (bal * avg).quantize(Decimal("0.0001"))
                ps.buy_info[market] = {"total_buys": 1, "total_invested": est}
                ps.data[market] = {
                    "peak_pnl": Decimal("0"),
                    "min_pnl": Decimal("0"),
                    "armed": False,
                    "avg_buy_price": avg,
                    # ★ [핵심] 시작 시점을 기준으로 쿨다운 타이머 시작 (17억초 오류 방지)
                    "last_additional_buy_ts": boot_now,
                    "last_additional_buy_price": avg,
                    "last_additional_buy_pnl": Decimal("0")
                }
        print(f"[INIT] 기존 보유 코인 {len(ps.buy_info)}개 관리 등록 완료 (2분 매수 쿨다운 적용)")
    except Exception as e:
        print(f"[INIT][WARN] 잔고 초기화 경고: {e}")

    # ================= 루프 진입 =================
    while True:
        try:
            # 1) 제외 코인 목록 주기적 갱신
            excluded = await get_excluded_markets()

            # 2) 업비트 잔고 조회
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)

            # 가용 KRW 계산
            available_krw = Decimal("0")
            for a in raw_accounts:
                if a.get("currency") == "KRW":
                    available_krw = Decimal(str(a.get("balance", "0")))
                    break

            # 3) 보유 코인 목록 구성 (제외 코인 완벽 차단)
            held_markets = []
            for a in raw_accounts:
                cur = a.get("currency")
                if cur and cur != "KRW":
                    mkt = f"KRW-{cur}"
                    if mkt not in excluded:  # [조건 1] 제외 코인은 시세 조회 및 관리 대상에서 제외
                        bal = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                        if bal > 0:
                            held_markets.append(mkt)

            price_map = await fetch_current_prices(held_markets)

            # 4) 포지션 정보 풍부화
            enriched = []
            for a in raw_accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW":
                    continue
                market = f"KRW-{cur}"

                if market in excluded:  # 제외 코인은 검토 프로세스 전면 생략
                    continue

                bal = Decimal(str(a.get("balance", "0")))
                locked = Decimal(str(a.get("locked", "0")))
                avg = Decimal(str(a.get("avg_buy_price", "0")))
                cur_price = price_map.get(market)

                if cur_price and avg > 0 and (bal + locked) > 0:
                    pnl = ((cur_price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                    enriched.append({
                        "market": market,
                        "balance": bal,
                        "locked": locked,
                        "avg_buy_price": avg,
                        "current_price": cur_price,
                        "pnl_percent": pnl
                    })

            # 활성 포지션 개수 확인
            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            # ================= [A] 매도 판단 및 실행 =================
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                bal = pos["balance"]
                st = ps.update_or_init(market, pnl, pos["avg_buy_price"])

                # 손절 조건 검증 (LOSS_CUT_RATE)
                if ENABLE_STOP and pnl <= LOSS_CUT_RATE:
                    print(f"[LOSS-CUT] {market} 손절 조건 도달 (PNL: {pnl}% <= {LOSS_CUT_RATE}%)")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.3)

                    # 잔고 재확인 후 전량 매도
                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            cur_bal = Decimal(str(a.get("balance", "0")))
                            if cur_bal * pos["current_price"] >= MIN_ORDER_NOTIONAL_KRW:
                                await order_market_sell(access_key, secret_key, market, quantize_volume(cur_bal))
                                ps.mark_sold(market)
                            break
                    continue

                # 익절 및 트레일링 판단
                should_sell, reason, cat = decide_sell(market, pnl, st)
                if should_sell:
                    print(f"[SELL-SIGNAL] {market} {cat} 매도 조건 달성: {reason}")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.2)

                    portion = HARD_TP_SELL_PORTION if cat == "HARD_TP1" else SELL_PORTION
                    sell_vol = quantize_volume(bal * portion)

                    if sell_vol * pos["current_price"] >= MIN_ORDER_NOTIONAL_KRW:
                        if LIVE_TRADING:
                            await order_market_sell(access_key, secret_key, market, sell_vol)
                        if portion >= Decimal("1.0"):
                            ps.mark_sold(market)
                        else:
                            ps.reduce_invested_after_sell(market, portion)

            # ================= [B] 추가 매수 실행 =================
            used_krw_add = await process_additional_buys(access_key, secret_key, enriched, ps, available_krw)
            available_krw -= used_krw_add

            # ================= [C] 신규 종목 매수 실행 =================
            used_krw_new = await process_new_candidate_buys(access_key, secret_key, ps, remaining_slots, available_krw)
            available_krw -= used_krw_new

            # 주기 리셋 점검
            if PERIODIC_RESET_SEC > 0 and (time.time() - start_ts) >= PERIODIC_RESET_SEC:
                print(f"[RESET] {PERIODIC_RESET_SEC}초 경과로 안전 주기 재시작 요청")
                raise ResetRequested()

        except ResetRequested:
            raise
        except Exception as e:
            print(f"[MONITOR][LOOP-ERR] {e}")

        await asyncio.sleep(INTERVAL_SECONDS)

# ============================================================
# 17. 컨트롤러 (동적 설정 반영 및 프로세스 수명 관리)
# ============================================================
_last_cfg_signature = None

async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    global _last_cfg_signature
    print(f"[CTRL] Controller 가동: User={user_no}, Server={server_no}")

    async def update_config_once():
        nonlocal task
        global _last_cfg_signature

        # Redis 캐시 설정 조회
        cfg = None
        try:
            cached_str = await redis_client.get(f"mtpond:setup:{user_no}")
            if cached_str:
                data = json.loads(cached_str)
                cfg = data[0] if isinstance(data, list) and data else data
        except Exception:
            pass

        if not cfg and API_BASE:
            try:
                async with httpx.AsyncClient(timeout=4.0) as cli:
                    r = await cli.get(f"{API_BASE}/api/mtpondsetup/{user_no}")
                    if r.status_code == 200:
                        d = r.json()
                        cfg = d[0] if isinstance(d, list) and d else d
            except:
                pass

        if not cfg:
            return

        active_flag = str(cfg.get("activeYN", "")).upper() == "Y"
        sig = tuple(sorted([(k, str(v)) for k, v in cfg.items() if k != "activeYN"]))

        if sig != _last_cfg_signature:
            apply_dynamic_config(cfg)
            _last_cfg_signature = sig

        if task and task.done():
            exc = task.exception()
            if isinstance(exc, ResetRequested):
                print("[CTRL] 주기 리셋 요청 수신 → 모니터링 정상 재기동")
            elif exc:
                print(f"[CTRL] 모니터링 비정상 종료 감지: {exc}")
            task = None

        if active_flag and task is None:
            print("[CTRL] activeYN=Y → 모니터링 태스크 시작")
            task = asyncio.create_task(monitor_positions(user_no, server_no))
        elif not active_flag and task:
            print("[CTRL] activeYN=N → 모니터링 태스크 중단")
            task.cancel()
            try:
                await task
            except:
                pass
            task = None

    # 초기 1회 실행
    await update_config_once()

    # 주기적 폴링 루프
    while True:
        try:
            await update_config_once()
        except Exception as e:
            print(f"[CTRL][ERR] 컨트롤러 주기 점검 예외: {e}")
        await asyncio.sleep(CONTROLLER_POLL_SEC)

# ============================================================
# 18. 진입점
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
        print("\n[MAIN] 종료 요청 수신")