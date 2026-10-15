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

# 주기 리셋 설정 (5분)
PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "300"))
class ResetRequested(Exception):
    pass

# ============================================================
# 4. STATIC CONFIG (설정 및 신규 조건)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "600"))                 # 10분 주기 갱신
COOLDOWN_AFTER_SELL_SEC = int(os.getenv("COOLDOWN_AFTER_SELL_SEC", "600"))   # 매도 후 10분 쿨다운

# 동적 조정 가능한 추천 필터 파라미터 기본값
BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))           # 기본 추천 점수 3점 이상
BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "120"))    # 기본 10분당 120건 (분당 12건 상당)

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
# 5. RUNTIME CONFIG (동적 설정 관리)
# ============================================================
ENABLE_STOP = (os.getenv("ENABLE_STOP", "1").upper() in ("1", "Y", "TRUE"))
STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))

_CONFIG_INITIALIZED = False

# 포지션 상태 추적 단일 클래스 정의
class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}   # 매도 후 쿨다운용
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
                # 추가 매수 방어 제어용 필드
                "last_additional_buy_ts": now,       # 시작 시점 기준으로 초기화
                "last_additional_buy_price": avg_price,
                "last_additional_buy_pnl": Decimal("0")
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
        self.last_sell_time[market] = time.time()
        self.data.pop(market, None)
        self.buy_info.pop(market, None)
        print(f"[COOLDOWN] {market} 매도 완료 → 재매수 금지 쿨다운 시작 ({COOLDOWN_AFTER_SELL_SEC}초)")

    def recently_sold(self, market: str, cooldown_sec: int = COOLDOWN_AFTER_SELL_SEC) -> bool:
        ts = self.last_sell_time.get(market)
        if not ts:
            return False
        return (time.time() - ts) < cooldown_sec

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

def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))              # 초기 매수 금액
    ADDITIONAL_BUY_KRW = Decimal(os.getenv("ADDITIONAL_BUY_KRW", "40000"))    # 추가 매수 금액
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))
    BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-2.2"))              # 추가 매수 하락 기준선
    BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.45"))
    STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
    LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))
    ENABLE_STOP = (str(os.getenv("ENABLE_STOP", "1")).upper() in ("1", "Y", "TRUE"))
    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "120"))

    _CONFIG_INITIALIZED = True
    print(f"[INIT] 설정 초기화 완료 (force={force}) | 추천점수>={BB_MIN_OVERALL_SCORE}, 10분체결>={BB_MIN_TRADE_COUNT_10M}건")

def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    changes = []

    def to_decimal_safe(v):
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
        val = to_decimal_safe(cfg.get("initAmt"))
        if val and val > 0 and val != RANGE_BUY_KRW:
            changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW} -> {val}")
            RANGE_BUY_KRW = val

    if "addAmt" in cfg:
        val = to_decimal_safe(cfg.get("addAmt"))
        if val and val > 0 and val != ADDITIONAL_BUY_KRW:
            changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW} -> {val}")
            ADDITIONAL_BUY_KRW = val

    if "limitAmt" in cfg:
        val = to_decimal_safe(cfg.get("limitAmt"))
        if val and val > 0 and val != MAX_TOTAL_INVEST_PER_MARKET:
            changes.append(f"MAX_TOTAL_INVEST_PER_MARKET {MAX_TOTAL_INVEST_PER_MARKET} -> {val}")
            MAX_TOTAL_INVEST_PER_MARKET = val

    if "minMargin" in cfg:
        raw = to_decimal_safe(cfg.get("minMargin"))
        if raw is not None and raw != BUY_RANGE_LOW:
            changes.append(f"BUY_RANGE_LOW {BUY_RANGE_LOW} -> {raw}")
            BUY_RANGE_LOW = raw

    if "maxMargin" in cfg:
        raw = to_decimal_safe(cfg.get("maxMargin"))
        if raw is not None and raw != BUY_RANGE_HIGH:
            changes.append(f"BUY_RANGE_HIGH {BUY_RANGE_HIGH} -> {raw}")
            BUY_RANGE_HIGH = raw

    if "lcRate" in cfg:
        val = to_decimal_safe(cfg.get("lcRate"))
        if val is not None and val != LOSS_CUT_RATE:
            changes.append(f"LOSS_CUT_RATE {LOSS_CUT_RATE} -> {val}")
            LOSS_CUT_RATE = val

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

    # 추천 점수 동적 설정 (minScore)
    if "minScore" in cfg:
        try:
            ms = int(cfg["minScore"])
            if ms != BB_MIN_OVERALL_SCORE:
                changes.append(f"BB_MIN_OVERALL_SCORE {BB_MIN_OVERALL_SCORE} -> {ms}")
                BB_MIN_OVERALL_SCORE = ms
        except:
            pass

    # 10분당 체결 건수 동적 설정 (minTrades10m) 또는 분당 체결 건수 (minTradesPerMin)
    if "minTrades10m" in cfg:
        try:
            mt = int(cfg["minTrades10m"])
            if mt != BB_MIN_TRADE_COUNT_10M:
                changes.append(f"BB_MIN_TRADE_COUNT_10M {BB_MIN_TRADE_COUNT_10M} -> {mt}")
                BB_MIN_TRADE_COUNT_10M = mt
        except:
            pass
    elif "minTradesPerMin" in cfg:
        try:
            # 분당 체결수 입력 시 10분 단위로 환산
            mt = int(cfg["minTradesPerMin"]) * 10
            if mt != BB_MIN_TRADE_COUNT_10M:
                changes.append(f"BB_MIN_TRADE_COUNT_10M(10분기준) {BB_MIN_TRADE_COUNT_10M} -> {mt}")
                BB_MIN_TRADE_COUNT_10M = mt
        except:
            pass

    if changes:
        print("[CFG] 동적 설정 업데이트 반영:\n  " + "\n  ".join(changes))

# ============================================================
# 6. Exclude Markets (거래 제외 코인 관리)
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
    except Exception:
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
    except Exception:
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
        print(f"[EXC] 거래 제외 목록 갱신 ({len(new_excluded)}개): {sorted(new_excluded)}")
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

_TRADABLE_MARKETS_SET: set[str] = set()
_TRADABLE_MARKETS_TS: float = 0.0

async def get_upbit_tradable_markets(force: bool = False) -> set[str]:
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
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패: {e}")

    return _TRADABLE_MARKETS_SET

async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets:
        return {}

    tradable = await get_upbit_tradable_markets()
    valid_markets = [m for m in markets if m in tradable] if tradable else markets
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
        # Fallback 처리
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
                        if m in _TRADABLE_MARKETS_SET:
                            _TRADABLE_MARKETS_SET.discard(m)
                except Exception:
                    pass
        return price_map

# ============================================================
# 9. BB-Patterns 기반 신규 매수 후보 발굴 엔진
# ============================================================
_bb_patterns_cache: Dict[str, Any] = {
    "fetched_ts": 0.0,
    "last_server_updated": None,
    "candidates": []
}

async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if (not force) and (now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC) and _bb_patterns_cache["candidates"]:
        return _bb_patterns_cache["candidates"]

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(BB_PATTERNS_API_URL)
            if resp.status_code == 202:
                print("[BB-PATTERNS] 백엔드 서버가 캔들 지표 수집/계산 중입니다 (202).")
                return _bb_patterns_cache["candidates"]
            elif resp.status_code != 200:
                print(f"[BB-PATTERNS][WARN] 상태코드 이상: {resp.status_code}")
                return _bb_patterns_cache["candidates"]
            data = resp.json()
    except Exception as e:
        print(f"[BB-PATTERNS][ERR] API 호출 실패: {e}")
        return _bb_patterns_cache["candidates"]

    if data.get("status") == "processing":
        return _bb_patterns_cache["candidates"]

    coins = data.get("coins", [])
    if not isinstance(coins, list) or len(coins) == 0:
        return _bb_patterns_cache["candidates"]

    server_updated = data.get("updated")
    excluded = await get_excluded_markets()
    qualified = []

    cnt_total = len(coins)
    cnt_excluded = 0
    cnt_score_drop = 0
    cnt_trade_drop = 0
    cnt_price_drop = 0

    for c in coins:
        market = c.get("market", "")
        if not market.startswith("KRW-"):
            continue

        if market in excluded:
            cnt_excluded += 1
            continue

        try:
            price = float(c.get("price", 0))
        except:
            price = 0
        if price < 50:
            cnt_price_drop += 1
            continue

        try:
            overall_score = int(c.get("overall_score", 0))
        except:
            overall_score = 0
        if overall_score < BB_MIN_OVERALL_SCORE:
            cnt_score_drop += 1
            continue

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

    qualified.sort(key=lambda x: (x["overall_score"], x["trade_count_10m"]), reverse=True)

    _bb_patterns_cache["fetched_ts"] = now
    _bb_patterns_cache["last_server_updated"] = server_updated
    _bb_patterns_cache["candidates"] = qualified

    print(
        f"\n[BB-PATTERNS 갱신] 서버시각: {server_updated}\n"
        f"  - 전체: {cnt_total}개 | 제외코인: {cnt_excluded}개 | 50원미만: {cnt_price_drop}개\n"
        f"  - 점수미달(<{BB_MIN_OVERALL_SCORE}): {cnt_score_drop}개 | 체결미달(<{BB_MIN_TRADE_COUNT_10M}건/10m): {cnt_trade_drop}개\n"
        f"  ▶ 최종 추천 진입 후보: {len(qualified)}개"
    )
    if qualified:
        for q in qualified[:5]:
            print(f"    * {q['market']} | 점수: {q['overall_score']}점 | 10분체결: {q['trade_count_10m']}건 | 현재가: {q['price']}원")
    print()

    return qualified

# ============================================================
# 10. 주문 보조 함수
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
# 11. 익절 및 손절 판단 엔진
# ============================================================
def decide_sell(market: str, pnl: Decimal, state: dict) -> Tuple[bool, str, Optional[str]]:
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)

    if hard_tp_taken and (not hard_tp2_taken) and pnl >= HARD_TP2_BASE:
        return True, f"HARD_TP2 {pnl}% >= {HARD_TP2_BASE}%", "HARD_TP2"

    if (not hard_tp_taken) and pnl >= HARD_TP:
        return True, f"HARD_TP1 {pnl}% >= {HARD_TP}%", "HARD_TP1"

    if (not armed) and pnl >= ARM_PNL:
        state["armed"] = True
        return False, f"ARMED {pnl}% (>= {ARM_PNL}%)", None

    if state["armed"] and peak >= TRAIL_START_PNL:
        drop = peak - pnl
        if drop >= TRAIL_DROP:
            return True, f"TRAIL_DROP {drop}% >= {TRAIL_DROP}% (peak={peak}% now={pnl}%)", "TRAIL"

    return False, "", None

# ============================================================
# 12. 추가 매수 프로세스
# ============================================================
ADDITIONAL_BUY_COOLDOWN_SEC = int(os.getenv("ADDITIONAL_BUY_COOLDOWN_SEC", "120"))  # 최소 2분

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

    try:
        drop_target = Decimal(str(BUY_RANGE_LOW))
        if drop_target > 0:
            drop_target = -abs(drop_target)
    except Exception:
        drop_target = Decimal("-2.2")

    excluded = await get_excluded_markets()

    for pos in enriched_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        cur_price = pos.get("current_price")

        if market in excluded or pnl is None or cur_price is None:
            continue

        # 하락폭 검증
        if pnl >= Decimal("0") or pnl > drop_target:
            continue

        total_buys, total_invested = ps.get_buy_stats(market)

        if total_invested <= 0:
            bal_eff = (pos["balance"] + pos["locked"])
            total_invested = (bal_eff * pos["avg_buy_price"]).quantize(Decimal("0.0001"))
            ps.buy_info[market] = {"total_buys": max(1, total_buys), "total_invested": total_invested}

        if max_cap > 0 and total_invested >= max_cap:
            continue

        if total_buys - 1 >= MAX_ADDITIONAL_BUYS:
            continue

        st = ps.data.get(market, {})
        last_add_ts = st.get("last_additional_buy_ts")
        last_add_price = st.get("last_additional_buy_price")
        last_add_pnl = st.get("last_additional_buy_pnl")

        # 2분 쿨다운 검증
        if last_add_ts is None:
            st["last_additional_buy_ts"] = now
            continue

        elapsed_sec = now - last_add_ts
        if elapsed_sec < ADDITIONAL_BUY_COOLDOWN_SEC:
            continue

        # 가격 변화 확인
        if last_add_price is not None:
            if cur_price == last_add_price:
                continue

            tick_unit = get_tick_unit(cur_price)
            if abs(cur_price - last_add_price) < tick_unit:
                continue

        # 직전 PNL 대비 추가 하락 확인
        if last_add_pnl is not None and (pnl - last_add_pnl) > Decimal("-0.8"):
            continue

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

        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(
                    f"[BUY-ADD] {market} 추가 매수 완료! pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원, 경과:{int(elapsed_sec)}초) amt={buy_amt} | 누적: {total_invested + buy_amt}/{max_cap}원 (uuid={resp.get('uuid')})")
            else:
                print(
                    f"[BUY-ADD][DRY] {market} 추가 매수: pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원) amt={buy_amt}")

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
# 13. 신규 종목 매수 프로세스
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

        if market in excluded:
            continue

        if ps.buy_info.get(market):
            continue

        if ps.recently_sold(market):
            continue

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
# 14. 매수 후 선지정 지정가 익절 주문 보조
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

                await cancel_all_open_orders_for_market(access_key, secret_key, market)

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
# [보완] 재시작 시 기존 제외 코인을 철저히 배제한 매도주문 재배치
# ============================================================
async def reseed_and_replace_all_open_orders(
        access_key: str,
        secret_key: str,
        ps: PositionState
):
    """
    재시작 시:
    1. 최신 제외 코인 목록을 강제 조회하여 확인
    2. 제외 코인은 주문 취소 및 신규 매도 대상에서 '완전 배제'
    3. 관리 대상 정상 보유 코인만 기존 미체결 취소 후 언락 잔고 포함 매도 재배치
    """
    print("[RESEED] 기존 등록 코인 미체결 주문 취소 및 매도 재배치 시작...")

    # 1. 제외 코인 목록을 캐시 무시하고 강제로 최신 동기화
    excluded = await get_excluded_markets(force_reload=True)

    # 2. 1차 계정 잔고 조회
    try:
        raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[RESEED][ERR] 1차 계정 조회 실패: {e}")
        return

    # 관리 대상 코인 식별 (★ 제외 코인은 여기서 즉시 원천 배제)
    target_markets = []
    for acc in raw_accounts:
        cur = acc.get("currency")
        if not cur or cur == "KRW":
            continue
        mkt = f"KRW-{cur}"

        # [필터] 제외 코인 배제
        if mkt in excluded:
            continue

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        if (bal + locked) > 0:
            target_markets.append(mkt)

    if not target_markets:
        print("[RESEED] 재배치할 관리 대상 보유 코인이 없습니다 (제외 코인 생략).")
        return

    # 3. 제외 코인이 아닌 '정상 관리 대상 코인'에 대해서만 기존 미체결 취소
    for mkt in target_markets:
        await cancel_all_open_orders_for_market(access_key, secret_key, mkt)

    # 4. 언락 대기
    await asyncio.sleep(0.5)

    # 5. 2차 계정 잔고 조회 (언락 반영된 잔고 확보)
    try:
        updated_accounts = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[RESEED][ERR] 2차 계정 조회 실패: {e}")
        updated_accounts = raw_accounts

    # 6. 관리 대상 코인만 매도 주문 재배치
    for acc in updated_accounts:
        cur = acc.get("currency")
        if not cur or cur == "KRW":
            continue
        mkt = f"KRW-{cur}"

        # ★ [이중 가드] 제외 코인이거나 target_markets에 없는 종목은 건드리지 않고 스킵
        if mkt in excluded or mkt not in target_markets:
            continue

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        avg = Decimal(str(acc.get("avg_buy_price", "0")))

        total_vol = bal + locked
        if total_vol <= 0 or avg <= 0:
            continue

        try:
            # 목표가 계산 (+0.6% 및 최우선 매도호가 보정)
            target_price_raw = avg * (Decimal("1") + (HARD_TP / Decimal("100")))
            best_bid, best_ask = await get_orderbook_top(mkt)
            limit_price = adjust_price_to_tick(max(target_price_raw, best_ask), side="ask")

            vol_q = quantize_volume(total_vol)
            notional = vol_q * limit_price

            if notional < MIN_ORDER_NOTIONAL_KRW:
                print(f"[RESEED][SKIP] {mkt} 주문 금액 미달 ({notional:.0f}원 < {MIN_ORDER_NOTIONAL_KRW}원)")
                continue

            if LIVE_TRADING:
                resp = await order_limit_sell(access_key, secret_key, mkt, vol_q, limit_price)
                uid = resp.get("uuid")

                # 상태 등록
                st = ps.data.setdefault(mkt, {})
                st["active_limit_uuid"] = uid
                st["limit_pending_category"] = "PRE_HARD_TP1"
                st["limit_submit_ts"] = time.time()
                print(f"[RESEED][OK] {mkt} 매도 주문 재배치 완료: {vol_q}개 @ {limit_price}원 (uuid={uid})")
            else:
                print(f"[RESEED][DRY] {mkt} 매도 주문 시뮬레이션: {vol_q}개 @ {limit_price}원")

        except Exception as e:
            print(f"[RESEED][ERR] {mkt} 매도 주문 재배치 실패: {e}")

    print("[RESEED] 정상 등록 코인 매도 주문 재배치 완료 (제외 코인 보호됨)\n")

# ============================================================
# 15. 메인 모니터링 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()

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

    excluded = await get_excluded_markets(force_reload=True)

    # 1. 초기 보유 잔고 동기화
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
                    "last_additional_buy_ts": boot_now,
                    "last_additional_buy_price": avg,
                    "last_additional_buy_pnl": Decimal("0")
                }
        print(f"[INIT] 기존 보유 코인 {len(ps.buy_info)}개 관리 등록 완료 (2분 매수 쿨다운 적용)")
    except Exception as e:
        print(f"[INIT][WARN] 잔고 초기화 경고: {e}")

    # ★ [핵심 추가] 재시작 시 기존 대기 주문 취소 및 잔여 코인 전량 매도 재배치 실행
    try:
        await reseed_and_replace_all_open_orders(access_key, secret_key, ps)
    except Exception as e:
        print(f"[RESEED][WARN] 재시작 주문 재배치 중 오류 발생: {e}")

    while True:
        try:
            excluded = await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)

            available_krw = Decimal("0")
            for a in raw_accounts:
                if a.get("currency") == "KRW":
                    available_krw = Decimal(str(a.get("balance", "0")))
                    break

            held_markets = []
            for a in raw_accounts:
                cur = a.get("currency")
                if cur and cur != "KRW":
                    mkt = f"KRW-{cur}"
                    if mkt not in excluded:
                        bal = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                        if bal > 0:
                            held_markets.append(mkt)

            price_map = await fetch_current_prices(held_markets)

            enriched = []
            for a in raw_accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW":
                    continue
                market = f"KRW-{cur}"

                if market in excluded:
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

            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            # [A] 매도 판단 및 실행
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                bal = pos["balance"]
                st = ps.update_or_init(market, pnl, pos["avg_buy_price"])

                if ENABLE_STOP and pnl <= LOSS_CUT_RATE:
                    print(f"[LOSS-CUT] {market} 손절 조건 도달 (PNL: {pnl}% <= {LOSS_CUT_RATE}%)")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.3)

                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            cur_bal = Decimal(str(a.get("balance", "0")))
                            if cur_bal * pos["current_price"] >= MIN_ORDER_NOTIONAL_KRW:
                                await order_market_sell(access_key, secret_key, market, quantize_volume(cur_bal))
                                ps.mark_sold(market)
                            break
                    continue

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

            # [B] 추가 매수 실행
            used_krw_add = await process_additional_buys(access_key, secret_key, enriched, ps, available_krw)
            available_krw -= used_krw_add

            # [C] 신규 종목 매수 실행
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
# 16. 컨트롤러 (재기동 시 설정 강제 리로드 탑재)
# ============================================================
_last_cfg_signature = None

async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    global _last_cfg_signature
    print(f"[CTRL] Controller 가동: User={user_no}, Server={server_no}")

    async def update_config_once():
        nonlocal task
        global _last_cfg_signature

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

        # 리셋 예외 감지 시 처리
        if task and task.done():
            exc = task.exception()
            if isinstance(exc, ResetRequested):
                print("[CTRL] 주기 리셋 요청 감지 → 최신 설정 강제 재초기화 후 재시작")
                init_config(force=True)  # ★ 재시작 시 설정 강제 리로드
                apply_dynamic_config(cfg)
            elif exc:
                print(f"[CTRL] 모니터링 비정상 종료 감지: {exc}")
                init_config(force=True)
            task = None

        if active_flag and task is None:
            print("[CTRL] activeYN=Y → 설정 최신화 확인 후 모니터링 태스크 시작")
            init_config(force=True)  # ★ 태스크 시작 직전 설정 최신화
            apply_dynamic_config(cfg)
            task = asyncio.create_task(monitor_positions(user_no, server_no))
        elif not active_flag and task:
            print("[CTRL] activeYN=N → 모니터링 태스크 중단")
            task.cancel()
            try:
                await task
            except:
                pass
            task = None

    await update_config_once()

    while True:
        try:
            await update_config_once()
        except Exception as e:
            print(f"[CTRL][ERR] 컨트롤러 주기 점검 예외: {e}")
        await asyncio.sleep(CONTROLLER_POLL_SEC)

# ============================================================
# 17. 진입점
# ============================================================
async def main():
    init_config(force=True)
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청 수신")