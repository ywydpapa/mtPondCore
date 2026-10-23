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

# 주기 리셋 설정 (24시간 1회)
PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "86400"))


class ResetRequested(Exception):
    pass


# ============================================================
# 4. STATIC CONFIG (설정 및 신규 조건)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))  # 3분 주기
COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "60"))  # 정상익절 1분
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "300"))  # 손절/방어 5분

# 캔들 변동폭 대비 익절 목표 비율 (65% 채택)
PROFIT_RANGE_RATIO = Decimal(os.getenv("PROFIT_RANGE_RATIO", "0.65"))
MIN_PROFIT_TARGET_PCT = Decimal("0.8")  # 최소 목표 익절선 (+0.8%)
MAX_PROFIT_TARGET_PCT = Decimal("4.0")  # 최대 목표 익절선 (+4.0%)
MIN_ROOM_TO_1H_HIGH_PCT = Decimal("1.2")  # 1시간봉 상단 저항까지 최소 여유폭 (+1.2%)

# 물타기(반등 추종) 및 손절 설정
MAX_ADDITIONAL_BUYS = 2  # 최대 물타기 2회
ADD_BUY_1_DROP = Decimal(os.getenv("ADD_BUY_1_DROP", "-2.2"))  # 1차 물타기 기준 (-2.2%)
ADD_BUY_2_DROP = Decimal(os.getenv("ADD_BUY_2_DROP", "-4.5"))  # 2차 물타기 기준 (-4.5%)
FINAL_LOSS_CUT_RATE = Decimal(os.getenv("FINAL_LOSS_CUT_RATE", "-6.5"))  # 최종 방어 손절선 (-6.5%)
MAX_LOSS_LIMIT_PCT = Decimal("-25.0")  # 손실이 -25% 초과(미만)일 경우 손절 차단

INTERVAL_SECONDS = 5
MIN_NOTIONAL_KRW = Decimal("5000")
MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
ORDER_NOTIONAL_BUFFER_PCT = Decimal("0.01")
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

# ============================================================
# 5. RUNTIME CONFIG (Redis 연동 동적 설정 관리)
# ============================================================
ENABLE_STOP = (os.getenv("ENABLE_STOP", "1").upper() in ("1", "Y", "TRUE"))
_CONFIG_INITIALIZED = False

# 전역 설정 변수 선언 (Redis 및 .env 폴백 연동)
MAX_ACTIVE_MARKETS = 10
RANGE_BUY_KRW = Decimal("40000")
ADDITIONAL_BUY_KRW = Decimal("40000")
MAX_TOTAL_INVEST_PER_MARKET = Decimal("120000")
BB_MIN_OVERALL_SCORE = 3
BB_MIN_TRADE_COUNT_10M = 60


class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.sell_history: Dict[str, Dict[str, Any]] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None:
            self.data[market] = {
                "peak_pnl": pnl,
                "min_pnl": pnl,
                "avg_buy_price": avg_price,
                "last_update_ts": now,
                "active_limit_uuid": None,
                "dynamic_tp": None,
                "last_additional_buy_ts": now,
                "last_additional_buy_price": avg_price,
                "last_additional_buy_pnl": Decimal("0")
            }
            return self.data[market]

        if st.get("avg_buy_price") != avg_price:
            st["avg_buy_price"] = avg_price
            st["peak_pnl"] = pnl
            st["min_pnl"] = pnl
            st["last_update_ts"] = now
            return st

        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl

        st["last_update_ts"] = now
        return st

    def mark_sold(self, market: str, reason: str = "DEFENSE"):
        now = time.time()
        is_tp = reason.upper() in ("TP", "HARD_TP", "TP_DONE", "DYNAMIC_TP")
        cooldown_sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_DEFENSE_SEC
        tag = "정상익절" if is_tp else "방어/손절"

        self.sell_history[market] = {
            "ts": now,
            "expire_ts": now + cooldown_sec,
            "reason": reason,
            "tag": tag
        }
        self.data.pop(market, None)
        self.buy_info.pop(market, None)
        print(f"[COOLDOWN] {market} 매도 완료 [{tag}: {reason}] → 재매수 금지 {cooldown_sec}초 적용")

    def recently_sold(self, market: str) -> bool:
        hist = self.sell_history.get(market)
        if not hist:
            return False
        return time.time() < hist["expire_ts"]

    def record_buy(self, market: str, krw_amount: Decimal):
        info = self.buy_info.setdefault(market, {"total_buys": 0, "total_invested": Decimal("0")})
        info["total_buys"] += 1
        info["total_invested"] += krw_amount

    def get_buy_stats(self, market: str) -> Tuple[int, Decimal]:
        info = self.buy_info.get(market)
        if not info:
            return 0, Decimal("0")
        return info["total_buys"], info["total_invested"]


def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
    ADDITIONAL_BUY_KRW = Decimal(os.getenv("ADDITIONAL_BUY_KRW", "40000"))
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "120000"))
    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "60"))

    _CONFIG_INITIALIZED = True
    print(f"[INIT] 기본 설정 초기화 완료 (force={force}) | 활성최대:{MAX_ACTIVE_MARKETS}개, 최초금액:{RANGE_BUY_KRW}원")


def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW, MAX_TOTAL_INVEST_PER_MARKET
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    def to_dec(v):
        try:
            return Decimal(str(v)) if v not in (None, "") else None
        except:
            return None

    for key in ("maxCoincnt", "max_active_markets", "MAX_ACTIVE_MARKETS"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv > 0: MAX_ACTIVE_MARKETS = nv
            except:
                pass

    for key in ("initAmt", "range_buy_krw", "RANGE_BUY_KRW"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0: RANGE_BUY_KRW = v

    for key in ("addAmt", "additional_buy_krw", "ADDITIONAL_BUY_KRW"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0: ADDITIONAL_BUY_KRW = v

    for key in ("limitAmt", "max_total_invest_per_market", "MAX_TOTAL_INVEST_PER_MARKET"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0: MAX_TOTAL_INVEST_PER_MARKET = v


async def sync_redis_config(user_no: int):
    try:
        c = await redis_client.get(f"mtpond:setup:{user_no}")
        if c:
            data = json.loads(c)
            cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
            if isinstance(cfg, dict):
                apply_dynamic_config(cfg)
    except Exception as e:
        print(f"[CONFIG][WARN] Redis 설정 동기화 실패: {e}")


# ============================================================
# 6. Exclude Markets (거래 제외 코인 관리)
# ============================================================
async def get_excoins(user_no: int) -> list[tuple] | None:
    async with SessionLocal() as session:
        sql = text("""SELECT DISTINCT market
                      FROM exCoinlist
                      WHERE userNo = :u
                        AND attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"u": user_no, "attr": "%XXX%"})
        return result.fetchall()


async def parse_exclude_markets() -> set[str]:
    try:
        uno = int(os.getenv("USER_NO", "0"))
        cached_coins = await redis_client.smembers(f"mtpond:excoins:{uno}")
        if cached_coins:
            return set(cached_coins)
        rows = await get_excoins(uno)
        if rows:
            ex_set = {str(r[0]).strip() for r in rows if r and r[0]}
            if ex_set:
                await redis_client.sadd(f"mtpond:excoins:{uno}", *ex_set)
                await redis_client.expire(f"mtpond:excoins:{uno}", 600)
            return ex_set
    except Exception:
        pass
    raw = os.getenv("EXCLUDE_MARKETS", "") or ""
    return set([p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()])


EXCLUDED_MARKETS: set[str] = set()
_excluded_last_update: float = 0
EXCLUDED_CACHE_TTL_SEC = 60


async def get_excluded_markets(force_reload: bool = False) -> set[str]:
    global EXCLUDED_MARKETS, _excluded_last_update
    now = time.time()
    if (not force_reload) and EXCLUDED_MARKETS and (now - _excluded_last_update) < EXCLUDED_CACHE_TTL_SEC:
        return EXCLUDED_MARKETS
    new_excluded = await parse_exclude_markets()
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


async def upbit_request(method: str, url: str, access_key: str, secret_key: str, params: dict = None,
                        max_retry: int = 3) -> dict:
    params = params or {}
    backoff = 1.0
    for attempt in range(1, max_retry + 1):
        token = build_upbit_jwt_with_params(access_key, secret_key, params) if (
                    params or method.upper() != "GET") else build_upbit_jwt_simple(access_key, secret_key)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                if method.upper() == "GET":
                    resp = await client.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    resp = await client.post(url, headers=headers, params=params)
                elif method.upper() == "DELETE":
                    resp = await client.delete(url, headers=headers, params=params)
            if resp.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} body={resp.text}")
            return resp.json()
        except Exception as e:
            if attempt >= max_retry: raise
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


async def register_loss_cut(
        user_no: int, market: str, quantity: Decimal, price: Decimal, loss_rate: Decimal):
    if not API_BASE:
        print("[LOSS-CUT-API][WARN] API_BASE가 설정되지 않아 손절 기록을 저장하지 못했습니다.")
        return

    payload = {
        "userNo": user_no,
        "coinCode": market,
        "losscutQty": float(quantity),
        "losscutPrice": float(price),
        "lossRate": float(loss_rate),
        "losscutReason": "손절 조건 도달",
    }
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            response = await client.post(f"{API_BASE}/api/regLC", json=payload)
            response.raise_for_status()
        print(f"[LOSS-CUT-API] {market} 손절 기록 저장 완료")
    except httpx.HTTPError as e:
        print(f"[LOSS-CUT-API][ERR] {market} 손절 기록 저장 실패: {e}")


def quantize_volume(vol: Decimal) -> Decimal:
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


# ============================================================
# 8. 틱 단위 및 시세 조회
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
    return Decimal("0.00001")


def adjust_price_to_tick(raw_price: Decimal, side: str = "ask") -> Decimal:
    price = Decimal(str(raw_price))
    if price <= 0: return Decimal("0")
    unit = get_tick_unit(price)
    steps = price / unit
    mode = ROUND_CEILING if side == "ask" else ROUND_FLOOR
    return (steps.to_integral_value(rounding=mode) * unit).quantize(Decimal("1") if unit >= 1 else unit)


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


async def get_upbit_tradable_markets() -> set[str]:
    global _TRADABLE_MARKETS_SET, _TRADABLE_MARKETS_TS
    now = time.time()
    if _TRADABLE_MARKETS_SET and (now - _TRADABLE_MARKETS_TS < 3600):
        return _TRADABLE_MARKETS_SET
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("https://api.upbit.com/v1/market/all?isDetails=false")
            if resp.status_code == 200:
                _TRADABLE_MARKETS_SET = {m["market"] for m in resp.json() if "market" in m}
                _TRADABLE_MARKETS_TS = now
    except Exception:
        pass
    return _TRADABLE_MARKETS_SET


async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets: return {}
    tradable = await get_upbit_tradable_markets()
    valid_markets = [m for m in markets if m in tradable] if tradable else markets
    if not valid_markets: return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(valid_markets)}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return {item["market"]: Decimal(str(item["trade_price"])) for item in r.json() if
                        item.get("trade_price") is not None}
    except Exception:
        pass
    return {}


# ============================================================
# 8.1. 비트코인 3단계 방어 체력 진단 함수 (★ 정지 장치)
# ============================================================
async def check_btc_market_health() -> Dict[str, Any]:
    """
    비트코인(KRW-BTC)의 1시간/4시간 캔들을 정밀 분석하여 시장 전체의 위험도 판별
    반환: {"status": "NORMAL" / "CAUTION" / "EMERGENCY", "btc_pnl_1h": 변동률, "msg": 상태메시지}
    """
    try:
        url = "https://api.upbit.com/v1/candles/minutes/60"
        async with httpx.AsyncClient(timeout=5.0) as client:
            res = await client.get(url, params={"market": "KRW-BTC", "count": 25})
            if res.status_code != 200:
                return {"status": "NORMAL", "btc_pnl_1h": 0.0, "msg": "BTC 캔들 조회 실패"}
            candles = res.json()

        cur_price = Decimal(str(candles[0]["trade_price"]))
        prev_1h_price = Decimal(str(candles[1]["trade_price"]))
        prev_4h_price = Decimal(str(candles[4]["trade_price"]))

        closes = [Decimal(str(c["trade_price"])) for c in candles]
        ma20 = sum(closes[:20]) / Decimal("20")

        pnl_1h = float(((cur_price - prev_1h_price) / prev_1h_price) * 100)
        pnl_4h = float(((cur_price - prev_4h_price) / prev_4h_price) * 100)

        # 1. 비상 상태 (EMERGENCY): 1시간 내 -1.8% 이상 급락 또는 4시간 내 -3.5% 이상 하락 또는 MA20 이탈 심화
        if pnl_1h <= -1.8 or pnl_4h <= -3.5 or cur_price < (ma20 * Decimal("0.985")):
            return {
                "status": "EMERGENCY",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"🚨 [BTC-EMERGENCY] 급락 감지! (1시간: {pnl_1h:+.2f}%, 4시간: {pnl_4h:+.2f}%, 현재가 < MA20)"
            }

        # 2. 주의 상태 (CAUTION): 1시간 내 -0.8% ~ -1.8% 조정 중 또는 MA20 하회
        elif pnl_1h <= -0.8 or cur_price < ma20:
            return {
                "status": "CAUTION",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"⚠️ [BTC-CAUTION] 조정세 감지 (1시간: {pnl_1h:+.2f}%, MA20 근접/하회)"
            }

        return {
            "status": "NORMAL",
            "btc_pnl_1h": round(pnl_1h, 2),
            "msg": "✅ [BTC-NORMAL] 안정적 추세 유지"
        }
    except Exception as e:
        return {"status": "NORMAL", "btc_pnl_1h": 0.0, "msg": f"BTC 분석 에러: {e}"}


# ============================================================
# 9. 1시간봉 중심 + 15분봉 결합 멀티 타임프레임(MTF) 분석 엔진
# ============================================================
_candle_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
CANDLE_CACHE_TTL = 60


def _calc_bb_and_metrics(candles: List[dict], range_lookback: int) -> Dict[str, Decimal]:
    closes = [float(c["trade_price"]) for c in candles]
    highs = [float(c["high_price"]) for c in candles[:range_lookback]]
    lows = [float(c["low_price"]) for c in candles[:range_lookback]]
    cur_price = Decimal(str(candles[0]["trade_price"]))

    mean = sum(closes) / len(closes)
    variance = sum([((x - mean) ** 2) for x in closes]) / len(closes)
    std_dev = math.sqrt(variance)

    bb_mid = Decimal(str(round(mean, 4)))
    bb_low = Decimal(str(round(mean - (2.0 * std_dev), 4)))
    bb_high = Decimal(str(round(mean + (2.0 * std_dev), 4)))

    max_h = Decimal(str(max(highs)))
    min_l = Decimal(str(min(lows)))
    range_pct = Decimal("0")
    if cur_price > 0 and min_l > 0:
        range_pct = ((max_h - min_l) / cur_price * Decimal("100")).quantize(Decimal("0.01"))

    return {
        "cur_price": cur_price,
        "bb_mid": bb_mid,
        "bb_low": bb_low,
        "bb_high": bb_high,
        "range_pct": range_pct
    }


async def analyze_mtf_trend_and_target(market: str, force: bool = False) -> Optional[Dict[str, Any]]:
    now = time.time()
    cached = _candle_cache.get(market)
    if (not force) and cached and (now - cached[0]) < CANDLE_CACHE_TTL:
        return cached[1]

    url_15m = "https://api.upbit.com/v1/candles/minutes/15"
    url_60m = "https://api.upbit.com/v1/candles/minutes/60"
    params = {"market": market, "count": 20}

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r15, r60 = await asyncio.gather(
                client.get(url_15m, params=params),
                client.get(url_60m, params=params)
            )
            if r15.status_code != 200 or r60.status_code != 200:
                return None

            c15 = r15.json()
            c60 = r60.json()
            if not isinstance(c15, list) or len(c15) < 15 or not isinstance(c60, list) or len(c60) < 15:
                return None

            m15 = _calc_bb_and_metrics(c15, range_lookback=8)
            m60 = _calc_bb_and_metrics(c60, range_lookback=6)
            cur_price = m15["cur_price"]
            if cur_price <= 0:
                return None

            c60_open_0 = Decimal(str(c60[0]["opening_price"]))
            c60_low_0 = Decimal(str(c60[0]["low_price"]))
            c60_low_1 = Decimal(str(c60[1]["low_price"]))
            c60_close_1 = Decimal(str(c60[1]["trade_price"]))

            is_1h_bullish_candle = cur_price >= c60_open_0
            is_1h_higher_low = c60_low_0 >= c60_low_1

            closes_60 = [float(c["trade_price"]) for c in c60]
            prev_ma60 = Decimal(str(round(sum(closes_60[1:]) / len(closes_60[1:]), 4)))
            is_1h_ma_rising = m60["bb_mid"] >= prev_ma60

            room_to_1h_high = ((m60["bb_high"] - cur_price) / cur_price * Decimal("100")).quantize(Decimal("0.01"))

            cond_1h_uptrend = (cur_price >= m60["bb_mid"]) or (
                        is_1h_ma_rising and (is_1h_bullish_candle or is_1h_higher_low))
            cond_1h_support_bounce = (cur_price >= m60["bb_low"]) and is_1h_bullish_candle and (
                        cur_price >= c60_close_1 or is_1h_higher_low)
            not_blocked_by_1h_resistance = (room_to_1h_high >= MIN_ROOM_TO_1H_HIGH_PCT) or (
                        is_1h_bullish_candle and is_1h_ma_rising and room_to_1h_high >= Decimal("0.4"))

            is_1h_healthy = (cond_1h_uptrend or cond_1h_support_bounce) and not_blocked_by_1h_resistance

            c15_open_0 = Decimal(str(c15[0]["opening_price"]))
            c15_low_0 = Decimal(str(c15[0]["low_price"]))
            c15_high_0 = Decimal(str(c15[0]["high_price"]))
            is_15m_bullish = cur_price >= c15_open_0

            is_15m_rebounding = (cur_price >= m15["bb_mid"] and is_15m_bullish) or (
                        cur_price >= m15["bb_low"] and is_15m_bullish)
            is_rebounding = bool(is_1h_healthy and is_15m_rebounding)

            candle_15m_size = c15_high_0 - c15_low_0
            lower_wick_ratio = ((cur_price - c15_low_0) / candle_15m_size) if candle_15m_size > 0 else Decimal("0")
            can_water = bool(is_15m_bullish or (lower_wick_ratio >= Decimal("0.4") and is_1h_higher_low))

            blended_range = (m60["range_pct"] * Decimal("0.60")) + (m15["range_pct"] * Decimal("0.40"))
            target_tp = (blended_range * PROFIT_RANGE_RATIO).quantize(Decimal("0.01"))

            if room_to_1h_high > Decimal("0.9") and target_tp > room_to_1h_high:
                target_tp = max(MIN_PROFIT_TARGET_PCT, (room_to_1h_high * Decimal("0.85")).quantize(Decimal("0.01")))

            if target_tp < MIN_PROFIT_TARGET_PCT: target_tp = MIN_PROFIT_TARGET_PCT
            if target_tp > MAX_PROFIT_TARGET_PCT: target_tp = MAX_PROFIT_TARGET_PCT

            result = {
                "is_rebounding": is_rebounding,
                "can_water": can_water,
                "range_15m_pct": m15["range_pct"],
                "range_1h_pct": m60["range_pct"],
                "room_to_1h_high": room_to_1h_high,
                "target_tp": target_tp,
                "bb_1h_mid": m60["bb_mid"],
                "bb_1h_high": m60["bb_high"],
                "is_1h_healthy": is_1h_healthy
            }
            _candle_cache[market] = (now, result)
            return result
    except Exception as e:
        print(f"[MTF-ANALYSIS][WARN] {market} 멀티 타임프레임 분석 실패: {e}")
        return None


# ============================================================
# 10. BB-Patterns 기반 신규 매수 후보 발굴
# ============================================================
_bb_patterns_cache: Dict[str, Any] = {"fetched_ts": 0.0, "candidates": []}


async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if (not force) and (now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC) and _bb_patterns_cache["candidates"]:
        return _bb_patterns_cache["candidates"]

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(BB_PATTERNS_API_URL)
            if resp.status_code != 200: return _bb_patterns_cache["candidates"]
            data = resp.json()
    except Exception:
        return _bb_patterns_cache["candidates"]

    coins = data.get("coins", [])
    if not isinstance(coins, list): return _bb_patterns_cache["candidates"]

    excluded = await get_excluded_markets()
    qualified = []

    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in excluded: continue
        try:
            score = int(c.get("overall_score", 0))
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
        except:
            continue

        if price < 30 or score < BB_MIN_OVERALL_SCORE or trades < BB_MIN_TRADE_COUNT_10M:
            continue

        qualified.append({
            "market": mkt,
            "price": Decimal(str(price)),
            "overall_score": score,
            "trade_count_10m": trades
        })

    qualified.sort(key=lambda x: (x["overall_score"], x["trade_count_10m"]), reverse=True)
    _bb_patterns_cache["fetched_ts"] = now
    _bb_patterns_cache["candidates"] = qualified
    return qualified


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


async def fetch_upbit_order_state(access_key: str, secret_key: str, uuid_: str) -> Optional[dict]:
    try:
        return await upbit_request("GET", "https://api.upbit.com/v1/order", access_key, secret_key,
                                   params={"uuid": uuid_}, max_retry=2)
    except Exception:
        return None


async def cancel_all_open_orders_for_market(access_key: str, secret_key: str, market: str):
    try:
        orders = await upbit_request("GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
                                     params={"market": market, "state": "wait"})
        if isinstance(orders, list):
            for od in orders:
                uid = od.get("uuid")
                if uid:
                    await upbit_request("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key,
                                        params={"uuid": uid}, max_retry=2)
    except Exception:
        pass


# ============================================================
# 12. 수동/추가 매수 감지 및 동기화 처리 함수
# ============================================================
async def sync_or_register_held_positions(access_key: str, secret_key: str, ps: PositionState, raw_accounts: List[dict],
                                          excluded: set):
    for acc in raw_accounts:
        cur = acc.get("currency")
        if not cur or cur == "KRW": continue
        mkt = f"KRW-{cur}"
        if mkt in excluded: continue

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        total_vol = bal + locked
        avg_price = Decimal(str(acc.get("avg_buy_price", "0")))

        if total_vol <= 0 or avg_price <= 0:
            continue

        info = ps.buy_info.get(mkt)
        if not info:
            invested_amt = (total_vol * avg_price).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": invested_amt}
            ps.update_or_init(mkt, Decimal("0"), avg_price)
            asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, mkt, ps))
        else:
            st = ps.data.get(mkt, {})
            old_avg = st.get("avg_buy_price", Decimal("0"))
            if old_avg > 0 and old_avg != avg_price:
                invested_amt = (total_vol * avg_price).quantize(Decimal("0.0001"))
                info["total_invested"] = invested_amt
                ps.update_or_init(mkt, Decimal("0"), avg_price)
                asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, mkt, ps))


# ============================================================
# 13. 1시간/15분봉 반등 지지 확인형 물타기 프로세스
# ============================================================
async def process_strategic_additional_buys(
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
        max_cap = Decimal("120000")

    excluded = await get_excluded_markets()

    for pos in enriched_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        cur_price = pos.get("current_price")

        if market in excluded or pnl is None or cur_price is None:
            continue

        total_buys, total_invested = ps.get_buy_stats(market)
        if total_invested <= 0:
            bal_eff = pos["balance"] + pos["locked"]
            total_invested = (bal_eff * pos["avg_buy_price"]).quantize(Decimal("0.0001"))
            ps.buy_info[market] = {"total_buys": max(1, total_buys), "total_invested": total_invested}

        if total_buys - 1 >= MAX_ADDITIONAL_BUYS or total_invested >= max_cap:
            continue

        req_drop = ADD_BUY_1_DROP if (total_buys == 1) else ADD_BUY_2_DROP
        if pnl > req_drop:
            continue

        st = ps.data.setdefault(market, {})
        last_add_ts = st.get("last_additional_buy_ts", 0.0)
        if now - last_add_ts < 180:
            continue

        analysis = await analyze_mtf_trend_and_target(market)
        if analysis and not analysis.get("can_water", True):
            continue

        cap_remain = max_cap - total_invested
        buy_amt = min(ADDITIONAL_BUY_KRW, cap_remain, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(
                    f"💧 [WATERING] {market} {total_buys}회차 반등확인 물타기 체결! PNL={pnl}% amt={buy_amt} (uuid={resp.get('uuid')})")
            else:
                print(f"💧 [WATERING][DRY] {market} {total_buys}회차 반등확인 물타기: PNL={pnl}% amt={buy_amt}")

            ps.record_buy(market, buy_amt)
            st["last_additional_buy_ts"] = now
            st["last_additional_buy_price"] = cur_price
            st["last_additional_buy_pnl"] = pnl

            available_krw -= buy_amt
            used_krw += buy_amt

            new_tp = analysis["target_tp"] if analysis else None
            asyncio.create_task(
                after_buy_preplace_dynamic_tp(access_key, secret_key, market, ps, target_tp_override=new_tp))

        except Exception as e:
            print(f"[WATERING][ERR] {market} 물타기 실패: {e}")

    return used_krw


# ============================================================
# 14. 신규 종목 매수 프로세스
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

    candidates = await fetch_bb_patterns_candidates()
    if not candidates:
        return used_krw

    excluded = await get_excluded_markets()
    buys_done = 0

    for item in candidates:
        if buys_done >= remaining_slots:
            break

        market = item["market"]
        if market in excluded or ps.buy_info.get(market) or ps.recently_sold(market):
            continue

        analysis = await analyze_mtf_trend_and_target(market)
        if not analysis or not analysis["is_rebounding"]:
            continue

        buy_amt = min(RANGE_BUY_KRW, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(
                    f"🚀 [BUY-NEW] {market} 1h+15m 추세 진입! 금액={buy_amt}원 | "
                    f"1h진폭={analysis['range_1h_pct']}% | 상단여유={analysis['room_to_1h_high']}% | "
                    f"목표익절=+{analysis['target_tp']}% (uuid={resp.get('uuid')})"
                )
            else:
                print(
                    f"🚀 [BUY-NEW][DRY] {market} 진입: 금액={buy_amt}원 "
                    f"1h진폭={analysis['range_1h_pct']}% 목표=+{analysis['target_tp']}%"
                )

            ps.record_buy(market, buy_amt)
            available_krw -= buy_amt
            used_krw += buy_amt
            buys_done += 1

            asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, market, ps,
                                                              target_tp_override=analysis["target_tp"]))

        except Exception as e:
            print(f"[BUY-NEW][ERR] {market} 매수 실패: {e}")

    return used_krw


# ============================================================
# 15. 지정가 TP 발주
# ============================================================
async def after_buy_preplace_dynamic_tp(access_key: str, secret_key: str, market: str, ps: PositionState,
                                        target_tp_override: Decimal = None):
    await asyncio.sleep(0.5)
    try:
        await cancel_all_open_orders_for_market(access_key, secret_key, market)
        await asyncio.sleep(0.3)

        accounts = await fetch_upbit_accounts(access_key, secret_key)
        base = market.split("-")[1]
        for acc in accounts:
            if acc.get("currency") == base:
                bal = Decimal(str(acc.get("balance", "0")))
                avg = Decimal(str(acc.get("avg_buy_price", "0")))
                if bal <= 0 or avg <= 0:
                    return

                if target_tp_override:
                    dynamic_tp = target_tp_override
                else:
                    analysis = await analyze_mtf_trend_and_target(market)
                    dynamic_tp = analysis["target_tp"] if analysis else Decimal("1.5")

                target_price = avg * (Decimal("1") + (dynamic_tp / Decimal("100")))
                best_bid, best_ask = await get_orderbook_top(market)
                limit_price = adjust_price_to_tick(max(target_price, best_ask), side="ask")

                vol = quantize_volume(bal)
                if vol * limit_price >= MIN_ORDER_NOTIONAL_KRW:
                    resp = await order_limit_sell(access_key, secret_key, market, vol, limit_price)
                    st = ps.data.setdefault(market, {})
                    st["active_limit_uuid"] = resp.get("uuid")
                    st["dynamic_tp"] = dynamic_tp
                    print(f"🎯 [TARGET-TP] {market} 지정가 익절 주문 배치 완료: {limit_price}원 (+{dynamic_tp}%)")
                break
    except Exception as e:
        print(f"[PRE_TP][ERR] {market} 익절 배치 실패: {e}")


# ============================================================
# 16. 메인 모니터링 루프 (비트코인 3단계 정지 장치 통합)
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()
    last_cfg_sync = time.time()

    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2
                      FROM traceUser
                      WHERE userNo = :u
                        AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        row = result.fetchone()
        if not row: return
        access_key, secret_key = row[0], row[1]

    ps = PositionState()
    print("=== [MTF 1h+15m + BTC 정지 장치 적용] 트레이딩 엔진 가동 ===")
    excluded = await get_excluded_markets(force_reload=True)

    # 초기 보유 코인 관리 등록
    try:
        init_accs = await fetch_upbit_accounts(access_key, secret_key)
        for acc in init_accs:
            cur = acc.get("currency")
            if not cur or cur == "KRW": continue
            mkt = f"KRW-{cur}"
            if mkt in excluded: continue
            bal = Decimal(str(acc.get("balance", "0"))) + Decimal(str(acc.get("locked", "0")))
            avg = Decimal(str(acc.get("avg_buy_price", "0")))
            if bal > 0 and avg > 0:
                ps.buy_info[mkt] = {"total_buys": 1, "total_invested": (bal * avg).quantize(Decimal("0.0001"))}
                ps.update_or_init(mkt, Decimal("0"), avg)
                asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, mkt, ps))
    except Exception as e:
        print(f"[INIT][WARN] {e}")

    while True:
        try:
            loop_now = time.time()

            if loop_now - last_cfg_sync >= CONTROLLER_POLL_SEC:
                last_cfg_sync = loop_now
                await sync_redis_config(user_no)

            # ★ 비트코인 3단계 방어 체력 진단 호출 (정지 장치)
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]
            btc_msg = btc_health["msg"]

            excluded = await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)

            await sync_or_register_held_positions(access_key, secret_key, ps, raw_accounts, excluded)

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
                        if (Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))) > 0:
                            held_markets.append(mkt)

            price_map = await fetch_current_prices(held_markets)

            enriched = []
            for a in raw_accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW": continue
                mkt = f"KRW-{cur}"
                if mkt in excluded: continue
                bal = Decimal(str(a.get("balance", "0")))
                locked = Decimal(str(a.get("locked", "0")))
                avg = Decimal(str(a.get("avg_buy_price", "0")))
                cur_price = price_map.get(mkt)

                if cur_price and avg > 0 and (bal + locked) > 0:
                    pnl = ((cur_price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                    enriched.append({
                        "market": mkt, "balance": bal, "locked": locked,
                        "avg_buy_price": avg, "current_price": cur_price, "pnl_percent": pnl
                    })

            active_uuids = [(m, st.get("active_limit_uuid")) for m, st in list(ps.data.items()) if
                            st.get("active_limit_uuid")]
            for mkt, uid in active_uuids:
                ord_info = await fetch_upbit_order_state(access_key, secret_key, uid)
                if ord_info and ord_info.get("state") == "done":
                    print(f"🎉 [TP-SUCCESS] {mkt} MTF 목표가 체결 완료! (uuid={uid})")
                    ps.mark_sold(mkt, reason="TP")
                    enriched = [p for p in enriched if p["market"] != mkt]

            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            # [A] 매도 판단: 최종 방어 손절 및 고점 대비 이익 보존 트레일링
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                cur_p = pos.get("current_price")
                avg_p = pos.get("avg_buy_price")

                if cur_p is None or avg_p is None or cur_p <= 0 or avg_p <= 0:
                    continue

                total_buys, _ = ps.get_buy_stats(market)
                st = ps.update_or_init(market, pnl, avg_p)

                # 만약 시장이 EMERGENCY 상태이면 기존 손절선보다 타이트하게 방어 손절(-1.8%) 적용
                effective_stop_cut = FINAL_LOSS_CUT_RATE
                if btc_status == "EMERGENCY":
                    effective_stop_cut = Decimal("-1.8")

                # 1. 2회 물타기 소진 상태에서 최종 손절선 도달 시 손절 (-25% 초과 보호)
                if total_buys - 1 >= MAX_ADDITIONAL_BUYS and pnl <= effective_stop_cut:
                    if pnl < MAX_LOSS_LIMIT_PCT:
                        print(f"🛡️ [LOSS-PROTECT] {market} 손실률이 {pnl}%로 -25%를 초과하여 손절 보류")
                    else:
                        print(f"🚨 [FINAL-DEFENSE] {market} 최종 손절 실행 (PNL: {pnl}%)")
                        await cancel_all_open_orders_for_market(access_key, secret_key, market)
                        await asyncio.sleep(0.4)
                        accs = await fetch_upbit_accounts(access_key, secret_key)
                        for a in accs:
                            if a.get("currency") == market.split("-")[1]:
                                bal_dec = Decimal(str(a.get("balance", "0")))
                                s_vol = quantize_volume(bal_dec)
                                if s_vol and (s_vol * cur_p) >= MIN_ORDER_NOTIONAL_KRW:
                                    if LIVE_TRADING:
                                        await order_market_sell(access_key, secret_key, market, s_vol)
                                    await register_loss_cut(user_no, market, s_vol, cur_p, pnl)
                                    ps.mark_sold(market, reason="LOSS_CUT")
                                break
                    continue

                raw_tp = st.get("dynamic_tp")
                target_tp = Decimal(str(raw_tp)) if raw_tp is not None else Decimal("1.5")
                peak = st.get("peak_pnl", Decimal("0"))

                if peak >= (target_tp * Decimal("0.8")):
                    if Decimal("0.35") <= pnl <= Decimal("0.50"):
                        print(f"✨ [PROFIT-SAVE] {market} 이익 보존 매도 (PNL: {pnl}%)")
                        await cancel_all_open_orders_for_market(access_key, secret_key, market)
                        await asyncio.sleep(0.3)
                        accs = await fetch_upbit_accounts(access_key, secret_key)
                        for a in accs:
                            if a.get("currency") == market.split("-")[1]:
                                bal_dec = Decimal(str(a.get("balance", "0")))
                                s_vol = quantize_volume(bal_dec)
                                if s_vol and (s_vol * cur_p) >= MIN_ORDER_NOTIONAL_KRW:
                                    if LIVE_TRADING:
                                        await order_market_sell(access_key, secret_key, market, s_vol)
                                    ps.mark_sold(market, reason="WEAK_TP")
                                break

            # [B] 지지선 반등 확인형 물타기 실행 (BTC EMERGENCY 상태가 아닐 때만 허용)
            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 추가 물타기(물타기 방어)를 중단합니다.")
            else:
                used_krw_add = await process_strategic_additional_buys(access_key, secret_key, enriched, ps,
                                                                       available_krw)
                available_krw -= used_krw_add

            # [C] 신규 매수 탐색 (★ BTC 상태 정지 장치 적용)
            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 모든 신규 매수 및 추세 진입을 전면 차단합니다.")
            elif btc_status == "CAUTION":
                print(f"⚠️ {btc_msg} -> 조정세 구간: 신규 매수를 일시 보류합니다.")
            else:
                # [NORMAL 상태일 때만 신규 매수 허용]
                used_krw_new = await process_new_candidate_buys(access_key, secret_key, ps, remaining_slots,
                                                                available_krw)
                available_krw -= used_krw_new

            # 1일 1회 정기 리셋
            if PERIODIC_RESET_SEC > 0 and (loop_now - start_ts) >= PERIODIC_RESET_SEC:
                raise ResetRequested()

        except ResetRequested:
            raise
        except Exception as e:
            print(f"[MONITOR][ERR] {e}")

        await asyncio.sleep(INTERVAL_SECONDS)


# ============================================================
# 17. 컨트롤러 및 진입점
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task = None
    print(f"[CTRL] Controller 시작 (User: {user_no}, Server: {server_no})")

    while True:
        try:
            await sync_redis_config(user_no)

            cfg = {}
            c = await redis_client.get(f"mtpond:setup:{user_no}")
            if c:
                try:
                    data = json.loads(c)
                    if isinstance(data, list):
                        cfg = data[0] if len(data) > 0 else {}
                    elif isinstance(data, dict):
                        cfg = data
                except Exception as json_err:
                    print(f"[CTRL][WARN] JSON 파싱 오류: {json_err}")

            if not cfg and API_BASE:
                try:
                    async with httpx.AsyncClient(timeout=4.0) as cli:
                        r = await cli.get(f"{API_BASE}/api/mtpondsetup/{user_no}")
                        if r.status_code == 200:
                            d = r.json()
                            if isinstance(d, list):
                                cfg = d[0] if len(d) > 0 else {}
                            elif isinstance(d, dict):
                                cfg = d
                except Exception:
                    pass

            if cfg:
                apply_dynamic_config(cfg)

            active_flag = str(cfg.get("activeYN", "Y")).upper() == "Y"

            if task and task.done():
                exc = task.exception()
                if isinstance(exc, ResetRequested):
                    print("[CTRL] 1일 정기 리셋 완료 → 모니터링 재기동")
                elif exc:
                    print(f"[CTRL] 모니터링 예외 복구 재기동: {exc}")
                task = None

            if active_flag and task is None:
                init_config(force=True)
                task = asyncio.create_task(monitor_positions(user_no, server_no))
            elif not active_flag and task:
                print("[CTRL] activeYN=N → 모니터링 태스크 중단")
                task.cancel()
                try:
                    await task
                except:
                    pass
                task = None

        except Exception as e:
            print(f"[CTRL][ERR] {type(e).__name__}: {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


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