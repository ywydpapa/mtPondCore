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

PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "86400"))


class ResetRequested(Exception):
    pass


# ============================================================
# 4. STATIC CONFIG (스캘핑 & 피라미딩 파라미터)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))
COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "60"))  # 익절 쿨다운 1분
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "300"))  # 방어/손절 쿨다운 5분

# 자금 배분 규칙
ACTIVE_CAPITAL_RATIO = Decimal("0.60")  # 총 자산의 60% 한도 투입

# [손절 및 방어 설정]
DEFENSE_TRIGGER_PCT = Decimal("-1.2")  # 방어 틱 사다리 재배치 (-1.2%)
HARD_STOP_LOSS_PCT = Decimal("-2.2")  # 최종 강제 시장가 청산 (-2.2%)

# [익절 및 사다리 설정]
MIN_SLOT_NOTIONAL_KRW = Decimal("10000")  # 1슬롯 기본 10,000원
LADDER_START_PCT = Decimal("0.3")  # 첫 익절 배치 시작선 (+0.3%)
LADDER_STEP_PCT = Decimal("0.3")  # 단계별 간격 (+0.3%)

# [피라미딩(불타기) 설정]
PYRAMID_BUY_KRW = Decimal("20000")  # 마지막 슬롯 잔여 시 추가 매수 금액 (20,000원)
MAX_PYRAMID_COUNT = 1  # 종목당 불타기 허용 횟수 (최대 1회로 제한)
PYRAMID_TRIGGER_MAX_EVAL = Decimal("14000")  # 마지막 1슬롯(약 1만원 부근) 잔여 판별 상한선

INTERVAL_SECONDS = 3
MIN_NOTIONAL_KRW = Decimal("5000")
MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

# ============================================================
# 5. RUNTIME CONFIG (Redis 연동 동적 설정 관리)
# ============================================================
_CONFIG_INITIALIZED = False
MAX_ACTIVE_MARKETS = 2
RANGE_BUY_KRW = Decimal("40000")
BB_MIN_OVERALL_SCORE = 3
BB_MIN_TRADE_COUNT_10M = 60


class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.sell_history: Dict[str, Dict[str, Any]] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None:
            self.data[market] = {
                "peak_pnl": pnl,
                "avg_buy_price": avg_price,
                "defense_mode": False,
                "is_processing_order": False,
                "pyramid_count": 0,  # 불타기 실행 횟수
                "last_order_ts": 0.0,
                "last_update_ts": now
            }
            return self.data[market]

        if st.get("avg_buy_price") != avg_price and avg_price > 0:
            st["avg_buy_price"] = avg_price
            st["peak_pnl"] = pnl
            st["defense_mode"] = False
            st["is_processing_order"] = False
            st["last_update_ts"] = now
            return st

        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl

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
        print(f"[COOLDOWN] {market} 사이클 정리 완료 [{tag}: {reason}] → 재매수 금지 {cooldown_sec}초 적용")

    def recently_sold(self, market: str) -> bool:
        hist = self.sell_history.get(market)
        if not hist:
            return False
        return time.time() < hist["expire_ts"]


def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
    default_markets = 5 if RANGE_BUY_KRW <= Decimal("25000") else 2
    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", str(default_markets)))

    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "60"))

    _CONFIG_INITIALIZED = True
    print(f"[INIT] 설정 초기화 완료 | 활성최대:{MAX_ACTIVE_MARKETS}개, 종목당할당:{RANGE_BUY_KRW}원")


def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    def to_dec(v):
        try:
            return Decimal(str(v)) if v not in (None, "") else None
        except:
            return None

    for key in ("initAmt", "range_buy_krw", "RANGE_BUY_KRW"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0:
                RANGE_BUY_KRW = v

    for key in ("maxCoincnt", "max_active_markets", "MAX_ACTIVE_MARKETS"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv > 0: MAX_ACTIVE_MARKETS = nv
            except:
                pass
        else:
            MAX_ACTIVE_MARKETS = 5 if RANGE_BUY_KRW <= Decimal("25000") else 2


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
# 6. Exclude Markets
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


def quantize_volume(vol: Decimal) -> Decimal:
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


# ============================================================
# 8. 틱 단위 및 호가 연산
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


def get_dynamic_ladder_config(price: Decimal) -> Tuple[Decimal, Decimal]:
    """
    코인 가격대에 맞춰 시작 익절 퍼센트와 스텝 간격을 동적으로 반환
    반환값: (start_pct, step_pct)
    """
    p = float(price)
    if p >= 100_000:       # 10만 원 이상 (BTC, BCH, SOL 등)
        return Decimal("0.12"), Decimal("0.08")
    elif p >= 10_000:      # 1만 원 ~ 10만 원 미만
        return Decimal("0.18"), Decimal("0.12")
    elif p >= 1_000:       # 1천 원 ~ 1만 원 미만
        return Decimal("0.25"), Decimal("0.20")
    else:  # 1천 원 미만 동전주
        return Decimal("0.30"), Decimal("0.30")


def adjust_price_to_tick(raw_price: Decimal, side: str = "ask") -> Decimal:
    price = Decimal(str(raw_price))
    if price <= 0: return Decimal("0")
    unit = get_tick_unit(price)
    steps = price / unit
    mode = ROUND_CEILING if side == "ask" else ROUND_FLOOR
    return (steps.to_integral_value(rounding=mode) * unit).quantize(Decimal("1") if unit >= 1 else unit)


async def get_orderbook_top(market: str) -> Tuple[Decimal, Decimal]:
    url = "https://api.upbit.com/v1/orderbook"
    params = {"markets": market}
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    bids = data[0].get("orderbook_units", [])
    return Decimal(str(bids[0]["bid_price"])), Decimal(str(bids[0]["ask_price"]))


async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets: return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(markets)}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return {item["market"]: Decimal(str(item["trade_price"])) for item in r.json() if
                        item.get("trade_price") is not None}
    except Exception:
        pass
    return {}


async def cancel_all_open_orders_for_market(access_key: str, secret_key: str, market: str):
    try:
        orders = await upbit_request(
            "GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
            params={"market": market, "state": "wait"}, max_retry=2
        )
        if not orders or not isinstance(orders, list):
            return

        tasks = [
            upbit_request("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key,
                          params={"uuid": od["uuid"]}, max_retry=2)
            for od in orders if isinstance(od, dict) and od.get("uuid")
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"[CANCEL-ALL][ERR] {market} 미체결 취소 실패: {e}")


def calculate_ladder_slots(total_eval: Decimal) -> Tuple[int, Decimal, Decimal]:
    """
    투자금액에 따라 (슬롯 개수, 시작 익절%, 스텝 증가폭%)을 동적으로 산출
    반환값: (slot_count, start_pct, step_pct)
    """
    amt = float(total_eval)

    if amt <= 55_000:          # [4만 ~ 5만 원]: 1만 원 단위 4~5슬롯
        slot_unit = Decimal("10000")
        slot_count = int(total_eval // slot_unit)
        start_pct = Decimal("0.30")
        step_pct = Decimal("0.20")

    elif amt <= 155_000:       # [6만 ~ 15만 원]: 1만 원 단위, 최대 10슬롯으로 압축
        slot_count = min(10, max(5, int(total_eval // Decimal("10000"))))
        start_pct = Decimal("0.25")
        step_pct = Decimal("0.12")

    elif amt <= 305_000:       # [16만 ~ 30만 원]: 2만 원 단위 격상 (8~10슬롯)
        slot_unit = Decimal("20000")
        slot_count = min(10, max(6, int(total_eval // slot_unit)))
        start_pct = Decimal("0.22")
        step_pct = Decimal("0.14")

    else:                      # [31만 ~ 50만 원]: 3만~5만 원 단위 격상 (8~10슬롯 유지)
        slot_unit = Decimal("40000")
        slot_count = min(10, max(8, int(total_eval // slot_unit)))
        start_pct = Decimal("0.20")
        step_pct = Decimal("0.16")

    return max(1, slot_count), start_pct, step_pct



# ============================================================
# 9. 상승 모멘텀 검증 (15분봉 정배열 & 실시간 양봉)
# ============================================================
async def check_bullish_momentum(market: str) -> bool:
    url_15m = "https://api.upbit.com/v1/candles/minutes/15"
    params = {"market": market, "count": 20}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url_15m, params=params)
            if resp.status_code != 200:
                return False
            candles = resp.json()

        if not candles or len(candles) < 20:
            return False

        cur_price = Decimal(str(candles[0]["trade_price"]))
        open_price = Decimal(str(candles[0]["opening_price"]))

        closes = [Decimal(str(c["trade_price"])) for c in candles]
        ma5 = sum(closes[:5]) / Decimal("5")
        ma20 = sum(closes[:20]) / Decimal("20")

        return bool((cur_price >= ma5) and (ma5 >= ma20) and (cur_price >= open_price))
    except Exception as e:
        print(f"[MOMENTUM][WARN] {market} 모멘텀 확인 실패: {e}")
        return False


# ============================================================
# 10. 후보 발굴
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


# ============================================================
# 12. [동적 적용] 4만~50만원 전 구간 대응 사다리 배치 함수
# ============================================================
async def place_ladder_tp_orders(
    access_key: str,
    secret_key: str,
    market: str,
    ps: PositionState,
    start_step_override: Decimal = None
):
    st = ps.data.setdefault(market, {})
    if st.get("is_processing_order"):
        return
    st["is_processing_order"] = True

    try:
        base = market.split("-")[1]
        avail_bal = Decimal("0")
        avg_price = Decimal("0")

        # 체결 잔고 확정 대기 (최대 4초 폴링)
        for _ in range(4):
            await asyncio.sleep(1.0)
            accounts = await fetch_upbit_accounts(access_key, secret_key)
            target_acc = next((acc for acc in accounts if acc.get("currency") == base), None)
            if target_acc:
                avail_bal = Decimal(str(target_acc.get("balance", "0")))
                avg_price = Decimal(str(target_acc.get("avg_buy_price", "0")))
                if avail_bal * avg_price >= MIN_ORDER_NOTIONAL_KRW:
                    break

        total_eval = avail_bal * avg_price
        if total_eval < MIN_ORDER_NOTIONAL_KRW:
            return

        # ★ [핵심] 4만 ~ 50만 원 전 구간 동적 슬롯 계산
        slot_count, def_start_pct, step_pct = calculate_ladder_slots(total_eval)
        start_pct = start_step_override if start_step_override else def_start_pct

        slice_vol = quantize_volume(avail_bal / Decimal(str(slot_count)))
        remaining_vol = avail_bal
        prev_limit_p = Decimal("0")

        print(
            f"📐 [LADDER-TP] {market} 동적 사다리 배치 시작 | "
            f"투자평가액: {total_eval:,.0f}원 | {slot_count}개 슬롯 | "
            f"시작: +{start_pct}% / 스텝: +{step_pct}%"
        )

        for i in range(1, slot_count + 1):
            curr_target_pct = start_pct + (step_pct * Decimal(str(i - 1)))
            target_p = avg_price * (Decimal("1") + (curr_target_pct / Decimal("100")))
            limit_p = adjust_price_to_tick(target_p, side="ask")

            # 틱 겹침 방지: 최소 1틱 이상 강제 상승
            if i > 1 and limit_p <= prev_limit_p:
                tick = get_tick_unit(limit_p)
                limit_p = prev_limit_p + tick

            # 마지막 슬롯은 잔여 오차 없이 전량 털어내도록 수량 일치
            order_v = quantize_volume(remaining_vol) if i == slot_count else slice_vol
            order_v = min(order_v, remaining_vol)

            if order_v * limit_p >= MIN_ORDER_NOTIONAL_KRW:
                if LIVE_TRADING:
                    await order_limit_sell(access_key, secret_key, market, order_v, limit_p)
                print(f"  └ [{market}] 슬롯 #{i}: {order_v}개 @ {limit_p}원 (+{curr_target_pct:.2f}%)")
                remaining_vol -= order_v
                prev_limit_p = limit_p
                await asyncio.sleep(0.15) # 업비트 API 초당 호출제한(초당 8회) 준수

        st["last_order_ts"] = time.time()

    except Exception as e:
        print(f"[LADDER-TP][ERR] {market} 사다리 주문 배치 실패: {e}")
    finally:
        st["is_processing_order"] = False


# ============================================================
# 13. 손절 시 바로 위 틱(+1~3틱) 반등 탈출 사다리 재배치
# ============================================================
async def place_defense_ladder_orders(access_key: str, secret_key: str, market: str, ps: PositionState):
    st = ps.data.setdefault(market, {})
    if st.get("is_processing_order"):
        return
    st["is_processing_order"] = True
    st["defense_mode"] = True

    try:
        print(f"🛡️ [DEFENSE-REBALANCE] {market} 손절선 도달! 주문 전량 취소 후 호가 +1~3틱 탈출 재배치")
        await cancel_all_open_orders_for_market(access_key, secret_key, market)
        await asyncio.sleep(0.4)

        base = market.split("-")[1]
        accounts = await fetch_upbit_accounts(access_key, secret_key)
        target_acc = next((acc for acc in accounts if acc.get("currency") == base), None)
        if not target_acc:
            return

        avail_bal = Decimal(str(target_acc.get("balance", "0")))
        if avail_bal <= 0:
            return

        _, best_ask = await get_orderbook_top(market)
        tick = get_tick_unit(best_ask)

        total_eval = avail_bal * best_ask
        if total_eval < MIN_ORDER_NOTIONAL_KRW:
            return

        slot_count = 3 if total_eval >= (MIN_ORDER_NOTIONAL_KRW * Decimal("3")) else 1
        slice_vol = quantize_volume(avail_bal / Decimal(str(slot_count)))
        remaining_vol = avail_bal

        for i in range(1, slot_count + 1):
            target_p = best_ask + (tick * Decimal(str(i - 1)))
            limit_p = adjust_price_to_tick(target_p, side="ask")

            order_v = quantize_volume(remaining_vol) if i == slot_count else slice_vol
            order_v = min(order_v, remaining_vol)

            if order_v * limit_p >= MIN_ORDER_NOTIONAL_KRW:
                if LIVE_TRADING:
                    await order_limit_sell(access_key, secret_key, market, order_v, limit_p)
                print(f"  └ [방어탈출 슬롯 #{i}] {order_v}개 @ {limit_p}원 (호가 +{i - 1}틱)")
                remaining_vol -= order_v
                await asyncio.sleep(0.15)

        st["last_order_ts"] = time.time()

    except Exception as e:
        print(f"[DEFENSE][ERR] {market} 방어 재배치 실패: {e}")
    finally:
        st["is_processing_order"] = False


# ============================================================
# 14. [수정] 수익 담보형 피라미딩(불타기) 연장 함수
# ============================================================
async def process_pyramiding_extension(
        access_key: str,
        secret_key: str,
        market: str,
        ps: PositionState,
        available_krw: Decimal,
        available_invest_krw: Decimal
):
    """
    앞선 1~3차 익절로 원금과 수익을 확보한 상태에서,
    마지막 1슬롯만 남았고 15분봉 상승 추세가 견고할 때 추가 매수하여 사다리를 상방으로 연장
    """
    st = ps.data.setdefault(market, {})
    if st.get("is_processing_order") or st.get("defense_mode"):
        return
    if st.get("pyramid_count", 0) >= MAX_PYRAMID_COUNT:
        return

    buy_amt = min(PYRAMID_BUY_KRW, available_krw, available_invest_krw)
    if buy_amt < MIN_ORDER_NOTIONAL_KRW:
        return

    st["is_processing_order"] = True
    try:
        print(f"🔥 [PYRAMIDING] {market} 마지막 슬롯 잔여 & 상승 모멘텀 유지! 피라미딩 불타기 매수 집행 (금액: {buy_amt:,.0f}원)")

        # 1. 기존 마지막 슬롯 미체결 주문 취소
        await cancel_all_open_orders_for_market(access_key, secret_key, market)
        await asyncio.sleep(0.3)

        # 2. 시장가 추가 매수
        if LIVE_TRADING:
            await order_market_buy_price(access_key, secret_key, market, buy_amt)

        st["pyramid_count"] += 1
        st["last_order_ts"] = time.time()
        print(f"✅ [PYRAMIDING-SUCCESS] {market} 불타기 완료! (누적 {st['pyramid_count']}회) → 사다리 상방 연장 재배치")

        # 3. 평단가 참조 버그 수정 (st에 기록된 avg_buy_price 활용)
        current_avg_price = Decimal(str(st.get("avg_buy_price", "0")))
        dyn_start, _ = get_dynamic_ladder_config(current_avg_price)
        pyramid_start_pct = (dyn_start * Decimal("1.5")).quantize(Decimal("0.01"))

        # 4. 상방 확장 사다리 재배치 트리거
        asyncio.create_task(
            place_ladder_tp_orders(
                access_key,
                secret_key,
                market,
                ps,
                start_step_override=pyramid_start_pct
            )
        )

    except Exception as e:
        print(f"[PYRAMIDING][ERR] {market} 불타기 실패: {e}")
    finally:
        st["is_processing_order"] = False


# ============================================================
# 15. 메인 모니터링 루프
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
    print("=== [단기 모멘텀 스캘핑 + 방어형 손절 + 수익 담보 불타기 엔진 가동] ===")
    excluded = await get_excluded_markets(force_reload=True)

    while True:
        try:
            loop_now = time.time()

            if loop_now - last_cfg_sync >= CONTROLLER_POLL_SEC:
                last_cfg_sync = loop_now
                await sync_redis_config(user_no)

            excluded = await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)

            # 1. 원화 및 코인 잔고 조회
            available_krw = Decimal("0")
            held_markets = []
            for a in raw_accounts:
                cur = a.get("currency")
                if cur == "KRW":
                    available_krw = Decimal(str(a.get("balance", "0")))
                else:
                    mkt = f"KRW-{cur}"
                    if mkt not in excluded:
                        total_vol = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                        if total_vol > 0:
                            held_markets.append(mkt)

            price_map = await fetch_current_prices(held_markets)

            total_coin_val = Decimal("0")
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
                    total_coin_val += (bal + locked) * cur_price
                    pnl = ((cur_price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                    enriched.append({
                        "market": mkt, "balance": bal, "locked": locked,
                        "avg_buy_price": avg, "current_price": cur_price, "pnl_percent": pnl
                    })

            # 전체 자금(원화 + 코인 평가액) 기준 60% 한도 제어
            total_equity = available_krw + total_coin_val
            active_capital_limit = total_equity * ACTIVE_CAPITAL_RATIO
            current_invested = total_coin_val
            available_invest_krw = max(Decimal("0"), active_capital_limit - current_invested)

            # 2. 보유 포지션 수시 감시, 불타기, 방어손절
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                cur_p = pos.get("current_price")
                avg_p = pos.get("avg_buy_price")
                avail_vol = pos["balance"]
                total_vol = pos["balance"] + pos["locked"]
                total_pos_eval = total_vol * cur_p

                if cur_p is None or avg_p is None or cur_p <= 0 or avg_p <= 0:
                    continue

                st = ps.update_or_init(market, pnl, avg_p)

                # [A] 최종 안전 손절 (-2.2% 도달 시 반등 실패 전량 시장가 청산)
                if pnl <= HARD_STOP_LOSS_PCT:
                    print(f"🚨 [HARD-STOP-LOSS] {market} 방어선 붕괴 ({pnl}%) → 시장가 전량 청산")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.4)
                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            b_dec = Decimal(str(a.get("balance", "0")))
                            s_vol = quantize_volume(b_dec)
                            if s_vol and (s_vol * cur_p) >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    await order_market_sell(access_key, secret_key, market, s_vol)
                                ps.mark_sold(market, reason="STOP_LOSS")
                            break
                    continue

                # [B] 방어 모드 진입: -1.2% 도달 시 현재가 바로 위 1~3틱에 분할 재배치
                if pnl <= DEFENSE_TRIGGER_PCT and not st.get("defense_mode"):
                    asyncio.create_task(place_defense_ladder_orders(access_key, secret_key, market, ps))
                    continue

                # [C] ★ [핵심 추가] 피라미딩(불타기) 조건 검증 및 연장
                # 조건: 방어 모드가 아니고, 불타기 미실행 상태이며, 총 잔여 평가액이 1구좌(약 14,000원 이하)로 압축된 경우
                is_last_slot_remaining = (total_pos_eval <= PYRAMID_TRIGGER_MAX_EVAL) and (
                            total_pos_eval >= MIN_ORDER_NOTIONAL_KRW)
                if not st.get("defense_mode") and st.get("pyramid_count",
                                                         0) < MAX_PYRAMID_COUNT and is_last_slot_remaining:
                    # 15분봉 정배열 및 양봉 상승 추세 지속 확인
                    if await check_bullish_momentum(market):
                        if available_krw >= MIN_ORDER_NOTIONAL_KRW and available_invest_krw >= MIN_ORDER_NOTIONAL_KRW:
                            asyncio.create_task(process_pyramiding_extension(
                                access_key, secret_key, market, ps, available_krw, available_invest_krw
                            ))
                            continue

                # [D] 수시 잔액 감시: 주문 취소 등으로 5,500원 이상의 잔여 코인이 놀고 있다면 즉시 사다리 재배치
                if not st.get("defense_mode") and not st.get("is_processing_order"):
                    if avail_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                        if loop_now - st.get("last_order_ts", 0.0) >= 3.0:
                            print(f"🔄 [AUTO-REALLOC] {market} 미배치 잔여 수량 ({avail_vol}개) → 사다리 재배치")
                            asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market, ps))

            # 전량 매도 완료된 종목 정리
            for mkt in list(ps.data.keys()):
                if mkt not in [p["market"] for p in enriched]:
                    is_defense = ps.data[mkt].get("defense_mode", False)
                    ps.mark_sold(mkt, reason="DEFENSE" if is_defense else "TP")

            # 3. 신규 매수 탐색 (슬롯 및 60% 한도 범위 내)
            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            if remaining_slots > 0 and available_krw >= MIN_ORDER_NOTIONAL_KRW and available_invest_krw >= MIN_ORDER_NOTIONAL_KRW:
                candidates = await fetch_bb_patterns_candidates()
                for item in candidates:
                    if remaining_slots <= 0:
                        break

                    market = item["market"]
                    if market in [p["market"] for p in enriched] or ps.recently_sold(market):
                        continue

                    # 15분봉 정배열 & 실시간 양봉 모멘텀 확인
                    if not await check_bullish_momentum(market):
                        continue

                    buy_amt = min(RANGE_BUY_KRW, available_krw, available_invest_krw)
                    if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                        continue

                    try:
                        if LIVE_TRADING:
                            resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                            print(f"🚀 [BUY] {market} 모멘텀 신규 진입: {buy_amt:,.0f}원 (uuid={resp.get('uuid')})")
                        else:
                            print(f"🚀 [BUY][DRY] {market} 신규 진입: {buy_amt:,.0f}원")

                        ps.update_or_init(market, Decimal("0"), item["price"])
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market, ps))

                        available_krw -= buy_amt
                        available_invest_krw -= buy_amt
                        current_invested += buy_amt
                        remaining_slots -= 1
                        await asyncio.sleep(1.0)

                    except Exception as e:
                        print(f"[BUY-ERR] {market} 매수 실패: {e}")

            # 24시간 1회 정기 리셋
            if PERIODIC_RESET_SEC > 0 and (loop_now - start_ts) >= PERIODIC_RESET_SEC:
                raise ResetRequested()

        except ResetRequested:
            raise
        except Exception as e:
            print(f"[MONITOR][ERR] {e}")

        await asyncio.sleep(INTERVAL_SECONDS)


# ============================================================
# 16. 컨트롤러 및 진입점
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
                    cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
                except Exception as json_err:
                    print(f"[CTRL][WARN] JSON 파싱 오류: {json_err}")

            if not cfg and API_BASE:
                try:
                    async with httpx.AsyncClient(timeout=4.0) as cli:
                        r = await cli.get(f"{API_BASE}/api/mtpondsetup/{user_no}")
                        if r.status_code == 200:
                            d = r.json()
                            cfg = d[0] if isinstance(d, list) and len(d) > 0 else d
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