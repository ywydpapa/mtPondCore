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
# 4. STATIC CONFIG (기본 상수)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))

COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "180"))  # 익절 후 재매수 방지 180초
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "300"))  # 손절 후 재매수 방지 300초

ACTIVE_CAPITAL_RATIO = Decimal("0.60")  # 총 자산의 60% 한도 투입
DEFENSE_TRIGGER_PCT = Decimal("-1.4")  # 방어 틱 사다리 재배치 (-1.4%)
HARD_STOP_LOSS_PCT = Decimal("-2.5")  # 최종 강제 시장가 청산 (-2.5%)

MIN_SLOT_NOTIONAL_KRW = Decimal("10000")  # 1슬롯 기본 10,000원
LADDER_START_PCT = Decimal("0.60")  # 첫 익절 배치 시작선 (+0.60%)
LADDER_STEP_PCT = Decimal("0.30")  # 단계별 간격 (+0.30%)

INTERVAL_SECONDS = 3
MIN_NOTIONAL_KRW = Decimal("5000")
MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

# ============================================================
# 5. RUNTIME CONFIG (Redis 우선 동적 설정 관리)
# ============================================================
_CONFIG_INITIALIZED = False
MAX_ACTIVE_MARKETS = 2
RANGE_BUY_KRW = Decimal("40000")  # 초기 폴백값 (Redis 로드 시 즉시 덮어씌워짐)
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
                "entry_ts": now,
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
    """환경변수 기반 1차 초기화 (Redis 연결 전 기본 틀 마련)"""
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
    print(f"[INIT] 시스템 1차 설정 완료 | 종목당 초기할당: {RANGE_BUY_KRW:,.0f}원, 활성최대: {MAX_ACTIVE_MARKETS}개")


def apply_dynamic_config(cfg: dict):
    """★ Redis 설정값을 전역 변수에 최우선 반영"""
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    def to_dec(v):
        try:
            return Decimal(str(v)) if v not in (None, "") else None
        except:
            return None

    # 1. 최초 투입 금액 우선 반영 (initAmt, range_buy_krw, firstBuyAmt 등 포괄 수용)
    for key in ("initAmt", "range_buy_krw", "RANGE_BUY_KRW", "firstBuyAmt", "init_amt"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0:
                old_val = RANGE_BUY_KRW
                RANGE_BUY_KRW = v
                if old_val != v:
                    print(f"📌 [REDIS-CONFIG] 최초 투입 금액 갱신: {old_val:,.0f}원 → {RANGE_BUY_KRW:,.0f}원")
                break

    # 2. 최대 종목 수 반영 (설정값이 없으면 투입 금액에 맞춰 자동 산정)
    has_custom_cnt = False
    for key in ("maxCoincnt", "max_active_markets", "MAX_ACTIVE_MARKETS"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv > 0:
                    MAX_ACTIVE_MARKETS = nv
                    has_custom_cnt = True
                    break
            except:
                pass

    if not has_custom_cnt:
        MAX_ACTIVE_MARKETS = 5 if RANGE_BUY_KRW <= Decimal("25000") else 2


async def sync_redis_config(user_no: int):
    """Redis에서 설정 JSON을 읽어와 즉시 반영"""
    try:
        c = await redis_client.get(f"mtpond:setup:{user_no}")
        if c:
            data = json.loads(c)
            cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
            if isinstance(cfg, dict):
                apply_dynamic_config(cfg)
                return True
    except Exception as e:
        print(f"[CONFIG][WARN] Redis 설정 동기화 실패: {e}")
    return False


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

        return bool((cur_price >= ma5) and (ma5 > ma20) and (cur_price >= open_price))
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
# 12. 사다리 계산 및 발주
# ============================================================
def calculate_ladder_slots(total_eval: Decimal, avg_price: Decimal) -> Tuple[int, Decimal, Decimal]:
    amt = float(total_eval)
    p = float(avg_price)

    if p >= 100_000:
        base_start = Decimal("0.45")
        base_step = Decimal("0.20")
    elif p >= 10_000:
        base_start = Decimal("0.55")
        base_step = Decimal("0.25")
    else:
        base_start = Decimal("0.60")
        base_step = Decimal("0.30")

    if amt <= 55_000:
        slot_count = max(1, int(total_eval // Decimal("10000")))
    elif amt <= 155_000:
        slot_count = min(10, max(5, int(total_eval // Decimal("10000"))))
    elif amt <= 305_000:
        slot_count = min(10, max(6, int(total_eval // Decimal("20000"))))
    else:
        slot_count = min(10, max(8, int(total_eval // Decimal("40000"))))

    return max(1, slot_count), base_start, base_step


async def place_ladder_tp_orders(access_key: str, secret_key: str, market: str, ps: PositionState):
    st = ps.data.setdefault(market, {})
    if st.get("is_processing_order"):
        return
    st["is_processing_order"] = True

    try:
        base = market.split("-")[1]
        avail_bal = Decimal("0")
        avg_price = Decimal("0")

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

        slot_count, start_pct, step_pct = calculate_ladder_slots(total_eval, avg_price)
        slice_vol = quantize_volume(avail_bal / Decimal(str(slot_count)))
        remaining_vol = avail_bal
        prev_limit_p = Decimal("0")

        print(
            f"📐 [LADDER-TP] {market} 익절 사다리 발주 | "
            f"평단: {avg_price:,.0f}원 | 총액: {total_eval:,.0f}원 ({slot_count}슬롯) | "
            f"시작: +{start_pct}% / 간격: +{step_pct}%"
        )

        for i in range(1, slot_count + 1):
            curr_target_pct = start_pct + (step_pct * Decimal(str(i - 1)))
            target_p = avg_price * (Decimal("1") + (curr_target_pct / Decimal("100")))
            limit_p = adjust_price_to_tick(target_p, side="ask")

            if i > 1 and limit_p <= prev_limit_p:
                tick = get_tick_unit(limit_p)
                limit_p = prev_limit_p + tick

            order_v = quantize_volume(remaining_vol) if i == slot_count else slice_vol
            order_v = min(order_v, remaining_vol)

            if order_v * limit_p >= MIN_ORDER_NOTIONAL_KRW:
                if LIVE_TRADING:
                    await order_limit_sell(access_key, secret_key, market, order_v, limit_p)
                print(f"  └ [{market}] 익절 슬롯 #{i}: {order_v}개 @ {limit_p}원 (+{curr_target_pct:.2f}%)")
                remaining_vol -= order_v
                prev_limit_p = limit_p
                await asyncio.sleep(0.15)

        st["last_order_ts"] = time.time()

    except Exception as e:
        print(f"[LADDER-TP][ERR] {market} 사다리 주문 배치 실패: {e}")
    finally:
        st["is_processing_order"] = False


# ============================================================
# 13. 방어형 틱 리밸런싱
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
# 14. 메인 모니터링 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()
    last_cfg_sync = time.time()

    # ★ 진입 전 Redis 설정을 즉시 1회 동기화하여 최초 투입 금액 우선 반영
    await sync_redis_config(user_no)

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
    print(f"=== [단기 모멘텀 스캘핑 엔진 가동] 종목당 투입예산: {RANGE_BUY_KRW:,.0f}원 | 최대종목: {MAX_ACTIVE_MARKETS}개 ===")
    excluded = await get_excluded_markets(force_reload=True)

    while True:
        try:
            loop_now = time.time()

            # 주기적 Redis 설정 재동기화
            if loop_now - last_cfg_sync >= CONTROLLER_POLL_SEC:
                last_cfg_sync = loop_now
                await sync_redis_config(user_no)

            excluded = await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)

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

            total_equity = available_krw + total_coin_val
            active_capital_limit = total_equity * ACTIVE_CAPITAL_RATIO
            current_invested = total_coin_val
            available_invest_krw = max(Decimal("0"), active_capital_limit - current_invested)

            # 포지션 감시
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                cur_p = pos.get("current_price")
                avg_p = pos.get("avg_buy_price")
                avail_vol = pos["balance"]

                if cur_p is None or avg_p is None or cur_p <= 0 or avg_p <= 0:
                    continue

                st = ps.update_or_init(market, pnl, avg_p)

                # 최종 손절
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

                # 방어 틱 사다리
                if pnl <= DEFENSE_TRIGGER_PCT and not st.get("defense_mode"):
                    asyncio.create_task(place_defense_ladder_orders(access_key, secret_key, market, ps))
                    continue

                # 잔여 수량 재배치
                if not st.get("defense_mode") and not st.get("is_processing_order"):
                    if avail_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                        if loop_now - st.get("last_order_ts", 0.0) >= 3.0:
                            print(f"🔄 [AUTO-REALLOC] {market} 미배치 잔여 수량 ({avail_vol}개) → 사다리 재배치")
                            asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market, ps))

            # 매도 완료 정리
            for mkt in list(ps.data.keys()):
                if mkt not in [p["market"] for p in enriched]:
                    is_defense = ps.data[mkt].get("defense_mode", False)
                    ps.mark_sold(mkt, reason="DEFENSE" if is_defense else "TP")

            # 신규 매수 (Redis 투입 예산 RANGE_BUY_KRW 반영)
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

                    if not await check_bullish_momentum(market):
                        continue

                    buy_amt = min(RANGE_BUY_KRW, available_krw, available_invest_krw)
                    if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                        continue

                    try:
                        if LIVE_TRADING:
                            resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                            print(f"🚀 [BUY] {market} 모멘텀 진입: {buy_amt:,.0f}원 (uuid={resp.get('uuid')})")
                        else:
                            print(f"🚀 [BUY][DRY] {market} 모멘텀 진입: {buy_amt:,.0f}원")

                        ps.update_or_init(market, Decimal("0"), item["price"])
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market, ps))

                        available_krw -= buy_amt
                        available_invest_krw -= buy_amt
                        current_invested += buy_amt
                        remaining_slots -= 1
                        await asyncio.sleep(1.0)

                    except Exception as e:
                        print(f"[BUY-ERR] {market} 매수 실패: {e}")

            if PERIODIC_RESET_SEC > 0 and (loop_now - start_ts) >= PERIODIC_RESET_SEC:
                raise ResetRequested()

        except ResetRequested:
            raise
        except Exception as e:
            print(f"[MONITOR][ERR] {e}")

        await asyncio.sleep(INTERVAL_SECONDS)


# ============================================================
# 15. 컨트롤러 및 진입점
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task = None
    print(f"[CTRL] Controller 시작 (User: {user_no}, Server: {server_no})")

    while True:
        try:
            # 1. Redis 최신 설정 먼저 로드
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

    # ★ 프로그램 진입 즉시 Redis 설정을 1차 동기화
    await sync_redis_config(user_no)

    await run_mtpond_controller(user_no, server_no)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청 수신")