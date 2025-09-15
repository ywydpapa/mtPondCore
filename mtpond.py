import os
import asyncio
import uuid
import hashlib
import urllib.parse
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Optional, Dict, Any, List, Tuple
import time
import math
import topuprise
import dotenv
import httpx
import jwt
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

dotenv.load_dotenv()

# =========================================
# DB
# =========================================
engine = create_async_engine(os.getenv("dburl"), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# =========================================
# 전략 / 환경 설정
# =========================================
WATCH_MIN_PNL = Decimal("-1.5")
ARM_PNL = Decimal("0.25")
HARD_TP = Decimal("1.5")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET

TRAIL_DROP = Decimal("0.15")
TRAIL_START_PNL = Decimal("0.45")

POLL_INTERVAL = 30
MIN_NOTIONAL_KRW = Decimal("5500")

SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "1.0"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "1.0"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))

ENABLE_RANGE_BUY = os.getenv("ENABLE_RANGE_BUY", "1") == "1"
BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-0.2"))
BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.15"))
RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "50000"))
MAX_BUY_PER_WINDOW = int(os.getenv("MAX_BUY_PER_WINDOW", "999"))
SKIP_BUY_IF_RECENT_SELL = True

COOLDOWN_AFTER_SELL = 10
BASE_UNIT = "KRW"

FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE

WHITELIST_MARKETS: List[str] = []
ENFORCE_WHITELIST = False

MAX_BACKOFF = 120
FIVE_MIN_SECONDS = 300

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

# =========================================
# 교집합(Intersection) 매수 전략 환경 변수
# =========================================
INTERSECTION_BUY_ENABLED = os.getenv("INTERSECTION_BUY_ENABLED", "1") == "1"
INTERSECTION_MIN_SCORE = Decimal(os.getenv("INTERSECTION_MIN_SCORE", "10"))
INTERSECTION_BUY_KRW = Decimal(os.getenv("INTERSECTION_BUY_KRW", "200000"))
INTERSECTION_MAX_BUY_PER_CYCLE = int(os.getenv("INTERSECTION_MAX_BUY_PER_CYCLE", "1"))
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "1200"))  # 20분

# =========================================
# 교집합 데이터 안전 처리 & 캐시 관련 새 환경 변수
# =========================================
INTERSECTION_USE_CACHE_ON_EMPTY = os.getenv("INTERSECTION_USE_CACHE_ON_EMPTY", "1") == "1"
INTERSECTION_CACHE_TTL_SEC = int(os.getenv("INTERSECTION_CACHE_TTL_SEC", "180"))
INTERSECTION_MAX_EMPTY_WARN = int(os.getenv("INTERSECTION_MAX_EMPTY_WARN", "5"))

# uprises() 결과 캐시 상태 (전역)
UPRISES_LAST_NONEMPTY: List[dict] = []
UPRISES_LAST_TS: float | None = None
UPRISES_EMPTY_STREAK: int = 0

# =========================================
# Upbit 관련
# =========================================
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"


# =========================================
# DB / 키 조회
# =========================================
async def get_keys(user_no: int, server_no: int) -> Optional[tuple]:
    async with SessionLocal() as session:
        sql = text("""
            SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        return result.fetchone()


# =========================================
# JWT 빌드
# =========================================
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


# =========================================
# HTTP 유틸
# =========================================
async def http_get_json(url: str, headers=None, params=None, timeout=10.0, max_retry=5):
    backoff = 2
    for attempt in range(1, max_retry + 1):
        try:
            to = httpx.Timeout(timeout, connect=5.0)
            async with httpx.AsyncClient(timeout=to) as client:
                resp = await client.get(url, headers=headers, params=params)
                if resp.status_code == 429:
                    print(f"[WARN] 429 Too Many Requests - 백오프 {backoff}s (attempt {attempt})")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                if resp.status_code >= 400:
                    raise RuntimeError(f"GET 실패 status={resp.status_code}, body={resp.text}")
                return resp.json()
        except Exception as e:
            if attempt == max_retry:
                raise
            print(f"[WARN] GET 오류: {e} (attempt {attempt}/{max_retry}) 백오프 {backoff}s")
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
                    print(f"[WARN] 429 POST - 백오프 {backoff}s (attempt {attempt})")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                if resp.status_code >= 400:
                    raise RuntimeError(f"POST 실패 status={resp.status_code}, body={resp.text}")
                return resp.json()
        except Exception as e:
            if attempt == max_retry:
                raise
            print(f"[WARN] POST 오류: {e} (attempt {attempt}/{max_retry}) 백오프 {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


# =========================================
# Upbit 계좌/가격
# =========================================
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
    price_map: Dict[str, Decimal] = {}
    for item in raw:
        market = item.get("market")
        trade_price = item.get("trade_price")
        if market and trade_price is not None:
            try:
                price_map[market] = Decimal(str(trade_price))
            except InvalidOperation:
                continue
    return price_map


def build_market_list_from_accounts(accounts, base_unit="KRW") -> List[str]:
    markets = []
    for acc in accounts:
        currency = acc.get("currency")
        unit = acc.get("unit_currency")
        if not currency or not unit:
            continue
        if unit != base_unit:
            continue
        if currency == base_unit:
            continue
        bal = acc.get("balance", "0")
        locked = acc.get("locked", "0")
        try:
            bal_d = Decimal(str(bal))
            locked_d = Decimal(str(locked))
        except:
            bal_d = Decimal("0")
            locked_d = Decimal("0")
        if bal_d == 0 and locked_d == 0:
            continue
        m = f"{base_unit}-{currency}"
        if ENFORCE_WHITELIST and WHITELIST_MARKETS and m not in WHITELIST_MARKETS:
            continue
        markets.append(m)
    seen = set()
    uniq = []
    for m in markets:
        if m not in seen:
            seen.add(m)
            uniq.append(m)
    return uniq


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


def enrich_accounts_with_prices(accounts: List[dict], price_map: Dict[str, Decimal], base_unit="KRW") -> List[dict]:
    enriched = []
    for acc in accounts:
        currency = acc.get("currency")
        unit = acc.get("unit_currency")
        avg_raw = acc.get("avg_buy_price")
        try:
            avg_buy_price = Decimal(str(avg_raw)) if avg_raw not in (None, "", "0") else Decimal("0")
        except InvalidOperation:
            avg_buy_price = Decimal("0")

        market = None
        current_price = None
        pnl_percent = None
        ratio = None
        if currency and unit == base_unit and currency != base_unit:
            market = f"{base_unit}-{currency}"
            current_price = price_map.get(market)
            if current_price is not None and avg_buy_price > 0:
                try:
                    pnl_percent = ((current_price - avg_buy_price) / avg_buy_price * Decimal("100")).quantize(Decimal("0.01"))
                    ratio = (current_price / avg_buy_price).quantize(Decimal("0.0001"))
                except:
                    pass

        def to_decimal(v):
            try:
                return Decimal(str(v))
            except:
                return Decimal("0")

        balance = to_decimal(acc.get("balance"))
        locked = to_decimal(acc.get("locked"))

        enriched.append({
            "currency": currency,
            "market": market,
            "unit_currency": unit,
            "balance": balance,
            "locked": locked,
            "avg_buy_price": avg_buy_price if avg_buy_price > 0 else None,
            "current_price": current_price,
            "pnl_percent": pnl_percent,
            "ratio_cur_over_avg": ratio
        })
    return enriched


# =========================================
# 주문
# =========================================
async def order_market_sell(access_key: str, secret_key: str, market: str, volume: Decimal) -> dict:
    params = {
        "market": market,
        "side": "ask",
        "volume": str(volume),
        "ord_type": "market",
    }
    jwt_token = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)


async def order_market_buy_price(access_key: str, secret_key: str, market: str, krw_amount: Decimal) -> dict:
    params = {
        "market": market,
        "side": "bid",
        "price": str(krw_amount),
        "ord_type": "price"
    }
    jwt_token = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)


async def get_order(access_key: str, secret_key: str, uuid_: str) -> dict:
    params = {"uuid": uuid_}
    jwt_token = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/json"
    }
    url = "https://api.upbit.com/v1/order"
    return await http_get_json(url, headers=headers, params=params)


# =========================================
# 상태 객체
# =========================================
class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}
        self.last_buy_window: Dict[str, float] = {}
        self.intersection_last_buy_time: Dict[str, float] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,
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
                "entry_source": self.data.get(market, {}).get("entry_source")
            }
            return self.data[market]
        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        st["prev_pnl"] = pnl
          # last_update_ts 업데이트
        st["last_update_ts"] = now
        return st

    def remove(self, market: str):
        if market in self.data:
            del self.data[market]

    def mark_sold(self, market: str):
        self.last_sell_time[market] = time.time()
        self.remove(market)

    def recently_sold(self, market: str) -> bool:
        ts = self.last_sell_time.get(market)
        if not ts:
            return False
        return (time.time() - ts) < 10

    def record_buy_window(self, market: str, window_start: float):
        self.last_buy_window[market] = window_start

    def bought_this_window(self, market: str, window_start: float) -> bool:
        return self.last_buy_window.get(market) == window_start

    def mark_intersection_buy(self, market: str):
        self.intersection_last_buy_time[market] = time.time()

    def recently_bought_intersection(self, market: str, cooldown: int) -> bool:
        ts = self.intersection_last_buy_time.get(market)
        if not ts:
            return False
        return (time.time() - ts) < cooldown


def apply_momentum_extension(state: dict):
    # (원래 자리 - 생략 가능)
    pass


def get_state_decimal(state: dict, key: str, default: Decimal) -> Decimal:
    v = state.get(key)
    if isinstance(v, Decimal):
        return v
    try:
        if v is not None:
            return Decimal(str(v))
    except:
        pass
    return default


def decide_sell(market: str, pnl: Decimal, state: dict) -> Tuple[bool, str, Optional[str]]:
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)
    hard_tp2_target = get_state_decimal(state, "dynamic_hard_tp2", HARD_TP2_BASE)

    if hard_tp_taken and (not hard_tp2_taken) and pnl >= hard_tp2_target:
        label = "HARD_TP2"
        if state.get("dynamic_hard_tp2") is not None and hard_tp2_target != HARD_TP2_BASE:
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


def safe_calc_volume(balance: Decimal, portion: Decimal) -> Decimal:
    portion = min(portion, Decimal("1"))
    vol = balance if portion >= 1 else balance * portion
    vol = vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    if vol <= Decimal("0"):
        return Decimal("0")
    return vol


async def align_to_half_minute():
    now = time.time()
    remainder = now % 30
    if remainder > 0.01:
        await asyncio.sleep(30 - remainder)


def is_five_minute_boundary(ts: float) -> Tuple[bool, float]:
    window_start = ts - (ts % FIVE_MIN_SECONDS)
    lt = time.localtime(ts)
    if lt.tm_min % 5 == 0 and lt.tm_sec == 0:
        return True, window_start
    return False, window_start


async def sleep_until_next_boundary():
    now = time.time()
    next_boundary = math.floor(now / 30) * 30 + 30
    await asyncio.sleep(max(0, next_boundary - now))


# =========================================
# 교집합 데이터 안전 처리 유틸
# =========================================
def _is_effectively_empty(candidates: List[dict]) -> bool:
    if not candidates:
        return True
    valid = any((c.get("market") and c.get("avg_score") is not None)
                for c in candidates if isinstance(c, dict))
    return not valid


def _normalize_uprises(raw) -> List[dict]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            m = r.get("market")
            s = r.get("avg_score")
            if m and s is not None:
                out.append({"market": m, "avg_score": s})
        return out
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            js = json.loads(txt)
        except Exception:
            return []
        return _normalize_uprises(js)
    if isinstance(raw, dict):
        if "multi_tf_intersection" in raw:
            items = raw.get("multi_tf_intersection", {}).get("items", [])
            return _normalize_uprises(items)
        if "items" in raw and isinstance(raw["items"], list):
            return _normalize_uprises(raw["items"])
        if raw.get("market") and raw.get("avg_score") is not None:
            return [{"market": raw["market"], "avg_score": raw["avg_score"]}]
        return []
    return []


async def get_intersection_candidates_safe() -> Tuple[List[dict], dict]:
    """
    uprises() 호출 + empty 처리 + 캐시 재사용
    meta: {
      source: fresh|cache|empty,
      empty_streak: int,
      fresh_ts: float or None,
      cache_age: float or None
    }
    """
    global UPRISES_LAST_NONEMPTY, UPRISES_LAST_TS, UPRISES_EMPTY_STREAK

    meta = {
        "source": None,
        "empty_streak": UPRISES_EMPTY_STREAK,
        "fresh_ts": None,
        "cache_age": None
    }

    try:
        raw = topuprise.uprises()
    except Exception as e:
        raw = None
        print(f"[WARN] uprises() 호출 예외: {e}")

    candidates = _normalize_uprises(raw)
    now = time.time()
    meta["fresh_ts"] = now

    if _is_effectively_empty(candidates):
        UPRISES_EMPTY_STREAK += 1
        meta["empty_streak"] = UPRISES_EMPTY_STREAK

        use_cache = False
        if (INTERSECTION_USE_CACHE_ON_EMPTY
                and UPRISES_LAST_NONEMPTY
                and UPRISES_LAST_TS is not None):
            age = now - UPRISES_LAST_TS
            meta["cache_age"] = age
            if age <= INTERSECTION_CACHE_TTL_SEC:
                use_cache = True

        if use_cache:
            meta["source"] = "cache"
            candidates = UPRISES_LAST_NONEMPTY
            if UPRISES_EMPTY_STREAK % INTERSECTION_MAX_EMPTY_WARN == 0:
                print(f"[INFO] uprises 빈응답 {UPRISES_EMPTY_STREAK}회 연속 → 캐시 재사용 age={int(meta['cache_age'])}s size={len(candidates)}")
        else:
            meta["source"] = "empty"
            if UPRISES_EMPTY_STREAK == INTERSECTION_MAX_EMPTY_WARN:
                print(f"[WARN] uprises 빈응답 연속 {UPRISES_EMPTY_STREAK}회 (캐시 미사용 또는 없음)")
            elif UPRISES_EMPTY_STREAK > INTERSECTION_MAX_EMPTY_WARN and UPRISES_EMPTY_STREAK % INTERSECTION_MAX_EMPTY_WARN == 0:
                print(f"[WARN] uprises 빈응답 누적 {UPRISES_EMPTY_STREAK}회")
        return candidates, meta
    else:
        UPRISES_EMPTY_STREAK = 0
        UPRISES_LAST_NONEMPTY = candidates
        UPRISES_LAST_TS = now
        meta["source"] = "fresh"
        meta["empty_streak"] = 0
        meta["cache_age"] = 0
        return candidates, meta


# =========================================
# 메인 모니터 루프
# =========================================
async def monitor_positions(user_no: int, server_no: int):
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("API 키 없음")
        return
    access_key, secret_key = keys
    ps = PositionState()
    print("=== 시작 ===")

    await align_to_half_minute()

    while True:
        try:
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print("[ERR] 잔고 조회 실패:", e)
            await sleep_until_next_boundary()
            continue

        markets = build_market_list_from_accounts(raw_accounts, base_unit=BASE_UNIT)
        try:
            price_map = await fetch_current_prices(markets)
        except Exception as e:
            print("[WARN] 가격 조회 실패:", e)
            price_map = {}

        enriched = enrich_accounts_with_prices(raw_accounts, price_map, base_unit=BASE_UNIT)
        available_krw = get_available_krw(raw_accounts)
        held_markets = {
            it["market"] for it in enriched
            if it.get("market") and it.get("balance") and it["balance"] > 0
        }

        actions = []
        sell_orders: List[Dict[str, Any]] = []

        # ---------- 보유 종목 처리 / 매도 판단 ----------
        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            avg = it.get("avg_buy_price")
            bal = it.get("balance")
            cur_price = it.get("current_price")
            if not market or pnl is None or avg is None or bal is None or cur_price is None:
                continue
            if pnl < WATCH_MIN_PNL:
                if market in ps.data:
                    ps.remove(market)
                continue
            if ps.recently_sold(market):
                continue
            notional = bal * cur_price
            if notional < MIN_NOTIONAL_KRW:
                continue
            st = ps.update_or_init(market, pnl, avg)
            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market,
                "pnl": pnl,
                "peak": st["peak_pnl"],
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
                    "market": market,
                    "volume": volume,
                    "pnl": pnl,
                    "category": category,
                    "reason": reason,
                    "state_ref": st,
                    "portion": portion
                })

        # ---------- 매도 실행 (현재 DRY 로그) ----------
        # ---------- 매도 실행 (LIVE 반영) ----------
        for so in sell_orders:
            market = so["market"]
            volume = so["volume"]
            category = so["category"]  # HARD_TP1 | HARD_TP2 | TRAIL | None
            pnl = so["pnl"]
            reason = so["reason"]
            st = so["state_ref"]
            portion = so["portion"]  # 사용한 비율 (0~1)

            # 방어: 0 이하이면 스킵
            if volume <= 0:
                print(f"[SKIP] {market} 매도 volume<=0 (calc={volume}) cat={category}")
                continue

            if not LIVE_TRADING:
                # DRY 모드
                print(f"[DRY_SELL] {market} cat={category} vol={volume} pnl={pnl}% reason={reason}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                else:
                    # TRAIL 또는 일반
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        # 부분매도 후 peak 재설정(선택)
                        st["peak_pnl"] = pnl
                        st["armed"] = False
                continue

            # === LIVE 모드 실제 주문 ===
            try:
                resp = await order_market_sell(access_key, secret_key, market, volume)
                uid = resp.get("uuid")
                print(f"[ORDER] SELL {market} cat={category} vol={volume} pnl={pnl}% uuid={uid} reason={reason}")

                # (선택) 1회 체결 확인
                # 너무 잦은 조회를 피하려면 필요 없으면 제거 가능
                if uid:
                    await asyncio.sleep(0.8)
                    try:
                        od = await get_order(access_key, secret_key, uid)
                        print(
                            f"[ORDER-CHK] SELL {market} state={od.get('state')} remaining_vol={od.get('remaining_volume')} paid_fee={od.get('paid_fee')}")
                    except Exception as ce:
                        print(f"[WARN] 매도 주문 조회 실패 {market} uuid={uid} err={ce}")

            except Exception as se:
                print(f"[ERR] 매도 주문 실패 {market}: {se}")
                # 실패 시 상태 변경 안 하고 다음 루프에서 다시 판단 가능
                continue

            # === 상태 갱신 ===
            if category == "HARD_TP1":
                st["hard_tp_taken"] = True
                # 부분매도 후 남은 물량 계속 추적
            elif category == "HARD_TP2":
                st["hard_tp2_taken"] = True
                if portion >= 1:
                    ps.mark_sold(market)
            else:
                # TRAIL 혹은 일반 (SELL_PORTION). 전부 청산이면 제거
                if portion >= 1:
                    ps.mark_sold(market)
                else:
                    # 부분매도 → peak 리셋 & 재무장 해제
                    st["peak_pnl"] = pnl
                    st["armed"] = False

        # ---------- 교집합 매수 ----------
        if INTERSECTION_BUY_ENABLED:
            try:
                intersection_candidates, iu_meta = await get_intersection_candidates_safe()
            except Exception as e:
                intersection_candidates = []
                iu_meta = {"source": "error", "empty_streak": -1}
                print(f"[WARN] 교집합 안전 호출 실패: {e}")

            if iu_meta.get("source") == "empty":
                print(f"[INFO] 교집합 데이터 없음(empty_streak={iu_meta.get('empty_streak')}) → 매수 스킵")
            else:
                if iu_meta.get("source") != "fresh":
                    print(f"[DEBUG] 교집합 후보 source={iu_meta.get('source')} size={len(intersection_candidates)} empty_streak={iu_meta.get('empty_streak')}")

                intersection_candidates.sort(key=lambda x: x.get("avg_score", 0), reverse=True)
                buys_done = 0
                for row in intersection_candidates:
                    if buys_done >= INTERSECTION_MAX_BUY_PER_CYCLE:
                        break
                    mkt = row.get("market")
                    score = row.get("avg_score")
                    if not mkt or score is None:
                        continue
                    try:
                        score_dec = Decimal(str(score))
                    except:
                        continue
                    if score_dec < INTERSECTION_MIN_SCORE:
                        continue
                    if mkt in held_markets:
                        continue
                    if SKIP_BUY_IF_RECENT_SELL and ps.recently_sold(mkt):
                        continue
                    if ps.recently_bought_intersection(mkt, INTERSECTION_BUY_COOLDOWN_SEC):
                        continue
                    if INTERSECTION_BUY_KRW < MIN_NOTIONAL_KRW:
                        print(f"[WARN] INTERSECTION_BUY_KRW({INTERSECTION_BUY_KRW}) < MIN_NOTIONAL_KRW({MIN_NOTIONAL_KRW}) → 중단")
                        break
                    if available_krw < INTERSECTION_BUY_KRW:
                        print(f"[INFO] 교집합 {mkt}매수 KRW 부족 need={INTERSECTION_BUY_KRW} avail={available_krw}")
                        break

                    if not LIVE_TRADING:
                        print(f"[DRY_INTERSECTION_BUY] src={iu_meta.get('source')} {mkt} score={score_dec} KRW={INTERSECTION_BUY_KRW}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        available_krw -= INTERSECTION_BUY_KRW
                        buys_done += 1
                        continue

                    # 실제 주문
                    try:
                        resp = await order_market_buy_price(access_key, secret_key, mkt, INTERSECTION_BUY_KRW)
                        uid = resp.get("uuid")
                        print(f"[ORDER] INTERSECTION BUY src={iu_meta.get('source')} {mkt} score={score_dec} KRW={INTERSECTION_BUY_KRW} uuid={uid}")
                        if uid:
                            await asyncio.sleep(1)
                            try:
                                od = await get_order(access_key, secret_key, uid)
                                print(f"[ORDER-CHK] INTERSECTION BUY {mkt} state={od.get('state')} paid_fee={od.get('paid_fee')}")
                            except Exception as oe:
                                print(f"[WARN] 교집합 매수 주문 조회 실패 {mkt} uuid={uid} err={oe}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        available_krw -= INTERSECTION_BUY_KRW
                        buys_done += 1
                    except Exception as e:
                        print(f"[ERR] 교집합 매수 실패 {mkt}: {e}")

        # ---------- Range 매수 (5분 경계) ----------
        now_ts = time.time()
        is_5m, window_start = is_five_minute_boundary(now_ts)
        if ENABLE_RANGE_BUY and is_5m:
            buys_executed = 0
            candidates = []
            for it in enriched:
                market = it.get("market")
                pnl = it.get("pnl_percent")
                cur_price = it.get("current_price")
                if not market or pnl is None or cur_price is None:
                    continue
                if ps.recently_sold(market):
                    continue
                if ps.bought_this_window(market, window_start):
                    continue
                if BUY_RANGE_LOW <= pnl <= BUY_RANGE_HIGH:
                    candidates.append((pnl, market))
            candidates.sort(key=lambda x: x[0])
            for pnl, market in candidates:
                if buys_executed >= MAX_BUY_PER_WINDOW:
                    break
                if available_krw < RANGE_BUY_KRW:
                    print(f"[INFO] KRW 부족: need {RANGE_BUY_KRW} available {available_krw}")
                    break
                if RANGE_BUY_KRW < MIN_NOTIONAL_KRW:
                    print(f"[WARN] RANGE_BUY_KRW({RANGE_BUY_KRW}) < MIN_NOTIONAL_KRW({MIN_NOTIONAL_KRW}) → 스킵")
                    break

                if not LIVE_TRADING:
                    print(f"[DRY_BUY] {market} pnl={pnl}% KRW={RANGE_BUY_KRW} window={int(window_start)}")
                    ps.record_buy_window(market, window_start)
                    ps.data.setdefault(market, {})["entry_source"] = "range"
                    available_krw -= RANGE_BUY_KRW
                    buys_executed += 1
                    continue

                # 실거래 주문 (시장가 금액 매수)
                try:
                    resp = await order_market_buy_price(access_key, secret_key, market, RANGE_BUY_KRW)
                    uid = resp.get("uuid")
                    print(f"[ORDER] RANGE BUY {market} pnl={pnl}% KRW={RANGE_BUY_KRW} uuid={uid}")
                    ps.record_buy_window(market, window_start)
                    ps.data.setdefault(market, {})["entry_source"] = "range"
                    available_krw -= RANGE_BUY_KRW
                    buys_executed += 1
                except Exception as e:
                    print(f"[ERR] RANGE 매수 실패 {market}: {e}")

        # ---------- 상태 로그 ----------
        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 결과:")
            for a in actions:
                status = "SELL" if a["sell"] else ("ARM" if a["reason"].startswith("ARMED") else "HOLD")
                print(f"  {a['market']} pnl={a['pnl']} peak={a['peak']} armed={a['armed']} -> {status} {a['reason']}")

        await sleep_until_next_boundary()


# =========================================
# main
# =========================================
async def main():
    user_no = 100013
    server_no = 21
    await monitor_positions(user_no, server_no)


if __name__ == "__main__":
    asyncio.run(main())