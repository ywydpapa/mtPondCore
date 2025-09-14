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
ARM_PNL = Decimal("0.3")

HARD_TP = Decimal("1.2")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET

TRAIL_DROP = Decimal("0.4")
TRAIL_START_PNL = Decimal("0.8")

POLL_INTERVAL = 30
MIN_NOTIONAL_KRW = Decimal("5500")

SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "0.3"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "0.8"))
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
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "1800"))  # 30분

async def get_keys(user_no: int, server_no: int) -> Optional[tuple]:
    async with SessionLocal() as session:
        sql = text("""
            SELECT apiKey1, apiKey2
            FROM traceUser
            WHERE userNo = :u AND serverNo = :s
            LIMIT 1
        """)
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        return result.fetchone()

def build_upbit_jwt_simple(access_key: str, secret_key: str) -> str:
    payload = { "access_key": access_key, "nonce": str(uuid.uuid4()) }
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

async def fetch_upbit_accounts(access_key: str, secret_key: str) -> List[Dict[str, Any]]:
    url = "https://api.upbit.com/v1/accounts"
    token = build_upbit_jwt_simple(access_key, secret_key)
    headers = { "Authorization": f"Bearer {token}", "Accept": "application/json" }
    return await http_get_json(url, headers=headers)

async def fetch_current_prices(markets: list[str]) -> dict[str, Decimal]:
    if not markets:
        return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(markets)}
    raw = await http_get_json(url, params=params)
    price_map: dict[str, Decimal] = {}
    for item in raw:
        market = item.get("market")
        trade_price = item.get("trade_price")
        if market and trade_price is not None:
            try:
                price_map[market] = Decimal(str(trade_price))
            except InvalidOperation:
                continue
    return price_map

def build_market_list_from_accounts(accounts, base_unit="KRW") -> list[str]:
    markets = []
    for acc in accounts:
        currency = acc.get("currency")
        unit = acc.get("unit_currency")
        if not currency or not unit: continue
        if unit != base_unit: continue
        if currency == base_unit: continue
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
        if acc.get("currency") == BASE_UNIT:
            try:
                bal = Decimal(str(acc.get("balance", "0")))
                locked = Decimal(str(acc.get("locked", "0")))
                return bal - locked
            except:
                return Decimal("0")
    return Decimal("0")

def enrich_accounts_with_prices(accounts: list[dict], price_map: dict[str, Decimal], base_unit="KRW") -> list[dict]:
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
            if ENFORCE_WHITELIST and WHITELIST_MARKETS and market not in WHITELIST_MARKETS:
                market = None
            else:
                current_price = price_map.get(market)
                if current_price is not None and avg_buy_price > 0:
                    try:
                        pnl_percent = ((current_price - avg_buy_price) / avg_buy_price * Decimal("100")).quantize(Decimal("0.01"))
                        ratio = (current_price / avg_buy_price).quantize(Decimal("0.0001"))
                    except:
                        pass

        def to_decimal(v):
            try: return Decimal(str(v))
            except: return Decimal("0")

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

UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

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
    params = { "uuid": uuid_ }
    jwt_token = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/json"
    }
    url = "https://api.upbit.com/v1/order"
    return await http_get_json(url, headers=headers, params=params)

class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.last_sell_time: Dict[str, float] = {}
        self.last_buy_window: Dict[str, float] = {}
        # 교집합 매수 기록
        self.intersection_last_buy_time: Dict[str, float] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st["avg_buy_price"] != avg_price:
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
                "last_update_ts": now
            }
            return self.data[market]
        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        st["prev_pnl"] = pnl
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
        if not ts: return False
        return (time.time() - ts) < COOLDOWN_AFTER_SELL

    def record_buy_window(self, market: str, window_start: float):
        self.last_buy_window[market] = window_start

    def bought_this_window(self, market: str, window_start: float) -> bool:
        return self.last_buy_window.get(market) == window_start

    # 교집합 매수 기록
    def mark_intersection_buy(self, market: str):
        self.intersection_last_buy_time[market] = time.time()

    def recently_bought_intersection(self, market: str, cooldown: int) -> bool:
        ts = self.intersection_last_buy_time.get(market)
        if not ts: return False
        return (time.time() - ts) < cooldown

def apply_momentum_extension(state: dict):
    if not ENABLE_DYNAMIC_MOMENTUM_TP:
        return
    if state.get("dynamic_hard_tp2") is not None:
        return
    now = time.time()
    entry_ts = state.get("entry_ts", now)
    dt = now - entry_ts
    extra_tp = Decimal("0")
    extra_trail = Decimal("0")
    tag = "BASE"

    if dt < MOMENTUM_TIER1_SEC:
        extra_tp = MOMENTUM_TIER1_TP_OFFSET
        extra_trail = MOMENTUM_TIER1_TRAIL_EXTRA
        tag = "FAST1"
    elif dt < MOMENTUM_TIER2_SEC:
        extra_tp = MOMENTUM_TIER2_TP_OFFSET
        extra_trail = MOMENTUM_TIER2_TRAIL_EXTRA
        tag = "FAST2"
    elif dt < MOMENTUM_TIER3_SEC:
        extra_tp = MOMENTUM_TIER3_TP_OFFSET
        extra_trail = MOMENTUM_TIER3_TRAIL_EXTRA
        tag = "MID"
    else:
        tag = "SLOW"

    if extra_tp > MOMENTUM_MAX_EXTRA_CAP:
        extra_tp = MOMENTUM_MAX_EXTRA_CAP

    dynamic_tp2 = HARD_TP2_BASE + extra_tp
    trail_drop_dyn = TRAIL_DROP + extra_trail

    state["dynamic_hard_tp2"] = dynamic_tp2
    state["trail_drop_dynamic"] = trail_drop_dyn
    state["htp1_time"] = now
    state["momentum_tag"] = tag

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
    sleep_dur = max(0, next_boundary - now)
    await asyncio.sleep(sleep_dur)

# =========================================
# 교집합 후보 가져오기
# =========================================
async def get_intersection_candidates() -> List[Dict[str, Any]]:
    """
    topuprise.uprises() 반환 케이스:
      1) 리스트: [{'market':..., 'avg_score':...}, ...]
      2) JSON 문자열: {"multi_tf_intersection": {"items":[...]} }
    """
    raw = topuprise.uprises()

    if isinstance(raw, list):
        cleaned = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            m = r.get("market")
            s = r.get("avg_score")
            if m is not None and s is not None:
                cleaned.append({"market": m, "avg_score": s})
        return cleaned

    if isinstance(raw, str):
        try:
            js = json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"uprises JSON parse 실패: {e}")
        if isinstance(js, list):
            return [
                {"market": r.get("market"), "avg_score": r.get("avg_score")}
                for r in js
                if isinstance(r, dict) and r.get("market") and r.get("avg_score") is not None
            ]
        if isinstance(js, dict):
            mti = js.get("multi_tf_intersection", {})
            items = mti.get("items", [])
            out = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                m = it.get("market")
                s = it.get("avg_score")
                if m is not None and s is not None:
                    out.append({"market": m, "avg_score": s})
            return out
        raise RuntimeError("uprises() JSON 구조 인식 실패")

    raise RuntimeError(f"uprises() 반환 타입 지원 안함: {type(raw)}")

async def monitor_positions(user_no: int, server_no: int):
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("API 키 없음")
        return
    access_key, secret_key = keys
    ps = PositionState()
    mode = "LIVE" if LIVE_TRADING else "DRY"
    print(f"=== PNL 모니터 시작 ({mode}) ===")
    print(
        f"Params:\n"
        f"  HARD_TP1={HARD_TP} SELL={HARD_TP_SELL_PORTION}  HARD_TP2_BASE={HARD_TP2_BASE} SELL={HARD_TP2_SELL_PORTION}\n"
        f"  TRAIL_START={TRAIL_START_PNL} TRAIL_DROP={TRAIL_DROP} dynamic={ENABLE_DYNAMIC_MOMENTUM_TP}\n"
        f"  RANGE_BUY={ENABLE_RANGE_BUY} range=[{BUY_RANGE_LOW},{BUY_RANGE_HIGH}] amt={RANGE_BUY_KRW}\n"
        f"  INTERSECTION_ENABLED={INTERSECTION_BUY_ENABLED} score>={INTERSECTION_MIN_SCORE} buy={INTERSECTION_BUY_KRW}"
    )
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

        # 현재 보유 중(양수 balance) 마켓
        held_markets = {
            it["market"] for it in enriched
            if it.get("market") and it.get("balance") and it["balance"] > 0
        }

        actions = []
        sell_orders: List[Dict[str, Any]] = []

        # === 매도 판정 ===
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
                    print(f"[SKIP] {market} volume<=0 (portion={portion})")
                    if category == "TRAIL" or (category == "HARD_TP2" and portion >= 1):
                        ps.mark_sold(market)
                    continue

                if category == "HARD_TP1":
                    apply_momentum_extension(st)

                sell_orders.append({
                    "market": market,
                    "volume": volume,
                    "pnl": pnl,
                    "category": category,
                    "reason": reason,
                    "state_ref": st,
                    "portion": portion
                })

        # === 매도 실행 ===
        for so in sell_orders:
            market = so["market"]
            volume = so["volume"]
            category = so["category"]
            pnl = so["pnl"]
            reason = so["reason"]
            st = so["state_ref"]
            portion = so["portion"]

            if not LIVE_TRADING:
                print(f"[DRY_SELL] {market} cat={category} portion={portion} vol={volume} pnl={pnl}% reason={reason} momentum={st.get('momentum_tag')}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                else:
                    ps.mark_sold(market)
                continue

            try:
                resp = await order_market_sell(access_key, secret_key, market, volume)
                uid = resp.get("uuid")
                print(f"[ORDER] SELL {market} cat={category} portion={portion} vol={volume} pnl={pnl}% reason={reason} uuid={uid} momentum={st.get('momentum_tag')}")
                if uid:
                    await asyncio.sleep(1)
                    try:
                        od = await get_order(access_key, secret_key, uid)
                        print(f"[ORDER-CHK] {market} state={od.get('state')} executed={od.get('executed_volume')} remain={od.get('remaining_volume')}")
                    except Exception as oe:
                        print(f"[WARN] 주문 조회 실패 {market} uuid={uid} err={oe}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                else:
                    ps.mark_sold(market)
            except Exception as e:
                print(f"[ERR] 매도 실패 {market}: {e}")

        # === 교집합 매수 (30초마다) ===
        if INTERSECTION_BUY_ENABLED:
            try:
                intersection_candidates = await get_intersection_candidates()
            except Exception as e:
                intersection_candidates = []
                print(f"[WARN] 교집합 데이터 조회 실패: {e}")

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
                    print(f"[WARN] INTERSECTION_BUY_KRW({INTERSECTION_BUY_KRW}) < MIN_NOTIONAL_KRW({MIN_NOTIONAL_KRW}) -> 중단")
                    break
                if available_krw < INTERSECTION_BUY_KRW:
                    print(f"[INFO] 교집합 매수 KRW 부족 need={INTERSECTION_BUY_KRW} avail={available_krw}")
                    break

                if not LIVE_TRADING:
                    print(f"[DRY_INTERSECTION_BUY] {mkt} score={score_dec} KRW={INTERSECTION_BUY_KRW}")
                    ps.mark_intersection_buy(mkt)
                    available_krw -= INTERSECTION_BUY_KRW
                    buys_done += 1
                    continue

                try:
                    resp = await order_market_buy_price(access_key, secret_key, mkt, INTERSECTION_BUY_KRW)
                    uid = resp.get("uuid")
                    print(f"[ORDER] INTERSECTION BUY {mkt} score={score_dec} KRW={INTERSECTION_BUY_KRW} uuid={uid}")
                    if uid:
                        await asyncio.sleep(1)
                        try:
                            od = await get_order(access_key, secret_key, uid)
                            print(f"[ORDER-CHK] INTERSECTION BUY {mkt} state={od.get('state')} paid_fee={od.get('paid_fee')}")
                        except Exception as oe:
                            print(f"[WARN] 교집합 매수 주문 조회 실패 {mkt} uuid={uid} err={oe}")
                    ps.mark_intersection_buy(mkt)
                    available_krw -= INTERSECTION_BUY_KRW
                    buys_done += 1
                except Exception as e:
                    print(f"[ERR] 교집합 매수 실패 {mkt}: {e}")

        # === 5분 경계 Range Buy ===
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
                if SKIP_BUY_IF_RECENT_SELL and ps.recently_sold(market):
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
                    print(f"[WARN] RANGE_BUY_KRW({RANGE_BUY_KRW}) < MIN_NOTIONAL_KRW({MIN_NOTIONAL_KRW}) -> 스킵")
                    break

                if not LIVE_TRADING:
                    print(f"[DRY_BUY] {market} pnl={pnl}% KRW={RANGE_BUY_KRW} window={int(window_start)}")
                    ps.record_buy_window(market, window_start)
                    available_krw -= RANGE_BUY_KRW
                    buys_executed += 1
                    continue

                try:
                    resp = await order_market_buy_price(access_key, secret_key, market, RANGE_BUY_KRW)
                    uid = resp.get("uuid")
                    print(f"[ORDER] BUY {market} pnl={pnl}% KRW={RANGE_BUY_KRW} uuid={uid}")
                    if uid:
                        await asyncio.sleep(1)
                        try:
                            od = await get_order(access_key, secret_key, uid)
                            print(f"[ORDER-CHK] BUY {market} state={od.get('state')} paid_fee={od.get('paid_fee')}")
                        except Exception as oe:
                            print(f"[WARN] 매수 주문 조회 실패 {market} uuid={uid} err={oe}")
                    ps.record_buy_window(market, window_start)
                    available_krw -= RANGE_BUY_KRW
                    buys_executed += 1
                except Exception as e:
                    print(f"[ERR] 매수 실패 {market}: {e}")

        # === 상태 출력 ===
        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 결과:")
            for a in actions:
                st = ps.data.get(a["market"])
                dyn_tp2 = st.get("dynamic_hard_tp2") if st else None
                mom_tag = st.get("momentum_tag") if st else None
                if a["sell"]:
                    status = f"SELL({a['cat']})"
                else:
                    status = "ARM" if a["reason"].startswith("ARMED") else "HOLD"
                extra = ""
                if dyn_tp2 and a["cat"] != "HARD_TP2":
                    extra = f" dynTP2={dyn_tp2}"
                if mom_tag:
                    extra += f" mom={mom_tag}"
                print(f"  {a['market']:<10} pnl={a['pnl']:>6} peak={a['peak']:>6} armed={a['armed']} -> {status} {a['reason']}{extra}")

        await sleep_until_next_boundary()

async def main():
    user_no = 100013
    server_no = 21
    await monitor_positions(user_no, server_no)

if __name__ == "__main__":
    asyncio.run(main())
