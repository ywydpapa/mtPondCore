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
from functools import lru_cache

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

dotenv.load_dotenv()

engine = create_async_engine(os.getenv("dburl"), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ---------------- 기본 파라미터 ----------------
WATCH_MIN_PNL = Decimal("-10")
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
RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
MAX_BUY_PER_WINDOW = int(os.getenv("MAX_BUY_PER_WINDOW", "999"))
SKIP_BUY_IF_RECENT_SELL = True

BASE_UNIT = "KRW"

# DRY 모드 대응 (기존 FORCE_LIVE True 고정 제거)
FORCE_LIVE = os.getenv("FORCE_LIVE", "0") == "1"
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
if not LIVE_TRADING:
    print("[MODE] DRY_RUN (실제 주문 미전송)")

WHITELIST_MARKETS: List[str] = []
ENFORCE_WHITELIST = False

MAX_BACKOFF = 120
FIVE_MIN_SECONDS = 300

ENABLE_DYNAMIC_MOMENTUM_TP = os.getenv("ENABLE_DYNAMIC_MOMENTUM_TP", "1") == "1"

# ---------------- ATR 모드 기본 환경변수 ----------------
ENABLE_ATR_MODE = os.getenv("ENABLE_ATR_MODE", "1") == "1"
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_UNIT_MIN = int(os.getenv("ATR_UNIT_MIN", "5"))
ATR_CACHE_SEC = int(os.getenv("ATR_CACHE_SEC", "60"))
ATR_STOP_MULT = Decimal(os.getenv("ATR_STOP_MULT", "1.5"))
ATR_TP_MULT = Decimal(os.getenv("ATR_TP_MULT", "2.5"))
ATR_MIN_STOP_PCT = Decimal(os.getenv("ATR_MIN_STOP_PCT", "0.0"))
ATR_MIN_TP_PCT = Decimal(os.getenv("ATR_MIN_TP_PCT", "0.0"))
ATR_RECALC_ON_ADD = os.getenv("ATR_RECALC_ON_ADD", "1") == "1"

# ---------------- ATR 부분 익절 + 트레일링 ----------------
ATR_TP_SELL_PORTION = Decimal(os.getenv("ATR_TP_SELL_PORTION", "0.5"))
ATR_TRAIL_ENABLE = os.getenv("ATR_TRAIL_ENABLE", "1") == "1"
ATR_TRAIL_MULT = Decimal(os.getenv("ATR_TRAIL_MULT", "1.0"))
ATR_TRAIL_ATR_REFRESH = os.getenv("ATR_TRAIL_ATR_REFRESH", "1") == "1"

# ---------------- ATR 고도화 (저변동/최소수익 보장) ----------------
FEE_RATE = Decimal(os.getenv("FEE_RATE", "0.0005"))             # 단건 수수료율
SLIPPAGE_PCT = Decimal(os.getenv("SLIPPAGE_PCT", "0.05"))       # 예상 슬리피지 (%)
MIN_PROFIT_PCT = Decimal(os.getenv("MIN_PROFIT_PCT", "0.40"))   # 최소 순이익 목표 (%)
ATR_MIN_ATR_PCT = Decimal(os.getenv("ATR_MIN_ATR_PCT", "0.15")) # Quiet 판단 ATR%
ATR_QUIET_MODE = os.getenv("ATR_QUIET_MODE", "scale")           # scale | skip
ATR_QUIET_SCALE_CAP = Decimal(os.getenv("ATR_QUIET_SCALE_CAP", "4"))
FEE_BUFFER_MULT = Decimal(os.getenv("FEE_BUFFER_MULT", "1.2"))
STOP_NOISE_FLOOR_PCT = Decimal(os.getenv("STOP_NOISE_FLOOR_PCT", "0.12"))
TP_NOISE_FLOOR_PCT = Decimal(os.getenv("TP_NOISE_FLOOR_PCT", "0.25"))
MIN_R_MULT = Decimal(os.getenv("MIN_R_MULT", "0"))              # 0 => 비활성

# ---------------- 교집합 매수 파라미터 ----------------
INTERSECTION_BUY_ENABLED = os.getenv("INTERSECTION_BUY_ENABLED", "1") == "1"
INTERSECTION_MIN_SCORE = Decimal(os.getenv("INTERSECTION_MIN_SCORE", "10"))
INTERSECTION_BUY_KRW = Decimal(os.getenv("INTERSECTION_BUY_KRW", "200000"))
INTERSECTION_MAX_BUY_PER_CYCLE = int(os.getenv("INTERSECTION_MAX_BUY_PER_CYCLE", "1"))
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "999999"))

INTERSECTION_USE_CACHE_ON_EMPTY = os.getenv("INTERSECTION_USE_CACHE_ON_EMPTY", "1") == "1"
INTERSECTION_CACHE_TTL_SEC = int(os.getenv("INTERSECTION_CACHE_TTL_SEC", "180"))
INTERSECTION_MAX_EMPTY_WARN = int(os.getenv("INTERSECTION_MAX_EMPTY_WARN", "5"))

UPRISES_LAST_NONEMPTY: List[dict] = []
UPRISES_LAST_TS: float | None = None
UPRISES_EMPTY_STREAK: int = 0

# ---------------- 추가매수 제한 ----------------
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "4"))
MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))

INTERVAL_SECONDS = 20

# ---------------- 제외 마켓 파싱 ----------------
def parse_exclude_markets() -> set:
    raw = os.getenv("EXCLUDE_MARKETS", "").strip()
    if not raw:
        return set()
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            return {str(x).strip() for x in arr if isinstance(x, (str,))}
        except Exception:
            pass
    parts = [p.strip() for p in raw.split(",")]
    return {p for p in parts if p}

EXCLUDED_MARKETS = parse_exclude_markets()

# ---------------- 손절 트레일 ----------------
ENABLE_STOP_TRAIL = os.getenv("ENABLE_STOP_TRAIL", "1") == "1"
STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-5"))
STOP_SELL_PORTION = Decimal(os.getenv("STOP_SELL_PORTION", "0.10"))
STOP_PEAK_INCREMENT = Decimal(os.getenv("STOP_PEAK_INCREMENT", "0.30"))
STOP_MAX_SELLS = int(os.getenv("STOP_MAX_SELLS", "6"))
STOP_DISABLE_NEW_BUYS = os.getenv("STOP_DISABLE_NEW_BUYS", "1") == "1"
STOP_REBOUND_COOLDOWN_SEC = int(os.getenv("STOP_REBOUND_COOLDOWN_SEC", "30"))

STOP_RECOVERY_EXIT_PNL = os.getenv("STOP_RECOVERY_EXIT_PNL", None)
if STOP_RECOVERY_EXIT_PNL is not None:
    try:
        STOP_RECOVERY_EXIT_PNL = Decimal(str(STOP_RECOVERY_EXIT_PNL))
    except:
        STOP_RECOVERY_EXIT_PNL = None
STOP_COOLDOWN_BATCH_SELL = os.getenv("STOP_COOLDOWN_BATCH_SELL", "1") == "1"
STOP_COOLDOWN_MAX_BATCH = int(os.getenv("STOP_COOLDOWN_MAX_BATCH", "5"))
STOP_RECOVERY_REQUIRE_COOLDOWN = os.getenv("STOP_RECOVERY_REQUIRE_COOLDOWN", "1") == "1"
STOP_RECOVERY_LOG = os.getenv("STOP_RECOVERY_LOG", "1") == "1"

UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

# ---------------- DB 키 조회 ----------------
async def get_keys(user_no: int, server_no: int) -> Optional[tuple]:
    async with SessionLocal() as session:
        sql = text("""
            SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        return result.fetchone()

# ---------------- JWT ----------------
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

# ---------------- HTTP Helper ----------------
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
        except Exception as e:
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
        except Exception as e:
            if attempt == max_retry:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

# ---------------- Upbit API 래퍼 ----------------
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
            bal = Decimal("0")
            locked = Decimal("0")
        if bal == 0 and locked == 0:
            continue
        m = f"{base_unit}-{c}"
        if ENFORCE_WHITELIST and WHITELIST_MARKETS and m not in WHITELIST_MARKETS:
            continue
        res.append(m)
    uniq = []
    seen = set()
    for m in res:
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
          # (원본 코드에서 인덴트 오류 가능성 주의 - 유지)
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
            "currency": c, "market": market, "unit_currency": u,
            "balance": to_d(acc.get("balance")),
            "locked": to_d(acc.get("locked")),
            "avg_buy_price": avg if avg > 0 else None,
            "current_price": cur,
            "pnl_percent": pnl,
            "ratio_cur_over_avg": ratio
        })
    return out

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

async def get_order(access_key, secret_key, uuid_):
    params = {"uuid": uuid_}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_get_json("https://api.upbit.com/v1/order", headers=headers, params=params)

# ---------------- 포지션 상태 ----------------
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
                "peak_pnl": pnl, "prev_pnl": pnl, "armed": False, "avg_buy_price": avg_price,
                "hard_tp_taken": False, "hard_tp2_taken": False, "dynamic_hard_tp2": None,
                "trail_drop_dynamic": None, "entry_ts": now, "htp1_time": None,
                "momentum_tag": None, "last_update_ts": now,
                "entry_source": self.data.get(market, {}).get("entry_source"),
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0,
                "worst_pnl": pnl, "stop_last_sell_ts": None,
                "stop_cooldown_flag": False, "stop_pending_peak": None, "stop_cooldown_start_ts": None,
                # ATR
                "atr_value": None,
                "atr_entry_price": avg_price,
                "atr_stop_price": None,
                "atr_tp_price": None,
                "atr_last_refresh_ts": None,
                "atr_hit": False,
                "atr_mode_active": ENABLE_ATR_MODE,
                # ATR 트레일
                "atr_trail_active": False,
                "atr_trail_high": None,
                "atr_trail_stop_price": None
            }
            return self.data[market]
        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if st.get("stop_triggered") and pnl < st.get("worst_pnl", pnl):
            st["worst_pnl"] = pnl
        st["prev_pnl"] = pnl
        st["last_update_ts"] = now
        return st

    def remove(self, market):
        if market in self.data:
            del self.data[market]

    def mark_sold(self, market):
        self.last_sell_time[market] = time.time()
        self.remove(market)
        if market in self.buy_info:
            del self.buy_info[market]

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
        if ATR_RECALC_ON_ADD:
            self.mark_atr_recalc_needed(market)

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
                return False, f"[SKIP] {market} 초기매수 금액({next_amount})>={max_total_invest}"
            return True, "INIT_OK"
        add_done = total_buys - 1
        if add_done >= max_additional_buys:
            return False, f"[SKIP] {market} 추가매수 한도 초과 (이미 {add_done}회)"
        if max_total_invest > 0 and (total_inv + next_amount) >= max_total_invest:
            return False, f"[SKIP] {market} 누적금액 {total_inv}+{next_amount}>={max_total_invest}"
        return True, "OK"

    def mark_atr_recalc_needed(self, market):
        st = self.data.get(market)
        if st:
            st["atr_last_refresh_ts"] = None

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

# ---------------- Percent 기반 결정 로직 ----------------
def decide_sell(market, pnl: Decimal, state: dict):
    if state.get("stop_triggered"):
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

async def sleep_until_next_boundary():
    now = time.time()
    nxt = math.floor(now / INTERVAL_SECONDS) * INTERVAL_SECONDS + INTERVAL_SECONDS
    await asyncio.sleep(max(0, nxt - now))

def is_five_minute_boundary(ts: float):
    window_start = ts - (ts % FIVE_MIN_SECONDS)
    lt = time.localtime(ts)
    return (lt.tm_min % 5 == 0 and lt.tm_sec == 0), window_start

def _is_effectively_empty(cands):
    if not cands:
        return True
    return not any((c.get("market") and c.get("avg_score") is not None) for c in cands if isinstance(c, dict))

def _normalize_uprises(raw):
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

# ---------------- ATR 캔들 / 계산 ----------------
async def fetch_upbit_candles_minutes(market: str, unit: int, count: int = (ATR_PERIOD + 3)):
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    params = {"market": market, "count": str(count)}
    try:
        data = await http_get_json(url, params=params)
        if isinstance(data, list):
            return list(reversed(data))
    except Exception as e:
        print(f"[WARN] 캔들 조회 실패 {market} unit={unit}: {e}")
    return []

def compute_atr_from_candles(candles: list, period: int) -> Optional[Decimal]:
    if len(candles) < period + 1:
        return None
    highs, lows, closes = [], [], []
    for c in candles:
        try:
            highs.append(Decimal(str(c["high_price"])))
            lows.append(Decimal(str(c["low_price"])))
            closes.append(Decimal(str(c["trade_price"])))
        except:
            return None
    trs = []
    for i in range(1, len(candles)):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
            return None
    first = sum(trs[:period]) / Decimal(period)
    atr = first
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / Decimal(period)
    return atr.quantize(Decimal("0.00000001"))

async def ensure_atr_levels(market: str, st: dict, current_price: Decimal, force: bool = False):
    if not st.get("atr_mode_active"):
        return
    now = time.time()
    last_ts = st.get("atr_last_refresh_ts")
    need = False
    if force or last_ts is None:
        need = True
    elif (now - last_ts) >= ATR_CACHE_SEC:
        need = True
    if not need:
        return

    candles = await fetch_upbit_candles_minutes(market, ATR_UNIT_MIN, ATR_PERIOD + 3)
    atr_val = compute_atr_from_candles(candles, ATR_PERIOD)
    if not atr_val or atr_val <= 0:
        print(f"[WARN] ATR 계산 실패 {market}")
        return

    entry_price = st.get("avg_buy_price") or current_price

    atr_pct = (atr_val / entry_price) * Decimal("100") if entry_price > 0 else None
    if not atr_pct or atr_pct <= 0:
        print(f"[ATR] {market} atr_pct 계산불가 -> skip")
        return

    quiet = atr_pct < ATR_MIN_ATR_PCT

    if quiet and ATR_QUIET_MODE == "skip":
        st["atr_mode_active"] = False
        st["atr_stop_price"] = None
        st["atr_tp_price"] = None
        st["atr_last_refresh_ts"] = time.time()
        print(f"[ATR-QUIET-SKIP] {market} atr_pct={atr_pct:.4f}%<{ATR_MIN_ATR_PCT}% -> ATR 비활성")
        return

    scale = Decimal("1")
    if quiet and ATR_QUIET_MODE == "scale" and atr_pct > 0:
        raw_scale = ATR_MIN_ATR_PCT / atr_pct
        if raw_scale > ATR_QUIET_SCALE_CAP:
            raw_scale = ATR_QUIET_SCALE_CAP
        scale = raw_scale

    raw_stop_pct = atr_pct * ATR_STOP_MULT * scale
    raw_tp_pct   = atr_pct * ATR_TP_MULT * scale

    fee_buffer_pct = (FEE_RATE * Decimal("2") * Decimal("100")) * FEE_BUFFER_MULT
    tp_floor = max(MIN_PROFIT_PCT + fee_buffer_pct + SLIPPAGE_PCT,
                   TP_NOISE_FLOOR_PCT,
                   ATR_MIN_TP_PCT)
    stop_floor = max(STOP_NOISE_FLOOR_PCT,
                     ATR_MIN_STOP_PCT,
                     fee_buffer_pct * Decimal("0.5"))

    stop_pct = max(raw_stop_pct, stop_floor)
    tp_pct   = max(raw_tp_pct, tp_floor)

    if MIN_R_MULT > 0 and stop_pct > 0:
        r_now = tp_pct / stop_pct
        if r_now < MIN_R_MULT:
            tp_pct = stop_pct * MIN_R_MULT

    atr_stop = entry_price * (Decimal("1") - stop_pct / Decimal("100"))
    atr_tp   = entry_price * (Decimal("1") + tp_pct   / Decimal("100"))

    def get_tick(p: Decimal) -> Decimal:
        v = float(p)
        if v >= 2000000: return Decimal("1000")
        if v >= 1000000: return Decimal("500")
        if v >= 500000:  return Decimal("100")
        if v >= 100000:  return Decimal("50")
        if v >= 10000:   return Decimal("10")
        if v >= 1000:    return Decimal("5")
        if v >= 100:     return Decimal("1")
        if v >= 10:      return Decimal("0.1")
        if v >= 1:       return Decimal("0.01")
        return Decimal("0.001")

    def snap_down(p: Decimal):
        t = get_tick(p)
        return ((p // t) * t).quantize(t)

    def snap_up(p: Decimal):
        t = get_tick(p)
        k = (p / t).to_integral_value(rounding=ROUND_DOWN)
        if k * t < p:
            k += 1
        return (k * t).quantize(t)

    atr_stop = snap_down(atr_stop)
    atr_tp   = snap_up(atr_tp)

    st["atr_value"] = atr_val
    st["atr_entry_price"] = entry_price
    st["atr_stop_price"] = atr_stop
    st["atr_tp_price"] = atr_tp
    st["atr_last_refresh_ts"] = now

    try:
        dist_stop_pct = (entry_price - atr_stop) / entry_price * 100
        dist_tp_pct = (atr_tp - entry_price) / entry_price * 100
        print(f"[ATR] {market} ATR={atr_val} atr_pct={atr_pct:.4f}% scale={scale:.2f} "
              f"stop={atr_stop}({dist_stop_pct:.3f}%) tp={atr_tp}({dist_tp_pct:.3f}%) quiet={quiet}")
    except:
        print(f"[ATR] {market} ATR={atr_val} stop={atr_stop} tp={atr_tp} quiet={quiet}")

# ---------------- 메인 모니터 루프 ----------------
async def monitor_positions(user_no: int, server_no: int):
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("API 키 없음")
        return
    access_key, secret_key = keys
    ps = PositionState()
    print("=== 시작 ===")
    if EXCLUDED_MARKETS:
        print(f"[INFO] 제외:{sorted(EXCLUDED_MARKETS)}")
    if ENABLE_STOP_TRAIL:
        print(f"[INFO] StopTrail trigger={STOP_TRIGGER_PNL}% portion={STOP_SELL_PORTION}")
    if ENABLE_ATR_MODE:
        print(f"[INFO] ATR Mode ON period={ATR_PERIOD} unit={ATR_UNIT_MIN} stop_mult={ATR_STOP_MULT} tp_mult={ATR_TP_MULT}")
        print(f"[INFO] ATR Quiet mode={ATR_QUIET_MODE} min_atr_pct={ATR_MIN_ATR_PCT}% scale_cap={ATR_QUIET_SCALE_CAP}")
        print(f"[INFO] Floors: stop_noise={STOP_NOISE_FLOOR_PCT}% tp_noise={TP_NOISE_FLOOR_PCT}% min_profit={MIN_PROFIT_PCT}% Rmin={MIN_R_MULT}")
        if ATR_TRAIL_ENABLE and ATR_TP_SELL_PORTION < 1:
            print(f"[INFO] ATR Trail tp_sell_portion={ATR_TP_SELL_PORTION} trail_mult={ATR_TRAIL_MULT} refresh={ATR_TRAIL_ATR_REFRESH}")

    # 초기 포지션 추적 설정
    try:
        init_accounts = await fetch_upbit_accounts(access_key, secret_key)
        init_markets = build_market_list_from_accounts(init_accounts, BASE_UNIT)
        init_prices = await fetch_current_prices(init_markets)
        init_enriched = enrich_accounts_with_prices(init_accounts, init_prices, BASE_UNIT)
        for it in init_enriched:
            mkt = it.get("market")
            bal = it.get("balance")
            avg = it.get("avg_buy_price")
            if not mkt or not bal or not avg or bal <= 0 or avg <= 0:
                continue
            est = (bal * avg).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": est}
            st = ps.data.setdefault(mkt, {})
            st["entry_source"] = "pre_existing"
            for k, v in {
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0, "worst_pnl": Decimal("0"),
                "stop_last_sell_ts": None, "stop_cooldown_flag": False, "stop_pending_peak": None,
                "stop_cooldown_start_ts": None
            }.items():
                st.setdefault(k, v)
        print(f"[INFO] 초기 추적 {len(ps.buy_info)}개")
    except Exception as e:
        print(f"[WARN] 초기화 실패: {e}")

    await align_to_half_minute()

    while True:
        try:
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print("[ERR] 잔고 조회 실패:", e)
            await sleep_until_next_boundary()
            continue

        markets = build_market_list_from_accounts(raw_accounts, BASE_UNIT)
        try:
            price_map = await fetch_current_prices(markets)
        except Exception as e:
            print("[WARN] 가격 조회 실패:", e)
            price_map = {}

        enriched = enrich_accounts_with_prices(raw_accounts, price_map, BASE_UNIT)
        available_krw = get_available_krw(raw_accounts)

        actions = []
        sell_orders = []

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
            if bal * cur_price < MIN_NOTIONAL_KRW:
                continue

            st = ps.update_or_init(market, pnl, avg)

            # 손절 트리거 진입
            if ENABLE_STOP_TRAIL and (not st.get("stop_triggered")) and pnl <= STOP_TRIGGER_PNL:
                st["stop_triggered"] = True
                st["stop_last_peak"] = pnl
                st["worst_pnl"] = pnl
                st["stop_sells_done"] = 0
                st["stop_last_sell_ts"] = None
                st["stop_cooldown_flag"] = False
                st["stop_pending_peak"] = None
                st["stop_cooldown_start_ts"] = None
                actions.append({
                    "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                    "sell": False, "cat": "STOP_ENTER",
                    "reason": f"STOP_TRIGGER {pnl}% <= {STOP_TRIGGER_PNL}%"
                })
                continue

            # (기존 STOP 모드 로직 유지 - 생략 또는 기존 코드 그대로)
            if ENABLE_STOP_TRAIL and st.get("stop_triggered"):
                # 간략 대기 상태만 표시 (자세한 것은 기존 코드 유지 가능)
                actions.append({
                    "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                    "sell": False, "cat": "STOP_WAIT",
                    "reason": "stop_mode_active(simple)"
                })
                continue

            # ATR 모드
            if ENABLE_ATR_MODE and st.get("atr_mode_active"):
                try:
                    await ensure_atr_levels(market, st, cur_price)
                except Exception as e:
                    print(f"[WARN] ATR ensure 실패 {market}: {e}")
                atr_stop = st.get("atr_stop_price")
                atr_tp = st.get("atr_tp_price")

                # 트레일 활성중 ATR 재계산
                if st.get("atr_trail_active") and ATR_TRAIL_ATR_REFRESH:
                    try:
                        await ensure_atr_levels(market, st, cur_price)
                    except Exception as e:
                        print(f"[WARN] ATR trail refresh 실패 {market}: {e}")

                # 트레일 처리
                if st.get("atr_trail_active"):
                    if st.get("atr_trail_high") is None or cur_price > st.get("atr_trail_high"):
                        st["atr_trail_high"] = cur_price
                        if st.get("atr_value"):
                            new_stop = cur_price - st["atr_value"] * ATR_TRAIL_MULT
                            base_floor = st.get("atr_stop_price") or new_stop
                            if new_stop < base_floor:
                                new_stop = base_floor
                            if st.get("atr_trail_stop_price") is None or new_stop > st["atr_trail_stop_price"]:
                                st["atr_trail_stop_price"] = new_stop
                                print(f"[ATR-TRAIL] {market} high={cur_price} trail_stop→{new_stop}")
                    tsp = st.get("atr_trail_stop_price")
                    if tsp and cur_price <= tsp:
                        volume = safe_calc_volume(bal, Decimal("1"))
                        if volume > 0:
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": True, "cat": "ATR_TRAIL_EXIT",
                                "reason": f"ATR_TRAIL cur={cur_price} <= {tsp}"
                            })
                            sell_orders.append({
                                "market": market, "volume": volume, "pnl": pnl, "category": "ATR_TRAIL_EXIT",
                                "reason": "ATR_TRAIL_STOP_HIT", "state_ref": st, "portion": Decimal("1")
                            })
                            continue
                    continue  # 트레일 중이면 percent 로직 스킵

                # ATR STOP / TP
                if atr_stop and atr_tp and atr_stop < atr_tp:
                    if cur_price <= atr_stop and not st.get("atr_hit"):
                        volume = safe_calc_volume(bal, Decimal("1"))
                        if volume > 0:
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": True, "cat": "ATR_STOP",
                                "reason": f"ATR_STOP cur={cur_price} <= {atr_stop}"
                            })
                            sell_orders.append({
                                "market": market, "volume": volume, "pnl": pnl, "category": "ATR_STOP",
                                "reason": "ATR_STOP_HIT", "state_ref": st, "portion": Decimal("1")
                            })
                            continue
                    elif cur_price >= atr_tp and not st.get("atr_hit"):
                        st["atr_hit"] = True
                        if ATR_TRAIL_ENABLE and ATR_TP_SELL_PORTION > 0 and ATR_TP_SELL_PORTION < 1:
                            portion = ATR_TP_SELL_PORTION
                            volume = safe_calc_volume(bal, portion)
                            if volume > 0:
                                st["atr_trail_active"] = True
                                st["atr_trail_high"] = cur_price
                                if st.get("atr_value"):
                                    init_trail_stop = cur_price - st["atr_value"] * ATR_TRAIL_MULT
                                    base_floor = st.get("atr_stop_price") or init_trail_stop
                                    if init_trail_stop < base_floor:
                                        init_trail_stop = base_floor
                                    if init_trail_stop >= cur_price:
                                        init_trail_stop = cur_price * Decimal("0.995")
                                    st["atr_trail_stop_price"] = init_trail_stop
                                actions.append({
                                    "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                    "sell": True, "cat": "ATR_TP_PARTIAL",
                                    "reason": f"ATR_TP_PARTIAL cur={cur_price} >= {atr_tp} portion={portion}"
                                })
                                sell_orders.append({
                                    "market": market, "volume": volume, "pnl": pnl, "category": "ATR_TP_PARTIAL",
                                    "reason": "ATR_TP_PARTIAL_HIT", "state_ref": st, "portion": portion
                                })
                                continue
                        volume = safe_calc_volume(bal, Decimal("1"))
                        if volume > 0:
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": True, "cat": "ATR_TP",
                                "reason": f"ATR_TP cur={cur_price} >= {atr_tp}"
                            })
                            sell_orders.append({
                                "market": market, "volume": volume, "pnl": pnl, "category": "ATR_TP",
                                "reason": "ATR_TP_HIT", "state_ref": st, "portion": Decimal("1")
                            })
                            continue

                if st.get("atr_hit") and not st.get("atr_trail_active"):
                    continue

            # Percent 기반 fallback
            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                "sell": sell, "cat": category, "reason": reason
            })
            if sell:
                if category == "HARD_TP1":
                    portion = HARD_TP_SELL_PORTION
                elif category == "HARD_TP2":
                    portion = HARD_TP2_SELL_PORTION
                else:
                    portion = SELL_PORTION
                volume = safe_calc_volume(bal, portion)
                if volume > 0:
                    sell_orders.append({
                        "market": market, "volume": volume, "pnl": pnl, "category": category,
                        "reason": reason, "state_ref": st, "portion": portion
                    })

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
            dry = not LIVE_TRADING

            def post_realized():
                nonlocal realized
                if cur_price and volume > 0:
                    try:
                        realized += cur_price * volume
                    except:
                        pass

            def after_category_processing():
                if category in ("STOP_PART", "STOP_FINAL"):
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    st["stop_last_sell_ts"] = time.time()
                elif category in ("ATR_STOP", "ATR_TP"):
                    ps.mark_sold(market)
                elif category == "ATR_TP_PARTIAL":
                    ps.reduce_invested_after_sell(market, portion)
                elif category == "ATR_TRAIL_EXIT":
                    ps.mark_sold(market)
                else:
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
                            st["peak_pnl"] = pnl
                            st["armed"] = False
                            ps.reduce_invested_after_sell(market, portion)

            if dry:
                print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
                after_category_processing()
                post_realized()
                continue

            try:
                resp = await order_market_sell(access_key, secret_key, market, volume)
                uid = resp.get("uuid")
                print(f"[ORDER] SELL {market} cat={category} vol={volume} pnl={pnl}% uuid={uid} reason={reason}")
                if uid:
                    await asyncio.sleep(0.8)
                    try:
                        od = await get_order(access_key, secret_key, uid)
                        print(f"[ORDER-CHK] {market} state={od.get('state')} rem={od.get('remaining_volume')} fee={od.get('paid_fee')}")
                    except Exception as ce:
                        print(f"[WARN] 주문조회 실패 {market} {ce}")
            except Exception as e:
                print(f"[ERR] 매도 실패 {market}: {e}")
                continue

            after_category_processing()
            post_realized()

        if realized > 0:
            available_krw += realized

        # 교집합 매수 (원본 유지 - 필요시 조정)
        if INTERSECTION_BUY_ENABLED:
            try:
                inter_cands, meta = await get_intersection_candidates_safe()
            except Exception as e:
                inter_cands = []
                meta = {"source": "error", "empty_streak": -1}
                print(f"[WARN] 교집합 호출 실패:{e}")
            if meta.get("source") != "empty":
                inter_cands.sort(key=lambda x: x.get("avg_score", 0), reverse=True)
                buys = 0
                for row in inter_cands:
                    if buys >= INTERSECTION_MAX_BUY_PER_CYCLE:
                        break
                    mkt = row.get("market")
                    sc = row.get("avg_score")
                    if not mkt or sc is None:
                        continue
                    if mkt in EXCLUDED_MARKETS:
                        continue
                    st2 = ps.data.get(mkt)
                    if STOP_DISABLE_NEW_BUYS and st2 and st2.get("stop_triggered"):
                        continue
                    try:
                        scd = Decimal(str(sc))
                    except:
                        continue
                    if scd < INTERSECTION_MIN_SCORE:
                        continue
                    can, msg = ps.can_additional_buy(mkt, INTERSECTION_BUY_KRW, MAX_ADDITIONAL_BUYS,
                                                     MAX_TOTAL_INVEST_PER_MARKET)
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
                        buys += 1
                        continue
                    try:
                        resp = await order_market_buy_price(access_key, secret_key, mkt, INTERSECTION_BUY_KRW)
                        uid = resp.get("uuid")
                        print(f"[ORDER] INTERSECTION BUY {mkt} score={scd} KRW={INTERSECTION_BUY_KRW} uuid={uid}")
                        if uid:
                            await asyncio.sleep(1)
                            try:
                                od = await get_order(access_key, secret_key, uid)
                                print(f"[ORDER-CHK] INTERSECTION BUY {mkt} state={od.get('state')} fee={od.get('paid_fee')}")
                            except Exception as oe:
                                print(f"[WARN] 주문조회 실패 {mkt}: {oe}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        ps.record_buy(mkt, INTERSECTION_BUY_KRW)
                        available_krw -= INTERSECTION_BUY_KRW
                        buys += 1
                    except Exception as e:
                        print(f"[ERR] 교집합 매수 실패 {mkt}: {e}")

        # 범위 매수 (기존 유지)
        now_ts = time.time()
        is5m, window_start = is_five_minute_boundary(now_ts)
        if ENABLE_RANGE_BUY and is5m:
            buys_exe = 0
            cands = []
            for it2 in enriched:
                m = it2.get("market")
                pnl2 = it2.get("pnl_percent")
                cur2 = it2.get("current_price")
                if not m or pnl2 is None or cur2 is None:
                    continue
                if m in EXCLUDED_MARKETS:
                    continue
                st2 = ps.data.get(m)
                if STOP_DISABLE_NEW_BUYS and st2 and st2.get("stop_triggered"):
                    continue
                if ps.recently_sold(m):
                    continue
                if ps.bought_this_window(m, window_start):
                    continue
                if BUY_RANGE_LOW <= pnl2 <= BUY_RANGE_HIGH:
                    cands.append((pnl2, m))
            cands.sort(key=lambda x: x[0])
            for pnl2, m in cands:
                if buys_exe >= MAX_BUY_PER_WINDOW:
                    break
                if available_krw < RANGE_BUY_KRW:
                    break
                if RANGE_BUY_KRW < MIN_NOTIONAL_KRW:
                    break
                can, msg = ps.can_additional_buy(m, RANGE_BUY_KRW, MAX_ADDITIONAL_BUYS, MAX_TOTAL_INVEST_PER_MARKET)
                if not can:
                    print(msg)
                    continue
                if not LIVE_TRADING:
                    print(f"[DRY_BUY] {m} pnl={pnl2}% KRW={RANGE_BUY_KRW}")
                    ps.record_buy_window(m, window_start)
                    ps.data.setdefault(m, {})["entry_source"] = "range"
                    ps.record_buy(m, RANGE_BUY_KRW)
                    available_krw -= RANGE_BUY_KRW
                    buys_exe += 1
                    continue
                try:
                    resp = await order_market_buy_price(access_key, secret_key, m, RANGE_BUY_KRW)
                    uid = resp.get("uuid")
                    print(f"[ORDER] RANGE BUY {m} pnl={pnl2}% KRW={RANGE_BUY_KRW} uuid={uid}")
                    ps.record_buy_window(m, window_start)
                    ps.data.setdefault(m, {})["entry_source"] = "range"
                    ps.record_buy(m, RANGE_BUY_KRW)
                    available_krw -= RANGE_BUY_KRW
                    buys_exe += 1
                except Exception as e:
                    print(f"[ERR] RANGE 매수 실패 {m}: {e}")

        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 상태:")
            for a in actions:
                cat = a["cat"]
                status = "HOLD"
                if a["sell"]:
                    status = "SELL"
                elif cat in ("STOP_ENTER", "STOP_WAIT", "STOP_HOLD", "STOP_COOLDOWN", "STOP_EXIT"):
                    status = cat
                elif a["reason"].startswith("ARMED"):
                    status = "ARM"
                print(f"  {a['market']} pnl={a['pnl']} peak={a['peak']} armed={a['armed']} cat={cat} -> {status} {a['reason']}")

        await sleep_until_next_boundary()

async def main():
    user_no = 100013
    server_no = 21
    await monitor_positions(user_no, server_no)

if __name__ == "__main__":
    asyncio.run(main())