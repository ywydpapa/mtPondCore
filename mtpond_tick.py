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
from decimal import ROUND_UP  # (NEW) 올림 사용

dotenv.load_dotenv()

engine = create_async_engine(os.getenv("dburl"), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

API_BASE = os.getenv("API_BASE", "").rstrip("/")
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "15"))

MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "20"))
ALLOW_ADDITIONAL_BUY_WHEN_FULL = os.getenv("ALLOW_ADDITIONAL_BUY_WHEN_FULL", "1") == "1"

# (옵션) 초과 시 자동 정리 기능 확장용 환경변수 (현재는 사용 안 하더라도 틀 제공)
PRUNE_EXCESS_ENABLED = os.getenv("PRUNE_EXCESS_ENABLED", "0") == "1"
TARGET_ACTIVE_MARKETS = int(os.getenv("TARGET_ACTIVE_MARKETS", "0"))  # 0이면 비활성
PRUNE_METHOD = os.getenv("PRUNE_METHOD", "SMALLEST_NOTIONAL")  # 또는 WORST_PNL

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
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "0.7"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))

ENABLE_RANGE_BUY = os.getenv("ENABLE_RANGE_BUY", "1") == "1"
BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-0.2"))
BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.15"))
RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
MAX_BUY_PER_WINDOW = int(os.getenv("MAX_BUY_PER_WINDOW", "999"))
SKIP_BUY_IF_RECENT_SELL = True

STOP_SIMPLE_MODE = os.getenv("STOP_SIMPLE_MODE", "1") == "1"
STOP_INITIAL_SELL_PORTION = Decimal(os.getenv("STOP_INITIAL_SELL_PORTION", "0.5"))  # 최초 50%
STOP_FINAL_MOVE_THRESHOLD = Decimal(os.getenv("STOP_FINAL_MOVE_THRESHOLD", "0.1"))  # PNL 절대변동 0.1%


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

INTERSECTION_BUY_ENABLED = os.getenv("INTERSECTION_BUY_ENABLED", "1") == "1"
INTERSECTION_MIN_SCORE = Decimal(os.getenv("INTERSECTION_MIN_SCORE", "10"))
INTERSECTION_BUY_KRW = Decimal(os.getenv("INTERSECTION_BUY_KRW", "200000"))
INTERSECTION_MAX_BUY_PER_CYCLE = int(os.getenv("INTERSECTION_MAX_BUY_PER_CYCLE", "1"))
INTERSECTION_BUY_COOLDOWN_SEC = int(os.getenv("INTERSECTION_BUY_COOLDOWN_SEC", "999999"))

INTERSECTION_USE_CACHE_ON_EMPTY = os.getenv("INTERSECTION_USE_CACHE_ON_EMPTY", "1") == "1"
INTERSECTION_CACHE_TTL_SEC = int(os.getenv("INTERSECTION_CACHE_TTL_SEC", "180"))
INTERSECTION_MAX_EMPTY_WARN = int(os.getenv("INTERSECTION_MAX_EMPTY_WARN", "5"))

# === (NEW) Intersection Tick Filter ===
INTERSECTION_TICK_FILTER_ENABLED = os.getenv("INTERSECTION_TICK_FILTER_ENABLED", "1") == "1"
INTERSECTION_TARGET_PROFIT_PCT = Decimal(os.getenv("INTERSECTION_TARGET_PROFIT_PCT", "0.4"))  # %
INTERSECTION_MAX_TICKS = int(os.getenv("INTERSECTION_MAX_TICKS", "10"))  # 10틱 포함

UPRISES_LAST_NONEMPTY: List[dict] = []
UPRISES_LAST_TS: float | None = None
UPRISES_EMPTY_STREAK: int = 0

MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "5"))
MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))

INTERVAL_SECONDS = 20

# --------------------------------
# 활성 포지션 계산 함수
# --------------------------------
def get_active_markets(enriched: List[dict]) -> List[str]:
    active = []
    for it in enriched:
        m = it.get("market"); bal = it.get("balance"); price = it.get("current_price")
        if not m or bal is None or price is None:
            continue
        try:
            notional = bal * price
            if notional >= MIN_NOTIONAL_KRW:
                active.append(m)
        except:
            pass
    return active

# (옵션) 초과 포지션 정리 후보 선정 함수 - 지금은 호출 안함 (원하면 monitor_positions 내부에서 사용)
def pick_prune_candidates(enriched: List[dict], target_count: int, method: str):
    # enriched 항목 중 활성 포지션만 추려 정렬
    rows = []
    for it in enriched:
        m = it.get("market"); bal = it.get("balance"); price = it.get("current_price"); pnl = it.get("pnl_percent")
        if not m or bal is None or price is None or pnl is None:
            continue
        try:
            notional = bal * price
            if notional >= MIN_NOTIONAL_KRW:
                rows.append({
                    "market": m,
                    "notional": notional,
                    "pnl": pnl,
                    "balance": bal
                })
        except:
            pass
    if len(rows) <= target_count:
        return []
    # 정렬 기준
    if method == "WORST_PNL":
        rows.sort(key=lambda r: r["pnl"])  # 손실이 큰(낮은) 순
    else:
        # 기본: 금액 작은 순
        rows.sort(key=lambda r: r["notional"])
    # 초과 개수만큼 후보 반환
    excess = len(rows) - target_count
    return rows[:excess]

async def fetch_mtpond_setup(user_no: int) -> dict | None:
    """
    /api/mtpondsetup/{userno} 가
      - [ { ... }, ... ] 형태면 첫 원소 사용
      - { ... } 단일 객체면 그대로 사용
      - 비어있으면 None
    """
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

        # 다양한 형태 수용
        if isinstance(data, list):
            if not data:
                return None
            if not isinstance(data[0], dict):
                return None
            return data[0]   # 첫 레코드만 사용
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        print(f"[WARN] mtpondsetup 네트워크 오류: {e}")
        return None


async def get_active_flag(user_no: int) -> bool | None:
    cfg = await fetch_mtpond_setup(user_no)
    if not cfg:
        return None
    val = str(cfg.get("activeYN", "")).upper()
    if val == "Y":
        return True
    if val == "N":
        return False
    return None


def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW
    global BUY_RANGE_LOW, BUY_RANGE_HIGH, MAX_TOTAL_INVEST_PER_MARKET
    global STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT
    try:
        if "maxCoincnt" in cfg and cfg["maxCoincnt"] is not None:
            MAX_ACTIVE_MARKETS = int(cfg["maxCoincnt"])
        if "initAmt" in cfg and cfg["initAmt"] is not None:
            # 초기 매수 전략이 교집합/범위 공용이면 둘 다 반영 (필요 시 분리)
            RANGE_BUY_KRW = Decimal(str(cfg["initAmt"]))
            # INTERSECTION_BUY_KRW = Decimal(str(cfg["initAmt"]))
        if "addAmt" in cfg and cfg["addAmt"] is not None:
            # 추가매수 금액을 RANGE 혹은 INTERSECTION 추가매수 로직에 반영하려면 별도 변수 필요
            pass
        if "limitAmt" in cfg and cfg["limitAmt"] is not None:
            MAX_TOTAL_INVEST_PER_MARKET = Decimal(str(cfg["limitAmt"]))
        if "minMargin" in cfg and cfg["minMargin"] is not None:
            BUY_RANGE_LOW = Decimal(str(cfg["minMargin"]))
        if "maxMargin" in cfg and cfg["maxMargin"] is not None:
            BUY_RANGE_HIGH = Decimal(str(cfg["maxMargin"]))
        if "lcRate" in cfg and cfg["lcRate"] is not None:
            STOP_TRIGGER_PNL = Decimal(str(cfg["lcRate"]))
        if "lcGap" in cfg and cfg["lcGap"] is not None:
            # 필요에 맞게 STOP_PEAK_INCREMENT 또는 STOP_FINAL_MOVE_THRESHOLD 중 선택
            # STOP_PEAK_INCREMENT = Decimal(str(cfg["lcGap"]))
            pass
    except Exception as e:
        print(f"[WARN] 동적 설정 적용 실패: {e}")


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

# Stop trail
ENABLE_STOP_TRAIL = os.getenv("ENABLE_STOP_TRAIL", "1") == "1"
STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.7"))
STOP_SELL_PORTION = Decimal(os.getenv("STOP_SELL_PORTION", "0.10"))
STOP_PEAK_INCREMENT = Decimal(os.getenv("STOP_PEAK_INCREMENT", "0.30"))
STOP_MAX_SELLS = int(os.getenv("STOP_MAX_SELLS", "6"))
STOP_DISABLE_NEW_BUYS = os.getenv("STOP_DISABLE_NEW_BUYS", "1") == "1"
STOP_REBOUND_COOLDOWN_SEC = int(os.getenv("STOP_REBOUND_COOLDOWN_SEC", "30"))
STOP_RECOVERY_EXIT_PNL = os.getenv("STOP_RECOVERY_EXIT_PNL", None)
if STOP_RECOVERY_EXIT_PNL is not None:
    try: STOP_RECOVERY_EXIT_PNL = Decimal(str(STOP_RECOVERY_EXIT_PNL))
    except: STOP_RECOVERY_EXIT_PNL = None
STOP_COOLDOWN_BATCH_SELL = os.getenv("STOP_COOLDOWN_BATCH_SELL", "1") == "1"
STOP_COOLDOWN_MAX_BATCH = int(os.getenv("STOP_COOLDOWN_MAX_BATCH", "5"))
STOP_RECOVERY_REQUIRE_COOLDOWN = os.getenv("STOP_RECOVERY_REQUIRE_COOLDOWN", "1") == "1"
STOP_RECOVERY_LOG = os.getenv("STOP_RECOVERY_LOG", "1") == "1"
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

async def get_keys(user_no: int, server_no: int) -> Optional[tuple]:
    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        return result.fetchone()

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
                    backoff = min(backoff * 2, MAX_BACKOFF); continue
                if resp.status_code >= 400:
                    raise RuntimeError(f"GET 실패 status={resp.status_code} body={resp.text}")
                return resp.json()
        except Exception:
            if attempt == max_retry: raise
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
                    backoff = min(backoff * 2, MAX_BACKOFF); continue
                if resp.status_code >= 400:
                    raise RuntimeError(f"POST 실패 status={resp.status_code} body={resp.text}")
                return resp.json()
        except Exception:
            if attempt == max_retry: raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

async def fetch_upbit_accounts(access_key: str, secret_key: str) -> List[Dict[str, Any]]:
    url = "https://api.upbit.com/v1/accounts"
    token = build_upbit_jwt_simple(access_key, secret_key)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return await http_get_json(url, headers=headers)

async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets: return {}
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(markets)}
    raw = await http_get_json(url, params=params)
    out = {}
    for item in raw:
        m = item.get("market"); p = item.get("trade_price")
        if m and p is not None:
            try: out[m] = Decimal(str(p))
            except: pass
    return out

# === (NEW) 호가단위 및 필요 틱 계산 ===
def calc_tick_size_upbit_krw(price: Decimal) -> Decimal:
    if price >= Decimal("2000000"): return Decimal("1000")
    if price >= Decimal("1000000"): return Decimal("500")
    if price >= Decimal("500000"):  return Decimal("100")
    if price >= Decimal("100000"):  return Decimal("50")
    if price >= Decimal("10000"):   return Decimal("10")
    if price >= Decimal("1000"):    return Decimal("5")
    if price >= Decimal("100"):     return Decimal("1")
    if price >= Decimal("10"):      return Decimal("0.1")
    return Decimal("0.01")

def ticks_needed_for_percent(price: Decimal, percent: Decimal) -> int:
    if price is None or price <= 0:
        return 10**9
    tick = calc_tick_size_upbit_krw(price)
    if tick <= 0:
        return 10**9
    target_abs = price * (percent / Decimal("100"))
    div = target_abs / tick
    ticks = int(div.to_integral_value(rounding=ROUND_UP))
    return max(1, ticks)

def build_market_list_from_accounts(accounts, base_unit="KRW") -> List[str]:
    res = []
    for acc in accounts:
        c = acc.get("currency"); u = acc.get("unit_currency")
        if not c or not u or u != base_unit or c == base_unit: continue
        try:
            bal = Decimal(str(acc.get("balance","0"))); locked = Decimal(str(acc.get("locked","0")))
        except:
            bal=Decimal("0"); locked=Decimal("0")
        if bal==0 and locked==0: continue
        m = f"{base_unit}-{c}"
        if ENFORCE_WHITELIST and WHITELIST_MARKETS and m not in WHITELIST_MARKETS: continue
        res.append(m)
    uniq=[]; seen=set()
    for m in res:
        if m not in seen: seen.add(m); uniq.append(m)
    return uniq

def get_available_krw(raw_accounts: List[Dict[str, Any]]) -> Decimal:
    for acc in raw_accounts:
        if acc.get("currency")=="KRW":
            try:
                bal=Decimal(str(acc.get("balance","0"))); locked=Decimal(str(acc.get("locked","0")))
                return bal-locked
            except: return Decimal("0")
    return Decimal("0")

def enrich_accounts_with_prices(accounts: List[dict], price_map: Dict[str, Decimal], base_unit="KRW"):
    out=[]
    for acc in accounts:
        c=acc.get("currency"); u=acc.get("unit_currency")
        avg_raw=acc.get("avg_buy_price")
        try: avg=Decimal(str(avg_raw)) if avg_raw not in (None,"","0") else Decimal("0")
        except: avg=Decimal("0")
        market=None; cur=None; pnl=None; ratio=None
        if c and u==base_unit and c!=base_unit:
            market=f"{base_unit}-{c}"
            cur = price_map.get(market)
            if cur and avg>0:
                try:
                    pnl=((cur-avg)/avg*Decimal("100")).quantize(Decimal("0.01"))
                    ratio=(cur/avg).quantize(Decimal("0.0001"))
                except: pass
        def to_d(v):
            try: return Decimal(str(v))
            except: return Decimal("0")
        out.append({
            "currency": c,"market": market,"unit_currency": u,
            "balance": to_d(acc.get("balance")),
            "locked": to_d(acc.get("locked")),
            "avg_buy_price": avg if avg>0 else None,
            "current_price": cur,
            "pnl_percent": pnl,
            "ratio_cur_over_avg": ratio
        })
    return out

async def order_market_sell(access_key, secret_key, market, volume:Decimal):
    params={"market":market,"side":"ask","volume":str(volume),"ord_type":"market"}
    jwt_t=build_upbit_jwt_with_params(access_key, secret_key, params)
    headers={"Authorization":f"Bearer {jwt_t}","Accept":"application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def order_market_buy_price(access_key, secret_key, market, krw_amount:Decimal):
    params={"market":market,"side":"bid","price":str(krw_amount),"ord_type":"price"}
    jwt_t=build_upbit_jwt_with_params(access_key, secret_key, params)
    headers={"Authorization":f"Bearer {jwt_t}","Accept":"application/json"}
    return await http_post_json(UPBIT_ORDER_URL, headers=headers, params=params)

async def get_order(access_key, secret_key, uuid_):
    params={"uuid":uuid_}
    jwt_t=build_upbit_jwt_with_params(access_key, secret_key, params)
    headers={"Authorization":f"Bearer {jwt_t}","Accept":"application/json"}
    return await http_get_json("https://api.upbit.com/v1/order", headers=headers, params=params)

class PositionState:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]]={}
        self.last_sell_time: Dict[str,float]={}
        self.last_buy_window: Dict[str,float]={}
        self.intersection_last_buy_time: Dict[str,float]={}
        self.buy_info: Dict[str,Dict[str,Any]]={}
    def update_or_init(self, market, pnl:Decimal, avg_price:Decimal):
        now=time.time()
        st=self.data.get(market)
        if st is None or st.get("avg_buy_price")!=avg_price:
            self.data[market]={
                "peak_pnl":pnl,"prev_pnl":pnl,"armed":False,"avg_buy_price":avg_price,
                "hard_tp_taken":False,"hard_tp2_taken":False,"dynamic_hard_tp2":None,
                "trail_drop_dynamic":None,"entry_ts":now,"htp1_time":None,
                "momentum_tag":None,"last_update_ts":now,
                "entry_source": self.data.get(market,{}).get("entry_source"),
                "stop_triggered":False,"stop_last_peak":None,"stop_sells_done":0,
                "worst_pnl":pnl,"stop_last_sell_ts":None,
                "stop_cooldown_flag":False,"stop_pending_peak":None,"stop_cooldown_start_ts":None
            }
            return self.data[market]
        if pnl>st["peak_pnl"]: st["peak_pnl"]=pnl
        if st.get("stop_triggered") and pnl < st.get("worst_pnl", pnl):
            st["worst_pnl"]=pnl
        st["prev_pnl"]=pnl; st["last_update_ts"]=now
        return st
    def remove(self, market):
        if market in self.data: del self.data[market]
    def mark_sold(self, market):
        self.last_sell_time[market]=time.time()
        self.remove(market)
        if market in self.buy_info: del self.buy_info[market]
    def reduce_invested_after_sell(self, market, portion:Decimal):
        if portion<=0: return
        info=self.buy_info.get(market)
        if not info or portion>=1: return
        try:
            remain=(Decimal("1")-portion)
            info["total_invested"]=(info["total_invested"]*remain).quantize(Decimal("0.0001"))
        except: pass
    def recently_sold(self, market):
        ts=self.last_sell_time.get(market)
        return bool(ts and (time.time()-ts)<10)
    def record_buy_window(self, market, w): self.last_buy_window[market]=w
    def bought_this_window(self, market, w): return self.last_buy_window.get(market)==w
    def mark_intersection_buy(self, market): self.intersection_last_buy_time[market]=time.time()
    def recently_bought_intersection(self, market, cooldown):
        ts=self.intersection_last_buy_time.get(market)
        return bool(ts and (time.time()-ts)<cooldown)
    def record_buy(self, market, krw_amount:Decimal):
        info=self.buy_info.get(market)
        if not info:
            self.buy_info[market]={"total_buys":1,"total_invested":krw_amount}
        else:
            info["total_buys"]+=1; info["total_invested"]+=krw_amount
    def get_buy_stats(self, market):
        info=self.buy_info.get(market)
        if not info: return 0, Decimal("0")
        return info["total_buys"], info["total_invested"]
    def can_additional_buy(self, market, next_amount:Decimal, max_additional_buys:int, max_total_invest:Decimal):
        st=self.data.get(market)
        if STOP_DISABLE_NEW_BUYS and st and st.get("stop_triggered"):
            return False, f"[SKIP] {market} 손절모드(추가매수금지)"
        total_buys,total_inv=self.get_buy_stats(market)
        if total_buys==0:
            if max_total_invest>0 and next_amount>=max_total_invest:
                return False,f"[SKIP] {market} 초기매수 금액({next_amount})>={max_total_invest}"
            return True,"INIT_OK"
        add_done=total_buys-1
        if add_done>=max_additional_buys:
            return False,f"[SKIP] {market} 추가매수 한도 초과 (이미 {add_done}회)"
        if max_total_invest>0 and (total_inv+next_amount)>=max_total_invest:
            return False,f"[SKIP] {market} 누적금액 {total_inv}+{next_amount}>={max_total_invest}"
        return True,"OK"

def get_state_decimal(state:dict, key:str, default:Decimal):
    v=state.get(key)
    if isinstance(v,Decimal): return v
    try:
        if v is not None: return Decimal(str(v))
    except: pass
    return default

def decide_sell(market, pnl:Decimal, state:dict):
    if state.get("stop_triggered"): return False,"",None
    peak=state["peak_pnl"]; armed=state["armed"]
    hard_tp_taken=state.get("hard_tp_taken",False)
    hard_tp2_taken=state.get("hard_tp2_taken",False)
    hard_tp2_target=get_state_decimal(state,"dynamic_hard_tp2",HARD_TP2_BASE)
    if hard_tp_taken and (not hard_tp2_taken) and pnl>=hard_tp2_target:
        label="HARD_TP2"
        if state.get("dynamic_hard_tp2") and hard_tp2_target!=HARD_TP2_BASE: label+="(dyn)"
        return True,f"{label} {pnl}% >= {hard_tp2_target}%","HARD_TP2"
    if (not hard_tp_taken) and pnl>=HARD_TP:
        return True,f"HARD_TP1 {pnl}% >= {HARD_TP}%","HARD_TP1"
    if (not armed) and pnl>=ARM_PNL:
        state["armed"]=True
        return False,f"ARMED {pnl}% (>= {ARM_PNL}%)",None
    trail_drop_used=get_state_decimal(state,"trail_drop_dynamic",TRAIL_DROP)
    if state["armed"] and peak>=TRAIL_START_PNL:
        drop=peak-pnl
        if drop>=trail_drop_used:
            return True,f"TRAIL_DROP {drop}% >= {trail_drop_used}% (peak={peak}% now={pnl}%)","TRAIL"
    return False,"",None

def safe_calc_volume(balance:Decimal, portion:Decimal):
    portion=min(portion,Decimal("1"))
    vol= balance if portion>=1 else balance*portion
    vol=vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    return vol if vol>0 else Decimal("0")

async def align_to_half_minute():
    now=time.time(); rem=now%INTERVAL_SECONDS
    if rem>0.01: await asyncio.sleep(INTERVAL_SECONDS-rem)

async def sleep_until_next_boundary():
    now=time.time()
    nxt=math.floor(now/INTERVAL_SECONDS)*INTERVAL_SECONDS+INTERVAL_SECONDS
    await asyncio.sleep(max(0,nxt-now))

def is_five_minute_boundary(ts:float):
    window_start=ts-(ts%FIVE_MIN_SECONDS)
    lt=time.localtime(ts)
    return (lt.tm_min%5==0 and lt.tm_sec==0), window_start

def _is_effectively_empty(cands):
    if not cands: return True
    return not any((c.get("market") and c.get("avg_score") is not None) for c in cands if isinstance(c,dict))

def _normalize_uprises(raw):
    if raw is None: return []
    if isinstance(raw,list):
        out=[]
        for r in raw:
            if not isinstance(r,dict): continue
            m=r.get("market"); s=r.get("avg_score")
            if m and s is not None: out.append({"market":m,"avg_score":s})
        return out
    if isinstance(raw,str):
        t=raw.strip()
        if not t: return []
        try: js=json.loads(t)
        except: return []
        return _normalize_uprises(js)
    if isinstance(raw,dict):
        if "multi_tf_intersection" in raw:
            return _normalize_uprises(raw.get("multi_tf_intersection",{}).get("items",[]))
        if "items" in raw and isinstance(raw["items"],list):
            return _normalize_uprises(raw["items"])
        if raw.get("market") and raw.get("avg_score") is not None:
            return [{"market":raw["market"],"avg_score":raw["avg_score"]}]
        return []
    return []

async def get_intersection_candidates_safe():
    global UPRISES_LAST_NONEMPTY, UPRISES_LAST_TS, UPRISES_EMPTY_STREAK
    meta={"source":None,"empty_streak":UPRISES_EMPTY_STREAK,"fresh_ts":None,"cache_age":None}
    try: raw=topuprise.uprises()
    except Exception as e:
        raw=None; print(f"[WARN] uprises() 예외:{e}")
    cands=_normalize_uprises(raw); now=time.time(); meta["fresh_ts"]=now
    if _is_effectively_empty(cands):
        UPRISES_EMPTY_STREAK+=1; meta["empty_streak"]=UPRISES_EMPTY_STREAK
        use_cache=False
        if INTERSECTION_USE_CACHE_ON_EMPTY and UPRISES_LAST_NONEMPTY and UPRISES_LAST_TS:
            age=now-UPRISES_LAST_TS; meta["cache_age"]=age
            if age<=INTERSECTION_CACHE_TTL_SEC: use_cache=True
        if use_cache:
            meta["source"]="cache"; cands=UPRISES_LAST_NONEMPTY
        else:
            meta["source"]="empty"
        return cands, meta
    else:
        UPRISES_EMPTY_STREAK=0
        UPRISES_LAST_NONEMPTY=cands; UPRISES_LAST_TS=now
        meta["source"]="fresh"; meta["empty_streak"]=0; meta["cache_age"]=0
        return cands, meta

async def monitor_positions(user_no:int, server_no:int):
    keys = await get_keys(user_no, server_no)
    active_flag = await get_active_flag(user_no)
    if active_flag is False:
        print(f"[INFO] activeYN=N 감지 → monitor_positions 종료 (user={user_no})")
        return
    if not keys:
        print("API 키 없음")
        return
    access_key, secret_key = keys
    ps = PositionState()
    print("=== 시작 ===")
    if EXCLUDED_MARKETS:
        print(f"[INFO] 제외:{sorted(EXCLUDED_MARKETS)}")
    if ENABLE_STOP_TRAIL:
        print(f"[INFO] StopTrail ON trigger={STOP_TRIGGER_PNL}% portion={STOP_SELL_PORTION} incr={STOP_PEAK_INCREMENT} "
              f"max_sells={STOP_MAX_SELLS} cooldown={STOP_REBOUND_COOLDOWN_SEC}s "
              f"recovery_exit={STOP_RECOVERY_EXIT_PNL} batch={STOP_COOLDOWN_BATCH_SELL} max_batch={STOP_COOLDOWN_MAX_BATCH} "
              f"simple_mode={STOP_SIMPLE_MODE}")
        if STOP_SIMPLE_MODE:
            print(f"[INFO] SIMPLE_STOP initial_portion={STOP_INITIAL_SELL_PORTION} final_move={STOP_FINAL_MOVE_THRESHOLD}%")
    print(f"[INFO] MAX_ACTIVE_MARKETS={MAX_ACTIVE_MARKETS} ALLOW_ADDITIONAL_BUY_WHEN_FULL={ALLOW_ADDITIONAL_BUY_WHEN_FULL}")

    # 초기 계좌 스냅샷
    try:
        init_accounts = await fetch_upbit_accounts(access_key, secret_key)
        init_markets = build_market_list_from_accounts(init_accounts, BASE_UNIT)
        init_prices = await fetch_current_prices(init_markets)
        init_enriched = enrich_accounts_with_prices(init_accounts, init_prices, BASE_UNIT)
        for it in init_enriched:
            mkt = it.get("market"); bal = it.get("balance"); avg = it.get("avg_buy_price")
            if not mkt or not bal or not avg or bal <= 0 or avg <= 0:
                continue
            est = (bal * avg).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": est}
            st = ps.data.setdefault(mkt, {})
            st["entry_source"] = "pre_existing"
            # stop 관련 기본 필드
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

    prev_active_count = None  # 활성 포지션 변화 감지 로그용

    while True:
        # 1) 계좌 / 시세 갱신
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

        # 2) 활성 포지션 목록
        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)
        if prev_active_count is None or prev_active_count != active_count:
            print(f"[PORTFOLIO] 활성 포지션 {active_count}개 (한도 {MAX_ACTIVE_MARKETS})")
            prev_active_count = active_count

        actions = []
        sell_orders = []

        # 3) 포지션 모니터 및 매도 의사결정
        for it in enriched:
            market = it.get("market"); pnl = it.get("pnl_percent")
            avg = it.get("avg_buy_price"); bal = it.get("balance"); cur_price = it.get("current_price")
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

            # -------------------------------
            # 손절 모드 진입
            # -------------------------------
            if ENABLE_STOP_TRAIL and (not st.get("stop_triggered")) and pnl <= STOP_TRIGGER_PNL:
                st["stop_triggered"] = True
                if STOP_SIMPLE_MODE:
                    # SIMPLE STOP 초기화
                    st["stop_simple_initial_pnl"] = pnl
                    st["stop_simple_initial_sell_done"] = False
                    st["stop_simple_final_done"] = False
                    actions.append({
                        "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                        "sell": False, "cat": "STOP_ENTER",
                        "reason": f"SIMPLE_STOP_TRIGGER {pnl}% <= {STOP_TRIGGER_PNL}%"
                    })
                    portion = STOP_INITIAL_SELL_PORTION
                    volume = safe_calc_volume(bal, portion)
                    if volume > 0:
                        sell_orders.append({
                            "market": market, "volume": volume, "pnl": pnl, "category": "STOP_SIMPLE_INIT",
                            "reason": f"SIMPLE_STOP INITIAL {portion*100}%", "state_ref": st, "portion": portion
                        })
                    else:
                        st["stop_simple_initial_sell_done"] = True
                        st["stop_simple_final_done"] = True
                        ps.mark_sold(market)
                    continue
                else:
                    # 기존 복잡 Stop Trail 초기화
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

            # -------------------------------
            # 손절 모드 진행
            # -------------------------------
            if ENABLE_STOP_TRAIL and st.get("stop_triggered"):
                if STOP_SIMPLE_MODE:
                    # SIMPLE STOP
                    if st.get("stop_simple_final_done"):
                        continue
                    init_pnl = st.get("stop_simple_initial_pnl")
                    if init_pnl is not None and st.get("stop_simple_initial_sell_done") and not st.get("stop_simple_final_done"):
                        diff = abs(pnl - init_pnl)
                        if diff >= STOP_FINAL_MOVE_THRESHOLD:
                            portion = Decimal("1")
                            volume = safe_calc_volume(bal, portion)
                            if volume > 0:
                                sell_orders.append({
                                    "market": market, "volume": volume, "pnl": pnl, "category": "STOP_SIMPLE_FINAL",
                                    "reason": f"SIMPLE_STOP FINAL diff={diff}>= {STOP_FINAL_MOVE_THRESHOLD}",
                                    "state_ref": st, "portion": portion
                                })
                            else:
                                st["stop_simple_final_done"] = True
                                ps.mark_sold(market)
                    continue  # SIMPLE STOP 은 여기서 종료

                # -------- 복잡 Stop Trail (기존) --------
                prev_peak = st.get("stop_last_peak")
                if prev_peak is None:
                    st["stop_last_peak"] = pnl
                    prev_peak = pnl

                # (A) 쿨다운 상태에서 회복 종료 체크
                if st.get("stop_cooldown_flag") and STOP_RECOVERY_EXIT_PNL is not None:
                    if pnl >= STOP_RECOVERY_EXIT_PNL:
                        if STOP_RECOVERY_LOG:
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": False, "cat": "STOP_EXIT",
                                "reason": f"RECOVERY_EXIT {pnl}% >= {STOP_RECOVERY_EXIT_PNL}%"
                            })
                        st["stop_triggered"] = False
                        st["stop_cooldown_flag"] = False
                        st["stop_pending_peak"] = None
                        st["stop_cooldown_start_ts"] = None
                        st["stop_last_peak"] = None
                        st["peak_pnl"] = pnl

                # (B) 쿨다운 상태에서 배치 매도
                if st.get("stop_cooldown_flag"):
                    last_ts = st.get("stop_last_sell_ts")
                    if last_ts is not None and (time.time() - last_ts) >= STOP_REBOUND_COOLDOWN_SEC:
                        pending_peak = st.get("stop_pending_peak") or st.get("stop_last_peak")
                        if pending_peak is not None:
                            total_rebound = pending_peak - st.get("stop_last_peak", pending_peak)
                            if total_rebound > 0:
                                increments = int(total_rebound / STOP_PEAK_INCREMENT)
                                if increments > 0:
                                    if not STOP_COOLDOWN_BATCH_SELL:
                                        increments = 1
                                    if STOP_MAX_SELLS > 0:
                                        increments = min(
                                            increments,
                                            STOP_COOLDOWN_MAX_BATCH,
                                            max(0, (STOP_MAX_SELLS - st["stop_sells_done"]))
                                        )
                                    else:
                                        increments = min(increments, STOP_COOLDOWN_MAX_BATCH)
                                    for i in range(increments):
                                        portion = STOP_SELL_PORTION
                                        volume = safe_calc_volume(bal, portion)
                                        if volume <= 0:
                                            break
                                        sell_orders.append({
                                            "market": market, "volume": volume, "pnl": pnl, "category": "STOP_PART",
                                            "reason": f"STOP_BATCH i={i+1}/{increments} rebound={total_rebound:.2f}%",
                                            "state_ref": st, "portion": portion
                                        })
                                        actions.append({
                                            "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                            "sell": True, "cat": "STOP_PART",
                                            "reason": f"STOP_BATCH i={i+1}/{increments}"
                                        })
                                        st["stop_sells_done"] += 1
                                        if STOP_MAX_SELLS > 0 and st["stop_sells_done"] >= STOP_MAX_SELLS:
                                            remaining_volume = bal - volume
                                            if remaining_volume > 0:
                                                v_all = safe_calc_volume(remaining_volume, Decimal("1"))
                                                if v_all > 0:
                                                    sell_orders.append({
                                                        "market": market, "volume": v_all, "pnl": pnl, "category": "STOP_FINAL",
                                                        "reason": f"STOP_MAX_SELLS {st['stop_sells_done']} reached",
                                                        "state_ref": st, "portion": Decimal("1")
                                                    })
                                                    actions.append({
                                                        "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                                                        "armed": st["armed"], "sell": True,
                                                        "cat": "STOP_FINAL",
                                                        "reason": f"STOP_MAX_SELLS {st['stop_sells_done']} reached"
                                                    })
                                            break
                                    st["stop_last_peak"] = st.get("stop_last_peak") + (STOP_PEAK_INCREMENT * increments)

                        st["stop_cooldown_flag"] = False
                        st["stop_pending_peak"] = None
                        st["stop_cooldown_start_ts"] = None

                # (C) 쿨다운 아님 → 반등 판단
                if st.get("stop_triggered") and not st.get("stop_cooldown_flag"):
                    if pnl > prev_peak + STOP_PEAK_INCREMENT:
                        last_ts = st.get("stop_last_sell_ts")
                        now_ts = time.time()
                        cooldown_remain = None
                        if last_ts is not None:
                            elapsed = now_ts - last_ts
                            if elapsed < STOP_REBOUND_COOLDOWN_SEC:
                                cooldown_remain = STOP_REBOUND_COOLDOWN_SEC - elapsed
                        if cooldown_remain is not None:
                            st["stop_cooldown_flag"] = True
                            st["stop_cooldown_start_ts"] = st.get("stop_cooldown_start_ts") or now_ts
                            prev_pending = st.get("stop_pending_peak")
                            if prev_pending is None or pnl > prev_pending:
                                st["stop_pending_peak"] = pnl
                            remain = int(cooldown_remain)
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": False, "cat": "STOP_COOLDOWN",
                                "reason": f"COOLDOWN_WAIT {remain}s pend_peak={st['stop_pending_peak']}"
                            })
                            if STOP_RECOVERY_EXIT_PNL is not None and pnl >= STOP_RECOVERY_EXIT_PNL and \
                               ((not STOP_RECOVERY_REQUIRE_COOLDOWN) or st["stop_cooldown_flag"]):
                                if STOP_RECOVERY_LOG:
                                    actions.append({
                                        "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                        "sell": False, "cat": "STOP_EXIT",
                                        "reason": f"RECOVERY_EXIT {pnl}% >= {STOP_RECOVERY_EXIT_PNL}% (cooldown)"
                                    })
                                st["stop_triggered"] = False
                                st["stop_cooldown_flag"] = False
                                st["stop_pending_peak"] = None
                                st["stop_cooldown_start_ts"] = None
                                st["stop_last_peak"] = None
                                st["peak_pnl"] = pnl
                            continue
                        portion = STOP_SELL_PORTION
                        volume = safe_calc_volume(bal, portion)
                        if volume > 0:
                            sell_orders.append({
                                "market": market, "volume": volume, "pnl": pnl, "category": "STOP_PART",
                                "reason": f"STOP_REBOUND {pnl}% > {prev_peak}+{STOP_PEAK_INCREMENT}",
                                "state_ref": st, "portion": portion
                            })
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": True, "cat": "STOP_PART",
                                "reason": f"STOP_REBOUND {pnl}% > {prev_peak}+{STOP_PEAK_INCREMENT}"
                            })
                            st["stop_last_peak"] = pnl
                            st["stop_sells_done"] += 1
                            if STOP_MAX_SELLS > 0 and st["stop_sells_done"] >= STOP_MAX_SELLS:
                                remaining_volume = bal - volume
                                if remaining_volume > 0:
                                    v_all = safe_calc_volume(remaining_volume, Decimal("1"))
                                    if v_all > 0:
                                        sell_orders.append({
                                            "market": market, "volume": v_all, "pnl": pnl, "category": "STOP_FINAL",
                                            "reason": f"STOP_MAX_SELLS {st['stop_sells_done']} reached",
                                            "state_ref": st, "portion": Decimal("1")
                                        })
                                        actions.append({
                                            "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                                            "armed": st["armed"], "sell": True,
                                            "cat": "STOP_FINAL",
                                            "reason": f"STOP_MAX_SELLS {st['stop_sells_done']} reached"
                                        })
                        else:
                            actions.append({
                                "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                                "sell": False, "cat": "STOP_HOLD",
                                "reason": "NO_VOLUME"
                            })
                        continue
                    else:
                        actions.append({
                            "market": market, "pnl": pnl, "peak": st["peak_pnl"], "armed": st["armed"],
                            "sell": False, "cat": "STOP_WAIT",
                            "reason": f"WAIT_REBOUND cur={pnl}% need>{prev_peak}+{STOP_PEAK_INCREMENT}"
                        })
                        continue

            # 4) 일반 익절 / 추적
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
                if volume <= 0:
                    if category in ("TRAIL", "HARD_TP2") and portion >= 1:
                        ps.mark_sold(market)
                    continue
                sell_orders.append({
                    "market": market, "volume": volume, "pnl": pnl, "category": category,
                    "reason": reason, "state_ref": st, "portion": portion
                })

        # 5) 매도 실행
        realized = Decimal("0")
        for so in sell_orders:
            market = so["market"]; volume = so["volume"]; category = so["category"]
            pnl = so["pnl"]; reason = so["reason"]; st = so["state_ref"]; portion = so["portion"]
            if volume <= 0:
                continue
            cur_price = None
            for _it in enriched:
                if _it.get("market") == market:
                    cur_price = _it.get("current_price")
                    break

            # DRY 모드
            if not LIVE_TRADING:
                print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
                if category in ("STOP_SIMPLE_INIT", "STOP_SIMPLE_FINAL"):
                    if category == "STOP_SIMPLE_INIT":
                        st["stop_simple_initial_sell_done"] = True
                        if portion < 1:
                            ps.reduce_invested_after_sell(market, portion)
                        else:
                            # 만약 100% 전량이 팔렸다면 바로 종료
                            st["stop_simple_final_done"] = True
                            ps.mark_sold(market)
                    else:  # STOP_SIMPLE_FINAL
                        st["stop_simple_final_done"] = True
                        ps.mark_sold(market)
                    if cur_price and portion > 0:
                        try: realized += cur_price * volume
                        except: pass
                    continue

                if category in ("STOP_PART", "STOP_FINAL"):
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                    st["stop_last_sell_ts"] = time.time()
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
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
                continue

            # LIVE 모드
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

            if category in ("STOP_SIMPLE_INIT", "STOP_SIMPLE_FINAL"):
                if category == "STOP_SIMPLE_INIT":
                    st["stop_simple_initial_sell_done"] = True
                    if portion < 1:
                        ps.reduce_invested_after_sell(market, portion)
                    else:
                        st["stop_simple_final_done"] = True
                        ps.mark_sold(market)
                else:
                    st["stop_simple_final_done"] = True
                    ps.mark_sold(market)
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
                continue

            if category in ("STOP_PART", "STOP_FINAL"):
                if portion >= 1:
                    ps.mark_sold(market)
                else:
                    ps.reduce_invested_after_sell(market, portion)
                st["stop_last_sell_ts"] = time.time()
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

            if cur_price and portion > 0:
                try: realized += cur_price * volume
                except:
                    pass

        if realized > 0:
            available_krw += realized

        # 6) 매도 후 활성 포지션 재계산
        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)

        # 7) 교집합 매수
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
                    mkt = row.get("market"); sc = row.get("avg_score")
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
                        print(f"[CAP] 교집합 매수 제한: active={active_count}/{MAX_ACTIVE_MARKETS} mkt={mkt} existing={is_existing}")
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
                        buys += 1
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
                                print(f"[ORDER-CHK] INTERSECTION BUY {mkt} state={od.get('state')} fee={od.get('paid_fee')}")
                            except Exception as oe:
                                print(f"[WARN] 주문조회 실패 {mkt}: {oe}")
                        ps.mark_intersection_buy(mkt)
                        ps.data.setdefault(mkt, {})["entry_source"] = "intersection"
                        ps.record_buy(mkt, INTERSECTION_BUY_KRW)
                        available_krw -= INTERSECTION_BUY_KRW
                        buys += 1
                        if not is_existing:
                            active_count += 1
                            active_set.add(mkt)
                    except Exception as e:
                        print(f"[ERR] 교집합 매수 실패 {mkt}: {e}")

        # 8) 범위 매수
        now_ts = time.time()
        is5m, window_start = is_five_minute_boundary(now_ts)
        if ENABLE_RANGE_BUY and is5m:
            buys_exe = 0
            cands = []
            for it2 in enriched:
                m = it2.get("market"); pnl = it2.get("pnl_percent"); cur = it2.get("current_price")
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
                    cands.append((pnl, m))
            cands.sort(key=lambda x: x[0])

            for pnl, m in cands:
                if buys_exe >= MAX_BUY_PER_WINDOW:
                    break
                if available_krw < RANGE_BUY_KRW:
                    break
                if RANGE_BUY_KRW < MIN_NOTIONAL_KRW:
                    break
                is_existing = (m in active_set)
                if active_count >= MAX_ACTIVE_MARKETS and ((not is_existing) or (not ALLOW_ADDITIONAL_BUY_WHEN_FULL)):
                    print(f"[CAP] 범위매수 제한: active={active_count}/{MAX_ACTIVE_MARKETS} m={m} existing={is_existing}")
                    continue
                can, msg = ps.can_additional_buy(m, RANGE_BUY_KRW, MAX_ADDITIONAL_BUYS, MAX_TOTAL_INVEST_PER_MARKET)
                if not can:
                    print(msg)
                    continue
                if not LIVE_TRADING:
                    print(f"[DRY_BUY] {m} pnl={pnl}% KRW={RANGE_BUY_KRW}")
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
                    print(f"[ORDER] RANGE BUY {m} pnl={pnl}% KRW={RANGE_BUY_KRW} uuid={uid}")
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
        # 9) 액션 로그
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
        # 10) 대기
        await sleep_until_next_boundary()

_last_cfg_signature = None

async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    last_state: bool | None = None
    print(f"[CTRL] MT POND Controller 시작 user={user_no} server={server_no} poll={CONTROLLER_POLL_SEC}s")
    while True:
        cfg = await fetch_mtpond_setup(user_no)
        if cfg:
            # activeYN 처리
            flag_val = str(cfg.get("activeYN", "")).upper()
            active_flag = (flag_val == "Y")
            # 동적 파라미터 적용 (원하면)
            sig = tuple(sorted((k, cfg[k]) for k in cfg if k != 'activeYN'))
            global _last_cfg_signature
            if sig != _last_cfg_signature:
                apply_dynamic_config(cfg)
                _last_cfg_signature = sig
        else:
            active_flag = None
        try:
            flag = await get_active_flag(user_no)
            now = time.strftime("%H:%M:%S")
            if flag is True:
                if task is None or task.done():
                    print(f"[CTRL {now}] activeYN=Y → monitor_positions 시작")
                    task = asyncio.create_task(monitor_positions(user_no, server_no))
                else:
                    pass
            elif flag is False:
                if task and not task.done():
                    print(f"[CTRL {now}] activeYN=N → 모니터 태스크 중지 시도")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        print("[CTRL] monitor_positions 정상 취소")
                    except Exception as e:
                        print(f"[CTRL] monitor_positions 예외 종료: {e}")
                    task = None
                else:
                    # 이미 없음
                    pass
            else:
                # flag is None (조회 실패)
                print(f"[CTRL {now}] activeYN 조회 실패(None) → 상태 유지")

            last_state = flag

            # task 가 예기치 않게 끝났는데 activeYN=Y 라면 재시작 시도
            if flag is True and task and task.done():
                print(f"[CTRL {now}] monitor_positions 비정상 종료 감지 → 재시작")
                task = asyncio.create_task(monitor_positions(user_no, server_no))

            await asyncio.sleep(CONTROLLER_POLL_SEC)
        except Exception as e:
            print(f"[CTRL] 루프 예외: {e}")
            await asyncio.sleep(CONTROLLER_POLL_SEC)


async def sleep_until_next_boundary():
    now=time.time()
    nxt=math.floor(now/INTERVAL_SECONDS)*INTERVAL_SECONDS+INTERVAL_SECONDS
    await asyncio.sleep(max(0,nxt-now))

async def main():
    user_no = int(os.getenv("USER_NO", "100013"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청(Ctrl+C)")
