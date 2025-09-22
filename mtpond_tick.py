"""
MT POND Trading Bot (정리 버전)
- 전역 변수 구조 정리
- 동적 설정 적용 함수 단일화
- Controller / Monitor 역할 분리 명확화
- 중복/불필요 코드 제거
"""

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
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP

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

ENABLE_STOP_TRAIL = os.getenv("ENABLE_STOP_TRAIL", "1") == "1"
STOP_SELL_PORTION = Decimal(os.getenv("STOP_SELL_PORTION", "0.10"))
STOP_MAX_SELLS = int(os.getenv("STOP_MAX_SELLS", "6"))
STOP_DISABLE_NEW_BUYS = os.getenv("STOP_DISABLE_NEW_BUYS", "1") == "1"
STOP_REBOUND_COOLDOWN_SEC = int(os.getenv("STOP_REBOUND_COOLDOWN_SEC", "30"))
STOP_COOLDOWN_BATCH_SELL = os.getenv("STOP_COOLDOWN_BATCH_SELL", "1") == "1"
STOP_COOLDOWN_MAX_BATCH = int(os.getenv("STOP_COOLDOWN_MAX_BATCH", "5"))
STOP_RECOVERY_REQUIRE_COOLDOWN = os.getenv("STOP_RECOVERY_REQUIRE_COOLDOWN", "1") == "1"
STOP_RECOVERY_LOG = os.getenv("STOP_RECOVERY_LOG", "1") == "1"
STOP_SIMPLE_MODE = os.getenv("STOP_SIMPLE_MODE", "1") == "1"
STOP_INITIAL_SELL_PORTION = Decimal(os.getenv("STOP_INITIAL_SELL_PORTION", "0.5"))
STOP_FINAL_MOVE_THRESHOLD = Decimal(os.getenv("STOP_FINAL_MOVE_THRESHOLD", "0.1"))

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

# ============================================================
# 5. RUNTIME CONFIG (동적으로 변하는 값) - init_config / apply_dynamic_config 로 관리
# ============================================================
_CONFIG_INITIALIZED = False
# 동적 변경 대상:
# MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, INTERSECTION_BUY_KRW, MAX_TOTAL_INVEST_PER_MARKET
# BUY_RANGE_LOW, BUY_RANGE_HIGH, STOP_TRIGGER_PNL, STOP_PEAK_INCREMENT
# ADDITIONAL_BUY_KRW, USE_TICK_RATE, TICK_RATE

# 기본값(환경변수 초기값 또는 fallback)은 init_config 에서 설정
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
    """
    원격 설정(JSON)으로부터 동적 파라미터 업데이트.
    """
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
        # 최대 코인 수
        if "maxCoincnt" in cfg:
            try:
                nv = int(cfg["maxCoincnt"])
                if nv > 0 and nv != MAX_ACTIVE_MARKETS:
                    changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {nv}")
                    MAX_ACTIVE_MARKETS = nv
            except:
                print(f"[CFG] maxCoincnt 변환 실패:{cfg.get('maxCoincnt')}")

        # 초기매수 금액 initAmt
        if "initAmt" in cfg:
            val = to_decimal_safe(cfg.get("initAmt"), "initAmt")
            if val and val > 0:
                if val != RANGE_BUY_KRW:
                    changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW} -> {val}")
                    RANGE_BUY_KRW = val
                if USE_INITAMT_FOR_INTERSECTION and val != INTERSECTION_BUY_KRW:
                    changes.append(f"INTERSECTION_BUY_KRW {INTERSECTION_BUY_KRW} -> {val}")
                    INTERSECTION_BUY_KRW = val

        # 추가매수 금액 addAmt
        if "addAmt" in cfg:
            val = to_decimal_safe(cfg.get("addAmt"), "addAmt")
            if val and val > 0 and val != ADDITIONAL_BUY_KRW:
                changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW} -> {val}")
                ADDITIONAL_BUY_KRW = val

        # 종목별 최대 누적 투자 limitAmt
        if "limitAmt" in cfg:
            val = to_decimal_safe(cfg.get("limitAmt"), "limitAmt")
            if val and val > 0 and val != MAX_TOTAL_INVEST_PER_MARKET:
                changes.append(f"MAX_TOTAL_INVEST_PER_MARKET {MAX_TOTAL_INVEST_PER_MARKET} -> {val}")
                MAX_TOTAL_INVEST_PER_MARKET = val

        # 매수 범위 minMargin / maxMargin
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

        # 손절 트리거 lcRate
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

        # 반등 인크리먼트 lcGap
        if "lcGap" in cfg:
            g = to_decimal_safe(cfg.get("lcGap"), "lcGap")
            if g is not None:
                if g < 0:
                    print(f"[CFG] lcGap 음수 {g} -> abs 처리")
                    g = abs(g)
                if g != STOP_PEAK_INCREMENT:
                    changes.append(f"STOP_PEAK_INCREMENT {STOP_PEAK_INCREMENT} -> {g}")
                    STOP_PEAK_INCREMENT = g

        # tickRate / tickYN
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

    # 논리 가드
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
    # 순서 유지 + 중복 제거
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
# 10. 시세 조회 보조
# ============================================================
TRADABLE_MARKETS_CACHE = set()
TRADABLE_MARKETS_CACHE_TS = 0
TRADABLE_CACHE_TTL_SEC = 3600

async def load_tradable_markets():
    """Upbit 지원 마켓 캐시 (1시간)"""
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
    """
    다수 조회 실패 시 개별 재시도.
    """
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
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl, "prev_pnl": pnl, "armed": False, "avg_buy_price": avg_price,
                "hard_tp_taken": False, "hard_tp2_taken": False, "dynamic_hard_tp2": None,
                "trail_drop_dynamic": None, "entry_ts": now, "htp1_time": None,
                "momentum_tag": None, "last_update_ts": now,
                "entry_source": self.data.get(market, {}).get("entry_source"),
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0,
                "worst_pnl": pnl, "stop_last_sell_ts": None,
                "stop_cooldown_flag": False, "stop_pending_peak": None, "stop_cooldown_start_ts": None
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
# 14. 매도 판단 로직
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

def decide_sell(market, pnl: Decimal, state: dict):
    if state.get("stop_triggered"):
        return False, "", None
    peak = state["peak_pnl"]
    armed = state["armed"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    hard_tp2_taken = state.get("hard_tp2_taken", False)
    hard_tp2_target = get_state_decimal(state, "dynamic_hard_tp2", HARD_TP2_BASE)

    # HARD_TP2
    if hard_tp_taken and (not hard_tp2_taken) and pnl >= hard_tp2_target:
        label = "HARD_TP2"
        if state.get("dynamic_hard_tp2") and hard_tp2_target != HARD_TP2_BASE:
            label += "(dyn)"
        return True, f"{label} {pnl}% >= {hard_tp2_target}%", "HARD_TP2"

    # HARD_TP1
    if (not hard_tp_taken) and pnl >= HARD_TP:
        return True, f"HARD_TP1 {pnl}% >= {HARD_TP}%", "HARD_TP1"

    # ARM
    if (not armed) and pnl >= ARM_PNL:
        state["armed"] = True
        return False, f"ARMED {pnl}% (>= {ARM_PNL}%)", None

    # TRAILING
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
    """tickYN=Y && TICK_RATE>0 이면 TICK_RATE 간격, 아니면 5초 격자."""
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

async def get_order(access_key, secret_key, uuid_):
    params = {"uuid": uuid_}
    jwt_t = build_upbit_jwt_with_params(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {jwt_t}", "Accept": "application/json"}
    return await http_get_json("https://api.upbit.com/v1/order", headers=headers, params=params)

# ============================================================
# 19. 메인 모니터 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    """activeYN=Y 일 때 Controller 가 실행하는 메인 루프."""
    keys = await get_keys(user_no, server_no)
    if not keys:
        print("[ERR] API 키 없음 → 종료")
        return

    access_key, secret_key = keys
    ps = PositionState()
    print("=== 모니터 시작 ===")
    if EXCLUDED_MARKETS:
        print(f"[INFO] 제외 목록: {sorted(EXCLUDED_MARKETS)}")
    if ENABLE_STOP_TRAIL:
        print(f"[INFO] StopTrail trigger={STOP_TRIGGER_PNL}% portion={STOP_SELL_PORTION} incr={STOP_PEAK_INCREMENT} "
              f"max_sells={STOP_MAX_SELLS} cooldown={STOP_REBOUND_COOLDOWN_SEC}s simple={STOP_SIMPLE_MODE}")
    print(f"[INFO] MAX_ACTIVE_MARKETS={MAX_ACTIVE_MARKETS} ALLOW_ADDITIONAL_BUY_WHEN_FULL={ALLOW_ADDITIONAL_BUY_WHEN_FULL}")

    # 초기 계좌/가격 동기화
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
        # enrich (필요한 최소 데이터)
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
        # (1) 잔고 / 시세
        try:
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print(f"[ERR] 잔고 조회 실패: {e}")
            await dynamic_sleep()
            continue

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

        # (2) 활성 포지션
        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)
        if prev_active_count is None or prev_active_count != active_count:
            print(f"[PORTFOLIO] 활성 {active_count}개 (한도 {MAX_ACTIVE_MARKETS})")
            prev_active_count = active_count

        actions = []
        sell_orders = []

        # (3) 포지션별 매도 판단
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

            # 3-1) 손절 진입
            if ENABLE_STOP_TRAIL and (not st.get("stop_triggered")) and pnl <= STOP_TRIGGER_PNL:
                st["stop_triggered"] = True
                if STOP_SIMPLE_MODE:
                    st["stop_simple_initial_pnl"] = pnl
                    st["stop_simple_initial_sell_done"] = False
                    st["stop_simple_final_done"] = False
                    actions.append({
                        "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                        "armed": st["armed"], "sell": False, "cat": "STOP_ENTER",
                        "reason": f"SIMPLE_STOP_TRIGGER {pnl}% <= {STOP_TRIGGER_PNL}%"
                    })
                    portion = STOP_INITIAL_SELL_PORTION
                    volume = safe_calc_volume(bal, portion)
                    if volume > 0:
                        sell_orders.append({
                            "market": market, "volume": volume, "pnl": pnl,
                            "category": "STOP_SIMPLE_INIT",
                            "reason": f"SIMPLE_STOP INITIAL {portion * 100:.1f}%",
                            "state_ref": st, "portion": portion
                        })
                    else:
                        st["stop_simple_initial_sell_done"] = True
                        st["stop_simple_final_done"] = True
                        ps.mark_sold(market)
                    continue
                else:
                    # (확장 손절 모드) 필요 시 추가 로직 삽입
                    st["stop_last_peak"] = pnl
                    st["worst_pnl"] = pnl
                    st["stop_sells_done"] = 0
                    actions.append({
                        "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                        "armed": st["armed"], "sell": False, "cat": "STOP_ENTER",
                        "reason": f"STOP_TRIGGER {pnl}% <= {STOP_TRIGGER_PNL}%"
                    })
                    continue

            # 3-2) SIMPLE STOP 진행
            if ENABLE_STOP_TRAIL and st.get("stop_triggered") and STOP_SIMPLE_MODE:
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
                                "market": market, "volume": volume, "pnl": pnl,
                                "category": "STOP_SIMPLE_FINAL",
                                "reason": f"SIMPLE_STOP FINAL diff={diff} >= {STOP_FINAL_MOVE_THRESHOLD}",
                                "state_ref": st, "portion": portion
                            })
                        else:
                            st["stop_simple_final_done"] = True
                            ps.mark_sold(market)
                continue  # SIMPLE STOP 시 일반 추적 스킵

            # (추후: 복잡 StopTrail 확장 로직 필요 시 여기에)

            # 3-3) 일반 익절 / 트레일
            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                "armed": st["armed"], "sell": sell, "cat": category, "reason": reason
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

        # (4) 매도 실행
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
                print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
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
                else:
                    # 일반 익절/트레일
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
                    try:
                        realized += cur_price * volume
                    except:
                        pass
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
                        print(f"[WARN] 주문조회 실패 {market}: {ce}")
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
                try:
                    realized += cur_price * volume
                except:
                    pass

        if realized > 0:
            available_krw += realized

        # (5) 매도 후 재계산
        active_markets = get_active_markets(enriched)
        active_set = set(active_markets)
        active_count = len(active_markets)

        # (6) 교집합 매수
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

        # (7) 범위 매수
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

            for pnl, m in candidates:
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

        # (8) 액션 로그 요약
        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 상태 요약:")
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

        # (9) 대기
        await dynamic_sleep()

# ============================================================
# 20. Controller (activeYN 감시)
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

            # 비정상 종료 감지 재시작
            if active_flag is True and task and task.done():
                print(f"[CTRL {now}] 모니터 태스크 종료 감지 → 재시작")
                task = asyncio.create_task(monitor_positions(user_no, server_no))

        except Exception as e:
            print(f"[CTRL] 루프 예외: {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)

# ============================================================
# 21. main
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