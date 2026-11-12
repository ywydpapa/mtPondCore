"""전략 8: [Micro-Tick Scalper] 500원 미만 동전주 1~3틱 초고속 스캘핑 퀀트 트레이딩 엔진.

[핵심 전략 정의]
1. 대상 코인 유니버스 (500원 미만 스윗스팟 & 고유동성):
   - 현재가 500원 미만 (또는 2~3틱 안에 +0.2% 이상 이익 달성 가능한 코인).
   - 거래대금 및 체결 빈도(BB-Patterns 10분 체결 30건 이상, 거래대금 상위) 최우선 선별.
   - 업비트 호가창 구조상 500원 미만 코인은 1~3틱만으로도 수수료(0.10%)를 훌쩍 넘는 +0.40%~+1.00% 순익을 즉시 확보 가능.

2. 1~3틱 초고속 마진 타겟팅 (Tick-Aware Scalp):
   - 1틱 가치 >= 0.45% (예: 100~220원, 10~22원): 매수가 + 1틱 지정가 매도!
   - 1틱 가치 0.20% ~ 0.45% (예: 220~500원): 매수가 + 2틱 지정가 매도! (+0.40%~+0.80%)
   - 그 외: 최소 3틱 (또는 +0.45% 이상 보장) 지정가 매도!
   - 수수료 0.10%를 훌쩍 넘는 실질 순익을 단 1~2호가 만에 즉시 확정.

3. 즉시 선(先) 지정가 매도 배치 (Spike Catcher):
   - 매수 체결 즉시 호가창에 지정가 매도를 걸어 호가 흔들림(Noise/Wick)에 즉시 익절.

4. 시장 안전망 및 리스크 관리:
   - BTC 3단계 체력 필터 (NORMAL / CAUTION / EMERGENCY: 비트코인 급락 시 신규 매수 차단).
   - 30분봉 진폭 연동 4단계 동적 손절 + 대폭락 안전망 (-14.0% ~ -15.0%).
   - 거래불가/상장폐지 코인 5중 방어 격리 로직 (API 404 및 지갑 오류 원천 차단).

5. DB 및 사용자 환경 연동:
   - DB traceUser 테이블 (apiKey1, apiKey2) + .env 폴백.
   - API_BASE 웹 설정(buyAmt, maxCoincnt 등) 연동.
   - Redis mtpond:status 웹 컨트롤러 기동/정지 완벽 지원.
"""

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
from typing import Optional, Dict, Any, List, Tuple, Set
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR, ROUND_CEILING
import dotenv
import httpx
import jwt
from sqlalchemy import text
from mtpond_common import (
    market_buy,
    quantize_volume,
    register_loss_cut as _register_loss_cut,
    get_dynamic_stop_limits,
)
from mtpond_config import (
    API_BASE,
    BASE_UNIT,
    BB_MIN_TRADE_COUNT_10M,
    BB_PATTERNS_API_URL,
    ENABLE_STOP,
    FORCE_LIVE,
    LIVE_TRADING,
    MAX_CONCURRENT_RESCUE,
    MIN_ORDER_NOTIONAL_KRW,
    RESCUE_MAX_DROP,
    RESCUE_MIN_DROP,
    UPBIT_ORDER_URL,
)
from mtpond_db import SessionLocal, engine

dotenv.load_dotenv()


async def get_keys(user_no: int, server_no: int) -> Tuple[Optional[str], Optional[str]]:
    """DB(traceUser)에서 사용자의 업비트 API 키(apiKey1, apiKey2)를 조회한다."""
    try:
        async with SessionLocal() as session:
            sql = text("""SELECT apiKey1, apiKey2
                          FROM traceUser
                          WHERE userNo = :u AND serverNo = :s LIMIT 1""")
            result = await session.execute(sql, {"u": user_no, "s": server_no})
            row = result.fetchone()
            if row and row[0] and row[1]:
                return str(row[0]).strip(), str(row[1]).strip()
    except Exception as e:
        print(f"[DB-CONFIG][WARN] traceUser 키 조회 실패: {e}")

    # 환경변수 폴백 (UPBIT_OPEN_API_ACCESS_KEY 등)
    ak = os.getenv("UPBIT_OPEN_API_ACCESS_KEY") or os.getenv("UPBIT_ACCESS_KEY")
    sk = os.getenv("UPBIT_OPEN_API_SECRET_KEY") or os.getenv("UPBIT_SECRET_KEY")
    return ak, sk


def _decimal_env(key: str, default: str) -> Decimal:
    raw = os.getenv(key, default)
    try:
        return Decimal(str(raw))
    except Exception:
        return Decimal(default)


# HTTP Connection Pool 재사용 클라이언트
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=4.0))
    return _http_client


async def close_http_client():
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


# 마켓별 동시성 락
_market_locks: Dict[str, asyncio.Lock] = {}


def get_market_lock(market: str) -> asyncio.Lock:
    if market not in _market_locks:
        _market_locks[market] = asyncio.Lock()
    return _market_locks[market]


# ============================================================
# 2. 환경 및 기본 설정
# ============================================================
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "3"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "56379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

redis_client = aioredis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    db=REDIS_DB,
    decode_responses=True,
    socket_timeout=5.0
)

# ============================================================
# 3. 500원 미만 마이크로 틱 스캘핑 핵심 파라미터
# ============================================================
MAX_TARGET_PRICE = Decimal("500.0")     # 500원 미만 코인만 대상
MIN_TARGET_PRICE = Decimal("0.5")       # 너무 작은 소수점 극저가 잡코인 배제 (0.5원 이상)
MIN_TICK_PROFIT_PCT = Decimal("0.20")   # 2~3틱 내 최소 이익 기준선 (+0.20%)

# [자금 관리 및 안전망]
INITIAL_BUY_KRW = _decimal_env("INITIAL_BUY_KRW", "10000")
ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "10000")
MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.20")  # 20% 현금 예비금 유지
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "1"))       # 스캘핑 특성상 물타기는 최대 1회만 제한

# [손절선 기본값]
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")        # 기본 하드 손절선 (-7.0%)
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-14.0")  # 대폭락 안전망 (-14.0%)

# [동적 범위 이탈 리셋 및 체결 지연 대기 파라미터]
RESET_DROP_TICKS = int(os.getenv("RESET_DROP_TICKS", "2"))   # 평단 대비 2틱 이상 하락 시 범위 이탈 감지
RESET_DROP_PCT = _decimal_env("RESET_DROP_PCT", "-0.80")     # 또는 -0.80% 이상 하락 시 리셋 발동
MAX_RESETS = int(os.getenv("MAX_RESETS", "2"))               # 최대 리셋 재배치 횟수 (2회)
RESET_COOLDOWN_SEC = 45                                     # 리셋 재배치 간 최소 대기 시간 (45초)
STAGNANT_TIMEOUT_SEC = 180                                  # 3분(180초) 동안 미체결 정체 시 신규 매수 보류

# [쿨다운 및 진입 간격]
COOLDOWN_TP_SEC = 90    # 익절 후 동일 코인 재진입 쿨다운 (1.5분)
COOLDOWN_SL_SEC = 600   # 손절 후 동일 코인 재진입 쿨다운 (10분)
ENTRY_INTERVAL_SEC = 90 # 전체 신규 매수 간격 최소 90초 (자금 고갈 방지)
_last_entry_ts: float = 0.0

BB_PATTERNS_CACHE_TTL_SEC = 20


def apply_dynamic_config(setup: Dict[str, Any]):
    global INITIAL_BUY_KRW, ADDITIONAL_BUY_KRW, MAX_ACTIVE_MARKETS
    global HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT, ENABLE_STOP
    changes = []

    # 1. 시작 매수 금액 (buyAmt or initAmt)
    buy_key = "initAmt" if "initAmt" in setup else ("buyAmt" if "buyAmt" in setup else None)
    if buy_key:
        try:
            b_amt = Decimal(str(setup[buy_key]))
            if b_amt > 0 and b_amt != INITIAL_BUY_KRW:
                changes.append(f"INITIAL_BUY_KRW: {INITIAL_BUY_KRW} -> {b_amt}")
                INITIAL_BUY_KRW = b_amt
        except Exception:
            pass

    # 2. 추가 매수 금액
    if "addAmt" in setup:
        try:
            a_amt = Decimal(str(setup["addAmt"]))
            if a_amt > 0 and a_amt != ADDITIONAL_BUY_KRW:
                changes.append(f"ADDITIONAL_BUY_KRW: {ADDITIONAL_BUY_KRW} -> {a_amt}")
                ADDITIONAL_BUY_KRW = a_amt
        except Exception:
            pass

    # 3. 최대 코인 수
    if "maxCoincnt" in setup:
        try:
            mc = int(setup["maxCoincnt"])
            if mc > 0 and mc != MAX_ACTIVE_MARKETS:
                changes.append(f"MAX_ACTIVE_MARKETS: {MAX_ACTIVE_MARKETS} -> {mc}")
                MAX_ACTIVE_MARKETS = mc
        except Exception:
            pass

    # 4. 손절 설정치 (hardStopLoss or lcRate)
    stop_key = "lcRate" if "lcRate" in setup else ("hardStopLoss" if "hardStopLoss" in setup else None)
    if stop_key:
        try:
            hsl = Decimal(str(setup[stop_key]))
            if hsl > 0:
                hsl = -abs(hsl)
            if hsl != HARD_STOP_LOSS_PCT:
                changes.append(f"HARD_STOP_LOSS_PCT: {HARD_STOP_LOSS_PCT} -> {hsl}")
                HARD_STOP_LOSS_PCT = hsl
        except Exception:
            pass

    if "maxDropLossCut" in setup:
        try:
            mdl = Decimal(str(setup["maxDropLossCut"]))
            if mdl > 0:
                mdl = -abs(mdl)
            if mdl != MAX_DROP_LOSS_CUT_PCT:
                changes.append(f"MAX_DROP_LOSS_CUT_PCT: {MAX_DROP_LOSS_CUT_PCT} -> {mdl}")
                MAX_DROP_LOSS_CUT_PCT = mdl
        except Exception:
            pass

    # 5. 손절 적용 여부 (stopYN)
    if "stopYN" in setup:
        stop_enabled = str(setup["stopYN"]).strip().upper() in ("1", "Y", "TRUE", "ON")
        if stop_enabled != ENABLE_STOP:
            changes.append(f"ENABLE_STOP: {ENABLE_STOP} -> {stop_enabled}")
            ENABLE_STOP = stop_enabled

    if changes:
        print("[CFG] mtpond8 동적 설정 반영:\n  " + "\n  ".join(changes))


async def sync_redis_config(user_no: int) -> Optional[Dict[str, Any]]:
    try:
        c = await redis_client.get(f"mtpond:setup:{user_no}")
        if c:
            data = json.loads(c)
            cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
            if isinstance(cfg, dict):
                apply_dynamic_config(cfg)
                return cfg
    except Exception as e:
        print(f"[CONFIG][WARN] Redis 설정 동기화 실패: {e}")
    return None


# ============================================================
# 4. 제외 코인 목록 관리
# ============================================================
async def get_excoins(user_no: int) -> List[Any]:
    async with SessionLocal() as session:
        sql = text("""SELECT exCoinlist
                      FROM mtSetup
                      WHERE userNo = :u AND attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"u": user_no, "attr": "%XXX%"})
        return result.fetchall()


async def parse_exclude_markets() -> Set[str]:
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


EXCLUDED_MARKETS: Set[str] = set()
_excluded_last_update: float = 0
EXCLUDED_CACHE_TTL_SEC = 60


async def get_excluded_markets(force_reload: bool = False, db_setup: Optional[Dict[str, Any]] = None) -> Set[str]:
    global EXCLUDED_MARKETS, _excluded_last_update
    now = time.time()
    if (not force_reload) and EXCLUDED_MARKETS and (now - _excluded_last_update) < EXCLUDED_CACHE_TTL_SEC:
        ex_res = set(EXCLUDED_MARKETS)
    else:
        new_excluded = await parse_exclude_markets()
        EXCLUDED_MARKETS = new_excluded
        _excluded_last_update = now
        ex_res = set(new_excluded)

    if db_setup and "exCoinlist" in db_setup:
        raw_db = db_setup.get("exCoinlist") or ""
        for coin in str(raw_db).split(","):
            c = coin.strip()
            if c:
                ex_res.add(c)
                if not c.startswith("KRW-"):
                    ex_res.add(f"KRW-{c}")
    return ex_res


# ============================================================
# 5. DB 매매 설정 조회 캐시
# ============================================================
DB_SETUP_CACHE_TTL_SEC = 5
_db_setup_cache: Dict[int, Tuple[float, Dict[str, Any]]] = {}


async def fetch_database_setup(user_no: int) -> Dict[str, Any]:
    global INITIAL_BUY_KRW, ADDITIONAL_BUY_KRW, MAX_ACTIVE_MARKETS
    now = time.time()
    cached = _db_setup_cache.get(user_no)
    if cached and now - cached[0] < DB_SETUP_CACHE_TTL_SEC:
        return cached[1]
    if not API_BASE:
        raise RuntimeError("API_BASE가 설정되지 않아 DB 매매 설정을 조회할 수 없습니다.")

    client = get_http_client()
    response = await client.get(f"{API_BASE}/api/mtpondsetup/{user_no}")
    response.raise_for_status()
    data = response.json()
    setup = data[0] if isinstance(data, list) and data else data
    if not isinstance(setup, dict):
        raise ValueError(f"유효하지 않은 DB 설정 응답: {data}")

    b_val = setup.get("initAmt") or setup.get("buyAmt") or INITIAL_BUY_KRW
    a_val = setup.get("addAmt") or ADDITIONAL_BUY_KRW
    hsl_val = setup.get("lcRate") or setup.get("hardStopLoss") or HARD_STOP_LOSS_PCT

    parsed = {
        "userNo": user_no,
        "activeVer": int(setup.get("activeVer", 8)),
        "activeYN": str(setup.get("activeYN", "N")).strip().upper(),
        "buyAmt": Decimal(str(b_val)),
        "addAmt": Decimal(str(a_val)),
        "maxCoincnt": int(setup.get("maxCoincnt", MAX_ACTIVE_MARKETS)),
        "limitAmt": Decimal(str(setup.get("limitAmt", 100000))),
        "stopYN": str(setup.get("stopYN", "Y")).strip().upper() in ("1", "Y", "TRUE", "ON"),
        "hardStopLoss": Decimal(str(hsl_val)),
        "maxDropLossCut": Decimal(str(setup.get("maxDropLossCut", MAX_DROP_LOSS_CUT_PCT))),
        "exCoinlist": str(setup.get("exCoinlist", "") or "").strip(),
    }
    apply_dynamic_config(parsed)
    _db_setup_cache[user_no] = (now, parsed)
    return parsed


# ============================================================
# 6. 업비트 API 및 서명 헬퍼 & 상폐/거래불가 마켓 방어
# ============================================================
_TRADABLE_MARKETS_SET: Set[str] = set()
_TRADABLE_MARKETS_TS: float = 0.0
_TRADABLE_MARKETS_CACHE_TTL_SEC: float = 1800.0  # 30분 캐시
_untradable_logged_set: Set[str] = set()


async def get_upbit_tradable_markets(force: bool = False) -> Set[str]:
    """업비트에서 현재 거래 지원 중인 전체 마켓 목록(KRW) 조회 및 캐싱."""
    global _TRADABLE_MARKETS_SET, _TRADABLE_MARKETS_TS
    now = time.time()
    if (not force) and _TRADABLE_MARKETS_SET and (now - _TRADABLE_MARKETS_TS < _TRADABLE_MARKETS_CACHE_TTL_SEC):
        return _TRADABLE_MARKETS_SET

    try:
        client = get_http_client()
        resp = await client.get("https://api.upbit.com/v1/market/all?isDetails=false", timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                _TRADABLE_MARKETS_SET = {m["market"] for m in data if isinstance(m, dict) and "market" in m}
                _TRADABLE_MARKETS_TS = now
    except Exception as e:
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패: {e}")

    return _TRADABLE_MARKETS_SET


async def fetch_current_prices_safe(
    markets: List[str],
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None
) -> Dict[str, Decimal]:
    """보유 마켓들의 현재가를 안전하게 조회 (사전 필터링 + 404 폴백 격리)."""
    if not markets:
        return {}

    tradable = await get_upbit_tradable_markets()
    valid_markets = [m for m in markets if m in tradable] if tradable else list(markets)
    if not valid_markets:
        return {}

    client = get_http_client()
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(valid_markets)}

    try:
        res = await client.get(url, params=params, timeout=5.0)
        if res.status_code == 200:
            raw = res.json()
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
    except Exception:
        pass

    # Fallback: 개별 조회로 상폐/오류 마켓 분리
    price_map: Dict[str, Decimal] = {}
    for m in valid_markets:
        try:
            r = await client.get(url, params={"markets": m}, timeout=3.0)
            if r.status_code == 200:
                items = r.json()
                if items and isinstance(items, list) and items[0].get("trade_price") is not None:
                    price_map[m] = Decimal(str(items[0]["trade_price"]))
            elif r.status_code == 404:
                _TRADABLE_MARKETS_SET.discard(m)
                if m not in _untradable_logged_set:
                    _untradable_logged_set.add(m)
                    print(f"⚠️ [DELISTED][WARN] {m} 거래소 상장폐지 감지 -> 제외")
        except Exception:
            pass

    return price_map


def build_jwt(access_key: str, secret_key: str, query: Optional[Dict[str, Any]] = None) -> str:
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    if query:
        filtered = {k: v for k, v in query.items() if v is not None}
        query_string = urllib.parse.urlencode(filtered)
        m = hashlib.sha512()
        m.update(query_string.encode('utf-8'))
        payload['query_hash'] = m.hexdigest()
        payload['query_hash_alg'] = 'SHA512'
    return jwt.encode(payload, secret_key, algorithm="HS256")


async def upbit_api(
    method: str, url: str, access_key: str, secret_key: str,
    params: Optional[Dict[str, Any]] = None
) -> Any:
    token = build_jwt(access_key, secret_key, params)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    client = get_http_client()
    m = method.upper()
    if m == "GET":
        r = await client.get(url, headers=headers, params=params)
    elif m == "POST":
        r = await client.post(url, headers=headers, params=params)
    elif m == "DELETE":
        r = await client.delete(url, headers=headers, params=params)
    else:
        raise ValueError(f"지원하지 않는 메서드: {m}")

    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json()


async def cancel_all_orders(access_key: str, secret_key: str, market: str):
    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        return
    try:
        orders = await upbit_api(
            "GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
            {"market": market, "state": "wait"}
        )
        if not orders or not isinstance(orders, list):
            return
        tasks = [
            upbit_api("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, {"uuid": o["uuid"]})
            for o in orders if isinstance(o, dict) and o.get("uuid")
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"[CANCEL][ERR] {market} 취소 실패: {e}")


async def cancel_single_order(access_key: str, secret_key: str, order_uuid: str) -> bool:
    """특정 UUID의 개별 주문만 단독 취소"""
    if not order_uuid or str(order_uuid).startswith("dry-"):
        return True
    try:
        await upbit_api("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, {"uuid": order_uuid})
        return True
    except Exception as e:
        print(f"[CANCEL-ORDER][WARN] uuid={order_uuid} 취소 예외: {e}")
        return False


def get_tick_unit(price: Decimal) -> Decimal:
    """업비트 원화 마켓 공식 호가 단위 반환."""
    p = float(price)
    if p >= 2_000_000: return Decimal("1000")
    if p >= 500_000:   return Decimal("500")
    if p >= 100_000:   return Decimal("100")
    if p >= 50_000:    return Decimal("50")
    if p >= 10_000:    return Decimal("10")
    if p >= 5_000:     return Decimal("5")
    if p >= 1_000:     return Decimal("1")
    if p >= 100:       return Decimal("1")
    if p >= 10:        return Decimal("0.1")
    if p >= 1:         return Decimal("0.01")
    if p >= Decimal("0.1"): return Decimal("0.001")
    if p >= Decimal("0.01"): return Decimal("0.0001")
    return Decimal("0.00001")


def adjust_price_to_tick(raw_price: Decimal, side: str = "ask") -> Decimal:
    unit = get_tick_unit(raw_price)
    steps = raw_price / unit
    mode = ROUND_CEILING if side == "ask" else ROUND_FLOOR
    return (steps.to_integral_value(rounding=mode) * unit).quantize(unit)


async def register_loss_cut(user_no: int, market: str, quantity: Decimal, price: Decimal, loss_rate: Decimal):
    await _register_loss_cut(API_BASE, user_no, market, quantity, price, loss_rate)


# ============================================================
# 7. 비트코인(BTC) 3단계 체력 필터 (BTC Market Regime)
# ============================================================
_btc_health_cache: Dict[str, Any] = {
    "ts": 0.0,
    "result": {"status": "NORMAL", "btc_pnl_1h": 0.0, "btc_pnl_4h": 0.0, "msg": "초기화 대기"}
}
BTC_HEALTH_CACHE_TTL_SEC = 30.0


async def check_btc_market_health() -> Dict[str, Any]:
    """KRW-BTC 캔들을 분석하여 시장 위험도 판별 (NORMAL, CAUTION, EMERGENCY)"""
    global _btc_health_cache
    now = time.time()
    if (now - _btc_health_cache["ts"]) < BTC_HEALTH_CACHE_TTL_SEC:
        return _btc_health_cache["result"]

    try:
        url = "https://api.upbit.com/v1/candles/minutes/60"
        client = get_http_client()
        res = await client.get(url, params={"market": "KRW-BTC", "count": 25})
        if res.status_code != 200:
            return _btc_health_cache["result"]
        candles = res.json()
        if not candles or len(candles) < 20:
            return _btc_health_cache["result"]

        cur_price = Decimal(str(candles[0]["trade_price"]))
        prev_1h_price = Decimal(str(candles[1]["trade_price"]))
        prev_4h_price = Decimal(str(candles[4]["trade_price"]))

        pnl_1h = float(((cur_price - prev_1h_price) / prev_1h_price) * Decimal("100"))
        pnl_4h = float(((cur_price - prev_4h_price) / prev_4h_price) * Decimal("100"))

        closes_20 = [Decimal(str(c["trade_price"])) for c in candles[:20]]
        ma20 = sum(closes_20) / Decimal("20")

        if pnl_1h <= -1.5 or pnl_4h <= -3.0 or cur_price < (ma20 * Decimal("0.985")):
            res_dict = {
                "status": "EMERGENCY",
                "btc_pnl_1h": round(pnl_1h, 2),
                "btc_pnl_4h": round(pnl_4h, 2),
                "msg": f"🚨 [BTC-EMERGENCY] 급락 감지! -> 500원 미만 신규 스캘핑 매수 완전 차단"
            }
        elif pnl_1h <= -0.7 or cur_price < ma20:
            res_dict = {
                "status": "CAUTION",
                "btc_pnl_1h": round(pnl_1h, 2),
                "btc_pnl_4h": round(pnl_4h, 2),
                "msg": f"⚠️ [BTC-CAUTION] 비트코인 조정세 -> 보수적 스캘핑 모드"
            }
        else:
            res_dict = {
                "status": "NORMAL",
                "btc_pnl_1h": round(pnl_1h, 2),
                "btc_pnl_4h": round(pnl_4h, 2),
                "msg": "✅ [BTC-NORMAL] 안정적 추세 -> 1~3틱 마이크로 스캘핑 정상 가동"
            }

        _btc_health_cache["ts"] = now
        _btc_health_cache["result"] = res_dict
        return res_dict
    except Exception as e:
        print(f"[BTC-HEALTH][WARN] 조회 실패: {e}")
        return _btc_health_cache["result"]


# ============================================================
# 8. 500원 미만 1~3틱 초고속 마진 타겟 계산 및 선(先)지정가 발주
# ============================================================
def calculate_micro_tick_target(market: str, avg_price: Decimal) -> Tuple[Decimal, Decimal, int]:
    """
    500원 미만 동전주 특화 1~3틱 목표가 계산 함수.
    
    [목표 틱수 산출 규칙]
    1) 1틱 가치가 0.45% 이상인 경우 (예: 100~220원, 10~22원):
       -> 단 1틱 위 지정가 매도! (+0.45% ~ +1.00% 실질 순익 즉시 확정)
    2) 1틱 가치가 0.20% ~ 0.45%인 경우 (예: 220~500원):
       -> 2틱 위 지정가 매도! (+0.40% ~ +0.90% 실질 순익 확정)
    3) 그 외:
       -> 최소 3틱 (또는 +0.40% 이상 확보) 지정가 매도!
       
    반환: (target_price, real_tp_pct, target_ticks)
    """
    tick = get_tick_unit(avg_price)
    one_tick_pct = (tick / avg_price) * Decimal("100")

    if one_tick_pct >= Decimal("0.45"):
        target_ticks = 1
    elif one_tick_pct >= Decimal("0.20"):
        target_ticks = 2
    else:
        target_ticks = 3

    # 기본 호가 단위 기준 최소 목표가 산출
    base_floor = adjust_price_to_tick(avg_price, side="floor")
    target_price = adjust_price_to_tick(base_floor + (tick * Decimal(str(target_ticks))), side="ask")
    if target_price <= avg_price:
        target_price = adjust_price_to_tick(avg_price + tick, side="ask")
        if target_price <= avg_price:
            target_price = avg_price + tick

    # 수수료(0.10%) 고려 최소 이익률(MIN_TICK_PROFIT_PCT: 0.20%) 보장
    real_tp_pct = ((target_price - avg_price) / avg_price * Decimal("100")).quantize(Decimal("0.01"))
    while real_tp_pct < MIN_TICK_PROFIT_PCT:
        target_price += tick
        real_tp_pct = ((target_price - avg_price) / avg_price * Decimal("100")).quantize(Decimal("0.01"))

    actual_ticks = int(round((target_price - base_floor) / tick))
    return target_price, real_tp_pct, max(1, actual_ticks)


async def place_tranche_limit_sell(
    access_key: str,
    secret_key: str,
    market: str,
    volume: Decimal,
    target_price: Decimal
) -> Optional[str]:
    """
    [개별 트랜치 독립 매도 발주]
    기존에 이미 등록되어 대기 중인 다른 트랜치 매도 주문들은 일체 취소하지 않고 건드리지 않으며,
    새로 매수된 트랜치 물량(volume)에 대해서만 독립적인 선(先) 지정가 매도 주문을 발주한다.
    """
    if volume <= 0 or target_price <= 0:
        return None

    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        print(f"[TRANCHE-SELL][SKIP] {market} 거래 불가능한 마켓이므로 발주 건너뜀")
        return None

    sell_vol = quantize_volume(volume)
    notional = sell_vol * target_price
    if notional < MIN_ORDER_NOTIONAL_KRW:
        print(f"[TRANCHE-SELL][WARN] {market} 최소 주문금액(5,000원) 미달: {notional:,.0f}원 (수량: {sell_vol})")
        return None

    order_uuid = None
    if LIVE_TRADING:
        try:
            # 동일 가격 및 동일 수량의 주문이 이미 open orders에 걸려있는지 중복 점검
            open_orders = await upbit_api(
                "GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
                {"market": market, "state": "wait"}
            )
            if open_orders and isinstance(open_orders, list):
                for o in open_orders:
                    if (
                        Decimal(str(o.get("price", "0"))) == target_price
                        and quantize_volume(Decimal(str(o.get("volume", "0")))) == sell_vol
                    ):
                        print(f"[TRANCHE-SELL][DEDUP] {market} {target_price}원에 동일 수량({sell_vol}) 주문 이미 대기 중 -> 중복 발주 방지")
                        return o.get("uuid")

            r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": market, "side": "ask", "volume": str(sell_vol), "price": str(target_price), "ord_type": "limit"
            })
            order_uuid = r.get("uuid")
        except Exception as e:
            print(f"[TRANCHE-SELL][ERR] {market} 독립 지정가 매도 발주 실패: {e}")
            return None
    else:
        order_uuid = f"dry-{uuid.uuid4().hex[:12]}"

    return order_uuid


async def reposition_tranches_limit_sell(
    access_key: str,
    secret_key: str,
    market: str,
    state: Dict[str, Any],
    cur_p: Decimal
) -> bool:
    """
    [동적 범위 이탈 리셋 (개별 트랜치 분리 유지)]
    추가 매수가 한도에 도달한 상태에서 가격이 하락하여 목표 지정가 범위를 벗어난 경우:
    현재가 대비 너무 높은 곳에 대기 중인 트랜치들의 매도 주문만 개별 취소하고,
    현재가(cur_p) 기준 1틱 위로 각각 재배치하여 1호가 미세 반등 시 순차적 탈출 및 원금 회수를 실현한다.
    """
    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        return False

    tick = get_tick_unit(cur_p)
    new_target = adjust_price_to_tick(cur_p + tick, side="ask")
    if new_target <= cur_p:
        new_target = cur_p + tick

    tranches = state.get("tranches", [])
    active_tranches = [t for t in tranches if t.get("status") == "WAITING_SELL"]
    if not active_tranches:
        return False

    reset_done = False
    for t in active_tranches:
        old_target = Decimal(str(t.get("target_price", 0)))
        if old_target > (new_target + tick):
            old_uuid = t.get("sell_uuid")
            if old_uuid:
                await cancel_single_order(access_key, secret_key, old_uuid)
                await asyncio.sleep(0.1)

            t_vol = Decimal(str(t.get("volume", 0)))
            new_uuid = await place_tranche_limit_sell(access_key, secret_key, market, t_vol, new_target)
            if new_uuid:
                t["target_price"] = str(new_target)
                t["sell_uuid"] = new_uuid
                t["reset_count"] = t.get("reset_count", 0) + 1
                reset_done = True
                print(
                    f"🔄 [DYNAMIC-RESET] {market} 트랜치 #{t['id']} 독립 리셋 완료!\n"
                    f"  └ 기존 목표가: {old_target}원 -> 신규 목표가: {new_target}원 (+1틱)\n"
                    f"  └ 1호가 미세 반등 시 독립 체결되어 현금 회수 가능"
                )

    if reset_done:
        state["reset_count"] = state.get("reset_count", 0) + 1
        state["last_reset_ts"] = time.time()
        return True
    return False


async def place_micro_tick_limit_sell(
    access_key: str,
    secret_key: str,
    market: str,
    state: Dict[str, Any],
    balance: Decimal,
    avg_price: Decimal
):
    """매수 체결 즉시 1~3틱 목표가로 호가창에 선(先) 지정가 매도 등록"""
    if balance <= 0 or avg_price <= 0:
        return

    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        print(f"[MICRO-TICK][SKIP] {market} 거래 불가능한 마켓이므로 발주 건너뜀")
        return

    # 기존 주문 취소 후 정리
    await cancel_all_orders(access_key, secret_key, market)
    await asyncio.sleep(0.3)

    target_price, real_tp_pct, ticks = calculate_micro_tick_target(market, avg_price)
    sell_vol = quantize_volume(balance)
    notional = sell_vol * target_price

    state["avg_price"] = avg_price
    state["target_price"] = target_price
    state["target_tp_pct"] = real_tp_pct
    state["target_ticks"] = ticks
    state["sell_vol"] = sell_vol

    if notional >= MIN_ORDER_NOTIONAL_KRW:
        if LIVE_TRADING:
            r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": market, "side": "ask", "volume": str(sell_vol), "price": str(target_price), "ord_type": "limit"
            })
            state["sell_order_uuid"] = r.get("uuid")

        print(
            f"🎯 [MICRO-TICK-SELL] {market} {ticks}틱 초고속 선(先)지정가 매도 완료!\n"
            f"  └ 수량: {sell_vol}개 @ {target_price}원 (+{real_tp_pct}% 이익 / {ticks}틱)"
        )
    else:
        print(f"[MICRO-TICK][WARN] {market} 최소 주문금액(5,500원) 미달: {notional}원")


async def reposition_micro_tick_limit_sell(
    access_key: str,
    secret_key: str,
    market: str,
    state: Dict[str, Any],
    cur_p: Decimal,
    total_vol: Decimal,
    avg_p: Decimal
) -> bool:
    """
    [동적 리셋] 가격이 하락하여 지정가 범위를 벗어난 경우:
    기존 지정가 매도를 취소하고 현재가(cur_p) 기준 1틱 위에 매도를 재배치하여,
    미세 반등(1호가 윗꼬리) 시 신속 탈출함으로써 손실이 커지는 것을 원천 방지한다.
    """
    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        return False

    tick = get_tick_unit(cur_p)
    new_target = adjust_price_to_tick(cur_p + tick, side="ask")
    if new_target <= cur_p:
        new_target = cur_p + tick

    # 1. 기존 지정가 매도 취소
    await cancel_all_orders(access_key, secret_key, market)
    await asyncio.sleep(0.3)

    sell_vol = quantize_volume(total_vol)
    notional = sell_vol * new_target

    reset_cnt = state.get("reset_count", 0) + 1
    state["target_price"] = new_target
    state["reset_count"] = reset_cnt
    state["last_reset_ts"] = time.time()

    if notional >= MIN_ORDER_NOTIONAL_KRW:
        if LIVE_TRADING:
            r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": market, "side": "ask", "volume": str(sell_vol), "price": str(new_target), "ord_type": "limit"
            })
            state["sell_order_uuid"] = r.get("uuid")

        drop_ticks = int((avg_p - cur_p) / tick) if tick > 0 else 0
        escape_pnl = ((new_target - avg_p) / avg_p * Decimal("100")).quantize(Decimal("0.01"))
        print(
            f"🔄 [DYNAMIC-RESET] {market} 범위 이탈 감지 (평단: {avg_p}원 -> 현재가: {cur_p}원, {drop_ticks}틱 하락)!\n"
            f"  └ 기존 매도 취소 후 현재가 기준 {new_target}원(+1틱)으로 재배치 완료! (리셋 {reset_cnt}회차 / 탈출 목표: {escape_pnl}%)\n"
            f"  └ 1호가 미세 반등 시 즉시 체결되어 -5%~-10% 큰 손실로 번지는 것을 원천 차단"
        )
        return True
    return False


# ============================================================
# 9. 포지션 관리자 (Redis 연동 및 지갑 복원)
# ============================================================
class PositionManager8:
    def __init__(self, user_no: int, r: aioredis.Redis):
        self.user_no = user_no
        self.r = r
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.cooldowns: Dict[str, float] = {}

    def get_redis_key(self) -> str:
        return f"mtpond8:positions:{self.user_no}"

    async def save_position(self, market: str, state: Dict[str, Any]):
        self.positions[market] = state
        await self.r.hset(self.get_redis_key(), market, json.dumps(state, default=str))

    async def remove_position(self, market: str):
        if market in self.positions:
            del self.positions[market]
        await self.r.hdel(self.get_redis_key(), market)

    async def restore_from_redis_and_wallet(self, access_key: str, secret_key: str):
        print("🔄 [RESTORE] mtpond8 시스템 기동: Redis 상태 및 500원 미만 지갑 잔고 복원 시작...")
        wallet_map: Dict[str, Dict[str, Decimal]] = {}
        tradable_markets = await get_upbit_tradable_markets()

        try:
            accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            for a in accs:
                cur = a.get("currency")
                if cur and cur != "KRW":
                    mkt = f"KRW-{cur}"
                    if tradable_markets and mkt not in tradable_markets:
                        continue
                    tot = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    avg = Decimal(str(a.get("avg_buy_price", "0")))
                    # 500원 미만 또는 기존 보유 코인만 선별
                    if tot > 0:
                        wallet_map[mkt] = {"total_vol": tot, "avg_price": avg}
        except Exception as e:
            print(f"[RESTORE][ERR] 업비트 잔고 조회 실패: {e}")
            return

        raw_positions = await self.r.hgetall(self.get_redis_key())
        if raw_positions:
            for mkt, s_val in raw_positions.items():
                if tradable_markets and mkt not in tradable_markets:
                    await self.r.hdel(self.get_redis_key(), mkt)
                    continue

                if mkt in wallet_map:
                    try:
                        item = json.loads(s_val)
                        real_avg = wallet_map[mkt]["avg_price"]
                        item["avg_price"] = real_avg if real_avg > 0 else Decimal(str(item.get("avg_price", "0")))
                        for dec_k in ("target_price", "sell_vol", "avg_price", "last_buy_price"):
                            if item.get(dec_k) is not None:
                                item[dec_k] = Decimal(str(item[dec_k]))
                        # 트랜치 목록 복원 및 하위 호환
                        if "tranches" not in item or not isinstance(item["tranches"], list):
                            item["tranches"] = [{
                                "id": 1,
                                "buy_price": str(item["avg_price"]),
                                "volume": str(wallet_map[mkt]["total_vol"]),
                                "target_price": str(item.get("target_price", item["avg_price"])),
                                "target_ticks": item.get("target_ticks", 1),
                                "target_tp_pct": str(item.get("target_tp_pct", "0.5")),
                                "sell_uuid": item.get("sell_order_uuid"),
                                "buy_ts": item.get("entry_ts", time.time()),
                                "status": "WAITING_SELL"
                            }]
                        self.positions[mkt] = item
                        print(f"  └ [포지션 복원] {mkt} (평단: {item['avg_price']}원 | 잔고: {wallet_map[mkt]['total_vol']} | 트랜치: {len(item.get('tranches', []))}개)")
                    except Exception as exc:
                        print(f"[RESTORE][WARN] {mkt} 파싱 실패: {exc}")
                else:
                    await self.r.hdel(self.get_redis_key(), mkt)

        # 지갑에 보유 중이지만 Redis에 없는 코인은 500원 미만인 경우 신규 편입
        for mkt, w_info in wallet_map.items():
            if tradable_markets and mkt not in tradable_markets:
                continue
            if mkt not in self.positions:
                avg_p = w_info["avg_price"]
                if avg_p <= MAX_TARGET_PRICE:
                    print(f"  └ ⚠️ [500원 미만 보유 코인 감지] {mkt} → 전략 8 신규 편입 (평단: {avg_p}원, 수량: {w_info['total_vol']})")
                    target_p, tp_pct, ticks = calculate_micro_tick_target(mkt, avg_p)
                    open_orders = await upbit_api(
                        "GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
                        {"market": mkt, "state": "wait"}
                    )
                    sell_uuid = None
                    if open_orders and isinstance(open_orders, list) and len(open_orders) > 0:
                        sell_uuid = open_orders[0].get("uuid")
                    else:
                        sell_uuid = await place_tranche_limit_sell(access_key, secret_key, mkt, w_info["total_vol"], target_p)

                    new_state = {
                        "avg_price": avg_p,
                        "last_buy_price": avg_p,
                        "entry_ts": time.time(),
                        "last_buy_ts": time.time(),
                        "buy_count": 1,
                        "reset_count": 0,
                        "tranches": [
                            {
                                "id": 1,
                                "buy_price": str(avg_p),
                                "volume": str(w_info["total_vol"]),
                                "target_price": str(target_p),
                                "target_ticks": ticks,
                                "target_tp_pct": str(tp_pct),
                                "sell_uuid": sell_uuid,
                                "buy_ts": time.time(),
                                "status": "WAITING_SELL"
                            }
                        ]
                    }
                    await self.save_position(mkt, new_state)

        print(f"✅ [RESTORE-COMPLETE] mtpond8 복원 완료. 관리 포지션: {list(self.positions.keys())}")

    def set_cooldown(self, market: str, is_tp: bool):
        sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_SL_SEC
        self.cooldowns[market] = time.time() + sec
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.r.set(f"mtpond8:cooldown:{self.user_no}:{market}", "1", ex=sec))
        except RuntimeError:
            pass
        print(f"[COOLDOWN] {market} {sec}초간 재진입 금지")

    async def is_cooling_down(self, market: str) -> bool:
        if time.time() < self.cooldowns.get(market, 0.0):
            return True
        try:
            if await self.r.exists(f"mtpond8:cooldown:{self.user_no}:{market}"):
                return True
        except Exception:
            pass
        return False


# ============================================================
# 10. 500원 미만 고활성 동전주 탐색 엔진
# ============================================================
_patterns_cache: Dict[str, Any] = {"ts": 0.0, "data": []}


async def fetch_all_candle_patterns() -> List[Dict[str, Any]]:
    global _patterns_cache
    now = time.time()
    if (now - _patterns_cache["ts"]) < BB_PATTERNS_CACHE_TTL_SEC and _patterns_cache["data"]:
        return _patterns_cache["data"]

    client = get_http_client()
    try:
        res = await client.get(BB_PATTERNS_API_URL, timeout=4.0)
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list):
                _patterns_cache["ts"] = now
                _patterns_cache["data"] = data
                return data
    except Exception as e:
        print(f"[BB-PATTERNS][WARN] 조회 실패: {e}")
    return _patterns_cache["data"]


# ============================================================
# 10. 3분봉 캔들 조회 및 연속 하락 배제 / 반전(반등) 신호 감지
# ============================================================
_candle_3m_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
CANDLE_3M_CACHE_TTL = 3.0  # 3초 캐시로 중복 API 호출 방지


async def get_3m_candles(market: str, count: int = 6) -> List[Dict[str, Any]]:
    """업비트 3분봉 캔들 데이터 조회 (캐시 3초)"""
    now = time.time()
    cached = _candle_3m_cache.get(market)
    if cached and (now - cached[0]) < CANDLE_3M_CACHE_TTL:
        return cached[1]

    url = "https://api.upbit.com/v1/candles/minutes/3"
    client = get_http_client()
    try:
        res = await client.get(url, params={"market": market, "count": count})
        if res.status_code == 200:
            candles = res.json()
            if isinstance(candles, list):
                _candle_3m_cache[market] = (now, candles)
                return candles
    except Exception as e:
        print(f"[CANDLE-3M][WARN] {market} 3분봉 조회 실패: {e}")
    return []


def evaluate_3m_reversal(candles: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    [3분봉 하락 연속 배제 및 반전/반등 신호 순수 평가 함수]
    
    1. 하락 연속 배제:
       - 현재 3분봉이 음봉(trade_price < opening_price)이면 하락 진행 중이므로 매수 절대 배제.
       - 직전 봉들이 연속 하락한 상태에서 반전이 확인되지 않은 경우 매수 배제.
    
    2. 반전(Rebound / Reversal) 확인:
       - 현재 3분봉이 양봉(trade_price > opening_price)으로 전환.
       - 직전 음봉의 종가를 회복(trade_price >= prev_close)하거나 저점 대비 반등 꼬리 형성.
       - 최근 3개봉 이동평균(MA3) 지지/돌파 확인.
    """
    if not candles or len(candles) < 3:
        return False, "CANDLE_DATA_UNAVAILABLE"

    try:
        c0 = candles[0]
        c1 = candles[1]
        c2 = candles[2]

        p0_close = Decimal(str(c0["trade_price"]))
        p0_open = Decimal(str(c0["opening_price"]))

        p1_close = Decimal(str(c1["trade_price"]))
        p1_open = Decimal(str(c1["opening_price"]))

        p2_close = Decimal(str(c2["trade_price"]))
        p2_open = Decimal(str(c2["opening_price"]))

        # 1. 현재 3분봉이 음봉인 경우 -> 무조건 하락 진행 중이므로 매수 차단
        if p0_close < p0_open:
            return False, f"현재 3분봉 음봉 하락 진행 중 ({p0_close} < {p0_open})"

        # 2. 직전 봉들의 하락 연속 여부 판별
        c1_is_red = (p1_close < p1_open)
        c2_is_red = (p2_close < p2_open)
        c1_dropped = (p1_close < p2_close)
        was_downtrend = c1_is_red and (c2_is_red or c1_dropped)

        # 3. 반전(Reversal) 확인
        is_green = (p0_close > p0_open)
        rebound_over_c1 = (p0_close >= p1_close)
        ma3 = (p0_close + p1_close + p2_close) / Decimal("3")
        above_ma3 = (p0_close >= ma3)

        if was_downtrend:
            # 연속 하락 추세였던 경우: 현재 봉이 반드시 양봉이어야 하고, 직전 음봉 종가를 넘어서거나 MA3 위에 올라서야 반전으로 인정
            if is_green and (rebound_over_c1 or above_ma3):
                return True, "연속 하락 후 3분봉 양봉 반전 확인"
            return False, "연속 하락 후 반전 미확인 (양봉 반등 대기)"

        # 직전이 단일 음봉이거나 횡보/상승이었던 경우:
        if is_green or above_ma3:
            return True, "3분봉 상승 반등 흐름 유지"

        return False, "3분봉 상승 모멘텀 미달"
    except Exception as e:
        return False, f"CANDLE_EVAL_ERROR({e})"


async def check_3m_reversal_signal(
    market: str,
    candles: Optional[List[Dict[str, Any]]] = None
) -> Tuple[bool, str]:
    """
    [3분봉 하락 연속 배제 및 반전/반등 신호 감지 비동기 래퍼]
    """
    if candles is None:
        candles = await get_3m_candles(market, count=6)
    return evaluate_3m_reversal(candles)


async def fetch_15m_rebound_signal(market: str) -> bool:
    """하위 호환용 래퍼: 3분봉 반전 신호 기반으로 평가"""
    ok, _ = await check_3m_reversal_signal(market)
    return ok


async def get_micro_tick_candidates(excluded: Set[str]) -> List[Dict[str, Any]]:
    """
    [완화된 동전주 후보 조건]
    1. 가격이 500원 미만 (또는 2~3틱 안에 +0.2% 이상 이익 가능한 코인).
    2. 너무 작은 극저가 잡코인 배제 (0.5원 이상).
    3. 10분 체결 건수 15건 이상 활성 코인 (조건 완화로 대상 풀 확대).
    4. 거래대금/체결빈도 상위 정렬.
    """
    patterns = await fetch_all_candle_patterns()
    tradable = await get_upbit_tradable_markets()
    candidates = []

    for row in patterns:
        if not isinstance(row, dict):
            continue
        mkt = row.get("market", "")
        if not mkt.startswith("KRW-") or mkt in excluded:
            continue
        if tradable and mkt not in tradable:
            continue

        try:
            price = float(row.get("price", 0))
            trades = int((row.get("trade_activity") or {}).get("trade_count_10m", 0))
            score = int(row.get("overall_score", 0))
            v30 = float(row.get("timeframes", {}).get("30m", {}).get("expected_volatility_pct", 0.0))
        except (ValueError, TypeError):
            continue

        # 조건 완화 검증: 0.5원 <= price <= 500원, 체결 15건 이상 (기존 30건에서 완화)
        if MIN_TARGET_PRICE <= Decimal(str(price)) <= MAX_TARGET_PRICE and trades >= 15:
            tick = get_tick_unit(Decimal(str(price)))
            three_tick_pct = ((tick * Decimal("3")) / Decimal(str(price))) * Decimal("100")

            # 2~3틱 안에 0.20% 이상 마진 보장 검증
            if three_tick_pct >= MIN_TICK_PROFIT_PCT:
                candidates.append({
                    "market": mkt,
                    "price": price,
                    "trades": trades,
                    "score": score,
                    "v30": v30,
                    "three_tick_pct": float(three_tick_pct)
                })

    # 체결 빈도 및 종합 점수 순 내림차순 정렬
    candidates.sort(key=lambda x: (x["trades"], x["score"]), reverse=True)

    # 만약 BB-Patterns 서버 지연/미응답 시 업비트 실시간 거래대금 상위 동전주 폴백 (기존 5억원에서 1.5억원으로 완화)
    if not candidates:
        try:
            client = get_http_client()
            all_krw = [m for m in tradable if m.startswith("KRW-") and m not in excluded]
            if all_krw:
                r_tickers = await client.get("https://api.upbit.com/v1/ticker", params={"markets": ",".join(all_krw[:100])})
                if r_tickers.status_code == 200:
                    for t in r_tickers.json():
                        p = float(t.get("trade_price", 0))
                        vol_24h = float(t.get("acc_trade_price_24h", 0))
                        if MIN_TARGET_PRICE <= Decimal(str(p)) <= MAX_TARGET_PRICE and vol_24h >= 150_000_000:
                            tick = get_tick_unit(Decimal(str(p)))
                            three_tick_pct = ((tick * Decimal("3")) / Decimal(str(p))) * Decimal("100")
                            if three_tick_pct >= MIN_TICK_PROFIT_PCT:
                                candidates.append({
                                    "market": t["market"],
                                    "price": p,
                                    "trades": int(vol_24h / 1e7),
                                    "score": 80,
                                    "v30": 2.0,
                                    "three_tick_pct": float(three_tick_pct)
                                })
                    candidates.sort(key=lambda x: x["trades"], reverse=True)
        except Exception:
            pass

    return candidates


# ============================================================
# 11. 메인 감시 및 트레이딩 루프 (mtpond8 Engine)
# ============================================================
async def run_mtpond8_engine(user_no: int, server_no: int):
    global _last_entry_ts
    access_key, secret_key = await get_keys(user_no, server_no)
    if not access_key or not secret_key:
        print(f"[mtpond8][FATAL] user={user_no}, server={server_no} 업비트 API 키를 DB(traceUser) 및 .env에서 찾지 못했습니다.")
        return

    pm = PositionManager8(user_no, redis_client)
    await pm.restore_from_redis_and_wallet(access_key, secret_key)

    print("=" * 60)
    print(f"🚀 [mtpond8] 500원 미만 1~3틱 마이크로 스캘퍼 퀀트 엔진 가동 (User: {user_no}, Server: {server_no})")
    print(f"  └ 대상: 500원 미만 고활성 동전주 (1~3틱 내 +0.2%~+1.0% 마진 보장 종목)")
    print(f"  └ 기법: 매수 체결 즉시 선(先) 지정가 매도 등록 (초고속 회전)")
    print(f"  └ 안전망: BTC 3단계 체력 필터 + 30분봉 진폭 동적 손절 + 20% 현금 예비금")
    print("=" * 60)

    loop_cnt = 0

    while True:
        try:
            loop_cnt += 1
            # 최신 설정 동기화 및 거래 비활성화(OFF) 감지 시 루프 종료
            r_cfg = await sync_redis_config(user_no)
            if r_cfg and str(r_cfg.get("activeYN", "Y")).strip().upper() == "N":
                print(f"⏸️ [mtpond8] activeYN=N 감지 -> 거래 루프를 안전하게 종료합니다. (User: {user_no})")
                break

            db_setup = await fetch_database_setup(user_no)
            if str(db_setup.get("activeYN", "Y")).strip().upper() == "N":
                print(f"⏸️ [mtpond8] DB activeYN=N 감지 -> 거래 루프를 안전하게 종료합니다. (User: {user_no})")
                break

            excluded_markets = await get_excluded_markets(db_setup=db_setup)

            # 비트코인 체력 진단
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]

            # 거래 가능 마켓 목록 최신화
            tradable_markets = await get_upbit_tradable_markets()

            # 업비트 계좌 및 잔고 조회
            accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            krw_available = Decimal("0")
            krw_locked = Decimal("0")
            wallet_map = {}

            for a in accounts:
                cur = a.get("currency")
                if cur == "KRW":
                    krw_available = Decimal(str(a.get("balance", "0")))
                    krw_locked = Decimal(str(a.get("locked", "0")))
                else:
                    mkt = f"KRW-{cur}"
                    if tradable_markets and mkt not in tradable_markets:
                        continue
                    tot_v = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    avg_p = Decimal(str(a.get("avg_buy_price", "0")))
                    if tot_v > 0:
                        wallet_map[mkt] = {"total_vol": tot_v, "avg_price": avg_p}

            # 현재 보유 코인 시세 안전 조회
            held_markets = list(wallet_map.keys())
            price_map = await fetch_current_prices_safe(held_markets, access_key, secret_key)

            # 총 평가액 및 현금 버퍼 계산
            coin_eval = sum(
                wallet_map[m]["total_vol"] * price_map.get(m, wallet_map[m]["avg_price"])
                for m in held_markets
            )
            total_equity = krw_available + krw_locked + coin_eval
            min_cash_reserve = total_equity * MIN_CASH_BUFFER_RATIO
            spendable_krw = max(Decimal("0"), krw_available - min_cash_reserve)

            if loop_cnt % 6 == 1:
                print(
                    f"💓 [mtpond8-HEARTBEAT] 총자산: {total_equity:,.0f}원 | 가용KRW: {krw_available:,.0f}원 "
                    f"(예비금: {min_cash_reserve:,.0f}원, 가용: {spendable_krw:,.0f}원) | "
                    f"포지션: {len(pm.positions)}/{db_setup['maxCoincnt']}개 | 비트코인: {btc_status}"
                )

            # ========================================================
            # 1. 기존 보유 포지션 관리 (익절 체결 확인 / 손절)
            # ========================================================
            for market in list(pm.positions.keys()):
                if tradable_markets and market not in tradable_markets:
                    print(f"⚠️ [DELISTED-EXIT] {market} 상폐 감지 -> 포지션 제거")
                    await pm.remove_position(market)
                    continue

                state = pm.positions[market]
                w_info = wallet_map.get(market)

                # 거래소에서 잔고가 완전히 0이 된 경우 (외부 매도 또는 전량 정리)
                if not w_info or w_info["total_vol"] <= Decimal("0.000001"):
                    print(f"🎉 [CLEARED-EXIT] {market} 전체 잔고 소진 확인 -> 포지션 정리 및 자금 회수")
                    await pm.remove_position(market)
                    pm.set_cooldown(market, is_tp=True)
                    continue

                cur_p = price_map.get(market)
                if not cur_p or cur_p <= 0:
                    cur_p = w_info["avg_price"]
                avg_p = w_info["avg_price"]
                total_vol = w_info["total_vol"]
                if cur_p <= 0 or avg_p <= 0:
                    continue

                pnl = ((cur_p - avg_p) / avg_p * Decimal("100")).quantize(Decimal("0.01"))
                tick = get_tick_unit(cur_p)
                now_ts = time.time()

                # ----------------------------------------------------
                # [독립 트랜치 관리]: 각 매수 건별 체결 여부 개별 감지
                # 목적: 하락 중에도 순간 반등 시 해당 트랜치만 즉시 이익 취득 & 현금 회수
                # ----------------------------------------------------
                tranches = state.get("tranches", [])
                active_tranches = [t for t in tranches if t.get("status") == "WAITING_SELL"]

                if LIVE_TRADING and active_tranches:
                    try:
                        open_orders = await upbit_api(
                            "GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
                            {"market": market, "state": "wait"}
                        )
                        open_uuids = {o["uuid"] for o in open_orders if isinstance(o, dict) and o.get("uuid")}
                        for t in active_tranches:
                            u = t.get("sell_uuid")
                            if u and u not in open_uuids:
                                try:
                                    od = await upbit_api("GET", "https://api.upbit.com/v1/order", access_key, secret_key, {"uuid": u})
                                    if od.get("state") == "done":
                                        t["status"] = "DONE"
                                        t["done_ts"] = time.time()
                                        print(
                                            f"🎉 [TRANCHE-TP] {market} 트랜치 #{t['id']} 독립 선(先)지정가 매도 체결 완료!\n"
                                            f"  └ 매수: {t.get('buy_price')}원 -> 매도: {t.get('target_price')}원 (+{t.get('target_tp_pct')}%) | 독립 이익 실현 & 현금 회수 완료!"
                                        )
                                    elif od.get("state") == "cancel":
                                        t["status"] = "CANCELLED"
                                except Exception as ord_err:
                                    print(f"[TRANCHE-CHECK][WARN] {u} 상태 조회 예외: {ord_err}")
                    except Exception as e:
                        print(f"[ORDERS-CHECK][WARN] {market} 주문 목록 조회 실패: {e}")
                elif (not LIVE_TRADING) and active_tranches:
                    for t in active_tranches:
                        tp = Decimal(str(t.get("target_price", "0")))
                        if cur_p >= tp:
                            t["status"] = "DONE"
                            t["done_ts"] = time.time()
                            print(
                                f"🎉 [DRY-TRANCHE-TP] {market} 트랜치 #{t['id']} 모의 독립 익절 체결 완료!\n"
                                f"  └ 매수: {t.get('buy_price')}원 -> 매도: {tp}원 (+{t.get('target_tp_pct')}%) | 가상 이익 실현 & 현금 회수!"
                            )

                # 체결 후 남은 활성 트랜치 갱신
                active_tranches = [t for t in tranches if t.get("status") == "WAITING_SELL"]
                if tranches and not active_tranches:
                    print(f"🎊 [ALL-TRANCHES-CLEARED] {market} 모든 투입 거래 독립 익절 완료! 포지션 완전 종료 및 쿨다운 진입")
                    await pm.remove_position(market)
                    pm.set_cooldown(market, is_tp=True)
                    continue

                # 활성 트랜치 중 매수가격 목록 추출
                active_buy_prices = [Decimal(str(t["buy_price"])) for t in active_tranches]
                lowest_buy_p = min(active_buy_prices, default=Decimal(str(state.get("last_buy_price", cur_p))))
                buy_cnt = int(state.get("buy_count", len(tranches) if tranches else 1))
                last_buy_ts = float(state.get("last_buy_ts", state.get("entry_ts", now_ts)))

                # ----------------------------------------------------
                # [단계 A] 1틱 하락 가격변동 감지 시 3분봉 반전(반등) 확인 후 독립 추가 구매
                # - 가격변동이 없는 경우 (cur_p == lowest_buy_p): 추가 구매 안 하고 기존 지정가 매도 대기
                # - 1틱 이상 하락했으나 3분봉이 음봉 연속 하락 중인 경우: 손실 방어를 위해 매수 보류!
                # - 1틱 이상 하락 후 3분봉이 양봉 전환 / 반전(반등)하는 순간: 즉시 독립 추가 구매 집행 -> 선지정가 매도 등록
                # - 이미 매수된 동일 가격대 중복 매수 차단 (각 트랜치는 반드시 다른 가격대여야 함)
                # ----------------------------------------------------
                drop_from_last = (lowest_buy_p - cur_p) / tick if tick > 0 else Decimal("0")

                # 종목별 누적 투자 한도(limitAmt) 및 추가 매수 가능 횟수 산출
                limit_krw = Decimal(str(db_setup.get("limitAmt", 100000)))
                add_krw = db_setup.get("addAmt", ADDITIONAL_BUY_KRW)
                init_krw = db_setup.get("buyAmt", INITIAL_BUY_KRW)

                max_allowed_buys = int(os.getenv("MAX_ADDITIONAL_BUYS", "19"))
                if add_krw > 0 and limit_krw > init_krw:
                    calc_buys = int((limit_krw - init_krw) // add_krw)
                    if calc_buys > 0:
                        max_allowed_buys = min(max_allowed_buys, calc_buys)

                current_invested = avg_p * total_vol
                under_limit = (current_invested + add_krw) <= (limit_krw * Decimal("1.05"))

                # 3분봉 반전/반등 신호 검증
                rebound_ok, rebound_reason = await check_3m_reversal_signal(market)

                # 현재가가 최저매수가보다 엄격하게 낮고, 이미 매수한 가격대가 아닐 때만 유효
                is_strictly_lower = (cur_p < lowest_buy_p) and (cur_p not in active_buy_prices)

                can_add_buy = (
                    buy_cnt <= max_allowed_buys
                    and under_limit
                    and spendable_krw >= add_krw
                    and (now_ts - last_buy_ts >= 10.0)
                    and btc_status != "EMERGENCY"
                    and rebound_ok
                    and is_strictly_lower
                )

                if drop_from_last >= Decimal("1"):
                    if not rebound_ok:
                        if loop_cnt % 6 == 1:
                            print(
                                f"⏳ [STEP-BUY-WAIT] {market} 1틱 하락 감지 (최저매수가: {lowest_buy_p}원 -> 현재가: {cur_p}원, -{drop_from_last:.0f}틱)!\n"
                                f"  └ 3분봉 하락 연속 진행 중 ({rebound_reason}) -> 손실 방어를 위해 3분봉 반전 시까지 추가 매수 보류"
                            )
                    elif not is_strictly_lower:
                        if loop_cnt % 6 == 1:
                            print(f"⏳ [STEP-BUY-WAIT] {market} 현재가 {cur_p}원은 이미 매수된 호가입니다 -> 중복 매수 방지 대기")
                    elif can_add_buy:
                        async with get_market_lock(market):
                            next_tranche_id = max([t.get("id", 0) for t in tranches], default=0) + 1
                            print(
                                f"💧 [INDEPENDENT-STEP-BUY] {market} 1틱 하락 후 3분봉 반전({rebound_reason}) 감지!\n"
                                f"  └ 최저매수가: {lowest_buy_p}원 -> 현재가: {cur_p}원 (-{drop_from_last:.0f}틱 하락 후 반등 턴)\n"
                                f"  └ 트랜치 #{next_tranche_id} 독립 추가 구매 {add_krw:,.0f}원 집행 (차수: {buy_cnt + 1}/{max_allowed_buys + 1}, 누적: {current_invested + add_krw:,.0f}/{limit_krw:,.0f}원)"
                            )
                            if LIVE_TRADING:
                                await market_buy(upbit_api, access_key, secret_key, market, add_krw, UPBIT_ORDER_URL)
                            await asyncio.sleep(0.8)

                            accs_after = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                            target_acc = next((x for x in accs_after if x.get("currency") == market.split("-")[1]), None)
                            if target_acc:
                                # 기존 주문 물량은 locked에 있으므로 이번 매수 물량은 balance에 위치
                                new_tranche_vol = Decimal(str(target_acc.get("balance", "0")))
                                if new_tranche_vol <= Decimal("0"):
                                    new_tranche_vol = (add_krw / cur_p).quantize(Decimal("0.00000001"), rounding=ROUND_FLOOR)
                                new_avg = Decimal(str(target_acc.get("avg_buy_price", "0")))
                            else:
                                new_tranche_vol = (add_krw / cur_p).quantize(Decimal("0.00000001"), rounding=ROUND_FLOOR)
                                new_avg = ((avg_p * total_vol) + add_krw) / (total_vol + new_tranche_vol)

                            # 이번 트랜치 매수가격(cur_p) 기준 독립 목표가 산출
                            new_target_p, new_tp_pct, new_ticks = calculate_micro_tick_target(market, cur_p)
                            # 기존 주문 호가와 중복되지 않도록 방어 (동일 호가 분산 방지)
                            existing_targets = {Decimal(str(t.get("target_price", "0"))) for t in active_tranches}
                            if new_target_p in existing_targets:
                                new_target_p = adjust_price_to_tick(cur_p + tick, side="ask")

                            # [독립 매도 발주] 기존 상위 매도 주문은 일체 취소하지 않고 신규 물량만 별도 주문 등록
                            new_sell_uuid = await place_tranche_limit_sell(access_key, secret_key, market, new_tranche_vol, new_target_p)

                            new_t_entry = {
                                "id": next_tranche_id,
                                "buy_price": str(cur_p),
                                "volume": str(new_tranche_vol),
                                "target_price": str(new_target_p),
                                "target_ticks": new_ticks,
                                "target_tp_pct": str(new_tp_pct),
                                "sell_uuid": new_sell_uuid,
                                "buy_ts": time.time(),
                                "status": "WAITING_SELL"
                            }
                            state.setdefault("tranches", []).append(new_t_entry)
                            state["buy_count"] = buy_cnt + 1
                            state["last_buy_price"] = cur_p
                            state["last_buy_ts"] = time.time()
                            state["avg_price"] = new_avg
                            await pm.save_position(market, state)

                            print(
                                f"  └ 🎯 트랜치 #{next_tranche_id} 독립 선(先)지정가 매도 등록 완료: {new_tranche_vol}개 @ {new_target_p}원 (+{new_tp_pct}% / {new_ticks}틱)\n"
                                f"  └ 📌 기존 상위 매도 주문들은 그대로 유지됨 (총 {len(active_tranches) + 1}개 독립 매도 분리 대기 중)"
                            )
                            spendable_krw -= add_krw
                            continue

                # ----------------------------------------------------
                # [단계 B] 추가 매수 한도 소진 후 범위 이탈 시 동적 리셋 감지 (개별 트랜치 유지)
                # ----------------------------------------------------
                drop_from_avg = (avg_p - cur_p) / tick if tick > 0 else Decimal("0")
                last_reset_ts = float(state.get("last_reset_ts", state.get("entry_ts", now_ts)))
                reset_cnt = int(state.get("reset_count", 0))

                is_range_breached = (drop_from_avg >= Decimal(str(RESET_DROP_TICKS)) or pnl <= RESET_DROP_PCT)
                exhausted_buys = (buy_cnt > max_allowed_buys or not under_limit)
                can_reset = (exhausted_buys and reset_cnt < MAX_RESETS and (now_ts - last_reset_ts >= RESET_COOLDOWN_SEC))

                if is_range_breached and can_reset:
                    async with get_market_lock(market):
                        success = await reposition_tranches_limit_sell(
                            access_key, secret_key, market, state, cur_p
                        )
                        if success:
                            await pm.save_position(market, state)
                            continue

                # 30분봉 동적 손절선 조회
                limits = await get_dynamic_stop_limits(market, patterns_data=_patterns_cache["data"], client=get_http_client())
                dyn_hard_stop = limits["hard_stop"]
                dyn_max_drop = limits["max_drop"]

                # 손절 조건 검증 (추가 매수 및 리셋 한도 소진 후 추가 하락 또는 대폭락 감지 시)
                is_hard_stop = bool(ENABLE_STOP and (exhausted_buys or reset_cnt >= MAX_RESETS) and pnl <= dyn_hard_stop)
                is_max_drop = bool(ENABLE_STOP and pnl <= dyn_max_drop)

                if is_hard_stop or is_max_drop:
                    reason = f"HARD_STOP({dyn_hard_stop}%)" if is_hard_stop else f"MAX_DROP_SAFETY({dyn_max_drop}%)"
                    print(f"🚨 [LOSS-CUT] {market} {reason} 발동 (PnL: {pnl}%) -> 전량 시장가 청산")
                    async with get_market_lock(market):
                        await cancel_all_orders(access_key, secret_key, market)
                        await asyncio.sleep(0.3)
                        rem_vol = quantize_volume(total_vol)
                        if rem_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                            if LIVE_TRADING:
                                await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                                    "market": market, "side": "ask", "volume": str(rem_vol), "ord_type": "market"
                                })
                            await register_loss_cut(user_no, market, rem_vol, cur_p, pnl)
                        await pm.remove_position(market)
                        pm.set_cooldown(market, is_tp=False)
                        continue

            # ========================================================
            # 2. 신규 500원 미만 동전주 탐색 및 진입
            #    [진입 제어]:
            #    - 기존 보유 코인 중 가격변동 없이 3분(180초) 이상 미체결 정체 중인 코인이 있으면 신규 매수 보류
            #    - 무작정 시간 경과만으로 사들이지 않고 기존 포지션 회전을 기다림
            # ========================================================
            active_cnt = len(pm.positions)
            max_coins = db_setup["maxCoincnt"]
            now_ts = time.time()

            # 보유 포지션 중 체결 지연/정체(최근 활동 이후 3분 이상) 종목 조사
            stagnant_coins = []
            for m, s in pm.positions.items():
                last_act_ts = max(
                    float(s.get("entry_ts", now_ts)),
                    float(s.get("last_buy_ts", 0.0)),
                    float(s.get("last_reset_ts", 0.0))
                )
                if (now_ts - last_act_ts) >= STAGNANT_TIMEOUT_SEC:
                    stagnant_coins.append(m)

            has_stagnant = len(stagnant_coins) > 0

            if has_stagnant and loop_cnt % 6 == 1:
                print(f"⏳ [WAIT-STAGNANT] {stagnant_coins} 포지션이 가격변동 없이 3분 이상 미체결 대기 중 -> 신규 매수를 보류하고 체결/변동을 기다립니다.")

            # 비트코인 상태 NORMAL, 슬롯 여유, 자금 여유, 진입 주기 경과, [정체 포지션 없음] 시에만 진입
            if (
                btc_status == "NORMAL"
                and active_cnt < max_coins
                and spendable_krw >= db_setup["buyAmt"]
                and (now_ts - _last_entry_ts >= ENTRY_INTERVAL_SEC)
                and not has_stagnant
            ):
                candidates = await get_micro_tick_candidates(excluded_markets)
                for cand in candidates:
                    target_market = cand["market"]
                    if (
                        target_market in held_markets
                        or target_market in pm.positions
                        or await pm.is_cooling_down(target_market)
                    ):
                        continue

                    # 3분봉 연속 하락 배제 및 반전/반등 신호 검증 (하락 연속 시 진입 차단)
                    rebound_ok, rebound_reason = await check_3m_reversal_signal(target_market)
                    if not rebound_ok:
                        continue

                    buy_krw = db_setup["buyAmt"]
                    if spendable_krw < buy_krw:
                        break

                    cand_p = Decimal(str(cand["price"]))
                    async with get_market_lock(target_market):
                        print(
                            f"🚀 [MICRO-TICK-ENTRY] {target_market} 진입 조건 만족 (3분봉 반전: {rebound_reason})!\n"
                            f"  └ 가격: {cand_p}원 (<500원) | 체결: {cand['trades']}건 | 점수: {cand['score']} | 매수: {buy_krw:,.0f}원"
                        )
                        if LIVE_TRADING:
                            await market_buy(upbit_api, access_key, secret_key, target_market, buy_krw, UPBIT_ORDER_URL)
                        await asyncio.sleep(0.8)

                        # 체결 계좌 확인 및 선(先) 지정가 매도 즉시 등록
                        accs_after = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                        target_acc = next((x for x in accs_after if x.get("currency") == target_market.split("-")[1]), None)
                        if target_acc:
                            new_tot = Decimal(str(target_acc.get("balance", "0"))) + Decimal(str(target_acc.get("locked", "0")))
                            new_avg = Decimal(str(target_acc.get("avg_buy_price", "0")))
                        else:
                            new_avg = cand_p
                            new_tot = (buy_krw / cand_p).quantize(Decimal("0.00000001"), rounding=ROUND_FLOOR)

                        target_p, tp_pct, ticks = calculate_micro_tick_target(target_market, cand_p)
                        sell_uuid = await place_tranche_limit_sell(access_key, secret_key, target_market, new_tot, target_p)

                        new_state = {
                            "avg_price": new_avg,
                            "last_buy_price": cand_p,
                            "entry_ts": time.time(),
                            "last_buy_ts": time.time(),
                            "buy_count": 1,
                            "reset_count": 0,
                            "tranches": [
                                {
                                    "id": 1,
                                    "buy_price": str(cand_p),
                                    "volume": str(new_tot),
                                    "target_price": str(target_p),
                                    "target_ticks": ticks,
                                    "target_tp_pct": str(tp_pct),
                                    "sell_uuid": sell_uuid,
                                    "buy_ts": time.time(),
                                    "status": "WAITING_SELL"
                                }
                            ]
                        }
                        await pm.save_position(target_market, new_state)
                        print(
                            f"  └ 🎯 트랜치 #1 독립 선(先)지정가 매도 등록: {new_tot}개 @ {target_p}원 (+{tp_pct}% / {ticks}틱)"
                        )
                        spendable_krw -= buy_krw
                        _last_entry_ts = time.time()
                        break

        except Exception as loop_err:
            print(f"[mtpond8][ERROR] 메인 루프 예외: {loop_err}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


# ============================================================
# 12. 컨트롤러 (웹 ON/OFF 상태 감시 및 기동)
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task: Optional[asyncio.Task] = None
    print(f"[CTRL] mtpond8 Controller 시작 (User: {user_no}, Server: {server_no})")

    while True:
        try:
            active_flag = False

            # 1. mtpond:status:{user_no} 키 우선 확인 (있는 경우)
            raw_status = await redis_client.get(f"mtpond:status:{user_no}")
            if raw_status is not None:
                active_flag = str(raw_status).strip().upper() in ("1", "TRUE", "ON", "START", "Y")

            # 2. mtpond:setup:{user_no} JSON 설정 확인
            c = await redis_client.get(f"mtpond:setup:{user_no}")
            if c:
                try:
                    data = json.loads(c)
                    cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
                    if isinstance(cfg, dict):
                        apply_dynamic_config(cfg)
                        if raw_status is None and "activeYN" in cfg:
                            active_flag = str(cfg.get("activeYN", "N")).strip().upper() == "Y"
                except Exception as parse_err:
                    print(f"[CTRL][WARN] Redis setup 파싱 오류: {parse_err}")

            # 3. Redis에 상태가 전혀 없으면 DB API 조회 (Fallback)
            if raw_status is None and not c:
                try:
                    db_setup = await fetch_database_setup(user_no)
                    active_flag = str(db_setup.get("activeYN", "N")).strip().upper() == "Y"
                except Exception as db_err:
                    print(f"[CTRL][WARN] DB 설정 조회 실패: {db_err}")

            # 기동 중 예외 종료 여부 감지
            if task is not None and task.done():
                exc = task.exception()
                if exc:
                    print(f"⚠️ [CTRL] mtpond8 엔진 예외 발생으로 종료됨: {exc} → 재기동 대기")
                task = None

            # ON 상태인데 task가 없으면 엔진 기동
            if active_flag and task is None:
                print(f"▶️ [CTRL] mtpond8 ON 감지 -> 엔진 기동 (User: {user_no}, Server: {server_no})")
                task = asyncio.create_task(run_mtpond8_engine(user_no, server_no))

            # OFF 상태인데 task가 실행 중이면 엔진 정지
            elif not active_flag and task is not None:
                print(f"⏹️ [CTRL] mtpond8 OFF 감지 -> 엔진 정지 요청 (User: {user_no})")
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                task = None

        except Exception as e:
            print(f"[CTRL][ERR] 컨트롤러 루프 예외: {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


# ============================================================
# 13. 진입점 (Entry Point)
# ============================================================
async def main():
    user_no = int(os.getenv("USER_NO", "1"))
    server_no = int(os.getenv("SERVER_NO", "1"))
    await run_mtpond_controller(user_no, server_no)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] mtpond8 프로세스 종료 요청 수신")
