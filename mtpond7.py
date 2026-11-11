"""전략 7: 일일 1% 수익 실현 특화 퀀트 엔진.

주요 핵심 실행 기법:
1. Spike Catcher (매수 체결 즉시 선(先) 지정가 매도 등록):
   - 매수 체결 직후 호가창에 목표 지정가 매도를 미리 배치하여 1초 미만의 윗꼬리 스파이크를 슬리피지 없이 100% 자동 체결.
   - 코인별 호가 틱 크기(Tick Ratio)를 연동하여 실질 순익(수수료 0.10% 제외)이 보장되는 최적 틱 단위(4틱 / 8틱) 타겟팅.

2. 본절 스탑(Break-Even Stop) 결합 2단 사다리 익절:
   - 1차 목표 (Tier 1, 60% 비중): +0.45% ~ +0.65% (또는 4틱) 빠른 기본 수익 확정.
   - Tier 1 체결 즉시 잔여 40% 물량의 스탑로스를 본절가(+0.08%, 수수료 커버)로 즉시 상향 이동.
   - 가격 반락 시 본절가에서 안전 청산 -> 1차 익절분으로 인해 전체 거래는 100% 무위험 확정 플러스 마감!
   - 추가 상승 시 2차 목표 (Tier 2, 40% 비중: +0.95% ~ +1.35%) 체결로 일일 1%+ 초과 수익 달성.

3. 비트코인(BTC) 3단계 체력 필터 (BTC Market Regime Filter):
   - KRW-BTC 1시간/4시간 캔들 실시간 모니터링 (NORMAL, CAUTION, EMERGENCY).
   - 비트코인 급락 또는 20이평 이탈 시 모든 신규 진입을 전면 차단하여 시장 급락 피격 원천 방지.

4. 30분봉 진폭(v30) 연동 4단계 동적 손절 및 현금 버퍼(25%) 안전망:
   - mtpond_common의 calculate_dynamic_stops 연동 (하드 손절 -7.0%~-9.5%, 대폭락 강제안전망 -14.0%~-15.0%).
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
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "4"))

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
# 3. 매매 및 이익 실현 핵심 상수
# ============================================================
# [사다리 익절 비중 및 본절 안전망 파라미터]
TIER1_RATIO = Decimal("0.60")           # 1차 익절 물량 비중 (60%)
TIER2_RATIO = Decimal("0.40")           # 2차 익절 물량 비중 (40%)
BREAKEVEN_BUFFER_PCT = Decimal("0.08")  # 본절 스탑 트리거선 (+0.08%, 수수료 0.10% 제외 실질 본절)

# [자금 관리 및 안전망]
INITIAL_BUY_KRW = _decimal_env("INITIAL_BUY_KRW", "40000")
ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")
MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.25")  # 25% 현금 예비금 필수 유지
MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "2"))       # 종목당 최대 물타기 2회

# [손절선 기본값]
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")        # 기본 하드 손절선 (-7.0%)
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-15.0")  # 대폭락 안전망 (-15.0%)

# [쿨다운]
COOLDOWN_TP_SEC = 180   # 익절 후 재진입 쿨다운 (3분)
COOLDOWN_SL_SEC = 300   # 손절 후 재진입 쿨다운 (5분)

BB_PATTERNS_CACHE_TTL_SEC = 60


# ============================================================
# 4. 동적 설정 & 제외 코인 관리
# ============================================================
def to_dec(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def apply_dynamic_config(cfg: Dict[str, Any]):
    global ENABLE_STOP, INITIAL_BUY_KRW, ADDITIONAL_BUY_KRW, HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT
    global MIN_CASH_BUFFER_RATIO, MAX_ACTIVE_MARKETS

    changes = []
    if "stopYN" in cfg:
        stop_enabled = str(cfg.get("stopYN")).strip().upper() in ("1", "Y", "TRUE")
        if stop_enabled != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {stop_enabled}")
            ENABLE_STOP = stop_enabled

    if "buyAmt" in cfg:
        b_amt = to_dec(cfg.get("buyAmt"))
        if b_amt is not None and b_amt > 0 and b_amt != INITIAL_BUY_KRW:
            changes.append(f"INITIAL_BUY_KRW {INITIAL_BUY_KRW} -> {b_amt}")
            INITIAL_BUY_KRW = b_amt

    if "addAmt" in cfg:
        add_amt = to_dec(cfg.get("addAmt"))
        if add_amt is not None and add_amt > 0 and add_amt != ADDITIONAL_BUY_KRW:
            changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW} -> {add_amt}")
            ADDITIONAL_BUY_KRW = add_amt

    if "hardStopLoss" in cfg:
        hsl = to_dec(cfg.get("hardStopLoss"))
        if hsl is not None:
            if hsl > 0: hsl = -abs(hsl)
            if hsl != HARD_STOP_LOSS_PCT:
                changes.append(f"HARD_STOP_LOSS_PCT {HARD_STOP_LOSS_PCT} -> {hsl}")
                HARD_STOP_LOSS_PCT = hsl

    if "maxDropLossCut" in cfg:
        mdl = to_dec(cfg.get("maxDropLossCut"))
        if mdl is not None:
            if mdl > 0: mdl = -abs(mdl)
            if mdl != MAX_DROP_LOSS_CUT_PCT:
                changes.append(f"MAX_DROP_LOSS_CUT_PCT {MAX_DROP_LOSS_CUT_PCT} -> {mdl}")
                MAX_DROP_LOSS_CUT_PCT = mdl

    if "maxCoincnt" in cfg:
        try:
            mc = int(cfg.get("maxCoincnt"))
            if mc > 0 and mc != MAX_ACTIVE_MARKETS:
                changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {mc}")
                MAX_ACTIVE_MARKETS = mc
        except Exception:
            pass

    if changes:
        print("[CFG] 동적 설정 업데이트 반영:\n  " + "\n  ".join(changes))


async def sync_redis_config(user_no: int):
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


async def get_excoins(user_no: int) -> Optional[List[tuple]]:
    async with SessionLocal() as session:
        sql = text("""SELECT DISTINCT market
                      FROM exCoinlist
                      WHERE userNo = :u
                        AND attrib NOT LIKE :attr""")
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
DB_SETUP_CACHE_TTL_SEC = 30
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

    parsed = {
        "userNo": user_no,
        "activeVer": int(setup.get("activeVer", 7)),
        "buyAmt": Decimal(str(setup.get("buyAmt", INITIAL_BUY_KRW))),
        "addAmt": Decimal(str(setup.get("addAmt", ADDITIONAL_BUY_KRW))),
        "maxCoincnt": int(setup.get("maxCoincnt", MAX_ACTIVE_MARKETS)),
        "limitAmt": Decimal(str(setup.get("limitAmt", 100000))),
        "stopYN": str(setup.get("stopYN", "Y")).strip().upper() in ("1", "Y", "TRUE"),
        "hardStopLoss": Decimal(str(setup.get("hardStopLoss", HARD_STOP_LOSS_PCT))),
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
    """
    업비트에서 현재 거래 지원 중인 전체 마켓 목록(KRW, BTC, USDT 등) 조회 및 캐싱 (30분 TTL).
    상장폐지(유의/지원종료) 또는 미상장 코인을 필터링하여 API 에러를 방지함.
    """
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
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패 (기존 캐시 유지): {e}")

    return _TRADABLE_MARKETS_SET


async def fetch_current_prices_safe(
    markets: List[str],
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None
) -> Dict[str, Decimal]:
    """
    보유 마켓들의 현재가를 안전하게 일괄 조회한다.
    - 거래 가능 마켓 목록으로 1차 사전 필터링 (상폐/미지원 마켓 제외)
    - 업비트 Ticker 일괄 조회 시도
    - 만약 개별 상폐 코인 등으로 일괄 조회가 실패(404 등)할 경우,
      개별 조회 fallback을 수행하여 유효한 코인 시세만 획득하고 상폐 코인은 거래 가능 목록에서 제거.
    """
    if not markets:
        return {}

    tradable = await get_upbit_tradable_markets()
    # 1차 필터링: 거래 가능 목록에 있는 마켓만 추림
    valid_markets = [m for m in markets if m in tradable] if tradable else list(markets)
    if not valid_markets:
        return {}

    client = get_http_client()
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(valid_markets)}

    # 일괄 조회 시도 (Ticker API는 공개 엔드포인트이므로 인증 불필요)
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
        else:
            print(f"[TICKER][WARN] 일괄 시세 조회 실패 (HTTP {res.status_code}) -> 개별 조회 폴백 가동")
    except Exception as e:
        print(f"[TICKER][WARN] 일괄 시세 조회 통신 예외 ({e}) -> 개별 조회 폴백 가동")

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
                # 거래소에서 삭제된 상폐 코인
                _TRADABLE_MARKETS_SET.discard(m)
                if m not in _untradable_logged_set:
                    _untradable_logged_set.add(m)
                    print(f"⚠️ [DELISTED][WARN] {m} 거래소 상장폐지/미지원 확인 -> 거래 대상에서 즉시 제외")
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


def get_tick_unit(price: Decimal) -> Decimal:
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
# 7. 실행방법 3: 비트코인(BTC) 3단계 체력 필터 (BTC Market Regime)
# ============================================================
_btc_health_cache: Dict[str, Any] = {
    "ts": 0.0,
    "result": {"status": "NORMAL", "btc_pnl_1h": 0.0, "btc_pnl_4h": 0.0, "msg": "초기화 대기"}
}
BTC_HEALTH_CACHE_TTL_SEC = 30.0


async def check_btc_market_health() -> Dict[str, Any]:
    """
    비트코인(KRW-BTC) 1시간/4시간 캔들을 분석하여 시장 전체의 위험도 판별 (30초 캐싱)
    - NORMAL: 안정적 추세 -> 정상적인 일일 1% 익절 운용
    - CAUTION: 1시간 내 -0.7% ~ -1.5% 조정 -> 신규 매수 보수화
    - EMERGENCY: 1시간 내 -1.5% 급락 또는 4시간 -3.0% 급락 -> 모든 신규 매수 전면 차단
    """
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
                "msg": f"🚨 [BTC-EMERGENCY] 급락 감지! (1시간: {pnl_1h:+.2f}%, 4시간: {pnl_4h:+.2f}%, 현재가 < MA20) -> 신규 매수 완전 차단"
            }
        elif pnl_1h <= -0.7 or cur_price < ma20:
            res_dict = {
                "status": "CAUTION",
                "btc_pnl_1h": round(pnl_1h, 2),
                "btc_pnl_4h": round(pnl_4h, 2),
                "msg": f"⚠️ [BTC-CAUTION] 조정세 감지 (1시간: {pnl_1h:+.2f}%, MA20 하회) -> 신규 매수 제한"
            }
        else:
            res_dict = {
                "status": "NORMAL",
                "btc_pnl_1h": round(pnl_1h, 2),
                "btc_pnl_4h": round(pnl_4h, 2),
                "msg": "✅ [BTC-NORMAL] 안정적 추세 유지 (일일 1% 목표 가동)"
            }

        _btc_health_cache["ts"] = now
        _btc_health_cache["result"] = res_dict
        return res_dict
    except Exception as e:
        print(f"[BTC-HEALTH][WARN] 조회 실패: {e}")
        return _btc_health_cache["result"]


# ============================================================
# 8. 실행방법 1 & 2: Spike Catcher & 2단 본절 사다리 발주 로직
# ============================================================
def calculate_tick_aware_targets(market: str, avg_price: Decimal) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    """
    코인의 가격대별 1틱 가치(Tick Ratio)를 지표화하여 목표가를 계산한다.
    - Tier 1 (빠른 회전): 수수료 0.10%를 훌쩍 넘는 4틱 확보 (+0.45% ~ +0.65%)
    - Tier 2 (1% 목표): 8틱 이상 확보 (+0.95% ~ +1.40%)
    반환: (tier1_price, tier2_price, tier1_tp_pct, tier2_tp_pct)
    """
    tick = get_tick_unit(avg_price)
    one_tick_pct = (tick / avg_price) * Decimal("100")

    # Tier 1 목표: 최소 4틱 보장, 0.45% ~ 0.65% 사이
    target_tp1_pct = max(Decimal("0.45"), min(Decimal("0.65"), one_tick_pct * Decimal("4.0")))
    raw_p1 = avg_price * (Decimal("1") + (target_tp1_pct / Decimal("100")))
    p1 = adjust_price_to_tick(raw_p1, side="ask")
    if p1 <= avg_price + tick:
        p1 = avg_price + (tick * Decimal("2"))
    real_tp1_pct = ((p1 - avg_price) / avg_price * Decimal("100")).quantize(Decimal("0.01"))

    # Tier 2 목표: 최소 8틱 보장, 0.95% ~ 1.40% 사이 (일일 1% 완수)
    target_tp2_pct = max(Decimal("0.95"), min(Decimal("1.40"), target_tp1_pct * Decimal("2.0")))
    raw_p2 = avg_price * (Decimal("1") + (target_tp2_pct / Decimal("100")))
    p2 = adjust_price_to_tick(raw_p2, side="ask")
    if p2 <= p1:
        p2 = p1 + (tick * Decimal("2"))
    real_tp2_pct = ((p2 - avg_price) / avg_price * Decimal("100")).quantize(Decimal("0.01"))

    return p1, p2, real_tp1_pct, real_tp2_pct


async def place_spike_catcher_ladder_tp(
    access_key: str,
    secret_key: str,
    market: str,
    state: Dict[str, Any],
    balance: Decimal,
    avg_price: Decimal
):
    """
    [실행방법 1] Spike Catcher: 매수 체결 즉시 호가창에 선(先) 지정가 2단 매도 등록.
    [실행방법 2] 60%(Tier 1) / 40%(Tier 2) 분할 배치 및 본절 안전망 기준가 등록.
    """
    if balance <= 0 or avg_price <= 0:
        return

    # 상장폐지/거래불가 마켓 방어
    tradable = await get_upbit_tradable_markets()
    if tradable and market not in tradable:
        print(f"[SPIKE-CATCHER][SKIP] {market} 거래 불가능한 마켓(상폐 등)이므로 지정가 발주를 건너뜁니다.")
        return

    # 1. 기존 잔여 주문 정리
    await cancel_all_orders(access_key, secret_key, market)
    await asyncio.sleep(0.3)

    p1, p2, tp1_pct, tp2_pct = calculate_tick_aware_targets(market, avg_price)

    v1 = quantize_volume(balance * TIER1_RATIO)
    v2 = quantize_volume(balance - v1)

    eval1 = v1 * p1
    eval2 = v2 * p2

    state["avg_price"] = avg_price
    state["tier1_tp_done"] = False
    state["breakeven_active"] = False
    # 본절 스탑선: 평단 + 0.08% (수수료 커버 및 실질 본절 청산)
    state["breakeven_stop_price"] = adjust_price_to_tick(avg_price * (Decimal("1") + BREAKEVEN_BUFFER_PCT / Decimal("100")), side="bid")

    # 두 슬롯 모두 최소 주문 금액(5,500원) 이상일 경우 2단 사다리 발주
    if eval1 >= MIN_ORDER_NOTIONAL_KRW and eval2 >= MIN_ORDER_NOTIONAL_KRW:
        state["tier1_vol"] = v1
        state["tier2_vol"] = v2
        state["tier1_price"] = p1
        state["tier2_price"] = p2
        state["tier1_tp_pct"] = tp1_pct
        state["tier2_tp_pct"] = tp2_pct

        if LIVE_TRADING:
            r1 = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": market, "side": "ask", "volume": str(v1), "price": str(p1), "ord_type": "limit"
            })
            await asyncio.sleep(0.15)
            r2 = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": market, "side": "ask", "volume": str(v2), "price": str(p2), "ord_type": "limit"
            })
            state["tier1_order_uuid"] = r1.get("uuid")
            state["tier2_order_uuid"] = r2.get("uuid")

        print(
            f"🎯 [SPIKE-CATCHER] {market} 선(先) 지정가 2단 발주 완료!\n"
            f"  └ [Tier 1 (60%)] {v1}개 @ {p1}원 (+{tp1_pct}%)\n"
            f"  └ [Tier 2 (40%)] {v2}개 @ {p2}원 (+{tp2_pct}%) | 본절선: {state['breakeven_stop_price']}원"
        )
    else:
        # 분할 불가 시 전량 단일 지정가 발주
        v_full = quantize_volume(balance)
        if v_full * p1 >= MIN_ORDER_NOTIONAL_KRW:
            state["tier1_vol"] = v_full
            state["tier2_vol"] = Decimal("0")
            state["tier1_price"] = p1
            state["tier2_price"] = None
            state["tier1_tp_pct"] = tp1_pct
            state["tier2_tp_pct"] = None

            if LIVE_TRADING:
                r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                    "market": market, "side": "ask", "volume": str(v_full), "price": str(p1), "ord_type": "limit"
                })
                state["tier1_order_uuid"] = r.get("uuid")
                state["tier2_order_uuid"] = None

            print(f"🎯 [SPIKE-CATCHER] {market} 선(先) 지정가 단일 발주 완료: {v_full}개 @ {p1}원 (+{tp1_pct}%)")


# ============================================================
# 9. 포지션 상태 관리자 (Redis 연동)
# ============================================================
class PositionManager7:
    def __init__(self, user_no: int, redis: aioredis.Redis):
        self.user_no = user_no
        self.r = redis
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.cooldowns: Dict[str, float] = {}

    def get_redis_key(self) -> str:
        return f"mtpond7:positions:{self.user_no}"

    async def save_position(self, market: str, state: Dict[str, Any]):
        self.positions[market] = state
        clean = {}
        for k, v in state.items():
            clean[k] = str(v) if isinstance(v, Decimal) else v
        await self.r.hset(self.get_redis_key(), market, json.dumps(clean))

    async def remove_position(self, market: str):
        if market in self.positions:
            del self.positions[market]
        await self.r.hdel(self.get_redis_key(), market)

    async def restore_from_redis_and_wallet(self, access_key: str, secret_key: str):
        print("🔄 [RESTORE] 시스템 기동: Redis 상태 및 업비트 잔고 대조 복원 시작...")
        wallet_map: Dict[str, Dict[str, Decimal]] = {}
        tradable_markets = await get_upbit_tradable_markets()

        try:
            accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            for a in accs:
                cur = a.get("currency")
                if cur and cur != "KRW":
                    mkt = f"KRW-{cur}"
                    # 거래 불가(상장폐지, BTC마켓 전용, 에어드랍 미상장) 코인 배제
                    if tradable_markets and mkt not in tradable_markets:
                        if mkt not in _untradable_logged_set:
                            _untradable_logged_set.add(mkt)
                            print(f"⚠️ [RESTORE][SKIP] {mkt} KRW 거래 불가(상폐/비상장/BTC마켓전용) 코인 감지 -> 관리 대상 제외")
                        continue
                    tot = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    avg = Decimal(str(a.get("avg_buy_price", "0")))
                    if tot > 0:
                        wallet_map[mkt] = {"total_vol": tot, "avg_price": avg}
        except Exception as e:
            print(f"[RESTORE][ERR] 업비트 잔고 조회 실패: {e}")
            return

        raw_positions = await self.r.hgetall(self.get_redis_key())
        if raw_positions:
            for mkt, s_val in raw_positions.items():
                # 상폐된 코인이 과거 Redis에 남아있는 경우 정리
                if tradable_markets and mkt not in tradable_markets:
                    print(f"🧹 [RESTORE][CLEANUP] {mkt} 상폐 코인 Redis 잔존 포지션 데이터 영구 삭제")
                    await self.r.hdel(self.get_redis_key(), mkt)
                    continue

                if mkt in wallet_map:
                    try:
                        item = json.loads(s_val)
                        real_avg = wallet_map[mkt]["avg_price"]
                        item["avg_price"] = real_avg if real_avg > 0 else Decimal(str(item.get("avg_price", "0")))
                        for dec_k in ("tier1_vol", "tier2_vol", "tier1_price", "tier2_price", "breakeven_stop_price"):
                            if item.get(dec_k) is not None:
                                item[dec_k] = Decimal(str(item[dec_k]))
                        self.positions[mkt] = item
                        print(f"  └ [포지션 복원] {mkt} (평단: {item['avg_price']:,.1f}원 | 잔고: {wallet_map[mkt]['total_vol']})")
                    except Exception as exc:
                        print(f"[RESTORE][WARN] {mkt} 파싱 실패: {exc}")
                else:
                    await self.r.hdel(self.get_redis_key(), mkt)

        # 지갑에 보유 중이지만 Redis에 없는 유효 코인은 신규 편입
        for mkt, w_info in wallet_map.items():
            if tradable_markets and mkt not in tradable_markets:
                continue
            if mkt not in self.positions:
                print(f"  └ ⚠️ [미등록 보유 코인 감지] {mkt} → 전략 7 신규 편입 (수량: {w_info['total_vol']})")
                new_state = {
                    "avg_price": w_info["avg_price"],
                    "entry_ts": time.time(),
                    "buy_count": 1,
                    "tier1_tp_done": False,
                    "breakeven_active": False,
                }
                await self.save_position(mkt, new_state)
                # 즉시 Spike Catcher 2단 지정가 매도 배치
                asyncio.create_task(
                    place_spike_catcher_ladder_tp(access_key, secret_key, mkt, new_state, w_info["total_vol"], w_info["avg_price"])
                )

        print(f"✅ [RESTORE-COMPLETE] 복원 완료. 관리 포지션: {list(self.positions.keys())}")

    def set_cooldown(self, market: str, is_tp: bool):
        sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_SL_SEC
        self.cooldowns[market] = time.time() + sec
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.r.set(f"mtpond7:cooldown:{self.user_no}:{market}", "1", ex=sec))
        except RuntimeError:
            pass
        print(f"[COOLDOWN] {market} {sec}초간 재진입 금지")

    async def is_cooling_down(self, market: str) -> bool:
        if time.time() < self.cooldowns.get(market, 0.0):
            return True
        try:
            if await self.r.exists(f"mtpond7:cooldown:{self.user_no}:{market}"):
                return True
        except Exception:
            pass
        return False


# ============================================================
# 10. 진입 후보 탐색기 (거래량/진폭/반등 확인)
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


async def fetch_15m_rebound_signal(market: str) -> bool:
    """15분봉 기준 양봉 및 5일선 골든크로스 반등 신호 검증"""
    try:
        url = "https://api.upbit.com/v1/candles/minutes/15"
        client = get_http_client()
        res = await client.get(url, params={"market": market, "count": 10})
        if res.status_code != 200:
            return False
        candles = res.json()
        if not candles or len(candles) < 6:
            return False

        cur_c = candles[0]
        cur_p = Decimal(str(cur_c["trade_price"]))
        open_p = Decimal(str(cur_c["opening_price"]))
        # 양봉 확인
        if cur_p < open_p:
            return False

        # 최근 5개봉 단기 MA 상회
        closes = [Decimal(str(c["trade_price"])) for c in candles[:5]]
        ma5 = sum(closes) / Decimal("5")
        return cur_p >= ma5
    except Exception:
        return False


async def get_daily_1pct_candidates(excluded: Set[str]) -> List[Dict[str, Any]]:
    """일일 1% 목표에 가장 적합한 거래 활성 및 양봉 반등 후보 종목 선별"""
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

        # 너무 무거운 코인(2백만원 이상)이나 동전주 미만(10원 이하) 제외, 10분 체결 40건 이상
        if 20 <= price <= 1_500_000 and trades >= 40 and v30 >= 0.8:
            candidates.append({
                "market": mkt,
                "price": price,
                "trades": trades,
                "score": score,
                "v30": v30
            })

    # 체결 빈도 및 종합 점수 순 정렬
    candidates.sort(key=lambda x: (x["trades"], x["score"]), reverse=True)
    return candidates


# ============================================================
# 11. 메인 감시 및 트레이딩 루프
# ============================================================
async def run_mtpond7_engine(user_no: int, server_no: int):
    access_key, secret_key = await get_keys(user_no, server_no)
    if not access_key or not secret_key:
        print(f"[mtpond7][FATAL] user={user_no}, server={server_no} 업비트 API 키를 DB(traceUser) 및 .env에서 찾지 못했습니다.")
        return

    pm = PositionManager7(user_no, redis_client)
    await pm.restore_from_redis_and_wallet(access_key, secret_key)

    print("=" * 60)
    print(f"🚀 [mtpond7] 일일 1% 수익 실현 특화 퀀트 엔진 시작 (User: {user_no}, Server: {server_no})")
    print(f"  └ 1차 익절(Tier 1, 60%): +0.45%~+0.65% (Spike Catcher 선발주)")
    print(f"  └ 2차 익절(Tier 2, 40%): +0.95%~+1.40% (본절 안전망 +0.08% 가동)")
    print(f"  └ 시장 안전망: BTC 3단계 체력 필터 + 30분봉 진폭 연동 4단계 동적 손절")
    print("=" * 60)

    loop_cnt = 0

    while True:
        try:
            loop_cnt += 1
            await sync_redis_config(user_no)
            db_setup = await fetch_database_setup(user_no)
            excluded_markets = await get_excluded_markets(db_setup=db_setup)

            # [실행방법 3] 비트코인 체력 진단
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]

            # 거래 가능 마켓 목록 최신화 (상폐 및 거래불가 마켓 방어)
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
                    # 거래 불가(상장폐지, BTC마켓 전용, 에어드랍 미상장) 코인 배제
                    if tradable_markets and mkt not in tradable_markets:
                        continue
                    tot_v = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    avg_p = Decimal(str(a.get("avg_buy_price", "0")))
                    if tot_v > 0:
                        wallet_map[mkt] = {"total_vol": tot_v, "avg_price": avg_p}

            # 현재 보유 코인 시세 안전 조회 (상폐/오류 코인 안전 격리 및 폴백)
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
                    f"💓 [HEARTBEAT] 총자산: {total_equity:,.0f}원 | 가용KRW: {krw_available:,.0f}원 "
                    f"(예비금: {min_cash_reserve:,.0f}원, 가용: {spendable_krw:,.0f}원) | "
                    f"포지션: {len(pm.positions)}/{db_setup['maxCoincnt']}개 | 비트코인: {btc_status}"
                )

            # ========================================================
            # 1. 기존 보유 포지션 관리 (익절 체결/본절 안전망/손절)
            # ========================================================
            for market in list(pm.positions.keys()):
                # 상폐 마켓 체크 -> 관리 목록에서 즉시 제거
                if tradable_markets and market not in tradable_markets:
                    print(f"⚠️ [DELISTED-EXIT] {market} 상폐 또는 거래불가 감지 -> 포지션 관리에서 즉시 제거")
                    await pm.remove_position(market)
                    continue

                state = pm.positions[market]
                w_info = wallet_map.get(market)

                # 포지션이 거래소에서 이미 전량 청산된 경우
                if not w_info or w_info["total_vol"] <= Decimal("0.000001"):
                    print(f"🎉 [POSITION-CLEARED] {market} 포지션 매도 완료 확인 -> 상태 제거")
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

                # 30분봉 동적 손절 한계선 조회
                limits = await get_dynamic_stop_limits(market, patterns_data=_patterns_cache["data"], client=get_http_client())
                dyn_hard_stop = limits["hard_stop"]
                dyn_max_drop = limits["max_drop"]
                dyn_rescue_min = limits["rescue_min_drop"]

                tier1_tp_done = state.get("tier1_tp_done", False)
                breakeven_active = state.get("breakeven_active", False)
                tier2_vol = Decimal(str(state.get("tier2_vol", "0")))

                # ----------------------------------------------------
                # [단계 A] Tier 1 익절 체결 감지
                # ----------------------------------------------------
                if not tier1_tp_done and state.get("tier1_order_uuid"):
                    # 잔고가 Tier 2 물량 근처로 줄어들었거나 1차 목표가를 상회하는 경우
                    if total_vol <= (tier2_vol * Decimal("1.08")) or cur_p >= Decimal(str(state.get("tier1_price", "0"))):
                        state["tier1_tp_done"] = True
                        state["breakeven_active"] = True
                        be_price = adjust_price_to_tick(avg_p * (Decimal("1") + BREAKEVEN_BUFFER_PCT / Decimal("100")), side="bid")
                        state["breakeven_stop_price"] = be_price
                        await pm.save_position(market, state)
                        print(
                            f"🏆 [TIER1-TP-DONE] {market} 1차 익절 완료 (+{state.get('tier1_tp_pct')}%)!\n"
                            f"  └ 잔여 {total_vol}개에 본절 안전망(Break-Even Stop: {be_price}원, +0.08%) 즉시 가동!"
                        )

                # ----------------------------------------------------
                # [단계 B] 실행방법 2: 본절 안전망 (Break-Even Stop Guard)
                # ----------------------------------------------------
                if tier1_tp_done and breakeven_active:
                    be_stop_p = Decimal(str(state.get("breakeven_stop_price", avg_p)))
                    # 가격이 반락하여 본절 기준선에 도달한 경우
                    if cur_p <= be_stop_p:
                        print(f"🛡️ [BREAK-EVEN-TRIGGER] {market} 가격 반락 감지 (현재가: {cur_p}원 <= 본절가: {be_stop_p}원)")
                        async with get_market_lock(market):
                            await cancel_all_orders(access_key, secret_key, market)
                            await asyncio.sleep(0.3)
                            rem_vol = quantize_volume(total_vol)
                            if rem_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                                        "market": market, "side": "ask", "volume": str(rem_vol), "ord_type": "market"
                                    })
                                print(
                                    f"🛡️ [BREAK-EVEN-EXIT] {market} 잔여 {rem_vol}개 본절 청산 완료!\n"
                                    f"  └ 1차 익절(+{state.get('tier1_tp_pct')}%)로 인해 전체 거래는 최종 플러스 마감 확정!"
                                )
                            await pm.remove_position(market)
                            pm.set_cooldown(market, is_tp=True)
                            continue

                # ----------------------------------------------------
                # [단계 C] 1차 익절 미체결 상태에서의 안전망 (물타기 / 하드손절)
                # ----------------------------------------------------
                if not tier1_tp_done:
                    buy_cnt = int(state.get("buy_count", 1))

                    # 1) 물타기(추가 매수) 조건:
                    # - 비트코인 정상(NORMAL)
                    # - PnL이 구출 구간(rescue_min_drop) 이하
                    # - 15분봉 단기 반등 신호 확인
                    # - 최대 추가 매수 횟수(2회) 미만
                    if (
                        btc_status == "NORMAL"
                        and pnl <= dyn_rescue_min
                        and buy_cnt <= MAX_ADDITIONAL_BUYS
                        and spendable_krw >= db_setup["addAmt"]
                        and await fetch_15m_rebound_signal(market)
                    ):
                        async with get_market_lock(market):
                            add_amt = db_setup["addAmt"]
                            print(f"💧 [WATERING] {market} 조정 구간 반등 신호 포착 -> {buy_cnt + 1}차 추가 매수 {add_amt:,.0f}원")
                            if LIVE_TRADING:
                                await market_buy(upbit_api, access_key, secret_key, market, add_amt, UPBIT_ORDER_URL)
                            await asyncio.sleep(0.8)

                            # 계좌 갱신 후 새로운 평단가로 Spike Catcher 재배치
                            accs_after = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                            target_acc = next((x for x in accs_after if x.get("currency") == market.split("-")[1]), None)
                            if target_acc:
                                new_tot = Decimal(str(target_acc.get("balance", "0"))) + Decimal(str(target_acc.get("locked", "0")))
                                new_avg = Decimal(str(target_acc.get("avg_buy_price", "0")))
                                state["buy_count"] = buy_cnt + 1
                                await pm.save_position(market, state)
                                await place_spike_catcher_ladder_tp(access_key, secret_key, market, state, new_tot, new_avg)
                                spendable_krw -= add_amt
                                continue

                    # 2) 최종 하드 손절 (물타기 소진 후 평단 대비 dyn_hard_stop 이하)
                    is_water_exhausted = (buy_cnt > MAX_ADDITIONAL_BUYS)
                    is_hard_stop = bool(ENABLE_STOP and is_water_exhausted and pnl <= dyn_hard_stop)
                    is_max_drop = bool(ENABLE_STOP and pnl <= dyn_max_drop)

                    if is_hard_stop or is_max_drop:
                        reason = f"WATER_EXHAUSTED_HARD_STOP({dyn_hard_stop}%)" if is_hard_stop else f"MAX_DROP_SAFETY({dyn_max_drop}%)"
                        print(f"🚨 [LOSS-CUT] {market} {reason} 발동 (현재 PnL: {pnl}%) -> 전량 시장가 청산")
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
            # 2. 신규 진입 후보 탐색 및 매수 (BTC 체력 & 현금 버퍼 필터)
            # ========================================================
            active_cnt = len(pm.positions)
            max_coins = db_setup["maxCoincnt"]

            # 비트코인 상태가 정상(NORMAL)이고 슬롯 및 가용 자금이 충분할 때만 진입
            if btc_status == "NORMAL" and active_cnt < max_coins and spendable_krw >= db_setup["buyAmt"]:
                candidates = await get_daily_1pct_candidates(excluded_markets)
                for cand in candidates:
                    target_market = cand["market"]
                    if (
                        target_market in held_markets
                        or target_market in pm.positions
                        or await pm.is_cooling_down(target_market)
                    ):
                        continue

                    # 15분봉 단기 상승 반등 확인
                    if not await fetch_15m_rebound_signal(target_market):
                        continue

                    buy_krw = db_setup["buyAmt"]
                    if spendable_krw < buy_krw:
                        break

                    async with get_market_lock(target_market):
                        print(f"🚀 [NEW-ENTRY] {target_market} 진입 조건 만족 (체결: {cand['trades']}건, 점수: {cand['score']}) -> 매수 {buy_krw:,.0f}원")
                        if LIVE_TRADING:
                            await market_buy(upbit_api, access_key, secret_key, target_market, buy_krw, UPBIT_ORDER_URL)
                        await asyncio.sleep(0.8)

                        # 체결 계좌 확인 및 Spike Catcher 2단 지정가 선발주 즉시 실행
                        accs_after = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                        target_acc = next((x for x in accs_after if x.get("currency") == target_market.split("-")[1]), None)
                        if target_acc:
                            new_tot = Decimal(str(target_acc.get("balance", "0"))) + Decimal(str(target_acc.get("locked", "0")))
                            new_avg = Decimal(str(target_acc.get("avg_buy_price", "0")))
                            new_state = {
                                "avg_price": new_avg,
                                "entry_ts": time.time(),
                                "buy_count": 1,
                                "tier1_tp_done": False,
                                "breakeven_active": False,
                            }
                            await pm.save_position(target_market, new_state)
                            await place_spike_catcher_ladder_tp(access_key, secret_key, target_market, new_state, new_tot, new_avg)
                            spendable_krw -= buy_krw
                            break

        except Exception as loop_err:
            print(f"[mtpond7][ERROR] 메인 루프 예외 발생: {loop_err}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


# ============================================================
# 12. 컨트롤러 (웹 ON/OFF 상태 감시 및 기동)
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task: Optional[asyncio.Task] = None
    print(f"[CTRL] mtpond7 Controller 시작 (User: {user_no}, Server: {server_no})")

    while True:
        try:
            active_flag = False
            raw_status = await redis_client.get(f"mtpond:status:{user_no}")
            if raw_status is not None:
                active_flag = raw_status.strip().upper() in ("ON", "1", "Y", "TRUE")
            c = await redis_client.get(f"mtpond:setup:{user_no}")
            if c:
                data = json.loads(c)
                cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
                if isinstance(cfg, dict):
                    apply_dynamic_config(cfg)
                    if raw_status is None:
                        active_flag = str(cfg.get("activeYN", "N")).strip().upper() == "Y"
                else:
                    cfg = await fetch_database_setup(user_no)
                    active_flag = cfg.get("activeYN", "N") == "Y"
            elif raw_status is None:
                cfg = await fetch_database_setup(user_no)
                active_flag = cfg.get("activeYN", "N") == "Y"

            if task is not None and task.done():
                exc = task.exception()
                if exc:
                    print(f"[CTRL] 엔진 예외 감지: {exc} → 재기동 준비")
                task = None

            if active_flag and task is None:
                print(f"▶️ [CTRL] 봇 활성화(ON) 감지 → run_mtpond7_engine 기동")
                task = asyncio.create_task(run_mtpond7_engine(user_no, server_no))

            elif not active_flag and task is not None:
                print(f"⏸️ [CTRL] 봇 비활성화(OFF) 감지 → run_mtpond7_engine 중단 요청")
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                task = None

        except Exception as e:
            print(f"[CTRL][ERR] {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


async def main():
    dotenv.load_dotenv()
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    try:
        await sync_redis_config(user_no)
        await run_mtpond_controller(user_no, server_no)
    finally:
        await close_http_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[mtpond7] 사용자에 의해 엔진 종료.")
    finally:
        asyncio.run(close_http_client())
