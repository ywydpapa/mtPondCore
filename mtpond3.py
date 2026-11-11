"""전략 3: 1시간·15분 멀티타임프레임 반등 종목에 진입한다.

진폭과 상단 여유로 동적 익절 목표를 정해 지정가 주문을 배치하고, 구출 후
반등 실패 시 전량 손절한다.
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
from typing import Optional, Dict, Any, List, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
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
    get_bb_min_trade_count_10m,
)
from mtpond_db import SessionLocal, engine

# ============================================================
# 2. 환경 설정
# ============================================================
def _decimal_env(name: str, default: str) -> Decimal:
    raw_value = os.getenv(name, default)
    try:
        return Decimal(raw_value.split("#", 1)[0].strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            f"환경 변수 {name} 값이 유효한 Decimal 숫자가 아닙니다: "
            f"{raw_value!r}."
        ) from exc


# ============================================================
# 3. 기본 상수
# ============================================================
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "5"))

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
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))  # 3분 주기
COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "60"))  # 정상익절 1분
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "300"))  # 손절/방어 5분

# 캔들 변동폭 대비 익절 목표 비율 (65% 채택)
PROFIT_RANGE_RATIO = _decimal_env("PROFIT_RANGE_RATIO", "0.65")
MIN_PROFIT_TARGET_PCT = Decimal("0.8")  # 최소 목표 익절선 (+0.8%)
MAX_PROFIT_TARGET_PCT = Decimal("4.0")  # 최대 목표 익절선 (+4.0%)
MIN_ROOM_TO_1H_HIGH_PCT = Decimal("1.2")  # 1시간봉 상단 저항까지 최소 여유폭 (+1.2%)

# 물타기(반등 추종) 및 손절 설정
MAX_ADDITIONAL_BUYS = 2  # 최대 물타기 2회
ADD_BUY_1_DROP = _decimal_env("ADD_BUY_1_DROP", "-2.2")  # 1차 물타기 기준 (-2.2%)
ADD_BUY_2_DROP = _decimal_env("ADD_BUY_2_DROP", "-4.5")  # 2차 물타기 기준 (-4.5%)

# 최종 방어 손절 설정 (물타기 소진 후 또는 구출 불가 시)
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")  # 물타기(2회) 소진 후 최종 손절선 (-7.0%)
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")  # 구출 불가 한계 손절선 (-22.0%)

# 유동성 보호 및 현금 버퍼 / 구출 예산 룰
MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.25")  # 총자산의 25% 현금 보존 버퍼
MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15")  # 구출 전용 예산 상한 (총자산의 15%)
MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")  # 구출 전용 절대 금액 상한 (80,000원)
RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600"))  # 구출 종료 후 10분 쿨다운
RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))  # 구출 후 1시간 내 미탈출 시 타임아웃 청산

# 구출 물타기 설정
_rescue_buy_lock = asyncio.Lock()
_market_locks: Dict[str, asyncio.Lock] = {}


def get_market_lock(market: str) -> asyncio.Lock:
    if market not in _market_locks:
        _market_locks[market] = asyncio.Lock()
    return _market_locks[market]

INTERVAL_SECONDS = 5
MIN_NOTIONAL_KRW = Decimal("5000")
ORDER_NOTIONAL_BUFFER_PCT = Decimal("0.01")

# ============================================================
# 5. RUNTIME CONFIG (Redis 연동 동적 설정 관리)
# ============================================================
_CONFIG_INITIALIZED = False

# 전역 설정 변수 선언 (Redis 및 .env 폴백 연동)
MAX_ACTIVE_MARKETS = 10
RANGE_BUY_KRW = Decimal("40000")
ADDITIONAL_BUY_KRW = Decimal("40000")
MAX_TOTAL_INVEST_PER_MARKET = Decimal("120000")
BB_MIN_OVERALL_SCORE = 3


class PositionState:
    def __init__(self, user_no: int = 0):
        self.user_no = user_no
        self.data: Dict[str, Dict[str, Any]] = {}
        self.sell_history: Dict[str, Dict[str, Any]] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}
        self.last_redis_save_ts: float = 0.0
        self.last_rescue_exit_ts: float = 0.0

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
                "last_additional_buy_pnl": Decimal("0"),
                "is_rescued": False,
                "rescue_buy_price": None
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


async def save_state_to_redis(user_no: int, ps: PositionState):
    """포지션 상태 및 매수 이력, 쿨다운 정보를 Redis에 영속 저장합니다."""
    if user_no <= 0:
        return
    key = f"mtpond:state:opt3:{user_no}"
    try:
        now = time.time()
        valid_sell_history = {
            m: v for m, v in ps.sell_history.items() if v.get("expire_ts", 0) > now
        }
        payload = {
            "saved_ts": now,
            "last_rescue_exit_ts": ps.last_rescue_exit_ts,
            "buy_info": {
                m: {
                    "total_buys": v.get("total_buys", 0),
                    "total_invested": str(v.get("total_invested", "0")),
                }
                for m, v in ps.buy_info.items()
            },
            "data": {
                m: {
                    k: (str(val) if isinstance(val, Decimal) else val)
                    for k, val in v.items()
                }
                for m, v in ps.data.items()
            },
            "sell_history": valid_sell_history,
        }
        await redis_client.set(key, json.dumps(payload), ex=86400 * 7)
        ps.last_redis_save_ts = now
    except Exception as e:
        print(f"[STATE-SAVE][WARN] Redis 상태 저장 실패: {e}")


async def restore_state_from_redis(user_no: int, ps: PositionState, raw_accounts: List[dict]):
    """시스템 재시작 또는 정기 리셋 시 Redis에서 포지션 상태를 복원합니다."""
    if user_no <= 0:
        return
    key = f"mtpond:state:opt3:{user_no}"
    try:
        raw = await redis_client.get(key)
        if not raw:
            return
        cached = json.loads(raw)

        ps.last_rescue_exit_ts = float(cached.get("last_rescue_exit_ts", 0.0))

        held_markets = set()
        for a in raw_accounts:
            cur = a.get("currency")
            if cur and cur != "KRW":
                bal = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                if bal > 0:
                    held_markets.add(f"KRW-{cur}")

        restored_positions = 0
        for m, v in cached.get("buy_info", {}).items():
            if m in held_markets:
                ps.buy_info[m] = {
                    "total_buys": int(v.get("total_buys", 1)),
                    "total_invested": Decimal(str(v.get("total_invested", "0"))),
                }
                restored_positions += 1

        for m, v in cached.get("data", {}).items():
            if m in held_markets:
                restored_item = {}
                for dk, dv in v.items():
                    if dk in ("peak_pnl", "min_pnl", "avg_buy_price", "dynamic_tp",
                              "last_additional_buy_price", "last_additional_buy_pnl",
                              "rescue_buy_price", "rescue_amount"):
                        restored_item[dk] = Decimal(str(dv)) if dv is not None else None
                    else:
                        restored_item[dk] = dv
                ps.data[m] = restored_item

        now = time.time()
        for m, v in cached.get("sell_history", {}).items():
            if v.get("expire_ts", 0) > now:
                ps.sell_history[m] = v

        if restored_positions > 0 or ps.sell_history:
            print(
                f"🔄 [STATE-RESTORE] Redis 상태 복원 완료: 보유 {restored_positions}개, "
                f"쿨다운 {len(ps.sell_history)}개, 최근구출경과: {int(now - ps.last_rescue_exit_ts)}초"
            )
    except Exception as e:
        print(f"[STATE-RESTORE][WARN] Redis 상태 복원 실패: {e}")


def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M
    global HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT
    global MIN_CASH_BUFFER_RATIO, MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW
    global RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = _decimal_env("RANGE_BUY_KRW", "40000")
    ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")
    MAX_TOTAL_INVEST_PER_MARKET = _decimal_env("MAX_TOTAL_INVEST_PER_MARKET", "120000")
    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = get_bb_min_trade_count_10m()
    HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")
    MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")

    MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.25")
    MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15")
    MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")
    RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600"))
    RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))

    _CONFIG_INITIALIZED = True
    print(
        f"[INIT] 환경 기본값 초기화 완료 (force={force}) | 활성최대:{MAX_ACTIVE_MARKETS}개, "
        f"최초금액:{RANGE_BUY_KRW}원, 손절:{HARD_STOP_LOSS_PCT}%, 현금버퍼:{MIN_CASH_BUFFER_RATIO*100:.0f}%, "
        f"구출상한:{MAX_RESCUE_BUDGET_KRW:,.0f}원"
    )


def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW, MAX_TOTAL_INVEST_PER_MARKET
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M
    global ENABLE_STOP, HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT
    global MIN_CASH_BUFFER_RATIO, MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW
    global RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).strip().upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            print(f"[REDIS-CONFIG] ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

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
            if v is not None and v.is_finite() and v >= 0: ADDITIONAL_BUY_KRW = v

    for key in ("limitAmt", "max_total_invest_per_market", "MAX_TOTAL_INVEST_PER_MARKET"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0: MAX_TOTAL_INVEST_PER_MARKET = v

    for key in ("hardStopPct", "hard_stop_loss_pct", "HARD_STOP_LOSS_PCT"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v is not None and v < 0:
                HARD_STOP_LOSS_PCT = v
                print(f"[REDIS-CONFIG] HARD_STOP_LOSS_PCT -> {HARD_STOP_LOSS_PCT}%")

    for key in ("maxDropLossCutPct", "max_drop_loss_cut_pct", "MAX_DROP_LOSS_CUT_PCT"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v is not None and v < 0:
                MAX_DROP_LOSS_CUT_PCT = v
                print(f"[REDIS-CONFIG] MAX_DROP_LOSS_CUT_PCT -> {MAX_DROP_LOSS_CUT_PCT}%")

    for key in ("minCashBufferRatio", "min_cash_buffer_ratio", "cashBufferRatio"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v is not None and Decimal("0") <= v < Decimal("1"):
                MIN_CASH_BUFFER_RATIO = v
                print(f"[REDIS-CONFIG] MIN_CASH_BUFFER_RATIO -> {MIN_CASH_BUFFER_RATIO * 100:.0f}%")

    for key in ("maxRescueBudgetRatio", "max_rescue_budget_ratio", "rescueBudgetRatio"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v is not None and v > 0:
                MAX_RESCUE_BUDGET_RATIO = v
                print(f"[REDIS-CONFIG] MAX_RESCUE_BUDGET_RATIO -> {MAX_RESCUE_BUDGET_RATIO * 100:.0f}%")

    for key in ("maxRescueBudgetKrw", "max_rescue_budget_krw", "rescueBudgetKrw"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v is not None and v > 0:
                MAX_RESCUE_BUDGET_KRW = v
                print(f"[REDIS-CONFIG] MAX_RESCUE_BUDGET_KRW -> {MAX_RESCUE_BUDGET_KRW:,.0f}원")

    for key in ("rescueGlobalCooldownSec", "rescue_global_cooldown_sec"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv >= 0:
                    RESCUE_GLOBAL_COOLDOWN_SEC = nv
                    print(f"[REDIS-CONFIG] RESCUE_GLOBAL_COOLDOWN_SEC -> {RESCUE_GLOBAL_COOLDOWN_SEC}초")
            except:
                pass

    for key in ("rescueTimeoutSec", "rescue_timeout_sec"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv >= 0:
                    RESCUE_TIMEOUT_SEC = nv
                    print(f"[REDIS-CONFIG] RESCUE_TIMEOUT_SEC -> {RESCUE_TIMEOUT_SEC}초")
            except:
                pass


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
    await _register_loss_cut(
        API_BASE, user_no, market, quantity, price, loss_rate
    )


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
            if not isinstance(candles, list) or len(candles) < 20:
                return {"status": "NORMAL", "btc_pnl_1h": 0.0, "msg": "BTC 캔들 데이터 부족"}

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
_bb_patterns_cache: Dict[str, Any] = {
    "fetched_ts": 0.0,
    "candidates": [],
    "trade_counts": {},
    "market_scores": {},
}


async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if not force and now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC:
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
    trade_counts = {}
    market_scores = {}

    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in excluded: continue

        # [핵심] advisor의 안전 추천 필터를 통과하지 못한 코인은 매수 완전 배제
        rec = c.get("recommendation")
        if not rec or not isinstance(rec, dict): continue

        try:
            score = int(c.get("overall_score", 0))
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
            p_score = float(rec.get("priority_score", 0))
        except:
            continue

        trade_counts[mkt] = trades
        market_scores[mkt] = score
        if price < 30 or trades < 25:
            continue

        qualified.append({
            "market": mkt,
            "price": Decimal(str(price)),
            "overall_score": score,
            "priority_score": p_score,
            "trade_count_10m": trades,
            "recommendation": rec
        })

    qualified.sort(key=lambda x: (x["priority_score"], x["overall_score"], x["trade_count_10m"]), reverse=True)
    _bb_patterns_cache["fetched_ts"] = now
    _bb_patterns_cache["candidates"] = qualified
    _bb_patterns_cache["trade_counts"] = trade_counts
    _bb_patterns_cache["market_scores"] = market_scores
    return qualified


async def has_minimum_rescue_trade_activity(market: str) -> bool:
    await fetch_bb_patterns_candidates()
    trade_count = _bb_patterns_cache["trade_counts"].get(market, 0)
    if trade_count < BB_MIN_TRADE_COUNT_10M:
        print(
            f"[RESCUE-SKIP] {market} 10분 거래량 부족 "
            f"({trade_count}/{BB_MIN_TRADE_COUNT_10M}건)"
        )
        return False
    return True


# ============================================================
# 11. 주문 실행 도우미
# ============================================================
async def order_market_buy_price(access_key: str, secret_key: str, market: str, krw_amount: Decimal):
    return await market_buy(
        upbit_request, access_key, secret_key, market, krw_amount,
        UPBIT_ORDER_URL, max_retry=2,
    )


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
    except Exception as e:
        print(f"[ORDER-CANCEL][ERR] {market} 미체결 주문 취소 실패: {e}")
        raise


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
                st["avg_buy_price"] = avg_price
                st["last_update_ts"] = time.time()
                asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, mkt, ps))


# ============================================================
# 13. 1시간/15분봉 반등 지지 확인형 물타기 프로세스
# ============================================================
async def _process_rescue_operations_unlocked(
        access_key: str,
        secret_key: str,
        enriched_positions: List[dict],
        ps: PositionState,
        effective_krw: Decimal,
        total_equity: Decimal,
        btc_status: str
) -> Decimal:
    """상승 추세와 거래량을 확인한 손실 종목을 안전 예산 한도 내에서 구출합니다."""
    used_krw = Decimal("0")
    if (
        effective_krw < MIN_ORDER_NOTIONAL_KRW
        or ADDITIONAL_BUY_KRW < MIN_ORDER_NOTIONAL_KRW
    ):
        return used_krw

    # [보호 1] 비트코인이 정배열/안정 상태(NORMAL)가 아니면 구출 금지 (조정/급락장 구출 차단)
    if btc_status != "NORMAL":
        return used_krw

    # [보호 2] 이전 구출 완료 후 글로벌 쿨다운(10분) 미경과 시 구출 차단 (순차적 자금 고갈 방지)
    now = time.time()
    if ps.last_rescue_exit_ts > 0 and (now - ps.last_rescue_exit_ts) < RESCUE_GLOBAL_COOLDOWN_SEC:
        return used_krw

    # [보호 3] 동시 구출 수(1개) 제한
    active_rescue_count = sum(
        1 for state in ps.data.values() if state.get("is_rescued", False)
    )
    if active_rescue_count >= MAX_CONCURRENT_RESCUE:
        return used_krw

    # [보호 4] 구출 전용 예산 캡 (총자산의 15% 및 최대 8만 원 상한)
    max_rescue_cap = min(
        (total_equity * MAX_RESCUE_BUDGET_RATIO).quantize(Decimal("1000")),
        MAX_RESCUE_BUDGET_KRW
    )
    current_active_rescue_krw = sum(
        Decimal(str(state.get("rescue_amount", "0")))
        for state in ps.data.values() if state.get("is_rescued", False)
    )
    if current_active_rescue_krw >= max_rescue_cap:
        return used_krw

    available_for_rescue = min(
        ADDITIONAL_BUY_KRW,
        max_rescue_cap - current_active_rescue_krw,
        effective_krw
    ).to_integral_value(rounding=ROUND_FLOOR)

    if available_for_rescue < MIN_ORDER_NOTIONAL_KRW:
        return used_krw

    await fetch_bb_patterns_candidates()
    rescue_candidates = []
    for pos in enriched_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        state = ps.data.get(market, {})
        if state.get("is_rescued", False) or pnl <= RESCUE_MAX_DROP:
            continue

        total_buys, _ = ps.get_buy_stats(market)
        if total_buys - 1 >= MAX_ADDITIONAL_BUYS or pnl <= RESCUE_MIN_DROP:
            rescue_candidates.append(pos)

    rescue_candidates.sort(
        key=lambda position: (
            _bb_patterns_cache["market_scores"].get(position["market"], 0),
            _bb_patterns_cache["trade_counts"].get(position["market"], 0),
        ),
        reverse=True,
    )
    for target in rescue_candidates:
        market = target["market"]
        analysis = await analyze_mtf_trend_and_target(market)
        if not analysis or not analysis.get("is_rebounding"):
            continue
        if not await has_minimum_rescue_trade_activity(market):
            continue

        balance = target["balance"] + target["locked"]
        avg_price = target["avg_buy_price"]
        try:
            if balance <= 0 or avg_price <= 0:
                continue

            buy_amount = available_for_rescue
        except Exception as e:
            print(f"[RESCUE][ERR] {market} 필요 매수액 계산 실패: {e}")
            continue

        if buy_amount < MIN_ORDER_NOTIONAL_KRW:
            continue

        print(
            f"🚑 [RESCUE-PLAN] {market} 상승 추세 및 거래량 확인! 구출 매수 "
            f"(주문금액: {buy_amount:,.0f}원, "
            f"유효가용KRW: {effective_krw:,.0f}원, 현재 손실률: {target['pnl_percent']}%)"
        )
        try:
            if LIVE_TRADING:
                response = await order_market_buy_price(
                    access_key, secret_key, market, buy_amount
                )
                print(f"🚑 [RESCUE-BUY] {market} 체결 완료 (uuid={response.get('uuid')})")
            else:
                print(f"🚑 [RESCUE-BUY][DRY] {market} 구출 매수: {buy_amount:,.0f}원")

            ps.record_buy(market, buy_amount)
            state = ps.data.setdefault(market, {})
            state["is_rescued"] = True
            state["rescue_buy_price"] = target["current_price"]
            state["rescue_amount"] = buy_amount
            state["rescue_ts"] = now
            used_krw += buy_amount
            if ps.user_no > 0:
                await save_state_to_redis(ps.user_no, ps)
        except Exception as e:
            print(f"[RESCUE][ERR] {market} 구출 매수 실패: {e}")
            continue

        try:
            await asyncio.sleep(0.8)
            await cancel_all_open_orders_for_market(access_key, secret_key, market)
            await asyncio.sleep(0.3)

            accounts = await fetch_upbit_accounts(access_key, secret_key)
            base = market.split("-")[1]
            for account in accounts:
                if account.get("currency") != base:
                    continue

                balance = Decimal(str(account.get("balance", "0")))
                new_avg = Decimal(str(account.get("avg_buy_price", "0")))
                if balance <= 0 or new_avg <= 0:
                    break

                target_exit_price = new_avg * Decimal("1.003")
                _, best_ask = await get_orderbook_top(market)
                limit_price = adjust_price_to_tick(
                    max(target_exit_price, best_ask), side="ask"
                )
                volume = quantize_volume(balance)
                if volume * limit_price >= MIN_ORDER_NOTIONAL_KRW:
                    response = await order_limit_sell(
                        access_key, secret_key, market, volume, limit_price
                    )
                    state["active_limit_uuid"] = response.get("uuid")
                    state["dynamic_tp"] = Decimal("0.3")
                    print(
                        f"🎯 [RESCUE-TP] {market} 탈출용 지정가 배치 완료: "
                        f"{limit_price}원 (신규평단: {new_avg}원)"
                    )
                break
        except Exception as e:
            print(f"[RESCUE-TP][ERR] {market} 탈출 지정가 배치 실패: {e}")

        break

    return used_krw


async def process_rescue_operations(
        access_key: str,
        secret_key: str,
        enriched_positions: List[dict],
        ps: PositionState,
        effective_krw: Decimal,
        total_equity: Decimal,
        btc_status: str
) -> Decimal:
    async with _rescue_buy_lock:
        return await _process_rescue_operations_unlocked(
            access_key, secret_key, enriched_positions, ps, effective_krw, total_equity, btc_status
        )


async def process_strategic_additional_buys(
        access_key: str,
        secret_key: str,
        enriched_positions: List[dict],
        ps: PositionState,
        effective_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if effective_krw < MIN_ORDER_NOTIONAL_KRW or ADDITIONAL_BUY_KRW <= 0:
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

        if ps.data.get(market, {}).get("is_rescued", False):
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
        buy_amt = min(ADDITIONAL_BUY_KRW, cap_remain, effective_krw)
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

            effective_krw -= buy_amt
            used_krw += buy_amt
            if ps.user_no > 0:
                await save_state_to_redis(ps.user_no, ps)

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
        effective_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if remaining_slots <= 0 or effective_krw < MIN_ORDER_NOTIONAL_KRW or RANGE_BUY_KRW <= 0:
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
        await asyncio.sleep(0.08)
        if not analysis or not analysis["is_rebounding"]:
            continue

        buy_amt = min(RANGE_BUY_KRW, effective_krw)
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
            effective_krw -= buy_amt
            used_krw += buy_amt
            buys_done += 1
            if ps.user_no > 0:
                await save_state_to_redis(ps.user_no, ps)

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
    lock = get_market_lock(market)
    if lock.locked():
        return

    async with lock:
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
                        if ps.user_no > 0:
                            await save_state_to_redis(ps.user_no, ps)
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

    ps = PositionState(user_no=user_no)
    print("=== [MTF 1h+15m + BTC 정지 장치 적용] 트레이딩 엔진 가동 ===")
    excluded = await get_excluded_markets(force_reload=True)

    # 초기 보유 코인 관리 등록 및 Redis 상태 복원
    try:
        init_accs = await fetch_upbit_accounts(access_key, secret_key)
        await restore_state_from_redis(user_no, ps, init_accs)

        for acc in init_accs:
            cur = acc.get("currency")
            if not cur or cur == "KRW": continue
            mkt = f"KRW-{cur}"
            if mkt in excluded: continue
            bal = Decimal(str(acc.get("balance", "0"))) + Decimal(str(acc.get("locked", "0")))
            avg = Decimal(str(acc.get("avg_buy_price", "0")))
            if bal > 0 and avg > 0:
                if mkt not in ps.buy_info:
                    ps.buy_info[mkt] = {"total_buys": 1, "total_invested": (bal * avg).quantize(Decimal("0.0001"))}
                st = ps.data.get(mkt)
                if not st:
                    ps.update_or_init(mkt, Decimal("0"), avg)
                else:
                    st["avg_buy_price"] = avg
                st = ps.data.get(mkt, {})
                if not st.get("active_limit_uuid"):
                    asyncio.create_task(after_buy_preplace_dynamic_tp(access_key, secret_key, mkt, ps))
        await save_state_to_redis(user_no, ps)
    except Exception as e:
        print(f"[INIT][WARN] 초기 상태 복원 및 등록 실패: {e}")

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
            total_coin_eval_krw = Decimal("0")
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
                    total_coin_eval_krw += (bal + locked) * cur_price
                    enriched.append({
                        "market": mkt, "balance": bal, "locked": locked,
                        "avg_buy_price": avg, "current_price": cur_price, "pnl_percent": pnl
                    })

            # 총 자산 및 필수 현금 보존 버퍼(기본 25%) 계산
            total_equity = available_krw + total_coin_eval_krw
            cash_buffer_krw = (total_equity * MIN_CASH_BUFFER_RATIO).quantize(Decimal("1000"))
            effective_krw = max(Decimal("0"), available_krw - cash_buffer_krw)

            active_uuids = [(m, st.get("active_limit_uuid")) for m, st in list(ps.data.items()) if
                            st.get("active_limit_uuid")]
            for mkt, uid in active_uuids:
                ord_info = await fetch_upbit_order_state(access_key, secret_key, uid)
                if ord_info and ord_info.get("state") == "done":
                    st = ps.data.get(mkt, {})
                    if st.get("is_rescued", False):
                        ps.last_rescue_exit_ts = loop_now
                        print(f"🎉 [RESCUE-TP-SUCCESS] {mkt} 구출 탈출 성공! (uuid={uid})")
                    else:
                        print(f"🎉 [TP-SUCCESS] {mkt} MTF 목표가 체결 완료! (uuid={uid})")
                    ps.mark_sold(mkt, reason="TP")
                    enriched = [p for p in enriched if p["market"] != mkt]
                    await save_state_to_redis(user_no, ps)

            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            # [보호] 현금 보존 버퍼 도달 알림 (유효 가용금이 부족한 경우 안내)
            if effective_krw < MIN_ORDER_NOTIONAL_KRW and available_krw >= MIN_ORDER_NOTIONAL_KRW:
                if int(loop_now) % 60 < INTERVAL_SECONDS:
                    print(
                        f"🛡️ [CASH-BUFFER] 총자산:{total_equity:,.0f}원 | 가용KRW:{available_krw:,.0f}원 "
                        f"<= 현금보존버퍼:{cash_buffer_krw:,.0f}원 ({MIN_CASH_BUFFER_RATIO*100:.0f}%) "
                        f"→ 신규/물타기/구출 매수 일시 정지"
                    )

            rescue_krw_used = Decimal("0")
            if ENABLE_STOP and btc_status == "NORMAL":
                rescue_krw_used = await process_rescue_operations(
                    access_key, secret_key, enriched, ps, effective_krw, total_equity, btc_status
                )
                effective_krw = max(Decimal("0"), effective_krw - rescue_krw_used)
                available_krw -= rescue_krw_used

            # [A] 매도 판단: 최종 방어 손절 및 고점 대비 이익 보존 트레일링
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                cur_p = pos.get("current_price")
                avg_p = pos.get("avg_buy_price")

                if cur_p is None or avg_p is None or cur_p <= 0 or avg_p <= 0:
                    continue

                st = ps.update_or_init(market, pnl, avg_p)

                # [A-1] 구출 포지션: 구출가 대비 -2.5% 반등 실패 손절 및 타임아웃 청산
                if ENABLE_STOP and st.get("is_rescued", False):
                    rescue_price = st.get("rescue_buy_price")
                    rescue_ts = float(st.get("rescue_ts", 0.0))
                    should_cut_rescue = False
                    rescue_cut_reason = "RESCUE_LOSS_CUT"

                    if rescue_price is not None and rescue_price > 0:
                        rescue_pnl = (
                            (cur_p - rescue_price) / rescue_price * Decimal("100")
                        )
                        if rescue_pnl <= Decimal("-2.5"):
                            should_cut_rescue = True
                            rescue_cut_reason = "RESCUE_LOSS_CUT"
                            print(
                                f"🚨 [RESCUE-FAIL] {market} 구출 매수 후 반등 실패로 "
                                f"전량 손절 (구출가대비: {rescue_pnl:.2f}%)"
                            )

                    # 구출 타임아웃 (1시간 동안 미탈출 시 자금 묶임 방지 강제 청산)
                    if not should_cut_rescue and rescue_ts > 0 and (loop_now - rescue_ts) >= RESCUE_TIMEOUT_SEC:
                        should_cut_rescue = True
                        rescue_cut_reason = "RESCUE_TIMEOUT"
                        print(
                            f"⏰ [RESCUE-TIMEOUT] {market} 구출 후 {RESCUE_TIMEOUT_SEC // 60}분 경과! "
                            f"자금 회수를 위해 전량 청산 (현재 PNL: {pnl}%)"
                        )

                    if should_cut_rescue:
                        await cancel_all_open_orders_for_market(
                            access_key, secret_key, market
                        )
                        await asyncio.sleep(0.3)
                        accounts = await fetch_upbit_accounts(access_key, secret_key)
                        for account in accounts:
                            if account.get("currency") != market.split("-")[1]:
                                continue

                            balance = Decimal(str(account.get("balance", "0")))
                            sell_volume = quantize_volume(balance)
                            if sell_volume and (
                                sell_volume * cur_p
                            ) >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    await order_market_sell(
                                        access_key, secret_key, market, sell_volume
                                    )
                                await register_loss_cut(
                                    user_no, market, sell_volume, cur_p, pnl
                                )
                                ps.mark_sold(market, reason=rescue_cut_reason)
                                ps.last_rescue_exit_ts = loop_now
                                await save_state_to_redis(user_no, ps)
                            break
                        continue

                # [A-2] 일반 포지션 최종 방어 손절:
                # 1) 물타기(2회) 소진 후 평단 대비 HARD_STOP_LOSS_PCT (기본 -7.0%) 이하 도달
                # 2) 또는 구출 한계선(MAX_DROP_LOSS_CUT_PCT, 기본 -22.0%) 이하로 추락하여 구출 불가 시
                elif ENABLE_STOP and not st.get("is_rescued", False):
                    total_buys, _ = ps.get_buy_stats(market)
                    is_water_exhausted = (total_buys - 1 >= MAX_ADDITIONAL_BUYS)
                    limits = await get_dynamic_stop_limits(market, patterns_data=_bb_patterns_cache.get("candidates"))
                    dyn_hard_stop = limits["hard_stop"]
                    dyn_max_drop = limits["max_drop"]

                    is_beyond_hard_stop = (is_water_exhausted and pnl <= dyn_hard_stop)
                    is_beyond_max_drop = (pnl <= dyn_max_drop)

                    if is_beyond_hard_stop or is_beyond_max_drop:
                        cut_reason = f"WATER_EXHAUSTED({dyn_hard_stop}%)" if is_beyond_hard_stop else f"DYNAMIC_MAX_DROP({dyn_max_drop}%)"
                        print(
                            f"🚨 [HARD-STOP-LOSS] {market} 최종 방어 동적손절 발동! "
                            f"(PNL: {pnl:.2f}%, 물타기: {total_buys}회, 사유: {cut_reason})"
                        )
                        await cancel_all_open_orders_for_market(access_key, secret_key, market)
                        await asyncio.sleep(0.3)
                        accounts = await fetch_upbit_accounts(access_key, secret_key)
                        for account in accounts:
                            if account.get("currency") != market.split("-")[1]:
                                continue

                            balance = Decimal(str(account.get("balance", "0")))
                            sell_volume = quantize_volume(balance)
                            if sell_volume and (sell_volume * cur_p) >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    await order_market_sell(access_key, secret_key, market, sell_volume)
                                await register_loss_cut(user_no, market, sell_volume, cur_p, pnl)
                                ps.mark_sold(market, reason=cut_reason)
                                await save_state_to_redis(user_no, ps)
                            break
                        continue

                # [A-3] 고점 대비 이익 보존 매도 (Weak TP)
                raw_tp = st.get("dynamic_tp")
                target_tp = Decimal(str(raw_tp)) if raw_tp is not None else Decimal("1.5")
                peak = st.get("peak_pnl", Decimal("0"))

                # 목표가의 80% 이상 상승(피크) 도달 후, +0.40% 이하로 반락할 때 이익 보존
                if peak >= (target_tp * Decimal("0.8")):
                    if pnl <= Decimal("0.40") and pnl >= Decimal("-0.50"):
                        print(f"✨ [PROFIT-SAVE] {market} 이익 보존 매도 (피크: +{peak}%, 현재 PNL: {pnl}%)")
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
                                    await save_state_to_redis(user_no, ps)
                                break

            # [B] 지지선 반등 확인형 물타기 실행 (BTC EMERGENCY 상태가 아닐 때만 허용)
            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 추가 물타기(물타기 방어)를 중단합니다.")
            else:
                used_krw_add = await process_strategic_additional_buys(access_key, secret_key, enriched, ps,
                                                                       effective_krw)
                effective_krw = max(Decimal("0"), effective_krw - used_krw_add)
                available_krw -= used_krw_add

            # [C] 신규 매수 탐색 (★ BTC 상태 정지 장치 적용)
            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 모든 신규 매수 및 추세 진입을 전면 차단합니다.")
            elif btc_status == "CAUTION":
                print(f"⚠️ {btc_msg} -> 조정세 구간: 신규 매수를 일시 보류합니다.")
            else:
                # [NORMAL 상태일 때만 신규 매수 허용]
                used_krw_new = await process_new_candidate_buys(access_key, secret_key, ps, remaining_slots,
                                                                effective_krw)
                effective_krw = max(Decimal("0"), effective_krw - used_krw_new)
                available_krw -= used_krw_new

            # 30초마다 Redis 상태 주기적 저장
            if loop_now - ps.last_redis_save_ts >= 30:
                await save_state_to_redis(user_no, ps)

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

            if task and task.done():
                exc = task.exception()
                if isinstance(exc, ResetRequested):
                    print("[CTRL] 1일 정기 리셋 완료 → 모니터링 재기동")
                elif exc:
                    print(f"[CTRL] 모니터링 예외 복구 재기동: {exc}")
                task = None

            if cfg:
                apply_dynamic_config(cfg)

            active_flag = str(cfg.get("activeYN", "N")).strip().upper() == "Y"

            if active_flag and task is None:
                init_config(force=True)
                if cfg:
                    apply_dynamic_config(cfg)
                print(
                    f"[CONFIG] 웹 설정 적용 완료 | 활성최대:{MAX_ACTIVE_MARKETS}개, "
                    f"최초금액:{RANGE_BUY_KRW}원, 추가금액:{ADDITIONAL_BUY_KRW}원, "
                    f"종목한도:{MAX_TOTAL_INVEST_PER_MARKET}원"
                )
                task = asyncio.create_task(monitor_positions(user_no, server_no))
            elif not active_flag and task:
                print("[CTRL] activeYN=N → 모니터링 태스크 중단")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                task = None

        except Exception as e:
            print(f"[CTRL][ERR] {type(e).__name__}: {e}")

        await asyncio.sleep(CONTROLLER_POLL_SEC)


async def main():
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청 수신")