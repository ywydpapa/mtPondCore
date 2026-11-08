"""전략 4: 볼린저 패턴과 상승 모멘텀 후보에 진입하며 BTC 상태로 신규 매수를 제어한다.

익절은 사다리 지정가로 관리하고, 방어 시 주문을 재배치한다. 구출 후 기준가에서
2.5% 하락하면 전량 손절한다.
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

PERIODIC_RESET_SEC = int(os.getenv("PERIODIC_RESET_SEC", "86400"))


class ResetRequested(Exception):
    pass


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


# ============================================================
# 4. STATIC CONFIG (기본 상수)
# ============================================================
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))

COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "180"))  # 익절 후 재매수 방지 180초
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "300"))  # 손절 후 재매수 방지 300초

ACTIVE_CAPITAL_RATIO = Decimal("0.60")  # 총 자산의 60% 한도 투입
DEFENSE_TRIGGER_PCT = _decimal_env("DEFENSE_TRIGGER_PCT", "-1.4")  # 방어 틱 사다리 재배치 (-1.4%)
_rescue_buy_lock = asyncio.Lock()

# [하드 손절 및 자금 안전망 설정]
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")        # 방어 실패 후 하드 손절
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")  # 구출 불가 대폭락 강제 손절
MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.25")  # 순자산 대비 최소 현금 예비금(25%)

# [구출 작전 통제 설정]
MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15") # 1회 구출 예산 상한 (순자산의 15%)
MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")     # 1회 구출 절대 상한액 (8만원)
RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600")) # 구출 종료 후 전역 쿨다운 (10분)
RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))                 # 구출 탈출 최대 대기 (1시간)

# 마켓별 동시성 락
_market_locks: Dict[str, asyncio.Lock] = {}

def get_market_lock(market: str) -> asyncio.Lock:
    if market not in _market_locks:
        _market_locks[market] = asyncio.Lock()
    return _market_locks[market]

MIN_SLOT_NOTIONAL_KRW = Decimal("10000")  # 1슬롯 기본 10,000원
LADDER_START_PCT = Decimal("0.60")  # 첫 익절 배치 시작선 (+0.60%)
LADDER_STEP_PCT = Decimal("0.30")  # 단계별 간격 (+0.30%)

INTERVAL_SECONDS = 3
MIN_NOTIONAL_KRW = Decimal("5000")

# ============================================================
# 5. RUNTIME CONFIG (Redis 우선 동적 설정 관리)
# ============================================================
_CONFIG_INITIALIZED = False
MAX_ACTIVE_MARKETS = 2
RANGE_BUY_KRW = Decimal("40000")  # 초기 폴백값 (Redis 로드 시 즉시 덮어씌워짐)
ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")
BB_MIN_OVERALL_SCORE = 3


class PositionState:
    def __init__(self, user_no: int = 0):
        self.user_no = user_no
        self.data: Dict[str, Dict[str, Any]] = {}
        self.sell_history: Dict[str, Dict[str, Any]] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}
        self.last_rescue_exit_ts: float = 0.0

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
                "last_update_ts": now,
                "is_rescued": False,
                "rescue_buy_price": None,
                "rescue_start_ts": None
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
        r = reason.upper()
        is_tp = r in ("TP", "HARD_TP", "TP_DONE", "DYNAMIC_TP")
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
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._set_redis_cooldown(market, cooldown_sec))
        except RuntimeError:
            pass
        print(f"[COOLDOWN] {market} 사이클 정리 완료 [{tag}: {reason}] → 재매수 금지 {cooldown_sec}초 적용")

    async def _set_redis_cooldown(self, market: str, cooldown_sec: int):
        if self.user_no > 0:
            try:
                redis_key = f"mtpond:cooldown:{self.user_no}:{market}"
                await redis_client.set(redis_key, "1", ex=cooldown_sec)
            except Exception:
                pass

    async def recently_sold(self, market: str) -> bool:
        now = time.time()
        hist = self.sell_history.get(market)
        if hist is not None:
            if now < hist["expire_ts"]:
                return True
            else:
                del self.sell_history[market]
                return False

        if self.user_no > 0:
            try:
                redis_key = f"mtpond:cooldown:{self.user_no}:{market}"
                val = await redis_client.get(redis_key)
                if val:
                    ttl = await redis_client.ttl(redis_key)
                    if ttl > 0:
                        self.sell_history[market] = {
                            "ts": now,
                            "expire_ts": now + ttl,
                            "reason": "RESTORED",
                            "tag": "복구된쿨다운"
                        }
                    return True
            except Exception:
                pass

        return False

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
    key = f"mtpond:state:opt4:{user_no}"
    try:
        now = time.time()
        payload = {
            "saved_ts": now,
            "last_rescue_exit_ts": getattr(ps, "last_rescue_exit_ts", 0.0),
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
            "sell_history": {
                m: {
                    "ts": v.get("ts", 0),
                    "expire_ts": v.get("expire_ts", 0),
                    "reason": v.get("reason", ""),
                    "tag": v.get("tag", "")
                }
                for m, v in ps.sell_history.items()
                if v.get("expire_ts", 0) > now
            }
        }
        await redis_client.set(key, json.dumps(payload), ex=86400 * 7)
    except Exception as e:
        print(f"[STATE][WARN] Redis 상태 저장 실패: {e}")


async def restore_state_from_redis(user_no: int, ps: PositionState, raw_accounts: List[dict]):
    """시스템 재시작 또는 정기 리셋 시 Redis에서 포지션 상태를 복원합니다."""
    if user_no <= 0:
        return
    key = f"mtpond:state:opt4:{user_no}"
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
                    if dk in ("peak_pnl", "avg_buy_price", "rescue_buy_price"):
                        restored_item[dk] = Decimal(str(dv)) if dv is not None else None
                    else:
                        restored_item[dk] = dv
                ps.data[m] = restored_item

        now = time.time()
        for m, v in cached.get("sell_history", {}).items():
            exp = float(v.get("expire_ts", 0))
            if exp > now:
                ps.sell_history[m] = v

        if restored_positions > 0:
            print(f"[STATE] Redis에서 이전 포지션 상태 {restored_positions}개 복원 완료 (마지막 구출 종료: {int(now - ps.last_rescue_exit_ts)}초 전)")
    except Exception as e:
        print(f"[STATE][WARN] Redis 상태 복원 실패: {e}")


def init_config(force: bool = False):
    """환경변수 기반 1차 초기화 (Redis 연결 전 기본 틀 마련)"""
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M
    global ENABLE_STOP, ADDITIONAL_BUY_KRW
    global HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT, DEFENSE_TRIGGER_PCT
    global MIN_CASH_BUFFER_RATIO, MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW
    global RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    RANGE_BUY_KRW = _decimal_env("RANGE_BUY_KRW", "40000")
    default_markets = 5 if RANGE_BUY_KRW <= Decimal("25000") else 2
    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", str(default_markets)))
    ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")
    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = get_bb_min_trade_count_10m()
    ENABLE_STOP = os.getenv("ENABLE_STOP", "1").strip().upper() in ("1", "Y", "TRUE")

    HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")
    MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")
    DEFENSE_TRIGGER_PCT = _decimal_env("DEFENSE_TRIGGER_PCT", "-1.4")
    MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.25")
    MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15")
    MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")
    RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600"))
    RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))

    _CONFIG_INITIALIZED = True
    print(f"[INIT] 시스템 1차 설정 완료 | 종목당 초기할당: {RANGE_BUY_KRW:,.0f}원, 활성최대: {MAX_ACTIVE_MARKETS}개")


def apply_dynamic_config(cfg: dict):
    """★ Redis 설정값을 전역 변수에 최우선 반영"""
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M
    global ENABLE_STOP, ADDITIONAL_BUY_KRW
    global HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT, DEFENSE_TRIGGER_PCT
    global MIN_CASH_BUFFER_RATIO, MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW
    global RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    changes = []

    def to_dec(v):
        try:
            return Decimal(str(v)) if v not in (None, "") else None
        except:
            return None

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).strip().upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

    # 1. 최초 투입 금액 우선 반영
    for key in ("initAmt", "range_buy_krw", "RANGE_BUY_KRW", "firstBuyAmt", "init_amt"):
        if key in cfg:
            v = to_dec(cfg.get(key))
            if v and v > 0 and v != RANGE_BUY_KRW:
                changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW:,.0f} -> {v:,.0f}")
                RANGE_BUY_KRW = v
                break

    if "addAmt" in cfg:
        value = to_dec(cfg.get("addAmt"))
        if value is not None and value.is_finite() and value >= 0 and value != ADDITIONAL_BUY_KRW:
            changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW:,.0f} -> {value:,.0f}")
            ADDITIONAL_BUY_KRW = value

    # 2. 최대 종목 수 반영
    has_custom_cnt = False
    for key in ("maxCoincnt", "max_active_markets", "MAX_ACTIVE_MARKETS"):
        if key in cfg:
            try:
                nv = int(cfg[key])
                if nv > 0 and nv != MAX_ACTIVE_MARKETS:
                    changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {nv}")
                    MAX_ACTIVE_MARKETS = nv
                    has_custom_cnt = True
                    break
            except:
                pass

    if not has_custom_cnt:
        target_default = 5 if RANGE_BUY_KRW <= Decimal("25000") else 2
        if MAX_ACTIVE_MARKETS != target_default:
            MAX_ACTIVE_MARKETS = target_default

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

    if "defenseTriggerPct" in cfg:
        dtp = to_dec(cfg.get("defenseTriggerPct"))
        if dtp is not None:
            if dtp > 0: dtp = -abs(dtp)
            if dtp != DEFENSE_TRIGGER_PCT:
                changes.append(f"DEFENSE_TRIGGER_PCT {DEFENSE_TRIGGER_PCT} -> {dtp}")
                DEFENSE_TRIGGER_PCT = dtp

    if "minCashBuffer" in cfg:
        mcb = to_dec(cfg.get("minCashBuffer"))
        if mcb is not None and mcb >= 0 and mcb != MIN_CASH_BUFFER_RATIO:
            changes.append(f"MIN_CASH_BUFFER_RATIO {MIN_CASH_BUFFER_RATIO} -> {mcb}")
            MIN_CASH_BUFFER_RATIO = mcb

    if "rescueBudgetRatio" in cfg:
        rbr = to_dec(cfg.get("rescueBudgetRatio"))
        if rbr is not None and rbr > 0 and rbr != MAX_RESCUE_BUDGET_RATIO:
            changes.append(f"MAX_RESCUE_BUDGET_RATIO {MAX_RESCUE_BUDGET_RATIO} -> {rbr}")
            MAX_RESCUE_BUDGET_RATIO = rbr

    if "rescueBudgetMaxKrw" in cfg:
        rbm = to_dec(cfg.get("rescueBudgetMaxKrw"))
        if rbm is not None and rbm > 0 and rbm != MAX_RESCUE_BUDGET_KRW:
            changes.append(f"MAX_RESCUE_BUDGET_KRW {MAX_RESCUE_BUDGET_KRW} -> {rbm}")
            MAX_RESCUE_BUDGET_KRW = rbm

    if "rescueGlobalCooldown" in cfg:
        try:
            rgc = int(cfg.get("rescueGlobalCooldown"))
            if rgc >= 0 and rgc != RESCUE_GLOBAL_COOLDOWN_SEC:
                changes.append(f"RESCUE_GLOBAL_COOLDOWN_SEC {RESCUE_GLOBAL_COOLDOWN_SEC} -> {rgc}")
                RESCUE_GLOBAL_COOLDOWN_SEC = rgc
        except Exception:
            pass

    if "rescueTimeout" in cfg:
        try:
            rto = int(cfg.get("rescueTimeout"))
            if rto > 0 and rto != RESCUE_TIMEOUT_SEC:
                changes.append(f"RESCUE_TIMEOUT_SEC {RESCUE_TIMEOUT_SEC} -> {rto}")
                RESCUE_TIMEOUT_SEC = rto
        except Exception:
            pass

    if changes:
        print("[CFG] 동적 설정 업데이트 반영:\n  " + "\n  ".join(changes))


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
    client = get_http_client()
    for attempt in range(1, max_retry + 1):
        token = build_upbit_jwt_with_params(access_key, secret_key, params) if (
                params or method.upper() != "GET") else build_upbit_jwt_simple(access_key, secret_key)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
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
    client = get_http_client()
    r = await client.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


async def register_loss_cut(
        user_no: int, market: str, quantity: Decimal, price: Decimal, loss_rate: Decimal):
    await _register_loss_cut(
        API_BASE, user_no, market, quantity, price, loss_rate
    )


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
    client = get_http_client()
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
        client = get_http_client()
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
# 8.1. 비트코인 3단계 방어 체력 진단 함수 (★ 정지 장치)
# ============================================================
_btc_health_cache: Dict[str, Any] = {
    "ts": 0.0,
    "result": {"status": "NORMAL", "btc_pnl_1h": 0.0, "msg": "초기화 대기"}
}
BTC_HEALTH_CACHE_TTL_SEC = 30.0


async def check_btc_market_health() -> Dict[str, Any]:
    """
    비트코인(KRW-BTC)의 1시간/4시간 캔들을 정밀 분석하여 시장 전체의 위험도 판별 (30초 캐싱)
    반환: {"status": "NORMAL" / "CAUTION" / "EMERGENCY", "btc_pnl_1h": 변동률, "msg": 상태메시지}
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

        cur_price = Decimal(str(candles[0]["trade_price"]))
        prev_1h_price = Decimal(str(candles[1]["trade_price"]))
        prev_4h_price = Decimal(str(candles[4]["trade_price"]))

        closes = [Decimal(str(c["trade_price"])) for c in candles]
        ma20 = sum(closes[:20]) / Decimal("20")

        pnl_1h = float(((cur_price - prev_1h_price) / prev_1h_price) * 100)
        pnl_4h = float(((cur_price - prev_4h_price) / prev_4h_price) * 100)

        # 1. 비상 상태 (EMERGENCY): 1시간 내 -1.8% 이상 급락 또는 4시간 내 -3.5% 이상 하락 또는 MA20 이탈 심화
        if pnl_1h <= -1.8 or pnl_4h <= -3.5 or cur_price < (ma20 * Decimal("0.985")):
            res_dict = {
                "status": "EMERGENCY",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"🚨 [BTC-EMERGENCY] 급락 감지! (1시간: {pnl_1h:+.2f}%, 4시간: {pnl_4h:+.2f}%, 현재가 < MA20)"
            }
        # 2. 주의 상태 (CAUTION): 1시간 내 -0.8% ~ -1.8% 조정 중 또는 MA20 하회
        elif pnl_1h <= -0.8 or cur_price < ma20:
            res_dict = {
                "status": "CAUTION",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"⚠️ [BTC-CAUTION] 조정세 감지 (1시간: {pnl_1h:+.2f}%, MA20 근접/하회)"
            }
        else:
            res_dict = {
                "status": "NORMAL",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": "✅ [BTC-NORMAL] 안정적 추세 유지"
            }
        _btc_health_cache["ts"] = now
        _btc_health_cache["result"] = res_dict
        return res_dict
    except Exception as e:
        return _btc_health_cache["result"]


# ============================================================
# 9. 상승 모멘텀 검증 (15분봉 정배열 & 실시간 양봉)
# ============================================================
async def check_bullish_momentum(market: str) -> bool:
    url_15m = "https://api.upbit.com/v1/candles/minutes/15"
    params = {"market": market, "count": 20}
    try:
        client = get_http_client()
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
_bb_patterns_cache: Dict[str, Any] = {
    "fetched_ts": 0.0,
    "candidates": [],
    "market_scores": {},
    "trade_counts": {},
}


async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if (not force) and (now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC) and _bb_patterns_cache["candidates"]:
        return _bb_patterns_cache["candidates"]

    try:
        client = get_http_client()
        resp = await client.get(BB_PATTERNS_API_URL)
        if resp.status_code != 200: return _bb_patterns_cache["candidates"]
        data = resp.json()
    except Exception:
        return _bb_patterns_cache["candidates"]

    coins = data.get("coins", [])
    if not isinstance(coins, list): return _bb_patterns_cache["candidates"]

    excluded = await get_excluded_markets()
    qualified = []
    market_scores = {}
    trade_counts = {}

    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-"): continue
        try:
            score = int(c.get("overall_score", 0))
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
        except:
            continue
        market_scores[mkt] = score
        trade_counts[mkt] = trades
        if mkt in excluded: continue

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
    _bb_patterns_cache["market_scores"] = market_scores
    _bb_patterns_cache["trade_counts"] = trade_counts
    return qualified


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


async def check_rescue_rebound_signal(market: str) -> bool:
    try:
        client = get_http_client()
        response = await client.get(
            "https://api.upbit.com/v1/candles/minutes/15",
            params={"market": market, "count": 10}
        )
        if response.status_code != 200:
            return False
        candles = response.json()
        if len(candles) < 2:
            return False
        current_close = Decimal(str(candles[0]["trade_price"]))
        current_open = Decimal(str(candles[0]["opening_price"]))
        current_low = Decimal(str(candles[0]["low_price"]))
        previous_close = Decimal(str(candles[1]["trade_price"]))
        previous_low = Decimal(str(candles[1]["low_price"]))
        return current_close > current_open and (
            current_low >= previous_low or current_close >= previous_close
        )
    except Exception as exc:
        print(f"[RESCUE-CHECK][ERR] {market} 15분봉 반등 확인 실패: {exc}")
        return False


async def _process_rescue_operations_unlocked(
        access_key: str,
        secret_key: str,
        positions: List[dict],
        ps: PositionState,
        available_krw: Decimal,
        total_equity: Decimal = Decimal("0")
) -> Decimal:
    if available_krw < MIN_ORDER_NOTIONAL_KRW or sum(
            1 for state in ps.data.values() if state.get("is_rescued", False)
    ) >= MAX_CONCURRENT_RESCUE:
        return Decimal("0")

    # 1. 구출 종료 후 전역 쿨다운 검사
    loop_now = time.time()
    if (loop_now - ps.last_rescue_exit_ts) < RESCUE_GLOBAL_COOLDOWN_SEC:
        return Decimal("0")

    # 2. 비트코인 시장 건전성 확인 (NORMAL 상태일 때만 구출 개시)
    btc_health = await check_btc_market_health()
    if btc_health.get("status") != "NORMAL":
        return Decimal("0")

    await fetch_bb_patterns_candidates()
    ranked_positions = sorted(
        positions,
        key=lambda item: (
            _bb_patterns_cache["market_scores"].get(item["market"], 0),
            _bb_patterns_cache["trade_counts"].get(item["market"], 0),
        ),
        reverse=True,
    )
    for pos in ranked_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        if pnl <= RESCUE_MAX_DROP or pnl > RESCUE_MIN_DROP:
            continue
        if _bb_patterns_cache["trade_counts"].get(market, 0) < BB_MIN_TRADE_COUNT_10M:
            continue
        state = ps.update_or_init(market, pnl, pos["avg_buy_price"])
        if state.get("is_rescued", False) or not await check_rescue_rebound_signal(market):
            continue

        balance = pos["balance"] + pos["locked"]
        avg_price = pos["avg_buy_price"]
        try:
            _, buy_price = await get_orderbook_top(market)
            target_avg = avg_price / Decimal("1.003")
            if balance <= 0 or buy_price <= 0 or buy_price >= target_avg:
                continue

            # 3. 구출 예산 상한 통제 (순자산 15% 및 절대 상한 8만원)
            equity_budget = (total_equity * MAX_RESCUE_BUDGET_RATIO) if total_equity > 0 else ADDITIONAL_BUY_KRW
            rescue_budget_cap = min(ADDITIONAL_BUY_KRW, MAX_RESCUE_BUDGET_KRW, equity_budget)
            buy_amount = min(rescue_budget_cap, available_krw).to_integral_value(
                rounding=ROUND_FLOOR
            )
        except Exception as exc:
            print(f"[RESCUE][ERR] {market} 매수액 계산 실패: {exc}")
            continue
        if buy_amount < MIN_ORDER_NOTIONAL_KRW:
            print(f"[RESCUE-SKIP] {market} 구출 매수액 {buy_amount:,.0f}원 "
                  f"(addAmt {ADDITIONAL_BUY_KRW:,.0f}원, 한도 {rescue_budget_cap:,.0f}원, 가용 {available_krw:,.0f}원) "
                  f"최소주문액 {MIN_ORDER_NOTIONAL_KRW:,.0f}원 미만")
            continue

        async with get_market_lock(market):
            buy_completed = False
            try:
                response = await order_market_buy_price(
                    access_key, secret_key, market, buy_amount
                ) if LIVE_TRADING else {}
                buy_completed = True
                state["is_rescued"] = True
                state["rescue_buy_price"] = pos["current_price"]
                state["rescue_start_ts"] = time.time()
                print(f"[RESCUE-BUY] {market} 구출 매수 {buy_amount:,.0f}원 "
                      f"(uuid={response.get('uuid')})")
                await asyncio.sleep(0.8)
                await cancel_all_open_orders_for_market(access_key, secret_key, market)
                await asyncio.sleep(0.3)
                accounts = await fetch_upbit_accounts(access_key, secret_key)
                account = next(
                    (item for item in accounts if item.get("currency") == market.split("-")[1]),
                    None
                )
                if account:
                    new_balance = Decimal(str(account.get("balance", "0")))
                    new_avg = Decimal(str(account.get("avg_buy_price", "0")))
                    if new_balance > 0 and new_avg > 0:
                        state["avg_buy_price"] = new_avg
                        _, best_ask = await get_orderbook_top(market)
                        limit_price = adjust_price_to_tick(
                            max(new_avg * Decimal("1.003"), best_ask), side="ask"
                        )
                        volume = quantize_volume(new_balance)
                        if volume * limit_price >= MIN_ORDER_NOTIONAL_KRW:
                            await order_limit_sell(
                                access_key, secret_key, market, volume, limit_price
                            )
                await save_state_to_redis(ps.user_no, ps)
                return buy_amount
            except Exception as exc:
                print(f"[RESCUE][ERR] {market} 구출 매수/탈출 주문 실패: {exc}")
                if buy_completed:
                    await save_state_to_redis(ps.user_no, ps)
                    return buy_amount
    return Decimal("0")


async def process_rescue_operations(
        access_key: str,
        secret_key: str,
        positions: List[dict],
        ps: PositionState,
        available_krw: Decimal,
        total_equity: Decimal = Decimal("0")
) -> Decimal:
    async with _rescue_buy_lock:
        return await _process_rescue_operations_unlocked(
            access_key, secret_key, positions, ps, available_krw, total_equity
        )


# ============================================================
# 12. [가격대별 맞춤] 동적 사다리 슬롯 및 익절선 계산
# ============================================================
def calculate_ladder_slots(total_eval: Decimal, avg_price: Decimal) -> Tuple[int, Decimal, Decimal]:
    """
    코인 단가(avg_price)와 총 투자평가액(total_eval)에 맞춰
    (슬롯 개수, 시작 익절%, 스텝 증가폭%)을 최적으로 산출
    """
    amt = float(total_eval)
    p = float(avg_price)

    # 1. 코인 단가에 따른 동적 시작선 및 간격 스케일링
    if p >= 100_000:  # 10만 원 이상 대형 코인 (BTC, BCH, SOL 등)
        base_start = Decimal("0.25")
        base_step = Decimal("0.12")
    elif p >= 10_000:  # 1만 ~ 10만 원 미만 중형 코인 (NMR 등)
        base_start = Decimal("0.35")
        base_step = Decimal("0.18")
    elif p >= 1_000:  # 1천 ~ 1만 원 미만
        base_start = Decimal("0.45")
        base_step = Decimal("0.25")
    else:  # 1천 원 미만 동전주 (GTC, TIA 등)
        base_start = Decimal("0.60")
        base_step = Decimal("0.30")

    # 2. 투자금 규모에 따른 슬롯 분할
    if amt <= 55_000:  # [4만 ~ 5만 원]: 1만 원 단위 4~5슬롯
        slot_count = max(1, int(total_eval // Decimal("10000")))
    elif amt <= 155_000:  # [6만 ~ 15만 원]: 5~10슬롯
        slot_count = min(10, max(5, int(total_eval // Decimal("10000"))))
    elif amt <= 305_000:  # [16만 ~ 30만 원]: 2만 원 단위 격상
        slot_count = min(10, max(6, int(total_eval // Decimal("20000"))))
    else:  # [31만 ~ 50만 원]: 4만 원 단위
        slot_count = min(10, max(8, int(total_eval // Decimal("40000"))))

    return max(1, slot_count), base_start, base_step


async def place_ladder_tp_orders(access_key: str, secret_key: str, market: str, ps: PositionState):
    st = ps.data.setdefault(market, {})
    if st.get("is_processing_order"):
        return
    st["is_processing_order"] = True

    try:
        async with get_market_lock(market):
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
            await save_state_to_redis(ps.user_no, ps)

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
        async with get_market_lock(market):
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
            await save_state_to_redis(ps.user_no, ps)

    except Exception as e:
        print(f"[DEFENSE][ERR] {market} 방어 재배치 실패: {e}")
    finally:
        st["is_processing_order"] = False


# ============================================================
# 14. 메인 모니터링 루프 (비트코인 3단계 정지 장치 통합)
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()
    last_cfg_sync = time.time()
    last_save_ts = time.time()

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

    ps = PositionState(user_no=user_no)

    # [상태 복구] Redis에서 이전 포지션 및 쿨다운 상태 복원
    try:
        raw_boot_accs = await fetch_upbit_accounts(access_key, secret_key)
        await restore_state_from_redis(user_no, ps, raw_boot_accs)
        await save_state_to_redis(user_no, ps)
    except Exception as e:
        print(f"[BOOT][WARN] Redis 상태 복구 실패: {e}")

    print(f"=== [단기 모멘텀 스캘핑 + BTC 정지 장치 적용] 종목당 투입예산: {RANGE_BUY_KRW:,.0f}원 | 최대종목: {MAX_ACTIVE_MARKETS}개 ===")
    excluded = await get_excluded_markets(force_reload=True)

    while True:
        try:
            loop_now = time.time()

            # 주기적 Redis 설정 재동기화
            if loop_now - last_cfg_sync >= CONTROLLER_POLL_SEC:
                last_cfg_sync = loop_now
                await sync_redis_config(user_no)

            # 주기적 Redis 상태 저장 (30초 주기)
            if loop_now - last_save_ts >= 30.0:
                last_save_ts = loop_now
                await save_state_to_redis(user_no, ps)

            # ★ 비트코인 3단계 방어 체력 진단 호출 (정지 장치)
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]
            btc_msg = btc_health["msg"]

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

            # [자금 안전망] 순자산 25% 현금 안전 버퍼 보호
            cash_buffer_krw = total_equity * MIN_CASH_BUFFER_RATIO
            effective_krw = max(Decimal("0"), available_krw - cash_buffer_krw)

            active_capital_limit = total_equity * ACTIVE_CAPITAL_RATIO
            current_invested = total_coin_val
            available_invest_krw = max(Decimal("0"), active_capital_limit - current_invested)
            effective_invest_krw = min(effective_krw, available_invest_krw)

            # [구출 작전] STOP 활성 + BTC가 EMERGENCY가 아닐 때 (순자산 15% 및 8만원 한도, 전역 쿨다운 적용)
            if ENABLE_STOP and btc_status != "EMERGENCY":
                rescue_used = await process_rescue_operations(
                    access_key, secret_key, enriched, ps, effective_krw, total_equity
                )
                available_krw -= rescue_used
                effective_krw = max(Decimal("0"), effective_krw - rescue_used)
                effective_invest_krw = max(Decimal("0"), effective_invest_krw - rescue_used)

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

                # A. 구출된 종목 관리
                if st.get("is_rescued", False):
                    rescue_price = st.get("rescue_buy_price")
                    rescue_start_ts = st.get("rescue_start_ts", 0.0)
                    is_rescue_drop = bool(ENABLE_STOP and rescue_price and cur_p <= rescue_price * Decimal("0.975"))
                    is_rescue_timeout = bool(ENABLE_STOP and rescue_start_ts > 0 and (loop_now - rescue_start_ts) >= RESCUE_TIMEOUT_SEC)

                    if is_rescue_drop or is_rescue_timeout:
                        reason_msg = "구출 후 2.5% 하락" if is_rescue_drop else f"구출 후 {RESCUE_TIMEOUT_SEC}초 타임아웃 초과"
                        print(f"🚨 [RESCUE-FAIL] {market} {reason_msg}, 전량 시장가 손절")
                        async with get_market_lock(market):
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
                                        await register_loss_cut(user_no, market, s_vol, cur_p, pnl)
                                        ps.last_rescue_exit_ts = loop_now
                                        ps.mark_sold(market, reason="RESCUE_LOSS_CUT" if is_rescue_drop else "RESCUE_TIMEOUT")
                                        await save_state_to_redis(user_no, ps)
                                    break
                    continue

                # B. 일반 포지션 관리

                # 1) 하드 손절 / 대폭락 강제 손절 검사 (코인별 30분봉 동적 진동폭 연동)
                limits = await get_dynamic_stop_limits(market, patterns_data=_bb_patterns_cache.get("candidates"), client=get_http_client())
                dyn_hard_stop = limits["hard_stop"]
                dyn_max_drop = limits["max_drop"]

                is_hard_stop = bool(ENABLE_STOP and st.get("defense_mode") and pnl <= dyn_hard_stop)
                is_max_drop = bool(ENABLE_STOP and pnl <= dyn_max_drop)

                if is_hard_stop or is_max_drop:
                    cut_reason = f"방어실패 동적손절({dyn_hard_stop}%)" if is_hard_stop else f"대폭락 동적안전망({dyn_max_drop}%)"
                    print(f"🚨 [HARD-STOP] {market} {cut_reason} 발동 (PnL: {pnl}%), 전량 시장가 청산")
                    async with get_market_lock(market):
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
                                    await register_loss_cut(user_no, market, s_vol, cur_p, pnl)
                                    ps.mark_sold(market, reason="HARD_STOP_LOSS" if is_hard_stop else "MAX_DROP_LOSS_CUT")
                                    await save_state_to_redis(user_no, ps)
                                break
                    continue

                # 2) 방어형 틱 사다리 진입 검사 (★ 기존 미호출 버그 정상 연결)
                if ENABLE_STOP and pnl <= DEFENSE_TRIGGER_PCT and not st.get("defense_mode") and not st.get("is_processing_order"):
                    print(f"🛡️ [DEFENSE-TRIGGER] {market} 손실률 {pnl}% <= 트리거선 {DEFENSE_TRIGGER_PCT}% -> 방어 호가 탈출 사다리 발주")
                    asyncio.create_task(place_defense_ladder_orders(access_key, secret_key, market, ps))
                    await save_state_to_redis(user_no, ps)
                    continue

                # 3) 잔여 수량 익절 사다리 재배치 (방어 모드가 아닐 때만)
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
                    await save_state_to_redis(user_no, ps)

            # 신규 매수 탐색 (★ BTC 상태 정지 장치 적용)
            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 모든 신규 매수 및 추세 진입을 전면 차단합니다.")
            elif btc_status == "CAUTION":
                print(f"⚠️ {btc_msg} -> 조정세 구간: 신규 매수를 일시 보류합니다.")
            else:
                # [NORMAL 상태일 때만 신규 매수 허용]
                if remaining_slots > 0 and effective_krw >= MIN_ORDER_NOTIONAL_KRW and effective_invest_krw >= MIN_ORDER_NOTIONAL_KRW:
                    candidates = await fetch_bb_patterns_candidates()
                    for item in candidates:
                        if remaining_slots <= 0:
                            break

                        market = item["market"]
                        if market in [p["market"] for p in enriched] or await ps.recently_sold(market):
                            continue

                        if not await check_bullish_momentum(market):
                            continue

                        buy_amt = min(RANGE_BUY_KRW, effective_krw, effective_invest_krw)
                        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                            continue

                        async with get_market_lock(market):
                            try:
                                if LIVE_TRADING:
                                    resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                                    print(f"🚀 [BUY] {market} 모멘텀 진입: {buy_amt:,.0f}원 (uuid={resp.get('uuid')})")
                                else:
                                    print(f"🚀 [BUY][DRY] {market} 모멘텀 진입: {buy_amt:,.0f}원")

                                ps.record_buy(market, buy_amt, item["price"])
                                ps.update_or_init(market, Decimal("0"), item["price"])
                                asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market, ps))
                                await save_state_to_redis(user_no, ps)

                                available_krw -= buy_amt
                                effective_krw -= buy_amt
                                effective_invest_krw -= buy_amt
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
                    cli = get_http_client()
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
    try:
        init_config(force=True)
        user_no = int(os.getenv("USER_NO", "100001"))
        server_no = int(os.getenv("SERVER_NO", "21"))

        # ★ 프로그램 진입 즉시 Redis 설정을 1차 동기화
        await sync_redis_config(user_no)

        await run_mtpond_controller(user_no, server_no)
    finally:
        await close_http_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청 수신")