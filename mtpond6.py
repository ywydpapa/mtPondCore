"""전략 6: 초기 20%, 물타기 40%, 돌파 10%, 현금 예비금 30%의 예산 버킷을 쓴다.

추세 진입과 횡보 그리드를 함께 운용하고 포지션당 돌파 매수를 제한한다.
추세·그리드는 지정가 익절, 구출 포지션은 기준가 대비 -2.5%에서 전량 손절한다.
일반 추세/그리드 포지션은 하드 손절(-7.0%) 및 최대 낙폭(-22.0%) 손절 안전망을 가동한다.
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
import httpx
import jwt
from sqlalchemy import text
from mtpond_common import (
    market_buy,
    quantize_volume,
    register_loss_cut as _register_loss_cut,
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
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "5"))

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
# 3. STATIC CONFIG (20% / 40% / 10% / 30% 자금 룰 & API 설정)
# ============================================================
BB_CACHE_TTL_SEC = 60  # 외부 분석 서버 캐시 주기 60초
ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")

# [하드 손절 및 자금 안전망 설정]
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")          # 추세/그리드 하드 손절 (-7.0%)
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")    # 대폭락 강제 손절 안전망 (-22.0%)

RATIO_INITIAL = Decimal("0.20")
RATIO_AVERAGING = Decimal("0.40")
RATIO_BREAKOUT = Decimal("0.10")
RATIO_CASH_RESERVE = Decimal("0.30")
BUY_FEE_BUFFER_RATIO = Decimal("0.01")
BUY_AMOUNT_UNIT_KRW = Decimal("10000")

# [구출 작전 통제 설정]
MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15") # 1회 구출 예산 상한 (순자산의 15%)
MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")     # 1회 구출 절대 상한액 (8만원)
RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600")) # 구출 종료 후 전역 쿨다운 (10분)
RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))                 # 구출 탈출 최대 대기 (1시간)
_last_rescue_completion_ts: float = 0.0

GRID_STEP_COUNT = 4  # 4단계 매수 그물망
GRID_INTERVAL_PCT = Decimal("0.5")  # -0.5% 간격
GRID_TARGET_TP_PCT = Decimal("0.5")  # +0.5% 합산 익절
_rescue_buy_lock = asyncio.Lock()

COOLDOWN_TP_SEC = 180
COOLDOWN_DEFENSE_SEC = 300


# ============================================================
# 4. 동적 설정 & 제외 코인 관리
# ============================================================
def to_dec(val: Any) -> Optional[Decimal]:
    if val is None: return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def apply_dynamic_config(cfg: Dict[str, Any]):
    global ENABLE_STOP, ADDITIONAL_BUY_KRW, HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT
    global MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW, RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    changes = []
    if "stopYN" in cfg:
        stop_enabled = str(cfg.get("stopYN")).strip().upper() in ("1", "Y", "TRUE")
        if stop_enabled != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {stop_enabled}")
            ENABLE_STOP = stop_enabled

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
# 5. 동적 포트폴리오 스케일링 계산기
# ============================================================
DB_SETUP_CACHE_TTL_SEC = 30
_db_setup_cache: Dict[int, Tuple[float, Dict[str, Any]]] = {}


async def fetch_database_setup(user_no: int) -> Dict[str, Any]:
    global ADDITIONAL_BUY_KRW
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
        raise ValueError("DB 매매 설정 응답이 객체가 아닙니다.")

    decimal_fields = ("limitAmt",)
    parsed: Dict[str, Any] = {}
    for key in decimal_fields:
        try:
            parsed[key] = Decimal(str(setup[key]))
        except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
            raise ValueError(f"DB 매매 설정의 {key} 값이 없거나 올바르지 않습니다.") from exc
    try:
        parsed["maxCoincnt"] = int(setup["maxCoincnt"])
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("DB 매매 설정의 maxCoincnt 값이 없거나 올바르지 않습니다.") from exc
    parsed["activeYN"] = str(setup.get("activeYN", "N")).strip().upper()
    if "stopYN" in setup:
        parsed["stopYN"] = str(setup["stopYN"]).strip().upper()
    if "exCoinlist" in setup:
        parsed["exCoinlist"] = str(setup.get("exCoinlist", "")).strip()
    if "addAmt" in setup:
        try:
            parsed["addAmt"] = Decimal(str(setup["addAmt"]))
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise ValueError("DB 매매 설정의 addAmt 값이 올바르지 않습니다.") from exc

    if any(not parsed[key].is_finite() for key in decimal_fields):
        raise ValueError("DB 매매 설정의 금액은 유한한 숫자여야 합니다.")
    if parsed["limitAmt"] <= 0:
        raise ValueError("DB limitAmt는 0보다 커야 합니다.")
    if parsed["maxCoincnt"] <= 0:
        raise ValueError("DB maxCoincnt는 0보다 커야 합니다.")
    if "addAmt" in parsed:
        if not parsed["addAmt"].is_finite() or parsed["addAmt"] < 0:
            raise ValueError("DB addAmt는 0 이상의 유한한 금액이어야 합니다.")
        ADDITIONAL_BUY_KRW = parsed["addAmt"]

    _db_setup_cache[user_no] = (now, parsed)
    return parsed


def calculate_bucket_limits(total_equity: Decimal) -> Dict[str, Decimal]:
    allocation_equity = floor_buy_amount(total_equity)
    return {
        "initial": floor_buy_amount(allocation_equity * RATIO_INITIAL),
        "averaging": floor_buy_amount(allocation_equity * RATIO_AVERAGING),
        "breakout": floor_buy_amount(allocation_equity * RATIO_BREAKOUT),
        "reserve": (allocation_equity * RATIO_CASH_RESERVE).quantize(Decimal("1"), rounding=ROUND_DOWN),
    }


def floor_buy_amount(amount: Decimal) -> Decimal:
    return (amount / BUY_AMOUNT_UNIT_KRW).to_integral_value(rounding=ROUND_DOWN) * BUY_AMOUNT_UNIT_KRW


def ceil_buy_amount(amount: Decimal) -> Decimal:
    return (amount / BUY_AMOUNT_UNIT_KRW).to_integral_value(rounding=ROUND_CEILING) * BUY_AMOUNT_UNIT_KRW


def calculate_bucket_usage(sm: "CompositeStateManager", accounts: List[Dict[str, Any]]) -> Dict[str, Decimal]:
    usage = {"initial": Decimal("0"), "averaging": Decimal("0"), "breakout": Decimal("0")}
    account_by_market = {
        f"KRW-{account.get('currency')}": account
        for account in accounts
        if account.get("currency") and account.get("currency") != "KRW"
    }
    market_states = dict(sm.trend_positions)
    if sm.grid_position and sm.grid_position.get("market"):
        market_states[sm.grid_position["market"]] = sm.grid_position

    for market in set(account_by_market) | set(market_states):
        account = account_by_market.get(market)
        held_qty = (Decimal(str(account.get("balance", "0"))) +
                    Decimal(str(account.get("locked", "0")))) if account else Decimal("0")
        avg_price = Decimal(str(account.get("avg_buy_price", "0"))) if account else Decimal("0")
        current_cost = held_qty * avg_price
        state = market_states.get(market)
        if not state:
            usage["initial"] += current_cost
            continue

        commitments = {
            bucket: Decimal(str(state.get(f"{bucket}_invested", "0")))
            for bucket in ("initial", "averaging", "breakout")
        }
        committed_total = sum(commitments.values(), Decimal("0"))
        if committed_total <= 0:
            state["initial_invested"] = current_cost
            commitments["initial"] = current_cost
            committed_total = current_cost
        accounted_cost = max(current_cost, committed_total)
        for bucket, amount in commitments.items():
            usage[bucket] += amount * accounted_cost / committed_total

    return {bucket: ceil_buy_amount(amount) for bucket, amount in usage.items()}


def create_budget_state(total_equity: Decimal, krw_available: Decimal, sm: "CompositeStateManager",
                        accounts: List[Dict[str, Any]], db_setup: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    market_states = dict(sm.trend_positions)
    if sm.grid_position and sm.grid_position.get("market"):
        market_states[sm.grid_position["market"]] = sm.grid_position
    market_used = {}
    market_bucket_used: Dict[str, Dict[str, Decimal]] = {}
    account_markets = {}
    for account in accounts:
        currency = account.get("currency")
        if not currency or currency == "KRW":
            continue
        market = f"KRW-{currency}"
        account_markets[market] = account

    for market in set(account_markets) | set(market_states):
        account = account_markets.get(market)
        volume = (Decimal(str(account.get("balance", "0"))) +
                  Decimal(str(account.get("locked", "0")))) if account else Decimal("0")
        average_price = Decimal(str(account.get("avg_buy_price", "0"))) if account else Decimal("0")
        actual_cost = volume * average_price
        state = market_states.get(market)
        commitments = {
            bucket: Decimal(str(state.get(f"{bucket}_invested", "0"))) if state else Decimal("0")
            for bucket in ("initial", "averaging", "breakout")
        }
        recorded_cost = sum(commitments.values(), Decimal("0"))
        market_used[market] = ceil_buy_amount(max(actual_cost, recorded_cost))
        if recorded_cost <= 0:
            market_bucket_used[market] = {
                "initial": ceil_buy_amount(actual_cost),
                "averaging": Decimal("0"),
                "breakout": Decimal("0")}
        else:
            accounted_cost = max(actual_cost, recorded_cost)
            market_bucket_used[market] = {
                bucket: ceil_buy_amount(amount * accounted_cost / recorded_cost)
                for bucket, amount in commitments.items()
            }
    limits = calculate_bucket_limits(total_equity)
    return {
        "limits": limits,
        "used": calculate_bucket_usage(sm, accounts),
        "market_used": market_used,
        "market_bucket_used": market_bucket_used,
        "per_market_limits": {
            bucket: floor_buy_amount(limits[bucket] / Decimal(str(db_setup["maxCoincnt"])))
            if db_setup else Decimal("0")
            for bucket in ("initial", "averaging", "breakout")
        },
        "equity": floor_buy_amount(total_equity),
        "krw_available": krw_available,
        "db_setup": db_setup,
    }


def capped_buy_amount(budget: Dict[str, Any], market: str, bucket: str, requested: Decimal,
                      market_limit: Decimal) -> Decimal:
    if not budget["db_setup"]:
        return Decimal("0")
    fee_factor = Decimal("1") + BUY_FEE_BUFFER_RATIO
    bucket_remaining = max(Decimal("0"), budget["limits"][bucket] - budget["used"][bucket])
    market_remaining = max(Decimal("0"), market_limit - budget["market_used"].get(market, Decimal("0")))
    market_bucket_used = budget["market_bucket_used"].get(market, {}).get(bucket, Decimal("0"))
    per_market_remaining = max(
        Decimal("0"), budget["per_market_limits"][bucket] - market_bucket_used)
    liquid_after_reserve = max(Decimal("0"), budget["krw_available"] - budget["limits"]["reserve"])
    amount = min(requested, bucket_remaining, market_remaining, per_market_remaining,
                 liquid_after_reserve / fee_factor)
    return floor_buy_amount(amount)


async def place_capped_buy(access_key: str, secret_key: str, market: str, bucket: str, requested: Decimal,
                           budget: Dict[str, Any], state: Dict[str, Any]) -> Decimal:
    db_setup = budget["db_setup"]
    if not db_setup:
        return Decimal("0")
    amount = capped_buy_amount(budget, market, bucket, requested, db_setup["limitAmt"])
    if amount < MIN_ORDER_NOTIONAL_KRW:
        return Decimal("0")
    print(f"[BUY-CAP] {market} bucket={bucket} buy={amount:,.0f} "
          f"remaining={budget['limits'][bucket] - budget['used'][bucket]:,.0f} "
          f"reserve={budget['limits']['reserve']:,.0f}")
    if LIVE_TRADING:
        await order_market_buy_price(access_key, secret_key, market, amount)
    debit_limit = amount * (Decimal("1") + BUY_FEE_BUFFER_RATIO)
    budget["used"][bucket] += amount
    budget["market_used"][market] = budget["market_used"].get(market, Decimal("0")) + amount
    market_usage = budget["market_bucket_used"].setdefault(
        market, {"initial": Decimal("0"), "averaging": Decimal("0"), "breakout": Decimal("0")})
    market_usage[bucket] += amount
    budget["krw_available"] -= debit_limit
    state[f"{bucket}_invested"] = Decimal(str(state.get(f"{bucket}_invested", "0"))) + amount
    return amount


def state_flag(state: Dict[str, Any], key: str) -> bool:
    return str(state.get(key, "")).strip().upper() in ("1", "Y", "TRUE")


async def fetch_rebound_signal(market: str) -> Optional[bool]:
    url = "https://api.upbit.com/v1/candles/minutes/15"
    try:
        client = get_http_client()
        response = await client.get(url, params={"market": market, "count": 20})
        response.raise_for_status()
        candles = response.json()
        if len(candles) < 20:
            raise ValueError(f"15분봉 응답이 부족합니다: {len(candles)}개")

        current_price = Decimal(str(candles[0]["trade_price"]))
        open_price = Decimal(str(candles[0]["opening_price"]))
        closes = [Decimal(str(candle["trade_price"])) for candle in candles]
        ma5 = sum(closes[:5]) / Decimal("5")
        ma20 = sum(closes[:20]) / Decimal("20")
        return current_price >= ma5 and ma5 > ma20 and current_price >= open_price
    except (httpx.HTTPError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
        print(f"[REBOUND-CHECK][ERR] {market} 15분봉 반등 확인 실패: {exc}")
        return None


async def check_bullish_breakout(market: str) -> bool:
    url = "https://api.upbit.com/v1/candles/minutes/15"
    try:
        client = get_http_client()
        response = await client.get(url, params={"market": market, "count": 20})
        response.raise_for_status()
        candles = response.json()
        if len(candles) < 20:
            raise ValueError(f"15분봉 응답이 부족합니다: {len(candles)}개")

        current_price = Decimal(str(candles[0]["trade_price"]))
        open_price = Decimal(str(candles[0]["opening_price"]))
        closes = [Decimal(str(candle["trade_price"])) for candle in candles]
        ma5 = sum(closes[:5]) / Decimal("5")
        ma20 = sum(closes[:20]) / Decimal("20")
        previous_high = max(Decimal(str(candle["high_price"])) for candle in candles[1:])
        return current_price > previous_high and current_price >= ma5 and ma5 > ma20 and current_price >= open_price
    except (httpx.HTTPError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
        print(f"[BREAKOUT-CHECK][ERR] {market} 15분봉 돌파 확인 실패: {exc}")
        return False


# ============================================================
# 6. 상태 관리 구조 (Redis 영속화 지원)
# ============================================================
class CompositeStateManager:
    def __init__(self, user_no: int, r_client: aioredis.Redis):
        self.user_no = user_no
        self.r = r_client
        self.trend_positions: Dict[str, Dict[str, Any]] = {}
        self.grid_position: Optional[Dict[str, Any]] = None
        self.cooldowns: Dict[str, float] = {}
        self.last_rescue_exit_ts: float = 0.0

    def get_grid_key(self) -> str:
        return f"mtpond6:state:grid:{self.user_no}"

    def get_trend_key(self) -> str:
        return f"mtpond6:state:trend:{self.user_no}"

    def get_meta_key(self) -> str:
        return f"mtpond6:meta:{self.user_no}"

    async def save_meta(self):
        try:
            await self.r.set(
                self.get_meta_key(),
                json.dumps({"last_rescue_exit_ts": self.last_rescue_exit_ts})
            )
        except Exception:
            pass

    async def restore_meta(self):
        try:
            raw = await self.r.get(self.get_meta_key())
            if raw:
                data = json.loads(raw)
                self.last_rescue_exit_ts = float(data.get("last_rescue_exit_ts", 0.0))
        except Exception:
            pass

    async def save_trend_position(self, market: str, data: dict):
        self.trend_positions[market] = data
        payload = {k: str(v) for k, v in data.items()}
        await self.r.hset(self.get_trend_key(), market, json.dumps(payload))

    async def remove_trend_position(self, market: str):
        self.trend_positions.pop(market, None)
        await self.r.hdel(self.get_trend_key(), market)

    async def save_grid_position(self, data: Optional[dict]):
        self.grid_position = data
        if data:
            clean_data = {}
            for k, v in data.items():
                if isinstance(v, Decimal):
                    clean_data[k] = str(v)
                elif isinstance(v, list):
                    clean_data[k] = v
                else:
                    clean_data[k] = v
            await self.r.set(self.get_grid_key(), json.dumps(clean_data))
        else:
            await self.r.delete(self.get_grid_key())

    async def restore_state_from_redis(self, access_key: str, secret_key: str):
        print(f"🔄 [RESTORE] 시스템 재기동 감지: Redis 및 업비트 계좌 대조 복원 시작...")
        await self.restore_meta()
        try:
            accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            wallet_map = {}
            for a in accs:
                cur = a.get("currency")
                if cur and cur != "KRW":
                    tot = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    avg = Decimal(str(a.get("avg_buy_price", "0")))
                    if tot > 0:
                        wallet_map[f"KRW-{cur}"] = {"total_vol": tot, "avg_price": avg}
        except Exception as e:
            print(f"[RESTORE][ERR] 계좌 조회 실패: {e}")
            return

        try:
            raw_grid = await self.r.get(self.get_grid_key())
            if raw_grid:
                grid_data = json.loads(raw_grid)
                mkt = grid_data.get("market")
                if mkt in wallet_map:
                    grid_data["base_price"] = Decimal(str(grid_data.get("base_price", "0")))
                    position_cost = wallet_map[mkt]["avg_price"] * wallet_map[mkt]["total_vol"]
                    committed = sum(
                        (Decimal(str(grid_data.get(f"{bucket}_invested", "0"))) for bucket in
                         ("initial", "averaging", "breakout")),
                        Decimal("0"))
                    if committed <= 0:
                        grid_data["initial_invested"] = str(position_cost)
                    grid_data.setdefault("averaging_attempted", False)
                    grid_data.setdefault("breakout_attempted", False)
                    self.grid_position = grid_data
                    print(f"  └ [그리드 복원] {mkt} (지갑 잔여: {wallet_map[mkt]['total_vol']}개)")
                else:
                    await self.r.delete(self.get_grid_key())
        except Exception as e:
            print(f"[RESTORE][WARN] 그리드 복원 실패: {e}")

        try:
            raw_trend = await self.r.hgetall(self.get_trend_key())
            if raw_trend:
                for mkt, str_val in raw_trend.items():
                    if mkt in wallet_map:
                        item = json.loads(str_val)
                        real_avg = wallet_map[mkt]["avg_price"]
                        item["avg_price"] = real_avg if real_avg > 0 else Decimal(str(item.get("avg_price", "0")))
                        item["entry_ts"] = float(item.get("entry_ts", time.time()))
                        position_cost = wallet_map[mkt]["avg_price"] * wallet_map[mkt]["total_vol"]
                        committed = sum(
                            (Decimal(str(item.get(f"{bucket}_invested", "0"))) for bucket in
                             ("initial", "averaging", "breakout")),
                            Decimal("0"))
                        if committed <= 0:
                            item["initial_invested"] = str(position_cost)
                        item.setdefault("averaging_attempted", False)
                        item.setdefault("breakout_attempted", False)
                        self.trend_positions[mkt] = item
                        print(f"  └ [추세 복원] {mkt} (평단: {item['avg_price']:,.0f}원)")
                    else:
                        await self.r.hdel(self.get_trend_key(), mkt)
        except Exception as e:
            print(f"[RESTORE][WARN] 추세 복원 실패: {e}")

        for mkt, w_info in wallet_map.items():
            if mkt not in self.trend_positions and (not self.grid_position or self.grid_position.get("market") != mkt):
                print(f"  └ ⚠️ [미등록 코인 감지] {mkt} → 추세 포지션 자동 편입 (수량: {w_info['total_vol']})")
                await self.save_trend_position(mkt, {
                    "avg_price": w_info["avg_price"],
                    "entry_ts": time.time(),
                    "initial_invested": str(w_info["avg_price"] * w_info["total_vol"]),
                    "averaging_attempted": False,
                    "breakout_attempted": False,
                })

        print(
            f"✅ [RESTORE-COMPLETE] 복원 완료: 그리드={self.grid_position.get('market') if self.grid_position else '없음'}, 추세={list(self.trend_positions.keys())}")

    def set_cooldown(self, market: str, is_tp: bool):
        sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_DEFENSE_SEC
        self.cooldowns[market] = time.time() + sec
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.r.set(f"mtpond6:cooldown:{self.user_no}:{market}", "1", ex=sec))
        except RuntimeError:
            pass
        print(f"[COOLDOWN] {market} {sec}초간 재진입 금지")

    async def is_cooling_down(self, market: str) -> bool:
        if time.time() < self.cooldowns.get(market, 0.0):
            return True
        try:
            if await self.r.exists(f"mtpond6:cooldown:{self.user_no}:{market}"):
                return True
        except Exception:
            pass
        return False


# ============================================================
# 7. Upbit API & 호가 헬퍼
# ============================================================
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


async def upbit_api(method: str, url: str, access_key: str, secret_key: str,
                    params: Optional[Dict[str, Any]] = None) -> Any:
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

    if r.status_code >= 400: raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json()


async def order_market_buy_price(
    access_key: str, secret_key: str, market: str, krw_amount: Decimal
) -> Any:
    return await market_buy(
        upbit_api, access_key, secret_key, market, krw_amount, UPBIT_ORDER_URL
    )


async def register_loss_cut(
        user_no: int, market: str, quantity: Decimal, price: Decimal, loss_rate: Decimal):
    await _register_loss_cut(
        API_BASE, user_no, market, quantity, price, loss_rate
    )


def calculate_ladder_volumes(total_volume: Decimal, total_eval: Decimal, avg_price: Decimal) -> List[Decimal]:
    if total_volume <= 0 or total_eval < MIN_ORDER_NOTIONAL_KRW or avg_price <= 0:
        return []

    slot_count = min(5, max(1, int(total_eval // Decimal("10000"))))
    while slot_count > 1:
        slice_krw = floor_buy_amount(total_eval / Decimal(str(slot_count)))
        slice_volume = quantize_volume(slice_krw / avg_price)
        if slice_volume * avg_price >= MIN_ORDER_NOTIONAL_KRW:
            break
        slot_count -= 1

    if slot_count == 1:
        return [quantize_volume(total_volume)]

    volumes = []
    remaining = quantize_volume(total_volume)
    for _ in range(slot_count - 1):
        volume = min(slice_volume, remaining)
        if volume * avg_price < MIN_ORDER_NOTIONAL_KRW:
            break
        volumes.append(volume)
        remaining = quantize_volume(remaining - volume)

    if remaining > 0:
        volumes.append(remaining)
    return volumes


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


async def cancel_all_orders(access_key: str, secret_key: str, market: str):
    try:
        orders = await upbit_api("GET", "https://api.upbit.com/v1/orders", access_key, secret_key,
                                 {"market": market, "state": "wait"})
        if not orders or not isinstance(orders, list): return
        tasks = [
            upbit_api("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, {"uuid": o["uuid"]})
            for o in orders if isinstance(o, dict) and o.get("uuid")
        ]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"[CANCEL][ERR] {market} 취소 실패: {e}")


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


async def fetch_rescue_best_ask(market: str) -> Decimal:
    client = get_http_client()
    response = await client.get(
        "https://api.upbit.com/v1/orderbook", params={"markets": market}
    )
    response.raise_for_status()
    data = response.json()
    return Decimal(str(data[0]["orderbook_units"][0]["ask_price"]))


async def _process_rescue_buy_unlocked(
        access_key: str,
        secret_key: str,
        sm: CompositeStateManager,
        market: str,
        state: Dict[str, Any],
        is_grid: bool,
        balance: Decimal,
        avg_price: Decimal,
        current_price: Decimal,
        pnl: Decimal,
        available_krw: Decimal,
        total_equity: Decimal = Decimal("0")
) -> Decimal:
    if available_krw < MIN_ORDER_NOTIONAL_KRW or pnl <= RESCUE_MAX_DROP or pnl > RESCUE_MIN_DROP:
        return Decimal("0")
    active_count = int(state_flag(sm.grid_position or {}, "is_rescued"))
    active_count += sum(
        1 for item in sm.trend_positions.values() if state_flag(item, "is_rescued")
    )
    if active_count >= MAX_CONCURRENT_RESCUE or state_flag(state, "is_rescued"):
        return Decimal("0")
    if not await check_rescue_rebound_signal(market):
        return Decimal("0")

    try:
        buy_price = await fetch_rescue_best_ask(market)
        target_avg = avg_price / Decimal("1.003")
        if balance <= 0 or buy_price <= 0 or buy_price >= target_avg:
            return Decimal("0")
        equity_budget = (total_equity * MAX_RESCUE_BUDGET_RATIO) if total_equity > 0 else ADDITIONAL_BUY_KRW
        rescue_cap = min(ADDITIONAL_BUY_KRW, MAX_RESCUE_BUDGET_KRW, equity_budget)
        buy_amount = min(rescue_cap, available_krw).to_integral_value(
            rounding=ROUND_FLOOR
        )
    except Exception as exc:
        print(f"[RESCUE][ERR] {market} 매수액 계산 실패: {exc}")
        return Decimal("0")
    if buy_amount < MIN_ORDER_NOTIONAL_KRW:
        print(f"[RESCUE-SKIP] {market} 구출 매수액 {buy_amount:,.0f}원 "
              f"(addAmt {ADDITIONAL_BUY_KRW:,.0f}원, 한도 {rescue_cap:,.0f}원, 가용 KRW {available_krw:,.0f}원) "
              f"최소주문액 {MIN_ORDER_NOTIONAL_KRW:,.0f}원 미만")
        return Decimal("0")

    async with get_market_lock(market):
        buy_completed = False
        try:
            result = await order_market_buy_price(
                access_key, secret_key, market, buy_amount
            ) if LIVE_TRADING else {}
            buy_completed = True
            state["is_rescued"] = True
            state["rescue_buy_price"] = current_price
            state["rescue_start_ts"] = time.time()
            state["averaging_invested"] = Decimal(str(state.get("averaging_invested", "0"))) + buy_amount
            if is_grid:
                await sm.save_grid_position(state)
            else:
                await sm.save_trend_position(market, state)
            print(f"[RESCUE-BUY] {market} 구출 매수 {buy_amount:,.0f}원 "
                  f"(uuid={result.get('uuid')})")

            await asyncio.sleep(0.8)
            await cancel_all_orders(access_key, secret_key, market)
            await asyncio.sleep(0.3)
            accounts = await upbit_api(
                "GET", "https://api.upbit.com/v1/accounts", access_key, secret_key
            )
            account = next(
                (item for item in accounts if item.get("currency") == market.split("-")[1]),
                None
            )
            if account:
                new_balance = Decimal(str(account.get("balance", "0")))
                new_avg = Decimal(str(account.get("avg_buy_price", "0")))
                if new_balance > 0 and new_avg > 0:
                    state["avg_price"] = new_avg
                    limit_price = adjust_price_to_tick(
                        max(new_avg * Decimal("1.003"), await fetch_rescue_best_ask(market)),
                        side="ask"
                    )
                    volume = quantize_volume(new_balance)
                    if volume * limit_price >= MIN_ORDER_NOTIONAL_KRW and LIVE_TRADING:
                        await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                            "market": market, "side": "ask", "volume": str(volume),
                            "price": str(limit_price), "ord_type": "limit"
                        })
                    if is_grid:
                        await sm.save_grid_position(state)
                    else:
                        await sm.save_trend_position(market, state)
            return buy_amount
        except Exception as exc:
            print(f"[RESCUE][ERR] {market} 구출 매수/탈출 주문 실패: {exc}")
            return buy_amount if buy_completed else Decimal("0")


async def process_rescue_buy(
        access_key: str,
        secret_key: str,
        sm: CompositeStateManager,
        market: str,
        state: Dict[str, Any],
        is_grid: bool,
        balance: Decimal,
        avg_price: Decimal,
        current_price: Decimal,
        pnl: Decimal,
        available_krw: Decimal,
        total_equity: Decimal = Decimal("0")
) -> Decimal:
    async with _rescue_buy_lock:
        return await _process_rescue_buy_unlocked(
            access_key, secret_key, sm, market, state, is_grid, balance,
            avg_price, current_price, pnl, available_krw, total_equity
        )


async def process_rescue_candidates(
        access_key: str,
        secret_key: str,
        sm: CompositeStateManager,
        accounts_by_market: Dict[str, Dict[str, Any]],
        price_map: Dict[str, Decimal],
        available_krw: Decimal,
        btc_status: str,
        total_equity: Decimal = Decimal("0"),
        excluded: Optional[Set[str]] = None
) -> Decimal:
    global _last_rescue_completion_ts
    if not ENABLE_STOP or btc_status != "NORMAL":
        return Decimal("0")
    now = time.time()
    if (now - _last_rescue_completion_ts) < RESCUE_GLOBAL_COOLDOWN_SEC or (now - sm.last_rescue_exit_ts) < RESCUE_GLOBAL_COOLDOWN_SEC:
        return Decimal("0")
    if sum(
            1 for state in [sm.grid_position, *sm.trend_positions.values()]
            if state and state_flag(state, "is_rescued")
    ) >= MAX_CONCURRENT_RESCUE:
        return Decimal("0")

    ex = excluded or set()
    pattern_rows = await fetch_all_candle_patterns()
    ranks = {}
    for row in pattern_rows:
        if not isinstance(row, dict):
            continue
        market = row.get("market", "")
        if not market.startswith("KRW-") or market in ex:
            continue
        try:
            ranks[market] = (
                int(row.get("overall_score", 0)),
                int((row.get("trade_activity") or {}).get("trade_count_10m", 0)),
            )
        except (TypeError, ValueError):
            continue

    positions = []
    if sm.grid_position:
        positions.append((sm.grid_position["market"], sm.grid_position, True))
    positions.extend(
        (market, state, False) for market, state in sm.trend_positions.items()
    )
    candidates = []
    for market, state, is_grid in positions:
        if market in ex:
            continue
        account = accounts_by_market.get(market)
        current_price = price_map.get(market, Decimal("0"))
        if not account or current_price <= 0 or state_flag(state, "is_rescued"):
            continue
        balance = Decimal(str(account.get("balance", "0"))) + Decimal(
            str(account.get("locked", "0"))
        )
        average_price = Decimal(str(account.get("avg_buy_price", "0")))
        if balance <= 0 or average_price <= 0:
            continue
        pnl = (current_price - average_price) / average_price * Decimal("100")
        score, trades = ranks.get(market, (0, 0))
        if not (RESCUE_MAX_DROP < pnl <= RESCUE_MIN_DROP) or trades < BB_MIN_TRADE_COUNT_10M:
            continue
        candidates.append(
            (score, trades, market, state, is_grid, balance, average_price, current_price, pnl)
        )

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    for _, _, market, state, is_grid, balance, average, current, pnl in candidates:
        used = await process_rescue_buy(
            access_key, secret_key, sm, market, state, is_grid, balance,
            average, current, pnl, available_krw, total_equity
        )
        if used > 0 or state_flag(state, "is_rescued"):
            return used
    return Decimal("0")


async def handle_rescue_stop(
        access_key: str,
        secret_key: str,
        sm: CompositeStateManager,
        market: str,
        state: Dict[str, Any],
        current_price: Decimal,
        pnl: Decimal,
        is_grid: bool
) -> bool:
    global _last_rescue_completion_ts
    rescue_price = state.get("rescue_buy_price")
    rescue_start_ts = float(state.get("rescue_start_ts", 0.0))
    if not ENABLE_STOP or not state_flag(state, "is_rescued") or rescue_price is None:
        return False
    rescue_price = Decimal(str(rescue_price))
    if rescue_price <= 0:
        return False

    loop_now = time.time()
    is_drop_fail = bool(current_price <= rescue_price * Decimal("0.975"))
    is_timeout_fail = bool(rescue_start_ts > 0 and (loop_now - rescue_start_ts) >= RESCUE_TIMEOUT_SEC)

    if not is_drop_fail and not is_timeout_fail:
        return False

    fail_reason = "구출 후 2.5% 하락" if is_drop_fail else f"구출 후 {RESCUE_TIMEOUT_SEC}초 타임아웃 초과"
    print(f"🚨 [RESCUE-FAIL] {market} {fail_reason} → 전량 시장가 청산")

    async with get_market_lock(market):
        await cancel_all_orders(access_key, secret_key, market)
        await asyncio.sleep(0.4)
        accounts = await upbit_api(
            "GET", "https://api.upbit.com/v1/accounts", access_key, secret_key
        )
        account = next(
            (item for item in accounts if item.get("currency") == market.split("-")[1]),
            None
        )
        if account:
            volume = quantize_volume(Decimal(str(account.get("balance", "0"))))
            if volume * current_price >= MIN_ORDER_NOTIONAL_KRW:
                if LIVE_TRADING:
                    await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                        "market": market, "side": "ask", "volume": str(volume), "ord_type": "market"
                    })
                await register_loss_cut(sm.user_no, market, volume, current_price, pnl)
                _last_rescue_completion_ts = loop_now
                sm.last_rescue_exit_ts = loop_now
                await sm.save_meta()
                sm.set_cooldown(market, is_tp=False)
                if is_grid:
                    await sm.save_grid_position(None)
                else:
                    await sm.remove_trend_position(market)
                print(f"[RESCUE-LOSS-CUT] {market} {fail_reason} 전량 청산 완료")
    return True


async def fetch_current_prices(markets: List[str], access_key: str, secret_key: str) -> Dict[str, Decimal]:
    if not markets: return {}
    try:
        data = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key,
                               {"markets": ",".join(markets)})
        return {item["market"]: Decimal(str(item["trade_price"])) for item in data if
                item.get("trade_price") is not None}
    except Exception:
        return {}


# ============================================================
# 8. 비트코인 3단계 방어 체력 진단 & 외부 캔들 추세 서버 연동
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
            res_data = {
                "status": "EMERGENCY",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"🚨 [BTC-EMERGENCY] 급락 감지! (1시간: {pnl_1h:+.2f}%, 4시간: {pnl_4h:+.2f}%, 현재가 < MA20)"
            }
        # 2. 주의 상태 (CAUTION): 1시간 내 -0.8% ~ -1.8% 조정 중 또는 MA20 하회
        elif pnl_1h <= -0.8 or cur_price < ma20:
            res_data = {
                "status": "CAUTION",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": f"⚠️ [BTC-CAUTION] 조정세 감지 (1시간: {pnl_1h:+.2f}%, MA20 근접/하회)"
            }
        else:
            res_data = {
                "status": "NORMAL",
                "btc_pnl_1h": round(pnl_1h, 2),
                "msg": "✅ [BTC-NORMAL] 안정적 추세 유지"
            }
        _btc_health_cache["ts"] = now
        _btc_health_cache["result"] = res_data
        return res_data
    except Exception as e:
        return {"status": "NORMAL", "btc_pnl_1h": 0.0, "msg": f"BTC 분석 에러: {e}"}


_patterns_cache: Dict[str, Any] = {"ts": 0.0, "data": []}


async def fetch_all_candle_patterns() -> List[Dict[str, Any]]:
    """외부 캔들 추세 분석 서버에서 전체 코인 최신 평가 데이터를 1회 일괄 조회"""
    now = time.time()
    if (now - _patterns_cache["ts"] < BB_CACHE_TTL_SEC) and _patterns_cache["data"]:
        return _patterns_cache["data"]

    try:
        client = get_http_client()
        resp = await client.get(BB_PATTERNS_API_URL)
        if resp.status_code == 200:
            body = resp.json()
            coins = body.get("coins", [])
            if isinstance(coins, list):
                _patterns_cache["ts"] = now
                _patterns_cache["data"] = coins
                return coins
    except Exception as e:
        print(f"[API-PATTERNS][WARN] 외부 캔들 추세 서버 조회 실패: {e}")

    return _patterns_cache.get("data", [])


async def get_trend_scalping_candidates(excluded: Optional[Set[str]] = None) -> List[Dict[str, Any]]:
    """추세 스캘핑용: 점수(overall_score >= 3) 및 단기 거래 체결량이 풍부한 종목 추출"""
    coins = await fetch_all_candle_patterns()
    qualified = []
    ex = excluded or set()
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in ex: continue
        try:
            score = int(c.get("overall_score", 0))
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
        except (ValueError, TypeError):
            continue

        if score >= 3 and trades >= 50 and price >= 30:
            qualified.append({
                "market": mkt,
                "overall_score": score,
                "trade_count": trades,
                "price": price
            })

    qualified.sort(key=lambda x: (x["overall_score"], x["trade_count"]), reverse=True)
    return qualified


async def get_grid_active_candidates(excluded: Optional[Set[str]] = None) -> List[Dict[str, Any]]:
    """그리드 하베스트용: 점수 무관, 10분 체결 빈도가 가장 높은 횡보형 종목 추출"""
    coins = await fetch_all_candle_patterns()
    candidates = []
    ex = excluded or set()
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in ex: continue
        try:
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
        except (ValueError, TypeError):
            continue

        if 50 <= price <= 50_000 and trades >= 80:
            candidates.append({
                "market": mkt,
                "trade_count": trades,
                "price": price
            })

    candidates.sort(key=lambda x: x["trade_count"], reverse=True)
    return candidates


async def check_bullish_trend(market: str) -> bool:
    """업비트 15분봉 정배열(5 > 20) & 양봉 재확인"""
    return await fetch_rebound_signal(market) is True


async def check_sideways_grid_candidate(market: str) -> bool:
    """그리드용 15분봉 밴드폭(1.2% ~ 3.0%) 횡보성 확인"""
    try:
        url = "https://api.upbit.com/v1/candles/minutes/15"
        client = get_http_client()
        res = await client.get(url, params={"market": market, "count": 20})
        if res.status_code != 200: return False
        candles = res.json()

        closes = [float(c["trade_price"]) for c in candles]
        mean = sum(closes) / len(closes)
        std = math.sqrt(sum((x - mean) ** 2 for x in closes) / len(closes))
        bandwidth_pct = ((4.0 * std) / mean) * 100.0
        return bool(1.2 <= bandwidth_pct <= 3.0)
    except Exception:
        return False


# ============================================================
# 9. 사다리 익절 주문 배치 함수
# ============================================================
async def place_ladder_tp_orders(access_key: str, secret_key: str, market: str):
    async with get_market_lock(market):
        try:
            await cancel_all_orders(access_key, secret_key, market)
            await asyncio.sleep(0.5)

            base = market.split("-")[1]
            accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            target_acc = next((a for a in accounts if a.get("currency") == base), None)
            if not target_acc: return

            avail_bal = Decimal(str(target_acc.get("balance", "0")))
            avg_price = Decimal(str(target_acc.get("avg_buy_price", "0")))
            total_eval = avail_bal * avg_price
            if total_eval < MIN_ORDER_NOTIONAL_KRW: return

            p = float(avg_price)
            if p >= 100_000:
                start_pct, step_pct = Decimal("0.25"), Decimal("0.12")
            elif p >= 10_000:
                start_pct, step_pct = Decimal("0.35"), Decimal("0.18")
            elif p >= 1_000:
                start_pct, step_pct = Decimal("0.45"), Decimal("0.25")
            else:
                start_pct, step_pct = Decimal("0.60"), Decimal("0.30")

            order_volumes = calculate_ladder_volumes(avail_bal, total_eval, avg_price)
            if not order_volumes:
                print(f"[LADDER-TP][SKIP] {market} 매도 가능 수량이 최소 주문금액 미만")
                return
            prev_limit = Decimal("0")

            print(f"📐 [LADDER-TP] {market} 사다리 익절 발주 "
                  f"(평단: {avg_price:,.0f}원 | 총액: {total_eval:,.0f}원 | {len(order_volumes)}슬롯)")

            for i, order_v in enumerate(order_volumes, start=1):
                curr_target_pct = start_pct + (step_pct * Decimal(str(i - 1)))
                target_p = avg_price * (Decimal("1") + (curr_target_pct / Decimal("100")))
                limit_p = adjust_price_to_tick(target_p, side="ask")

                if i > 1 and limit_p <= prev_limit:
                    limit_p = prev_limit + get_tick_unit(limit_p)

                if order_v * limit_p < MIN_ORDER_NOTIONAL_KRW:
                    print(f"[LADDER-TP][SKIP] {market} 슬롯 #{i} 최소 주문금액 미달")
                    continue
                if LIVE_TRADING:
                    await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                        "market": market, "side": "ask", "volume": str(order_v), "price": str(limit_p),
                        "ord_type": "limit"
                    })
                print(f"  └ [슬롯 #{i}] {order_v}개 @ {limit_p}원 (+{curr_target_pct:.2f}%)")
                prev_limit = limit_p
                await asyncio.sleep(0.15)

        except Exception as e:
            print(f"[LADDER-TP][ERR] {market} 사다리 주문 실패: {e}")


# ============================================================
# 10. 단일 종목 그리드 하베스트 발주 및 관리
# ============================================================
async def deploy_single_grid_harvest(access_key: str, secret_key: str, market: str, budget: Decimal, step_amt: Decimal,
                                     sm: CompositeStateManager):
    async with get_market_lock(market):
        print(f"🕸️ [GRID-HARVEST] {market} 그리드 배치 (총예산: {budget:,.0f}원 | 단계당: {step_amt:,.0f}원)")
        await cancel_all_orders(access_key, secret_key, market)

        t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": market})
        cur_p = Decimal(str(t_res[0]["trade_price"]))
        grid_orders = []
        position = {
            "market": market,
            "base_price": cur_p,
            "grid_orders": grid_orders,
            "last_tp_uuid": None,
            "deployed_ts": time.time(),
            "initial_invested": str(budget),
            "averaging_attempted": False,
            "breakout_attempted": False,
            "deploying": True,
        }
        await sm.save_grid_position(position)

        try:
            if LIVE_TRADING:
                buy_r = await order_market_buy_price(
                    access_key, secret_key, market, step_amt
                )
                grid_orders.append(buy_r.get("uuid"))

            prev_buy_p = cur_p
            for i in range(1, GRID_STEP_COUNT):
                drop_pct = GRID_INTERVAL_PCT * Decimal(str(i))
                raw_target_p = cur_p * (Decimal("1") - (drop_pct / Decimal("100")))
                limit_buy_p = adjust_price_to_tick(raw_target_p, side="bid")

                if limit_buy_p >= prev_buy_p:
                    limit_buy_p = prev_buy_p - get_tick_unit(limit_buy_p)

                vol = quantize_volume(step_amt / limit_buy_p)
                if vol * limit_buy_p < MIN_ORDER_NOTIONAL_KRW:
                    raise ValueError(f"그리드 #{i + 1} 주문 금액이 최소 주문금액보다 작습니다.")
                if LIVE_TRADING:
                    r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                        "market": market, "side": "bid", "volume": str(vol), "price": str(limit_buy_p),
                        "ord_type": "limit"
                    })
                    grid_orders.append(r.get("uuid"))
                    print(f"  └ [그리드 매수망 #{i + 1}] {vol}개 @ {limit_buy_p}원 (호가 분리)")
                    await asyncio.sleep(0.15)

                prev_buy_p = limit_buy_p

            position["deploying"] = False
            await sm.save_grid_position(position)
        except Exception:
            try:
                await cancel_all_orders(access_key, secret_key, market)
            except Exception as cancel_error:
                print(f"[GRID-HARVEST][ERR] {market} 주문 취소 실패: {cancel_error}")
            accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            target = next((a for a in accs if a.get("currency") == market.split("-")[1]), None)
            held = Decimal(str(target.get("balance", "0"))) + Decimal(str(target.get("locked", "0"))) if target else Decimal("0")
            if held > 0:
                position["deploying"] = False
                await sm.save_grid_position(position)
            else:
                await sm.save_grid_position(None)
            raise


async def manage_grid_position(access_key: str, secret_key: str, sm: CompositeStateManager,
                               budget_state: Dict[str, Any]):
    global _last_rescue_completion_ts
    if not sm.grid_position or sm.grid_position.get("deploying"):
        return

    mkt = sm.grid_position["market"]
    async with get_market_lock(mkt):
        accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
        base = mkt.split("-")[1]
        acc = next((a for a in accounts if a.get("currency") == base), None)

        if not acc:
            sm.grid_position = None
            await sm.save_grid_position(None)
            return

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        total_vol = bal + locked
        avg_p = Decimal(str(acc.get("avg_buy_price", "0")))

        if total_vol <= 0 or (total_vol * avg_p) < MIN_ORDER_NOTIONAL_KRW:
            print(f"🎉 [GRID-COMPLETE] {mkt} 횡보 그리드 목표 달성 및 청산 완료!")
            await cancel_all_orders(access_key, secret_key, mkt)
            if state_flag(sm.grid_position, "is_rescued"):
                _last_rescue_completion_ts = time.time()
                sm.last_rescue_exit_ts = time.time()
                await sm.save_meta()
            sm.set_cooldown(mkt, is_tp=True)
            await sm.save_grid_position(None)
            return

        t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": mkt})
        cur_p = Decimal(str(t_res[0]["trade_price"]))
        if avg_p <= 0 or cur_p <= 0: return

        pnl = ((cur_p - avg_p) / avg_p) * Decimal("100")

        grid_state = sm.grid_position

        # 1. 구출 상태 확인
        if state_flag(grid_state, "is_rescued"):
            await handle_rescue_stop(
                access_key, secret_key, sm, mkt, grid_state, cur_p, pnl, is_grid=True
            )
            return

        # 2. 그리드 하드 손절 (-7.0%) 및 대폭락 강제 손절 (-22.0%)
        is_hard_stop = bool(ENABLE_STOP and pnl <= HARD_STOP_LOSS_PCT)
        is_max_drop = bool(ENABLE_STOP and pnl <= MAX_DROP_LOSS_CUT_PCT)
        if is_hard_stop or is_max_drop:
            reason = "HARD_STOP_LOSS" if is_hard_stop else "MAX_DROP_LOSS_CUT"
            print(f"🚨 [GRID-STOP] {mkt} 그리드 손절 발동 ({reason}, PnL: {pnl:.2f}%) → 전량 시장가 청산")
            await cancel_all_orders(access_key, secret_key, mkt)
            await asyncio.sleep(0.4)
            accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            target_acc = next((a for a in accs if a.get("currency") == base), None)
            if target_acc:
                rem_vol = quantize_volume(Decimal(str(target_acc.get("balance", "0"))))
                if rem_vol and (rem_vol * cur_p) >= MIN_ORDER_NOTIONAL_KRW:
                    if LIVE_TRADING:
                        await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                            "market": mkt, "side": "ask", "volume": str(rem_vol), "ord_type": "market"
                        })
                    await register_loss_cut(sm.user_no, mkt, rem_vol, cur_p, pnl)
                    sm.set_cooldown(mkt, is_tp=False)
                    await sm.save_grid_position(None)
            return

        # 3. 그리드 익절(TP) 발주 및 수량 동기화
        target_tp_p = adjust_price_to_tick(avg_p * (Decimal("1") + (GRID_TARGET_TP_PCT / Decimal("100"))), side="ask")
        current_tp_vol = Decimal(str(grid_state.get("tp_volume", "0")))

        if bal * cur_p >= MIN_ORDER_NOTIONAL_KRW and (not grid_state.get("last_tp_uuid") or bal != current_tp_vol):
            old_uuid = grid_state.get("last_tp_uuid")
            if old_uuid:
                try:
                    await upbit_api("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, {"uuid": old_uuid})
                    await asyncio.sleep(0.3)
                    accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                    target_acc = next((a for a in accs if a.get("currency") == base), None)
                    if target_acc:
                        bal = Decimal(str(target_acc.get("balance", "0")))
                except Exception:
                    pass

            quant_vol = quantize_volume(bal)
            if quant_vol * target_tp_p >= MIN_ORDER_NOTIONAL_KRW:
                if LIVE_TRADING:
                    tp_res = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                        "market": mkt, "side": "ask", "volume": str(quant_vol), "price": str(target_tp_p),
                        "ord_type": "limit"
                    })
                    grid_state["last_tp_uuid"] = tp_res.get("uuid")
                else:
                    grid_state["last_tp_uuid"] = "dry_tp_uuid"
                grid_state["tp_volume"] = str(quant_vol)
                grid_state["tp_price"] = str(target_tp_p)
                await sm.save_grid_position(grid_state)
                print(f"🎯 [GRID-TP-PLACED] {mkt} 합산 평단가({avg_p:,.0f}원) 대비 +{GRID_TARGET_TP_PCT}% 매도 등록: {quant_vol}개 @ {target_tp_p}원")

        # 4. 그리드 물타기 (Averaging 40% 버킷)
        db_setup = budget_state["db_setup"]
        btc_status = budget_state.get("btc_status", "NORMAL")
        if (db_setup and btc_status == "NORMAL" and -5.0 <= pnl <= -2.5
                and not state_flag(sm.grid_position, "averaging_attempted")
                and await fetch_rebound_signal(mkt)):
            amount = await place_capped_buy(
                access_key, secret_key, mkt, "averaging",
                budget_state["per_market_limits"]["averaging"], budget_state, sm.grid_position)
            if amount > 0:
                sm.grid_position["averaging_attempted"] = True
                await sm.save_grid_position(sm.grid_position)
                print(f"[AVERAGING-BUY] {mkt} 그리드 조정 구간 반등 감지, 1회 물타기 {amount:,.0f}원")

        # 5. 그리드 돌파 불타기 (Breakout 10% 버킷)
        if (db_setup and budget_state.get("allow_breakout", True) and pnl > 0
                and not state_flag(sm.grid_position, "breakout_attempted")
                and await check_bullish_breakout(mkt)):
            amount = await place_capped_buy(
                access_key, secret_key, mkt, "breakout",
                budget_state["per_market_limits"]["breakout"], budget_state, sm.grid_position)
            if amount > 0:
                sm.grid_position["breakout_attempted"] = True
                await sm.save_grid_position(sm.grid_position)
                print(f"[BREAKOUT-BUY] {mkt} 그리드 최근 고점 돌파, 1회 불타기 {amount:,.0f}원")


# ============================================================
# 11. 메인 트레이딩 엔진 루프 (비트코인 3단계 방어 통합)
# ============================================================
async def run_hybrid_engine(user_no: int, server_no: int):
    global _last_rescue_completion_ts
    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2
                      FROM traceUser
                      WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        row = result.fetchone()
        if not row:
            print(f"[DB-CONFIG][ERR] user={user_no}, server={server_no} API 키를 DB에서 찾지 못했습니다.")
            return
        access_key, secret_key = row[0], row[1]

    await sync_redis_config(user_no)
    state_manager = CompositeStateManager(user_no, redis_client)
    await state_manager.restore_state_from_redis(access_key, secret_key)
    last_heartbeat_ts = 0.0
    last_config_error_ts = 0.0
    last_cfg_sync = time.time()
    print("[mtpond6] 총자산 기준 초기20% / 물타기40% / 불타기10% / 현금예비금30% 시작")

    while True:
        try:
            now = time.time()
            if now - last_cfg_sync >= 30:
                last_cfg_sync = now
                await sync_redis_config(user_no)

            db_setup = None
            try:
                db_setup = await fetch_database_setup(user_no)
            except (httpx.HTTPError, ValueError, RuntimeError) as config_error:
                if now - last_config_error_ts >= 30:
                    last_config_error_ts = now
                    print(f"[DB-CONFIG][ERR] 신규 매수/물타기 중지: {config_error}")

            excluded_markets = await get_excluded_markets(db_setup=db_setup)
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]
            accounts = await upbit_api(
                "GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            krw_available = Decimal("0")
            krw_locked = Decimal("0")
            accounts_by_market = {}
            held_markets = {}
            for account in accounts:
                currency = account.get("currency")
                if currency == "KRW":
                    krw_available = Decimal(str(account.get("balance", "0")))
                    krw_locked = Decimal(str(account.get("locked", "0")))
                elif currency:
                    market = f"KRW-{currency}"
                    if market not in excluded_markets:
                        volume = Decimal(str(account.get("balance", "0"))) + Decimal(
                            str(account.get("locked", "0")))
                        if volume > 0:
                            held_markets[market] = volume
                            accounts_by_market[market] = account

            price_map = await fetch_current_prices(list(held_markets), access_key, secret_key)
            valuation_complete = all(market in price_map for market in held_markets)
            if not valuation_complete:
                print("[BUY-CAP][WARN] 일부 보유 종목 현재가 조회 실패; 신규매수/물타기 차단")
            coin_value = sum(
                (volume * price_map.get(market, Decimal("0")) for market, volume in held_markets.items()),
                Decimal("0"))
            total_equity = krw_available + krw_locked + coin_value
            budget = create_budget_state(
                total_equity, krw_available, state_manager, accounts,
                db_setup if valuation_complete else None)
            budget["allow_breakout"] = btc_status == "NORMAL"
            budget["btc_status"] = btc_status

            # 사다리 익절 자동 재배치
            for market, account in accounts_by_market.items():
                if state_manager.grid_position and state_manager.grid_position.get("market") == market:
                    continue
                available_volume = Decimal(str(account.get("balance", "0")))
                total_volume = available_volume + Decimal(str(account.get("locked", "0")))
                average_price = Decimal(str(account.get("avg_buy_price", "0")))
                current_price = price_map.get(market, Decimal("0"))
                if current_price <= 0 or total_volume * current_price < MIN_ORDER_NOTIONAL_KRW:
                    continue
                state = state_manager.trend_positions.setdefault(market, {
                    "avg_price": average_price,
                    "entry_ts": now,
                    "initial_invested": (total_volume * average_price),
                    "averaging_attempted": False,
                    "breakout_attempted": False,
                    "realloc_ts": 0.0,
                })
                position_pnl = (current_price - average_price) / average_price * Decimal("100") if average_price > 0 else Decimal("0")
                if state_flag(state, "is_rescued") or (
                        ENABLE_STOP and RESCUE_MAX_DROP < position_pnl <= RESCUE_MIN_DROP):
                    continue
                if now - float(state.get("realloc_ts", "0")) >= 10:
                    state["realloc_ts"] = now
                    asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market))

            if now - last_heartbeat_ts >= 60:
                last_heartbeat_ts = now
                print(
                    f"[mtpond6][HEARTBEAT] 평가액={budget['equity']:,.0f}원 원화={krw_available:,.0f}원 "
                    f"예비금={budget['limits']['reserve']:,.0f}원 "
                    f"버킷 사용 초기/물타기/불타기="
                    f"{budget['used']['initial']:,.0f}/{budget['used']['averaging']:,.0f}/"
                    f"{budget['used']['breakout']:,.0f}원 설정={'DB' if db_setup else '불가'} | 시장={btc_status}")

            # 구출 관리
            rescue_used = await process_rescue_candidates(
                access_key, secret_key, state_manager, accounts_by_market,
                price_map, budget["krw_available"], btc_status, total_equity, excluded_markets
            )
            budget["krw_available"] -= rescue_used

            # 그리드 포지션 관리
            await manage_grid_position(access_key, secret_key, state_manager, budget)

            # 추세 포지션 관리 (익절 완료, 손절, 물타기, 불타기)
            for market in list(state_manager.trend_positions):
                if market not in held_markets:
                    print(f"🎉 [TREND-COMPLETE] {market} 모든 사다리 익절 체결 완료!")
                    if state_flag(state_manager.trend_positions[market], "is_rescued"):
                        _last_rescue_completion_ts = now
                        state_manager.last_rescue_exit_ts = now
                        await state_manager.save_meta()
                    state_manager.set_cooldown(market, is_tp=True)
                    await state_manager.remove_trend_position(market)
                    continue

                state = state_manager.trend_positions[market]
                account = accounts_by_market.get(market)
                if not account:
                    continue
                average_price = Decimal(str(account.get("avg_buy_price", "0")))
                if average_price <= 0:
                    continue
                state["avg_price"] = average_price
                ticker = await upbit_api(
                    "GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": market})
                current_price = Decimal(str(ticker[0]["trade_price"]))
                pnl = (current_price - average_price) / average_price * Decimal("100")

                # 1. 구출된 종목 손절 관리
                if state_flag(state, "is_rescued"):
                    await handle_rescue_stop(
                        access_key, secret_key, state_manager, market, state,
                        current_price, pnl, is_grid=False
                    )
                    continue

                # 2. 일반 추세 포지션 하드 손절 (-7.0%) 및 대폭락 강제 손절 (-22.0%)
                is_hard_stop = bool(ENABLE_STOP and pnl <= HARD_STOP_LOSS_PCT)
                is_max_drop = bool(ENABLE_STOP and pnl <= MAX_DROP_LOSS_CUT_PCT)
                if is_hard_stop or is_max_drop:
                    reason = "HARD_STOP_LOSS" if is_hard_stop else "MAX_DROP_LOSS_CUT"
                    print(f"🚨 [TREND-STOP] {market} 추세 포지션 손절 발동 ({reason}, PnL: {pnl:.2f}%) → 전량 시장가 청산")
                    async with get_market_lock(market):
                        await cancel_all_orders(access_key, secret_key, market)
                        await asyncio.sleep(0.4)
                        accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                        target_acc = next((a for a in accs if a.get("currency") == market.split("-")[1]), None)
                        if target_acc:
                            rem_vol = quantize_volume(Decimal(str(target_acc.get("balance", "0"))))
                            if rem_vol and (rem_vol * current_price) >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                                        "market": market, "side": "ask", "volume": str(rem_vol), "ord_type": "market"
                                    })
                                await register_loss_cut(state_manager.user_no, market, rem_vol, current_price, pnl)
                                state_manager.set_cooldown(market, is_tp=False)
                                await state_manager.remove_trend_position(market)
                    continue

                # 3. 물타기 (Averaging 40% 버킷)
                if (db_setup and btc_status == "NORMAL" and -5.0 <= pnl <= -2.5
                        and not state_flag(state, "averaging_attempted")
                        and await fetch_rebound_signal(market)):
                    amount = await place_capped_buy(
                        access_key, secret_key, market, "averaging",
                        budget["per_market_limits"]["averaging"], budget, state)
                    if amount > 0:
                        state["averaging_attempted"] = True
                        await state_manager.save_trend_position(market, state)
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market))
                        print(f"[AVERAGING-BUY] {market} 조정 구간 반등 감지, 1회 물타기 {amount:,.0f}원")

                # 4. 불타기 (Breakout 10% 버킷)
                if (db_setup and btc_status == "NORMAL" and pnl > 0
                        and not state_flag(state, "breakout_attempted")
                        and await check_bullish_breakout(market)):
                    amount = await place_capped_buy(
                        access_key, secret_key, market, "breakout",
                        budget["per_market_limits"]["breakout"], budget, state)
                    if amount > 0:
                        state["breakout_attempted"] = True
                        await state_manager.save_trend_position(market, state)
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market))
                        print(f"[BREAKOUT-BUY] {market} 최근 고점 돌파, 1회 불타기 {amount:,.0f}원")

            if not db_setup or btc_status != "NORMAL":
                await asyncio.sleep(4)
                continue

            market_count = len(state_manager.trend_positions) + (
                1 if state_manager.grid_position else 0)
            if market_count < db_setup["maxCoincnt"]:
                candidates = await get_trend_scalping_candidates(excluded_markets)
                for candidate in candidates:
                    market = candidate["market"]
                    if (market in held_markets
                            or market in excluded_markets
                            or (state_manager.grid_position
                                and state_manager.grid_position.get("market") == market)
                            or await state_manager.is_cooling_down(market)
                            or not await check_bullish_trend(market)):
                        continue
                    ticker = await upbit_api(
                        "GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": market})
                    current_price = Decimal(str(ticker[0]["trade_price"]))
                    state = {
                        "avg_price": current_price,
                        "entry_ts": now,
                        "initial_invested": Decimal("0"),
                        "averaging_attempted": False,
                        "breakout_attempted": False,
                    }
                    amount = await place_capped_buy(
                        access_key, secret_key, market, "initial",
                        budget["per_market_limits"]["initial"], budget, state)
                    if amount <= 0:
                        print(f"[INITIAL-BUY][CAP] {market} 예비금 또는 초기매수 한도로 주문 중지")
                        break
                    await state_manager.save_trend_position(market, state)
                    asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, market))
                    print(f"[INITIAL-BUY] {market} {amount:,.0f}원")
                    break

            if state_manager.grid_position is None and market_count < db_setup["maxCoincnt"]:
                for candidate in await get_grid_active_candidates(excluded_markets):
                    market = candidate["market"]
                    if (market in held_markets
                            or market in excluded_markets
                            or await state_manager.is_cooling_down(market)):
                        continue
                    if not await check_sideways_grid_candidate(market):
                        continue
                    initial_remaining = max(
                        Decimal("0"), budget["limits"]["initial"] - budget["used"]["initial"])
                    allowed = capped_buy_amount(
                        budget, market, "initial", initial_remaining, db_setup["limitAmt"])
                    step_amount = floor_buy_amount(allowed / Decimal(str(GRID_STEP_COUNT)))
                    grid_budget = step_amount * Decimal(str(GRID_STEP_COUNT))
                    if step_amount < MIN_ORDER_NOTIONAL_KRW:
                        print(f"[GRID-BUY][CAP] {market} 예산/예비금 한도로 주문 중지")
                        break
                    await deploy_single_grid_harvest(
                        access_key, secret_key, market, grid_budget, step_amount, state_manager)
                    grid_debit = grid_budget * (Decimal("1") + BUY_FEE_BUFFER_RATIO)
                    budget["used"]["initial"] += grid_budget
                    budget["market_used"][market] = budget["market_used"].get(market, Decimal("0")) + grid_budget
                    market_bucket_usage = budget["market_bucket_used"].setdefault(
                        market, {"initial": Decimal("0"), "averaging": Decimal("0"),
                                 "breakout": Decimal("0")})
                    market_bucket_usage["initial"] += grid_budget
                    budget["krw_available"] -= grid_debit
                    print(f"[GRID-BUY] {market} {grid_budget:,.0f}원")
                    break

        except Exception as error:
            print(f"[HYBRID-LOOP][ERR] {error}")

        await asyncio.sleep(4)


# ============================================================
# 12. Redis On/Off 감시 컨트롤러 및 진입점
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task: Optional[asyncio.Task] = None
    global ENABLE_STOP
    print(f"[CTRL] Controller 시작 (User: {user_no}, Server: {server_no})")

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
                    active_flag = cfg["activeYN"] == "Y"
            elif raw_status is None:
                cfg = await fetch_database_setup(user_no)
                active_flag = cfg["activeYN"] == "Y"

            if task is not None and task.done():
                exc = task.exception()
                if exc:
                    print(f"[CTRL] 엔진 예외 감지: {exc} → 재기동 준비")
                task = None

            if active_flag and task is None:
                print(f"▶️ [CTRL] 봇 활성화(ON) 감지 → run_hybrid_engine 기동")
                task = asyncio.create_task(run_hybrid_engine(user_no, server_no))

            elif not active_flag and task is not None:
                print(f"⏸️ [CTRL] 봇 비활성화(OFF) 감지 → run_hybrid_engine 중단 요청")
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
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    try:
        await run_mtpond_controller(user_no, server_no)
    finally:
        await close_http_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 봇을 안전하게 종료합니다.")
