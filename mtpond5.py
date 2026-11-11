"""전략 5: 총자산을 추세 스캘핑 50%, 그리드 20%, 현금 버퍼 30%로 운용한다.

추세 후보가 없을 때 횡보 후보로 그리드를 구성한다. 추세는 사다리 익절,
그리드는 합산 평단 기준 익절을 사용하며 구출 후 -2.5%에서 전량 손절한다.
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
# 3. STATIC CONFIG (50% / 20% / 30% 자금 룰 & 안전망 설정)
# ============================================================
BB_CACHE_TTL_SEC = 60  # 외부 분석 서버 캐시 주기 60초
ADDITIONAL_BUY_KRW = _decimal_env("ADDITIONAL_BUY_KRW", "40000")

# [하드 손절 및 자금 안전망 설정]
HARD_STOP_LOSS_PCT = _decimal_env("HARD_STOP_LOSS_PCT", "-7.0")        # 추세/그리드 하드 손절 (-7.0%)
MAX_DROP_LOSS_CUT_PCT = _decimal_env("MAX_DROP_LOSS_CUT_PCT", "-22.0")  # 대폭락 강제 손절 안전망 (-22.0%)
MIN_CASH_BUFFER_RATIO = _decimal_env("MIN_CASH_BUFFER_RATIO", "0.30")  # 순자산 대비 최소 현금 예비금(기본 30%)

RATIO_TREND = Decimal("0.50")  # 추세 스캘핑: 50%
RATIO_GRID = Decimal("0.20")   # 그리드 하베스트: 20%
RATIO_CASH_BUFFER = MIN_CASH_BUFFER_RATIO  # 현금 버퍼: 기본 30% (동적 조정 가능)

# [구출 작전 통제 설정]
MAX_RESCUE_BUDGET_RATIO = _decimal_env("MAX_RESCUE_BUDGET_RATIO", "0.15") # 1회 구출 예산 상한 (순자산의 15%)
MAX_RESCUE_BUDGET_KRW = _decimal_env("MAX_RESCUE_BUDGET_KRW", "80000")     # 1회 구출 절대 상한액 (8만원)
RESCUE_GLOBAL_COOLDOWN_SEC = int(os.getenv("RESCUE_GLOBAL_COOLDOWN_SEC", "600")) # 구출 종료 후 전역 쿨다운 (10분)
RESCUE_TIMEOUT_SEC = int(os.getenv("RESCUE_TIMEOUT_SEC", "3600"))                 # 구출 탈출 최대 대기 (1시간)

GRID_STEP_COUNT = 4  # 4단계 매수 그물망
GRID_INTERVAL_PCT = Decimal("0.5")  # -0.5% 간격
GRID_TARGET_TP_PCT = Decimal("0.5")  # +0.5% 합산 익절
_rescue_buy_lock = asyncio.Lock()

COOLDOWN_TP_SEC = 180
COOLDOWN_DEFENSE_SEC = 300


# ============================================================
# 4. 동적 포트폴리오 스케일링 계산기
# ============================================================
def calculate_dynamic_portfolio(total_equity: Decimal) -> Dict[str, Any]:
    eq = float(total_equity)

    if eq < 500_000:
        trend_markets = 2
        trend_per_trade = Decimal("40000")
        grid_budget = Decimal("32000")
        grid_target_markets = 1
    elif eq < 1_500_000:
        trend_markets = 2
        trend_per_trade = (total_equity * RATIO_TREND / Decimal("2")).quantize(Decimal("1000"))
        grid_budget = (total_equity * RATIO_GRID).quantize(Decimal("1000"))
        grid_target_markets = 1
    elif eq < 2_500_000:
        trend_markets = 3
        trend_per_trade = (total_equity * RATIO_TREND / Decimal("3")).quantize(Decimal("1000"))
        grid_budget = (total_equity * RATIO_GRID).quantize(Decimal("1000"))
        grid_target_markets = 1
    elif eq < 3_500_000:
        trend_markets = 3
        trend_per_trade = (total_equity * RATIO_TREND / Decimal("3")).quantize(Decimal("1000"))
        grid_budget = (total_equity * RATIO_GRID).quantize(Decimal("1000"))
        grid_target_markets = 1
    elif eq < 4_500_000:
        trend_markets = 4
        trend_per_trade = (total_equity * RATIO_TREND / Decimal("4")).quantize(Decimal("1000"))
        grid_budget = (total_equity * RATIO_GRID / Decimal("2")).quantize(Decimal("1000"))
        grid_target_markets = 2
    else:
        trend_markets = 5
        trend_per_trade = (total_equity * RATIO_TREND / Decimal("5")).quantize(Decimal("1000"))
        grid_budget = (total_equity * RATIO_GRID / Decimal("2")).quantize(Decimal("1000"))
        grid_target_markets = 2

    return {
        "trend_markets": trend_markets,
        "trend_per_trade": trend_per_trade,
        "grid_budget": grid_budget,
        "grid_step_amt": (grid_budget / Decimal(str(GRID_STEP_COUNT))).quantize(Decimal("100")),
        "grid_target_markets": grid_target_markets,
        "cash_buffer_limit": (total_equity * MIN_CASH_BUFFER_RATIO).quantize(Decimal("1000"))
    }


def apply_dynamic_config(cfg: Dict[str, Any]):
    global ENABLE_STOP, ADDITIONAL_BUY_KRW, HARD_STOP_LOSS_PCT, MAX_DROP_LOSS_CUT_PCT
    global MIN_CASH_BUFFER_RATIO, RATIO_CASH_BUFFER, MAX_RESCUE_BUDGET_RATIO, MAX_RESCUE_BUDGET_KRW
    global RESCUE_GLOBAL_COOLDOWN_SEC, RESCUE_TIMEOUT_SEC

    changes = []

    def to_dec(v):
        try:
            return Decimal(str(v)) if v not in (None, "") else None
        except Exception:
            return None

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).strip().upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

    if "addAmt" in cfg:
        val = to_dec(cfg.get("addAmt"))
        if val is not None and val.is_finite() and val >= 0 and val != ADDITIONAL_BUY_KRW:
            changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW:,.0f} -> {val:,.0f}")
            ADDITIONAL_BUY_KRW = val

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

    if "minCashBuffer" in cfg:
        mcb = to_dec(cfg.get("minCashBuffer"))
        if mcb is not None and mcb >= 0 and mcb != MIN_CASH_BUFFER_RATIO:
            changes.append(f"MIN_CASH_BUFFER_RATIO {MIN_CASH_BUFFER_RATIO} -> {mcb}")
            MIN_CASH_BUFFER_RATIO = mcb
            RATIO_CASH_BUFFER = mcb

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


# ============================================================
# 4.1. Exclude Markets (제외 코인 필터링)
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
# 5. 상태 관리 구조 (Redis 영속화 지원)
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
        return f"mtpond:state:grid:{self.user_no}"

    def get_trend_key(self) -> str:
        return f"mtpond:state:trend:{self.user_no}"

    def get_meta_key(self) -> str:
        return f"mtpond:state:meta:{self.user_no}"

    async def save_meta(self):
        try:
            payload = {"last_rescue_exit_ts": self.last_rescue_exit_ts}
            await self.r.set(self.get_meta_key(), json.dumps(payload), ex=86400)
        except Exception:
            pass

    async def save_trend_position(self, market: str, data: dict):
        self.trend_positions[market] = data
        payload = {k: (str(v) if isinstance(v, Decimal) else v) for k, v in data.items()}
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
                else:
                    clean_data[k] = v
            await self.r.set(self.get_grid_key(), json.dumps(clean_data))
        else:
            await self.r.delete(self.get_grid_key())

    def set_cooldown(self, market: str, is_tp: bool):
        sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_DEFENSE_SEC
        now = time.time()
        self.cooldowns[market] = now + sec
        if self.user_no > 0:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.r.set(f"mtpond:cooldown:{self.user_no}:{market}", "1", ex=sec))
            except RuntimeError:
                pass
        print(f"[COOLDOWN] {market} {sec}초간 재진입 금지")

    async def is_cooling_down(self, market: str) -> bool:
        now = time.time()
        if now < self.cooldowns.get(market, 0.0):
            return True
        if market in self.cooldowns:
            del self.cooldowns[market]

        if self.user_no > 0:
            try:
                v = await self.r.get(f"mtpond:cooldown:{self.user_no}:{market}")
                if v:
                    ttl = await self.r.ttl(f"mtpond:cooldown:{self.user_no}:{market}")
                    if ttl > 0:
                        self.cooldowns[market] = now + ttl
                        return True
            except Exception:
                pass
        return False

    async def restore_state_from_redis(self, access_key: str, secret_key: str):
        print(f"🔄 [RESTORE] 시스템 재기동 감지: Redis 및 업비트 계좌 대조 복원 시작...")
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
            raw_meta = await self.r.get(self.get_meta_key())
            if raw_meta:
                meta_data = json.loads(raw_meta)
                self.last_rescue_exit_ts = float(meta_data.get("last_rescue_exit_ts", 0.0))
        except Exception:
            pass

        try:
            raw_grid = await self.r.get(self.get_grid_key())
            if raw_grid:
                grid_data = json.loads(raw_grid)
                mkt = grid_data.get("market")
                if mkt in wallet_map:
                    grid_data["base_price"] = Decimal(str(grid_data.get("base_price", "0")))
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
                        self.trend_positions[mkt] = item
                        print(f"  └ [추세 복원] {mkt} (평단: {item['avg_price']:,.0f}원)")
                    else:
                        await self.r.hdel(self.get_trend_key(), mkt)
        except Exception as e:
            print(f"[RESTORE][WARN] 추세 복원 실패: {e}")

        for mkt, w_info in wallet_map.items():
            if mkt not in self.trend_positions and (not self.grid_position or self.grid_position.get("market") != mkt):
                print(f"  └ ⚠️ [미등록 코인 감지] {mkt} → 추세 포지션 자동 편입 (수량: {w_info['total_vol']})")
                await self.save_trend_position(mkt, {"avg_price": w_info["avg_price"], "entry_ts": time.time()})

        print(
            f"✅ [RESTORE-COMPLETE] 복원 완료: 그리드={self.grid_position.get('market') if self.grid_position else '없음'}, 추세={list(self.trend_positions.keys())}")


# ============================================================
# 6. Upbit API & 호가 헬퍼
# ============================================================
def build_jwt(access_key: str, secret_key: str, query: dict = None) -> str:
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    if query:
        filtered = {k: v for k, v in query.items() if v is not None}
        query_string = urllib.parse.urlencode(filtered)
        m = hashlib.sha512()
        m.update(query_string.encode('utf-8'))
        payload['query_hash'] = m.hexdigest()
        payload['query_hash_alg'] = 'SHA512'
    return jwt.encode(payload, secret_key, algorithm="HS256")


async def upbit_api(method: str, url: str, access_key: str, secret_key: str, params: dict = None) -> Any:
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


def state_flag(state: Dict[str, Any], key: str) -> bool:
    return str(state.get(key, "")).strip().upper() in ("1", "Y", "TRUE")


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

    # 1. 구출 종료 후 전역 쿨다운 검사
    loop_now = time.time()
    if (loop_now - sm.last_rescue_exit_ts) < RESCUE_GLOBAL_COOLDOWN_SEC:
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

        # 2. 구출 예산 상한 통제 (순자산 15% 및 8만원 한도)
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
        total_equity: Decimal = Decimal("0")
) -> Decimal:
    if not ENABLE_STOP or btc_status != "NORMAL":
        return Decimal("0")
    if sum(
            1 for state in [sm.grid_position, *sm.trend_positions.values()]
            if state and state_flag(state, "is_rescued")
    ) >= MAX_CONCURRENT_RESCUE:
        return Decimal("0")

    excluded = await get_excluded_markets()
    pattern_rows = await fetch_all_candle_patterns()
    ranks = {}
    for row in pattern_rows:
        if not isinstance(row, dict):
            continue
        market = row.get("market", "")
        if not market.startswith("KRW-"):
            continue
        try:
            ranks[market] = (
                int(row.get("overall_score", 0)),
                int((row.get("trade_activity") or {}).get("trade_count_10m", 0)),
            )
        except (TypeError, ValueError):
            continue

    candidates = []
    positions = []
    if sm.grid_position:
        positions.append((sm.grid_position["market"], sm.grid_position, True))
    positions.extend(
        (market, state, False) for market, state in sm.trend_positions.items()
    )
    for market, state, is_grid in positions:
        if market in excluded:
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
# 7. 비트코인 3단계 방어 체력 진단 (30초 캐싱) & 외부 캔들 추세 서버 연동
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


async def get_trend_scalping_candidates() -> List[Dict[str, Any]]:
    """추세 스캘핑용: advisor의 5대 안전장치 및 마켓레짐을 통과한 엄선된 추천 종목(recommendation)만 추출"""
    excluded = await get_excluded_markets()
    coins = await fetch_all_candle_patterns()
    qualified = []
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in excluded: continue

        # [핵심] advisor의 안전 추천 필터를 통과하지 못한 코인은 매수 완전 배제
        rec = c.get("recommendation")
        if not rec or not isinstance(rec, dict):
            continue

        try:
            score = int(c.get("overall_score", 0))
            price = float(c.get("price", 0))
            trades = int((c.get("trade_activity") or {}).get("trade_count_10m", 0))
            p_score = float(rec.get("priority_score", 0))
        except (ValueError, TypeError):
            continue

        if trades >= 25 and price >= 30:
            qualified.append({
                "market": mkt,
                "overall_score": score,
                "priority_score": p_score,
                "trade_count": trades,
                "price": price,
                "recommendation": rec
            })

    qualified.sort(key=lambda x: (x["priority_score"], x["overall_score"], x["trade_count"]), reverse=True)
    return qualified


async def get_grid_active_candidates() -> List[Dict[str, Any]]:
    """그리드 하베스트용: 점수 무관, 10분 체결 빈도가 가장 높은 횡보형 종목 추출"""
    excluded = await get_excluded_markets()
    coins = await fetch_all_candle_patterns()
    candidates = []
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-") or mkt in excluded: continue
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
    """업비트 실시간 15분봉 정배열(5 > 20) & 양봉 재확인"""
    try:
        url = "https://api.upbit.com/v1/candles/minutes/15"
        client = get_http_client()
        res = await client.get(url, params={"market": market, "count": 20})
        if res.status_code != 200: return False
        candles = res.json()

        cur_price = Decimal(str(candles[0]["trade_price"]))
        open_price = Decimal(str(candles[0]["opening_price"]))
        closes = [Decimal(str(c["trade_price"])) for c in candles]
        ma5 = sum(closes[:5]) / Decimal("5")
        ma20 = sum(closes[:20]) / Decimal("20")

        return bool((cur_price >= ma5) and (ma5 > ma20) and (cur_price >= open_price))
    except Exception:
        return False


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
# 8. 사다리 익절 주문 배치 함수
# ============================================================
async def place_ladder_tp_orders(access_key: str, secret_key: str, market: str, ps: CompositeStateManager = None):
    try:
        async with get_market_lock(market):
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

            slot_count = max(1, min(10, int(total_eval // Decimal("10000"))))
            slice_vol = quantize_volume(avail_bal / Decimal(str(slot_count)))
            rem_vol = avail_bal
            prev_limit = Decimal("0")

            print(f"📐 [LADDER-TP] {market} 사다리 익절 발주 (평단: {avg_price:,.0f}원 | 총액: {total_eval:,.0f}원 | {slot_count}슬롯)")

            for i in range(1, slot_count + 1):
                curr_target_pct = start_pct + (step_pct * Decimal(str(i - 1)))
                target_p = avg_price * (Decimal("1") + (curr_target_pct / Decimal("100")))
                limit_p = adjust_price_to_tick(target_p, side="ask")

                if i > 1 and limit_p <= prev_limit:
                    limit_p = prev_limit + get_tick_unit(limit_p)

                order_v = quantize_volume(rem_vol) if i == slot_count else slice_vol
                order_v = min(order_v, rem_vol)

                if order_v * limit_p >= MIN_ORDER_NOTIONAL_KRW and LIVE_TRADING:
                    await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                        "market": market, "side": "ask", "volume": str(order_v), "price": str(limit_p), "ord_type": "limit"
                    })
                    print(f"  └ [슬롯 #{i}] {order_v}개 @ {limit_p}원 (+{curr_target_pct:.2f}%)")
                    rem_vol -= order_v
                    prev_limit = limit_p
                    await asyncio.sleep(0.15)

            if ps and market in ps.trend_positions:
                ps.trend_positions[market]["last_order_ts"] = time.time()
                await ps.save_trend_position(market, ps.trend_positions[market])

    except Exception as e:
        print(f"[LADDER-TP][ERR] {market} 사다리 주문 실패: {e}")


# ============================================================
# 9. 단일 종목 그리드 하베스트 발주 및 관리
# ============================================================
async def deploy_single_grid_harvest(access_key: str, secret_key: str, market: str, budget: Decimal, step_amt: Decimal,
                                     sm: CompositeStateManager):
    print(f"🕸️ [GRID-HARVEST] {market} 그리드 배치 (총예산: {budget:,.0f}원 | 단계당: {step_amt:,.0f}원)")
    async with get_market_lock(market):
        await cancel_all_orders(access_key, secret_key, market)

        t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": market})
        cur_p = Decimal(str(t_res[0]["trade_price"]))
        grid_orders = []

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
            if LIVE_TRADING:
                r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                    "market": market, "side": "bid", "volume": str(vol), "price": str(limit_buy_p), "ord_type": "limit"
                })
                grid_orders.append(r.get("uuid"))
                print(f"  └ [그리드 매수망 #{i + 1}] {vol}개 @ {limit_buy_p}원 (호가 분리)")
                await asyncio.sleep(0.15)

            prev_buy_p = limit_buy_p

        await sm.save_grid_position({
            "market": market,
            "base_price": cur_p,
            "grid_orders": grid_orders,
            "last_tp_uuid": None,
            "deployed_ts": time.time(),
            "deploying": False
        })


async def manage_grid_position(
        access_key: str,
        secret_key: str,
        sm: CompositeStateManager,
        available_krw: Decimal,
        btc_status: str
) -> Decimal:
    if not sm.grid_position or sm.grid_position.get("deploying"):
        return Decimal("0")

    mkt = sm.grid_position["market"]
    async with get_market_lock(mkt):
        accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
        base = mkt.split("-")[1]
        acc = next((a for a in accounts if a.get("currency") == base), None)

        if not acc:
            sm.grid_position = None
            await sm.save_grid_position(None)
            return Decimal("0")

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        total_vol = bal + locked
        avg_p = Decimal(str(acc.get("avg_buy_price", "0")))

        if total_vol <= 0 or (total_vol * avg_p) < MIN_ORDER_NOTIONAL_KRW:
            print(f"🎉 [GRID-COMPLETE] {mkt} 횡보 그리드 목표 달성 및 청산 완료!")
            await cancel_all_orders(access_key, secret_key, mkt)
            sm.set_cooldown(mkt, is_tp=True)
            await sm.save_grid_position(None)
            return Decimal("0")

        t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": mkt})
        cur_p = Decimal(str(t_res[0]["trade_price"]))
        if avg_p <= 0 or cur_p <= 0: return Decimal("0")

        pnl = ((cur_p - avg_p) / avg_p) * Decimal("100")

        grid_state = sm.grid_position

        # 1. 구출 상태 확인
        if state_flag(grid_state, "is_rescued"):
            await handle_rescue_stop(
                access_key, secret_key, sm, mkt, grid_state, cur_p, pnl, is_grid=True
            )
            return Decimal("0")

        # 2. 그리드 하드 손절 및 대폭락 강제 손절 (코인별 30분봉 동적 진동폭 연동)
        limits = await get_dynamic_stop_limits(mkt, patterns_data=_patterns_cache.get("data"), client=get_http_client())
        dyn_hard_stop = limits["hard_stop"]
        dyn_max_drop = limits["max_drop"]

        is_hard_stop = bool(ENABLE_STOP and pnl <= dyn_hard_stop)
        is_max_drop = bool(ENABLE_STOP and pnl <= dyn_max_drop)
        if is_hard_stop or is_max_drop:
            reason = f"DYNAMIC_HARD_STOP({dyn_hard_stop}%)" if is_hard_stop else f"DYNAMIC_MAX_DROP({dyn_max_drop}%)"
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
            return Decimal("0")

        # 3. TP 발주 및 수량 변화 시 재배치
        target_tp_p = adjust_price_to_tick(avg_p * (Decimal("1") + (GRID_TARGET_TP_PCT / Decimal("100"))), side="ask")
        current_tp_vol = Decimal(str(grid_state.get("tp_volume", "0")))

        # 잔고가 있고(bal > 0), 아직 TP가 안 걸렸거나 잔고 수량이 달라졌을 때 (추가 매수 체결 등)
        if bal * cur_p >= MIN_ORDER_NOTIONAL_KRW and (not grid_state.get("last_tp_uuid") or bal != current_tp_vol):
            # 기존 TP 주문이 있었다면 취소
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
                print(f"🎯 [GRID-TP-PLACED] {mkt} 평단({avg_p:,.0f}원) 대비 +{GRID_TARGET_TP_PCT}% 매도 등록: {quant_vol}개 @ {target_tp_p}원")
    return Decimal("0")


# ============================================================
# 10. 메인 트레이딩 엔진 루프 (비트코인 3단계 방어 통합)
# ============================================================
async def run_hybrid_engine(user_no: int, server_no: int):
    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2
                      FROM traceUser
                      WHERE userNo = :u
                        AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        row = result.fetchone()
        if not row: return
        access_key, secret_key = row[0], row[1]

    # ★ Redis 최신 설정 동기화
    await sync_redis_config(user_no)

    sm = CompositeStateManager(user_no, redis_client)
    await sm.restore_state_from_redis(access_key, secret_key)

    last_heartbeat_ts = time.time()
    last_cfg_sync = time.time()
    last_save_ts = time.time()
    print(f"=== [복합 포트폴리오 엔진 가동] 설정비율: 추세 {int(RATIO_TREND*100)}% / 그리드 {int(RATIO_GRID*100)}% / 현금버퍼 {int(MIN_CASH_BUFFER_RATIO*100)}% (BTC 3단계 방어 활성) ===")

    while True:
        try:
            loop_now = time.time()

            # 주기적 Redis 설정 동기화
            if loop_now - last_cfg_sync >= CONTROLLER_POLL_SEC:
                last_cfg_sync = loop_now
                await sync_redis_config(user_no)

            # 주기적 Redis 상태 저장
            if loop_now - last_save_ts >= 30.0:
                last_save_ts = loop_now
                await sm.save_meta()

            # ★ 비트코인 3단계 방어 체력 진단 호출 (30초 캐싱)
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]
            btc_msg = btc_health["msg"]

            excluded = await get_excluded_markets()

            # 1. 잔고 및 보유 코인 집계
            accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
            krw_balance = Decimal("0")
            held_coins = {}

            for a in accounts:
                cur = a.get("currency")
                if cur == "KRW":
                    krw_balance = Decimal(str(a.get("balance", "0")))
                elif cur:
                    mkt = f"KRW-{cur}"
                    if mkt not in excluded:
                        tot_v = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                        if tot_v > 0:
                            held_coins[mkt] = tot_v

            price_map = await fetch_current_prices(
                list(held_coins.keys()), access_key, secret_key
            )

            # 2. 미배치 잔여 코인 자동 감지 및 재매도 트리거
            for a in accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW": continue
                mkt = f"KRW-{cur}"
                if mkt in excluded: continue
                if sm.grid_position and sm.grid_position.get("market") == mkt: continue

                b_avail = Decimal(str(a.get("balance", "0")))
                b_avg = Decimal(str(a.get("avg_buy_price", "0")))
                if b_avail * b_avg >= MIN_ORDER_NOTIONAL_KRW:
                    st = sm.trend_positions.setdefault(mkt,
                                                       {"avg_price": b_avg, "entry_ts": time.time(), "realloc_ts": 0.0})
                    current_price = price_map.get(mkt, Decimal("0"))
                    position_pnl = ((current_price - b_avg) / b_avg * Decimal("100")) if b_avg > 0 else Decimal("0")
                    if state_flag(st, "is_rescued") or (
                            ENABLE_STOP and RESCUE_MAX_DROP < position_pnl <= RESCUE_MIN_DROP):
                        continue
                    if loop_now - float(st.get("realloc_ts", 0.0)) >= 10.0:
                        st["realloc_ts"] = loop_now
                        print(f"📥 [AUTO-REALLOC] {mkt} 미배치 잔고({b_avail}개) 감지 → 사다리 재배치 실행")
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, mkt, sm))

            # 3. 총자산 산출
            total_coin_val = Decimal("0")
            for mkt, vol in held_coins.items():
                total_coin_val += vol * price_map.get(mkt, Decimal("0"))

            total_equity = krw_balance + total_coin_val
            port_cfg = calculate_dynamic_portfolio(total_equity)
            trend_per_trade = port_cfg["trend_per_trade"]
            max_trend_slots = port_cfg["trend_markets"]
            grid_budget = port_cfg["grid_budget"]
            grid_step_amt = port_cfg["grid_step_amt"]
            cash_buffer_limit = port_cfg["cash_buffer_limit"]

            # 30% 현금 안전 버퍼 보호
            investable_krw = max(Decimal("0"), krw_balance - cash_buffer_limit)

            if loop_now - last_heartbeat_ts >= 60:
                last_heartbeat_ts = loop_now
                grid_status = sm.grid_position.get("market") if sm.grid_position else "없음"
                trend_status = list(sm.trend_positions.keys()) if sm.trend_positions else "없음"
                print(
                    f"💓 [HEARTBEAT] 총자산: {total_equity:,.0f}원 (원화: {krw_balance:,.0f}원 / 버퍼: {cash_buffer_limit:,.0f}원) | "
                    f"가용투자: {investable_krw:,.0f}원 | 그리드: {grid_status} | 추세: {trend_status} | 시장상태: {btc_status}"
                )

            # 4. 구출 작전 관리 (investable_krw 및 total_equity 반영)
            rescue_used = await process_rescue_candidates(
                access_key, secret_key, sm,
                {f"KRW-{a['currency']}": a for a in accounts if a.get("currency") not in (None, "KRW")},
                price_map, investable_krw, btc_status, total_equity
            )
            krw_balance -= rescue_used
            investable_krw = max(Decimal("0"), investable_krw - rescue_used)

            # 5. 그리드 포지션 관리
            grid_rescue_used = await manage_grid_position(
                access_key, secret_key, sm, investable_krw, btc_status
            )
            krw_balance -= grid_rescue_used
            investable_krw = max(Decimal("0"), investable_krw - grid_rescue_used)

            # 6. 추세 포지션 관리
            for mkt in list(sm.trend_positions.keys()):
                if mkt not in held_coins:
                    print(f"🎉 [TREND-COMPLETE] {mkt} 모든 사다리 익절 체결 완료!")
                    sm.set_cooldown(mkt, is_tp=True)
                    await sm.remove_trend_position(mkt)
                    continue

                base = mkt.split("-")[1]
                acc_info = next((a for a in accounts if a.get("currency") == base), None)
                if acc_info:
                    real_avg = Decimal(str(acc_info.get("avg_buy_price", "0")))
                    if real_avg > 0: sm.trend_positions[mkt]["avg_price"] = real_avg

                avg_p = sm.trend_positions[mkt]["avg_price"]
                if avg_p <= 0: continue

                t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key,
                                        {"markets": mkt})
                cur_p = Decimal(str(t_res[0]["trade_price"]))
                pnl = ((cur_p - avg_p) / avg_p) * Decimal("100")

                state = sm.trend_positions[mkt]

                # A. 구출된 종목 관리
                if state_flag(state, "is_rescued"):
                    await handle_rescue_stop(
                        access_key, secret_key, sm, mkt, state, cur_p, pnl, is_grid=False
                    )
                    continue

                # B. 추세 포지션 하드 손절 및 대폭락 강제 손절 (코인별 30분봉 동적 진동폭 연동)
                limits = await get_dynamic_stop_limits(mkt, patterns_data=_patterns_cache.get("data"), client=get_http_client())
                dyn_hard_stop = limits["hard_stop"]
                dyn_max_drop = limits["max_drop"]

                is_hard_stop = bool(ENABLE_STOP and pnl <= dyn_hard_stop)
                is_max_drop = bool(ENABLE_STOP and pnl <= dyn_max_drop)
                if is_hard_stop or is_max_drop:
                    reason = f"DYNAMIC_HARD_STOP({dyn_hard_stop}%)" if is_hard_stop else f"DYNAMIC_MAX_DROP({dyn_max_drop}%)"
                    print(f"🚨 [TREND-STOP] {mkt} 추세 포지션 손절 발동 ({reason}, PnL: {pnl:.2f}%) → 전량 시장가 청산")
                    async with get_market_lock(mkt):
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
                                await sm.remove_trend_position(mkt)
                    continue

            # =============================================================
            # ★ 7. 신규 매수 탐색 (for mkt 루프 밖으로 독립 분리된 정상 인덴트!)
            # =============================================================
            if btc_status == "EMERGENCY":
                print(f"🛑 {btc_msg} -> 모든 신규 매수 및 진입을 전면 차단합니다.")
            elif btc_status == "CAUTION":
                print(f"⚠️ {btc_msg} -> 조정세 구간: 신규 매수를 일시 보류합니다.")
            else:
                # [NORMAL 상태]
                trend_slots_remain = max_trend_slots - len(sm.trend_positions)
                trend_executed = False  # 이번 루프에서 추세 스캘핑 진입 여부 추적

                # -------------------------------------------------------------
                # [1순위] 추세 스캘핑 적극 탐색 (상승 모멘텀 종목 우선 공략)
                # -------------------------------------------------------------
                if trend_slots_remain > 0 and investable_krw >= trend_per_trade:
                    trend_candidates = await get_trend_scalping_candidates()
                    for cand_item in trend_candidates:
                        cand = cand_item["market"]
                        if cand in excluded:
                            continue
                        grid_mkt = sm.grid_position.get("market") if sm.grid_position else None
                        if cand not in held_coins and cand != grid_mkt:
                            if not await sm.is_cooling_down(cand) and await check_bullish_trend(cand):
                                async with get_market_lock(cand):
                                    t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key,
                                                            secret_key, {"markets": cand})
                                    cur_p = Decimal(str(t_res[0]["trade_price"]))

                                    print(
                                        f"🚀 [TREND-BUY] 모멘텀 진입: {cand} (점수: {cand_item['overall_score']}) | 금액: {trend_per_trade:,.0f}원")
                                    if LIVE_TRADING:
                                        await order_market_buy_price(
                                            access_key, secret_key, cand, trend_per_trade
                                        )
                                    await sm.save_trend_position(cand,
                                                                    {"avg_price": cur_p, "entry_ts": time.time()})
                                    asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, cand, sm))
                                    krw_balance -= trend_per_trade
                                    investable_krw = max(Decimal("0"), investable_krw - trend_per_trade)
                                    trend_executed = True
                                    break

                # -------------------------------------------------------------
                # [2순위] 추세 스캘핑이 저조할 때(진입 종목이 없을 때) 그리드 하베스트 발동
                # -------------------------------------------------------------
                if not trend_executed and sm.grid_position is None and investable_krw >= grid_budget:
                    grid_candidates = await get_grid_active_candidates()
                    for cand_item in grid_candidates:
                        cand = cand_item["market"]
                        if cand in excluded:
                            continue
                        if cand not in held_coins:
                            if not await sm.is_cooling_down(cand) and await check_sideways_grid_candidate(cand):
                                async with get_market_lock(cand):
                                    print(
                                        f"🎯 [GRID-PICK] 추세 스캘핑 부진/조건 미달 → 횡보 그리드 하베스트 전환 발동: {cand} (10분 거래수: {cand_item['trade_count']})")
                                    sm.grid_position = {"market": cand, "deploying": True}
                                    asyncio.create_task(
                                        deploy_single_grid_harvest(access_key, secret_key, cand, grid_budget,
                                                                    grid_step_amt, sm))
                                    krw_balance -= grid_budget
                                    investable_krw = max(Decimal("0"), investable_krw - grid_budget)
                                    break

        except Exception as e:
            print(f"[HYBRID-LOOP][ERR] {e}")

        await asyncio.sleep(4)


# ============================================================
# 11. Redis On/Off 감시 컨트롤러 및 진입점
# ============================================================
async def run_mtpond_controller(user_no: int, server_no: int):
    task: Optional[asyncio.Task] = None
    print(f"[CTRL] Controller 시작 (User: {user_no}, Server: {server_no})")

    while True:
        try:
            # 1. Redis 최신 설정 먼저 로드
            await sync_redis_config(user_no)

            active_flag = True
            raw_status = await redis_client.get(f"mtpond:status:{user_no}")
            if raw_status is not None:
                active_flag = raw_status.strip().upper() in ("ON", "1", "Y", "TRUE")

            c = await redis_client.get(f"mtpond:setup:{user_no}")
            cfg = None
            if c:
                try:
                    data = json.loads(c)
                    cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
                except Exception:
                    pass

            if not cfg and API_BASE:
                try:
                    cli = get_http_client()
                    response = await cli.get(
                        f"{API_BASE}/api/mtpondsetup/{user_no}"
                    )
                    if response.status_code == 200:
                        data = response.json()
                        cfg = data[0] if isinstance(data, list) and data else data
                except Exception:
                    pass

            if cfg and isinstance(cfg, dict):
                apply_dynamic_config(cfg)
                if raw_status is None and "activeYN" in cfg:
                    active_flag = str(cfg.get("activeYN", "Y")).strip().upper() == "Y"

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
    try:
        user_no = int(os.getenv("USER_NO", "100001"))
        server_no = int(os.getenv("SERVER_NO", "21"))
        await sync_redis_config(user_no)
        await run_mtpond_controller(user_no, server_no)
    finally:
        await close_http_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 봇을 안전하게 종료합니다.")
