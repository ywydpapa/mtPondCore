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
import dotenv
import httpx
import jwt
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

# 주기 리셋 설정 (1일)
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

# ============================================================
# 4. STATIC CONFIG (설정 및 신규 조건)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = int(os.getenv("BB_CACHE_TTL_SEC", "180"))
COOLDOWN_AFTER_SELL_SEC = int(os.getenv("COOLDOWN_AFTER_SELL_SEC", "180"))

BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "60"))

ARM_PNL = Decimal("0.25")
HARD_TP = Decimal("0.6")
HARD_TP2_OFFSET = Decimal(os.getenv("HARD_TP2_OFFSET", "0.5"))
HARD_TP2_BASE = HARD_TP + HARD_TP2_OFFSET
TRAIL_DROP = Decimal("0.15")
TRAIL_START_PNL = Decimal("0.45")

SELL_PORTION = Decimal(os.getenv("SELL_PORTION", "1.0"))
HARD_TP_SELL_PORTION = Decimal(os.getenv("HARD_TP_SELL_PORTION", "1.0"))
HARD_TP2_SELL_PORTION = Decimal(os.getenv("HARD_TP2_SELL_PORTION", "1.0"))

INTERVAL_SECONDS = 5
MIN_NOTIONAL_KRW = Decimal("5000")
MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
ORDER_NOTIONAL_BUFFER_PCT = Decimal("0.01")
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"

PREPLACE_HARD_TP = os.getenv("PREPLACE_HARD_TP", "1") == "1"
PREPLACE_TP_PORTION = os.getenv("PREPLACE_TP_PORTION")
PREPLACE_TP_TIMEOUT_SEC = int(os.getenv("PREPLACE_TP_TIMEOUT_SEC", "0"))
PREPLACE_ON_START = os.getenv("PREPLACE_ON_START", "0") == "1"

MAX_ADDITIONAL_BUYS = int(os.getenv("MAX_ADDITIONAL_BUYS", "5"))

# [쿨다운 사유별 분리 설정]
COOLDOWN_TP_SEC = int(os.getenv("COOLDOWN_TP_SEC", "60"))            # 정상 익절 후 1분
COOLDOWN_DEFENSE_SEC = int(os.getenv("COOLDOWN_DEFENSE_SEC", "1800")) # 손절/급락방어 후 30분

# ============================================================
# [신규 진입 필터 강화] 3분봉 수급 및 이평선 돌파 확인 엔진
# ============================================================
_candle_3m_cache: Dict[str, Tuple[float, bool]] = {}
CANDLE_3M_CACHE_TTL = 30

async def check_3m_candle_trend(market: str) -> bool:
    """
    3분봉 기준 강화된 진입 조건:
    1. 현재 3분봉이 확실한 양봉 (몸통 크기 >= 0.15%)
    2. 현재가가 최근 4개 봉 평균(단기 이평) 위
    3. 직전 봉이 음봉이었을 경우, 직전 음봉의 중심값(50%) 이상을 뚫고 올라왔는지 확인
    4. 거래량이 실린 반등인지 확인 (현재 봉 거래량 > 직전 봉 거래량의 70%)
    """
    now = time.time()
    cached = _candle_3m_cache.get(market)
    if cached and (now - cached[0]) < CANDLE_3M_CACHE_TTL:
        return cached[1]

    url = "https://api.upbit.com/v1/candles/minutes/3"
    params = {"market": market, "count": 5}

    try:
        client = get_http_client()
        resp = await client.get(url, params=params)
        if resp.status_code == 200:
            candles = resp.json()
            if isinstance(candles, list) and len(candles) >= 4:
                c0 = candles[0]  # 현재 3분봉
                c1 = candles[1]  # 직전 3분봉
                c2 = candles[2]  # 전전 3분봉

                c0_close = Decimal(str(c0["trade_price"]))
                c0_open = Decimal(str(c0["opening_price"]))
                c0_vol = Decimal(str(c0["candle_acc_trade_volume"]))

                c1_close = Decimal(str(c1["trade_price"]))
                c1_open = Decimal(str(c1["opening_price"]))
                c1_vol = Decimal(str(c1["candle_acc_trade_volume"]))

                # 1) 현재 봉이 명확한 양봉인지 (몸통 0.15% 이상)
                body_pct = (c0_close - c0_open) / c0_open * Decimal("100")
                if body_pct < Decimal("0.15"):
                    _candle_3m_cache[market] = (now, False)
                    return False

                # 2) 최근 4개 봉 평균(단기 이평선) 위인지 검증
                closes = [Decimal(str(c["trade_price"])) for c in candles[:4]]
                sma_short = sum(closes) / Decimal("4")
                if c0_close <= sma_short:
                    _candle_3m_cache[market] = (now, False)
                    return False

                # 3) 직전 캔들이 음봉이었다면, 음봉 몸통 중심값 이상 회복했는지 검증
                if c1_close < c1_open:
                    c1_mid = (c1_open + c1_close) / Decimal("2")
                    if c0_close < c1_mid:
                        _candle_3m_cache[market] = (now, False)
                        return False

                # 4) 직전 2개 연속 음봉으로 흘러내리던 밴드워크 하락장 직후인지 검증
                c2_close = Decimal(str(c2["trade_price"]))
                c2_open = Decimal(str(c2["opening_price"]))
                if (c1_close < c1_open) and (c2_close < c2_open):
                    # 연속 음봉 후 첫 양봉일 땐 직전 음봉의 시가(완전 장악)를 넘어서야 인정
                    if c0_close < c1_open:
                        _candle_3m_cache[market] = (now, False)
                        return False

                # 5) 거래량 동반 여부 (완전 꼬리없는 거래량 절벽 양봉 배제)
                if c1_vol > 0 and (c0_vol < c1_vol * Decimal("0.7")):
                    _candle_3m_cache[market] = (now, False)
                    return False

                _candle_3m_cache[market] = (now, True)
                return True
    except Exception as e:
        print(f"[CANDLE-3M][WARN] {market} 3분봉 진입 검증 실패: {e}")

    return False

# ============================================================
# 5. RUNTIME CONFIG (동적 설정 관리)
# ============================================================
ENABLE_STOP = (os.getenv("ENABLE_STOP", "1").upper() in ("1", "Y", "TRUE"))
STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))

_CONFIG_INITIALIZED = False

class PositionState:
    def __init__(self, user_no: int = 0):
        self.user_no = user_no
        self.data: Dict[str, Dict[str, Any]] = {}
        self.sell_history: Dict[str, Dict[str, Any]] = {}
        self.buy_info: Dict[str, Dict[str, Any]] = {}

    def update_or_init(self, market: str, pnl: Decimal, avg_price: Decimal):
        now = time.time()
        st = self.data.get(market)
        if st is None or st.get("avg_buy_price") != avg_price:
            self.data[market] = {
                "peak_pnl": pnl,
                "min_pnl": pnl,
                "last_drawdown": Decimal("0"),
                "max_drawdown": Decimal("0"),
                "armed": False,
                "avg_buy_price": avg_price,
                "hard_tp_taken": False,
                "hard_tp2_taken": False,
                "last_update_ts": now,
                "active_limit_uuid": None,
                "limit_pending_category": None,
                "limit_submit_ts": None,
                "last_additional_buy_ts": now,
                "last_additional_buy_price": avg_price,
                "last_additional_buy_pnl": Decimal("0")
            }
            return self.data[market]

        if pnl > st["peak_pnl"]:
            st["peak_pnl"] = pnl
        if pnl < st.get("min_pnl", pnl):
            st["min_pnl"] = pnl

        drawdown = st["peak_pnl"] - pnl if pnl < st["peak_pnl"] else Decimal("0")
        st["last_drawdown"] = drawdown
        if drawdown > st.get("max_drawdown", Decimal("0")):
            st["max_drawdown"] = drawdown

        st["last_update_ts"] = now
        return st

    def mark_sold(self, market: str, reason: str = "DEFENSE"):
        """
        매도 사유에 따른 차등 쿨다운 등록
        - 정상 익절/약익절: 60초
        - 손절/급락방어: 1800초 (30분)
        """
        now = time.time()
        r = reason.upper()
        if r in ("TP", "HARD_TP", "TP_DONE", "DYNAMIC_TP", "WEAK_TP"):
            cooldown_sec = COOLDOWN_TP_SEC
            tag = "익절/약익절"
        elif r in ("LOSS_CUT", "PANIC_EXIT", "DEFENSE"):
            cooldown_sec = COOLDOWN_DEFENSE_SEC
            tag = "손절/급락방어"
        else:
            cooldown_sec = COOLDOWN_DEFENSE_SEC
            tag = "기타방어"

        expire_ts = now + cooldown_sec
        self.sell_history[market] = {
            "ts": now,
            "expire_ts": expire_ts,
            "reason": reason,
            "tag": tag
        }

        # 내부 추적 상태 정리
        self.data.pop(market, None)
        self.buy_info.pop(market, None)

        asyncio.create_task(self._set_redis_cooldown(market, cooldown_sec))
        print(f"[COOLDOWN] {market} 매도 완료 [{tag}: {reason}] → 재매수 금지 {cooldown_sec}초 적용 (만료: {int(cooldown_sec)}초 후)")

    async def _set_redis_cooldown(self, market: str, cooldown_sec: int):
        if self.user_no > 0:
            try:
                redis_key = f"mtpond:cooldown:{self.user_no}:{market}"
                await redis_client.set(redis_key, "1", ex=cooldown_sec)
            except Exception:
                pass

    async def recently_sold(self, market: str) -> bool:
        """해당 코인의 쿨다운 만료 여부 판별 (메모리 우선, 없을 시 Redis 조회)"""
        now = time.time()
        hist = self.sell_history.get(market)
        if hist is not None:
            if now < hist["expire_ts"]:
                return True
            else:
                del self.sell_history[market]
                return False

        # 프로세스 재시작 등으로 메모리에 없을 때만 Redis 확인
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

    def reduce_invested_after_sell(self, market: str, portion: Decimal):
        info = self.buy_info.get(market)
        if not info or portion <= 0 or portion >= 1:
            return
        info["total_invested"] = (info["total_invested"] * (Decimal("1") - portion)).quantize(Decimal("0.0001"))

def init_config(force: bool = False):
    global _CONFIG_INITIALIZED
    if _CONFIG_INITIALIZED and not force:
        return

    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "10"))
    RANGE_BUY_KRW = Decimal(os.getenv("RANGE_BUY_KRW", "40000"))
    ADDITIONAL_BUY_KRW = Decimal(os.getenv("ADDITIONAL_BUY_KRW", "40000"))
    MAX_TOTAL_INVEST_PER_MARKET = Decimal(os.getenv("MAX_TOTAL_INVEST_PER_MARKET", "400000"))
    BUY_RANGE_LOW = Decimal(os.getenv("BUY_RANGE_LOW", "-2.2"))
    BUY_RANGE_HIGH = Decimal(os.getenv("BUY_RANGE_HIGH", "0.45"))
    STOP_TRIGGER_PNL = Decimal(os.getenv("STOP_TRIGGER_PNL", "-1.2"))
    LOSS_CUT_RATE = Decimal(os.getenv("LOSS_CUT_RATE", "-3.0"))
    ENABLE_STOP = (str(os.getenv("ENABLE_STOP", "1")).upper() in ("1", "Y", "TRUE"))
    BB_MIN_OVERALL_SCORE = int(os.getenv("BB_MIN_OVERALL_SCORE", "3"))
    BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "60"))

    _CONFIG_INITIALIZED = True
    print(f"[INIT] 설정 초기화 완료 (force={force}) | 추천점수>={BB_MIN_OVERALL_SCORE}, 10분체결>={BB_MIN_TRADE_COUNT_10M}건")

def apply_dynamic_config(cfg: dict):
    global MAX_ACTIVE_MARKETS, RANGE_BUY_KRW, ADDITIONAL_BUY_KRW
    global MAX_TOTAL_INVEST_PER_MARKET, BUY_RANGE_LOW, BUY_RANGE_HIGH
    global STOP_TRIGGER_PNL, LOSS_CUT_RATE, ENABLE_STOP
    global BB_MIN_OVERALL_SCORE, BB_MIN_TRADE_COUNT_10M

    changes = []

    def to_decimal_safe(v):
        try:
            if v is None or str(v).strip() == "":
                return None
            return Decimal(str(v))
        except Exception:
            return None

    if "maxCoincnt" in cfg:
        try:
            nv = int(cfg["maxCoincnt"])
            if nv > 0 and nv != MAX_ACTIVE_MARKETS:
                changes.append(f"MAX_ACTIVE_MARKETS {MAX_ACTIVE_MARKETS} -> {nv}")
                MAX_ACTIVE_MARKETS = nv
        except Exception:
            pass

    if "initAmt" in cfg:
        val = to_decimal_safe(cfg.get("initAmt"))
        if val and val > 0 and val != RANGE_BUY_KRW:
            changes.append(f"RANGE_BUY_KRW {RANGE_BUY_KRW} -> {val}")
            RANGE_BUY_KRW = val

    if "addAmt" in cfg:
        val = to_decimal_safe(cfg.get("addAmt"))
        if val and val > 0 and val != ADDITIONAL_BUY_KRW:
            changes.append(f"ADDITIONAL_BUY_KRW {ADDITIONAL_BUY_KRW} -> {val}")
            ADDITIONAL_BUY_KRW = val

    if "limitAmt" in cfg:
        val = to_decimal_safe(cfg.get("limitAmt"))
        if val and val > 0 and val != MAX_TOTAL_INVEST_PER_MARKET:
            changes.append(f"MAX_TOTAL_INVEST_PER_MARKET {MAX_TOTAL_INVEST_PER_MARKET} -> {val}")
            MAX_TOTAL_INVEST_PER_MARKET = val

    if "minMargin" in cfg:
        raw = to_decimal_safe(cfg.get("minMargin"))
        if raw is not None and raw != BUY_RANGE_LOW:
            changes.append(f"BUY_RANGE_LOW {BUY_RANGE_LOW} -> {raw}")
            BUY_RANGE_LOW = raw

    if "maxMargin" in cfg:
        raw = to_decimal_safe(cfg.get("maxMargin"))
        if raw is not None and raw != BUY_RANGE_HIGH:
            changes.append(f"BUY_RANGE_HIGH {BUY_RANGE_HIGH} -> {raw}")
            BUY_RANGE_HIGH = raw

    if "lcRate" in cfg:
        val = to_decimal_safe(cfg.get("lcRate"))
        if val is not None and val != LOSS_CUT_RATE:
            changes.append(f"LOSS_CUT_RATE {LOSS_CUT_RATE} -> {val}")
            LOSS_CUT_RATE = val

    if "stopYN" in cfg:
        flag = str(cfg.get("stopYN")).upper() in ("1", "Y", "TRUE")
        if flag != ENABLE_STOP:
            changes.append(f"ENABLE_STOP {ENABLE_STOP} -> {flag}")
            ENABLE_STOP = flag

    if "minScore" in cfg:
        try:
            ms = int(cfg["minScore"])
            if ms != BB_MIN_OVERALL_SCORE:
                changes.append(f"BB_MIN_OVERALL_SCORE {BB_MIN_OVERALL_SCORE} -> {ms}")
                BB_MIN_OVERALL_SCORE = ms
        except Exception:
            pass

    if "minTrades10m" in cfg:
        try:
            mt = int(cfg["minTrades10m"])
            if mt != BB_MIN_TRADE_COUNT_10M:
                changes.append(f"BB_MIN_TRADE_COUNT_10M {BB_MIN_TRADE_COUNT_10M} -> {mt}")
                BB_MIN_TRADE_COUNT_10M = mt
        except Exception:
            pass

    if changes:
        print("[CFG] 동적 설정 업데이트 반영:\n  " + "\n  ".join(changes))


# ============================================================
# [듀얼 타임프레임] 3분봉 & 10분봉 동시 하락 국면 판정 엔진
# ============================================================
_dual_trend_cache: Dict[str, Tuple[float, bool]] = {}


async def check_dual_timeframe_downtrend(market: str) -> bool:
    """
    3분봉과 10분봉이 둘 다 하락 추세(Downtrend)인지 동시 판별
    True 반환 시: 두 타임프레임 모두 하락 국면 -> 확실한 손절 타이밍
    False 반환 시: 한쪽이라도 반등/지지가 남아있음 -> 섣부른 손절 방지
    """
    now = time.time()
    cached = _dual_trend_cache.get(market)
    if cached and (now - cached[0]) < 15:  # 15초 캐시
        return cached[1]

    client = get_http_client()
    down_3m = False
    down_10m = False

    try:
        # 1. 3분봉 확인 (최근 3개)
        r3 = await client.get("https://api.upbit.com/v1/candles/minutes/3", params={"market": market, "count": 3})
        if r3.status_code == 200:
            c3 = r3.json()
            if len(c3) >= 2:
                c0_price = Decimal(str(c3[0]["trade_price"]))
                c0_open = Decimal(str(c3[0]["opening_price"]))
                c1_close = Decimal(str(c3[1]["trade_price"]))
                # 3분봉이 음봉이거나 직전 종가보다 아래
                if (c0_price < c0_open) or (c0_price < c1_close):
                    down_3m = True

        # 2. 10분봉 확인 (최근 3개)
        r10 = await client.get("https://api.upbit.com/v1/candles/minutes/10", params={"market": market, "count": 3})
        if r10.status_code == 200:
            c10 = r10.json()
            if len(c10) >= 2:
                c10_0 = c10[0]
                c10_1 = c10[1]
                p10_0 = Decimal(str(c10_0["trade_price"]))
                o10_0 = Decimal(str(c10_0["opening_price"]))
                l10_1 = Decimal(str(c10_1["low_price"]))  # 직전 10분봉 저점

                # 10분봉이 음봉이면서 직전 10분봉 저점을 깨고 내려가는 국면
                if (p10_0 < o10_0) and (p10_0 <= l10_1):
                    down_10m = True

        # 3분봉과 10분봉이 둘 다 하락 추세일 때만 True
        is_both_falling = down_3m and down_10m
        _dual_trend_cache[market] = (now, is_both_falling)
        return is_both_falling

    except Exception as e:
        print(f"[DUAL-TF][WARN] {market} 듀얼 하락 추세 조회 실패: {e}")

    # 조회 실패 시에는 안전하게 손절 보류
    return False

# ============================================================
# 6. Exclude Markets (거래 제외 코인 관리)
# ============================================================
async def get_excoins(user_no: int) -> list[tuple] | None:
    async with SessionLocal() as session:
        sql = text("""SELECT DISTINCT market FROM exCoinlist WHERE userNo = :u AND attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"u": user_no, "attr": "%XXX%"})
        return result.fetchall()

def _env_excluded_set() -> set[str]:
    raw = os.getenv("EXCLUDE_MARKETS", "") or ""
    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    return set(parts)

async def parse_exclude_markets() -> set[str]:
    try:
        uno = int(os.getenv("USER_NO", "0"))
    except Exception:
        uno = 0

    try:
        redis_key = f"mtpond:excoins:{uno}"
        cached_coins = await redis_client.smembers(redis_key)
        if cached_coins:
            return set(cached_coins)
    except Exception:
        pass

    try:
        rows = await get_excoins(uno)
        if rows:
            ex_set = {str(r[0]).strip() for r in rows if r and r[0]}
            try:
                if ex_set:
                    await redis_client.sadd(f"mtpond:excoins:{uno}", *ex_set)
                    await redis_client.expire(f"mtpond:excoins:{uno}", 600)
            except Exception:
                pass
            return ex_set
    except Exception:
        pass

    return _env_excluded_set()

EXCLUDED_MARKETS: set[str] = set()
_excluded_last_update: float = 0
EXCLUDED_CACHE_TTL_SEC = 60

async def get_excluded_markets(force_reload: bool = False) -> set[str]:
    global EXCLUDED_MARKETS, _excluded_last_update
    now = time.time()
    if (not force_reload) and EXCLUDED_MARKETS and (now - _excluded_last_update) < EXCLUDED_CACHE_TTL_SEC:
        return EXCLUDED_MARKETS
    new_excluded = await parse_exclude_markets()
    if EXCLUDED_MARKETS != new_excluded:
        print(f"[EXC] 거래 제외 목록 갱신 ({len(new_excluded)}개): {sorted(new_excluded)}")
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

async def upbit_request(method: str, url: str, access_key: str, secret_key: str, params: dict = None, max_retry: int = 3) -> dict:
    params = params or {}
    backoff = 1.0
    client = get_http_client()
    for attempt in range(1, max_retry + 1):
        token = build_upbit_jwt_with_params(access_key, secret_key, params) if (params or method.upper() != "GET") else build_upbit_jwt_simple(access_key, secret_key)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            if method.upper() == "GET":
                resp = await client.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                resp = await client.post(url, headers=headers, params=params)
            elif method.upper() == "DELETE":
                resp = await client.delete(url, headers=headers, params=params)
            else:
                raise RuntimeError(f"Unsupported method {method}")

            if resp.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} body={resp.text}")
            return resp.json()
        except Exception as e:
            if attempt >= max_retry:
                raise
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

def quantize_volume(vol: Decimal) -> Decimal:
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

# ============================================================
# 8. 호가 및 틱 단위 보정
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
    if p >= 0.001:     return Decimal("0.00001")
    if p >= 0.0001:    return Decimal("0.000001")
    return Decimal("0.00000001")

def adjust_price_to_tick(raw_price: Decimal, side: str = "ask") -> Decimal:
    price = Decimal(str(raw_price))
    if price <= 0:
        return Decimal("0")
    unit = get_tick_unit(price)
    steps = price / unit
    mode = ROUND_CEILING if side == "ask" else ROUND_FLOOR
    steps_i = steps.to_integral_value(rounding=mode)
    adjusted = steps_i * unit
    return adjusted.quantize(Decimal("1") if unit >= 1 else unit)

async def get_orderbook_top(market: str):
    url = "https://api.upbit.com/v1/orderbook"
    params = {"markets": market}
    client = get_http_client()
    r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    bids = data[0].get("orderbook_units", [])
    return Decimal(str(bids[0]["bid_price"])), Decimal(str(bids[0]["ask_price"]))

_TRADABLE_MARKETS_SET: set[str] = set()
_TRADABLE_MARKETS_TS: float = 0.0

async def get_upbit_tradable_markets(force: bool = False) -> set[str]:
    global _TRADABLE_MARKETS_SET, _TRADABLE_MARKETS_TS
    now = time.time()
    if (not force) and _TRADABLE_MARKETS_SET and (now - _TRADABLE_MARKETS_TS < 3600):
        return _TRADABLE_MARKETS_SET

    try:
        client = get_http_client()
        resp = await client.get("https://api.upbit.com/v1/market/all?isDetails=false")
        if resp.status_code == 200:
            data = resp.json()
            _TRADABLE_MARKETS_SET = {m["market"] for m in data if "market" in m}
            _TRADABLE_MARKETS_TS = now
    except Exception as e:
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패: {e}")

    return _TRADABLE_MARKETS_SET

async def fetch_current_prices(markets: List[str]) -> Dict[str, Decimal]:
    if not markets:
        return {}

    tradable = await get_upbit_tradable_markets()
    valid_markets = [m for m in markets if m in tradable] if tradable else markets
    if not valid_markets:
        return {}

    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": ",".join(valid_markets)}
    client = get_http_client()

    try:
        r = await client.get(url, params=params)
        if r.status_code == 200:
            raw = r.json()
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
            raise RuntimeError(f"HTTP {r.status_code}")
    except Exception:
        price_map = {}
        for m in valid_markets:
            try:
                res = await client.get(url, params={"markets": m})
                if res.status_code == 200:
                    items = res.json()
                    if items and items[0].get("trade_price") is not None:
                        price_map[m] = Decimal(str(items[0]["trade_price"]))
                elif res.status_code == 404:
                    if m in _TRADABLE_MARKETS_SET:
                        _TRADABLE_MARKETS_SET.discard(m)
            except Exception:
                pass
        return price_map

# ============================================================
# 9. BB-Patterns 기반 신규 매수 후보 발굴 엔진
# ============================================================
_bb_patterns_cache: Dict[str, Any] = {
    "fetched_ts": 0.0,
    "last_server_updated": None,
    "candidates": []
}

async def fetch_bb_patterns_candidates(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if (not force) and (now - _bb_patterns_cache["fetched_ts"] < BB_CACHE_TTL_SEC) and _bb_patterns_cache["candidates"]:
        return _bb_patterns_cache["candidates"]

    try:
        client = get_http_client()
        resp = await client.get(BB_PATTERNS_API_URL)
        if resp.status_code == 202:
            return _bb_patterns_cache["candidates"]
        elif resp.status_code != 200:
            return _bb_patterns_cache["candidates"]
        data = resp.json()
    except Exception as e:
        print(f"[BB-PATTERNS][ERR] API 호출 실패: {e}")
        return _bb_patterns_cache["candidates"]

    if data.get("status") == "processing":
        return _bb_patterns_cache["candidates"]

    coins = data.get("coins", [])
    if not isinstance(coins, list) or len(coins) == 0:
        return _bb_patterns_cache["candidates"]

    server_updated = data.get("updated")
    excluded = await get_excluded_markets()
    qualified = []

    cnt_total = len(coins)
    cnt_excluded = 0
    cnt_score_drop = 0
    cnt_trade_drop = 0
    cnt_price_drop = 0

    for c in coins:
        market = c.get("market", "")
        if not market.startswith("KRW-"):
            continue

        if market in excluded:
            cnt_excluded += 1
            continue

        try:
            price = float(c.get("price", 0))
        except Exception:
            price = 0
        if price < 30:
            cnt_price_drop += 1
            continue

        try:
            overall_score = int(c.get("overall_score", 0))
        except Exception:
            overall_score = 0
        if overall_score < BB_MIN_OVERALL_SCORE:
            cnt_score_drop += 1
            continue

        act = c.get("trade_activity") or {}
        try:
            count_10m = int(act.get("trade_count_10m", 0))
        except Exception:
            count_10m = 0

        if count_10m < BB_MIN_TRADE_COUNT_10M:
            cnt_trade_drop += 1
            continue

        qualified.append({
            "market": market,
            "price": Decimal(str(price)),
            "overall_score": overall_score,
            "trade_count_10m": count_10m,
            "trade_count_1h": act.get("trade_count_1h", 0),
            "recommendation": c.get("recommendation")
        })

    qualified.sort(key=lambda x: (x["overall_score"], x["trade_count_10m"]), reverse=True)

    _bb_patterns_cache["fetched_ts"] = now
    _bb_patterns_cache["last_server_updated"] = server_updated
    _bb_patterns_cache["candidates"] = qualified

    print(
        f"\n[BB-PATTERNS 갱신] 서버시각: {server_updated}\n"
        f"  - 전체: {cnt_total}개 | 제외코인: {cnt_excluded}개 | 30원미만: {cnt_price_drop}개\n"
        f"  - 점수미달(<{BB_MIN_OVERALL_SCORE}): {cnt_score_drop}개 | 체결미달(<{BB_MIN_TRADE_COUNT_10M}건/10m): {cnt_trade_drop}개\n"
        f"  ▶ 최종 추천 진입 후보: {len(qualified)}개"
    )
    if qualified:
        for q in qualified[:5]:
            print(f"    * {q['market']} | 점수: {q['overall_score']}점 | 10분체결: {q['trade_count_10m']}건 | 현재가: {q['price']}원")
    print()

    return qualified

# ============================================================
# 10. 주문 보조 함수
# ============================================================
async def order_market_buy_price(access_key: str, secret_key: str, market: str, krw_amount: Decimal):
    params = {"market": market, "side": "bid", "price": str(krw_amount), "ord_type": "price"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_market_sell(access_key: str, secret_key: str, market: str, volume: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "ord_type": "market"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def order_limit_sell(access_key: str, secret_key: str, market: str, volume: Decimal, price: Decimal):
    params = {"market": market, "side": "ask", "volume": str(volume), "price": str(price), "ord_type": "limit"}
    return await upbit_request("POST", UPBIT_ORDER_URL, access_key, secret_key, params=params, max_retry=2)

async def fetch_upbit_order_state(access_key: str, secret_key: str, uuid_: str) -> Optional[dict]:
    try:
        params = {"uuid": uuid_}
        return await upbit_request("GET", "https://api.upbit.com/v1/order", access_key, secret_key, params=params, max_retry=2)
    except Exception:
        return None

async def cancel_order(access_key: str, secret_key: str, uuid_: str):
    params = {"uuid": uuid_}
    try:
        return await upbit_request("DELETE", "https://api.upbit.com/v1/order", access_key, secret_key, params=params, max_retry=2)
    except Exception as e:
        print(f"[CANCEL] 주문 취소 실패 ({uuid_}): {e}")

async def cancel_all_open_orders_for_market(access_key: str, secret_key: str, market: str):
    try:
        orders = await upbit_request("GET", "https://api.upbit.com/v1/orders", access_key, secret_key, params={"market": market, "state": "wait"})
        if isinstance(orders, list):
            for od in orders:
                uid = od.get("uuid")
                if uid:
                    await cancel_order(access_key, secret_key, uid)
    except Exception as e:
        print(f"[CANCEL-ALL][ERR] {market} 주문 전체 취소 실패: {e}")

# ============================================================
# [급락 대응] 캔들 평균 진폭(변동성) 산출 및 캐시
# ============================================================
VOLATILITY_CACHE_TTL_SEC = 180
_volatility_cache: Dict[str, Tuple[float, Decimal]] = {}
PANIC_DROP_MULTIPLIER = Decimal(os.getenv("PANIC_DROP_MULTIPLIER", "1.8"))

async def get_candle_volatility(market: str, unit: int = 10, count: int = 10) -> Decimal:
    now = time.time()
    cached = _volatility_cache.get(market)
    if cached and (now - cached[0]) < VOLATILITY_CACHE_TTL_SEC:
        return cached[1]

    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    params = {"market": market, "count": count}

    try:
        client = get_http_client()
        resp = await client.get(url, params=params)
        if resp.status_code == 200:
            candles = resp.json()
            if isinstance(candles, list) and len(candles) > 0:
                amp_sum = Decimal("0")
                for c in candles:
                    h = Decimal(str(c["high_price"]))
                    l = Decimal(str(c["low_price"]))
                    o = Decimal(str(c["opening_price"]))
                    if o > 0:
                        amp_sum += ((h - l) / o) * Decimal("100")

                avg_amp = (amp_sum / Decimal(str(len(candles)))).quantize(Decimal("0.01"))
                _volatility_cache[market] = (now, avg_amp)
                return avg_amp
    except Exception as e:
        print(f"[VOLATILITY][WARN] {market} 캔들 진폭 조회 실패: {e}")

    return Decimal("2.0")

# ============================================================
# [종목별 개별 적용] 코인별 동적 익절 목표치 계산
# ============================================================
async def get_dynamic_target_pnl(market: str, avg_price: Decimal) -> Decimal:
    avg_vol = await get_candle_volatility(market, unit=10, count=10)
    target_pnl = (avg_vol / Decimal("2")).quantize(Decimal("0.01"))

    if target_pnl < Decimal("0.5"):
        target_pnl = Decimal("0.5")

    if avg_price > 0:
        tick = get_tick_unit(avg_price)
        one_tick_pct = (tick / avg_price) * Decimal("100")
        min_tick_target = (one_tick_pct * Decimal("1.5")).quantize(Decimal("0.01"))
        if target_pnl < min_tick_target:
            target_pnl = min_tick_target

    if target_pnl > Decimal("2.5"):
        target_pnl = Decimal("2.5")

    return target_pnl

# ============================================================
# 11. 익절 및 손절 판단 엔진
# ============================================================
def decide_sell(market: str, pnl: Decimal, state: dict) -> Tuple[bool, str, Optional[str]]:
    peak = state["peak_pnl"]
    hard_tp_taken = state.get("hard_tp_taken", False)
    target_tp = state.get("dynamic_tp", Decimal("0.6"))

    if (not hard_tp_taken) and pnl >= target_tp:
        return True, f"DYNAMIC_TP {pnl}% >= {target_tp}%", "HARD_TP1"

    check_arm = (target_tp * Decimal("0.75")).quantize(Decimal("0.01"))
    wait_gate = (target_tp * Decimal("0.85")).quantize(Decimal("0.01"))

    if peak >= check_arm:
        if pnl >= wait_gate:
            return False, f"WAIT_TP (pnl={pnl}% >= {wait_gate}% | 목표={target_tp}%)", None

        # 약익절: +0.10% ~ +0.25% 구간 이익 보존
        if Decimal("0.10") <= pnl <= Decimal("0.25"):
            return True, f"WEAK_TP_EXIT (peak={peak}% -> now={pnl}% | 이익보존)", "WEAK_TP"

    return False, "", None


# ============================================================
# 12. 추가 매수 프로세스 (반등 확인형 물타기 & 손절 우선 적용)
# ============================================================
ADDITIONAL_BUY_COOLDOWN_SEC = int(os.getenv("ADDITIONAL_BUY_COOLDOWN_SEC", "120"))


async def process_additional_buys(
        access_key: str,
        secret_key: str,
        enriched_positions: List[dict],
        ps: PositionState,
        available_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if available_krw < MIN_ORDER_NOTIONAL_KRW or ADDITIONAL_BUY_KRW <= 0:
        return used_krw

    now = time.time()

    try:
        max_cap = Decimal(str(MAX_TOTAL_INVEST_PER_MARKET))
    except Exception:
        max_cap = Decimal("0")

    try:
        drop_target = Decimal(str(BUY_RANGE_LOW))
        if drop_target > 0:
            drop_target = -abs(drop_target)
    except Exception:
        drop_target = Decimal("-2.2")

    excluded = await get_excluded_markets()

    for pos in enriched_positions:
        market = pos["market"]
        pnl = pos["pnl_percent"]
        cur_price = pos.get("current_price")

        if market in excluded or pnl is None or cur_price is None:
            continue

        # 쿨다운 중인 코인은 추가 매수 제외
        if await ps.recently_sold(market):
            continue

        # -------------------------------------------------------------
        # [최우선] 손절 라인 및 캔들 진폭 초과 급락 시 추가 매수 금지 및 즉시 탈출
        # -------------------------------------------------------------
        avg_vol = await get_candle_volatility(market, unit=10, count=10)
        panic_threshold = -(avg_vol * PANIC_DROP_MULTIPLIER)

        # 1) 진폭 초과 급락 또는 2) 설정된 손절 라인(LOSS_CUT_RATE) 이하 도달 시
        if pnl <= panic_threshold or (ENABLE_STOP and pnl <= LOSS_CUT_RATE):
            reason_tag = "PANIC_EXIT" if pnl <= panic_threshold else "LOSS_CUT"
            print(f"\n🚨 [ADD-BUY-ABORT][{reason_tag}] {market} 손절선 도달로 추가 매수 중단 및 긴급 청산 (PNL: {pnl}%)")

            await cancel_all_open_orders_for_market(access_key, secret_key, market)
            await asyncio.sleep(0.3)

            accs = await fetch_upbit_accounts(access_key, secret_key)
            currency = market.split("-")[1]
            for a in accs:
                if a.get("currency") == currency:
                    actual_bal = Decimal(str(a.get("balance", "0")))
                    sell_vol = quantize_volume(actual_bal)
                    if sell_vol * cur_price >= MIN_ORDER_NOTIONAL_KRW:
                        if LIVE_TRADING:
                            resp = await order_market_sell(access_key, secret_key, market, sell_vol)
                            print(f"   ▶ 시장가 손절 청산 완료: 수량={sell_vol} (uuid={resp.get('uuid')})")
                        else:
                            print(f"   ▶ [DRY] 시장가 손절 청산 시뮬레이션: 수량={sell_vol}")
                        ps.mark_sold(market, reason=reason_tag)
                    break
            continue

        # 아직 하락 폭 기준선(-2.2% 등)에 도달하지 않은 경우 스킵
        if pnl >= Decimal("0") or pnl > drop_target:
            continue

        # -------------------------------------------------------------
        # [핵심] 하락 목표치에 도달했어도 "상승/반등 신호(3분봉)" 확인 전까지 대기
        # -------------------------------------------------------------
        is_rebounding = await check_3m_candle_trend(market)
        if not is_rebounding:
            # print(f"[BUY-ADD][WAIT] {market} pnl={pnl}% 도달했으나 3분봉 음봉/하락 진행 중 → 반등 확인 전까지 추가 매수 보류")
            continue

        # -------------------------------------------------------------
        # 누적 투자 한도 및 횟수 검증
        # -------------------------------------------------------------
        total_buys, total_invested = ps.get_buy_stats(market)

        if total_invested <= 0:
            bal_eff = (pos["balance"] + pos["locked"])
            total_invested = (bal_eff * pos["avg_buy_price"]).quantize(Decimal("0.0001"))
            ps.buy_info[market] = {"total_buys": max(1, total_buys), "total_invested": total_invested}

        if max_cap > 0 and total_invested >= max_cap:
            continue

        if total_buys - 1 >= MAX_ADDITIONAL_BUYS:
            continue

        st = ps.data.get(market, {})
        last_add_ts = st.get("last_additional_buy_ts")
        last_add_price = st.get("last_additional_buy_price")
        last_add_pnl = st.get("last_additional_buy_pnl")

        if last_add_ts is None:
            st["last_additional_buy_ts"] = now
            continue

        elapsed_sec = now - last_add_ts
        if elapsed_sec < ADDITIONAL_BUY_COOLDOWN_SEC:
            continue

        if last_add_price is not None:
            if cur_price == last_add_price:
                continue

            tick_unit = get_tick_unit(cur_price)
            if abs(cur_price - last_add_price) < tick_unit:
                continue

        # 직전 추가 매수 시점보다 최소 -0.8% 이상 더 하락한 상태에서 반등할 때만 유효
        if last_add_pnl is not None and (pnl - last_add_pnl) > Decimal("-0.8"):
            continue

        cap_remain = (max_cap - total_invested) if max_cap > 0 else ADDITIONAL_BUY_KRW
        if cap_remain < MIN_ORDER_NOTIONAL_KRW:
            continue

        buy_amt = min(ADDITIONAL_BUY_KRW, cap_remain, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        if max_cap > 0 and (total_invested + buy_amt) > max_cap:
            buy_amt = max_cap - total_invested
            if buy_amt < MIN_ORDER_NOTIONAL_KRW:
                continue

        # -------------------------------------------------------------
        # 반등 확인형 추가 매수 실행
        # -------------------------------------------------------------
        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(
                    f"[BUY-ADD] {market} 반등 확인 추가 매수 완료! pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원, 경과:{int(elapsed_sec)}초) amt={buy_amt} | 누적: {total_invested + buy_amt}/{max_cap}원 (uuid={resp.get('uuid')})")
            else:
                print(
                    f"[BUY-ADD][DRY] {market} 반등 확인 추가 매수: pnl={pnl}% (이전가:{last_add_price} -> 현재가:{cur_price}원) amt={buy_amt}")

            ps.record_buy(market, buy_amt)
            st["last_additional_buy_ts"] = now
            st["last_additional_buy_price"] = cur_price
            st["last_additional_buy_pnl"] = pnl

            available_krw -= buy_amt
            used_krw += buy_amt

            if PREPLACE_HARD_TP and LIVE_TRADING:
                asyncio.create_task(after_buy_preplace_tp(access_key, secret_key, market, ps))

        except Exception as e:
            print(f"[BUY-ADD][ERR] {market} 추가 매수 실패: {e}")

    return used_krw

# ============================================================
# 13. 신규 종목 매수 프로세스
# ============================================================
async def process_new_candidate_buys(
    access_key: str,
    secret_key: str,
    ps: PositionState,
    remaining_slots: int,
    available_krw: Decimal
) -> Decimal:
    used_krw = Decimal("0")
    if remaining_slots <= 0 or available_krw < MIN_ORDER_NOTIONAL_KRW or RANGE_BUY_KRW <= 0:
        return used_krw

    try:
        max_cap = Decimal(str(MAX_TOTAL_INVEST_PER_MARKET))
    except Exception:
        max_cap = Decimal("0")

    candidates = await fetch_bb_patterns_candidates()
    if not candidates:
        return used_krw

    excluded = await get_excluded_markets()
    buys_done = 0

    for item in candidates:
        if buys_done >= remaining_slots:
            break

        market = item["market"]

        if market in excluded:
            continue

        if ps.buy_info.get(market):
            continue

        if await ps.recently_sold(market):
            continue

        # 3분봉 상승세 검증 (음봉 시 진입 차단)
        is_uptrend_3m = await check_3m_candle_trend(market)
        if not is_uptrend_3m:
            continue

        target_amt = RANGE_BUY_KRW
        if max_cap > 0 and target_amt > max_cap:
            target_amt = max_cap

        buy_amt = min(target_amt, available_krw)
        if buy_amt < MIN_ORDER_NOTIONAL_KRW:
            continue

        try:
            if LIVE_TRADING:
                resp = await order_market_buy_price(access_key, secret_key, market, buy_amt)
                print(f"[BUY-NEW] {market} 신규 진입 (3분봉 상승 확인)! 점수={item['overall_score']} 10분체결={item['trade_count_10m']}건 amt={buy_amt} (uuid={resp.get('uuid')})")
            else:
                print(f"[BUY-NEW][DRY] {market} 신규 진입: 점수={item['overall_score']} amt={buy_amt}")

            ps.record_buy(market, buy_amt)
            available_krw -= buy_amt
            used_krw += buy_amt
            buys_done += 1

            if PREPLACE_HARD_TP and LIVE_TRADING:
                asyncio.create_task(after_buy_preplace_tp(access_key, secret_key, market, ps))

        except Exception as e:
            print(f"[BUY-NEW][ERR] {market} 신규 매수 실패: {e}")

    return used_krw

# ============================================================
# 14. 매수 후 선지정 지정가 익절 주문 보조
# ============================================================
async def after_buy_preplace_tp(access_key: str, secret_key: str, market: str, ps: PositionState):
    await asyncio.sleep(0.5)
    try:
        accounts = await fetch_upbit_accounts(access_key, secret_key)
        base = market.split("-")[1]
        for acc in accounts:
            if acc.get("currency") == base:
                bal = Decimal(str(acc.get("balance", "0")))
                avg = Decimal(str(acc.get("avg_buy_price", "0")))
                if bal <= 0 or avg <= 0:
                    return

                await cancel_all_open_orders_for_market(access_key, secret_key, market)

                dynamic_tp = await get_dynamic_target_pnl(market, avg)
                target_price = avg * (Decimal("1") + (dynamic_tp / Decimal("100")))
                best_bid, best_ask = await get_orderbook_top(market)
                limit_price = adjust_price_to_tick(max(target_price, best_ask), side="ask")

                vol = quantize_volume(bal)
                if vol * limit_price >= MIN_ORDER_NOTIONAL_KRW:
                    resp = await order_limit_sell(access_key, secret_key, market, vol, limit_price)
                    st = ps.data.setdefault(market, {})
                    st["active_limit_uuid"] = resp.get("uuid")
                    st["dynamic_tp"] = dynamic_tp
                    st["limit_pending_category"] = "PRE_DYNAMIC_TP"
                    st["limit_submit_ts"] = time.time()
                    print(f"[PRE_TP] {market} 개별 익절 발주 완료: vol={vol} price={limit_price} (개별목표: +{dynamic_tp}%)")
                break
    except Exception as e:
        print(f"[PRE_TP][ERR] {market} 개별 선지정 주문 실패: {e}")

# ============================================================
# 15. reseed_and_replace_all_open_orders (평단가 0원 코인 보호 & ps 동기화 완료)
# ============================================================
async def reseed_and_replace_all_open_orders(
        access_key: str,
        secret_key: str,
        ps: PositionState
):
    print("[RESEED] 기존 등록 코인 미체결 주문 취소 및 잔여 잔고 검증 시작...")
    excluded = await get_excluded_markets(force_reload=True)

    try:
        raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[RESEED][ERR] 1차 계정 조회 실패: {e}")
        return

    target_markets = []
    for acc in raw_accounts:
        cur = acc.get("currency")
        if not cur or cur == "KRW":
            continue
        mkt = f"KRW-{cur}"

        if mkt in excluded:
            continue

        bal = Decimal(str(acc.get("balance", "0")))
        locked = Decimal(str(acc.get("locked", "0")))
        if (bal + locked) > 0:
            target_markets.append(mkt)

    if not target_markets:
        print("[RESEED] 관리 대상 보유 코인이 없습니다 (취소/재주문 생략).")
        return

    for mkt in target_markets:
        await cancel_all_open_orders_for_market(access_key, secret_key, mkt)

    await asyncio.sleep(0.5)

    try:
        updated_accounts = await fetch_upbit_accounts(access_key, secret_key)
    except Exception as e:
        print(f"[RESEED][ERR] 2차 계정 조회 실패: {e}")
        updated_accounts = raw_accounts

    price_map = await fetch_current_prices(target_markets)

    for acc in updated_accounts:
        cur = acc.get("currency")
        if not cur or cur == "KRW":
            continue
        mkt = f"KRW-{cur}"

        if mkt in excluded or mkt not in target_markets:
            continue

        bal = Decimal(str(acc.get("balance", "0")))
        raw_avg = Decimal(str(acc.get("avg_buy_price", "0")))
        cur_price = price_map.get(mkt)

        # 평단가 0원 코인은 임의 매도 시 손실 위험이 있으므로 관리 제외
        if raw_avg <= 0:
            print(f"[RESEED][SKIP] {mkt} 평균매수가 0원 감지 → 임의 매도 금지, 사용자 관리 대상 유지")
            continue

        avg = raw_avg

        if bal <= 0:
            ps.data.pop(mkt, None)
            ps.buy_info.pop(mkt, None)
            continue

        vol_q = quantize_volume(bal)
        if vol_q <= 0:
            ps.data.pop(mkt, None)
            ps.buy_info.pop(mkt, None)
            continue

        ref_price = cur_price if (cur_price and cur_price > 0) else avg
        est_notional = vol_q * ref_price

        if est_notional < MIN_ORDER_NOTIONAL_KRW:
            print(f"[RESEED][SKIP] {mkt} 최소 주문 금액 미달 ({est_notional:.0f}원 < {MIN_ORDER_NOTIONAL_KRW}원) → 관리 목록 정리")
            ps.data.pop(mkt, None)
            ps.buy_info.pop(mkt, None)
            continue

        try:
            dynamic_tp = await get_dynamic_target_pnl(mkt, avg)
            target_price_raw = avg * (Decimal("1") + (dynamic_tp / Decimal("100")))
            best_bid, best_ask = await get_orderbook_top(mkt)
            limit_price = adjust_price_to_tick(max(target_price_raw, best_ask), side="ask")

            final_notional = vol_q * limit_price
            if final_notional < MIN_ORDER_NOTIONAL_KRW:
                continue

            # 정상 등록 종목의 투자 금액 및 상태 동기화
            est_total = (vol_q * avg).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": est_total}

            st = ps.data.setdefault(mkt, {})
            st["dynamic_tp"] = dynamic_tp
            st["avg_buy_price"] = avg

            if LIVE_TRADING:
                resp = await order_limit_sell(access_key, secret_key, mkt, vol_q, limit_price)
                uid = resp.get("uuid")
                st["active_limit_uuid"] = uid
                st["limit_pending_category"] = "PRE_DYNAMIC_TP"
                st["limit_submit_ts"] = time.time()
                print(f"[RESEED][OK] {mkt} 개별 매도 재등록: {vol_q}개 @ {limit_price}원 (개별목표: +{dynamic_tp}%, uuid={uid})")
            else:
                print(f"[RESEED][DRY] {mkt} 개별 매도 시뮬레이션: {vol_q}개 @ {limit_price}원 (개별목표: +{dynamic_tp}%)")

        except Exception as e:
            print(f"[RESEED][ERR] {mkt} 매도 주문 재배치 실패: {e}")

    print("[RESEED] 유효 잔고 코인 매도 재등록 작업 완료\n")

# ============================================================
# 16. 메인 모니터링 루프
# ============================================================
async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()
    last_cfg_sync_ts = time.time()

    async with SessionLocal() as session:
        sql = text("""SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo = :u AND serverNo = :s LIMIT 1""")
        result = await session.execute(sql, {"u": user_no, "s": server_no})
        row = result.fetchone()
        if not row:
            print("[ERR] API Key를 찾을 수 없습니다.")
            return
        access_key, secret_key = row[0], row[1]

    ps = PositionState(user_no=user_no)
    print("=== 자동매매 모니터링 엔진 가동 시작 ===")

    excluded = await get_excluded_markets(force_reload=True)

    try:
        init_accs = await fetch_upbit_accounts(access_key, secret_key)
        boot_now = time.time()
        for acc in init_accs:
            cur = acc.get("currency")
            if not cur or cur == "KRW":
                continue
            market = f"KRW-{cur}"

            if market in excluded:
                continue

            bal = Decimal(str(acc.get("balance", "0"))) + Decimal(str(acc.get("locked", "0")))
            avg = Decimal(str(acc.get("avg_buy_price", "0")))
            if bal > 0 and avg > 0:
                est = (bal * avg).quantize(Decimal("0.0001"))
                ps.buy_info[market] = {"total_buys": 1, "total_invested": est}
                ps.data[market] = {
                    "peak_pnl": Decimal("0"),
                    "min_pnl": Decimal("0"),
                    "armed": False,
                    "avg_buy_price": avg,
                    "last_additional_buy_ts": boot_now,
                    "last_additional_buy_price": avg,
                    "last_additional_buy_pnl": Decimal("0")
                }
        print(f"[INIT] 기존 보유 코인 {len(ps.buy_info)}개 관리 등록 완료")
    except Exception as e:
        print(f"[INIT][WARN] 잔고 초기화 경고: {e}")

    try:
        await reseed_and_replace_all_open_orders(access_key, secret_key, ps)
    except Exception as e:
        print(f"[RESEED][WARN] 재시작 주문 재배치 중 오류: {e}")

    while True:
        try:
            loop_now = time.time()

            if loop_now - last_cfg_sync_ts >= CONTROLLER_POLL_SEC:
                last_cfg_sync_ts = loop_now
                try:
                    cached_str = await redis_client.get(f"mtpond:setup:{user_no}")
                    if cached_str:
                        data = json.loads(cached_str)
                        cfg = data[0] if isinstance(data, list) and data else data
                        apply_dynamic_config(cfg)
                except Exception:
                    pass

            excluded = await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
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
                        bal = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                        if bal > 0:
                            held_markets.append(mkt)

            price_map = await fetch_current_prices(held_markets)

            enriched = []
            for a in raw_accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW":
                    continue
                market = f"KRW-{cur}"

                if market in excluded:
                    continue

                # 손절/방어로 이미 매도 처리된 코인은 잔고 체결 지연 동안 관리 목록에서 배제
                if await ps.recently_sold(market):
                    continue

                bal = Decimal(str(a.get("balance", "0")))
                locked = Decimal(str(a.get("locked", "0")))
                avg = Decimal(str(a.get("avg_buy_price", "0")))
                cur_price = price_map.get(market)

                if cur_price and avg > 0 and (bal + locked) > 0:
                    pnl = ((cur_price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                    enriched.append({
                        "market": market,
                        "balance": bal,
                        "locked": locked,
                        "avg_buy_price": avg,
                        "current_price": cur_price,
                        "pnl_percent": pnl
                    })

            active_uuids = [
                (m, st.get("active_limit_uuid"))
                for m, st in list(ps.data.items())
                if st.get("active_limit_uuid")
            ]

            for mkt, uid in active_uuids:
                ord_info = await fetch_upbit_order_state(access_key, secret_key, uid)
                if not ord_info:
                    continue

                state = ord_info.get("state")
                if state == "done":
                    exec_vol = ord_info.get("executed_volume", "0")
                    price = ord_info.get("price", "0")
                    print(f"[TP-DONE] {mkt} 선지정 지정가 익절 체결 완료! 수량={exec_vol} 가격={price}원 (uuid={uid})")
                    ps.mark_sold(mkt, reason="TP_DONE")
                    enriched = [p for p in enriched if p["market"] != mkt]
                elif state == "cancel":
                    if mkt in ps.data:
                        ps.data[mkt]["active_limit_uuid"] = None

            active_count = len(enriched)
            remaining_slots = max(0, MAX_ACTIVE_MARKETS - active_count)

            # ================= [A] 매도 판단 및 실행 =================
            for pos in enriched:
                market = pos["market"]
                pnl = pos["pnl_percent"]
                bal = pos["balance"]
                cur_p = pos["current_price"]
                st = ps.update_or_init(market, pnl, pos["avg_buy_price"])

                # =============================================================
                # 1. 캔들 진폭 기반 동적 손절선 계산 (최소 -1.0% 보장)
                # =============================================================
                avg_vol = await get_candle_volatility(market, unit=10, count=10)

                # 진폭 기반 임계치 계산 (단, 너무 얕은 손절 방지를 위해 최소 -1.0% 바닥 강제)
                calculated_cut = -(avg_vol * Decimal("1.5"))
                panic_threshold = min(calculated_cut, Decimal("-1.0"))  # 최소 -1.0% 이하에서만 작동

                # =============================================================
                # 2. 듀얼 타임프레임(3m & 10m) 하락 국면 검증 손절
                # =============================================================
                is_panic_cut = (pnl <= panic_threshold)
                is_fixed_cut = (ENABLE_STOP and pnl <= LOSS_CUT_RATE)

                # 손절 라인에 걸렸을 때 바로 자르지 않고 10분봉/3분봉 동시 하락인지 확인
                if is_panic_cut or is_fixed_cut:
                    # 두 타임프레임 모두 하락 국면인지 확인
                    is_dual_downtrend = await check_dual_timeframe_downtrend(market)

                    # -3.5%를 초과하는 절대 마지노선이 아니라면, 10분봉이 지지하고 있을 경우 손절 유보!
                    if not is_dual_downtrend and pnl > Decimal("-3.5"):
                        # print(f"[LOSS-HOLD] {market} PNL: {pnl}% (임계치 도달했으나 10분봉 지지 확인 -> 손절 유보)")
                        continue

                    # 둘 다 하락 국면 확정 시 손절 집행
                    reason_tag = "PANIC_EXIT" if is_panic_cut else "LOSS_CUT"
                    print(f"\n🚨 [{reason_tag}] {market} 3m & 10m 동시 하락 국면 확정! PNL: {pnl}% (손절 임계치: {panic_threshold}%)")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.4)

                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            actual_bal = Decimal(str(a.get("balance", "0")))
                            sell_vol = quantize_volume(actual_bal)
                            if sell_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    resp = await order_market_sell(access_key, secret_key, market, sell_vol)
                                    print(f"   ▶ 시장가 손절 청산 완료: 수량={sell_vol} (uuid={resp.get('uuid')})")
                                else:
                                    print(f"   ▶ [DRY] 시장가 손절 청산 시뮬레이션: 수량={sell_vol}")
                                # 30분 재매수 금지 쿨다운 적용
                                ps.mark_sold(market, reason=reason_tag)
                            break
                    print("──────────────────────────────────────────────\n")
                    continue

                # 2. 고정 손절 (LOSS_CUT_RATE)
                if ENABLE_STOP and pnl <= LOSS_CUT_RATE:
                    print(f"[LOSS-CUT] {market} 고정 손절 도달 (PNL: {pnl}% <= {LOSS_CUT_RATE}%)")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.4)

                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            actual_bal = Decimal(str(a.get("balance", "0")))
                            sell_vol = quantize_volume(actual_bal)
                            if sell_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                                if LIVE_TRADING:
                                    resp = await order_market_sell(access_key, secret_key, market, sell_vol)
                                    print(f"   ▶ 시장가 손절 체결: 수량={sell_vol} (uuid={resp.get('uuid')})")
                                ps.mark_sold(market, reason="LOSS_CUT")
                            break
                    continue

                # 3. 익절 및 약익절/대기 판단
                should_sell, reason, cat = decide_sell(market, pnl, st)
                if should_sell:
                    if cat == "WEAK_TP" and pnl < Decimal("0.10"):
                        continue

                    print(f"[SELL-SIGNAL] {market} {cat} 매도 조건 달성: {reason}")
                    await cancel_all_open_orders_for_market(access_key, secret_key, market)
                    await asyncio.sleep(0.3)

                    accs = await fetch_upbit_accounts(access_key, secret_key)
                    actual_bal = bal
                    for a in accs:
                        if a.get("currency") == market.split("-")[1]:
                            actual_bal = Decimal(str(a.get("balance", "0")))
                            break

                    portion = HARD_TP_SELL_PORTION if cat == "HARD_TP1" else SELL_PORTION
                    sell_vol = quantize_volume(actual_bal * portion)

                    if sell_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                        if LIVE_TRADING:
                            await order_market_sell(access_key, secret_key, market, sell_vol)
                            print(f"[SELL-EXEC] {market} {cat} 매도 체결: {sell_vol}개 (PNL: {pnl}%)")
                        else:
                            print(f"[SELL-DRY] {market} {cat} 매도 시뮬레이션: {sell_vol}개")

                        if portion >= Decimal("1.0"):
                            sell_reason = "TP" if cat in ("HARD_TP1", "HARD_TP2", "DYNAMIC_TP") else "WEAK_TP"
                            ps.mark_sold(market, reason=sell_reason)
                        else:
                            ps.reduce_invested_after_sell(market, portion)
                    else:
                        print(f"[SELL-SKIP] {market} 매도 금액 미달로 스킵 (수량={sell_vol})")

            # ================= [B] 추가 매수 실행 =================
            used_krw_add = await process_additional_buys(access_key, secret_key, enriched, ps, available_krw)
            available_krw -= used_krw_add

            # ================= [C] 신규 종목 매수 실행 =================
            used_krw_new = await process_new_candidate_buys(access_key, secret_key, ps, remaining_slots, available_krw)
            available_krw -= used_krw_new

            if PERIODIC_RESET_SEC > 0 and (loop_now - start_ts) >= PERIODIC_RESET_SEC:
                print(f"[RESET] {PERIODIC_RESET_SEC}초(1일) 경과: 데몬 일일 정기 재기동")
                raise ResetRequested()

        except ResetRequested:
            raise
        except Exception as e:
            print(f"[MONITOR][LOOP-ERR] {e}")

        await asyncio.sleep(INTERVAL_SECONDS)

# ============================================================
# 17. 컨트롤러
# ============================================================
_last_cfg_signature = None

async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    global _last_cfg_signature
    print(f"[CTRL] Controller 가동: User={user_no}, Server={server_no}")

    async def update_config_once():
        nonlocal task
        global _last_cfg_signature

        cfg = None
        try:
            cached_str = await redis_client.get(f"mtpond:setup:{user_no}")
            if cached_str:
                data = json.loads(cached_str)
                cfg = data[0] if isinstance(data, list) and data else data
        except Exception:
            pass

        if not cfg and API_BASE:
            try:
                client = get_http_client()
                r = await client.get(f"{API_BASE}/api/mtpondsetup/{user_no}")
                if r.status_code == 200:
                    d = r.json()
                    cfg = d[0] if isinstance(d, list) and d else d
            except Exception:
                pass

        if not cfg:
            return

        active_flag = str(cfg.get("activeYN", "")).upper() == "Y"
        sig = tuple(sorted([(k, str(v)) for k, v in cfg.items() if k != "activeYN"]))

        if sig != _last_cfg_signature:
            apply_dynamic_config(cfg)
            _last_cfg_signature = sig

        if task and task.done():
            exc = task.exception()
            if isinstance(exc, ResetRequested):
                print("[CTRL] 일일 정기 리셋 완료 → 모니터링 재기동")
                init_config(force=True)
                apply_dynamic_config(cfg)
            elif exc:
                print(f"[CTRL] 🚨 모니터링 비정상 예외 종료 감지: {exc} → 즉시 복구 재기동")
                init_config(force=True)
                apply_dynamic_config(cfg)
            task = None

        if active_flag and task is None:
            print("[CTRL] activeYN=Y → 설정 최신화 확인 후 모니터링 태스크 시작")
            init_config(force=True)
            apply_dynamic_config(cfg)
            task = asyncio.create_task(monitor_positions(user_no, server_no))
        elif not active_flag and task:
            print("[CTRL] activeYN=N → 모니터링 태스크 중단")
            task.cancel()
            try:
                await task
            except Exception:
                pass
            task = None

    await update_config_once()

    while True:
        try:
            await update_config_once()
        except Exception as e:
            print(f"[CTRL][ERR] 컨트롤러 주기 점검 예외: {e}")
        await asyncio.sleep(CONTROLLER_POLL_SEC)

# ============================================================
# 18. 진입점
# ============================================================
async def main():
    init_config(force=True)
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청 수신")