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
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR, ROUND_CEILING
import dotenv
import httpx
import jwt
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# ============================================================
# 2. 환경 및 기본 설정
# ============================================================
dotenv.load_dotenv()

engine = create_async_engine(os.getenv("dburl"), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
API_BASE = os.getenv("API_BASE", "").rstrip("/")
CONTROLLER_POLL_SEC = int(os.getenv("CONTROLLER_POLL_SEC", "5"))
BASE_UNIT = "KRW"

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
# 3. STATIC CONFIG (50% / 20% / 30% 자금 룰 & API 설정)
# ============================================================
BB_PATTERNS_API_URL = os.getenv("BB_PATTERNS_API_URL", "http://becog.iptime.org:8088/api/bb-patterns")
BB_CACHE_TTL_SEC = 60  # 외부 분석 서버 캐시 주기 60초

RATIO_TREND = Decimal("0.50")  # 추세 스캘핑: 50%
RATIO_GRID = Decimal("0.20")  # 그리드 하베스트: 20%
RATIO_CASH_BUFFER = Decimal("0.30")  # 현금 버퍼: 30% (항시 유지)

TREND_STOP_LOSS_PCT = Decimal("-1.4")
TREND_HARD_STOP_PCT = Decimal("-2.5")

GRID_STEP_COUNT = 4  # 4단계 매수 그물망
GRID_INTERVAL_PCT = Decimal("0.5")  # -0.5% 간격
GRID_TARGET_TP_PCT = Decimal("0.5")  # +0.5% 합산 익절
GRID_HARD_STOP_PCT = Decimal("-2.2")  # -2.2% 하단 이탈 손절

MIN_ORDER_NOTIONAL_KRW = Decimal("5500")
COOLDOWN_TP_SEC = 180
COOLDOWN_DEFENSE_SEC = 300
FORCE_LIVE = True
LIVE_TRADING = (os.getenv("UPBIT_LIVE") == "1") or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"


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
        "cash_buffer_limit": (total_equity * RATIO_CASH_BUFFER).quantize(Decimal("1000"))
    }


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

    def get_grid_key(self) -> str:
        return f"mtpond:state:grid:{self.user_no}"

    def get_trend_key(self) -> str:
        return f"mtpond:state:trend:{self.user_no}"

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

    def set_cooldown(self, market: str, is_tp: bool):
        sec = COOLDOWN_TP_SEC if is_tp else COOLDOWN_DEFENSE_SEC
        self.cooldowns[market] = time.time() + sec
        print(f"[COOLDOWN] {market} {sec}초간 재진입 금지")

    def is_cooling_down(self, market: str) -> bool:
        return time.time() < self.cooldowns.get(market, 0.0)


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

    async with httpx.AsyncClient(timeout=8.0) as client:
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


def quantize_volume(vol: Decimal) -> Decimal:
    return vol.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


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
    return Decimal("0.01")


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
# 7. 비트코인 3단계 방어 체력 진단 & 외부 캔들 추세 서버 연동
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


_patterns_cache: Dict[str, Any] = {"ts": 0.0, "data": []}


async def fetch_all_candle_patterns() -> List[Dict[str, Any]]:
    """외부 캔들 추세 분석 서버에서 전체 코인 최신 평가 데이터를 1회 일괄 조회"""
    now = time.time()
    if (now - _patterns_cache["ts"] < BB_CACHE_TTL_SEC) and _patterns_cache["data"]:
        return _patterns_cache["data"]

    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
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
    """추세 스캘핑용: 점수(overall_score >= 3) 및 단기 거래 체결량이 풍부한 종목 추출"""
    coins = await fetch_all_candle_patterns()
    qualified = []
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-"): continue
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


async def get_grid_active_candidates() -> List[Dict[str, Any]]:
    """그리드 하베스트용: 점수 무관, 10분 체결 빈도가 가장 높은 횡보형 종목 추출"""
    coins = await fetch_all_candle_patterns()
    candidates = []
    for c in coins:
        mkt = c.get("market", "")
        if not mkt.startswith("KRW-"): continue
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
        async with httpx.AsyncClient(timeout=4.0) as client:
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
        async with httpx.AsyncClient(timeout=4.0) as client:
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

    except Exception as e:
        print(f"[LADDER-TP][ERR] {market} 사다리 주문 실패: {e}")


# ============================================================
# 9. 단일 종목 그리드 하베스트 발주 및 관리
# ============================================================
async def deploy_single_grid_harvest(access_key: str, secret_key: str, market: str, budget: Decimal, step_amt: Decimal,
                                     sm: CompositeStateManager):
    print(f"🕸️ [GRID-HARVEST] {market} 그리드 배치 (총예산: {budget:,.0f}원 | 단계당: {step_amt:,.0f}원)")
    await cancel_all_orders(access_key, secret_key, market)

    t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": market})
    cur_p = Decimal(str(t_res[0]["trade_price"]))
    grid_orders = []

    if LIVE_TRADING:
        buy_r = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
            "market": market, "side": "bid", "price": str(step_amt), "ord_type": "price"
        })
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
        "deployed_ts": time.time()
    })


async def manage_grid_position(access_key: str, secret_key: str, sm: CompositeStateManager):
    if not sm.grid_position or sm.grid_position.get("deploying"):
        return

    mkt = sm.grid_position["market"]
    accounts = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
    base = mkt.split("-")[1]
    acc = next((a for a in accounts if a.get("currency") == base), None)

    if not acc:
        sm.grid_position = None
        return

    bal = Decimal(str(acc.get("balance", "0")))
    locked = Decimal(str(acc.get("locked", "0")))
    total_vol = bal + locked
    avg_p = Decimal(str(acc.get("avg_buy_price", "0")))

    if total_vol <= 0 or (total_vol * avg_p) < MIN_ORDER_NOTIONAL_KRW:
        print(f"🎉 [GRID-COMPLETE] {mkt} 횡보 그리드 목표 달성 및 청산 완료!")
        await cancel_all_orders(access_key, secret_key, mkt)
        sm.set_cooldown(mkt, is_tp=True)
        await sm.save_grid_position(None)
        return

    t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key, secret_key, {"markets": mkt})
    cur_p = Decimal(str(t_res[0]["trade_price"]))
    if avg_p <= 0: return

    pnl = ((cur_p - avg_p) / avg_p) * Decimal("100")

    if pnl <= GRID_HARD_STOP_PCT:
        print(f"🚨 [GRID-STOP] {mkt} 박스권 하단 붕괴 ({pnl:.2f}%) → 그리드 전량 시장가 손절")
        await cancel_all_orders(access_key, secret_key, mkt)
        await asyncio.sleep(0.4)
        accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
        target_a = next((a for a in accs if a.get("currency") == base), None)
        if target_a and LIVE_TRADING:
            s_vol = quantize_volume(Decimal(str(target_a.get("balance", "0"))))
            if s_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                    "market": mkt, "side": "ask", "volume": str(s_vol), "ord_type": "market"
                })
        sm.set_cooldown(mkt, is_tp=False)
        await sm.save_grid_position(None)
        return

    if bal * cur_p >= MIN_ORDER_NOTIONAL_KRW and not sm.grid_position.get("last_tp_uuid"):
        target_tp_p = adjust_price_to_tick(avg_p * (Decimal("1") + (GRID_TARGET_TP_PCT / Decimal("100"))), side="ask")
        if LIVE_TRADING:
            tp_res = await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                "market": mkt, "side": "ask", "volume": str(quantize_volume(bal)), "price": str(target_tp_p),
                "ord_type": "limit"
            })
            sm.grid_position["last_tp_uuid"] = tp_res.get("uuid")
            print(f"🎯 [GRID-TP-PLACED] {mkt} 합산 평단가({avg_p:,.0f}원) 대비 +{GRID_TARGET_TP_PCT}% 매도 등록 (@{target_tp_p}원)")


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

    sm = CompositeStateManager(user_no, redis_client)
    await sm.restore_state_from_redis(access_key, secret_key)

    last_heartbeat_ts = time.time()
    print(f"=== [복합 포트폴리오 엔진 가동] 설정비율: 추세 50% / 그리드 20% / 현금버퍼 30% (BTC 3단계 방어 활성) ===")

    while True:
        try:
            loop_now = time.time()

            # ★ 비트코인 3단계 방어 체력 진단 호출
            btc_health = await check_btc_market_health()
            btc_status = btc_health["status"]
            btc_msg = btc_health["msg"]

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
                    tot_v = Decimal(str(a.get("balance", "0"))) + Decimal(str(a.get("locked", "0")))
                    if tot_v > 0:
                        held_coins[mkt] = tot_v

            # 2. 미배치 잔여 코인 자동 감지 및 재매도 트리거
            for a in accounts:
                cur = a.get("currency")
                if not cur or cur == "KRW": continue
                mkt = f"KRW-{cur}"
                if sm.grid_position and sm.grid_position.get("market") == mkt: continue

                b_avail = Decimal(str(a.get("balance", "0")))
                b_avg = Decimal(str(a.get("avg_buy_price", "0")))
                if b_avail * b_avg >= MIN_ORDER_NOTIONAL_KRW:
                    st = sm.trend_positions.setdefault(mkt,
                                                       {"avg_price": b_avg, "entry_ts": time.time(), "realloc_ts": 0.0})
                    if loop_now - float(st.get("realloc_ts", 0.0)) >= 10.0:
                        st["realloc_ts"] = loop_now
                        print(f"📥 [AUTO-REALLOC] {mkt} 미배치 잔고({b_avail}개) 감지 → 사다리 재배치 실행")
                        asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, mkt, sm))

            # 3. 총자산 산출
            total_coin_val = Decimal("0")
            if held_coins:
                price_map = await fetch_current_prices(list(held_coins.keys()), access_key, secret_key)
                for mkt, vol in held_coins.items():
                    total_coin_val += vol * price_map.get(mkt, Decimal("0"))

            total_equity = krw_balance + total_coin_val
            port_cfg = calculate_dynamic_portfolio(total_equity)
            trend_per_trade = port_cfg["trend_per_trade"]
            max_trend_slots = port_cfg["trend_markets"]
            grid_budget = port_cfg["grid_budget"]
            grid_step_amt = port_cfg["grid_step_amt"]
            cash_buffer_limit = port_cfg["cash_buffer_limit"]

            if loop_now - last_heartbeat_ts >= 60:
                last_heartbeat_ts = loop_now
                grid_status = sm.grid_position.get("market") if sm.grid_position else "없음"
                trend_status = list(sm.trend_positions.keys()) if sm.trend_positions else "없음"
                print(
                    f"💓 [HEARTBEAT] 총자산: {total_equity:,.0f}원 (원화: {krw_balance:,.0f}원 / 버퍼: {cash_buffer_limit:,.0f}원) | "
                    f"그리드: {grid_status} | 추세: {trend_status} | 시장상태: {btc_status}"
                )

            # 4. 그리드 포지션 관리
            await manage_grid_position(access_key, secret_key, sm)

            # 5. 추세 포지션 관리
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

                # 만약 시장이 EMERGENCY 상태이면 기존 손절선보다 타이트하게 방어 손절(-1.8%) 적용
                effective_stop_cut = TREND_HARD_STOP_PCT
                if btc_status == "EMERGENCY":
                    effective_stop_cut = Decimal("-1.8")

                if pnl <= effective_stop_cut:
                    print(f"🚨 [TREND-STOP] {mkt} 손절선 도달 (현재 PNL: {pnl:.2f}% / 기준: {effective_stop_cut}%) → 전량 시장가 청산")
                    await cancel_all_orders(access_key, secret_key, mkt)
                    await asyncio.sleep(0.4)
                    accs = await upbit_api("GET", "https://api.upbit.com/v1/accounts", access_key, secret_key)
                    cur_a = next((a for a in accs if a.get("currency") == base), None)
                    if cur_a and LIVE_TRADING:
                        s_vol = quantize_volume(Decimal(str(cur_a.get("balance", "0"))))
                        if s_vol * cur_p >= MIN_ORDER_NOTIONAL_KRW:
                            await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                                "market": mkt, "side": "ask", "volume": str(s_vol), "ord_type": "market"
                            })
                    sm.set_cooldown(mkt, is_tp=False)
                    await sm.remove_trend_position(mkt)

                # 6. 신규 매수 탐색 (★ BTC 상태 및 현금버퍼 30% 보존 조건 검증)
                if btc_status == "EMERGENCY":
                    print(f"🛑 {btc_msg} -> 모든 신규 매수 및 진입을 전면 차단합니다.")
                elif btc_status == "CAUTION":
                    print(f"⚠️ {btc_msg} -> 조정세 구간: 신규 매수를 일시 보류합니다.")
                else:
                    # [NORMAL 상태]
                    investable_krw = krw_balance - cash_buffer_limit
                    trend_slots_remain = max_trend_slots - len(sm.trend_positions)

                    trend_executed = False  # 이번 루프에서 추세 스캘핑 진입 여부 추적

                    # -------------------------------------------------------------
                    # [1순위] 추세 스캘핑 적극 탐색 (상승 모멘텀 종목 우선 공략)
                    # -------------------------------------------------------------
                    if trend_slots_remain > 0 and investable_krw >= trend_per_trade:
                        trend_candidates = await get_trend_scalping_candidates()
                        for cand_item in trend_candidates:
                            cand = cand_item["market"]
                            if cand not in held_coins and cand != (
                            sm.grid_position.get("market") if sm.grid_position else None):
                                if not sm.is_cooling_down(cand) and await check_bullish_trend(cand):
                                    t_res = await upbit_api("GET", "https://api.upbit.com/v1/ticker", access_key,
                                                            secret_key, {"markets": cand})
                                    cur_p = Decimal(str(t_res[0]["trade_price"]))

                                    print(
                                        f"🚀 [TREND-BUY] 모멘텀 진입: {cand} (점수: {cand_item['overall_score']}) | 금액: {trend_per_trade:,.0f}원")
                                    if LIVE_TRADING:
                                        await upbit_api("POST", UPBIT_ORDER_URL, access_key, secret_key, {
                                            "market": cand, "side": "bid", "price": str(trend_per_trade),
                                            "ord_type": "price"
                                        })
                                    await sm.save_trend_position(cand,
                                                                    {"avg_price": cur_p, "entry_ts": time.time()})
                                    asyncio.create_task(place_ladder_tp_orders(access_key, secret_key, cand, sm))
                                    krw_balance -= trend_per_trade
                                    investable_krw -= trend_per_trade
                                    trend_executed = True  # 추세 진입 성공 플래그 ON
                                    break

                    # -------------------------------------------------------------
                    # [2순위] 추세 스캘핑이 저조할 때(진입 종목이 없을 때) 그리드 하베스트 발동
                    # -------------------------------------------------------------
                    if not trend_executed and sm.grid_position is None and investable_krw >= grid_budget:
                        grid_candidates = await get_grid_active_candidates()
                        for cand_item in grid_candidates:
                            cand = cand_item["market"]
                            if cand not in held_coins:
                                if await check_sideways_grid_candidate(cand):
                                    print(
                                        f"🎯 [GRID-PICK] 추세 스캘핑 부진/조건 미달 → 횡보 그리드 하베스트 전환 발동: {cand} (10분 거래수: {cand_item['trade_count']})")
                                    sm.grid_position = {"market": cand, "deploying": True}
                                    asyncio.create_task(
                                        deploy_single_grid_harvest(access_key, secret_key, cand, grid_budget,
                                                                    grid_step_amt, sm))
                                    krw_balance -= grid_budget
                                    investable_krw -= grid_budget
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
            active_flag = True
            raw_status = await redis_client.get(f"mtpond:status:{user_no}")
            if raw_status is not None:
                active_flag = raw_status.strip().upper() in ("ON", "1", "Y", "TRUE")
            else:
                c = await redis_client.get(f"mtpond:setup:{user_no}")
                if c:
                    data = json.loads(c)
                    cfg = data[0] if isinstance(data, list) and len(data) > 0 else data
                    if isinstance(cfg, dict):
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
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 봇을 안전하게 종료합니다.")