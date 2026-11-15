"""공통 시장가 매수, 수량 처리 및 손절 기록 API."""

import os
import time
import httpx
from decimal import Decimal, ROUND_DOWN
from typing import Any, Awaitable, Callable, Optional, Dict, Set, Tuple, List

_VOLATILITY_CACHE: Dict[str, Dict[str, Any]] = {}
VOLATILITY_CACHE_TTL_SEC = 300  # 5분 캐시


def calculate_dynamic_stops(v30: float) -> Dict[str, Decimal]:
    """
    30분봉 진동폭(v30 %)을 기반으로 코인별 1파동 복구형 동적 손절/구출 한계선을 계산한다.
    단계별 서열 구조:
    1. 정상 매매 및 진동 구간 (0% ~ rescue_min)
    2. 구출/물타기 반등 확인 구간 (rescue_min ~ rescue_max: -3.0% ~ -8.0%)
    3. 최종 하드 손절선 (hard_stop: 구출 구간 이탈 또는 물타기 소진 후 방어선, -7.0% ~ -9.5%)
    4. 대폭락 강제 안전망 (max_drop: 사용자 지정 -15% 수준 안전망, -14.0% ~ -15.0% 캡)
    """
    v30_safe = max(1.0, float(v30))

    # 1) 구출(Averaging) 허용 구간: -3.0% ~ -8.0%
    rescue_min = -max(3.0, min(4.5, v30_safe * 1.0))
    rescue_max = -max(6.0, min(8.0, v30_safe * 2.0))
    if rescue_min <= rescue_max:
        rescue_max = rescue_min - 2.0

    # 2) 최종 하드 손절선 (구출 구간 이탈 후 또는 물타기 소진 시 최종 방어선: -7.0% ~ -9.5%)
    hard_sl = -max(7.0, min(9.5, v30_safe * 2.2))
    if hard_sl >= rescue_max:
        hard_sl = rescue_max - 0.5

    # 3) 대폭락 강제 손절선 (-15% 수준 안전망)
    # 30분봉 진동폭을 연동하되, 사용자 지정(-15% 수준)에 맞춰 최대 -15.0%로 엄격히 제한
    env_max = os.getenv("MAX_DROP_LOSS_CUT_PCT")
    try:
        max_cap = abs(float(env_max)) if env_max else 15.0
    except (ValueError, TypeError):
        max_cap = 15.0

    max_drop = -max(min(14.0, max_cap), min(max_cap, v30_safe * 4.2))
    if max_drop >= hard_sl:
        max_drop = min(-Decimal(str(round(max_cap, 2))), Decimal(str(round(hard_sl, 2))) - Decimal("2.0"))

    return {
        "v30": Decimal(str(round(v30_safe, 2))),
        "hard_stop": Decimal(str(round(hard_sl, 2))),
        "rescue_min_drop": Decimal(str(round(rescue_min, 2))),
        "rescue_max_drop": Decimal(str(round(rescue_max, 2))),
        "max_drop": Decimal(str(round(max_drop, 2))),
    }


async def get_dynamic_stop_limits(
    market: str,
    patterns_data: Optional[list] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Decimal]:
    """
    지정한 마켓의 최신 30분봉 평균 진동폭을 조회하여 동적 손절 한계를 반환한다.
    1. 외부 bb-patterns 캐시 데이터에서 30m expected_volatility_pct 확인
    2. 메모리 캐시 확인 (5분 TTL)
    3. 업비트 30분봉 API 14개로 직접 계산
    4. 실패 시 기본 진동폭 3.28% 적용
    """
    now = time.time()

    # 1. 전달받은 patterns_data 탐색
    if patterns_data:
        for item in patterns_data:
            if item.get("market") == market:
                tf_30 = item.get("timeframes", {}).get("30m", {})
                v = tf_30.get("expected_volatility_pct")
                if v and v > 0:
                    _VOLATILITY_CACHE[market] = {"val": float(v), "ts": now}
                    return calculate_dynamic_stops(float(v))

    # 2. 로컬 메모리 캐시 확인
    cached = _VOLATILITY_CACHE.get(market)
    if cached and (now - cached["ts"]) < VOLATILITY_CACHE_TTL_SEC:
        return calculate_dynamic_stops(cached["val"])

    # 3. 업비트 30분봉 캔들 조회 직접 계산
    url = f"https://api.upbit.com/v1/candles/minutes/30?market={market}&count=15"
    try:
        if client is not None:
            res = await client.get(url, timeout=3.5)
        else:
            async with httpx.AsyncClient(timeout=3.5) as local_client:
                res = await local_client.get(url)

        if res.status_code == 200:
            candles = res.json()
            if isinstance(candles, list) and len(candles) >= 5:
                ranges = []
                for c in candles:
                    o = float(c.get("opening_price", 0))
                    h = float(c.get("high_price", 0))
                    l = float(c.get("low_price", 0))
                    if o > 0:
                        ranges.append((h - l) / o * 100.0)
                if ranges:
                    avg_v = sum(ranges) / len(ranges)
                    _VOLATILITY_CACHE[market] = {"val": avg_v, "ts": now}
                    return calculate_dynamic_stops(avg_v)
    except Exception as e:
        print(f"[DYNAMIC-STOP][WARN] {market} 진동폭 계산 실패(폴백 사용): {e}")

    # 4. 폴백: 기본 진동폭 3.28%
    return calculate_dynamic_stops(3.28)


def quantize_volume(volume: Decimal) -> Decimal:
    """업비트 주문 수량을 소수점 8자리로 내림한다."""
    return volume.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


async def market_buy(
    request: Callable[..., Awaitable[Any]],
    access_key: str,
    secret_key: str,
    market: str,
    krw_amount: Decimal,
    order_url: str,
    **request_options: Any,
) -> Any:
    """지정한 업비트 요청 함수를 사용해 KRW 금액 기준 시장가 매수를 실행한다."""
    params = {
        "market": market,
        "side": "bid",
        "price": str(krw_amount),
        "ord_type": "price",
    }
    return await request(
        "POST",
        order_url,
        access_key,
        secret_key,
        params=params,
        **request_options,
    )


async def register_loss_cut(
    api_base: str,
    user_no: int,
    market: str,
    quantity: Decimal,
    price: Decimal,
    loss_rate: Decimal,
    client: Optional[httpx.AsyncClient] = None,
) -> None:
    if not api_base:
        print("[LOSS-CUT-API][WARN] API_BASE가 설정되지 않아 손절 기록을 저장하지 못했습니다.")
        return

    payload = {
        "userNo": user_no,
        "coinCode": market,
        "losscutQty": float(quantity),
        "losscutPrice": float(price),
        "currentPrice": float(price),
        "lossRate": float(loss_rate),
        "losscutReason": "손절 조건 도달",
    }

    try:
        if client is None:
            async with httpx.AsyncClient(timeout=4.0) as request_client:
                response = await request_client.post(
                    f"{api_base}/api/regLC", json=payload
                )
                response.raise_for_status()
        else:
            response = await client.post(f"{api_base}/api/regLC", json=payload)
            response.raise_for_status()
        print(f"[LOSS-CUT-API] {market} 손절 기록 저장 완료")
    except httpx.HTTPError as error:
        print(f"[LOSS-CUT-API][ERR] {market} 손절 기록 저장 실패: {error}")


# ============================================================
# 업비트 거래 가능 마켓 캐싱
# ============================================================
_TRADABLE_MARKETS_SET: Set[str] = set()
_TRADABLE_MARKETS_TS: float = 0.0
_TRADABLE_MARKETS_CACHE_TTL_SEC: float = 1800.0  # 30분 캐시


async def get_upbit_tradable_markets(
    force: bool = False,
    client: Optional[httpx.AsyncClient] = None
) -> Set[str]:
    """업비트 전체 마켓 중 현재 거래 가능한(상장폐지/거래정지 제외) KRW 마켓 목록을 조회하여 캐싱."""
    global _TRADABLE_MARKETS_SET, _TRADABLE_MARKETS_TS
    now = time.time()
    if (not force) and _TRADABLE_MARKETS_SET and (now - _TRADABLE_MARKETS_TS < _TRADABLE_MARKETS_CACHE_TTL_SEC):
        return set(_TRADABLE_MARKETS_SET)

    url = "https://api.upbit.com/v1/market/all?isDetails=true"
    try:
        if client is not None:
            res = await client.get(url, timeout=5.0)
        else:
            async with httpx.AsyncClient(timeout=5.0) as local_client:
                res = await local_client.get(url)

        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list):
                valid = {
                    item["market"] for item in data
                    if isinstance(item, dict)
                    and item.get("market", "").startswith("KRW-")
                    and item.get("market_warning") != "DELISTING"
                }
                if valid:
                    _TRADABLE_MARKETS_SET = valid
                    _TRADABLE_MARKETS_TS = now
                    return set(valid)
    except Exception as e:
        print(f"[MARKETS][WARN] 업비트 마켓 목록 조회 실패: {e}")

    return set(_TRADABLE_MARKETS_SET)


# ============================================================
# 3분봉 / 15분봉 캔들 조회 및 캐싱
# ============================================================
_candle_3m_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
CANDLE_3M_CACHE_TTL = 3.0

_candle_15m_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
CANDLE_15M_CACHE_TTL = 15.0


async def get_3m_candles(
    market: str,
    count: int = 8,
    client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    """업비트 3분봉 캔들 데이터 조회 (캐시 3초)"""
    now = time.time()
    cached = _candle_3m_cache.get(market)
    if cached and (now - cached[0]) < CANDLE_3M_CACHE_TTL:
        return cached[1]

    url = "https://api.upbit.com/v1/candles/minutes/3"
    try:
        if client is not None:
            res = await client.get(url, params={"market": market, "count": count}, timeout=4.0)
        else:
            async with httpx.AsyncClient(timeout=4.0) as local_client:
                res = await local_client.get(url, params={"market": market, "count": count})

        if res.status_code == 200:
            candles = res.json()
            if isinstance(candles, list):
                _candle_3m_cache[market] = (now, candles)
                return candles
    except Exception as e:
        print(f"[CANDLE-3M][WARN] {market} 3분봉 조회 실패: {e}")
    return []


async def get_15m_candles(
    market: str,
    count: int = 22,
    client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    """업비트 15분봉 캔들 데이터 조회 (캐시 15초)"""
    now = time.time()
    cached = _candle_15m_cache.get(market)
    if cached and (now - cached[0]) < CANDLE_15M_CACHE_TTL:
        return cached[1]

    url = "https://api.upbit.com/v1/candles/minutes/15"
    try:
        if client is not None:
            res = await client.get(url, params={"market": market, "count": count}, timeout=4.0)
        else:
            async with httpx.AsyncClient(timeout=4.0) as local_client:
                res = await local_client.get(url, params={"market": market, "count": count})

        if res.status_code == 200:
            candles = res.json()
            if isinstance(candles, list):
                _candle_15m_cache[market] = (now, candles)
                return candles
    except Exception as e:
        print(f"[CANDLE-15M][WARN] {market} 15분봉 조회 실패: {e}")
    return []


# ============================================================
# 15분봉 상위 추세 필터 - 떨어지는 칼날 배제
# ============================================================
async def check_15m_uptrend_or_range(
    market: str,
    candles: Optional[List[Dict[str, Any]]] = None,
    client: Optional[httpx.AsyncClient] = None
) -> Tuple[bool, str]:
    """
    [15분봉 상위 추세 필터 - 떨어지는 칼날 배제]
    - 현재가가 15분봉 20 이동평균선(MA20) 대비 1.5% 이상 하회 중이면 계단식 하락장으로 판별하여 진입 차단.
    - 최근 1시간(15분봉 4개) 누적 하락률이 -2.5% 이하인 종목 배제.
    """
    if candles is None:
        candles = await get_15m_candles(market, count=22, client=client)
    if not candles or len(candles) < 20:
        return True, "15M_DATA_INSUFFICIENT_SKIP"

    try:
        cur_p = Decimal(str(candles[0]["trade_price"]))
        prev_1h_p = Decimal(str(candles[4]["trade_price"]))
        closes_20 = [Decimal(str(c["trade_price"])) for c in candles[:20]]
        ma20 = sum(closes_20) / Decimal("20")

        pnl_1h = ((cur_p - prev_1h_p) / prev_1h_p) * Decimal("100")
        if pnl_1h <= Decimal("-2.5"):
            return False, f"15분봉 기준 1시간 급락 진행 중 ({pnl_1h:.2f}%)"

        if cur_p < (ma20 * Decimal("0.985")):
            return False, f"15분봉 MA20 하향 이탈 상태 ({cur_p} < {ma20:.4f})"

        return True, "15분봉 횡보/상승 추세 양호"
    except Exception as e:
        return True, f"15M_EVAL_SKIP({e})"


# ============================================================
# 3분봉 장악형 양봉 반전 신호 검증
# ============================================================
def evaluate_3m_reversal(candles: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    [강화된 3분봉 장악형 반전/반등 신호 순수 평가 함수]
    - 단순 1틱짜리 데드캣 바운스(숨고르기 양봉)를 걸러내기 위해 다음 조건을 모두 충족해야 반등으로 인정:
      1. 현재 3분봉이 확실한 양봉(trade_price > opening_price)이어야 함.
      2. 직전 3분봉이 음봉이었다면, 현재 양봉 종가가 직전 음봉의 시가(p1_open) 이상을 장악(돌파)해야 함.
      3. 최근 3개 3분봉 종가 평균(MA3)을 상회해야 함.
      4. 최근 5개 3분봉 중 음봉이 4개 이상인 극심한 투매 구간에서는 직전 봉 고가(p1_high)까지 돌파해야 인정.
    """
    if not candles or len(candles) < 5:
        return False, "CANDLE_DATA_UNAVAILABLE"

    try:
        c0, c1, c2 = candles[0], candles[1], candles[2]

        p0_close = Decimal(str(c0["trade_price"]))
        p0_open = Decimal(str(c0["opening_price"]))

        p1_close = Decimal(str(c1["trade_price"]))
        p1_open = Decimal(str(c1["opening_price"]))
        p1_high = Decimal(str(c1.get("high_price", c1.get("trade_price"))))

        p2_close = Decimal(str(c2["trade_price"]))

        # 1. 현재 3분봉이 확실한 양봉이어야 함 (시가 동일 도지/음봉 배제)
        if p0_close <= p0_open:
            return False, f"현재 3분봉 양봉 미전환 ({p0_close} <= {p0_open})"

        # 2. 직전 봉이 음봉이었다면 직전 음봉의 몸통(시가) 이상을 장악해야 함
        if p1_close < p1_open:
            if p0_close < p1_open:
                return False, f"직전 음봉 시가 미돌파 ({p0_close} < {p1_open})"

        # 3. 3봉 종가 이동평균(MA3) 상회 검증
        ma3 = (p0_close + p1_close + p2_close) / Decimal("3")
        if p0_close <= ma3:
            return False, f"3분봉 MA3 미달 ({p0_close} <= {ma3:.4f})"

        # 4. 최근 5개 봉(c1~c5) 중 음봉이 4개 이상인 지속 투매 구간이면 직전 봉 고가(p1_high) 돌파까지 요구
        recent_reds = sum(
            1 for c in candles[1:6]
            if Decimal(str(c["trade_price"])) < Decimal(str(c["opening_price"]))
        )
        if recent_reds >= 4 and p0_close <= p1_high:
            return False, f"연속 투매(음봉 {recent_reds}개) 후 직전 고가({p1_high}) 미돌파"

        return True, "3분봉 장악형 양봉 반전 확인"
    except Exception as e:
        return False, f"CANDLE_EVAL_ERROR({e})"


async def check_3m_reversal_signal(
    market: str,
    candles: Optional[List[Dict[str, Any]]] = None,
    client: Optional[httpx.AsyncClient] = None
) -> Tuple[bool, str]:
    """[3분봉 하락 연속 배제 및 반전/반등 신호 감지 비동기 래퍼]"""
    if candles is None:
        candles = await get_3m_candles(market, count=8, client=client)
    return evaluate_3m_reversal(candles)

