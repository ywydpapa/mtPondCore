"""공통 시장가 매수, 수량 처리 및 손절 기록 API."""

import os
import time
import httpx
from decimal import Decimal, ROUND_DOWN
from typing import Any, Awaitable, Callable, Optional, Dict

_VOLATILITY_CACHE: Dict[str, Dict[str, Any]] = {}
VOLATILITY_CACHE_TTL_SEC = 300  # 5분 캐시


def calculate_dynamic_stops(v30: float) -> Dict[str, Decimal]:
    """
    30분봉 진동폭(v30 %)을 기반으로 코인별 1파동 복구형 동적 손절/구출 한계선을 계산한다.
    - hard_stop: 30분봉 진동폭 기반 최종 방어 손절선 (최소 -5.5%, 최대 -8.0% 캡)
    - rescue_min_drop: 구출/물타기 시작점 -(v30 * 1.0) (최소 -3.0%, 최대 -4.5%)
    - rescue_max_drop: 구출 진입 허용 하한선 -(v30 * 2.0) (최소 -6.0%, 최대 -8.5%)
    - max_drop: 대폭락 강제 손절 한계선 (사용자 지정: -15% 수준 안전망)
    """
    v30_safe = max(1.0, float(v30))

    # 1) 최종 하드 손절 (물타기 소진 또는 방어 실패 후 기준선: -5.5% ~ -8.0% 캡)
    hard_sl = -max(5.5, min(8.0, v30_safe * 1.8))

    # 2) 구출(Averaging) 허용 구간
    rescue_min = -max(3.0, min(4.5, v30_safe * 1.0))
    rescue_max = -max(6.0, min(8.5, v30_safe * 2.0))
    if rescue_min <= rescue_max:
        rescue_max = rescue_min - 2.0

    # 3) 대폭락 강제 손절선 (-15% 수준 안전망)
    # 30분봉 진동폭을 연동하되, 사용자 지정(-15% 수준)에 맞춰 -14.0% ~ -15.0% 범위로 캡 설정
    env_max = os.getenv("MAX_DROP_LOSS_CUT_PCT")
    try:
        max_cap = abs(float(env_max)) if env_max else 15.0
    except (ValueError, TypeError):
        max_cap = 15.0

    max_drop = -max(min(14.0, max_cap), min(max_cap, v30_safe * 4.2))
    if max_drop >= rescue_max:
        max_drop = min(-Decimal(str(round(max_cap, 2))), rescue_max - Decimal("2.0"))

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
