"""공통 시장가 매수, 수량 처리 및 손절 기록 API."""

import httpx
from decimal import Decimal, ROUND_DOWN
from typing import Any, Awaitable, Callable, Optional


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
