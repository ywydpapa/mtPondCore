from decimal import Decimal
import os
import ccxt

def create_bithumb(api_key: str, secret: str, rate_limit: bool = True) -> ccxt.Exchange:
    ex = ccxt.bithumb({
        "apiKey": "1deea37ce554fffa65601bbbcf0c77ca8dfe2a662bacee",
        "secret": "MjAxYzg0MjZjZmMyMGY5N2E4N2VlMjUxZmNmODE2MTAxZGI5YzIwMzc5YzA4ZmY2MjU2MDI4YWVlZWMxMQ==",
        "enableRateLimit": rate_limit,
    })
    # 마켓 메타 로드(정밀도/심볼 등 내부 캐시)
    ex.load_markets()
    return ex

def read_wallet(ex: ccxt.Exchange, include_zero: bool = False):
    """
    빗썸 지갑(현물) 잔고를 읽어 리스트로 반환합니다.
    반환 예:
    [
      {"currency": "KRW", "free": Decimal("12345"), "locked": Decimal("0"), "total": Decimal("12345")},
      {"currency": "BTC", "free": Decimal("0.001"), "locked": Decimal("0"), "total": Decimal("0.001")},
      ...
    ]
    - include_zero=False면 총 잔고가 0인 통화는 제외합니다.
    """
    bal = ex.fetch_balance()

    free_map = bal.get("free", {}) or {}
    used_map = bal.get("used", {}) or {}
    total_map = bal.get("total", {}) or {}

    out = []
    for cur, total in total_map.items():
        # ccxt는 'free'/'used' 맵에 동일 통화 키가 들어 있습니다.
        free_v = free_map.get(cur, 0) or 0
        used_v = used_map.get(cur, 0) or 0

        total_d = Decimal(str(total or 0))
        free_d = Decimal(str(free_v))
        used_d = Decimal(str(used_v))

        if not include_zero and total_d == 0:
            continue

        out.append({
            "currency": cur,
            "free": free_d,
            "locked": used_d,
            "total": total_d,
        })

    # KRW를 위로 정렬
    out.sort(key=lambda x: (x["currency"] != "KRW", x["currency"]))
    return out

if __name__ == "__main__":
    # 실제 키는 환경변수나 .env에서 불러오세요
    API_KEY = "cc75da1ff0ef4debffefde4670fe3e29"
    API_SECRET = "ae812b4e6fe923fb29a7a1f9302315dd"

    if not API_KEY or not API_SECRET:
        raise SystemExit("환경변수 BITHUMB_API_KEY / BITHUMB_API_SECRET를 설정하세요.")

    ex = create_bithumb(API_KEY, API_SECRET)
    wallet = read_wallet(ex, include_zero=False)
    for row in wallet:
        print(f"{row['currency']}: free={row['free']}, locked={row['locked']}, total={row['total']}")
