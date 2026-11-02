"""mtpond1~6에서 공유하는 환경 설정과 기본 전략 기준."""

import os
from decimal import Decimal, InvalidOperation

import dotenv


dotenv.load_dotenv()

API_BASE = os.getenv("API_BASE", "").rstrip("/")
BASE_UNIT = "KRW"
BB_PATTERNS_API_URL = os.getenv(
    "BB_PATTERNS_API_URL",
    "http://becog.iptime.org:8088/api/bb-patterns",
)
BB_MIN_TRADE_COUNT_10M = int(os.getenv("BB_MIN_TRADE_COUNT_10M", "60"))

RESCUE_MIN_DROP = Decimal("-8.0")
RESCUE_MAX_DROP = Decimal("-22.0")
MAX_CONCURRENT_RESCUE = 1
raw_min_order_notional = os.getenv("MIN_ORDER_NOTIONAL_KRW", "5500")
try:
    MIN_ORDER_NOTIONAL_KRW = Decimal(
        raw_min_order_notional.split("#", 1)[0].strip()
    )
except (InvalidOperation, ValueError, TypeError) as error:
    raise ValueError(
        "환경 변수 MIN_ORDER_NOTIONAL_KRW 값이 유효한 Decimal 숫자가 "
        f"아닙니다: {raw_min_order_notional!r}"
    ) from error


def is_stop_enabled() -> bool:
    return os.getenv("ENABLE_STOP", "1").strip().upper() in ("1", "Y", "TRUE")


def get_bb_min_trade_count_10m() -> int:
    return int(
        os.getenv("BB_MIN_TRADE_COUNT_10M", str(BB_MIN_TRADE_COUNT_10M))
    )


ENABLE_STOP = is_stop_enabled()
FORCE_LIVE = True
LIVE_TRADING = os.getenv("UPBIT_LIVE") == "1" or FORCE_LIVE
UPBIT_ORDER_URL = "https://api.upbit.com/v1/orders"
