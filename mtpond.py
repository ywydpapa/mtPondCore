"""웹 설정에 지정된 mtpond 전략 엔진을 실행하는 진입점."""

import asyncio
import importlib
import json
import os
from decimal import Decimal, InvalidOperation
from typing import Any

import dotenv
import httpx
import redis.asyncio as aioredis


dotenv.load_dotenv()

USER_NO = int(os.getenv("USER_NO", "100001"))
API_BASE = os.getenv("API_BASE", "").rstrip("/")
SETUP_REDIS_KEY = f"mtpond:setup:{USER_NO}"


def create_redis_client() -> aioredis.Redis:
    redis_host = os.getenv("REDIS_HOST")
    if redis_host:
        return aioredis.Redis(
            host=redis_host,
            port=int(os.getenv("REDIS_PORT", "56379")),
            password=os.getenv("REDIS_PASSWORD"),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            retry_on_timeout=True,
        )

    return aioredis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        retry_on_timeout=True,
    )


def parse_setup(data: Any) -> dict[str, Any]:
    if isinstance(data, list):
        data = data[0] if data else None
    if not isinstance(data, dict):
        raise ValueError("웹 설정 응답이 객체가 아닙니다.")
    return data


async def fetch_setup(redis_client: aioredis.Redis) -> dict[str, Any]:
    if not API_BASE:
        raise RuntimeError(
            "API_BASE가 설정되지 않아 최신 웹 설정 버전을 확인할 수 없습니다."
        )

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(5.0, connect=3.0)
    ) as client:
        response = await client.get(f"{API_BASE}/api/mtpondsetup/{USER_NO}")
        response.raise_for_status()
        setup = parse_setup(response.json())

    engine_version = get_engine_version(setup)

    try:
        cached = await redis_client.get(SETUP_REDIS_KEY)
    except Exception as exc:
        print(f"[CONFIG][WARN] Redis 설정 조회 실패: {exc}")
        cached = None

    if cached:
        try:
            cached_setup = parse_setup(json.loads(cached))
            cached_version = get_engine_version(cached_setup)
            if cached_version != engine_version:
                print(
                    f"[CONFIG] 엔진 버전 변경 감지: "
                    f"{cached_version} → {engine_version}"
                )
            else:
                print(f"[CONFIG] 엔진 버전 변경 없음: {engine_version}")
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"[CONFIG][WARN] Redis 설정 데이터가 올바르지 않습니다: {exc}")

    try:
        await redis_client.set(SETUP_REDIS_KEY, json.dumps(setup), ex=3600)
    except Exception as exc:
        print(f"[CONFIG][WARN] Redis 설정 캐시 저장 실패: {exc}")

    return setup


def get_engine_version(setup: dict[str, Any]) -> int:
    raw_version = setup.get("activeVer")
    if raw_version is None:
        raise ValueError(
            "웹 설정에 activeVer 값이 없습니다. API 설정에 1~6 정수 값을 지정하세요."
        )

    try:
        version = Decimal(str(raw_version).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            f"웹 설정 activeVer 값이 올바르지 않습니다: {raw_version!r}"
        ) from exc

    if not version.is_finite() or version != version.to_integral_value():
        raise ValueError(f"웹 설정 activeVer는 1~6 정수여야 합니다: {raw_version!r}")

    engine_version = int(version)
    if engine_version not in range(1, 7):
        raise ValueError(
            f"지원하지 않는 activeVer={engine_version}; 1~6 중 하나를 선택하세요."
        )
    return engine_version


async def main() -> None:
    redis_client = create_redis_client()
    try:
        setup = await fetch_setup(redis_client)
        engine_version = get_engine_version(setup)
        module_name = f"mtpond{engine_version}"
        print(f"[MAIN] 웹 설정 activeVer={engine_version} → {module_name}.py 실행")

        strategy = importlib.import_module(module_name)
        await strategy.main()
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청(Ctrl+C)")
