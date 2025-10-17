# ============================================================
# index_monitor_service.py
# 인덱스(폭락) 모니터링 단일 프로세스
#  - ENV + DB에서 코인별 임계값/인터벌 + 실행간격(poll_sec) 로딩
#  - 캔들 조회 후 변동률 계산하여 하락 신호를 외부로 전달
# ============================================================

import os
import asyncio
import time
from decimal import Decimal as D
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, false
import aiohttp
from datetime import datetime, timezone, timedelta
import dotenv
from typing import List, Dict, Any, Optional
import json
dotenv.load_dotenv()

db_url = os.getenv("dburl")
if not db_url:
    raise RuntimeError("Environment variable 'dburl' is required")
engine = create_async_engine(db_url, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# API_BASE는 현재 사용하지 않으므로 제거하거나 향후 사용 시 활성화
# API_BASE = os.getenv("API_BASE", "").rstrip("/")

_UPBIT_MIN_UNITS = {1, 3, 5, 10, 15, 30, 60, 240}
KST = timezone(timedelta(hours=9))

async def get_indexcoins() -> list[tuple] | None:
    async with SessionLocal() as session:
        # 스키마: icMarket, icMonrate, icMoninterval
        # 필요 시 icMonpollsec 추가 후 SELECT 컬럼 확장
        sql = text("""SELECT icMarket, icMonrate, icMoninterval FROM indexCoinlist WHERE attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"attr": "%XXX%"})
        return result.fetchall()

def _coerce_unit(unit_min: int) -> int:
    allowed = sorted(_UPBIT_MIN_UNITS)
    # 가장 가까운 허용 단위로 보정
    return min(allowed, key=lambda x: abs(x - unit_min))


async def fetch_candles(
    market: str,
    unit_min: int,
    count: int = 10,
    *,
    session: aiohttp.ClientSession | None = None,
    retry: int = 2,
    cooldown_sec: float = 0.15
) -> list[dict]:
    """
    Upbit 분봉 캔들 조회
    - endpoint: /v1/candles/minutes/{unit}
    - 파라미터: market, count
    - 응답: 최신순 배열
    반환 형식(내부 표준화):
      [
        { "opening_price": Decimal/str, "trade_price": Decimal/str, "timestamp": int(ms) },
        ...
      ]
    주의:
      - unit_min은 Upbit가 허용하는 값 중 하나여야 합니다: 1,3,5,10,15,30,60,240
      - 응답 최신순이지만, 호출부에서는 timestamp로 다시 정렬해 사용합니다.
    """
    if unit_min not in _UPBIT_MIN_UNITS:
        # 불허 단위면 보정하여 사용(경고)
        coerced = _coerce_unit(unit_min)
        # 선택: 엄격히 막으려면 ValueError 발생
        # raise ValueError(f"Upbit 지원 단위가 아닙니다. unit_min={unit_min} allowed={sorted(_UPBIT_MIN_UNITS)}")
        unit_min = coerced

    url = f"https://api.upbit.com/v1/candles/minutes/{unit_min}"
    params = {"market": market, "count": min(max(int(count), 1), 200)}  # Upbit count 최대 200
    headers = {
        "Accept": "application/json"
    }

    owns_session = False
    if session is None:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        owns_session = True

    try:
        attempt = 0
        while True:
            attempt += 1
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 429:
                        # 레이트리밋 → 짧게 대기 후 재시도
                        retry_after = float(resp.headers.get("Retry-After", "0.5"))
                        await asyncio.sleep(max(cooldown_sec, retry_after))
                        if attempt <= retry:
                            continue
                        raise RuntimeError(f"Upbit 429 rate limited after {attempt} attempts")
                    if resp.status >= 500:
                        # 서버 에러 → 재시도
                        if attempt <= retry:
                            await asyncio.sleep(cooldown_sec * attempt)
                            continue
                        text_body = await resp.text()
                        raise RuntimeError(f"Upbit 5xx error: {resp.status} {text_body}")
                    if resp.status != 200:
                        text_body = await resp.text()
                        raise RuntimeError(f"Upbit error: {resp.status} {text_body}")

                    data = await resp.json(content_type=None)

                    out: list[dict] = []
                    for c in data:
                        ts = c.get("timestamp")
                        # Upbit timestamp는 ms 단위(epoch ms)
                        if ts is None:
                            # KST 문자열을 KST 타임존으로 해석 후 epoch ms 변환
                            try:
                                kst_str = c.get("candle_date_time_kst")
                                if kst_str:
                                    dt_kst = datetime.strptime(kst_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=KST)
                                    ts = int(dt_kst.timestamp() * 1000)
                                else:
                                    continue
                            except Exception:
                                continue
                        out.append({
                            "opening_price": str(c.get("opening_price")),
                            "trade_price": str(c.get("trade_price")),
                            "timestamp": int(ts)
                        })

                    # timestamp 오름차순 정렬하여 반환
                    out.sort(key=lambda x: x["timestamp"])
                    return out
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                if attempt <= retry:
                    await asyncio.sleep(cooldown_sec * attempt)
                    continue
                raise RuntimeError(f"Upbit 네트워크 오류 재시도 초과: {e}") from e
    finally:
        if owns_session:
            await session.close()

# ------------------------------------------------------------
# INDEX 설정 컨테이너 (ENV + DB 병합, per-market 지원)
# ------------------------------------------------------------
INDEX_CFG: Dict[str, Any] = {
    "defaults": {
        "interval_min": 5,                # int (분)
        "drop_rate_pct": D("-3.0"),       # Decimal (%)
        "auto_restart_rate_pct": D("2.0"),# Decimal (%)
        "poll_sec": 10.0                  # float (초) - ENV/DB로 덮어쓰기
    },
    "by_market": {},  # { "KRW-BTC": {"interval_min": 5, "drop_rate_pct": D("-1.5")} }
    "markets": []     # 모니터링 대상 목록
}

# ------------------------------------------------------------
# 파서 & 로더
# ------------------------------------------------------------
def _parse_markets_env(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    seps = [",", "\n", "\t", " "]
    txt = raw.strip()
    for s in seps[1:]:
        txt = txt.replace(s, seps[0])
    parts = [p.strip().upper() for p in txt.split(seps[0]) if p.strip()]
    cleaned = []
    for p in parts:
        if "-" not in p:
            continue
        base, cur = p.split("-", 1)
        if base == "KRW" and cur:
            cleaned.append(f"{base}-{cur}")
    out, seen = [], set()
    for m in cleaned:
        if m not in seen:
            out.append(m); seen.add(m)
    return out

def _parse_interval_env(raw: Optional[str], default_min: int = 5) -> int:
    if not raw:
        return default_min
    t = str(raw).strip().lower()
    if t.endswith("m"):
        t = t[:-1]
    try:
        v = int(t)
        return v if v > 0 else default_min
    except Exception:
        print(f"[INDEX][WARN] 잘못된 INDEX_INTERVAL='{raw}' → default {default_min}m")
        return default_min

def _parse_percent_env(raw: Optional[str], default_pct: D) -> D:
    if raw is None:
        return default_pct
    s = str(raw).strip().replace("%", "")
    try:
        v = D(s)
        return v
    except Exception:
        print(f"[INDEX][WARN] 잘못된 퍼센트 '{raw}' → default {default_pct}%")
        return default_pct

def parse_index_env_defaults():
    global INDEX_CFG
    raw_interval = os.getenv("INDEX_INTERVAL", "5m")
    raw_drop = os.getenv("INDEX_RATE", "-3%")
    raw_restart = os.getenv("INDEX_AUTORESTART_RATE", "2%")
    raw_markets = os.getenv("INDEX_MARKETS", "")
    raw_poll = os.getenv("INDEX_POLL_SEC", None)

    interval_min = _parse_interval_env(raw_interval, default_min=5)
    drop_pct = _parse_percent_env(raw_drop, D("-3.0"))
    restart_pct = _parse_percent_env(raw_restart, D("2.0"))
    if restart_pct <= D("0"):
        print(f"[INDEX][WARN] AUTORESTART_RATE {restart_pct}% <= 0 → 2.0%로 보정")
        restart_pct = D("2.0")

    env_markets = _parse_markets_env(raw_markets)

    # poll_sec 파싱
    poll_sec = INDEX_CFG["defaults"].get("poll_sec", 10.0)
    if raw_poll:
        try:
            poll_candidate = float(raw_poll)
            # 안전 범위 1~300초
            poll_sec = max(1.0, min(300.0, poll_candidate))
        except Exception:
            print(f"[INDEX][WARN] 잘못된 INDEX_POLL_SEC='{raw_poll}' → 기본 {poll_sec}s 유지")

    INDEX_CFG["defaults"]["interval_min"] = interval_min
    INDEX_CFG["defaults"]["drop_rate_pct"] = drop_pct
    INDEX_CFG["defaults"]["auto_restart_rate_pct"] = restart_pct
    INDEX_CFG["defaults"]["poll_sec"] = poll_sec
    INDEX_CFG["markets"] = env_markets  # DB 로딩 시 대체/확장 가능

    print(f"[INDEX][ENV] defaults interval={interval_min}m drop={drop_pct}% autorestart={restart_pct}% poll_sec={poll_sec}s markets={env_markets}")

async def load_index_from_db():
    """
    DB의 indexCoinlist에서 per-market 설정 로딩
    SELECT icMarket, icMonrate, icMoninterval FROM indexCoinlist WHERE attrib NOT LIKE '%XXX%'
    """
    global INDEX_CFG
    try:
        rows = await get_indexcoins()
    except Exception as e:
        print(f"[INDEX][DB][ERR] get_indexcoins 실패: {e}")
        rows = None

    by_market = {}
    db_markets: List[str] = []

    if rows:
        for r in rows:
            try:
                icMarket, icMonrate, icMoninterval = r[0], r[1], r[2]
            except Exception:
                continue
            m = str(icMarket).strip().upper() if icMarket else ""
            if not m or "-" not in m:
                continue
            base, cur = m.split("-", 1)
            if base != "KRW" or not cur:
                continue

            # 퍼센트/인터벌 파싱
            try:
                drop_pct = _parse_percent_env(str(icMonrate), INDEX_CFG["defaults"]["drop_rate_pct"])
            except Exception:
                drop_pct = INDEX_CFG["defaults"]["drop_rate_pct"]

            try:
                interval_min = int(str(icMoninterval).strip()) if str(icMoninterval).strip() else INDEX_CFG["defaults"]["interval_min"]
                if interval_min <= 0:
                    interval_min = INDEX_CFG["defaults"]["interval_min"]
            except Exception:
                interval_min = INDEX_CFG["defaults"]["interval_min"]

            by_market[m] = {"interval_min": interval_min, "drop_rate_pct": drop_pct}
            db_markets.append(m)

    if db_markets:
        INDEX_CFG["markets"] = db_markets
    elif not INDEX_CFG["markets"]:
        INDEX_CFG["markets"] = ["KRW-BTC"]  # 안전 기본값

    INDEX_CFG["by_market"] = by_market
    print(f"[INDEX][DB] markets={INDEX_CFG['markets']} by_market={len(by_market)}개 로드 완료")

def get_index_config() -> Dict[str, Any]:
    defaults = INDEX_CFG.get("defaults", {})
    by_market = INDEX_CFG.get("by_market", {})
    markets = INDEX_CFG.get("markets", [])
    return {
        "defaults": {
            "interval_min": int(defaults.get("interval_min", 5)),
            "drop_rate_pct": D(str(defaults.get("drop_rate_pct", "-3.0"))),
            "auto_restart_rate_pct": D(str(defaults.get("auto_restart_rate_pct", "2.0"))),
            "poll_sec": float(defaults.get("poll_sec", 10.0))
        },
        "by_market": {
            m: {
                "interval_min": int(v.get("interval_min", defaults.get("interval_min", 5))),
                "drop_rate_pct": D(str(v.get("drop_rate_pct", defaults.get("drop_rate_pct", "-3.0"))))
            } for m, v in by_market.items()
        },
        "markets": list(markets)
    }

# ------------------------------------------------------------
# 계산/모니터링
# ------------------------------------------------------------
def _calc_return_pct_from_candles(candles: List[Dict[str, Any]]) -> Optional[D]:
    """
    가장 오래된 캔들의 opening_price 대비, 최신 캔들의 trade_price 변동률(%)
    """
    if not candles or len(candles) < 2:
        return None
    cs = sorted(candles, key=lambda x: x.get("timestamp", 0))
    try:
        open_price = D(str(cs[0]["opening_price"]))
        last_price = D(str(cs[-1]["trade_price"]))
        if open_price <= 0:
            return None
        pct = (last_price - open_price) / open_price * D("100")
        return pct
    except Exception:
        return None

def _calc_bar_returns(candles: List[Dict[str, Any]]) -> Optional[List[D]]:
    """
    분봉별 산술 수익률 r[i] = (close - open) / open
    입력 candles는 timestamp 오름차순이어야 하며, 최소 10개를 권장.
    """
    if not candles or len(candles) < 3:
        return None
    cs = sorted(candles, key=lambda x: x.get("timestamp", 0))
    out: List[D] = []
    try:
        for c in cs:
            o = D(str(c["opening_price"]))
            c_ = D(str(c["trade_price"]))
            if o <= 0:
                return None
            out.append((c_ - o) / o)
        return out
    except Exception:
        return None

def _fmt4(x) -> str:
    """
    수치(Decimal/float/str/None)를 소숫점 4자리까지 문자열로 포맷.
    퍼센트 단위 값(예: -1.234567)을 '-1.2346' 형태로 반환.
    """
    if x is None:
        return "NA"
    try:
        from decimal import Decimal, ROUND_HALF_UP
        d = Decimal(str(x))
        return str(d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
    except Exception:
        try:
            return f"{float(x):.4f}"
        except Exception:
            return "NA"

def _d(x) -> D:
    try:
        return D(str(x))
    except Exception:
        return D("0")

# ------------------------------------------------------------
# 폭락 감지 로직
# ------------------------------------------------------------
def _calc_crash_signal_from_candles(
    candles: List[Dict[str, Any]],
    drop_threshold_pct: D = D("-1.5"),
    epsilon_pct: D | float | str = D("0.05")
) -> Dict[str, Any]:
    """
    - 최근 10개의 5분봉 중 마지막 1개 또는 2개의 하락이
      '나머지 평균 변화율' 대비 100% 이상 크고,
      마지막 하락봉(들)의 합 하락률이 drop_threshold_pct(기본 -1.5%) 이하일 때 발동.
    """
    res = {
        "trigger": False, "mode": None,
        "last1": None, "last2_sum": None,
        "base_avg_1": None, "base_avg_2": None,
        "g1": None, "g2": None,
        "threshold_pct": drop_threshold_pct
    }
    if not candles or len(candles) < 10:
        return res

    r = _calc_bar_returns(candles)
    if r is None or len(r) < 10:
        return res

    # 비율 → % 단위
    r_pct = [ri * D("100") for ri in r]

    base_avg_1 = sum(r_pct[:-1]) / D(len(r_pct) - 1)    # r[1..9]
    base_avg_2 = sum(r_pct[:-2]) / D(len(r_pct) - 2)    # r[1..8]
    ε = _d(epsilon_pct)

    g1 = max(abs(base_avg_1), ε)  # %
    g2 = max(abs(base_avg_2), ε)  # %

    last1 = r_pct[-1]             # r[10] %
    last2_sum = r_pct[-2] + r_pct[-1]  # r[9]+r[10] %

    # 조건 설정
    cond_A1 = last1 <= -g1
    cond_B1 = last1 <= drop_threshold_pct

    cond_A2 = last2_sum <= -D("2") * g2
    cond_B2 = last2_sum <= drop_threshold_pct

    trigger1 = cond_A1 and cond_B1
    trigger2 = cond_A2 and cond_B2

    res.update({
        "last1": last1, "last2_sum": last2_sum,
        "base_avg_1": base_avg_1, "base_avg_2": base_avg_2,
        "g1": g1, "g2": g2
    })

    if trigger1:
        res["trigger"] = True
        res["mode"] = "last1"
    elif trigger2:
        res["trigger"] = True
        res["mode"] = "last2"

    return res

# ------------------------------------------------------------
# 계산/모니터링 (동적 실행 간격 + 주기적 재로딩)
# ------------------------------------------------------------
async def monitor_index_markets(on_trigger=None,
                                poll_sec: float = 10.0,
                                count_candles: int = 10,
                                verbose: bool = True,
                                stop_event: Optional[asyncio.Event] = None,
                                reload_sec: float = 600.0):
    # 초기 구성 로드
    cfg = get_index_config()
    defaults = cfg["defaults"]
    markets = cfg["markets"]
    by_market = cfg["by_market"]

    # 실행 간격 동기화(ENV/DB 우선, 없으면 인자 사용)
    def _clamp_poll(v):
        try:
            v = float(v)
        except Exception:
            v = float(poll_sec)
        # 안전 범위: 1s ~ 300s
        return max(1.0, min(300.0, v))

    cur_poll_sec = _clamp_poll(defaults.get("poll_sec", poll_sec))

    if not markets:
        print("[INDEX][MON] 모니터링 대상이 없습니다.")
        return

    # DB 재로딩 타이머
    last_reload_ts = time.time()

    while True:
        if stop_event and stop_event.is_set():
            if verbose:
                print("[INDEX][MON] stop_event → 종료")
            break

        # 선행: 주기적 DB 설정 재로딩
        now_ts = time.time()
        if reload_sec and (now_ts - last_reload_ts) >= reload_sec:
            try:
                await load_index_from_db()
                cfg = get_index_config()
                defaults = cfg["defaults"]
                markets = cfg["markets"]
                by_market = cfg["by_market"]
                prev = cur_poll_sec
                cur_poll_sec = _clamp_poll(defaults.get("poll_sec", cur_poll_sec))
                last_reload_ts = now_ts
                if verbose:
                    print(f"[INDEX][MON] 설정 재로딩 완료: markets={markets}, poll_sec={cur_poll_sec:.2f}s (was {prev:.2f}s), next reload in {int(reload_sec)}s")
            except Exception as e:
                if verbose:
                    print(f"[INDEX][MON][WARN] 설정 재로딩 실패: {e}")

        hits = []
        started = time.time()

        for m in markets:
            mc = by_market.get(m, {})
            # Upbit 허용 단위로 보정
            interval_min_raw = int(mc.get("interval_min", defaults["interval_min"]))
            interval_min = _coerce_unit(interval_min_raw)

            # DB/ENV의 drop_rate_pct를 폭락 임계치로 사용
            drop_threshold = D(str(mc.get("drop_rate_pct", defaults["drop_rate_pct"])))

            try:
                candles = await fetch_candles(m, unit_min=interval_min, count=count_candles)
            except Exception as e:
                if verbose:
                    print(f"[INDEX][MON][{m}] 캔들 조회 실패: {e}")
                continue

            pct = _calc_return_pct_from_candles(candles)

            crash = _calc_crash_signal_from_candles(
                candles,
                drop_threshold_pct=drop_threshold,
                epsilon_pct=D("0.05")  # 0.05% 가드. 필요시 ENV/DB로 노출 가능
            )

            if verbose:
                if pct is not None:
                    print(
                        f"[INDEX][MON][{m}] {interval_min}m x{count_candles} Δ={_fmt4(pct)}% thr={_fmt4(drop_threshold)}%"
                        f" | Crash last1={_fmt4(crash.get('last1'))}% last2Sum={_fmt4(crash.get('last2_sum'))}%"
                        f" base1={_fmt4(crash.get('base_avg_1'))}% base2={_fmt4(crash.get('base_avg_2'))}%"
                        f" g1={_fmt4(crash.get('g1'))}% g2={_fmt4(crash.get('g2'))}% mode={crash.get('mode')}"
                    )
                else:
                    print(
                        f"[INDEX][MON][{m}] Δ 계산 불가 thr={_fmt4(drop_threshold)}%"
                        f" | Crash last1={_fmt4(crash.get('last1'))}% last2Sum={_fmt4(crash.get('last2_sum'))}% mode={crash.get('mode')}"
                    )

            if crash.get("trigger"):
                selected_pct = crash.get("last1") if crash.get("mode") == "last1" else crash.get("last2_sum")
                hits.append({
                    "market": m,
                    "pct": _fmt4(selected_pct),            # 문자열(4자리)로 통일
                    "threshold": _fmt4(drop_threshold),    # 문자열(4자리)
                    "interval_min": interval_min,
                    "mode": crash.get("mode"),
                    "details": {
                        "last1_pct": _fmt4(crash.get("last1")),
                        "last2_sum_pct": _fmt4(crash.get("last2_sum")),
                        "base_avg_1_pct": _fmt4(crash.get("base_avg_1")),
                        "base_avg_2_pct": _fmt4(crash.get("base_avg_2")),
                        "g1_pct": _fmt4(crash.get("g1")),
                        "g2_pct": _fmt4(crash.get("g2"))
                    }
                })

        if hits and on_trigger:
            try:
                await on_trigger(hits)
            except Exception as e:
                print(f"[INDEX][MON] on_trigger 에러: {e}")

        # 간단 슬립(설정 변경은 다음 루프에서 즉시 반영)
        sleep_for = cur_poll_sec
        try:
            await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            if verbose:
                print("[INDEX][MON] Cancelled → 종료")
            break

# ------------------------------------------------------------
# 신호 전달 콜백 예시
# ------------------------------------------------------------

async def on_index_drop_trigger(
    hits: List[Dict[str, Any]],
    threshold_ratio: float = 0.7,
):

    if threshold_ratio <= 0 or threshold_ratio > 1:
        raise ValueError("threshold_ratio must be in (0, 1].")

    # 1) 전체 대상 수 조회
    try:
        rows = await get_indexcoins()
    except Exception as e:
        print(f"[INDEX][ERROR] Failed to load total list: {e}")
        # 조회 실패 시, 기존 동작처럼 로그만 남기고 종료
        markets = [h.get("market") for h in hits]
        print(f"[INDEX][SIGNAL] Drop hits={len(hits)} {markets} (threshold check skipped)")
        return

    if not rows:
        print("[INDEX][WARN] No rows from indexCoinlist. Skipping threshold check.")
        markets = [h.get("market") for h in hits]
        print(f"[INDEX][SIGNAL] Drop hits={len(hits)} {markets}")
        return

    total_count = len(rows)

    # 2) 비율 계산
    markets = [h.get("market") for h in hits]
    drop_count = len(hits)
    ratio = drop_count / total_count if total_count > 0 else 0.0

    print(
        f"[INDEX][SIGNAL] Drop hits={drop_count}/{total_count} "
        f"({ratio:.1%}) {markets} | threshold={threshold_ratio:.0%}"
    )

    # 3) 임계 충족 시 TODO 실행
    if ratio >= threshold_ratio:
        # TODO: 웹훅/메시지큐/DB 업데이트
        # await send_webhook({...})
        # await redis.publish("index_drop", json.dumps(hits))
        # await set_autostop(True)
        print("[INDEX][ACTION] Threshold met. Executing TODO actions...")
    else:
        print("[INDEX][ACTION] Threshold not met. Skipping TODO actions.")

# ------------------------------------------------------------
# 엔트리포인트
# ------------------------------------------------------------
async def run_index_monitor():
    """
    단일 프로세스 엔트리: ENV → DB → 모니터 시작
    """
    parse_index_env_defaults()
    await load_index_from_db()

    stop_event = asyncio.Event()
    task = asyncio.create_task(
        monitor_index_markets(
            on_trigger=on_index_drop_trigger,
            poll_sec=10.0,
            count_candles=10,
            verbose=True,
            stop_event=stop_event,
            reload_sec=600.0  # 10분마다 재로딩
        )
    )

    try:
        await task
    except asyncio.CancelledError:
        stop_event.set()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(run_index_monitor())
    except KeyboardInterrupt:
        print("\n[INDEX] Interrupted by user")
