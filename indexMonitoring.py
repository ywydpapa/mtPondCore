# ============================================================
# index_monitor_service.py
# 인덱스(폭락/상승) 멀티 타임프레임(5m/15m) 모니터링
#  - 5m 30개 로드 → 5m 10개(50분) 시그널
#  - 5m → 15m roll-up 10개(150분) 시그널
#  - 15m 임계치는 5m 임계치의 2배
#  - 콜백 비율 계산 시 같은 코인은 1회만 카운트
# ============================================================

import os
import asyncio
import time
from decimal import Decimal as D
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import aiohttp
from datetime import datetime, timezone, timedelta
import dotenv
from typing import List, Dict, Any, Optional, Tuple
dotenv.load_dotenv()

db_url = os.getenv("dburl")
if not db_url:
    raise RuntimeError("Environment variable 'dburl' is required")
engine = create_async_engine(db_url, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

_UPBIT_MIN_UNITS = {1, 3, 5, 10, 15, 30, 60, 240}
KST = timezone(timedelta(hours=9))

async def get_indexcoins() -> list[tuple] | None:
    async with SessionLocal() as session:
        sql = text("""SELECT icMarket, icMonrate, icMoninterval, icRestartRate FROM indexCoinlist WHERE attrib NOT LIKE :attr""")
        result = await session.execute(sql, {"attr": "%XXX%"})
        return result.fetchall()

async def update_index_signal(signal:str):
    async with SessionLocal() as session:
        sql = text("""UPDATE indexSignal set attrib = :attx""")
        await session.execute(sql, {"attx": "XXXUPXXXUP"})
        sql = text("""INSERT INTO indexSignal (idxSignal) values (:signal)""")
        await session.execute(sql, {"signal": signal})
        await session.commit()

def _coerce_unit(unit_min: int) -> int:
    allowed = sorted(_UPBIT_MIN_UNITS)
    return min(allowed, key=lambda x: abs(x - unit_min))

async def fetch_candles(
    market: str,
    unit_min: int,
    count: int = 30,
    *,
    session: aiohttp.ClientSession | None = None,
    retry: int = 2,
    cooldown_sec: float = 0.15
) -> list[dict]:
    if unit_min not in _UPBIT_MIN_UNITS:
        unit_min = _coerce_unit(unit_min)

    url = f"https://api.upbit.com/v1/candles/minutes/{unit_min}"
    params = {"market": market, "count": min(max(int(count), 1), 200)}
    headers = {"Accept": "application/json"}

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
                        retry_after = float(resp.headers.get("Retry-After", "0.5"))
                        await asyncio.sleep(max(cooldown_sec, retry_after))
                        if attempt <= retry:
                            continue
                        raise RuntimeError(f"Upbit 429 rate limited after {attempt} attempts")
                    if resp.status >= 500:
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
                        if ts is None:
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
# INDEX 설정
# ------------------------------------------------------------
INDEX_CFG: Dict[str, Any] = {
    "defaults": {
        "interval_min": 5,                 # 주 표시 인터벌(5m)
        "drop_rate_pct": D("-3.0"),        # 5m 임계 하락률(%)
        "auto_restart_rate_pct": D("2.0"), # 5m 임계 상승률(%)
        "poll_sec": 10.0
    },
    "by_market": {},
    "markets": []
}

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

    poll_sec = INDEX_CFG["defaults"].get("poll_sec", 10.0)
    if raw_poll:
        try:
            poll_candidate = float(raw_poll)
            poll_sec = max(1.0, min(300.0, poll_candidate))
        except Exception:
            print(f"[INDEX][WARN] 잘못된 INDEX_POLL_SEC='{raw_poll}' → 기본 {poll_sec}s 유지")

    INDEX_CFG["defaults"]["interval_min"] = interval_min
    INDEX_CFG["defaults"]["drop_rate_pct"] = drop_pct
    INDEX_CFG["defaults"]["auto_restart_rate_pct"] = restart_pct
    INDEX_CFG["defaults"]["poll_sec"] = poll_sec
    INDEX_CFG["markets"] = env_markets

    print(f"[INDEX][ENV] defaults interval={interval_min}m drop={drop_pct}% autorestart={restart_pct}% poll_sec={poll_sec}s markets={env_markets}")

async def load_index_from_db():
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
                icMarket, icMonrate, icMoninterval, icRestartRate = r[0], r[1], r[2], r[3]
            except Exception:
                continue
            m = str(icMarket).strip().upper() if icMarket else ""
            if not m or "-" not in m:
                continue
            base, cur = m.split("-", 1)
            if base != "KRW" or not cur:
                continue

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

            try:
                restart_pct = _parse_percent_env(str(icRestartRate), INDEX_CFG["defaults"]["auto_restart_rate_pct"])
                if restart_pct <= D("0"):
                    restart_pct = INDEX_CFG["defaults"]["auto_restart_rate_pct"]
            except Exception:
                restart_pct = INDEX_CFG["defaults"]["auto_restart_rate_pct"]

            by_market[m] = {
                "interval_min": interval_min,
                "drop_rate_pct": drop_pct,
                "auto_restart_rate_pct": restart_pct,
            }
            db_markets.append(m)

    if db_markets:
        INDEX_CFG["markets"] = db_markets
    elif not INDEX_CFG["markets"]:
        INDEX_CFG["markets"] = ["KRW-BTC"]

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
            "poll_sec": float(defaults.get("poll_sec", 60.0))
        },
        "by_market": {
            m: {
                "interval_min": int(v.get("interval_min", defaults.get("interval_min", 5))),
                "drop_rate_pct": D(str(v.get("drop_rate_pct", defaults.get("drop_rate_pct", "-3.0")))),
                "auto_restart_rate_pct": D(str(v.get("auto_restart_rate_pct", defaults.get("auto_restart_rate_pct", "2.0")))),
            } for m, v in by_market.items()
        },
        "markets": list(markets)
    }

# ------------------------------------------------------------
# 공통 계산
# ------------------------------------------------------------
from typing import Optional

def _calc_return_pct_from_candles(candles: List[Dict[str, Any]]) -> Optional[D]:
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
# Roll-up: 5m → 15m
# ------------------------------------------------------------
def _rollup_to_15m_from_5m(candles_5m: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cs = sorted(candles_5m, key=lambda x: x.get("timestamp", 0))
    out: List[Dict[str, Any]] = []
    for i in range(0, len(cs) // 3):
        group = cs[i*3:(i+1)*3]
        if len(group) < 3:
            break
        o = group[0]["opening_price"]
        c = group[-1]["trade_price"]
        ts = group[-1]["timestamp"]
        out.append({
            "opening_price": str(o),
            "trade_price": str(c),
            "timestamp": int(ts)
        })
    return out

# ------------------------------------------------------------
# 시그널
# ------------------------------------------------------------
def _calc_crash_signal_from_candles(
    candles: List[Dict[str, Any]],
    drop_threshold_pct: D = D("-1.5"),
    epsilon_pct: D | float | str = D("0.05")
) -> Dict[str, Any]:
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

    r_pct = [ri * D("100") for ri in r]
    base_avg_1 = sum(r_pct[:-1]) / D(len(r_pct) - 1)
    base_avg_2 = sum(r_pct[:-2]) / D(len(r_pct) - 2)
    ε = _d(epsilon_pct)
    g1 = max(abs(base_avg_1), ε)
    g2 = max(abs(base_avg_2), ε)

    last1 = r_pct[-1]
    last2_sum = r_pct[-2] + r_pct[-1]

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

def _calc_upside_signal_from_candles(
    candles: List[Dict[str, Any]],
    restart_threshold_pct: D = D("2.0"),
    epsilon_pct: D | float | str = D("0.05"),
    enhanced: bool = False,
) -> Dict[str, Any]:
    res = {
        "trigger": False, "mode": None,
        "delta_total_pct": None,
        "last1": None, "last2_sum": None,
        "base_avg_1": None, "base_avg_2": None,
        "g1": None, "g2": None,
        "threshold_pct": restart_threshold_pct
    }
    if not candles or len(candles) < 3:
        return res

    delta_total_pct = _calc_return_pct_from_candles(candles)
    if delta_total_pct is None:
        return res

    res["delta_total_pct"] = delta_total_pct
    base_hit = delta_total_pct >= restart_threshold_pct

    if not enhanced:
        if base_hit:
            res["trigger"] = True
            res["mode"] = "total"
        return res

    r = _calc_bar_returns(candles)
    if r is None or len(r) < 10:
        if base_hit:
            res["trigger"] = True
            res["mode"] = "total"
        return res

    r_pct = [ri * D("100") for ri in r]
    base_avg_1 = sum(r_pct[:-1]) / D(len(r_pct) - 1)
    base_avg_2 = sum(r_pct[:-2]) / D(len(r_pct) - 2)
    ε = _d(epsilon_pct)
    g1 = max(abs(base_avg_1), ε)
    g2 = max(abs(base_avg_2), ε)

    last1 = r_pct[-1]
    last2_sum = r_pct[-2] + r_pct[-1]

    res.update({
        "last1": last1, "last2_sum": last2_sum,
        "base_avg_1": base_avg_1, "base_avg_2": base_avg_2,
        "g1": g1, "g2": g2
    })

    cond_A1 = last1 >= g1
    cond_B1 = last1 >= restart_threshold_pct

    cond_A2 = last2_sum >= D("2") * g2
    cond_B2 = last2_sum >= restart_threshold_pct

    trigger1 = cond_A1 and cond_B1
    trigger2 = cond_A2 and cond_B2

    if base_hit:
        res["trigger"] = True
        res["mode"] = "total"
    elif trigger1:
        res["trigger"] = True
        res["mode"] = "last1"
    elif trigger2:
        res["trigger"] = True
        res["mode"] = "last2"

    return res

# ------------------------------------------------------------
# 모니터 루프 (5m + 15m)
# ------------------------------------------------------------
async def monitor_index_markets(on_trigger=None,
                                poll_sec: float = 30.0,
                                count_candles_5m_total: int = 30,
                                verbose: bool = True,
                                stop_event: Optional[asyncio.Event] = None,
                                reload_sec: float = 600.0,
                                on_trigger_up=None,
                                enhance_upside: bool = True):
    cfg = get_index_config()
    defaults = cfg["defaults"]
    markets = cfg["markets"]
    by_market = cfg["by_market"]

    def _clamp_poll(v):
        try:
            v = float(v)
        except Exception:
            v = float(poll_sec)
        return max(1.0, min(300.0, v))

    cur_poll_sec = _clamp_poll(defaults.get("poll_sec", poll_sec))

    if not markets:
        print("[INDEX][MON] 모니터링 대상이 없습니다.")
        return

    last_reload_ts = time.time()

    while True:
        if stop_event and stop_event.is_set():
            if verbose:
                print("[INDEX][MON] stop_event → 종료")
            break

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
                    print(f"[INDEX][MON] 설정 재로딩 완료: markets={markets}, poll_sec={cur_poll_sec:.2f}s (was {prev:.2f}s)")
            except Exception as e:
                if verbose:
                    print(f"[INDEX][MON][WARN] 설정 재로딩 실패: {e}")

        hits_down: List[Dict[str, Any]] = []
        hits_up: List[Dict[str, Any]] = []

        for m in markets:
            # per-market 임계치(5m 기준)
            mc = by_market.get(m, {})
            drop_threshold_5m = D(str(mc.get("drop_rate_pct", defaults["drop_rate_pct"])))
            restart_threshold_5m = D(str(mc.get("auto_restart_rate_pct", defaults["auto_restart_rate_pct"])))
            # 15m 임계치 = 5m의 2배
            drop_threshold_15m = drop_threshold_5m * D("2")
            restart_threshold_15m = restart_threshold_5m * D("2")

            try:
                candles_5m_all = await fetch_candles(m, unit_min=5, count=count_candles_5m_total)
            except Exception as e:
                if verbose:
                    print(f"[INDEX][MON][{m}] 5m 캔들 조회 실패: {e}")
                continue

            if len(candles_5m_all) < 10:
                if verbose:
                    print(f"[INDEX][MON][{m}] 5m 캔들 부족: got {len(candles_5m_all)} < 10")
                continue

            candles_5m_10 = candles_5m_all[-10:]

            pct_5m_total = _calc_return_pct_from_candles(candles_5m_10)
            crash_5m = _calc_crash_signal_from_candles(candles_5m_10, drop_threshold_pct=drop_threshold_5m, epsilon_pct=D("0.05"))
            upside_5m = _calc_upside_signal_from_candles(candles_5m_10, restart_threshold_pct=restart_threshold_5m, epsilon_pct=D("0.05"), enhanced=enhance_upside)

            candles_15m_10 = _rollup_to_15m_from_5m(candles_5m_all)
            if len(candles_15m_10) >= 10:
                candles_15m_10 = candles_15m_10[-10:]
                pct_15m_total = _calc_return_pct_from_candles(candles_15m_10)
                crash_15m = _calc_crash_signal_from_candles(candles_15m_10, drop_threshold_pct=drop_threshold_15m, epsilon_pct=D("0.05"))
                upside_15m = _calc_upside_signal_from_candles(candles_15m_10, restart_threshold_pct=restart_threshold_15m, epsilon_pct=D("0.05"), enhanced=enhance_upside)
            else:
                pct_15m_total, crash_15m, upside_15m = None, {"trigger": False}, {"trigger": False}

            if verbose:
                print(
                    f"[INDEX][MON][{m}] 5m x10 Δ={_fmt4(pct_5m_total)}% "
                    f"thrDown5={_fmt4(drop_threshold_5m)}% thrUp5={_fmt4(restart_threshold_5m)}% "
                    f"| Crash5 last1={_fmt4(crash_5m.get('last1'))}% last2Sum={_fmt4(crash_5m.get('last2_sum'))}% mode={crash_5m.get('mode')} "
                    f"| Ups5 Δ={_fmt4(upside_5m.get('delta_total_pct'))}% mode={upside_5m.get('mode')}"
                )
                print(
                    f"[INDEX][MON][{m}] 15m x10 Δ={_fmt4(pct_15m_total)}% "
                    f"thrDown15={_fmt4(drop_threshold_15m)}% thrUp15={_fmt4(restart_threshold_15m)}% "
                    f"| Crash15 last1={_fmt4(crash_15m.get('last1'))}% last2Sum={_fmt4(crash_15m.get('last2_sum'))}% mode={crash_15m.get('mode')} "
                    f"| Ups15 Δ={_fmt4(upside_15m.get('delta_total_pct'))}% mode={upside_15m.get('mode')}"
                )

            # hits(5m)
            if crash_5m.get("trigger"):
                sel = crash_5m.get("last1") if crash_5m.get("mode") == "last1" else crash_5m.get("last2_sum")
                hits_down.append({
                    "market": m,
                    "timeframe": "5m",
                    "pct": _fmt4(sel),
                    "threshold": _fmt4(drop_threshold_5m),
                    "interval_min": 5,
                    "mode": crash_5m.get("mode"),
                    "direction": "down",
                })
            if upside_5m.get("trigger"):
                sel = upside_5m.get("last1") if upside_5m.get("mode") == "last1" else (upside_5m.get("last2_sum") if upside_5m.get("mode") == "last2" else upside_5m.get("delta_total_pct"))
                hits_up.append({
                    "market": m,
                    "timeframe": "5m",
                    "pct": _fmt4(sel),
                    "threshold": _fmt4(restart_threshold_5m),
                    "interval_min": 5,
                    "mode": upside_5m.get("mode"),
                    "direction": "up",
                })

            # hits(15m)
            if crash_15m.get("trigger"):
                sel = crash_15m.get("last1") if crash_15m.get("mode") == "last1" else crash_15m.get("last2_sum")
                hits_down.append({
                    "market": m,
                    "timeframe": "15m",
                    "pct": _fmt4(sel),
                    "threshold": _fmt4(drop_threshold_15m),
                    "interval_min": 15,
                    "mode": crash_15m.get("mode"),
                    "direction": "down",
                })
            if upside_15m.get("trigger"):
                sel = upside_15m.get("last1") if upside_15m.get("mode") == "last1" else (upside_15m.get("last2_sum") if upside_15m.get("mode") == "last2" else upside_15m.get("delta_total_pct"))
                hits_up.append({
                    "market": m,
                    "timeframe": "15m",
                    "pct": _fmt4(sel),
                    "threshold": _fmt4(restart_threshold_15m),
                    "interval_min": 15,
                    "mode": upside_15m.get("mode"),
                    "direction": "up",
                })

        # 콜백
        if hits_down and on_trigger:
            try:
                await on_trigger(hits_down)
            except Exception as e:
                print(f"[INDEX][MON] on_trigger(Down) 에러: {e}")

        if hits_up and on_trigger_up:
            try:
                await on_trigger_up(hits_up)
            except Exception as e:
                print(f"[INDEX][MON] on_trigger_up 에러: {e}")

        try:
            await asyncio.sleep(cur_poll_sec)
        except asyncio.CancelledError:
            if verbose:
                print("[INDEX][MON] Cancelled → 종료")
            break

# ------------------------------------------------------------
# 콜백: 코인 중복 제거하여 비율 계산
# ------------------------------------------------------------
def _aggregate_timeframes(hits: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    market -> [timeframe 리스트] 로 변환
    """
    agg: Dict[str, List[str]] = {}
    for h in hits:
        m = h.get("market")
        tf = h.get("timeframe")
        if not m:
            continue
        agg.setdefault(m, [])
        if tf and tf not in agg[m]:
            agg[m].append(tf)
    # 표시를 위해 타임프레임 정렬
    for k in agg:
        agg[k].sort(key=lambda x: (x != "5m", x))  # 5m 우선
    return agg

async def on_index_drop_trigger(
    hits: List[Dict[str, Any]],
    threshold_ratio: float = 0.7,
):
    try:
        rows = await get_indexcoins()
    except Exception as e:
        print(f"[INDEX][ERROR] Failed to load total list: {e}")
        agg = _aggregate_timeframes(hits)
        flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
        print(f"[INDEX][SIGNAL] Drop hits={len(agg)}/{len(agg)} (threshold check skipped) {flat}")
        return
    if not rows:
        print("[INDEX][WARN] No rows from indexCoinlist. Skipping threshold check.")
        agg = _aggregate_timeframes(hits)
        flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
        print(f"[INDEX][SIGNAL] Drop hits={len(agg)} {flat}")
        return

    total_count = len(rows)  # 전체 모니터링 코인 수
    agg = _aggregate_timeframes(hits)
    unique_count = len(agg)  # 코인 단위 유니크 카운트
    ratio = unique_count / total_count if total_count > 0 else 0.0
    flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
    print(f"[INDEX][SIGNAL] Drop hits={unique_count}/{total_count} ({ratio:.1%}) {flat} | threshold={threshold_ratio:.0%}")
    if ratio >= threshold_ratio:
        print("[INDEX][ACTION] Threshold met. Executing TODO actions...")
        await update_index_signal("DOWN")
    else:
        print("[INDEX][ACTION] Threshold not met. Skipping TODO actions.")
        await update_index_signal("down")

async def on_index_up_trigger(
    hits: List[Dict[str, Any]],
    threshold_ratio: float = 0.5,
):
    try:
        rows = await get_indexcoins()
    except Exception as e:
        print(f"[INDEX][ERROR] Failed to load total list (up): {e}")
        agg = _aggregate_timeframes(hits)
        flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
        print(f"[INDEX][SIGNAL][UP] Upside hits={len(agg)}/{len(agg)} (threshold check skipped) {flat}")
        return
    if not rows:
        print("[INDEX][WARN] No rows from indexCoinlist. Skipping threshold check (up).")
        agg = _aggregate_timeframes(hits)
        flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
        print(f"[INDEX][SIGNAL][UP] Upside hits={len(agg)} {flat}")
        return
    total_count = len(rows)
    agg = _aggregate_timeframes(hits)
    unique_count = len(agg)
    ratio = unique_count / total_count if total_count > 0 else 0.0
    flat = [f"{m}[{','.join(tfs)}]" for m, tfs in agg.items()]
    print(f"[INDEX][SIGNAL][UP] Upside hits={unique_count}/{total_count} ({ratio:.1%}) {flat} | threshold={threshold_ratio:.0%}")
    if ratio >= threshold_ratio:
        print("[INDEX][ACTION][UP] Threshold met. Executing TODO actions...")
        await update_index_signal("UP")
    else:
        print("[INDEX][ACTION][UP] Threshold not met. Skipping TODO actions.")
        await update_index_signal("up")

# ------------------------------------------------------------
# 엔트리포인트
# ------------------------------------------------------------
async def run_index_monitor():
    parse_index_env_defaults()
    await load_index_from_db()

    stop_event = asyncio.Event()
    task = asyncio.create_task(
        monitor_index_markets(
            on_trigger=on_index_drop_trigger,
            on_trigger_up=on_index_up_trigger,
            poll_sec=30.0,
            count_candles_5m_total=30,
            verbose=True,
            stop_event=stop_event,
            reload_sec=600.0,
            enhance_upside=True
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
