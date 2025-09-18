import time
import threading
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Callable, Iterable, Tuple
import requests

# ==============================
# 설정
# ==============================
API_URL = "http://ywydpapa.iptime.org:8000/api/bbtrend30"
REFRESH_INTERVAL_SEC = 30
TIMEOUT_SEC = 8
DEFAULT_TOP_N = 8
TARGET_TIMEFRAMES = ["5m", "15m", "30m"]

# 스코어 / 필터 파라미터
RSI_OVERHEAT = 72
RSI_GOOD_LOW, RSI_GOOD_HIGH = 50, 70

# MACD_Hist 교차 감지 활성화
ENABLE_MACD_HIST_CROSS_DETECTION = True
# 아주 작은 미세한 진동(노이즈) 배제 위한 최소 절대값 (필요 시 조정)
MACD_HIST_MIN_ABS_FOR_EVENT = 0.0001
# 하락 전환도 보고 싶지 않다면 False 로
DETECT_DOWN_CROSS = True
# 수익률 / 틱 필터 설정
TARGET_PROFIT_PERCENT = 0.5   # 0.5% 목표
MAX_TICKS_FOR_TARGET = 10     # 10틱 이내
USE_TICK_FILTER = True        # 필요 시 On/Off
TICK_RULE = "UPBIT_KRW"       # 룰 스위치 (추후 확장 여지)

# ==============================
# 데이터 모델
# ==============================
@dataclass
class CoinIndicator:
    market: str
    timeframe: str
    close: float
    BB_Pos: float
    BandWidth: float
    Range_Pos20: float
    RSI: float
    ATR: float
    MACD: float
    MACD_Signal: float
    MACD_Hist: float
    Trend10: Optional[str]
    time: str

    def basic_momentum_score(self) -> float:
        rsi_score = 0
        if RSI_GOOD_LOW <= self.RSI <= RSI_GOOD_HIGH:
            rsi_score = 8
        elif self.RSI > RSI_OVERHEAT:
            rsi_score = -5
        bw_penalty = 0
        if self.BandWidth > 8:
            bw_penalty = - (self.BandWidth - 8) * 0.5
        return self.BB_Pos * 0.4 + self.MACD_Hist * 25 + rsi_score + bw_penalty

    def trend10_score(self) -> float:
        if not self.Trend10:
            return 0.0
        score_map = {'U': 2, 'u': 1, '-': 0, 'd': -1, 'D': -2}
        total = 0
        length = len(self.Trend10)
        for i, ch in enumerate(self.Trend10):
            w = (i + 1) / length  # 뒤쪽(최근) 가중
            total += w * score_map.get(ch, 0)
        return total

    def combined_score(self) -> float:
        return self.basic_momentum_score() + self.trend10_score() * 3

    def ticks_needed_for_percent(self, percent: float, rule: str = "UPBIT_KRW") -> int:
        import math
        tick_size = calc_tick_size(self.close, rule)
        target_abs = self.close * (percent / 100.0)
        if tick_size <= 0:
            return 10**9
        return max(1, math.ceil(target_abs / tick_size))


def calc_tick_size(price: float, rule: str = "UPBIT_KRW") -> float:
    if rule == "UPBIT_KRW":
        if price >= 2_000_000: return 1000
        if price >= 1_000_000: return 500
        if price >= 500_000: return 100
        if price >= 100_000: return 50
        if price >= 10_000: return 10
        if price >= 1_000: return 5
        if price >= 100: return 1
        if price >= 10: return 0.1
        return 0.01
    else:
        return 0.01

# ==============================
# 타임프레임 저장소 + MACD_Hist 교차 감지
# ==============================
class TimeframeStore:
    def __init__(self, timeframe: str):
        self.timeframe = timeframe
        self._coins: Dict[str, CoinIndicator] = {}
        self._prev_macd_hist: Dict[str, float] = {}
        self.last_updated: Optional[float] = None

    def update(self, coins: List[CoinIndicator]) -> List[dict]:
        """
        코인 리스트 갱신하면서 MACD_Hist 교차 이벤트 탐지.
        반환: 이벤트 리스트
        """
        events = []
        new_map = {}
        for c in coins:
            prev = self._prev_macd_hist.get(c.market)
            curr = c.MACD_Hist
            # 교차 감지 로직
            if ENABLE_MACD_HIST_CROSS_DETECTION and prev is not None:
                # 상승 전환: prev <= 0, curr > 0
                if prev <= 0 and curr > 0 and abs(curr) >= MACD_HIST_MIN_ABS_FOR_EVENT:
                    events.append({
                        "type": "MACD_HIST_CROSS",
                        "direction": "UP",
                        "timeframe": self.timeframe,
                        "market": c.market,
                        "prev": prev,
                        "curr": curr,
                        "time": c.time,
                        "RSI": c.RSI,
                        "BB_Pos": c.BB_Pos,
                        "MACD": c.MACD,
                        "MACD_Signal": c.MACD_Signal
                    })
                # 하락 전환(옵션)
                if DETECT_DOWN_CROSS and prev >= 0 and curr < 0 and abs(curr) >= MACD_HIST_MIN_ABS_FOR_EVENT:
                    events.append({
                        "type": "MACD_HIST_CROSS",
                        "direction": "DOWN",
                        "timeframe": self.timeframe,
                        "market": c.market,
                        "prev": prev,
                        "curr": curr,
                        "time": c.time,
                        "RSI": c.RSI,
                        "BB_Pos": c.BB_Pos,
                        "MACD": c.MACD,
                        "MACD_Signal": c.MACD_Signal
                    })
            # prev 없으면 최초 진입 → 아직 교차 판단 X
            new_map[c.market] = c
            self._prev_macd_hist[c.market] = curr

        self._coins = new_map
        self.last_updated = time.time()
        return events

    def all(self) -> List[CoinIndicator]:
        return list(self._coins.values())

    def get(self, market: str) -> Optional[CoinIndicator]:
        return self._coins.get(market)

    def top_by(self, key_func: Callable[[CoinIndicator], float], n: int) -> List[CoinIndicator]:
        return sorted(self._coins.values(), key=key_func, reverse=True)[:n]

# ==============================
# 멀티 타임프레임 저장소
# ==============================
class MultiTimeframeStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._stores: Dict[str, TimeframeStore] = {}
        self.last_fetch_status: Optional[str] = None
        self.last_fetch_error: Optional[str] = None
        self.last_server_updated: Optional[str] = None
        # 이벤트 중복 방지 (market,timeframe,direction,time) 키
        self._emitted_event_keys = set()

    def update_from_response(self, resp_json: dict) -> List[dict]:
        with self._lock:
            all_events = []
            self.last_fetch_status = resp_json.get("status")
            self.last_server_updated = resp_json.get("updated")
            tfs = resp_json.get("timeframes", {})
            if not isinstance(tfs, dict):
                raise ValueError("timeframes 필드가 dict 아님")
            for tf_name, arr in tfs.items():
                if TARGET_TIMEFRAMES and tf_name not in TARGET_TIMEFRAMES:
                    continue
                coins: List[CoinIndicator] = []
                for row in arr:
                    try:
                        c = CoinIndicator(
                            market=str(row.get("market","")),
                            timeframe=tf_name,
                            close=float(row.get("close") or 0),
                            BB_Pos=float(row.get("BB_Pos") or 0),
                            BandWidth=float(row.get("BandWidth") or 0),
                            Range_Pos20=float(row.get("Range_Pos20") or 0),
                            RSI=float(row.get("RSI") or 0),
                            ATR=float(row.get("ATR") or 0),
                            MACD=float(row.get("MACD") or 0),
                            MACD_Signal=float(row.get("MACD_Signal") or 0),
                            MACD_Hist=float(row.get("MACD_Hist") or 0),
                            Trend10=row.get("Trend10"),
                            time=str(row.get("time",""))
                        )
                        coins.append(c)
                    except Exception as parse_e:
                        print(f"[WARN] parse 실패 tf={tf_name} err={parse_e} raw={row}")
                if tf_name not in self._stores:
                    self._stores[tf_name] = TimeframeStore(tf_name)
                events = self._stores[tf_name].update(coins)
                # 중복 제거
                for ev in events:
                    key = (ev["market"], ev["timeframe"], ev["direction"], ev["time"])
                    if key in self._emitted_event_keys:
                        continue
                    self._emitted_event_keys.add(key)
                    all_events.append(ev)
            self.last_fetch_error = None
            return all_events

    def set_error(self, msg: str):
        with self._lock:
            self.last_fetch_error = msg

    def timeframes(self) -> List[str]:
        with self._lock:
            return list(self._stores.keys())

    def all_in_timeframe(self, timeframe: str) -> List[CoinIndicator]:
        with self._lock:
            store = self._stores.get(timeframe)
            return store.all() if store else []

    def top_by(self, timeframe: str, key_func: Callable[[CoinIndicator], float], n: int) -> List[CoinIndicator]:
        with self._lock:
            store = self._stores.get(timeframe)
            if not store:
                return []
            return store.top_by(key_func, n)

    def multi_tf_intersection(
            self,
            timeframes: Iterable[str],
            key_func: Callable[[CoinIndicator], float],
            per_tf_top: int = 12,
            final_limit: int = 10,
            min_tf_presence: Optional[int] = None,
            mandatory_tf: Optional[str] = None,
            score_mode: str = "avg",
            # --- 새 파라미터들 ---
            apply_tick_filter: bool = True,
            target_profit_percent: float = 0.5,
            max_ticks_for_target: int = 10,
            tick_inclusive: bool = True,  # True: <= max, False: < max
            tick_rule: str = "UPBIT_KRW",
            tick_filter_mode: str = "mandatory",  # "mandatory" | "all" | "any"
            # mandatory: mandatory_tf 인디케이터로만 판정
            # all: 모든 포함된 TF 의 인디케이터가 조건 충족해야
            # any: 하나라도 조건 충족하면 OK
    ) -> List[Tuple[str, Dict[str, CoinIndicator]]]:
        """
        여러 타임프레임 상위 per_tf_top 코인을 모아
        - 등장 타임프레임 수 >= min_tf_presence
        - mandatory_tf 포함(옵션)
        - (옵션) 틱 필터: target_profit_percent 달성에 필요한 틱 수 조건
        를 만족하는 코인 반환.

        반환: [(market, {tf: CoinIndicator, ...}), ...]
        """
        with self._lock:
            tf_list = list(timeframes)
            if not tf_list:
                return []

            if min_tf_presence is None:
                min_tf_presence = len(tf_list)

            # 각 TF 상위 수집
            rank_sets: Dict[str, Dict[str, CoinIndicator]] = {}
            for tf in tf_list:
                store = self._stores.get(tf)
                if not store:
                    return []
                top_coins = store.top_by(key_func, per_tf_top)
                rank_sets[tf] = {c.market: c for c in top_coins}

            # 마켓별 TF 매핑
            market_map: Dict[str, Dict[str, CoinIndicator]] = {}
            for tf, mdict in rank_sets.items():
                for m, coin in mdict.items():
                    market_map.setdefault(m, {})[tf] = coin

            candidates = []
            for market, tf_coins in market_map.items():
                presence = len(tf_coins)
                if presence < min_tf_presence:
                    continue
                if mandatory_tf and mandatory_tf not in tf_coins:
                    continue

                # ---- 틱 필터 판정 ----
                if apply_tick_filter:
                    # ticks_needed 계산 함수
                    def _ticks(c: CoinIndicator) -> int:
                        return c.ticks_needed_for_percent(target_profit_percent, tick_rule)

                    # 허용 조건 함수
                    def _ok(tn: int) -> bool:
                        return tn <= max_ticks_for_target if tick_inclusive else tn < max_ticks_for_target

                    tick_pass = True
                    if tick_filter_mode == "mandatory":
                        # mandatory_tf 지정이 반드시 존재한다고 가정
                        base_tf = mandatory_tf if mandatory_tf else next(iter(tf_coins.keys()))
                        tn = _ticks(tf_coins[base_tf])
                        tick_pass = _ok(tn)
                    elif tick_filter_mode == "all":
                        tick_pass = all(_ok(_ticks(ci)) for ci in tf_coins.values())
                    elif tick_filter_mode == "any":
                        tick_pass = any(_ok(_ticks(ci)) for ci in tf_coins.values())
                    else:
                        # 잘못된 모드 → 안전하게 실패 처리 또는 기본 any
                        tick_pass = any(_ok(_ticks(ci)) for ci in tf_coins.values())

                    if not tick_pass:
                        continue

                # 점수 계산 (평균)
                scores = [key_func(c) for c in tf_coins.values()]
                if score_mode == "avg":
                    combined_score = sum(scores) / len(scores)
                else:
                    combined_score = sum(scores) / len(scores)

                candidates.append((market, presence, combined_score, tf_coins))

            # 정렬: (등장 TF 수, 평균 점수)
            candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)

            out = []
            for market, _presence, _sc, tf_coins in candidates[:final_limit]:
                out.append((market, tf_coins))
            return out

    def snapshot_summary(self) -> dict:
        with self._lock:
            return {
                "timeframes": {
                    tf: len(store.all()) for tf, store in self._stores.items()
                },
                "last_fetch_error": self.last_fetch_error,
                "server_updated": self.last_server_updated
            }

store = MultiTimeframeStore()

# ==============================
# Fetch 로직
# ==============================
def fetch_once(verbose: bool = True) -> List[dict]:
    try:
        r = requests.get(API_URL, timeout=TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            raise ValueError(f"status != ok : {data.get('status')}")
        events = store.update_from_response(data)
        if verbose:
            print(f"[INFO] fetch 성공 updated={data.get('updated')} tfs={store.timeframes()} events={len(events)}")
        return events
    except Exception as e:
        store.set_error(str(e))
        if verbose:
            print(f"[ERROR] fetch 실패: {e}")
        return []

# ==============================
# 추천/필터/출력 유틸
# ==============================
def filter_candidates(coins: List[CoinIndicator]) -> List[CoinIndicator]:
    out = []
    for c in coins:
        if not (c.BB_Pos >= 60 and c.MACD_Hist > 0 and 45 <= c.RSI <= 75):
            continue
        if USE_TICK_FILTER:
            ticks_needed = c.ticks_needed_for_percent(TARGET_PROFIT_PERCENT, TICK_RULE)
            if ticks_needed > MAX_TICKS_FOR_TARGET:
                continue
        out.append(c)
    return out


def print_top_per_timeframe(n: int = DEFAULT_TOP_N):
    for tf in store.timeframes():
        top_combined = store.top_by(tf, lambda c: c.combined_score(), n)
        print(f"\n--- {tf} TOP (combined_score) ---")
        for c in top_combined:
            tick_info = ""
            if USE_TICK_FILTER:
                tn = c.ticks_needed_for_percent(TARGET_PROFIT_PERCENT, TICK_RULE)
                tick_info = f" ticksNeeded@{TARGET_PROFIT_PERCENT:.2f}%={tn}"
            print(f"{c.market:12} sc={c.combined_score():8.2f} BB={c.BB_Pos:7.2f} "
                  f"MACD_Hist={c.MACD_Hist:7.3f} RSI={c.RSI:5.1f} Trend10={c.Trend10}"
                  f"{tick_info}")


def print_intersection_example():
    tfs = ["5m","15m","30m"]
    inter = store.multi_tf_intersection(
        tfs,
        lambda c: c.combined_score(),
        per_tf_top=12,
        final_limit=10,
        min_tf_presence=2,
        mandatory_tf="15m",
        # --- 틱 필터 파라미터 ---
        apply_tick_filter=True,
        target_profit_percent=0.5,
        max_ticks_for_target=10,
        tick_inclusive=True,          # 10틱 포함하려면 True, 10 이상 제외하려면 False
        tick_rule="UPBIT_KRW",
        tick_filter_mode="mandatory"  # 15m 인디케이터 기준 판정
    )
    print("\n=== Multi-TF Intersection (>=2 TF & must include 15m & Tick Filter) ===")
    if not inter:
        print("(조건 충족 코인 없음 또는 데이터 부족)")
        return
    for market, detail_map in inter:
        scores = {tf: detail_map[tf].combined_score() for tf in detail_map}
        avg_score = sum(scores.values()) / len(scores)
        tf_list_str = ",".join(sorted(detail_map.keys()))
        # 틱 정보 (mandatory_tf 기준)
        if "15m" in detail_map:
            ticks_needed = detail_map["15m"].ticks_needed_for_percent(0.5, "UPBIT_KRW")
        else:
            # fallback
            any_tf = next(iter(detail_map.keys()))
            ticks_needed = detail_map[any_tf].ticks_needed_for_percent(0.5, "UPBIT_KRW")
        print(f"{market:12} avg={avg_score:7.2f} ticks(15m)={ticks_needed:2d} TFs=({tf_list_str}) " +
              " ".join(f"{tf}:{scores[tf]:.1f}" for tf in sorted(scores.keys())))


def print_macd_hist_cross_events(events: List[dict]):
    if not events:
        return
    # 상승 전환 먼저
    ups = [e for e in events if e.get("direction") == "UP"]
    downs = [e for e in events if e.get("direction") == "DOWN"]
    if ups:
        print("\n[MACD_Hist 상승 전환 감지]")
        for e in ups:
            print(f"{e['timeframe']:>3} {e['market']:12} prev={e['prev']:7.4f} -> curr={e['curr']:7.4f}  RSI={e['RSI']:5.1f} BB_Pos={e['BB_Pos']:7.2f}")
    if downs:
        print("\n[MACD_Hist 하락 전환 감지]")
        for e in downs:
            print(f"{e['timeframe']:>3} {e['market']:12} prev={e['prev']:7.4f} -> curr={e['curr']:7.4f}  RSI={e['RSI']:5.1f} BB_Pos={e['BB_Pos']:7.2f}")

# ==============================
# 스케줄러
# ==============================
class FetchScheduler:
    def __init__(self, interval: int = REFRESH_INTERVAL_SEC):
        self.interval = interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("[INFO] 스케줄러 시작")

    def _loop(self):
        while not self._stop.is_set():
            events = fetch_once(verbose=False)
            print_top_per_timeframe()
            print_intersection_example()
            print_macd_hist_cross_events(events)
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        print("[INFO] 스케줄러 중지")

# ==============================
# 실행 예시
# ==============================
def main():
    events = fetch_once(verbose=True)
    print_top_per_timeframe()
    print_intersection_example()
    print_macd_hist_cross_events(events)

    scheduler = FetchScheduler(interval=REFRESH_INTERVAL_SEC)
    scheduler.start()
    try:
        end_t = time.time() + 180
        while time.time() < end_t:
            time.sleep(5)
        scheduler.stop()
    except KeyboardInterrupt:
        scheduler.stop()

if __name__ == "__main__":
    main()