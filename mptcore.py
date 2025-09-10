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
        final_limit: int = 10
    ) -> List[Tuple[str, Dict[str, CoinIndicator]]]:
        with self._lock:
            tf_list = list(timeframes)
            rank_sets: Dict[str, Dict[str, CoinIndicator]] = {}
            for tf in tf_list:
                store = self._stores.get(tf)
                if not store:
                    return []
                top_coins = store.top_by(key_func, per_tf_top)
                rank_sets[tf] = {c.market: c for c in top_coins}
            common_markets = set.intersection(*(set(d.keys()) for d in rank_sets.values()))
            scored = []
            for m in common_markets:
                score_sum = 0.0
                details: Dict[str, CoinIndicator] = {}
                for tf in tf_list:
                    c = rank_sets[tf][m]
                    details[tf] = c
                    score_sum += key_func(c)
                avg_score = score_sum / len(tf_list)
                scored.append((m, avg_score, details))
            scored.sort(key=lambda x: x[1], reverse=True)
            return [(m, det) for (m, _s, det) in scored[:final_limit]]

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
        if c.BB_Pos >= 60 and c.MACD_Hist > 0 and 45 <= c.RSI <= 75:
            out.append(c)
    return out

def print_top_per_timeframe(n: int = DEFAULT_TOP_N):
    for tf in store.timeframes():
        top_combined = store.top_by(tf, lambda c: c.combined_score(), n)
        print(f"\n--- {tf} TOP (combined_score) ---")
        for c in top_combined:
            print(f"{c.market:12} sc={c.combined_score():8.2f} BB={c.BB_Pos:7.2f} MACD_Hist={c.MACD_Hist:7.3f} RSI={c.RSI:5.1f} Trend10={c.Trend10}")

def print_intersection_example():
    tfs = ["5m","15m","30m"]
    inter = store.multi_tf_intersection(tfs, lambda c: c.combined_score(), per_tf_top=12, final_limit=10)
    print("\n=== Multi-TF Intersection (combined_score) ===")
    if not inter:
        print("(교집합 없음 or 데이터 부족)")
        return
    for market, detail_map in inter:
        scores = {tf: detail_map[tf].combined_score() for tf in detail_map}
        avg_score = sum(scores.values()) / len(scores)
        print(f"{market:12} avg={avg_score:7.2f} " +
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