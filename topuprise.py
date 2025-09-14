import time
import threading
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable, Iterable, Tuple
import requests
import json

# ==============================
# 설정
# ==============================
API_URL = "http://ywydpapa.iptime.org:8000/api/bbtrend30"
REFRESH_INTERVAL_SEC = 30
TIMEOUT_SEC = 8
DEFAULT_TOP_N = 8
TARGET_TIMEFRAMES = ["5m", "15m", "30m"]
MIN_BANDWIDTH = 0.7   # 0.7% 미만 코인 제외 (BandWidth 단위가 %)

# 스코어 / 필터 파라미터
RSI_OVERHEAT = 72
RSI_GOOD_LOW, RSI_GOOD_HIGH = 50, 70

# MACD_Hist 교차 감지 활성화
ENABLE_MACD_HIST_CROSS_DETECTION = True
MACD_HIST_MIN_ABS_FOR_EVENT = 0.0001
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
            w = (i + 1) / length  # 최근(뒤쪽) 가중
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
        events = []
        new_map = {}
        for c in coins:
            prev = self._prev_macd_hist.get(c.market)
            curr = c.MACD_Hist
            if ENABLE_MACD_HIST_CROSS_DETECTION and prev is not None:
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
            min_avg_score: float | None = None,
            min_bandwidth: float | None = None
    ) -> List[Tuple[str, Dict[str, CoinIndicator], float]]:
        """
        반환: [(market, {tf: CoinIndicator}, avg_score)]
        """
        with self._lock:
            tf_list = list(timeframes)
            rank_sets: Dict[str, Dict[str, CoinIndicator]] = {}
            for tf in tf_list:
                store = self._stores.get(tf)
                if not store:
                    return []
                all_coins = store.all()
                if min_bandwidth is not None:
                    all_coins = [c for c in all_coins if c.BandWidth >= min_bandwidth]
                else:
                    if MIN_BANDWIDTH is not None:
                        all_coins = [c for c in all_coins if c.BandWidth >= MIN_BANDWIDTH]
                if not all_coins:
                    return []
                sorted_coins = sorted(all_coins, key=key_func, reverse=True)
                if per_tf_top and per_tf_top > 0:
                    sorted_coins = sorted_coins[:per_tf_top]
                rank_sets[tf] = {c.market: c for c in sorted_coins}

            if not rank_sets:
                return []
            common_markets = set.intersection(*(set(d.keys()) for d in rank_sets.values()))
            if not common_markets:
                return []

            rows: List[Tuple[str, float, Dict[str, CoinIndicator]]] = []
            for m in common_markets:
                score_sum = 0.0
                details: Dict[str, CoinIndicator] = {}
                for tf in tf_list:
                    c = rank_sets[tf][m]
                    details[tf] = c
                    score_sum += key_func(c)
                avg_score = score_sum / len(tf_list)
                if min_avg_score is not None and avg_score < min_avg_score:
                    continue
                rows.append((m, avg_score, details))

            rows.sort(key=lambda x: x[1], reverse=True)
            if final_limit and final_limit > 0:
                rows = rows[:final_limit]

            return [(m, details, avg) for (m, avg, details) in rows]

    def snapshot_summary(self) -> dict:
        with self._lock:
            return {
                "timeframes": {tf: len(store.all()) for tf, store in self._stores.items()},
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
# JSON 빌더 유틸
# ==============================
def build_top_per_timeframe_json(n: int = DEFAULT_TOP_N) -> dict:
    """
    각 타임프레임 top combined_score 목록 JSON 구조 반환
    """
    result = {}
    for tf in store.timeframes():
        coins = store.top_by(tf, lambda c: c.combined_score(), n)
        result[tf] = [
            {
                "market": c.market,
                "score": round(c.combined_score(), 4),
                "BB_Pos": round(c.BB_Pos, 4),
                "BandWidth": round(c.BandWidth, 4),
                "MACD_Hist": round(c.MACD_Hist, 6),
                "RSI": round(c.RSI, 3),
                "Trend10": c.Trend10,
                "time": c.time
            }
            for c in coins
        ]
    return {
        "type": "top_per_timeframe",
        "generated_at": time.time(),
        "top_n": n,
        "data": result
    }

def build_intersection_json(
    tfs: Optional[List[str]] = None,
    per_tf_top: int = 12,
    final_limit: int = 10,
    min_avg_score: Optional[float] = None,
    min_bandwidth: Optional[float] = None,
    round_digits: int = 4,
    include_fields: tuple = ("BB_Pos","BandWidth","RSI","MACD_Hist","Trend10","time")
) -> dict:
    if tfs is None:
        tfs = ["5m","15m","30m"]
    rows = store.multi_tf_intersection(
        tfs,
        lambda c: c.combined_score(),
        per_tf_top=per_tf_top,
        final_limit=final_limit,
        min_avg_score=min_avg_score,
        min_bandwidth=min_bandwidth
    )
    items = []
    for market, detail_map, avg_score in rows:
        per_tf = {}
        for tf, coin in detail_map.items():
            entry = {
                "score": round(coin.combined_score(), round_digits)
            }
            for fld in include_fields:
                if hasattr(coin, fld):
                    val = getattr(coin, fld)
                    if isinstance(val, float):
                        entry[fld] = round(val, 6)
                    else:
                        entry[fld] = val
            per_tf[tf] = entry
        items.append({
            "market": market,
            "avg_score": round(avg_score, round_digits),
            "per_timeframe": per_tf
        })
    return {
        "type": "multi_tf_intersection",
        "generated_at": time.time(),
        "timeframes": tfs,
        "count": len(items),
        "per_tf_top": per_tf_top,
        "final_limit": final_limit,
        "min_avg_score": min_avg_score,
        "min_bandwidth": min_bandwidth,
        "items": items
    }

def build_macd_hist_events_json(events: List[dict]) -> dict:
    # 그대로 events 리스트를 JSON 구조로 래핑
    return {
        "type": "macd_hist_cross_events",
        "generated_at": time.time(),
        "count": len(events),
        "events": events
    }

def build_full_snapshot_json(
    top_n: int = DEFAULT_TOP_N,
    intersection_tfs: Optional[List[str]] = None,
    per_tf_top: int = 12,
    final_limit: int = 10
) -> dict:
    """
    fetch → events 처리 후 한 번의 루프에서 출력할 완성 스냅샷 JSON
    """
    return {
        "snapshot_generated_at": time.time(),
        "summary": store.snapshot_summary(),
        "top_per_timeframe": build_top_per_timeframe_json(top_n),
        "multi_tf_intersection": build_intersection_json(
            tfs=intersection_tfs or ["5m","15m","30m"],
            per_tf_top=per_tf_top,
            final_limit=final_limit
        ),
        # NOTE: events 는 fetch 시점에 따로 빌드/주입 (스케줄러에서)
        # 여기서는 빈 구조로 초기화 가능
        "macd_hist_events": None
    }

# ==============================
# 스케줄러
# ==============================
class FetchScheduler:
    def __init__(self, interval: int = REFRESH_INTERVAL_SEC, indent: Optional[int] = None):
        self.interval = interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.indent = indent  # None 이면 compact 한 줄

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
            # 루프마다 전체 스냅샷 구성
            snapshot = build_full_snapshot_json()
            snapshot["macd_hist_events"] = build_macd_hist_events_json(events)
            # JSON 출력 (한 번)
            print(json.dumps(snapshot, ensure_ascii=False, indent=self.indent))
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
    snapshot = build_full_snapshot_json()
    snapshot["macd_hist_events"] = build_macd_hist_events_json(events)
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))

    scheduler = FetchScheduler(interval=REFRESH_INTERVAL_SEC, indent=None)
    scheduler.start()
    try:
        end_t = time.time() + 300
        while time.time() < end_t:
            time.sleep(5)
        scheduler.stop()
    except KeyboardInterrupt:
        scheduler.stop()

def uprises():
    events = fetch_once(verbose=True)
    snapshot = build_full_snapshot_json()
    snapshot["macd_hist_events"] = build_macd_hist_events_json(events)
    return json.dumps(snapshot, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    tupt = uprises()
    tupt = json.loads(tupt)
    print(tupt["multi_tf_intersection"]["items"])
