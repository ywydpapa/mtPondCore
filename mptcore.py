import time
import threading
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Callable, Iterable, Tuple
import requests
import math

# ==============================
# 설정
# ==============================
API_URL = "http://ywydpapa.iptime.org:8000/api/bbtrend30"
REFRESH_INTERVAL_SEC = 30
TIMEOUT_SEC = 8
DEFAULT_TOP_N = 8
TARGET_TIMEFRAMES = ["5m", "15m", "30m"]   # 원하는 타임프레임 필터 (None 또는 빈 리스트면 전체)

# 임계값(필터/스코어용) - 필요 시 조정
RSI_OVERHEAT = 72
RSI_GOOD_LOW, RSI_GOOD_HIGH = 50, 70
BB_POS_BREAKOUT = 100
BB_POS_STRONG_LOW = -20

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
        """
        기본 모멘텀 점수 예시:
        - BB_Pos (0~100 구간 + 이상 돌파) 가중
        - MACD_Hist 양수 가점
        - RSI 중간 상단대(50~70) 가점, 과열 패널티
        - BandWidth(변동성)가 너무 과도하면 감점(예: 폭발 후 피로)
        """
        rsi_score = 0
        if RSI_GOOD_LOW <= self.RSI <= RSI_GOOD_HIGH:
            rsi_score = 8
        elif self.RSI > RSI_OVERHEAT:
            rsi_score = -5
        bw_penalty = 0
        if self.BandWidth > 8:
            bw_penalty = - (self.BandWidth - 8) * 0.5  # 과도한 확장 벌점
        return self.BB_Pos * 0.4 + self.MACD_Hist * 25 + rsi_score + bw_penalty

    def trend10_score(self) -> float:
        """
        Trend10 문자열 기반 간단 점수:
        U=+2, u=+1, -=0, d=-1, D=-2
        """
        if not self.Trend10:
            return 0.0
        score_map = {'U': 2, 'u': 1, '-': 0, 'd': -1, 'D': -2}
        total = 0
          # 최근 패턴 뒤쪽(최신)이 더 가중치 높게 (선형 가중치)
        length = len(self.Trend10)
        for i, ch in enumerate(self.Trend10):
            w = (i + 1) / length
            total += w * score_map.get(ch, 0)
        return total

    def combined_score(self) -> float:
        return self.basic_momentum_score() + self.trend10_score() * 3

# ==============================
# 타임프레임 저장소
# ==============================
class TimeframeStore:
    def __init__(self, timeframe: str):
        self.timeframe = timeframe
        self._coins: Dict[str, CoinIndicator] = {}
        self.last_updated: Optional[float] = None

    def update(self, coins: List[CoinIndicator]):
        self._coins = {c.market: c for c in coins}
        self.last_updated = time.time()

    def all(self) -> List[CoinIndicator]:
        return list(self._coins.values())

    def get(self, market: str) -> Optional[CoinIndicator]:
        return self._coins.get(market)

    def top_by(self, key_func: Callable[[CoinIndicator], float], n: int) -> List[CoinIndicator]:
        return sorted(self._coins.values(), key=key_func, reverse=True)[:n]

# ==============================
# 멀티 타임프레임 통합 저장소
# ==============================
class MultiTimeframeStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._stores: Dict[str, TimeframeStore] = {}
        self.last_fetch_status: Optional[str] = None
        self.last_fetch_error: Optional[str] = None
        self.last_server_updated: Optional[str] = None

    def update_from_response(self, resp_json: dict):
        with self._lock:
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
                        # 단일 레코드 에러는 무시
                        print(f"[WARN] parse 실패 tf={tf_name} err={parse_e} raw={row}")
                if tf_name not in self._stores:
                    self._stores[tf_name] = TimeframeStore(tf_name)
                self._stores[tf_name].update(coins)
            self.last_fetch_error = None

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
        """
        각 타임프레임별 상위(per_tf_top) 코인을 뽑고
        교집합(모든 타임프레임에 등장) 종목을 반환.

        반환 형식: [(market, {tf: CoinIndicator, ...}), ...] 점수는 합산(/평균) 정렬.
        """
        with self._lock:
            tf_list = list(timeframes)
            rank_sets: Dict[str, Dict[str, CoinIndicator]] = {}
            for tf in tf_list:
                store = self._stores.get(tf)
                if not store:
                    return []
                top_coins = store.top_by(key_func, per_tf_top)
                rank_sets[tf] = {c.market: c for c in top_coins}

            # 교집합
            common_markets = set.intersection(*(set(d.keys()) for d in rank_sets.values()))
            scored = []
            for m in common_markets:
                # 합산 스코어
                score_sum = 0.0
                details: Dict[str, CoinIndicator] = {}
                for tf in tf_list:
                    c = rank_sets[tf][m]
                    details[tf] = c
                    score_sum += key_func(c)
                avg_score = score_sum / len(tf_list)
                scored.append((m, avg_score, details))

            scored.sort(key=lambda x: x[1], reverse=True)
            # 결과 형태 가공
            result = [(m, det) for (m, _s, det) in scored[:final_limit]]
            return result

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
def fetch_once(verbose: bool = True):
    try:
        r = requests.get(API_URL, timeout=TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            raise ValueError(f"status != ok : {data.get('status')}")
        store.update_from_response(data)
        if verbose:
            print(f"[INFO] fetch 성공 updated={data.get('updated')} tfs={store.timeframes()}")
    except Exception as e:
        store.set_error(str(e))
        if verbose:
            print(f"[ERROR] fetch 실패: {e}")

# ==============================
# 추천/필터 유틸
# ==============================
def filter_candidates(coins: List[CoinIndicator]) -> List[CoinIndicator]:
    """
    1차 필터 예시:
    - BB_Pos >= 60
    - MACD_Hist > 0 (상승 모멘텀)
    - RSI 45~75
    """
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
    """
    5m,15m,30m 모두에서 상위권(각 타임프레임 combined_score 상위 12개)에 드는 교집합 종목 출력
    """
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
            fetch_once(verbose=False)
            print_top_per_timeframe()
            print_intersection_example()
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
    fetch_once(verbose=True)
    print_top_per_timeframe()
    print_intersection_example()

    scheduler = FetchScheduler(interval=REFRESH_INTERVAL_SEC)
    scheduler.start()
    try:
        # 3분 후 종료 (원하면 while True 로)
          # 테스트 목적 제한
        end_t = time.time() + 180
        while time.time() < end_t:
            time.sleep(5)
        scheduler.stop()
    except KeyboardInterrupt:
        scheduler.stop()

if __name__ == "__main__":
    main()