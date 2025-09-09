import requests
import time
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# ==============================
# Config
# ==============================
TOP30_ENDPOINT = "http://ywydpapa.iptime.org:8000/api/top30coins"
UPBIT_BASE = "https://api.upbit.com/v1/candles/minutes/{unit}?market={market}&count={count}"
TIMEFRAMES = [5, 15, 30]
LOOKBACK_CANDLES = 120
RECENT_N = 20
BB_PERIOD = 20
BB_K = 2
SLOPE_WINDOW = 10
TOLERANCE_RATIO = 0.07
HOLD_CANDLES = 3
REQUEST_SLEEP = 0.06
EXCLUDE_INCOMPLETE_CANDLE = True

# 최근 추세 분석 창
RECENT_TREND_WINDOW = 10

# 스케줄 설정: 2분 간격 실행
INTERVAL_MINUTES = 2
MAX_RUNS = None   # None = 무한, 정수 지정 시 해당 횟수 후 종료

# ==============================
# Helpers / Logging
# ==============================
def log_warn(msg: str):
    print(f"[WARN] {msg}")

def now_kst() -> pd.Timestamp:
    return pd.Timestamp.utcnow() + pd.Timedelta(hours=9)

# ==============================
# Data Fetch
# ==============================
def get_top_markets(endpoint: str = TOP30_ENDPOINT) -> List[str]:
    try:
        r = requests.get(endpoint, timeout=5)
        r.raise_for_status()
        js = r.json()
        markets = js.get("markets", [])
        return markets
    except Exception as e:
        log_warn(f"top markets fetch failed: {e}")
        return []

def fetch_upbit_minutes(market: str, unit: int, count: int = LOOKBACK_CANDLES, allow_debug: bool=False) -> pd.DataFrame:
    url = UPBIT_BASE.format(unit=unit, market=market, count=count)
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            log_warn(f"{market} {unit}m status {r.status_code}")
            return pd.DataFrame()
        data = r.json()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)

        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()]
        if 'timestamp' in df.columns:
            df = df.rename(columns={'timestamp': 'timestamp_original'})
        if 'candle_date_time_kst' not in df.columns:
            log_warn(f"{market} {unit}m: candle_date_time_kst missing")
            return pd.DataFrame()

        df['candle_date_time_kst'] = pd.to_datetime(df['candle_date_time_kst'])
        df = df.drop_duplicates(subset='candle_date_time_kst', keep='last')

        needed_map = {
            'opening_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'trade_price': 'close',
            'candle_acc_trade_volume': 'volume'
        }
        for orig in needed_map.keys():
            if orig not in df.columns:
                df[orig] = np.nan

        df = df.rename(columns=needed_map)
        df = df.rename(columns={'candle_date_time_kst': 'timestamp'})
        df = df[['timestamp','open','high','low','close','volume']]
        df = df.sort_values('timestamp').reset_index(drop=True)

        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()]
        return df
    except Exception as e:
        log_warn(f"fetch failed {market} {unit}m: {e}")
        return pd.DataFrame()

# ==============================
# Incomplete Candle Handling
# ==============================
def remove_incomplete_last(df: pd.DataFrame, unit: int) -> (pd.DataFrame, bool):
    if df.empty:
        return df, False
    last_ts = df['timestamp'].iloc[-1]
    if not isinstance(last_ts, pd.Timestamp):
        last_ts = pd.to_datetime(last_ts)
    current = now_kst()
    if last_ts.tzinfo is None or last_ts.tzinfo.utcoffset(last_ts) is None:
        last_ts = last_ts.tz_localize(current.tzinfo)
    else:
        last_ts = last_ts.tz_convert(current.tzinfo)
    expected_close = last_ts + pd.Timedelta(minutes=unit)
    if expected_close > current:
        return df.iloc[:-1].copy(), True
    return df, False

# ==============================
# Calculations
# ==============================
def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if len(df) == 0:
        return df
    df['return_pct'] = df['close'].pct_change()
    df['log_return'] = np.log(df['close']).diff()
    return df

def add_bollinger(df: pd.DataFrame, period: int = BB_PERIOD, k: float = BB_K) -> pd.DataFrame:
    df = df.copy()
    if len(df) < period:
        for c in ['bb_mid','bb_sd','bb_upper','bb_lower','band_pos']:
            df[c] = np.nan
        return df
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_sd'] = df['close'].rolling(period).std(ddof=0)
    df['bb_upper'] = df['bb_mid'] + k * df['bb_sd']
    df['bb_lower'] = df['bb_mid'] - k * df['bb_sd']
    width = df['bb_upper'] - df['bb_lower']
    width = width.replace(0, np.nan)
    df['band_pos'] = (df['close'] - df['bb_lower']) / width
    return df

def compute_slope(series: pd.Series) -> Optional[float]:
    y = series.dropna()
    if len(y) < SLOPE_WINDOW:
        return None
    y = y.iloc[-SLOPE_WINDOW:]
    x = np.arange(len(y))
    try:
        coeffs = np.polyfit(x, y.values, 1)
        return float(coeffs[0])
    except Exception:
        return None

def compute_recent_stats(df: pd.DataFrame, n: int = RECENT_N) -> Dict[str, Any]:
    if len(df) < n + 1:
        return {}
    recent = df.tail(n)
    avg_return_pct = recent['return_pct'].mean()
    avg_abs_return_pct = recent['return_pct'].abs().mean()
    body_pct = (recent['close'] - recent['open']).abs() / recent['open']
    range_pct = (recent['high'] - recent['low']) / recent['open']
    avg_body_pct = body_pct.mean()
    avg_range_pct = range_pct.mean()
    return {
        "avg_return_pct": float(avg_return_pct) if pd.notna(avg_return_pct) else None,
        "avg_abs_return_pct": float(avg_abs_return_pct) if pd.notna(avg_abs_return_pct) else None,
        "avg_body_pct": float(avg_body_pct) if pd.notna(avg_body_pct) else None,
        "avg_range_pct": float(avg_range_pct) if pd.notna(avg_range_pct) else None,
    }

# ==============================
# Recent Window Trend / Touch / Reversal
# ==============================
def compute_recent_window_trend(df: pd.DataFrame,
                                tolerance: float = TOLERANCE_RATIO,
                                window: int = RECENT_TREND_WINDOW) -> Dict[str, Any]:
    if len(df) < window or 'band_pos' not in df.columns:
        return {
            "recent_trend": "NEUTRAL",
            "recent_trend_kr": "중립",
            "recent_upper_touch": False,
            "recent_lower_touch": False,
            "recent_reversal": "NONE",
            "recent_reversal_kr": "없음",
            "recent_detail": {"usable": False}
        }

    sub = df.tail(window)
    band_pos = sub['band_pos']
    returns = sub['return_pct']

    upper_touch = (band_pos >= (1 - tolerance)).any()
    lower_touch = (band_pos <= tolerance).any()
    mean_band_pos = band_pos.mean()
    mean_return = returns.mean()

    if upper_touch and (mean_return or 0) > 0:
        trend = "UP"
    elif lower_touch and (mean_return or 0) < 0:
        trend = "DOWN"
    elif mean_band_pos > 0.6 and (mean_return or 0) > 0:
        trend = "UP"
    elif mean_band_pos < 0.4 and (mean_return or 0) < 0:
        trend = "DOWN"
    else:
        trend = "NEUTRAL"

    half = window // 2
    first = sub.iloc[:half]
    second = sub.iloc[-half:]

    def half_slope(vals: pd.Series) -> Optional[float]:
        y = vals.dropna()
        if len(y) < 3:
            return None
        x = np.arange(len(y))
        try:
            return float(np.polyfit(x, y.values, 1)[0])
        except Exception:
            return None

    slope_first = half_slope(first['bb_mid'])
    slope_second = half_slope(second['bb_mid'])
    mean_first_bp = first['band_pos'].mean()
    mean_second_bp = second['band_pos'].mean()

    reversal = "NONE"
    if slope_first is not None and slope_second is not None:
        if slope_first > 0 and slope_second < 0 and mean_first_bp > 0.5 and mean_second_bp < 0.5:
            reversal = "DOWN"
        elif slope_first < 0 and slope_second > 0 and mean_first_bp < 0.5 and mean_second_bp > 0.5:
            reversal = "UP"

    kr_map_trend = {"UP": "상승", "DOWN": "하락", "NEUTRAL": "중립"}
    kr_map_rev = {"UP": "상방반전", "DOWN": "하방반전", "NONE": "없음"}

    return {
        "recent_trend": trend,
        "recent_trend_kr": kr_map_trend.get(trend, "중립"),
        "recent_upper_touch": bool(upper_touch),
        "recent_lower_touch": bool(lower_touch),
        "recent_reversal": reversal,
        "recent_reversal_kr": kr_map_rev.get(reversal, "없음"),
        "recent_detail": {
            "window": window,
            "mean_band_pos": float(mean_band_pos) if pd.notna(mean_band_pos) else None,
            "mean_return_pct": float(mean_return) if pd.notna(mean_return) else None,
            "slope_first": slope_first,
            "slope_second": slope_second,
            "mean_first_band_pos": float(mean_first_bp) if pd.notna(mean_first_bp) else None,
            "mean_second_band_pos": float(mean_second_bp) if pd.notna(mean_second_bp) else None
        }
    }

# ==============================
# Signal Logic
# ==============================
def derive_bb_signal(row: pd.Series, slope: Optional[float], tolerance: float = TOLERANCE_RATIO) -> str:
    band_pos = row.get('band_pos', np.nan)
    if pd.isna(band_pos) or slope is None:
        return "NO_SIGNAL"
    upper_touch = band_pos >= (1 - tolerance)
    lower_touch = band_pos <= tolerance
    if upper_touch and slope <= 0:
        return "SELL"
    if lower_touch and slope >= 0:
        return "BUY"
    if band_pos > 0.6 and slope > 0:
        return "UP_TREND"
    if band_pos < 0.4 and slope < 0:
        return "DOWN_TREND"
    return "NO_SIGNAL"

def simplify_signal(sig: str) -> str:
    if sig in ("BUY","UP_TREND"):
        return "UP"
    if sig in ("SELL","DOWN_TREND"):
        return "DOWN"
    return "-"

# ==============================
# Per-Timeframe Processing
# ==============================
def process_timeframe(market: str, unit: int, debug: bool=False) -> Dict[str, Any]:
    raw = fetch_upbit_minutes(market, unit, LOOKBACK_CANDLES, allow_debug=debug)
    if raw.empty:
        return {"error": "no_data"}

    incomplete_dropped = False
    if EXCLUDE_INCOMPLETE_CANDLE:
        raw, incomplete_dropped = remove_incomplete_last(raw, unit)
        if raw.empty:
            return {"error": "no_complete_candle"}

    df = add_returns(raw)
    df = add_bollinger(df)
    slope = compute_slope(df['bb_mid'])
    if df.empty:
        return {"error": "after_processing_empty"}

    last = df.iloc[-1]
    stats = compute_recent_stats(df)
    recent_info = compute_recent_window_trend(df, window=RECENT_TREND_WINDOW)

    band_pos = last.get('band_pos', np.nan)
    slope_val = slope if slope is not None else np.nan
    signal = derive_bb_signal(last, slope)
    avg_body = stats.get('avg_body_pct')
    if avg_body is not None:
        expected_profit_long_pct = avg_body * HOLD_CANDLES
        expected_profit_short_pct = avg_body * HOLD_CANDLES
    else:
        expected_profit_long_pct = None
        expected_profit_short_pct = None

    result = {
        "last_price": float(last['close']),
        "band_pos": float(band_pos) if pd.notna(band_pos) else None,
        "slope": float(slope_val) if pd.notna(slope_val) else None,
        "bb_signal": signal,
        "upper_touch": bool(band_pos >= (1 - TOLERANCE_RATIO)) if pd.notna(band_pos) else False,
        "lower_touch": bool(band_pos <= TOLERANCE_RATIO) if pd.notna(band_pos) else False,
        "expected_profit_long_pct": expected_profit_long_pct,
        "expected_profit_short_pct": expected_profit_short_pct,
        "recent_trend": recent_info["recent_trend"],
        "recent_trend_kr": recent_info["recent_trend_kr"],
        "recent_upper_touch": recent_info["recent_upper_touch"],
        "recent_lower_touch": recent_info["recent_lower_touch"],
        "recent_reversal": recent_info["recent_reversal"],
        "recent_reversal_kr": recent_info["recent_reversal_kr"],
        "recent_detail": recent_info["recent_detail"]
    }
    result.update(stats)

    if debug:
        result["row_timestamp"] = str(last['timestamp'])
        result["candles_loaded"] = len(df)
        result["incomplete_dropped"] = incomplete_dropped
        if slope is not None and last['close'] != 0:
            result["slope_normalized"] = float(slope / last['close'])
        else:
            result["slope_normalized"] = None
    return result

# ==============================
# Aggregation Logic
# ==============================
def aggregate_signals(tf_dict: Dict[str, Dict[str, Any]]) -> str:
    signals = []
    for info in tf_dict.values():
        s = info.get('bb_signal')
        if s:
            signals.append(s)
    if not signals:
        return "NO_SIGNAL"
    if "BUY" in signals and "SELL" not in signals:
        return "BUY"
    if "SELL" in signals and "BUY" not in signals:
        return "SELL"
    up_trends = signals.count("UP_TREND")
    down_trends = signals.count("DOWN_TREND")
    if up_trends > down_trends:
        return "UP_TREND"
    if down_trends > up_trends:
        return "DOWN_TREND"
    return "NO_SIGNAL"

# ==============================
# Main Scan
# ==============================
def run_scan(markets: Optional[List[str]] = None,
             timeframes: List[int] = TIMEFRAMES,
             debug: bool=False) -> List[Dict[str, Any]]:
    if markets is None:
        markets = get_top_markets()
    results = []
    for m in markets:
        tf_result = {}
        for tf in timeframes:
            time.sleep(REQUEST_SLEEP)
            data = process_timeframe(m, tf, debug=debug)
            tf_result[f"{tf}m"] = data
        agg = aggregate_signals(tf_result)
        results.append({
            "market": m,
            "timeframes": tf_result,
            "aggregated_signal": agg
        })
    return results

# ==============================
# Utility: Flatten
# ==============================
def flatten_results(results: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in results:
        market = item['market']
        agg = item['aggregated_signal']
        for tf, info in item['timeframes'].items():
            row = {"market": market, "timeframe": tf, "aggregated_signal": agg}
            row.update(info)
            rows.append(row)
    return pd.DataFrame(rows)

# ==============================
# Formatter
# ==============================
def format_recent_trend_line(item: Dict[str, Any],
                             timeframes: List[int] = TIMEFRAMES,
                             include_extras_tf=(5, 15),
                             no_space_between: bool=False) -> str:
    market = item.get("market", "UNKNOWN")
    parts = [market]
    sep = "" if no_space_between else " "
    for tf in timeframes:
        key = f"{tf}m"
        info = item['timeframes'].get(key, {})
        trend_kr = info.get("recent_trend_kr", "중립")

        extras = []
        if tf in include_extras_tf:
            if info.get("recent_upper_touch"):
                extras.append("상터치")
            if info.get("recent_lower_touch"):
                extras.append("하터치")
            rev = info.get("recent_reversal")
            if rev == "UP":
                extras.append("반전↑")
            elif rev == "DOWN":
                extras.append("반전↓")

        if extras:
            label = f"{tf}분({trend_kr}," + ",".join(extras) + ")"
        else:
            label = f"{tf}분({trend_kr})"
        parts.append(label)
    return sep.join(parts)

# ==============================
# Scheduling Helpers
# ==============================
def align_to_even_minute(ts: pd.Timestamp, interval: int = INTERVAL_MINUTES) -> pd.Timestamp:
    """
    ts 이후(또는 현재)가 되는 가장 가까운 짝수( interval 배수 ) 분 0초 시각 반환.
    interval=2 이면 분%2==0 & 초==0.
    """
    base = ts.replace(second=0, microsecond=0)
    # 이미 정각 & 짝수분이면 그대로
    if ts.second == 0 and (ts.minute % interval == 0):
        return base
    minute = ts.minute
    # 다음 interval 배수 분
    next_min = minute + (interval - (minute % interval))
    if next_min >= 60:
        base = base + pd.Timedelta(hours=1)
        base = base.replace(minute=0)
    else:
        base = base.replace(minute=next_min)
    return base

# ==============================
# Main Loop (Every 2 Minutes)
# ==============================
if __name__ == "__main__":
    try:
        run_counter = 0
        next_run = align_to_even_minute(now_kst(), interval=INTERVAL_MINUTES)
        print(f"[INFO] First scheduled run at {next_run} (KST)")

        while True:
            now = now_kst()
            # 대기
            if now < next_run:
                sleep_sec = (next_run - now).total_seconds()
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
            # 실행 시간 (이론적 스케줄 기준)
            run_time = next_run
            start_actual = now_kst()

            markets = get_top_markets()
            if not markets:
                print(f"[{start_actual}] No markets retrieved.")
            else:
                scan = run_scan(markets[:30], debug=False)
                # 알파벳(사전) 순 정렬
                scan_sorted = sorted(scan, key=lambda x: x['market'])
                header = f"=== 최근 10개 봉 추세/터치/반전 (5분,15분 확장) Scheduled:{run_time} Started:{start_actual} ==="
                print(header)
                for item in scan_sorted:
                    print(format_recent_trend_line(item))

            run_counter += 1
            if (MAX_RUNS is not None) and (run_counter >= MAX_RUNS):
                print(f"[INFO] Reached MAX_RUNS={MAX_RUNS}. Exiting.")
                break

            # 다음 실행 시각 고정 간격 증가
            next_run = next_run + pd.Timedelta(minutes=INTERVAL_MINUTES)
            # 만약 현재 시간이 이미 다음_run 을 지나쳤다면(지연), 현재 시간을 기준으로 다시 정렬
            now_after = now_kst()
            if now_after > next_run:
                # 여러 주기가 밀린 경우를 방지
                missed = int((now_after - next_run).total_seconds() // (INTERVAL_MINUTES * 60)) + 1
                next_run = next_run + pd.Timedelta(minutes=INTERVAL_MINUTES * missed)
                print(f"[WARN] Delayed execution detected. Skipping {missed} interval(s). Next run reset to {next_run}")

    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt received. Stopping scheduler.")