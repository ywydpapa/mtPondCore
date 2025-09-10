import requests
import time
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
from math import sqrt

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

# 스케줄 설정
INTERVAL_MINUTES = 2
MAX_RUNS = None  # None=무한

# 고급 반전 탐지 파라미터
ADV_CFG = {
    "window": 10,
    "tolerance": 0.07,
    "band_pos_mid_upper": 0.55,
    "band_pos_mid_lower": 0.45,
    "band_pos_gap_min": 0.18,
    "slope_min_abs": 0.0,              # 필요 시 (예: 0.0001) 조정
    "r2_min": 0.40,
    "cluster_min_ratio": 0.30,
    "band_contraction_lookback": 50,
    "band_contraction_quantile": 0.30,
    "confirm_recent_bars": 3,
    "atr_period": 14,
    "atr_expansion_factor": 1.15,
    "volume_ma_period": 20,
    "volume_factor": 1.4,
    "cool_off_bars": 5,
}

# (market, timeframe)별 마지막 반전 상태 저장 전역 dict
# key: (market, timeframe_int) -> {"last_index": int, "direction": "UP"/"DOWN"}
REVERSAL_STATE_STORE: Dict[Tuple[str,int], Dict[str, Any]] = {}

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
# Advanced Reversal Helpers
# ==============================
def _linear_regression_stats(y: np.ndarray):
    n = len(y)
    x = np.arange(n)
    if n < 3:
        return None, None, None, None
    x_mean = x.mean()
    y_mean = y.mean()
    ss_tot = ((y - y_mean) ** 2).sum()
    cov_xy = ((x - x_mean) * (y - y_mean)).sum()
    ss_x = ((x - x_mean) ** 2).sum()
    if ss_x == 0:
        return None, None, None, None
    slope = cov_xy / ss_x
    intercept = y_mean - slope * x_mean
    y_pred = slope * x + intercept
    ss_res = ((y - y_pred) ** 2).sum()
    r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0
    if n > 2:
        se = sqrt((ss_res / (n - 2)) / ss_x) if ss_x != 0 else None
    else:
        se = None
    t_stat = slope / se if (se and se != 0) else None
    return slope, r2, se, t_stat

def _atr(df: pd.DataFrame, period: int):
    if not set(['high','low','close']).issubset(df.columns):
        return None
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)
    tr = np.maximum(high - low, np.maximum((high - prev_close).abs(), (low - prev_close).abs()))
    return tr.rolling(period).mean()

# ==============================
# Advanced Reversal Detection
# ==============================
def compute_recent_window_trend_advanced(
    df: pd.DataFrame,
    prev_reversal_state: dict = None,
    cfg: dict = ADV_CFG
) -> dict:
    """
    고급 반전/추세 탐지:
    - trigger / pre / confirm / quality / volume / ATR / cool-off
    반환 구조:
    {
      recent_trend, recent_trend_kr,
      recent_upper_touch, recent_lower_touch,
      recent_reversal, recent_reversal_kr,
      recent_reversal_grade, recent_reversal_grade_kr,
      recent_detail: {...}
    }
    """
    window = cfg["window"]
    tolerance = cfg["tolerance"]
    if len(df) < window + 5 or 'band_pos' not in df.columns:
        return {
            "recent_trend": "NEUTRAL",
            "recent_trend_kr": "중립",
            "recent_upper_touch": False,
            "recent_lower_touch": False,
            "recent_reversal": "NONE",
            "recent_reversal_kr": "없음",
            "recent_reversal_grade": "NONE",
            "recent_reversal_grade_kr": "없음",
            "recent_detail": {"usable": False}
        }

    # 밴드폭 확보
    if {'bb_upper', 'bb_lower'}.issubset(df.columns) and 'band_width' not in df.columns:
        df = df.copy()
        df['band_width'] = df['bb_upper'] - df['bb_lower']
    elif 'band_width' not in df.columns:
        df = df.copy()
        df['band_width'] = np.nan

    sub = df.tail(window).copy()
    band_pos = sub['band_pos']
    returns = sub['return_pct']

    upper_touch = (band_pos >= (1 - tolerance)).any()
    lower_touch = (band_pos <= tolerance).any()
    mean_bp = band_pos.mean()
    mean_ret = returns.mean()

    if upper_touch and (mean_ret or 0) > 0:
        trend = "UP"
    elif lower_touch and (mean_ret or 0) < 0:
        trend = "DOWN"
    elif mean_bp > 0.6 and (mean_ret or 0) > 0:
        trend = "UP"
    elif mean_bp < 0.4 and (mean_ret or 0) < 0:
        trend = "DOWN"
    else:
        trend = "NEUTRAL"

    half = window // 2
    first = sub.iloc[:half]
    second = sub.iloc[-half:]

    bb_mid = df['bb_mid']
    first_mid = first['bb_mid'].dropna()
    second_mid = second['bb_mid'].dropna()
    slope_first = r2_first = slope_second = r2_second = None
    t_first = t_second = None
    if len(first_mid) >= 3:
        slope_first, r2_first, _, t_first = _linear_regression_stats(first_mid.values)
    if len(second_mid) >= 3:
        slope_second, r2_second, _, t_second = _linear_regression_stats(second_mid.values)

    mean_first_bp = first['band_pos'].mean()
    mean_second_bp = second['band_pos'].mean()
    band_pos_gap = abs(mean_first_bp - mean_second_bp)

    cluster_up_ratio = (first['band_pos'] >= (1 - tolerance)).mean()
    cluster_down_ratio = (first['band_pos'] <= tolerance).mean()

    # 수축 판단: 전체 df 기준 (lookback) 과거 폭 분위 → sub first 폭 비교
    contraction_region = df.tail(cfg["band_contraction_lookback"])['band_width']
    contraction_thresh = contraction_region.quantile(cfg["band_contraction_quantile"]) if contraction_region.notna().any() else np.nan
    first_band_width_mean = first['band_width'].mean()
    band_contraction = (pd.notna(contraction_thresh)
                        and pd.notna(first_band_width_mean)
                        and first_band_width_mean <= contraction_thresh)

    # ATR
    atr_series = _atr(df, cfg["atr_period"])
    atr_last = atr_series.iloc[-1] if atr_series is not None else None
    atr_mean = (atr_series.tail(cfg["atr_period"] * 2).mean()
                if atr_series is not None else None)
    atr_expansion = False
    if (atr_last is not None) and (atr_mean is not None) and atr_mean != 0:
        atr_expansion = atr_last > atr_mean * cfg["atr_expansion_factor"]

    # 거래량
    if 'volume' in df.columns:
        vol_ma = df['volume'].rolling(cfg["volume_ma_period"]).mean()
        vol_last = df['volume'].iloc[-1]
        vol_ma_last = vol_ma.iloc[-1]
        volume_expansion = (pd.notna(vol_ma_last)
                             and vol_ma_last > 0
                             and vol_last > vol_ma_last * cfg["volume_factor"])
    else:
        volume_expansion = False

    confirm_n = cfg["confirm_recent_bars"]
    tail_confirm = sub.tail(confirm_n)
    mid_up = cfg["band_pos_mid_upper"]
    mid_dn = cfg["band_pos_mid_lower"]

    # slope 절대값 필터 옵션 (slope_min_abs > 0 인 경우)
    slope_min_abs = cfg.get("slope_min_abs", 0.0) or 0.0
    slope_first_ok = (slope_first is not None and abs(slope_first) >= slope_min_abs)
    slope_second_ok = (slope_second is not None and abs(slope_second) >= slope_min_abs)

    trigger_up = (slope_first_ok and slope_second_ok and
                  slope_first < 0 and slope_second > 0 and
                  mean_first_bp < mid_dn and mean_second_bp > mid_up and
                  band_pos_gap >= cfg["band_pos_gap_min"])

    trigger_down = (slope_first_ok and slope_second_ok and
                    slope_first > 0 and slope_second < 0 and
                    mean_first_bp > mid_up and mean_second_bp < mid_dn and
                    band_pos_gap >= cfg["band_pos_gap_min"])

    pre_up = (cluster_down_ratio >= cfg["cluster_min_ratio"]) and band_contraction
    pre_down = (cluster_up_ratio >= cfg["cluster_min_ratio"]) and band_contraction

    confirm_up = trigger_up and (tail_confirm['band_pos'] > mid_up).all()
    confirm_down = trigger_down and (tail_confirm['band_pos'] < mid_dn).all()

    quality_ok = ((r2_first or 0) >= cfg["r2_min"] and (r2_second or 0) >= cfg["r2_min"])

    cool_off_active = False
    if prev_reversal_state and "last_index" in prev_reversal_state:
        last_idx = prev_reversal_state["last_index"]
        current_idx = df.index[-1]
        if (current_idx - last_idx) < cfg["cool_off_bars"]:
            cool_off_active = True

    reversal = "NONE"
    Grade = "NONE"
    if not cool_off_active:
        if trigger_up:
            if pre_up and confirm_up and quality_ok and (volume_expansion or atr_expansion):
                reversal = "UP"; Grade = "STRONG"
            elif pre_up and confirm_up and quality_ok:
                reversal = "UP"; Grade = "CONFIRMED"
            else:
                reversal = "UP"; Grade = "POTENTIAL"
        elif trigger_down:
            if pre_down and confirm_down and quality_ok and (volume_expansion or atr_expansion):
                reversal = "DOWN"; Grade = "STRONG"
            elif pre_down and confirm_down and quality_ok:
                reversal = "DOWN"; Grade = "CONFIRMED"
            else:
                reversal = "DOWN"; Grade = "POTENTIAL"

    kr_map_trend = {"UP": "상승", "DOWN": "하락", "NEUTRAL": "중립"}
    kr_map_rev = {"UP": "상승반전", "DOWN": "하락반전", "NONE": "없음"}
    kr_grade = {"STRONG": "강", "CONFIRMED": "확정", "POTENTIAL": "잠재", "NONE": "없음"}

    return {
        "recent_trend": trend,
        "recent_trend_kr": kr_map_trend.get(trend, "중립"),
        "recent_upper_touch": bool(upper_touch),
        "recent_lower_touch": bool(lower_touch),
        "recent_reversal": reversal,
        "recent_reversal_kr": kr_map_rev.get(reversal, "없음"),
        "recent_reversal_grade": Grade,
        "recent_reversal_grade_kr": kr_grade.get(Grade, "없음"),
        "recent_detail": {
            "usable": True,
            "window": window,
            "mean_band_pos": float(mean_bp) if pd.notna(mean_bp) else None,
            "mean_return_pct": float(mean_ret) if pd.notna(mean_ret) else None,
            "slope_first": slope_first,
            "slope_second": slope_second,
            "r2_first": r2_first,
            "r2_second": r2_second,
            "t_first": t_first,
            "t_second": t_second,
            "mean_first_band_pos": float(mean_first_bp) if pd.notna(mean_first_bp) else None,
            "mean_second_band_pos": float(mean_second_bp) if pd.notna(mean_second_bp) else None,
            "band_pos_gap": band_pos_gap,
            "cluster_up_ratio": cluster_up_ratio,
            "cluster_down_ratio": cluster_down_ratio,
            "band_contraction": band_contraction,
            "first_band_width_mean": float(first_band_width_mean) if pd.notna(first_band_width_mean) else None,
            "contraction_thresh": float(contraction_thresh) if pd.notna(contraction_thresh) else None,
            "atr_last": float(atr_last) if atr_last is not None else None,
            "atr_mean": float(atr_mean) if atr_mean is not None else None,
            "atr_expansion": atr_expansion,
            "volume_expansion": volume_expansion,
            "trigger_up": trigger_up,
            "trigger_down": trigger_down,
            "pre_up": pre_up,
            "pre_down": pre_down,
            "confirm_up": confirm_up,
            "confirm_down": confirm_down,
            "quality_ok": quality_ok,
            "cool_off_active": cool_off_active,
            "slope_first_ok": slope_first_ok,
            "slope_second_ok": slope_second_ok
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

# 추가 상수 (기존 Config 아래에 배치)
BAND_WIDTH_THRESHOLD_PCT = 0.01  # (이전 분류 로직이 필요하면 유지)
BAND_WIDTH_BASE_TF = 5
BW_AVG_PERIOD = 20               # 최근 평균 폭 계산에 사용할 최대 캔들 수

def process_timeframe(market: str, unit: int,
                      reversal_state_store: Optional[Dict[Tuple[str,int], Dict[str,Any]]] = None,
                      debug: bool=False) -> Dict[str, Any]:
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

    # ===== Bollinger 폭 % (전체 시리즈 & 마지막 / 평균) 계산 추가 =====
    # (bb_upper, bb_lower 존재 & close 유효한 경우만)
    if {'bb_upper', 'bb_lower', 'close'}.issubset(df.columns):
        df['band_width_pct'] = (df['bb_upper'] - df['bb_lower']) / df['close']
    else:
        df['band_width_pct'] = np.nan

    slope = compute_slope(df['bb_mid'])
    if df.empty:
        return {"error": "after_processing_empty"}

    last = df.iloc[-1]
    stats = compute_recent_stats(df)

    prev_state = reversal_state_store.get((market, unit)) if reversal_state_store is not None else None
    recent_info = compute_recent_window_trend_advanced(df, prev_reversal_state=prev_state, cfg=ADV_CFG)

    # 반전 상태 저장
    if reversal_state_store is not None:
        if (recent_info.get("recent_reversal") in ("UP","DOWN") and
            recent_info.get("recent_reversal_grade") != "NONE" and
            recent_info.get("recent_detail", {}).get("cool_off_active") is False):
            reversal_state_store[(market, unit)] = {
                "last_index": int(df.index[-1]),
                "direction": recent_info.get("recent_reversal")
            }

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

    # (이전) 마지막 폭 퍼센트
    last_band_width_pct = float(last['band_width_pct']) if pd.notna(last.get('band_width_pct')) else None

    # 최근 BW_AVG_PERIOD (최대 20) 평균 폭
    bw_recent = df['band_width_pct'].dropna().tail(BW_AVG_PERIOD)
    if not bw_recent.empty:
        band_width_pct_avg20 = float(bw_recent.mean())
    else:
        band_width_pct_avg20 = None

    # 최근 20개 캔들 상승/하락 평균 body
    bull_avg_body = None
    bear_avg_body = None
    if len(df) >= 2:
        recent_n = df.tail(RECENT_N).copy()
        if 'open' in recent_n.columns and 'close' in recent_n.columns:
            recent_n['body_pct_tmp'] = (recent_n['close'] - recent_n['open']) / recent_n['open']
            bull_series = recent_n.loc[recent_n['body_pct_tmp'] > 0, 'body_pct_tmp']
            bear_series = recent_n.loc[recent_n['body_pct_tmp'] < 0, 'body_pct_tmp']
            if not bull_series.empty:
                bull_avg_body = float(bull_series.mean())
            if not bear_series.empty:
                bear_avg_body = float(bear_series.mean())

    result = {
        "last_price": float(last['close']),
        "band_pos": float(band_pos) if pd.notna(band_pos) else None,
        "slope": float(slope_val) if pd.notna(slope_val) else None,
        "bb_signal": signal,
        "upper_touch": bool(band_pos >= (1 - TOLERANCE_RATIO)) if pd.notna(band_pos) else False,
        "lower_touch": bool(band_pos <= TOLERANCE_RATIO) if pd.notna(band_pos) else False,
        "expected_profit_long_pct": expected_profit_long_pct,
        "expected_profit_short_pct": expected_profit_short_pct,
        "recent_trend": recent_info.get("recent_trend"),
        "recent_trend_kr": recent_info.get("recent_trend_kr"),
        "recent_upper_touch": recent_info.get("recent_upper_touch"),
        "recent_lower_touch": recent_info.get("recent_lower_touch"),
        "recent_reversal": recent_info.get("recent_reversal"),
        "recent_reversal_kr": recent_info.get("recent_reversal_kr"),
        "recent_reversal_grade": recent_info.get("recent_reversal_grade"),
        "recent_reversal_grade_kr": recent_info.get("recent_reversal_grade_kr"),
        "recent_detail": recent_info.get("recent_detail", {}),
        # 폭 관련 (단일 & 평균)
        "band_width_pct": last_band_width_pct,
        "band_width_pct_avg20": band_width_pct_avg20,
        "recent20_bull_avg_body_pct": bull_avg_body,
        "recent20_bear_avg_body_pct": bear_avg_body
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
             reversal_state_store: Optional[Dict[Tuple[str,int], Dict[str,Any]]] = None,
             debug: bool=False) -> List[Dict[str, Any]]:
    if markets is None:
        markets = get_top_markets()
    results = []
    for m in markets:
        tf_result = {}
        for tf in timeframes:
            time.sleep(REQUEST_SLEEP)
            data = process_timeframe(m, tf, reversal_state_store=reversal_state_store, debug=debug)
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
                             no_space_between: bool=False,
                             base_width_tf: int = BAND_WIDTH_BASE_TF) -> str:
    """
    예: KRW-BTC 5분(상승,반전↑:강) 15분(중립) 30분(하락) 폭:0.83%(20평균:0.95%)
    """
    market = item.get("market", "UNKNOWN")
    parts = [market]
    sep = "" if no_space_between else " "

    for tf in timeframes:
        key = f"{tf}m"
        info = item['timeframes'].get(key, {})
        trend_kr = info.get("recent_trend_kr", "중립")

        label_core = f"{tf}분({trend_kr}"
        if tf in include_extras_tf:
            rev = info.get("recent_reversal")
            grade_kr = info.get("recent_reversal_grade_kr")
            if rev in ("UP","DOWN") and grade_kr not in ("없음", None):
                arrow = "↑" if rev == "UP" else "↓"
                label_core += f",반전{arrow}:{grade_kr}"
        label_core += ")"
        parts.append(label_core)

    # 폭 표시 (기준 타임프레임)
    base_key = f"{base_width_tf}m"
    base_info = item['timeframes'].get(base_key, {})
    bw_cur = base_info.get("band_width_pct")
    bw_avg = base_info.get("band_width_pct_avg20")

    if bw_cur is not None:
        cur_str = f"{bw_cur * 100:.2f}%"
    else:
        cur_str = "N/A"
    if bw_avg is not None:
        avg_str = f"{bw_avg * 100:.2f}%"
    else:
        avg_str = "N/A"

    # 필요하다면 분류 태그도 덧붙이고 싶을 경우:
    # classification = ""
    # if bw_cur is not None:
    #     classification = " (<1%)" if bw_cur < BAND_WIDTH_THRESHOLD_PCT else " (≥1%)"
    # width_tag = f"폭:{cur_str}{classification}(20평균:{avg_str})"

    width_tag = f"폭:{cur_str}(20평균:{avg_str})"
    parts.append(width_tag)

    return sep.join(parts)

# ==============================
# Scheduling Helpers
# ==============================
def align_to_even_minute(ts: pd.Timestamp, interval: int = INTERVAL_MINUTES) -> pd.Timestamp:
    base = ts.replace(second=0, microsecond=0)
    if ts.second == 0 and (ts.minute % interval == 0):
        return base
    minute = ts.minute
    next_min = minute + (interval - (minute % interval))
    if next_min >= 60:
        base = base + pd.Timedelta(hours=1)
        base = base.replace(minute=0)
    else:
        base = base.replace(minute=next_min)
    return base

# ==============================
# Main Loop
# ==============================
if __name__ == "__main__":
    try:
        run_counter = 0
        next_run = align_to_even_minute(now_kst(), interval=INTERVAL_MINUTES)
        print(f"[INFO] First scheduled run at {next_run} (KST)")

        while True:
            now = now_kst()
            if now < next_run:
                sleep_sec = (next_run - now).total_seconds()
                if sleep_sec > 0:
                    time.sleep(sleep_sec)

            run_time = next_run
            start_actual = now_kst()

            markets = get_top_markets()
            if not markets:
                print(f"[{start_actual}] No markets retrieved.")
            else:
                scan = run_scan(markets[:30],
                                reversal_state_store=REVERSAL_STATE_STORE,
                                debug=False)
                scan_sorted = sorted(scan, key=lambda x: x['market'])
                header = f"=== 고급 반전 (window={ADV_CFG['window']}) 5/15분 확장 Scheduled:{run_time} Started:{start_actual} ==="
                print(header)
                for item in scan_sorted:
                    print(format_recent_trend_line(item))

            run_counter += 1
            if (MAX_RUNS is not None) and (run_counter >= MAX_RUNS):
                print(f"[INFO] Reached MAX_RUNS={MAX_RUNS}. Exiting.")
                break

            next_run = next_run + pd.Timedelta(minutes=INTERVAL_MINUTES)
            now_after = now_kst()
            if now_after > next_run:
                missed = int((now_after - next_run).total_seconds() // (INTERVAL_MINUTES * 60)) + 1
                next_run = next_run + pd.Timedelta(minutes=INTERVAL_MINUTES * missed)
                print(f"[WARN] Delayed execution detected. Skipping {missed} interval(s). Next run reset to {next_run}")

    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt received. Stopping scheduler.")