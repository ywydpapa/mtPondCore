import requests
import pandas as pd
import time
import random
import math
from typing import Optional

TOP30_URL = "http://ywydpapa.iptime.org:8000/api/top30coins"
UPBIT_5M_URL = "https://api.upbit.com/v1/candles/minutes/5"

# 기본 파라미터
BB_PERIOD = 20
BB_K = 2
HHLL_PERIOD = 20
RSI_PERIOD = 14
ATR_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
CANDLE_COUNT = 200      # 필요 시 120 등 감소 가능
BASE_SLEEP = 0.18       # 호출 간 기본 대기
MAX_RETRY = 5

def parse_remaining_req(headers):
    rem = headers.get("Remaining-Req")
    if not rem:
        return None
    parts = rem.replace(" ", "").split(";")
    info = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            info[k] = v
    for key in ["min", "sec"]:
        if key in info and info[key].isdigit():
            info[key] = int(info[key])
    return info

def dynamic_throttle(headers):
    info = parse_remaining_req(headers)
    if not info:
        return
    sec_left = info.get("sec")
    min_left = info.get("min")
    if isinstance(sec_left, int) and sec_left <= 1:
        time.sleep(0.8)
    elif isinstance(sec_left, int) and sec_left <= 3:
        time.sleep(0.4)
    if isinstance(min_left, int) and min_left < 20:
        time.sleep(0.2)

def request_with_retry(url, params=None, timeout=5):
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                if attempt > MAX_RETRY:
                    raise RuntimeError(f"429 초과: 재시도 한도 {MAX_RETRY}")
                wait = (2 ** (attempt - 1)) * 0.5 + random.uniform(0, 0.3)
                print(f"[429] 백오프 {wait:.2f}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                if attempt > MAX_RETRY:
                    raise RuntimeError(f"서버 오류 지속 {resp.status_code}")
                wait = (2 ** (attempt - 1)) * 0.4
                print(f"[{resp.status_code}] 서버 재시도 대기 {wait:.2f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            dynamic_throttle(resp.headers)
            return resp
        except requests.RequestException as e:
            if attempt > MAX_RETRY:
                raise
            wait = (2 ** (attempt - 1)) * 0.4 + random.uniform(0, 0.25)
            print(f"[예외:{e}] 재시도 대기 {wait:.2f}s (attempt {attempt})")
            time.sleep(wait)

def fetch_top30_markets():
    r = request_with_retry(TOP30_URL)
    data = r.json()
    markets = data.get("markets", [])
    return markets[:30]

def fetch_upbit_5m(market, count=CANDLE_COUNT):
    params = {"market": market, "count": count}
    r = request_with_retry(UPBIT_5M_URL, params=params)
    data = r.json()
    rows = []
    for item in reversed(data):  # 시간 오름차순
        rows.append({
            "market": market,
            "timestamp": item["timestamp"],
            "dt": item["candle_date_time_kst"],
            "open": item["opening_price"],
            "high": item["high_price"],
            "low": item["low_price"],
            "close": item["trade_price"],
            "volume": item["candle_acc_trade_volume"]
        })
    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["dt"])
    df.set_index("dt", inplace=True)
    return df

def add_indicators(df,
                   bb_period=BB_PERIOD,
                   bb_k=BB_K,
                   hhll_period=HHLL_PERIOD,
                   rsi_period=RSI_PERIOD,
                   use_high_low=True,
                   atr_period=ATR_PERIOD,
                   macd_fast=MACD_FAST,
                   macd_slow=MACD_SLOW,
                   macd_signal=MACD_SIGNAL):
    """
    df: close (+ high, low) 포함 DataFrame (시간 오름차순 인덱스)
    추가 지표: Bollinger, BandWidth, HH/LL, Range_Pos20, RSI, ATR, MACD
    """
    out = df.copy()

    # ---------- Bollinger ----------
    if len(out) >= bb_period:
        out["MA"] = out["close"].rolling(bb_period).mean()
        out["STD"] = out["close"].rolling(bb_period).std(ddof=0)
        out["Upper"] = out["MA"] + bb_k * out["STD"]
        out["Lower"] = out["MA"] - bb_k * out["STD"]
        denom = bb_k * out["STD"]
        out["BB_Pos"] = (out["close"] - out["MA"]) / denom * 100
        out.loc[(denom == 0) | denom.isna(), "BB_Pos"] = 0.0
        # BandWidth
        bw_denom = out["MA"]
        out["BandWidth"] = (out["Upper"] - out["Lower"]) / bw_denom * 100
        out.loc[(bw_denom == 0) | bw_denom.isna(), "BandWidth"] = 0.0
    else:
        out[["MA", "STD", "Upper", "Lower", "BB_Pos", "BandWidth"]] = None

    # ---------- HH / LL ----------
    if use_high_low and all(c in out.columns for c in ["high", "low"]):
        out["HH20"] = out["high"].rolling(hhll_period).max()
        out["LL20"] = out["low"].rolling(hhll_period).min()
    else:
        out["HH20"] = out["close"].rolling(hhll_period).max()
        out["LL20"] = out["close"].rolling(hhll_period).min()

    span = out["HH20"] - out["LL20"]
    out["Range_Pos20"] = (out["close"] - out["LL20"]) / span * 100
    out.loc[(span == 0) | span.isna(), "Range_Pos20"] = 0.0

    # ---------- RSI ----------
    delta = out["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss
    out["RSI"] = 100 - (100 / (1 + rs))
    out.loc[avg_loss == 0, "RSI"] = 100
    out.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0

    # ---------- ATR ----------
    # True Range 구성
    if all(c in out.columns for c in ["high", "low"]):
        prev_close = out["close"].shift(1)
        tr_components = pd.concat([
            (out["high"] - out["low"]),
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs()
        ], axis=1)
        tr = tr_components.max(axis=1)
        out["ATR"] = tr.ewm(alpha=1/atr_period, adjust=False).mean()
    else:
        out["ATR"] = None

    # ---------- MACD ----------
    ema_fast = out["close"].ewm(span=macd_fast, adjust=False).mean()
    ema_slow = out["close"].ewm(span=macd_slow, adjust=False).mean()
    out["MACD"] = ema_fast - ema_slow
    out["MACD_Signal"] = out["MACD"].ewm(span=macd_signal, adjust=False).mean()
    out["MACD_Hist"] = out["MACD"] - out["MACD_Signal"]

    return out

def collect_all():
    markets = fetch_top30_markets()
    random.shuffle(markets)
    frames = []
    for idx, m in enumerate(markets, 1):
        try:
            df = fetch_upbit_5m(m)
            if df.empty:
                print(f"[정보] {m} 빈 DataFrame")
                continue
            df_ind = add_indicators(df)
            frames.append(df_ind)
        except Exception as e:
            print(f"[경고] {m} 수집 실패: {e}")
        time.sleep(BASE_SLEEP)
    if not frames:
        return pd.DataFrame()
    all_df = pd.concat(frames).sort_values(["market", "timestamp"])
    return all_df

if __name__ == "__main__":
    all_df = collect_all()
    if all_df.empty:
        print("수집 실패")
    else:
        last = (all_df
                .groupby("market")
                .tail(1)
                .sort_values("BB_Pos", ascending=False))

        # 출력 컬럼 확장 (새 지표 포함)
        cols = [
            "market","close",
            "MA","Upper","Lower","BB_Pos","BandWidth",
            "HH20","LL20","Range_Pos20",
            "RSI","ATR",
            "MACD","MACD_Signal","MACD_Hist"
        ]
        existing_cols = [c for c in cols if c in last.columns]
        print(last[existing_cols].to_string())
