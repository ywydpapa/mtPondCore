import requests
import pandas as pd
import time
import random
from typing import Optional, List
from datetime import datetime, timedelta

# -------------------- 설정 --------------------
TOP30_URL = "http://ywydpapa.iptime.org:8000/api/top30coins"
MINUTES_URL_FMT = "https://api.upbit.com/v1/candles/minutes/{unit}"

# 다중 타임프레임 (필요시 조정 가능)
TIMEFRAMES = [5, 15, 30]    # 분 단위

# 지표 기본 파라미터
BB_PERIOD = 20
BB_K = 2
HHLL_PERIOD = 20
RSI_PERIOD = 14
ATR_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# 캔들 수 (너무 크게 하면 API 호출 부담 증가)
CANDLE_COUNT = 120
BASE_SLEEP = 0.18     # API 호출 간 기본 대기 (필요 시 조정)
MAX_RETRY = 5

# ------------------------------------------------
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
  # 초 단위 잔여 요청이 적으면 추가 대기
  if isinstance(sec_left, int) and sec_left <= 1:
    time.sleep(0.8)
  elif isinstance(sec_left, int) and sec_left <= 3:
    time.sleep(0.4)
  # 분 단위 마진 줄어들면 추가 살짝 대기
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
        wait = (2 ** (attempt - 1)) * 0.5
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
      wait = (2 ** (attempt - 1)) * 0.4
      print(f"[예외:{e}] 재시도 대기 {wait:.2f}s (attempt {attempt})")
      time.sleep(wait)

def fetch_top30_markets():
  r = request_with_retry(TOP30_URL)
  data = r.json()
  markets = data.get("markets", [])
  return markets[:30]

def fetch_upbit_minutes(market: str, unit: int, count: int = CANDLE_COUNT) -> pd.DataFrame:
  """
  Upbit 분봉 (unit 분) 캔들 수집 (시간 오름차순 반환)
  """
  url = MINUTES_URL_FMT.format(unit=unit)
  params = {"market": market, "count": count}
  r = request_with_retry(url, params=params)
  data = r.json()
  rows = []
  # Upbit API는 최신 → 과거 순으로 주므로 reverse
  for item in reversed(data):
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
  if df.empty:
    return df
  df["dt"] = pd.to_datetime(df["dt"])
  df.set_index("dt", inplace=True)
  return df

def _add_trend10(df: pd.DataFrame, window: int = 10,
                 doji_char: str = "-") -> pd.DataFrame:
  out = df.copy()
  if not {"open", "close"}.issubset(out.columns):
    out["Trend10"] = None
    return out
  mid = (out["open"] + out["close"]) / 2.0
  out["_mid"] = mid
  patterns = [None] * len(out)
  for i in range(window - 1, len(out)):
    win_slice = out.iloc[i - window + 1: i + 1]
    avg_mid = win_slice["_mid"].mean()
    chars = []
    for _, row in win_slice.iterrows():
      o = row["open"]; c = row["close"]
      m = (o + c) / 2.0
      high_flag = m >= avg_mid
      if c > o:
        chars.append("U" if high_flag else "u")
      elif c < o:
        chars.append("D" if high_flag else "d")
      else:
        chars.append(doji_char)
    patterns[i] = "".join(chars)
  out["Trend10"] = patterns
  out.drop(columns=["_mid"], inplace=True, errors="ignore")
  return out

def add_indicators(df: pd.DataFrame,
                   bb_period=BB_PERIOD,
                   bb_k=BB_K,
                   hhll_period=HHLL_PERIOD,
                   rsi_period=RSI_PERIOD,
                   atr_period=ATR_PERIOD,
                   macd_fast=MACD_FAST,
                   macd_slow=MACD_SLOW,
                   macd_signal=MACD_SIGNAL):
  out = df.copy()

  # Bollinger
  if len(out) >= bb_period:
    out["MA"] = out["close"].rolling(bb_period).mean()
    out["STD"] = out["close"].rolling(bb_period).std(ddof=0)
    out["Upper"] = out["MA"] + bb_k * out["STD"]
    out["Lower"] = out["MA"] - bb_k * out["STD"]
    denom = bb_k * out["STD"]
    out["BB_Pos"] = (out["close"] - out["MA"]) / denom * 100
    out.loc[(denom == 0) | denom.isna(), "BB_Pos"] = 0.0
    bw_denom = out["MA"]
    out["BandWidth"] = (out["Upper"] - out["Lower"]) / bw_denom * 100
    out.loc[(bw_denom == 0) | bw_denom.isna(), "BandWidth"] = 0.0
  else:
    out[["MA", "STD", "Upper", "Lower", "BB_Pos", "BandWidth"]] = None

  # HH/LL
  if all(c in out.columns for c in ["high", "low"]):
    out["HH20"] = out["high"].rolling(hhll_period).max()
    out["LL20"] = out["low"].rolling(hhll_period).min()
  else:
    out["HH20"] = out["close"].rolling(hhll_period).max()
    out["LL20"] = out["close"].rolling(hhll_period).min()
  span = out["HH20"] - out["LL20"]
  out["Range_Pos20"] = (out["close"] - out["LL20"]) / span * 100
  out.loc[(span == 0) | span.isna(), "Range_Pos20"] = 0.0

  # RSI (Wilder 방식 유사: ewm)
  delta = out["close"].diff()
  gain = delta.clip(lower=0)
  loss = -delta.clip(upper=0)
  avg_gain = gain.ewm(alpha=1 / rsi_period, adjust=False).mean()
  avg_loss = loss.ewm(alpha=1 / rsi_period, adjust=False).mean()
  rs = avg_gain / avg_loss
  out["RSI"] = 100 - (100 / (1 + rs))
  out.loc[avg_loss == 0, "RSI"] = 100
  out.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0

  # ATR (Wilder)
  if all(c in out.columns for c in ["high", "low"]):
    prev_close = out["close"].shift(1)
    tr_components = pd.concat([
      (out["high"] - out["low"]),
      (out["high"] - prev_close).abs(),
      (out["low"] - prev_close).abs()
    ], axis=1)
    tr = tr_components.max(axis=1)
    out["ATR"] = tr.ewm(alpha=1 / atr_period, adjust=False).mean()
  else:
    out["ATR"] = None

  # MACD
  ema_fast = out["close"].ewm(span=macd_fast, adjust=False).mean()
  ema_slow = out["close"].ewm(span=macd_slow, adjust=False).mean()
  out["MACD"] = ema_fast - ema_slow
  out["MACD_Signal"] = out["MACD"].ewm(span=macd_signal, adjust=False).mean()
  out["MACD_Hist"] = out["MACD"] - out["MACD_Signal"]

  # Trend10
  out = _add_trend10(out, window=10)
  return out

def collect_all():
  """
  모든 market × timeframe 조합 캔들 + 지표 수집 후 결합 DataFrame 반환
  """
  markets = fetch_top30_markets()
  random.shuffle(markets)  # 호출 패턴 분산
  frames = []

  for m in markets:
    for tf in TIMEFRAMES:
      try:
        df = fetch_upbit_minutes(m, tf)
        if df.empty:
          print(f"[정보] {m} {tf}m 빈 DataFrame")
          continue
        df_ind = add_indicators(df)
        df_ind["timeframe"] = f"{tf}m"
        frames.append(df_ind)
      except Exception as e:
        print(f"[경고] {m} {tf}m 수집 실패: {e}")
      time.sleep(BASE_SLEEP)  # 속도 조절

  if not frames:
    return pd.DataFrame()
  all_df = pd.concat(frames).sort_values(["timeframe", "market", "timestamp"])
  return all_df

def print_snapshot(all_df: pd.DataFrame):
  """
  각 timeframe별 마지막 캔들 지표 스냅샷 출력
  """
  if all_df.empty:
    print("데이터 없음")
    return

  last = (all_df
          .groupby(["timeframe", "market"])
          .tail(1)
          .copy())

  # 정렬 키: timeframe → BB_Pos 내림차순
  for tf in sorted(last["timeframe"].unique(),
                   key=lambda x: int(x.replace("m", ""))):
    sub = last[last["timeframe"] == tf].sort_values("BB_Pos", ascending=False)
    cols = [
      "market", "close",
      "MA", "Upper", "Lower", "BB_Pos", "BandWidth",
      "HH20", "LL20", "Range_Pos20",
      "RSI", "ATR",
      "MACD", "MACD_Signal", "MACD_Hist",
      "Trend10"
    ]
    existing = [c for c in cols if c in sub.columns]
    print(f"\n=== [{tf}] Snapshot ({len(sub)} symbols) ===")
    print(sub[existing].to_string())
    print("-" * 60)

def next_even_3min_boundary(now: datetime) -> datetime:
  candidate = now.replace(second=0, microsecond=0)
  if now.second != 0 or now.microsecond != 0:
    candidate += timedelta(minutes=1)
  if candidate.minute % 3 != 0:
    candidate += timedelta(minutes=1)
  return candidate

def mainloop():
  all_df = collect_all()
  if all_df.empty:
    print("수집 실패")
  else:
    print_snapshot(all_df)

if __name__ == "__main__":
  # 초기 정렬: 3분 경계
  first = next_even_3min_boundary(datetime.now())
  wait = (first - datetime.now()).total_seconds()
  if wait > 0:
    time.sleep(wait)
  scheduled = first

  while True:
    now = datetime.now()
    if now < scheduled:
      time.sleep((scheduled - now).total_seconds())

    mainloop()

    scheduled += timedelta(minutes=3)
    now = datetime.now()
    while scheduled <= now:
      scheduled += timedelta(minutes=3)

    sleep_sec = (scheduled - datetime.now()).total_seconds()
    if sleep_sec > 0:
      time.sleep(sleep_sec)
