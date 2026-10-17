"""
永豐金 Shioaji API 封裝。

- 整個行程只維持一條已登入的連線（文件規定每個行程一個連線），所有呼叫以鎖序列化，
  讓 Flask 多執行緒與 ThreadPoolExecutor 可以安全共用。
- 即時報價用 snapshots（查一次回一次），歷史日 K 用 kbars（回傳 1 分 K，本地聚合成日 K）。
- 日 K 以檔案快取，每次只補抓快取之後的區間，避免撞到 kbars 次數與每日流量限制。
"""

import datetime
import json
import os
import sys
import threading
import time

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

try:
    import shioaji as sj
except Exception as _import_err:  # pragma: no cover - 只在未安裝時發生
    sj = None
    _IMPORT_ERROR = _import_err
else:
    _IMPORT_ERROR = None


CACHE_DIR_DAILY = "/tmp/stock_data_cache/sj_daily"
os.makedirs(CACHE_DIR_DAILY, exist_ok=True)

SNAPSHOT_TTL = 15            # 秒；個股快照的記憶體快取
MARKET_SNAPSHOT_TTL = 60     # 秒；全市場快照的記憶體快取
META_TTL = 6 * 3600          # 秒；商品檔（名稱、昨收、交易所）
CODES_TTL = 24 * 3600        # 秒；全市場股票代碼清單
SNAPSHOT_BATCH = 500         # snapshots 單次上限
KBARS_WINDOW_DAYS = 30       # kbars 單次查詢區間上限
DAILY_CLOSE_HOUR = 14        # 台北時間 14:00 之後才把「今天」的日 K 寫進檔案快取

TZ_TAIPEI = datetime.timezone(datetime.timedelta(hours=8))
LISTED_EXCHANGES = ("TSE", "OTC")   # 上市 / 上櫃；OES 為興櫃，不納入排行


class ShioajiUnavailable(Exception):
    """未設定金鑰、登入失敗或 API 呼叫失敗。"""


_lock = threading.RLock()
_api = None
_login_failed_at = 0.0
_LOGIN_RETRY_INTERVAL = 60

_meta_cache = {}        # code -> (ts, meta dict | None)
_snapshot_cache = {}    # code -> (ts, Snapshot)
_market_cache = {"ts": 0.0, "snapshots": None}
_codes_cache = {"ts": 0.0, "codes": None}
_daily_locks = {}


# ---------------------------------------------------------------------------
# 連線
# ---------------------------------------------------------------------------

def _config():
    api_key = os.getenv("SHIOAJI_API_KEY", "").strip()
    secret_key = os.getenv("SHIOAJI_SECRET_KEY", "").strip()
    simulation = os.getenv("SHIOAJI_SIMULATION", "true").strip().lower() != "false"
    return api_key, secret_key, simulation


def is_configured():
    api_key, secret_key, _ = _config()
    return bool(api_key and secret_key)


def get_api():
    """回傳已登入的 Shioaji 實例；第一次呼叫時才登入。"""
    global _api, _login_failed_at
    if sj is None:
        raise ShioajiUnavailable(f"shioaji 套件未安裝：{_IMPORT_ERROR}")
    with _lock:
        if _api is not None:
            return _api
        if time.time() - _login_failed_at < _LOGIN_RETRY_INTERVAL:
            raise ShioajiUnavailable("剛才登入永豐 API 失敗，請稍後再試。")
        api_key, secret_key, simulation = _config()
        if not api_key or not secret_key:
            raise ShioajiUnavailable("尚未設定 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY，請在 .env 填入。")
        try:
            api = sj.Shioaji(simulation=simulation)
            api.login(api_key=api_key, secret_key=secret_key)
        except Exception as e:
            _login_failed_at = time.time()
            raise ShioajiUnavailable(f"登入永豐 API 失敗：{e}") from e
        _api = api
        print(f"Shioaji 已登入（{'模擬' if simulation else '正式'}模式）", file=sys.stderr)
        return _api


def logout():
    global _api
    with _lock:
        if _api is None:
            return
        try:
            _api.logout()
        except Exception:
            pass
        _api = None


def _call(fn, *args, **kwargs):
    """所有 API 呼叫都經過這裡：持鎖、統一把例外轉成 ShioajiUnavailable。"""
    api = get_api()
    with _lock:
        try:
            return fn(api, *args, **kwargs)
        except ShioajiUnavailable:
            raise
        except Exception as e:
            raise ShioajiUnavailable(str(e)) from e


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def ns_to_datetime(ts_ns):
    """
    Shioaji 的 ts 為奈秒整數，但其值已是「台北時間」直接編成 epoch（官方範例直接 cast 成
    naive datetime 即顯示 14:30），所以這裡用 UTC 解讀、不再加 8 小時。
    """
    try:
        seconds = int(ts_ns) / 1_000_000_000
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc).replace(tzinfo=None)


def _now_taipei():
    return datetime.datetime.now(tz=TZ_TAIPEI).replace(tzinfo=None)


def _get_contract(api, code):
    return api.contracts.get(code)


# ---------------------------------------------------------------------------
# 商品檔
# ---------------------------------------------------------------------------

def get_stock_meta(code):
    """
    回傳 {code, name, exchange, is_listed, reference, update_date}；查無代碼回 None。
    """
    code = str(code).strip().upper()
    now = time.time()
    cached = _meta_cache.get(code)
    if cached and now - cached[0] < META_TTL:
        return cached[1]

    def _fetch(api):
        contract = api.contracts.get(code)
        if contract is None or getattr(contract, "security_type", None) != "STK":
            return None
        info = api.contracts.info(contract)
        exchange = str(getattr(info, "exchange", None) or getattr(contract, "exchange", "") or "")
        update_date = getattr(info, "update_date", None)
        return {
            "code": code,
            "name": (getattr(info, "name", "") or "").strip(),
            "exchange": exchange,
            "is_listed": exchange == "TSE",
            "reference": float(getattr(info, "reference", 0) or 0) or None,
            "update_date": update_date.isoformat() if hasattr(update_date, "isoformat") else None,
        }

    meta = _call(_fetch)
    _meta_cache[code] = (now, meta)
    return meta


def get_all_stock_codes():
    """上市 + 上櫃全部股票（含 ETF，不含權證與興櫃）的 [(code, exchange)]，快取一天。"""
    now = time.time()
    if _codes_cache["codes"] is not None and now - _codes_cache["ts"] < CODES_TTL:
        return list(_codes_cache["codes"])

    def _fetch(api):
        items = []
        for c in api.contracts.list(sj.SecurityType.Stock):
            exchange = str(getattr(c, "exchange", "") or "")
            if exchange in LISTED_EXCHANGES:
                items.append((str(c.code), exchange))
        return items

    codes = _call(_fetch)
    _codes_cache["codes"] = codes
    _codes_cache["ts"] = now
    return list(codes)


# ---------------------------------------------------------------------------
# 快照
# ---------------------------------------------------------------------------

def _snapshots_for_codes(api, codes):
    contracts = []
    for code in codes:
        c = api.contracts.get(code)
        if c is not None:
            contracts.append(c)
    result = {}
    for start in range(0, len(contracts), SNAPSHOT_BATCH):
        batch = contracts[start:start + SNAPSHOT_BATCH]
        for snap in api.snapshots(batch) or []:
            result[str(snap.code)] = snap
    return result


def get_snapshots(codes):
    """回傳 {code: Snapshot}；15 秒內重複查同一檔直接回快取。"""
    codes = [str(c).strip().upper() for c in codes if str(c).strip()]
    now = time.time()
    result = {}
    missing = []
    for code in dict.fromkeys(codes):
        cached = _snapshot_cache.get(code)
        if cached and now - cached[0] < SNAPSHOT_TTL:
            result[code] = cached[1]
        else:
            missing.append(code)

    if missing:
        fetched = _call(_snapshots_for_codes, missing)
        for code, snap in fetched.items():
            _snapshot_cache[code] = (now, snap)
            result[code] = snap
    return result


def get_market_snapshots(force=False):
    """
    全市場（上市 + 上櫃）快照，分批 500 檔查詢；60 秒內重複呼叫直接回快取。
    回傳 (snapshots dict, fetched_at epoch, from_cache)。
    """
    now = time.time()
    with _lock:
        cached = _market_cache["snapshots"]
        if cached is not None and not force and now - _market_cache["ts"] < MARKET_SNAPSHOT_TTL:
            return cached, _market_cache["ts"], True

        codes = [code for code, _ in get_all_stock_codes()]
        snapshots = _call(_snapshots_for_codes, codes)
        fetched_at = time.time()
        _market_cache["snapshots"] = snapshots
        _market_cache["ts"] = fetched_at
        for code, snap in snapshots.items():
            _snapshot_cache[code] = (fetched_at, snap)
        return snapshots, fetched_at, False


def snapshot_to_dict(snap):
    return {
        "code": str(snap.code),
        "exchange": str(snap.exchange),
        "datetime": ns_to_datetime(snap.ts),
        "open": float(snap.open or 0),
        "high": float(snap.high or 0),
        "low": float(snap.low or 0),
        "close": float(snap.close or 0),
        "change_price": float(snap.change_price or 0),
        "change_rate": float(snap.change_rate or 0),
        "total_volume": int(snap.total_volume or 0),
        "yesterday_volume": float(snap.yesterday_volume or 0),
        "volume_ratio": float(snap.volume_ratio or 0),
        "total_amount": float(snap.total_amount or 0),
    }


# ---------------------------------------------------------------------------
# 歷史日 K
# ---------------------------------------------------------------------------

def _daily_lock(code):
    with _lock:
        lock = _daily_locks.get(code)
        if lock is None:
            lock = _daily_locks[code] = threading.Lock()
        return lock


def _daily_cache_path(code):
    return os.path.join(CACHE_DIR_DAILY, f"{code}.json")


def _read_daily_cache(code):
    path = _daily_cache_path(code)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("days") or {}
    except Exception:
        return {}


def _write_daily_cache(code, days):
    try:
        with open(_daily_cache_path(code), "w", encoding="utf-8") as f:
            json.dump({"code": code, "updated": _now_taipei().isoformat(), "days": days}, f, ensure_ascii=False)
    except Exception as e:
        print(f"提醒：寫入日 K 快取 {code} 失敗。({e})", file=sys.stderr)


def _fetch_kbars_as_daily(api, contract, start, end):
    """
    查一段 kbars（1 分 K），聚合成 {YYYY-MM-DD: {o,h,l,c,v}}。
    Volume 單位為張，這裡乘 1000 轉成股數，與原本 Yahoo 的欄位單位一致。
    """
    kbars = api.kbars(contract, start=start.isoformat(), end=end.isoformat())
    raw = kbars.dict() if hasattr(kbars, "dict") else kbars.model_dump()
    ts = list(raw.get("ts") or [])
    if not ts:
        return {}
    df = pd.DataFrame({
        "ts": ts,
        "Open": raw.get("Open"),
        "High": raw.get("High"),
        "Low": raw.get("Low"),
        "Close": raw.get("Close"),
        "Volume": raw.get("Volume"),
    })
    # ts 已是台北時間編成的 epoch（見 ns_to_datetime），直接當 naive datetime 用
    df["dt"] = pd.to_datetime(df["ts"].astype("int64"), unit="ns")
    df["date"] = df["dt"].dt.strftime("%Y-%m-%d")
    df = df.sort_values("dt")
    grouped = df.groupby("date").agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last"),
        Volume=("Volume", "sum"),
    )
    days = {}
    for date_str, row in grouped.iterrows():
        days[date_str] = {
            "o": float(row["Open"]),
            "h": float(row["High"]),
            "l": float(row["Low"]),
            "c": float(row["Close"]),
            "v": float(row["Volume"]) * 1000.0,
        }
    return days


def _days_to_dataframe(days):
    if not days:
        return None
    rows = []
    for date_str in sorted(days):
        d = days[date_str]
        rows.append({
            "Date": pd.Timestamp(date_str),
            "Open": d["o"], "High": d["h"], "Low": d["l"], "Close": d["c"], "Volume": d["v"],
        })
    df = pd.DataFrame(rows).set_index("Date")
    return df


def get_daily_history(code, months=6):
    """
    回傳近 months 個月的日 K DataFrame（index 為日期、欄位 Open/High/Low/Close/Volume，Volume 為股數），
    無資料回 None。盤中「今天」那一天是進行中的聚合，之後請用 snapshot 覆蓋。
    """
    code = str(code).strip().upper()
    today = _now_taipei().date()
    earliest = today - datetime.timedelta(days=int(months * 31))

    with _daily_lock(code):
        days = _read_daily_cache(code)
        # 只保留需要的範圍
        days = {d: v for d, v in days.items() if d >= earliest.isoformat()}

        cached_dates = sorted(days)
        if cached_dates:
            start = datetime.date.fromisoformat(cached_dates[-1]) + datetime.timedelta(days=1)
        else:
            start = earliest
        if start > today:
            start = today

        def _fetch(api):
            contract = api.contracts.get(code)
            if contract is None:
                return None
            fetched = {}
            cur = start
            while cur <= today:
                end = min(cur + datetime.timedelta(days=KBARS_WINDOW_DAYS - 1), today)
                fetched.update(_fetch_kbars_as_daily(api, contract, cur, end))
                cur = end + datetime.timedelta(days=1)
            return fetched

        fetched = _call(_fetch)
        if fetched is None:
            return None
        days.update(fetched)

        # 今天收盤前不把今天寫進檔案，避免快取到不完整的日 K
        persist = dict(days)
        if _now_taipei().hour < DAILY_CLOSE_HOUR:
            persist.pop(today.isoformat(), None)
        _write_daily_cache(code, persist)

    return _days_to_dataframe(days)


def merge_snapshot_into_daily(df, snap):
    """用 snapshot 的開高低收量覆蓋（或補上）當日那一列。"""
    if snap is None:
        return df
    info = snapshot_to_dict(snap)
    if info["datetime"] is None or info["close"] <= 0:
        return df
    snap_date = pd.Timestamp(info["datetime"].date())
    row = {
        "Open": info["open"] or info["close"],
        "High": info["high"] or info["close"],
        "Low": info["low"] or info["close"],
        "Close": info["close"],
        "Volume": info["total_volume"] * 1000.0,
    }
    if df is None or df.empty:
        return pd.DataFrame([row], index=pd.DatetimeIndex([snap_date], name="Date"))
    df = df.copy()
    if snap_date in df.index:
        for k, v in row.items():
            df.loc[snap_date, k] = v
    elif snap_date > df.index[-1]:
        df.loc[snap_date] = row
    return df.sort_index()
