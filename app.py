import sys
import time
import datetime
import requests
import re
import os
import json
import copy
import threading
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, request, render_template
from concurrent.futures import ThreadPoolExecutor

import sj_market
from sj_market import ShioajiUnavailable

# 支援的顯示字元編碼，Windows 命令列輸出若出現編碼錯誤時可用
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None

CACHE_DIR_TWSE = "/tmp/stock_data_cache/twse_t86"
CACHE_DIR_TPEX = "/tmp/stock_data_cache/tpex_3insti"
os.makedirs(CACHE_DIR_TWSE, exist_ok=True)
os.makedirs(CACHE_DIR_TPEX, exist_ok=True)

_cache_lock = threading.Lock()
_date_locks = {}
_market_chip_index = {}  # (market, date_str) -> {stock_code: {foreign_net, trust_net}}
_stock_chip_cache = {}   # (stock_code, dates_tuple, is_listed) -> {date_str: {foreign_net, trust_net}}
_analysis_cache = {}     # stock_code -> last analyze payload (for delayed chip prompts)

def fetch_twse_t86_data(date_str):
    """
    抓取並快取證交所三大法人買賣明細 (T86)
    """
    cache_path = os.path.join(CACHE_DIR_TWSE, f"{date_str}.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get('data'):
                return cached
        except Exception:
            pass
            
    url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        if data.get('stat') == 'OK' and data.get('data'):
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data
    except Exception as e:
        print(f"提醒：無法獲取 TWSE {date_str} 籌碼數據。({e})", file=sys.stderr)
    return None

def fetch_tpex_3insti_data(date_str):
    """
    抓取並快取櫃買中心三大法人買賣明細
    """
    try:
        dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        roc_year = dt.year - 1911
        roc_date_str = f"{roc_year}/{dt.month:02d}/{dt.day:02d}"
    except Exception:
        return None
        
    cache_path = os.path.join(CACHE_DIR_TPEX, f"{date_str}.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            tables = cached.get('tables') or []
            if tables and tables[0].get('data'):
                return cached
        except Exception:
            pass
            
    url = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={roc_date_str}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        tables = data.get('tables', [])
        if tables and len(tables) > 0 and tables[0].get('data'):
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data
    except Exception as e:
        print(f"提醒：無法獲取 TPEx {date_str} ({roc_date_str}) 籌碼數據。({e})", file=sys.stderr)
    return None

def format_net_lots(val_shares):
    lots = round(val_shares / 1000, 1)
    if lots > 0:
        return f"+{lots:g}"
    elif lots < 0:
        return f"{lots:g}"
    else:
        return "0"

_EMPTY_CHIP = {"foreign_net": "-", "trust_net": "-"}


def _parse_share_int(value):
    return int(str(value).replace(',', ''))


def _index_twse_t86(data):
    index = {}
    for row in data.get('data') or []:
        try:
            code = str(row[0]).strip()
            foreign_net = _parse_share_int(row[4])
            foreign_dealer_net = _parse_share_int(row[7])
            trust_net = _parse_share_int(row[10])
            index[code] = {
                "foreign_net": format_net_lots(foreign_net + foreign_dealer_net),
                "trust_net": format_net_lots(trust_net)
            }
        except (IndexError, TypeError, ValueError):
            continue
    return index


def _index_tpex_3insti(data):
    index = {}
    tables = data.get('tables') or []
    if not tables:
        return index
    for row in tables[0].get('data') or []:
        try:
            code = str(row[0]).strip()
            foreign_net = _parse_share_int(row[10])
            trust_net = _parse_share_int(row[13])
            index[code] = {
                "foreign_net": format_net_lots(foreign_net),
                "trust_net": format_net_lots(trust_net)
            }
        except (IndexError, TypeError, ValueError):
            continue
    return index


def _get_date_lock(cache_key):
    with _cache_lock:
        lock = _date_locks.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _date_locks[cache_key] = lock
        return lock


def get_market_chip_index(market, date_str):
    """
    取得某日全市場法人買賣超 index。先查記憶體，沒有再走檔案/網路。
    """
    cache_key = (market, date_str)
    with _cache_lock:
        cached = _market_chip_index.get(cache_key)
        if cached is not None:
            return cached

    date_lock = _get_date_lock(cache_key)
    with date_lock:
        with _cache_lock:
            cached = _market_chip_index.get(cache_key)
            if cached is not None:
                return cached

        if market == "twse":
            raw = fetch_twse_t86_data(date_str)
            index = _index_twse_t86(raw) if raw else None
        else:
            raw = fetch_tpex_3insti_data(date_str)
            index = _index_tpex_3insti(raw) if raw else None

        # 沒抓到，或抓到的是空表：不要快取，讓下次再試。
        if not index:
            return None

        with _cache_lock:
            _market_chip_index[cache_key] = index
        return index


def _normalize_chip_date(date_obj):
    if hasattr(date_obj, "strftime"):
        return date_obj.strftime("%Y%m%d")
    return str(date_obj).replace("-", "")[:8]


# ---------------------------------------------------------------------------
# 每日行情（成交金額、漲跌幅），族群排行用。快取方式與 T86 相同。
# ---------------------------------------------------------------------------

CACHE_DIR_TWSE_QUOTES = "/tmp/stock_data_cache/twse_quotes"
CACHE_DIR_TPEX_QUOTES = "/tmp/stock_data_cache/tpex_quotes"
os.makedirs(CACHE_DIR_TWSE_QUOTES, exist_ok=True)
os.makedirs(CACHE_DIR_TPEX_QUOTES, exist_ok=True)

_market_quotes_index = {}  # (market, date_str) -> {code: {turnover, close, change_pct}}
_QUOTES_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def _read_json_file(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json_file(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        print(f"提醒：寫入快取 {path} 失敗。({e})", file=sys.stderr)


def fetch_twse_daily_quotes(date_str):
    """
    證交所 MI_INDEX（全部不含權證）。回應有多張表，只留「每日收盤行情」那張的 fields + data。
    """
    cache_path = os.path.join(CACHE_DIR_TWSE_QUOTES, f"{date_str}.json")
    cached = _read_json_file(cache_path)
    if cached:
        return cached

    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999"
    try:
        r = requests.get(url, headers=_QUOTES_HEADERS, timeout=20)
        data = r.json()
        if data.get('stat') != 'OK':
            return None
        table = None
        for t in data.get('tables') or []:
            if '每日收盤行情' in str(t.get('title', '')) and t.get('data'):
                table = t
                break
        if table is None:
            return None
        slim = {"stat": "OK", "date": date_str, "fields": table.get('fields') or [], "data": table.get('data') or []}
        _write_json_file(cache_path, slim)
        return slim
    except Exception as e:
        print(f"提醒：無法獲取 TWSE {date_str} 每日行情。({e})", file=sys.stderr)
    return None


def fetch_tpex_daily_quotes(date_str):
    """
    櫃買中心上櫃每日收盤行情（不含權證）。
    """
    try:
        dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        roc_date_str = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
    except Exception:
        return None

    cache_path = os.path.join(CACHE_DIR_TPEX_QUOTES, f"{date_str}.json")
    cached = _read_json_file(cache_path)
    if cached:
        return cached

    url = (
        "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
        f"?l=zh-tw&o=json&d={roc_date_str}&se=EW"
    )
    try:
        r = requests.get(url, headers=_QUOTES_HEADERS, timeout=20)
        data = r.json()
        tables = data.get('tables') or []
        if not tables or not tables[0].get('data'):
            return None
        slim = {"stat": "OK", "date": date_str, "fields": tables[0].get('fields') or [], "data": tables[0]['data']}
        _write_json_file(cache_path, slim)
        return slim
    except Exception as e:
        print(f"提醒：無法獲取 TPEx {date_str} ({roc_date_str}) 每日行情。({e})", file=sys.stderr)
    return None


def _strip_tags(value):
    return re.sub(r'<[^>]+>', '', str(value)).strip()


def _find_field(fields, keyword):
    """用欄位名稱找索引（忽略空白），找不到回 None。"""
    for i, name in enumerate(fields or []):
        if keyword in str(name).replace(' ', ''):
            return i
    return None


def _to_float(value):
    try:
        text = str(value).replace(',', '').replace('+', '').strip()
        if text in ('', '-', '--', 'X', '除權', '除息', '除權息'):
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        return int(str(value).replace(',', '').strip())
    except (TypeError, ValueError):
        return 0


def _change_pct(close, diff):
    if close is None or diff is None:
        return None
    prev = close - diff
    if prev <= 0:
        return None
    return diff / prev * 100.0


def _index_twse_quotes(raw):
    fields = raw.get('fields') or []
    i_code = _find_field(fields, '證券代號')
    i_amt = _find_field(fields, '成交金額')
    i_vol = _find_field(fields, '成交股數')
    i_close = _find_field(fields, '收盤價')
    i_sign = _find_field(fields, '漲跌(+/-)')
    i_diff = _find_field(fields, '漲跌價差')
    if None in (i_code, i_amt, i_close):
        return {}
    index = {}
    for row in raw.get('data') or []:
        try:
            code = str(row[i_code]).strip()
            volume = _to_int(row[i_vol]) if i_vol is not None else 0
            close = _to_float(row[i_close])
            change_pct = None
            if i_sign is not None and i_diff is not None and close is not None:
                sign_text = _strip_tags(row[i_sign])
                diff = _to_float(row[i_diff])
                if diff is not None:
                    if '-' in sign_text:
                        diff = -diff
                    elif '+' not in sign_text:
                        diff = 0.0
                    change_pct = _change_pct(close, diff)
            index[code] = {"turnover": _to_int(row[i_amt]), "volume": volume, "close": close, "change_pct": change_pct}
        except (IndexError, TypeError):
            continue
    return index


def _index_tpex_quotes(raw):
    fields = raw.get('fields') or []
    i_code = _find_field(fields, '代號')
    i_amt = _find_field(fields, '成交金額')
    i_vol = _find_field(fields, '成交股數')
    i_close = _find_field(fields, '收盤')
    i_chg = _find_field(fields, '漲跌')
    if None in (i_code, i_amt, i_close):
        return {}
    index = {}
    for row in raw.get('data') or []:
        try:
            code = str(row[i_code]).strip()
            volume = _to_int(row[i_vol]) if i_vol is not None else 0
            close = _to_float(row[i_close])
            diff = _to_float(row[i_chg]) if i_chg is not None else None
            if diff is not None and '-' in str(row[i_chg]):
                diff = -abs(diff)
            index[code] = {"turnover": _to_int(row[i_amt]), "volume": volume, "close": close, "change_pct": _change_pct(close, diff)}
        except (IndexError, TypeError):
            continue
    return index


def get_market_quotes_index(market, date_str):
    """某日全市場成交金額與漲跌幅 index，兩層快取同 get_market_chip_index。"""
    cache_key = (market, date_str)
    with _cache_lock:
        cached = _market_quotes_index.get(cache_key)
        if cached is not None:
            return cached

    date_lock = _get_date_lock(("quotes",) + cache_key)
    with date_lock:
        with _cache_lock:
            cached = _market_quotes_index.get(cache_key)
            if cached is not None:
                return cached

        if market == "twse":
            raw = fetch_twse_daily_quotes(date_str)
            index = _index_twse_quotes(raw) if raw else None
        else:
            raw = fetch_tpex_daily_quotes(date_str)
            index = _index_tpex_quotes(raw) if raw else None

        if not index:
            return {}
        with _cache_lock:
            _market_quotes_index[cache_key] = index
        return index


CHIP_FETCH_WORKERS = 5
CHIP_FETCH_ATTEMPTS = 3  # 第一次加上再試 2 次
CHIP_HISTORY_DAYS = 10   # 個股法人只抓最近幾個交易日


def get_stock_historical_institutional_investors(stock_code, is_taiwan_stock, is_listed, dates_list):
    """
    批量取得特定個股在給定日期列表中的外資與投信買賣超張數。
    回傳 (results, complete)。檔案沒抓到的日期不會寫進快取；
    檔案有抓到但沒有這支股票，才記成 '-'。
    """
    date_keys = [_normalize_chip_date(d) for d in dates_list]
    results = {date_str: dict(_EMPTY_CHIP) for date_str in date_keys}

    if not is_taiwan_stock:
        return results, True

    cache_key = (stock_code, tuple(date_keys), bool(is_listed))
    with _cache_lock:
        cached = _stock_chip_cache.get(cache_key)
        if cached is not None:
            return copy.deepcopy(cached), True

    market = "twse" if is_listed else "tpex"
    pending = list(dict.fromkeys(date_keys))
    resolved = {}

    def fetch_for_date(date_str):
        index = get_market_chip_index(market, date_str)
        if not index:
            return date_str, None
        return date_str, dict(index.get(stock_code, _EMPTY_CHIP))

    for attempt in range(CHIP_FETCH_ATTEMPTS):
        if not pending:
            break
        if attempt:
            time.sleep(0.4)
        next_pending = []
        with ThreadPoolExecutor(max_workers=CHIP_FETCH_WORKERS) as executor:
            futures = {executor.submit(fetch_for_date, d): d for d in pending}
            for future, date_str in futures.items():
                try:
                    _, res = future.result()
                except Exception:
                    res = None
                if res is None:
                    next_pending.append(date_str)
                else:
                    resolved[date_str] = res
        pending = next_pending

    for date_str, chip in resolved.items():
        results[date_str] = chip

    complete = not pending
    if complete:
        with _cache_lock:
            _stock_chip_cache[cache_key] = copy.deepcopy(results)
    return results, complete


def _store_analysis_cache(res):
    cached = copy.deepcopy(res)
    cached.pop("chart_history", None)
    cached.pop("prompt", None)
    cached.pop("prompt_classic", None)
    cached.pop("basic_prompt", None)
    cached["cached_at"] = time.time()
    with _cache_lock:
        _analysis_cache[res["stock_code"]] = cached


def _get_analysis_cache(stock_code):
    with _cache_lock:
        cached = _analysis_cache.get(stock_code)
        if cached is None:
            return None
        return copy.deepcopy(cached)


def apply_chip_to_analysis(data, inst_data):
    data = copy.deepcopy(data)
    for item in data.get("history") or []:
        date_key = str(item.get("date", "")).replace("-", "")
        chip = inst_data.get(date_key, _EMPTY_CHIP)
        item["foreign_net"] = chip.get("foreign_net", "-")
        item["trust_net"] = chip.get("trust_net", "-")
    data["has_chip"] = True
    return data


def _dates_from_history(data):
    dates_list = []
    for item in data.get("history") or []:
        try:
            dates_list.append(datetime.datetime.strptime(item["date"], "%Y-%m-%d"))
        except (KeyError, TypeError, ValueError):
            continue
    return dates_list


def attach_chip_if_needed(data, want_chip):
    """
    依需求把法人籌碼補進分析資料。台股且 want_chip 才會查（走兩層快取）。
    回傳 (data, inst_data)；不需要籌碼時 inst_data 為 None。
    """
    if not want_chip or not data.get("is_taiwan_stock"):
        data = copy.deepcopy(data)
        data["has_chip"] = False
        return data, None

    inst_data, chip_complete = get_stock_historical_institutional_investors(
        data["stock_code"],
        True,
        data.get("is_listed", True),
        _dates_from_history(data)[:CHIP_HISTORY_DAYS]
    )
    data = apply_chip_to_analysis(data, inst_data)
    data["chip_complete"] = chip_complete
    return data, inst_data


def apply_cached_chip(data, want_chip):
    """
    只用記憶體裡已經載入的法人。不打網路。
    沒有已載入的籌碼時 has_chip 為 False。
    """
    if not want_chip or not data.get("is_taiwan_stock"):
        data = copy.deepcopy(data)
        data["has_chip"] = False
        return data

    date_keys = [_normalize_chip_date(d) for d in _dates_from_history(data)[:CHIP_HISTORY_DAYS]]
    is_listed = bool(data.get("is_listed", True))
    cache_key = (data["stock_code"], tuple(date_keys), is_listed)
    market = "twse" if is_listed else "tpex"

    with _cache_lock:
        cached = _stock_chip_cache.get(cache_key)
        if cached is not None:
            inst_data = copy.deepcopy(cached)
        else:
            inst_data = {}
            for date_str in date_keys:
                index = _market_chip_index.get((market, date_str))
                if not index:
                    continue
                inst_data[date_str] = dict(index.get(data["stock_code"], _EMPTY_CHIP))

    if not inst_data:
        data = copy.deepcopy(data)
        data["has_chip"] = False
        return data
    return apply_chip_to_analysis(data, inst_data)


def fetch_twse_realtime(stock_code):
    """
    從證交所官方 API 抓取台股即時資訊；目前只用來取得五檔買賣單（價格與成交量改由 Shioaji 提供）。
    """
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        # Step 1: 訪問首頁取得 Session Cookies
        session.get('https://mis.twse.com.tw/stock/index.jsp', headers=headers, timeout=10)
        
        # Step 2: 查詢即時 API (上市 tse / 上櫃 otc 同時查詢，看哪一個有資料)
        timestamp = int(time.time() * 1000)
        url = f'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_{stock_code}.tw|otc_{stock_code}.tw&_={timestamp}'
        
        r = session.get(url, headers=headers, timeout=10)
        data = r.json()
        
        msg_array = data.get('msgArray', [])
        if not msg_array:
            return None
            
        # 尋找匹配的股票資料
        stock_info = None
        for item in msg_array:
            if item.get('c') == stock_code:
                stock_info = item
                break
                
        if not stock_info and msg_array:
            # 備用：若代碼沒有完全一致，但有回傳資料，取第一個有價格的
            for item in msg_array:
                if item.get('z') and item.get('z') != '-':
                    stock_info = item
                    break
            if not stock_info:
                stock_info = msg_array[0]
                
        if not stock_info or not stock_info.get('c'):
            return None
            
        return stock_info
    except Exception as e:
        print(f"提醒：無法從證交所 API 取得五檔資料。({e})", file=sys.stderr)
        return None

def fetch_yfinance_data(symbol, period="6mo"):
    """
    從 Yahoo Finance 抓取歷史數據（預設抓取 6 個月，確保能計算 60 日均線）。
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"錯誤：無法從 Yahoo Finance 取得 {symbol} 的歷史數據。({e})", file=sys.stderr)
        return None

def calculate_technical_indicators(df):
    """
    計算技術指標：MA5, MA20, RSI-14。
    """
    if df is None or df.empty:
        return df
        
    # 複製一份避免警告
    df = df.copy()
    
    # 移動平均線 MA
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    
    # 相對強弱指標 RSI (Wilder 氏平滑法，TradingView 標準)
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    
    # 避免除以 0
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    df['RSI14'] = 100 - (100 / (1 + rs))
    
    return df

def parse_bid_ask(rt_info):
    """
    解析最佳五檔買賣價。
    """
    if not rt_info:
        return "無資料", "無資料"
        
    b_prices = rt_info.get('b', '').split('_')
    b_vols = rt_info.get('g', '').split('_')
    a_prices = rt_info.get('a', '').split('_')
    a_vols = rt_info.get('f', '').split('_')
    
    bids = []
    for p, v in zip(b_prices, b_vols):
        if p and p != '-' and float(p) > 0:
            bids.append(f"{float(p):.2f} ({v}張)")
            
    asks = []
    for p, v in zip(a_prices, a_vols):
        if p and p != '-' and float(p) > 0:
            asks.append(f"{float(p):.2f} ({v}張)")
            
    bids_str = " | ".join(bids) if bids else "無即時買單資料"
    asks_str = " | ".join(asks) if asks else "無即時賣單資料"
    
    return bids_str, asks_str

TAIWAN_STOCK_MAPPING = None
_code_name_cache = {}     # code -> name
_code_sector_cache = {}   # code -> 產業別（去掉結尾「業」，但保留「工業」「事業」）；權證 / ETF 沒有產業別，不會出現在這裡
_code_market_cache = {}   # code -> "twse" | "tpex"

_ISIN_ROW_RE = re.compile(r'<tr[^>]*>(.*?)</tr>', re.S | re.I)
_ISIN_CELL_RE = re.compile(r'<td[^>]*>(.*?)</td>', re.S | re.I)
_ISIN_CODE_NAME_RE = re.compile(r'^\s*([0-9A-Z]+)\s*　\s*(.+?)\s*$')


def _parse_isin_rows(html, market):
    """
    解析 ISIN 清單頁。每列欄位：代號 名稱 | ISIN | 上市日 | 市場別 | 產業別 | CFI | 備註。
    回傳 [(code, name, sector)]，sector 可能為空字串。
    """
    rows = []
    for row_html in _ISIN_ROW_RE.findall(html):
        cells = [re.sub(r'<[^>]+>', '', c).strip() for c in _ISIN_CELL_RE.findall(row_html)]
        if not cells:
            continue
        m = _ISIN_CODE_NAME_RE.match(cells[0])
        if not m:
            continue
        code, name = m.group(1).strip(), m.group(2).strip()
        sector = cells[4].strip() if len(cells) >= 5 else ""
        # 半導體業 → 半導體；但「水泥工業」「觀光事業」不能只砍最後一個「業」
        if sector.endswith("業") and not sector.endswith(("工業", "事業")):
            sector = sector[:-1]
        rows.append((code, name, sector))
    return rows


def get_taiwan_stock_mapping():
    """
    下載一次 ISIN 清單，同時建好三張表：name->code（回傳值）、code->name、code->sector。
    """
    global TAIWAN_STOCK_MAPPING
    if TAIWAN_STOCK_MAPPING is not None:
        return TAIWAN_STOCK_MAPPING
        
    print("正在下載並建立台股名稱與代碼對照表...", file=sys.stderr)
    mapping = {}
    code_name = {}
    code_sector = {}
    code_market = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    urls = [
        ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "twse"),
        ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", "tpex"),
    ]
    for url, market in urls:
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.encoding = 'big5'
            for code, name, sector in _parse_isin_rows(r.text, market):
                mapping[name] = code
                code_name.setdefault(code, name)
                code_market.setdefault(code, market)
                if sector:
                    code_sector.setdefault(code, sector)
        except Exception as e:
            print(f"提醒：無法從 {url} 獲取股票清單。({e})", file=sys.stderr)
            
    if mapping:
        with _cache_lock:
            _code_name_cache.update(code_name)
            _code_sector_cache.update(code_sector)
            _code_market_cache.update(code_market)
    TAIWAN_STOCK_MAPPING = mapping
    return TAIWAN_STOCK_MAPPING


def get_code_to_sector():
    with _cache_lock:
        if _code_sector_cache:
            return _code_sector_cache
    get_taiwan_stock_mapping()
    with _cache_lock:
        return _code_sector_cache

def resolve_stock_code_by_name(name):
    mapping = get_taiwan_stock_mapping()
    if not mapping:
        return None
        
    name = name.strip()
    # 1. 嘗試完全匹配
    if name in mapping:
        return mapping[name]
        
    # 2. 嘗試模糊匹配
    candidates = []
    for k, v in mapping.items():
        if name in k:
            candidates.append((k, v))
            
    if not candidates:
        return None
        
    # 排序：優先選擇代碼長度短的（優先一般股票與ETF，避開長度為6位數的權證），其次選擇字元長度短的
    candidates.sort(key=lambda x: (len(x[1]), len(x[0])))
    return candidates[0][1]

def calculate_single_neckline(df, lookback=20):
    """
    使用 Volume Profile（成交量分布）計算支撐與壓力位。
    將歷史 K 線的成交量分配到各價位區間，找出成交量最密集的價位（HVN），
    再從中挑出離現價最近的上方壓力與下方支撐。
    
    Args:
        df: 歷史 K 線 DataFrame，需包含 High, Low, Close, Volume 欄位
        lookback: 回看天數（短期=20, 中期=60）
    """
    if df is None or len(df) < 5:
        return "-", "-"
    
    # 取最近 lookback 天的數據
    sub_df = df.tail(lookback).copy()
    if sub_df.empty:
        return "-", "-"
    
    current_val = df['Close'].values[-1]
    
    # --- 先計算 ATR-14（用於最小距離門檻與 Fallback） ---
    atr = 0
    atr_df = df.tail(15).copy()
    if len(atr_df) >= 2:
        tr_values = []
        for i in range(1, len(atr_df)):
            h = atr_df['High'].iloc[i]
            l = atr_df['Low'].iloc[i]
            prev_c = atr_df['Close'].iloc[i - 1]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            tr_values.append(tr)
        atr = sum(tr_values) / len(tr_values) if tr_values else 0
    
    # 最小距離門檻：支撐壓力至少要離現價 0.5 個 ATR，才有實戰參考價值
    min_distance = atr * 0.5 if atr > 0 else current_val * 0.005
    
    # --- Step 1: 建立 Volume Profile ---
    price_min = sub_df['Low'].min()
    price_max = sub_df['High'].max()
    
    if pd.isna(price_min) or pd.isna(price_max) or price_max <= price_min:
        return "-", "-"
    
    # 切成 15 格，讓每個區間夠寬，能區分出有意義的密集交易帶
    price_range = price_max - price_min
    num_bins = 15
    bin_size = price_range / num_bins
    
    if bin_size <= 0:
        return "-", "-"
    
    # 每個 bin 的中心價格與累積成交量
    bin_centers = [price_min + (i + 0.5) * bin_size for i in range(num_bins)]
    bin_volumes = [0.0] * num_bins
    
    for _, row in sub_df.iterrows():
        h = row['High']
        l = row['Low']
        c = row['Close']
        v = row['Volume']
        
        if pd.isna(h) or pd.isna(l) or pd.isna(c) or pd.isna(v) or v <= 0:
            continue
        
        # 找出這根 K 線覆蓋的 bin 範圍
        bin_start = max(0, int((l - price_min) / bin_size))
        bin_end = min(num_bins - 1, int((h - price_min) / bin_size))
        
        if bin_start > bin_end:
            continue
        
        # 將成交量分配到覆蓋的 bins，靠近收盤價的 bin 獲得更多權重
        weights = []
        total_weight = 0.0
        for b in range(bin_start, bin_end + 1):
            center = bin_centers[b]
            # 用高斯型權重：離收盤價越近，權重越高
            dist = abs(center - c)
            spread = max(h - l, bin_size)  # 避免除以零
            w = max(0.1, 1.0 - (dist / spread))  # 最低權重 0.1
            weights.append(w)
            total_weight += w
        
        # 正規化後分配成交量
        if total_weight > 0:
            for idx_b, b in enumerate(range(bin_start, bin_end + 1)):
                bin_volumes[b] += v * (weights[idx_b] / total_weight)
    
    # --- Step 2: 找出高量節點 (HVN) ---
    if max(bin_volumes) == 0:
        return "-", "-"
    
    # 計算成交量的平均值，只取高於平均值的 bins 作為 HVN
    avg_vol = sum(bin_volumes) / len(bin_volumes)
    
    # 找出所有高於平均成交量的價位節點 (HVN)
    hvn_list = []  # (price, volume)
    for i in range(num_bins):
        if bin_volumes[i] > avg_vol:
            hvn_list.append((bin_centers[i], bin_volumes[i]))
    
    # --- Step 3: 從 HVN 中找上方壓力與下方支撐（需超過最小距離門檻） ---
    upper_neckline = "-"
    lower_neckline = "-"
    
    # 上頸線：高於現價且距離超過 min_distance 的 HVN 中，離現價最近的
    upper_candidates = [(p, v) for p, v in hvn_list if p > current_val + min_distance]
    if upper_candidates:
        upper_candidates.sort(key=lambda x: x[0])  # 按價格由低到高
        upper_neckline = f"{upper_candidates[0][0]:.2f}"
    
    # 下頸線：低於現價且距離超過 min_distance 的 HVN 中，離現價最近的
    lower_candidates = [(p, v) for p, v in hvn_list if p < current_val - min_distance]
    if lower_candidates:
        lower_candidates.sort(key=lambda x: x[0], reverse=True)  # 按價格由高到低
        lower_neckline = f"{lower_candidates[0][0]:.2f}"
    
    # --- Step 4: ATR Fallback (找不到符合距離的 HVN 時使用動態波動幅度) ---
    if upper_neckline == "-":
        if atr > 0:
            upper_neckline = f"{current_val + 1.5 * atr:.2f}"
        else:
            val = df['High'].tail(20).max()
            upper_neckline = f"{val:.2f}" if pd.notna(val) else "-"
    
    if lower_neckline == "-":
        if atr > 0:
            lower_neckline = f"{current_val - 1.5 * atr:.2f}"
        else:
            val = df['Low'].tail(20).min()
            lower_neckline = f"{val:.2f}" if pd.notna(val) else "-"
    
    return upper_neckline, lower_neckline

def calculate_trendlines(df, window=5):
    """
    計算上升與下降趨勢線的座標段（起點與延伸到今天的終點）
    使用最後兩個局部極值高點（下降趨勢線）與低點（上升趨勢線）
    """
    if df is None or len(df) < (window * 2 + 1):
        return None, None
        
    sub_df = df.tail(90)
    close = sub_df['Close'].values
    highs_series = sub_df['High'].values
    lows_series = sub_df['Low'].values
    dates = sub_df.index
    
    peaks = []
    troughs = []
    
    for i in range(window, len(sub_df) - window):
        # 局部最高點
        if all(highs_series[i] >= highs_series[i - j] for j in range(1, window + 1)) and \
           all(highs_series[i] >= highs_series[i + j] for j in range(1, window + 1)):
            peaks.append((i, highs_series[i], dates[i]))
            
        # 局部最低點
        if all(lows_series[i] <= lows_series[i - j] for j in range(1, window + 1)) and \
           all(lows_series[i] <= lows_series[i + j] for j in range(1, window + 1)):
            troughs.append((i, lows_series[i], dates[i]))
            
    trendline_rising = None
    trendline_falling = None
    
    today_idx = len(sub_df) - 1
    today_date_str = dates[today_idx].strftime('%Y-%m-%d') if hasattr(dates[today_idx], 'strftime') else str(dates[today_idx])
    
    # 上升趨勢線 (Support Trendline): 由最後兩個局部低點相連
    if len(troughs) >= 2:
        t1 = troughs[-2]
        t2 = troughs[-1]
        
        idx1, val1, date1 = t1
        idx2, val2, date2 = t2
        
        if idx2 > idx1:
            slope = (val2 - val1) / (idx2 - idx1)
            val_today = val1 + slope * (today_idx - idx1)
            
            date1_str = date1.strftime('%Y-%m-%d') if hasattr(date1, 'strftime') else str(date1)
            
            trendline_rising = {
                "start": {"date": date1_str, "price": round(val1, 2)},
                "end": {"date": today_date_str, "price": round(val_today, 2)}
            }
            
    # 下降趨勢線 (Resistance Trendline): 由最後兩個局部高點相連
    if len(peaks) >= 2:
        p1 = peaks[-2]
        p2 = peaks[-1]
        
        idx1, val1, date1 = p1
        idx2, val2, date2 = p2
        
        if idx2 > idx1:
            slope = (val2 - val1) / (idx2 - idx1)
            val_today = val1 + slope * (today_idx - idx1)
            
            date1_str = date1.strftime('%Y-%m-%d') if hasattr(date1, 'strftime') else str(date1)
            
            trendline_falling = {
                "start": {"date": date1_str, "price": round(val1, 2)},
                "end": {"date": today_date_str, "price": round(val_today, 2)}
            }
            
    return trendline_rising, trendline_falling

def get_stock_analysis_data(stock_code, include_chip=True):
    if not stock_code:
        return {"success": False, "error": "請輸入有效的代碼！"}
        
    # 1. 判斷與清理台股格式 (支援包含字母的台股與 ETF，如 00981A, 2881A, 2330.TW 等)
    clean_stock_code = stock_code.upper().strip()
    is_taiwan_stock = False
    
    # 如果輸入含有中文，嘗試將其轉換為股票代碼
    if re.search(r'[\u4e00-\u9fa5]', clean_stock_code):
        resolved_code = resolve_stock_code_by_name(clean_stock_code)
        if resolved_code:
            clean_stock_code = resolved_code
            is_taiwan_stock = True
        else:
            return {"success": False, "error": f"找不到名稱中含有「{stock_code}」的台股代碼，請檢查輸入是否正確。"}
    
    if '.' in clean_stock_code:
        parts = clean_stock_code.split('.')
        suffix = parts[-1]
        if suffix in ('TW', 'TWO'):
            is_taiwan_stock = True
            clean_stock_code = '.'.join(parts[:-1])
    else:
        # 如果無後綴，但符合台股特徵：以數字開頭、只包含字母或數字、長度介於 4 到 6 碼之間
        if clean_stock_code and clean_stock_code[0].isdigit() and clean_stock_code.isalnum() and 4 <= len(clean_stock_code) <= 6:
            is_taiwan_stock = True
        
    df = None
    stock_name = stock_code
    is_listed_tw = False
    
    # 即時欄位預設值
    current_price = None
    open_price = "-"
    high_price = "-"
    low_price = "-"
    volume_str = "-"
    yesterday_close = None
    price_change_str = "-"
    bids_str, asks_str = "無", "無"
    data_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if is_taiwan_stock:
        # A. 台股路徑：永豐 Shioaji API（商品檔 → 快照 → 日 K）
        try:
            meta = sj_market.get_stock_meta(clean_stock_code)
        except ShioajiUnavailable as e:
            return {"success": False, "error": f"無法使用永豐 Shioaji API：{e}"}
        if meta is None:
            return {"success": False, "error": f"永豐商品檔查無代碼 {clean_stock_code}，請確認輸入是否正確。"}

        stock_name = meta.get("name") or stock_code
        is_listed_tw = bool(meta.get("is_listed"))
        yesterday_close = meta.get("reference")

        snap = None
        try:
            snap = sj_market.get_snapshots([clean_stock_code]).get(clean_stock_code)
        except ShioajiUnavailable as e:
            print(f"提醒：無法取得 {clean_stock_code} 的即時快照。({e})", file=sys.stderr)

        try:
            df = sj_market.get_daily_history(clean_stock_code)
        except ShioajiUnavailable as e:
            print(f"提醒：無法取得 {clean_stock_code} 的歷史 K 線。({e})", file=sys.stderr)

        if snap is not None:
            info = sj_market.snapshot_to_dict(snap)
            if info["close"] > 0:
                current_price = info["close"]
                open_price = f"{info['open']:.2f}" if info["open"] > 0 else "-"
                high_price = f"{info['high']:.2f}" if info["high"] > 0 else "-"
                low_price = f"{info['low']:.2f}" if info["low"] > 0 else "-"
                volume_str = f"{info['total_volume']}"
                # 昨收以快照的「現價 - 漲跌」為準，商品檔的參考價只是備援
                derived_prev = info["close"] - info["change_price"]
                if derived_prev > 0:
                    yesterday_close = derived_prev
                price_change_str = f"{info['change_price']:+.2f} ({info['change_rate']:+.2f}%)"
                if info["datetime"] is not None:
                    data_time = info["datetime"].strftime("%Y-%m-%d %H:%M:%S")
                df = sj_market.merge_snapshot_into_daily(df, snap)

        # B. 五檔仍從證交所取得（Shioaji 快照只有最佳一檔）；失敗不影響主流程
        rt_info = fetch_twse_realtime(clean_stock_code)
        if rt_info:
            bids_str, asks_str = parse_bid_ask(rt_info)
    else:
        # 美股或其他市場直接使用 yfinance
        yf_symbol = stock_code.upper()
        df = fetch_yfinance_data(yf_symbol)
        
    # 如果完全無法取得歷史 K 線
    if df is None or df.empty:
        return {"success": False, "error": f"無法取得代碼 {stock_code} 的任何市場數據，請檢查代碼是否正確。"}
        
    # 計算技術指標
    df = calculate_technical_indicators(df)
    
    # 如果沒走台股 API，或是台股 API 沒拿到最新即時價，就從 history 最後一筆拿
    if current_price is None:
        last_row = df.iloc[-1]
        current_price = last_row['Close']
        open_price = f"{last_row['Open']:.2f}"
        high_price = f"{last_row['High']:.2f}"
        low_price = f"{last_row['Low']:.2f}"
        # Volume 轉成張 (美股也是除以 1000 以簡化顯示)
        volume_str = f"{int(last_row['Volume'] / 1000)}"
        
        # 尋找昨收
        if len(df) > 1:
            yesterday_close = df.iloc[-2]['Close']
            diff = current_price - yesterday_close
            percent = (diff / yesterday_close) * 100
            price_change_str = f"{diff:+.2f} ({percent:+.2f}%)"
        else:
            yesterday_close = last_row['Open']
            price_change_str = "0.00 (0.00%)"
            
        data_time = df.index[-1].strftime("%Y-%m-%d")
        
    # 取出今日的技術指標
    latest_row = df.iloc[-1]
    ma5_val = latest_row.get('MA5', None)
    ma20_val = latest_row.get('MA20', None)
    ma60_val = latest_row.get('MA60', None)
    rsi14_val = latest_row.get('RSI14', None)
    
    ma5_str = f"{ma5_val:.2f}" if pd.notna(ma5_val) else "計算中"
    ma20_str = f"{ma20_val:.2f}" if pd.notna(ma20_val) else "計算中"
    ma60_str = f"{ma60_val:.2f}" if pd.notna(ma60_val) else "計算中"
    rsi14_str = f"{rsi14_val:.2f}" if pd.notna(rsi14_val) else "計算中"
    
    # 判斷股價與均線關係
    ma5_relation = "之上 ↗" if (pd.notna(ma5_val) and current_price >= ma5_val) else "之下 ↘"
    ma20_relation = "之上 ↗" if (pd.notna(ma20_val) and current_price >= ma20_val) else "之下 ↘"
    ma60_relation = "之上 ↗" if (pd.notna(ma60_val) and current_price >= ma60_val) else "之下 ↘"
    
    # 判斷 RSI 狀態
    rsi14_status = "盤整"
    if pd.notna(rsi14_val):
        if rsi14_val >= 70:
            rsi14_status = "超買區 (偏高，注意回檔)"
        elif rsi14_val <= 30:
            rsi14_status = "超賣區 (偏低，可能有反彈機會)"
        else:
            rsi14_status = "中性區"
            
    # 整理近 30 天的 K 線表格
    history_list = []
    # 取最近 30 筆
    recent_df = df.tail(30)
    
    # 法人只抓最近 CHIP_HISTORY_DAYS 個交易日；K 線表仍是 30 天
    recent_dates = [idx.date() if hasattr(idx, 'date') else idx for idx in recent_df.index]
    if include_chip:
        inst_data, _chip_complete = get_stock_historical_institutional_investors(
            clean_stock_code, is_taiwan_stock, is_listed_tw, recent_dates[-CHIP_HISTORY_DAYS:]
        )
    else:
        inst_data = {}
    
    for idx, row in reversed(list(recent_df.iterrows())):
        date_name = idx.strftime("%Y-%m-%d")
        date_key = idx.strftime("%Y%m%d")
        c_val = row['Close']
        o_val = row['Open']
        h_val = row['High']
        l_val = row['Low']
        v_val = row['Volume']
        v_lots = int(v_val / 1000) if pd.notna(v_val) else 0
        
        m5 = f"{row['MA5']:.2f}" if pd.notna(row.get('MA5')) else "-"
        m20 = f"{row['MA20']:.2f}" if pd.notna(row.get('MA20')) else "-"
        m60 = f"{row['MA60']:.2f}" if pd.notna(row.get('MA60')) else "-"
        r14 = f"{row['RSI14']:.2f}" if pd.notna(row.get('RSI14')) else "-"
        
        # 獲取法人數據
        foreign_net = inst_data.get(date_key, {}).get("foreign_net", "-")
        trust_net = inst_data.get(date_key, {}).get("trust_net", "-")
        
        history_list.append({
            "date": date_name,
            "close": f"{c_val:.2f}" if pd.notna(c_val) else "-",
            "open": f"{o_val:.2f}" if pd.notna(o_val) else "-",
            "high": f"{h_val:.2f}" if pd.notna(h_val) else "-",
            "low": f"{l_val:.2f}" if pd.notna(l_val) else "-",
            "volume": v_lots,
            "foreign_net": foreign_net,
            "trust_net": trust_net,
            "ma5": m5,
            "ma20": m20,
            "ma60": m60,
            "rsi14": r14
        })
        
    # 整理所有歷史數據供圖表使用 (由舊到新順序)
    chart_history_list = []
    for idx, row in list(df.iterrows()):
        date_name = idx.strftime("%Y-%m-%d")
        c_val = row['Close']
        o_val = row['Open']
        h_val = row['High']
        l_val = row['Low']
        v_val = row['Volume']
        v_lots = int(v_val / 1000) if pd.notna(v_val) else 0
        
        m5 = float(f"{row['MA5']:.2f}") if pd.notna(row.get('MA5')) else None
        m20 = float(f"{row['MA20']:.2f}") if pd.notna(row.get('MA20')) else None
        m60 = float(f"{row['MA60']:.2f}") if pd.notna(row.get('MA60')) else None
        r14 = float(f"{row['RSI14']:.2f}") if pd.notna(row.get('RSI14')) else None
        
        chart_history_list.append({
            "date": date_name,
            "close": float(f"{c_val:.2f}") if pd.notna(c_val) else None,
            "open": float(f"{o_val:.2f}") if pd.notna(o_val) else None,
            "high": float(f"{h_val:.2f}") if pd.notna(h_val) else None,
            "low": float(f"{l_val:.2f}") if pd.notna(l_val) else None,
            "volume": v_lots,
            "ma5": m5,
            "ma20": m20,
            "ma60": m60,
            "rsi14": r14
        })
        
    upper_neckline_short, lower_neckline_short = calculate_single_neckline(df, lookback=20)
    upper_neckline_mid, lower_neckline_mid = calculate_single_neckline(df, lookback=60)
    trendline_rising, trendline_falling = calculate_trendlines(df, window=5)
        
    return {
        "success": True,
        "stock_code": clean_stock_code,
        "stock_name": stock_name,
        "data_time": data_time,
        "current_price": f"{current_price:.2f}",
        "price_change": price_change_str,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "volume": volume_str,
        "yesterday_close": f"{yesterday_close:.2f}" if yesterday_close else "-",
        "bids": bids_str,
        "asks": asks_str,
        "ma5": ma5_str,
        "ma5_relation": ma5_relation,
        "ma20": ma20_str,
        "ma20_relation": ma20_relation,
        "ma60": ma60_str,
                "ma60_relation": ma60_relation,
        "rsi14": rsi14_str,
        "rsi14_status": rsi14_status,
        "upper_neckline_short": upper_neckline_short,
        "lower_neckline_short": lower_neckline_short,
        "upper_neckline_mid": upper_neckline_mid,
        "lower_neckline_mid": lower_neckline_mid,
        "upper_neckline": upper_neckline_short,  # Backward compatibility
        "lower_neckline": lower_neckline_short,  # Backward compatibility
        "trendline_rising": trendline_rising,
        "trendline_falling": trendline_falling,
        "history": history_list,
        "chart_history": chart_history_list,
        "is_taiwan_stock": is_taiwan_stock,
        "is_listed": is_listed_tw,
        "has_chip": bool(include_chip and is_taiwan_stock)
    }


STRATEGY_PRESETS = {
    "general": {
        "label": "通用分析",
        "role": "專業的量化交易專家與理財分析師",
        "rules": [],
    },
    "trend_ma": {
        "label": "均線趨勢",
        "role": "順勢交易員",
        "rules": [
            "只有在 MA5 > MA20 > MA60 呈多頭排列、且現價站上 MA5 時，才視為符合進場條件。",
            "均線糾結、或空頭排列時一律不進場，不要用其他理由勉強找買點。",
            "出場條件：收盤跌破 MA20 即視為趨勢轉弱。",
        ],
    },
    "pullback": {
        "label": "拉回支撐",
        "role": "波段拉回買進的交易員",
        "rules": [
            "中長期趨勢必須向上（現價位於 MA60 之上），否則不屬於拉回，而是下跌。",
            "只在回測第一下頸線、MA20 或上升趨勢線附近，且成交量較前幾日縮小時才考慮進場。",
            "必須看到止跌訊號（例如收盤收復當日開盤價或前一日高點）再進場，不接刀。",
        ],
    },
    "breakout": {
        "label": "突破追價",
        "role": "突破策略交易員",
        "rules": [
            "只有在收盤帶量（成交量至少為近 5 日平均的 1.5 倍）突破第一上頸線或下降趨勢線時，才視為符合條件。",
            "突破後回測不跌破突破價位才追價；假突破即放棄。",
            "RSI-14 高於 80 時追價風險偏高，須明確提醒。",
        ],
    },
    "chip_follow": {
        "label": "籌碼跟主力",
        "role": "以法人籌碼為主要依據的交易員",
        "rules": [
            "外資與投信必須同向、且連續至少 3 個交易日買超，才視為主力進場。",
            "價格尚未大漲（現價距 MA20 不超過 5%）才適合跟進；已經噴出就不追。",
            "法人轉為賣超即為出場訊號，不要等技術面確認。",
        ],
        "needs_chip": True,
    },
    "risk_check": {
        "label": "持股健檢",
        "role": "風險控管顧問",
        "rules": [
            "先以我的持有成本為基準，算出目前部位是獲利或虧損、幅度多少。",
            "判斷原本的趨勢是否仍成立：現價與 MA20 / MA60 的位置、法人是否轉向。",
            "結論只能是「加碼 / 續抱 / 減碼」三者之一，並給出明確的執行價位。",
        ],
    },
}

HORIZON_PRESETS = {
    "day": {
        "label": "當日 / 隔日短線",
        "role_suffix": "短線交易員",
        "note": "以當日五檔、開高低價與 MA5 為主要依據，MA60 只作為背景參考。",
    },
    "swing": {
        "label": "波段 (5 至 20 個交易日)",
        "role_suffix": "波段交易員",
        "note": "以 MA20、第一頸線與近 20 日成交量密集區為主要依據。",
    },
    "long": {
        "label": "中長線 (1 至 3 個月以上)",
        "role_suffix": "中長線投資人",
        "note": "以 MA60、第二頸線與 60 日結構為主要依據，不必對短線雜訊過度反應。",
    },
}

POSITION_KINDS = ("none", "holding", "reduce")


def normalize_prompt_options(strategy=None, position=None, horizon=None, web_search=True):
    """把使用者選項整理成安全的預設值。"""
    strategy = strategy if strategy in STRATEGY_PRESETS else "general"
    horizon = horizon if horizon in HORIZON_PRESETS else "swing"

    pos = dict(position or {})
    kind = pos.get("kind", "none")
    if kind not in POSITION_KINDS:
        kind = "none"

    def _to_float(value):
        try:
            value = float(value)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    normalized_position = {
        "kind": kind,
        "cost": _to_float(pos.get("cost")),
        "lots": _to_float(pos.get("lots")),
    }
    return {
        "strategy": strategy,
        "position": normalized_position,
        "horizon": horizon,
        "web_search": bool(web_search),
    }


def describe_position(position):
    kind = position.get("kind", "none")
    cost = position.get("cost")
    lots = position.get("lots")
    cost_str = f"{cost:g}" if cost else None
    lots_str = f"{lots:g}" if lots else None

    if kind == "holding":
        parts = ["我目前**已持有**這支股票"]
        if cost_str:
            parts.append(f"成本約 {cost_str} 元")
        if lots_str:
            parts.append(f"共 {lots_str} 張")
        return "，".join(parts) + "。"
    if kind == "reduce":
        text = "我目前持有這支股票，正在考慮**減碼或出清**"
        if cost_str:
            text += f"，成本約 {cost_str} 元"
        return text + "。"
    return "我目前**尚未持有**這支股票，正在評估是否進場。"


def build_task_section(strategy, position, horizon, web_search, has_chip, section_no=4):
    preset = STRATEGY_PRESETS.get(strategy, STRATEGY_PRESETS["general"])
    horizon_preset = HORIZON_PRESETS.get(horizon, HORIZON_PRESETS["swing"])
    kind = position.get("kind", "none")

    lines = [f"## {section_no}. AI 決策分析任務", ""]
    lines.append(f"- **我的狀態**：{describe_position(position)}")
    lines.append(f"- **分析週期**：{horizon_preset['label']}。{horizon_preset['note']}")
    lines.append(f"- **採用策略**：「{preset['label']}」，請只用這套策略評估，不要混入其他策略的觀點。")
    if has_chip:
        lines.append("- **籌碼資料**：上表已附外資與投信買賣超，買超代表主力偏多，賣超代表主力偏空，請納入判斷。")
    elif preset.get("needs_chip"):
        lines.append("- **注意**：此策略依賴法人籌碼，但本次未附籌碼資料。若無法判斷，請直接說明資料不足。")
    if web_search:
        lines.append("- **可聯網時**：請主動查詢最新一季 EPS、營收成長、本益比與近期重大新聞，並說明它們是否支持或抵觸技術面結論。")

    if preset["rules"]:
        lines.append("")
        lines.append("### 策略規則")
        for rule in preset["rules"]:
            lines.append(f"- {rule}")

    lines.append("")
    lines.append("### 請嚴格依照以下格式回答，不要省略任何一項")
    lines.append(f"1. **策略適配判斷**：一句話說明目前是否符合「{preset['label']}」的條件（適配 / 不適配 / 接近但尚未成立）。")

    if kind == "holding":
        lines.append("2. **部位決策**：只回答「加碼 / 續抱 / 減碼」三者之一，並給出執行價位。不要把它當成新單重新分析進場點。")
    elif kind == "reduce":
        lines.append("2. **減碼決策**：只回答「現在減碼 / 反彈至某價位再減 / 續抱」三者之一，並給出明確價位。")
    else:
        lines.append("2. **操作點位**：進場區間、停損價、停利目標。")

    lines.append("   - **價位必須引用上方提供的頸線、均線或趨勢線數字**，並註明依據哪一條；不可自行推估未提供的價位。")
    lines.append("3. **策略失效條件**：明確寫出跌破或突破哪個價位、或出現什麼籌碼變化時，這套策略就視為失效、應該放棄。")
    lines.append("4. **核心理由與風險**：最多三點，簡述判斷邏輯與主要風險。")
    lines.append("5. **資料不足時**：若上述資料不足以判斷，請直接說明缺少什麼，不要硬給方向。")
    return "\n".join(lines)


def format_prompt(data, web_search=True, strategy="general", position=None, horizon="swing"):
    opts = normalize_prompt_options(strategy, position, horizon, web_search)
    strategy = opts["strategy"]
    position = opts["position"]
    horizon = opts["horizon"]
    web_search = opts["web_search"]
    preset = STRATEGY_PRESETS[strategy]
    horizon_preset = HORIZON_PRESETS[horizon]

    is_tw = data.get("is_taiwan_stock", False) and data.get("has_chip", False)
    # 整理近 30 天的 K 線表格
    history_table_rows = []
    for item in data["history"]:
        if is_tw:
            history_table_rows.append(
                f"| {item['date']} | {item['close']} | {item['open']} | {item['high']} | {item['low']} | {item['volume']} | {item.get('foreign_net', '-')} | {item.get('trust_net', '-')} | {item['ma5']} | {item['ma20']} | {item['ma60']} | {item['rsi14']} |"
            )
        else:
            history_table_rows.append(
                f"| {item['date']} | {item['close']} | {item['open']} | {item['high']} | {item['low']} | {item['volume']} | {item['ma5']} | {item['ma20']} | {item['ma60']} | {item['rsi14']} |"
            )
    history_table = "\n".join(history_table_rows)
    
    if is_tw:
        table_header = """| 日期 | 收盤價 | 開盤價 | 最高價 | 最低價 | 成交量 (張) | 外資買賣超(張) | 投信買賣超(張) | MA5 | MA20 | MA60 | RSI-14 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"""
        chip_instructions = "、**外資與投信的籌碼動向（買超代表主力偏多，賣超代表主力偏空）**"
    else:
        table_header = """| 日期 | 收盤價 | 開盤價 | 最高價 | 最低價 | 成交量 (張) | MA5 | MA20 | MA60 | RSI-14 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"""
        chip_instructions = ""

    if strategy != "general":
        section_4 = build_task_section(strategy, position, horizon, web_search, is_tw)
        intro = (
            f"您是一位{preset['role']}，同時是{horizon_preset['role_suffix']}。"
            f"以下是代碼 `{data['stock_code']}` 的即時數據與技術指標，"
            f"請只以「{preset['label']}」策略評估這支股票，並依照最後的格式回答。"
        )
    elif web_search:
        intro = (
            f"您是一位{preset['role']}。以下是代碼 `{data['stock_code']}` 的即時數據與技術指標，"
            "請為我分析其目前的走勢，並給出具體的「買入」、「賣出」或「觀望」建議。"
        )
        section_4 = f"""## 4. AI 決策分析任務
請依據上述提供的即時市況、技術指標與籌碼數據{chip_instructions}，並結合您的金融專業知識（**若您具備網路搜尋功能，請主動搜尋該股最新基本面如：最新一季 EPS、營收成長率、本益比、近期重大新聞或法說會展望**），為我進行以下分析：

1. **多維度走勢研判**：目前個股處於多頭、空頭還是盤整格局？多空勢力如何？
2. **基本面與估值評估（可聯網查詢）**：從最新的獲利表現（EPS/營收）與產業前景來看，目前股價估值是偏高、合理還是便宜？是否有潛在利多或利空事件？
3. **關鍵支撐與壓力**：根據近期量價結構，短期內上下方的關鍵壓力位與支撐位在哪裡？
4. **具體交易建議**：
   - **建議的行動**：明確給出 (買入 / 賣出 / 觀望)。
   - **具體操作點位**：建議的建倉/買入區間、停利目標價、防守停損價。
   - **核心理由**：簡述您做出此決策的技術、籌碼、基本面分析邏輯與風險提示。"""
    else:
        intro = (
            f"您是一位{preset['role']}。以下是代碼 `{data['stock_code']}` 的即時數據與技術指標，"
            "請為我分析其目前的走勢，並給出具體的「買入」、「賣出」或「觀望」建議。"
        )
        section_4 = f"""## 4. AI 決策分析任務
請依據上述的即時價格、均線結構（黃金交叉/死亡交叉）、RSI 強弱勢指標{chip_instructions}以及近期走勢排列與量能變化，為我進行以下分析：
1. **走勢型態判斷**：目前處於多頭、空頭還是盤整格局？多空勢力如何？
2. **關鍵支撐與壓力**：根據近期走勢及成交量，短期內上下方的關鍵壓力位與支撐位在哪裡？
3. **具體交易建議**：
   - **建議的行動**：明確給出 (買入 / 賣出 / 觀望)。
   - **具體操作點位**：建議的建倉/買入價格區間、停利/目標價、防守停損價。
   - **核心理由**：簡述您做出此決策的技術與籌碼分析邏輯與風險提示。"""

    prompt = f"""# {data['stock_name']} 分析提示詞

{intro}

## 1. 基本資訊與即時市況
- **名稱/代碼**：{data['stock_name']} ({data['stock_code']})
- **數據時間**：{data['data_time']} (為最新成交或更新時間)
- **當前即時價格**：`{data['current_price']}` 元
- **今日漲跌幅**：`{data['price_change']}`
- **今日開盤價**：{data['open']} 元
- **今日最高價**：{data['high']} 元
- **今日最低價**：{data['low']} 元
- **今日累計成交量**：{data['volume']} 張 (1張=1000股)
- **昨收價**：{data['yesterday_close']} 元
- **最佳五檔買單 (價格/張數)**：{data['bids']}
- **最佳五檔賣單 (價格/張數)**：{data['asks']}

## 2. 當前技術指標 (已結合即時價格)
- **第一上頸線 (短期壓力)**：{data['upper_neckline_short']} 元 (近20日成交量密集壓力區)
- **第二上頸線 (中期壓力)**：{data['upper_neckline_mid']} 元 (近60日成交量密集壓力區)
- **第一下頸線 (短期支撐)**：{data['lower_neckline_short']} 元 (近20日成交量密集支撐區)
- **第二下頸線 (中期支撐)**：{data['lower_neckline_mid']} 元 (近60日成交量密集支撐區)
- **上升趨勢線 (斜率支撐)**：{f"從 {data['trendline_rising']['start']['date']} ({data['trendline_rising']['start']['price']}元) 延伸至今天 ({data['trendline_rising']['end']['price']}元)" if data.get('trendline_rising') else "未形成有效線段"}
- **下降趨勢線 (斜率壓力)**：{f"從 {data['trendline_falling']['start']['date']} ({data['trendline_falling']['start']['price']}元) 延伸至今天 ({data['trendline_falling']['end']['price']}元)" if data.get('trendline_falling') else "未形成有效線段"}
- **5 日均線 (MA5)**：{data['ma5']} 元 (當前價格在 MA5 {data['ma5_relation']})
- **20 日均線 (MA20)**：{data['ma20']} 元 (當前價格在 MA20 {data['ma20_relation']})
- **60 日均線 (MA60)**：{data['ma60']} 元 (當前價格在 MA60 {data['ma60_relation']})
- **相對強弱指標 (RSI-14)**：{data['rsi14']} ({data['rsi14_status']})

## 3. 近期走勢 (最近 30 個交易日，由新到舊)
{table_header}
{history_table}

---

{section_4}
"""
    return prompt


def format_basic_prompt(data):
    prompt = f"""# {data['stock_name']} 即時基本資訊

## 1. 基本資訊與即時市況
- **名稱/代碼**：{data['stock_name']} ({data['stock_code']})
- **數據時間**：{data['data_time']} (為最新成交或更新時間)
- **當前即時價格**：`{data['current_price']}` 元
- **今日漲跌幅**：`{data['price_change']}`
- **今日開盤價**：{data['open']} 元
- **今日最高價**：{data['high']} 元
- **今日最低價**：{data['low']} 元
- **今日累計成交量**：{data['volume']} 張 (1張=1000股)
- **昨收價**：{data['yesterday_close']} 元
- **最佳五檔買單 (價格/張數)**：{data['bids']}
- **最佳五檔賣單 (價格/張數)**：{data['asks']}
"""
    return prompt


def _relation_short(relation):
    if not relation:
        return "-"
    return "上" if "之上" in relation else "下"


def _sum_recent_chip(history, days=5):
    """近 N 日外資 + 投信買賣超合計（張）。沒有任何數值就回 '-'。"""
    total = 0.0
    found = False
    for item in (history or [])[:days]:
        for key in ("foreign_net", "trust_net"):
            raw = item.get(key, "-")
            if raw in (None, "-", ""):
                continue
            try:
                total += float(str(raw).replace("+", ""))
                found = True
            except ValueError:
                continue
    if not found:
        return "-"
    return f"{total:+.1f}"


def summarize_for_compare(data):
    """把單支分析結果壓成比較表用的一行摘要。"""
    return {
        "stock_code": data.get("stock_code", "-"),
        "stock_name": data.get("stock_name", "-"),
        "current_price": data.get("current_price", "-"),
        "price_change": data.get("price_change", "-"),
        "ma_position": f"MA5{_relation_short(data.get('ma5_relation'))} / MA20{_relation_short(data.get('ma20_relation'))} / MA60{_relation_short(data.get('ma60_relation'))}",
        "rsi14": data.get("rsi14", "-"),
        "chip_5d": _sum_recent_chip(data.get("history")) if data.get("has_chip") else "-",
        "support": data.get("lower_neckline_short", "-"),
        "resistance": data.get("upper_neckline_short", "-"),
        "data_time": data.get("data_time", "-"),
        "has_chip": bool(data.get("has_chip")),
    }


def format_compare_prompt(items, web_search=True, strategy="general", position=None, horizon="swing"):
    opts = normalize_prompt_options(strategy, position, horizon, web_search)
    preset = STRATEGY_PRESETS[opts["strategy"]]
    horizon_preset = HORIZON_PRESETS[opts["horizon"]]
    kind = opts["position"]["kind"]
    has_chip = any(item.get("has_chip") for item in items)

    header = "| 代碼 / 名稱 | 現價 | 漲跌 | 均線位置 | RSI-14 | 近 5 日外資+投信 (張) | 短期支撐 | 短期壓力 |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    for item in items:
        rows.append(
            f"| {item['stock_code']} {item['stock_name']} | {item['current_price']} | {item['price_change']} | {item['ma_position']} | {item['rsi14']} | {item['chip_5d']} | {item['support']} | {item['resistance']} |"
        )

    if kind == "none":
        state_line = "我目前都尚未持有，想從中挑出最值得優先進場的標的。"
    elif kind == "holding":
        state_line = "這些都是我目前持有的股票，想知道哪些該續抱、哪些該加碼或減碼。"
    else:
        state_line = "這些是我持有且考慮減碼的股票，想知道該優先減哪一支。"

    lines = [
        f"# 多股比較提示詞（{preset['label']}）",
        "",
        f"您是一位{preset['role']}，同時是{horizon_preset['role_suffix']}。以下是 {len(items)} 支股票的即時摘要，請只用「{preset['label']}」策略橫向比較，並依照最後的格式回答。",
        "",
        "## 1. 標的摘要",
        f"- **我的狀態**：{state_line}",
        f"- **分析週期**：{horizon_preset['label']}。{horizon_preset['note']}",
        "- 均線位置欄位中「上」代表現價在該均線之上、「下」代表之下。",
    ]
    if has_chip:
        lines.append("- 近 5 日外資+投信欄位為買賣超合計，正數代表法人偏多，負數偏空；「-」代表無資料或非台股。")
    if opts["web_search"]:
        lines.append("- **可聯網時**：請主動查詢各股最新一季 EPS、營收成長與近期重大新聞，並說明是否改變排序。")
    lines += ["", header] + rows

    if preset["rules"]:
        lines += ["", "## 2. 策略規則"]
        for rule in preset["rules"]:
            lines.append(f"- {rule}")

    lines += [
        "",
        "## 3. 請嚴格依照以下格式回答",
        f"1. **排序**：依「{preset['label']}」的適配度由高到低排列，每支一句話說明（適配 / 不適配 / 接近但尚未成立）。",
    ]
    if kind == "none":
        lines.append("2. **首選**：最值得優先進場的一支，給進場區間、停損、停利。價位必須引用上表的支撐 / 壓力 / 均線數字，不可自行推估。")
        lines.append("3. **不建議**：明確點出目前不該碰的股票與原因。")
    elif kind == "holding":
        lines.append("2. **部位決策**：每支只回答「加碼 / 續抱 / 減碼」三者之一，並給執行價位，價位必須引用上表數字。")
        lines.append("3. **優先處理**：若只能動一支，先處理哪一支？為什麼？")
    else:
        lines.append("2. **減碼順序**：先減哪一支、在什麼價位減，價位必須引用上表數字。")
        lines.append("3. **可續抱**：哪一支暫時不必動，理由是什麼？")
    lines.append("4. **全部不適合時**：直接說明，不要勉強挑一支。")
    lines.append("5. **資料不足時**：說明缺少什麼資料，不要硬給方向。")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 策略燈號：把 STRATEGY_PRESETS 的文字規則真的算一次
# ---------------------------------------------------------------------------

def _f(value):
    """把 '2425.00' / '+12.5' / '-' 這類字串安全轉成 float，失敗回 None。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("+", "")
    if text in ("", "-"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pct(a, b):
    """(a - b) / b * 100，b 為 0 或 None 時回 None。"""
    if a is None or b in (None, 0):
        return None
    return (a - b) / b * 100.0


def _avg_volume(history, start=1, end=6):
    vols = [_f(item.get("volume")) for item in (history or [])[start:end]]
    vols = [v for v in vols if v is not None]
    if not vols:
        return None
    return sum(vols) / len(vols)


def _chip_streak(history, days):
    """回傳 (外資連續買超天數, 投信連續買超天數, 有幾天有資料)，只看最近 days 筆。"""
    f_streak = t_streak = 0
    f_broken = t_broken = False
    with_data = 0
    for item in (history or [])[:days]:
        f = _f(item.get("foreign_net"))
        t = _f(item.get("trust_net"))
        if f is None and t is None:
            continue
        with_data += 1
        if not f_broken and f is not None and f > 0:
            f_streak += 1
        else:
            f_broken = True
        if not t_broken and t is not None and t > 0:
            t_streak += 1
        else:
            t_broken = True
    return f_streak, t_streak, with_data


def _result(status, reasons, metrics=None):
    return {"status": status, "reasons": reasons, "metrics": metrics or {}}


def evaluate_strategy(data, strategy, position=None):
    """
    依策略規則給燈號：fit / near / no / na。
    輸入是 get_stock_analysis_data 的結果（含籌碼時 chip_follow 才能判斷）。
    """
    position = position or {}
    price = _f(data.get("current_price"))
    ma5 = _f(data.get("ma5"))
    ma20 = _f(data.get("ma20"))
    ma60 = _f(data.get("ma60"))
    history = data.get("history") or []
    today_vol = _f(history[0].get("volume")) if history else None
    avg5_vol = _avg_volume(history)
    vol_ratio = (today_vol / avg5_vol) if (today_vol is not None and avg5_vol) else None

    if strategy == "trend_ma":
        if None in (price, ma5, ma20, ma60):
            return _result("na", ["均線資料不足"])
        bullish = ma5 > ma20 > ma60
        metrics = {"ma5": f"{ma5:.2f}", "ma20": f"{ma20:.2f}", "ma60": f"{ma60:.2f}"}
        if bullish and price >= ma5:
            return _result("fit", ["MA5 > MA20 > MA60 多頭排列", "現價站上 MA5"], metrics)
        if bullish:
            return _result("near", ["均線多頭排列", f"現價 {price:.2f} 尚未站上 MA5 {ma5:.2f}"], metrics)
        if ma5 < ma20 < ma60:
            return _result("no", ["均線空頭排列，不進場"], metrics)
        return _result("no", ["均線糾結，未形成多頭排列"], metrics)

    if strategy == "pullback":
        if None in (price, ma20, ma60):
            return _result("na", ["均線資料不足"])
        if price <= ma60:
            return _result("no", [f"現價 {price:.2f} 在 MA60 {ma60:.2f} 之下，不屬於拉回"])
        neck = _f(data.get("lower_neckline_short"))
        support = max([v for v in (neck, ma20) if v is not None])
        support_name = "MA20" if support == ma20 else "第一下頸線"
        dist = _pct(price, support)
        shrink = vol_ratio is not None and vol_ratio < 1.0
        metrics = {
            "support": f"{support:.2f}",
            "support_name": support_name,
            "dist_pct": f"{dist:+.2f}" if dist is not None else "-",
            "vol_ratio": f"{vol_ratio:.2f}" if vol_ratio is not None else "-",
        }
        reasons = [f"距 {support_name} {support:.2f} 約 {dist:+.1f}%"]
        if vol_ratio is not None:
            reasons.append(f"今日量為近 5 日均量的 {vol_ratio:.2f} 倍")
        if abs(dist) <= 2 and shrink:
            return _result("fit", ["趨勢向上（現價在 MA60 之上）"] + reasons + ["量縮，符合拉回條件"], metrics)
        if abs(dist) <= 2:
            return _result("near", reasons + ["價位到了，但成交量尚未縮小"], metrics)
        if abs(dist) <= 4:
            return _result("near", reasons + ["接近支撐，尚未進入 2% 範圍"], metrics)
        return _result("no", reasons + ["離支撐太遠，還不到拉回買點"], metrics)

    if strategy == "breakout":
        upper = _f(data.get("upper_neckline_short"))
        if price is None or upper is None:
            return _result("na", ["頸線資料不足"])
        gap = _pct(price, upper)
        metrics = {
            "resistance": f"{upper:.2f}",
            "gap_pct": f"{gap:+.2f}" if gap is not None else "-",
            "vol_ratio": f"{vol_ratio:.2f}" if vol_ratio is not None else "-",
        }
        vol_text = f"今日量為近 5 日均量的 {vol_ratio:.2f} 倍" if vol_ratio is not None else "成交量資料不足"
        if price > upper and vol_ratio is not None and vol_ratio >= 1.5:
            return _result("fit", [f"現價 {price:.2f} 突破第一上頸線 {upper:.2f}", vol_text + "，帶量突破"], metrics)
        if price > upper:
            return _result("near", [f"現價已過第一上頸線 {upper:.2f}", vol_text + "，量能不足 1.5 倍"], metrics)
        if gap is not None and gap >= -1:
            return _result("near", [f"距第一上頸線 {upper:.2f} 只差 {abs(gap):.1f}%", vol_text], metrics)
        return _result("no", [f"現價 {price:.2f} 距第一上頸線 {upper:.2f} 尚有 {abs(gap):.1f}%"], metrics)

    if strategy == "chip_follow":
        if not data.get("has_chip"):
            return _result("na", ["需開啟法人籌碼才能判斷"])
        f_streak, t_streak, with_data = _chip_streak(history, 3)
        if with_data == 0:
            return _result("na", ["沒有法人籌碼資料"])
        dist = _pct(price, ma20)
        metrics = {
            "foreign_streak": str(f_streak),
            "trust_streak": str(t_streak),
            "dist_ma20_pct": f"{dist:+.2f}" if dist is not None else "-",
        }
        streak_text = f"外資連續買超 {f_streak} 日、投信連續買超 {t_streak} 日"
        dist_text = f"現價距 MA20 {dist:+.1f}%" if dist is not None else "MA20 資料不足"
        both3 = f_streak >= 3 and t_streak >= 3
        both2 = f_streak >= 2 and t_streak >= 2
        if both3 and dist is not None and dist <= 5:
            return _result("fit", [streak_text, dist_text + "，尚未大漲"], metrics)
        if both3 and dist is not None and dist <= 8:
            return _result("near", [streak_text, dist_text + "，漲幅偏高"], metrics)
        if both3:
            return _result("no", [streak_text, dist_text + "，已經噴出，不追"], metrics)
        if both2:
            return _result("near", [streak_text + "，尚未滿 3 日", dist_text], metrics)
        return _result("no", [streak_text + "，法人未同步買超"], metrics)

    if strategy == "risk_check":
        cost = position.get("cost")
        if not cost or price is None:
            return _result("na", ["未輸入持有成本"])
        pnl = _pct(price, cost)
        dist20 = _pct(price, ma20)
        metrics = {"pnl_pct": f"{pnl:+.2f}", "dist_ma20_pct": f"{dist20:+.2f}" if dist20 is not None else "-"}
        reasons = [f"成本 {cost:g}，目前 {'獲利' if pnl >= 0 else '虧損'} {abs(pnl):.1f}%"]
        if data.get("has_chip"):
            total = _sum_recent_chip(history, 3)
            metrics["chip_3d"] = total
            if total != "-":
                reasons.append(f"近 3 日外資+投信合計 {total} 張")
        if ma20 is None or ma60 is None:
            return _result("na", reasons + ["均線資料不足"], metrics)
        if price >= ma20:
            return _result("fit", reasons + [f"現價在 MA20 {ma20:.2f} 之上，趨勢仍成立"], metrics)
        if price >= ma60:
            return _result("near", reasons + [f"跌破 MA20 {ma20:.2f}，仍守住 MA60 {ma60:.2f}"], metrics)
        return _result("no", reasons + [f"跌破 MA60 {ma60:.2f}，原趨勢已失效"], metrics)

    return _result("na", [])


# ---------------------------------------------------------------------------
# 法人籌碼排行：用既有的全市場 index 快取
# ---------------------------------------------------------------------------

_chip_dates_cache = {}    # (market, want) -> (found_at, [date_str...])
CHIP_DATES_TTL = 600      # 秒；每 10 分鐘重新確認一次今天的資料有沒有出來
RANK_DAYS_ALLOWED = (1, 3, 5)
RANK_METRICS = ("foreign", "trust", "both")
RANK_LIMIT_MAX = 50


def find_latest_chip_dates(market, days, max_back=25):
    """
    從今天往回找最近 days 個有法人資料的交易日（新到舊）。
    非交易日或抓失敗時 get_market_chip_index 會回 None，剛好可以跳過。
    """
    cache_key = (market, days)
    now = time.time()
    with _cache_lock:
        cached = _chip_dates_cache.get(cache_key)
        if cached and now - cached[0] < CHIP_DATES_TTL:
            return list(cached[1])

    today = datetime.date.today()
    candidates = []
    for back in range(max_back):
        d = today - datetime.timedelta(days=back)
        if d.weekday() < 5:
            candidates.append(d.strftime("%Y%m%d"))

    found = []
    batch = 5
    for start in range(0, len(candidates), batch):
        chunk = candidates[start:start + batch]
        with ThreadPoolExecutor(max_workers=batch) as executor:
            indexes = list(executor.map(lambda d: get_market_chip_index(market, d), chunk))
        for date_str, index in zip(chunk, indexes):
            if index:
                found.append(date_str)
        if len(found) >= days:
            break

    found = found[:days]
    if found:
        with _cache_lock:
            _chip_dates_cache[cache_key] = (now, list(found))
    return found


def get_code_to_name():
    with _cache_lock:
        if _code_name_cache:
            return _code_name_cache
    get_taiwan_stock_mapping()
    with _cache_lock:
        return _code_name_cache


def _fmt_lots(value):
    return f"{value:+,.0f}"


def _rank_metric_value(metric, foreign, trust):
    """回傳 (買超側是否成立, 賣超側是否成立, 排序值)。"""
    if metric == "foreign":
        return foreign > 0, foreign < 0, foreign
    if metric == "trust":
        return trust > 0, trust < 0, trust
    return (foreign > 0 and trust > 0), (foreign < 0 and trust < 0), foreign + trust


def build_chip_rank(market, metric="foreign", days=1, exclude_etf=True, limit=20):
    days = days if days in RANK_DAYS_ALLOWED else 1
    metric = metric if metric in RANK_METRICS else "foreign"
    limit = max(1, min(int(limit or 20), RANK_LIMIT_MAX))

    # 多抓 2 天，才能算出實際連續天數
    dates = find_latest_chip_dates(market, days + 2)
    if len(dates) < days:
        return {"success": False, "error": "法人資料尚未更新或暫時無法取得，請稍後再試。"}

    indexes = [get_market_chip_index(market, d) for d in dates]
    use_dates = dates[:days]
    names = get_code_to_name()

    buy_rows, sell_rows = [], []
    for code in indexes[0].keys():
        if exclude_etf and code.startswith("00"):
            continue
        series = []
        for index in indexes:
            entry = index.get(code)
            if entry is None:
                series.append(None)
                continue
            f = _f(entry.get("foreign_net"))
            t = _f(entry.get("trust_net"))
            series.append((f or 0.0, t or 0.0))

        window = series[:days]
        if any(item is None for item in window):
            continue

        flags = [_rank_metric_value(metric, f, t) for f, t in window]
        buy_ok = all(flag[0] for flag in flags)
        sell_ok = all(flag[1] for flag in flags)
        if not buy_ok and not sell_ok:
            continue

        side = 0 if buy_ok else 1
        streak = 0
        for item in series:
            if item is None or not _rank_metric_value(metric, item[0], item[1])[side]:
                break
            streak += 1

        foreign_total = sum(f for f, _ in window)
        trust_total = sum(t for _, t in window)
        row = {
            "code": code,
            "name": names.get(code, ""),
            "foreign_net": _fmt_lots(foreign_total),
            "trust_net": _fmt_lots(trust_total),
            "total": _fmt_lots(foreign_total + trust_total),
            "streak_days": streak,
            "_sort": sum(flag[2] for flag in flags),
        }
        (buy_rows if buy_ok else sell_rows).append(row)

    buy_rows.sort(key=lambda r: r["_sort"], reverse=True)
    sell_rows.sort(key=lambda r: r["_sort"])
    for row in buy_rows + sell_rows:
        row.pop("_sort", None)

    def _pretty(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"

    return {
        "success": True,
        "market": market,
        "metric": metric,
        "days": days,
        "exclude_etf": exclude_etf,
        "dates": [_pretty(d) for d in use_dates],
        "latest_date": _pretty(use_dates[0]),
        "oldest_date": _pretty(use_dates[-1]),
        "rows": buy_rows[:limit],
        "sell": sell_rows[:limit],
    }


SECTOR_SORTS = ("chip", "change", "turnover")
SECTOR_TOP_N = 3


def build_sector_rank(market, days=1, sort="chip", limit=30):
    """
    依官方產業別把個股彙總成族群：檔數、漲跌家數、平均漲跌、成交金額、法人合計，
    並附成交金額前 3（指標股）與法人買超前 3。
    """
    days = days if days in RANK_DAYS_ALLOWED else 1
    sort = sort if sort in SECTOR_SORTS else "chip"
    limit = max(1, min(int(limit or 30), 60))

    dates = find_latest_chip_dates(market, days)
    if len(dates) < days:
        return {"success": False, "error": "法人資料尚未更新或暫時無法取得，請稍後再試。"}

    chip_indexes = [get_market_chip_index(market, d) for d in dates]
    quotes = get_market_quotes_index(market, dates[0])
    sectors = get_code_to_sector()
    names = get_code_to_name()
    with _cache_lock:
        code_market = dict(_code_market_cache)

    codes = set(chip_indexes[0].keys()) | set(quotes.keys())
    groups = {}
    missing_quotes = 0
    missing_chip = 0

    for code in codes:
        sector = sectors.get(code)
        if not sector:
            continue
        if code_market.get(code, market) != market:
            continue

        q = quotes.get(code)
        foreign = trust = 0.0
        has_chip = False
        for index in chip_indexes:
            entry = index.get(code)
            if not entry:
                continue
            has_chip = True
            foreign += _f(entry.get("foreign_net")) or 0.0
            trust += _f(entry.get("trust_net")) or 0.0
        if q is None:
            missing_quotes += 1
        if not has_chip:
            missing_chip += 1

        g = groups.get(sector)
        if g is None:
            g = groups[sector] = {
                "sector": sector, "count": 0, "up_count": 0, "down_count": 0,
                "chg_sum": 0.0, "chg_n": 0, "turnover": 0, "foreign": 0.0, "trust": 0.0, "stocks": [],
            }
        g["count"] += 1
        change_pct = q.get("change_pct") if q else None
        turnover = q.get("turnover", 0) if q else 0
        if change_pct is not None:
            g["chg_sum"] += change_pct
            g["chg_n"] += 1
            if change_pct > 0:
                g["up_count"] += 1
            elif change_pct < 0:
                g["down_count"] += 1
        g["turnover"] += turnover
        g["foreign"] += foreign
        g["trust"] += trust
        g["stocks"].append({
            "code": code,
            "name": names.get(code, ""),
            "change_pct": change_pct,
            "turnover": turnover,
            "net": foreign + trust,
        })

    def _pct_str(value):
        return f"{value:+.2f}" if value is not None else None

    def _yi(value):
        return round(value / 1e8, 1)

    rows = []
    for g in groups.values():
        avg_change = (g["chg_sum"] / g["chg_n"]) if g["chg_n"] else None
        leaders = sorted(g["stocks"], key=lambda s: s["turnover"], reverse=True)[:SECTOR_TOP_N]
        chip_top = sorted([s for s in g["stocks"] if s["net"] > 0], key=lambda s: s["net"], reverse=True)[:SECTOR_TOP_N]
        total = g["foreign"] + g["trust"]
        rows.append({
            "sector": g["sector"],
            "count": g["count"],
            "up_count": g["up_count"],
            "down_count": g["down_count"],
            "avg_change_pct": _pct_str(avg_change),
            "turnover_yi": _yi(g["turnover"]),
            "foreign_net": _fmt_lots(g["foreign"]),
            "trust_net": _fmt_lots(g["trust"]),
            "total": _fmt_lots(total),
            "leaders": [
                {"code": s["code"], "name": s["name"], "change_pct": _pct_str(s["change_pct"]), "turnover_yi": _yi(s["turnover"])}
                for s in leaders
            ],
            "chip_top": [
                {"code": s["code"], "name": s["name"], "net": _fmt_lots(s["net"]), "change_pct": _pct_str(s["change_pct"])}
                for s in chip_top
            ],
            "_chip": total,
            "_change": avg_change if avg_change is not None else float("-inf"),
            "_turnover": g["turnover"],
        })

    key = {"chip": "_chip", "change": "_change", "turnover": "_turnover"}[sort]
    # 同分時以成交金額、再以名稱決定，讓排序穩定
    rows.sort(key=lambda r: (r[key], r["_turnover"], r["sector"]), reverse=True)
    for r in rows:
        for k in ("_chip", "_change", "_turnover"):
            r.pop(k, None)

    def _pretty(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"

    return {
        "success": True,
        "market": market,
        "days": days,
        "sort": sort,
        "dates": [_pretty(d) for d in dates],
        "latest_date": _pretty(dates[0]),
        "oldest_date": _pretty(dates[-1]),
        "has_quotes": bool(quotes),
        "missing_quotes": missing_quotes,
        "missing_chip": missing_chip,
        "rows": rows[:limit],
    }


# ---------------------------------------------------------------------------
# 成交量大增排行（全市場即時快照 vs 昨量 / 近 5 日均量）
# ---------------------------------------------------------------------------

VOLUME_RANK_METRICS = ("yesterday", "avg5")
VOLUME_RANK_MARKETS = ("all", "twse", "tpex")
VOLUME_RANK_LIMIT_MAX = 100
VOLUME_RANK_MIN_VOLUME_DEFAULT = 500   # 張；今日量低於這個數字的冷門股不列入
VOLUME_AVG_DAYS = 5
VOLUME_AVG_MIN_DAYS = 3                # 至少要有 3 個交易日的日量才算均量
QUOTE_DATES_TTL = 600
_quote_dates_cache = {}  # (market, before_date) -> (ts, [dates])

_EXCHANGE_TO_MARKET = {"TSE": "twse", "OTC": "tpex"}


def find_latest_quote_dates(market, days, before_date, max_back=30):
    """
    從 before_date（不含）往回找最近 days 個有每日行情的交易日（新到舊）。
    行情檔已有檔案快取，非交易日會回空 dict，可直接跳過。
    """
    cache_key = (market, before_date.isoformat())
    now = time.time()
    with _cache_lock:
        cached = _quote_dates_cache.get(cache_key)
        if cached and now - cached[0] < QUOTE_DATES_TTL and len(cached[1]) >= days:
            return list(cached[1][:days])

    candidates = []
    for back in range(1, max_back + 1):
        d = before_date - datetime.timedelta(days=back)
        if d.weekday() < 5:
            candidates.append(d.strftime("%Y%m%d"))

    found = []
    batch = 5
    for start in range(0, len(candidates), batch):
        chunk = candidates[start:start + batch]
        with ThreadPoolExecutor(max_workers=batch) as executor:
            indexes = list(executor.map(lambda d: get_market_quotes_index(market, d), chunk))
        for date_str, index in zip(chunk, indexes):
            if index:
                found.append(date_str)
        if len(found) >= days:
            break

    found = found[:days]
    if found:
        with _cache_lock:
            _quote_dates_cache[cache_key] = (now, list(found))
    return found


def _avg_volume_lots_index(market, before_date):
    """{code: 近 5 日平均成交量（張）}；資料不足 VOLUME_AVG_MIN_DAYS 天的代碼不列入。"""
    dates = find_latest_quote_dates(market, VOLUME_AVG_DAYS, before_date)
    if len(dates) < VOLUME_AVG_MIN_DAYS:
        return {}, dates
    indexes = [get_market_quotes_index(market, d) for d in dates]
    totals = {}
    counts = {}
    for index in indexes:
        for code, q in index.items():
            vol = q.get("volume") or 0
            if vol <= 0:
                continue
            totals[code] = totals.get(code, 0) + vol
            counts[code] = counts.get(code, 0) + 1
    avg = {}
    for code, total in totals.items():
        if counts[code] >= VOLUME_AVG_MIN_DAYS:
            avg[code] = total / counts[code] / 1000.0
    return avg, dates


def build_volume_surge_rank(metric="yesterday", market="all", min_volume=None, exclude_etf=True, limit=30,
                            kind=None, group=None, force=False):
    metric = metric if metric in VOLUME_RANK_METRICS else "yesterday"
    market = market if market in VOLUME_RANK_MARKETS else "all"
    limit = max(1, min(int(limit or 30), VOLUME_RANK_LIMIT_MAX))
    group = (group or "").strip() or None
    kind = kind if kind in GROUP_RANK_KINDS else None
    # 有指定產業 / 概念時，只保留該群組裡的代碼
    allowed_codes = None
    if kind and group:
        allowed_codes = {code for code, gs in _group_membership(kind).items() if group in gs}
    try:
        min_volume = int(min_volume) if min_volume is not None else VOLUME_RANK_MIN_VOLUME_DEFAULT
    except (TypeError, ValueError):
        min_volume = VOLUME_RANK_MIN_VOLUME_DEFAULT
    min_volume = max(0, min_volume)

    try:
        snapshots, fetched_at, from_cache = sj_market.get_market_snapshots(force=force)
    except ShioajiUnavailable as e:
        return {"success": False, "error": f"無法取得永豐全市場快照：{e}"}
    if not snapshots:
        return {"success": False, "error": "永豐全市場快照為空，請稍後再試。"}

    # 以「多數股票的快照日期」判定今天是哪個交易日（少數股票的 ts 會是商品檔更新時間，不能用 max）
    date_counts = {}
    date_latest = {}
    for s in snapshots.values():
        dt = sj_market.ns_to_datetime(getattr(s, "ts", 0))
        if dt is None:
            continue
        d = dt.date()
        date_counts[d] = date_counts.get(d, 0) + 1
        if d not in date_latest or dt > date_latest[d]:
            date_latest[d] = dt
    if date_counts:
        snap_date = max(date_counts, key=lambda d: date_counts[d])
        snap_dt = date_latest[snap_date]
    else:
        snap_dt = datetime.datetime.now(tz=sj_market.TZ_TAIPEI).replace(tzinfo=None)
        snap_date = snap_dt.date()

    names = get_code_to_name()
    markets = ("twse", "tpex") if market == "all" else (market,)

    avg_index = {}
    baseline_dates = {}
    if metric == "avg5":
        for m in markets:
            avg_m, dates_m = _avg_volume_lots_index(m, snap_date)
            avg_index[m] = avg_m
            baseline_dates[m] = dates_m

    rows = []
    for code, snap in snapshots.items():
        snap_market = _EXCHANGE_TO_MARKET.get(str(snap.exchange))
        if snap_market is None or snap_market not in markets:
            continue
        if exclude_etf and code.startswith("00"):
            continue
        if allowed_codes is not None and code not in allowed_codes:
            continue
        today_volume = int(snap.total_volume or 0)
        if today_volume < min_volume:
            continue

        yesterday_volume = float(snap.yesterday_volume or 0)
        avg5 = avg_index.get(snap_market, {}).get(code)

        if metric == "yesterday":
            if yesterday_volume <= 0:
                continue
            ratio = today_volume / yesterday_volume
        else:
            if not avg5 or avg5 <= 0:
                continue
            ratio = today_volume / avg5

        close = float(snap.close or 0)
        rows.append({
            "code": code,
            "name": names.get(code, ""),
            "market": snap_market,
            "close": f"{close:.2f}" if close > 0 else "-",
            "change_rate": f"{float(snap.change_rate or 0):+.2f}",
            "today_volume": today_volume,
            "yesterday_volume": int(yesterday_volume),
            "avg5_volume": int(round(avg5)) if avg5 else None,
            "ratio": round(ratio, 2),
            "_sort": ratio,
        })

    rows.sort(key=lambda r: (r["_sort"], r["today_volume"]), reverse=True)
    for r in rows:
        r.pop("_sort", None)

    def _pretty(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"

    return {
        "success": True,
        "metric": metric,
        "market": market,
        "min_volume": min_volume,
        "exclude_etf": exclude_etf,
        "kind": kind if allowed_codes is not None else None,
        "group": group if allowed_codes is not None else None,
        "group_size": len(allowed_codes) if allowed_codes is not None else None,
        "matched": len(rows),
        "data_time": snap_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "fetched_at": datetime.datetime.fromtimestamp(fetched_at, tz=sj_market.TZ_TAIPEI).strftime("%H:%M:%S"),
        "from_cache": from_cache,
        "cache_ttl": sj_market.MARKET_SNAPSHOT_TTL,
        "baseline_dates": {m: [_pretty(d) for d in ds] for m, ds in baseline_dates.items()},
        "total_scanned": len(snapshots),
        "rows": rows[:limit],
    }


# ---------------------------------------------------------------------------
# 族群分類的股票排行（官方產業別 / 概念股，每組只列成交金額前 N 檔）
# ---------------------------------------------------------------------------

GROUP_RANK_KINDS = ("industry", "concept")
GROUP_RANK_SORTS = ("amount", "change")
GROUP_RANK_TOP_N = 30
CONCEPTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "concepts.json")
CONCEPTS_TTL = 300
_concepts_cache = {"ts": 0.0, "data": None}


def load_concepts():
    """讀 concepts.json → {概念名稱: [代碼]}；5 分鐘內重複呼叫直接回快取，檔案壞掉回空 dict。"""
    now = time.time()
    with _cache_lock:
        cached = _concepts_cache["data"]
        if cached is not None and now - _concepts_cache["ts"] < CONCEPTS_TTL:
            return cached
    data = {}
    try:
        with open(CONCEPTS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for name, codes in raw.items():
            if name.startswith("_") or not isinstance(codes, list):
                continue
            cleaned = []
            for c in codes:
                c = str(c).strip()
                if c and c not in cleaned:
                    cleaned.append(c)
            if cleaned:
                data[name] = cleaned
    except Exception as e:
        print(f"提醒：讀取概念股清單失敗。({e})", file=sys.stderr)
    with _cache_lock:
        _concepts_cache["data"] = data
        _concepts_cache["ts"] = now
    return data


def _snapshot_date_time(snapshots):
    """以多數股票的快照日期判定交易日，回傳該日最晚的 datetime。"""
    date_counts = {}
    date_latest = {}
    for s in snapshots.values():
        dt = sj_market.ns_to_datetime(getattr(s, "ts", 0))
        if dt is None:
            continue
        d = dt.date()
        date_counts[d] = date_counts.get(d, 0) + 1
        if d not in date_latest or dt > date_latest[d]:
            date_latest[d] = dt
    if date_counts:
        snap_date = max(date_counts, key=lambda d: date_counts[d])
        return date_latest[snap_date]
    return datetime.datetime.now(tz=sj_market.TZ_TAIPEI).replace(tzinfo=None)


def _group_membership(kind):
    """code -> [群組名稱]。industry 用官方產業別（一檔一組）；concept 用 concepts.json（一檔可多組）。"""
    if kind == "industry":
        sectors = get_code_to_sector()
        return {code: [sector] for code, sector in sectors.items() if sector}
    membership = {}
    for concept, codes in load_concepts().items():
        for code in codes:
            membership.setdefault(code, []).append(concept)
    return membership


def build_grouped_stock_rank(kind="industry", market="all", sort="amount", group=None, top_n=GROUP_RANK_TOP_N, force=False):
    """
    全市場快照依官方產業別或概念股分組。回傳所有群組的摘要（檔數、漲跌家數、等權平均漲跌、成交金額合計），
    以及被選取那一組（沒指定就取排序後第一組）成交金額前 top_n 檔的股票清單。
    """
    kind = kind if kind in GROUP_RANK_KINDS else "industry"
    market = market if market in VOLUME_RANK_MARKETS else "all"
    sort = sort if sort in GROUP_RANK_SORTS else "amount"
    top_n = max(1, min(int(top_n or GROUP_RANK_TOP_N), 50))
    group = (group or "").strip() or None

    try:
        snapshots, fetched_at, from_cache = sj_market.get_market_snapshots(force=force)
    except ShioajiUnavailable as e:
        return {"success": False, "error": f"無法取得永豐全市場快照：{e}"}
    if not snapshots:
        return {"success": False, "error": "永豐全市場快照為空，請稍後再試。"}

    names = get_code_to_name()
    markets = ("twse", "tpex") if market == "all" else (market,)
    membership = _group_membership(kind)

    groups = {}
    for code, snap in snapshots.items():
        group_names = membership.get(code)
        if not group_names:
            continue
        snap_market = _EXCHANGE_TO_MARKET.get(str(snap.exchange))
        if snap_market is None or snap_market not in markets:
            continue
        close = float(snap.close or 0)
        if close <= 0:
            continue
        change_rate = float(snap.change_rate or 0)
        amount = float(snap.total_amount or 0)
        row = {
            "code": code,
            "name": names.get(code, ""),
            "market": snap_market,
            "close": f"{close:.2f}",
            "change_rate": round(change_rate, 2),
            "volume": int(snap.total_volume or 0),
            "amount_yi": round(amount / 1e8, 2),
            "_amount": amount,
        }
        for g in group_names:
            bucket = groups.setdefault(g, {"stocks": [], "change_sum": 0.0, "up": 0, "down": 0, "amount": 0.0})
            bucket["stocks"].append(row)
            bucket["change_sum"] += change_rate
            bucket["amount"] += amount
            if change_rate > 0:
                bucket["up"] += 1
            elif change_rate < 0:
                bucket["down"] += 1

    result_groups = []
    for name, g in groups.items():
        count = len(g["stocks"])
        if count == 0:
            continue
        result_groups.append({
            "group": name,
            "count": count,
            "up_count": g["up"],
            "down_count": g["down"],
            "avg_change_pct": round(g["change_sum"] / count, 2),
            "amount_yi": round(g["amount"] / 1e8, 1),
        })

    if sort == "change":
        result_groups.sort(key=lambda g: (g["avg_change_pct"], g["amount_yi"]), reverse=True)
    else:
        result_groups.sort(key=lambda g: (g["amount_yi"], g["count"]), reverse=True)

    # 被選取的群組：指定名稱不存在（或沒指定）就退回排序後第一組
    selected = None
    if result_groups:
        if group is None or group not in groups:
            group = result_groups[0]["group"]
        top = sorted(groups[group]["stocks"], key=lambda r: r["_amount"], reverse=True)[:top_n]
        summary = next(g for g in result_groups if g["group"] == group)
        selected = dict(summary, stocks=[{k: v for k, v in r.items() if not k.startswith("_")} for r in top])

    snap_dt = _snapshot_date_time(snapshots)
    return {
        "success": True,
        "kind": kind,
        "market": market,
        "sort": sort,
        "top_n": top_n,
        "data_time": snap_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "fetched_at": datetime.datetime.fromtimestamp(fetched_at, tz=sj_market.TZ_TAIPEI).strftime("%H:%M:%S"),
        "from_cache": from_cache,
        "cache_ttl": sj_market.MARKET_SNAPSHOT_TTL,
        "total_groups": len(result_groups),
        "total_stocks": sum(g["count"] for g in result_groups),
        "groups": result_groups,
        "selected": selected,
    }


# Initialize Flask app
app_web = Flask(__name__)


def parse_prompt_options(args):
    """從 query string 讀取策略相關選項，缺少就用預設值。"""
    position = {
        "kind": args.get("position", "none"),
        "cost": args.get("cost"),
        "lots": args.get("lots"),
    }
    return normalize_prompt_options(
        strategy=args.get("strategy", "general"),
        position=position,
        horizon=args.get("horizon", "swing"),
        web_search=(args.get("web", "1").strip() != "0"),
    )


def _want_chip(args):
    return args.get("chip", "0").strip() == "1"


def _build_prompt_for(data, opts):
    return format_prompt(
        data,
        web_search=opts["web_search"],
        strategy=opts["strategy"],
        position=opts["position"],
        horizon=opts["horizon"],
    )

@app_web.route('/')
def index():
    return render_template('index.html')

@app_web.route('/api/analyze')
def api_analyze():
    code = request.args.get('code', '').strip()
    chip_param = request.args.get('chip', '0').strip()
    include_chip = (chip_param == '1')
    
    if not code:
        return jsonify({"success": False, "error": "請輸入有效的代碼！"})
    
    res = get_stock_analysis_data(code, include_chip=include_chip)
    if not res["success"]:
        return jsonify(res)

    _store_analysis_cache(res)
    opts = parse_prompt_options(request.args)
    res["prompt"] = _build_prompt_for(res, opts)
    res["basic_prompt"] = format_basic_prompt(res)
    res["prompt_options"] = opts
    return jsonify(res)


@app_web.route('/api/chip')
def api_chip():
    code = request.args.get('code', '').strip().upper()
    if not code:
        return jsonify({"success": False, "error": "請輸入有效的代碼！"})

    cached = _get_analysis_cache(code)
    if not cached:
        return jsonify({"success": False, "error": "請先查詢股票，再載入法人籌碼。"})
    if not cached.get("is_taiwan_stock"):
        return jsonify({"success": False, "error": "法人籌碼僅支援台股。"})

    data, inst_data = attach_chip_if_needed(cached, True)
    opts = parse_prompt_options(request.args)
    return jsonify({
        "success": True,
        "stock_code": data["stock_code"],
        "inst_by_date": inst_data,
        "history": data["history"],
        "prompt": _build_prompt_for(data, opts),
        "chip_complete": bool(data.get("chip_complete")),
    })


@app_web.route('/api/prompt')
def api_prompt():
    """只重組提示詞。不重抓股價，也不向證交所重抓法人。"""
    code = request.args.get('code', '').strip().upper()
    if not code:
        return jsonify({"success": False, "error": "請輸入有效的代碼！"})

    cached = _get_analysis_cache(code)
    if not cached:
        return jsonify({"success": False, "error": "請先查詢股票，再產生提示詞。"})

    data = apply_cached_chip(cached, _want_chip(request.args))
    opts = parse_prompt_options(request.args)
    return jsonify({
        "success": True,
        "stock_code": data["stock_code"],
        "has_chip": bool(data.get("has_chip")),
        "prompt": _build_prompt_for(data, opts),
        "prompt_options": opts,
    })


COMPARE_CACHE_TTL = 300  # 秒；5 分鐘內查過就直接用，避免重抓即時價
COMPARE_WORKERS = 5


def _load_analysis_for_compare(code):
    cached = _get_analysis_cache(code)
    if cached and (time.time() - cached.get("cached_at", 0)) < COMPARE_CACHE_TTL:
        return cached
    try:
        res = get_stock_analysis_data(code, include_chip=False)
    except Exception as e:
        print(f"提醒：比較時無法取得 {code} 的資料。({e})", file=sys.stderr)
        return {"success": False}
    if not res.get("success"):
        return res
    _store_analysis_cache(res)
    return _get_analysis_cache(res["stock_code"]) or res


@app_web.route('/api/compare')
def api_compare():
    raw_codes = request.args.get('codes', '')
    codes = []
    for part in raw_codes.split(','):
        part = part.strip().upper()
        if part and part not in codes:
            codes.append(part)

    if len(codes) < 2:
        return jsonify({"success": False, "error": "請至少選擇 2 支股票進行比較。"})

    want_chip = _want_chip(request.args)
    opts = parse_prompt_options(request.args)

    _prefetch_snapshots(codes)
    with ThreadPoolExecutor(max_workers=COMPARE_WORKERS) as executor:
        loaded = list(executor.map(_load_analysis_for_compare, codes))

    items = []
    failed = []
    for code, data in zip(codes, loaded):
        if not data or not data.get("success", True) or not data.get("stock_code"):
            failed.append(code)
            continue
        data, _ = attach_chip_if_needed(data, want_chip)
        items.append(summarize_for_compare(data))

    if len(items) < 2:
        return jsonify({
            "success": False,
            "error": "可用的股票不足 2 支，無法比較。" + (f"（失敗：{', '.join(failed)}）" if failed else "")
        })

    prompt = format_compare_prompt(
        items,
        web_search=opts["web_search"],
        strategy=opts["strategy"],
        position=opts["position"],
        horizon=opts["horizon"],
    )
    return jsonify({
        "success": True,
        "items": items,
        "failed": failed,
        "prompt": prompt,
        "prompt_options": opts,
    })


def _parse_codes(raw):
    codes = []
    for part in (raw or '').split(','):
        part = part.strip().upper()
        if part and part not in codes:
            codes.append(part)
    return codes


def _is_taiwan_code(code):
    """與 get_stock_analysis_data 相同的台股判斷（不含中文名稱）。"""
    code = str(code or '').strip().upper()
    if '.' in code:
        return code.rsplit('.', 1)[-1] in ('TW', 'TWO')
    return bool(code) and code[0].isdigit() and code.isalnum() and 4 <= len(code) <= 6


def _strip_tw_suffix(code):
    code = str(code or '').strip().upper()
    if '.' in code and code.rsplit('.', 1)[-1] in ('TW', 'TWO'):
        return code.rsplit('.', 1)[0]
    return code


def _prefetch_snapshots(codes):
    """一次把多檔台股快照抓進 sj_market 的快取，之後逐檔分析時就不會各打一次 snapshots。"""
    tw_codes = [_strip_tw_suffix(c) for c in codes if _is_taiwan_code(c)]
    if not tw_codes:
        return
    try:
        sj_market.get_snapshots(tw_codes)
    except ShioajiUnavailable as e:
        print(f"提醒：批次預抓快照失敗，改為逐檔查詢。({e})", file=sys.stderr)


@app_web.route('/api/scan')
def api_scan():
    """自選股策略掃描：每支回一個燈號與理由，不產生提示詞。"""
    codes = _parse_codes(request.args.get('codes', ''))
    if not codes:
        return jsonify({"success": False, "error": "請至少提供 1 支股票代碼。"})

    _prefetch_snapshots(codes)
    want_chip = _want_chip(request.args)
    opts = parse_prompt_options(request.args)
    strategy = opts["strategy"]
    position = opts["position"]

    def load_and_evaluate(code):
        data = _load_analysis_for_compare(code)
        if not data or not data.get("success", True) or not data.get("stock_code"):
            return code, None
        data, _ = attach_chip_if_needed(data, want_chip)
        verdict = evaluate_strategy(data, strategy, position)
        return code, {
            "stock_code": data["stock_code"],
            "stock_name": data.get("stock_name", ""),
            "current_price": data.get("current_price", "-"),
            "price_change": data.get("price_change", "-"),
            "has_chip": bool(data.get("has_chip")),
            "status": verdict["status"],
            "reasons": verdict["reasons"],
            "metrics": verdict["metrics"],
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(load_and_evaluate, codes))

    items, failed = [], []
    for code, item in results:
        if item is None:
            failed.append(code)
        else:
            items.append(item)

    return jsonify({
        "success": True,
        "strategy": strategy,
        "items": items,
        "failed": failed,
        "scanned_at": time.strftime("%H:%M:%S"),
    })


@app_web.route('/api/rank')
def api_rank():
    """法人買賣超排行（單日或連續 N 日），資料來自既有的全市場 index 快取。"""
    market = request.args.get('market', 'twse').strip().lower()
    if market not in ("twse", "tpex"):
        market = "twse"
    metric = request.args.get('metric', 'foreign').strip().lower()
    try:
        days = int(request.args.get('days', '1'))
    except ValueError:
        days = 1
    exclude_etf = request.args.get('exclude_etf', '1').strip() != '0'
    try:
        limit = int(request.args.get('limit', '20'))
    except ValueError:
        limit = 20

    try:
        result = build_chip_rank(market, metric=metric, days=days, exclude_etf=exclude_etf, limit=limit)
    except Exception as e:
        print(f"提醒：排行計算失敗。({e})", file=sys.stderr)
        return jsonify({"success": False, "error": "排行計算失敗，請稍後再試。"})
    return jsonify(result)


@app_web.route('/api/sector')
def api_sector():
    """族群輪動排行：依官方產業別彙總法人買賣超、漲跌與成交金額，附指標股。"""
    market = request.args.get('market', 'twse').strip().lower()
    if market not in ("twse", "tpex"):
        market = "twse"
    sort = request.args.get('sort', 'chip').strip().lower()
    try:
        days = int(request.args.get('days', '1'))
    except ValueError:
        days = 1
    try:
        limit = int(request.args.get('limit', '30'))
    except ValueError:
        limit = 30

    try:
        result = build_sector_rank(market, days=days, sort=sort, limit=limit)
    except Exception as e:
        print(f"提醒：族群排行計算失敗。({e})", file=sys.stderr)
        return jsonify({"success": False, "error": "族群排行計算失敗，請稍後再試。"})
    return jsonify(result)


@app_web.route('/api/volume_rank')
def api_volume_rank():
    """成交量大增排行：全市場快照（60 秒快取）對昨量或近 5 日均量的倍數。"""
    metric = request.args.get('metric', 'yesterday').strip().lower()
    market = request.args.get('market', 'all').strip().lower()
    exclude_etf = request.args.get('exclude_etf', '1').strip() != '0'
    min_volume = request.args.get('min_volume')
    kind = request.args.get('kind', '').strip().lower() or None
    group = request.args.get('group', '')
    try:
        limit = int(request.args.get('limit', '30'))
    except ValueError:
        limit = 30

    try:
        result = build_volume_surge_rank(
            metric=metric, market=market, min_volume=min_volume,
            exclude_etf=exclude_etf, limit=limit, kind=kind, group=group,
        )
    except Exception as e:
        print(f"提醒：量增排行計算失敗。({e})", file=sys.stderr)
        return jsonify({"success": False, "error": "量增排行計算失敗，請稍後再試。"})
    return jsonify(result)


@app_web.route('/api/group_rank')
def api_group_rank():
    """族群分類的股票排行：全市場快照依官方產業別或概念股分組，每組只列成交金額前 N 檔。"""
    kind = request.args.get('kind', 'industry').strip().lower()
    market = request.args.get('market', 'all').strip().lower()
    sort = request.args.get('sort', 'amount').strip().lower()
    group = request.args.get('group', '')
    try:
        top_n = int(request.args.get('top_n', str(GROUP_RANK_TOP_N)))
    except ValueError:
        top_n = GROUP_RANK_TOP_N

    try:
        result = build_grouped_stock_rank(kind=kind, market=market, sort=sort, group=group, top_n=top_n)
    except Exception as e:
        print(f"提醒：分類排行計算失敗。({e})", file=sys.stderr)
        return jsonify({"success": False, "error": "分類排行計算失敗，請稍後再試。"})
    return jsonify(result)

def start_web_server():
    import os
    port = int(os.environ.get("PORT", 5000))
    print("\n" + "="*50)
    print(f" 正在啟動網頁服務，請在瀏覽器打開：http://127.0.0.1:{port} ")
    print("="*50 + "\n")
    app_web.run(host='0.0.0.0', port=port)

def run_cli(stock_code):
    print(f"正在抓取 {stock_code} 的相關數據，請稍候...")
    res = get_stock_analysis_data(stock_code)
    if not res["success"]:
        print(f"錯誤：{res['error']}")
        return
        
    prompt = format_prompt(res, web_search=True)
    # 輸出到螢幕
    print("\n" + "="*50)
    print(" 成功生成 AI 提示詞！請複製下方框線內的內容貼給 AI ")
    print("="*50 + "\n")
    print(prompt)
    print("="*50)

def main():
    # 取得股票代碼輸入
    if len(sys.argv) > 1:
        stock_code = sys.argv[1].strip()
        # 如果是啟動網頁服務
        if stock_code.lower() in ('--web', 'web'):
            start_web_server()
            return
        
        # 否則執行 CLI 分析
        run_cli(stock_code)
    else:
        # 無參數情況
        # 檢查是否為互動式終端機
        is_interactive = sys.stdin.isatty()
        if is_interactive:
            # 互動式 CLI
            print("歡迎使用提示助手！")
            user_input = input("請輸入代碼 (例如 2330 或 AAPL)，或輸入 'web' 啟動網頁服務: ").strip()
            if not user_input:
                print("錯誤：請輸入有效的代碼或指令！")
                return
            if user_input.lower() == 'web':
                start_web_server()
            else:
                run_cli(user_input)
        else:
            # 非互動式終端機（例如 Docker 啟動，無 tty）
            # 預設啟動網頁服務
            start_web_server()

if __name__ == "__main__":
    try:
        main()
    finally:
        # 結束前主動登出永豐連線，避免佔用連線數
        sj_market.logout()
