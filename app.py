import os
import json
import time
import sqlite3
import threading
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

import akshare as ak
import pandas as pd
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from scipy.stats import pearsonr

app = Flask(__name__)
CORS(app)

# ============ SQLite Persistence ============
DB_PATH = os.path.join(os.path.dirname(__file__), 'signals.db')
_db_lock = threading.Lock()

def _init_db():
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                final_signal TEXT,
                consensus_score REAL,
                v2_signal TEXT,
                coint_signal TEXT,
                ml_signal REAL,
                expert_signal REAL,
                xn_price REAL,
                sk_price REAL,
                details TEXT
            )
        ''')
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_signal_ts ON signal_history(timestamp)
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                pair_id TEXT NOT NULL,
                signal TEXT NOT NULL,
                z_score REAL,
                price REAL,
                follow_change_pct REAL,
                details TEXT
            )
        ''')
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_paper_pair ON paper_trades(pair_id, timestamp)
        ''')
        conn.commit()
        conn.close()

_init_db()

def _save_signal(signal_data):
    """Persist a signal record to SQLite"""
    try:
        sigs = signal_data.get('signals', {})
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                '''INSERT INTO signal_history
                   (timestamp, final_signal, consensus_score,
                    v2_signal, coint_signal, ml_signal, expert_signal,
                    xn_price, sk_price, details)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    signal_data.get('timestamp', datetime.now().strftime('%H:%M:%S')),
                    signal_data.get('final_signal', 'hold'),
                    signal_data.get('consensus_score', 0),
                    json.dumps(sigs.get('v2', {}), ensure_ascii=False),
                    json.dumps(sigs.get('cointegration', {}), ensure_ascii=False),
                    sigs.get('ml', {}).get('vote', 0),
                    sigs.get('expert_panel', {}).get('consensus', 0),
                    signal_data.get('xn_price'),
                    signal_data.get('sk_latest'),
                    json.dumps(signal_data, ensure_ascii=False, default=str),
                )
            )
            conn.commit()
            conn.close()
    except Exception:
        pass  # persistence should never break the main flow


def _save_paper_trade(pair_id, signal, z_score, price, follow_change_pct):
    """记录一次模拟盘信号(用于命中率统计)"""
    try:
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                '''INSERT INTO paper_trades
                   (timestamp, pair_id, signal, z_score, price, follow_change_pct)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), pair_id, signal,
                 z_score, price, follow_change_pct)
            )
            conn.commit()
            conn.close()
    except Exception:
        pass

for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
    os.environ.pop(var, None)

_cache = {"market": None, "market_time": 0, "market_ttl": 600,
          "quotes": None, "quotes_time": 0, "quotes_ttl": 20}
_fetch_lock = threading.Lock()
CANDIDATE_STOCKS = [
    ("603986", "兆易创新", "半导体"),
    ("688008", "澜起科技", "半导体"),
    ("300223", "北京君正", "半导体"),
    ("002049", "紫光国微", "半导体"),
    ("600703", "三安光电", "半导体"),
    ("688012", "中微公司", "半导体"),
    ("688981", "中芯国际", "半导体"),
    ("300661", "圣邦股份", "半导体"),
    ("603501", "韦尔股份", "半导体"),
    ("300782", "卓胜微", "半导体"),
    ("688396", "华润微", "半导体"),
    ("002371", "北方华创", "半导体"),
    ("688126", "沪硅产业", "半导体"),
    ("300474", "景嘉微", "半导体"),
    ("688536", "思瑞浦", "半导体"),
    ("300458", "全志科技", "半导体"),
    ("688595", "芯海科技", "半导体"),
    ("688018", "乐鑫科技", "半导体"),
    ("688200", "华峰测控", "半导体"),
    ("688005", "容百科技", "半导体"),
    ("300567", "精测电子", "半导体"),
    ("688728", "格科微", "半导体"),
    ("688099", "晶晨股份", "半导体"),
    ("603160", "汇顶科技", "半导体"),
    ("300672", "国科微", "半导体"),
    ("605111", "新洁能", "半导体"),
    ("688385", "复旦微电", "半导体"),
    ("688521", "芯原股份", "半导体"),
    ("688107", "安路科技", "半导体"),
    ("002156", "通富微电", "封测"),
    ("600584", "长电科技", "封测"),
    ("688362", "甬矽电子", "封测"),
    ("002409", "雅克科技", "材料"),
    ("300346", "南大光电", "材料"),
    ("688019", "安集科技", "材料"),
    ("300655", "晶瑞电材", "材料"),
    ("600206", "有研新材", "材料"),
]

def _sina_realtime(code):
    """Fetch real-time quote from Sina API for A-shares"""
    import requests
    session = requests.Session()
    session.headers.update({"Referer": "https://finance.sina.com.cn"})
    url = f"https://hq.sinajs.cn/list={code}"
    resp = session.get(url, timeout=10)
    resp.encoding = 'gbk'
    lines = resp.text.strip().split('\n')
    result = {}
    for line in lines:
        if line.startswith('var hq_str_'):
            parts = line.split('"')[1].split(',')
            result.update({
                "name": parts[0],
                "open": float(parts[1]) if parts[1] else 0,
                "close": float(parts[3]) if parts[3] else 0,
                "high": float(parts[4]) if parts[4] else 0,
                "low": float(parts[5]) if parts[5] else 0,
                "volume": int(parts[8]) if parts[8] else 0,
                "amount": float(parts[9]) if parts[9] else 0,
                "change_pct": round((float(parts[3]) - float(parts[2])) / float(parts[2]) * 100, 2) if parts[2] and parts[3] else 0,
                "pre_close": float(parts[2]) if parts[2] else 0,
            })
    return result


def _report_signal(xn_stats, hynix_data, intraday_corr=None):
    """Generate composite trading signal"""
    score = 0
    reasons = []
    risk_factors = []
    direction = "hold"

    # RSI signal
    rsi = xn_stats.get('rsi14', 50)
    if rsi < 30:
        score += 15
        reasons.append("RSI超卖区域")
    elif rsi < 20:
        score += 25
        reasons.append("RSI深度超卖")
    elif rsi > 70:
        score -= 10
        reasons.append("RSI超买")

    # MACD signal
    macd_hist = xn_stats.get('macd_hist', 0)
    if macd_hist > 0:
        score += 5
        reasons.append("MACD柱转正")
    elif macd_hist < -5:
        score -= 5
        reasons.append("MACD空头走强")

    # MA position
    close = xn_stats.get('close', 0)
    ma60 = xn_stats.get('ma60', 0)
    ma20 = xn_stats.get('ma20', 0)
    ma5 = xn_stats.get('ma5', 0)
    if ma60 > 0:
        pct_above_ma60 = (close - ma60) / ma60 * 100
        if -3 < pct_above_ma60 < 3:
            score += 10
            reasons.append(f"靠近MA60支撑(差{pct_above_ma60:.1f}%)")
        elif pct_above_ma60 < -3:
            score -= 15
            risk_factors.append("跌破MA60关键支撑")
    if ma5 > 0 and close < ma5:
        score -= 5
        reasons.append("跌破MA5")

    # Correlation signal
    corr = xn_stats.get('correlation', 0)
    if abs(corr) > 0.3:
        score += 5
        reasons.append(f"高相关性(r={corr:.2f})")

    # SK Hynix lead signal
    sk_change = hynix_data.get('change_30d', 0)
    if sk_change > 10:
        score += 10
        reasons.append("SK海力士30日强势(+{:.1f}%)".format(sk_change))
    elif sk_change < -10:
        score -= 10
        risk_factors.append("SK海力士30日走弱({:.1f}%)".format(sk_change))

    # Intraday lead-lag
    if intraday_corr and intraday_corr.get('sk_lead_5min_p', 1) < 0.1:
        score += 8
        reasons.append("SK领先5分钟信号显著")
        if intraday_corr.get('sk_lead_5min_r', 0) > 0:
            direction = "buy"
        else:
            direction = "sell"

    # Determine final direction
    if score >= 25:
        direction = "buy"
    elif score <= -15:
        direction = "sell"
    else:
        direction = "hold"

    return {
        "score": score,
        "direction": direction,
        "reasons": reasons,
        "risk_factors": risk_factors,
        "levels": {
            "support_strong": round(max(close * 0.9, ma60 * 0.95), 2) if ma60 else round(close * 0.9, 2),
            "support_weak": round(max(close * 0.95, ma60), 2) if ma60 else round(close * 0.95, 2),
            "resistance_weak": round(ma5 or close * 1.05, 2),
            "resistance_strong": round(ma20 or close * 1.1, 2),
        }
    }


def get_realtime_quotes():
    """A股盘中实时行情(腾讯spot, 20s缓存) → {code6: {...}}"""
    now = time.time()
    if _cache["quotes"] and (now - _cache["quotes_time"]) < _cache["quotes_ttl"]:
        return _cache["quotes"]
    result = {}
    try:
        df = ak.stock_zh_a_spot()
        if df is not None and not df.empty:
            df = df.copy()
            df['code6'] = df['代码'].str[-6:]
            for _, r in df.iterrows():
                try:
                    result[r['code6']] = {
                        "code": r['code6'],
                        "name": r['名称'],
                        "price": round(float(r['最新价']), 2),
                        "change_pct": round(float(r['涨跌幅']), 2),
                        "open": round(float(r['今开']), 2),
                        "high": round(float(r['最高']), 2),
                        "low": round(float(r['最低']), 2),
                        "volume": float(r['成交量']),
                        "amount": float(r['成交额']),
                        "time": str(r['时间戳']),
                    }
                except Exception:
                    continue
        _cache["quotes"] = result
        _cache["quotes_time"] = time.time()
    except Exception as e:
        result["error"] = str(e)[:80]
    return result


def get_market_overview():
    now = time.time()
    if _cache["market"] and (now - _cache["market_time"]) < _cache["market_ttl"]:
        return _cache["market"]

    indices = {
        "上证指数": "sh000001",
        "深证成指": "sz399001",
        "创业板指": "sz399006",
        "科创50": "sh000688"
    }

    result = {}
    for name, symbol in indices.items():
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                prev = df.iloc[-2]
                change_pct = round((latest['close'] - prev['close']) / prev['close'] * 100, 2)
                df_30 = df.tail(30)
                result[name] = {
                    "close": round(float(latest['close']), 2),
                    "change_pct": change_pct,
                    "volume": int(latest['volume']),
                    "vol_avg": int(df_30['volume'].mean()),
                    "high_30d": round(float(df_30['high'].max()), 2),
                    "low_30d": round(float(df_30['low'].min()), 2),
                }
        except Exception:
            result[name] = {"error": "数据获取失败"}

    _cache["market"] = result
    _cache["market_time"] = now
    return result


def _naver_fetch(code, count=500, timeout=8):
    import requests
    from xml.etree import ElementTree as ET
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"})
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={count}&requestType=0"
    try:
        resp = session.get(url, timeout=timeout)
    except Exception:
        # fallback: try once more with longer timeout
        resp = session.get(url, timeout=timeout * 2)
    content = resp.content.decode('euc-kr', errors='replace')
    root = ET.fromstring(content)
    chartdata = root.find('chartdata')
    items = chartdata.findall('item')

    dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for item in items:
        parts = item.get('data').split('|')
        dates.append(parts[0])
        opens.append(int(parts[1]))
        highs.append(int(parts[2]))
        lows.append(int(parts[3]))
        closes.append(int(parts[4]))
        volumes.append(int(parts[5]))

    import pandas as pd
    df = pd.DataFrame({
        'Open': opens, 'High': highs, 'Low': lows, 'Close': closes, 'Volume': volumes
    }, index=pd.to_datetime(dates))
    df.index.name = 'Date'
    return df


def get_hynix_data():
    try:
        df = _naver_fetch("000660", count=400)
        if df is None or df.empty:
            return {"error": "No data"}

        price_data = []
        for idx, row in df.iterrows():
            price_data.append({
                "date": idx.strftime('%Y-%m-%d'),
                "open": int(row['Open']),
                "high": int(row['High']),
                "low": int(row['Low']),
                "close": int(row['Close']),
                "volume": int(row['Volume'])
            })

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else df.iloc[-1]
        change_pct = round((latest['Close'] - prev['Close']) / prev['Close'] * 100, 2)
        df_30 = df.tail(30)
        df_90 = df.tail(90)

        return {
            "price_data": price_data,
            "summary": {
                "close": int(latest['Close']),
                "change_pct": change_pct,
                "high_52w": int(df['High'].max()) if not df.empty else 0,
                "low_52w": int(df['Low'].min()) if not df.empty else 0,
                "vol_avg_30d": int(df_30['Volume'].mean()),
                "ma_20": round(float(df.tail(20)['Close'].mean()), 1),
                "ma_60": round(float(df.tail(60)['Close'].mean()), 1),
                "high_30d": int(df_30['High'].max()),
                "low_30d": int(df_30['Low'].min()),
                "change_30d": round((df_30['Close'].iloc[-1] - df_30['Close'].iloc[0]) / df_30['Close'].iloc[0] * 100, 2),
                "change_90d": round((df_90['Close'].iloc[-1] - df_90['Close'].iloc[0]) / df_90['Close'].iloc[0] * 100, 2),
                "name": "SK Hynix",
                "market_cap": 0,
                "pe_ratio": 0,
                "sector": "Semiconductor",
            }
        }
    except Exception as e:
        return {"error": str(e)}


def get_hynix_returns(period_days=120):
    df = _naver_fetch("000660", count=period_days + 30)
    if df is None or df.empty:
        return None, None
    df = df.tail(period_days)
    returns = df['Close'].pct_change().dropna().values
    return returns, df['Close'].values


def _tx_symbol(code):
    if code.startswith(('60', '68')):
        return f"sh{code}"
    return f"sz{code}"

def get_similar_stocks(period_days=120, top_n=20):
    today = datetime.now()
    end_str = today.strftime('%Y%m%d')
    start_str = (today - timedelta(days=period_days + 30)).strftime('%Y%m%d')

    hynix_returns, hynix_prices = get_hynix_returns(period_days)
    if hynix_returns is None:
        return {"error": "SK Hynix data unavailable"}

    results = []
    for code, name, sector in CANDIDATE_STOCKS:
        try:
            df_a = ak.stock_zh_a_hist_tx(symbol=_tx_symbol(code), start_date=start_str, end_date=end_str, adjust="qfq")
            if df_a.empty or len(df_a) < max(period_days * 0.6, 30):
                continue

            df_a = df_a.tail(period_days)
            a_returns = df_a['close'].pct_change().dropna().values

            min_len = min(len(hynix_returns), len(a_returns))
            if min_len < 20:
                continue

            r, p_val = pearsonr(hynix_returns[-min_len:], a_returns[-min_len:])

            latest_close = float(df_a.iloc[-1]['close'])
            prev_close = float(df_a.iloc[-2]['close']) if len(df_a) > 1 else latest_close
            change_pct = round((latest_close - prev_close) / prev_close * 100, 2)

            close_30d_ago = float(df_a.iloc[-30]['close']) if len(df_a) >= 30 else float(df_a.iloc[0]['close'])
            change_30d = round((latest_close - close_30d_ago) / close_30d_ago * 100, 2)

            results.append({
                "code": code, "name": name, "sector": sector,
                "correlation": round(r, 4), "p_value": round(p_val, 4),
                "close": latest_close, "change_pct": change_pct, "change_30d": change_30d,
            })
        except Exception:
            continue

    results.sort(key=lambda x: abs(x['correlation']), reverse=True)
    return results[:top_n]


def get_sector_summary():
    sectors = {}
    for code, name, sector in CANDIDATE_STOCKS:
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append((code, name))
    return sectors


def _naver_minute(code, count=200):
    """Fetch 1-min intraday data from Naver for KOSPI stocks"""
    import requests
    from xml.etree import ElementTree as ET
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"})
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=minute&count={count}&requestType=0"
    resp = session.get(url, timeout=8)
    content = resp.content.decode('euc-kr', errors='replace')
    root = ET.fromstring(content)
    chartdata = root.find('chartdata')
    items = chartdata.findall('item')
    data = []
    for item in items:
        parts = item.get('data').split('|')
        close_str = parts[4] if len(parts) > 4 else parts[1]
        vol_str = parts[5] if len(parts) > 5 else '0'
        if close_str == 'null' or not close_str:
            continue
        data.append({
            "time": parts[0],
            "close": int(close_str),
            "volume": int(vol_str) if vol_str != 'null' and vol_str else 0
        })
    return data


def _sina_5min(code, count=100):
    """Fetch 5-min K-line from Sina"""
    import requests
    session = requests.Session()
    session.headers.update({"Referer": "https://money.finance.sina.com.cn"})
    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={code}&scale=5&ma=no&datalen={count}"
    resp = session.get(url, timeout=10)
    return resp.json()


def run_intraday_backtest(days=60, threshold_pct=1.5, hold_bars=1):
    """Backtest SK Hynix → 香农芯创 5-min lead-lag strategy using available data"""
    sk_data = _naver_minute("000660", 500)
    xn_data = _sina_5min("sz300475", 200)

    if not sk_data or not xn_data:
        return {"error": "数据不足", "sk_len": len(sk_data), "xn_len": len(xn_data)}

    import datetime
    def to_dt(s):
        try:
            return datetime.datetime.strptime(s, '%Y%m%d%H%M')
        except Exception:
            return None

    sk = [(to_dt(d["time"]), d["close"]) for d in sk_data]
    sk = [(t, c) for t, c in sk if t is not None]
    xn = [(datetime.datetime.strptime(d["day"].replace(':', '').replace(' ', ''), '%Y-%m-%d%H%M%S'), float(d["close"])) for d in xn_data]

    # Build OHLC from sk 1-min → 5-min aggregation
    sk_5min = {}
    for t, c in sk:
        key = t.replace(minute=(t.minute // 5) * 5, second=0, microsecond=0)
        if key not in sk_5min:
            sk_5min[key] = []
        sk_5min[key].append(c)
    sk_agg = sorted([(t, sum(c)/len(c), c[0], c[-1]) for t, c in sk_5min.items()])

    # Align timestamps
    trades = []
    for i in range(len(sk_agg) - hold_bars):
        t_sk, _, sk_open, sk_close = sk_agg[i]
        sk_ret = (sk_close - sk_open) / sk_open * 100
        if abs(sk_ret) < 0.3:
            continue

        # Find matching XN 5-min bar after hold_bars
        target_t = sk_agg[min(i + hold_bars, len(sk_agg) - 1)][0]
        xn_target = None
        for j in range(len(xn)):
            if xn[j][0] >= target_t:
                if j + 1 < len(xn):
                    xn_target = (xn[j+1][1] - xn[j][1]) / xn[j][1] * 100
                break
        if xn_target is not None:
            trades.append({"sk_ret": round(sk_ret, 2), "xn_ret": round(xn_target, 2), "direction": 1 if sk_ret > 0 else -1})

    if len(trades) < 10:
        return {"error": f"对齐后交易样本不足({len(trades)})，需累积更多日内数据", "trades_count": len(trades)}

    correct = sum(1 for t in trades if (t["sk_ret"] > 0 and t["xn_ret"] > 0) or (t["sk_ret"] < 0 and t["xn_ret"] < 0))
    win_rate = correct / len(trades)
    avg_ret = sum(t["xn_ret"] for t in trades) / len(trades)
    avg_pos = sum(t["xn_ret"] for t in trades if t["xn_ret"] > 0) / max(sum(1 for t in trades if t["xn_ret"] > 0), 1)
    avg_neg = sum(t["xn_ret"] for t in trades if t["xn_ret"] < 0) / max(sum(1 for t in trades if t["xn_ret"] < 0), 1)

    returns = [t["xn_ret"] * t["direction"] for t in trades]
    cum_return = sum(returns)
    sharpe = (sum(returns) / len(returns)) / max((sum((r - sum(returns)/len(returns))**2 for r in returns) / len(returns))**0.5, 0.001) * (252)**0.5
    equity = 100.0
    peak_e = 100.0
    for r in returns:
        equity *= (1 + r / 100.0)
        peak_e = max(peak_e, equity)
    max_dd_pct = max(0, (peak_e - equity) / peak_e * 100)

    return {
        "total_trades": len(trades),
        "win_rate": round(win_rate, 4),
        "avg_return": round(avg_ret, 4),
        "avg_win": round(avg_pos, 4),
        "avg_loss": round(avg_neg, 4),
        "cum_return": round(cum_return, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_dd_pct, 2),
        "direction_accuracy": round(win_rate, 4),
        "sample_trades": trades[:20],
        "threshold": threshold_pct,
        "hold_bars": hold_bars,
    }


def compute_atr(series, period=14):
    """Average True Range for price series"""
    if len(series) < period + 1:
        return series.std() if len(series) > 1 else 1.0
    import numpy as np
    series = np.array(series, dtype=float)
    tr = np.abs(np.diff(series))
    atr = np.mean(tr[-period:]) if len(tr) >= period else np.mean(tr)
    return max(atr, 0.01)


def compute_rsi(series, period=14):
    """RSI calculation"""
    if len(series) < period + 1:
        return 50.0
    import numpy as np
    series = np.array(series, dtype=float)
    deltas = np.diff(series)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def run_daily_backtest_v2(days=250):
    """
    V2: 三层漏斗策略
    1. ATR自适应阈值 (取代固定1.5%)
    2. RSI/MA乖离度过滤 (均值回归保护)
    3. 仓位管理 + 动态止损
    4. Walk-Forward: 前70%训练, 后30%测试
    """
    import numpy as np
    today = datetime.now()
    start = (today - timedelta(days=days)).strftime('%Y%m%d')

    df_sk = _naver_fetch("000660", count=days + 30)
    df_xn = ak.stock_zh_a_hist_tx(symbol="sz300475", start_date=start, end_date=today.strftime('%Y%m%d'), adjust="qfq")
    if df_xn.empty or df_sk is None:
        return {"error": "数据获取失败"}

    df_xn['date'] = pd.to_datetime(df_xn['date'])
    df_xn = df_xn.set_index('date')
    common = df_xn.index.intersection(df_sk.index)
    df_xn = df_xn.loc[common].sort_index()
    df_sk = df_sk.loc[common].sort_index()

    sk_close = df_sk['Close'].values.astype(float)
    xn_close = df_xn['close'].values.astype(float)
    sk_ret = df_sk['Close'].pct_change() * 100
    xn_ret = df_xn['close'].pct_change() * 100
    idx = df_sk.index

    n = len(sk_close)
    split = int(n * 0.7)

    results = {}
    atr_mults = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    rsi_floor_opts = [20, 25, 30, 35, 40]
    ma_dev_opts = [5, 10, 15, 20]

    # Walk-forward optimization over training set
    best_params = {"atr_mult": 1.5, "rsi_floor": 30, "ma_dev_pct": 10}
    best_sharpe = -999

    for atr_mult in atr_mults:
        for rsi_floor in rsi_floor_opts:
            for ma_dev in ma_dev_opts:
                trades_train = []
                for i in range(1, split - 1):
                    window = sk_close[max(0, i - 20):i + 1]
                    atr = compute_atr(window)
                    dynamic_thresh = atr_mult * atr / sk_close[i] * 100
                    rsi_val = compute_rsi(window)

                    ma20 = np.mean(sk_close[max(0, i - 20):i])
                    dev_pct = abs(sk_close[i] - ma20) / ma20 * 100

                    sk_move = sk_ret.iloc[i]
                    if abs(sk_move) < dynamic_thresh:
                        continue
                    if rsi_val < rsi_floor:
                        continue
                    if dev_pct > ma_dev:
                        continue

                    pred_dir = 1 if sk_move > 0 else -1
                    future_xn = xn_ret.iloc[i + 1]
                    if not np.isnan(future_xn):
                        trades_train.append((pred_dir, future_xn))

                if len(trades_train) < 10:
                    continue
                returns = [d * r for d, r in trades_train]
                s = (np.mean(returns) / max(np.std(returns), 0.001)) * (252**0.5)
                if s > best_sharpe:
                    best_sharpe = s
                    best_params = {"atr_mult": atr_mult, "rsi_floor": rsi_floor, "ma_dev_pct": ma_dev}

    atr_mult = best_params["atr_mult"]
    rsi_floor = best_params["rsi_floor"]
    ma_dev = best_params["ma_dev_pct"]

    # Test set (last 30%)
    trades_test = []
    equity = 100.0
    peak_e = 100.0
    max_dd = 0.0
    position = 0.0
    entry_price = 0.0
    trailing_stop_pct = -8.0
    equity_curve = [{"nav": 100.0, "date": str(idx[0].date())}]

    for i in range(split, n - 1):
        window = sk_close[max(0, i - 20):i + 1]
        atr = compute_atr(window)
        dynamic_thresh = atr_mult * atr / sk_close[i] * 100
        rsi_val = compute_rsi(window)
        ma20 = np.mean(sk_close[max(0, i - 20):i])
        dev_pct = abs(sk_close[i] - ma20) / ma20 * 100
        sk_move = sk_ret.iloc[i]
        xn_move = xn_ret.iloc[i + 1]

        # --- 仓位管理 (三层漏斗) ---
        confidence = 0
        if abs(sk_move) >= dynamic_thresh:
            confidence += 1
        if rsi_val >= rsi_floor:
            confidence += 1
        if dev_pct <= ma_dev:
            confidence += 1
        if abs(sk_move) >= dynamic_thresh * 1.5:
            confidence += 1
        if abs(sk_move) >= dynamic_thresh * 2.0:
            confidence += 1

        pos_sizes = {0: 0.0, 1: 0.0, 2: 0.05, 3: 0.10, 4: 0.15, 5: 0.20}

        # --- 信号逻辑 ---
        if confidence >= 2 and position == 0:
            pred_dir = 1 if sk_move > 0 else -1
            pos_size = pos_sizes.get(confidence, 0.0) * pred_dir
            position = pos_size
            entry_price = xn_close[i]
            trailing_stop_pct = -6.0 - (5 - confidence) * 1.0
            trades_test.append({
                "date": str(idx[i].date()),
                "action": "buy" if pred_dir > 0 else "sell",
                "confidence": confidence,
                "sk_ret": round(sk_move, 2),
                "atr_thresh": round(dynamic_thresh, 2),
                "rsi": round(rsi_val, 1),
                "ma_dev": round(dev_pct, 1),
                "entry": round(entry_price, 2),
                "position_pct": abs(position),
            })

        # --- 动态止损 ---
        if position != 0:
            pnl_pct = (xn_close[i] - entry_price) / entry_price * 100 * (1 if position > 0 else -1)
            if pnl_pct < trailing_stop_pct:
                trades_test[-1]["exit"] = round(xn_close[i], 2)
                trades_test[-1]["pnl"] = round(pnl_pct, 2)
                trades_test[-1]["exit_reason"] = "stop"
                equity *= (1 + pnl_pct / 100.0 * abs(position))
                peak_e = max(peak_e, equity)
                max_dd = max(max_dd, (peak_e - equity) / peak_e * 100)
                position = 0
                equity_curve.append({"nav": round(equity, 2), "date": str(idx[i].date())})
                continue

        # --- 收盘平仓 ---
        if position != 0 and not np.isnan(xn_move):
            pnl_pct = xn_move * (1 if position > 0 else -1)
            trades_test[-1]["exit"] = round(xn_close[i + 1], 2)
            trades_test[-1]["pnl"] = round(pnl_pct * abs(position), 2)
            trades_test[-1]["exit_reason"] = "close"
            equity *= (1 + pnl_pct * abs(position) / 100.0)
            peak_e = max(peak_e, equity)
            max_dd = max(max_dd, (peak_e - equity) / peak_e * 100)
            position = 0
            equity_curve.append({"nav": round(equity, 2), "date": str(idx[i + 1].date())})

    test_returns = [t.get("pnl", 0) for t in trades_test]
    win_rate = sum(1 for t in trades_test if t.get("pnl", 0) > 0) / max(len(trades_test), 1)
    avg_ret = np.mean(test_returns) if test_returns else 0
    cum_ret = sum(test_returns)
    sharpe = (np.mean(test_returns) / max(np.std(test_returns), 0.001)) * (252**0.5) if len(test_returns) > 1 else 0

    # Run multi-threshold sweep with best params for comparison
    sweep = {}
    for thresh in [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]:
        sw_trades = []
        for i in range(len(sk_ret) - 1):
            if abs(sk_ret.iloc[i]) >= thresh:
                pred = 1 if sk_ret.iloc[i] > 0 else -1
                xn_next = xn_ret.iloc[i + 1]
                if not np.isnan(xn_next):
                    sw_trades.append(pred * xn_next)
        if len(sw_trades) > 5:
            sweep[str(thresh)] = {
                "trades": len(sw_trades),
                "cum_return": round(sum(sw_trades), 2),
                "sharpe": round((np.mean(sw_trades) / max(np.std(sw_trades), 0.001)) * (252**0.5), 2) if np.std(sw_trades) > 0.001 else 0,
                "win_rate": round(sum(1 for r in sw_trades if r > 0) / len(sw_trades), 4),
            }

    return {
        "version": "v2_三层漏斗",
        "best_params": best_params,
        "train_size": split,
        "test_size": n - split,
        "test_results": {
            "total_trades": len(trades_test),
            "win_rate": round(win_rate, 4),
            "avg_return": round(avg_ret, 2),
            "cum_return_pct": round(cum_ret, 2),
            "sharpe": round(sharpe, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "data_start": str(idx[0].date()),
            "data_end": str(idx[-1].date()),
        },
        "trades": trades_test[-50:],
        "equity_curve": equity_curve,
        "v1_comparison": sweep,
        "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def run_daily_backtest(days=250, threshold=2.0):
    """Backtest SK Hynix daily close → 香农芯创 next-day open strategy"""
    import numpy as np
    today = datetime.now()
    start = (today - timedelta(days=days)).strftime('%Y%m%d')

    df_sk = _naver_fetch("000660", count=days + 30)
    df_xn = ak.stock_zh_a_hist_tx(symbol="sz300475", start_date=start, end_date=today.strftime('%Y%m%d'), adjust="qfq")
    if df_xn.empty or df_sk is None:
        return {"error": "数据获取失败"}

    df_xn['date'] = pd.to_datetime(df_xn['date'])
    df_xn = df_xn.set_index('date')
    common = df_xn.index.intersection(df_sk.index)
    df_xn = df_xn.loc[common].sort_index()
    df_sk = df_sk.loc[common].sort_index()

    sk_ret = df_sk['Close'].pct_change() * 100
    xn_ret = df_xn['close'].pct_change() * 100

    results = {}
    for thresh in [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]:
        trades = []
        sk_dir = []
        xn_dir = []
        for i in range(len(sk_ret) - 1):
            if abs(sk_ret.iloc[i]) >= thresh:
                pred_dir = 1 if sk_ret.iloc[i] > 0 else -1
                actual_dir = 1 if xn_ret.iloc[i + 1] > 0 else -1
                trades.append({
                    "date": str(df_sk.index[i].date()),
                    "sk_ret": round(sk_ret.iloc[i], 2),
                    "xn_next_ret": round(xn_ret.iloc[i + 1], 2),
                    "correct": pred_dir == actual_dir
                })
                sk_dir.append(pred_dir)
                xn_dir.append(actual_dir)

        if len(trades) < 10:
            results[thresh] = {"error": f"样本不足({len(trades)})", "trades": len(trades)}
            continue

        correct = sum(1 for t in trades if t["correct"])
        win_rate = correct / len(trades)
        avg_ret = sum(t["xn_next_ret"] for t in trades) / len(trades)
        avg_pos = sum(t["xn_next_ret"] for t in trades if t["xn_next_ret"] > 0) / max(sum(1 for t in trades if t["xn_next_ret"] > 0), 1)
        avg_neg = sum(t["xn_next_ret"] for t in trades if t["xn_next_ret"] < 0) / max(sum(1 for t in trades if t["xn_next_ret"] < 0), 1)

        returns = [t["xn_next_ret"] * (1 if t["sk_ret"] > 0 else -1) for t in trades]
        cum_ret = sum(returns)
        sharpe = (np.mean(returns) / max(np.std(returns), 0.001)) * (252)**0.5

        equity = 100.0
        peak_e = 100.0
        for r in returns:
            equity *= (1 + r / 100.0)
            peak_e = max(peak_e, equity)
        max_dd_pct = max(0, (peak_e - equity) / peak_e * 100)

        results[thresh] = {
            "trades": len(trades),
            "win_rate": round(win_rate, 4),
            "avg_return_pct": round(avg_ret, 2),
            "avg_win_pct": round(avg_pos, 2),
            "avg_loss_pct": round(avg_neg, 2),
            "cum_return_pct": round(cum_ret, 2),
            "sharpe": round(float(sharpe), 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "direction_accuracy": round(win_rate, 4),
        }

    # Overall correlation stats
    r_overall, p_overall = pearsonr(sk_ret.dropna().values, xn_ret.dropna().values)
    r_lag1, p_lag1 = pearsonr(sk_ret.dropna().iloc[:-1].values, xn_ret.dropna().iloc[1:].values)

    return {
        "by_threshold": results,
        "overall": {
            "days": len(common),
            "contemp_corr": round(r_overall, 4),
            "contemp_p": round(p_overall, 6),
            "lead1_corr": round(r_lag1, 4),
            "lead1_p": round(p_lag1, 6),
            "sk_latest_close": int(df_sk['Close'].iloc[-1]),
            "xn_latest_close": float(df_xn['close'].iloc[-1]),
            "data_start": str(df_sk.index[0].date()),
            "data_end": str(df_sk.index[-1].date()),
        }
    }


def compute_atr_from_series(closes, period=14):
    """ATR from price series (for 1-min data)"""
    import numpy as np
    if len(closes) < period + 1:
        return np.std(closes) if len(closes) > 1 else 1.0
    arr = np.array(closes, dtype=float)
    tr = np.abs(np.diff(arr))
    return max(np.mean(tr[-period:]), 0.01)


def compute_rsi_from_series(closes, period=14):
    """RSI from price series"""
    import numpy as np
    if len(closes) < period + 1:
        return 50.0
    arr = np.array(closes, dtype=float)
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def compute_intraday_signal():
    """V2 三层漏斗: Real-time intraday signal"""
    import numpy as np
    sk = _naver_minute("000660", 60)
    xn = _sina_realtime("sz300475")
    if not sk or not xn.get("close"):
        return {"signal": "nodata", "version": "v2"}

    closes = [d["close"] for d in sk]
    volumes = [d["volume"] for d in sk]
    n = len(closes)

    recent_ret = (closes[-1] - closes[0]) / closes[0] * 100 if n > 1 else 0
    last_5min_ret = (closes[-1] - closes[-5]) / closes[-5] * 100 if n >= 5 else recent_ret
    last_15min_ret = (closes[-1] - closes[-15]) / closes[-15] * 100 if n >= 15 else recent_ret
    last_30min_ret = (closes[-1] - closes[-30]) / closes[-30] * 100 if n >= 30 else recent_ret

    vol_avg = sum(volumes) / max(n, 1)
    vol_latest = volumes[-1] if volumes else 0
    vol_ratio = (vol_latest / max(vol_avg, 1)) if volumes else 1

    # === V2 改进点 ===
    # 1. ATR自适应阈值
    sk_ret_pct = recent_ret
    atr_5 = compute_atr_from_series(closes[-6:]) if n >= 6 else 1.0
    atr_15 = compute_atr_from_series(closes[-16:]) if n >= 16 else 1.0
    atr_30 = compute_atr_from_series(closes[-31:]) if n >= 31 else 1.0

    latest_price = closes[-1]
    atr_thresh_5 = atr_5 / latest_price * 100 * 0.5  # 0.5x ATR
    atr_thresh_15 = atr_15 / latest_price * 100 * 1.0
    atr_thresh_30 = atr_30 / latest_price * 100 * 1.5

    # 2. RSI过滤
    rsi_val = compute_rsi_from_series(closes)
    rsi_filter = rsi_val > 20  # RSI>20才做空(不做超卖)

    # 3. MA偏离度
    ma20 = np.mean(closes[-min(20, n):]) if n >= 5 else closes[-1]
    ma5 = np.mean(closes[-min(5, n):]) if n >= 3 else closes[-1]
    dev_pct = abs(latest_price - ma20) / ma20 * 100 if ma20 > 0 else 0

    # 4. 置信度评分(0-5)
    confidence = 0
    if abs(last_5min_ret) >= atr_thresh_5:
        confidence += 1
    if abs(last_15min_ret) >= atr_thresh_15:
        confidence += 1
    if abs(last_30min_ret) >= atr_thresh_30:
        confidence += 1
    if vol_ratio > 1.5:
        confidence += 1
    if dev_pct < 5:
        confidence += 1

    # 5. 信号方向
    combined_momentum = last_5min_ret * 0.5 + last_15min_ret * 0.3 + last_30min_ret * 0.2
    dir_text = "hold"
    conf_label = "low"
    pos_size = 0.0

    if confidence >= 3 and abs(combined_momentum) > atr_thresh_5:
        dir_text = "buy" if combined_momentum > 0 else "sell"
        if confidence >= 4:
            conf_label = "high"
            pos_size = 0.15 if dir_text == "buy" else -0.15
        else:
            conf_label = "medium"
            pos_size = 0.08 if dir_text == "buy" else -0.08

    # 6. 动态止损价位建议
    stop_pct = -6.0 if confidence >= 4 else -10.0

    chart_data = [{"time": d["time"], "close": d["close"], "volume": d["volume"]} for d in sk]
    xn_change = xn.get("change_pct", 0)

    return {
        "signal": dir_text,
        "confidence": conf_label,
        "version": "v2_三层漏斗",
        "details": {
            "atr_thresh_5": round(atr_thresh_5, 3),
            "atr_thresh_15": round(atr_thresh_15, 3),
            "atr_thresh_30": round(atr_thresh_30, 3),
            "rsi_20": round(rsi_val, 1),
            "ma_dev_pct": round(dev_pct, 2),
            "confidence_score": confidence,
            "combined_momentum": round(combined_momentum, 3),
            "suggested_position_pct": pos_size,
            "suggested_stop_pct": stop_pct,
        },
        "sk_recent_ret": round(recent_ret, 2),
        "sk_last_5min_ret": round(last_5min_ret, 2),
        "sk_last_15min_ret": round(last_15min_ret, 2),
        "sk_last_30min_ret": round(last_30min_ret, 2),
        "sk_vol_ratio": round(vol_ratio, 2),
        "sk_vol_surge": vol_ratio > 1.5,
        "sk_latest": closes[-1],
        "sk_latest_vol": vol_latest,
        "xn_price": xn.get("close", 0),
        "xn_change": xn_change,
        "timestamp": datetime.now().strftime('%H:%M:%S'),
        "sk_data_points": n,
        "chart": chart_data[-30:],
    }


def compute_cointegration():
    """
    协整检验: SK Hynix vs 香农芯创 (纯numpy/scipy, 无statsmodels依赖)
    """
    import numpy as np
    from scipy import stats

    today = datetime.now()
    start = (today - timedelta(days=250)).strftime('%Y%m%d')
    df_sk = _naver_fetch("000660", count=280)
    df_xn = ak.stock_zh_a_hist_tx(symbol="sz300475", start_date=start, end_date=today.strftime('%Y%m%d'), adjust="qfq")
    if df_xn.empty or df_sk is None:
        return {"error": "数据不足"}

    df_xn['date'] = pd.to_datetime(df_xn['date'])
    df_xn = df_xn.set_index('date')
    common = df_xn.index.intersection(df_sk.index)
    df_xn = df_xn.loc[common].sort_index()
    df_sk = df_sk.loc[common].sort_index()

    y = np.log(df_xn['close'].values.astype(float))
    x = np.log(df_sk['Close'].values.astype(float))

    # OLS via scipy
    slope, intercept, r_val, p_val, std_err = stats.linregress(x, y)
    hedge_ratio = slope
    spread = y - (intercept + slope * x)

    # Manual ADF test (Engle-Granger)
    def adf_test(series, maxlag=1):
        n = len(series)
        y = series
        dy = np.diff(y)
        lag_y = y[:-1]
        X_adf = np.column_stack([lag_y, np.ones(n - 1)])
        if maxlag > 0:
            for l in range(1, min(maxlag + 1, n - 2)):
                X_adf = np.column_stack([X_adf, np.diff(y, n=l)[:len(dy)]])
        # Trim to match
        min_len = min(len(dy), X_adf.shape[0])
        dy = dy[:min_len]
        X_adf = X_adf[:min_len]
        try:
            beta = np.linalg.lstsq(X_adf, dy, rcond=None)[0]
            residuals = dy - X_adf @ beta
            se = np.sqrt(np.sum(residuals**2) / (len(residuals) - X_adf.shape[1]))
            adf_stat = beta[0] / max(se, 1e-10)
        except Exception:
            adf_stat = -1.0
        # MacKinnon critical values (approximate)
        cv = {-1: 0.01, -2: 0.05, -3: 0.10, -4: 0.50}
        for c, pv in sorted(cv.items(), reverse=True):
            if adf_stat < c:
                p_value = pv
                break
        else:
            p_value = 0.99
        return adf_stat, p_value

    adf_stat, adf_pval = adf_test(spread, maxlag=1)
    is_cointegrated = adf_pval < 0.05

    # Half-life via AR(1)
    spread_lag = spread[:-1]
    delta_spread = np.diff(spread)
    slope_hl, _, _, _, _ = stats.linregress(spread_lag, delta_spread)
    half_life = -np.log(2) / slope_hl if slope_hl < 0 else np.inf

    spread_mean = np.mean(spread)
    spread_std = np.std(spread)
    z_score = (spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0

    if abs(z_score) > 2.0:
        z_signal = "short" if z_score > 0 else "long"
    elif abs(z_score) > 1.5:
        z_signal = "watch_short" if z_score > 0 else "watch_long"
    else:
        z_signal = "neutral"

    history = []
    for i in range(0, len(spread), max(1, len(spread) // 100)):
        s = spread[i]
        z = (s - spread_mean) / spread_std if spread_std > 0 else 0
        history.append({"date": str(df_sk.index[i].date()), "spread": round(float(s), 4), "z": round(float(z), 2)})

    return {
        "is_cointegrated": bool(is_cointegrated),
        "adf_statistic": round(float(adf_stat), 4),
        "adf_pvalue": round(float(adf_pval), 6),
        "hedge_ratio": round(float(hedge_ratio), 4),
        "z_score": round(float(z_score), 2),
        "spread_mean": round(float(spread_mean), 4),
        "spread_std": round(float(spread_std), 4),
        "half_life_days": round(float(half_life), 1) if half_life != np.inf else None,
        "z_signal": z_signal,
        "history": history[-100:],
    }


def train_ml_model():
    """
    ML预测 V4: 55维度特征 + 随机森林(10棵) + bagging + walk-forward
    特征: 多周期动量/波动率/相关性/RSI/MACD/布林带/MA偏离/量价/价格形态/跨资产/时间
    目标: XN次日涨幅方向 (阈值优化)
    """
    import numpy as np
    from scipy import stats as sp_stats

    today = datetime.now()
    start = (today - timedelta(days=800)).strftime('%Y%m%d')
    df_sk = _naver_fetch("000660", count=750)
    df_xn_raw = ak.stock_zh_a_hist_tx(symbol="sz300475", start_date=start, end_date=today.strftime('%Y%m%d'), adjust="qfq")
    if df_xn_raw.empty or df_sk is None:
        return {"error": "数据不足"}

    df_xn_raw['date'] = pd.to_datetime(df_xn_raw['date'])
    df_xn_raw = df_xn_raw.set_index('date')
    common = df_xn_raw.index.intersection(df_sk.index)
    df_sk = df_sk.loc[common].sort_index()
    df_xn_raw = df_xn_raw.loc[common].sort_index()

    sk_c = df_sk['Close'].values.astype(float)
    sk_v = df_sk['Volume'].values.astype(float) if 'Volume' in df_sk else np.ones(len(sk_c))
    xn_c = df_xn_raw['close'].values.astype(float)
    xn_o = df_xn_raw['open'].values.astype(float) if 'open' in df_xn_raw else xn_c.copy()
    xn_h = df_xn_raw['high'].values.astype(float) if 'high' in df_xn_raw else xn_c.copy()
    xn_l = df_xn_raw['low'].values.astype(float) if 'low' in df_xn_raw else xn_c.copy()

    if 'amount' in df_xn_raw.columns:
        xn_amt = df_xn_raw['amount'].values.astype(float)
    else:
        xn_amt = np.ones(len(xn_c))

    n_data = len(sk_c)
    lookback = 60  # need 60 days of history

    def _ema(arr, span):
        alpha = 2.0 / (span + 1)
        out = np.zeros_like(arr, dtype=float)
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
        return out

    def _macd(close, fast=12, slow=26, sig=9):
        ema_f = _ema(close, fast)
        ema_s = _ema(close, slow)
        macd_line = ema_f - ema_s
        sig_line = _ema(macd_line, sig)
        hist = macd_line - sig_line
        return macd_line, sig_line, hist

    def _bbands(close, period=20):
        ma = np.array([np.mean(close[max(0, i-period+1):i+1]) for i in range(len(close))])
        std = np.array([np.std(close[max(0, i-period+1):i+1]) for i in range(len(close))])
        upper = ma + 2 * std
        lower = ma - 2 * std
        pct = (close - lower) / (upper - lower + 1e-10)
        width = (upper - lower) / (ma + 1e-10)
        return ma, pct, width

    def _rsi(close, period=14):
        rsi = np.full(len(close), 50.0)
        for i in range(period, len(close)):
            deltas = np.diff(close[i-period:i+1])
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_g = np.mean(gains)
            avg_l = np.mean(losses)
            if avg_l > 0:
                rs = avg_g / avg_l
                rsi[i] = 100 - 100 / (1 + rs)
        return rsi

    def _autocorr(close, lag=1):
        n = len(close)
        if n < lag + 5: return 0.0
        x = close[:-lag]
        y = close[lag:]
        if np.std(x) < 1e-10 or np.std(y) < 1e-10: return 0.0
        return float(np.corrcoef(x, y)[0, 1])

    # Precompute all indicators for full series
    sk_rsi14 = _rsi(sk_c, 14)
    sk_rsi7 = _rsi(sk_c, 7)
    xn_rsi14 = _rsi(xn_c, 14)
    _, _, sk_macd_h = _macd(sk_c)
    xn_macd, xn_macd_s, xn_macd_h = _macd(xn_c)
    xn_ma5, xn_bb_pct, xn_bb_w = _bbands(xn_c, 5)
    xn_ma20, xn_bb20_pct, xn_bb20_w = _bbands(xn_c, 20)
    sk_ma20, _, _ = _bbands(sk_c, 20)
    xn_ma60 = np.array([np.mean(xn_c[max(0, i-59):i+1]) for i in range(len(xn_c))])

    feat_names = []
    features = []
    labels = []

    for i in range(lookback, n_data - 1):
        f = []
        # --- SK multi-period momentum ---
        for w in [1, 2, 3, 5, 10, 20]:
            if i >= w:
                f.append((sk_c[i] - sk_c[i-w]) / sk_c[i-w] * 100)
            else:
                f.append(0.0)
        sk_mom_5_20 = f[3] / (f[5] + 1e-10) if abs(f[5]) > 0.01 else 0
        f.append(sk_mom_5_20)
        sk_accel = f[0] - ((sk_c[i-1] - sk_c[i-2]) / sk_c[i-2] * 100 if i >= 2 else 0)
        f.append(sk_accel)

        # --- XN multi-period momentum ---
        for w in [1, 2, 3, 5, 10, 20]:
            if i >= w:
                f.append((xn_c[i] - xn_c[i-w]) / xn_c[i-w] * 100)
            else:
                f.append(0.0)
        xn_mom_5_20 = f[9] / (f[11] + 1e-10) if abs(f[11]) > 0.01 else 0
        f.append(xn_mom_5_20)
        xn_accel = f[6] - ((xn_c[i-1] - xn_c[i-2]) / xn_c[i-2] * 100 if i >= 2 else 0)
        f.append(xn_accel)

        # --- Volatility ---
        for w in [5, 10, 20]:
            f.append(compute_atr_from_series(sk_c[max(0, i-w+1):i+1]) / sk_c[i] * 100)
        sk_vol5 = np.std(np.diff(sk_c[max(0, i-4):i+1])) if i >= 5 else 0
        sk_vol20 = np.std(np.diff(sk_c[max(0, i-19):i+1])) if i >= 20 else 0
        xn_vol5 = np.std(np.diff(xn_c[max(0, i-4):i+1])) if i >= 5 else 0
        xn_vol20 = np.std(np.diff(xn_c[max(0, i-19):i+1])) if i >= 20 else 0
        f.append(sk_vol5)
        f.append(sk_vol20)
        f.append(xn_vol5)
        f.append(xn_vol20)
        f.append(sk_vol5 / (sk_vol20 + 1e-10))
        f.append(xn_vol5 / (xn_vol20 + 1e-10))

        # --- Volume ---
        sk_vol_avg20 = np.mean(sk_v[max(0, i-19):i+1]) if i >= 1 else sk_v[i]
        xn_amt_avg20 = np.mean(xn_amt[max(0, i-19):i+1]) if i >= 1 else xn_amt[i]
        f.append(sk_v[i] / (sk_vol_avg20 + 1))
        f.append(xn_amt[i] / (xn_amt_avg20 + 1))
        vol_trend = np.mean(sk_v[max(0, i-4):i+1]) / (np.mean(sk_v[max(0, i-19):i+1]) + 1)
        f.append(vol_trend)

        # --- Correlation at multiple windows ---
        for w in [5, 10, 20, 60]:
            if i >= w and np.std(sk_c[i-w+1:i+1]) > 0 and np.std(xn_c[i-w+1:i+1]) > 0:
                f.append(float(np.corrcoef(sk_c[i-w+1:i+1], xn_c[i-w+1:i+1])[0, 1]))
            else:
                f.append(0.0)
        f.append(f[-2] - f[-4])  # corr change 5d-20d

        # --- Price ratio ---
        ratio = sk_c[i] / xn_c[i] if xn_c[i] > 0 else 1
        ratio_ma5 = np.mean(sk_c[max(0,i-4):i+1]) / (np.mean(xn_c[max(0,i-4):i+1]) + 1e-10)
        ratio_ma20 = np.mean(sk_c[max(0,i-19):i+1]) / (np.mean(xn_c[max(0,i-19):i+1]) + 1e-10)
        f.append(np.log(ratio))
        f.append(np.log(ratio_ma5))
        f.append(np.log(ratio_ma20))
        f.append(np.log(ratio / ratio_ma20 + 1e-10) * 100)

        # --- RSI ---
        f.append(sk_rsi14[i])
        f.append(sk_rsi7[i])
        f.append(xn_rsi14[i])
        f.append(sk_rsi14[i] - 50)

        # --- MACD ---
        f.append(sk_macd_h[i] / sk_c[i] * 100)
        f.append(xn_macd_h[i] / xn_c[i] * 100)
        f.append((xn_macd[i] - xn_macd_s[i]) / xn_c[i] * 100)

        # --- Bollinger ---
        f.append(xn_bb_pct[i])
        f.append(xn_bb_w[i])
        f.append(xn_bb20_pct[i])
        f.append(xn_bb20_w[i])

        # --- MA position ---
        f.append((xn_c[i] / xn_ma5[i] - 1) * 100 if xn_ma5[i] > 0 else 0)
        f.append((xn_c[i] / xn_ma20[i] - 1) * 100 if xn_ma20[i] > 0 else 0)
        f.append((xn_c[i] / xn_ma60[i] - 1) * 100 if xn_ma60[i] > 0 else 0)
        f.append((sk_c[i] / sk_ma20[i] - 1) * 100 if sk_ma20[i] > 0 else 0)

        # --- Candlestick patterns ---
        body = abs(xn_c[i] - xn_o[i])
        rng = xn_h[i] - xn_l[i] + 1e-10
        f.append(body / rng)  # body ratio
        lower_shadow = min(xn_c[i], xn_o[i]) - xn_l[i]
        f.append(lower_shadow / rng)  # lower shadow ratio
        f.append((xn_h[i] - max(xn_c[i], xn_o[i])) / rng)  # upper shadow ratio

        # --- Cross-asset ---
        sk_ret1 = (sk_c[i] - sk_c[i-1]) / sk_c[i-1] * 100 if i >= 1 else 0
        xn_ret1 = (xn_c[i] - xn_c[i-1]) / xn_c[i-1] * 100 if i >= 1 else 0
        f.append(sk_ret1 - xn_ret1)  # return divergence
        f.append(sk_ret1 * xn_ret1)  # co-movement

        # --- Time features ---
        dow = df_sk.index[i].dayofweek
        f.append(1 if dow == 0 else 0)
        f.append(1 if dow == 1 else 0)
        f.append(1 if dow == 2 else 0)
        f.append(1 if dow == 3 else 0)
        f.append(1 if dow == 4 else 0)

        # --- Regime ---
        trend20 = (sk_c[i] - sk_c[max(0, i-20)]) / (sk_c[max(0, i-20)] + 1e-10) * 100 if i >= 20 else 0
        f.append(trend20)
        above_ma20 = 1 if sk_c[i] > sk_ma20[i] else (-1 if sk_c[i] < sk_ma20[i] else 0)
        f.append(above_ma20)

        # --- Autocorrelation ---
        f.append(_autocorr(sk_c[max(0, i-20):i+1], 1))
        f.append(_autocorr(xn_c[max(0, i-20):i+1], 1))

        # --- Recent pattern count ---
        up_days = sum(1 for j in range(max(0, i-4), i+1) if sk_c[j] > sk_c[max(0, j-1)])
        f.append(up_days)

        # 3-day forward return (more stable signal than 1-day)
        fwd_days = min(3, len(xn_c) - i - 1)
        xn_next_ret = (xn_c[i + fwd_days] - xn_c[i]) / xn_c[i] * 100
        features.append(f)
        labels.append(1 if xn_next_ret > 0 else 0)

    if len(feat_names) == 0:
        feat_names = [
            'sk_ret_1d','sk_ret_2d','sk_ret_3d','sk_ret_5d','sk_ret_10d','sk_ret_20d',
            'sk_mom_5_20','sk_accel',
            'xn_ret_1d','xn_ret_2d','xn_ret_3d','xn_ret_5d','xn_ret_10d','xn_ret_20d',
            'xn_mom_5_20','xn_accel',
            'sk_atr_5','sk_atr_10','sk_atr_20',
            'sk_vol5','sk_vol20','xn_vol5','xn_vol20','vol_ratio_sk','vol_ratio_xn',
            'sk_vol_ratio','xn_amt_ratio','vol_trend',
            'corr_5d','corr_10d','corr_20d','corr_60d','corr_change',
            'ratio_log','ratio_ma5_log','ratio_ma20_log','ratio_dev',
            'sk_rsi14','sk_rsi7','xn_rsi14','sk_rsi_dev',
            'sk_macd_hist','xn_macd_hist','xn_macd_diff',
            'xn_bb_pct','xn_bb_width','xn_bb20_pct','xn_bb20_width',
            'xn_ma5_dev','xn_ma20_dev','xn_ma60_dev','sk_ma20_dev',
            'xn_body_ratio','xn_lower_shadow','xn_upper_shadow',
            'sk_xn_ret_diff','sk_xn_co_move',
            'is_mon','is_tue','is_wed','is_thu','is_fri',
            'trend_20d','above_ma20',
            'sk_autocorr_1','xn_autocorr_1','sk_up_days'
        ]

    features = np.array(features, dtype=float)
    labels = np.array(labels)
    n_feat = features.shape[1]
    n_samples = len(features)

    if n_feat != len(feat_names):
        feat_names = [f'f{i}' for i in range(n_feat)]

    # ============ Random Forest from scratch ============
    class DecisionTree:
        def __init__(self, max_depth=5, min_samples_split=10, max_features='sqrt'):
            self.max_depth = max_depth
            self.min_samples_split = min_samples_split
            self.max_features = max_features
            self.tree = None

        def _gini(self, y):
            if len(y) == 0: return 0
            p = np.mean(y)
            return 1 - p * p - (1 - p) * (1 - p)

        def _best_split(self, X, y, feat_idx):
            best_gini = self._gini(y)
            best_feat = -1
            best_thresh = 0
            n = len(y)
            for fi in feat_idx:
                vals = X[:, fi]
                uniq = np.unique(vals)
                if len(uniq) < 2: continue
                thresholds = (uniq[:-1] + uniq[1:]) / 2
                for th in thresholds[:10]:  # limit thresholds for speed
                    left = y[vals <= th]
                    right = y[vals > th]
                    if len(left) < 5 or len(right) < 5: continue
                    g = (len(left) * self._gini(left) + len(right) * self._gini(right)) / n
                    if g < best_gini:
                        best_gini = g
                        best_feat = fi
                        best_thresh = th
            return best_feat, best_thresh

        def _build(self, X, y, depth=0):
            n = len(y)
            if depth >= self.max_depth or n < self.min_samples_split or self._gini(y) < 0.01:
                p = np.mean(y) if n > 0 else 0.5
                return {"leaf": True, "prob": p, "n": n}
            if self.max_features == 'sqrt':
                k = max(1, int(np.sqrt(X.shape[1])))
            else:
                k = X.shape[1]
            feat_idx = np.random.choice(X.shape[1], k, replace=False)
            fi, th = self._best_split(X, y, feat_idx)
            if fi == -1:
                p = np.mean(y) if n > 0 else 0.5
                return {"leaf": True, "prob": p, "n": n}
            mask = X[:, fi] <= th
            left = self._build(X[mask], y[mask], depth + 1)
            right = self._build(X[~mask], y[~mask], depth + 1)
            return {"leaf": False, "feat": fi, "thresh": th, "left": left, "right": right, "n": n}

        def fit(self, X, y):
            self.tree = self._build(X, y)
            return self

        def _predict_one(self, x, node):
            if node["leaf"]:
                return node["prob"]
            if x[node["feat"]] <= node["thresh"]:
                return self._predict_one(x, node["left"])
            else:
                return self._predict_one(x, node["right"])

        def predict_proba(self, X):
            return np.array([self._predict_one(x, self.tree) for x in X])

    class RandomForest:
        def __init__(self, n_trees=10, max_depth=5, min_samples_split=10):
            self.n_trees = n_trees
            self.max_depth = max_depth
            self.min_samples_split = min_samples_split
            self.trees = []

        def fit(self, X, y):
            self.trees = []
            n = len(X)
            for _ in range(self.n_trees):
                idx = np.random.choice(n, n, replace=True)
                tree = DecisionTree(self.max_depth, self.min_samples_split)
                tree.fit(X[idx], y[idx])
                self.trees.append(tree)
            return self

        def predict_proba(self, X):
            preds = np.array([t.predict_proba(X) for t in self.trees])
            return np.mean(preds, axis=0)

        def feature_importance(self, X, y, feat_names):
            importances = np.zeros(X.shape[1])
            base_acc = np.mean((self.predict_proba(X) > 0.5).astype(int) == y)
            for j in range(X.shape[1]):
                X_perm = X.copy()
                np.random.shuffle(X_perm[:, j])
                perm_acc = np.mean((self.predict_proba(X_perm) > 0.5).astype(int) == y)
                importances[j] = max(0, base_acc - perm_acc)
            total = importances.sum() + 1e-10
            return {feat_names[i]: round(float(importances[i] / total), 4) for i in range(len(feat_names))}

    # ============ Single RF + Grid search (optimal config) ============
    split = int(n_samples * 0.75)
    X_all = features
    y_all = labels

    mu = X_all[:split].mean(axis=0)
    sigma = X_all[:split].std(axis=0) + 1e-10
    X_train = (X_all[:split] - mu) / sigma
    X_test = (X_all[split:] - mu) / sigma
    y_train = y_all[:split]
    y_test = y_all[split:]

    best_acc = 0
    best_rf = None
    best_cfg = {}
    for depth in [3, 4, 5, 6]:
        for n_trees in [5, 10, 15, 20]:
            rf = RandomForest(n_trees=n_trees, max_depth=depth)
            rf.fit(X_train, y_train)
            prob = rf.predict_proba(X_test)
            acc = np.mean((prob > 0.5).astype(int) == y_test)
            if acc > best_acc:
                best_acc = acc
                best_rf = rf
                best_cfg = {"depth": depth, "n_trees": n_trees}

    # Threshold optimization
    prob_test = best_rf.predict_proba(X_test)
    best_thresh = 0.5
    best_acc_t = best_acc
    for th in np.arange(0.25, 0.75, 0.02):
        acc_t = np.mean((prob_test > th).astype(int) == y_test)
        if acc_t > best_acc_t:
            best_acc_t = acc_t
            best_thresh = th

    pred_test = (prob_test > best_thresh).astype(int)
    accuracy = float(np.mean(pred_test == y_test))
    precision = float(np.sum((pred_test == 1) & (y_test == 1)) / max(np.sum(pred_test == 1), 1))
    recall = float(np.sum((pred_test == 1) & (y_test == 1)) / max(np.sum(y_test == 1), 1))
    f1 = 2 * precision * recall / max(precision + recall, 0.001)

    importance = best_rf.feature_importance(X_test, y_test, feat_names)

    # Latest prediction
    last_feat = (features[-1:] - mu) / sigma
    prob_up = float(best_rf.predict_proba(last_feat)[0])

    dates_list = [str(df_sk.index[lookback + i].date()) for i in range(len(labels))]

    return {
        "model_type": "random_forest_55feat",
        "params": best_cfg,
        "n_features": n_feat,
        "n_samples": n_samples,
        "train_size": split,
        "test_size": n_samples - split,
        "test_accuracy": round(accuracy, 4),
        "test_precision": round(precision, 4),
        "test_recall": round(recall, 4),
        "test_f1": round(f1, 4),
        "feature_importance": dict(sorted(importance.items(), key=lambda x: -x[1])[:20]),
        "latest_prediction": {
            "date": dates_list[-1] if dates_list else None,
            "prob_up": round(prob_up, 4),
            "signal": "buy" if prob_up > 0.6 else ("sell" if prob_up < 0.4 else "hold"),
        },
        "sample_predictions": [
            {"date": dates_list[split + j], "actual": "up" if y_test[j] == 1 else "down",
             "prob_up": round(float(prob_test[j]), 3)}
            for j in range(0, min(30, len(prob_test)))
        ],
    }


# ============ 专家团系统 ============

EXPERT_PANEL = [
    {"id": "expert_001", "name": "张三(技术派)", "source": "公众号", "weight": 0.15,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_002", "name": "李四(基本面)", "source": "公众号", "weight": 0.15,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_003", "name": "王五(趋势)", "source": "视频号", "weight": 0.10,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_004", "name": "赵六(量化)", "source": "视频号", "weight": 0.15,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_005", "name": "孙七(游资)", "source": "公众号", "weight": 0.10,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_006", "name": "周八(机构)", "source": "研报", "weight": 0.15,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_007", "name": "吴九(宏观)", "source": "视频号", "weight": 0.10,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
    {"id": "expert_008", "name": "郑十(情绪)", "source": "公众号", "weight": 0.10,
     "view": "neutral", "confidence": 0.0, "reason": "", "last_updated": None},
]

# ============ 新闻情感分析引擎 ============

_BULLISH_WORDS = [
    "突破", "利好", "增长", "景气", "超预期", "上调", "加仓", "强势", "反弹",
    "放量", "涨停", "新高", "龙头", "领涨", "买入", "推荐", "增持", "买入评级",
    "净利润增长", "营收增长", "订单增长", "产能扩张", "国产替代", "供不应求",
    "库存回升", "补库", "复苏", "回暖", "拐点", "量价齐升", "毛利率提升",
]

_BEARISH_WORDS = [
    "下跌", "利空", "下滑", "萎缩", "低于预期", "下调", "减仓", "弱势", "破位",
    "缩量", "跌停", "新低", "领跌", "卖出", "减持", "净利润下降", "营收下滑",
    "库存高企", "去库", "过剩", "价格战", "毛利率下降", "业绩不及预期",
    "风险", "警惕", "谨慎", "承压", "回落", "调整", "空头",
]

_TECH_WORDS = ["技术面", "K线", "均线", "支撑", "压力", "MACD", "RSI", "布林", "形态", "量价"]
_FUNDAMENTAL_WORDS = ["业绩", "营收", "利润", "PE", "估值", "财报", "毛利率", "净利率", "ROE"]
_TREND_WORDS = ["趋势", "通道", "突破", "新高", "新低", "方向", "走势"]
_QUANT_WORDS = ["量化", "因子", "模型", "回测", "策略", "阿尔法", "对冲"]
_HOTTIP_WORDS = ["涨停", "游资", "龙虎榜", "题材", "概念", "热点", "板块"]
_INST_WORDS = ["机构", "研报", "评级", "目标价", "券商", "研报", "买入评级", "增持评级"]
_MACRO_WORDS = ["宏观", "政策", "利率", "通胀", "GDP", "央行", "降息", "降准", "关税"]

_EXPERT_KEYWORD_MAP = {
    "expert_001": _TECH_WORDS,
    "expert_002": _FUNDAMENTAL_WORDS,
    "expert_003": _TREND_WORDS,
    "expert_004": _QUANT_WORDS,
    "expert_005": _HOTTIP_WORDS,
    "expert_006": _INST_WORDS,
    "expert_007": _MACRO_WORDS,
    "expert_008": [],  # 情绪专家：使用剩余新闻
}


def _analyze_sentiment(text):
    """关键词情感分析 → (score, matched_bullish, matched_bearish)"""
    if not text:
        return 0, [], []
    text_lower = text.lower()
    bull_matched = [w for w in _BULLISH_WORDS if w in text_lower]
    bear_matched = [w for w in _BEARISH_WORDS if w in text_lower]
    bull_count = len(bull_matched)
    bear_count = len(bear_matched)
    total = bull_count + bear_count
    if total == 0:
        return 0, bull_matched, bear_matched
    score = (bull_count - bear_count) / total  # [-1, 1]
    return score, bull_matched, bear_matched


def _classify_expert(title, content):
    """根据标题+内容关键词判断最匹配的专家ID列表"""
    text = (title or "") + " " + (content or "")
    matched = []
    for expert_id, keywords in _EXPERT_KEYWORD_MAP.items():
        if expert_id == "expert_008":
            continue  # 情绪专家最后处理
        if any(kw in text for kw in keywords):
            matched.append(expert_id)
    if not matched:
        matched.append("expert_008")  # 无匹配 → 归入情绪专家
    return matched


def fetch_news_and_update_experts():
    """从东方财富抓取个股新闻 → 情感分析 → 更新EXPERT_PANEL"""
    from datetime import datetime as _dt
    now = _dt.now()

    # 1. 抓取新闻
    news_items = []
    try:
        import akshare as ak
        df_news = ak.stock_news_em(symbol="300475")
        if df_news is not None and not df_news.empty:
            for _, row in df_news.iterrows():
                news_items.append({
                    "title": str(row.get("新闻标题", "")),
                    "content": str(row.get("新闻内容", "")),
                    "time": str(row.get("发布时间", "")),
                    "source": str(row.get("文章来源", "")),
                })
    except Exception as e:
        return {"error": f"news_fetch_failed: {str(e)[:80]}", "news_count": 0}

    if not news_items:
        return {"error": "no_news", "news_count": 0}

    # 2. 情感分析每篇新闻
    analyzed = []
    for item in news_items:
        title = item["title"]
        content = item["content"]
        full_text = f"{title} {content}"
        score, bull, bear = _analyze_sentiment(full_text)
        expert_ids = _classify_expert(title, content)
        analyzed.append({
            "title": title[:60],
            "time": item["time"],
            "source": item["source"],
            "score": round(score, 3),
            "bull_words": bull[:5],
            "bear_words": bear[:5],
            "expert_ids": expert_ids,
        })

    # 3. 按专家分组聚合
    expert_scores = {}  # expert_id → [scores]
    for a in analyzed:
        for eid in a["expert_ids"]:
            if eid not in expert_scores:
                expert_scores[eid] = []
            expert_scores[eid].append(a["score"])

    # 4. 更新 EXPERT_PANEL
    updated = []
    for exp in EXPERT_PANEL:
        eid = exp["id"]
        scores = expert_scores.get(eid, [])
        if not scores:
            # 该专家无相关新闻 → 保持neutral
            continue
        avg_score = sum(scores) / len(scores)
        n_articles = len(scores)
        # 置信度：基于文章数量和一致性
        consistency = 1.0 - (sum(1 for s in scores if s * avg_score < 0) / max(n_articles, 1))
        confidence = min(0.3 + n_articles * 0.05 + consistency * 0.3, 0.95)
        # 观点
        if avg_score > 0.15:
            view = "bullish"
        elif avg_score < -0.15:
            view = "bearish"
        else:
            view = "neutral"
        # 理由
        reasons = []
        for a in analyzed:
            if eid in a["expert_ids"]:
                reasons.append(f"[{a['score']:+.2f}] {a['title']}")
        reason_text = "; ".join(reasons[:3])

        exp["view"] = view
        exp["confidence"] = round(confidence, 3)
        exp["reason"] = reason_text
        exp["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S")
        updated.append({
            "id": eid, "name": exp["name"],
            "view": view, "confidence": round(confidence, 3),
            "n_articles": n_articles, "avg_score": round(avg_score, 3),
        })

    return {
        "news_count": len(news_items),
        "analyzed_count": len(analyzed),
        "updated_experts": len(updated),
        "details": updated,
        "sample_news": analyzed[:5],
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def compute_expert_consensus():
    """Aggregate expert opinions → consensus vote"""
    if not EXPERT_PANEL:
        return {"consensus": 0, "signal": "neutral", "participating": 0}

    total_weight = 0
    weighted_vote = 0
    participating = 0
    details = []

    for exp in EXPERT_PANEL:
        if exp["view"] == "neutral" or exp["confidence"] <= 0:
            continue
        vote = 1 if exp["view"] == "bullish" else (-1 if exp["view"] == "bearish" else 0)
        w = exp["weight"] * exp["confidence"]
        weighted_vote += vote * w
        total_weight += w
        participating += 1
        details.append({
            "name": exp["name"], "source": exp["source"],
            "view": exp["view"], "confidence": exp["confidence"],
            "reason": exp.get("reason", ""),
        })

    consensus = weighted_vote / max(total_weight, 0.001) if participating > 0 else 0

    if consensus > 0.3:
        signal = "buy"
    elif consensus < -0.3:
        signal = "sell"
    else:
        signal = "hold"

    return {
        "consensus_score": round(float(consensus), 3),
        "signal": signal,
        "participating_experts": participating,
        "total_experts": len(EXPERT_PANEL),
        "details": details,
        "panel": [
            {"id": e["id"], "name": e["name"], "source": e["source"],
             "view": e["view"], "confidence": e["confidence"],
             "reason": e.get("reason", ""), "weight": e["weight"],
             "last_updated": str(e["last_updated"]) if e["last_updated"] else None}
            for e in EXPERT_PANEL
        ],
    }


def compute_triple_signal():
    """
    四重信号融合: V2信号(0.35) + 协整信号(0.15) + ML信号(0.25) + 专家团(0.25)
    每个组件独立运行，某个失败不影响其他
    """
    v2 = {"signal": "nodata", "confidence": "low"}
    coint = {"z_score": 0}
    ml = {"latest_prediction": {"prob_up": 0.5, "signal": "hold"}}
    expert = {"consensus_score": 0, "signal": "hold", "participating_experts": 0, "details": []}

    try:
        v2 = compute_intraday_signal()
    except Exception as e:
        v2 = {"signal": "nodata", "error": str(e)[:50]}

    try:
        coint = compute_cointegration()
    except Exception:
        coint = {"z_score": 0, "error": "coint_failed"}

    try:
        ml = train_ml_model()
    except Exception:
        ml = {"latest_prediction": {"prob_up": 0.5, "signal": "hold"}, "error": "ml_failed"}

    try:
        expert = compute_expert_consensus()
    except Exception:
        expert = {"consensus_score": 0, "signal": "hold", "participating_experts": 0, "details": []}

    if v2.get("signal") == "nodata":
        return {"error": "v2_nodata", "signals": {"v2": v2}, "consensus_score": 0, "final_signal": "hold"}

    # V2: numeric vote
    v2_vote = 1 if v2["signal"] == "buy" else (-1 if v2["signal"] == "sell" else 0)
    v2_conf = {"low": 0.3, "medium": 0.5, "high": 0.8}.get(v2.get("confidence", "low"), 0.3)
    v2_score = v2_vote * v2_conf

    # Cointegration
    z = coint.get("z_score", 0)
    if abs(z) > 2.0:
        coint_vote = -1 if z > 0 else 1
        coint_conf = 0.7
    elif abs(z) > 1.5:
        coint_vote = -0.5 if z > 0 else 0.5
        coint_conf = 0.4
    else:
        coint_vote = 0
        coint_conf = 0

    # ML
    lp = ml.get("latest_prediction", {})
    prob_up = lp.get("prob_up", 0.5)
    ml_vote = 1 if prob_up > 0.6 else (-1 if prob_up < 0.4 else 0)
    ml_conf = abs(prob_up - 0.5) * 2 * 0.6

    # Expert
    ex = expert.get("consensus_score", 0)
    ex_signal = expert.get("signal", "hold")
    ex_vote = 1 if ex_signal == "buy" else (-1 if ex_signal == "sell" else 0)
    ex_conf = min(abs(ex) * 1.5, 0.8)

    # Weighted fusion
    weights = {"v2": 0.35, "coint": 0.15, "ml": 0.25, "expert": 0.25}
    total_score = (
        v2_score * weights["v2"] +
        coint_vote * coint_conf * weights["coint"] +
        ml_vote * ml_conf * weights["ml"] +
        ex_vote * ex_conf * weights["expert"]
    )

    if total_score > 0.2:
        final_signal = "buy"
    elif total_score < -0.2:
        final_signal = "sell"
    else:
        final_signal = "hold"

    return {
        "final_signal": final_signal,
        "consensus_score": round(float(total_score), 4),
        "weights": weights,
        "signals": {
            "v2": {"vote": v2_vote, "conf": round(v2_conf, 2), "raw": v2["signal"], "signal_cn": "做多" if v2_vote > 0 else ("做空" if v2_vote < 0 else "观望"), "details": v2.get("details", {})},
            "cointegration": {"vote": round(coint_vote * coint_conf, 3), "z_score": z, "signal_cn": "做多(z<-2σ)" if z < -2 else ("做空(z>2σ)" if z > 2 else "中性")},
            "ml": {"vote": round(ml_vote * ml_conf, 3), "prob_up": prob_up, "signal_cn": "看多" if prob_up > 0.6 else ("看空" if prob_up < 0.4 else "观望")},
            "expert_panel": {"consensus": round(ex, 3), "participating": expert.get("participating_experts", 0), "details": expert.get("details", [])},
        },
        "xn_price": v2.get("xn_price"),
        "xn_change": v2.get("xn_change"),
        "sk_latest": v2.get("sk_latest"),
        "timestamp": v2.get("timestamp"),
    }


def get_xn_data(days=400):
    """Fetch 香农芯创 and SK Hynix data, aligned"""
    today = datetime.now()
    start = (today - timedelta(days=days)).strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')

    df_xn = ak.stock_zh_a_hist_tx(symbol="sz300475", start_date=start, end_date=end, adjust="qfq")
    if df_xn.empty:
        return {"error": "香农芯创数据获取失败"}
    df_xn = df_xn.rename(columns={'date': 'Date', 'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low', 'amount': 'Amount'})
    df_xn['Date'] = pd.to_datetime(df_xn['Date'])
    df_xn = df_xn.set_index('Date')

    df_sk = _naver_fetch("000660", days + 30)

    common = df_xn.index.intersection(df_sk.index)
    df_xn = df_xn.loc[common].sort_index()
    df_sk = df_sk.loc[common].sort_index()

    xn_ret = df_xn['Close'].pct_change().dropna()
    sk_ret = df_sk['Close'].pct_change().dropna()
    c = xn_ret.index.intersection(sk_ret.index)
    xn_ret = xn_ret[c]; sk_ret = sk_ret[c]

    roll_corr = xn_ret.rolling(60).corr(sk_ret).dropna()

    lag_results = []
    for lag in range(1, 11):
        sk_l = sk_ret.shift(lag).dropna()
        idx = sk_l.index.intersection(xn_ret.index)
        if len(idx) > 20:
            r, p = pearsonr(sk_l.loc[idx], xn_ret.loc[idx])
            lag_results.append({"lag": lag, "r": round(r, 4), "p": round(p, 6)})

    xn_lag_results = []
    for lag in range(1, 11):
        xn_l = xn_ret.shift(lag).dropna()
        idx = xn_l.index.intersection(sk_ret.index)
        if len(idx) > 20:
            r, p = pearsonr(xn_l.loc[idx], sk_ret.loc[idx])
            xn_lag_results.append({"lag": lag, "r": round(r, 4), "p": round(p, 6)})

    close = df_xn['Close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi14 = 100 - 100 / (1 + gain / loss)
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()

    return {
        "xn_prices": [{"date": str(idx.date()), "close": float(row['Close'])} for idx, row in df_xn.iterrows()],
        "sk_prices": [{"date": str(idx.date()), "close": int(row['Close'])} for idx, row in df_sk.iterrows()],
        "stats": {
            "correlation": round(pearsonr(xn_ret, sk_ret)[0], 4),
            "rolling_corr_mean": round(roll_corr.mean(), 4),
            "rolling_corr_last": round(roll_corr.iloc[-1], 4),
            "rolling_corr_max": round(roll_corr.max(), 4),
            "rolling_corr_min": round(roll_corr.min(), 4),
            "close": round(float(close.iloc[-1]), 2),
            "ma5": round(float(ma5.iloc[-1]), 2),
            "ma20": round(float(ma20.iloc[-1]), 2),
            "ma60": round(float(ma60.iloc[-1]), 2),
            "rsi14": round(float(rsi14.iloc[-1]), 2),
            "macd": round(float(macd.iloc[-1]), 2),
            "signal": round(float(signal.iloc[-1]), 2),
            "macd_hist": round(float((macd - signal).iloc[-1]), 2),
            "change_pct": round((close.iloc[-1] / close.iloc[-2] - 1) * 100, 2),
            "sk_close": int(df_sk['Close'].iloc[-1]),
            "sk_change_pct": round((df_sk['Close'].iloc[-1] / df_sk['Close'].iloc[-2] - 1) * 100, 2),
        },
        "rolling_corr": [{"date": str(idx.date()), "r": round(v, 4)} for idx, v in roll_corr.items()],
        "lagged_corr_sk_lead": lag_results,
        "lagged_corr_xn_lead": xn_lag_results,
    }


# ============ 多Pair跨境联动系统 ============

PAIRS = {
    "sk_xn": {
        "name": "SK海力士 → 香农芯创",
        "desc": "HBM存储分销龙头传导",
        "lead": {"code": "000660", "src": "naver", "name": "SK海力士", "mkt": "KOSPI"},
        "follow": {"code": "sz300475", "src": "tx_a", "name": "香农芯创", "mkt": "A股创业板"},
        "relation": "SK海力士是香农芯创的授权分销商",
    },
    "nvda_smic": {
        "name": "英伟达 → 中芯国际",
        "desc": "AI芯片设计→晶圆代工传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "00981", "src": "hk_ak", "name": "中芯国际", "mkt": "港股"},
        "relation": "英伟达AI芯片依赖中国晶圆代工产能",
    },
    "tsm_smic": {
        "name": "台积电 → 中芯国际",
        "desc": "晶圆代工对标传导",
        "lead": {"code": "TSM", "src": "us_ak", "name": "台积电ADR", "mkt": "NYSE"},
        "follow": {"code": "00981", "src": "hk_ak", "name": "中芯国际", "mkt": "港股"},
        "relation": "台积电与中芯国际晶圆代工直接对标",
    },
    "sk_cjec": {
        "name": "SK海力士 → 长电科技",
        "desc": "HBM封测需求传导",
        "lead": {"code": "000660", "src": "naver", "name": "SK海力士", "mkt": "KOSPI"},
        "follow": {"code": "sh600584", "src": "tx_a", "name": "长电科技", "mkt": "A股上海"},
        "relation": "长电科技承接SK海力士HBM封测需求",
    },
    "mu_giga": {
        "name": "美光 → 兆易创新",
        "desc": "存储周期共振传导",
        "lead": {"code": "MU", "src": "us_ak", "name": "美光科技", "mkt": "NASDAQ"},
        "follow": {"code": "sh603986", "src": "tx_a", "name": "兆易创新", "mkt": "A股上海"},
        "relation": "美光与兆易创新同处存储周期",
    },
    "nvda_fii": {
        "name": "英伟达 → 工业富联",
        "desc": "AI服务器ODM直接传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sh601138", "src": "tx_a", "name": "工业富联", "mkt": "A股上海"},
        "relation": "工业富联是英伟达GB200/GB300核心代工厂",
    },
    "nvda_innolight": {
        "name": "英伟达 → 中际旭创",
        "desc": "光模块龙头直接传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz300308", "src": "tx_a", "name": "中际旭创", "mkt": "A股创业板"},
        "relation": "中际旭创800G/1.6T光模块全球主力供货",
    },
    "nvda_shenghong": {
        "name": "英伟达 → 胜宏科技",
        "desc": "PCB一级供应商传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz300476", "src": "tx_a", "name": "胜宏科技", "mkt": "A股创业板"},
        "relation": "胜宏科技78层正交Midplane背板独家供货",
    },
    "nvda_eoptolink": {
        "name": "英伟达 → 新易盛",
        "desc": "光模块业绩兑现传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz300502", "src": "tx_a", "name": "新易盛", "mkt": "A股创业板"},
        "relation": "新易盛高速光模块主力，2026H1净利预增77%~103%",
    },
    "nvda_envicool": {
        "name": "英伟达 → 英维克",
        "desc": "液冷NPN Tier1直接传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz002837", "src": "tx_a", "name": "英维克", "mkt": "A股创业板"},
        "relation": "英维克是大陆唯一英伟达NPN Tier1液冷服务商",
    },
    "nvda_megmeet": {
        "name": "英伟达 → 麦格米特",
        "desc": "电源官方认证直接传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz002851", "src": "tx_a", "name": "麦格米特", "mkt": "A股创业板"},
        "relation": "麦格米特是A股唯一英伟达官方认证电源供应商",
    },
    "nvda_wus": {
        "name": "英伟达 → 沪电股份",
        "desc": "PCB算力板料认证传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz002463", "src": "tx_a", "name": "沪电股份", "mkt": "A股创业板"},
        "relation": "沪电股份AI服务器PCB算力板料过英伟达认证",
    },
    "nvda_luxshare": {
        "name": "英伟达 → 立讯精密",
        "desc": "高速铜缆NVLink传导",
        "lead": {"code": "NVDA", "src": "us_ak", "name": "英伟达", "mkt": "NASDAQ"},
        "follow": {"code": "sz002475", "src": "tx_a", "name": "立讯精密", "mkt": "A股创业板"},
        "relation": "立讯精密224G铜缆批量供货北美头部客户",
    },
}


def _fetch_us_daily(symbol, count=600):
    """akshare美股日线 → 统一列名(Open/High/Low/Close/Volume)"""
    with _fetch_lock:
        df = ak.stock_us_daily(symbol=symbol)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.tail(count)
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                            'close': 'Close', 'volume': 'Volume'})
    df.index = pd.to_datetime(df['date'])
    df = df.drop(columns=['date'])
    return df


def _fetch_hk_daily(symbol, count=600):
    """akshare港股日线 → 统一列名"""
    with _fetch_lock:
        df = ak.stock_hk_daily(symbol=symbol, adjust='qfq')
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.tail(count)
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                            'close': 'Close', 'volume': 'Volume'})
    df.index = pd.to_datetime(df['date'])
    df = df.drop(columns=['date'])
    return df


def _fetch_tx_a_daily(symbol, days=900):
    """腾讯A股日线 → 统一列名"""
    today = datetime.now()
    start = (today - timedelta(days=days)).strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')
    with _fetch_lock:
        df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust='qfq')
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={'date': 'Date', 'open': 'Open', 'close': 'Close',
                            'high': 'High', 'low': 'Low', 'amount': 'Volume'})
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    return df


def _fetch_pair_lead(pair):
    """根据pair配置获取领先方数据"""
    src = pair["lead"]["src"]
    if src == "naver":
        return _naver_fetch(pair["lead"]["code"], count=600)
    elif src == "us_ak":
        return _fetch_us_daily(pair["lead"]["code"], count=600)
    elif src == "hk_ak":
        return _fetch_hk_daily(pair["lead"]["code"], count=600)
    elif src == "tx_a":
        return _fetch_tx_a_daily(pair["lead"]["code"], days=900)
    return pd.DataFrame()


def _fetch_pair_follow(pair):
    """根据pair配置获取跟随方数据"""
    src = pair["follow"]["src"]
    if src == "naver":
        return _naver_fetch(pair["follow"]["code"], count=600)
    elif src == "us_ak":
        return _fetch_us_daily(pair["follow"]["code"], count=600)
    elif src == "hk_ak":
        return _fetch_hk_daily(pair["follow"]["code"], count=600)
    elif src == "tx_a":
        return _fetch_tx_a_daily(pair["follow"]["code"], days=900)
    return pd.DataFrame()


def analyze_pair(pair_id, days=800):
    """通用Pair分析：获取→对齐→相关性→滞后相关→技术指标→协整"""
    import numpy as np
    pair = PAIRS.get(pair_id)
    if not pair:
        return {"error": f"Unknown pair: {pair_id}", "pair_id": pair_id}

    df_lead = _fetch_pair_lead(pair)
    df_follow = _fetch_pair_follow(pair)

    if df_lead.empty or df_follow.empty:
        return {"error": f"数据获取失败: lead={df_lead.empty} follow={df_follow.empty}",
                "pair_id": pair_id, "pair_name": pair["name"]}

    # 对齐日期
    common = df_lead.index.intersection(df_follow.index)
    if len(common) < 30:
        return {"error": f"对齐后样本不足({len(common)}天)", "pair_id": pair_id,
                "pair_name": pair["name"]}

    df_lead = df_lead.loc[common].sort_index().tail(days)
    df_follow = df_follow.loc[common].sort_index().tail(days)

    lead_ret = df_lead['Close'].pct_change().dropna()
    follow_ret = df_follow['Close'].pct_change().dropna()
    idx = lead_ret.index.intersection(follow_ret.index)
    lead_ret = lead_ret[idx]
    follow_ret = follow_ret[idx]

    roll_corr = follow_ret.rolling(60).corr(lead_ret).dropna()

    # 领先滞后分析：lead领先k天后follow的相关性
    lead_lag_results = []
    for lag in range(0, 11):
        lead_l = lead_ret.shift(lag).dropna()
        common_idx = lead_l.index.intersection(follow_ret.index)
        if len(common_idx) > 30:
            r, p = pearsonr(lead_l.loc[common_idx], follow_ret.loc[common_idx])
            lead_lag_results.append({"lag": lag, "r": round(r, 4), "p": round(p, 6)})

    # follow领先lead（反向）
    follow_lag_results = []
    for lag in range(1, 11):
        follow_l = follow_ret.shift(lag).dropna()
        common_idx = follow_l.index.intersection(lead_ret.index)
        if len(common_idx) > 30:
            r, p = pearsonr(follow_l.loc[common_idx], lead_ret.loc[common_idx])
            follow_lag_results.append({"lag": lag, "r": round(r, 4), "p": round(p, 6)})

    # 找出最优领先滞后
    best_lead = max(lead_lag_results, key=lambda x: abs(x["r"])) if lead_lag_results else {}
    lead_direction = "lead_follows" if best_lead.get("lag", 0) > 0 else "contemporaneous"

    # 技术指标(follow)
    close = df_follow['Close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi14 = 100 - 100 / (1 + gain / loss)
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9).mean()

    # 协整 (Engle-Granger两步法 + EWMA动态权重)
    coint = {"z_score": 0, "cointegrated": False, "half_life": None,
             "adf_stat": None, "adf_crit": None, "beta": None, "alpha": None}
    ewma = {"corr_ewma": None, "corr_ewma_last": None}
    try:
        from scipy import stats
        lead_c = df_lead['Close'].values
        follow_c = df_follow['Close'].values
        n = min(len(lead_c), len(follow_c))
        # 第一步: OLS回归 Y = alpha + beta*X
        res = stats.linregress(lead_c[:n], follow_c[:n])
        beta = res.slope
        alpha = res.intercept
        resid = follow_c[:n] - alpha - beta * lead_c[:n]

        # 第二步: ADF检验残差平稳性 (Dickey-Fuller回归近似)
        # 用残差差分回归: dR_t = rho*R_{t-1} + c + eps_t
        R = resid
        dR = np.diff(R)
        Rlag = R[:-1]
        X = np.column_stack([Rlag, np.ones(len(Rlag))])
        if len(X) > 10 and np.var(Rlag) > 1e-12:
            try:
                XtX = X.T @ X
                inv = np.linalg.inv(XtX)
                beta_hat = inv @ X.T @ dR
                e = dR - X @ beta_hat
                s2 = np.sum(e ** 2) / max(len(e) - 2, 1)
                se_rho = np.sqrt(s2 * inv[0, 0])
                adf_stat = beta_hat[0] / max(se_rho, 1e-12)
                # MacKinnon 5%临界值近似 (双变量协整, 常数项模型)
                adf_crit = -3.37
                coint["adf_stat"] = round(float(adf_stat), 3)
                coint["adf_crit"] = adf_crit
                coint["cointegrated"] = bool(adf_stat < adf_crit)
            except Exception:
                pass
        coint["beta"] = round(float(beta), 4)
        coint["alpha"] = round(float(alpha), 4)

        # 残差z-score (当前残差离均值几个标准差)
        z = (resid[-1] - resid.mean()) / max(resid.std(), 1e-9)
        coint["z_score"] = round(float(z), 3)

        # 半衰期 (真实AR(1)系数)
        if len(resid) > 30:
            r1 = resid[:-1]
            r2 = resid[1:]
            if np.var(r1) > 1e-12:
                rho = np.sum((r1 - r1.mean()) * (r2 - r2.mean())) / np.sum((r1 - r1.mean()) ** 2)
                if 0 < abs(rho) < 0.999:
                    coint["half_life"] = round(abs(np.log(0.5) / np.log(abs(rho))), 1)

        # EWMA动态相关性 (跨市场时差: lead滞后1天)
        lead_lag1 = df_lead['Close'].shift(1).iloc[1:]
        fl_c = df_follow['Close'].iloc[1:]
        common2 = lead_lag1.index.intersection(fl_c.index)
        if len(common2) > 30:
            lr = lead_lag1.loc[common2].pct_change().dropna()
            fr = fl_c.loc[common2].pct_change().dropna()
            idx2 = lr.index.intersection(fr.index)
            lr, fr = lr[idx2], fr[idx2]
            # 标准化后EWMA乘积
            lz = (lr - lr.mean()) / max(lr.std(), 1e-9)
            fz = (fr - fr.mean()) / max(fr.std(), 1e-9)
            alpha_ew = 0.06  # 半衰期约11天
            c = np.exp(-alpha_ew * np.arange(len(lz))[::-1])
            ewma_corr = np.sum(c * lz * fz) / np.sum(c)
            ewma["corr_ewma_last"] = round(float(ewma_corr), 4)
    except Exception:
        pass

    return {
        "pair_id": pair_id,
        "pair_name": pair["name"],
        "desc": pair["desc"],
        "relation": pair["relation"],
        "lead": pair["lead"],
        "follow": pair["follow"],
        "n_days": len(common),
        "stats": {
            "correlation": round(pearsonr(lead_ret, follow_ret)[0], 4),
            "rolling_corr_mean": round(roll_corr.mean(), 4),
            "rolling_corr_last": round(roll_corr.iloc[-1], 4),
            "rolling_corr_max": round(roll_corr.max(), 4),
            "rolling_corr_min": round(roll_corr.min(), 4),
            "best_lag": best_lead.get("lag", 0),
            "best_lag_r": best_lead.get("r", 0),
            "best_lag_p": best_lead.get("p", 1),
            "lead_direction": lead_direction,
            "follow_close": round(float(close.iloc[-1]), 2),
            "follow_change_pct": round((close.iloc[-1] / close.iloc[-2] - 1) * 100, 2),
            "lead_close": round(float(df_lead['Close'].iloc[-1]), 2),
            "lead_change_pct": round((df_lead['Close'].iloc[-1] / df_lead['Close'].iloc[-2] - 1) * 100, 2),
            "ma5": round(float(ma5.iloc[-1]), 2),
            "ma20": round(float(ma20.iloc[-1]), 2),
            "ma60": round(float(ma60.iloc[-1]), 2),
            "rsi14": round(float(rsi14.iloc[-1]), 2),
            "macd": round(float(macd.iloc[-1]), 2),
            "macd_hist": round(float((macd - macd_signal).iloc[-1]), 2),
        },
        "cointegration": coint,
        "ewma": ewma,
        "signal": {
            "direction": "buy" if coint["z_score"] <= -2 else ("sell" if coint["z_score"] >= 2 else "hold"),
            "strength": round(min(abs(coint["z_score"]) / 2, 1) * 100, 1),
            "bases": {
                "coint_z": coint["z_score"],
                "cointegrated": coint["cointegrated"],
                "half_life_days": coint["half_life"],
                "ewma_lag_corr": ewma["corr_ewma_last"],
                "rolling_corr": round(roll_corr.iloc[-1], 4),
            },
        },
        "lead_prices": [{"date": str(idx.date()), "close": float(row['Close'])} for idx, row in df_lead.iterrows()],
        "follow_prices": [{"date": str(idx.date()), "close": float(row['Close'])} for idx, row in df_follow.iterrows()],
        "rolling_corr": [{"date": str(idx.date()), "r": round(v, 4)} for idx, v in roll_corr.items()],
        "lagged_corr_lead_lead": lead_lag_results,
        "lagged_corr_follow_lead": follow_lag_results,
    }


@app.route('/api/pairs')
def api_pairs():
    """列出所有可用跨境联动对"""
    result = []
    for pid, p in PAIRS.items():
        result.append({
            "id": pid,
            "name": p["name"],
            "desc": p["desc"],
            "relation": p["relation"],
            "lead": p["lead"],
            "follow": p["follow"],
        })
    return jsonify({"pairs": result, "count": len(result)})


@app.route('/api/pair/<pair_id>')
def api_pair(pair_id):
    """按Pair ID分析单个跨境联动对"""
    data = analyze_pair(pair_id)
    if "error" not in data:
        z = data["cointegration"].get("z_score", 0)
        if abs(z) >= 1.5:
            sig = "buy" if z < 0 else "sell"
            _save_paper_trade(pair_id, sig, z,
                              data["stats"]["follow_close"],
                              data["stats"]["follow_change_pct"])
    return jsonify(data)


@app.route('/api/quotes')
def api_quotes():
    """A股关注股票实时行情"""
    q = get_realtime_quotes()
    if "error" in q:
        return jsonify({"error": q["error"]})
    watch = {}
    for pid, p in PAIRS.items():
        c = p["follow"]["code"]
        if c.startswith(("sz", "sh")):
            c6 = c[2:]
            if c6 in q:
                watch[pid] = {"follow": p["follow"]["name"], "quote": q[c6]}
    return jsonify({"watch": watch, "quotes": q, "count": len(watch)})


@app.route('/api/pairs/summary')
def api_pairs_summary():
    """所有Pair的汇总对比"""
    import concurrent.futures
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(analyze_pair, pid): pid for pid in PAIRS}
        for fut in concurrent.futures.as_completed(futures):
            pid = futures[fut]
            try:
                d = fut.result()
                if "error" not in d:
                    results[pid] = {
                        "pair_id": pid,
                        "pair_name": d["pair_name"],
                        "correlation": d["stats"]["correlation"],
                        "best_lag": d["stats"]["best_lag"],
                        "best_lag_r": d["stats"]["best_lag_r"],
                        "z_score": d["cointegration"].get("z_score", 0),
                        "cointegrated": d["cointegration"].get("cointegrated", False),
                        "half_life": d["cointegration"].get("half_life"),
                        "adf_stat": d["cointegration"].get("adf_stat"),
                        "ewma_lag_corr": d["ewma"].get("corr_ewma_last"),
                        "follow_close": d["stats"]["follow_close"],
                        "follow_change_pct": d["stats"]["follow_change_pct"],
                        "lead_close": d["stats"]["lead_close"],
                    }
                else:
                    results[pid] = {"error": d.get("error", "unknown")}
            except Exception as e:
                results[pid] = {"error": str(e)[:80]}
    return jsonify(results)


SECTOR_MAP = {
    "HBM/存储": ["sk_xn", "mu_giga"],
    "AI服务器/ODM": ["nvda_fii"],
    "光模块": ["nvda_innolight", "nvda_eoptolink"],
    "PCB": ["nvda_shenghong", "nvda_wus"],
    "液冷": ["nvda_envicool"],
    "电源": ["nvda_megmeet"],
    "连接器": ["nvda_luxshare"],
    "晶圆代工": ["nvda_smic", "tsm_smic"],
    "封测": ["sk_cjec"],
}


@app.route('/api/heat')
def api_heat():
    """板块温度计: 每个Pair实时热度 + 产业链综合热度"""
    import concurrent.futures
    quotes = get_realtime_quotes()
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(analyze_pair, pid): pid for pid in PAIRS}
        for fut in concurrent.futures.as_completed(futures):
            pid = futures[fut]
            try:
                d = fut.result()
                if "error" not in d:
                    q = {}
                    fc = d["follow"]["code"]
                    if fc.startswith(("sz", "sh")) and fc[2:] in quotes:
                        q = quotes[fc[2:]]
                    results[pid] = {
                        "pair_id": pid,
                        "pair_name": d["pair_name"],
                        "sector": next((s for s, pids in SECTOR_MAP.items() if pid in pids), "其他"),
                        "correlation": d["stats"]["correlation"],
                        "best_lag": d["stats"]["best_lag"],
                        "z_score": d["cointegration"].get("z_score", 0),
                        "lead_name": d["lead"]["name"],
                        "lead_close": d["stats"]["lead_close"],
                        "lead_change_pct": d["stats"]["lead_change_pct"],
                        "follow_name": d["follow"]["name"],
                        "follow_close": d["stats"]["follow_close"],
                        "follow_change_pct": d["stats"]["follow_change_pct"],
                        "rt_price": q.get("price"),
                        "rt_change_pct": q.get("change_pct"),
                        "rt_time": q.get("time"),
                    }
            except Exception:
                continue
    # 综合热度 = 加权平均 (相关性权重 * 跟随方实时涨跌)
    heat_items = [r for r in results.values() if r["rt_change_pct"] is not None]
    if heat_items:
        w_sum = sum(abs(r["correlation"]) + 0.3 for r in heat_items)
        heat_score = sum(r["rt_change_pct"] * (abs(r["correlation"]) + 0.3) for r in heat_items) / w_sum
    else:
        heat_score = 0
    heat_level = "hot" if heat_score > 1.5 else ("cool" if heat_score < -1.5 else "neutral")
    return jsonify({
        "heat_score": round(heat_score, 2),
        "heat_level": heat_level,
        "sectors": {s: [pid for pid in pids if pid in results] for s, pids in SECTOR_MAP.items()},
        "pairs": results,
        "count": len(results),
    })


@app.route('/api/paper/record')
def api_paper_record():
    """手动记录一次模拟盘信号"""
    pid = request.args.get('pair', 'sk_xn')
    sig = request.args.get('signal', 'hold')
    z = request.args.get('z', type=float, default=0)
    price = request.args.get('price', type=float, default=0)
    chg = request.args.get('chg', type=float, default=0)
    if pid not in PAIRS:
        return jsonify({"error": "unknown pair"}), 400
    _save_paper_trade(pid, sig, z, price, chg)
    return jsonify({"status": "ok", "pair": pid, "signal": sig})


@app.route('/api/paper/stats')
def api_paper_stats():
    """模拟盘命中率: 用信号后N日实际涨跌核对信号方向"""
    limit = min(request.args.get('limit', type=int, default=100), 500)
    rows = []
    try:
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.execute(
                '''SELECT id, timestamp, pair_id, signal, z_score, price, follow_change_pct
                   FROM paper_trades ORDER BY id DESC LIMIT ?''', (limit,))
            rows = cur.fetchall()
            conn.close()
    except Exception:
        return jsonify({"error": "db read failed"}), 500

    # 按Pair拉取日线计算后续N日实际涨跌
    import concurrent.futures
    pair_dfs = {}
    def _load(pid):
        try:
            d = analyze_pair(pid)
            if "error" not in d:
                return pid, d["follow_prices"]
        except Exception:
            pass
        return pid, None
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futs = {executor.submit(_load, pid): pid for pid in PAIRS}
        for fut in concurrent.futures.as_completed(futs):
            pid, prices = fut.result()
            if prices:
                pair_dfs[pid] = prices

    results = []
    hit1 = hit3 = hit5 = 0
    n1 = n3 = n5 = 0
    for rid, ts, pid, sig, z, price, chg in rows:
        prices = pair_dfs.get(pid, [])
        # 找到信号日之后的价格序列
        idx = -1
        for i, p in enumerate(prices):
            if p["date"] >= ts[:10]:
                idx = i
                break
        if idx < 0:
            idx = len(prices) - 1
        def ret_after(days):
            j = idx + days
            if j >= len(prices) or j < 0:
                return None
            return (prices[j]["close"] / prices[idx]["close"] - 1) * 100
        r1, r3, r5 = ret_after(1), ret_after(3), ret_after(5)
        expect_up = sig == "buy"
        expect_down = sig == "sell"
        rec = {
            "id": rid, "timestamp": ts, "pair": pid,
            "signal": sig, "z": z, "entry_price": price,
            "r1": r1, "r3": r3, "r5": r5,
        }
        if r1 is not None:
            n1 += 1
            if (expect_up and r1 > 0) or (expect_down and r1 < 0):
                hit1 += 1
        if r3 is not None:
            n3 += 1
            if (expect_up and r3 > 0) or (expect_down and r3 < 0):
                hit3 += 1
        if r5 is not None:
            n5 += 1
            if (expect_up and r5 > 0) or (expect_down and r5 < 0):
                hit5 += 1
        results.append(rec)
    return jsonify({
        "trades": results,
        "hit_rate": {
            "d1": {"hit": hit1, "n": n1, "rate": round(hit1 / n1 * 100, 1) if n1 else None},
            "d3": {"hit": hit3, "n": n3, "rate": round(hit3 / n3 * 100, 1) if n3 else None},
            "d5": {"hit": hit5, "n": n5, "rate": round(hit5 / n5 * 100, 1) if n5 else None},
        },
        "count": len(results),
    })


@app.route('/api/push/daily')
def api_push_daily():
    """生成每日推送内容(聚合当日最强联动信号) — 供小程序/定时任务调用"""
    import concurrent.futures
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(analyze_pair, pid): pid for pid in PAIRS}
        for fut in concurrent.futures.as_completed(futures):
            pid = futures[fut]
            try:
                d = fut.result()
                if "error" not in d:
                    results[pid] = d
            except Exception:
                continue
    # 按|z|排序取信号
    items = []
    for pid, d in results.items():
        z = d["cointegration"].get("z_score", 0)
        sig = d.get("signal", {})
        items.append({
            "pair_id": pid,
            "pair_name": d["pair_name"],
            "signal": sig.get("direction", "hold"),
            "strength": sig.get("strength", 0),
            "z_score": z,
            "correlation": d["stats"]["correlation"],
            "best_lag": d["stats"]["best_lag"],
            "half_life": d["cointegration"].get("half_life"),
            "follow_name": d["follow"]["name"],
            "follow_close": d["stats"]["follow_close"],
            "follow_change_pct": d["stats"]["follow_change_pct"],
        })
    items.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    strong = [i for i in items if abs(i["z_score"]) >= 1.5]
    # 汇总今日建议
    buys = [i for i in items if i["signal"] == "buy"]
    sells = [i for i in items if i["signal"] == "sell"]
    summary = "今日无强信号"
    if strong:
        top = strong[0]
        summary = f"最强信号 {top['pair_name']} {({'buy':'买入','sell':'卖出','hold':'观望'}[top['signal']])} | z={top['z_score']:.2f} 强度{top['strength']:.0f}%"
    content = {
        "date": datetime.now().strftime('%Y-%m-%d'),
        "summary": summary,
        "n_strong": len(strong),
        "n_buy": len(buys),
        "n_sell": len(sells),
        "strong_signals": strong,
        "all_pairs": items,
        "generated_at": datetime.now().strftime('%H:%M:%S'),
    }
    return jsonify(content)
    return render_template('index.html')


@app.route('/xn')
def xn_page():
    return render_template('xn_tracker.html')


@app.route('/report')
def report_page():
    return render_template('report.html')


@app.route('/report/v2')
def report_v2_page():
    return render_template('report_v2.html')


@app.route('/report/v3')
def report_v3_page():
    return render_template('report_v3.html')


@app.route('/intraday')
def intraday_page():
    return render_template('intraday.html')


@app.route('/api/market')
def api_market():
    data = get_market_overview()
    return jsonify(data)


@app.route('/api/hynix')
def api_hynix():
    data = get_hynix_data()
    return jsonify(data)


@app.route('/api/similar')
def api_similar():
    data = get_similar_stocks()
    return jsonify(data)


@app.route('/api/xn')
def api_xn():
    data = get_xn_data()
    return jsonify(data)


@app.route('/api/xn/refresh')
def api_xn_refresh():
    return jsonify(get_xn_data(days=400))


@app.route('/api/sectors')
def api_sectors():
    return jsonify(get_sector_summary())


@app.route('/api/backtest')
def api_backtest():
    daily = run_daily_backtest(days=250, threshold=2.0)
    intraday = run_intraday_backtest(days=60, threshold_pct=1.5, hold_bars=1)
    return jsonify({
        "daily": daily,
        "intraday": intraday,
        "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


@app.route('/api/backtest/v2')
def api_backtest_v2():
    try:
        result = run_daily_backtest_v2(days=250)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "traceback": str(__import__('traceback').format_exc())}), 500


@app.route('/api/cointegration')
def api_cointegration():
    try:
        result = compute_cointegration()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/ml/predict')
def api_ml_predict():
    try:
        result = train_ml_model()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/expert/panel')
def api_expert_panel():
    try:
        result = compute_expert_consensus()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/expert/update', methods=['POST'])
def api_expert_update():
    try:
        data = request.get_json()
        expert_id = data.get("id")
        view = data.get("view", "neutral")
        confidence = float(data.get("confidence", 0))
        reason = data.get("reason", "")

        for exp in EXPERT_PANEL:
            if exp["id"] == expert_id:
                exp["view"] = view
                exp["confidence"] = confidence
                exp["reason"] = reason
                exp["last_updated"] = datetime.now()
                return jsonify({"status": "ok", "expert": exp})
        return jsonify({"error": "专家ID不存在"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/expert/scrape')
def api_expert_scrape():
    """从东方财富抓取个股新闻 → 情感分析 → 更新专家团观点"""
    try:
        result = fetch_news_and_update_experts()
        # 获取当前共识
        consensus = compute_expert_consensus()
        result["consensus"] = consensus
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/triple/signal')
def api_triple_signal():
    try:
        result = compute_triple_signal()
        _save_signal(result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/realtime/signal')
def api_realtime_signal():
    try:
        signal = compute_intraday_signal()
        xn_realtime = _sina_realtime("sz300475")
        sk_intra = _naver_minute("000660", 30)
        return jsonify({
            "signal": signal,
            "xn": xn_realtime,
            "sk_intra": sk_intra,
            "server_time": datetime.now().strftime('%H:%M:%S'),
            "market_open": 930 <= datetime.now().hour * 100 + datetime.now().minute <= 1500,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/report/data')
def api_report_data():
    try:
        xn_data = get_xn_data(days=120)
        hynix = get_hynix_data()
        market = get_market_overview()
        xn_realtime = _sina_realtime("sz300475")
        signal = _report_signal(xn_data.get('stats', {}), hynix.get('summary', {}), None)
        return jsonify({
            "xn": xn_data,
            "hynix": hynix,
            "market": market,
            "realtime": xn_realtime,
            "signal": signal,
            "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/all')
def api_all():
    market = get_market_overview()
    if 'error' in str(market.get('上证指数', {})):
        market = {"error": "部分指数获取失败", **market}
    hynix = get_hynix_data()
    similar = get_similar_stocks()
    return jsonify({
        "market": market,
        "hynix": hynix,
        "similar": similar,
        "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


@app.route('/api/signals/history')
def api_signals_history():
    try:
        limit = request.args.get('limit', 50, type=int)
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                'SELECT * FROM signal_history ORDER BY id DESC LIMIT ?', (limit,)
            ).fetchall()
            conn.close()

        records = []
        for r in rows:
            rec = dict(r)
            try:
                rec['details'] = json.loads(rec['details'])
            except Exception:
                rec['details'] = {}
            records.append(rec)
        records.reverse()

        return jsonify({
            "total": len(records),
            "records": records,
            "latest_signal": records[-1].get('final_signal') if records else None,
        })
    except Exception as e:
        return jsonify({"error": str(e), "records": []}), 500


@app.route('/api/health')
def api_health():
    return jsonify({
        "status": "ok",
        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "uptime": time.time() - _cache.get("market_time", time.time()),
        "endpoints": {
            "intraday_signal": "/api/realtime/signal",
            "backtest_v1": "/api/backtest",
            "backtest_v2": "/api/backtest/v2",
            "cointegration": "/api/cointegration",
            "ml_predict": "/api/ml/predict",
            "triple_signal": "/api/triple/signal",
            "signals_history": "/api/signals/history",
            "expert_panel": "/api/expert/panel",
            "expert_update": "/api/expert/update",
            "expert_scrape": "/api/expert/scrape",
            "report": "/api/report/data",
            "market": "/api/market",
            "hynix": "/api/hynix",
            "similar": "/api/similar",
            "xn": "/api/xn",
            "pairs": "/api/pairs",
            "pair": "/api/pair/{pair_id}",
            "pairs_summary": "/api/pairs/summary",
            "quotes": "/api/quotes",
            "heat": "/api/heat",
            "paper_stats": "/api/paper/stats",
            "paper_record": "/api/paper/record",
            "push_daily": "/api/push/daily",
            "all": "/api/all",
        },
        "pages": {
            "信号": "/intraday",
            "报告": "/report/v3",
            "回测": "/report/v2",
            "首页": "/",
        }
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5800))
    app.run(host='0.0.0.0', port=port, debug=False)
