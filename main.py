"""
=======================================================
  SMC Multi-Pair Bot  —  v12.6
  Pairs    : 18 pair forex + komoditas + BTCUSD
  Strategy : SMC (forex) + News Sentiment (BTC)
  AI       : Groq Llama3 (analisis makro & sentimen berita)
  Data     : yfinance + FRED API + NewsAPI + RSS Feed
  Notif    : Telegram

  CHANGELOG v12.6:
  [FIX] detect_structure: majority voting dari 3 lookback
        (40/60/80 candle) — lebih akurat baca trend.
        Partial trend (HH atau HL saja) sudah cukup.
  [FIX] detect_liquidity_sweep: ref window 50 candle,
        sweep zone diperluas ke 20 candle terakhir.
  [FIX] analyze_pair: 4H SIDEWAYS tidak skip — fallback
        ke struktur 1H. Skip hanya kalau keduanya SIDEWAYS.

  CHANGELOG v12.5:
  [NEW] SL/TP forex berbasis pip tetap per pair.
  [FIX] "Risk kalkulasi invalid, skip" tidak muncul lagi.

  CHANGELOG v12.3:
  [NEW] BTCUSD: strategi News Sentiment (RSS+NewsAPI+Groq).
  [FIX] KeyError 'structure' saat kirim sinyal BTC.
=======================================================
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_OK = True
except ImportError:
    PSYCOPG2_OK = False
    print("[DB] psycopg2 tidak tersedia, mode tanpa database")

BOT_TOKEN           = os.environ.get("BOT_TOKEN", "")
CHAT_ID             = os.environ.get("CHAT_ID", "")
GROQ_API_KEY        = os.environ.get("GROQ_API_KEY", "")
NEWS_API_KEY        = os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY        = os.environ.get("FRED_API_KEY", "")
CRYPTOPANIC_API_KEY = os.environ.get("CRYPTOPANIC_API_KEY", "")
CHECK_INTERVAL      = int(os.environ.get("CHECK_INTERVAL", "300"))
DATABASE_URL        = os.environ.get("DATABASE_URL", "")

MAX_SIGNALS_PER_CYCLE = 3

PAIRS = {
    "XAUUSD" : "GC=F",
    "USDJPY" : "JPY=X",
    "AUDCAD" : "AUDCAD=X",
    "EURJPY" : "EURJPY=X",
    "EURUSD" : "EURUSD=X",
    "GBPUSD" : "GBPUSD=X",
    "USDCHF" : "USDCHF=X",
    "USDCAD" : "USDCAD=X",
    "AUDUSD" : "AUDUSD=X",
    "NZDUSD" : "NZDUSD=X",
    "GBPJPY" : "GBPJPY=X",
    "CADJPY" : "CADJPY=X",
    "CHFJPY" : "CHFJPY=X",
    "EURGBP" : "EURGBP=X",
    "EURAUD" : "EURAUD=X",
    "GBPAUD" : "GBPAUD=X",
    "XAGUSD" : "SI=F",
    "USOIL"  : "CL=F",
    "BTCUSD" : "BTC-USD",
}

# Pair yang pakai strategi EMA+RSI scalping (bukan SMC)
SCALPING_PAIRS = {"BTCUSD"}

# Konfigurasi pip per pair
# pip_size  = nilai 1 pip dalam harga (misal 0.0001 untuk EURUSD)
# sl_pip    = maksimal SL dalam pip
# tp_pip    = target TP dalam pip (RR 1:2)
PIP_CONFIG = {
    "EURUSD" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "GBPUSD" : {"pip_size": 0.0001, "sl_pip": 30, "tp_pip": 60},
    "AUDUSD" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "NZDUSD" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "USDCHF" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "USDCAD" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "AUDCAD" : {"pip_size": 0.0001, "sl_pip": 25, "tp_pip": 50},
    "EURGBP" : {"pip_size": 0.0001, "sl_pip": 20, "tp_pip": 40},
    "EURAUD" : {"pip_size": 0.0001, "sl_pip": 30, "tp_pip": 60},
    "GBPAUD" : {"pip_size": 0.0001, "sl_pip": 35, "tp_pip": 70},
    "USDJPY" : {"pip_size": 0.01,   "sl_pip": 30, "tp_pip": 60},
    "EURJPY" : {"pip_size": 0.01,   "sl_pip": 35, "tp_pip": 70},
    "GBPJPY" : {"pip_size": 0.01,   "sl_pip": 40, "tp_pip": 80},
    "CADJPY" : {"pip_size": 0.01,   "sl_pip": 30, "tp_pip": 60},
    "CHFJPY" : {"pip_size": 0.01,   "sl_pip": 30, "tp_pip": 60},
    "XAUUSD" : {"pip_size": 0.1,    "sl_pip": 150,"tp_pip": 300},
    "XAGUSD" : {"pip_size": 0.001,  "sl_pip": 100,"tp_pip": 200},
    "USOIL"  : {"pip_size": 0.01,   "sl_pip": 80, "tp_pip": 160},
}


# ───────────────────────────────────────────────
#  DATABASE FUNCTIONS
# ───────────────────────────────────────────────

def get_db_conn():
    if not PSYCOPG2_OK or not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    except Exception as e:
        print(f"[DB] Koneksi gagal: {e}")
        return None

def init_db():
    conn = get_db_conn()
    if conn is None:
        print("[DB] Skip init — tidak ada koneksi")
        return
    try:
        cur = conn.cursor()

        # Tabel riwayat sinyal
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id          SERIAL PRIMARY KEY,
                created_at  TIMESTAMP DEFAULT NOW(),
                pair        VARCHAR(10),
                action      VARCHAR(5),
                entry       FLOAT,
                sl          FLOAT,
                tp          FLOAT,
                rr          FLOAT,
                sl_pip      FLOAT,
                tp_pip      FLOAT,
                structure   VARCHAR(20),
                st_level    FLOAT,
                signal_age  VARCHAR(30),
                is_btc      BOOLEAN DEFAULT FALSE,
                result      VARCHAR(10) DEFAULT NULL,
                closed_at   TIMESTAMP  DEFAULT NULL
            )
        """)

        # Tabel trade aktif yang belum kena TP/SL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS open_trades (
                id          SERIAL PRIMARY KEY,
                signal_id   INTEGER,
                pair        VARCHAR(10),
                symbol      VARCHAR(20),
                action      VARCHAR(5),
                entry       FLOAT,
                sl          FLOAT,
                tp          FLOAT,
                opened_at   TIMESTAMP DEFAULT NOW()
            )
        """)

        # Tabel state sent_signals
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sent_signals_state (
                pair        VARCHAR(10) PRIMARY KEY,
                sig_key     TEXT,
                sent_at     FLOAT
            )
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("[DB] ✅ Tabel siap (signal_history, open_trades, sent_signals_state)")
    except Exception as e:
        print(f"[DB] init_db error: {e}")
        try: conn.close()
        except: pass

def save_signal_history(result):
    """Simpan sinyal ke DB, return signal_id."""
    conn = get_db_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        is_btc = bool(result.get("scalping", False))
        cur.execute("""
            INSERT INTO signal_history
                (pair, action, entry, sl, tp, rr, sl_pip, tp_pip,
                 structure, st_level, signal_age, is_btc)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            result.get("pair"),
            result.get("action"),
            result.get("entry"),
            result.get("sl"),
            result.get("tp"),
            result.get("rr"),
            result.get("sl_pip", 0),
            result.get("tp_pip", 0),
            result.get("structure", ""),
            result.get("st_level", result.get("confidence", 0)),
            result.get("signal_age", result.get("sentiment", "")),
            is_btc,
        ))
        signal_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DB] ✅ Sinyal {result.get('pair')} tersimpan (id={signal_id})")
        return signal_id
    except Exception as e:
        print(f"[DB] save_signal_history error: {e}")
        try: conn.close()
        except: pass
        return None

def save_open_trade(signal_id, result, symbol):
    """Simpan trade aktif ke open_trades."""
    conn = get_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO open_trades
                (signal_id, pair, symbol, action, entry, sl, tp)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            signal_id,
            result.get("pair"),
            symbol,
            result.get("action"),
            result.get("entry"),
            result.get("sl"),
            result.get("tp"),
        ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DB] 📂 Open trade {result.get('pair')} disimpan")
    except Exception as e:
        print(f"[DB] save_open_trade error: {e}")
        try: conn.close()
        except: pass

def check_open_trades():
    """Cek semua open trade, update result jika kena TP atau SL."""
    conn = get_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM open_trades")
        trades = cur.fetchall()
        cur.close()

        if not trades:
            conn.close()
            return

        print(f"[DB] Checking {len(trades)} open trade...")

        for trade in trades:
            pair     = trade["pair"]
            symbol   = trade["symbol"]
            action   = trade["action"]
            entry    = trade["entry"]
            sl       = trade["sl"]
            tp       = trade["tp"]
            trade_id = trade["id"]
            sig_id   = trade["signal_id"]

            # Ambil harga terkini
            try:
                ticker = yf.Ticker(symbol)
                df_tmp = ticker.history(period="1d", interval="5m")
                if df_tmp is None or len(df_tmp) < 2:
                    continue
                current_price = df_tmp["Close"].iloc[-2]
            except Exception as e:
                print(f"[DB] Gagal ambil harga {pair}: {e}")
                continue

            result = None
            if action == "BUY":
                if current_price >= tp:
                    result = "PROFIT"
                elif current_price <= sl:
                    result = "LOSS"
            elif action == "SELL":
                if current_price <= tp:
                    result = "PROFIT"
                elif current_price >= sl:
                    result = "LOSS"

            if result:
                print(f"[DB] {pair} → {result} | Price:{round(current_price,5)} TP:{tp} SL:{sl}")

                # Hapus dari open_trades
                cur2 = conn.cursor()
                cur2.execute("DELETE FROM open_trades WHERE id = %s", (trade_id,))

                # Update result di signal_history
                cur2.execute("""
                    UPDATE signal_history
                    SET result = %s, closed_at = NOW()
                    WHERE id = %s
                """, (result, sig_id))
                conn.commit()
                cur2.close()

                # Kirim notif Telegram
                emj = "✅" if result == "PROFIT" else "❌"
                msg = (
                    f"{emj} <b>TRADE CLOSED — {pair}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📌 Arah        : {action}\n"
                    f"💰 Entry       : {entry}\n"
                    f"📍 Close Price : {round(current_price, 5)}\n"
                    f"🎯 TP          : {tp}\n"
                    f"🛑 SL          : {sl}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{'🏆 PROFIT — TP tercapai!' if result == 'PROFIT' else '💸 LOSS — SL kena.'}"
                )
                send_telegram(msg)

        conn.close()
    except Exception as e:
        print(f"[DB] check_open_trades error: {e}")
        try: conn.close()
        except: pass

def load_sent_signals():
    """Load state sent_signals dari DB saat bot start."""
    conn = get_db_conn()
    if conn is None:
        return {}, {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT pair, sig_key, sent_at FROM sent_signals_state")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        signals      = {r["pair"]: r["sig_key"] for r in rows}
        signals_time = {r["pair"]: r["sent_at"]  for r in rows}
        print(f"[DB] ✅ Load {len(signals)} state sinyal dari DB")
        return signals, signals_time
    except Exception as e:
        print(f"[DB] load_sent_signals error: {e}")
        try: conn.close()
        except: pass
        return {}, {}

def save_sent_signal_state(pair, sig_key, sent_at):
    """Simpan/update state sent_signals ke DB."""
    conn = get_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sent_signals_state (pair, sig_key, sent_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (pair) DO UPDATE
                SET sig_key = EXCLUDED.sig_key,
                    sent_at = EXCLUDED.sent_at
        """, (pair, sig_key, sent_at))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] save_sent_signal_state error: {e}")
        try: conn.close()
        except: pass

# ───────────────────────────────────────────────

def is_valid_session():
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    in_london  = 7 <= hour < 16
    in_newyork = 12 <= hour < 21
    in_asia    = 0 <= hour < 7
    return in_london or in_newyork or in_asia

def get_data(symbol, interval, period):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if (df is None or len(df) < 10) and interval == "4h":
            print(f"[DATA] {symbol} tidak support 4h, fallback ke 1h")
            df = ticker.history(period="30d", interval="1h")
        if df is None or len(df) < 10:
            return None
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"datetime": "time", "date": "time"})
        col = pd.to_datetime(df["time"])
        if col.dt.tz is not None:
            col = col.dt.tz_convert(None)
        else:
            col = col.dt.tz_localize(None)
        df["time"] = col
        df = df[["time", "open", "high", "low", "close", "volume"]]
        return df.reset_index(drop=True)
    except Exception as e:
        print(f"[DATA ERROR] {symbol} {interval}: {e}")
        return None

def find_swing_points(df, lookback=30):
    highs = []
    lows  = []
    data  = df.tail(lookback).reset_index(drop=True)
    for i in range(1, len(data) - 1):
        if data["high"].iloc[i] > data["high"].iloc[i-1] and \
           data["high"].iloc[i] > data["high"].iloc[i+1]:
            highs.append((i, data["high"].iloc[i]))
        if data["low"].iloc[i] < data["low"].iloc[i-1] and \
           data["low"].iloc[i] < data["low"].iloc[i+1]:
            lows.append((i, data["low"].iloc[i]))
    return highs, lows

def detect_structure(df):
    if len(df) < 15:
        return "SIDEWAYS"

    def _check(lookback):
        swing_highs, swing_lows = find_swing_points(df, lookback=lookback)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return "SIDEWAYS"
        sh1 = swing_highs[-2][1]; sh2 = swing_highs[-1][1]
        sl1 = swing_lows[-2][1];  sl2 = swing_lows[-1][1]
        hh = sh2 > sh1; hl = sl2 > sl1
        ll = sl2 < sl1; lh = sh2 < sh1
        if hh and hl:        return "UPTREND"
        elif ll and lh:      return "DOWNTREND"
        # partial: 1 tanda sudah cukup (lebih longgar)
        elif hh or hl:       return "UPTREND"
        elif ll or lh:       return "DOWNTREND"
        return "SIDEWAYS"

    # Majority voting dari 3 lookback berbeda
    votes = [_check(lb) for lb in (40, 60, 80)]
    up   = votes.count("UPTREND")
    down = votes.count("DOWNTREND")
    if up >= 2:   return "UPTREND"
    if down >= 2: return "DOWNTREND"
    # Tidak ada mayoritas → pakai big picture (lookback 80)
    return _check(80)

def detect_liquidity_sweep(df, structure):
    if len(df) < 20:
        return False, None
    # Perluas referensi ke 50 candle, exclude 3 candle terbaru agar tidak bias
    ref_win  = min(50, len(df) - 3)
    ref_high = df["high"].iloc[-ref_win:-3].max()
    ref_low  = df["low"].iloc[-ref_win:-3].min()
    # Perluas sweep zone ke 20 candle terakhir
    sweep_zone = df.iloc[-20:-1]
    for i in range(len(sweep_zone) - 1, -1, -1):
        c = sweep_zone.iloc[i]
        if structure == "UPTREND":
            if c["low"] < ref_low:
                return True, round(ref_low, 5)
        elif structure == "DOWNTREND":
            if c["high"] > ref_high:
                return True, round(ref_high, 5)
    return False, None

def detect_bos(df, structure):
    if len(df) < 9:
        return False, None
    prev_high = df["high"].iloc[-8:-3].max()
    prev_low  = df["low"].iloc[-8:-3].min()
    if structure == "UPTREND":
        # cek 2 candle terbaru setelah sweep
        if df["high"].iloc[-2] > prev_high or df["high"].iloc[-3] > prev_high:
            return True, round(prev_high, 4)
    elif structure == "DOWNTREND":
        if df["low"].iloc[-2] < prev_low or df["low"].iloc[-3] < prev_low:
            return True, round(prev_low, 4)
    return False, None

def detect_fvg(df, structure, pair=""):
    if len(df) < 4:
        print(f"[{pair}] FVG: data terlalu sedikit")
        return False, None, None
    search_window = min(15, len(df) - 3)
    if search_window < 1:
        return False, None, None
    fvg_found = 0
    fvg_valid = 0
    for i in range(len(df) - 3, len(df) - search_window - 1, -1):
        c1 = df.iloc[i]
        c3 = df.iloc[i + 2]
        if structure == "UPTREND":
            gap_low  = c1["high"]
            gap_high = c3["low"]
            if gap_high > gap_low:
                fvg_found += 1
                candles_after = df.iloc[i + 3:]
                if len(candles_after) >= 1 and candles_after["low"].min() > gap_low:
                    fvg_valid += 1
                    print(f"[{pair}] FVG Bullish: {round(gap_low,4)}–{round(gap_high,4)}")
                    return True, round(gap_low, 4), round(gap_high, 4)
        elif structure == "DOWNTREND":
            fvg_top = c1["low"]
            fvg_bot = c3["high"]
            if fvg_bot < fvg_top:
                fvg_found += 1
                candles_after = df.iloc[i + 3:]
                if len(candles_after) >= 1 and candles_after["high"].max() < fvg_top:
                    fvg_valid += 1
                    print(f"[{pair}] FVG Bearish: {round(fvg_bot,4)}–{round(fvg_top,4)}")
                    return True, round(fvg_bot, 4), round(fvg_top, 4)
    print(f"[{pair}] FVG: ditemukan {fvg_found} gap, valid & unfilled: {fvg_valid} → tidak ada")
    return False, None, None

def confirm_entry(df, structure):
    if len(df) < 3:
        return False
    prev = df.iloc[-3]
    curr = df.iloc[-2]
    cb  = abs(curr["close"] - curr["open"])
    atr = (df["high"] - df["low"]).tail(14).mean()
    if atr == 0:
        return False
    # Body candle minimal 8% ATR (lebih wajar dari sebelumnya)
    if cb < atr * 0.08:
        return False
    if structure == "UPTREND":
        return (
            curr["close"] > curr["open"] and   # candle M15 harus bullish
            curr["close"] > prev["close"]       # close lebih tinggi dari candle sebelumnya
        )
    elif structure == "DOWNTREND":
        return (
            curr["close"] < curr["open"] and   # candle M15 harus bearish
            curr["close"] < prev["close"]       # close lebih rendah dari candle sebelumnya
        )
    return False

def calc_sl_tp(df, structure, sweep_level, pair=""):
    price       = df["close"].iloc[-2]
    last_candle = df.iloc[-2]

    cfg      = PIP_CONFIG.get(pair, None)
    pip_size = cfg["pip_size"] if cfg else 0.0001
    sl_pip   = cfg["sl_pip"]   if cfg else 30
    tp_pip   = cfg["tp_pip"]   if cfg else 60

    sl_dist = sl_pip * pip_size
    tp_dist = tp_pip * pip_size

    if structure == "UPTREND":
        sl_candle = last_candle["low"] - (pip_size * 3)
        sl_ideal  = min(sl_candle, sweep_level - (pip_size * 3))
        if (price - sl_ideal) > sl_dist:
            sl = round(price - sl_dist, 5)
            print(f"[CALC] {pair} SL fallback ke {sl_pip} pip")
        else:
            sl = round(sl_ideal, 5)
        risk_pip      = round((price - sl) / pip_size, 1)
        tp            = round(price + tp_dist, 5)
        tp_pip_actual = round((tp - price) / pip_size, 1)
        action        = "BUY"
    else:
        sl_candle = last_candle["high"] + (pip_size * 3)
        sl_ideal  = max(sl_candle, sweep_level + (pip_size * 3))
        if (sl_ideal - price) > sl_dist:
            sl = round(price + sl_dist, 5)
            print(f"[CALC] {pair} SL fallback ke {sl_pip} pip")
        else:
            sl = round(sl_ideal, 5)
        risk_pip      = round((sl - price) / pip_size, 1)
        tp            = round(price - tp_dist, 5)
        tp_pip_actual = round((price - tp) / pip_size, 1)
        action        = "SELL"

    if risk_pip <= 0:
        return None, round(price, 5), round(price, 5), round(price, 5), 0, 0
    rr = round(tp_pip_actual / risk_pip, 1) if risk_pip > 0 else 0
    print(f"[CALC] {pair} SL:{risk_pip}pip TP:{tp_pip_actual}pip RR:1:{rr}")
    return action, round(price, 5), sl, tp, risk_pip, tp_pip_actual

def get_news(pair):
    keywords = {
        "XAUUSD": "gold XAU USD Federal Reserve",
        "USDJPY": "USD JPY Bank of Japan Fed",
        "AUDCAD": "AUD CAD Australia Canada oil",
        "EURJPY": "EUR JPY Euro Japan ECB",
        "EURUSD": "EUR USD Euro ECB Federal Reserve",
        "GBPUSD": "GBP USD Bank of England Fed",
        "USDCHF": "USD CHF Swiss National Bank",
        "USDCAD": "USD CAD Canada oil Bank of Canada",
        "AUDUSD": "AUD USD Australia RBA",
        "NZDUSD": "NZD USD New Zealand RBNZ",
        "GBPJPY": "GBP JPY Bank of England Japan",
        "CADJPY": "CAD JPY Canada Japan oil",
        "CHFJPY": "CHF JPY Swiss Japan",
        "EURGBP": "EUR GBP ECB Bank of England",
        "EURAUD": "EUR AUD ECB Australia",
        "GBPAUD": "GBP AUD Britain Australia",
        "XAGUSD": "silver XAG USD commodities",
        "USOIL" : "crude oil WTI OPEC",
    }
    kw = keywords.get(pair, "forex")
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q"       : kw,
                "language": "en",
                "sortBy"  : "publishedAt",
                "pageSize": 3,
                "apiKey"  : NEWS_API_KEY,
            },
            timeout=10
        )
        if r.status_code == 429:
            print(f"[NEWS] Rate limit (429), skip {pair}")
            return []
        if r.status_code != 200:
            print(f"[NEWS] HTTP {r.status_code} untuk {pair}")
            return []
        articles = r.json().get("articles", [])
        return [a["title"] for a in articles[:3]]
    except Exception as e:
        print(f"[NEWS ERROR] {e}")
        return []

def get_fred_data():
    indicators = {
        "Fed Rate"    : "FEDFUNDS",
        "CPI US"      : "CPIAUCSL",
        "NFP"         : "PAYEMS",
        "GDP US"      : "GDP",
        "DXY"         : "DTWEXBGS",
        "Unemployment": "UNRATE",
    }
    result = {}
    for name, series_id in indicators.items():
        try:
            r = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id" : series_id,
                    "api_key"   : FRED_API_KEY,
                    "file_type" : "json",
                    "sort_order": "desc",
                    "limit"     : 2,
                },
                timeout=10
            )
            if r.status_code != 200:
                print(f"[FRED] HTTP {r.status_code} untuk {name}, skip")
                continue
            obs = r.json().get("observations", [])
            if len(obs) >= 2:
                latest = obs[0]["value"]
                prev   = obs[1]["value"]
                if latest == "." or prev == ".":
                    print(f"[FRED] {name}: data kosong (.), skip")
                    continue
                result[name] = {
                    "latest": latest,
                    "prev"  : prev,
                    "change": "NAIK"  if float(latest) > float(prev)
                              else "TURUN" if float(latest) < float(prev)
                              else "SAMA",
                }
        except Exception as e:
            print(f"[FRED ERROR] {name}: {e}")
    return result

def format_fred_data(fred_data):
    if not fred_data:
        return "Data ekonomi tidak tersedia"
    lines  = []
    arrows = {"NAIK": "⬆️", "TURUN": "⬇️", "SAMA": "➡️"}
    for name, data in fred_data.items():
        arrow = arrows.get(data["change"], "")
        lines.append(f"• {name}: {data['latest']} {arrow} (sebelumnya: {data['prev']})")
    return "\n".join(lines)

def analyze_with_groq(pair, structure, action, headlines, fred_data):
    if not GROQ_API_KEY:
        return "Analisis AI tidak tersedia."
    try:
        news_text = "\n".join([f"- {h}" for h in headlines]) if headlines else "Tidak ada berita terkini."
        fred_text = ""
        if fred_data:
            fred_text = f"""
Data Ekonomi Makro Terkini:
- Fed Rate    : {fred_data.get('Fed Rate', {}).get('latest', 'N/A')}% ({fred_data.get('Fed Rate', {}).get('change', 'N/A')})
- CPI US      : {fred_data.get('CPI US', {}).get('latest', 'N/A')} ({fred_data.get('CPI US', {}).get('change', 'N/A')})
- NFP         : {fred_data.get('NFP', {}).get('latest', 'N/A')}K ({fred_data.get('NFP', {}).get('change', 'N/A')})
- GDP US      : {fred_data.get('GDP US', {}).get('latest', 'N/A')} ({fred_data.get('GDP US', {}).get('change', 'N/A')})
- Unemployment: {fred_data.get('Unemployment', {}).get('latest', 'N/A')}% ({fred_data.get('Unemployment', {}).get('change', 'N/A')})
- DXY         : {fred_data.get('DXY', {}).get('latest', 'N/A')} ({fred_data.get('DXY', {}).get('change', 'N/A')})"""

        prompt = f"""Kamu adalah analis trading forex dan ekonomi makro profesional tingkat senior.

Pair: {pair}
Sinyal Teknikal: {action}
Struktur Market: {structure}

{fred_text}

Berita Terkini:
{news_text}

Tugas kamu:
1. Analisis apakah kondisi ekonomi makro mendukung sinyal {action} pada {pair}
2. Jelaskan dampak data Fed Rate, CPI, NFP terhadap {pair}
3. Apakah fundamental SEJALAN atau BERLAWANAN dengan sinyal teknikal?
4. Berikan prediksi arah harga berdasarkan fundamental

Jawab dalam Bahasa Indonesia, maksimal 5 kalimat, langsung ke poin tanpa intro."""

        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type" : "application/json"
            },
            json={
                "model"     : "llama-3.1-8b-instant",
                "messages"  : [{"role": "user", "content": prompt}],
                "max_tokens": 300,
            },
            timeout=15
        )
        if r.status_code != 200:
            print(f"[GROQ] HTTP {r.status_code}: {r.text[:100]}")
            return "Analisis AI tidak tersedia saat ini."
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[GROQ ERROR] {e}")
        return "Analisis AI tidak tersedia saat ini."

def send_telegram(message):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code == 200:
            print("[TELEGRAM] ✅ Terkirim!")
        else:
            print(f"[TELEGRAM] ❌ {r.text}")
    except Exception as e:
        print(f"[TELEGRAM] ❌ {e}")


def get_btc_news_cryptopanic():
    """Ambil berita BTC terbaru dari CryptoPanic API."""
    if not CRYPTOPANIC_API_KEY:
        print("[CRYPTOPANIC] API key tidak ada, skip")
        return []
    try:
        r = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={
                "auth_token" : CRYPTOPANIC_API_KEY,
                "currencies" : "BTC",
                "filter"     : "hot",
                "public"     : "true",
            },
            timeout=10
        )
        if r.status_code != 200:
            print(f"[CRYPTOPANIC] HTTP {r.status_code}")
            return []
        results = r.json().get("results", [])
        headlines = []
        for item in results[:8]:
            title  = item.get("title", "")
            source = item.get("source", {}).get("title", "")
            votes  = item.get("votes", {})
            bull   = votes.get("positive", 0)
            bear   = votes.get("negative", 0)
            if title:
                headlines.append({
                    "title"    : title,
                    "source"   : source,
                    "bullish"  : bull,
                    "bearish"  : bear,
                })
        print(f"[CRYPTOPANIC] {len(headlines)} berita diambil")
        return headlines
    except Exception as e:
        print(f"[CRYPTOPANIC ERROR] {e}")
        return []

def get_btc_news_newsapi():
    """Ambil berita BTC/crypto dari NewsAPI."""
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q"       : "Bitcoin BTC crypto Trump interest rate Fed",
                "language": "en",
                "sortBy"  : "publishedAt",
                "pageSize": 6,
                "apiKey"  : NEWS_API_KEY,
            },
            timeout=10
        )
        if r.status_code == 429:
            print("[NEWSAPI] Rate limit, skip BTC news")
            return []
        if r.status_code != 200:
            print(f"[NEWSAPI] HTTP {r.status_code}")
            return []
        articles = r.json().get("articles", [])
        return [a["title"] for a in articles[:6] if a.get("title")]
    except Exception as e:
        print(f"[NEWSAPI BTC ERROR] {e}")
        return []

def get_btc_news_rss():
    """Ambil berita BTC terbaru dari RSS feed CoinDesk & CoinTelegraph (delay ~5-10 menit)."""
    feeds = [
        ("CoinDesk",       "https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ("CoinTelegraph",  "https://cointelegraph.com/rss"),
        ("Bitcoin Magazine","https://bitcoinmagazine.com/.rss/full/"),
    ]
    keywords = [
        "bitcoin", "btc", "crypto", "trump", "fed", "interest rate",
        "etf", "sec", "whale", "halving", "fomc", "rate cut", "rate hike",
        "inflation", "recession", "tariff", "regulation",
    ]
    headlines = []
    for source, url in feeds:
        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                print(f"[RSS] {source} HTTP {r.status_code}, skip")
                continue
            root = ET.fromstring(r.content)
            items = root.findall(".//item")
            count = 0
            for item in items[:20]:
                title = item.findtext("title", "").strip()
                if not title:
                    continue
                title_lower = title.lower()
                # Hanya ambil berita yang relevan dengan BTC/crypto/makro
                if any(kw in title_lower for kw in keywords):
                    headlines.append(f"[{source}] {title}")
                    count += 1
                if count >= 4:
                    break
            print(f"[RSS] {source}: {count} berita relevan")
        except Exception as e:
            print(f"[RSS ERROR] {source}: {e}")
    print(f"[RSS] Total: {len(headlines)} berita dari RSS")
    return headlines

def analyze_btc_sentiment_groq(cp_news, na_news, rss_news, price):
    """Kirim semua berita ke Groq, minta analisis sentimen & sinyal BTC."""
    if not GROQ_API_KEY:
        print("[GROQ] API key tidak ada")
        return None, "Tidak tersedia", []

    # Gabungkan berita dari ketiga sumber
    all_headlines = []
    cp_titles     = []
    for item in cp_news:
        line = f"- [{item['source']}] {item['title']}"
        if item["bullish"] or item["bearish"]:
            line += f" (👍{item['bullish']} 👎{item['bearish']})"
        all_headlines.append(line)
        cp_titles.append(item["title"])
    for h in na_news:
        all_headlines.append(f"- [NewsAPI] {h}")
    for h in rss_news:
        all_headlines.append(f"- {h}")

    if not all_headlines:
        print("[GROQ] Tidak ada berita untuk dianalisis")
        return None, "Tidak ada berita", []

    news_block = "\n".join(all_headlines)

    prompt = f"""Kamu adalah analis crypto profesional. Harga BTC saat ini: ${price:,.2f}

Berikut berita-berita terbaru Bitcoin dari berbagai sumber:
{news_block}

Tugasmu:
1. Identifikasi berita PALING PENTING yang mempengaruhi harga BTC (terutama: kebijakan Trump, suku bunga Fed/FOMC, regulasi crypto, ETF Bitcoin, whale movement, halving)
2. Tentukan sentimen keseluruhan: BULLISH, BEARISH, atau NEUTRAL
3. Jika BULLISH → rekomendasikan BUY. Jika BEARISH → rekomendasikan SELL. Jika NEUTRAL → NO TRADE
4. Berikan alasan singkat 3-4 kalimat

Jawab HANYA dalam format JSON berikut (tanpa teks lain, tanpa markdown):
{{
  "signal": "BUY" atau "SELL" atau "NO_TRADE",
  "sentiment": "BULLISH" atau "BEARISH" atau "NEUTRAL",
  "confidence": angka 1-100,
  "key_news": "judul berita paling berpengaruh",
  "reason": "alasan singkat 3-4 kalimat dalam Bahasa Indonesia"
}}"""

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type" : "application/json",
            },
            json={
                "model"      : "llama-3.1-8b-instant",
                "messages"   : [{"role": "user", "content": prompt}],
                "max_tokens" : 400,
                "temperature": 0.3,
            },
            timeout=20
        )
        if r.status_code != 200:
            print(f"[GROQ BTC] HTTP {r.status_code}: {r.text[:100]}")
            return None, "Groq error", cp_titles + na_news

        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Bersihkan jika ada markdown
        raw = raw.replace("```json", "").replace("```", "").strip()

        import json
        parsed = json.loads(raw)
        signal     = parsed.get("signal", "NO_TRADE")
        sentiment  = parsed.get("sentiment", "NEUTRAL")
        confidence = parsed.get("confidence", 0)
        key_news   = parsed.get("key_news", "-")
        reason     = parsed.get("reason", "-")

        print(f"[GROQ BTC] Signal:{signal} | Sentimen:{sentiment} | Confidence:{confidence}%")
        print(f"[GROQ BTC] Key News: {key_news}")

        return {
            "signal"    : signal,
            "sentiment" : sentiment,
            "confidence": confidence,
            "key_news"  : key_news,
            "reason"    : reason,
        }, reason, cp_titles + na_news + rss_news

    except Exception as e:
        print(f"[GROQ BTC ERROR] {e}")
        return None, "Parse error", cp_titles + na_news + rss_news

def analyze_btc_scalping(pair, symbol):
    """Strategi News Sentiment Analysis untuk BTCUSD — berbasis CryptoPanic + NewsAPI + Groq AI."""
    print(f"\n[{pair}] Menganalisis (News Sentiment)...")

    # Ambil harga BTC saat ini
    df_1h = get_data(symbol, "1h", "3d")
    if df_1h is None or len(df_1h) < 5:
        print(f"[{pair}] Data harga tidak tersedia")
        return None

    price = df_1h["close"].iloc[-2]
    atr   = (df_1h["high"] - df_1h["low"]).tail(14).mean()

    # Ambil berita dari ketiga sumber
    cp_news  = get_btc_news_cryptopanic()
    na_news  = get_btc_news_newsapi()
    rss_news = get_btc_news_rss()

    total_news = len(cp_news) + len(na_news) + len(rss_news)
    print(f"[{pair}] Total berita: CryptoPanic={len(cp_news)} NewsAPI={len(na_news)} RSS={len(rss_news)}")

    if total_news == 0:
        print(f"[{pair}] Tidak ada berita tersedia, skip")
        return None

    # Analisis sentimen via Groq
    groq_result, reason, all_headlines = analyze_btc_sentiment_groq(cp_news, na_news, rss_news, price)

    if groq_result is None:
        print(f"[{pair}] Groq gagal menganalisis")
        return None

    signal     = groq_result["signal"]
    sentiment  = groq_result["sentiment"]
    confidence = groq_result["confidence"]
    key_news   = groq_result["key_news"]

    # Minimum confidence 55% agar tidak sembarangan
    if confidence < 55:
        print(f"[{pair}] Confidence terlalu rendah ({confidence}%), NO TRADE")
        return None

    if signal == "NO_TRADE":
        print(f"[{pair}] Sentimen NEUTRAL → NO TRADE")
        return None

    # Kalkulasi SL/TP berbasis ATR
    buffer = atr * 0.3
    if signal == "BUY":
        sl   = round(price - atr * 1.5 - buffer, 2)
        risk = price - sl
        if risk <= 0:
            return None
        tp = round(price + risk * 2.0, 2)
    else:  # SELL
        sl   = round(price + atr * 1.5 + buffer, 2)
        risk = sl - price
        if risk <= 0:
            return None
        tp = round(price - risk * 2.0, 2)

    rr = round(abs(tp - price) / abs(price - sl), 2) if abs(price - sl) > 0 else 0

    print(f"[{pair}] ✅ SINYAL {signal} | Entry:{price} SL:{sl} TP:{tp} RR:1:{rr} | Confidence:{confidence}%")

    return {
        "pair"       : pair,
        "action"     : signal,
        "entry"      : round(price, 2),
        "sl"         : sl,
        "tp"         : tp,
        "rr"         : rr,
        "sentiment"  : sentiment,
        "confidence" : confidence,
        "key_news"   : key_news,
        "reason"     : reason,
        "headlines"  : all_headlines[:5],
        "scalping"   : True,
    }

def calculate_supertrend(df, period=10, multiplier=3.0):
    df    = df.copy().reset_index(drop=True)
    high  = df['high']
    low   = df['low']
    close = df['close']

    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()

    hl2   = (high + low) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    final_upper = upper.copy()
    final_lower = lower.copy()

    for i in range(1, len(df)):
        final_upper.iloc[i] = (
            upper.iloc[i]
            if upper.iloc[i] < final_upper.iloc[i-1]
            or close.iloc[i-1] > final_upper.iloc[i-1]
            else final_upper.iloc[i-1]
        )
        final_lower.iloc[i] = (
            lower.iloc[i]
            if lower.iloc[i] > final_lower.iloc[i-1]
            or close.iloc[i-1] < final_lower.iloc[i-1]
            else final_lower.iloc[i-1]
        )

    supertrend        = pd.Series(index=df.index, dtype=bool)
    supertrend.iloc[0] = True
    for i in range(1, len(df)):
        if   close.iloc[i] > final_upper.iloc[i-1]:
            supertrend.iloc[i] = True
        elif close.iloc[i] < final_lower.iloc[i-1]:
            supertrend.iloc[i] = False
        else:
            supertrend.iloc[i] = supertrend.iloc[i-1]

    df['supertrend_bull']  = supertrend
    df['supertrend_level'] = final_lower.where(supertrend, final_upper)
    df['atr']              = atr
    return df


def analyze_pair(pair, symbol):
    print(f"\n{'='*10} [{pair}] {'='*10}")
    print(f"[{pair}] Menganalisis...")

    df_1h  = get_data(symbol, "1h",  "30d")
    df_15m = get_data(symbol, "15m", "5d")
    df_5m  = get_data(symbol, "5m",  "5d")

    if df_1h is None or len(df_1h) < 20:
        print(f"[{pair}] Data 1H tidak tersedia, skip")
        return None
    if df_15m is None or len(df_15m) < 20:
        print(f"[{pair}] Data 15M tidak tersedia, skip")
        return None

    # --- EMA 50 pada 1H sebagai trend filter ---
    df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
    ema50_1h       = df_1h['ema50'].iloc[-2]
    price_1h       = df_1h['close'].iloc[-2]
    trend_1h       = "UPTREND" if price_1h > ema50_1h else "DOWNTREND"
    print(f"[{pair}] Trend 1H (EMA50): {trend_1h} | Price:{round(price_1h,5)} EMA50:{round(ema50_1h,5)}")

    # --- Supertrend pada 15M ---
    df_15m       = calculate_supertrend(df_15m, period=10, multiplier=3.0)
    st_bull_15m  = df_15m['supertrend_bull'].iloc[-2]
    st_level_15m = df_15m['supertrend_level'].iloc[-2]
    st_dir_15m   = "UPTREND" if st_bull_15m else "DOWNTREND"

    if pd.isna(st_level_15m):
        st_level_15m = (
            df_15m['low'].iloc[-20:].min()
            if st_bull_15m else
            df_15m['high'].iloc[-20:].max()
        )
        print(f"[{pair}] ST Level NaN → fallback: {round(st_level_15m,5)}")

    print(f"[{pair}] Supertrend 15M: {st_dir_15m} | Level:{round(st_level_15m,5)}")

    # --- Trend 1H dan Supertrend 15M harus searah ---
    if trend_1h != st_dir_15m:
        print(f"[{pair}] 1H ({trend_1h}) vs ST 15M ({st_dir_15m}) berlawanan → NO TRADE")
        return None

    structure = trend_1h

    # --- Konfirmasi entry via EMA 21 pada 5M ---
    if df_5m is not None and len(df_5m) >= 20:
        df_5m['ema21'] = df_5m['close'].ewm(span=21, adjust=False).mean()
        price_5m       = df_5m['close'].iloc[-2]
        ema21_5m       = df_5m['ema21'].iloc[-2]
        if structure == "UPTREND" and price_5m < ema21_5m:
            print(f"[{pair}] 5M: harga di bawah EMA21 → belum konfirmasi BUY")
            return None
        elif structure == "DOWNTREND" and price_5m > ema21_5m:
            print(f"[{pair}] 5M: harga di atas EMA21 → belum konfirmasi SELL")
            return None
        print(f"[{pair}] 5M konfirmasi ✅ Price:{round(price_5m,5)} EMA21:{round(ema21_5m,5)}")
        df_entry = df_5m
    else:
        print(f"[{pair}] Data 5M tidak ada, pakai 15M untuk entry")
        df_entry = df_15m

    # --- Kalkulasi SL/TP ---
    action, entry, sl, tp, sl_pip_val, tp_pip_val = calc_sl_tp(
        df_entry, structure, st_level_15m, pair
    )
    if action is None:
        print(f"[{pair}] Risk kalkulasi invalid, skip")
        return None

    rr = round(tp_pip_val / sl_pip_val, 1) if sl_pip_val > 0 else 0

    # Cek apakah Supertrend baru flip
    st_prev    = df_15m['supertrend_bull'].iloc[-3]
    st_curr    = df_15m['supertrend_bull'].iloc[-2]
    signal_age = "🔥 BARU FLIP" if st_prev != st_curr else "✅ Sudah Konfirmasi"

    print(f"[{pair}] ✅ SINYAL {action} | Entry:{entry} SL:{sl}(-{sl_pip_val}pip) TP:{tp}(+{tp_pip_val}pip) RR:1:{rr}")

    return {
        "pair"          : pair,
        "action"        : action,
        "entry"         : entry,
        "optimal_entry" : round(st_level_15m, 5),
        "sl"            : sl,
        "tp"            : tp,
        "rr"            : rr,
        "sl_pip"        : sl_pip_val,
        "tp_pip"        : tp_pip_val,
        "structure"     : trend_1h,
        "structure_15m" : st_dir_15m,
        "ema50_1h"      : round(ema50_1h, 5),
        "st_level"      : round(st_level_15m, 5),
        "signal_age"    : signal_age,
    }
  
def main():
    print("=" * 55)
    print("  SMC Multi-Pair Bot  v12.6  [Railway]")
    print(f"  Pairs   : {len(PAIRS)} pairs aktif")
    print(f"  Interval: {CHECK_INTERVAL}s")
    print(f"  Max sinyal/cycle: {MAX_SIGNALS_PER_CYCLE}")
    print("=" * 55)

    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN / CHAT_ID belum diset!")
        return

    send_telegram(
        "🤖 <b>SMC Multi-Pair Bot v12.2 — ONLINE!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Pairs       : {len(PAIRS)} pair aktif\n"
        "📈 Strategy    : Smart Money Concept\n"
        "🧠 AI          : Groq Llama3 (Makro Ekonomi)\n"
        "📰 News        : NewsAPI\n"
        "📊 Ekonomi     : FRED API (Fed Rate, CPI, NFP, GDP)\n"
        "🕐 Session     : London, New York & Asia\n"
        f"🔢 Max Sinyal  : {MAX_SIGNALS_PER_CYCLE} per siklus\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Bot berjalan 24 jam di Railway!"
    )

    init_db()
    sent_signals, sent_signals_time = load_sent_signals()
    SIGNAL_EXPIRE_SECS = 4 * 3600
    fred_data          = {}
    fred_timer         = 0

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Scanning {len(PAIRS)} pairs...")

        if not is_valid_session():
            now_utc = datetime.now(timezone.utc)
            print(f"[SESSION] Di luar sesi ({now_utc.strftime('%H:%M')} UTC) → skip scan")
            time.sleep(CHECK_INTERVAL)
            continue

        if time.time() - fred_timer > 3600:
            print("[FRED] Update data ekonomi...")
            fred_data  = get_fred_data()
            fred_timer = time.time()
            print(f"[FRED] {len(fred_data)} indikator berhasil diambil")

        signals_this_cycle = 0
        check_open_trades()

        for pair, symbol in PAIRS.items():
            if signals_this_cycle >= MAX_SIGNALS_PER_CYCLE:
                print(f"[LIMIT] Max {MAX_SIGNALS_PER_CYCLE} sinyal tercapai, skip sisa pair")
                break

            try:
                if pair in SCALPING_PAIRS:
                    result = analyze_btc_scalping(pair, symbol)
                else:
                    result = analyze_pair(pair, symbol)
                if result is None:
                    continue

                if result.get('scalping'):
                    sig_key = f"{pair}_{result['action']}_{result['sentiment']}"
                else:
                    sig_key = f"{pair}_{result['action']}_{result['st_level']}"
                now_ts       = time.time()
                last_key     = sent_signals.get(pair)
                last_time    = sent_signals_time.get(pair, 0)
                signal_stale = (now_ts - last_time) > SIGNAL_EXPIRE_SECS
                if last_key == sig_key and not signal_stale:
                    print(f"[{pair}] Sinyal sama & belum expire, skip.")
                    continue
                sent_signals[pair]      = sig_key
                sent_signals_time[pair] = now_ts
                save_sent_signal_state(pair, sig_key, now_ts)

                emj       = "🟢" if result["action"] == "BUY" else "🔴"
                trend_emj = "📈" if result.get("structure", "UPTREND") == "UPTREND" else "📉"

                if result.get("scalping"):
                    # BTC: berita sudah ada di result, skip get_news & groq makro
                    headlines   = result.get("headlines", [])
                    ai_analysis = ""
                else:
                    headlines   = get_news(pair)
                    ai_analysis = analyze_with_groq(
                        pair, result["structure"],
                        result["action"], headlines, fred_data
                    )

                news_text = "\n".join([f"   • {h[:55]}..." for h in headlines]) if headlines else "   • Tidak tersedia"
                fred_text = format_fred_data(fred_data) if fred_data else "   • Tidak tersedia"

                if result.get("scalping"):
                    sent_emj  = "🟢 BULLISH" if result["sentiment"] == "BULLISH" else "🔴 BEARISH"
                    news_list = "\n".join([f"   • {h[:60]}..." for h in result.get("headlines", [])]) or "   • Tidak tersedia"
                    msg = (
                        f"{emj} <b>BTC NEWS SIGNAL {result['action']} — {pair}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⏱️ Waktu        : {now_str}\n"
                        f"💰 Entry        : ${result['entry']:,.2f}\n"
                        f"🛑 Stop Loss    : ${result['sl']:,.2f}\n"
                        f"🎯 Take Profit  : ${result['tp']:,.2f}\n"
                        f"⚖️ R:R Ratio    : 1:{result['rr']}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 Sentimen     : {sent_emj}\n"
                        f"🎯 Confidence   : {result['confidence']}%\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔑 <b>Berita Kunci:</b>\n"
                        f"   {result['key_news'][:80]}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📰 <b>Berita Terkini:</b>\n{news_list}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🧠 <b>Analisis AI:</b>\n{result['reason']}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⚠️ Risiko maks 1-2% per trade!"
                    )
                else:
                    msg = (
                        f"{emj} <b>SINYAL {result['action']} — {pair}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⏱️ Waktu          : {now_str}\n"
                        f"💰 Entry Market   : {result['entry']}\n"
                        f"🎯 Entry Optimal  : {result['optimal_entry']}  ← harga terbaik\n"
                        f"🛑 Stop Loss      : {result['sl']}  (-{result.get('sl_pip', '?')} pip)\n"
                        f"🎯 Take Profit    : {result['tp']}  (+{result.get('tp_pip', '?')} pip)\n"
                        f"⚖️ R:R Ratio      : 1:{result['rr']}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{trend_emj} <b>Trend 1H (EMA50) :</b> {result['structure']}\n"
                        f"📊 <b>Supertrend 15M  :</b> {result['structure_15m']}\n"
                        f"📉 EMA50 1H          : {result['ema50_1h']}\n"
                        f"🔀 ST Level 15M      : {result['st_level']}\n"
                        f"📍 Status Sinyal     : {result['signal_age']}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 <b>Data Ekonomi Makro:</b>\n{fred_text}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📰 <b>Berita Terkini:</b>\n{news_text}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🧠 <b>Analisis AI Makro:</b>\n{ai_analysis}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ <b>SEMUA SYARAT TERPENUHI!</b>\n"
                        f"⚠️ Risiko maks 1-2% per trade!"
                    )
                send_telegram(msg)
                signal_id = save_signal_history(result)
                save_open_trade(signal_id, result, symbol)
                signals_this_cycle += 1

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")

        print(f"[CYCLE DONE] {signals_this_cycle} sinyal dikirim cycle ini")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
