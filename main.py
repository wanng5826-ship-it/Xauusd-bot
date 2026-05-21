"""
=======================================================
  SMC Multi-Pair Bot  —  v11.0
  Pairs    : 18 pair forex + komoditas
  Strategy : Smart Money Concept (SMC)
  AI       : Groq Llama3 (analisis makro ekonomi)
  Data     : yfinance + FRED API + NewsAPI
  Notif    : Telegram

  CHANGELOG v11.0:
  [FIX v11-1] detect_fvg: loop dibalik dari TERBARU ke TERTUA agar
              FVG yang paling fresh/relevan yang dikembalikan (sebelumnya
              loop tertua ke terbaru → return FVG lama yang mungkin sudah
              tidak relevan)
  [FIX v11-2] confirm_entry: min_body dinaikkan dari 0.0001 ke 0.001
              agar filter doji lebih efektif — sebelumnya terlalu kecil
              sehingga hampir semua candle lolos tanpa filter
  [FIX v11-3] detect_fvg DOWNTREND: rename variabel gap_high/gap_low
              menjadi fvg_top/fvg_bot agar tidak membingungkan (nama
              sebelumnya terbalik dari isinya secara intuitif)
  [FIX v10-1] detect_fvg: loop end diperbaiki agar candle terbaru ikut dicek (end=len-2, i_max=len-3)
  [FIX v10-2] calc_sl_tp: return saat risk<=0 tidak return tp=None (mencegah TypeError di analyze_pair)
  [FIX v10-3] get_fred_data: skip nilai "." dari FRED API (mencegah ValueError saat float("."))
  [FIX v10-4] analyze_with_groq: model llama3-8b-8192 diganti ke llama-3.1-8b-instant (deprecated)
  [FIX v10-5] confirm_entry: pakai iloc[-3] & iloc[-2] bukan -2 & -1 (hindari live candle belum closed)
  [FIX v10-6] detect_liquidity_sweep: pakai iloc[-2] & slice [-5:-2] (hindari live candle)
  [FIX v10-7] calc_sl_tp: pakai iloc[-2] konsisten dengan confirm_entry (candle konfirmasi sama)
  [FIX v10-8] detect_fvg: loop range end = len(df)-2 eksklusif, i_max = len(df)-3 (benar & lengkap)
  [FIX v10-9] get_fred_data: cek HTTP status code sebelum parse JSON (hindari KeyError saat API error)
  [FIX v10-10] analyze_with_groq: cek HTTP status Groq sebelum akses choices (hindari KeyError)

  CHANGELOG v7.0:
  [FIX 1] df_4h interval diperbaiki dari "1h" ke "4h"
  [FIX 2] detect_structure pakai swing high/low yang valid
  [FIX 3] detect_fvg dengan logging & batas pencarian jelas
  [FIX 4] Filter trading session (London + NY only)
  [FIX 5] SL/TP berbasis struktur candle + sweep level
  [FIX 6] Max concurrent signals = 3 per scan cycle

  CHANGELOG v8.0:
  [FIX 7] get_data: tz_localize crash, pakai tz_convert dulu
  [FIX 8] calc_sl_tp: sweep_level jadi safety net SL
  [FIX 9] detect_fvg: candles_after off-by-one (i+2 ke i+3)
  [FIX A] get_fred_data: key GDP sekarang konsisten GDP US
  [FIX B] confirm_entry: threshold doji relative bukan hardcode

  CHANGELOG v9.0:
  [FIX C] calc_sl_tp: guard risk <= 0, tidak kirim sinyal invalid
  [FIX D] detect_fvg: loop end diperbaiki ke len(df)-3
  [FIX E] detect_bos: pakai candle ke-2 dari terakhir (skip live candle)
  [FIX F] sent_signals: expire otomatis setelah 4 jam
  [FIX G] get_news: cek HTTP status code, handle 429 rate limit
  [FIX H] get_data: fallback interval 1h jika 4h tidak tersedia
=======================================================
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
#  ⚙️  KONFIGURASI
# ─────────────────────────────────────────────────────
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
NEWS_API_KEY   = os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY   = os.environ.get("FRED_API_KEY", "")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "300"))

# [FIX 6] Maksimum sinyal yang dikirim per satu siklus scan
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
}

# ─────────────────────────────────────────────────────
#  [FIX 4] FILTER TRADING SESSION (UTC)
#  London : 07:00 – 16:00 UTC
#  New York: 12:00 – 21:00 UTC
#  Overlap : 12:00 – 16:00 UTC (paling liquid)
# ─────────────────────────────────────────────────────
def is_valid_session():
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    in_london  = 7 <= hour < 16
    in_newyork = 12 <= hour < 21
    in_asia    = 0 <= hour < 7
    return in_london or in_newyork or in_asia

# ─────────────────────────────────────────────────────
#  📡  AMBIL DATA CANDLE
# ─────────────────────────────────────────────────────
def get_data(symbol, interval, period):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        # [FIX H] Beberapa pair tidak support interval 4h di yfinance
        # Fallback ke 1h dengan period lebih panjang
        if (df is None or len(df) < 10) and interval == "4h":
            print(f"[DATA] {symbol} tidak support 4h, fallback ke 1h")
            df = ticker.history(period="30d", interval="1h")
        if df is None or len(df) < 10:
            return None
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"datetime": "time", "date": "time"})
        # [FIX 7] tz_convert dulu sebelum tz_localize biar tidak crash
        # kalau yfinance return data yang sudah timezone-aware
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

# ─────────────────────────────────────────────────────
#  [FIX 2] DETECT STRUCTURE — SWING HIGH/LOW VALID
#  Swing High: candle[i].high > candle[i-1].high AND
#              candle[i].high > candle[i+1].high
#  Swing Low : candle[i].low  < candle[i-1].low  AND
#              candle[i].low  < candle[i+1].low
# ─────────────────────────────────────────────────────
def find_swing_points(df, lookback=30):
    """
    Cari swing high dan swing low yang valid
    dalam N candle terakhir.
    Return: (list of swing_highs, list of swing_lows)
    Setiap item = (index, price)
    """
    highs = []
    lows  = []
    data  = df.tail(lookback).reset_index(drop=True)

    for i in range(1, len(data) - 1):
        # Swing High
        if data["high"].iloc[i] > data["high"].iloc[i-1] and \
           data["high"].iloc[i] > data["high"].iloc[i+1]:
            highs.append((i, data["high"].iloc[i]))
        # Swing Low
        if data["low"].iloc[i] < data["low"].iloc[i-1] and \
           data["low"].iloc[i] < data["low"].iloc[i+1]:
            lows.append((i, data["low"].iloc[i]))

    return highs, lows

def detect_structure(df):
    """
    Tentukan struktur market berdasarkan urutan
    swing high/low yang valid (HH/HL atau LL/LH).
    Butuh minimal 2 swing high & 2 swing low.
    """
    if len(df) < 15:
        return "SIDEWAYS"

    swing_highs, swing_lows = find_swing_points(df, lookback=40)

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "SIDEWAYS"

    # Ambil 2 swing terakhir
    sh1_price = swing_highs[-2][1]
    sh2_price = swing_highs[-1][1]
    sl1_price = swing_lows[-2][1]
    sl2_price = swing_lows[-1][1]

    hh = sh2_price > sh1_price  # Higher High
    hl = sl2_price > sl1_price  # Higher Low
    ll = sl2_price < sl1_price  # Lower Low
    lh = sh2_price < sh1_price  # Lower High

    if hh and hl:
        return "UPTREND"
    elif ll and lh:
        return "DOWNTREND"
    return "SIDEWAYS"

# ─────────────────────────────────────────────────────
#  📊  SMC — LIQUIDITY SWEEP
# ─────────────────────────────────────────────────────
def detect_liquidity_sweep(df, structure):
    if len(df) < 6:  # [FIX v10-6] butuh 6 karena pakai iloc[-2] dan slice [-5:-2]
        return False, None
    prev_high = df["high"].iloc[-5:-2].max()  # [FIX v10-6] hindari live candle
    prev_low  = df["low"].iloc[-5:-2].min()   # [FIX v10-6] hindari live candle
    curr = df.iloc[-2]                         # [FIX v10-6] pakai candle closed, bukan live
    if structure == "DOWNTREND":
        # Harga naik dulu ambil likuiditas di atas, lalu tutup di bawah
        if curr["high"] >= prev_high and curr["close"] < prev_high:
            return True, prev_high
    elif structure == "UPTREND":
        # Harga turun dulu ambil likuiditas di bawah, lalu tutup di atas
        if curr["low"] <= prev_low and curr["close"] > prev_low:
            return True, prev_low
    return False, None

# ─────────────────────────────────────────────────────
#  📊  SMC — BREAK OF STRUCTURE (BOS)
# ─────────────────────────────────────────────────────
def detect_bos(df, structure):
    if len(df) < 6:
        return False, None
    # [FIX E] Gunakan iloc[-2] bukan iloc[-1] untuk skip live candle
    # Candle terakhir (iloc[-1]) belum closed di real-time, bisa false signal
    if structure == "UPTREND":
        prev_high = df["high"].iloc[-5:-2].max()
        if df["high"].iloc[-2] > prev_high:   # ← pakai -2
            return True, round(prev_high, 4)
    elif structure == "DOWNTREND":
        prev_low = df["low"].iloc[-5:-2].min()
        if df["low"].iloc[-2] < prev_low:     # ← pakai -2
            return True, round(prev_low, 4)
    return False, None

# ─────────────────────────────────────────────────────
#  [FIX 3] DETECT FVG — DENGAN LOGGING & BATAS JELAS
#  Fair Value Gap:
#  UPTREND   → candle[i].high < candle[i+2].low  (gap bullish)
#  DOWNTREND → candle[i].low  > candle[i+2].high (gap bearish)
#  Syarat tambahan: gap belum terisi (unfilled)
# ─────────────────────────────────────────────────────
def detect_fvg(df, structure, pair=""):
    """
    Cari FVG yang valid dalam 15 candle terakhir.
    Lebih deskriptif: log berapa FVG ditemukan vs valid.
    """
    if len(df) < 3:
        print(f"[{pair}] FVG: data terlalu sedikit")
        return False, None, None

    # [FIX D] Loop sampai len(df)-3, bukan len(df)-2
    # karena triplet butuh i, i+1, i+2 → i max = len(df)-3
    search_window = min(15, len(df) - 3)
    if search_window < 1:
        return False, None, None
    fvg_found     = 0
    fvg_valid     = 0

    # [FIX v10-8] end = len(df)-2 (eksklusif), jadi i_max = len(df)-3
    # Ini benar karena akses i+2 = (len(df)-3)+2 = len(df)-1 → valid
    # [FIX v11-1] Loop dari TERBARU ke TERTUA agar dapat FVG paling relevan
    for i in range(len(df) - 3, len(df) - search_window - 1, -1):
        c1 = df.iloc[i]
        c3 = df.iloc[i + 2]

        if structure == "UPTREND":
            # Bullish FVG: gap antara high[i] dan low[i+2]
            gap_low  = c1["high"]
            gap_high = c3["low"]
            if gap_high > gap_low:
                fvg_found += 1
                # Cek apakah gap belum terisi (candle setelahnya tidak masuk gap)
                # [FIX 9] Mulai dari i+3, bukan i+2, karena c3=iloc[i+2]
                # adalah candle ke-3 dari triplet, bukan candle setelahnya
                candles_after = df.iloc[i + 3:]
                if len(candles_after) == 0 or candles_after["low"].min() > gap_low:
                    fvg_valid += 1
                    print(f"[{pair}] FVG Bullish ditemukan: {round(gap_low,4)}–{round(gap_high,4)}")
                    return True, round(gap_low, 4), round(gap_high, 4)

        elif structure == "DOWNTREND":
            # Bearish FVG: gap antara low[i] dan high[i+2]
            # [FIX v11-3] Rename variable agar tidak membingungkan
            # fvg_top = c1["low"] = batas ATAS FVG bearish (harga lebih tinggi)
            # fvg_bot = c3["high"] = batas BAWAH FVG bearish (harga lebih rendah)
            fvg_top = c1["low"]
            fvg_bot = c3["high"]
            if fvg_bot < fvg_top:
                fvg_found += 1
                # Cek apakah gap belum terisi
                # [FIX 9] Mulai dari i+3, bukan i+2
                candles_after = df.iloc[i + 3:]
                if len(candles_after) == 0 or candles_after["high"].max() < fvg_top:
                    fvg_valid += 1
                    print(f"[{pair}] FVG Bearish ditemukan: {round(fvg_bot,4)}–{round(fvg_top,4)}")
                    return True, round(fvg_bot, 4), round(fvg_top, 4)

    print(f"[{pair}] FVG: ditemukan {fvg_found} gap, valid & unfilled: {fvg_valid} → tidak ada")
    return False, None, None

# ─────────────────────────────────────────────────────
#  📊  KONFIRMASI ENTRY (15M)
# ─────────────────────────────────────────────────────
def confirm_entry(df, structure):
    if len(df) < 3:  # [FIX v10-5] butuh minimal 3 candle karena pakai -2 dan -3
        return False
    prev = df.iloc[-3]   # [FIX v10-5] hindari live candle, pakai candle yang sudah closed
    curr = df.iloc[-2]   # [FIX v10-5] sama seperti FIX E di BOS
    pb = abs(prev["close"] - prev["open"])
    cb = abs(curr["close"] - curr["open"])
    # [FIX B] Threshold relative terhadap harga, bukan hardcode 0.0001
    # [FIX v11-2] Dinaikkan ke 0.001 agar filter doji lebih ketat (sebelumnya 0.0001 terlalu kecil)
    # Cocok untuk semua pair termasuk XAUUSD dan GBPJPY
    min_body = prev["close"] * 0.001
    if pb < min_body:
        return False
    if structure == "UPTREND":
        return (curr["close"] > curr["open"] and
                cb >= pb * 0.7 and
                curr["close"] > prev["high"])
    elif structure == "DOWNTREND":
        return (curr["close"] < curr["open"] and
                cb >= pb * 0.7 and
                curr["close"] < prev["low"])
    return False

# ─────────────────────────────────────────────────────
#  [FIX 5] SL/TP BERBASIS STRUKTUR CANDLE + SWEEP
#  SL: di luar candle konfirmasi + buffer ATR kecil
#      (lebih ketat dari sebelumnya yang cuma ATR*0.5
#       dari sweep level tanpa melihat candle terakhir)
#  TP: RR 1:3 dari entry ke SL
# ─────────────────────────────────────────────────────
def calc_sl_tp(df, structure, sweep_level):
    """
    SL ditempatkan:
    - UPTREND  : di bawah low candle konfirmasi (15M) - buffer kecil
    - DOWNTREND: di atas high candle konfirmasi (15M) + buffer kecil
    Buffer = ATR(14) * 0.2

    [FIX 8] sweep_level dipakai sebagai safety net:
    - UPTREND  : SL tidak boleh lebih tinggi dari sweep_level
    - DOWNTREND: SL tidak boleh lebih rendah dari sweep_level
    Ini mencegah SL yang terlalu sempit dan mudah kena spike.

    TP = entry + risk * 3 (RR 1:3)
    """
    price       = df["close"].iloc[-2]   # [FIX v10-7] konsisten dengan confirm_entry, pakai candle closed
    atr         = (df["high"] - df["low"]).tail(14).mean()
    last_candle = df.iloc[-2]             # [FIX v10-7] SL berbasis candle konfirmasi yang sama
    buffer      = atr * 0.2

    if structure == "UPTREND":
        sl_candle = last_candle["low"] - buffer
        # [FIX 8] Pakai yang lebih rendah antara candle SL vs sweep level
        sl    = round(min(sl_candle, sweep_level - buffer), 4)
        risk  = price - sl
        # [FIX C] Guard: risk harus positif, kalau tidak sinyal ini invalid
        if risk <= 0:
            return None, round(price, 4), round(price, 4), round(price, 4)  # [FIX v10-2] tp tidak boleh None, isi dummy agar tidak TypeError
        tp    = round(price + risk * 3, 4)
        action = "BUY"
    else:
        sl_candle = last_candle["high"] + buffer
        # [FIX 8] Pakai yang lebih tinggi antara candle SL vs sweep level
        sl    = round(max(sl_candle, sweep_level + buffer), 4)
        risk  = sl - price
        # [FIX C] Guard: risk harus positif
        if risk <= 0:
            return None, round(price, 4), round(price, 4), round(price, 4)  # [FIX v10-2] sama
        tp    = round(price - risk * 3, 4)
        action = "SELL"

    return action, round(price, 4), sl, tp

# ─────────────────────────────────────────────────────
#  📰  BERITA
# ─────────────────────────────────────────────────────
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
        # [FIX G] Cek status code, jangan diam-diam gagal
        if r.status_code == 429:
            print(f"[NEWS] Rate limit kena (429), skip berita untuk {pair}")
            return []
        if r.status_code != 200:
            print(f"[NEWS] HTTP {r.status_code} untuk {pair}")
            return []
        articles = r.json().get("articles", [])
        return [a["title"] for a in articles[:3]]
    except Exception as e:
        print(f"[NEWS ERROR] {e}")
        return []

# ─────────────────────────────────────────────────────
#  📊  DATA EKONOMI FRED
# ─────────────────────────────────────────────────────
def get_fred_data():
    indicators = {
        "Fed Rate"    : "FEDFUNDS",
        "CPI US"      : "CPIAUCSL",
        "NFP"         : "PAYEMS",
        "GDP US"      : "GDP",       # [FIX A] key sekarang "GDP US" agar konsisten dengan analyze_with_groq
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
            # [FIX v10-9] Cek status code sebelum parse JSON
            if r.status_code != 200:
                print(f"[FRED] HTTP {r.status_code} untuk {name}, skip")
                continue
            obs = r.json().get("observations", [])
            if len(obs) >= 2:
                latest = obs[0]["value"]
                prev   = obs[1]["value"]
                # [FIX v10-3] FRED kadang return "." untuk data kosong → skip
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

# ─────────────────────────────────────────────────────
#  🧠  GROQ AI ANALISIS MAKRO
# ─────────────────────────────────────────────────────
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
                "model"     : "llama-3.1-8b-instant",  # [FIX v10-4] llama3-8b-8192 deprecated
                "messages"  : [{"role": "user", "content": prompt}],
                "max_tokens": 300,
            },
            timeout=15
        )
        # [FIX v10-10] Cek status code Groq sebelum akses choices
        if r.status_code != 200:
            print(f"[GROQ] HTTP {r.status_code}: {r.text[:100]}")
            return "Analisis AI tidak tersedia saat ini."
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[GROQ ERROR] {e}")
        return "Analisis AI tidak tersedia saat ini."

# ─────────────────────────────────────────────────────
#  📬  TELEGRAM
# ─────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────
#  🔍  ANALISIS PER PAIR
# ─────────────────────────────────────────────────────
def analyze_pair(pair, symbol):
    print(f"\n[{pair}] Menganalisis...")

    # [FIX 1] df_4h sekarang pakai interval "4h" yang benar
    df_4h  = get_data(symbol, "4h",  "60d")   # ← FIX: dulu "1h"
    df_1h  = get_data(symbol, "1h",  "7d")
    df_15m = get_data(symbol, "15m", "2d")

    if df_4h is None or df_1h is None or df_15m is None:
        print(f"[{pair}] Data tidak tersedia")
        return None

    # [FIX 2] Gunakan detect_structure versi swing point
    structure_4h = detect_structure(df_4h)
    structure_1h = detect_structure(df_1h)

    print(f"[{pair}] Struktur 4H: {structure_4h} | 1H: {structure_1h}")

    if structure_4h == "SIDEWAYS" or structure_1h == "SIDEWAYS":
        print(f"[{pair}] SIDEWAYS → NO TRADE")
        return None

    if structure_4h != structure_1h:
        print(f"[{pair}] Konflik struktur → NO TRADE")
        return None

    structure = structure_4h

    swept, sweep_level = detect_liquidity_sweep(df_1h, structure)
    if not swept:
        print(f"[{pair}] Tidak ada liquidity sweep")
        return None

    bos, bos_level = detect_bos(df_1h, structure)
    if not bos:
        print(f"[{pair}] Tidak ada BOS")
        return None

    # [FIX 3] Sekarang detect_fvg punya logging yang jelas
    fvg, fvg_low, fvg_high = detect_fvg(df_1h, structure, pair=pair)
    if not fvg:
        print(f"[{pair}] Tidak ada FVG valid")
        return None

    confirmed = confirm_entry(df_15m, structure)
    if not confirmed:
        print(f"[{pair}] Entry belum terkonfirmasi di 15M")
        return None

    # [FIX 5] SL/TP berbasis candle + sweep
    action, entry, sl, tp = calc_sl_tp(df_15m, structure, sweep_level)
    # [FIX C] Skip jika risk invalid (action=None)
    if action is None:
        print(f"[{pair}] Risk kalkulasi invalid (SL di atas/bawah entry), skip")
        return None
    rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0

    print(f"[{pair}] ✅ SINYAL {action} | Entry:{entry} SL:{sl} TP:{tp} RR:1:{rr}")
    return {
        "pair"     : pair,
        "action"   : action,
        "entry"    : entry,
        "sl"       : sl,
        "tp"       : tp,
        "rr"       : rr,
        "structure": structure,
        "sweep"    : sweep_level,
        "bos"      : bos_level,
        "fvg_low"  : fvg_low,
        "fvg_high" : fvg_high,
    }

# ─────────────────────────────────────────────────────
#  🚀  MAIN
# ─────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  SMC Multi-Pair Bot  v11.0  [Railway]")
    print(f"  Pairs   : {len(PAIRS)} pairs aktif")
    print(f"  Interval: {CHECK_INTERVAL}s")
    print(f"  Max sinyal/cycle: {MAX_SIGNALS_PER_CYCLE}")
    print("=" * 55)

    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN / CHAT_ID belum diset!")
        return

    send_telegram(
        "🤖 <b>SMC Multi-Pair Bot v11 — ONLINE!</b>\n"
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

    sent_signals       = {}   # pair → sig_key
    sent_signals_time  = {}   # pair → timestamp kapan sinyal dikirim
    SIGNAL_EXPIRE_SECS = 4 * 3600  # [FIX F] expire setelah 4 jam
    fred_data          = {}
    fred_timer         = 0

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Scanning {len(PAIRS)} pairs...")

        # [FIX 4] Cek sesi trading sebelum scan
        if not is_valid_session():
            now_utc = datetime.now(timezone.utc)
            print(f"[SESSION] Di luar sesi London/NY ({now_utc.strftime('%H:%M')} UTC) → skip scan")
            time.sleep(CHECK_INTERVAL)
            continue

        # Update FRED data setiap 1 jam
        if time.time() - fred_timer > 3600:
            print("[FRED] Update data ekonomi...")
            fred_data  = get_fred_data()
            fred_timer = time.time()
            print(f"[FRED] {len(fred_data)} indikator berhasil diambil")

        # [FIX 6] Counter sinyal per siklus
        signals_this_cycle = 0

        for pair, symbol in PAIRS.items():
            # [FIX 6] Stop kalau sudah capai batas
            if signals_this_cycle >= MAX_SIGNALS_PER_CYCLE:
                print(f"[LIMIT] Max {MAX_SIGNALS_PER_CYCLE} sinyal tercapai, sisa pair di-skip cycle ini")
                break

            try:
                result = analyze_pair(pair, symbol)
                if result is None:
                    continue

                sig_key      = f"{pair}_{result['action']}_{result['entry']}"
                now_ts       = time.time()
                last_key     = sent_signals.get(pair)
                last_time    = sent_signals_time.get(pair, 0)
                signal_stale = (now_ts - last_time) > SIGNAL_EXPIRE_SECS
                # [FIX F] Skip hanya kalau sinyal SAMA dan belum expired
                if last_key == sig_key and not signal_stale:
                    print(f"[{pair}] Sinyal sama & belum expire, skip.")
                    continue
                sent_signals[pair]      = sig_key
                sent_signals_time[pair] = now_ts

                headlines   = get_news(pair)
                ai_analysis = analyze_with_groq(
                    pair, result["structure"],
                    result["action"], headlines, fred_data
                )

                emj       = "🟢" if result["action"] == "BUY" else "🔴"
                trend_emj = "📈" if result["structure"] == "UPTREND" else "📉"
                news_text = "\n".join([f"   • {h[:55]}..." for h in headlines]) if headlines else "   • Tidak tersedia"
                fred_text = format_fred_data(fred_data) if fred_data else "   • Tidak tersedia"

                msg = (
                    f"{emj} <b>SINYAL {result['action']} — {pair}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ Waktu      : {now_str}\n"
                    f"💰 Entry      : {result['entry']}\n"
                    f"🛑 Stop Loss  : {result['sl']}\n"
                    f"🎯 Take Profit: {result['tp']}\n"
                    f"⚖️ R:R Ratio  : 1:{result['rr']}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{trend_emj} <b>Struktur:</b> {result['structure']}\n"
                    f"💧 Liquidity Sweep : {result['sweep']}\n"
                    f"🔀 BOS Level       : {result['bos']}\n"
                    f"📦 FVG Zone        : {result['fvg_low']} – {result['fvg_high']}\n"
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
                signals_this_cycle += 1  # [FIX 6] Tambah counter

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")

        print(f"[CYCLE DONE] {signals_this_cycle} sinyal dikirim cycle ini")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
