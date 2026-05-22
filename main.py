"""
=======================================================
  SMC Multi-Pair Bot  —  v12.2
  Pairs    : 18 pair forex + komoditas
  Strategy : Smart Money Concept (SMC)
  AI       : Groq Llama3 (analisis makro ekonomi)
  Data     : yfinance + FRED API + NewsAPI
  Notif    : Telegram

  CHANGELOG v12.2:
  [FIX v12-8] analyze_pair: filter posisi harga vs FVG dihapus,
              diganti info status posisi harga terhadap FVG
              agar sinyal tetap sering tapi trader bisa filter manual
=======================================================
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone

BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
NEWS_API_KEY   = os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY   = os.environ.get("FRED_API_KEY", "")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "300"))

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
    swing_highs, swing_lows = find_swing_points(df, lookback=40)
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "SIDEWAYS"
    sh1_price = swing_highs[-2][1]
    sh2_price = swing_highs[-1][1]
    sl1_price = swing_lows[-2][1]
    sl2_price = swing_lows[-1][1]
    hh = sh2_price > sh1_price
    hl = sl2_price > sl1_price
    ll = sl2_price < sl1_price
    lh = sh2_price < sh1_price
    if hh and hl:
        return "UPTREND"
    elif ll and lh:
        return "DOWNTREND"
    return "SIDEWAYS"

def detect_liquidity_sweep(df, structure):
    if len(df) < 6:
        return False, None
    prev_high = df["high"].iloc[-5:-2].max()
    prev_low  = df["low"].iloc[-5:-2].min()
    curr = df.iloc[-2]
    if structure == "DOWNTREND":
        if curr["high"] >= prev_high and curr["close"] < prev_high:
            return True, prev_high
    elif structure == "UPTREND":
        if curr["low"] <= prev_low and curr["close"] > prev_low:
            return True, prev_low
    return False, None

def detect_bos(df, structure):
    if len(df) < 9:
        return False, None
    if structure == "UPTREND":
        prev_high = df["high"].iloc[-8:-3].max()
        if df["high"].iloc[-2] > prev_high:
            return True, round(prev_high, 4)
    elif structure == "DOWNTREND":
        prev_low = df["low"].iloc[-8:-3].min()
        if df["low"].iloc[-2] < prev_low:
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
    pb = abs(prev["close"] - prev["open"])
    cb = abs(curr["close"] - curr["open"])
    atr = (df["high"] - df["low"]).tail(14).mean()
    min_body = atr * 0.1
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

def calc_sl_tp(df, structure, sweep_level):
    price       = df["close"].iloc[-2]
    atr         = (df["high"] - df["low"]).tail(14).mean()
    last_candle = df.iloc[-2]
    buffer      = atr * 0.2
    if structure == "UPTREND":
        sl_candle = last_candle["low"] - buffer
        sl        = round(min(sl_candle, sweep_level - buffer), 4)
        risk      = price - sl
        if risk <= 0:
            return None, round(price, 4), round(price, 4), round(price, 4)
        if risk > atr * 3:
            print(f"[CALC] SL terlalu jauh, skip")
            return None, round(price, 4), round(price, 4), round(price, 4)
        tp     = round(price + risk * 3, 4)
        action = "BUY"
    else:
        sl_candle = last_candle["high"] + buffer
        sl        = round(max(sl_candle, sweep_level + buffer), 4)
        risk      = sl - price
        if risk <= 0:
            return None, round(price, 4), round(price, 4), round(price, 4)
        if risk > atr * 3:
            print(f"[CALC] SL terlalu jauh, skip")
            return None, round(price, 4), round(price, 4), round(price, 4)
        tp     = round(price - risk * 3, 4)
        action = "SELL"
    return action, round(price, 4), sl, tp

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

def analyze_pair(pair, symbol):
    print(f"\n[{pair}] Menganalisis...")

    df_4h  = get_data(symbol, "4h",  "60d")
    df_1h  = get_data(symbol, "1h",  "7d")
    df_15m = get_data(symbol, "15m", "2d")

    if df_4h is None or df_1h is None or df_15m is None:
        print(f"[{pair}] Data tidak tersedia")
        return None

    structure_4h = detect_structure(df_4h)
    structure_1h = detect_structure(df_1h)

    print(f"[{pair}] Struktur 4H: {structure_4h} | 1H: {structure_1h}")

    if structure_4h == "SIDEWAYS":
        print(f"[{pair}] 4H SIDEWAYS → NO TRADE")
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

    fvg, fvg_low, fvg_high = detect_fvg(df_1h, structure, pair=pair)
    if not fvg:
        print(f"[{pair}] Tidak ada FVG valid")
        return None

    # [FIX v12-8] Info posisi harga vs FVG tanpa filter/skip
    current_price = df_15m["close"].iloc[-2]
    if structure == "UPTREND":
        if fvg_low <= current_price <= fvg_high:
            fvg_status = "✅ Harga di dalam FVG — entry sekarang"
        elif current_price < fvg_low:
            jarak = round(fvg_low - current_price, 4)
            fvg_status = f"⏳ Harga {jarak} di bawah FVG — tunggu naik"
        else:
            jarak = round(current_price - fvg_high, 4)
            fvg_status = f"⚠️ Harga {jarak} di atas FVG — sudah lewat"
    else:
        if fvg_low <= current_price <= fvg_high:
            fvg_status = "✅ Harga di dalam FVG — entry sekarang"
        elif current_price > fvg_high:
            jarak = round(current_price - fvg_high, 4)
            fvg_status = f"⏳ Harga {jarak} di atas FVG — tunggu turun"
        else:
            jarak = round(fvg_low - current_price, 4)
            fvg_status = f"⚠️ Harga {jarak} di bawah FVG — sudah lewat"

    confirmed = confirm_entry(df_15m, structure)
    if not confirmed:
        print(f"[{pair}] Entry belum terkonfirmasi di 15M")
        return None

    action, entry, sl, tp = calc_sl_tp(df_15m, structure, sweep_level)
    if action is None:
        print(f"[{pair}] Risk kalkulasi invalid, skip")
        return None

    optimal_entry = fvg_low if structure == "UPTREND" else fvg_high
    rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0

    print(f"[{pair}] ✅ SINYAL {action} | Entry:{entry} Optimal:{optimal_entry} SL:{sl} TP:{tp} RR:1:{rr}")
    return {
        "pair"          : pair,
        "action"        : action,
        "entry"         : entry,
        "optimal_entry" : optimal_entry,
        "sl"            : sl,
        "tp"            : tp,
        "rr"            : rr,
        "structure"     : structure,
        "structure_1h"  : structure_1h,
        "sweep"         : sweep_level,
        "bos"           : bos_level,
        "fvg_low"       : fvg_low,
        "fvg_high"      : fvg_high,
        "fvg_status"    : fvg_status,
    }

def main():
    print("=" * 55)
    print("  SMC Multi-Pair Bot  v12.2  [Railway]")
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

    sent_signals       = {}
    sent_signals_time  = {}
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

        for pair, symbol in PAIRS.items():
            if signals_this_cycle >= MAX_SIGNALS_PER_CYCLE:
                print(f"[LIMIT] Max {MAX_SIGNALS_PER_CYCLE} sinyal tercapai, skip sisa pair")
                break

            try:
                result = analyze_pair(pair, symbol)
                if result is None:
                    continue

                sig_key      = f"{pair}_{result['action']}_{result['fvg_low']}_{result['fvg_high']}"
                now_ts       = time.time()
                last_key     = sent_signals.get(pair)
                last_time    = sent_signals_time.get(pair, 0)
                signal_stale = (now_ts - last_time) > SIGNAL_EXPIRE_SECS
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
                    f"⏱️ Waktu          : {now_str}\n"
                    f"💰 Entry Market   : {result['entry']}\n"
                    f"🎯 Entry Optimal  : {result['optimal_entry']}  ← harga terbaik\n"
                    f"🛑 Stop Loss      : {result['sl']}\n"
                    f"🎯 Take Profit    : {result['tp']}\n"
                    f"⚖️ R:R Ratio      : 1:{result['rr']}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{trend_emj} <b>Struktur 4H :</b> {result['structure']}\n"
                    f"📊 <b>Struktur 1H :</b> {result['structure_1h']}\n"
                    f"💧 Liquidity Sweep : {result['sweep']}\n"
                    f"🔀 BOS Level       : {result['bos']}\n"
                    f"📦 FVG Zone        : {result['fvg_low']} – {result['fvg_high']}\n"
                    f"📍 Posisi Harga    : {result['fvg_status']}\n"
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
                signals_this_cycle += 1

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")

        print(f"[CYCLE DONE] {signals_this_cycle} sinyal dikirim cycle ini")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
