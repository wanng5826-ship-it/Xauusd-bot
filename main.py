"""
=======================================================
  SMC Multi-Pair Bot  —  v6.0
  Pairs    : 18 pair forex + komoditas
  Strategy : Smart Money Concept (SMC)
  AI       : Groq Llama3 (analisis makro ekonomi)
  Data     : yfinance + FRED API + NewsAPI
  Notif    : Telegram
=======================================================
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime

# ─────────────────────────────────────────────────────
#  ⚙️  KONFIGURASI
# ─────────────────────────────────────────────────────
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
NEWS_API_KEY   = os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY   = os.environ.get("FRED_API_KEY", "")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "300"))

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
#  📡  AMBIL DATA CANDLE
# ─────────────────────────────────────────────────────
def get_data(symbol, interval, period):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if df is None or len(df) < 10:
            return None
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"datetime": "time", "date": "time"})
        df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
        df = df[["time", "open", "high", "low", "close", "volume"]]
        return df.reset_index(drop=True)
    except Exception as e:
        print(f"[DATA ERROR] {symbol} {interval}: {e}")
        return None

# ─────────────────────────────────────────────────────
#  📊  SMC ANALYSIS
# ─────────────────────────────────────────────────────
def detect_structure(df):
    if len(df) < 10:
        return "SIDEWAYS"
    highs = df["high"].values
    lows  = df["low"].values
    hh = highs[-1] > highs[-3] and highs[-3] > highs[-5]
    hl = lows[-1]  > lows[-3]  and lows[-3]  > lows[-5]
    ll = lows[-1]  < lows[-3]  and lows[-3]  < lows[-5]
    lh = highs[-1] < highs[-3] and highs[-3] < highs[-5]
    if hh and hl:
        return "UPTREND"
    elif ll and lh:
        return "DOWNTREND"
    return "SIDEWAYS"

def detect_liquidity_sweep(df, structure):
    if len(df) < 5:
        return False, None
    prev_high = df["high"].iloc[-4:-1].max()
    prev_low  = df["low"].iloc[-4:-1].min()
    curr = df.iloc[-1]
    if structure == "DOWNTREND":
        if curr["high"] >= prev_high and curr["close"] < prev_high:
            return True, prev_high
    elif structure == "UPTREND":
        if curr["low"] <= prev_low and curr["close"] > prev_low:
            return True, prev_low
    return False, None

def detect_bos(df, structure):
    if len(df) < 6:
        return False, None
    if structure == "UPTREND":
        prev_high = df["high"].iloc[-5:-2].max()
        if df["high"].iloc[-1] > prev_high:
            return True, prev_high
    elif structure == "DOWNTREND":
        prev_low = df["low"].iloc[-5:-2].min()
        if df["low"].iloc[-1] < prev_low:
            return True, prev_low
    return False, None

def detect_fvg(df, structure):
    if len(df) < 3:
        return False, None, None
    for i in range(len(df)-3, max(len(df)-10, 0), -1):
        c1 = df.iloc[i]
        c3 = df.iloc[i+2]
        if structure == "UPTREND":
            gap_low  = c1["high"]
            gap_high = c3["low"]
            if gap_high > gap_low:
                if df["low"].iloc[i+2:].min() > gap_low:
                    return True, round(gap_low, 4), round(gap_high, 4)
        elif structure == "DOWNTREND":
            gap_high = c1["low"]
            gap_low  = c3["high"]
            if gap_low < gap_high:
                if df["high"].iloc[i+2:].max() < gap_high:
                    return True, round(gap_low, 4), round(gap_high, 4)
    return False, None, None

def confirm_entry(df, structure):
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    pb = abs(prev["close"] - prev["open"])
    cb = abs(curr["close"] - curr["open"])
    if pb < 0.0001:
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
    price = df["close"].iloc[-1]
    atr   = (df["high"] - df["low"]).tail(14).mean()
    if structure == "UPTREND":
        sl = round(sweep_level - atr * 0.5, 4)
        tp = round(price + atr * 3, 4)
        action = "BUY"
    else:
        sl = round(sweep_level + atr * 0.5, 4)
        tp = round(price - atr * 3, 4)
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
        articles = r.json().get("articles", [])
        return [a["title"] for a in articles[:3]]
    except Exception as e:
        print(f"[NEWS ERROR] {e}")
        return []

# ─────────────────────────────────────────────────────
#  📊  DATA EKONOMI FRED
# ─────────────────────────────────────────────────────
def get_fred_data():
    """Ambil data ekonomi makro dari FRED API"""
    indicators = {
        "Fed Rate"   : "FEDFUNDS",
        "CPI US"     : "CPIAUCSL",
        "NFP"        : "PAYEMS",
        "GDP US"     : "GDP",
        "DXY"        : "DTWEXBGS",
        "Unemployment": "UNRATE",
    }
    result = {}
    for name, series_id in indicators.items():
        try:
            r = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id"    : series_id,
                    "api_key"      : FRED_API_KEY,
                    "file_type"    : "json",
                    "sort_order"   : "desc",
                    "limit"        : 2,
                },
                timeout=10
            )
            obs = r.json().get("observations", [])
            if len(obs) >= 2:
                latest = obs[0]["value"]
                prev   = obs[1]["value"]
                result[name] = {
                    "latest": latest,
                    "prev"  : prev,
                    "change": "NAIK" if float(latest) > float(prev) else "TURUN" if float(latest) < float(prev) else "SAMA",
                }
        except Exception as e:
            print(f"[FRED ERROR] {name}: {e}")
    return result

def format_fred_data(fred_data):
    if not fred_data:
        return "Data ekonomi tidak tersedia"
    lines = []
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
- Fed Rate: {fred_data.get('Fed Rate', {}).get('latest', 'N/A')}% ({fred_data.get('Fed Rate', {}).get('change', 'N/A')})
- CPI US: {fred_data.get('CPI US', {}).get('latest', 'N/A')} ({fred_data.get('CPI US', {}).get('change', 'N/A')})
- NFP: {fred_data.get('NFP', {}).get('latest', 'N/A')}K ({fred_data.get('NFP', {}).get('change', 'N/A')})
- GDP US: {fred_data.get('GDP', {}).get('latest', 'N/A')} ({fred_data.get('GDP', {}).get('change', 'N/A')})
- Unemployment: {fred_data.get('Unemployment', {}).get('latest', 'N/A')}% ({fred_data.get('Unemployment', {}).get('change', 'N/A')})
- DXY: {fred_data.get('DXY', {}).get('latest', 'N/A')} ({fred_data.get('DXY', {}).get('change', 'N/A')})"""

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
                "model"     : "llama3-8b-8192",
                "messages"  : [{"role": "user", "content": prompt}],
                "max_tokens": 300,
            },
            timeout=15
        )
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
    df_4h  = get_data(symbol, "1h",  "30d")
    df_1h  = get_data(symbol, "1h",  "7d")
    df_15m = get_data(symbol, "15m", "2d")

    if df_4h is None or df_1h is None or df_15m is None:
        print(f"[{pair}] Data tidak tersedia")
        return None

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

    fvg, fvg_low, fvg_high = detect_fvg(df_1h, structure)
    if not fvg:
        print(f"[{pair}] Tidak ada FVG")
        return None

    confirmed = confirm_entry(df_15m, structure)
    if not confirmed:
        print(f"[{pair}] Entry belum terkonfirmasi")
        return None

    action, entry, sl, tp = calc_sl_tp(df_15m, structure, sweep_level)
    rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0

    print(f"[{pair}] ✅ SINYAL {action} | Entry:{entry} SL:{sl} TP:{tp}")
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
    print("  SMC Multi-Pair Bot  v6.0  [Railway]")
    print(f"  Pairs   : {len(PAIRS)} pairs aktif")
    print(f"  Interval: {CHECK_INTERVAL}s")
    print("=" * 55)

    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN / CHAT_ID belum diset!")
        return

    send_telegram(
        "🤖 <b>SMC Multi-Pair Bot v6 — ONLINE!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Pairs     : {len(PAIRS)} pair aktif\n"
        "📈 Strategy  : Smart Money Concept\n"
        "🧠 AI        : Groq Llama3 (Makro Ekonomi)\n"
        "📰 News      : NewsAPI\n"
        "📊 Ekonomi   : FRED API (Fed Rate, CPI, NFP, GDP)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Bot berjalan 24 jam di Railway!"
    )

    sent_signals = {}
    fred_data    = {}
    fred_timer   = 0

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Scanning {len(PAIRS)} pairs...")

        # Update FRED data setiap 1 jam
        if time.time() - fred_timer > 3600:
            print("[FRED] Update data ekonomi...")
            fred_data  = get_fred_data()
            fred_timer = time.time()
            print(f"[FRED] {len(fred_data)} indikator berhasil diambil")

        for pair, symbol in PAIRS.items():
            try:
                result = analyze_pair(pair, symbol)
                if result is None:
                    continue

                sig_key = f"{pair}_{result['action']}_{result['entry']}"
                if sent_signals.get(pair) == sig_key:
                    print(f"[{pair}] Sinyal sama, skip.")
                    continue
                sent_signals[pair] = sig_key

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

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
