"""
=======================================================
  Momentum Candle Bot  —  H4
  Pairs    : 18 pair forex + komoditas + BTCUSD
  Strategi : Deteksi momentum candle di H4
             Body > 70% panjang candle
             Body > 2x rata-rata 10 candle sebelumnya
             Searah trend H4
  Notif    : Telegram
  Data     : yfinance (delay ~15-30 menit)
=======================================================
"""

import os
import time
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone

BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "900"))  # 15 menit default

# ── Threshold momentum candle ──────────────────────────
BODY_RATIO_MIN    = 0.65   # body minimal 65% dari total panjang candle
BODY_MULT_MIN     = 1.8    # body minimal 1.8x rata-rata 10 candle sebelumnya
CANDLE_AVG_LOOKBACK = 10   # rata-rata dari 10 candle sebelumnya
COOLDOWN_SECS     = 4 * 3600  # cooldown 4 jam per pair

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

# ── Telegram ──────────────────────────────────────────
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM] Token/Chat ID belum diset")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code == 200:
            print("[TELEGRAM] ✅ Terkirim")
        else:
            print(f"[TELEGRAM] ❌ {r.text[:100]}")
    except Exception as e:
        print(f"[TELEGRAM] ❌ {e}")

# ── Ambil data H4 ─────────────────────────────────────
def get_data_4h(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="30d", interval="4h")
        if df is None or len(df) < 20:
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
        print(f"[DATA ERROR] {symbol}: {e}")
        return None

# ── Deteksi trend H4 sederhana ────────────────────────
def detect_trend(df):
    if len(df) < 20:
        return "SIDEWAYS"
    close = df["close"]
    ma20  = close.tail(20).mean()
    ma10  = close.tail(10).mean()
    last  = close.iloc[-2]
    if last > ma20 and ma10 > ma20:
        return "UPTREND"
    elif last < ma20 and ma10 < ma20:
        return "DOWNTREND"
    return "SIDEWAYS"

# ── Deteksi momentum candle ───────────────────────────
def detect_momentum_candle(df):
    """
    Cek candle terbaru (index -2, candle yang sudah closed).
    Syarat:
    1. Body >= BODY_RATIO_MIN dari total panjang candle
    2. Body >= BODY_MULT_MIN x rata-rata body 10 candle sebelumnya
    3. Arah candle searah trend
    """
    if len(df) < CANDLE_AVG_LOOKBACK + 3:
        return False, None, {}

    candle = df.iloc[-2]  # candle closed terbaru
    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    total_range = h - l
    body        = abs(c - o)

    if total_range == 0:
        return False, None, {}

    # Syarat 1: body ratio
    body_ratio = body / total_range
    if body_ratio < BODY_RATIO_MIN:
        return False, None, {"body_ratio": round(body_ratio, 2)}

    # Syarat 2: body vs rata-rata
    prev_candles = df.iloc[-CANDLE_AVG_LOOKBACK-2:-2]
    avg_body = abs(prev_candles["close"] - prev_candles["open"]).mean()
    if avg_body == 0:
        return False, None, {}
    body_mult = body / avg_body
    if body_mult < BODY_MULT_MIN:
        return False, None, {
            "body_ratio": round(body_ratio, 2),
            "body_mult" : round(body_mult, 2)
        }

    # Tentukan arah candle
    direction = "BULLISH" if c > o else "BEARISH"

    info = {
        "direction" : direction,
        "open"      : round(o, 5),
        "high"      : round(h, 5),
        "low"       : round(l, 5),
        "close"     : round(c, 5),
        "body_ratio": round(body_ratio * 100, 1),
        "body_mult" : round(body_mult, 1),
        "candle_time": str(df.iloc[-2]["time"]),
    }
    return True, direction, info

# ── Hitung SL & TP ────────────────────────────────────
def calc_sl_tp(df, direction):
    candle = df.iloc[-2]
    price  = candle["close"]
    atr    = (df["high"] - df["low"]).tail(14).mean()

    if direction == "BULLISH":
        sl = round(candle["low"] - atr * 0.5, 5)
        tp = round(price + (price - sl) * 2, 5)
        action = "BUY"
    else:
        sl = round(candle["high"] + atr * 0.5, 5)
        tp = round(price - (sl - price) * 2, 5)
        action = "SELL"

    risk = abs(price - sl)
    rr   = round(abs(tp - price) / risk, 1) if risk > 0 else 0
    return action, round(price, 5), sl, tp, rr

# ── Format pesan alert ────────────────────────────────
def format_alert(pair, action, entry, sl, tp, rr, trend, info):
    now_str = datetime.now().strftime("%H:%M:%S")
    emj     = "🟢" if action == "BUY" else "🔴"
    t_emj   = "📈" if trend == "UPTREND" else "📉" if trend == "DOWNTREND" else "➡️"
    c_emj   = "🕯️🟢" if info["direction"] == "BULLISH" else "🕯️🔴"

    msg = (
        f"{emj} <b>MOMENTUM CANDLE H4 — {pair}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ Waktu          : {now_str}\n"
        f"💰 Entry          : {entry}\n"
        f"🛑 Stop Loss      : {sl}\n"
        f"🎯 Take Profit    : {tp}\n"
        f"⚖️ R:R Ratio      : 1:{rr}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{t_emj} Trend H4        : {trend}\n"
        f"{c_emj} Candle          : {info['direction']}\n"
        f"📊 Body Ratio     : {info['body_ratio']}% dari candle\n"
        f"📏 Body vs Avg    : {info['body_mult']}x rata-rata\n"
        f"🕐 Waktu Candle   : {info['candle_time']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>SL</b> di bawah/atas candle + 0.5 ATR\n"
        f"📌 <b>TP</b> = 2x risk (RR 1:2)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Bukan jaminan profit!\n"
        f"💡 Selalu gunakan risk management!"
    )
    return msg

# ── Cek sesi trading aktif ────────────────────────────
def is_valid_session():
    hour = datetime.now(timezone.utc).hour
    return (7 <= hour < 16) or (12 <= hour < 21) or (0 <= hour < 7)

# ── Analisis satu pair ────────────────────────────────
def analyze_pair(pair, symbol):
    print(f"[{pair}] Menganalisis momentum candle H4...")
    df = get_data_4h(symbol)
    if df is None:
        print(f"[{pair}] Data tidak tersedia, skip")
        return None

    trend = detect_trend(df)
    print(f"[{pair}] Trend: {trend}")

    if trend == "SIDEWAYS":
        print(f"[{pair}] Sideways → skip")
        return None

    found, direction, info = detect_momentum_candle(df)
    if not found:
        print(f"[{pair}] Tidak ada momentum candle valid")
        return None

    # Cek arah candle searah trend
    if trend == "UPTREND" and direction != "BULLISH":
        print(f"[{pair}] Candle BEARISH tapi trend UPTREND → skip")
        return None
    if trend == "DOWNTREND" and direction != "BEARISH":
        print(f"[{pair}] Candle BULLISH tapi trend DOWNTREND → skip")
        return None

    action, entry, sl, tp, rr = calc_sl_tp(df, direction)
    print(f"[{pair}] ✅ SINYAL {action} | Entry:{entry} SL:{sl} TP:{tp} RR:1:{rr}")
    print(f"[{pair}] Body:{info['body_ratio']}% | {info['body_mult']}x avg")

    return {
        "pair"  : pair,
        "action": action,
        "entry" : entry,
        "sl"    : sl,
        "tp"    : tp,
        "rr"    : rr,
        "trend" : trend,
        "info"  : info,
    }

# ── Main loop ─────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Momentum Candle Bot  H4")
    print(f"  Pairs   : {len(PAIRS)} pair aktif")
    print(f"  Interval: {CHECK_INTERVAL}s ({CHECK_INTERVAL//60} menit)")
    print(f"  Body Min: {BODY_RATIO_MIN*100}% | {BODY_MULT_MIN}x avg")
    print("=" * 55)

    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN / CHAT_ID belum diset!")
        return

    send_telegram(
        "🕯️ <b>Momentum Candle Bot H4 — ONLINE!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Pairs     : {len(PAIRS)} pair aktif\n"
        "📈 Strategi  : Momentum Candle H4\n"
        "🔍 Syarat    :\n"
        f"   • Body ≥ {int(BODY_RATIO_MIN*100)}% dari panjang candle\n"
        f"   • Body ≥ {BODY_MULT_MIN}x rata-rata 10 candle\n"
        "   • Searah trend H4\n"
        f"⏱️ Interval  : setiap {CHECK_INTERVAL//60} menit\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Bot berjalan di Railway!"
    )

    sent_cache = {}

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Scanning {len(PAIRS)} pairs...")

        if not is_valid_session():
            now_utc = datetime.now(timezone.utc)
            print(f"[SESSION] Di luar sesi ({now_utc.strftime('%H:%M')} UTC) → skip")
            time.sleep(CHECK_INTERVAL)
            continue

        signals_sent = 0

        for pair, symbol in PAIRS.items():
            try:
                result = analyze_pair(pair, symbol)
                if result is None:
                    continue

                # Cooldown per pair
                last_sent = sent_cache.get(pair, 0)
                if time.time() - last_sent < COOLDOWN_SECS:
                    print(f"[{pair}] Cooldown aktif, skip")
                    continue

                msg = format_alert(
                    result["pair"], result["action"],
                    result["entry"], result["sl"], result["tp"],
                    result["rr"], result["trend"], result["info"]
                )
                send_telegram(msg)
                sent_cache[pair] = time.time()
                signals_sent += 1
                time.sleep(1)

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")
                continue

        print(f"[CYCLE DONE] {signals_sent} sinyal dikirim dari {len(PAIRS)} pair")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
