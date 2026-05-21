
=======================================================
  XAUUSD Counter-Trend Engulfing Bot  —  v4.0
  Deploy   : Railway (via GitHub)
  Data     : Alpha Vantage (live XAUUSD 1 menit)
  Timeframe: 1 Menit
  Logika   : Engulfing BERLAWANAN trend + S/R 200 candle
  Notif    : Telegram Bot
=======================================================
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────────────
#  ⚙️  KONFIGURASI
# ─────────────────────────────────────────────────────

BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
AV_API_KEY     = os.environ.get("AV_API_KEY", "IMZQ2A4YPAD5VSTN")

N_CANDLES      = int(os.environ.get("N_CANDLES", "200"))
EMA_FAST       = int(os.environ.get("EMA_FAST", "9"))
EMA_SLOW       = int(os.environ.get("EMA_SLOW", "21"))
SR_STRENGTH    = int(os.environ.get("SR_STRENGTH", "2"))
SR_ZONE        = float(os.environ.get("SR_ZONE", "0.80"))
SR_NEAR_ZONE   = float(os.environ.get("SR_NEAR_ZONE", "2.50"))
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "60"))

# ─────────────────────────────────────────────────────
#  📡  AMBIL DATA DARI ALPHA VANTAGE
# ─────────────────────────────────────────────────────

def get_candles():
    """
    Ambil data XAUUSD 1 menit dari Alpha Vantage.
    Simbol: XAU/USD (spot gold)
    """
    print("[DATA] Mengambil data dari Alpha Vantage...")
    url = "https://www.alphavantage.co/query"
    params = {
        "function"    : "FX_INTRADAY",
        "from_symbol" : "XAU",
        "to_symbol"   : "USD",
        "interval"    : "1min",
        "outputsize"  : "full",
        "apikey"      : AV_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        # Cek error
        if "Error Message" in data:
            print(f"[AV ERROR] {data['Error Message']}")
            return None

        if "Note" in data:
            print("[AV] Rate limit tercapai, tunggu 1 menit...")
            time.sleep(60)
            return None

        key = "Time Series FX (1min)"
        if key not in data:
            print(f"[AV ERROR] Key tidak ditemukan: {list(data.keys())}")
            return None

        ts = data[key]
        rows = []
        for dt_str, val in ts.items():
            rows.append({
                "time"  : pd.to_datetime(dt_str),
                "open"  : float(val["1. open"]),
                "high"  : float(val["2. high"]),
                "low"   : float(val["3. low"]),
                "close" : float(val["4. close"]),
                "volume": 0,
            })

        df = pd.DataFrame(rows)
        df = df.sort_values("time").reset_index(drop=True)
        df = df.tail(N_CANDLES).reset_index(drop=True)

        print(f"[DATA] {len(df)} candle dari Alpha Vantage (XAUUSD 1 menit)")
        return df

    except Exception as e:
        print(f"[AV ERROR] {e}")
        return None

# ─────────────────────────────────────────────────────
#  📊  EMA
# ─────────────────────────────────────────────────────

def hitung_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# ─────────────────────────────────────────────────────
#  📈  TREND
# ─────────────────────────────────────────────────────

def get_trend(df):
    df = df.copy()
    df["ef"] = hitung_ema(df["close"], EMA_FAST)
    df["es"] = hitung_ema(df["close"], EMA_SLOW)
    ef = round(df["ef"].iloc[-1], 2)
    es = round(df["es"].iloc[-1], 2)

    if ef > es:
        return {"trend": "UPTREND",   "dir": "UP",   "emoji": "📈", "ema_fast": ef, "ema_slow": es}
    elif ef < es:
        return {"trend": "DOWNTREND", "dir": "DOWN", "emoji": "📉", "ema_fast": ef, "ema_slow": es}
    else:
        return {"trend": "SIDEWAYS",  "dir": "NONE", "emoji": "↔️", "ema_fast": ef, "ema_slow": es}

# ─────────────────────────────────────────────────────
#  🕯️  ENGULFING COUNTER-TREND
# ─────────────────────────────────────────────────────

def detect_engulfing(df, trend_dir):
    if len(df) < 3:
        return None, None

    prev = df.iloc[-2]
    curr = df.iloc[-1]
    pb   = abs(prev["close"] - prev["open"])
    cb   = abs(curr["close"] - curr["open"])

    if pb < 0.10 or cb < 0.10:
        return None, None

    bullish = (
        prev["close"] < prev["open"] and
        curr["close"] > curr["open"] and
        curr["open"]  <= prev["close"] + 0.15 and
        curr["close"] >= prev["open"]  - 0.15 and
        cb >= pb * 0.75
    )
    bearish = (
        prev["close"] > prev["open"] and
        curr["close"] < curr["open"] and
        curr["open"]  >= prev["close"] - 0.15 and
        curr["close"] <= prev["open"]  + 0.15 and
        cb >= pb * 0.75
    )

    if bullish and trend_dir == "DOWN":
        return "BULLISH", "BUY"
    if bearish and trend_dir == "UP":
        return "BEARISH", "SELL"
    return None, None

# ─────────────────────────────────────────────────────
#  🏔️  SUPPORT & RESISTANCE
# ─────────────────────────────────────────────────────

def find_sr(df):
    highs  = df["high"].values.tolist()
    lows   = df["low"].values.tolist()
    closes = df["close"].values.tolist()
    n      = len(df)
    win    = 3

    ph, pl = [], []
    for i in range(win, n - win):
        if all(highs[i] >= highs[i-j] for j in range(1, win+1)) and \
           all(highs[i] >= highs[i+j] for j in range(1, win+1)):
            ph.append(highs[i])
        if all(lows[i] <= lows[i-j] for j in range(1, win+1)) and \
           all(lows[i] <= lows[i+j] for j in range(1, win+1)):
            pl.append(lows[i])

    def cluster(levels):
        if not levels:
            return []
        ls = sorted(levels)
        clusters, group = [], [ls[0]]
        for p in ls[1:]:
            if p - group[-1] <= SR_ZONE:
                group.append(p)
            else:
                clusters.append(group)
                group = [p]
        clusters.append(group)
        result = []
        for g in clusters:
            if len(g) >= SR_STRENGTH:
                result.append({
                    "level"   : round(sum(g)/len(g), 2),
                    "strength": len(g),
                })
        return sorted(result, key=lambda x: x["strength"], reverse=True)

    curr  = closes[-1]
    res   = [x for x in cluster(ph) if x["level"] > curr]
    sup   = [x for x in cluster(pl) if x["level"] < curr]
    for x in res: x["type"] = "RESISTANCE"
    for x in sup: x["type"] = "SUPPORT"
    return sup, res


def near_sr(price, sup, res):
    for lvl in (sup + res):
        if abs(price - lvl["level"]) <= SR_NEAR_ZONE:
            return True, lvl["level"], lvl["type"], lvl["strength"]
    return False, None, None, None

# ─────────────────────────────────────────────────────
#  ✅  CEK SINYAL
# ─────────────────────────────────────────────────────

def check_signal(df):
    trend          = get_trend(df)
    engulf, action = detect_engulfing(df, trend["dir"])
    sup, res       = find_sr(df)
    price          = round(df["close"].iloc[-1], 2)
    is_near, sr_lv, sr_tp, sr_st = near_sr(price, sup, res)

    valid = (engulf is not None) and is_near
    return {
        "signal"     : action if valid else None,
        "engulf"     : engulf,
        "trend"      : trend,
        "price"      : price,
        "near_sr"    : is_near,
        "sr_level"   : sr_lv,
        "sr_type"    : sr_tp,
        "sr_strength": sr_st,
        "supports"   : sup[:4],
        "resistances": res[:4],
    }

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


def format_sr(sup, res):
    lines = ["🔴 <b>Resistance:</b>"]
    for r in (res[:3] if res else []):
        lines.append(f"   • ${r['level']}  ({r['strength']}x sentuhan)")
    if not res:
        lines.append("   • Tidak terdeteksi")
    lines.append("🟢 <b>Support:</b>")
    for s in (sup[:3] if sup else []):
        lines.append(f"   • ${s['level']}  ({s['strength']}x sentuhan)")
    if not sup:
        lines.append("   • Tidak terdeteksi")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────
#  🚀  MAIN
# ─────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  XAUUSD Counter-Trend Bot  v4.0  [Railway]")
    print(f"  Data    : Alpha Vantage (XAUUSD live 1 menit)")
    print(f"  Candle  : {N_CANDLES}  |  EMA {EMA_FAST}/{EMA_SLOW}")
    print(f"  Interval: {CHECK_INTERVAL}s")
    print("=" * 55)

    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN / CHAT_ID belum diset!")
        return

    send_telegram(
        "🤖 <b>XAUUSD Bot v4 — ONLINE di Railway!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Pair      : XAUUSD\n"
        f"⏱️ Timeframe : 1 Menit\n"
        f"📈 Data      : Alpha Vantage ({N_CANDLES} candle)\n"
        f"🔍 Strategi  : Engulfing counter-trend + S/R\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Bot berjalan 24 jam di Railway!"
    )

    last_candle_id = None

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        try:
            print(f"\n[{now_str}] Cek sinyal...")
            df = get_candles()

            if df is None or len(df) < 50:
                print("[WARN] Data kurang, coba lagi...")
                time.sleep(CHECK_INTERVAL)
                continue

            result = check_signal(df)
            t      = result["trend"]

            print(f"  Harga    : ${result['price']}")
            print(f"  Trend    : {t['trend']} (EMA{EMA_FAST}={t['ema_fast']} / EMA{EMA_SLOW}={t['ema_slow']})")
            print(f"  Engulfing: {result['engulf'] or '-'}")
            print(f"  Dekat S/R: {'YA → '+str(result['sr_type'])+' $'+str(result['sr_level']) if result['near_sr'] else 'Tidak'}")
            print(f"  Sinyal   : {result['signal'] or 'Belum valid'}")

            if result["signal"]:
                candle_ts = str(df["time"].iloc[-1])
                if last_candle_id != candle_ts:
                    last_candle_id = candle_ts
                    action = result["signal"]
                    emj    = "🟢" if action == "BUY" else "🔴"

                    logic = (
                        f"   📉 Trend DOWNTREND\n"
                        f"   🕯️ Bullish Engulfing muncul\n"
                        f"   🏔️ Dekat SUPPORT ${result['sr_level']}\n"
                        f"   → Potensi REVERSAL NAIK"
                    ) if action == "BUY" else (
                        f"   📈 Trend UPTREND\n"
                        f"   🕯️ Bearish Engulfing muncul\n"
                        f"   🏔️ Dekat RESISTANCE ${result['sr_level']}\n"
                        f"   → Potensi REVERSAL TURUN"
                    )

                    send_telegram(
                        f"{emj} <b>SINYAL {action} — XAUUSD</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⏱️ Waktu     : {now_str}\n"
                        f"💰 Harga     : ${result['price']}\n"
                        f"⏳ Timeframe : 1 Menit\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{t['emoji']} <b>Trend:</b> {t['trend']}\n"
                        f"   EMA{EMA_FAST} : {t['ema_fast']}  |  EMA{EMA_SLOW} : {t['ema_slow']}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🏔️ <b>S&amp;R ({N_CANDLES} candle):</b>\n"
                        f"{format_sr(result['supports'], result['resistances'])}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎯 <b>Logika:</b>\n{logic}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ <b>SEMUA SYARAT TERPENUHI!</b>\n"
                        f"⚠️ Pakai manajemen risiko!"
                    )
                else:
                    print("  [SKIP] Sudah dikirim.")

        except KeyboardInterrupt:
            send_telegram("⛔ <b>XAUUSD Bot dihentikan.</b>")
            break
        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
ENDOFFILE
echo "Done: $?"
