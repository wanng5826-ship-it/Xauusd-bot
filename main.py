"""
=======================================================
  SMC Multi-Pair Bot  —  v13.0
  Pairs    : 19 pair forex + komoditas + BTCUSD
  Strategy : Chart Pattern (forex) + News Sentiment (BTC)
  AI       : Groq Llama3 (analisis makro & sentimen berita)
  Data     : yfinance + FRED API + NewsAPI + RSS Feed
  Notif    : Telegram

  CHANGELOG v13.0 — STRATEGI BARU: CHART PATTERN
  [NEW] Ganti total strategi SMC → Chart Pattern:
        Reversal : Double Bottom, Double Top,
                   Head & Shoulders, Inv H&S
        Continuation: Bull Flag, Bear Flag,
                      Ascending Triangle,
                      Descending Triangle
  [NEW] SL/TP otomatis dari ukuran pattern (ATR-based)
        bukan fixed pip — lebih akurat per pattern.
  [NEW] Konfirmasi 15M: RSI filter + candle searah.
  [NEW] Minimum RR 1:1.5 wajib terpenuhi.
  [NEW] 1 pair = 1 trade aktif (dari v12.9).

  CHANGELOG v12.9:
  [FIX] 1 pair = 1 sinyal aktif, tunggu TP/SL.
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
    "EURUSD" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "GBPUSD" : {"pip_size": 0.0001, "sl_pip": 60,  "tp_pip": 30},
    "AUDUSD" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "NZDUSD" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "USDCHF" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "USDCAD" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "AUDCAD" : {"pip_size": 0.0001, "sl_pip": 50,  "tp_pip": 25},
    "EURGBP" : {"pip_size": 0.0001, "sl_pip": 40,  "tp_pip": 20},
    "EURAUD" : {"pip_size": 0.0001, "sl_pip": 60,  "tp_pip": 30},
    "GBPAUD" : {"pip_size": 0.0001, "sl_pip": 70,  "tp_pip": 35},
    "USDJPY" : {"pip_size": 0.01,   "sl_pip": 60,  "tp_pip": 30},
    "EURJPY" : {"pip_size": 0.01,   "sl_pip": 70,  "tp_pip": 35},
    "GBPJPY" : {"pip_size": 0.01,   "sl_pip": 80,  "tp_pip": 40},
    "CADJPY" : {"pip_size": 0.01,   "sl_pip": 60,  "tp_pip": 30},
    "CHFJPY" : {"pip_size": 0.01,   "sl_pip": 60,  "tp_pip": 30},
    "XAUUSD" : {"pip_size": 0.1,    "sl_pip": 300, "tp_pip": 150},
    "XAGUSD" : {"pip_size": 0.001,  "sl_pip": 200, "tp_pip": 100},
    "USOIL"  : {"pip_size": 0.01,   "sl_pip": 160, "tp_pip": 80},
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
            str(result.get("pair")),
            str(result.get("action")),
            float(result.get("entry")   or 0),
            float(result.get("sl")      or 0),
            float(result.get("tp")      or 0),
            float(result.get("rr")      or 0),
            float(result.get("sl_pip")  or 0),
            float(result.get("tp_pip")  or 0),
            str(result.get("structure", "")),
            float(result.get("st_level") or result.get("confidence") or 0),
            str(result.get("signal_age") or result.get("sentiment") or ""),
            bool(is_btc),
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
            str(result.get("pair")),
            str(symbol),
            str(result.get("action")),
            float(result.get("entry") or 0),
            float(result.get("sl")    or 0),
            float(result.get("tp")    or 0),
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

def get_wr_stats(period_days=7):
    """Ambil statistik winrate dari signal_history DB."""
    conn = get_db_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Total sinyal periode ini
        cur.execute("""
            SELECT COUNT(*) FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
        """, (period_days,))
        total = cur.fetchone()[0]

        # Yang sudah closed (ada result)
        cur.execute("""
            SELECT COUNT(*) FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            AND result IS NOT NULL
        """, (period_days,))
        closed = cur.fetchone()[0]

        # PROFIT
        cur.execute("""
            SELECT COUNT(*) FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            AND result = 'PROFIT'
        """, (period_days,))
        profit = cur.fetchone()[0]

        # LOSS
        cur.execute("""
            SELECT COUNT(*) FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            AND result = 'LOSS'
        """, (period_days,))
        loss = cur.fetchone()[0]

        # Masih open
        open_trades_count = closed and (total - closed) or 0

        # WR per pair
        cur.execute("""
            SELECT pair,
                   COUNT(*) FILTER (WHERE result IS NOT NULL) AS closed,
                   COUNT(*) FILTER (WHERE result = 'PROFIT')  AS wins,
                   COUNT(*) FILTER (WHERE result = 'LOSS')    AS losses
            FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY pair
            ORDER BY closed DESC
        """, (period_days,))
        per_pair = cur.fetchall()

        # WR BTC vs Forex
        cur.execute("""
            SELECT is_btc,
                   COUNT(*) FILTER (WHERE result IS NOT NULL) AS closed,
                   COUNT(*) FILTER (WHERE result = 'PROFIT')  AS wins
            FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            AND result IS NOT NULL
            GROUP BY is_btc
        """, (period_days,))
        by_type = cur.fetchall()

        # Streak: berapa kali profit/loss berturut-turut terakhir
        cur.execute("""
            SELECT result FROM signal_history
            WHERE result IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT 10
        """)
        recent = [r[0] for r in cur.fetchall()]

        # Detail trade closed — jam buka, tutup, entry, pair, action
        cur.execute("""
            SELECT pair, action, entry, sl, tp, result,
                   created_at, closed_at
            FROM signal_history
            WHERE created_at >= NOW() - INTERVAL '%s days'
            AND result IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT 20
        """, (period_days,))
        trade_details = cur.fetchall()

        cur.close()
        conn.close()

        winrate = round(profit / closed * 100, 1) if closed > 0 else 0
        open_count = total - closed

        return {
            "period_days"   : period_days,
            "total"         : total,
            "closed"        : closed,
            "profit"        : profit,
            "loss"          : loss,
            "open_count"    : open_count,
            "winrate"       : winrate,
            "per_pair"      : per_pair,
            "by_type"       : by_type,
            "recent"        : recent,
            "trade_details" : trade_details,
        }
    except Exception as e:
        print(f"[DB] get_wr_stats error: {e}")
        try: conn.close()
        except: pass
        return None


def send_wr_report(period_days=7):
    """Buat dan kirim laporan WR ke Telegram."""
    stats = get_wr_stats(period_days)
    if stats is None:
        send_telegram("⚠️ <b>WR Report</b>\nDatabase tidak tersedia atau belum ada data.")
        return

    if stats["closed"] == 0:
        send_telegram(
            f"📊 <b>WR Report — {period_days} Hari Terakhir</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Belum ada trade yang closed dalam {period_days} hari terakhir.\n"
            f"📂 Total sinyal dikirim : {stats['total']}\n"
            f"🔓 Masih open           : {stats['open_count']}"
        )
        return

    # Bar WR visual
    filled = int(stats["winrate"] / 10)
    bar    = "🟢" * filled + "⬜" * (10 - filled)

    # Streak terbaru
    streak_str = ""
    if stats["recent"]:
        streak_icons = {"PROFIT": "✅", "LOSS": "❌"}
        streak_str = " ".join(streak_icons.get(r, "❓") for r in stats["recent"])

    # Per pair table
    pair_lines = []
    for row in stats["per_pair"]:
        if row["closed"] == 0:
            continue
        wr_pair = round(row["wins"] / row["closed"] * 100, 1) if row["closed"] > 0 else 0
        icon    = "🏆" if wr_pair >= 60 else ("⚠️" if wr_pair >= 40 else "❌")
        pair_lines.append(
            f"  {icon} {row['pair']:<8} {row['wins']}W/{row['losses']}L  WR:{wr_pair}%"
        )
    pair_text = "\n".join(pair_lines) if pair_lines else "  (belum ada data per pair)"

    # Breakdown BTC vs Forex
    btc_line   = ""
    forex_line = ""
    for row in stats["by_type"]:
        wr_t  = round(row["wins"] / row["closed"] * 100, 1) if row["closed"] > 0 else 0
        label = "BTC" if row["is_btc"] else "Forex/Komoditas"
        line  = f"  {'₿' if row['is_btc'] else '💱'} {label}: {row['wins']}W/{row['closed'] - row['wins']}L  WR:{wr_t}%"
        if row["is_btc"]:
            btc_line = line
        else:
            forex_line = line

    # Detail trade closed — jam buka, tutup, durasi, entry, close
    detail_lines = []
    pip_cfg = PIP_CONFIG  # pakai konfigurasi pip yang sudah ada
    for t in stats["trade_details"]:
        pair      = t["pair"]
        action    = t["action"]
        entry     = t["entry"]
        result    = t["result"]
        opened_at = t["created_at"]
        closed_at = t["closed_at"]

        emj = "✅" if result == "PROFIT" else "❌"

        # Format jam (WIB = UTC+7)
        try:
            from datetime import timezone, timedelta
            wib = timezone(timedelta(hours=7))
            buka_str  = opened_at.astimezone(wib).strftime("%d/%m %H:%M") if opened_at else "-"
            tutup_str = closed_at.astimezone(wib).strftime("%d/%m %H:%M") if closed_at else "-"
        except Exception:
            buka_str  = str(opened_at)[:16] if opened_at else "-"
            tutup_str = str(closed_at)[:16] if closed_at else "-"

        # Durasi
        durasi_str = "-"
        if opened_at and closed_at:
            try:
                delta     = closed_at - opened_at
                total_min = int(delta.total_seconds() / 60)
                jam       = total_min // 60
                mnt       = total_min % 60
                durasi_str = f"{jam}j {mnt}m" if jam > 0 else f"{mnt}m"
            except Exception:
                durasi_str = "-"

        # Estimasi close price (entry ± tp/sl)
        cfg      = pip_cfg.get(pair, {})
        pip_size = cfg.get("pip_size", 0.0001)
        tp_pip   = cfg.get("tp_pip", 50)
        sl_pip   = cfg.get("sl_pip", 25)
        if result == "PROFIT":
            if action == "BUY":
                close_est = round(entry + tp_pip * pip_size, 5)
                pip_val   = f"+{tp_pip}pip"
            else:
                close_est = round(entry - tp_pip * pip_size, 5)
                pip_val   = f"+{tp_pip}pip"
        else:
            if action == "BUY":
                close_est = round(entry - sl_pip * pip_size, 5)
                pip_val   = f"-{sl_pip}pip"
            else:
                close_est = round(entry + sl_pip * pip_size, 5)
                pip_val   = f"-{sl_pip}pip"

        detail_lines.append(
            f"{emj} <b>{result} — {pair} {action}</b>\n"
            f"   📥 Buka  : {buka_str} WIB\n"
            f"   📤 Tutup : {tutup_str} WIB\n"
            f"   ⏱️ Durasi: {durasi_str}\n"
            f"   💰 Entry : {entry}\n"
            f"   🎯 Close : {close_est}  ({pip_val})"
        )
    detail_text = "\n\n".join(detail_lines) if detail_lines else "  (belum ada detail)"

    wr_emj = "🏆" if stats["winrate"] >= 60 else ("⚠️" if stats["winrate"] >= 40 else "🔴")

    msg = (
        f"📊 <b>WR REPORT — {period_days} Hari Terakhir</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{wr_emj} <b>Winrate Keseluruhan</b>\n"
        f"  {bar}  {stats['winrate']}%\n\n"
        f"📈 Total Sinyal : {stats['total']}\n"
        f"✅ Profit       : {stats['profit']}\n"
        f"❌ Loss         : {stats['loss']}\n"
        f"🔓 Masih Open   : {stats['open_count']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>Breakdown Strategi:</b>\n"
        f"{forex_line}\n"
        f"{btc_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>Per Pair:</b>\n{pair_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 <b>10 Trade Terakhir:</b>\n"
        f"  {streak_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 <b>Detail Trade Closed:</b>\n\n"
        f"{detail_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 SMC Bot v12.8 | Data: PostgreSQL Railway"
    )
    send_telegram(msg)
    print(f"[WR] Report dikirim — WR {stats['winrate']}% ({stats['profit']}W/{stats['loss']}L)")


def get_open_pairs():
    """Ambil semua pair yang masih ada di open_trades."""
    conn = get_db_conn()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT pair FROM open_trades")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return set(r[0] for r in rows)
    except Exception as e:
        print(f"[DB] get_open_pairs error: {e}")
        try: conn.close()
        except: pass
        return set()


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

# ───────────────────────────────────────────────
#  TELEGRAM & NEWS & GROQ
# ───────────────────────────────────────────────

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
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": kw, "language": "en", "sortBy": "publishedAt",
                    "pageSize": 3, "apiKey": NEWS_API_KEY},
            timeout=10
        )
        if r.status_code != 200:
            return []
        return [a["title"] for a in r.json().get("articles", [])[:3]]
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
    if not FRED_API_KEY:
        return result
    for name, series_id in indicators.items():
        try:
            r = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={"series_id": series_id, "api_key": FRED_API_KEY,
                        "file_type": "json", "sort_order": "desc", "limit": 2},
                timeout=10
            )
            if r.status_code != 200:
                continue
            obs = r.json().get("observations", [])
            if len(obs) >= 2:
                latest = obs[0]["value"]
                prev   = obs[1]["value"]
                if latest == "." or prev == ".":
                    continue
                result[name] = {
                    "latest": latest, "prev": prev,
                    "change": "NAIK" if float(latest) > float(prev)
                              else "TURUN" if float(latest) < float(prev) else "SAMA",
                }
        except Exception as e:
            print(f"[FRED ERROR] {name}: {e}")
    return result

def format_fred_data(fred_data):
    if not fred_data:
        return "Data ekonomi tidak tersedia"
    arrows = {"NAIK": "⬆️", "TURUN": "⬇️", "SAMA": "➡️"}
    lines  = []
    for name, data in fred_data.items():
        lines.append(f"• {name}: {data['latest']} {arrows.get(data['change'],'')} (sblm: {data['prev']})")
    return "\n".join(lines)

def analyze_with_groq(pair, structure, action, headlines, fred_data):
    if not GROQ_API_KEY:
        return "Analisis AI tidak tersedia."
    try:
        news_text = "\n".join([f"- {h}" for h in headlines]) if headlines else "Tidak ada berita."
        fred_text = ""
        if fred_data:
            fred_text = (
                f"Fed Rate: {fred_data.get('Fed Rate',{}).get('latest','N/A')}% | "
                f"CPI: {fred_data.get('CPI US',{}).get('latest','N/A')} | "
                f"NFP: {fred_data.get('NFP',{}).get('latest','N/A')}K | "
                f"DXY: {fred_data.get('DXY',{}).get('latest','N/A')}"
            )
        prompt = (
            f"Analis trading senior. Pair:{pair} Sinyal:{action} "
            f"Pattern:{structure}\n{fred_text}\nBerita:\n{news_text}\n"
            f"Apakah fundamental mendukung sinyal {action} {pair}? "
            f"Jawab Bahasa Indonesia, max 4 kalimat, langsung ke poin."
        )
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 300},
            timeout=15
        )
        if r.status_code != 200:
            return "Analisis AI tidak tersedia saat ini."
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[GROQ ERROR] {e}")
        return "Analisis AI tidak tersedia saat ini."

# ── BTC News functions ──────────────────────────

def get_btc_news_cryptopanic():
    if not CRYPTOPANIC_API_KEY:
        print("[CRYPTOPANIC] API key tidak ada, skip")
        return []
    try:
        r = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": CRYPTOPANIC_API_KEY, "currencies": "BTC",
                    "filter": "hot", "public": "true"},
            timeout=10
        )
        if r.status_code != 200:
            return []
        results   = r.json().get("results", [])
        headlines = []
        for item in results[:8]:
            title = item.get("title", "")
            votes = item.get("votes", {})
            if title:
                headlines.append({"title": title, "source": item.get("source", {}).get("title", ""),
                                   "bullish": votes.get("positive", 0), "bearish": votes.get("negative", 0)})
        print(f"[CRYPTOPANIC] {len(headlines)} berita diambil")
        return headlines
    except Exception as e:
        print(f"[CRYPTOPANIC ERROR] {e}")
        return []

def get_btc_news_newsapi():
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": "Bitcoin BTC crypto Trump interest rate Fed",
                    "language": "en", "sortBy": "publishedAt",
                    "pageSize": 6, "apiKey": NEWS_API_KEY},
            timeout=10
        )
        if r.status_code != 200:
            return []
        return [a["title"] for a in r.json().get("articles", [])[:6]]
    except Exception as e:
        print(f"[NEWSAPI BTC ERROR] {e}")
        return []

def get_btc_news_rss():
    feeds = [
        "https://feeds.feedburner.com/CoinDesk",
        "https://cointelegraph.com/rss",
        "https://bitcoinmagazine.com/.rss/full/",
    ]
    headlines = []
    for url in feeds:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue
            import xml.etree.ElementTree as ET
            root  = ET.fromstring(r.content)
            items = root.findall(".//item")
            for item in items[:3]:
                title = item.findtext("title", "")
                if title:
                    headlines.append(title.strip())
        except Exception:
            continue
    return headlines

def analyze_btc_sentiment_groq(cp_news, na_news, rss_news, price):
    import json
    cp_titles = [n["title"] for n in cp_news]
    na_news   = na_news if isinstance(na_news, list) else []
    all_news  = cp_titles + na_news + rss_news
    if not GROQ_API_KEY:
        return None, "No Groq key", all_news
    news_block = "\n".join([f"- {h[:120]}" for h in all_news[:15]])
    cp_votes   = "\n".join([
        f"  [{n['bullish']}🐂/{n['bearish']}🐻] {n['title'][:80]}"
        for n in cp_news[:5]
    ]) if cp_news else "  Tidak ada data CryptoPanic."
    prompt = f"""Kamu adalah analis sentimen crypto profesional.
BTC saat ini: ${price:,.2f}

CryptoPanic community votes:
{cp_votes}

Berita terkini (CoinDesk, CoinTelegraph, NewsAPI):
{news_block}

Analisis sentimen dan berikan keputusan trading BTC.
Jawab HANYA dalam JSON (tanpa markdown):
{{
  "signal": "BUY" | "SELL" | "NO_TRADE",
  "sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",
  "confidence": 0-100,
  "key_news": "judul berita paling berpengaruh",
  "reason": "alasan singkat max 3 kalimat"
}}"""
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 400, "temperature": 0.3},
            timeout=20
        )
        if r.status_code != 200:
            return None, "Groq error", all_news
        raw    = r.json()["choices"][0]["message"]["content"].strip()
        raw    = raw.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)
        return parsed, parsed.get("reason", "-"), all_news
    except Exception as e:
        print(f"[GROQ BTC ERROR] {e}")
        return None, "Parse error", all_news

def analyze_btc_scalping(pair, symbol):
    print(f"\n[{pair}] Menganalisis (News Sentiment)...")
    df_1h = get_data(symbol, "1h", "3d")
    if df_1h is None or len(df_1h) < 5:
        return None
    price = df_1h["close"].iloc[-2]
    atr   = (df_1h["high"] - df_1h["low"]).tail(14).mean()
    cp_news  = get_btc_news_cryptopanic()
    na_news  = get_btc_news_newsapi()
    rss_news = get_btc_news_rss()
    total_news = len(cp_news) + len(na_news) + len(rss_news)
    print(f"[{pair}] Berita: CP={len(cp_news)} NA={len(na_news)} RSS={len(rss_news)}")
    if total_news == 0:
        return None
    groq_result, reason, all_headlines = analyze_btc_sentiment_groq(cp_news, na_news, rss_news, price)
    if groq_result is None:
        return None
    signal     = groq_result.get("signal", "NO_TRADE")
    sentiment  = groq_result.get("sentiment", "NEUTRAL")
    confidence = groq_result.get("confidence", 0)
    key_news   = groq_result.get("key_news", "-")
    if confidence < 55 or signal == "NO_TRADE":
        print(f"[{pair}] Confidence {confidence}% / NO_TRADE → skip")
        return None
    buffer = atr * 0.3
    if signal == "BUY":
        sl = round(price - atr * 1.5 - buffer, 2)
        tp = round(price + (price - sl) * 2.0, 2)
    else:
        sl = round(price + atr * 1.5 + buffer, 2)
        tp = round(price - (sl - price) * 2.0, 2)
    rr = round(abs(tp - price) / abs(price - sl), 2) if abs(price - sl) > 0 else 0
    print(f"[{pair}] ✅ {signal} Entry:{price} SL:{sl} TP:{tp} RR:1:{rr} Conf:{confidence}%")
    return {
        "pair": pair, "action": signal, "entry": round(price, 2),
        "sl": sl, "tp": tp, "rr": rr,
        "sentiment": sentiment, "confidence": confidence,
        "key_news": key_news, "reason": reason,
        "headlines": all_headlines[:5], "scalping": True,
    }


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

# ───────────────────────────────────────────────
#  CHART PATTERN STRATEGY
# ───────────────────────────────────────────────

def calc_atr(df, period=14):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean().iloc[-1]

def calc_rsi(df, period=14):
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(period, min_periods=1).mean()
    loss  = (-delta.clip(upper=0)).rolling(period, min_periods=1).mean()
    rs    = gain / loss.replace(0, 1e-10)
    return (100 - 100 / (1 + rs)).iloc[-1]

def find_pivots(series, order=5):
    """Cari pivot high & low dengan window order candle kiri-kanan."""
    highs, lows = [], []
    arr = series.values
    for i in range(order, len(arr) - order):
        window = arr[i - order: i + order + 1]
        if arr[i] == max(window):
            highs.append((i, arr[i]))
        if arr[i] == min(window):
            lows.append((i, arr[i]))
    return highs, lows

# ── Pattern detectors ───────────────────────────

def detect_consecutive_candles(df, lookback=30, min_count=5):
    """
    Deteksi candle beruntun searah (minimal min_count candle).
    Syarat ketat:
    - Min 5 candle beruntun
    - Body candle harus >= 40% dari range candle (bukan doji/kecil)
    - Tidak boleh ada candle berlawanan di tengah
    Return: ('UP'/'DOWN', jumlah candle, start_idx) atau None
    """
    data = df.tail(lookback).reset_index(drop=True)
    n    = len(data)
    atr  = calc_atr(df)

    for i in range(n - min_count, 0, -1):
        # Cek candle turun beruntun
        down_count = 0
        for j in range(i, min(i + 15, n)):
            candle = data.iloc[j]
            body   = abs(candle["close"] - candle["open"])
            rng    = candle["high"] - candle["low"]
            # Harus bearish, body cukup besar, bukan doji
            if (candle["close"] < candle["open"] and
                rng > 0 and body / rng >= 0.4 and
                body >= atr * 0.3):
                down_count += 1
            else:
                break
        if down_count >= min_count:
            return ("DOWN", down_count, i)

        # Cek candle naik beruntun
        up_count = 0
        for j in range(i, min(i + 15, n)):
            candle = data.iloc[j]
            body   = abs(candle["close"] - candle["open"])
            rng    = candle["high"] - candle["low"]
            # Harus bullish, body cukup besar, bukan doji
            if (candle["close"] > candle["open"] and
                rng > 0 and body / rng >= 0.4 and
                body >= atr * 0.3):
                up_count += 1
            else:
                break
        if up_count >= min_count:
            return ("UP", up_count, i)

    return None


def detect_momentum_candle(df, after_idx, direction, atr, min_ratio=1.5):
    """
    Deteksi candle panjang/tajam berlawanan setelah candle beruntun.
    direction: arah candle BERUNTUN ('UP'/'DOWN')
    Return: (momentum_idx, momentum_body, momentum_high, momentum_low) atau None
    """
    data = df.tail(20).reset_index(drop=True)
    # Cari candle berlawanan yang tajam setelah candle beruntun
    for i in range(after_idx, len(data) - 1):
        candle = data.iloc[i]
        body   = abs(candle["close"] - candle["open"])
        # Harus berlawanan dan body > 1.5x ATR
        if direction == "DOWN" and candle["close"] > candle["open"] and body >= atr * min_ratio:
            # Cek candle berikutnya ikut arah momentum (naik)
            next_c = data.iloc[i + 1]
            if next_c["close"] > next_c["open"]:
                return (i, body, candle["high"], candle["low"], candle["close"], candle["open"])
        elif direction == "UP" and candle["close"] < candle["open"] and body >= atr * min_ratio:
            # Cek candle berikutnya ikut arah momentum (turun)
            next_c = data.iloc[i + 1]
            if next_c["close"] < next_c["open"]:
                return (i, body, candle["high"], candle["low"], candle["close"], candle["open"])
    return None


def detect_pullback_and_entry(df, momentum_idx, momentum_body, momentum_high, momentum_low,
                               momentum_close, momentum_open, direction):
    """
    Setelah momentum candle, tunggu pullback lalu konfirmasi entry.
    direction: arah MOMENTUM candle ('UP'/'DOWN')
    - Pullback tidak boleh lewat 80% body momentum candle
    - Entry saat muncul candle konfirmasi ikut arah momentum (engulfing pullback)
    Return: (action, entry_price, pullback_low/high) atau None
    """
    data         = df.tail(20).reset_index(drop=True)
    max_pullback = momentum_body * 0.8

    if direction == "UP":
        # Momentum naik → pullback turun → entry BUY
        pullback_limit = momentum_close - max_pullback  # batas bawah pullback
        for i in range(momentum_idx + 2, len(data) - 1):
            candle = data.iloc[i]
            # Cek pullback tidak lewat 80%
            if candle["low"] < pullback_limit:
                return None  # Pullback terlalu dalam → SKIP
            # Konfirmasi: candle turun (pullback) diikuti candle naik (engulfing)
            if candle["close"] < candle["open"]:  # candle pullback
                next_c = data.iloc[i + 1]
                if next_c["close"] > next_c["open"] and next_c["close"] > candle["open"]:
                    # Candle naik menelan pullback → ENTRY BUY
                    return ("BUY", round(next_c["close"], 5), round(momentum_low, 5))
    else:
        # Momentum turun → pullback naik → entry SELL
        pullback_limit = momentum_close + max_pullback  # batas atas pullback
        for i in range(momentum_idx + 2, len(data) - 1):
            candle = data.iloc[i]
            # Cek pullback tidak lewat 80%
            if candle["high"] > pullback_limit:
                return None  # Pullback terlalu dalam → SKIP
            # Konfirmasi: candle naik (pullback) diikuti candle turun (engulfing)
            if candle["close"] > candle["open"]:  # candle pullback
                next_c = data.iloc[i + 1]
                if next_c["close"] < next_c["open"] and next_c["close"] < candle["open"]:
                    # Candle turun menelan pullback → ENTRY SELL
                    return ("SELL", round(next_c["close"], 5), round(momentum_high, 5))
    return None


def scan_momentum_pullback(df_5m, df_15m, pair):
    """
    Strategi utama: Momentum Candle + Pullback + Engulfing Entry.
    Timeframe: M5 untuk entry, 15M untuk konfirmasi trend.
    """
    atr  = calc_atr(df_5m)
    data = df_5m.tail(30).reset_index(drop=True)

    # Step 1: Deteksi candle beruntun
    consec = detect_consecutive_candles(df_5m, lookback=25, min_count=3)
    if consec is None:
        print(f"[{pair}] Tidak ada candle beruntun → NO TRADE")
        return None

    consec_dir, consec_count, consec_start = consec
    consec_end = consec_start + consec_count

    # High/Low dari candle beruntun (untuk TP)
    # Ambil high/low candle PERTAMA dari candle beruntun (level terdekat)
    consec_data  = data.iloc[consec_start:consec_end]
    if consec_dir == "DOWN":
        consec_high = consec_data["high"].iloc[0]
        consec_low  = consec_data["low"].min()
    else:
        consec_high = consec_data["high"].max()
        consec_low  = consec_data["low"].iloc[0]

    print(f"[{pair}] Candle beruntun {consec_dir} ({consec_count} candle)")

    # Step 2: Deteksi momentum candle berlawanan
    mom = detect_momentum_candle(df_5m, consec_end, consec_dir, atr)
    if mom is None:
        print(f"[{pair}] Tidak ada momentum candle → NO TRADE")
        return None

    mom_idx, mom_body, mom_high, mom_low, mom_close, mom_open = mom
    print(f"[{pair}] Momentum candle ditemukan di idx {mom_idx}")

    # Step 3: Deteksi pullback + konfirmasi entry
    # Arah momentum berlawanan dengan candle beruntun
    mom_dir = "UP" if consec_dir == "DOWN" else "DOWN"
    entry_result = detect_pullback_and_entry(
        df_5m, mom_idx, mom_body, mom_high, mom_low, mom_close, mom_open, mom_dir
    )
    if entry_result is None:
        print(f"[{pair}] Pullback terlalu dalam atau belum konfirmasi → NO TRADE")
        return None

    action, entry_price, sl_ref = entry_result

    # Konfirmasi 15M searah
    last_15m = df_15m.iloc[-2]
    if action == "BUY" and last_15m["close"] < last_15m["open"]:
        print(f"[{pair}] 15M bearish → belum konfirmasi BUY")
        return None
    if action == "SELL" and last_15m["close"] > last_15m["open"]:
        print(f"[{pair}] 15M bullish → belum konfirmasi SELL")
        return None

    # Hitung SL dan TP
    cfg      = PIP_CONFIG.get(pair, {})
    pip_size = cfg.get("pip_size", 0.0001)
    buffer   = atr * 0.3

    if action == "BUY":
        sl = round(mom_low - buffer, 5)
        tp = round(consec_high - buffer, 5)  # sedikit di bawah high beruntun
    else:
        sl = round(mom_high + buffer, 5)
        tp = round(consec_low + buffer, 5)   # sedikit di atas low beruntun

    risk   = abs(entry_price - sl)
    reward = abs(tp - entry_price)
    if risk <= 0 or reward <= 0:
        print(f"[{pair}] SL/TP tidak valid → skip")
        return None

    rr     = round(reward / risk, 1)
    sl_pip = round(risk / pip_size, 1)
    tp_pip = round(reward / pip_size, 1)

    if rr < 1.0:
        print(f"[{pair}] RR terlalu rendah ({rr}) → skip")
        return None

    print(f"[{pair}] ✅ {action} | Entry:{entry_price} SL:{sl} TP:{tp} RR:1:{rr}")
    return action, "Momentum+Pullback", entry_price, sl, tp, sl_pip, tp_pip, rr


def detect_consolidation(df, after_idx, max_body_ratio=0.4, min_candles=2):
    """
    Deteksi konsolidasi: candle kecil-kecil setelah candle beruntun.
    Konsolidasi = body candle < max_body_ratio * ATR selama min_candles candle.
    Return: True jika konsolidasi, False jika tidak
    """
    data = df.tail(25).reset_index(drop=True)
    atr  = calc_atr(df)
    small_count = 0
    for i in range(after_idx, min(after_idx + 6, len(data))):
        candle = data.iloc[i]
        body   = abs(candle["close"] - candle["open"])
        if body < atr * max_body_ratio:
            small_count += 1
        else:
            break
    return small_count >= min_candles


def scan_consolidation_breakout(df_5m, df_15m, pair):
    """
    Kondisi 2: Candle beruntun → konsolidasi → entry ikut arah beruntun.
    """
    atr  = calc_atr(df_5m)
    data = df_5m.tail(30).reset_index(drop=True)

    # Deteksi candle beruntun
    consec = detect_consecutive_candles(df_5m, lookback=25, min_count=3)
    if consec is None:
        return None

    consec_dir, consec_count, consec_start = consec
    consec_end  = consec_start + consec_count
    consec_data = data.iloc[consec_start:consec_end]
    consec_high = consec_data["high"].max()
    consec_low  = consec_data["low"].min()

    # Cek setelah beruntun ada konsolidasi (bukan momentum candle besar)
    is_consol = detect_consolidation(df_5m, consec_end, min_candles=2)
    if not is_consol:
        return None

    print(f"[{pair}] Konsolidasi setelah candle beruntun {consec_dir} → ikut arah beruntun")

    # Konfirmasi: candle terakhir ikut arah beruntun
    last = data.iloc[-1]
    if consec_dir == "DOWN" and last["close"] > last["open"]:
        print(f"[{pair}] Candle terakhir berlawanan → belum konfirmasi SELL")
        return None
    if consec_dir == "UP" and last["close"] < last["open"]:
        print(f"[{pair}] Candle terakhir berlawanan → belum konfirmasi BUY")
        return None

    # Konfirmasi 15M searah
    last_15m = df_15m.iloc[-2]
    action   = "SELL" if consec_dir == "DOWN" else "BUY"
    if action == "BUY" and last_15m["close"] < last_15m["open"]:
        print(f"[{pair}] 15M bearish → belum konfirmasi BUY")
        return None
    if action == "SELL" and last_15m["close"] > last_15m["open"]:
        print(f"[{pair}] 15M bullish → belum konfirmasi SELL")
        return None

    # Hitung SL/TP
    cfg      = PIP_CONFIG.get(pair, {})
    pip_size = cfg.get("pip_size", 0.0001)
    buffer   = atr * 0.3
    price    = data["close"].iloc[-1]

    if action == "BUY":
        sl = round(consec_low - buffer, 5)
        # TP = high candle pertama beruntun (resistance terdekat) - buffer
        tp = round(consec_data["high"].iloc[0] - buffer, 5)
    else:
        sl = round(consec_high + buffer, 5)
        # TP = low candle pertama beruntun (support terdekat) + buffer
        tp = round(consec_data["low"].iloc[0] + buffer, 5)

    risk   = abs(price - sl)
    reward = abs(tp - price)
    if risk <= 0 or reward <= 0:
        return None

    rr     = round(reward / risk, 1)
    sl_pip = round(risk / pip_size, 1)
    tp_pip = round(reward / pip_size, 1)

    if rr < 1.0:
        print(f"[{pair}] RR terlalu rendah ({rr}) → skip")
        return None

    print(f"[{pair}] ✅ {action} | Entry:{round(price,5)} SL:{sl} TP:{tp} RR:1:{rr}")
    return action, "Konsolidasi+Breakout", round(price, 5), sl, tp, sl_pip, tp_pip, rr


def analyze_pair(pair, symbol):
    print(f"\n{'='*10} [{pair}] {'='*10}")
    print(f"[{pair}] Menganalisis (Momentum+Pullback)...")

    df_5m  = get_data(symbol, "5m",  "5d")
    df_15m = get_data(symbol, "15m", "10d")

    if df_5m is None or len(df_5m) < 30:
        print(f"[{pair}] Data 5M tidak cukup, skip")
        return None
    if df_15m is None or len(df_15m) < 20:
        print(f"[{pair}] Data 15M tidak cukup, skip")
        return None

    # Kondisi 1: Momentum candle + pullback + engulfing
    result = scan_momentum_pullback(df_5m, df_15m, pair)

    # Kondisi 2: Candle beruntun + konsolidasi (kalau kondisi 1 tidak ada)
    if result is None:
        result = scan_consolidation_breakout(df_5m, df_15m, pair)

    if result is None:
        print(f"[{pair}] Tidak ada setup → NO TRADE")
        return None

    action, strategy_name, entry, sl, tp, sl_pip_val, tp_pip_val, rr = result

    return {
        "pair"         : pair,
        "action"       : action,
        "entry"        : entry,
        "optimal_entry": entry,
        "sl"           : sl,
        "tp"           : tp,
        "rr"           : rr,
        "structure"    : action,
        "structure_15m": strategy_name,
        "ema50_1h"     : entry,
        "st_level"     : entry,
        "sl_pip"       : sl_pip_val,
        "tp_pip"       : tp_pip_val,
        "signal_age"   : f"🕯️ {strategy_name}",
        "scalping"     : False,
    }
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

def main():
    print("=" * 55)
    print("  Chart Pattern Bot  v13.0  [Railway]")
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
    wr_timer           = 0          # Timer WR report harian
    last_update_id     = 0          # Untuk polling command Telegram

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Scanning {len(PAIRS)} pairs...")

        # ── Cek command /wr dari Telegram ──────────────────────
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 1},
                timeout=5
            )
            if r.status_code == 200:
                updates = r.json().get("result", [])
                for upd in updates:
                    last_update_id = upd["update_id"]
                    text = upd.get("message", {}).get("text", "").strip().lower()
                    if text == "/wr":
                        print("[CMD] /wr diterima → kirim WR report 7 hari")
                        send_wr_report(7)
                    elif text == "/wr30":
                        print("[CMD] /wr30 diterima → kirim WR report 30 hari")
                        send_wr_report(30)
                    elif text == "/wr1":
                        print("[CMD] /wr1 diterima → kirim WR report hari ini")
                        send_wr_report(1)
        except Exception as e:
            print(f"[CMD] getUpdates error: {e}")

        # ── WR report otomatis setiap 24 jam ───────────────────
        if time.time() - wr_timer > 86400:
            if wr_timer > 0:   # Skip saat pertama kali bot start
                print("[WR] Kirim laporan WR harian otomatis...")
                send_wr_report(7)
            wr_timer = time.time()

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

        # Ambil pair yang masih open — tidak boleh kirim sinyal baru
        open_pairs = get_open_pairs()
        if open_pairs:
            print(f"[LOCK] Pair masih open (skip): {', '.join(open_pairs)}")

        for pair, symbol in PAIRS.items():
            if signals_this_cycle >= MAX_SIGNALS_PER_CYCLE:
                print(f"[LIMIT] Max {MAX_SIGNALS_PER_CYCLE} sinyal tercapai, skip sisa pair")
                break

            # ── KUNCI UTAMA: skip kalau masih ada trade aktif ──
            if pair in open_pairs:
                print(f"[{pair}] Trade masih open → tunggu TP/SL dulu, skip.")
                continue

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
