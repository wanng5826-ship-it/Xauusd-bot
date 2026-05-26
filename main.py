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

def detect_double_bottom(df, tolerance=0.003):
    """Double Bottom → BUY signal."""
    _, lows = find_pivots(df["low"], order=5)
    if len(lows) < 2:
        return False, None, None
    l1_i, l1 = lows[-2]
    l2_i, l2 = lows[-1]
    if l2_i <= l1_i:
        return False, None, None
    if abs(l1 - l2) / l1 > tolerance:
        return False, None, None
    # Neckline = high antara 2 lows
    neck = df["high"].iloc[l1_i:l2_i].max()
    curr = df["close"].iloc[-1]
    # Konfirmasi: harga breakout di atas neckline
    if curr > neck:
        return True, round(neck, 5), round((neck - min(l1, l2)), 5)
    return False, None, None

def detect_double_top(df, tolerance=0.003):
    """Double Top → SELL signal."""
    highs, _ = find_pivots(df["high"], order=5)
    if len(highs) < 2:
        return False, None, None
    h1_i, h1 = highs[-2]
    h2_i, h2 = highs[-1]
    if h2_i <= h1_i:
        return False, None, None
    if abs(h1 - h2) / h1 > tolerance:
        return False, None, None
    neck = df["low"].iloc[h1_i:h2_i].min()
    curr = df["close"].iloc[-1]
    if curr < neck:
        return True, round(neck, 5), round((max(h1, h2) - neck), 5)
    return False, None, None

def detect_head_and_shoulders(df, tolerance=0.005):
    """Head & Shoulders → SELL signal."""
    highs, _ = find_pivots(df["high"], order=4)
    if len(highs) < 3:
        return False, None, None
    l_i, lsh = highs[-3]
    h_i, head = highs[-2]
    r_i, rsh  = highs[-1]
    if not (l_i < h_i < r_i):
        return False, None, None
    if head <= lsh or head <= rsh:
        return False, None, None
    if abs(lsh - rsh) / lsh > tolerance * 3:
        return False, None, None
    _, lows = find_pivots(df["low"], order=4)
    lows_between = [v for i, v in lows if l_i < i < r_i]
    if not lows_between:
        return False, None, None
    neck = sum(lows_between) / len(lows_between)
    curr = df["close"].iloc[-1]
    if curr < neck:
        target = round(head - neck, 5)
        return True, round(neck, 5), target
    return False, None, None

def detect_inv_head_and_shoulders(df, tolerance=0.005):
    """Inverse H&S → BUY signal."""
    _, lows = find_pivots(df["low"], order=4)
    if len(lows) < 3:
        return False, None, None
    l_i, lsh = lows[-3]
    h_i, head = lows[-2]
    r_i, rsh  = lows[-1]
    if not (l_i < h_i < r_i):
        return False, None, None
    if head >= lsh or head >= rsh:
        return False, None, None
    if abs(lsh - rsh) / lsh > tolerance * 3:
        return False, None, None
    highs, _ = find_pivots(df["high"], order=4)
    highs_between = [v for i, v in highs if l_i < i < r_i]
    if not highs_between:
        return False, None, None
    neck = sum(highs_between) / len(highs_between)
    curr = df["close"].iloc[-1]
    if curr > neck:
        target = round(neck - head, 5)
        return True, round(neck, 5), target
    return False, None, None

def detect_bull_flag(df):
    """Bull Flag → BUY signal."""
    if len(df) < 30:
        return False, None, None
    pole_df     = df.iloc[-30:-15]
    flag_df     = df.iloc[-15:]
    pole_gain   = (pole_df["close"].iloc[-1] - pole_df["close"].iloc[0]) / pole_df["close"].iloc[0]
    flag_retrace = (flag_df["close"].iloc[-1] - flag_df["close"].iloc[0]) / flag_df["close"].iloc[0]
    if pole_gain < 0.005:
        return False, None, None
    if not (-0.005 >= flag_retrace >= -0.015):
        return False, None, None
    breakout_level = flag_df["high"].max()
    curr = df["close"].iloc[-1]
    if curr > breakout_level:
        pole_height = pole_df["close"].iloc[-1] - pole_df["close"].iloc[0]
        return True, round(breakout_level, 5), round(pole_height, 5)
    return False, None, None

def detect_bear_flag(df):
    """Bear Flag → SELL signal."""
    if len(df) < 30:
        return False, None, None
    pole_df      = df.iloc[-30:-15]
    flag_df      = df.iloc[-15:]
    pole_drop    = (pole_df["close"].iloc[0] - pole_df["close"].iloc[-1]) / pole_df["close"].iloc[0]
    flag_retrace = (flag_df["close"].iloc[-1] - flag_df["close"].iloc[0]) / flag_df["close"].iloc[0]
    if pole_drop < 0.005:
        return False, None, None
    if not (0.005 <= flag_retrace <= 0.015):
        return False, None, None
    breakout_level = flag_df["low"].min()
    curr = df["close"].iloc[-1]
    if curr < breakout_level:
        pole_height = pole_df["close"].iloc[0] - pole_df["close"].iloc[-1]
        return True, round(breakout_level, 5), round(pole_height, 5)
    return False, None, None

def detect_ascending_triangle(df):
    """Ascending Triangle → BUY signal."""
    if len(df) < 20:
        return False, None, None
    recent      = df.iloc[-20:]
    resistance  = recent["high"].max()
    lows_series = recent["low"]
    # Cek lower-lows membentuk ascending (pakai regresi sederhana)
    x     = range(len(lows_series))
    mean_x = sum(x) / len(x)
    mean_y = lows_series.mean()
    num   = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, lows_series))
    den   = sum((xi - mean_x)**2 for xi in x)
    slope = num / den if den != 0 else 0
    curr  = df["close"].iloc[-1]
    if slope > 0 and curr > resistance * 0.998:
        target = resistance - recent["low"].min()
        return True, round(resistance, 5), round(target, 5)
    return False, None, None

def detect_descending_triangle(df):
    """Descending Triangle → SELL signal."""
    if len(df) < 20:
        return False, None, None
    recent    = df.iloc[-20:]
    support   = recent["low"].min()
    high_series = recent["high"]
    x     = range(len(high_series))
    mean_x = sum(x) / len(x)
    mean_y = high_series.mean()
    num   = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, high_series))
    den   = sum((xi - mean_x)**2 for xi in x)
    slope = num / den if den != 0 else 0
    curr  = df["close"].iloc[-1]
    if slope < 0 and curr < support * 1.002:
        target = recent["high"].max() - support
        return True, round(support, 5), round(target, 5)
    return False, None, None


def scan_patterns(df_1h, df_15m, pair):
    """
    Scan semua pattern di H1, konfirmasi di 15M.
    Return: (action, pattern_name, breakout_level, pattern_height) atau None
    """
    results = []

    # BUY patterns
    ok, level, height = detect_double_bottom(df_1h)
    if ok: results.append(("BUY", "Double Bottom", level, height))

    ok, level, height = detect_inv_head_and_shoulders(df_1h)
    if ok: results.append(("BUY", "Inv Head & Shoulders", level, height))

    ok, level, height = detect_bull_flag(df_1h)
    if ok: results.append(("BUY", "Bull Flag", level, height))

    ok, level, height = detect_ascending_triangle(df_1h)
    if ok: results.append(("BUY", "Ascending Triangle", level, height))

    # SELL patterns
    ok, level, height = detect_double_top(df_1h)
    if ok: results.append(("SELL", "Double Top", level, height))

    ok, level, height = detect_head_and_shoulders(df_1h)
    if ok: results.append(("SELL", "Head & Shoulders", level, height))

    ok, level, height = detect_bear_flag(df_1h)
    if ok: results.append(("SELL", "Bear Flag", level, height))

    ok, level, height = detect_descending_triangle(df_1h)
    if ok: results.append(("SELL", "Descending Triangle", level, height))

    if not results:
        return None

    # Ambil pattern pertama yang ditemukan
    action, pattern_name, level, height = results[0]
    print(f"[{pair}] Pattern: {pattern_name} → {action} | Breakout:{level} Height:{height}")

    # Konfirmasi 15M: RSI filter
    rsi_15m = calc_rsi(df_15m)
    if action == "BUY"  and rsi_15m > 75:
        print(f"[{pair}] RSI 15M overbought ({round(rsi_15m,1)}) → skip BUY")
        return None
    if action == "SELL" and rsi_15m < 25:
        print(f"[{pair}] RSI 15M oversold ({round(rsi_15m,1)}) → skip SELL")
        return None

    # Konfirmasi 15M: candle searah
    last_15m = df_15m.iloc[-2]
    if action == "BUY"  and last_15m["close"] < last_15m["open"]:
        print(f"[{pair}] Candle 15M bearish → belum konfirmasi BUY")
        return None
    if action == "SELL" and last_15m["close"] > last_15m["open"]:
        print(f"[{pair}] Candle 15M bullish → belum konfirmasi SELL")
        return None

    return action, pattern_name, level, height


def calc_sl_tp_pattern(df, action, breakout_level, pattern_height, pair):
    """SL/TP otomatis dari ukuran pattern (pattern height)."""
    cfg      = PIP_CONFIG.get(pair, {})
    pip_size = cfg.get("pip_size", 0.0001)
    price    = df["close"].iloc[-1]
    atr      = calc_atr(df)

    # SL: di belakang breakout level + buffer 1 ATR
    if action == "BUY":
        sl = round(breakout_level - atr, 5)
        tp = round(price + pattern_height, 5)
    else:
        sl = round(breakout_level + atr, 5)
        tp = round(price - pattern_height, 5)

    risk = abs(price - sl)
    reward = abs(tp - price)
    if risk <= 0:
        return None, price, price, price, 0, 0

    rr       = round(reward / risk, 1)
    sl_pip   = round(risk / pip_size, 1)
    tp_pip   = round(reward / pip_size, 1)

    # Minimum RR 1:1.5
    if rr < 1.5:
        print(f"[{pair}] RR terlalu rendah ({rr}) → skip")
        return None, price, price, price, 0, 0

    print(f"[{pair}] SL:{sl_pip}pip TP:{tp_pip}pip RR:1:{rr}")
    return action, round(price, 5), round(sl, 5), round(tp, 5), sl_pip, tp_pip


def analyze_pair(pair, symbol):
    print(f"\n{'='*10} [{pair}] {'='*10}")
    print(f"[{pair}] Menganalisis (Chart Pattern)...")

    df_1h  = get_data(symbol, "1h",  "60d")
    df_15m = get_data(symbol, "15m", "10d")

    if df_1h is None or len(df_1h) < 50:
        print(f"[{pair}] Data 1H tidak cukup, skip")
        return None
    if df_15m is None or len(df_15m) < 20:
        print(f"[{pair}] Data 15M tidak cukup, skip")
        return None

    # Scan pattern
    pattern_result = scan_patterns(df_1h, df_15m, pair)
    if pattern_result is None:
        print(f"[{pair}] Tidak ada pattern terdeteksi → NO TRADE")
        return None

    action, pattern_name, breakout_level, pattern_height = pattern_result

    # Hitung SL/TP dari ukuran pattern
    action, entry, sl, tp, sl_pip_val, tp_pip_val = calc_sl_tp_pattern(
        df_1h, action, breakout_level, pattern_height, pair
    )
    if action is None:
        print(f"[{pair}] Kalkulasi SL/TP gagal, skip")
        return None

    rr = round(tp_pip_val / sl_pip_val, 1) if sl_pip_val > 0 else 0
    print(f"[{pair}] ✅ SINYAL {action} | Pattern:{pattern_name} | Entry:{entry} SL:{sl} TP:{tp} RR:1:{rr}")

    return {
        "pair"         : pair,
        "action"       : action,
        "entry"        : entry,
        "optimal_entry": round(breakout_level, 5),
        "sl"           : sl,
        "tp"           : tp,
        "rr"           : rr,
        "structure"    : action,
        "structure_15m": pattern_name,
        "ema50_1h"     : round(breakout_level, 5),
        "st_level"     : round(breakout_level, 5),
        "sl_pip"       : sl_pip_val,
        "tp_pip"       : tp_pip_val,
        "signal_age"   : f"📐 {pattern_name}",
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
