# 🤖 XAUUSD Counter-Trend Engulfing Bot

Bot sinyal trading XAUUSD otomatis — berjalan 24 jam di Railway.

**Strategi:**
- Data real-time dari TradingView (200 candle, TF 1 menit)
- Deteksi trend via EMA 9 / EMA 21
- Sinyal saat Engulfing BERLAWANAN trend (reversal setup)
- Konfirmasi dari level Support & Resistance otomatis
- Notifikasi langsung ke Telegram

---

## 📁 Struktur File

```
xauusd-bot/
├── main.py           ← kode utama bot
├── requirements.txt  ← library Python
├── Procfile          ← perintah start untuk Railway
├── railway.json      ← konfigurasi Railway
└── .gitignore
```

---

## 🚀 Cara Deploy ke Railway (Step by Step)

### LANGKAH 1 — Upload ke GitHub

1. Buka **github.com** → login
2. Klik tombol **"+"** → **New repository**
3. Nama repo: `xauusd-bot` → klik **Create repository**
4. Upload semua file ini ke repo (klik **Add file → Upload files**)
5. Pastikan semua file terupload:
   - `main.py`
   - `requirements.txt`
   - `Procfile`
   - `railway.json`
   - `.gitignore`

### LANGKAH 2 — Buat Project di Railway

1. Buka **railway.app** → login (bisa pakai akun GitHub)
2. Klik **"New Project"**
3. Pilih **"Deploy from GitHub repo"**
4. Pilih repo `xauusd-bot` yang tadi dibuat
5. Railway akan otomatis detect dan mulai build

### LANGKAH 3 — Set Environment Variables (WAJIB)

Di Railway, buka project → tab **"Variables"** → tambahkan:

| Variable Name   | Value                        |
|-----------------|------------------------------|
| `BOT_TOKEN`     | Token bot Telegram kamu      |
| `CHAT_ID`       | Chat ID Telegram kamu        |
| `N_CANDLES`     | `200`                        |
| `CHECK_INTERVAL`| `60`                         |
| `EMA_FAST`      | `9`                          |
| `EMA_SLOW`      | `21`                         |
| `SR_NEAR_ZONE`  | `2.50`                       |

> ⚠️ BOT_TOKEN dan CHAT_ID adalah yang paling wajib diisi!
> Jangan pernah tulis token langsung di kode — gunakan Variables Railway.

### LANGKAH 4 — Pastikan pakai Worker (bukan Web)

1. Di Railway project → tab **"Settings"**
2. Pastikan start command adalah: `python main.py`
3. Atau Railway akan baca otomatis dari `Procfile`

### LANGKAH 5 — Deploy!

1. Klik **Deploy** (atau Railway deploy otomatis setelah Variables diset)
2. Buka tab **"Logs"** untuk pantau output bot
3. Cek Telegram — bot akan kirim pesan "ONLINE di Railway!" jika berhasil

---

## 🔑 Cara Dapat BOT_TOKEN dan CHAT_ID

**BOT_TOKEN:**
1. Buka Telegram → cari `@BotFather`
2. Ketik `/newbot`
3. Ikuti instruksi → copy token yang diberikan

**CHAT_ID:**
1. Buka Telegram → cari `@userinfobot`
2. Ketik `/start`
3. Bot akan balas dengan ID kamu — copy angkanya

---

## 📊 Cara Kerja Sinyal

```
Kondisi BUY:
  Trend  → DOWNTREND (EMA9 < EMA21)
  Pola   → Bullish Engulfing muncul
  Lokasi → Harga dekat level SUPPORT
  Hasil  → Potensi reversal naik → Sinyal BUY

Kondisi SELL:
  Trend  → UPTREND (EMA9 > EMA21)
  Pola   → Bearish Engulfing muncul
  Lokasi → Harga dekat level RESISTANCE
  Hasil  → Potensi reversal turun → Sinyal SELL
```

---

## 🔄 Update Kode

Cukup edit file di GitHub → Railway otomatis redeploy!

---

⚠️ **Disclaimer:** Bot ini hanya alat bantu analisis teknikal.
Selalu gunakan manajemen risiko. Trading mengandung risiko kerugian.
