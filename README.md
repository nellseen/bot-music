# 🎵 Telegram Artist Music Downloader Bot (High-Throughput & Archive Edition)

Bot Telegram Python tingkat lanjut yang dirancang untuk menyapu seluruh katalog lagu artis dari YouTube Music (**Songs**, semua **Album**, serta semua **Singles & EP**), mengunduh dengan arsitektur pipeline konkuren berkecepatan tinggi, mengarsipkan file ke channel Telegram arsip, serta menyimpannya dalam database cache persisten SQLite.

---

## ✨ Fitur Unggulan Terbaru

1. ⚡ **Producer-Consumer Pipeline (Download & Upload Paralel):**
   - Proses download dan upload berjalan konkuren menggunakan worker terpisah (`DOWNLOAD_WORKERS` dan `UPLOAD_WORKERS`).
   - Tidak perlu menunggu satu lagu selesai diunggah sebelum mulai mengunduh lagu berikutnya.
   - Menggunakan bounded queue (`MAX_QUEUE_SIZE`) untuk mencegah kehabisan memori atau disk.

2. 🏛️ **Pengarsipan Wajib Channel (`ARCHIVE_CHANNEL_ID`):**
   - Setiap lagu yang berhasil diproses wajib dikirim ke channel arsip terlebih dahulu.
   - Bot mengekstrak Telegram `file_id` dari pesan arsip, lalu meneruskannya ke pengguna secara instan tanpa mengunggah ulang file fisik.
   - Validasi ketat terhadap format channel (mencegah kesalahan placeholder `-00`).

3. 💾 **Cache Persisten SQLite (Pengiriman Ulang Instan):**
   - Metadata dan `file_id` Telegram dicatat ke dalam database SQLite (`music_cache.db`).
   - Permintaan lagu yang sudah pernah diarsipkan akan dikirimkan **secara instan (0 detik download/upload)** menggunakan `file_id`.

4. 📋 **Multi-Job Queue (Antrean Multi-Artis Bersamaan):**
   - Pengguna dapat meminta artis baru (misalnya `Peterpan`) saat proses unduh artis sebelumnya (`Noah`) masih berlangsung.
   - Job baru langsung masuk ke antrean berikutnya tanpa mereset, menimpa, atau membatalkan proses aktif.
   - Dilengkapi menu `/queue` atau `📋 Status Antrean` untuk memantau progres antrean.

5. 🎛️ **Konfigurasi Audio Fleksibel via ENV:**
   - `AUDIO_QUALITY=best`: Memilih stream audio murni terbaik dari YouTube Music (tanpa stream video).
   - `AUDIO_FORMAT=mp3`: Format audio target.
   - `AUDIO_BITRATE=0`: Nilai `0` atau `best` menjaga kualitas bitrate sumber asli tanpa re-encode lossy yang sia-sia.

6. 🛡️ **Koneksi Polling & Timeout Tangguh:**
   - Konfigurasi timeout tingkat library: `BOT_API_TIMEOUT`, `POLLING_TIMEOUT`, dan `LONG_POLLING_TIMEOUT`.
   - Pembersihan webhook otomatis saat startup untuk mencegah bentrok polling.
   - Deteksi ramah dan penanganan error `409 Conflict`.

7. 🛑 **Pembatalan Kooperatif & Pembersihan Otomatis:**
   - Tombol `❌ Batalkan` menghentikan proses unduhan secara aman dan membersihkan file temporary seketika tanpa mengganggu job pengguna lain.

---

## 📁 Struktur Berkas

```text
├── app.py              # Program utama bot Telegram (High-Throughput Pipeline)
├── test_bot.py         # Test suite otomatis (10 pengujian runtime wajib)
├── music_cache.db      # Database SQLite persistent cache (dibuat otomatis)
├── requirements.txt    # Daftar dependensi Python terbaru (PyPI)
├── .env.example        # Template konfigurasi environment variable
└── README.md           # Dokumentasi teknis proyek
```

---

## ⚙️ Konfigurasi Environment Variable (`.env`)

Salin `.env.example` ke `.env` lalu sesuaikan nilainya:

```env
# Token Bot Telegram dari @BotFather (WAJIB)
BOT_TOKEN="your_telegram_bot_token_here"

# ID Channel Arsip Telegram Wajib (Gunakan angka -100xxxxxxxxxx atau @username)
# JANGAN gunakan "-00" karena itu adalah placeholder contoh.
ARCHIVE_CHANNEL_ID="-1001234567890"

# Kualitas dan Format Audio
AUDIO_QUALITY="best"
AUDIO_FORMAT="mp3"
AUDIO_BITRATE="0"

# Konkurensi & Worker Pipeline
DOWNLOAD_WORKERS="2"
UPLOAD_WORKERS="2"
MAX_QUEUE_SIZE="5"

# Timeout Jaringan & Polling Telegram
BOT_API_TIMEOUT="60"
POLLING_TIMEOUT="30"
LONG_POLLING_TIMEOUT="30"

# Batas Percobaan Ulang (Retry)
MAX_RETRIES="3"
LOG_LEVEL="INFO"
```

---

## 🚀 Panduan Menjalankan

### 1. Pasang Dependensi
```bash
pip install -r requirements.txt
```

Pastikan **FFmpeg** sudah terpasang di sistem (`ffmpeg -version`).

### 2. Jalankan Bot
```bash
python3 app.py
```

### 3. Jalankan Pengujian Runtime
```bash
python3 test_bot.py
```

---

## 📱 Contoh Interaksi Telegram

1. Kirim `/start` ke bot di Telegram.
2. Ketik nama artis pertama: `Noah`.
3. Klik tombol `✅ Ya, Download (166 Lagu)`.
4. Saat unduhan Noah masih berjalan, ketik nama artis kedua: `Peterpan`.
5. Klik tombol konfirmasi untuk Peterpan: Bot akan memberi tahu:
   `⏳ Artis Dimasukkan ke Antrean (Posisi #2): Peterpan`
6. Tekan tombol `📋 Status Antrean` kapan saja untuk melihat status aktif dan antrean menunggu.
