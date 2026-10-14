# 🎵 Telegram Artist Music Downloader Bot

Bot Telegram berbasis Python yang dirancang untuk mencari, mengumpulkan, dan mengunduh seluruh katalog musik artis dari YouTube Music (mencakup bagian **Songs**, seluruh **Album**, serta seluruh **Singles & EP**) dan mengirimkannya langsung ke chat Telegram dalam format audio MP3 berkualitas tinggi.

---

## ✨ Fitur Utama

- 🔍 **Penyapuan Katalog Penuh:** Mengumpulkan lagu dari playlist resmi artis, semua album rilis, dan singles/EP tanpa batas arbitrer (bebas dari limit buatan 50 atau 100 lagu).
- 🧹 **Deduplikasi Cerdas:** Memastikan satu lagu tidak diunduh atau dikirim berulang kali dengan sistem deduplikasi berbasis `videoId`.
- 🛡️ **Keamanan Terjamin:** Token bot disimpan aman di file `.env` (environment variable `TELEGRAM_BOT_TOKEN`) dan tidak di-hardcode di source code.
- ⚡ **Antrean Non-Blocking:** Unduhan berjalan di background worker thread terpisah, menjaga bot tetap responsif melayani pengguna lain.
- 🔄 **Toleransi Kesalahan & Retry:** Mekanisme coba ulang otomatis untuk kegagalan jaringan sementara. Kegagalan satu lagu tidak membatalkan sisa antrean.
- 🛑 **Pembatalan Kooperatif Riil:** Tombol `❌ Batalkan` langsung menghentikan proses unduhan dan membersihkan file sementara seketika.
- 📦 **Pembersihan Otomatis:** File audio MP3 sementara diisolasi dalam direktori temp khusus per sesi dan langsung dibersihkan setelah dikirim atau saat terjadi error.
- 📊 **Progres & Laporan Akhir:** Status progres berkala yang aman dari rate-limit Telegram, serta ringkasan akhir lengkap dengan tombol coba lagi (*Retry*) untuk lagu yang gagal.

---

## 📁 Struktur Berkas

```text
├── app.py              # Program utama bot Telegram
├── test_bot.py         # Test suite otomatis (12 skenario pengujian)
├── requirements.txt    # Daftar dependensi Python terbaru
├── .env.example        # Contoh konfigurasi environment variable
└── README.md           # Dokumentasi lengkap proyek
```

---

## ⚙️ Prasyarat Sistem

1. **Python:** Versi `3.10` atau yang lebih baru.
2. **FFmpeg:** Dibutuhkan oleh `yt-dlp` untuk konversi audio ke format MP3.
3. **Akun Bot Telegram:** Token bot dari [@BotFather](https://t.me/BotFather).

---

## 🚀 Panduan Instalasi & Setup

### 1. Instalasi FFmpeg

- **Ubuntu / Debian:**
  ```bash
  sudo apt update && sudo apt install -y ffmpeg
  ```
- **macOS (via Homebrew):**
  ```bash
  brew install ffmpeg
  ```
- **Windows (PowerShell):**
  ```powershell
  winget install Gyan.FFmpeg
  ```
  *Atau unduh manual dari [ffmpeg.org](https://ffmpeg.org) dan masukkan path folder `bin` ke System PATH.*

Verifikasi instalasi FFmpeg dengan perintah:
```bash
ffmpeg -version
```

---

### 2. Pasang Dependensi Python

Pasang dependensi sesuai `requirements.txt`:
```bash
pip install -r requirements.txt
```

Atau perbarui ke versi terbaru langsung dari PyPI:
```bash
pip install -U pyTelegramBotAPI yt-dlp ytmusicapi python-dotenv
```

---

### 3. Konfigurasi Token Bot (`.env`)

Salin file `.env.example` menjadi `.env`:
```bash
cp .env.example .env
```

Buka file `.env` dan masukkan token bot Telegram Anda:
```env
TELEGRAM_BOT_TOKEN="123456789:ABCdefGHIjklMNOpqrsTUVwxyz"
AUDIO_BITRATE="192"
MAX_RETRIES="2"
LOG_LEVEL="INFO"
```

> ⚠️ **Catatan Keamanan:** Jangan pernah mempublikasikan file `.env` atau token bot ke GitHub/repositori publik.

---

## 🎮 Cara Menjalankan Bot

Jalankan program utama:
```bash
python3 app.py
```

Jika berhasil, Anda akan melihat log:
```text
[INFO] [MusicBot] 🤖 Bot Telegram Musik Artis Aktif...
[INFO] [MusicBot] Bitrate MP3: 192 kbps | Max Retries: 2
[INFO] [MusicBot] Menunggu pesan pengguna...
```

---

## 🧪 Menjalankan Pengujian Otomatis

Untuk memastikan seluruh fungsi, penanganan error, pembersihan file, dan pembatalan berjalan dengan benar:
```bash
python3 test_bot.py
```

Semua 12 unit test akan dieksekusi menggunakan modul mock tanpa memerlukan token bot Telegram aktif.

---

## 📱 Panduan Penggunaan di Telegram

1. Buka chat dengan bot Anda di Telegram dan ketik `/start`.
2. Ketik **nama artis** yang ingin diunduh (misalnya: `Noah`, `Peterpan`, atau `Sheila On 7`).
3. Bot akan menyapu YouTube Music dan menampilkan ringkasan katalog yang ditemukan:
   - Jumlah lagu di playlist utama.
   - Jumlah album rilis yang diproses.
   - Jumlah singles & EP yang diproses.
   - Total lagu unik yang siap diunduh.
4. Klik tombol **`✅ Ya, Download`** untuk memulai proses pengunduhan.
5. Bot akan mengirimkan lagu satu per satu langsung ke chat Telegram Anda lengkap dengan nama penyanyi dan judul lagu.
6. Untuk menghentikan proses kapan saja, klik tombol keyboard **`❌ Batalkan`**.

---

## 🛡️ Catatan Teknis & Batasan

- **Batas Ukuran File Telegram:** Bot API memiliki batas upload file maksimal 50 MB. Lagu/audio yang durasinya sangat panjang (seperti rekaman konser penuh 3 jam) akan dilewati secara aman dan dicatat di laporan akhir.
- **Geoblocking:** Beberapa konten YouTube Music mungkin dibatasi secara regional oleh pemilik hak cipta. Lagu yang dibatasi tidak akan merusak antrean dan dapat dicoba lagi melalui tombol *Retry*.
