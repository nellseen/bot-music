# 🎵 Telegram Artist & Single Track Music Downloader Bot

Bot Telegram Python tingkat lanjut yang dirancang untuk mengunduh lagu individual maupun seluruh katalog artis (**Songs**, semua **Album**, serta semua **Singles & EP**) dari YouTube Music dengan arsitektur konkuren berkecepatan tinggi, sistem pengarsipan channel wajib, cache persisten SQLite, dan fitur pemulihan otomatis (*Auto-Resume*).

---

## ✨ Fitur Unggulan

1. 🔍 **Pencarian Single Lagu Langsung (`/song` & `/search`):**
   - Cari lagu spesifik berdasarkan judul atau judul + nama artis.
   - Contoh: `/song Separuh Aku` atau `/song Separuh Aku Noah` atau `/search Hampa Ari Lasso`.
   - Menampilkan 5–8 hasil relevan lengkap dengan judul, artis, album, dan durasi.
   - Pilihan lagu menggunakan tombol inline keyboard (`1️⃣`, `2️⃣`, `3️⃣`, dst.) dengan validasi keamanan sesi privat.

2. ⚡ **Producer-Consumer Pipeline (Download & Upload Konkuren):**
   - Download worker dan upload worker berjalan secara simultan menggunakan buffer antrean berbatas (`MAX_QUEUE_SIZE`).
   - Tidak perlu menunggu satu lagu selesai diunggah sebelum mulai mengunduh lagu berikutnya.

3. 🏛️ **Pengarsipan Wajib Channel (`ARCHIVE_CHANNEL_ID`):**
   - Setiap lagu wajib berhasil dikirim ke channel arsip Telegram terlebih dahulu.
   - Telegram `file_id` disimpan ke database SQLite, lalu diteruskan ke pengguna secara instan tanpa mengunggah ulang file fisik.
   - Validasi ketat format channel (menolak placeholder `-00`).

4. 💾 **Smart Cache Persisten SQLite (`music_cache.db`):**
   - Lagu yang sudah pernah diarsipkan akan dikirim ulang secara instan (**0 detik latency**) menggunakan `file_id` jika diminta kembali.

5. 🔄 **Auto-Resume Setelah Restart / Crash:**
   - Semua job disimpan ke tabel `persistent_jobs` di database SQLite.
   - Jika bot mati atau server dimulai ulang saat proses unduh/upload sedang berjalan, bot otomatis melanjutkan pekerjaan tertunda saat menyala kembali.

6. 📋 **Multi-Job Unified Queue:**
   - Antrean terpadu untuk katalog artis dan single lagu.
   - Pengguna dapat menambahkan lagu/artis baru saat unduhan lain sedang berjalan tanpa saling menimpa atau mereset antrean.

7. 🩺 **Monitoring & Perintah Admin:**
   - `/queue` — Menampilkan daftar antrean dan pekerjaan aktif.
   - `/status` — Memeriksa status pekerjaan pengguna.
   - `/stats` — Statistik total lagu di cache, pekerjaan selesai, dan uptime bot.
   - `/health` — Diagnostik kesehatan database, worker threads, dan status channel arsip (dibatasi `ADMIN_USER_IDS`).
   - `/cancel` — Membatalkan pekerjaan aktif atau antrean tertentu.

8. 🎛️ **Konfigurasi Audio Fleksibel via ENV:**
   - `AUDIO_QUALITY=best`: Stream audio murni terbaik (`bestaudio/best`).
   - `AUDIO_FORMAT=mp3`: Format audio target.
   - `AUDIO_BITRATE=0`: Menjaga kualitas sumber asli tanpa re-encode lossy yang sia-sia.

---

## 📁 Struktur Berkas

```text
├── app.py              # Program utama bot Telegram (Full-Featured Pipeline)
├── test_bot.py         # Test suite otomatis (14 pengujian runtime wajib)
├── music_cache.db      # Database SQLite persistent cache & auto-resume
├── requirements.txt    # Daftar dependensi Python terbaru
├── .env.example        # Template konfigurasi environment variable
└── README.md           # Dokumentasi teknis lengkap proyek
```

---

## ⚙️ Konfigurasi Environment Variable (`.env`)

Salin `.env.example` ke `.env` lalu sesuaikan isinya:

```env
# Token Bot Telegram dari @BotFather (WAJIB)
BOT_TOKEN="your_telegram_bot_token_here"

# ID Channel Arsip Telegram Wajib (Gunakan angka -100xxxxxxxxxx atau @username)
# PERHATIAN: Ganti placeholder "-00" dengan ID channel Telegram Anda.
ARCHIVE_CHANNEL_ID="-1001234567890"

# Daftar ID Telegram User Admin dipisahkan tanda koma (opsional)
ADMIN_USER_IDS="123456789,987654321"

# Kualitas dan Format Audio
AUDIO_QUALITY="best"
AUDIO_FORMAT="mp3"
AUDIO_BITRATE="0"

# Konkurensi & Worker Pipeline
DOWNLOAD_WORKERS="2"
UPLOAD_WORKERS="2"
MAX_QUEUE_SIZE="5"
MAX_USER_CONCURRENT_JOBS="3"

# Timeout Jaringan & Polling Telegram
BOT_API_TIMEOUT="60"
POLLING_TIMEOUT="30"
LONG_POLLING_TIMEOUT="30"
MAX_RETRIES="3"
LOG_LEVEL="INFO"
```

---

## 🏛️ Petunjuk Setup Channel Arsip Telegram

1. Buat **Channel Baru** di Telegram (Bisa Privat atau Publik).
2. Masukkan bot Anda ke dalam channel tersebut sebagai **Administrator** dengan izin **Kirim Pesan / Kirim Media**.
3. Dapatkan Channel ID:
   - Jika channel publik: gunakan `@username_channel` Anda.
   - Jika channel privat: teruskan pesan dari channel ke bot pembaca ID (misal `@userinfobot`) untuk mendapatkan ID diawali `-100...` (contoh: `-1001928374650`).
4. Masukkan ID tersebut ke variabel `ARCHIVE_CHANNEL_ID` di file `.env`.

---

## 📱 Daftar Perintah Bot Telegram

| Perintah | Deskripsi |
|---|---|
| `/start` | Memulai bot dan menampilkan menu tombol keyboard. |
| `/song <judul>` | Mencari lagu tunggal dan memilih dari hasil pencarian (misal `/song Separuh Aku Noah`). |
| `/search <judul>` | Sama seperti `/song`, alternatif perintah pencarian lagu tunggal. |
| `/queue` atau `/status` | Melihat status antrean aktif dan pekerjaan yang sedang menunggu. |
| `/cancel [job_id]` | Membatalkan pekerjaan aktif atau antrean milik pengguna. |
| `/stats` | Menampilkan statistik total cache lagu, pekerjaan selesai, dan uptime bot. |
| `/health` | Memeriksa status kesehatan internal bot (khusus Admin). |
| *Nama Artis Langsung* | Mengirim teks nama artis (misal: `Noah`) untuk menyapu dan mengunduh seluruh katalog album & single artis. |

---

## 🧪 Hasil Pengujian Runtime (14 Test Cases)

Jalankan test suite kapan saja:
```bash
python3 test_bot.py
```

Seluruh 14 pengujian lulus dengan status **OK**:
```text
test_01_syntax_imports_and_dependencies ................................. ok
test_02_search_song_title_only (/song Separuh Aku) ..................... ok
test_03_search_song_title_and_artist (/song Separuh Aku Noah) .......... ok
test_04_search_empty_and_api_error_handling ............................ ok
test_05_inline_keyboard_pick_song_callback ............................. ok
test_06_unauthorized_user_callback_protection .......................... ok
test_07_enqueue_single_while_artist_running (antrean terpadu) .......... ok
test_08_multiple_jobs_no_overwriting ................................... ok
test_09_mandatory_channel_archive (arsip wajib ke channel) .............. ok
test_10_cache_instant_resend (pengiriman instan dari cache file_id) ..... ok
test_11_retry_mechanism (exponential backoff retry) .................... ok
test_12_auto_resume_after_bot_restart (pemulihan otomatis pasca restart) ok
test_13_channel_id_env_validation (deteksi placeholder -00) ............ ok
test_14_polling_timeouts_applied (verifikasi timeout telebot.apihelper) . ok

----------------------------------------------------------------------
Ran 14 tests in 5.760s

OK
```

---

## 🚀 Menjalankan Bot

```bash
python3 app.py
```
