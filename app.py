#!/usr/bin/env python3
"""
Telegram Artist Music Downloader Bot - High-Throughput & Archive Edition
========================================================================
Fitur Utama:
1. Producer-Consumer Pipeline: Download dan upload berjalan secara paralel (konkuren)
   sehingga tidak perlu menunggu satu lagu selesai diunggah sebelum mengunduh lagu berikutnya.
2. Pengarsipan Wajib Channel: Setiap lagu wajib berhasil dikirim ke ARCHIVE_CHANNEL_ID
   terlebih dahulu, diambil 'file_id'-nya, lalu diteruskan ke pengguna secara instan.
3. Cache Persisten SQLite: Menyimpan 'file_id' Telegram berdasarkan videoId. Permintaan
   lagu yang sama dikirimkan langsung dari cache tanpa download ulang (0 detik latency).
4. Multi-Job Queue Asli: Pengguna dapat menambahkan artis baru (misal Peterpan saat Noah
   sedang berjalan) ke antrean tanpa mereset atau menghentikan proses aktif.
5. Konfigurasi Audio via ENV: AUDIO_QUALITY, AUDIO_FORMAT, dan AUDIO_BITRATE tanpa re-encode mubazir.
6. Toleransi Jaringan & Polling Tangguh: Konfigurasi timeout BOT_API_TIMEOUT, POLLING_TIMEOUT,
   pembersihan webhook otomatis, dan penanganan ramah 409 Conflict.
7. Deduplikasi, Penyapuan Katalog Lengkap YouTube Music, dan Pembatalan Kooperatif.
"""

import html
import logging
import os
import queue
import re
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

# Muat environment variable dari .env jika tersedia
from dotenv import load_dotenv
load_dotenv()

import telebot
from telebot import types
import yt_dlp
from ytmusicapi import YTMusic
from ytmusicapi.navigation import nav

# ==============================================================================
# 1. KONFIGURASI DAN LOGGING
# ==============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MusicBot")

# Token Telegram (mendukung BOT_TOKEN atau TELEGRAM_BOT_TOKEN)
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip() or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

if not BOT_TOKEN:
    logger.critical(
        "FATAL: Token Telegram belum disetel! Harap setel 'BOT_TOKEN' atau 'TELEGRAM_BOT_TOKEN' di file .env"
    )
    if "pytest" not in sys.modules and "--dry-run" not in sys.argv:
        print("\n" + "=" * 70, file=sys.stderr)
        print("❌ KESALAHAN: BOT_TOKEN / TELEGRAM_BOT_TOKEN TIDAK DITEMUKAN!", file=sys.stderr)
        print("Harap setel token bot di file .env Anda.", file=sys.stderr)
        print("Contoh: BOT_TOKEN=\"123456789:ABCdefGHIjklMNOpqrsTUVwxyz\"", file=sys.stderr)
        print("=" * 70 + "\n", file=sys.stderr)
        sys.exit(1)

# Channel Pengarsipan Wajib
RAW_ARCHIVE_CHANNEL_ID = os.getenv("ARCHIVE_CHANNEL_ID", "").strip()

def validate_channel_id(ch_str: str) -> Tuple[Optional[Any], Optional[str]]:
    """Memvalidasi konfigurasi ARCHIVE_CHANNEL_ID."""
    if not ch_str:
        return None, "ARCHIVE_CHANNEL_ID belum diatur di .env."
    if ch_str in ["-00", "00", "0", "-0"]:
        return None, (
            "ARCHIVE_CHANNEL_ID bernilai placeholder '-00'. "
            "Harap ganti dengan ID numerik channel asli (contoh: -1001234567890) "
            "atau username channel publik (@channelname) di file .env."
        )
    if ch_str.startswith("@"):
        return ch_str, None
    try:
        val = int(ch_str)
        return val, None
    except ValueError:
        return None, f"Format channel '{ch_str}' tidak valid. Gunakan angka -100... atau @username."

ARCHIVE_CHANNEL_TARGET, ARCHIVE_CHANNEL_WARNING = validate_channel_id(RAW_ARCHIVE_CHANNEL_ID)
if ARCHIVE_CHANNEL_WARNING:
    logger.warning("PERINGATAN ARSIP: %s", ARCHIVE_CHANNEL_WARNING)

# Kualitas & Format Audio
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "best").strip().lower()
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3").strip().lower()
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "0").strip()

# Konfigurasi Worker & Antrean
DOWNLOAD_WORKERS = max(1, int(os.getenv("DOWNLOAD_WORKERS", "2")))
UPLOAD_WORKERS = max(1, int(os.getenv("UPLOAD_WORKERS", "2")))
MAX_QUEUE_SIZE = max(1, int(os.getenv("MAX_QUEUE_SIZE", "5")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "3")))

# Konfigurasi Timeout Jaringan & Polling Telegram
BOT_API_TIMEOUT = int(os.getenv("BOT_API_TIMEOUT", "60"))
POLLING_TIMEOUT = int(os.getenv("POLLING_TIMEOUT", "30"))
LONG_POLLING_TIMEOUT = int(os.getenv("LONG_POLLING_TIMEOUT", "30"))

# Konfigurasi batas file dan jeda throttled
MAX_TELEGRAM_FILE_SIZE = 49.5 * 1024 * 1024  # 49.5 MB
THROTTLE_PROGRESS_SECONDS = 5.0

# Inisialisasi API Helper Timeout
telebot.apihelper.CONNECT_TIMEOUT = BOT_API_TIMEOUT
telebot.apihelper.READ_TIMEOUT = BOT_API_TIMEOUT
telebot.apihelper.LONG_POLLING_TIMEOUT = LONG_POLLING_TIMEOUT

# Inisialisasi Klien Bot dan YouTube Music
bot = telebot.TeleBot(BOT_TOKEN or "123456789:AAH_DUMMY_TOKEN_FOR_INITIALIZATION")
ytmusic = YTMusic()

# ==============================================================================
# 2. PERSISTENT CACHE DATABASE (SQLITE)
# ==============================================================================

DB_PATH = os.getenv("DB_PATH", "music_cache.db")
DB_LOCK = threading.Lock()

def init_db() -> None:
    """Inisialisasi tabel SQLite untuk cache audio dan riwayat job."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audio_cache (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    artist TEXT,
                    file_id TEXT NOT NULL,
                    archive_msg_id INTEGER,
                    file_size INTEGER,
                    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_history (
                    job_id TEXT PRIMARY KEY,
                    chat_id INTEGER,
                    artist_name TEXT,
                    total_tracks INTEGER,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP
                );
            """)
            conn.commit()
        finally:
            conn.close()

def get_cached_track(video_id: str) -> Optional[Dict[str, Any]]:
    """Mengambil record file_id audio dari cache jika sudah pernah diarsipkan."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT video_id, title, artist, file_id, archive_msg_id FROM audio_cache WHERE video_id = ?", (video_id,))
            row = cur.fetchone()
            if row:
                return {
                    "video_id": row[0],
                    "title": row[1],
                    "artist": row[2],
                    "file_id": row[3],
                    "archive_msg_id": row[4],
                }
            return None
        finally:
            conn.close()

def save_cached_track(video_id: str, title: str, artist: str, file_id: str, archive_msg_id: Optional[Any], file_size: Any) -> None:
    """Menyimpan record lagu yang berhasil diarsipkan ke database."""
    safe_archive_id = None
    if archive_msg_id is not None:
        try:
            safe_archive_id = int(archive_msg_id)
        except (ValueError, TypeError):
            safe_archive_id = None

    safe_fsize = 0
    try:
        safe_fsize = int(file_size)
    except (ValueError, TypeError):
        safe_fsize = 0

    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO audio_cache (video_id, title, artist, file_id, archive_msg_id, file_size)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (str(video_id), str(title), str(artist), str(file_id), safe_archive_id, safe_fsize))
            conn.commit()
        finally:
            conn.close()

init_db()

# ==============================================================================
# 3. STRUKTUR DATA JOB & MANAGER ANTREAN THREAD-SAFE
# ==============================================================================

class DownloadJob:
    """Representasi satu tugas pengunduhan katalog artis."""

    def __init__(
        self,
        job_id: str,
        chat_id: int,
        user_id: int,
        artist_name: str,
        query: str,
        tracks: List[Dict[str, Any]],
        catalog_summary: Dict[str, Any],
    ):
        self.job_id: str = job_id
        self.chat_id: int = chat_id
        self.user_id: int = user_id
        self.artist_name: str = artist_name
        self.query: str = query
        self.tracks: List[Dict[str, Any]] = tracks
        self.catalog_summary: Dict[str, Any] = catalog_summary
        self.total_tracks: int = len(tracks)

        self.status: str = "queued"  # queued, downloading, uploading, completed, cancelled, failed
        self.created_at: float = time.time()
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.progress_msg_id: Optional[int] = None

        self.cancel_event: threading.Event = threading.Event()
        self.cached_count: int = 0
        self.downloaded_count: int = 0
        self.archived_count: int = 0
        self.delivered_count: int = 0
        self.failed_count: int = 0
        self.skipped_count: int = 0
        self.failed_tracks: List[Dict[str, Any]] = []

class JobQueueManager:
    """Pengelola antrean multi-job yang thread-safe untuk mendukung permintaan beruntun."""

    def __init__(self):
        self.lock = threading.RLock()
        self.active_job: Optional[DownloadJob] = None
        self.pending_queue: List[DownloadJob] = []
        self.worker_thread: Optional[threading.Thread] = None

    def enqueue(self, job: DownloadJob) -> int:
        """Menambahkan job baru ke antrean. Mengembalikan posisi dalam antrean (1 = aktif)."""
        with self.lock:
            self.pending_queue.append(job)
            position = len(self.pending_queue) + (1 if self.active_job else 0)
            logger.info("Job %s ('%s') dimasukkan ke antrean. Posisi: %d", job.job_id, job.artist_name, position)

            if self.active_job is None:
                self._start_next_job_unlocked()

            return position

    def _start_next_job_unlocked(self) -> None:
        """Mengambil job berikutnya dari antrean dan memulai worker thread."""
        if not self.pending_queue:
            self.active_job = None
            return

        self.active_job = self.pending_queue.pop(0)
        self.active_job.status = "downloading"
        self.active_job.started_at = time.time()

        self.worker_thread = threading.Thread(
            target=self._run_job_pipeline,
            args=(self.active_job,),
            name=f"JobWorker-{self.active_job.job_id[:8]}",
            daemon=True
        )
        self.worker_thread.start()

    def _run_job_pipeline(self, job: DownloadJob) -> None:
        """Menjalankan pipeline eksekusi untuk job aktif lalu melanjutkan ke job berikutnya."""
        try:
            execute_high_throughput_pipeline(job)
        except Exception as e:
            logger.exception("Kesalahan fatal pada pipeline job %s: %s", job.job_id, e)
            job.status = "failed"
        finally:
            with self.lock:
                self.active_job = None
                self._start_next_job_unlocked()

    def cancel_job_for_chat(self, chat_id: int) -> Tuple[bool, str]:
        """Membatalkan job yang aktif atau menghapus job yang masih mengantre untuk chat_id tertentu."""
        with self.lock:
            # 1. Cek jika job aktif milik chat_id ini
            if self.active_job and self.active_job.chat_id == chat_id:
                self.active_job.cancel_event.set()
                self.active_job.status = "cancelled"
                return True, f"Proses unduhan aktif untuk '{self.active_job.artist_name}' sedang dihentikan."

            # 2. Cek jika ada job di antrean pending milik chat_id ini
            for idx, job in enumerate(self.pending_queue):
                if job.chat_id == chat_id:
                    removed = self.pending_queue.pop(idx)
                    removed.cancel_event.set()
                    removed.status = "cancelled"
                    return True, f"Antrean untuk '{removed.artist_name}' berhasil dibatalkan."

            return False, "Tidak ada proses aktif atau antrean untuk chat ini."

    def get_status_summary(self) -> str:
        """Menghasilkan teks ringkasan status antrean saat ini."""
        with self.lock:
            lines = ["📋 <b>Status Antrean Unduhan Saat Ini:</b>\n"]
            if self.active_job:
                p = self.active_job
                lines.append(
                    f"▶️ <b>Sedang Diproses:</b> <i>{html.escape(p.artist_name)}</i>\n"
                    f"   Status: <b>{p.status.upper()}</b>\n"
                    f"   Progres: {p.delivered_count}/{p.total_tracks} terkirim "
                    f"(⚡ {p.cached_count} cache, 📥 {p.downloaded_count} unduh, ❌ {p.failed_count} gagal)"
                )
            else:
                lines.append("<i>Tidak ada tugas yang sedang aktif diproses.</i>")

            if self.pending_queue:
                lines.append(f"\n⏳ <b>Menunggu di Antrean ({len(self.pending_queue)} artis):</b>")
                for i, q in enumerate(self.pending_queue, 1):
                    lines.append(f"{i}. <b>{html.escape(q.artist_name)}</b> ({q.total_tracks} lagu)")
            else:
                lines.append("\n<i>Antrean berikutnya kosong.</i>")

            return "\n".join(lines)

JOB_MANAGER = JobQueueManager()
PENDING_CONFIRMATION: Dict[int, DownloadJob] = {}  # chat_id -> Job yang menunggu tombol konfirmasi
CONFIRMATION_LOCK = threading.Lock()

# ==============================================================================
# 4. MESIN PENCARIAN & AUDIT KATALOG ARTIS YOUTUBE MUSIC
# ==============================================================================

def _extract_all_items_by_key(data: Any, key: str) -> List[Any]:
    results: List[Any] = []
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                results.append(v)
            results.extend(_extract_all_items_by_key(v, key))
    elif isinstance(data, list):
        for item in data:
            results.extend(_extract_all_items_by_key(item, key))
    return results

def get_artist_releases_robust(
    yt: YTMusic,
    artist_id: str,
    section_name: str,
    section_data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Mengambil seluruh rilis (Albums atau Singles/EPs) artis secara lengkap."""
    releases: List[Dict[str, Any]] = []
    seen_browse_ids: Set[str] = set()

    for item in section_data.get("results", []):
        bid = item.get("browseId")
        if bid and bid not in seen_browse_ids and (bid.startswith("MPRE") or "title" in item):
            seen_browse_ids.add(bid)
            releases.append(item)

    params = section_data.get("params")
    if params:
        try:
            full_list = yt.get_artist_albums(artist_id, params, limit=None)
            for item in full_list:
                bid = item.get("browseId")
                if bid and bid not in seen_browse_ids:
                    seen_browse_ids.add(bid)
                    releases.append(item)
        except Exception as e:
            logger.debug("get_artist_albums standar gagal (%s) untuk %s, beralih ke parser browse langsung...", e, section_name)
            try:
                body = {"browseId": artist_id, "params": params}
                resp = yt._send_request("browse", body)
                two_rows = _extract_all_items_by_key(resp, "musicTwoRowItemRenderer")
                for it in two_rows:
                    bid = nav(it, ["navigationEndpoint", "browseEndpoint", "browseId"], True)
                    page_type = nav(
                        it,
                        ["navigationEndpoint", "browseEndpoint", "browseEndpointContextSupportedConfigs", "browseEndpointContextMusicConfig", "pageType"],
                        True
                    )
                    if bid and (bid.startswith("MPRE") or page_type == "MUSIC_PAGE_TYPE_ALBUM"):
                        if bid not in seen_browse_ids:
                            title = nav(it, ["title", "runs", 0, "text"], True) or "Tanpa Judul"
                            seen_browse_ids.add(bid)
                            releases.append({"title": title, "browseId": bid})
            except Exception as e2:
                logger.warning("Gagal mengambil rilis tambahan untuk %s: %s", section_name, e2)

    return releases

def crawl_artist_catalog(yt: YTMusic, query: str) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    """Menyapu seluruh pustaka lagu artis (Songs, Albums, Singles & EPs) dengan deduplikasi."""
    seen_ids: Set[str] = set()
    tracks_to_download: List[Dict[str, Any]] = []
    artist_name = query.strip()
    summary = {
        "is_official_artist": False,
        "is_fallback": False,
        "songs_count": 0,
        "albums_count": 0,
        "singles_count": 0,
        "unique_total": 0,
    }

    artist_results = yt.search(query, filter="artists", limit=5)
    selected_artist = None

    if artist_results:
        for art in artist_results:
            cand_name = art.get("artist") or art.get("name", "")
            if cand_name.strip().lower() == query.strip().lower():
                selected_artist = art
                break
        if not selected_artist:
            selected_artist = artist_results[0]

    if selected_artist and selected_artist.get("browseId"):
        artist_id = selected_artist["browseId"]
        artist_name = selected_artist.get("artist") or selected_artist.get("name") or query
        summary["is_official_artist"] = True
        logger.info("Mengumpulkan katalog resmi untuk: '%s' (ID: %s)", artist_name, artist_id)

        try:
            artist_data = yt.get_artist(artist_id)
        except Exception as e:
            logger.error("Gagal get_artist(%s): %s", artist_id, e)
            artist_data = {}

        # 1. Bagian Songs
        songs_section = artist_data.get("songs", {})
        for track in songs_section.get("results", []):
            vid = track.get("videoId")
            title = track.get("title")
            if vid and title and vid not in seen_ids:
                seen_ids.add(vid)
                tracks_to_download.append({"title": title, "videoId": vid, "source": "Songs"})
                summary["songs_count"] += 1

        songs_browse_id = songs_section.get("browseId")
        if songs_browse_id:
            try:
                playlist_data = yt.get_playlist(songs_browse_id, limit=None)
                for track in playlist_data.get("tracks", []):
                    vid = track.get("videoId")
                    title = track.get("title")
                    if vid and title and vid not in seen_ids:
                        seen_ids.add(vid)
                        tracks_to_download.append({"title": title, "videoId": vid, "source": "Songs Playlist"})
                        summary["songs_count"] += 1
            except Exception as e:
                logger.warning("Error playlist lagu artis (%s): %s", songs_browse_id, e)

        # 2. Bagian Albums
        albums_section = artist_data.get("albums", {})
        albums_list = get_artist_releases_robust(yt, artist_id, "albums", albums_section)
        summary["albums_count"] = len(albums_list)
        for album in albums_list:
            alb_id = album.get("browseId")
            alb_title = album.get("title", "Album")
            if not alb_id:
                continue
            try:
                album_detail = yt.get_album(alb_id)
                for track in album_detail.get("tracks", []):
                    vid = track.get("videoId")
                    title = track.get("title")
                    if vid and title and vid not in seen_ids:
                        seen_ids.add(vid)
                        tracks_to_download.append({
                            "title": title,
                            "videoId": vid,
                            "album": alb_title,
                            "source": f"Album: {alb_title}"
                        })
            except Exception as e:
                logger.warning("Gagal album '%s': %s", alb_title, e)

        # 3. Bagian Singles & EPs
        singles_section = artist_data.get("singles", {})
        singles_list = get_artist_releases_robust(yt, artist_id, "singles", singles_section)
        summary["singles_count"] = len(singles_list)
        for single in singles_list:
            sgl_id = single.get("browseId")
            sgl_title = single.get("title", "Single")
            if not sgl_id:
                continue
            try:
                single_detail = yt.get_album(sgl_id)
                for track in single_detail.get("tracks", []):
                    vid = track.get("videoId")
                    title = track.get("title")
                    if vid and title and vid not in seen_ids:
                        seen_ids.add(vid)
                        tracks_to_download.append({
                            "title": title,
                            "videoId": vid,
                            "album": sgl_title,
                            "source": f"Single/EP: {sgl_title}"
                        })
            except Exception as e:
                logger.warning("Gagal single '%s': %s", sgl_title, e)

    # 4. Pencarian Cadangan Umum jika tidak ada profil artis resmi
    if not tracks_to_download:
        summary["is_fallback"] = True
        logger.info("Katalog artis resmi kosong untuk '%s'. Menjalankan pencarian cadangan...", query)
        try:
            general_results = yt.search(query, filter="songs", limit=100)
            if not general_results:
                general_results = yt.search(query, limit=100)

            for item in general_results:
                vid = item.get("videoId")
                title = item.get("title")
                if vid and title and vid not in seen_ids:
                    seen_ids.add(vid)
                    artists = item.get("artists", [])
                    item_artist = artists[0].get("name", query) if artists else query
                    tracks_to_download.append({
                        "title": title,
                        "videoId": vid,
                        "album": item.get("album", {}).get("name", "Single"),
                        "artist": item_artist,
                        "source": "Pencarian Umum"
                    })
        except Exception as e:
            logger.error("Gagal pencarian cadangan: %s", e)

    summary["unique_total"] = len(tracks_to_download)
    return artist_name, tracks_to_download, summary

# ==============================================================================
# 5. PIPELINE DOWNLOAD & UPLOAD PRODUCER-CONSUMER (HIGH THROUGHPUT)
# ==============================================================================

def execute_high_throughput_pipeline(job: DownloadJob) -> None:
    """
    Eksekutor pipeline download -> upload yang konkuren dan efisien.
    1. Cek lagu yang sudah ada di SQLite audio_cache -> kirim langsung via file_id.
    2. Lagu yang belum ada di-cache dimasukkan ke antrean transfer berbatas (Producer-Consumer).
    3. Download worker mengunduh & mengonversi audio ke MP3.
    4. Upload worker mengunggah audio ke ARCHIVE_CHANNEL_ID terlebih dahulu,
       menyimpan file_id ke cache, lalu meneruskannya ke pemohon.
    """
    chat_id = job.chat_id
    artist_name = job.artist_name
    tracks = job.tracks
    total = len(tracks)

    logger.info("Memulai pipeline job %s (%d lagu untuk '%s').", job.job_id, total, artist_name)

    # Kirim pesan status awal jika belum ada
    if not job.progress_msg_id:
        try:
            init_msg = bot.send_message(
                chat_id,
                f"📥 <b>Memproses Katalog: {html.escape(artist_name)}</b>\n\n"
                f"Total lagu: <b>{total} lagu</b>\n"
                f"Memeriksa cache arsip dan menyiapkan pipeline unduhan...",
                parse_mode="HTML"
            )
            job.progress_msg_id = init_msg.message_id
        except Exception as e:
            logger.error("Gagal mengirim pesan status awal: %s", e)

    last_progress_edit = time.time()
    def update_progress_ui(force: bool = False) -> None:
        nonlocal last_progress_edit
        now = time.time()
        if not force and (now - last_progress_edit < THROTTLE_PROGRESS_SECONDS):
            return
        last_progress_edit = now
        if not job.progress_msg_id:
            return
        try:
            progress_text = (
                f"⚡ <b>Pipeline Aktif: {html.escape(artist_name)}</b>\n\n"
                f"📊 <b>Progres:</b> [{job.delivered_count}/{total}]\n"
                f"• ⚡ Terkirim dari Cache: <b>{job.cached_count}</b>\n"
                f"• 📥 Berhasil Diunduh: <b>{job.downloaded_count}</b>\n"
                f"• 🏛️ Tersimpan di Arsip: <b>{job.archived_count}</b>\n"
                f"• ❌ Gagal: <b>{job.failed_count}</b>\n"
                f"• ⏭️ Dilewati (>50MB): <b>{job.skipped_count}</b>\n\n"
                f"<i>Ketik '❌ Batalkan' kapan saja untuk membatalkan antrean.</i>"
            )
            bot.edit_message_text(
                progress_text,
                chat_id=chat_id,
                message_id=job.progress_msg_id,
                parse_mode="HTML"
            )
        except Exception:
            pass

    # TAHAP 1: Pisahkan antara Cache Hit dan Uncached Tracks
    uncached_tracks: List[Tuple[int, Dict[str, Any]]] = []

    for idx, track in enumerate(tracks, 1):
        if job.cancel_event.is_set():
            break

        vid = track.get("videoId")
        cached = get_cached_track(vid) if vid else None

        if cached and cached.get("file_id"):
            # Cache HIT: Kirim langsung menggunakan file_id tanpa download ulang
            try:
                caption_text = f"🎵 [{idx}/{total}] {track['title']} (⚡ Arsip)"
                bot.send_audio(
                    chat_id=chat_id,
                    audio=cached["file_id"],
                    title=track["title"],
                    performer=track.get("artist") or artist_name,
                    caption=caption_text,
                    timeout=30
                )
                job.cached_count += 1
                job.delivered_count += 1
                update_progress_ui()
                time.sleep(0.3)  # Jeda mikro anti-flood
            except Exception as e:
                logger.warning("Gagal mengirim dari cache untuk [%s]: %s. Akan diunduh ulang.", track['title'], e)
                uncached_tracks.append((idx, track))
        else:
            uncached_tracks.append((idx, track))

    if job.cancel_event.is_set():
        logger.info("Job %s dibatalkan saat pengecekan cache.", job.job_id)
        return

    # TAHAP 2: Producer-Consumer Pipeline untuk Lagu yang Belum di-Cache
    if uncached_tracks:
        transfer_queue: queue.Queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        download_task_queue: queue.Queue = queue.Queue()
        for item in uncached_tracks:
            download_task_queue.put(item)

        # Opsi yt-dlp optimal sesuai konfigurasi audio ENV
        postprocessor = {
            "key": "FFmpegExtractAudio",
            "preferredcodec": AUDIO_FORMAT,
        }
        if AUDIO_BITRATE and AUDIO_BITRATE not in ["0", "best", "none"]:
            postprocessor["preferredquality"] = str(AUDIO_BITRATE)
        else:
            postprocessor["preferredquality"] = "0"  # Menjaga kualitas sumber tertinggi

        # A. Download Worker Function (Producer)
        def download_worker_func():
            with tempfile.TemporaryDirectory(prefix=f"tg_dl_worker_{job.job_id[:8]}_") as worker_temp_dir:
                ydl_opts = {
                    "format": "bestaudio/best",
                    "outtmpl": os.path.join(worker_temp_dir, "%(id)s.%(ext)s"),
                    "postprocessors": [postprocessor],
                    "quiet": True,
                    "no_warnings": True,
                    "noplaylist": True,
                    "socket_timeout": 30,
                    "retries": MAX_RETRIES,
                    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    while not job.cancel_event.is_set():
                        try:
                            item = download_task_queue.get_nowait()
                        except queue.Empty:
                            break

                        idx, track = item
                        track_title = track.get("title", f"Track {idx}")
                        video_id = track.get("videoId")
                        track_url = f"https://www.youtube.com/watch?v={video_id}"

                        mp3_path = None
                        success = False
                        err_reason = "Gagal ekstraksi"

                        for attempt in range(1, MAX_RETRIES + 1):
                            if job.cancel_event.is_set():
                                break
                            try:
                                info = ydl.extract_info(track_url, download=True)
                                if info:
                                    fname = ydl.prepare_filename(info)
                                    base, _ = os.path.splitext(fname)
                                    candidate = base + f".{AUDIO_FORMAT}"
                                    if os.path.exists(candidate):
                                        mp3_path = candidate
                                        success = True
                                        break
                            except Exception as dl_err:
                                err_reason = str(dl_err)
                                logger.warning("Percobaan %d/%d download [%s] gagal: %s", attempt, MAX_RETRIES, track_title, dl_err)
                                if attempt < MAX_RETRIES:
                                    time.sleep(1.5 * attempt)

                        if success and mp3_path and os.path.exists(mp3_path):
                            job.downloaded_count += 1
                            # Kirim ke transfer queue untuk diupload
                            transfer_queue.put({
                                "idx": idx,
                                "track": track,
                                "file_path": mp3_path,
                                "status": "ok"
                            })
                        else:
                            transfer_queue.put({
                                "idx": idx,
                                "track": track,
                                "file_path": None,
                                "status": "error",
                                "error": err_reason
                            })

                        download_task_queue.task_done()

        # B. Upload / Archive Worker Function (Consumer)
        def upload_worker_func():
            while not job.cancel_event.is_set():
                try:
                    pkg = transfer_queue.get(timeout=2.0)
                except queue.Empty:
                    if download_task_queue.empty() and all(not t.is_alive() for t in download_threads):
                        break
                    continue

                idx = pkg["idx"]
                track = pkg["track"]
                track_title = track.get("title", f"Track {idx}")
                video_id = track.get("videoId")
                file_path = pkg.get("file_path")

                if pkg.get("status") == "error":
                    job.failed_count += 1
                    err_msg = pkg.get("error", "Unduhan gagal")
                    logger.error("Lagu [%s] gagal diunduh: %s", track_title, err_msg)
                    job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": err_msg[:80]})
                    update_progress_ui()
                    transfer_queue.task_done()
                    continue

                if not file_path or not os.path.exists(file_path):
                    job.failed_count += 1
                    job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": "File audio tidak ditemukan"})
                    update_progress_ui()
                    transfer_queue.task_done()
                    continue

                try:
                    fsize = os.path.getsize(file_path)
                    if fsize > MAX_TELEGRAM_FILE_SIZE:
                        job.skipped_count += 1
                        reason_msg = f"Ukuran ({fsize / (1024*1024):.1f} MB) melebihi batas 50MB"
                        logger.warning("Lagu [%s] dilewati: %s", track_title, reason_msg)
                        job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": reason_msg})
                        update_progress_ui()
                        continue

                    # 1. PENGARSIPAN WAJIB KE CHANNEL ARSIP
                    file_id = None
                    archive_msg_id = None

                    if ARCHIVE_CHANNEL_TARGET:
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                with open(file_path, "rb") as afh:
                                    arc_caption = f"🎵 <b>{html.escape(track_title)}</b>\n👤 {html.escape(job.artist_name)}\n🆔 <code>{video_id}</code>"
                                    arch_msg = bot.send_audio(
                                        chat_id=ARCHIVE_CHANNEL_TARGET,
                                        audio=afh,
                                        title=track_title,
                                        performer=track.get("artist") or job.artist_name,
                                        caption=arc_caption,
                                        parse_mode="HTML",
                                        timeout=90
                                    )
                                    if arch_msg and arch_msg.audio:
                                        file_id = arch_msg.audio.file_id
                                        archive_msg_id = arch_msg.message_id
                                        job.archived_count += 1
                                        break
                            except Exception as arc_err:
                                logger.warning("Arsip ke channel gagal (percobaan %d/%d): %s", attempt, MAX_RETRIES, arc_err)
                                time.sleep(2.0 * attempt)

                    # Jika ada channel arsip tapi arsip gagal total, tandai kegagalan arsip
                    if ARCHIVE_CHANNEL_TARGET and not file_id:
                        job.failed_count += 1
                        fail_reason = "Gagal arsip ke channel"
                        job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": fail_reason})
                        update_progress_ui()
                        continue

                    # Simpan ke cache persisten jika berhasil diarsipkan
                    if file_id:
                        save_cached_track(video_id, track_title, job.artist_name, file_id, archive_msg_id, fsize)

                    # 2. PENGIRIMAN KE PENGGUNA
                    # Jika file_id tersedia, kirim via file_id (instan); jika tidak, kirim file fisik
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            caption_text = f"🎵 [{idx}/{total}] {track_title}"
                            if file_id:
                                bot.send_audio(
                                    chat_id=chat_id,
                                    audio=file_id,
                                    title=track_title,
                                    performer=track.get("artist") or job.artist_name,
                                    caption=caption_text,
                                    timeout=60
                                )
                            else:
                                with open(file_path, "rb") as afh:
                                    bot.send_audio(
                                        chat_id=chat_id,
                                        audio=afh,
                                        title=track_title,
                                        performer=track.get("artist") or job.artist_name,
                                        caption=caption_text,
                                        timeout=90
                                    )
                            job.delivered_count += 1
                            break
                        except Exception as user_send_err:
                            logger.warning("Gagal kirim ke pengguna [%s] (percobaan %d): %s", track_title, attempt, user_send_err)
                            if attempt == MAX_RETRIES:
                                job.failed_count += 1
                                job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": f"Gagal kirim Telegram: {str(user_send_err)[:50]}"})
                            time.sleep(1.5 * attempt)

                    update_progress_ui()

                finally:
                    # Bersihkan file lokal setelah selesai diunggah
                    try:
                        if file_path and os.path.exists(file_path):
                            os.remove(file_path)
                    except OSError:
                        pass
                    transfer_queue.task_done()

        # Jalankan Download Workers dan Upload Workers
        download_threads = [
            threading.Thread(target=download_worker_func, name=f"DL-Worker-{i}", daemon=True)
            for i in range(DOWNLOAD_WORKERS)
        ]
        upload_threads = [
            threading.Thread(target=upload_worker_func, name=f"UP-Worker-{i}", daemon=True)
            for i in range(UPLOAD_WORKERS)
        ]

        for t in download_threads:
            t.start()
        for t in upload_threads:
            t.start()

        for t in download_threads:
            t.join()
        for t in upload_threads:
            t.join()

    # TAHAP 3: Laporan Akhir Pekerjaan
    job.finished_at = time.time()
    job.status = "completed" if not job.cancel_event.is_set() else "cancelled"
    update_progress_ui(force=True)

    duration_sec = int((job.finished_at or time.time()) - (job.started_at or job.created_at))
    m, s = divmod(duration_sec, 60)

    if job.cancel_event.is_set():
        bot.send_message(
            chat_id,
            f"🛑 <b>Unduhan Dibatalkan: {html.escape(artist_name)}</b>\n"
            f"Dihentikan atas permintaan pengguna. Terkirim: {job.delivered_count}/{total}.",
            parse_mode="HTML"
        )
        return

    report_lines = [
        f"🎉 <b>Katalog Selesai Diproses!</b>",
        f"Artis: <b>{html.escape(artist_name)}</b>",
        f"⏱️ Waktu pengerjaan: {m}m {s}s\n",
        f"📊 <b>Ringkasan Pengiriman:</b>",
        f"• Total Katalog: <b>{total}</b> lagu",
        f"• ⚡ Terkirim dari Cache: <b>{job.cached_count}</b>",
        f"• 📥 Diunduh Baru: <b>{job.downloaded_count}</b>",
        f"• 🏛️ Tersimpan di Channel Arsip: <b>{job.archived_count}</b>",
        f"• ❌ Gagal: <b>{job.failed_count}</b>",
        f"• ⏭️ Dilewati (>50MB): <b>{job.skipped_count}</b>"
    ]

    retry_markup = None
    if job.failed_tracks:
        report_lines.append(f"\n⚠️ <b>Daftar Lagu Gagal ({len(job.failed_tracks)}):</b>")
        for ft in job.failed_tracks[:8]:
            report_lines.append(f"- <i>{html.escape(ft['title'])}</i> ({html.escape(ft['reason'])})")
        if len(job.failed_tracks) > 8:
            report_lines.append(f"<i>...dan {len(job.failed_tracks) - 8} lagu lainnya.</i>")

        retry_session_id = str(uuid.uuid4())
        retry_job = DownloadJob(
            job_id=retry_session_id,
            chat_id=chat_id,
            user_id=job.user_id,
            artist_name=artist_name,
            query=job.query,
            tracks=job.failed_tracks,
            catalog_summary={"is_retry": True}
        )
        with CONFIRMATION_LOCK:
            PENDING_CONFIRMATION[chat_id] = retry_job

        retry_markup = types.InlineKeyboardMarkup()
        btn_retry = types.InlineKeyboardButton(
            f"🔄 Coba Lagi Lagu Gagal ({len(job.failed_tracks)})",
            callback_data=f"confirm_dl:{retry_session_id}"
        )
        retry_markup.add(btn_retry)

    try:
        bot.send_message(
            chat_id,
            "\n".join(report_lines),
            parse_mode="HTML",
            reply_markup=retry_markup
        )
    except Exception as e:
        logger.error("Gagal mengirim laporan akhir: %s", e)

# ==============================================================================
# 6. HANDLER PERINTAH & PESAN TELEGRAM
# ==============================================================================

@bot.message_handler(commands=["start", "help"])
def send_welcome(message: types.Message) -> None:
    """Menampilkan sambutan dan menu keyboard utama."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    btn_help = types.KeyboardButton("🔍 Cara Pakai")
    btn_queue = types.KeyboardButton("📋 Status Antrean")
    btn_cancel = types.KeyboardButton("❌ Batalkan")
    markup.add(btn_help, btn_queue, btn_cancel)

    welcome_text = (
        "👋 <b>Selamat Datang di Bot Downloader Musik Artis (High-Throughput Edition)!</b>\n\n"
        "Fitur unggulan:\n"
        "• <b>Penyapuan Katalog Penuh:</b> Mengambil Songs, Album, dan Singles/EP.\n"
        "• <b>Multi-Job Queue:</b> Anda bisa meminta artis baru (misal Peterpan saat Noah sedang diproses) "
        "dan otomatis masuk ke antrean berikutnya!\n"
        "• <b>Pengarsipan Channel & Cache:</b> Setiap lagu disimpan di arsip Telegram dan dapat dikirim instan jika diminta lagi.\n\n"
        "💡 <i>Ketik nama artis (contoh: <code>Noah</code> atau <code>Peterpan</code>) untuk memulai.</i>"
    )
    bot.reply_to(message, welcome_text, parse_mode="HTML", reply_markup=markup)

@bot.message_handler(commands=["queue", "status"])
def show_queue_status_command(message: types.Message) -> None:
    """Menampilkan status antrean saat ini."""
    status_text = JOB_MANAGER.get_status_summary()
    bot.reply_to(message, status_text, parse_mode="HTML")

@bot.message_handler(func=lambda msg: msg.text in ["🔍 Cara Pakai", "📋 Status Antrean", "❌ Batalkan"])
def handle_reply_keyboard(message: types.Message) -> None:
    """Menangani tombol reply keyboard navigasi utama."""
    chat_id = message.chat.id

    if message.text == "🔍 Cara Pakai":
        instructions = (
            "💡 <b>Cara Menggunakan Bot:</b>\n\n"
            "1. <b>Ketik Nama Artis</b> (contoh: <code>Noah</code>, <code>Peterpan</code>, <code>Coldplay</code>).\n"
            "2. Bot akan menyapu YouTube Music dan menampilkan jumlah lagu unik serta album rilis.\n"
            "3. Klik <b>'✅ Ya, Download'</b> untuk memulai proses.\n"
            "4. <b>Multi-Antrean:</b> Jika unduhan sedang berjalan dan Anda ingin artis lain, cukup ketik nama artis lagi! "
            "Artis baru otomatis masuk ke antrean berikutnya tanpa membatalkan proses yang sedang berjalan.\n"
            "5. Klik <b>'📋 Status Antrean'</b> kapan saja untuk melihat posisi job Anda."
        )
        bot.reply_to(message, instructions, parse_mode="HTML")

    elif message.text == "📋 Status Antrean":
        show_queue_status_command(message)

    elif message.text == "❌ Batalkan":
        # Hapus sesi konfirmasi jika ada
        with CONFIRMATION_LOCK:
            pending = PENDING_CONFIRMATION.pop(chat_id, None)

        # Batalkan job aktif atau job di antrean
        cancelled, msg = JOB_MANAGER.cancel_job_for_chat(chat_id)
        if cancelled:
            bot.reply_to(message, f"🛑 <b>Pembatalan Berhasil:</b> {html.escape(msg)}", parse_mode="HTML")
        elif pending:
            bot.reply_to(message, "🚫 Sesi konfirmasi pencarian tertunda telah dibatalkan.", parse_mode="HTML")
        else:
            bot.reply_to(message, "ℹ️ Tidak ada sesi pencarian atau unduhan yang sedang aktif untuk Anda.", parse_mode="HTML")

@bot.message_handler(func=lambda msg: True)
def process_artist_search_request(message: types.Message) -> None:
    """Menangani input nama artis dari pengguna."""
    query = (message.text or "").strip()

    if not query or query.startswith("/"):
        return
    if len(query) > 120:
        bot.reply_to(message, "⚠️ Nama artis terlalu panjang. Silakan masukkan nama yang lebih ringkas.", parse_mode="HTML")
        return

    chat_id = message.chat.id
    status_msg = bot.reply_to(
        message,
        f"🔍 <b>Menyapu seluruh pustaka lagu untuk:</b> <i>{html.escape(query)}</i>...\n"
        f"Sedang memeriksa Songs, Album, dan Singles/EP di YouTube Music...",
        parse_mode="HTML"
    )

    try:
        artist_name, tracks, summary = crawl_artist_catalog(ytmusic, query)
        total = len(tracks)

        if total == 0:
            bot.edit_message_text(
                f"❌ Tidak ditemukan lagu untuk <b>{html.escape(query)}</b> di YouTube Music.\n"
                f"Silakan periksa ejaan nama artis dan coba lagi.",
                chat_id=chat_id,
                message_id=status_msg.message_id,
                parse_mode="HTML"
            )
            return

        session_id = str(uuid.uuid4())
        job = DownloadJob(
            job_id=session_id,
            chat_id=chat_id,
            user_id=message.from_user.id if message.from_user else chat_id,
            artist_name=artist_name,
            query=query,
            tracks=tracks,
            catalog_summary=summary
        )

        with CONFIRMATION_LOCK:
            PENDING_CONFIRMATION[chat_id] = job

        inline_markup = types.InlineKeyboardMarkup(row_width=2)
        btn_confirm = types.InlineKeyboardButton(
            f"✅ Ya, Download ({total} Lagu)",
            callback_data=f"confirm_dl:{session_id}"
        )
        btn_cancel = types.InlineKeyboardButton(
            "❌ Batal",
            callback_data=f"cancel_dl:{session_id}"
        )
        inline_markup.add(btn_confirm, btn_cancel)

        catalog_info = []
        if summary.get("is_official_artist"):
            catalog_info.append(f"⭐ <b>Profil Artis Resmi:</b> {html.escape(artist_name)}")
            if summary.get("songs_count"):
                catalog_info.append(f"• Lagu dari Playlist Utama: <b>{summary['songs_count']}</b>")
            if summary.get("albums_count"):
                catalog_info.append(f"• Album Diproses: <b>{summary['albums_count']} album</b>")
            if summary.get("singles_count"):
                catalog_info.append(f"• Singles & EPs: <b>{summary['singles_count']} rilis</b>")
        else:
            catalog_info.append(f"⚠️ <i>Katalog profil artis resmi tidak ditemukan.</i>")
            catalog_info.append(f"Menjalankan hasil pencarian trek musik umum.")

        catalog_info_str = "\n".join(catalog_info)

        bot.edit_message_text(
            f"📦 <b>Katalog Ditemukan!</b>\n\n"
            f"{catalog_info_str}\n\n"
            f"🎯 <b>Total Lagu Unik:</b> <b>{total} lagu</b>\n\n"
            f"Apakah Anda ingin mulai mengunduh seluruh {total} lagu ini?",
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML",
            reply_markup=inline_markup
        )

    except Exception as e:
        logger.exception("Kesalahan saat mencari katalog: %s", e)
        bot.edit_message_text(
            f"⚠️ Terjadi kesalahan saat menjelajahi katalog: {html.escape(str(e))}",
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML"
        )

# ==============================================================================
# 7. HANDLER CALLBACK INLINE (KONFIRMASI / BATAL / ANTREAN)
# ==============================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_dl"))
def handle_cancel_inline(call: types.CallbackQuery) -> None:
    chat_id = call.message.chat.id
    with CONFIRMATION_LOCK:
        PENDING_CONFIRMATION.pop(chat_id, None)

    JOB_MANAGER.cancel_job_for_chat(chat_id)

    try:
        bot.answer_callback_query(call.id, "Sesi dibatalkan.")
        bot.edit_message_text(
            "❌ <b>Proses dibatalkan.</b> Sesi aktif telah dibersihkan.",
            chat_id=chat_id,
            message_id=call.message.message_id,
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_dl"))
def handle_confirm_inline(call: types.CallbackQuery) -> None:
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    requested_id = parts[1] if len(parts) > 1 else None

    with CONFIRMATION_LOCK:
        job = PENDING_CONFIRMATION.get(chat_id)
        if not job or (requested_id and job.job_id != requested_id):
            try:
                bot.answer_callback_query(call.id, "Sesi kedaluwarsa atau sudah diproses.", show_alert=True)
                bot.edit_message_text(
                    "⚠️ <b>Sesi kedaluwarsa atau sudah diproses.</b>\nSilakan cari nama artis kembali.",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return

        PENDING_CONFIRMATION.pop(chat_id, None)

    # Masukkan job ke antrean JobQueueManager
    job.progress_msg_id = call.message.message_id
    position = JOB_MANAGER.enqueue(job)

    try:
        if position == 1:
            bot.answer_callback_query(call.id, "Memulai unduhan langsung...")
            bot.edit_message_text(
                f"📥 <b>Memulai Pengunduhan: {html.escape(job.artist_name)}</b>\n\n"
                f"Pipeline aktif: Download dan upload berjalan konkuren.\n"
                f"<i>Ketik '❌ Batalkan' untuk menghentikan proses.</i>",
                chat_id=chat_id,
                message_id=call.message.message_id,
                parse_mode="HTML"
            )
        else:
            bot.answer_callback_query(call.id, f"Masuk antrean posisi #{position}!")
            bot.edit_message_text(
                f"⏳ <b>Artis Dimasukkan ke Antrean (Posisi #{position}): {html.escape(job.artist_name)}</b>\n\n"
                f"Lagu akan otomatis diproses setelah pekerjaan aktif saat ini selesai.\n"
                f"Gunakan menu <b>'📋 Status Antrean'</b> untuk melihat perkembangan.",
                chat_id=chat_id,
                message_id=call.message.message_id,
                parse_mode="HTML"
            )
    except Exception:
        pass

# ==============================================================================
# 8. MAIN RUNNER DENGAN KONEKSI TELEGRAM TANGGUH
# ==============================================================================

def main() -> None:
    """Fungsi utama untuk menjalankan bot Telegram dengan toleransi timeout dan anti-bentrok."""
    logger.info("=" * 65)
    logger.info("🤖 Bot Telegram Musik Artis Aktif (High-Throughput Edition)...")
    logger.info("Konfigurasi Audio: format=%s, quality=%s, bitrate=%s", AUDIO_FORMAT, AUDIO_QUALITY, AUDIO_BITRATE)
    logger.info("Worker Pipeline: Download=%d, Upload=%d, MaxQueue=%d", DOWNLOAD_WORKERS, UPLOAD_WORKERS, MAX_QUEUE_SIZE)
    logger.info("Timeout Jaringan: BotApi=%ds, Polling=%ds", BOT_API_TIMEOUT, POLLING_TIMEOUT)
    if ARCHIVE_CHANNEL_TARGET:
        logger.info("Channel Pengarsipan Wajib: %s", ARCHIVE_CHANNEL_TARGET)
    else:
        logger.warning("Channel Pengarsipan Wajib: TIDAK AKTIF (%s)", ARCHIVE_CHANNEL_WARNING)
    logger.info("=" * 65)

    # Bersihkan webhook lama agar tidak bentrok dengan polling getUpdates
    try:
        logger.info("Memeriksa status webhook dan membersihkan pending updates...")
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
        logger.info("Status webhook bersih. Memulai polling getUpdates...")
    except Exception as wh_err:
        logger.warning("Peringatan saat membersihkan webhook: %s", wh_err)

    # Polling loop dengan penanganan timeout dan 409 Conflict
    while True:
        try:
            bot.infinity_polling(
                timeout=POLLING_TIMEOUT,
                long_polling_timeout=LONG_POLLING_TIMEOUT,
                skip_pending=True
            )
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot dihentikan oleh pengguna.")
            break
        except telebot.apihelper.ApiTelegramException as api_err:
            if getattr(api_err, "error_code", None) == 409 or "Conflict" in str(api_err):
                logger.error(
                    "\n" + "!" * 70 + "\n"
                    "❌ TERJADI BENTROK TOKEN TELEGRAM (Error 409 Conflict)!\n"
                    "Penyebab: Ada proses bot lain yang masih aktif menggunakan token ini.\n"
                    "Solusi: Tutup terminal/proses lain (`pkill -f app.py`) atau /revoke token di @BotFather.\n"
                    "!" * 70 + "\n"
                )
                time.sleep(15)
            else:
                logger.error("Terjadi galat API Telegram: %s. Reconnecting dalam 5 detik...", api_err)
                time.sleep(5)
        except Exception as e:
            if "Conflict" in str(e):
                logger.error("Terdeteksi konflik polling: %s. Jeda 15 detik...", e)
                time.sleep(15)
            else:
                logger.error("Terjadi galat pada polling: %s. Melakukan rekoneksi...", e)
                time.sleep(5)

if __name__ == "__main__":
    main()
