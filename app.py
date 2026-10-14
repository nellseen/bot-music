#!/usr/bin/env python3
"""
Telegram Artist & Single Track Music Downloader Bot
===================================================
High-Throughput, Persistent Auto-Resume, Archive & Smart Cache Edition.

Fitur Utama:
1. Search Single Lagu (/song & /search):
   Pencarian langsung lagu per judul atau judul + nama artis. Menampilkan 5-8 hasil
   dengan inline keyboard untuk memilih lagu tertentu.
2. Multi-Job Unified Queue:
   Satu antrean terpadu untuk download katalog artis penuh maupun single song.
   Pengguna dapat menambahkan artis atau lagu baru kapan saja tanpa menimpa proses aktif.
3. Pengarsipan Wajib Channel (ARCHIVE_CHANNEL_ID):
   Setiap lagu wajib berhasil dikirim ke channel arsip terlebih dahulu, diambil
   file_id-nya, lalu dikirim ke pengguna pemohon secara instan.
4. Smart Persistent Cache (SQLite):
   Menyimpan metadata dan file_id Telegram berdasarkan videoId di SQLite.
   Lagu yang pernah diproses dikirim ulang seketika (0 detik latency).
5. Auto-Resume Setelah Restart / Crash:
   Job yang sedang berjalan atau mengantre disimpan di database persistent_jobs.
   Saat bot menyala kembali, job tertunda otomatis dilanjutkan.
6. Producer-Consumer Pipeline:
   Download worker dan upload worker berjalan secara konkuren dengan buffer berbatas
   (MAX_QUEUE_SIZE) untuk kecepatan transfer maksimal dan konsumsi memori yang aman.
7. Monitoring & Perintah Admin:
   /queue, /status, /cancel, /stats, /health, dengan kontrol akses ADMIN_USER_IDS.
8. Konfigurasi Audio via ENV:
   AUDIO_QUALITY, AUDIO_FORMAT, dan AUDIO_BITRATE tanpa re-encode mubazir.
"""

import html
import json
import logging
import os
import queue
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

# Muat environment variable dari .env
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

# Kualitas dan Format Audio
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "best").strip().lower()
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3").strip().lower()
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "0").strip()

# Konfigurasi Worker & Antrean
DOWNLOAD_WORKERS = max(1, int(os.getenv("DOWNLOAD_WORKERS", "2")))
UPLOAD_WORKERS = max(1, int(os.getenv("UPLOAD_WORKERS", "2")))
MAX_QUEUE_SIZE = max(1, int(os.getenv("MAX_QUEUE_SIZE", "5")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "3")))
MAX_USER_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_USER_CONCURRENT_JOBS", "3")))

# Daftar ID Admin
RAW_ADMIN_IDS = os.getenv("ADMIN_USER_IDS", "").strip()
ADMIN_USER_IDS: Set[int] = set()
if RAW_ADMIN_IDS:
    for raw_id in RAW_ADMIN_IDS.split(","):
        try:
            ADMIN_USER_IDS.add(int(raw_id.strip()))
        except ValueError:
            pass

def is_admin(user_id: int) -> bool:
    """Memeriksa apakah user_id memiliki hak akses admin."""
    return user_id in ADMIN_USER_IDS

# Konfigurasi Timeout Jaringan & Polling Telegram
BOT_API_TIMEOUT = int(os.getenv("BOT_API_TIMEOUT", "60"))
POLLING_TIMEOUT = int(os.getenv("POLLING_TIMEOUT", "30"))
LONG_POLLING_TIMEOUT = int(os.getenv("LONG_POLLING_TIMEOUT", "30"))

# Konfigurasi batas file dan throttled UI
MAX_TELEGRAM_FILE_SIZE = 49.5 * 1024 * 1024  # 49.5 MB
THROTTLE_PROGRESS_SECONDS = 5.0
SEARCH_SESSION_EXPIRY_SECONDS = 600  # 10 menit

# Inisialisasi API Helper Timeout
telebot.apihelper.CONNECT_TIMEOUT = BOT_API_TIMEOUT
telebot.apihelper.READ_TIMEOUT = BOT_API_TIMEOUT
telebot.apihelper.LONG_POLLING_TIMEOUT = LONG_POLLING_TIMEOUT

# Inisialisasi Klien Bot dan YouTube Music
bot = telebot.TeleBot(BOT_TOKEN or "123456789:AAH_DUMMY_TOKEN_FOR_INITIALIZATION")
ytmusic = YTMusic()
START_TIME = time.time()

# ==============================================================================
# 2. PERSISTENT CACHE & JOB DATABASE (SQLITE) DENGAN AUTO-RESUME
# ==============================================================================

DB_PATH = os.getenv("DB_PATH", "music_cache.db")
DB_LOCK = threading.Lock()

def init_db() -> None:
    """Inisialisasi tabel SQLite untuk cache audio dan persistent jobs."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            # Tabel 1: Audio Cache
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
            # Tabel 2: Persistent Jobs untuk Auto-Resume
            conn.execute("""
                CREATE TABLE IF NOT EXISTS persistent_jobs (
                    job_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    job_type TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    query TEXT NOT NULL,
                    total_tracks INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    tracks_json TEXT NOT NULL,
                    delivered_count INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL
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

def save_persistent_job_to_db(job: Any) -> None:
    """Menyimpan atau memperbarui status DownloadJob ke tabel persistent_jobs."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            tracks_json = json.dumps(job.tracks)
            conn.execute("""
                INSERT OR REPLACE INTO persistent_jobs 
                (job_id, chat_id, user_id, job_type, artist_name, query, total_tracks, status, tracks_json, delivered_count, created_at, started_at, finished_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.job_id,
                job.chat_id,
                job.user_id,
                job.job_type,
                job.artist_name,
                job.query,
                job.total_tracks,
                job.status,
                tracks_json,
                job.delivered_count,
                job.created_at,
                job.started_at,
                job.finished_at
            ))
            conn.commit()
        finally:
            conn.close()

def update_persistent_job_status(job_id: str, status: str, finished_at: Optional[float] = None, delivered_count: Optional[int] = None) -> None:
    """Memperbarui status pekerjaan di SQLite."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            if finished_at is not None and delivered_count is not None:
                conn.execute("UPDATE persistent_jobs SET status = ?, finished_at = ?, delivered_count = ? WHERE job_id = ?", (status, finished_at, delivered_count, job_id))
            elif finished_at is not None:
                conn.execute("UPDATE persistent_jobs SET status = ?, finished_at = ? WHERE job_id = ?", (status, finished_at, job_id))
            elif delivered_count is not None:
                conn.execute("UPDATE persistent_jobs SET status = ?, delivered_count = ? WHERE job_id = ?", (status, delivered_count, job_id))
            else:
                conn.execute("UPDATE persistent_jobs SET status = ? WHERE job_id = ?", (status, job_id))
            conn.commit()
        finally:
            conn.close()

def get_uncompleted_persistent_jobs() -> List[Dict[str, Any]]:
    """Mengambil job yang belum selesai ('queued' atau 'running') saat bot crash/restart."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT job_id, chat_id, user_id, job_type, artist_name, query, total_tracks, status, tracks_json, delivered_count, created_at
                FROM persistent_jobs
                WHERE status IN ('queued', 'running', 'downloading', 'uploading')
                ORDER BY created_at ASC
            """)
            rows = cur.fetchall()
            jobs = []
            for r in rows:
                try:
                    tracks = json.loads(r[8])
                except Exception:
                    tracks = []
                jobs.append({
                    "job_id": r[0],
                    "chat_id": r[1],
                    "user_id": r[2],
                    "job_type": r[3],
                    "artist_name": r[4],
                    "query": r[5],
                    "total_tracks": r[6],
                    "status": r[7],
                    "tracks": tracks,
                    "delivered_count": r[9],
                    "created_at": r[10]
                })
            return jobs
        finally:
            conn.close()

def get_database_stats() -> Dict[str, Any]:
    """Mendapatkan statistik ringkas dari database untuk perintah /stats."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM audio_cache")
            total_cached = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM persistent_jobs WHERE status = 'completed'")
            total_completed = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM persistent_jobs WHERE status = 'failed'")
            total_failed = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM persistent_jobs WHERE status IN ('queued', 'running', 'downloading', 'uploading')")
            total_active = cur.fetchone()[0]

            return {
                "total_cached": total_cached,
                "total_completed": total_completed,
                "total_failed": total_failed,
                "total_active": total_active
            }
        finally:
            conn.close()

init_db()

# ==============================================================================
# 3. STRUKTUR DATA JOB, SESI PENCARIAN, & MANAGER ANTREAN THREAD-SAFE
# ==============================================================================

class DownloadJob:
    """Representasi satu tugas pengunduhan (katalog artis maupun single song)."""

    def __init__(
        self,
        job_id: str,
        chat_id: int,
        user_id: int,
        artist_name: str,
        query: str,
        tracks: List[Dict[str, Any]],
        catalog_summary: Optional[Dict[str, Any]] = None,
        job_type: str = "artist",  # "artist" atau "single"
    ):
        self.job_id: str = job_id
        self.chat_id: int = chat_id
        self.user_id: int = user_id
        self.artist_name: str = artist_name
        self.query: str = query
        self.tracks: List[Dict[str, Any]] = tracks
        self.catalog_summary: Dict[str, Any] = catalog_summary or {}
        self.job_type: str = job_type
        self.total_tracks: int = len(tracks)

        self.status: str = "queued"
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

class SingleSearchSession:
    """Menyimpan sesi pencarian lagu (/song atau /search) dengan validasi requester."""

    def __init__(self, search_id: str, chat_id: int, requester_user_id: int, query: str, results: List[Dict[str, Any]]):
        self.search_id: str = search_id
        self.chat_id: int = chat_id
        self.requester_user_id: int = requester_user_id
        self.query: str = query
        self.results: List[Dict[str, Any]] = results
        self.created_at: float = time.time()

class JobQueueManager:
    """Pengelola antrean multi-job yang thread-safe untuk mendukung permintaan beruntun dan auto-resume."""

    def __init__(self):
        self.lock = threading.RLock()
        self.active_job: Optional[DownloadJob] = None
        self.pending_queue: List[DownloadJob] = []
        self.worker_thread: Optional[threading.Thread] = None

    def enqueue(self, job: DownloadJob) -> int:
        """Menambahkan job baru ke antrean. Mengembalikan posisi dalam antrean (1 = aktif)."""
        with self.lock:
            # Periksa kuota job per pengguna
            user_jobs = sum(1 for q in self.pending_queue if q.user_id == job.user_id)
            if self.active_job and self.active_job.user_id == job.user_id:
                user_jobs += 1

            if user_jobs >= MAX_USER_CONCURRENT_JOBS and not is_admin(job.user_id):
                raise ValueError(f"Batas maksimal antrean per pengguna ({MAX_USER_CONCURRENT_JOBS} job) telah tercapai. Harap tunggu antrean Anda selesai.")

            self.pending_queue.append(job)
            save_persistent_job_to_db(job)
            position = len(self.pending_queue) + (1 if self.active_job else 0)
            logger.info("Job %s ('%s', tipe: %s) dimasukkan ke antrean. Posisi: %d", job.job_id, job.artist_name, job.job_type, position)

            if self.active_job is None:
                self._start_next_job_unlocked()

            return position

    def _start_next_job_unlocked(self) -> None:
        """Mengambil job berikutnya dari antrean dan memulai worker thread."""
        if not self.pending_queue:
            self.active_job = None
            return

        self.active_job = self.pending_queue.pop(0)
        self.active_job.status = "running"
        self.active_job.started_at = time.time()
        update_persistent_job_status(self.active_job.job_id, "running")

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
            update_persistent_job_status(job.job_id, "failed", finished_at=time.time())
        finally:
            with self.lock:
                self.active_job = None
                self._start_next_job_unlocked()

    def cancel_job_for_chat(self, chat_id: int, user_id: int, target_job_id: Optional[str] = None) -> Tuple[bool, str]:
        """Membatalkan job yang aktif atau menghapus job yang masih mengantre."""
        with self.lock:
            # 1. Cek jika job aktif cocok
            if self.active_job:
                matches_active = False
                if target_job_id and self.active_job.job_id == target_job_id:
                    matches_active = True
                elif not target_job_id and self.active_job.chat_id == chat_id:
                    matches_active = True

                if matches_active:
                    # Validasi otorisasi
                    if not is_admin(user_id) and self.active_job.user_id != user_id:
                        return False, "Anda tidak memiliki izin untuk membatalkan pekerjaan pengguna lain."
                    self.active_job.cancel_event.set()
                    self.active_job.status = "cancelled"
                    update_persistent_job_status(self.active_job.job_id, "cancelled", finished_at=time.time())
                    return True, f"Proses unduhan aktif untuk '{self.active_job.artist_name}' sedang dihentikan."

            # 2. Cek jika ada job di antrean pending
            for idx, job in enumerate(self.pending_queue):
                matches_pending = False
                if target_job_id and job.job_id == target_job_id:
                    matches_pending = True
                elif not target_job_id and job.chat_id == chat_id:
                    matches_pending = True

                if matches_pending:
                    if not is_admin(user_id) and job.user_id != user_id:
                        return False, "Anda tidak memiliki izin untuk membatalkan antrean pengguna lain."
                    removed = self.pending_queue.pop(idx)
                    removed.cancel_event.set()
                    removed.status = "cancelled"
                    update_persistent_job_status(removed.job_id, "cancelled", finished_at=time.time())
                    return True, f"Antrean untuk '{removed.artist_name}' berhasil dibatalkan."

            return False, "Tidak ditemukan antrean aktif untuk dibatalkan."

    def get_status_summary(self, user_id: Optional[int] = None) -> str:
        """Menghasilkan teks ringkasan status antrean saat ini."""
        with self.lock:
            lines = ["📋 <b>Status Antrean Unduhan Musik:</b>\n"]
            if self.active_job:
                p = self.active_job
                job_desc = "Single Lagu" if p.job_type == "single" else "Katalog Artis"
                lines.append(
                    f"▶️ <b>Sedang Diproses ({job_desc}):</b>\n"
                    f"   <b>{html.escape(p.artist_name)}</b>\n"
                    f"   Status: <b>{p.status.upper()}</b> | ID: <code>{p.job_id[:8]}</code>\n"
                    f"   Progres: {p.delivered_count}/{p.total_tracks} terkirim "
                    f"(⚡ {p.cached_count} cache, 📥 {p.downloaded_count} unduh, ❌ {p.failed_count} gagal)"
                )
            else:
                lines.append("<i>Tidak ada tugas yang sedang aktif diproses.</i>")

            if self.pending_queue:
                lines.append(f"\n⏳ <b>Menunggu di Antrean ({len(self.pending_queue)} pekerjaan):</b>")
                for i, q in enumerate(self.pending_queue, 1):
                    q_type = "🎵 Single" if q.job_type == "single" else "📦 Artis"
                    lines.append(f"{i}. [{q_type}] <b>{html.escape(q.artist_name)}</b> ({q.total_tracks} lagu) - ID: <code>{q.job_id[:8]}</code>")
            else:
                lines.append("\n<i>Antrean berikutnya kosong.</i>")

            return "\n".join(lines)

JOB_MANAGER = JobQueueManager()
PENDING_CONFIRMATION: Dict[int, DownloadJob] = {}
SINGLE_SEARCH_SESSIONS: Dict[str, SingleSearchSession] = {}
CONFIRMATION_LOCK = threading.Lock()
SEARCH_LOCK = threading.Lock()

def auto_resume_uncompleted_jobs() -> None:
    """Memulihkan job yang terputus akibat restart atau crash saat bot dinyalakan kembali."""
    pending_jobs_data = get_uncompleted_persistent_jobs()
    if not pending_jobs_data:
        logger.info("Auto-Resume: Tidak ada job tertunda di database.")
        return

    logger.info("Auto-Resume: Memulihkan %d job tertunda dari database...", len(pending_jobs_data))
    for data in pending_jobs_data:
        try:
            job = DownloadJob(
                job_id=data["job_id"],
                chat_id=data["chat_id"],
                user_id=data["user_id"],
                artist_name=data["artist_name"],
                query=data["query"],
                tracks=data["tracks"],
                catalog_summary={"is_resumed": True},
                job_type=data["job_type"]
            )
            job.created_at = data["created_at"]
            JOB_MANAGER.enqueue(job)
            logger.info("Job %s ('%s') berhasil dipulihkan ke antrean.", job.job_id, job.artist_name)
        except Exception as e:
            logger.error("Gagal memulihkan job %s: %s", data.get("job_id"), e)

# Jalankan auto-resume saat modul dimuat
auto_resume_uncompleted_jobs()

# ==============================================================================
# 4. PENCARIAN SINGLE LAGU (/song & /search)
# ==============================================================================

def search_single_songs(yt: YTMusic, query: str, limit: int = 8) -> List[Dict[str, Any]]:
    """Mencari daftar lagu tunggal berdasarkan judul atau judul + artis."""
    results: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    try:
        raw_results = yt.search(query, filter="songs", limit=limit * 2)
        if not raw_results:
            raw_results = yt.search(query, limit=limit * 2)

        for item in raw_results:
            vid = item.get("videoId")
            title = item.get("title")
            if not vid or not title or vid in seen_ids:
                continue

            seen_ids.add(vid)
            artists = item.get("artists", [])
            artist_name = ", ".join([a.get("name", "") for a in artists]) if artists else "Unknown Artist"
            album = item.get("album", {}).get("name", "-") if item.get("album") else "-"
            duration = item.get("duration", "-")

            results.append({
                "title": title,
                "videoId": vid,
                "artist": artist_name,
                "album": album,
                "duration": duration,
                "source": "Pencarian Single Lagu"
            })

            if len(results) >= limit:
                break
    except Exception as e:
        logger.error("Gagal menjalankan search_single_songs(%s): %s", query, e)

    return results

# ==============================================================================
# 5. MESIN PENCARIAN & AUDIT KATALOG ARTIS YOUTUBE MUSIC
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
            logger.debug("get_artist_albums standar gagal (%s) untuk %s, beralih ke parser browse...", e, section_name)
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
                logger.warning("Gagal rilis browse untuk %s: %s", section_name, e2)

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
# 6. PIPELINE DOWNLOAD & UPLOAD PRODUCER-CONSUMER (HIGH THROUGHPUT)
# ==============================================================================

def execute_high_throughput_pipeline(job: DownloadJob) -> None:
    """Eksekutor pipeline download & upload konkuren dengan arsip wajib dan cache persisten."""
    chat_id = job.chat_id
    artist_name = job.artist_name
    tracks = job.tracks
    total = len(tracks)

    logger.info("Memulai pipeline job %s (%d lagu, tipe: %s, artist: '%s').", job.job_id, total, job.job_type, artist_name)

    # Kirim status awal
    if not job.progress_msg_id:
        try:
            init_msg = bot.send_message(
                chat_id,
                f"📥 <b>Memproses: {html.escape(artist_name)}</b>\n\n"
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
            desc = "Single Lagu" if job.job_type == "single" else "Katalog Artis"
            progress_text = (
                f"⚡ <b>Pipeline Aktif ({desc}): {html.escape(artist_name)}</b>\n\n"
                f"📊 <b>Progres:</b> [{job.delivered_count}/{total}]\n"
                f"• ⚡ Terkirim dari Cache: <b>{job.cached_count}</b>\n"
                f"• 📥 Berhasil Diunduh: <b>{job.downloaded_count}</b>\n"
                f"• 🏛️ Tersimpan di Arsip: <b>{job.archived_count}</b>\n"
                f"• ❌ Gagal: <b>{job.failed_count}</b>\n"
                f"• ⏭️ Dilewati (>50MB): <b>{job.skipped_count}</b>\n\n"
                f"<i>Ketik '❌ Batalkan' untuk membatalkan pekerjaan ini.</i>"
            )
            bot.edit_message_text(
                progress_text,
                chat_id=chat_id,
                message_id=job.progress_msg_id,
                parse_mode="HTML"
            )
        except Exception:
            pass

    # TAHAP 1: Pisahkan Cache Hit dan Uncached Tracks
    uncached_tracks: List[Tuple[int, Dict[str, Any]]] = []

    for idx, track in enumerate(tracks, 1):
        if job.cancel_event.is_set():
            break

        vid = track.get("videoId")
        cached = get_cached_track(vid) if vid else None

        if cached and cached.get("file_id"):
            # Cache HIT: Kirim langsung via file_id
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
                update_persistent_job_status(job.job_id, "running", delivered_count=job.delivered_count)
                update_progress_ui()
                time.sleep(0.2)
            except Exception as e:
                logger.warning("Gagal kirim dari cache [%s]: %s. Akan diunduh ulang.", track['title'], e)
                uncached_tracks.append((idx, track))
        else:
            uncached_tracks.append((idx, track))

    if job.cancel_event.is_set():
        logger.info("Job %s dibatalkan saat pengecekan cache.", job.job_id)
        update_persistent_job_status(job.job_id, "cancelled", finished_at=time.time())
        return

    # TAHAP 2: Producer-Consumer Pipeline untuk Lagu yang Belum di-Cache
    if uncached_tracks:
        transfer_queue: queue.Queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        download_task_queue: queue.Queue = queue.Queue()
        for item in uncached_tracks:
            download_task_queue.put(item)

        postprocessor = {
            "key": "FFmpegExtractAudio",
            "preferredcodec": AUDIO_FORMAT,
        }
        if AUDIO_BITRATE and AUDIO_BITRATE not in ["0", "best", "none"]:
            postprocessor["preferredquality"] = str(AUDIO_BITRATE)
        else:
            postprocessor["preferredquality"] = "0"

        # A. Download Worker (Producer)
        def download_worker_func():
            with tempfile.TemporaryDirectory(prefix=f"tg_dl_{job.job_id[:8]}_") as worker_temp_dir:
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

        # B. Upload & Archive Worker (Consumer)
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

                    # Jika target arsip disetel tapi gagal diarsipkan, jangan klaim berhasil
                    if ARCHIVE_CHANNEL_TARGET and not file_id:
                        job.failed_count += 1
                        fail_reason = "Gagal arsip ke channel"
                        job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": fail_reason})
                        update_progress_ui()
                        continue

                    # Simpan ke cache persisten
                    if file_id:
                        save_cached_track(video_id, track_title, job.artist_name, file_id, archive_msg_id, fsize)

                    # 2. PENGIRIMAN KE PENGGUNA
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
                            update_persistent_job_status(job.job_id, "running", delivered_count=job.delivered_count)
                            break
                        except Exception as user_send_err:
                            logger.warning("Gagal kirim ke pengguna [%s] (percobaan %d): %s", track_title, attempt, user_send_err)
                            if attempt == MAX_RETRIES:
                                job.failed_count += 1
                                job.failed_tracks.append({"title": track_title, "videoId": video_id, "reason": f"Gagal kirim Telegram: {str(user_send_err)[:50]}"})
                            time.sleep(1.5 * attempt)

                    update_progress_ui()

                finally:
                    try:
                        if file_path and os.path.exists(file_path):
                            os.remove(file_path)
                    except OSError:
                        pass
                    transfer_queue.task_done()

        # Jalankan thread pool worker
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
    final_status = "completed" if not job.cancel_event.is_set() else "cancelled"
    job.status = final_status
    update_persistent_job_status(job.job_id, final_status, finished_at=job.finished_at, delivered_count=job.delivered_count)
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

    desc = "Single Lagu" if job.job_type == "single" else "Katalog Artis"
    report_lines = [
        f"🎉 <b>{desc} Selesai Diproses!</b>",
        f"Judul/Artis: <b>{html.escape(artist_name)}</b>",
        f"⏱️ Waktu pengerjaan: {m}m {s}s\n",
        f"📊 <b>Ringkasan Pengiriman:</b>",
        f"• Total: <b>{total}</b> lagu",
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
            catalog_summary={"is_retry": True},
            job_type=job.job_type
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
# 7. HANDLER PERINTAH & PESAN TELEGRAM
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
        "👋 <b>Selamat Datang di Bot Downloader Musik Artis & Single Track!</b>\n\n"
        "Fitur utama:\n"
        "• <b>Pencarian Single Lagu:</b> Ketik <code>/song Judul Lagu</code> atau <code>/song Judul Artis</code>.\n"
        "• <b>Katalog Artis Penuh:</b> Ketik langsung nama artis (misal <code>Noah</code> atau <code>Peterpan</code>).\n"
        "• <b>Multi-Job Queue:</b> Tambahkan banyak artis/lagu sekaligus ke antrean tanpa saling menimpa!\n"
        "• <b>Auto-Resume & Smart Cache:</b> Resisten terhadap restart server dan pengiriman instan dari cache.\n\n"
        "Gunakan tombol di bawah atau ketik perintah bantuan untuk info lebih lanjut."
    )
    bot.reply_to(message, welcome_text, parse_mode="HTML", reply_markup=markup)

@bot.message_handler(commands=["song", "search"])
def handle_single_song_search_command(message: types.Message) -> None:
    """Menangani perintah /song dan /search untuk mencari 1 lagu spesifik."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        guide_text = (
            "💡 <b>Cara Menggunakan Pencarian Lagu:</b>\n\n"
            "Ketik perintah diikuti judul lagu atau judul + artis.\n\n"
            "<i>Contoh penggunaan:</i>\n"
            "• <code>/song Separuh Aku</code>\n"
            "• <code>/song Separuh Aku Noah</code>\n"
            "• <code>/song Hampa Ari Lasso</code>\n"
            "• <code>/search Dan Sheila On 7</code>\n\n"
            "Bot akan menampilkan daftar hasil untuk dipilih."
        )
        bot.reply_to(message, guide_text, parse_mode="HTML")
        return

    query = parts[1].strip()
    if len(query) > 120:
        bot.reply_to(message, "⚠️ Kata kunci pencarian terlalu panjang (maks 120 karakter).", parse_mode="HTML")
        return

    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else chat_id

    status_msg = bot.reply_to(
        message,
        f"🔍 <b>Mencari lagu:</b> <i>{html.escape(query)}</i>...",
        parse_mode="HTML"
    )

    try:
        results = search_single_songs(ytmusic, query, limit=8)
        if not results:
            bot.edit_message_text(
                f"❌ Tidak ditemukan lagu untuk '<b>{html.escape(query)}</b>'. Coba periksa ejaan judul atau nama artis.",
                chat_id=chat_id,
                message_id=status_msg.message_id,
                parse_mode="HTML"
            )
            return

        search_id = str(uuid.uuid4())
        session = SingleSearchSession(search_id, chat_id, user_id, query, results)
        with SEARCH_LOCK:
            SINGLE_SEARCH_SESSIONS[search_id] = session

        result_lines = [f"🔍 <b>Hasil Pencarian Lagu:</b> \"<i>{html.escape(query)}</i>\"\n"]
        for idx, r in enumerate(results, 1):
            dur_str = f" • {r['duration']}" if r.get('duration') and r['duration'] != '-' else ""
            alb_str = f" ({r['album']})" if r.get('album') and r['album'] != '-' else ""
            result_lines.append(f"<b>{idx}.</b> <b>{html.escape(r['title'])}</b> — <i>{html.escape(r['artist'])}</i>{alb_str}{dur_str}")

        result_lines.append("\n<i>Pilih nomor lagu yang ingin Anda unduh di bawah ini:</i>")

        # Tombol Inline Angka (1-8)
        markup = types.InlineKeyboardMarkup(row_width=4)
        number_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]
        btn_list = []
        for idx in range(len(results)):
            emoji_label = number_emojis[idx] if idx < len(number_emojis) else f"#{idx+1}"
            btn_list.append(types.InlineKeyboardButton(emoji_label, callback_data=f"pick_song:{search_id}:{idx}"))

        # Tambahkan tombol per baris 4 tombol
        for i in range(0, len(btn_list), 4):
            markup.row(*btn_list[i:i+4])

        markup.row(types.InlineKeyboardButton("❌ Batal Pencarian", callback_data=f"cancel_search:{search_id}"))

        bot.edit_message_text(
            "\n".join(result_lines),
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML",
            reply_markup=markup
        )

    except Exception as e:
        logger.exception("Gagal mencari lagu tunggal: %s", e)
        bot.edit_message_text(
            f"⚠️ Terjadi kesalahan saat mencari lagu: {html.escape(str(e))}",
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML"
        )

@bot.message_handler(commands=["queue", "status"])
def show_queue_status_command(message: types.Message) -> None:
    """Menampilkan status antrean saat ini."""
    status_text = JOB_MANAGER.get_status_summary()
    bot.reply_to(message, status_text, parse_mode="HTML")

@bot.message_handler(commands=["stats"])
def show_statistics_command(message: types.Message) -> None:
    """Menampilkan statistik database dan antrean."""
    stats = get_database_stats()
    uptime_sec = int(time.time() - START_TIME)
    uptime_h, rem = divmod(uptime_sec, 3600)
    uptime_m, uptime_s = divmod(rem, 60)

    stats_text = (
        "📊 <b>Statistik Bot & Database:</b>\n\n"
        f"• ⚡ Lagu Terarsip di Cache: <b>{stats['total_cached']}</b> lagu\n"
        f"• ✅ Pekerjaan Selesai: <b>{stats['total_completed']}</b>\n"
        f"• ❌ Pekerjaan Gagal: <b>{stats['total_failed']}</b>\n"
        f"• ⏳ Pekerjaan Aktif/Antre: <b>{stats['total_active']}</b>\n"
        f"• ⏱️ Uptime Bot: <b>{uptime_h}j {uptime_m}m {uptime_s}s</b>\n"
    )
    bot.reply_to(message, stats_text, parse_mode="HTML")

@bot.message_handler(commands=["health"])
def show_health_command(message: types.Message) -> None:
    """Menampilkan status kesehatan internal bot (worker, channel, database)."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id) and ADMIN_USER_IDS:
        bot.reply_to(message, "⛔ Perintah /health hanya dapat diakses oleh Admin bot.", parse_mode="HTML")
        return

    # Cek koneksi DB
    db_ok = False
    try:
        with DB_LOCK:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("SELECT 1").fetchone()
            conn.close()
        db_ok = True
    except Exception:
        db_ok = False

    archive_status = "✅ Aktif" if ARCHIVE_CHANNEL_TARGET else f"⚠️ Non-aktif ({ARCHIVE_CHANNEL_WARNING or 'belum diatur'})"

    health_text = (
        "🩺 <b>Status Kesehatan Sistem (Health Check):</b>\n\n"
        f"• <b>Database SQLite:</b> {'✅ Normal' if db_ok else '❌ Error'}\n"
        f"• <b>Download Workers:</b> {DOWNLOAD_WORKERS} thread\n"
        f"• <b>Upload Workers:</b> {UPLOAD_WORKERS} thread\n"
        f"• <b>Channel Arsip:</b> {archive_status}\n"
        f"• <b>Format Audio:</b> {AUDIO_FORMAT} (Bitrate: {AUDIO_BITRATE})\n"
        f"• <b>Active Threads:</b> {threading.active_count()} thread aktif\n"
    )
    bot.reply_to(message, health_text, parse_mode="HTML")

@bot.message_handler(commands=["cancel"])
def handle_cancel_command(message: types.Message) -> None:
    """Membatalkan job yang aktif atau job spesifik (/cancel <job_id>)."""
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else chat_id
    parts = message.text.split(maxsplit=1)
    target_job_id = parts[1].strip() if len(parts) > 1 else None

    cancelled, msg = JOB_MANAGER.cancel_job_for_chat(chat_id, user_id, target_job_id)
    if cancelled:
        bot.reply_to(message, f"🛑 <b>Pembatalan:</b> {html.escape(msg)}", parse_mode="HTML")
    else:
        bot.reply_to(message, f"ℹ️ {html.escape(msg)}", parse_mode="HTML")

@bot.message_handler(func=lambda msg: msg.text in ["🔍 Cara Pakai", "📋 Status Antrean", "❌ Batalkan"])
def handle_reply_keyboard(message: types.Message) -> None:
    """Menangani tombol reply keyboard navigasi utama."""
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else chat_id

    if message.text == "🔍 Cara Pakai":
        instructions = (
            "💡 <b>Panduan Penggunaan Bot:</b>\n\n"
            "1. <b>Download Satu Lagu:</b>\n"
            "   Gunakan perintah <code>/song Judul Lagu</code> atau <code>/song Judul Artis</code>.\n"
            "   <i>Contoh:</i> <code>/song Separuh Aku Noah</code>\n\n"
            "2. <b>Download Seluruh Katalog Artis:</b>\n"
            "   Ketik langsung nama artis tanpa garis miring.\n"
            "   <i>Contoh:</i> <code>Noah</code> atau <code>Peterpan</code>\n\n"
            "3. <b>Multi-Job Queue:</b>\n"
            "   Anda dapat mengirim lagu/artis baru saat unduhan sedang berjalan. "
            "Pekerjaan baru otomatis mengantre di urutan berikutnya tanpa saling menimpa!\n\n"
            "4. <b>Status & Batal:</b>\n"
            "   Klik '📋 Status Antrean' untuk cek progres, atau '❌ Batalkan' untuk berhenti."
        )
        bot.reply_to(message, instructions, parse_mode="HTML")

    elif message.text == "📋 Status Antrean":
        show_queue_status_command(message)

    elif message.text == "❌ Batalkan":
        with CONFIRMATION_LOCK:
            pending = PENDING_CONFIRMATION.pop(chat_id, None)

        cancelled, msg = JOB_MANAGER.cancel_job_for_chat(chat_id, user_id)
        if cancelled:
            bot.reply_to(message, f"🛑 <b>Pembatalan Berhasil:</b> {html.escape(msg)}", parse_mode="HTML")
        elif pending:
            bot.reply_to(message, "🚫 Sesi konfirmasi pencarian tertunda telah dibatalkan.", parse_mode="HTML")
        else:
            bot.reply_to(message, "ℹ️ Tidak ada antrean atau unduhan aktif untuk Anda.", parse_mode="HTML")

@bot.message_handler(func=lambda msg: True)
def process_artist_search_request(message: types.Message) -> None:
    """Menangani input nama artis dari pengguna untuk katalog penuh."""
    query = (message.text or "").strip()

    if not query or query.startswith("/"):
        return
    if len(query) > 120:
        bot.reply_to(message, "⚠️ Nama artis terlalu panjang (maksimal 120 karakter).", parse_mode="HTML")
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
            catalog_summary=summary,
            job_type="artist"
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
# 8. HANDLER CALLBACK INLINE (PILIH LAGU SINGLE / KONFIRMASI / BATAL)
# ==============================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("pick_song:"))
def handle_pick_single_song(call: types.CallbackQuery) -> None:
    """Menangani pemilihan satu lagu dari hasil perintah /song atau /search."""
    chat_id = call.message.chat.id
    user_id = call.from_user.id if call.from_user else chat_id
    parts = call.data.split(":")
    if len(parts) < 3:
        bot.answer_callback_query(call.id, "Data tombol tidak valid.")
        return

    search_id = parts[1]
    try:
        chosen_idx = int(parts[2])
    except ValueError:
        bot.answer_callback_query(call.id, "Indeks lagu tidak valid.")
        return

    with SEARCH_LOCK:
        session = SINGLE_SEARCH_SESSIONS.get(search_id)
        if not session:
            bot.answer_callback_query(call.id, "Sesi pencarian telah kedaluwarsa. Silakan cari ulang.", show_alert=True)
            try:
                bot.edit_message_text("⚠️ Sesi pencarian ini telah kedaluwarsa.", chat_id=chat_id, message_id=call.message.message_id)
            except Exception:
                pass
            return

        # Validasi bahwa yang menekan tombol adalah pengguna pemohon asli
        if session.requester_user_id != user_id and not is_admin(user_id):
            bot.answer_callback_query(call.id, "Tombol pencarian ini hanya dapat dipilih oleh pemohon asli.", show_alert=True)
            return

        if chosen_idx < 0 or chosen_idx >= len(session.results):
            bot.answer_callback_query(call.id, "Lagu yang dipilih tidak ditemukan.")
            return

        chosen_track = session.results[chosen_idx]
        SINGLE_SEARCH_SESSIONS.pop(search_id, None)

    # Buat DownloadJob untuk Single Track
    job_id = str(uuid.uuid4())
    job = DownloadJob(
        job_id=job_id,
        chat_id=chat_id,
        user_id=user_id,
        artist_name=f"{chosen_track['title']} - {chosen_track['artist']}",
        query=session.query,
        tracks=[chosen_track],
        catalog_summary={"is_single": True},
        job_type="single"
    )

    try:
        job.progress_msg_id = call.message.message_id
        pos = JOB_MANAGER.enqueue(job)
        bot.answer_callback_query(call.id, f"Lagu #{chosen_idx+1} dipilih!")

        dur_str = f" ({chosen_track['duration']})" if chosen_track.get('duration') and chosen_track['duration'] != '-' else ""
        if pos == 1:
            bot.edit_message_text(
                f"📥 <b>Memproses Single Lagu:</b>\n"
                f"🎵 <b>{html.escape(chosen_track['title'])}</b> — <i>{html.escape(chosen_track['artist'])}</i>{dur_str}\n\n"
                f"Sedang mengunduh dan mengarsipkan ke channel Telegram...",
                chat_id=chat_id,
                message_id=call.message.message_id,
                parse_mode="HTML"
            )
        else:
            bot.edit_message_text(
                f"⏳ <b>Single Lagu Masuk Antrean (Posisi #{pos}):</b>\n"
                f"🎵 <b>{html.escape(chosen_track['title'])}</b> — <i>{html.escape(chosen_track['artist'])}</i>{dur_str}\n\n"
                f"Akan diproses otomatis setelah pekerjaan aktif saat ini selesai.",
                chat_id=chat_id,
                message_id=call.message.message_id,
                parse_mode="HTML"
            )
    except ValueError as ve:
        bot.answer_callback_query(call.id, str(ve), show_alert=True)
    except Exception as e:
        logger.error("Gagal enqueue single song: %s", e)
        bot.answer_callback_query(call.id, "Gagal menambahkan lagu ke antrean.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_search:"))
def handle_cancel_search_inline(call: types.CallbackQuery) -> None:
    parts = call.data.split(":")
    search_id = parts[1] if len(parts) > 1 else None
    if search_id:
        with SEARCH_LOCK:
            SINGLE_SEARCH_SESSIONS.pop(search_id, None)

    try:
        bot.answer_callback_query(call.id, "Pencarian dibatalkan.")
        bot.edit_message_text("❌ Pencarian lagu dibatalkan.", chat_id=call.message.chat.id, message_id=call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_dl"))
def handle_cancel_inline(call: types.CallbackQuery) -> None:
    chat_id = call.message.chat.id
    user_id = call.from_user.id if call.from_user else chat_id
    with CONFIRMATION_LOCK:
        PENDING_CONFIRMATION.pop(chat_id, None)

    JOB_MANAGER.cancel_job_for_chat(chat_id, user_id)

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

    job.progress_msg_id = call.message.message_id
    try:
        position = JOB_MANAGER.enqueue(job)
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
    except ValueError as ve:
        bot.answer_callback_query(call.id, str(ve), show_alert=True)
    except Exception as e:
        logger.error("Gagal enqueue job: %s", e)
        bot.answer_callback_query(call.id, "Gagal memasukkan tugas ke antrean.")

# ==============================================================================
# 9. MAIN RUNNER DENGAN KONEKSI TELEGRAM TANGGUH
# ==============================================================================

def main() -> None:
    """Fungsi utama untuk menjalankan bot Telegram dengan toleransi timeout dan anti-bentrok."""
    logger.info("=" * 65)
    logger.info("🤖 Bot Telegram Musik Artis & Single Track Aktif...")
    logger.info("Konfigurasi Audio: format=%s, quality=%s, bitrate=%s", AUDIO_FORMAT, AUDIO_QUALITY, AUDIO_BITRATE)
    logger.info("Worker Pipeline: Download=%d, Upload=%d, MaxQueue=%d", DOWNLOAD_WORKERS, UPLOAD_WORKERS, MAX_QUEUE_SIZE)
    logger.info("Timeout Jaringan: BotApi=%ds, Polling=%ds", BOT_API_TIMEOUT, POLLING_TIMEOUT)
    if ARCHIVE_CHANNEL_TARGET:
        logger.info("Channel Pengarsipan Wajib: %s", ARCHIVE_CHANNEL_TARGET)
    else:
        logger.warning("Channel Pengarsipan Wajib: TIDAK AKTIF (%s)", ARCHIVE_CHANNEL_WARNING)
    logger.info("=" * 65)

    # Bersihkan webhook lama
    try:
        logger.info("Memeriksa status webhook dan membersihkan pending updates...")
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
        logger.info("Status webhook bersih. Memulai polling getUpdates...")
    except Exception as wh_err:
        logger.warning("Peringatan saat membersihkan webhook: %s", wh_err)

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
                logger.error("Terjadi galat API Telegram: %s. Reconnecting...", api_err)
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
