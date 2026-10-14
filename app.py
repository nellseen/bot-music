#!/usr/bin/env python3
"""
Telegram Artist Music Downloader Bot
====================================
Fitur Utama:
- Pencarian katalog artis lengkap YouTube Music (Songs, Albums, Singles & EPs).
- Pengambilan seluruh rilis tanpa batas buatan (mengatasi batasan API dan pagination).
- Deduplikasi cerdas berbasis videoId unik.
- Pengunduhan stabil via yt-dlp dan konversi MP3 via FFmpeg.
- Pengiriman file audio Telegram satu per satu lengkap dengan judul dan nama artis.
- Keamanan: Token disimpan dalam environment variable (TELEGRAM_BOT_TOKEN), tidak di-hardcode.
- Penanganan sesi, pencegahan eksekusi ganda, dan pembatalan kooperatif.
- Laporan progres berkala dan ringkasan akhir mendalam dengan opsi coba lagi (retry).
"""

import html
import logging
import os
import sys
import tempfile
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

# Muat environment variable dari .env jika ada
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

# Validasi Keamanan Token Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

if not TELEGRAM_BOT_TOKEN:
    logger.critical(
        "FATAL: Environment variable 'TELEGRAM_BOT_TOKEN' belum disetel!\n"
        "Silakan buat file '.env' atau ekspor environment variable:\n"
        "  export TELEGRAM_BOT_TOKEN='token_bot_anda'\n"
        "CATATAN KEAMANAN: Jika token lama Anda pernah terekspos di kode sumber publik,\n"
        "segera lakukan ROTASI token baru melalui @BotFather di Telegram."
    )
    # Jika dijalankan sebagai script mandiri tanpa token, tampilkan instruksi jelas lalu keluar
    if "pytest" not in sys.modules and "--dry-run" not in sys.argv:
        print("\n" + "=" * 70, file=sys.stderr)
        print("❌ KESALAHAN KONFIGURASI: TELEGRAM_BOT_TOKEN TIDAK DITEMUKAN!", file=sys.stderr)
        print("Harap setel token bot Telegram Anda di environment variable atau file .env.", file=sys.stderr)
        print("Contoh: TELEGRAM_BOT_TOKEN=\"123456789:ABCdefGHIjklMNOpqrsTUVwxyz\"", file=sys.stderr)
        print("=" * 70 + "\n", file=sys.stderr)
        sys.exit(1)

# Inisialisasi bot Telegram dan klien YouTube Music
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN or "123456789:AAH_DUMMY_TOKEN_FOR_INITIALIZATION")
ytmusic = YTMusic()

# Parameter kualitas dan batas download
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "192")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
MAX_TELEGRAM_FILE_SIZE = 49.5 * 1024 * 1024  # 49.5 MB (Batas aman API Telegram 50MB)
THROTTLE_PROGRESS_SECONDS = 6.0  # Jeda minimal edit pesan progres agar terhindar dari FloodWait 429

# ==============================================================================
# 2. PENGELOLAAN SESI & KONKURENSI
# ==============================================================================

class MusicSession:
    """Menyimpan status dan data antrean pencarian / download untuk pengguna."""

    def __init__(
        self,
        session_id: str,
        chat_id: int,
        artist_name: str,
        query: str,
        tracks: List[Dict[str, Any]],
        catalog_summary: Dict[str, Any],
    ):
        self.session_id: str = session_id
        self.chat_id: int = chat_id
        self.artist_name: str = artist_name
        self.query: str = query
        self.tracks: List[Dict[str, Any]] = tracks
        self.catalog_summary: Dict[str, Any] = catalog_summary
        self.created_at: float = time.time()
        self.is_downloading: bool = False
        self.cancel_event: threading.Event = threading.Event()
        self.failed_tracks: List[Dict[str, Any]] = []

# Registry sesi dengan thread-lock
SESSION_LOCK = threading.Lock()
PENDING_SESSIONS: Dict[int, MusicSession] = {}   # Menunggu konfirmasi: chat_id -> MusicSession
ACTIVE_DOWNLOADS: Dict[int, MusicSession] = {}   # Sedang mengunduh: chat_id -> MusicSession

def get_pending_session(chat_id: int) -> Optional[MusicSession]:
    with SESSION_LOCK:
        return PENDING_SESSIONS.get(chat_id)

def set_pending_session(chat_id: int, session: MusicSession) -> None:
    with SESSION_LOCK:
        PENDING_SESSIONS[chat_id] = session

def pop_pending_session(chat_id: int) -> Optional[MusicSession]:
    with SESSION_LOCK:
        return PENDING_SESSIONS.pop(chat_id, None)

def get_active_download(chat_id: int) -> Optional[MusicSession]:
    with SESSION_LOCK:
        return ACTIVE_DOWNLOADS.get(chat_id)

def set_active_download(chat_id: int, session: MusicSession) -> None:
    with SESSION_LOCK:
        ACTIVE_DOWNLOADS[chat_id] = session

def remove_active_download(chat_id: int) -> Optional[MusicSession]:
    with SESSION_LOCK:
        return ACTIVE_DOWNLOADS.pop(chat_id, None)

# ==============================================================================
# 3. MESIN PENCARIAN & AUDIT KATALOG ARTIS YOUTUBE MUSIC
# ==============================================================================

def _extract_all_items_by_key(data: Any, key: str) -> List[Any]:
    """Helper rekursif untuk mengekstrak seluruh node dengan key tertentu dari respons nested."""
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
    """
    Mengambil seluruh rilis (Albums atau Singles/EPs) milik seorang artis secara lengkap.
    Mengatasi anomali ytmusicapi di mana browseId bernilai None atau struktur respons YouTube
    Music menggunakan layout musicTwoRowItemRenderer yang tidak terduga.
    """
    releases: List[Dict[str, Any]] = []
    seen_browse_ids: Set[str] = set()

    # 1. Ambil rilis awal yang sudah disertakan langsung di artist_data
    for item in section_data.get("results", []):
        bid = item.get("browseId")
        if bid and bid not in seen_browse_ids and (bid.startswith("MPRE") or "title" in item):
            seen_browse_ids.add(bid)
            releases.append(item)

    # 2. Jika ada parameter pagination/view-all, jelajahi halaman katalog lengkapnya
    params = section_data.get("params")
    if params:
        # Coba metode standar library ytmusicapi terlebih dahulu
        try:
            full_list = yt.get_artist_albums(artist_id, params, limit=None)
            for item in full_list:
                bid = item.get("browseId")
                if bid and bid not in seen_browse_ids:
                    seen_browse_ids.add(bid)
                    releases.append(item)
        except Exception as e:
            logger.debug("get_artist_albums standar gagal (%s) untuk %s, beralih ke parser browse langsung...", e, section_name)
            # Fallback ke pemanggilan langsung endpoint 'browse' YouTube Music
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
                    # Pastikan rilis adalah album/single nyata (dimulai dengan MPRE atau pageType ALBUM)
                    if bid and (bid.startswith("MPRE") or page_type == "MUSIC_PAGE_TYPE_ALBUM"):
                        if bid not in seen_browse_ids:
                            title = nav(it, ["title", "runs", 0, "text"], True) or "Tanpa Judul"
                            seen_browse_ids.add(bid)
                            releases.append({"title": title, "browseId": bid})
            except Exception as e2:
                logger.warning("Gagal mengambil rilis tambahan untuk %s: %s", section_name, e2)

    return releases

def crawl_artist_catalog(yt: YTMusic, query: str) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Melakukan penjelajahan menyeluruh terhadap katalog lagu seorang artis.
    Mencakup:
    1. Songs (Playlist lagu populer/semua lagu artis).
    2. Albums (Seluruh album studio, live, dan kompilasi).
    3. Singles & EPs (Seluruh rilis single dan mini-album).
    4. Deduplikasi videoId.
    5. Fallback transparan jika artis tidak memiliki entri direktori resmi.
    """
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

    # 1. Cari profil Artis di YouTube Music
    artist_results = yt.search(query, filter="artists", limit=5)
    selected_artist = None

    if artist_results:
        # Prioritaskan kecocokan nama persis (case-insensitive) jika ada
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
        logger.info("Mengumpulkan katalog resmi untuk artis: '%s' (ID: %s)", artist_name, artist_id)

        try:
            artist_data = yt.get_artist(artist_id)
        except Exception as e:
            logger.error("Gagal memanggil get_artist(%s): %s", artist_id, e)
            artist_data = {}

        # A. Pengumpulan dari Bagian 'Songs' (Playlist Lagu)
        songs_section = artist_data.get("songs", {})
        # Tambahkan lagu dari preview 'results'
        for track in songs_section.get("results", []):
            vid = track.get("videoId")
            title = track.get("title")
            if vid and title and vid not in seen_ids:
                seen_ids.add(vid)
                tracks_to_download.append({"title": title, "videoId": vid, "source": "Songs"})
                summary["songs_count"] += 1

        # Tambahkan lagu dari playlist 'browseId' jika tersedia
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
                logger.warning("Error saat mengambil playlist lagu artis (%s): %s", songs_browse_id, e)

        # B. Pengumpulan dari SEMUA Album
        albums_section = artist_data.get("albums", {})
        albums_list = get_artist_releases_robust(yt, artist_id, "albums", albums_section)
        summary["albums_count"] = len(albums_list)
        logger.info("Ditemukan %d album untuk %s. Mengambil trek...", len(albums_list), artist_name)

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
                logger.warning("Gagal mengambil trek untuk album '%s' (%s): %s", alb_title, alb_id, e)

        # C. Pengumpulan dari SEMUA Singles & EPs
        singles_section = artist_data.get("singles", {})
        singles_list = get_artist_releases_robust(yt, artist_id, "singles", singles_section)
        summary["singles_count"] = len(singles_list)
        logger.info("Ditemukan %d singles & EPs untuk %s. Mengambil trek...", len(singles_list), artist_name)

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
                logger.warning("Gagal mengambil trek untuk single/EP '%s' (%s): %s", sgl_title, sgl_id, e)

    # 2. Pencarian Cadangan (Fallback) jika bukan artis atau tidak ada lagu di katalog resmi
    if not tracks_to_download:
        summary["is_fallback"] = True
        logger.info("Katalog artis resmi tidak ditemukan atau kosong untuk '%s'. Melakukan pencarian trek umum...", query)
        try:
            # Cari lagu dengan filter 'songs' agar terfokus pada musik, bukan video acak
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
            logger.error("Gagal melakukan pencarian cadangan umum untuk '%s': %s", query, e)

    summary["unique_total"] = len(tracks_to_download)
    logger.info("Total %d lagu unik dikumpulkan untuk '%s'.", len(tracks_to_download), artist_name)
    return artist_name, tracks_to_download, summary

# ==============================================================================
# 4. MESIN PENGUNDUHAN & PENGIRIMAN AUDIO TELEGRAM (WORKER THREAD)
# ==============================================================================

def execute_download_worker(session: MusicSession, initial_msg_id: int) -> None:
    """
    Worker yang mengeksekusi pengunduhan dan pengiriman lagu satu per satu ke chat Telegram.
    Dijalankan di thread terpisah agar proses polling Telegram tidak terblokir.
    """
    chat_id = session.chat_id
    artist_name = session.artist_name
    tracks = session.tracks
    total = len(tracks)

    logger.info("Memulai antrean pengunduhan untuk chat_id %d (%d lagu untuk '%s').", chat_id, total, artist_name)

    success_count = 0
    failed_count = 0
    skipped_count = 0
    start_time = time.time()
    last_progress_edit = time.time()

    # Buat direktori sementara terisolasi untuk sesi ini
    with tempfile.TemporaryDirectory(prefix=f"tg_bot_dl_{chat_id}_") as temp_dir:
        ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": AUDIO_BITRATE,
            }],
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "socket_timeout": 30,
            "retries": 3,
            "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
        }

        # Gunakan satu instance YoutubeDL yang dikelola dengan baik
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            for idx, track in enumerate(tracks, 1):
                # 1. Periksa apakah proses dibatalkan oleh pengguna
                if session.cancel_event.is_set():
                    logger.info("Pengunduhan untuk chat_id %d dibatalkan secara kooperatif pada lagu %d/%d.", chat_id, idx, total)
                    try:
                        bot.send_message(
                            chat_id,
                            f"🛑 <b>Pengunduhan Dibatalkan.</b>\n"
                            f"Proses dihentikan atas permintaan Anda pada lagu {idx}/{total}.\n"
                            f"Lagu yang sudah terkirim: {success_count}.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.error("Gagal mengirim pesan pembatalan: %s", e)
                    break

                track_title = track.get("title", f"Track {idx}")
                video_id = track.get("videoId")
                track_artist = track.get("artist") or artist_name
                track_url = f"https://www.youtube.com/watch?v={video_id}"

                # 2. Update progres berkala di Telegram (Throttled untuk mencegah rate-limit)
                now = time.time()
                if (now - last_progress_edit >= THROTTLE_PROGRESS_SECONDS) or (idx == 1):
                    last_progress_edit = now
                    try:
                        progress_text = (
                            f"📥 <b>Sedang Mengunduh Katalog: {html.escape(artist_name)}</b>\n\n"
                            f"🎵 <b>Lagu saat ini:</b> [{idx}/{total}] <i>{html.escape(track_title)}</i>\n"
                            f"✅ Berhasil dikirim: {success_count}\n"
                            f"❌ Gagal: {failed_count}\n"
                            f"⏭️ Dilewati: {skipped_count}\n\n"
                            f"<i>Ketik atau klik tombol '❌ Batalkan' kapan saja untuk menghentikan antrean.</i>"
                        )
                        bot.edit_message_text(
                            progress_text,
                            chat_id=chat_id,
                            message_id=initial_msg_id,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass  # Abaikan kesalahan pengeditan sementara

                # 3. Proses pengunduhan dengan sistem Retry bertahap
                mp3_file_path = None
                download_success = False
                failure_reason = "Kesalahan tidak diketahui"

                for attempt in range(1, MAX_RETRIES + 2):
                    if session.cancel_event.is_set():
                        break
                    try:
                        info = ydl.extract_info(track_url, download=True)
                        if info:
                            filename = ydl.prepare_filename(info)
                            base, _ = os.path.splitext(filename)
                            candidate_mp3 = base + ".mp3"
                            if os.path.exists(candidate_mp3):
                                mp3_file_path = candidate_mp3
                                download_success = True
                                break
                    except Exception as err:
                        failure_reason = str(err)
                        logger.warning(
                            "Percobaan %d/%d gagal untuk [%s] (%s): %s",
                            attempt, MAX_RETRIES + 1, track_title, video_id, err
                        )
                        if attempt <= MAX_RETRIES:
                            time.sleep(2.0 * attempt)

                if session.cancel_event.is_set():
                    if mp3_file_path and os.path.exists(mp3_file_path):
                        try:
                            os.remove(mp3_file_path)
                        except OSError:
                            pass
                    break

                # 4. Validasi file dan pengiriman ke Telegram
                if download_success and mp3_file_path and os.path.exists(mp3_file_path):
                    try:
                        file_size = os.path.getsize(mp3_file_path)

                        # Cek batasan ukuran file Telegram Bot API (50 MB)
                        if file_size > MAX_TELEGRAM_FILE_SIZE:
                            skipped_count += 1
                            reason_msg = f"Ukuran file ({file_size / (1024*1024):.1f} MB) melebihi batas Telegram (50MB)"
                            logger.warning("Lagu [%s] dilewati: %s", track_title, reason_msg)
                            session.failed_tracks.append({
                                "title": track_title,
                                "videoId": video_id,
                                "reason": reason_msg
                            })
                        else:
                            # Kirim Audio ke Pengguna Telegram
                            with open(mp3_file_path, "rb") as audio_fh:
                                caption_text = f"🎵 [{idx}/{total}] {track_title}"
                                bot.send_audio(
                                    chat_id=chat_id,
                                    audio=audio_fh,
                                    title=track_title,
                                    performer=track_artist,
                                    caption=caption_text,
                                    timeout=90
                                )
                            success_count += 1
                            logger.info("Berhasil mengirim [%d/%d]: %s", idx, total, track_title)
                    except Exception as send_err:
                        failed_count += 1
                        err_str = str(send_err)
                        logger.error("Gagal mengirim audio Telegram [%s]: %s", track_title, err_str)
                        session.failed_tracks.append({
                            "title": track_title,
                            "videoId": video_id,
                            "reason": f"Gagal kirim Telegram: {err_str[:80]}"
                        })
                    finally:
                        # Bersihkan file MP3 segera setelah diproses
                        try:
                            if os.path.exists(mp3_file_path):
                                os.remove(mp3_file_path)
                        except OSError:
                            pass
                else:
                    failed_count += 1
                    logger.error("Gagal mengunduh lagu [%s] setelah retry: %s", track_title, failure_reason)
                    session.failed_tracks.append({
                        "title": track_title,
                        "videoId": video_id,
                        "reason": f"Unduhan gagal: {failure_reason[:80]}"
                    })

    # 5. Pembersihan status aktif & Laporan Akhir
    remove_active_download(chat_id)
    duration_sec = int(time.time() - start_time)
    minutes, seconds = divmod(duration_sec, 60)

    if session.cancel_event.is_set():
        return

    # Buat Laporan Akhir yang Rapi
    report_lines = [
        f"🎉 <b>Proses Unduhan Selesai!</b>",
        f"Artis: <b>{html.escape(artist_name)}</b>",
        f"⏱️ Waktu pengerjaan: {minutes}m {seconds}s\n",
        f"📊 <b>Ringkasan Pengiriman:</b>",
        f"• Total Lagu: <b>{total}</b>",
        f"• ✅ Berhasil Dikirim: <b>{success_count}</b>",
        f"• ❌ Gagal: <b>{failed_count}</b>",
        f"• ⏭️ Dilewati (>50MB): <b>{skipped_count}</b>"
    ]

    retry_markup = None
    if session.failed_tracks:
        report_lines.append(f"\n⚠️ <b>Daftar Lagu Gagal ({len(session.failed_tracks)}):</b>")
        for ft in session.failed_tracks[:10]:
            report_lines.append(f"- <i>{html.escape(ft['title'])}</i> ({html.escape(ft['reason'])})")
        if len(session.failed_tracks) > 10:
            report_lines.append(f"<i>...dan {len(session.failed_tracks) - 10} lagu lainnya.</i>")

        # Simpan sesi perbaikan untuk tombol retry
        set_pending_session(chat_id, MusicSession(
            session_id=str(uuid.uuid4()),
            chat_id=chat_id,
            artist_name=artist_name,
            query=session.query,
            tracks=session.failed_tracks,
            catalog_summary={"is_retry": True, "unique_total": len(session.failed_tracks)}
        ))
        retry_markup = types.InlineKeyboardMarkup()
        btn_retry = types.InlineKeyboardButton(
            f"🔄 Coba Lagi Lagu Gagal ({len(session.failed_tracks)})",
            callback_data="confirm_dl"
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
# 5. HANDLER PERINTAH & PESAN TELEGRAM
# ==============================================================================

@bot.message_handler(commands=["start", "help"])
def send_welcome(message: types.Message) -> None:
    """Menampilkan pesan sambutan awal dan tombol keyboard kontrol."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    btn_help = types.KeyboardButton("🔍 Cara Pakai")
    btn_cancel = types.KeyboardButton("❌ Batalkan")
    markup.add(btn_help, btn_cancel)

    welcome_text = (
        "👋 <b>Selamat Datang di Bot Downloader Musik Artis!</b>\n\n"
        "Kirimkan nama artis untuk menyapu dan mengunduh seluruh katalog lagu mereka "
        "langsung ke Telegram dalam format MP3 berkualitas tinggi.\n\n"
        "💡 <i>Contoh pencarian:</i> <code>Noah</code> atau <code>Peterpan</code>\n\n"
        "Gunakan menu di bawah untuk bantuan atau pembatalan."
    )
    bot.reply_to(message, welcome_text, parse_mode="HTML", reply_markup=markup)

@bot.message_handler(func=lambda msg: msg.text in ["🔍 Cara Pakai", "❌ Batalkan"])
def handle_reply_keyboard(message: types.Message) -> None:
    """Menangani tombol reply keyboard navigasi utama."""
    chat_id = message.chat.id

    if message.text == "🔍 Cara Pakai":
        instructions = (
            "💡 <b>Cara Menggunakan Bot:</b>\n\n"
            "1. <b>Ketik Nama Artis</b> langsung di kolom chat (contoh: <code>Noah</code>, <code>Coldplay</code>, <code>Sheila On 7</code>).\n"
            "2. Bot akan menyapu YouTube Music untuk menemukan <b>Songs</b>, seluruh <b>Album</b>, serta seluruh <b>Singles & EP</b>.\n"
            "3. Bot akan menampilkan jumlah lagu unik yang ditemukan dan rincian albumnya.\n"
            "4. Klik tombol inline <b>'✅ Ya, Download'</b> untuk memulai proses.\n"
            "5. Seluruh lagu akan dikirim bertahap sebagai file audio Telegram.\n"
            "6. Jika ingin berhenti di tengah jalan, cukup klik <b>'❌ Batalkan'</b>."
        )
        bot.reply_to(message, instructions, parse_mode="HTML")

    elif message.text == "❌ Batalkan":
        # 1. Batalkan unduhan aktif jika ada
        active = get_active_download(chat_id)
        if active:
            active.cancel_event.set()
            remove_active_download(chat_id)
            bot.reply_to(
                message,
                "🛑 <b>Sinyal pembatalan dikirim.</b> Pengunduhan sedang dihentikan secara aman...",
                parse_mode="HTML"
            )
            return

        # 2. Hapus sesi konfirmasi yang belum dijalankan
        pending = pop_pending_session(chat_id)
        if pending:
            bot.reply_to(message, "🚫 Sesi pencarian tertunda telah dibatalkan.", parse_mode="HTML")
        else:
            bot.reply_to(message, "ℹ️ Tidak ada sesi pencarian atau unduhan yang sedang aktif.", parse_mode="HTML")

@bot.message_handler(func=lambda msg: True)
def process_artist_search_request(message: types.Message) -> None:
    """Menangani input teks nama artis dari pengguna."""
    query = (message.text or "").strip()

    # Validasi input
    if not query or query.startswith("/"):
        return
    if len(query) > 120:
        bot.reply_to(message, "⚠️ Nama artis terlalu panjang. Silakan masukkan nama yang lebih ringkas.", parse_mode="HTML")
        return

    chat_id = message.chat.id

    # Cegah pengguna memicu proses baru jika masih ada download aktif
    active = get_active_download(chat_id)
    if active and not active.cancel_event.is_set():
        bot.reply_to(
            message,
            f"⚠️ <b>Pengunduhan Masih Berjalan!</b>\n"
            f"Anda sedang mengunduh lagu untuk <b>{html.escape(active.artist_name)}</b>.\n"
            f"Harap tunggu hingga selesai atau klik tombol <b>'❌ Batalkan'</b> terlebih dahulu.",
            parse_mode="HTML"
        )
        return

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

        # Buat sesi baru
        session_id = str(uuid.uuid4())
        session = MusicSession(
            session_id=session_id,
            chat_id=chat_id,
            artist_name=artist_name,
            query=query,
            tracks=tracks,
            catalog_summary=summary
        )
        set_pending_session(chat_id, session)

        # Buat Inline Keyboard Konfirmasi
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

        # Rincian informasi katalog untuk transparansi kepada pengguna
        catalog_info = []
        if summary.get("is_official_artist"):
            catalog_info.append(f"⭐ <b>Profil Artis Resmi:</b> {html.escape(artist_name)}")
            if summary.get("songs_count"):
                catalog_info.append(f"• Lagu dari Playlist Utama: <b>{summary['songs_count']}</b>")
            if summary.get("albums_count"):
                catalog_info.append(f"• Album Diproses: <b>{summary['albums_count']} album</b>")
            if summary.get("singles_count"):
                catalog_info.append(f"• Singles & EPs Diproses: <b>{summary['singles_count']} rilis</b>")
        else:
            catalog_info.append(f"⚠️ <i>Katalog profil artis resmi tidak ditemukan.</i>")
            catalog_info.append(f"Menjalankan hasil pencarian trek musik umum.")

        catalog_info_str = "\n".join(catalog_info)

        bot.edit_message_text(
            f"📦 <b>Katalog Ditemukan!</b>\n\n"
            f"{catalog_info_str}\n\n"
            f"🎯 <b>Total Lagu Unik:</b> <b>{total} lagu</b>\n\n"
            f"Apakah Anda ingin mulai mengunduh seluruh {total} lagu ini sekarang?",
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML",
            reply_markup=inline_markup
        )

    except Exception as e:
        logger.exception("Kesalahan sistem saat mencari musik: %s", e)
        bot.edit_message_text(
            f"⚠️ Terjadi kesalahan saat menjelajahi katalog: {html.escape(str(e))}",
            chat_id=chat_id,
            message_id=status_msg.message_id,
            parse_mode="HTML"
        )

# ==============================================================================
# 6. HANDLER CALLBACK INLINE BUTTON (KONFIRMASI / BATAL)
# ==============================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_dl"))
def handle_cancel_inline(call: types.CallbackQuery) -> None:
    """Menangani pembatalan melalui tombol inline."""
    chat_id = call.message.chat.id
    pop_pending_session(chat_id)

    # Cek jika ada unduhan aktif
    active = get_active_download(chat_id)
    if active:
        active.cancel_event.set()
        remove_active_download(chat_id)

    try:
        bot.answer_callback_query(call.id, "Proses telah dibatalkan.")
    except Exception:
        pass

    try:
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
    """Menangani konfirmasi pengunduhan melalui tombol inline."""
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    requested_session_id = parts[1] if len(parts) > 1 else None

    # Verifikasi sesi
    pending = get_pending_session(chat_id)
    if not pending:
        try:
            bot.answer_callback_query(call.id, "Sesi kedaluwarsa atau sudah diproses.", show_alert=True)
            bot.edit_message_text(
                "⚠️ <b>Sesi kedaluwarsa atau sudah diproses.</b>\nSilakan ketik nama artis lagi untuk memulai pencarian baru.",
                chat_id=chat_id,
                message_id=call.message.message_id,
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    # Validasi UUID sesi untuk mencegah klik ganda atau tombol kadaluarsa
    if requested_session_id and pending.session_id != requested_session_id:
        try:
            bot.answer_callback_query(call.id, "Tombol ini berasal dari pencarian lama.", show_alert=True)
        except Exception:
            pass
        return

    # Pindahkan sesi dari PENDING ke ACTIVE
    session = pop_pending_session(chat_id)
    if not session:
        return

    set_active_download(chat_id, session)

    try:
        bot.answer_callback_query(call.id, "Memulai antrean unduhan...")
    except Exception:
        pass

    total = len(session.tracks)
    try:
        bot.edit_message_text(
            f"📥 <b>Memulai Pengunduhan: {html.escape(session.artist_name)}</b>\n\n"
            f"Menyiapkan {total} lagu... Konversi MP3 dan pengiriman akan berjalan bertahap.\n"
            f"<i>Tekan '❌ Batalkan' di bawah kapan saja untuk menghentikan proses.</i>",
            chat_id=chat_id,
            message_id=call.message.message_id,
            parse_mode="HTML"
        )
    except Exception:
        pass

    # Jalankan worker di thread terpisah agar bot tetap responsif
    worker_thread = threading.Thread(
        target=execute_download_worker,
        args=(session, call.message.message_id),
        name=f"Worker-{chat_id}",
        daemon=True
    )
    worker_thread.start()

# ==============================================================================
# 7. MAIN RUNNER DENGAN AUTO-RECONNECT
# ==============================================================================

def main() -> None:
    """Fungsi utama untuk menjalankan bot Telegram."""
    logger.info("=" * 60)
    logger.info("🤖 Bot Telegram Musik Artis Aktif...")
    logger.info("Bitrate MP3: %s kbps | Max Retries: %d", AUDIO_BITRATE, MAX_RETRIES)
    logger.info("Menunggu pesan pengguna...")
    logger.info("=" * 60)

    # 1. Bersihkan webhook lama jika ada agar tidak bentrok dengan metode polling getUpdates
    try:
        logger.info("Memeriksa status webhook dan membersihkan pending updates...")
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
        logger.info("Status webhook bersih. Memulai polling getUpdates...")
    except Exception as wh_err:
        logger.warning("Peringatan saat memeriksa/menghapus webhook: %s", wh_err)

    # 2. Infinity polling dengan penanganan cerdas untuk error 409 Conflict
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20, skip_pending=True)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot dihentikan oleh pengguna.")
            break
        except telebot.apihelper.ApiTelegramException as api_err:
            if getattr(api_err, "error_code", None) == 409 or "Conflict" in str(api_err):
                logger.error(
                    "\n" + "!" * 70 + "\n"
                    "❌ TERJADI BENTROK TOKEN TELEGRAM (Error 409 Conflict: Terminated by other getUpdates)!\n"
                    "PENYEBAB: Ada instance bot lain yang sedang aktif berjalan menggunakan token yang sama.\n\n"
                    "SOLUSI MENGATASINYA:\n"
                    "1. Tutup / Stop proses script bot di terminal lain, tab lain, atau server lain.\n"
                    "   - Di Linux / Mac: jalankan `pkill -f app.py`\n"
                    "   - Di Windows: matikan proses Python di Task Manager atau tutup command prompt lain.\n"
                    "2. Jika token lama Anda pernah dipakai orang lain atau terekspos di publik:\n"
                    "   - Buka Telegram -> chat dengan @BotFather\n"
                    "   - Ketik /revoke lalu pilih bot Anda untuk mendapatkan TOKEN BARU.\n"
                    "   - Masukkan token baru tersebut ke file .env.\n"
                    "!" * 70 + "\n"
                )
                time.sleep(15)  # Beri jeda agar tidak terus-menerus bertabrakan request
            else:
                logger.error("Terjadi galat API Telegram: %s. Melakukan rekoneksi dalam 5 detik...", api_err)
                time.sleep(5)
        except Exception as e:
            if "Conflict" in str(e):
                logger.error("Terdeteksi konflik proses bot: %s. Jeda 15 detik sebelum mencoba lagi...", e)
                time.sleep(15)
            else:
                logger.error("Terjadi galat pada polling: %s. Melakukan rekoneksi dalam 5 detik...", e)
                time.sleep(5)

if __name__ == "__main__":
    main()

