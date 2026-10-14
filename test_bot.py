#!/usr/bin/env python3
"""
Comprehensive 14-Point Test Suite for Upgraded Telegram Music Bot
=================================================================
Menguji ke-14 skenario pengujian wajib:
1. Pencarian lagu hanya dengan judul (/song Separuh Aku).
2. Pencarian dengan judul + artis (/song Separuh Aku Noah).
3. Pemilihan hasil lagu menggunakan inline keyboard callback.
4. Pencarian tanpa hasil dan penanganan error API.
5. Penambahan single lagu saat job katalog artis sedang berjalan.
6. Penambahan beberapa job beruntun tanpa saling menimpa.
7. Pengarsipan wajib setiap lagu yang berhasil ke ARCHIVE_CHANNEL_ID.
8. Pengiriman ulang instan dari SQLite Cache (file_id) tanpa download ulang.
9. Sistem Retry saat download/upload mengalami error sementara.
10. Auto-Resume: pemulihan job tertunda saat bot restart / crash.
11. Proteksi callback kedaluwarsa atau dari pengguna tidak berwenang.
12. Konfigurasi timeout polling dan koneksi Telegram.
13. Validasi konfigurasi environment variable (.env).
14. Pemeriksaan sintaks Python, seluruh import, dan struktur dependensi.
"""

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

# Atur environment variable dummy untuk testing
os.environ["BOT_TOKEN"] = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz123456"
os.environ["ARCHIVE_CHANNEL_ID"] = "-1009876543210"
os.environ["LOG_LEVEL"] = "WARNING"
TEST_DB_FILE = os.path.join(tempfile.gettempdir(), f"test_music_cache_14_{os.getpid()}.db")
os.environ["DB_PATH"] = TEST_DB_FILE

import telebot
import app
app.DB_PATH = TEST_DB_FILE
from app import (
    DownloadJob,
    JobQueueManager,
    SingleSearchSession,
    auto_resume_uncompleted_jobs,
    crawl_artist_catalog,
    execute_high_throughput_pipeline,
    get_cached_track,
    get_uncompleted_persistent_jobs,
    init_db,
    save_cached_track,
    save_persistent_job_to_db,
    search_single_songs,
    validate_channel_id,
)


class Test14PointTelegramMusicBot(unittest.TestCase):

    def setUp(self):
        # Bersihkan dan inisialisasi ulang database SQLite test
        init_db()
        with app.CONFIRMATION_LOCK:
            app.PENDING_CONFIRMATION.clear()
        with app.SEARCH_LOCK:
            app.SINGLE_SEARCH_SESSIONS.clear()

    def test_01_syntax_imports_and_dependencies(self):
        """1. Sintaks seluruh file, fungsi baru, dan dependensi valid."""
        self.assertTrue(callable(app.search_single_songs))
        self.assertTrue(callable(app.handle_pick_single_song))
        self.assertTrue(callable(app.auto_resume_uncompleted_jobs))
        self.assertTrue(callable(app.crawl_artist_catalog))
        self.assertTrue(callable(app.execute_high_throughput_pipeline))
        self.assertIsNotNone(app.JOB_MANAGER)

    def test_02_search_song_title_only(self):
        """2. Pencarian lagu hanya dengan judul (/song Separuh Aku)."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = [
            {"title": "Separuh Aku", "videoId": "VID_01", "artists": [{"name": "NOAH"}], "album": {"name": "Seperti Seharusnya"}, "duration": "4:27"},
            {"title": "Separuh", "videoId": "VID_02", "artists": [{"name": "Jemimah"}], "album": {"name": "Single"}, "duration": "4:18"},
        ]

        results = search_single_songs(mock_yt, "Separuh Aku", limit=5)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["title"], "Separuh Aku")
        self.assertEqual(results[0]["artist"], "NOAH")
        self.assertEqual(results[0]["videoId"], "VID_01")
        self.assertEqual(results[0]["duration"], "4:27")

    def test_03_search_song_title_and_artist(self):
        """3. Pencarian lagu dengan judul + artis (/song Separuh Aku Noah)."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = [
            {"title": "Separuh Aku", "videoId": "VID_NOAH", "artists": [{"name": "NOAH"}], "album": {"name": "Seperti Seharusnya"}, "duration": "4:27"},
        ]

        results = search_single_songs(mock_yt, "Separuh Aku Noah", limit=5)
        self.assertEqual(len(results), 1)
        self.assertIn("NOAH", results[0]["artist"])
        self.assertEqual(results[0]["videoId"], "VID_NOAH")

    def test_04_search_empty_and_api_error_handling(self):
        """4. Pencarian tanpa hasil dan penanganan error API."""
        mock_yt = MagicMock()
        # Kasus A: Hasil kosong
        mock_yt.search.return_value = []
        empty_res = search_single_songs(mock_yt, "QueryLaguTidakAda999", limit=5)
        self.assertEqual(len(empty_res), 0)

        # Kasus B: API melempar Exception
        mock_yt.search.side_effect = RuntimeError("YouTube Music API connection timeout")
        err_res = search_single_songs(mock_yt, "ErrorQuery", limit=5)
        self.assertEqual(len(err_res), 0)

    def test_05_inline_keyboard_pick_song_callback(self):
        """5. Pemilihan hasil menggunakan inline keyboard callback."""
        search_id = "test_search_123"
        results = [
            {"title": "Hampa", "artist": "Ari Lasso", "videoId": "VID_HAMPA", "duration": "4:50", "album": "Keseimbangan"}
        ]
        session = SingleSearchSession(search_id, chat_id=101, requester_user_id=555, query="Hampa Ari Lasso", results=results)
        with app.SEARCH_LOCK:
            app.SINGLE_SEARCH_SESSIONS[search_id] = session

        call = MagicMock()
        call.message.chat.id = 101
        call.message.message_id = 888
        call.from_user.id = 555
        call.data = f"pick_song:{search_id}:0"

        with patch.object(app.JOB_MANAGER, "enqueue", return_value=1) as mock_enqueue:
            with patch("app.bot") as mock_bot:
                app.handle_pick_single_song(call)
                mock_enqueue.assert_called_once()
                enqueued_job = mock_enqueue.call_args[0][0]
                self.assertEqual(enqueued_job.job_type, "single")
                self.assertEqual(enqueued_job.tracks[0]["videoId"], "VID_HAMPA")
                mock_bot.answer_callback_query.assert_called_with(call.id, "Lagu #1 dipilih!")

    def test_06_unauthorized_user_callback_protection(self):
        """6. Proteksi callback: pengguna lain tidak boleh memilih dari sesi privat pengguna lain."""
        search_id = "test_search_priv"
        session = SingleSearchSession(search_id, chat_id=102, requester_user_id=111, query="Test", results=[{"title": "T", "videoId": "V"}])
        with app.SEARCH_LOCK:
            app.SINGLE_SEARCH_SESSIONS[search_id] = session

        call = MagicMock()
        call.from_user.id = 999  # Pengguna berbeda!
        call.data = f"pick_song:{search_id}:0"

        with patch("app.bot") as mock_bot:
            app.handle_pick_single_song(call)
            mock_bot.answer_callback_query.assert_called_with(
                call.id, "Tombol pencarian ini hanya dapat dipilih oleh pemohon asli.", show_alert=True
            )

    def test_07_enqueue_single_while_artist_running(self):
        """7. Penambahan single lagu saat job artis sedang berjalan (antrean terpadu)."""
        manager = JobQueueManager()

        job_artist = DownloadJob(
            job_id="job_artist_noah",
            chat_id=201,
            user_id=201,
            artist_name="Noah",
            query="Noah",
            tracks=[{"title": "Separuh Aku", "videoId": "VID_1"}],
            job_type="artist"
        )
        job_single = DownloadJob(
            job_id="job_single_peterpan",
            chat_id=202,
            user_id=202,
            artist_name="Mimpi Yang Sempurna",
            query="Mimpi Yang Sempurna",
            tracks=[{"title": "Mimpi Yang Sempurna", "videoId": "VID_2"}],
            job_type="single"
        )

        with patch.object(manager, "_run_job_pipeline"):
            pos1 = manager.enqueue(job_artist)
            self.assertEqual(pos1, 1)
            self.assertEqual(manager.active_job.job_type, "artist")

            # Single lagu ditambahkan saat artis aktif
            pos2 = manager.enqueue(job_single)
            self.assertEqual(pos2, 2)
            self.assertEqual(manager.active_job.artist_name, "Noah")
            self.assertEqual(len(manager.pending_queue), 1)
            self.assertEqual(manager.pending_queue[0].job_type, "single")

    def test_08_multiple_jobs_no_overwriting(self):
        """8. Penambahan beberapa job tanpa saling menimpa."""
        manager = JobQueueManager()
        jobs = [
            DownloadJob(f"j_{i}", chat_id=i, user_id=i, artist_name=f"Artis {i}", query="", tracks=[])
            for i in range(1, 4)
        ]

        with patch.object(manager, "_run_job_pipeline"):
            for j in jobs:
                manager.enqueue(j)

            self.assertEqual(manager.active_job.job_id, "j_1")
            self.assertEqual(len(manager.pending_queue), 2)
            self.assertEqual(manager.pending_queue[0].job_id, "j_2")
            self.assertEqual(manager.pending_queue[1].job_id, "j_3")

    def test_09_mandatory_channel_archive(self):
        """9. Pengarsipan wajib: lagu dikirim ke ARCHIVE_CHANNEL_ID (channel only)."""
        job = DownloadJob(
            job_id="job_arc_09",
            chat_id=301,
            user_id=301,
            artist_name="Artis Arsip 9",
            query="",
            tracks=[{"title": "Lagu Arsip", "videoId": "VID_ARC_09"}]
        )

        mock_bot = MagicMock()
        mock_arc_msg = MagicMock()
        mock_arc_msg.message_id = 777
        mock_arc_msg.audio.file_id = "TG_FILE_ID_09"
        mock_bot.send_audio.return_value = mock_arc_msg

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_ARC_09.mp3")
            with open(tmp_mp3, "wb") as f:
                f.write(b"AUDIO")
            instance.extract_info.return_value = {"id": "VID_ARC_09", "ext": "mp3"}
            instance.prepare_filename.return_value = tmp_mp3

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -1009876543210):
                    with patch("app.SEND_AUDIO_TO_USER", False):
                        execute_high_throughput_pipeline(job)

        # Sesuai "lagu cukup di kirim ke ch gausah ke bot", send_audio hanya 1x ke channel
        self.assertEqual(mock_bot.send_audio.call_count, 1)
        self.assertEqual(mock_bot.send_audio.call_args.kwargs["chat_id"], -1009876543210)
        self.assertEqual(job.archived_count, 1)
        self.assertEqual(job.delivered_count, 1)

    def test_09b_optional_user_delivery(self):
        """9b. Pengiriman ganda ke channel DAN user saat SEND_AUDIO_TO_USER=True."""
        job = DownloadJob(
            job_id="job_arc_09b",
            chat_id=302,
            user_id=302,
            artist_name="Artis Dual",
            query="",
            tracks=[{"title": "Lagu Dual", "videoId": "VID_ARC_09B"}]
        )

        mock_bot = MagicMock()
        mock_arc_msg = MagicMock()
        mock_arc_msg.message_id = 778
        mock_arc_msg.audio.file_id = "TG_FILE_ID_09B"
        mock_bot.send_audio.side_effect = [mock_arc_msg, MagicMock()]

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_ARC_09B.mp3")
            with open(tmp_mp3, "wb") as f:
                f.write(b"AUDIO")
            instance.extract_info.return_value = {"id": "VID_ARC_09B", "ext": "mp3"}
            instance.prepare_filename.return_value = tmp_mp3

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -1009876543210):
                    with patch("app.SEND_AUDIO_TO_USER", True):
                        execute_high_throughput_pipeline(job)

        self.assertEqual(mock_bot.send_audio.call_count, 2)
        self.assertEqual(mock_bot.send_audio.call_args_list[0].kwargs["chat_id"], -1009876543210)
        self.assertEqual(mock_bot.send_audio.call_args_list[1].kwargs["chat_id"], 302)

    def test_10_cache_instant_resend(self):
        """10. Pengiriman ulang instan dari cache SQLite (file_id) ke channel tanpa download ulang."""
        save_cached_track("VID_FAST_10", "Lagu Instan", "Artis Cepat", "FILE_ID_INSTAN", 123, 500000)

        job = DownloadJob(
            job_id="job_fast_10",
            chat_id=401,
            user_id=401,
            artist_name="Artis Cepat",
            query="",
            tracks=[{"title": "Lagu Instan", "videoId": "VID_FAST_10"}]
        )

        mock_bot = MagicMock()
        with patch("yt_dlp.YoutubeDL") as MockYDL:
            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -1009876543210):
                    with patch("app.SEND_AUDIO_TO_USER", False):
                        execute_high_throughput_pipeline(job)
            MockYDL.assert_not_called()  # Tidak ada unduh ulang!

        mock_bot.send_audio.assert_called_once()
        self.assertEqual(mock_bot.send_audio.call_args.kwargs["chat_id"], -1009876543210)
        self.assertEqual(mock_bot.send_audio.call_args.kwargs["audio"], "FILE_ID_INSTAN")
        self.assertEqual(job.cached_count, 1)
        self.assertEqual(job.archived_count, 1)

    def test_11_retry_mechanism(self):
        """11. Sistem Retry: percobaan ulang saat download atau upload mengalami error sementara."""
        job = DownloadJob(
            job_id="job_retry_11",
            chat_id=501,
            user_id=501,
            artist_name="Artis Retry",
            query="",
            tracks=[{"title": "Lagu Retry", "videoId": "VID_RETRY_11"}]
        )

        mock_bot = MagicMock()
        mock_arc = MagicMock()
        mock_arc.message_id = 999
        mock_arc.audio.file_id = "RETRY_FILE_ID"
        mock_bot.send_audio.side_effect = [mock_arc, MagicMock()]

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            attempts = 0
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_RETRY_11.mp3")

            def fake_extract_info(url, download=True):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise ConnectionResetError("Jaringan terputus sementara")
                with open(tmp_mp3, "wb") as f:
                    f.write(b"AUDIO")
                return {"id": "VID_RETRY_11", "ext": "mp3"}

            instance.extract_info.side_effect = fake_extract_info
            instance.prepare_filename.return_value = tmp_mp3

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -10012345):
                    execute_high_throughput_pipeline(job)

        self.assertGreaterEqual(attempts, 2)
        self.assertEqual(job.delivered_count, 1)

    def test_12_auto_resume_after_bot_restart(self):
        """12. Auto-Resume: pemulihan job tertunda saat bot restart / crash."""
        job_crashed = DownloadJob(
            job_id="job_crash_12",
            chat_id=601,
            user_id=601,
            artist_name="Artis Crash",
            query="Artis Crash",
            tracks=[{"title": "Lagu Tertunda", "videoId": "VID_RESUME_12"}],
            job_type="single"
        )
        job_crashed.status = "running"
        save_persistent_job_to_db(job_crashed)

        # Cek database mendeteksi job uncompleted
        uncompleted = get_uncompleted_persistent_jobs()
        self.assertTrue(any(j["job_id"] == "job_crash_12" for j in uncompleted))

        # Simulasi restart dan auto_resume
        with patch.object(app.JOB_MANAGER, "enqueue") as mock_enqueue:
            auto_resume_uncompleted_jobs()
            mock_enqueue.assert_called()
            resumed_job = mock_enqueue.call_args[0][0]
            self.assertEqual(resumed_job.job_id, "job_crash_12")
            self.assertEqual(resumed_job.artist_name, "Artis Crash")

    def test_13_channel_id_env_validation(self):
        """13. Validasi konfigurasi ARCHIVE_CHANNEL_ID di .env."""
        # Kasus placeholder '-00' ditolak
        target_00, warn_00 = validate_channel_id("-00")
        self.assertIsNone(target_00)
        self.assertIn("placeholder", warn_00)

        # Kasus channel ID sah
        target_valid, warn_valid = validate_channel_id("-1001234567890")
        self.assertEqual(target_valid, -1001234567890)
        self.assertIsNone(warn_valid)

    def test_14_polling_timeouts_applied(self):
        """14. Parameter timeout diterapkan pada telebot.apihelper."""
        self.assertEqual(telebot.apihelper.CONNECT_TIMEOUT, app.BOT_API_TIMEOUT)
        self.assertEqual(telebot.apihelper.READ_TIMEOUT, app.BOT_API_TIMEOUT)
        self.assertEqual(telebot.apihelper.LONG_POLLING_TIMEOUT, app.LONG_POLLING_TIMEOUT)

    def test_15_colored_logging_formatter(self):
        """15. Verifikasi formatter logging berwarna ANSI dan penyorotan tag."""
        import logging
        formatter = app.ColoredFormatter()
        
        # Test INFO record dengan tag [CHANNEL] dan kutipan lagu
        record_info = logging.LogRecord(
            name="MusicBot",
            level=logging.INFO,
            pathname="app.py",
            lineno=100,
            msg="📢 [CHANNEL] Mengirim 'Separuh Aku' ke channel",
            args=(),
            exc_info=None
        )
        formatted_info = formatter.format(record_info)
        self.assertIn("\033[92m", formatted_info)  # ANSI Info green badge
        self.assertIn("[INFO   ]", formatted_info)
        self.assertIn("[CHANNEL]", formatted_info)
        self.assertIn("Separuh Aku", formatted_info)

        # Test ERROR record
        record_err = logging.LogRecord(
            name="MusicBot",
            level=logging.ERROR,
            pathname="app.py",
            lineno=105,
            msg="❌ [GAGAL] Unduhan terputus",
            args=(),
            exc_info=None
        )
        formatted_err = formatter.format(record_err)
        self.assertIn("\033[91m", formatted_err)  # ANSI Error red badge
        self.assertIn("[ERROR  ]", formatted_err)

    def test_16_send_audio_to_user_mode(self):
        """16. Verifikasi nilai default SEND_AUDIO_TO_USER adalah False (cukup ke CH)."""
        # Default harus False sesuai permintaan: "lagu cukup di kirim ke ch gausah ke bot"
        self.assertFalse(app.SEND_AUDIO_TO_USER)


if __name__ == "__main__":
    unittest.main(verbosity=2)
