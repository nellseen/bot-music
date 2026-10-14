#!/usr/bin/env python3
"""
Comprehensive Test Suite for Upgraded Telegram Music Bot
=========================================================
Menguji ke-10 persyaratan pengujian wajib:
1. Pemeriksaan sintaks Python dan seluruh dependensi import.
2. Pengujian Multi-Job Queue (Simulasi Noah lalu Peterpan saat Noah masih berjalan).
3. Pengujian Job baru tidak menimpa job aktif.
4. Pengujian pengarsipan wajib ke ARCHIVE_CHANNEL_ID sebelum dikirim ke user.
5. Pengujian pengiriman ulang instan dari SQLite Cache (file_id) tanpa download ulang.
6. Pengujian sistem Retry saat download atau upload mengalami error sementara.
7. Pengujian pembatalan kooperatif satu job tanpa merusak antrean pengguna lain.
8. Pengujian pembersihan file sementara (cleanup temp directory).
9. Pengujian validasi format ARCHIVE_CHANNEL_ID (mendeteksi placeholder '-00').
10. Pengujian konfigurasi timeout API dan polling Telegram.
"""

import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

# Atur environment variable dummy untuk pengujian
os.environ["BOT_TOKEN"] = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz123456"
os.environ["ARCHIVE_CHANNEL_ID"] = "-1009876543210"
os.environ["LOG_LEVEL"] = "WARNING"
TEST_DB_FILE = os.path.join(tempfile.gettempdir(), f"test_music_cache_{os.getpid()}.db")
os.environ["DB_PATH"] = TEST_DB_FILE

import telebot
import app
app.DB_PATH = TEST_DB_FILE
from app import (
    DownloadJob,
    JobQueueManager,
    crawl_artist_catalog,
    execute_high_throughput_pipeline,
    get_cached_track,
    init_db,
    save_cached_track,
    validate_channel_id,
)


class TestUpgradedTelegramMusicBot(unittest.TestCase):

    def setUp(self):
        # Inisialisasi DB SQLite in-memory untuk setiap pengujian
        init_db()
        # Bersihkan sesi dan antrean
        with app.CONFIRMATION_LOCK:
            app.PENDING_CONFIRMATION.clear()

    def test_01_syntax_and_imports(self):
        """1. Sintaks seluruh file dan import dependensi valid."""
        self.assertTrue(callable(app.crawl_artist_catalog))
        self.assertTrue(callable(app.execute_high_throughput_pipeline))
        self.assertTrue(callable(app.validate_channel_id))
        self.assertIsNotNone(app.JOB_MANAGER)

    def test_02_channel_id_validation(self):
        """2. Validasi ARCHIVE_CHANNEL_ID: deteksi placeholder '-00' dan validasi channel angka / @username."""
        # Kasus placeholder '-00'
        target, warning = validate_channel_id("-00")
        self.assertIsNone(target)
        self.assertIn("placeholder", warning)

        # Kasus kosong
        target_empty, warn_empty = validate_channel_id("")
        self.assertIsNone(target_empty)

        # Kasus valid angka -100...
        valid_target, warn_valid = validate_channel_id("-1001234567890")
        self.assertEqual(valid_target, -1001234567890)
        self.assertIsNone(warn_valid)

        # Kasus valid @username
        valid_user, warn_user = validate_channel_id("@musik_arsip_channel")
        self.assertEqual(valid_user, "@musik_arsip_channel")
        self.assertIsNone(warn_user)

    def test_03_multi_job_queue_noah_then_peterpan(self):
        """3. Simulasi permintaan Noah lalu Peterpan: Peterpan masuk antrean tanpa menimpa Noah."""
        manager = JobQueueManager()

        job_noah = DownloadJob(
            job_id="job_noah_1",
            chat_id=101,
            user_id=101,
            artist_name="Noah",
            query="Noah",
            tracks=[{"title": "Separuh Aku", "videoId": "VID_NOAH_1"}],
            catalog_summary={}
        )

        job_peterpan = DownloadJob(
            job_id="job_peterpan_2",
            chat_id=102,
            user_id=102,
            artist_name="Peterpan",
            query="Peterpan",
            tracks=[{"title": "Mimpi Yang Sempurna", "videoId": "VID_PP_1"}],
            catalog_summary={}
        )

        with patch.object(manager, "_run_job_pipeline"):
            pos1 = manager.enqueue(job_noah)
            self.assertEqual(pos1, 1)
            self.assertEqual(manager.active_job.artist_name, "Noah")

            # Saat Noah aktif, Peterpan ditambahkan
            pos2 = manager.enqueue(job_peterpan)
            self.assertEqual(pos2, 2)
            # Job aktif tetap Noah, Peterpan ada di pending queue
            self.assertEqual(manager.active_job.artist_name, "Noah")
            self.assertEqual(len(manager.pending_queue), 1)
            self.assertEqual(manager.pending_queue[0].artist_name, "Peterpan")

    def test_04_cooperative_cancellation_isolated(self):
        """4. Pembatalan satu job tidak merusak atau membatalkan job pengguna lain di antrean."""
        manager = JobQueueManager()

        job1 = DownloadJob("j1", chat_id=201, user_id=201, artist_name="Band A", query="Band A", tracks=[], catalog_summary={})
        job2 = DownloadJob("j2", chat_id=202, user_id=202, artist_name="Band B", query="Band B", tracks=[], catalog_summary={})

        with patch.object(manager, "_run_job_pipeline"):
            manager.enqueue(job1)
            manager.enqueue(job2)

            # Batalkan job2 milik user 202
            cancelled, msg = manager.cancel_job_for_chat(202)
            self.assertTrue(cancelled)
            self.assertEqual(job2.status, "cancelled")
            self.assertTrue(job2.cancel_event.is_set())

            # Job1 milik user 201 tetap aktif dan tidak tersentuh
            self.assertEqual(manager.active_job.job_id, "j1")
            self.assertFalse(manager.active_job.cancel_event.is_set())
            self.assertEqual(len(manager.pending_queue), 0)

    def test_05_archive_channel_mandatory_before_user_delivery(self):
        """5. Pengarsipan wajib: lagu dikirim ke ARCHIVE_CHANNEL_ID dahulu, lalu ke user via file_id."""
        job = DownloadJob(
            job_id="test_arc_05",
            chat_id=301,
            user_id=301,
            artist_name="Artis Arsip",
            query="Artis Arsip",
            tracks=[{"title": "Lagu Wajib Arsip", "videoId": "VID_ARC_01"}],
            catalog_summary={}
        )

        mock_bot = MagicMock()

        # Mock pesan pengarsipan channel menghasilkan file_id
        mock_arc_msg = MagicMock()
        mock_arc_msg.message_id = 9999
        mock_arc_msg.audio.file_id = "TELEGRAM_FILE_ID_ABC123"

        # Panggilan pertama send_audio ke channel, panggilan kedua ke user
        mock_bot.send_audio.side_effect = [mock_arc_msg, MagicMock()]

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_ARC_01.mp3")
            with open(tmp_mp3, "wb") as f:
                f.write(b"AUDIO_DATA")
            instance.extract_info.return_value = {"id": "VID_ARC_01", "ext": "mp3"}
            instance.prepare_filename.return_value = tmp_mp3

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -1009876543210):
                    execute_high_throughput_pipeline(job)

        # Harus memanggil send_audio 2 kali:
        # 1. ke ARCHIVE_CHANNEL_TARGET
        # 2. ke chat_id user (menggunakan file_id)
        self.assertEqual(mock_bot.send_audio.call_count, 2)
        call1 = mock_bot.send_audio.call_args_list[0]
        call2 = mock_bot.send_audio.call_args_list[1]

        self.assertEqual(call1.kwargs["chat_id"], -1009876543210)
        self.assertEqual(call2.kwargs["chat_id"], 301)
        self.assertEqual(call2.kwargs["audio"], "TELEGRAM_FILE_ID_ABC123")
        self.assertEqual(job.archived_count, 1)
        self.assertEqual(job.delivered_count, 1)

    def test_06_instant_cache_hit_delivery(self):
        """6. Cache Hit: Lagu yang sudah ada di SQLite dikirim instan via file_id tanpa download ulang."""
        # Simpan lagu ke SQLite cache terlebih dahulu
        save_cached_track(
            video_id="VID_CACHED_01",
            title="Lagu Terkenal",
            artist="Artis Legend",
            file_id="CACHED_FILE_ID_XYZ",
            archive_msg_id=777,
            file_size=1024000
        )

        job = DownloadJob(
            job_id="test_cache_06",
            chat_id=401,
            user_id=401,
            artist_name="Artis Legend",
            query="Artis Legend",
            tracks=[{"title": "Lagu Terkenal", "videoId": "VID_CACHED_01"}],
            catalog_summary={}
        )

        mock_bot = MagicMock()

        # yt_dlp TIDAK boleh dieksekusi sama sekali karena cache hit
        with patch("yt_dlp.YoutubeDL") as MockYDL:
            with patch("app.bot", mock_bot):
                execute_high_throughput_pipeline(job)
            MockYDL.assert_not_called()

        # Lagu harus langsung dikirim ke chat user via file_id
        mock_bot.send_audio.assert_called_once()
        self.assertEqual(mock_bot.send_audio.call_args.kwargs["audio"], "CACHED_FILE_ID_XYZ")
        self.assertEqual(job.cached_count, 1)
        self.assertEqual(job.delivered_count, 1)

    def test_07_retry_on_download_and_upload_error(self):
        """7. Sistem Retry: jika download/upload gagal sementara, dilakukan percobaan ulang."""
        job = DownloadJob(
            job_id="test_retry_07",
            chat_id=501,
            user_id=501,
            artist_name="Artis Retry",
            query="Artis Retry",
            tracks=[{"title": "Lagu Percobaan", "videoId": "VID_RETRY"}],
            catalog_summary={}
        )

        mock_bot = MagicMock()
        mock_arc = MagicMock()
        mock_arc.message_id = 12345
        mock_arc.audio.file_id = "RETRY_FILE_ID"

        # Mock download: percobaan pertama gagal, kedua berhasil
        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            attempts = 0
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_RETRY.mp3")

            def fake_extract_info(url, download=True):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise TimeoutError("Network timeout sementara")
                with open(tmp_mp3, "wb") as f:
                    f.write(b"AUDIO")
                return {"id": "VID_RETRY", "ext": "mp3"}

            instance.extract_info.side_effect = fake_extract_info
            instance.prepare_filename.return_value = tmp_mp3

            mock_bot.send_audio.side_effect = [mock_arc, MagicMock()]

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -10012345):
                    execute_high_throughput_pipeline(job)

        # Percobaan berhasil setelah retry
        self.assertGreaterEqual(attempts, 2)
        self.assertEqual(job.delivered_count, 1)
        self.assertEqual(job.failed_count, 0)

    def test_08_temporary_file_cleanup(self):
        """8. Pembersihan file sementara: file mp3 lokal terhapus setelah berhasil diunggah."""
        job = DownloadJob(
            job_id="test_clean_08",
            chat_id=601,
            user_id=601,
            artist_name="Artis Clean",
            query="Artis Clean",
            tracks=[{"title": "Lagu Bersih", "videoId": "VID_CLEAN_08"}],
            catalog_summary={}
        )

        created_files = []
        mock_bot = MagicMock()
        mock_arc = MagicMock()
        mock_arc.message_id = 54321
        mock_arc.audio.file_id = "CLEAN_FILE_ID"
        mock_bot.send_audio.side_effect = [mock_arc, MagicMock()]

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_file = os.path.join(tempfile.gettempdir(), "VID_CLEAN_08.mp3")

            def fake_extract(url, download=True):
                with open(tmp_file, "wb") as f:
                    f.write(b"SAMPLE")
                created_files.append(tmp_file)
                return {"id": "VID_CLEAN_08", "ext": "mp3"}

            instance.extract_info.side_effect = fake_extract
            instance.prepare_filename.return_value = tmp_file

            with patch("app.bot", mock_bot):
                with patch("app.ARCHIVE_CHANNEL_TARGET", -10012345):
                    execute_high_throughput_pipeline(job)

        # File lokal harus sudah terhapus
        for f in created_files:
            self.assertFalse(os.path.exists(f))

    def test_09_timeouts_applied_to_telebot(self):
        """9. Konfigurasi timeout BOT_API_TIMEOUT dan LONG_POLLING_TIMEOUT diterapkan ke telebot.apihelper."""
        self.assertEqual(telebot.apihelper.CONNECT_TIMEOUT, app.BOT_API_TIMEOUT)
        self.assertEqual(telebot.apihelper.READ_TIMEOUT, app.BOT_API_TIMEOUT)
        self.assertEqual(telebot.apihelper.LONG_POLLING_TIMEOUT, app.LONG_POLLING_TIMEOUT)

    def test_10_queue_status_command_output(self):
        """10. Perintah /queue dan /status menghasilkan ringkasan antrean yang informatif."""
        manager = JobQueueManager()
        summary = manager.get_status_summary()
        self.assertIn("Status Antrean", summary)
        self.assertIn("Tidak ada tugas", summary)


if __name__ == "__main__":
    unittest.main(verbosity=2)
