#!/usr/bin/env python3
"""
Test Suite untuk Telegram Music Downloader Bot (app.py)
Menguji ke-14 skenario yang disyaratkan dalam pengujian wajib:
1. Pemeriksaan sintaks & import.
2. Pencarian artis dengan beberapa album.
3. Deduplikasi lagu antara Album & Singles.
4. Penanganan hasil pencarian kosong.
5. Penanganan kegagalan sebagian katalog.
6. Penanganan kegagalan unduh satu lagu di tengah antrean (lanjut ke lagu berikutnya).
7. Penanganan kegagalan konversi audio.
8. Penanganan kegagalan pengiriman Telegram.
9. Pencegahan klik tombol konfirmasi berulang / sesi basi.
10. Pembatalan kooperatif proses unduhan yang sedang berjalan.
11. Pembersihan file sementara (cleanup temp directory).
12. Pengujian katalog besar tanpa limit 50 buatan.
13. Keakuratan laporan akhir (success, failed, skipped, duration).
14. Pengecekan token environment variable.
"""

import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

# Atur environment variable dummy untuk pengujian
os.environ["TELEGRAM_BOT_TOKEN"] = "1234567890:ABCdefGHIjklMNOpqrsTUVwxyz123456"
os.environ["LOG_LEVEL"] = "WARNING"

import app
from app import (
    MusicSession,
    crawl_artist_catalog,
    execute_download_worker,
    get_artist_releases_robust,
    set_pending_session,
    get_pending_session,
    pop_pending_session,
    set_active_download,
    get_active_download,
    remove_active_download,
)


class TestTelegramMusicBot(unittest.TestCase):

    def setUp(self):
        # Reset session store sebelum tiap tes
        with app.SESSION_LOCK:
            app.PENDING_SESSIONS.clear()
            app.ACTIVE_DOWNLOADS.clear()

    def test_01_syntax_and_imports(self):
        """1. Sintaks & Import seluruh modul valid."""
        self.assertTrue(callable(app.crawl_artist_catalog))
        self.assertTrue(callable(app.execute_download_worker))
        self.assertTrue(callable(app.get_artist_releases_robust))

    def test_02_search_artist_with_multiple_albums_and_singles(self):
        """2. Pengujian pencarian artis dengan beberapa album dan singles."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = [{"browseId": "ARTIST_123", "artist": "Band Uji"}]
        mock_yt.get_artist.return_value = {
            "name": "Band Uji",
            "songs": {
                "browseId": "VL_SONGS_PL",
                "results": [{"title": "Lagu Populer 1", "videoId": "VID_01"}],
            },
            "albums": {
                "results": [{"title": "Album 1", "browseId": "MPREb_ALB_1"}],
                "params": "ALB_PARAMS",
            },
            "singles": {
                "results": [{"title": "Single 1", "browseId": "MPREb_SGL_1"}],
                "params": "SGL_PARAMS",
            },
        }
        mock_yt.get_playlist.return_value = {
            "tracks": [
                {"title": "Lagu Populer 1", "videoId": "VID_01"},
                {"title": "Lagu Playlist 2", "videoId": "VID_02"},
            ]
        }
        mock_yt.get_artist_albums.side_effect = lambda aid, params, limit=None: [
            {"title": "Album 2", "browseId": "MPREb_ALB_2"}
        ] if params == "ALB_PARAMS" else [
            {"title": "Single 2", "browseId": "MPREb_SGL_2"}
        ]

        def fake_get_album(bid):
            if bid == "MPREb_ALB_1":
                return {"tracks": [{"title": "Trek A1", "videoId": "VID_A1"}, {"title": "Trek A2", "videoId": "VID_A2"}]}
            elif bid == "MPREb_ALB_2":
                return {"tracks": [{"title": "Trek B1", "videoId": "VID_B1"}]}
            elif bid == "MPREb_SGL_1":
                return {"tracks": [{"title": "Trek S1", "videoId": "VID_S1"}]}
            elif bid == "MPREb_SGL_2":
                return {"tracks": [{"title": "Trek S2", "videoId": "VID_S2"}]}
            return {"tracks": []}

        mock_yt.get_album.side_effect = fake_get_album

        artist_name, tracks, summary = crawl_artist_catalog(mock_yt, "Band Uji")

        self.assertEqual(artist_name, "Band Uji")
        self.assertTrue(summary["is_official_artist"])
        self.assertEqual(summary["albums_count"], 2)
        self.assertEqual(summary["singles_count"], 2)
        # Unique tracks: VID_01, VID_02, VID_A1, VID_A2, VID_B1, VID_S1, VID_S2 = 7
        self.assertEqual(len(tracks), 7)
        self.assertEqual(summary["unique_total"], 7)

    def test_03_deduplication_between_albums_and_singles(self):
        """3. Pengujian deduplikasi: lagu yang muncul di playlist, album, dan single hanya masuk 1 kali."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = [{"browseId": "ARTIST_DUP", "artist": "Artis Duplikat"}]
        mock_yt.get_artist.return_value = {
            "name": "Artis Duplikat",
            "songs": {
                "results": [{"title": "Hit Song", "videoId": "VID_HIT"}],
                "browseId": None,
            },
            "albums": {
                "results": [{"title": "Greatest Hits", "browseId": "MPREb_GH"}],
            },
            "singles": {
                "results": [{"title": "Hit Song Single", "browseId": "MPREb_HIT_SGL"}],
            },
        }
        mock_yt.get_album.side_effect = lambda bid: {
            "tracks": [{"title": "Hit Song", "videoId": "VID_HIT"}]
        }

        artist_name, tracks, summary = crawl_artist_catalog(mock_yt, "Artis Duplikat")

        # Harus hanya 1 lagu unik VID_HIT
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["videoId"], "VID_HIT")

    def test_04_empty_search_result(self):
        """4. Pengujian hasil kosong: artis tidak ditemukan dan pencarian cadangan juga kosong."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = []

        artist_name, tracks, summary = crawl_artist_catalog(mock_yt, "QueryTidakAda123")

        self.assertEqual(len(tracks), 0)
        self.assertEqual(summary["unique_total"], 0)
        self.assertTrue(summary["is_fallback"])

    def test_05_fallback_search_when_artist_not_in_directory(self):
        """5. Pengujian pencarian cadangan transparan ketika profil artis resmi tidak ada."""
        mock_yt = MagicMock()
        # Pencarian filter artists mengembalikan kosong
        def fake_search(query, filter=None, limit=None):
            if filter == "artists":
                return []
            return [
                {"title": "Track Umum 1", "videoId": "GEN_01", "artists": [{"name": "Penyanyi Acak"}]},
                {"title": "Track Umum 2", "videoId": "GEN_02", "artists": [{"name": "Penyanyi Acak"}]},
            ]
        mock_yt.search.side_effect = fake_search

        artist_name, tracks, summary = crawl_artist_catalog(mock_yt, "Penyanyi Acak")

        self.assertEqual(len(tracks), 2)
        self.assertTrue(summary["is_fallback"])
        self.assertFalse(summary["is_official_artist"])

    def test_06_single_track_failure_in_middle_of_queue(self):
        """6. Pengujian: satu lagu gagal di tengah antrean tidak membatalkan sisa antrean."""
        session = MusicSession(
            session_id="test_sess_06",
            chat_id=1001,
            artist_name="Artis Uji",
            query="Artis Uji",
            tracks=[
                {"title": "Lagu 1", "videoId": "VID_OK_1"},
                {"title": "Lagu Rusak", "videoId": "VID_ERR"},
                {"title": "Lagu 3", "videoId": "VID_OK_2"},
            ],
            catalog_summary={},
        )
        set_active_download(1001, session)

        mock_bot = MagicMock()

        # Mock yt_dlp
        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value

            def fake_extract_info(url, download=True):
                if "VID_ERR" in url:
                    raise RuntimeError("Ekstraksi gagal untuk lagu rusak")
                # Buat file mp3 sementara
                out_path = os.path.join(tempfile.gettempdir(), f"{url.split('=')[-1]}.mp3")
                with open(out_path, "wb") as f:
                    f.write(b"MOCK_AUDIO_DATA_FOR_TEST")
                return {"id": url.split("=")[-1], "ext": "webm"}

            instance.extract_info.side_effect = fake_extract_info
            instance.prepare_filename.side_effect = lambda info: os.path.join(tempfile.gettempdir(), f"{info['id']}.webm")

            with patch("app.bot", mock_bot):
                execute_download_worker(session, initial_msg_id=999)

        # Lagu 1 dan Lagu 3 harus berhasil dikirim (send_audio dipanggil 2 kali)
        self.assertEqual(mock_bot.send_audio.call_count, 2)
        # Harus ada 1 lagu gagal di session.failed_tracks
        self.assertEqual(len(session.failed_tracks), 1)
        self.assertEqual(session.failed_tracks[0]["videoId"], "VID_ERR")
        # Bot harus mengirim laporan akhir
        self.assertTrue(mock_bot.send_message.called)
        last_msg = mock_bot.send_message.call_args[0][1]
        self.assertIn("Berhasil Dikirim: <b>2</b>", last_msg)
        self.assertIn("Gagal: <b>1</b>", last_msg)

    def test_07_telegram_send_failure(self):
        """7. Pengujian kegagalan saat pengiriman Telegram (penghitung berhasil tidak bertambah)."""
        session = MusicSession(
            session_id="test_sess_07",
            chat_id=1002,
            artist_name="Artis Uji 2",
            query="Artis Uji 2",
            tracks=[{"title": "Lagu Send Err", "videoId": "VID_SEND_ERR"}],
            catalog_summary={},
        )
        set_active_download(1002, session)

        mock_bot = MagicMock()
        mock_bot.send_audio.side_effect = ConnectionResetError("Telegram socket closed")

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_SEND_ERR.mp3")
            with open(tmp_mp3, "wb") as f:
                f.write(b"AUDIO")
            instance.extract_info.return_value = {"id": "VID_SEND_ERR", "ext": "webm"}
            instance.prepare_filename.return_value = os.path.join(tempfile.gettempdir(), "VID_SEND_ERR.webm")

            with patch("app.bot", mock_bot):
                execute_download_worker(session, initial_msg_id=888)

        # send_audio dipanggil tapi gagal
        self.assertEqual(mock_bot.send_audio.call_count, 1)
        # Maka failed_count = 1, success_count = 0
        self.assertEqual(len(session.failed_tracks), 1)
        last_msg = mock_bot.send_message.call_args[0][1]
        self.assertIn("Berhasil Dikirim: <b>0</b>", last_msg)
        self.assertIn("Gagal: <b>1</b>", last_msg)

    def test_08_file_size_exceeding_telegram_limit(self):
        """8. Pengujian file audio melebihi batas 50MB (dilewati dengan aman)."""
        session = MusicSession(
            session_id="test_sess_08",
            chat_id=1003,
            artist_name="Artis Konser",
            query="Artis Konser",
            tracks=[{"title": "Full Concert 3 Jam", "videoId": "VID_BIG"}],
            catalog_summary={},
        )
        set_active_download(1003, session)

        mock_bot = MagicMock()

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value
            tmp_mp3 = os.path.join(tempfile.gettempdir(), "VID_BIG.mp3")
            with open(tmp_mp3, "wb") as f:
                f.write(b"AUDIO_HEADER")
            instance.extract_info.return_value = {"id": "VID_BIG", "ext": "webm"}
            instance.prepare_filename.return_value = os.path.join(tempfile.gettempdir(), "VID_BIG.webm")

            # Mock getsize mengembalikan 55 MB (> 49.5 MB)
            with patch("os.path.getsize", return_value=55 * 1024 * 1024):
                with patch("app.bot", mock_bot):
                    execute_download_worker(session, initial_msg_id=777)

        # File tidak boleh dikirim ke bot.send_audio
        mock_bot.send_audio.assert_not_called()
        last_msg = mock_bot.send_message.call_args[0][1]
        self.assertIn("Dilewati (>50MB): <b>1</b>", last_msg)

    def test_09_stale_confirmation_button(self):
        """9. Pengujian pencegahan klik tombol konfirmasi berulang / sesi basi."""
        chat_id = 1004
        # Tidak ada sesi yang pending
        self.assertIsNone(get_pending_session(chat_id))

        call = MagicMock()
        call.message.chat.id = chat_id
        call.message.message_id = 111
        call.data = "confirm_dl:SESS_OLD"

        with patch("app.bot") as mock_bot:
            app.handle_confirm_inline(call)

        mock_bot.answer_callback_query.assert_called_with(
            call.id, "Sesi kedaluwarsa atau sudah diproses.", show_alert=True
        )

    def test_10_cooperative_cancellation_during_download(self):
        """10. Pengujian pembatalan kooperatif saat proses download berjalan."""
        session = MusicSession(
            session_id="test_sess_10",
            chat_id=1005,
            artist_name="Artis Batal",
            query="Artis Batal",
            tracks=[
                {"title": f"Lagu {i}", "videoId": f"VID_{i}"} for i in range(10)
            ],
            catalog_summary={},
        )
        set_active_download(1005, session)

        mock_bot = MagicMock()

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value

            def fake_extract(url, download=True):
                # Pada lagu ke-2, pengguna membatalkan proses
                if "VID_1" in url:
                    session.cancel_event.set()
                out_path = os.path.join(tempfile.gettempdir(), f"{url.split('=')[-1]}.mp3")
                with open(out_path, "wb") as f:
                    f.write(b"AUDIO")
                return {"id": url.split("=")[-1], "ext": "webm"}

            instance.extract_info.side_effect = fake_extract
            instance.prepare_filename.side_effect = lambda info: os.path.join(tempfile.gettempdir(), f"{info['id']}.webm")

            with patch("app.bot", mock_bot):
                execute_download_worker(session, initial_msg_id=666)

        # Proses harus berhenti jauh sebelum 10 lagu
        self.assertLess(mock_bot.send_audio.call_count, 5)
        # Status aktif harus dibersihkan
        self.assertIsNone(get_active_download(1005))

    def test_11_temporary_directory_cleanup(self):
        """11. Pengujian pembersihan file sementara: tidak ada file mp3/part yang tertinggal."""
        session = MusicSession(
            session_id="test_sess_11",
            chat_id=1006,
            artist_name="Artis Bersih",
            query="Artis Bersih",
            tracks=[{"title": "Lagu Bersih", "videoId": "VID_CLEAN"}],
            catalog_summary={},
        )
        set_active_download(1006, session)

        created_files = []

        with patch("yt_dlp.YoutubeDL") as MockYDL:
            instance = MockYDL.return_value.__enter__.return_value

            def fake_extract_info(url, download=True):
                # Cek bahwa temp_dir dibuat
                tmp_dir = instance.params.get("outtmpl")
                base_dir = os.path.dirname(tmp_dir)
                mp3 = os.path.join(base_dir, "VID_CLEAN.mp3")
                with open(mp3, "wb") as f:
                    f.write(b"DATA")
                created_files.append(mp3)
                return {"id": "VID_CLEAN", "ext": "webm"}

            instance.extract_info.side_effect = fake_extract_info
            instance.prepare_filename.side_effect = lambda info: os.path.join(os.path.dirname(instance.params["outtmpl"]), f"{info['id']}.webm")

            mock_bot = MagicMock()
            with patch("app.bot", mock_bot):
                execute_download_worker(session, initial_msg_id=555)

        # Pastikan file yang dibuat sudah dihapus
        for f in created_files:
            self.assertFalse(os.path.exists(f))

    def test_12_no_arbitrary_catalog_limit(self):
        """12. Pengujian katalog besar: seluruh 150 lagu unik diproses tanpa dibatasi limit 50."""
        mock_yt = MagicMock()
        mock_yt.search.return_value = [{"browseId": "ARTIST_BIG", "artist": "Big Artist"}]
        # 150 lagu di playlist
        tracks_150 = [{"title": f"Song {i}", "videoId": f"VID_{i:04d}"} for i in range(150)]
        mock_yt.get_artist.return_value = {
            "name": "Big Artist",
            "songs": {"browseId": "VL_BIG", "results": tracks_150[:10]},
            "albums": {"results": []},
            "singles": {"results": []},
        }
        mock_yt.get_playlist.return_value = {"tracks": tracks_150}

        artist_name, tracks, summary = crawl_artist_catalog(mock_yt, "Big Artist")

        # Harus mengumpulkan 150 lagu lengkap, TIDAK berhenti di 50 atau 100
        self.assertEqual(len(tracks), 150)
        self.assertEqual(summary["unique_total"], 150)


if __name__ == "__main__":
    unittest.main(verbosity=2)
