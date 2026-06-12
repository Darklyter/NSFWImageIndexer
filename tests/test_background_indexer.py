"""P4.03/P4.06/P4.12 — BackgroundIndexer stop + skip_folders matching."""
import queue

from src.llmii import BackgroundIndexer


def make_indexer(root, skip_folders=None, **kw):
    return BackgroundIndexer(
        str(root), queue.Queue(), [".jpg"],
        skip_folders=skip_folders or [], **kw
    )


class TestShouldSkipDirectory:
    def test_bare_name_matches_component_only(self, tmp_path):
        idx = make_indexer(tmp_path, skip_folders=["old"])
        assert idx._should_skip_directory(str(tmp_path / "old"))
        assert idx._should_skip_directory(str(tmp_path / "old" / "sub"))
        # substring matches must NOT skip
        assert not idx._should_skip_directory(str(tmp_path / "golden"))
        assert not idx._should_skip_directory(str(tmp_path / "soldiers"))

    def test_full_path_match_includes_subdirs(self, tmp_path):
        skip = str(tmp_path / "skipme")
        idx = make_indexer(tmp_path, skip_folders=[skip])
        assert idx._should_skip_directory(skip)
        assert idx._should_skip_directory(str(tmp_path / "skipme" / "deeper"))
        assert not idx._should_skip_directory(str(tmp_path / "skipmeplus"))

    def test_no_skip_folders(self, tmp_path):
        idx = make_indexer(tmp_path)
        assert not idx._should_skip_directory(str(tmp_path))


class TestStop:
    def test_indexing_complete_set_even_on_stop(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"x")
        idx = make_indexer(tmp_path)
        idx.stop()
        idx.run()  # run synchronously
        assert idx.indexing_complete is True

    def test_is_daemon(self, tmp_path):
        assert make_indexer(tmp_path).daemon is True

    def test_normal_run_finds_files(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"x")
        (tmp_path / "b.jpg").write_bytes(b"x")
        (tmp_path / "skip.txt").write_bytes(b"x")
        q = queue.Queue()
        idx = BackgroundIndexer(str(tmp_path), q, [".jpg"])
        idx.run()
        assert idx.indexing_complete
        assert idx.total_files_found == 2
        directory, files = q.get_nowait()
        assert len(files) == 2

    def test_indexing_complete_set_on_crash(self, tmp_path, monkeypatch):
        idx = make_indexer(tmp_path)

        def boom(d):
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(idx, "_index_directory", boom)
        try:
            idx.run()
        except RuntimeError:
            pass
        assert idx.indexing_complete is True, (
            "consumer would wait forever on the queue"
        )
