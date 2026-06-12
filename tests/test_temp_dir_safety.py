"""P1.06 — _clear_temp_dir must never delete a directory it didn't create."""
from pathlib import Path

from src.llmii import FileProcessor


def make_processor(temp_dir):
    fp = FileProcessor.__new__(FileProcessor)
    fp.temp_dir = Path(temp_dir)
    fp.callback = lambda msg: None
    return fp


def test_refuses_to_delete_foreign_directory(tmp_path):
    foreign = tmp_path / "temp"
    foreign.mkdir()
    precious = foreign / "user_data.txt"
    precious.write_text("do not delete")

    fp = make_processor(foreign)
    fp._clear_temp_dir()

    assert precious.exists(), "_clear_temp_dir deleted a foreign directory"


def test_clears_marked_directory(tmp_path):
    ours = tmp_path / "llmii_zip_extract"
    ours.mkdir()
    (ours / ".llmii_temp").touch()
    (ours / "stale_extract.jpg").write_bytes(b"x")

    fp = make_processor(ours)
    fp._clear_temp_dir()

    assert not ours.exists()


def test_noop_when_missing(tmp_path):
    fp = make_processor(tmp_path / "nonexistent")
    fp._clear_temp_dir()  # must not raise or create anything
    assert not (tmp_path / "nonexistent").exists()
