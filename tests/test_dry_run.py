"""P1.05 — pretend mode (dry_run) must not mutate the filesystem."""
from pathlib import Path

from src.llmii import FileProcessor


class StubConfig:
    dry_run = True
    sidecar_dir = ""
    output_mode = "json"


def make_processor():
    fp = FileProcessor.__new__(FileProcessor)
    fp.config = StubConfig()
    fp.callback = lambda msg: None
    return fp


def test_rename_to_invalid_noop_in_dry_run(tmp_path):
    fp = make_processor()
    img = tmp_path / "bad.jpg"
    img.write_bytes(b"\xff\xd8")
    backup = tmp_path / "bad.jpg_original"
    backup.write_bytes(b"\xff\xd8backup")

    assert fp.rename_to_invalid(str(img)) is False
    assert img.exists(), "dry run renamed the file"
    assert backup.exists(), "dry run deleted the _original backup"
    assert list(tmp_path.glob("*.invalid")) == []


def test_fix_file_extension_noop_in_dry_run(tmp_path):
    fp = make_processor()
    img = tmp_path / "photo.png"  # actually a JPEG per metadata
    img.write_bytes(b"\xff\xd8")

    result = fp.fix_file_extension(str(img), "jpg")
    assert result == str(img)
    assert img.exists(), "dry run renamed the extension"
    assert not (tmp_path / "photo.jpg").exists()


def test_rename_still_works_when_not_dry_run(tmp_path):
    fp = make_processor()
    fp.config.dry_run = False
    img = tmp_path / "bad.jpg"
    img.write_bytes(b"\xff\xd8")

    assert fp.rename_to_invalid(str(img)) is True
    assert not img.exists()
    assert len(list(tmp_path.glob("*.invalid"))) == 1
