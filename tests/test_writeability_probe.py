"""P1.04 — writeability probe must be non-destructive and never reach the DB."""
import json
import os
from pathlib import Path

from src.llmii import FileProcessor


class StubConfig:
    sidecar_dir = ""
    output_mode = "json"
    dry_run = False


def make_processor(tmp_path, output_mode="json"):
    fp = FileProcessor.__new__(FileProcessor)
    fp.config = StubConfig()
    fp.config.output_mode = output_mode
    fp.temp_dir = Path(tmp_path) / "temp"
    fp.db_conn = None
    fp.db_run_id = None
    fp.callback = lambda msg: None
    return fp


def test_probe_does_not_clobber_existing_sidecar(tmp_path):
    fp = make_processor(tmp_path)
    img = tmp_path / "photo.jpg"
    img.write_bytes(b"\xff\xd8")
    sidecar = tmp_path / "photo.json"
    original = {"Description": "precious caption", "Keywords": ["keep"],
                "Status": "success", "Identifier": "uuid-1"}
    sidecar.write_text(json.dumps(original), encoding="utf-8")

    assert fp._test_writeability(str(img)) is True
    # The old probe rewrote this file with empty data
    assert json.loads(sidecar.read_text(encoding="utf-8")) == original


def test_probe_true_when_sidecar_missing_but_dir_writable(tmp_path):
    fp = make_processor(tmp_path)
    img = tmp_path / "new.jpg"
    img.write_bytes(b"\xff\xd8")
    assert fp._test_writeability(str(img)) is True


def test_probe_skips_db_mode_entirely(tmp_path):
    fp = make_processor(tmp_path, output_mode="db")

    # Any DB access would explode — the probe must never touch the DB
    class Bomb:
        def __getattr__(self, name):
            raise AssertionError("probe touched the DB")

    fp.db_conn = Bomb()
    assert fp._test_writeability(str(tmp_path / "x.jpg")) is True


def test_probe_handles_unbuilt_sidecar_dir(tmp_path):
    fp = make_processor(tmp_path)
    fp.config.sidecar_dir = str(tmp_path / "not" / "yet" / "created")
    img = tmp_path / "a.jpg"
    img.write_bytes(b"\xff\xd8")
    # Walks up to nearest existing parent instead of crashing
    assert fp._test_writeability(str(img)) is True
