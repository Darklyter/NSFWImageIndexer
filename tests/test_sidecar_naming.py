"""P1.07 — same-stem images must not share a sidecar; legacy names still read."""
import json
from pathlib import Path

from src.llmii import FileProcessor


class StubConfig:
    sidecar_dir = ""
    dry_run = False


def make_processor():
    fp = FileProcessor.__new__(FileProcessor)
    fp.config = StubConfig()
    fp.callback = lambda msg: None
    return fp


def test_same_stem_images_get_distinct_sidecars(tmp_path):
    fp = make_processor()
    jpg = str(tmp_path / "photo.jpg")
    png = str(tmp_path / "photo.png")
    assert fp._get_sidecar_path(jpg) != fp._get_sidecar_path(png)


def test_write_read_roundtrip_new_naming(tmp_path):
    fp = make_processor()
    img = str(tmp_path / "photo.jpg")
    meta = {"MWG:Description": "cap", "MWG:Keywords": ["k"],
            "XMP:Status": "success", "XMP:Identifier": "id-1"}
    assert fp._write_json_sidecar(img, meta) is True
    assert (tmp_path / "photo.jpg.json").exists()

    data = fp._read_json_sidecar(img)
    assert data["Status"] == "success"
    assert data["Keywords"] == ["k"]


def test_legacy_stem_sidecar_still_read(tmp_path):
    fp = make_processor()
    img = str(tmp_path / "old.jpg")
    legacy = tmp_path / "old.json"
    legacy.write_text(json.dumps({"Status": "success", "Identifier": "legacy-id"}),
                      encoding="utf-8")
    data = fp._read_json_sidecar(img)
    assert data is not None
    assert data["Identifier"] == "legacy-id"


def test_new_name_preferred_over_legacy(tmp_path):
    fp = make_processor()
    img = str(tmp_path / "both.jpg")
    (tmp_path / "both.json").write_text(json.dumps({"Identifier": "old"}), encoding="utf-8")
    (tmp_path / "both.jpg.json").write_text(json.dumps({"Identifier": "new"}), encoding="utf-8")
    assert fp._read_json_sidecar(img)["Identifier"] == "new"
