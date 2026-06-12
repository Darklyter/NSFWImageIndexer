"""P1.03 — zip images in JSON mode must persist sidecars and skip done work."""
import os
import zipfile
from pathlib import Path

from src.llmii import FileProcessor


class StubConfig:
    sidecar_dir = ""
    output_mode = "json"
    reprocess_all = False
    reprocess_failed = False
    reprocess_sparse = False
    reprocess_sparse_min = 5
    image_extensions_filter = ""
    dry_run = False


def make_processor(tmp_path):
    fp = FileProcessor.__new__(FileProcessor)
    fp.config = StubConfig()
    fp.temp_dir = Path(tmp_path) / "temp"
    fp.db_conn = None
    fp.db_run_id = None
    fp._zip_file_map = {}
    fp.callback = lambda msg: None
    fp.image_extensions = {"JPEG": [".jpg", ".jpeg"], "ZIP": [".zip"]}
    return fp


def make_zip(tmp_path, name="Studio - Gallery (Jane Doe).zip"):
    zip_path = Path(tmp_path) / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("img001.jpg", b"\xff\xd8fakejpegdata")
        zf.writestr("notes.txt", b"not an image")
    return str(zip_path)


def test_zip_sidecar_path_is_persistent_and_deterministic(tmp_path):
    fp = make_processor(tmp_path)
    zip_path = make_zip(tmp_path)
    composite = f"{os.path.normpath(os.path.abspath(zip_path))}::img001.jpg"
    p1 = fp._zip_sidecar_path(composite)
    p2 = fp._zip_sidecar_path(composite)
    assert p1 == p2
    # Must NOT be inside the temp extraction dir
    assert not os.path.normpath(p1).startswith(os.path.normpath(str(fp.temp_dir)))
    # Default scheme: next to the zip
    assert os.path.dirname(os.path.dirname(p1)) == os.path.normpath(str(tmp_path))


def test_full_cycle_extract_write_cleanup_skip(tmp_path):
    fp = make_processor(tmp_path)
    zip_path = make_zip(tmp_path)

    # First extraction: the jpg comes out, the txt does not
    extracted = fp._extract_single_zip(zip_path)
    assert len(extracted) == 1
    temp_file = extracted[0]
    assert os.path.exists(temp_file)
    composite = fp._zip_file_map[os.path.normpath(temp_file)][0]

    # Simulate successful processing: write_metadata in json mode
    metadata = {
        "SourceFile": temp_file,
        "MWG:Description": "a test caption",
        "MWG:Keywords": ["test"],
        "XMP:Status": "success",
        "XMP:Identifier": "uuid-123",
        "_zip_db_key": composite,
        "_zip_source": os.path.basename(zip_path),
    }
    assert fp.write_metadata(temp_file, metadata) is True

    # Sidecar must live OUTSIDE the temp dir and survive cleanup
    sidecar = fp._zip_sidecar_path(composite)
    assert os.path.exists(sidecar)
    fp._cleanup_zip_temp(zip_path)
    assert not os.path.exists(temp_file)
    assert os.path.exists(sidecar), "sidecar was destroyed by zip temp cleanup"

    # Second run: nothing to extract — already done via sidecar check
    extracted2 = fp._extract_single_zip(zip_path)
    assert extracted2 == []


def test_reprocess_all_overrides_sidecar_skip(tmp_path):
    fp = make_processor(tmp_path)
    zip_path = make_zip(tmp_path)
    extracted = fp._extract_single_zip(zip_path)
    composite = fp._zip_file_map[os.path.normpath(extracted[0])][0]
    fp._write_json_sidecar("", {"XMP:Status": "success"},
                           sidecar_path=fp._zip_sidecar_path(composite))
    fp._cleanup_zip_temp(zip_path)

    fp.config.reprocess_all = True
    assert len(fp._extract_single_zip(zip_path)) == 1
