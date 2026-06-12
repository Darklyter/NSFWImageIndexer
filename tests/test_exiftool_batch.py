"""P1.02 — one unreadable file must not drop the whole ExifTool batch."""
import exiftool

from src.llmii import FileProcessor


def _execute_error():
    # Bypass the constructor; we only need an instance to raise.
    return exiftool.exceptions.ExifToolExecuteError.__new__(
        exiftool.exceptions.ExifToolExecuteError
    )


class FakeExifTool:
    """Batch call fails; per-file calls succeed except for the bad file."""

    def __init__(self, bad_file):
        self.bad_file = bad_file

    def get_tags(self, files, tags=None, params=None):
        if len(files) > 1:
            raise _execute_error()
        if files[0] == self.bad_file:
            raise _execute_error()
        return [{"SourceFile": files[0], "File:FileType": "JPEG"}]


def make_processor(bad_file):
    fp = FileProcessor.__new__(FileProcessor)
    fp.et = FakeExifTool(bad_file)
    fp.callback = lambda msg: None
    fp.failed_validations = []
    return fp


def test_batch_failure_salvages_readable_files():
    files = ["a.jpg", "bad.jpg", "c.jpg"]
    fp = make_processor("bad.jpg")
    results = fp._get_tags_resilient(files, tags=["File:FileType"], params=[])
    assert [r["SourceFile"] for r in results] == ["a.jpg", "c.jpg"]
    assert fp.failed_validations == ["bad.jpg"]


def test_healthy_batch_single_call():
    fp = make_processor(bad_file=None)
    results = fp._get_tags_resilient(["a.jpg"], tags=["File:FileType"], params=[])
    assert len(results) == 1
    assert fp.failed_validations == []
