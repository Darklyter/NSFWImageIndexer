"""P1.01 — Config.from_args() CLI regressions."""
import sys

import pytest

from src.llmii import Config


def parse(argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["llmii.py"] + argv)
    return Config.from_args()


def test_from_args_does_not_crash(monkeypatch):
    # type="store_true" used to raise ValueError at add_argument time
    config = parse(["somedir"], monkeypatch)
    assert config.directory == "somedir"


def test_store_true_flags_work(monkeypatch):
    config = parse(["somedir", "--rename-invalid", "--preserve-date"], monkeypatch)
    assert config.rename_invalid is True
    assert config.preserve_date is True


def test_unsupplied_args_keep_config_defaults(monkeypatch):
    config = parse(["somedir"], monkeypatch)
    # argparse defaults must not clobber Config.__init__ values
    assert config.gen_count == 1500
    assert config.res_limit == 1280
    assert config.reprocess_orphans is True
    assert config.rename_invalid is False


def test_gen_count_is_int(monkeypatch):
    config = parse(["somedir", "--gen-count", "300"], monkeypatch)
    assert config.gen_count == 300
    assert isinstance(config.gen_count, int)


def test_api_url_default_preserved(monkeypatch):
    config = parse(["somedir"], monkeypatch)
    assert config.api_url == "http://localhost:5001"
