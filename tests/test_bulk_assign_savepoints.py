"""P1.09 — a failing keyword in bulk-assign must not lose earlier keywords."""
import importlib.util
import os
import sys
import types

import pytest

# tag_review.py imports PyQt6 at module level; provide the real one if
# installed (it is a project dependency).
pytest.importorskip("PyQt6")


def load_tag_review():
    spec = importlib.util.spec_from_file_location(
        "tag_review",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "tag_review.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Simulates psycopg2 savepoint semantics for the bulk-assign loop."""

    def __init__(self, conn, fail_keywords):
        self.conn = conn
        self.fail_keywords = fail_keywords
        self.rowcount = 0
        self._pending = []  # statements since last savepoint

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if s == "SAVEPOINT bulk_kw":
            self._pending = []
            return
        if s == "RELEASE SAVEPOINT bulk_kw":
            self.conn.uncommitted.extend(self._pending)
            self._pending = []
            return
        if s == "ROLLBACK TO SAVEPOINT bulk_kw":
            self._pending = []
            return
        if s.startswith("SELECT id FROM tags"):
            self._select_result = (42,)
            return
        if s.startswith("SELECT t.tag FROM tag_aliases"):
            # No pre-existing alias conflicts in this scenario
            self._select_result = None
            return
        kw = params[-1] if params else None
        if kw in self.fail_keywords and "INSERT INTO image_keywords " in s:
            raise RuntimeError("simulated insert failure")
        if "INSERT INTO image_keywords " in s:
            self.rowcount = 3
        self._pending.append((s, params))

    def fetchone(self):
        return self._select_result

    def close(self):
        pass


class FakeConn:
    def __init__(self, fail_keywords):
        self.fail_keywords = fail_keywords
        self.uncommitted = []
        self.committed = []
        self.full_rollbacks = 0

    def cursor(self):
        return FakeCursor(self, self.fail_keywords)

    def commit(self):
        self.committed.extend(self.uncommitted)
        self.uncommitted = []

    def rollback(self):
        self.full_rollbacks += 1
        self.uncommitted = []


class DialogHarness:
    """Drives BulkAssignDialog._assign_selected without building the widget."""

    def __init__(self, mod, conn, keywords):
        self.dlg = mod.BulkAssignDialog.__new__(mod.BulkAssignDialog)
        d = self.dlg
        d.conn = conn
        d.all_keywords = [(k, 1) for k in keywords]
        d._checked = list(keywords)

        class FakeItem:
            def __init__(self, text):
                self._t = text

            def text(self):
                return self._t

        class FakeTagList:
            def selectedItems(self_inner):
                return [FakeItem("Canonical Tag")]

        class FakeLabel:
            def setText(self_inner, t):
                self_inner.last = t

        d.tag_list = FakeTagList()
        d.status_label = FakeLabel()
        d._checked_keywords = lambda: list(keywords)
        d._filter_keywords = lambda: None
        d.parent = lambda: None


def test_earlier_keywords_survive_later_failure():
    mod = load_tag_review()
    conn = FakeConn(fail_keywords={"kw2"})
    h = DialogHarness(mod, conn, ["kw1", "kw2", "kw3"])

    h.dlg._assign_selected()

    committed_aliases = [p for (s, p) in conn.committed if "tag_aliases" in s]
    committed_kw = {p[1] for (s, p) in conn.committed if "tag_aliases" in s}
    # kw1 and kw3 persisted; kw2 rolled back alone
    assert committed_kw == {"kw1", "kw3"}
    assert len(committed_aliases) == 2
    assert conn.full_rollbacks == 0, "full-transaction rollback wiped the batch"
    # Reporting matches reality
    assert set(k for k, _ in h.dlg.all_keywords) == {"kw2"}
