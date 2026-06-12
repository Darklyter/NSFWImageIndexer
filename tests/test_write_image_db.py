"""P2.01/P2.02/P2.05/P2.06 — write_image_to_db semantics (mock cursor)."""
import re

from src import llmii_db


class FakeCursor:
    def __init__(self, identifier_taken=False):
        self.identifier_taken = identifier_taken
        self.statements = []  # (normalized_sql, params)
        self._next = (1,)

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.statements.append((s, params))
        if s.startswith("SELECT 1 FROM images WHERE identifier"):
            self._next = (1,) if self.identifier_taken else None
        elif s.startswith("SELECT id FROM images WHERE lower(path)"):
            self._next = None  # no existing row — exercise the INSERT path
        elif "RETURNING id" in s or s.startswith("SELECT id"):
            self._next = (1,)
        else:
            self._next = None

    def fetchone(self):
        return self._next

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, identifier_taken=False):
        self.cur = FakeCursor(identifier_taken)
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


META = {
    "XMP:Identifier": "11111111-1111-1111-1111-111111111111",
    "XMP:Status": "success",
    "MWG:Description": "cap",
    "MWG:Keywords": ["Tag A"],
    "_raw_keywords": ["tag a"],
    "_debug_map": {"junkword": None},
}


def find(statements, pattern):
    return [(s, p) for s, p in statements if re.search(pattern, s)]


def test_default_clears_keywords_image_wide():
    conn = FakeConn()
    llmii_db.write_image_to_db(conn, r"C:\pics\a.jpg", dict(META), run_id=7)
    dels = find(conn.cur.statements, r"DELETE FROM image_keywords WHERE")
    assert dels, "no image_keywords delete issued"
    sql, params = dels[0]
    assert "tagger_run_id" not in sql, "delete still scoped to current run"
    dels_un = find(conn.cur.statements, r"DELETE FROM image_keywords_unmatched")
    assert "tagger_run_id" not in dels_un[0][0]
    # raw output stays per-run provenance
    dels_raw = find(conn.cur.statements, r"DELETE FROM image_keywords_raw")
    assert "tagger_run_id" in dels_raw[0][0]


def test_keep_history_scopes_deletes_to_run():
    conn = FakeConn()
    llmii_db.write_image_to_db(conn, r"C:\pics\a.jpg", dict(META), run_id=7,
                               keep_history=True)
    dels = find(conn.cur.statements, r"DELETE FROM image_keywords WHERE")
    assert "tagger_run_id" in dels[0][0]


def test_sha256_passed_to_images_insert():
    conn = FakeConn()
    llmii_db.write_image_to_db(conn, r"C:\pics\a.jpg", dict(META), run_id=7,
                               sha256="abc123")
    ins = find(conn.cur.statements, r"INSERT INTO images")
    assert ins
    assert "sha256" in ins[0][0]
    assert "abc123" in ins[0][1]


def test_duplicate_identifier_gets_fresh_uuid():
    conn = FakeConn(identifier_taken=True)
    llmii_db.write_image_to_db(conn, r"C:\pics\copy.jpg", dict(META), run_id=7)
    ins = find(conn.cur.statements, r"INSERT INTO images")[0]
    inserted_identifier = ins[1][0]
    assert inserted_identifier != META["XMP:Identifier"], (
        "duplicate embedded UUID was reused — would raise IntegrityError"
    )


def test_unique_identifier_is_kept():
    conn = FakeConn(identifier_taken=False)
    llmii_db.write_image_to_db(conn, r"C:\pics\a.jpg", dict(META), run_id=7)
    ins = find(conn.cur.statements, r"INSERT INTO images")[0]
    assert ins[1][0] == META["XMP:Identifier"]


def test_status_only_write_never_touches_keywords():
    """Verification-sweep blocker: a failed reprocess with empty keywords
    must not run the image-wide replacement deletes."""
    conn = FakeConn()
    failed = {
        "XMP:Identifier": META["XMP:Identifier"],
        "XMP:Status": "failed",
        "MWG:Keywords": [],
        "_raw_keywords": [],
        "_debug_map": {},
        "_status_only": True,
    }
    llmii_db.write_image_to_db(conn, r"C:\pics\a.jpg", failed, run_id=8)
    assert conn.committed
    # Run status recorded...
    assert find(conn.cur.statements, r"INSERT INTO image_run_status")
    # ...but no keyword/description statements of any kind
    assert not find(conn.cur.statements, r"DELETE FROM image_keywords")
    assert not find(conn.cur.statements, r"INSERT INTO image_keywords")
    assert not find(conn.cur.statements, r"image_descriptions")
