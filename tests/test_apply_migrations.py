"""P1.08 — one failing migration must not abort the rest or poison the conn."""
from src import llmii_db


class FakeCursor:
    def __init__(self, fail_on_substring):
        self.fail_on = fail_on_substring
        self.executed = []
        self.savepoint_rollbacks = 0

    def execute(self, sql, params=None):
        if sql == "ROLLBACK TO SAVEPOINT migration_step":
            self.savepoint_rollbacks += 1
            return
        if sql in ("SAVEPOINT migration_step", "RELEASE SAVEPOINT migration_step"):
            return
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("simulated DDL failure")
        self.executed.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, fail_on_substring=None):
        self.cur = FakeCursor(fail_on_substring)
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True


def test_all_migrations_apply_cleanly():
    conn = FakeConn()
    llmii_db.apply_migrations(conn)
    assert conn.committed
    assert conn.cur.savepoint_rollbacks == 0
    assert len(conn.cur.executed) > 10


def test_one_failure_does_not_abort_the_rest():
    # Fail the studio_galleries table; everything else must still run
    conn = FakeConn(fail_on_substring="studio_galleries ALTER")
    llmii_db.apply_migrations(conn)  # must not raise
    assert conn.committed
    assert conn.cur.savepoint_rollbacks == 1
    # Statements after the failing one were still executed
    assert any("performer_tags" in sql for sql in conn.cur.executed)
