"""Live integration check against the llmii_scratch database.

Run manually: .venv/Scripts/python tests/integration_scratch_db.py
Exercises the Phase 2 semantics that mocked tests can't fully prove.
Safe: touches only the llmii_scratch database, never production.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import llmii_db

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail and not cond else ''}")


def main():
    s = json.load(open('settings.json', encoding='utf-8'))
    conn = llmii_db.get_connection(s['db_host'], s['db_port'], s['db_user'],
                                   s['db_password'], 'llmii_scratch')
    cur0 = conn.cursor()
    # Clean slate for repeatable runs
    for t in ('image_keywords', 'image_keywords_raw', 'image_keywords_unmatched',
              'image_run_status', 'image_descriptions', 'image_performers',
              'performer_tags', 'studio_images', 'studio_galleries',
              'images', 'galleries', 'performers', 'studios',
              'tag_aliases', 'tags', 'tagger_runs'):
        cur0.execute(f'TRUNCATE TABLE {t} CASCADE')
    conn.commit()
    cur0.close()

    run1 = llmii_db.create_tagger_run(conn, 'integration-test')
    run2 = llmii_db.create_tagger_run(conn, 'integration-test')

    meta = {
        'XMP:Identifier': '11111111-1111-1111-1111-111111111111',
        'XMP:Status': 'success',
        'MWG:Description': 'a test caption',
        'MWG:Keywords': ['Tag Alpha', 'Tag Beta'],
        '_raw_keywords': ['tag alpha', 'tag beta'],
        '_debug_map': {'junk keyword': None},
    }

    print("\n[1] write_image_to_db + keyword replacement semantics (D4)")
    llmii_db.write_image_to_db(conn, r'C:\scratch\a.jpg', dict(meta), run1, sha256='hash-a')
    meta2 = dict(meta)
    meta2['MWG:Keywords'] = ['Tag Gamma']  # model changed its mind on reprocess
    llmii_db.write_image_to_db(conn, r'C:\scratch\a.jpg', meta2, run2, sha256='hash-a')
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT t.tag FROM image_keywords ik JOIN tags t ON t.id = ik.tag_id
            JOIN images i ON i.id = ik.image_id WHERE i.path = %s
        """, (r'C:\scratch\a.jpg',))
        tags_now = {r[0] for r in cur.fetchall()}
    check("reprocess replaces keywords image-wide", tags_now == {'Tag Gamma'},
          f"got {tags_now}")

    print("\n[2] keep_history=True preserves prior runs")
    run3 = llmii_db.create_tagger_run(conn, 'integration-test')
    meta3 = dict(meta)
    meta3['MWG:Keywords'] = ['Tag Delta']
    llmii_db.write_image_to_db(conn, r'C:\scratch\a.jpg', meta3, run3,
                               sha256='hash-a', keep_history=True)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT t.tag FROM image_keywords ik JOIN tags t ON t.id = ik.tag_id
            JOIN images i ON i.id = ik.image_id WHERE i.path = %s
        """, (r'C:\scratch\a.jpg',))
        tags_now = {r[0] for r in cur.fetchall()}
    check("keep_history retains existing tags", tags_now == {'Tag Gamma', 'Tag Delta'},
          f"got {tags_now}")

    print("\n[3] keyword_count is DISTINCT across runs")
    status = llmii_db.get_image_status_batch(conn, [r'C:\scratch\a.jpg'])
    ident, st, kw_count = status[r'C:\scratch\a.jpg']
    check("status batch returns row", st == 'success')
    check("keyword_count counts distinct tags", kw_count == 2, f"got {kw_count}")

    print("\n[4] duplicate embedded identifier gets fresh UUID (no IntegrityError)")
    llmii_db.write_image_to_db(conn, r'C:\scratch\copy-of-a.jpg', dict(meta), run1,
                               sha256='hash-a')
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT identifier) FROM images")
        check("identifiers distinct per path", cur.fetchone()[0] == 2)

    print("\n[5] find_duplicate_images via sha256")
    dupes = llmii_db.find_duplicate_images(conn)
    check("duplicate pair found", len(dupes) == 1 and len(dupes[0][1]) == 2,
          f"got {dupes}")

    print("\n[6] merge_tag preserves pinned performer_tags")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO performers (name) VALUES ('Test Performer') RETURNING id")
        perf_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM tags WHERE tag = 'Tag Gamma'")
        src_tag = cur.fetchone()[0]
        cur.execute("SELECT id FROM tags WHERE tag = 'Tag Delta'")
        tgt_tag = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO performer_tags (performer_id, tag_id, image_count, total_images, pinned)
            VALUES (%s, %s, 5, 10, TRUE)
        """, (perf_id, src_tag))
    conn.commit()
    llmii_db.merge_tag(conn, 'Tag Gamma', 'Tag Delta')
    with conn.cursor() as cur:
        cur.execute("SELECT tag_id, pinned FROM performer_tags WHERE performer_id = %s", (perf_id,))
        rows = cur.fetchall()
    check("pinned row remapped to target", rows == [(tgt_tag, True)], f"got {rows}")

    print("\n[7] backfill skips negated keywords")
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM images LIMIT 1")
        img_id = cur.fetchone()[0]
        for kw in ('not nude', 'fully nude'):
            cur.execute("""
                INSERT INTO image_keywords_unmatched (image_id, tagger_run_id, keyword)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (img_id, run1, kw))
    conn.commit()
    counts = llmii_db.backfill_normalizers(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT keyword FROM image_keywords_unmatched")
        remaining = {r[0] for r in cur.fetchall()}
    check("'fully nude' promoted", counts.get('Nude', 0) == 1, f"counts {counts}")
    check("'not nude' NOT promoted (stays unmatched)", 'not nude' in remaining,
          f"remaining {remaining}")

    print("\n[8] export/load round-trip keeps alias-less tags")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO tags (tag) VALUES ('Lonely Tag') ON CONFLICT DO NOTHING")
    conn.commit()
    exported = llmii_db.export_tags(conn)
    names = {e['Tag'] for e in exported}
    check("alias-less tag exported", 'Lonely Tag' in names)

    print("\n[9] case-only rename allowed")
    ok = llmii_db.rename_tag(conn, 'Lonely Tag', 'LONELY TAG')
    check("case-only rename succeeds", ok is True)

    print("\n[10] get_processed_paths: case-variant + LIKE-metachar prefix")
    paths_lower = llmii_db.get_processed_paths(conn, r'c:\SCRATCH')
    check("case-variant prefix matches", len(paths_lower) >= 1, f"got {paths_lower}")
    paths_meta = llmii_db.get_processed_paths(conn, r'c:\scr_tch')
    check("underscore not a wildcard", len(paths_meta) == 0, f"got {paths_meta}")

    print("\n[11] stats sanity")
    stats = llmii_db.get_stats(conn)
    check("get_stats runs", isinstance(stats, dict) and stats['total_images'] == 2)
    health = llmii_db.health_check(conn)
    check("health_check runs", isinstance(health, dict))

    llmii_db.finish_tagger_run(conn, run1, status='success')
    llmii_db.finish_tagger_run(conn, run2, status='cancelled')
    llmii_db.finish_tagger_run(conn, run3, status='failed')
    with conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM tagger_runs GROUP BY status ORDER BY status")
        run_statuses = dict(cur.fetchall())
    check("run statuses recorded (success/cancelled/failed)",
          run_statuses == {'cancelled': 1, 'failed': 1, 'success': 1},
          f"got {run_statuses}")

    conn.close()
    print(f"\n{'='*50}\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", FAIL)
        sys.exit(1)


if __name__ == '__main__':
    main()
