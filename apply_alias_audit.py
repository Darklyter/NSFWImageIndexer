#!/usr/bin/env python3
"""Apply the alias-audit results (tag_alias_audit_results.json): reassign/remove
mis-curated aliases and retag affected images via image_keywords_raw provenance.
Idempotent. --apply to write (default dry-run).

Correction folded in: the light/pink-brown nipple/areola terms flagged 'remove'
from Dark Areolas are instead REASSIGNED to Pink Areolas (right shade, keeps
the color info) — see PINK_FIX.
"""
import json, sys
sys.path.insert(0, '.')
from src import llmii_db

APPLY = '--apply' in sys.argv
PINK_FIX = {'light brown nipples', 'pinkish-brown nipples', 'pink-brown nipples',
            'light brown areolas', 'pinkish brown nipples', 'pink brown nipples'}


def main():
    s = json.load(open('settings.json', encoding='utf-8'))
    sus = json.load(open('tag_alias_audit_results.json', encoding='utf-8'))

    # Fold in the Pink Areolas correction
    for c in sus:
        if c['alias'].lower() in PINK_FIX:
            c['action'] = 'reassign'; c['suggested_tag'] = 'Pink Areolas'

    conn = llmii_db.get_connection(s['db_host'], s['db_port'], s['db_user'],
                                   s['db_password'], s['db_name'])
    cur = conn.cursor()
    cur.execute('SELECT tag, id FROM tags')
    tagid = {t.lower(): i for t, i in cur.fetchall()}

    reassign = [c for c in sus if c['action'] == 'reassign' and c.get('suggested_tag', '').lower() in tagid]
    remove = [c for c in sus if c['action'] == 'remove']
    bad_target = [c for c in sus if c['action'] == 'reassign' and c.get('suggested_tag', '').lower() not in tagid]
    if bad_target:
        print("WARNING: reassign targets not found (skipped):",
              [(c['alias'], c['suggested_tag']) for c in bad_target])

    impactful = [c for c in sus if c['images'] > 0]
    print(f"{'DRY RUN — ' if not APPLY else ''}alias-audit apply:")
    print(f"  reassign aliases: {len(reassign)}   remove aliases: {len(remove)}")
    print(f"  suspects with live image impact: {len(impactful)} "
          f"(~{sum(c['images'] for c in impactful):,} image-tags to correct)")
    if not APPLY:
        print("\nRe-run with --apply to write.")
        conn.close(); return

    def wid(c): return tagid.get(c['tag'].lower())

    # Phase 1 — alias table changes
    for c in reassign:
        w, r = wid(c), tagid[c['suggested_tag'].lower()]
        if w:
            cur.execute("UPDATE tag_aliases SET tag_id=%s WHERE lower(alias)=%s AND tag_id=%s",
                        (r, c['alias'].lower(), w))
    for c in remove:
        w = wid(c)
        if w:
            cur.execute("DELETE FROM tag_aliases WHERE lower(alias)=%s AND tag_id=%s",
                        (c['alias'].lower(), w))
    conn.commit()

    # Phase 2 — retag images for the live ones, using the corrected alias table
    added = removed = 0
    for c in impactful:
        w = wid(c)
        if not w:
            continue
        a = c['alias'].lower()
        if c['action'] == 'reassign':
            r = tagid[c['suggested_tag'].lower()]
            cur.execute("""INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                           SELECT DISTINCT image_id, %s, tagger_run_id FROM image_keywords_raw
                           WHERE lower(keyword)=%s ON CONFLICT DO NOTHING""", (r, a))
            added += cur.rowcount
        # Remove the wrong tag W from images that had this alias and are no
        # longer justified for W by any remaining alias or W's own name.
        cur.execute("""
            DELETE FROM image_keywords ik
            WHERE ik.tag_id=%s
              AND EXISTS (SELECT 1 FROM image_keywords_raw r WHERE r.image_id=ik.image_id AND lower(r.keyword)=%s)
              AND NOT EXISTS (
                SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND (
                    lower(r2.keyword)=lower((SELECT tag FROM tags WHERE id=%s))
                    OR EXISTS (SELECT 1 FROM tag_aliases a2 WHERE a2.tag_id=%s AND lower(a2.alias)=lower(r2.keyword))
                ))
        """, (w, a, w, w))
        removed += cur.rowcount
    conn.commit(); conn.close()
    print(f"\nAPPLIED:")
    print(f"  aliases reassigned: {len(reassign)}   removed: {len(remove)}")
    print(f"  image-tags added (reassign targets): {added:,}")
    print(f"  image-tags removed (wrong tags):      {removed:,}")


if __name__ == '__main__':
    main()
