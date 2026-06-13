#!/usr/bin/env python3
"""Apply the Assorted-bucket recovery (tag_assorted_results.json): move
accidentally-ignored aliases off 'Assorted Additional Tags' to real/new tags,
move genuinely-unwanted ones into settings.json keyword_blacklist, and re-sort
the affected images. Idempotent. --apply to write (default dry-run).
"""
import json, sys
sys.path.insert(0, '.')
from src import llmii_db

APPLY = '--apply' in sys.argv
ASSORTED = 'Assorted Additional Tags'


def main():
    s = json.load(open('settings.json', encoding='utf-8'))
    r = json.load(open('tag_assorted_results.json', encoding='utf-8'))
    conn = llmii_db.get_connection(s['db_host'], s['db_port'], s['db_user'],
                                   s['db_password'], s['db_name'])
    cur = conn.cursor()
    cur.execute("SELECT tag, id FROM tags")
    tagid = {t.lower(): i for t, i in cur.fetchall()}
    aid = tagid[ASSORTED.lower()]

    recover = [c for c in r['recover'] if c['tag'].lower() in tagid]
    new_tags = r['new_tags']
    blacklist = sorted({c['keyword'] for c in r['blacklist']})

    print(f"{'DRY RUN — ' if not APPLY else ''}Assorted recovery:")
    print(f"  recover->existing: {len(recover)}   new tags: {len(new_tags)}   "
          f"blacklist->keyword_blacklist: {len(blacklist)}")
    if not APPLY:
        print("\nRe-run with --apply to write.")
        conn.close(); return

    # (keyword -> target tag id) for every alias we move off Assorted
    moves = {}
    # 1. New tags: create, map their aliases
    created = 0
    for nt in new_tags:
        tid = llmii_db._upsert_tag(cur, nt['tag']); created += 1
        for a in nt['aliases']:
            moves[a.lower()] = tid
    # 2. Recover aliases -> existing tags
    for c in recover:
        moves[c['keyword'].lower()] = tagid[c['tag'].lower()]

    # 3. Alias-table: move recovered/new aliases off Assorted; delete blacklisted
    for a, tid in moves.items():
        cur.execute("UPDATE tag_aliases SET tag_id=%s WHERE lower(alias)=%s AND tag_id=%s", (tid, a, aid))
    for kw in blacklist:
        cur.execute("DELETE FROM tag_aliases WHERE lower(alias)=%s AND tag_id=%s", (kw.lower(), aid))
    conn.commit()

    # 4. Retag: add the recovered/new target tag to images whose raw had the alias
    added = 0
    for a, tid in moves.items():
        cur.execute("""INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                       SELECT DISTINCT image_id, %s, tagger_run_id FROM image_keywords_raw
                       WHERE lower(keyword)=%s ON CONFLICT DO NOTHING""", (tid, a))
        added += cur.rowcount

    # 5. Strip the Assorted tag from images no longer justified by any remaining
    #    Assorted alias (the skipped/dormant ones) or the tag name itself.
    cur.execute("""
        DELETE FROM image_keywords ik WHERE ik.tag_id=%s AND NOT EXISTS (
            SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND (
                lower(r2.keyword)=%s
                OR EXISTS (SELECT 1 FROM tag_aliases a2 WHERE a2.tag_id=%s AND lower(a2.alias)=lower(r2.keyword))
            ))
    """, (aid, ASSORTED.lower(), aid))
    assorted_removed = cur.rowcount
    conn.commit(); conn.close()

    # 6. Merge blacklist into settings.json keyword_blacklist
    existing = set(s.get('keyword_blacklist', []))
    s['keyword_blacklist'] = sorted(existing | set(blacklist))
    json.dump(s, open('settings.json', 'w', encoding='utf-8'), indent=4, ensure_ascii=False)

    print(f"\nAPPLIED:")
    print(f"  new tags created:            {created}")
    print(f"  aliases moved off Assorted:  {len(moves)}")
    print(f"  aliases -> keyword_blacklist:{len(blacklist)} (now {len(s['keyword_blacklist'])} total)")
    print(f"  image-tags added (recovered):{added:,}")
    print(f"  Assorted tag stripped from:  {assorted_removed:,} images")


if __name__ == '__main__':
    main()
