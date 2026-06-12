#!/usr/bin/env python3
"""Apply a tag-reconcile proposal (tag_proposals_full.json) to the database.

Pairs with the `tag-reconcile` workflow. Idempotent and reversible:
  - aliases:   INSERT INTO tag_aliases (keyword -> tag)  ON CONFLICT DO NOTHING
  - new_tags:  create the canonical tag, then add its member aliases
  - blacklist: merge keywords into settings.json 'keyword_blacklist' and
               delete those rows from image_keywords_unmatched
  - finally:   promote_aliased_unmatched() retro-tags every now-aliased image

Usage:  python apply_tag_proposals.py [proposals.json]   (default tag_proposals_full.json)
        python apply_tag_proposals.py --dry-run           (report only, no writes)
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src import llmii_db

PROPOSAL = next((a for a in sys.argv[1:] if not a.startswith('-')), 'tag_proposals_full.json')
DRY = '--dry-run' in sys.argv


def main():
    s = json.load(open('settings.json', encoding='utf-8'))
    p = json.load(open(PROPOSAL, encoding='utf-8'))
    canon = {t.strip().lower() for t in open('tag_canonical.txt', encoding='utf-8').read().splitlines() if t.strip()}

    # Fold any new_tag that collides with an existing canonical tag into plain
    # aliases on that existing tag (don't create a duplicate).
    aliases = [(a['keyword'], a['tag']) for a in p['aliases']]
    new_tags = []
    for nt in p['new_tags']:
        if nt['tag'].strip().lower() in canon:
            for a in nt['aliases']:
                aliases.append((a, nt['tag']))
        else:
            new_tags.append(nt)

    blacklist = sorted({b['keyword'] for b in p['blacklist']})

    print(f"proposal: {PROPOSAL}")
    print(f"  aliases to add:      {len(aliases)}")
    print(f"  new tags to create:  {len(new_tags)}")
    print(f"  blacklist keywords:  {len(blacklist)}")
    if DRY:
        print("DRY RUN — no changes written.")
        return

    conn = llmii_db.get_connection(s['db_host'], s['db_port'], s['db_user'],
                                   s['db_password'], s['db_name'])
    created_tags = added_aliases = 0
    try:
        with conn.cursor() as cur:
            # 1. New tags + their member aliases
            for nt in new_tags:
                tag_id = llmii_db._upsert_tag(cur, nt['tag'])
                created_tags += 1
                for a in nt['aliases']:
                    cur.execute(
                        "INSERT INTO tag_aliases (tag_id, alias) VALUES (%s,%s) "
                        "ON CONFLICT (alias) DO NOTHING", (tag_id, a))
                    added_aliases += cur.rowcount

            # 2. Aliases onto existing/just-created tags
            tag_id_cache = {}
            for keyword, tag in aliases:
                key = tag.strip().lower()
                if key not in tag_id_cache:
                    tag_id_cache[key] = llmii_db._upsert_tag(cur, tag)
                cur.execute(
                    "INSERT INTO tag_aliases (tag_id, alias) VALUES (%s,%s) "
                    "ON CONFLICT (alias) DO NOTHING", (tag_id_cache[key], keyword))
                added_aliases += cur.rowcount

            # 3. Blacklist: delete those rows from unmatched (case-insensitive)
            cur.execute(
                "DELETE FROM image_keywords_unmatched WHERE lower(keyword) = ANY(%s)",
                ([b.lower() for b in blacklist],))
            deleted_unmatched = cur.rowcount
        conn.commit()

        # 4. Retro-tag every now-aliased unmatched image
        promoted = llmii_db.promote_aliased_unmatched(conn)
    finally:
        conn.close()

    # 5. Persist keyword_blacklist into settings.json (merge with existing)
    existing_bl = set(s.get('keyword_blacklist', []))
    s['keyword_blacklist'] = sorted(existing_bl | set(blacklist))
    json.dump(s, open('settings.json', 'w', encoding='utf-8'), indent=4, ensure_ascii=False)

    print(f"APPLIED:")
    print(f"  new tags created:        {created_tags}")
    print(f"  aliases added:           {added_aliases}")
    print(f"  unmatched rows deleted:  {deleted_unmatched:,} (blacklisted)")
    print(f"  images retro-tagged:     {promoted:,} (promote_aliased_unmatched)")
    print(f"  keyword_blacklist now:   {len(s['keyword_blacklist'])} terms in settings.json")


if __name__ == '__main__':
    main()
