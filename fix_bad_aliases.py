#!/usr/bin/env python3
"""One-off: fix two mis-curated aliases found during image review and correct
the images they wrongly tagged. Idempotent; --apply to write (default dry-run).

1. Big Nipples (id 358) had nipple-STATE aliases (Erect/Hard/Pointy/Perky/...)
   and MEDIUM aliases — so every "erect nipples" image became "Big Nipples"
   (Erect Nipples tag had 0 images). Move the state aliases to Erect Nipples,
   the medium ones to their right tags, then un-tag images that got Big Nipples
   only via those (no genuine size word in their raw keywords) and tag them
   Erect Nipples instead.
2. Home Room (id 1491) had Steps/Stairs/Staircase aliases — outdoor-steps shots
   got mislabeled as an indoor room. Move those to the Stairs tag (2696) and
   un-tag the wrongly-tagged images.
"""
import json, sys
sys.path.insert(0, '.')
from src import llmii_db

APPLY = '--apply' in sys.argv

BIG, ERECT, MED_BOOBS, NIPPLES, HOMEROOM, STAIRS = 358, 1026, 1807, 1979, 1491, 2696

# Big Nipples aliases that are STATE, not size -> Erect Nipples
ERECT_ALIASES = ['erect nipple', 'erect nipples', 'erect nipplesm',
    'erect brown nipples', 'hard nipples', 'tight nipples', 'swollen nipples',
    'pointy nipples', 'pointy nips', 'perky nipples', 'glass cutter nipples',
    'nipples pop out']
# Big Nipples MEDIUM aliases -> their correct tags
MED_REASSIGN = {'tits (medium)': MED_BOOBS, 'nipple (medium)': NIPPLES}
# Home Room aliases that mean stairs -> Stairs tag
STAIR_ALIASES = ['steps', 'stairs', 'staircase']
# genuine Home Room aliases (keep)
HOME_WORDS = ['kitchen', 'bedroom', 'living room', 'livingroom', 'open living room',
    'dining room', 'laundry room', 'apartment house', 'stove',
    'pink girly teen bedroom', 'home room (kitchen/bedroom/etc)']
SIZE_RE = r'\m(big|huge|large|giant|massive|puffy|thick)'  # raw words that justify Big Nipples


def main():
    s = json.load(open('settings.json', encoding='utf-8'))
    conn = llmii_db.get_connection(s['db_host'], s['db_port'], s['db_user'],
                                   s['db_password'], s['db_name'])
    cur = conn.cursor()

    def count(sql, p):
        cur.execute(sql, p); return cur.fetchone()[0]

    # --- dry-run report ---
    big_lose = count("""
        SELECT COUNT(*) FROM image_keywords ik WHERE ik.tag_id=%s
          AND EXISTS (SELECT 1 FROM image_keywords_raw r WHERE r.image_id=ik.image_id AND lower(r.keyword)=ANY(%s))
          AND NOT EXISTS (SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND r2.keyword ~* %s)
    """, (BIG, ERECT_ALIASES, SIZE_RE))
    erect_gain = count("""
        SELECT COUNT(DISTINCT r.image_id) FROM image_keywords_raw r WHERE lower(r.keyword)=ANY(%s)
    """, (ERECT_ALIASES,))
    home_lose = count("""
        SELECT COUNT(*) FROM image_keywords ik WHERE ik.tag_id=%s
          AND EXISTS (SELECT 1 FROM image_keywords_raw r WHERE r.image_id=ik.image_id AND lower(r.keyword)=ANY(%s))
          AND NOT EXISTS (SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND lower(r2.keyword)=ANY(%s))
    """, (HOMEROOM, STAIR_ALIASES, HOME_WORDS))
    stairs_gain = count("""
        SELECT COUNT(DISTINCT r.image_id) FROM image_keywords_raw r WHERE lower(r.keyword)=ANY(%s)
    """, (STAIR_ALIASES,))

    print(f"{'DRY RUN — ' if not APPLY else ''}planned changes:")
    print(f"  alias moves: {len(ERECT_ALIASES)} state->Erect Nipples, "
          f"{len(MED_REASSIGN)} medium->correct tags, {len(STAIR_ALIASES)} ->Stairs")
    print(f"  Big Nipples removed from ~{big_lose:,} images (erect/state, no size word)")
    print(f"  Erect Nipples added to    ~{erect_gain:,} images")
    print(f"  Home Room removed from    ~{home_lose:,} images (steps/stairs, not a home room)")
    print(f"  Stairs added to           ~{stairs_gain:,} images")
    if not APPLY:
        print("\nRe-run with --apply to write.")
        conn.close(); return

    # --- apply ---
    # 1. Move aliases
    cur.execute("UPDATE tag_aliases SET tag_id=%s WHERE lower(alias)=ANY(%s)", (ERECT, ERECT_ALIASES))
    for alias, tid in MED_REASSIGN.items():
        cur.execute("UPDATE tag_aliases SET tag_id=%s WHERE lower(alias)=%s", (tid, alias))
    cur.execute("UPDATE tag_aliases SET tag_id=%s WHERE lower(alias)=ANY(%s)", (STAIRS, STAIR_ALIASES))

    # 2. Un-tag spurious Big Nipples
    cur.execute("""
        DELETE FROM image_keywords ik WHERE ik.tag_id=%s
          AND EXISTS (SELECT 1 FROM image_keywords_raw r WHERE r.image_id=ik.image_id AND lower(r.keyword)=ANY(%s))
          AND NOT EXISTS (SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND r2.keyword ~* %s)
    """, (BIG, ERECT_ALIASES, SIZE_RE))
    big_removed = cur.rowcount
    # 3. Add Erect Nipples from raw
    cur.execute("""
        INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
        SELECT DISTINCT r.image_id, %s, r.tagger_run_id FROM image_keywords_raw r
        WHERE lower(r.keyword)=ANY(%s) ON CONFLICT DO NOTHING
    """, (ERECT, ERECT_ALIASES))
    erect_added = cur.rowcount
    # 4. Un-tag spurious Home Room
    cur.execute("""
        DELETE FROM image_keywords ik WHERE ik.tag_id=%s
          AND EXISTS (SELECT 1 FROM image_keywords_raw r WHERE r.image_id=ik.image_id AND lower(r.keyword)=ANY(%s))
          AND NOT EXISTS (SELECT 1 FROM image_keywords_raw r2 WHERE r2.image_id=ik.image_id AND lower(r2.keyword)=ANY(%s))
    """, (HOMEROOM, STAIR_ALIASES, HOME_WORDS))
    home_removed = cur.rowcount
    # 5. Add Stairs from raw
    cur.execute("""
        INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
        SELECT DISTINCT r.image_id, %s, r.tagger_run_id FROM image_keywords_raw r
        WHERE lower(r.keyword)=ANY(%s) ON CONFLICT DO NOTHING
    """, (STAIRS, STAIR_ALIASES))
    stairs_added = cur.rowcount
    conn.commit(); conn.close()

    print(f"\nAPPLIED:")
    print(f"  Big Nipples removed:  {big_removed:,}")
    print(f"  Erect Nipples added:  {erect_added:,}")
    print(f"  Home Room removed:    {home_removed:,}")
    print(f"  Stairs added:         {stairs_added:,}")


if __name__ == '__main__':
    main()
