"""PostgreSQL database integration for ImageIndexer.

Provides connection helpers, path-based gallery/performer parsing, and
functions to write image processing results into the ai_captioning schema.
"""

import re
import os
import uuid as _uuid_mod
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

_SCHEMA = 'ai_captioning'
# Sentinel value used in debug_map for blacklisted tags (matches llmii._BLACKLISTED)
_BLACKLISTED_SENTINEL = "__blacklisted__"

# Date folder pattern: YYYY-MM or YYYY-MM-DD
_DATE_RE = re.compile(r'^\d{4}-\d{2}')

# Date in zip filename: YYYY-MM-DD
_ZIP_DATE_RE = re.compile(r'\d{4}-\d{2}-\d{2}')


def parse_gallery_and_performers(file_path):
    """Return (gallery_name, [performer_names]) derived from the file path.

    Gallery  — always the immediate parent directory of the image file.
    Performers — only populated when 'SuicideGirls' (case-insensitive) appears
                 somewhere in the path.

    SuicideGirls path structure:
        .../SuicideGirls/[alpha-index/]Performer/YYYY-MM[-DD]/Gallery/image.jpg

    The performer is the path segment immediately before the first YYYY-MM date
    segment that follows the SuicideGirls directory. A single-letter alpha-index
    folder is automatically skipped because the performer is always the segment
    directly before the date, not directly after SuicideGirls.

    If the performer string contains ' and ' (case-insensitive) it is split into
    multiple performer names.

    Examples:
        C:/temp/SuicideGirls/Adavisser/2022-11/Body Heat/img.jpg
            -> gallery="Body Heat", performers=["Adavisser"]

        Z:/SuicideGirls/A/Abbiss and Ginary/2010-08/Amor Et Psyche/img.jpg
            -> gallery="Amor Et Psyche", performers=["Abbiss", "Ginary"]
    """
    path = Path(file_path)
    gallery = path.parent.name  # immediate parent directory

    parts = path.parts

    # Locate 'SuicideGirls' in the path (case-insensitive)
    sg_idx = None
    for i, part in enumerate(parts):
        if part.lower() == 'suicidegirls':
            sg_idx = i
            break

    if sg_idx is None:
        return gallery, []

    after_sg = parts[sg_idx + 1:]

    # Find the first date segment (YYYY-MM) after the SuicideGirls directory
    date_idx = None
    for i, part in enumerate(after_sg):
        if _DATE_RE.match(part):
            date_idx = i
            break

    # Need at least one segment before the date to be the performer
    if date_idx is None or date_idx == 0:
        return gallery, []

    performer_str = after_sg[date_idx - 1]

    # Split on ' and ' for multi-performer sets
    if re.search(r'\s+and\s+', performer_str, flags=re.IGNORECASE):
        performers = [p.strip() for p in re.split(r'\s+and\s+', performer_str, flags=re.IGNORECASE)]
    else:
        performers = [performer_str]

    return gallery, performers


def parse_zip_metadata(zip_path):
    """Extract studio name and performer names from a zip filename.

    Studio detection (applied in order; first match wins):
      1. Text before the first ' - ' separator in the stem.
      2. Text before the first YYYY-MM-DD date pattern in the stem.
      3. No studio detected (returns None).

    Performer detection:
      Parenthesised block at the end of the stem, e.g. '(Alice, Bob)'.
      Names are split on commas.  Bracketed extras like '[17]' or '[480x320]'
      are stripped before parsing.

    Parameters
    ----------
    zip_path : str or Path — path to (or just the filename of) the zip file.

    Returns
    -------
    (studio_name_or_None, [performer_names])

    Examples
    --------
    'Studio Name - 2023-01-15 Set Title (Alice, Bob) [12] [1280x960].zip'
        → studio='Studio Name', performers=['Alice', 'Bob']

    'Pornstar Platinum 2010-07-14 Erotic Reading (Charisma Cappelli) [17] [480x320].zip'
        → studio='Pornstar Platinum', performers=['Charisma Cappelli']

    'Just A Gallery Name.zip'
        → studio=None, performers=[]
    """
    stem = Path(zip_path).stem  # filename without .zip

    # Strip bracketed extras like [17] [480x320] that appear after the main title
    clean = re.sub(r'\s*\[[^\]]*\]\s*', ' ', stem).strip()

    # Extract performers from a (Name1, Name2) block
    performers = []
    perf_match = re.search(r'\(([^)]+)\)', clean)
    if perf_match:
        perf_str = perf_match.group(1)
        performers = [p.strip() for p in perf_str.split(',') if p.strip()]
        # Remove the performer block so it doesn't interfere with studio detection
        clean = (clean[:perf_match.start()] + clean[perf_match.end():]).strip()

    # Studio detection — rule 1: text before first ' - '
    studio = None
    dash_pos = clean.find(' - ')
    if dash_pos != -1:
        candidate = clean[:dash_pos].strip()
        studio = candidate or None
    else:
        # Rule 2: text before first YYYY-MM-DD date
        date_match = _ZIP_DATE_RE.search(clean)
        if date_match:
            candidate = clean[:date_match.start()].strip()
            studio = candidate or None

    return studio, performers


# ---------------------------------------------------------------------------
# Internal upsert helpers — all accept an open cursor
# ---------------------------------------------------------------------------

def _upsert_gallery(cur, name):
    """Insert gallery if not present, return its id.

    Uses DO NOTHING to avoid advancing the sequence on every call for an
    already-existing gallery (DO UPDATE consumes a sequence value even when
    the conflict branch is taken, causing large gaps in gallery IDs).
    """
    cur.execute(
        "INSERT INTO galleries (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute("SELECT id FROM galleries WHERE name = %s", (name,))
        row = cur.fetchone()
    return row[0]


def _upsert_performer(cur, name):
    """Insert performer if not present, return its id."""
    cur.execute(
        "INSERT INTO performers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute("SELECT id FROM performers WHERE name = %s", (name,))
        row = cur.fetchone()
    return row[0]


def _upsert_image(cur, file_path, identifier, gallery_id=None, zip_source=None):
    """Insert or update image row, return its id.

    For images extracted from zip archives, file_path may be a composite key
    of the form ``zip_absolute_path::internal/archive/path``.  In that case the
    stored filename is extracted from the internal portion of the key.
    """
    path_str = str(file_path)
    if '::' in path_str:
        # Composite key: zip_path::internal/path
        _, internal = path_str.split('::', 1)
        filename = Path(internal).name
    else:
        filename = Path(path_str).name

    cur.execute(
        """
        INSERT INTO images (identifier, filename, path, gallery_id, zip_source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (path) DO UPDATE SET
            identifier = EXCLUDED.identifier,
            gallery_id = EXCLUDED.gallery_id,
            zip_source = EXCLUDED.zip_source,
            updated_at = now()
        RETURNING id
        """,
        (identifier, filename, path_str, gallery_id, zip_source),
    )
    return cur.fetchone()[0]


def _upsert_studio(cur, name):
    """Insert studio if not present, return its id."""
    cur.execute(
        "INSERT INTO studios (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute("SELECT id FROM studios WHERE name = %s", (name,))
        row = cur.fetchone()
    return row[0]


def _upsert_tag(cur, tag_name):
    """Insert canonical tag if not present, return its id.

    Uses DO NOTHING to avoid advancing the sequence for every already-matched
    keyword written per image.  The tags.tag column is citext so the fallback
    SELECT uses case-insensitive comparison automatically.
    """
    cur.execute(
        "INSERT INTO tags (tag) VALUES (%s) ON CONFLICT (tag) DO NOTHING RETURNING id",
        (tag_name,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute("SELECT id FROM tags WHERE tag = %s", (tag_name,))
        row = cur.fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# Schema migration helpers
# ---------------------------------------------------------------------------

def apply_migrations(conn):
    """Apply incremental schema changes that may not exist in older databases.

    Safe to call on every startup — all statements use IF NOT EXISTS / ADD
    COLUMN IF NOT EXISTS so they are no-ops when the schema is already current.
    """
    migrations = [
        # studios tables (add_studios.sql equivalent)
        """
        CREATE TABLE IF NOT EXISTS studios (
            id         SERIAL PRIMARY KEY,
            name       TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS studio_galleries (
            studio_id  INTEGER NOT NULL REFERENCES studios(id)  ON DELETE CASCADE,
            gallery_id INTEGER NOT NULL REFERENCES galleries(id) ON DELETE CASCADE,
            PRIMARY KEY (studio_id, gallery_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS studio_galleries_gallery_idx ON studio_galleries (gallery_id)",
        """
        CREATE TABLE IF NOT EXISTS studio_images (
            studio_id INTEGER NOT NULL REFERENCES studios(id) ON DELETE CASCADE,
            image_id  INTEGER NOT NULL REFERENCES images(id)  ON DELETE CASCADE,
            PRIMARY KEY (studio_id, image_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS studio_images_image_idx ON studio_images (image_id)",
        # zip_source column (add_zip_source.sql equivalent)
        "ALTER TABLE images ADD COLUMN IF NOT EXISTS zip_source TEXT",
        # Fix FK type mismatch: studio linking tables declared gallery_id/image_id as
        # INTEGER but galleries.id and images.id are BIGINT.  ALTER is a no-op if the
        # column is already bigint.
        "ALTER TABLE studio_galleries ALTER COLUMN gallery_id TYPE bigint",
        "ALTER TABLE studio_images    ALTER COLUMN image_id   TYPE bigint",
        # Index on zip_source so queries like "all images from zip X" don't scan the table
        "CREATE INDEX IF NOT EXISTS images_zip_source_idx ON images (zip_source) WHERE zip_source IS NOT NULL",
        # performer_tags: canonical tags statistically assigned to performers
        """
        CREATE TABLE IF NOT EXISTS performer_tags (
            performer_id  INTEGER NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
            tag_id        INTEGER NOT NULL REFERENCES tags(id)       ON DELETE CASCADE,
            image_count   INTEGER NOT NULL DEFAULT 0,
            total_images  INTEGER NOT NULL DEFAULT 0,
            assigned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (performer_id, tag_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS performer_tags_tag_idx ON performer_tags (tag_id)",
        # performer_tags override columns (manual pin/exclude/add)
        "ALTER TABLE performer_tags ADD COLUMN IF NOT EXISTS pinned         BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE performer_tags ADD COLUMN IF NOT EXISTS excluded       BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE performer_tags ADD COLUMN IF NOT EXISTS manually_added BOOLEAN NOT NULL DEFAULT FALSE",
        # tags: global flag to exclude a tag from all performer_tags
        "ALTER TABLE tags ADD COLUMN IF NOT EXISTS exclude_from_performers BOOLEAN NOT NULL DEFAULT FALSE",
    ]
    with conn.cursor() as cur:
        for sql in migrations:
            cur.execute(sql)
    conn.commit()


def get_connection(host, port, user, password, dbname, apply_schema_migrations=True):
    """Open and return a psycopg2 connection to the ai_captioning schema.

    When *apply_schema_migrations* is True (default), incremental schema changes
    (studios tables, zip_source column, etc.) are applied automatically so the
    caller never needs to run the SQL migration files by hand.

    Raises ImportError if psycopg2 is not installed.
    Raises psycopg2.OperationalError on connection failure.
    """
    if not HAS_PSYCOPG2:
        raise ImportError(
            "psycopg2 is not installed. Run: pip install psycopg2-binary"
        )
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        dbname=dbname,
        options=f'-c search_path={_SCHEMA},public',
    )
    if apply_schema_migrations:
        try:
            apply_migrations(conn)
        except Exception as e:
            print(f"Warning: schema migration error: {e}")
    return conn


# ---------------------------------------------------------------------------
# Public run management
# ---------------------------------------------------------------------------

def create_tagger_run(conn, tagger_name='ImageIndexer', params=None):
    """Insert a new tagger_run row with status='running', return its id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tagger_runs (tagger_name, params_json, status)
            VALUES (%s, %s, 'running')
            RETURNING id
            """,
            (tagger_name, psycopg2.extras.Json(params) if params else None),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_tagger_run(conn, run_id, status='success'):
    """Mark a tagger_run as finished with the given status."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tagger_runs SET status = %s, finished_at = now() WHERE id = %s",
            (status, run_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Skip-check helper
# ---------------------------------------------------------------------------

def get_image_status_batch(conn, file_paths):
    """Return {path: (identifier, status, keyword_count)} for a batch of file paths.

    For each path already recorded in the database, returns the stored
    identifier UUID, the most-recent processing status across all tagger
    runs, and the total number of matched keywords stored for the image.
    Paths not found in the database are omitted from the result.

    Used by FileProcessor._get_metadata_batch() so that DB-mode runs can
    skip previously processed images without relying on JSON sidecars.
    """
    if not file_paths:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.path,
                   i.identifier,
                   (SELECT irs.status
                    FROM image_run_status irs
                    WHERE irs.image_id = i.id
                    ORDER BY irs.processed_at DESC
                    LIMIT 1) AS status,
                   (SELECT COUNT(*)
                    FROM image_keywords ik
                    WHERE ik.image_id = i.id) AS keyword_count
            FROM images i
            WHERE i.path = ANY(%s)
            """,
            (list(file_paths),),
        )
        return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Studio helpers
# ---------------------------------------------------------------------------

def upsert_studio(conn, name):
    """Ensure a studio row exists and return its id.

    Safe to call repeatedly; no error if the studio already exists.
    """
    with conn.cursor() as cur:
        studio_id = _upsert_studio(cur, name)
    conn.commit()
    return studio_id


def link_studio_gallery(conn, studio_name, gallery_name):
    """Link a studio to a gallery by name, creating both if necessary.

    Returns (studio_id, gallery_id).
    """
    with conn.cursor() as cur:
        studio_id  = _upsert_studio(cur, studio_name)
        gallery_id = _upsert_gallery(cur, gallery_name)
        cur.execute(
            """
            INSERT INTO studio_galleries (studio_id, gallery_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (studio_id, gallery_id),
        )
    conn.commit()
    return studio_id, gallery_id


def link_studio_image(conn, studio_name, image_path):
    """Link a studio to an image by file path.

    The image row must already exist (i.e. write_image_to_db has been called).
    Returns (studio_id, image_id), or raises ValueError if the image is not
    found in the database.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM images WHERE path = %s", (str(image_path),))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Image not found in database: {image_path}")
        image_id  = row[0]
        studio_id = _upsert_studio(cur, studio_name)
        cur.execute(
            """
            INSERT INTO studio_images (studio_id, image_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (studio_id, image_id),
        )
    conn.commit()
    return studio_id, image_id


# ---------------------------------------------------------------------------
# Main write function
# ---------------------------------------------------------------------------

def write_image_to_db(conn, file_path, metadata, run_id, zip_source=None):
    """Write all image processing results for one image to the database.

    Parameters
    ----------
    conn       : open psycopg2 connection
    file_path  : absolute path to the source image file, OR a composite key
                 of the form ``zip_absolute_path::internal/archive/path`` for
                 images that were extracted from a zip archive.
    metadata   : dict produced by FileProcessor — expected keys:
                   MWG:Description, MWG:Keywords, XMP:Identifier, XMP:Status,
                   _raw_keywords (list), _debug_map (dict).
                 Optional zip-specific keys set by FileProcessor._expand_zips():
                   _zip_studio    — studio name detected from the zip filename
                   _zip_performers — list of performer names from the zip filename
    run_id     : tagger_runs.id for the current processing run
    zip_source : base filename of the originating zip archive (e.g. 'sets.zip'),
                 or None for regular image files.
    """
    # images.identifier is uuid NOT NULL — generate a UUID if none was assigned yet
    # (process_file always assigns one, but guard here so the INSERT never fails)
    identifier   = metadata.get('XMP:Identifier') or str(_uuid_mod.uuid4())
    description  = metadata.get('MWG:Description') or ''
    keywords     = metadata.get('MWG:Keywords') or []
    raw_keywords = metadata.get('_raw_keywords') or []
    debug_map    = metadata.get('_debug_map') or {}
    status       = metadata.get('XMP:Status') or 'success'

    # For zip images the DB key is a composite 'zip_path::internal_path'.
    # Use the internal path portion for gallery/performer parsing.
    file_path_str = str(file_path)
    is_zip_image  = '::' in file_path_str
    if is_zip_image:
        zip_part, internal_part = file_path_str.split('::', 1)
        parse_path = internal_part          # parse gallery/performers from internal path
        detection_path = file_path_str      # SuicideGirls/Galleries check on full composite
    else:
        parse_path = file_path_str
        detection_path = file_path_str

    gallery_name, performer_names = parse_gallery_and_performers(parse_path)

    # Merge performers from the zip filename (higher priority than path-based detection)
    zip_performers = metadata.get('_zip_performers') or []
    if zip_performers:
        performer_names = zip_performers

    # Studio detection — priority order:
    #   1. SuicideGirls (path-based, highest priority)
    #   2. Zip filename studio  (_zip_studio in metadata)
    #   3. Galleries directory (path-based)
    _zip_studio      = metadata.get('_zip_studio')
    _detected_studio = None

    # Walk all path components (for composite keys, check both sides)
    _check_parts = []
    for _seg in detection_path.replace('::', '/').split('/'):
        # Also split Windows backslash segments
        for _part in _seg.split('\\'):
            if _part:
                _check_parts.append(_part)

    _sg_found      = False
    _galleries_dir = None
    for _part in _check_parts:
        if _part.lower().replace(' ', '') == 'suicidegirls':
            _sg_found = True
            break
        if _part.lower() == 'galleries':
            # The studio is the directory the image lives in (parent dir)
            if is_zip_image:
                _galleries_dir = Path(internal_part).parent.name or None
            else:
                _galleries_dir = Path(file_path_str).parent.name or None

    if _sg_found:
        _detected_studio = 'Suicide Girls'
    elif _zip_studio:
        _detected_studio = _zip_studio
    elif _galleries_dir:
        _detected_studio = _galleries_dir

    try:
        with conn.cursor() as cur:
            # Gallery
            gallery_id = _upsert_gallery(cur, gallery_name) if gallery_name else None

            # Image (pass composite key as path; zip_source stored for provenance)
            image_id = _upsert_image(cur, file_path, identifier, gallery_id, zip_source)

            # Studio (auto-detected from path)
            if _detected_studio:
                studio_id = _upsert_studio(cur, _detected_studio)
                if gallery_id is not None:
                    cur.execute(
                        """
                        INSERT INTO studio_galleries (studio_id, gallery_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (studio_id, gallery_id),
                    )
                cur.execute(
                    """
                    INSERT INTO studio_images (studio_id, image_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (studio_id, image_id),
                )

            # Performers
            for pname in performer_names:
                perf_id = _upsert_performer(cur, pname)
                cur.execute(
                    """
                    INSERT INTO image_performers (image_id, performer_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (image_id, perf_id),
                )

            # Per-image run status
            cur.execute(
                """
                INSERT INTO image_run_status (image_id, tagger_run_id, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (image_id, tagger_run_id) DO UPDATE SET
                    status       = EXCLUDED.status,
                    processed_at = now()
                """,
                (image_id, run_id, status),
            )

            # Description (one row per image, updated in place)
            if description:
                cur.execute(
                    """
                    INSERT INTO image_descriptions (image_id, description, tagger_run_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (image_id) DO UPDATE SET
                        description   = EXCLUDED.description,
                        tagger_run_id = EXCLUDED.tagger_run_id,
                        updated_at    = now()
                    """,
                    (image_id, description, run_id),
                )

            # Replace keyword sets for this image+run (handles reprocessing)
            cur.execute(
                "DELETE FROM image_keywords WHERE image_id = %s AND tagger_run_id = %s",
                (image_id, run_id),
            )
            cur.execute(
                "DELETE FROM image_keywords_raw WHERE image_id = %s AND tagger_run_id = %s",
                (image_id, run_id),
            )
            cur.execute(
                "DELETE FROM image_keywords_unmatched WHERE image_id = %s AND tagger_run_id = %s",
                (image_id, run_id),
            )

            # Final matched keywords → canonical tags
            for kw in keywords:
                tag_id = _upsert_tag(cur, kw)
                cur.execute(
                    """
                    INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (image_id, tag_id, run_id),
                )

            # Raw LLM output keywords
            for kw in raw_keywords:
                cur.execute(
                    """
                    INSERT INTO image_keywords_raw (image_id, tagger_run_id, keyword)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (image_id, run_id, kw),
                )

            # Unmatched keywords: debug_map value is None (not blacklisted, not matched)
            for kw, resolved in debug_map.items():
                if resolved is None:
                    cur.execute(
                        """
                        INSERT INTO image_keywords_unmatched (image_id, tagger_run_id, keyword)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (image_id, run_id, kw),
                    )

        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def load_tags_from_file(conn, json_path, progress_callback=None):
    """Load tags and aliases from a mastertags JSON file into the database.

    The file is expected to be a JSON array of objects with "Tag" and "Alias"
    keys (title-case, as produced by mastertags.json).  Entries are inserted
    with ON CONFLICT DO NOTHING so existing tags and aliases are never
    overwritten — safe to call repeatedly for incremental patch files.

    Parameters
    ----------
    conn              : open psycopg2 connection
    json_path         : path to the JSON file (str or Path)
    progress_callback : optional callable(current, total) for progress updates

    Returns
    -------
    dict with keys: tags_added, aliases_added, tags_skipped, aliases_skipped
    """
    import json as _json

    with open(json_path, 'r', encoding='utf-8') as f:
        entries = _json.load(f)

    total = len(entries)

    # Collect unique tag names and build alias list
    # Use dict to preserve insertion order and deduplicate tags
    tag_names = {}   # lower -> original-case first seen
    alias_pairs = [] # list of (tag_lower, alias_original)

    for entry in entries:
        tag   = (entry.get('Tag')   or entry.get('tag')   or '').strip()
        alias = (entry.get('Alias') or entry.get('alias') or '').strip()
        if not tag or not alias:
            continue
        tag_names.setdefault(tag.lower(), tag)
        alias_pairs.append((tag.lower(), alias))

    stats = {'tags_added': 0, 'aliases_added': 0,
             'tags_skipped': 0, 'aliases_skipped': 0}

    with conn.cursor() as cur:
        # --- 1. Bulk-insert all unique tag names ---
        # We batch these to avoid hitting parameter limits.
        unique_tags = list(tag_names.values())
        batch_size = 500

        for i in range(0, len(unique_tags), batch_size):
            batch = unique_tags[i:i + batch_size]
            # Build a multi-row INSERT; citext handles case-insensitive uniqueness
            args = ','.join(cur.mogrify('(%s)', (t,)).decode() for t in batch)
            cur.execute(
                f'INSERT INTO tags (tag) VALUES {args} '
                f'ON CONFLICT (tag) DO NOTHING'
            )
            stats['tags_added'] += cur.rowcount
            stats['tags_skipped'] += len(batch) - cur.rowcount

        conn.commit()

        # --- 2. Fetch id map for all known tags (case-insensitive via citext) ---
        cur.execute('SELECT lower(tag::text), id FROM tags')
        tag_id_map = {row[0]: row[1] for row in cur.fetchall()}

        # --- 3. Bulk-insert aliases in batches ---
        processed = 0
        for i in range(0, len(alias_pairs), batch_size):
            batch = alias_pairs[i:i + batch_size]
            rows = []
            for tag_lower, alias in batch:
                tag_id = tag_id_map.get(tag_lower)
                if tag_id is not None:
                    rows.append((tag_id, alias))

            if rows:
                args = ','.join(
                    cur.mogrify('(%s,%s)', r).decode() for r in rows
                )
                cur.execute(
                    f'INSERT INTO tag_aliases (tag_id, alias) VALUES {args} '
                    f'ON CONFLICT (alias) DO NOTHING'
                )
                stats['aliases_added']   += cur.rowcount
                stats['aliases_skipped'] += len(rows) - cur.rowcount

            processed += len(batch)
            if progress_callback:
                progress_callback(processed, total)

        conn.commit()

    return stats


def assign_performer_tags(conn):
    """Assign canonical tags to performers based on statistical prevalence.

    For each performer that has at least one processed image, finds all tags
    that appear on more than 40% of that performer's images and upserts rows
    into performer_tags.  Rows that no longer meet the threshold are removed,
    except those that are pinned, manually added, or marked as excluded
    (excluded rows are tombstones that prevent auto-reassignment).

    Tags with ``exclude_from_performers = TRUE`` on the tags table are skipped
    globally regardless of prevalence.

    Returns a dict with keys:
        performers_checked  — number of performers evaluated
        tags_assigned       — rows inserted or updated in performer_tags
        tags_removed        — rows removed (no longer meet threshold)
    """
    assigned = 0
    removed = 0

    with conn.cursor() as cur:
        # All performers with at least one image in image_performers
        cur.execute("""
            SELECT p.id, COUNT(ip.image_id) AS total
            FROM   performers p
            JOIN   image_performers ip ON ip.performer_id = p.id
            GROUP  BY p.id
            HAVING COUNT(ip.image_id) > 0
        """)
        performers = cur.fetchall()

        for performer_id, total_images in performers:
            # Tags on more than 40% of this performer's images.
            # count * 5 > total * 2  ⟺  count/total > 2/5 = 0.40  (avoids float division)
            # Skip globally excluded tags.
            cur.execute("""
                SELECT ik.tag_id, COUNT(DISTINCT ik.image_id) AS tag_count
                FROM   image_performers ip
                JOIN   image_keywords   ik ON ik.image_id = ip.image_id
                JOIN   tags             t  ON t.id = ik.tag_id
                WHERE  ip.performer_id = %s
                  AND  NOT t.exclude_from_performers
                GROUP  BY ik.tag_id
                HAVING COUNT(DISTINCT ik.image_id) * 5 > %s * 2
            """, (performer_id, total_images))
            qualifying = {row[0]: row[1] for row in cur.fetchall()}

            if qualifying:
                # Upsert qualifying tags.
                # ON CONFLICT DO UPDATE only fires when NOT excluded (excluded rows are
                # tombstones — they block re-insertion without being removed).
                for tag_id, image_count in qualifying.items():
                    cur.execute("""
                        INSERT INTO performer_tags
                            (performer_id, tag_id, image_count, total_images, assigned_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (performer_id, tag_id) DO UPDATE
                            SET image_count  = EXCLUDED.image_count,
                                total_images = EXCLUDED.total_images,
                                assigned_at  = now()
                        WHERE NOT performer_tags.excluded
                    """, (performer_id, tag_id, image_count, total_images))
                    assigned += cur.rowcount

                # Remove stale threshold rows — keep pinned, manually_added, and excluded.
                cur.execute("""
                    DELETE FROM performer_tags
                    WHERE  performer_id = %s
                      AND  tag_id != ALL(%s)
                      AND  NOT pinned
                      AND  NOT manually_added
                      AND  NOT excluded
                """, (performer_id, list(qualifying.keys())))
                removed += cur.rowcount
            else:
                # Performer has images but no qualifying tags — remove non-protected rows.
                cur.execute("""
                    DELETE FROM performer_tags
                    WHERE  performer_id = %s
                      AND  NOT pinned
                      AND  NOT manually_added
                      AND  NOT excluded
                """, (performer_id,))
                removed += cur.rowcount

    conn.commit()
    return {
        'performers_checked': len(performers),
        'tags_assigned':      assigned,
        'tags_removed':       removed,
    }


def pin_performer_tag(conn, performer_id, tag_id, pinned=True):
    """Set or clear the pinned flag on a performer_tag row.

    Pinned tags survive threshold-based stale removal in assign_performer_tags.
    No-op if the row does not exist or is marked excluded.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE performer_tags
               SET pinned = %s
             WHERE performer_id = %s AND tag_id = %s AND NOT excluded
        """, (pinned, performer_id, tag_id))
    conn.commit()


def exclude_performer_tag(conn, performer_id, tag_id):
    """Mark a tag as excluded for one performer.

    Creates a tombstone row (excluded=TRUE) if one doesn't already exist,
    or flips any existing row to excluded and clears pinned/manually_added.
    The tombstone prevents assign_performer_tags from re-inserting the tag.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO performer_tags
                (performer_id, tag_id, image_count, total_images,
                 excluded, pinned, manually_added, assigned_at)
            VALUES (%s, %s, 0, 0, TRUE, FALSE, FALSE, now())
            ON CONFLICT (performer_id, tag_id) DO UPDATE
                SET excluded       = TRUE,
                    pinned         = FALSE,
                    manually_added = FALSE
        """, (performer_id, tag_id))
    conn.commit()


def include_performer_tag(conn, performer_id, tag_id):
    """Remove a per-performer exclusion tombstone.

    Deletes the excluded row so assign_performer_tags can re-add the tag if
    the threshold is met, or you can manually re-add it via add_performer_tag.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM performer_tags
            WHERE performer_id = %s AND tag_id = %s AND excluded = TRUE
        """, (performer_id, tag_id))
    conn.commit()


def exclude_tag_globally(conn, tag_id):
    """Globally exclude a tag from all performer_tags assignments.

    Sets ``tags.exclude_from_performers = TRUE`` and removes every
    performer_tags row for this tag (regardless of pinned/manually_added).
    After this call, assign_performer_tags will skip the tag for all performers.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tags SET exclude_from_performers = TRUE WHERE id = %s",
            (tag_id,)
        )
        cur.execute(
            "DELETE FROM performer_tags WHERE tag_id = %s",
            (tag_id,)
        )
    conn.commit()


def include_tag_globally(conn, tag_id):
    """Clear the global performer exclusion on a tag.

    Sets ``tags.exclude_from_performers = FALSE``.  Does not automatically
    re-run assignment — call assign_performer_tags to repopulate.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tags SET exclude_from_performers = FALSE WHERE id = %s",
            (tag_id,)
        )
    conn.commit()


def add_performer_tag(conn, performer_id, tag_id):
    """Manually assign a tag to a performer.

    Inserts a row with ``manually_added=TRUE, pinned=TRUE``.  If an excluded
    tombstone exists for this pair it is cleared (the manual add takes
    precedence).  The row survives future assign_performer_tags runs.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO performer_tags
                (performer_id, tag_id, image_count, total_images,
                 manually_added, pinned, excluded, assigned_at)
            VALUES (%s, %s, 0, 0, TRUE, TRUE, FALSE, now())
            ON CONFLICT (performer_id, tag_id) DO UPDATE
                SET manually_added = TRUE,
                    pinned         = TRUE,
                    excluded       = FALSE
        """, (performer_id, tag_id))
    conn.commit()


def get_all_tags(conn):
    """Return ``[(id, tag_name)]`` for all canonical tags, sorted by name."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, tag
            FROM   tags
            WHERE  NOT COALESCE(exclude_from_performers, FALSE)
            ORDER  BY tag
        """)
        return cur.fetchall()


def clear_database(conn):
    """Truncate image / run tables while preserving the master tag vocabulary.

    The tags and tag_aliases tables are intentionally excluded so that the
    curated tag list survives a database reset.  Everything related to
    processed images and tagger runs is removed.
    """
    tables = [
        'image_keywords_unmatched',
        'image_keywords_raw',
        'image_keywords',
        'image_descriptions',
        'image_run_status',
        'image_performers',
        'performer_tags',
        'studio_images',
        'studio_galleries',
        'tagger_runs',
        'images',
        'galleries',
        'performers',
        'studios',
    ]
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f'TRUNCATE TABLE {table} CASCADE')
    conn.commit()


def backfill_colored_hair(conn):
    """Promote unmatched hair-color keywords to 'Colored Hair' / 'Multicolored Hair'.

    Scans every row in image_keywords_unmatched whose keyword ends in 'hair'
    and contains at least one unnatural colour (blue, pink, purple, etc.).
    Matching rows are:
      - inserted into image_keywords with the appropriate canonical tag, and
      - deleted from image_keywords_unmatched.

    Natural hair colours (black, brown, blonde, red, auburn, …) are left
    untouched so they continue to appear in the unmatched list for manual review.

    Returns (colored_count, multicolored_count) — the number of rows promoted
    into each tag.
    """
    _HAIR_COLOR_WORDS = frozenset({
        'black', 'white', 'red', 'blue', 'green', 'yellow', 'pink', 'purple',
        'orange', 'brown', 'grey', 'gray', 'silver', 'gold', 'golden', 'teal',
        'turquoise', 'lavender', 'violet', 'magenta', 'cyan', 'coral', 'amber',
        'blonde', 'brunette', 'auburn', 'platinum', 'strawberry', 'ash',
        'copper', 'chestnut', 'caramel', 'honey', 'champagne', 'ombre',
    })
    _UNNATURAL_HAIR_COLORS = frozenset({
        'blue', 'green', 'purple', 'violet', 'pink', 'teal', 'turquoise',
        'lavender', 'magenta', 'cyan', 'coral', 'orange', 'yellow',
        'silver', 'gold', 'golden',
    })

    def _classify(kw):
        """Return 'Colored Hair', 'Multicolored Hair', or None."""
        k = kw.lower().strip()
        if not k.endswith('hair'):
            return None
        prefix = re.sub(r'\s*hair\s*$', '', k).strip()
        if not prefix:
            return None
        tokens = re.split(r'\s+and\s+|\s*-\s*|\s+', prefix)
        unnatural = [t for t in tokens if t in _UNNATURAL_HAIR_COLORS]
        if not unnatural:
            return None
        all_colors = [t for t in tokens if t in _HAIR_COLOR_WORDS]
        if len(all_colors) >= 2 or ' and ' in prefix:
            return 'Multicolored Hair'
        return 'Colored Hair'

    # Fetch all unmatched keywords in one query
    with conn.cursor() as cur:
        cur.execute(
            "SELECT image_id, tagger_run_id, keyword FROM image_keywords_unmatched"
        )
        rows = cur.fetchall()

    # Classify each row
    to_promote = [
        (image_id, run_id, kw, _classify(kw))
        for image_id, run_id, kw in rows
        if _classify(kw) is not None
    ]

    if not to_promote:
        return 0, 0

    colored_count = 0
    multicolored_count = 0

    with conn.cursor() as cur:
        colored_tag_id     = _upsert_tag(cur, 'Colored Hair')
        multicolored_tag_id = _upsert_tag(cur, 'Multicolored Hair')

        for image_id, run_id, kw, tag_name in to_promote:
            tag_id = colored_tag_id if tag_name == 'Colored Hair' else multicolored_tag_id

            cur.execute(
                """
                INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (image_id, tag_id, run_id),
            )
            cur.execute(
                """
                DELETE FROM image_keywords_unmatched
                WHERE image_id = %s AND tagger_run_id = %s AND keyword = %s
                """,
                (image_id, run_id, kw),
            )

            if tag_name == 'Colored Hair':
                colored_count += 1
            else:
                multicolored_count += 1

    conn.commit()
    return colored_count, multicolored_count


def backfill_normalizers(conn):
    """Apply the nudity-level and pubic-hair normalizers to every row in
    image_keywords_unmatched and promote matching rows to image_keywords.

    Mirrors the logic in llmii.process_keywords._normalize_nudity() and
    _normalize_pubic_hair()/_normalize_labia() so that images processed before
    those normalizers existed get their keywords classified without reprocessing.

    Returns a dict mapping canonical_tag_name → count_of_rows_promoted.
    """
    _NUDITY_RULES = [
        (re.compile(
            r'\b(fully\s+nude|completely\s+nude|fully\s+naked|completely\s+naked|'
            r'total\s+nudity|full\s+nudity|entirely\s+naked|entirely\s+nude)\b', re.I),
         'Nude'),
        (re.compile(
            r'\b(nude|naked|undressed|unclothed|bare\s+body|full\s+frontal)\b', re.I),
         'Nude'),
        (re.compile(
            r'\b(topless|bare\s+chested|bare\s+breasted|shirtless)\b', re.I),
         'Topless'),
        (re.compile(
            r'\b(bottomless|bare\s+below\s+the\s+waist)\b', re.I),
         'Bottomless'),
    ]

    _PUBIC_HAIR_RULES = [
        (re.compile(
            r'\b(shaved?\s+(pub(ic|es|is)|pussy)|bare\s+(pub(ic|es|is)|pussy)|'
            r'clean[\s-]shaved?\s+(pub|gen|pussy)|hairless\s+pub)', re.I),
         'Shaved Pussy'),
        (re.compile(r'\b(landing\s+strip|racing\s+stripe?)\b', re.I),
         'Landing Strip'),
        (re.compile(
            r'\b(full\s+bush|full\s+pub|unshaved?\s+(pub|pussy)|'
            r'unshaved?\s+gen|hairy\s+(pub|pussy))\b', re.I),
         'Full Bush'),
        (re.compile(r'\b(natural\s+(pub|pussy))\b', re.I),
         'Natural Pubic Hair'),
        (re.compile(
            r'\b(trimmed?\s+(pub|pussy|gen)|neat(ly)?\s+trim|'
            r'trimmed?\s+hair.{0,15}pub|cropped?\s+(pub|pussy))\b', re.I),
         'Trimmed Pussy'),
    ]

    _LABIA_RULES = [
        (re.compile(r'\b(gaping|wide[\s-]+open)\s+(pussy|vagina|labia)\b', re.I),
         'Gaping Pussy'),
        (re.compile(r'\b(spread|open|parted|exposed|apart)\s+labia\b', re.I),
         'Spread Labia'),
        (re.compile(r'\blabia\s+(spread|open|parted|exposed|apart)\b', re.I),
         'Spread Labia'),
        (re.compile(r'\b(spread|open|parted|exposed)\s+(pussy|vagina)\b', re.I),
         'Spread Pussy'),
    ]

    def _classify(kw):
        k = kw.lower().strip()
        # Nudity level
        for pattern, canonical in _NUDITY_RULES:
            if pattern.search(k):
                return canonical
        # Pubic hair (guard: must mention pubic area / pussy)
        if re.search(r'\b(pub(ic|es|is)|genital|vulva|vagina|labia|crotch|pussy)\b', k):
            for pattern, canonical in _PUBIC_HAIR_RULES:
                if pattern.search(k):
                    return canonical
        # Labia appearance (guard: must mention labia/pussy/vagina/vulva)
        if re.search(r'\b(labia|pussy|vagina|vulva)\b', k):
            for pattern, canonical in _LABIA_RULES:
                if pattern.search(k):
                    return canonical
        return None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT image_id, tagger_run_id, keyword FROM image_keywords_unmatched"
        )
        rows = cur.fetchall()

    to_promote = [
        (image_id, run_id, kw, _classify(kw))
        for image_id, run_id, kw in rows
        if _classify(kw) is not None
    ]

    if not to_promote:
        return {}

    counts = {}
    with conn.cursor() as cur:
        # Pre-upsert all unique canonical tags we'll need
        tag_ids = {}
        for _, _, _, tag_name in to_promote:
            if tag_name not in tag_ids:
                tag_ids[tag_name] = _upsert_tag(cur, tag_name)

        for image_id, run_id, kw, tag_name in to_promote:
            tag_id = tag_ids[tag_name]
            cur.execute(
                """
                INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (image_id, tag_id, run_id),
            )
            cur.execute(
                """
                DELETE FROM image_keywords_unmatched
                WHERE image_id = %s AND tagger_run_id = %s AND keyword = %s
                """,
                (image_id, run_id, kw),
            )
            counts[tag_name] = counts.get(tag_name, 0) + 1

    conn.commit()
    return counts


def export_tags(conn):
    """Return all tag/alias pairs as a list of dicts for JSON export.

    Each row in the result represents one alias row joined to its canonical tag:
        [{"Tag": "Full Tag Name", "Alias": "alias text"}, ...]

    Sorted by tag name then alias so the export is deterministic.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.tag, a.alias
            FROM   tag_aliases a
            JOIN   tags        t ON t.id = a.tag_id
            ORDER  BY t.tag, a.alias
            """
        )
        return [{"Tag": row[0], "Alias": row[1]} for row in cur.fetchall()]


def backfill_from_raw(conn):
    """Apply all normalizers to raw keywords in image_keywords_raw.

    Unlike backfill_normalizers (which only scans image_keywords_unmatched),
    this function scans every raw keyword ever emitted by the LLM and promotes
    those that match a normalizer directly to image_keywords — even if they
    were never written to image_keywords_unmatched (e.g. because they were
    previously discarded as unmatched).

    This is useful after adding new normalizer rules: it recovers tags from
    all historical raw output without requiring a full reprocess.

    Returns a dict mapping canonical_tag_name → count_of_rows_inserted.
    """
    # Mirror the normalizer rule sets from process_keywords / backfill_normalizers
    _NUDITY_RULES = [
        (re.compile(
            r'\b(fully\s+nude|completely\s+nude|fully\s+naked|completely\s+naked|'
            r'total\s+nudity|full\s+nudity|entirely\s+naked|entirely\s+nude)\b', re.I),
         'Nude'),
        (re.compile(
            r'\b(nude|naked|undressed|unclothed|bare\s+body|full\s+frontal)\b', re.I),
         'Nude'),
        (re.compile(
            r'\b(topless|bare\s+chested|bare\s+breasted|shirtless)\b', re.I),
         'Topless'),
        (re.compile(
            r'\b(bottomless|bare\s+below\s+the\s+waist)\b', re.I),
         'Bottomless'),
    ]
    _PUBIC_HAIR_RULES = [
        (re.compile(
            r'\b(shaved?\s+(pub(ic|es|is)|pussy)|bare\s+(pub(ic|es|is)|pussy)|'
            r'clean[\s-]shaved?\s+(pub|gen|pussy)|hairless\s+pub)', re.I),
         'Shaved Pussy'),
        (re.compile(r'\b(landing\s+strip|racing\s+stripe?)\b', re.I),
         'Landing Strip'),
        (re.compile(
            r'\b(full\s+bush|full\s+pub|unshaved?\s+(pub|pussy)|'
            r'unshaved?\s+gen|hairy\s+(pub|pussy))\b', re.I),
         'Full Bush'),
        (re.compile(r'\b(natural\s+(pub|pussy))\b', re.I),
         'Natural Pubic Hair'),
        (re.compile(
            r'\b(trimmed?\s+(pub|pussy|gen)|neat(ly)?\s+trim|'
            r'trimmed?\s+hair.{0,15}pub|cropped?\s+(pub|pussy))\b', re.I),
         'Trimmed Pussy'),
    ]
    _LABIA_RULES = [
        (re.compile(r'\b(gaping|wide[\s-]+open)\s+(pussy|vagina|labia)\b', re.I),
         'Gaping Pussy'),
        (re.compile(r'\b(spread|open|parted|exposed|apart)\s+labia\b', re.I),
         'Spread Labia'),
        (re.compile(r'\blabia\s+(spread|open|parted|exposed|apart)\b', re.I),
         'Spread Labia'),
        (re.compile(r'\b(spread|open|parted|exposed)\s+(pussy|vagina)\b', re.I),
         'Spread Pussy'),
    ]

    def _classify(kw):
        k = kw.lower().strip()
        for pattern, canonical in _NUDITY_RULES:
            if pattern.search(k):
                return canonical
        if re.search(r'\b(pub(ic|es|is)|genital|vulva|vagina|labia|crotch|pussy)\b', k):
            for pattern, canonical in _PUBIC_HAIR_RULES:
                if pattern.search(k):
                    return canonical
        if re.search(r'\b(labia|pussy|vagina|vulva)\b', k):
            for pattern, canonical in _LABIA_RULES:
                if pattern.search(k):
                    return canonical
        return None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT image_id, tagger_run_id, keyword FROM image_keywords_raw"
        )
        rows = cur.fetchall()

    to_promote = [
        (image_id, run_id, kw, _classify(kw))
        for image_id, run_id, kw in rows
        if _classify(kw) is not None
    ]

    if not to_promote:
        return {}

    counts = {}
    with conn.cursor() as cur:
        tag_ids = {}
        for _, _, _, tag_name in to_promote:
            if tag_name not in tag_ids:
                tag_ids[tag_name] = _upsert_tag(cur, tag_name)

        for image_id, run_id, kw, tag_name in to_promote:
            tag_id = tag_ids[tag_name]
            cur.execute(
                """
                INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (image_id, tag_id, run_id),
            )
            if cur.rowcount:
                counts[tag_name] = counts.get(tag_name, 0) + 1

    conn.commit()
    return counts


def get_stats(conn):
    """Return a dict of database statistics.

    Keys
    ----
    total_images        — total rows in images table
    processed_images    — images with at least one run_status row
    avg_keywords        — average matched keyword count per image (all images)
    zero_keyword_images — images with 0 matched keywords
    sparse_images       — images with 1–4 matched keywords
    total_unmatched     — total rows in image_keywords_unmatched
    unique_unmatched    — distinct keyword strings in image_keywords_unmatched
    top_tags            — list of (tag, image_count) for the 15 most-used tags
    total_runs          — total rows in tagger_runs
    stuck_runs          — tagger_runs with status='running' started > 1 hour ago
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM images")
        total_images = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT image_id) FROM image_run_status
        """)
        processed_images = cur.fetchone()[0]

        cur.execute("""
            SELECT COALESCE(AVG(kc), 0)
            FROM (
                SELECT COUNT(*) AS kc
                FROM images i
                LEFT JOIN image_keywords ik ON ik.image_id = i.id
                GROUP BY i.id
            ) sub
        """)
        avg_keywords = float(cur.fetchone()[0])

        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT i.id
                FROM images i
                LEFT JOIN image_keywords ik ON ik.image_id = i.id
                GROUP BY i.id
                HAVING COUNT(ik.tag_id) = 0
            ) sub
        """)
        zero_keyword_images = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT i.id
                FROM images i
                JOIN image_keywords ik ON ik.image_id = i.id
                GROUP BY i.id
                HAVING COUNT(ik.tag_id) BETWEEN 1 AND 4
            ) sub
        """)
        sparse_images = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM image_keywords_unmatched")
        total_unmatched = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT keyword) FROM image_keywords_unmatched")
        unique_unmatched = cur.fetchone()[0]

        cur.execute("""
            SELECT t.tag, COUNT(DISTINCT ik.image_id) AS img_count
            FROM image_keywords ik
            JOIN tags t ON t.id = ik.tag_id
            GROUP BY t.tag
            ORDER BY img_count DESC
            LIMIT 15
        """)
        top_tags = [(row[0], row[1]) for row in cur.fetchall()]

        cur.execute("SELECT COUNT(*) FROM tagger_runs")
        total_runs = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM tagger_runs
            WHERE status = 'running'
              AND started_at < now() - interval '1 hour'
        """)
        stuck_runs = cur.fetchone()[0]

    return {
        'total_images':        total_images,
        'processed_images':    processed_images,
        'avg_keywords':        round(avg_keywords, 1),
        'zero_keyword_images': zero_keyword_images,
        'sparse_images':       sparse_images,
        'total_unmatched':     total_unmatched,
        'unique_unmatched':    unique_unmatched,
        'top_tags':            top_tags,
        'total_runs':          total_runs,
        'stuck_runs':          stuck_runs,
    }


def health_check(conn):
    """Return a dict describing the health of the database.

    Keys
    ----
    stuck_runs           — list of (id, tagger_name, started_at) for runs stuck
                           in 'running' state for > 1 hour
    promotable_unmatched — count of unmatched keywords that already have an alias
                           (i.e. the alias was added AFTER the image was processed)
    orphaned_keywords    — image_keywords rows whose tag_id has no matching tags row
                           (should always be 0; non-zero indicates referential integrity issue)
    total_unmatched      — total rows currently in image_keywords_unmatched
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, tagger_name, started_at
            FROM tagger_runs
            WHERE status = 'running'
              AND started_at < now() - interval '1 hour'
            ORDER BY started_at
        """)
        stuck_runs = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*)
            FROM image_keywords_unmatched iku
            WHERE EXISTS (
                SELECT 1 FROM tag_aliases ta WHERE ta.alias = iku.keyword
            )
        """)
        promotable_unmatched = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM image_keywords ik
            WHERE NOT EXISTS (
                SELECT 1 FROM tags t WHERE t.id = ik.tag_id
            )
        """)
        orphaned_keywords = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM image_keywords_unmatched")
        total_unmatched = cur.fetchone()[0]

    return {
        'stuck_runs':           stuck_runs,
        'promotable_unmatched': promotable_unmatched,
        'orphaned_keywords':    orphaned_keywords,
        'total_unmatched':      total_unmatched,
    }


def rename_tag(conn, old_name, new_name):
    """Rename a canonical tag in-place.

    The tag's id (and all foreign-key references) are unchanged; only the
    tags.tag column value is updated.

    Raises ValueError if old_name is not found or new_name already exists.
    Returns True on success.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM tags WHERE tag = %s", (old_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Tag '{old_name}' not found.")

        cur.execute("SELECT id FROM tags WHERE tag = %s", (new_name,))
        if cur.fetchone():
            raise ValueError(f"A tag named '{new_name}' already exists.")

        cur.execute("UPDATE tags SET tag = %s WHERE tag = %s", (new_name, old_name))

    conn.commit()
    return True


def merge_tag(conn, source_name, target_name):
    """Merge source_name into target_name.

    Operations (all in one transaction):
      1. Reassign image_keywords rows: source_id → target_id
         (rows that would duplicate an existing target row are deleted first)
      2. Reassign source's tag_aliases to target
         (aliases that would conflict with existing target aliases are dropped)
      3. Add source tag name as an alias for target (ON CONFLICT DO NOTHING)
      4. Delete source from tags

    Returns the number of image_keywords rows reassigned (not deleted) to target.
    Raises ValueError if either tag is not found, or if source == target.
    """
    if source_name.lower() == target_name.lower():
        raise ValueError("Source and target tags are the same.")

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM tags WHERE tag = %s", (source_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Source tag '{source_name}' not found.")
        source_id = row[0]

        cur.execute("SELECT id FROM tags WHERE tag = %s", (target_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Target tag '{target_name}' not found.")
        target_id = row[0]

        # 1a. Delete source rows that would create duplicates after reassignment
        cur.execute(
            """
            DELETE FROM image_keywords
            WHERE tag_id = %s
              AND (image_id, tagger_run_id) IN (
                  SELECT image_id, tagger_run_id
                  FROM image_keywords
                  WHERE tag_id = %s
              )
            """,
            (source_id, target_id),
        )

        # 1b. Reassign remaining source rows to target
        cur.execute(
            "UPDATE image_keywords SET tag_id = %s WHERE tag_id = %s",
            (target_id, source_id),
        )
        reassigned = cur.rowcount

        # 2a. Drop source aliases that conflict with existing target aliases
        cur.execute(
            """
            DELETE FROM tag_aliases
            WHERE tag_id = %s
              AND alias IN (SELECT alias FROM tag_aliases WHERE tag_id = %s)
            """,
            (source_id, target_id),
        )

        # 2b. Reassign remaining source aliases to target
        cur.execute(
            "UPDATE tag_aliases SET tag_id = %s WHERE tag_id = %s",
            (target_id, source_id),
        )

        # 3. Add source tag name as alias for target (future lookups resolve correctly)
        cur.execute(
            """
            INSERT INTO tag_aliases (tag_id, alias)
            VALUES (%s, %s)
            ON CONFLICT (alias) DO NOTHING
            """,
            (target_id, source_name),
        )

        # 4. Delete source tag (all FK references already removed above)
        cur.execute("DELETE FROM tags WHERE id = %s", (source_id,))

    conn.commit()
    return reassigned


def promote_aliased_unmatched(conn):
    """Promote unmatched keywords that now have aliases to image_keywords.

    Handles the case where:
      1. Image was processed → keyword landed in image_keywords_unmatched
      2. Later: an alias was added in tag_review (or via backfill)
      3. This function finds and promotes those rows automatically.

    Returns count of rows promoted.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
            SELECT iku.image_id, ta.tag_id, iku.tagger_run_id
            FROM image_keywords_unmatched iku
            JOIN tag_aliases ta ON ta.alias = iku.keyword
            ON CONFLICT DO NOTHING
            """
        )
        promoted = cur.rowcount

        # Remove the now-matched rows from unmatched
        cur.execute(
            """
            DELETE FROM image_keywords_unmatched iku
            WHERE EXISTS (
                SELECT 1 FROM tag_aliases ta WHERE ta.alias = iku.keyword
            )
            """
        )

    conn.commit()
    return promoted


def get_run_history(conn):
    """Return all tagger runs ordered by start time descending.

    Each entry is a dict with:
      id, tagger_name, status, started_at, finished_at,
      duration_seconds (int), params_json (str or None)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                tagger_name,
                status,
                started_at,
                finished_at,
                EXTRACT(EPOCH FROM
                    (COALESCE(finished_at, NOW()) - started_at)
                )::int AS duration_s,
                params_json
            FROM tagger_runs
            ORDER BY started_at DESC
            """
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def find_orphaned_paths(conn):
    """Return list of (image_id, path) for DB images whose files no longer exist on disk.

    Skips zip-composite paths (those containing '::') since those files are
    expected to be absent most of the time (they live inside archives).
    """
    import os as _os
    with conn.cursor() as cur:
        cur.execute("SELECT id, path FROM images WHERE path NOT LIKE '%::%'")
        rows = cur.fetchall()
    return [(row[0], row[1]) for row in rows if not _os.path.exists(row[1])]


def remove_orphaned_images(conn, image_ids):
    """Delete image records (and all cascaded child rows) for the given image IDs.

    Returns the count of image rows deleted.
    """
    if not image_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM images WHERE id = ANY(%s)", (list(image_ids),))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def export_keywords_csv(conn, output_path):
    """Export all images with their canonical keywords to a CSV file.

    Output columns: path, gallery, keywords
    The 'keywords' column is a semicolon-separated list of canonical tag names.

    Returns the number of image rows written.
    """
    import csv as _csv
    from collections import defaultdict as _defaultdict

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.path,
                   COALESCE(g.name, '') AS gallery,
                   t.tag
            FROM   images i
            LEFT   JOIN galleries g       ON g.id = i.gallery_id
            LEFT   JOIN image_keywords ik ON ik.image_id = i.id
            LEFT   JOIN tags t            ON t.id = ik.tag_id
            ORDER  BY i.path, t.tag
            """
        )
        rows = cur.fetchall()

    image_data = _defaultdict(lambda: {'gallery': '', 'keywords': []})
    for path, gallery, tag in rows:
        image_data[path]['gallery'] = gallery
        if tag:
            image_data[path]['keywords'].append(tag)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = _csv.writer(f)
        writer.writerow(['path', 'gallery', 'keywords'])
        for path in sorted(image_data):
            d = image_data[path]
            writer.writerow([path, d['gallery'], ';'.join(d['keywords'])])

    return len(image_data)


def find_duplicate_images(conn):
    """Return a list of duplicate groups: [(sha256, [path, ...]), ...]
    sorted by group size descending.  Only groups with 2+ images are returned.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT sha256, array_agg(path ORDER BY path) AS paths
            FROM   {_SCHEMA}.images
            WHERE  sha256 IS NOT NULL
            GROUP  BY sha256
            HAVING COUNT(*) > 1
            ORDER  BY COUNT(*) DESC, sha256
            """
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def get_processed_paths(conn, directory_prefix=None):
    """Return a set of normalised file paths that have been successfully processed.

    Used by the resume-session feature to pre-populate the BackgroundIndexer
    skip-set so that already-done files are never sent to ExifTool.

    Args:
        conn: active psycopg2 connection
        directory_prefix: if given, only return paths under this directory
    """
    with conn.cursor() as cur:
        if directory_prefix:
            prefix = os.path.normpath(directory_prefix)
            # Match both forward and backslash variants
            cur.execute(
                f"""
                SELECT i.path
                FROM   {_SCHEMA}.images i
                JOIN   {_SCHEMA}.image_run_status irs ON irs.image_id = i.id
                WHERE  irs.status = 'success'
                  AND  (i.path LIKE %s OR i.path LIKE %s)
                """,
                (
                    prefix.replace('\\', '/') + '%',
                    prefix.replace('/', '\\') + '%',
                ),
            )
        else:
            cur.execute(
                f"""
                SELECT i.path
                FROM   {_SCHEMA}.images i
                JOIN   {_SCHEMA}.image_run_status irs ON irs.image_id = i.id
                WHERE  irs.status = 'success'
                """
            )
        return {os.path.normpath(row[0]) for row in cur.fetchall()}
