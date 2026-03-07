# ImageIndexer — Developer Guide

> **Purpose of this document**: Bring a developer (human or AI) up to full speed
> on the current architecture, data flow, schema, and design decisions so they can
> continue development without needing to rediscover anything from scratch.
>
> **State captured**: March 2026 (updated with performer_tags table, explore_performers.py tool, tag_review --performer filter, and performer tag management functions).

---

## 1. What the Project Does

ImageIndexer is a desktop application that processes image files using a **local LLM**
(via KoboldCPP) to generate captions and keyword tags, then writes the results to:

- **JSON sidecar files** — one `.json` file per image, stored alongside the image
- **PostgreSQL database** — a relational schema (`ai_captioning`) storing images,
  galleries, studios, performers, tags, and run history
- **Both** — written simultaneously

The user selects a source directory; the app crawls it, reads each image, sends it
to the LLM, normalises the returned keywords against a master vocabulary, then writes
results. Zip archives are supported: each zip is extracted to a temp folder,
processed image-by-image, then the temp folder is deleted before moving to the next zip.
Files within every directory are processed in **alphabetical order**.

---

## 2. Repository Layout

```
ImageIndexer-main/
├── launcher.py              # Entry point — starts the PyQt6 GUI
├── tag_review.py            # Standalone tool: review/assign unmatched keywords; supports --performer filter
├── explore_performers.py    # Standalone tool: browse performers, view/manage performer tags
├── debug_json.py            # Debug utility for inspecting sidecar JSON files
├── patch_settings.py        # One-off settings migration helper
├── pyproject.toml           # Dependencies (PyQt6, psycopg2 optional, pillow, etc.)
├── settings.json            # Runtime config (auto-created by GUI, git-ignored)
├── DEVELOPER_GUIDE.md       # This document
│
├── src/
│   ├── llmii.py             # Core engine: Config, BackgroundIndexer, FileProcessor, LLMProcessor
│   ├── llmii_gui.py         # PyQt6 GUI: SettingsDialog, IndexerThread, main window
│   ├── llmii_db.py          # PostgreSQL integration: schema, upserts, write functions
│   ├── llmii_utils.py       # Keyword normalisation, de-pluralisation, JSON repair
│   ├── llmii_setup.py       # GPU detection, KoboldCPP launch helper
│   ├── image_processor.py   # Image loading, resizing, base64 encoding
│   ├── help_text.py         # HTML help text displayed in the GUI
│   └── config.py            # PROJECT_ROOT / RESOURCES_DIR path constants
│
└── sql/
    ├── add_studios.sql      # Historical migration (now embedded in apply_migrations)
    └── add_zip_source.sql   # Historical migration (now embedded in apply_migrations)
```

> **Note**: The `.sql` files are kept for reference only. All migrations now run
> automatically via `llmii_db.apply_migrations()` on every DB connection.

---

## 3. PostgreSQL Schema (`ai_captioning`)

The database must have the `ai_captioning` schema.  The base schema is created
externally (not by this app); `apply_migrations()` adds extensions to it safely.

### Core tables (base schema — created externally)

```sql
-- citext extension required for case-insensitive tag matching
CREATE EXTENSION IF NOT EXISTS citext;

tags (
    id         BIGSERIAL PRIMARY KEY,
    tag        CITEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
)

tag_aliases (
    id         BIGSERIAL PRIMARY KEY,
    tag_id     BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    alias      CITEXT NOT NULL,
    UNIQUE (alias)
)

galleries (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
)

performers (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
)

images (
    id          BIGSERIAL PRIMARY KEY,
    identifier  UUID NOT NULL,           -- XMP:Identifier assigned by the tagger
    filename    TEXT NOT NULL,
    path        TEXT UNIQUE NOT NULL,    -- absolute path OR composite zip key
    gallery_id  BIGINT REFERENCES galleries(id),
    zip_source  TEXT,                    -- base filename of source zip, or NULL
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
)

tagger_runs (
    id           BIGSERIAL PRIMARY KEY,
    tagger_name  TEXT NOT NULL DEFAULT 'ImageIndexer',
    params_json  JSONB,
    status       TEXT NOT NULL DEFAULT 'running',  -- running|success|failed
    started_at   TIMESTAMPTZ DEFAULT now(),
    finished_at  TIMESTAMPTZ
)

image_run_status (
    image_id       BIGINT NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    tagger_run_id  BIGINT NOT NULL REFERENCES tagger_runs(id) ON DELETE CASCADE,
    status         TEXT NOT NULL,   -- success|failed|retry
    processed_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (image_id, tagger_run_id)
)

image_descriptions (
    image_id       BIGINT PRIMARY KEY REFERENCES images(id) ON DELETE CASCADE,
    description    TEXT NOT NULL,
    tagger_run_id  BIGINT REFERENCES tagger_runs(id),
    updated_at     TIMESTAMPTZ DEFAULT now()
)

image_keywords (
    image_id       BIGINT NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    tag_id         BIGINT NOT NULL REFERENCES tags(id)   ON DELETE CASCADE,
    tagger_run_id  BIGINT REFERENCES tagger_runs(id) ON DELETE CASCADE,
    UNIQUE (image_id, tag_id, tagger_run_id)
)

image_keywords_raw (
    id             BIGSERIAL PRIMARY KEY,
    image_id       BIGINT NOT NULL REFERENCES images(id)        ON DELETE CASCADE,
    tagger_run_id  BIGINT NOT NULL REFERENCES tagger_runs(id)   ON DELETE CASCADE,
    keyword        TEXT NOT NULL,
    UNIQUE (image_id, tagger_run_id, keyword)
)

image_keywords_unmatched (
    id             BIGSERIAL PRIMARY KEY,
    image_id       BIGINT NOT NULL REFERENCES images(id)        ON DELETE CASCADE,
    tagger_run_id  BIGINT NOT NULL REFERENCES tagger_runs(id)   ON DELETE CASCADE,
    keyword        TEXT NOT NULL,
    UNIQUE (image_id, tagger_run_id, keyword)
)

image_performers (
    image_id      BIGINT NOT NULL REFERENCES images(id)      ON DELETE CASCADE,
    performer_id  BIGINT NOT NULL REFERENCES performers(id)  ON DELETE CASCADE,
    PRIMARY KEY (image_id, performer_id)
)
```

### Tables added by `apply_migrations()` (auto-applied on connect)

```sql
studios (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
)

studio_galleries (
    studio_id  BIGINT NOT NULL REFERENCES studios(id)   ON DELETE CASCADE,
    gallery_id BIGINT NOT NULL REFERENCES galleries(id) ON DELETE CASCADE,
    PRIMARY KEY (studio_id, gallery_id)
)

studio_images (
    studio_id  BIGINT NOT NULL REFERENCES studios(id) ON DELETE CASCADE,
    image_id   BIGINT NOT NULL REFERENCES images(id)  ON DELETE CASCADE,
    PRIMARY KEY (studio_id, image_id)
)

-- Column added to images:
images.zip_source  TEXT   -- base filename of source .zip, NULL for non-zip images

-- Index for zip provenance queries:
CREATE INDEX images_zip_source_idx ON images (zip_source) WHERE zip_source IS NOT NULL;

performer_tags (
    performer_id  INTEGER NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    tag_id        INTEGER NOT NULL REFERENCES tags(id)       ON DELETE CASCADE,
    image_count   INTEGER NOT NULL DEFAULT 0,   -- images with this tag (from last assign run)
    total_images  INTEGER NOT NULL DEFAULT 0,   -- total images for this performer
    assigned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    pinned        BOOLEAN NOT NULL DEFAULT FALSE,        -- keep even if below threshold
    excluded      BOOLEAN NOT NULL DEFAULT FALSE,        -- tombstone: never auto-assign
    manually_added BOOLEAN NOT NULL DEFAULT FALSE,       -- user-assigned; always kept
    PRIMARY KEY (performer_id, tag_id)
)

-- Column added to tags:
tags.exclude_from_performers  BOOLEAN NOT NULL DEFAULT FALSE
    -- global blacklist: tag is never auto-assigned to any performer
```

**`performer_tags` row semantics:**

| State | `pinned` | `excluded` | `manually_added` | Meaning |
|---|---|---|---|---|
| Threshold-assigned | F | F | F | Normal auto-assigned row |
| Pinned | T | F | F | Auto-assigned + kept below threshold |
| Manually added | T | F | T | User-assigned; always kept |
| Excluded (tombstone) | F | T | F | Blocks auto-reassignment for this performer |

Excluded rows are never shown in the UI — they exist solely to prevent `assign_performer_tags` from re-inserting the tag. To un-exclude, delete the row or call `include_performer_tag()`.

### Tag vocabulary

Tags are loaded from a **master JSON file** (`mastertags.json` by default) via
"Load Tag File into DB..." in the settings dialog.  The file format is:

```json
[
  {"Tag": "Full Canonical Tag Name", "Alias": "alias text"},
  {"Tag": "Full Canonical Tag Name", "Alias": "another alias"}
]
```

Each alias row maps one lookup string to one canonical tag.  At keyword-matching
time the LLM's raw output is fuzzy-matched against aliases using RapidFuzz.
The matched canonical tag is what gets stored in `image_keywords`.

Tags and aliases are **never cleared** by "Clear Database" — only image/run data
is removed.

---

## 4. `BackgroundIndexer` — Directory & File Discovery

`BackgroundIndexer` runs on a background thread.  It crawls the source directory,
filters files by extension, and puts `(directory, file_list)` batches into
`metadata_queue` for `FileProcessor` to consume.

### Key attributes

| Attribute | Type | Description |
|---|---|---|
| `total_files_found` | int | Running count of files put into the queue |
| `total_directories` | int | Total non-skipped directories discovered; set **before** indexing starts so the GUI can display an accurate denominator for the directory progress bar |
| `indexing_complete` | bool | Set to `True` when the walk finishes |

### Ordering

Directories are sorted alphabetically before indexing begins (`directories.sort()`).
Within each directory, files are enumerated with `sorted(os.listdir(directory))` so
the queue delivers them in case-sensitive alphabetical order.

---

## 5. Core Data Flow

```
User selects directory
        │
        ▼
BackgroundIndexer (background thread)
        ├─ Walks directory tree, sorts directories alphabetically
        ├─ Sets total_directories before first _index_directory() call
        └─ Puts (directory, sorted_file_list) into metadata_queue
                │
                ▼
FileProcessor.process_directory()
        │
        ├─ Clears temp directory
        ├─ Reloads tag vocabulary from DB (if DB mode)
        │
        └─ Loop over metadata_queue batches:
                │
                ├─ Separate: image_files (sorted) vs zip_files (sorted)
                │
                ├─ Update _progress: dirs_total, zips_total, images_total → _emit_progress()
                │
                ├─ _process_file_list(image_files, on_file_done=…)
                │       │
                │       └─ For each batch of 50:
                │               ExifTool batch read
                │               → process_file(metadata) × N
                │               → on_file_done() after each  ← increments images_done,
                │                                               emits progress to GUI
                │
                └─ For each zip_path in zip_files (sorted):
                        │
                        ├─ Update _progress: mode='zip', images_total=len(extracted)
                        │
                        ├─ _extract_single_zip(zip_path)
                        │       ├─ Parse studio/performers from zip filename
                        │       ├─ List internal images matching ext filter
                        │       ├─ DB batch-check already-processed composites (skip them)
                        │       ├─ Extract remaining to temp/<zip_stem>/
                        │       ├─ Register in _zip_file_map: temp_path →
                        │       │     (composite_key, zip_source, studio, performers)
                        │       └─ Return list of extracted temp paths
                        │
                        ├─ _process_file_list(extracted, on_file_done=…)
                        │       └─ [same batch loop; on_file_done updates zip image progress]
                        │
                        └─ _cleanup_zip_temp(zip_path)    ← finally block; always runs
                               + zips_done += 1, emit progress
                │
                └─ dirs_done += 1, emit progress
```

### Inside `process_file(metadata)`

```
check_uuid()  →  skip if already success (unless reprocess flags set)
                 assign new UUID if new file
        │
image_processor.process_image()  →  base64-encoded JPEG
        │
generate_metadata()  →  LLM caption + keyword generation
        │
[optional retry if status == 'retry']
        │
Copy zip routing fields from original metadata into updated_metadata:
    _zip_db_key, _zip_source, _zip_studio, _zip_performers
    (generate_metadata() returns a fresh dict; these fields must be copied manually)
        │
Emit image_data callback to GUI:
    studio, performers, caption, keywords shown live in metadata panel
        │
write_metadata(file_path, updated_metadata)
    ├─ JSON sidecar  (if output_mode in ['json', 'both'])
    └─ write_image_to_db()  (if output_mode in ['db', 'both'])
```

### Inside `write_image_to_db()`

```
Derive parse_path and detection_path from file_path
    (splits on '::' for zip composite keys)

parse_gallery_and_performers(parse_path)    → gallery_name, performer_names
metadata.get('_zip_performers')             → overrides path-based performers if set

Studio detection (priority order):
    1. 'SuicideGirls' anywhere in path   → 'Suicide Girls'
    2. metadata._zip_studio              → studio from zip filename
    3. 'Galleries' directory in path     → parent dir name

Within a single transaction:
    _upsert_gallery()     → gallery_id
    _upsert_image()       → image_id   (stores composite key, zip_source)
    _upsert_studio()      → studio_id  (if detected)
    studio_galleries INSERT ON CONFLICT DO NOTHING
    studio_images    INSERT ON CONFLICT DO NOTHING
    _upsert_performer() × N  + image_performers INSERT × N
    image_run_status  UPSERT
    image_descriptions UPSERT
    DELETE existing image_keywords / image_keywords_raw / image_keywords_unmatched
        (replace semantics on reprocess)
    _upsert_tag() × N  + image_keywords INSERT × N
    image_keywords_raw INSERT ON CONFLICT DO NOTHING × N
    image_keywords_unmatched INSERT × N  (raw keywords with no alias match)
COMMIT
```

---

## 6. Zip File Handling

### Composite DB key

Images inside zip archives are identified in the database by a **composite key**:

```
/absolute/path/to/archive.zip::internal/path/to/image.jpg
```

This is stored in `images.path`.  The `::` separator is the split point; the
left side is the zip's absolute path, the right side is the path as reported
by `zipfile.ZipFile`.

### Per-zip processing sequence

1. `_extract_single_zip(zip_path)`:
   - Opens the zip, lists images matching the extension filter (from `_accepted_image_exts()`)
   - Builds composite keys for all internal images
   - In DB mode: batch-queries `get_image_status_batch()` to find already-done composites
   - Skips already-`success` images (unless `reprocess_all`) and already-`failed`
     images (unless `reprocess_failed` or `reprocess_all`)
   - Prints `"Extracting N/M image(s) from: archive.zip"` or `"All images already processed"`
   - Extracts only unprocessed images to `temp/<zip_stem>/`
   - Registers in `self._zip_file_map`: `{normpath(temp_file): (composite_key, zip_source_name, zip_studio, [zip_performers])}`
   - Returns `[]` if all images already done (no-op, no disk write)

2. `_process_file_list(extracted_paths, on_file_done=…)` — same batch ExifTool loop
   used for regular images.  `_zip_file_map` lookup injects `_zip_db_key`,
   `_zip_source`, `_zip_studio`, `_zip_performers` into the metadata dict.
   `on_file_done()` is called after every `process_file()` to tick the image
   progress bar.

3. `_cleanup_zip_temp(zip_path)` — called in a `finally` block so it always runs,
   even if the user pauses or stops mid-zip.  After cleanup, `zips_done` is
   incremented and progress is emitted.

### Zip metadata parsing

`parse_zip_metadata(zip_path)` extracts studio and performers from the zip **filename**:

| Filename pattern | Studio | Performers |
|---|---|---|
| `Studio - 2023-01-15 Set (Alice, Bob) [17] [1280x960].zip` | `Studio` | `['Alice', 'Bob']` |
| `Pornstar Platinum 2010-07-14 Title (Charisma Cappelli).zip` | `Pornstar Platinum` | `['Charisma Cappelli']` |
| `Just A Gallery Name.zip` | `None` | `[]` |

Rules:
- Strip `[...]` bracketed suffixes first
- Performers: last `(Name1, Name2)` group, split on commas
- Studio: text before first ` - ` separator, or before first `YYYY-MM-DD` date

---

## 7. Path-Based Gallery / Performer Parsing

`parse_gallery_and_performers(file_path)` is used for **all** images (regular and zip).

- **Gallery** = immediate parent directory of the image file (always)
- **Performers** = only populated when `suicidegirls` (case-insensitive) appears
  in the path, using structure:
  ```
  .../SuicideGirls/[alpha-index/]Performer/YYYY-MM[-DD]/Gallery/image.jpg
  ```
  The performer is the segment immediately **before** the first `YYYY-MM` date folder.
  Multi-performer sets use ` and ` as a separator: `"Alice and Bob"` → `['Alice', 'Bob']`.

For zip images, `parse_gallery_and_performers` runs on the **internal** path (right
side of the `::` composite key).  Zip-detected performers (`_zip_performers`) take
priority over path-detected performers if present.

---

## 8. `Config` Class — All Fields

Defined in `src/llmii.py`.  All fields set in `__init__`, populated from GUI via
`run_indexer()` and from CLI via `argparse`.

| Field | Type | Default | Description |
|---|---|---|---|
| `directory` | str | None | Source image directory |
| `api_url` | str | None | KoboldCPP API base URL |
| `api_password` | str | None | KoboldCPP API password |
| `no_crawl` | bool | False | Don't recurse subdirectories |
| `no_backup` | bool | False | Skip file backups before writing |
| `text_completion` | bool | False | Use text-completion API instead of chat |
| `dry_run` | bool | False | Process but don't write anything |
| `quick_fail` | bool | False | Fail after first bad LLM response |
| `skip_verify` | bool | False | Skip ExifTool file validation |
| `reprocess_failed` | bool | False | Re-run files with status=failed |
| `reprocess_all` | bool | False | Re-run all files regardless of status |
| `reprocess_orphans` | bool | True | Fix files with UUID but no status |
| `detailed_caption` | bool | False | Separate caption + keyword queries |
| `short_caption` | bool | True | Single combined caption+keyword query |
| `no_caption` | bool | False | Keywords only, no caption |
| `update_caption` | bool | False | Append to existing caption |
| `update_keywords` | bool | False | Don't clear existing keywords |
| `normalize_keywords` | bool | True | Always True — normalise keyword case/spacing |
| `depluralize_keywords` | bool | False | Convert plural keywords to singular |
| `limit_word_count` | bool | True | Reject keywords over N words |
| `max_words_per_keyword` | int | 2 | Word limit per keyword |
| `split_and_entries` | bool | True | Split "X and Y" into two keywords |
| `ban_prompt_words` | bool | True | Reject keywords that repeat instruction words |
| `no_digits_start` | bool | True | Reject keywords starting with 3+ digits |
| `min_word_length` | bool | True | Require words ≥ 2 characters |
| `latin_only` | bool | True | Reject non-Latin characters |
| `tag_blacklist` | list | [] | Substrings; keywords matching any are dropped |
| `tag_fuzzy_threshold` | int | 88 | RapidFuzz match score threshold (0–100) |
| `gen_count` | int | 250 | Max tokens to generate |
| `res_limit` | int | 448 | Max image dimension before resize (pixels) |
| `system_instruction` | str | "You are a helpful assistant." | LLM system prompt |
| `caption_instruction` | str | "Describe the image..." | Caption query text |
| `keyword_instruction` | str | "" | Extra keyword query instructions |
| `tag_instruction` | str | `'Return JSON: {"Keywords": []}'` | Keyword format instruction |
| `instruction` | str | (long template) | Full assembled prompt |
| `temperature` | float | 0.2 | LLM temperature |
| `top_p` | float | 1.0 | LLM top-p |
| `rep_pen` | float | 1.01 | LLM repetition penalty |
| `top_k` | int | 100 | LLM top-k |
| `min_p` | float | 0.05 | LLM min-p |
| `use_default_badwordsids` | bool | False | Use KoboldCPP bad word filtering |
| `use_json_grammar` | bool | False | Force JSON grammar output |
| `rename_invalid` | bool | False | Rename unreadable files to `.invalid` |
| `preserve_date` | bool | False | Keep file modification date after write |
| `fix_extension` | bool | False | Correct mismatched file extensions |
| `image_extensions` | dict | {JPEG:[...], PNG:[...], ... ZIP:['.zip']} | Supported extensions by type |
| `image_extensions_filter` | str | `"jpg,jpeg,webp,zip"` | User-selected extension filter |
| `skip_folders` | list | [] | Directory names to skip entirely |
| `tags_file` | str | `"mastertags.json"` | Master tag vocabulary JSON path |
| `temp_folder` | str | `"temp"` | Zip extraction temp directory |
| `reprocess_sparse` | bool | False | Also reprocess images with fewer than `reprocess_sparse_min` keywords |
| `reprocess_sparse_min` | int | 5 | Keyword-count threshold for sparse reprocess |
| `output_mode` | str | `'json'` | `'json'` \| `'db'` \| `'both'` |
| `use_sidecar` | bool | False | Write to `.xmp` sidecar files |
| `sidecar_dir` | str | `""` | Custom sidecar directory (empty = alongside image) |
| `db_host` | str | `'localhost'` | PostgreSQL host |
| `db_port` | int | 5432 | PostgreSQL port |
| `db_user` | str | `''` | PostgreSQL user |
| `db_password` | str | `''` | PostgreSQL password |
| `db_name` | str | `''` | PostgreSQL database name |

---

## 9. `llmii_db.py` — Public API Reference

### Connection

```python
get_connection(host, port, user, password, dbname, apply_schema_migrations=True)
    → psycopg2 connection
```
Opens a connection with `search_path=ai_captioning,public`.  Automatically runs
`apply_migrations()` unless opted out.  Raises `ImportError` if psycopg2 missing.

```python
apply_migrations(conn)
```
Idempotent — safe to call on every startup.  Adds studios tables, `zip_source` column,
FK type fixes (BIGINT), the `zip_source` partial index, the `performer_tags` table,
and the `pinned` / `excluded` / `manually_added` columns on `performer_tags` plus
`exclude_from_performers` on `tags`.  Also called by `explore_performers.py` on startup
since it connects directly via psycopg2 rather than through `get_connection()`.

### Path parsing

```python
parse_gallery_and_performers(file_path) → (gallery_name: str, performers: list[str])
parse_zip_metadata(zip_path)            → (studio: str|None, performers: list[str])
```

### Run management

```python
create_tagger_run(conn, tagger_name='ImageIndexer', params=None) → run_id: int
finish_tagger_run(conn, run_id, status='success')
```

### Status queries

```python
get_image_status_batch(conn, file_paths: list[str])
    → dict[path, (identifier: str, status: str, keyword_count: int)]
```
Used by `_extract_single_zip` and `_process_file_list` to skip already-processed images.
Works for both real paths and composite zip keys.  The `keyword_count` field is used
for the sparse-reprocess feature (`reprocess_sparse` / `reprocess_sparse_min` in Config).

### Main write function

```python
write_image_to_db(conn, file_path, metadata, run_id, zip_source=None)
```

`metadata` keys consumed:
- `MWG:Description` — caption text
- `MWG:Keywords` — list of resolved canonical tag strings
- `XMP:Identifier` — UUID string
- `XMP:Status` — `success | failed | retry`
- `_raw_keywords` — list of raw LLM keyword strings before matching
- `_debug_map` — `{raw_kw: resolved_tag | None | "__blacklisted__"}`
- `_zip_studio` — studio name from zip filename (optional)
- `_zip_performers` — list of performer names from zip filename (optional)

### Tag vocabulary

```python
load_tags_from_file(conn, json_path, progress_callback=None)
    → {tags_added, aliases_added, tags_skipped, aliases_skipped}

export_tags(conn)
    → list[{"Tag": str, "Alias": str}]   # sorted by tag name, then alias
```

`export_tags()` joins `tag_aliases → tags` and returns every alias row.  Used by
the "Export Tags..." button to produce a JSON backup of the vocabulary.

### Maintenance

```python
clear_database(conn)
```
Truncates all image/run tables.  **Does NOT touch `tags` or `tag_aliases`.**

Tables cleared: `image_keywords_unmatched`, `image_keywords_raw`, `image_keywords`,
`image_descriptions`, `image_run_status`, `image_performers`, `performer_tags`,
`studio_images`, `studio_galleries`, `tagger_runs`, `images`, `galleries`,
`performers`, `studios`.

### Performer tag assignment

```python
assign_performer_tags(conn)
    → {'performers_checked': int, 'tags_assigned': int, 'tags_removed': int}
```
For each performer with at least one image, finds all canonical tags that appear on
more than **40%** of that performer's images (`count * 5 > total * 2`) and upserts
them into `performer_tags`.  Rules:

- Tags with `tags.exclude_from_performers = TRUE` are **skipped globally**.
- Rows with `excluded = TRUE` are tombstones — the upsert's `WHERE NOT performer_tags.excluded`
  clause leaves them untouched, blocking re-insertion.
- Stale deletion preserves rows where `pinned OR manually_added OR excluded`.

```python
pin_performer_tag(conn, performer_id, tag_id, pinned=True)
```
Sets or clears `pinned` on a `performer_tags` row.  No-op if the row is excluded.

```python
exclude_performer_tag(conn, performer_id, tag_id)
```
Creates or updates a tombstone row (`excluded=TRUE, pinned=FALSE, manually_added=FALSE`)
for one performer.  Prevents the tag from being auto-reassigned to this performer.

```python
include_performer_tag(conn, performer_id, tag_id)
```
Deletes an excluded tombstone, allowing the tag to be re-added by a future
`assign_performer_tags` run or manually.

```python
exclude_tag_globally(conn, tag_id)
```
Sets `tags.exclude_from_performers = TRUE` and deletes **all** `performer_tags` rows
for that tag across every performer.

```python
include_tag_globally(conn, tag_id)
```
Clears `tags.exclude_from_performers`.  Does not re-run assignment automatically.

```python
add_performer_tag(conn, performer_id, tag_id)
```
Manually assigns a tag (`manually_added=TRUE, pinned=TRUE, excluded=FALSE`).
If an excluded tombstone exists for the pair, it is overwritten (manual add takes
precedence).  The row survives future `assign_performer_tags` runs.

```python
get_all_tags(conn)
    → [(id: int, tag_name: str), ...]
```
Returns all canonical tags where `exclude_from_performers = FALSE`, sorted
alphabetically.  Used by `explore_performers.py`'s Add Tag dialog.

### Backfill helpers

These functions recover tags from historical data without reprocessing images:

```python
backfill_normalizers(conn)
    → dict[canonical_tag, count]
```
Applies nudity-level, pubic-hair, and labia normalizers to every row in
`image_keywords_unmatched`. Matching rows are promoted to `image_keywords` and
deleted from `image_keywords_unmatched`. Returns a count per promoted tag.

```python
backfill_from_raw(conn)
    → dict[canonical_tag, count]
```
Applies the same normalizers to every row in `image_keywords_raw` (the full raw
LLM output, not just the unmatched subset). Inserts matched rows into
`image_keywords` (`ON CONFLICT DO NOTHING`). Use this after adding new normalizer
rules to recover tags from all historical raw output without reprocessing.

```python
backfill_colored_hair(conn)
    → (colored_count: int, multicolored_count: int)
```
Promotes unnatural-colored hair keywords (e.g. "blue hair") from `image_keywords_unmatched`
to `Colored Hair` or `Multicolored Hair` tags.

```python
promote_aliased_unmatched(conn)
    → promoted: int
```
Finds rows in `image_keywords_unmatched` whose keyword now has an entry in `tag_aliases`
(added after the image was processed) and promotes them to `image_keywords` in bulk.
Returns the count of rows promoted. This is the complement to tag_review.py individual
assignment — it handles mass promotion after aliases have been added.

### Analytics

```python
get_stats(conn)
    → dict
```
Returns a snapshot of database coverage:

| Key | Description |
|---|---|
| `total_images` | Total rows in `images` |
| `processed_images` | Images with ≥1 `image_run_status` row |
| `avg_keywords` | Average matched keyword count per image |
| `zero_keyword_images` | Images with 0 matched keywords |
| `sparse_images` | Images with 1–4 matched keywords |
| `total_unmatched` | Total rows in `image_keywords_unmatched` |
| `unique_unmatched` | Distinct keyword strings in `image_keywords_unmatched` |
| `top_tags` | `[(tag, image_count), …]` for the 15 most-used tags |
| `total_runs` | Total rows in `tagger_runs` |
| `stuck_runs` | Count of runs with `status='running'` started > 1 hour ago |

```python
health_check(conn)
    → dict
```

| Key | Description |
|---|---|
| `stuck_runs` | `[(id, tagger_name, started_at), …]` for stuck runs |
| `promotable_unmatched` | Count of unmatched keywords that now have aliases |
| `orphaned_keywords` | `image_keywords` rows whose `tag_id` has no matching `tags` row (should be 0) |
| `total_unmatched` | Total rows in `image_keywords_unmatched` |

---

## 10. GUI Architecture

### Class hierarchy

```
QMainWindow  (main window defined inline in llmii_gui.py)
    ├── SettingsDialog  (QDialog — all settings live here)
    ├── IndexerThread   (QThread — runs FileProcessor off the UI thread)
    ├── APICheckThread  (QThread — polls KoboldCPP until connected)
    └── PauseHandler    (QObject — receives pause/stop signals from IndexerThread)
```

### Callback message types

`FileProcessor` communicates with the GUI by calling `self.callback(message)`.
`IndexerThread.process_callback()` dispatches on `message['type']`:

| `type` value | Signal emitted | Handler |
|---|---|---|
| `'image_data'` | `image_processed` | `update_image_preview()` |
| `'progress'` | `progress_update` | `update_progress_bars()` |
| *(string / other)* | `output_received` | `update_output()` (log area) |

### Signal definitions on `IndexerThread`

```python
output_received = pyqtSignal(str)
image_processed = pyqtSignal(str, str, list, list, dict, str, str, list)
#                             b64  cap  kws   raw   dbg  path  stu  perfs
progress_update = pyqtSignal(dict)
```

### Signal chain — image preview

```
FileProcessor.process_file()
    └── callback({'type': 'image_data',
                  'base64_image', 'caption', 'keywords', 'raw_keywords',
                  'debug_map', 'file_path', 'studio', 'performers'})
            ▼
IndexerThread.process_callback()
    └── image_processed.emit(base64_image, caption, keywords, raw_keywords,
                              debug_map, file_path, studio, performers)
            ▼
MainWindow.update_image_preview()
    ├── appends to self.image_history  (8-tuple, see below)
    └── MainWindow.display_image()
            ├── self.filename_label.setText(...)
            ├── self.studio_label.setText(...)      ← "Studio: X" or ""
            ├── self.performers_label.setText(...)  ← "Performers: A, B" or ""
            ├── self.caption_label.setText(...)
            ├── self.keywords_widget.set_keywords(...)
            └── self.raw_keywords_widget.set_keywords(...)
```

### Signal chain — progress bars

```
FileProcessor._emit_progress()
    └── callback({'type': 'progress',
                  'dirs_total', 'dirs_done',
                  'zips_total', 'zips_done',
                  'images_total', 'images_done',
                  'mode'})          ← 'dir' or 'zip'
            ▼
IndexerThread.process_callback()
    └── progress_update.emit(data_dict)
            ▼
MainWindow.update_progress_bars(data)
    ├── dir_progress_bar   — always visible; "dirs_done / dirs_total"
    ├── zip_progress_row   — visible only when zips_total > 0;
    │                        "zips_done / zips_total"
    └── image_progress_bar — always visible; label text is
                             "Zip images:" (mode='zip') or "Images:" (mode='dir');
                             "images_done / images_total"
```

Progress bars are shown when `run_indexer()` starts and hidden when
`indexer_finished()` fires.

### `_progress` dict keys (emitted with every `'progress'` message)

| Key | Type | Meaning |
|---|---|---|
| `dirs_total` | int | Total directories discovered by `BackgroundIndexer` |
| `dirs_done` | int | Directories fully processed (incremented after each queue item) |
| `zips_total` | int | Zip files in the **current** directory batch (0 if none) |
| `zips_done` | int | Zips completed in the current directory batch |
| `images_total` | int | Images in the current context (directory or single zip) |
| `images_done` | int | Images processed in the current context |
| `mode` | str | `'dir'` — images bar tracks directory images; `'zip'` — tracks current zip |

### Image history navigation

`self.image_history` is a list of 8-tuples:
```python
(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)
```
Navigation buttons (First / Prev / Next / Last) unpack this tuple and call
`display_image()` directly.  `current_position == -1` means "always show the
most recently processed image".

### Settings persistence

All settings are stored in `settings.json` at the working directory.
`SettingsDialog.load_settings()` reads it on dialog open; `save_settings()` writes
it.  The main window calls `save_settings()` via `run_indexer()` before starting.

### DB buttons in SettingsDialog

Two rows of buttons in the Database group:

**Row 1 — connection & vocabulary**

| Button | Method | Action |
|---|---|---|
| Test Connection | `_test_db_connection()` | Opens and closes a connection; shows success/error |
| Load Tag File into DB... | `_load_tags_to_db()` | File picker → `load_tags_from_file()` → stats dialog |
| Export Tags... | `_export_tags()` | `export_tags()` → file save dialog → JSON array |
| Backfill Colored Hair | `_backfill_colored_hair()` | Promotes unnatural hair-colour keywords from unmatched |
| Backfill All Normalizers | `_backfill_normalizers()` | Applies nudity/pubic-hair/labia normalizers to all unmatched |
| Clear Database | `_clear_database()` | Confirmation → `clear_database()` (tags/aliases preserved) |

**Row 2 — analytics & repair**

| Button | Method | Action |
|---|---|---|
| Backfill From Raw | `_backfill_from_raw()` | Applies normalizers to all `image_keywords_raw` rows |
| Promote Aliased Unmatched | `_promote_aliased_unmatched()` | Bulk-promotes unmatched keywords that now have aliases |
| DB Stats | `_db_stats()` | Shows coverage stats from `get_stats()` in a message box |
| Health Check | `_db_health_check()` | Shows health issues; offers to auto-promote aliased unmatched |

**Row 3 — performer tags**

| Button | Method | Action |
|---|---|---|
| Assign Performer Tags | `_assign_performer_tags()` | Runs `assign_performer_tags()`; shows performers checked / tags added / tags removed |

---

## 11. Keyword Processing Pipeline

`FileProcessor.process_keywords(metadata, new_keywords, return_debug=False)` is called
after the LLM returns raw keyword strings.  Each keyword passes through a series of
filters and normalizers in order:

```
1.  _NEGATIVE_RE       — drop "no X", "not X", "without X"
2.  _UNCERTAIN_RE      — drop "possibly X", "appears to be X", "may have X", etc.
3.  _CONTENT_LEVEL_TAGS passthrough
                       — "sfw", "nudity", "explicit" bypass the tag matcher
4.  _normalize_hair    — "blue-purple hair" → sorted "blue and purple hair"
5.  _resolve_colored_hair
                       — maps unnatural hair colours → "Colored Hair" / "Multicolored Hair"
6.  _normalize_piercing — maps location phrasings → "Piercing - Location"
7.  _normalize_tattoo  — maps location phrasings → "Tattoo - Location"
8.  _normalize_nudity  — "fully nude", "topless", etc. → "Nude" / "Topless" / "Bottomless"
9.  _normalize_pubic_hair
                       — "shaved pussy", "trimmed pubes", etc.
                          → "Shaved Pussy" / "Trimmed Pussy" / "Natural Pubic Hair" /
                             "Full Bush" / "Landing Strip"
10. _normalize_labia   — "spread labia", "gaping pussy", etc.
                          → "Spread Labia" / "Spread Pussy" / "Gaping Pussy"
11. TagMatcher fuzzy lookup (primary)
                       — RapidFuzz token_sort_ratio against tag_aliases
12. TagMatcher fallback lookup (secondary)
                       — fallback tag file (stashdb_tags.json or similar)
13. normalize_keyword  — if no TagMatcher active: de-plural, strip, case-normalise
```

After the keyword loop, **caption-based pattern extraction** runs:
- Reads `metadata['MWG:Description']`, splits on sentence boundaries (`[.!?]+`)
- Skips sentences containing negation words (not, no, without, never, etc.)
- Runs `_normalize_nudity`, `_normalize_pubic_hair`, `_normalize_labia` on each sentence
- Any canonical tags found are added to the keyword set

This recovers tags like "Nude" or "Spread Labia" that appear in the LLM caption
but were not emitted as discrete keywords.

### Canonical tag names for body-specific normalizers

| Category | Canonical values |
|---|---|
| Nudity level | `Nude`, `Topless`, `Bottomless` |
| Pubic hair | `Shaved Pussy`, `Trimmed Pussy`, `Natural Pubic Hair`, `Full Bush`, `Landing Strip` |
| Labia | `Gaping Pussy`, `Spread Labia`, `Spread Pussy` |
| Colored hair | `Colored Hair`, `Multicolored Hair` |
| Piercings | `Piercing - Nose`, `Piercing - Nipple`, etc. |
| Tattoos | `Tattoo - Arm`, `Tattoo - Back`, etc. |

Bare adjectives like "Shaved" are intentionally avoided — they are ambiguous
("shaved head" vs. pubic).

---

## 12. `tag_review.py` — Tag Review Tool

A standalone GUI (`python tag_review.py`) for reviewing unmatched keywords from
`image_keywords_unmatched` and assigning them to canonical tags.

### Performer filter

```bash
python tag_review.py --performer "Performer Name"
```

When `--performer` is given, `_load_data()` joins through `image_performers` so only
unmatched keywords from images associated with that performer are shown.  The window
title changes to `"Tag Review — Performer Name"`.  This mode is launched automatically
by the **Tag Review…** button in `explore_performers.py`.

### Features

- **Sequential review**: presents keywords sorted by occurrence count; each keyword
  shows one sample image and (if available) the LLM caption for that image
- **Tag picker**: searchable list of all canonical tags; Enter auto-assigns when
  one result remains; arrow keys navigate
- **Near-miss suggestions**: keywords that score 55–threshold in RapidFuzz
  `token_sort_ratio` are shown as clickable buttons below the tag picker to
  pre-fill the search box
- **Caption display**: the LLM-generated caption for the sample image is shown
  below the image path in an italic label (truncated to 280 chars); fetched via
  `LEFT JOIN image_descriptions`
- **Bulk Assign**: filter unmatched keywords by substring, check any subset,
  assign all selected to one canonical tag
- **View All Keywords**: full sortable table of all unmatched keywords with counts
- **Create New Tag**: creates a brand-new canonical tag from the current keyword

### Assignment logic

When a keyword is assigned to a tag:
1. `INSERT INTO tag_aliases (tag_id, alias) VALUES … ON CONFLICT DO NOTHING`
2. `INSERT INTO image_keywords … SELECT … FROM image_keywords_unmatched WHERE keyword = %s ON CONFLICT DO NOTHING`
3. `DELETE FROM image_keywords_unmatched WHERE keyword = %s`

The tag and alias are immediately available to `TagMatcher` on the next processing
run (TagMatcher reloads from DB at the start of each `process_directory()` call).

---

## 13. `explore_performers.py` — Explore Performers Tool

A standalone GUI (`python explore_performers.py`) for browsing performers stored in
the database and managing their associated tags.

### Layout (three resizable panels via `QSplitter`)

| Panel | Contents |
|---|---|
| **Left** | Searchable performer list with image counts |
| **Centre** | Scalable image preview (`_ScaledImageLabel`) + filename + ◄/► nav (keyboard ←/→ also works); starts on a random image for the selected performer |
| **Right** | Vertical splitter with three sections (see below) |

**Right panel sections:**

1. **Performer Tags** — flow-layout chips from `performer_tags` (excluding tombstoned / globally-excluded rows). Right-click any chip for a context menu. Buttons: **Add Tag…** (opens `_AddTagDialog`), **Tag Review…** (launches `tag_review.py --performer <name>`).
2. **All Image Tags** — `QListWidget` of every canonical tag on this performer's images, sorted by occurrence count, excluding tags already in Performer Tags. Format: `Tag Name  (47  82%)`. Right-click to attach or globally blacklist.
3. **Caption / Image Tags** — LLM caption for the current image + flow-layout chips of the current image's canonical tags.

### Chip colours (Performer Tags section)

| Colour | `pinned` | `manually_added` | Meaning |
|---|---|---|---|
| Green | F | F | Threshold-assigned (>40% of images) |
| Orange / bold | T | F | Pinned — kept below threshold |
| Purple / italic | T | T | Manually added by user |

### Right-click chip menu

- **Pin / Unpin** — calls `pin_performer_tag()`
- **Remove for this performer** — calls `exclude_performer_tag()` (tombstone)
- **Blacklist for ALL performers** — confirmation → `exclude_tag_globally()`

### Right-click All Image Tags menu

- **Attach to performer** — calls `add_performer_tag()` (manually_added=TRUE, pinned=TRUE); tag moves from this list up to Performer Tags chips immediately
- **Blacklist for ALL performers** — same as chip menu

### DB connection

`explore_performers.py` connects directly via psycopg2 (not through `llmii_db.get_connection()`),
so it calls `llmii_db.apply_migrations(conn)` explicitly on startup to ensure the new
`performer_tags` columns exist.  All DB mutation operations delegate to `llmii_db`
functions when available (`_HAS_LLMII_DB`), with inline fallback SQL otherwise.

### Key classes

| Class | Purpose |
|---|---|
| `_ScaledImageLabel` | `QLabel` subclass; stores original `QPixmap` and rescales in `resizeEvent` |
| `FlowLayout` | `QLayout` subclass; wraps child widgets based on available width |
| `_TagChip` | Interactive `QLabel` chip with `customContextMenuRequested` and per-chip callbacks |
| `_AddTagDialog` | Searchable `QDialog` for picking a canonical tag to manually assign |
| `ExplorePerformersWindow` | `QMainWindow`; owns all three panels and all DB interaction |

---

## 14. Key Design Decisions & Gotchas

### Files are processed in alphabetical order

`BackgroundIndexer` sorts its directory list with `directories.sort()` and iterates
files with `sorted(os.listdir(directory))`.  `FileProcessor.process_directory()` also
re-sorts the `image_files` and `zip_files` lists after pulling them from the queue
as a safety measure.  Result: within each directory, images are processed A→Z.

### Upsert pattern for name-lookup tables

All four name-lookup tables (`galleries`, `performers`, `studios`, `tags`) use
**`ON CONFLICT DO NOTHING` + fallback SELECT** instead of `DO UPDATE`.

Reason: PostgreSQL pre-allocates the next sequence value before evaluating the
conflict branch, so `DO UPDATE SET name = EXCLUDED.name` wastes a sequence
value on every conflict, causing large gaps in IDs (1, 63, 110, 191...).
The `DO NOTHING` + `SELECT` pattern keeps IDs sequential.

### Zip routing fields must survive `generate_metadata()`

`generate_metadata()` returns a **fresh dict** populated only with LLM output.
The original `metadata` dict (which has `_zip_db_key`, `_zip_source`,
`_zip_studio`, `_zip_performers`) is a different object.

In `process_file()`, immediately before calling `write_metadata()`, these four
fields are explicitly copied from `metadata` into `updated_metadata`:

```python
for _k in ('_zip_db_key', '_zip_source', '_zip_studio', '_zip_performers'):
    if _k in metadata:
        updated_metadata[_k] = metadata[_k]
```

If this copy is missing, `write_metadata` gets no composite key and no zip_source,
so the DB row uses the temp file path and NULL zip_source — silent data corruption.
The same four fields are also used to build the `image_data` callback dict for
the GUI, so they must be read from the original `metadata` (not `updated_metadata`)
at callback time since the copy hasn't happened yet.

### `images.identifier` is UUID NOT NULL

The column has no default.  Code must never pass `''` as identifier.
The fallback throughout the codebase is `str(uuid.uuid4())` — never an empty string.

### Composite key format

`images.path` stores either:
- An absolute filesystem path for regular images: `/data/photos/img.jpg`
- A composite key for zip-extracted images: `/data/zips/archive.zip::folder/img.jpg`

The `::` separator is the split point.  All code that checks whether an image is
zip-sourced tests `'::' in path_str`.

### `get_image_status_batch()` handles both key types

The same function is used to skip-check both regular images (by real path) and
zip images (by composite key), so the DB-mode skip logic works uniformly.

### `clear_database()` preserves tags

The tag vocabulary is curated manually and is expensive to rebuild.  `clear_database()`
intentionally skips `tags` and `tag_aliases`.  The warning dialog in the GUI
reflects this.

### Sparse reprocess

When `config.reprocess_sparse = True`, images that are already marked `success` but
have fewer than `config.reprocess_sparse_min` matched keywords are also re-queued.
The check is done inside `_check_uuid_status()`: if the DB returns a `keyword_count`
below the threshold, `metadata['XMP:Status']` is set to `None` so the image is
treated as unprocessed.  This requires DB mode — `get_image_status_batch()` is the
only caller that has keyword counts.  Sidecar-only mode cannot use this feature.

### Performers are not added as keyword tags

Performer names are stored exclusively in `performers` and `image_performers` tables.
They are never added to `image_keywords`.  Adding performer names as tags would
pollute the keyword vocabulary with thousands of person-specific values.

### Near-miss threshold

`tag_review.py` uses RapidFuzz `token_sort_ratio` with the same `tag_fuzzy_threshold`
from `settings.json` (default 90).  Keywords scoring between 55 (floor) and 90
(threshold) are shown as near-miss suggestions — they are close but not close enough
to have matched automatically.  This helps reviewers spot near-duplicates.

### Schema migrations are automatic

`get_connection()` calls `apply_migrations()` by default on every connection.
All migration statements use `IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` so they
are no-ops when already applied.  The `.sql` files in `sql/` are historical
reference only — they are not executed by the application.

### Extension filter applies inside zips too

`_accepted_image_exts()` computes the set of allowed image extensions from the
user's extension filter, minus `.zip` itself.  This same set is used when listing
images inside a zip archive, so if the user filters to `jpg,jpeg` only, `.png`
files inside a zip are skipped.

### Studio detection priority

For every image written to the DB:
1. `SuicideGirls` anywhere in the path → studio = `'Suicide Girls'` (highest priority)
2. `_zip_studio` from zip filename parse → studio from zip
3. `Galleries` directory in path → studio = parent directory name
4. No studio detected → no `studio_images` / `studio_galleries` rows written

### GUI studio/performers display vs DB studio detection

For the live image preview, studio and performers are derived **before** `write_metadata`
is called.  For zip images, `_zip_studio` and `_zip_performers` from the original
`metadata` are used directly.  For regular images, `parse_gallery_and_performers(file_path)`
is called for performers, and the path is scanned for `suicidegirls` for studio.

The `Galleries` directory studio heuristic (rule 3 above) is **not** replicated in
the GUI callback — only the DB write uses it.  This means the studio label in the
GUI may be blank for some non-zip, non-SuicideGirls images that would still have a
studio row written to the database.

### Progress bar denominator accuracy

`BackgroundIndexer.total_directories` counts **all non-skipped directories** found
by `os.walk`, set before indexing starts.  `dirs_done` is incremented only for
directories that actually had files in the queue.  If a directory contains no
matching files, it is never put in the queue, so the directory progress bar may
not reach 100% if empty directories exist under the source root.  This is acceptable —
the bar still accurately reflects work progress.

---

## 15. Dependencies

| Package | Required | Purpose |
|---|---|---|
| `PyQt6` | Yes | GUI framework |
| `pyexiftool` | Yes | ExifTool Python wrapper (batch metadata reads) |
| `pillow` | Yes | JPEG/PNG/GIF/WEBP image loading and resizing |
| `pillow-heif` | Yes | HEIF/HEIC image support |
| `rawpy` | Yes | RAW image support (CR2, NEF, ARW, etc.) |
| `requests` | Yes | HTTP calls to KoboldCPP API |
| `rapidfuzz` | Yes | Fuzzy string matching for tag resolution |
| `json-repair` | Yes | Fix malformed JSON from LLM output |
| `regex` | Yes | Extended regex (used in keyword normalisation) |
| `psycopg2` | Optional | PostgreSQL integration — gracefully absent if missing |
| ExifTool | Yes (external) | Must be installed separately; called via pyexiftool |

Install: `pip install -e .` (reads `pyproject.toml`).

---

## 16. Running the Application

```bash
# GUI mode
python launcher.py

# The GUI will:
# 1. Poll the KoboldCPP API URL until connected
# 2. Enable the Run button once the API responds
# 3. Use settings.json for all persistent configuration
```

Settings are saved to `settings.json` in the working directory when the indexer
is started.  The file is human-readable JSON and can be edited manually.

---

## 17. Database Setup Checklist

1. Create a PostgreSQL database (any name — set in settings)
2. Create the `ai_captioning` schema: `CREATE SCHEMA ai_captioning;`
3. Create the `citext` extension: `CREATE EXTENSION citext SCHEMA ai_captioning;`
4. Create the base tables (tags, tag_aliases, galleries, performers, images,
   tagger_runs, image_run_status, image_descriptions, image_keywords,
   image_keywords_raw, image_keywords_unmatched, image_performers)
5. Connect via the GUI — `apply_migrations()` adds studios and zip_source automatically
6. Load your master tag vocabulary via **Settings → Load Tag File into DB...**

The app will run in JSON-only mode (`output_mode = 'json'`) even without a database.
Switch to `'db'` or `'both'` in settings to enable DB writes.
