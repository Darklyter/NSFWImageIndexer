# Fix Plan — Code Review Remediation (June 2026)

Remediation of ~110 distinct confirmed findings from two multi-agent review rounds
(June 11, 2026). Raw verified findings archived at `.claude/review-round1.json` (51
confirmed) and `.claude/review-round2.json` (77 confirmed, ~60 distinct after dedupe).
Every finding below was adversarially verified against the actual code; fix approaches
incorporate the verifiers' corrections.

## Strategy

- **Branching:** Commit the in-progress LM Studio feature work on `main` first (it is
  complete, reviewed feature work — keep it out of the fix diff). Then all fixes happen
  on branch `fix/code-review-2026-06`.
- **Commits:** One commit per High/Medium item; Low items grouped into per-file cluster
  commits. Every commit subject cites its plan ID (e.g. `P3.04`), so any regression
  bisects to a single finding.
- **Tests:** New `tests/` directory with pytest (installed into `.venv` at Phase 0).
  Pure functions (normalize_keyword, de_pluralize, clean_tags, clean_json,
  markdown_list_to_dict, the `_normalize_*` rules, TagMatcher) get regression tests
  written **before** each fix where feasible (red → green). Stateful code is verified by
  targeted harness scripts + the Phase 8 sweep.
- **Scratch DB:** Phase 2 validated against a scratch database `llmii_scratch` created
  via `create_database.py --dbname llmii_scratch` — never against the production DB.
  Migration paths tested both from fresh schema and from a pre-migration schema.
- **Phase gates:** each phase ends with `py_compile` over all sources, full pytest run,
  and the phase's targeted verification. No phase starts until the previous gate passes.
- **Final sweep (Phase 8):** a multi-agent verification workflow re-checks every fixed
  finding against the diff, plus a fresh regression review of all changed code.

## Decision points (defaults applied unless overridden)

| ID | Decision | Default (recommended) | Alternative |
|----|----------|----------------------|-------------|
| D1 | Sidecar stem collision (`photo.jpg` + `photo.png` → one `photo.json`) | **Extension-preserving names** (`photo.jpg.json`) for new writes; reads check new name first, then legacy `photo.json` fallback. No migration of existing sidecars needed. | Keep legacy naming, only detect collision and warn/skip second file |
| D2 | `images.sha256` never populated (Find Duplicates always empty) | **Compute SHA-256 during processing** (file bytes are already read for the VLM; hash adds negligible cost) and pass to `_upsert_image`. Existing rows stay NULL until reprocessed. | Drop the Find Duplicates button instead |
| D3 | Zip + JSON output mode destroys its own sidecars | **Full fix:** write zip-image sidecars to a persistent location keyed by `<zip_abs>::<member>`, add JSON-mode already-done check in `_extract_single_zip`. | Warn-and-skip zips when `output_mode='json'` with no sidecar_dir |
| D4 | Keyword history across runs (stale tags persist; counts inflate) | **Mirror the `update_keywords` setting:** when "Don't clear existing keywords" is OFF, reprocess deletes ALL prior runs' `image_keywords` rows for the image before insert; when ON, keep history. `keyword_count` becomes `COUNT(DISTINCT tag_id)` either way. | Always delete all prior rows (lose run history) |
| D5 | `--model` ignored in GUI setup mode | **Doc fix only** — help text already scopes it to terminal mode; clarify further. | Thread model selection through SetupApp |

---

## Phase 0 — Baseline & scaffolding

| # | Task |
|---|------|
| P0.1 | Commit LM Studio feature work on `main` (README.md, settings.json.example, src/llmii.py, src/llmii_gui.py) |
| P0.2 | Create branch `fix/code-review-2026-06` |
| P0.3 | `pip install pytest` into `.venv`; create `tests/` + `tests/conftest.py` (adds `src/` to sys.path) |
| P0.4 | Baseline smoke: `py_compile` all .py; import llmii, llmii_db, llmii_utils, image_processor; record baseline behavior of functions about to change (golden tests on CURRENT behavior where it's correct) |
| P0.5 | Create scratch DB `llmii_scratch` via create_database.py (skip if PG unreachable; Phase 2 then verified by SQL review + mocked tests, flagged in final report) |

**Gate:** clean compile, imports OK, pytest runs (0 tests).

---

## Phase 1 — Stop the destruction (all 8 Highs + destructive Mediums)

Everything here either destroys data, silently loses work, or crashes the process.

| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P1.01 | argparse `type="store_true"` — CLI 100% dead | llmii.py:796-797 | `action="store_true"` ×2; add `type=int` to `--gen-count`; align argparse defaults with Config via `default=argparse.SUPPRESS` + only setattr supplied args |
| P1.02 | One unreadable file aborts 50-file ExifTool batch | llmii.py:2036, 2090-2093 | In `except ExifToolExecuteError`: parse `e.stdout` (contains valid JSON for readable files); fall back to per-file retry for the remainder; record dropped files as failed (NOT `-m`, which doesn't cover hard errors) |
| P1.03 | Zip+JSON mode: sidecars written into temp dir then rmtree'd | llmii.py:2491-2494, 1776, 1725 | Per D3: persistent sidecar keyed by `<zip>::<member>` composite; JSON-mode done-check in `_extract_single_zip` |
| P1.04 | Writeability probe → DB CHECK violation; files with warnings permanently skipped in db mode; clobbers sidecars; junk image rows | llmii.py:2161-2173, create_database.py:381 | Probe must not reach DB or sidecar: replace `write_metadata` call with a direct file-writability check (os.access + open-for-append probe or ExifTool dry write). Also fix the inverted `minor >= warnings` condition (R1: should test when warnings are serious, per comment intent) |
| P1.05 | `dry_run` renames files and deletes `_original` backups | llmii.py:2154-2155, 2197-2207, 1358 | Gate `rename_to_invalid` and `fix_file_extension` on `not config.dry_run`; in dry run log what WOULD happen; also clear `success` flag correctly so dry-run failures aren't reported as successes (2233) |
| P1.06 | `_clear_temp_dir` rmtree's cwd-relative `temp/` unconditionally | llmii.py:1175, 1534, 1657-1665 | Only clear when zips will be processed; use a namespaced dir (`temp/llmii_zip_extract/`) so a foreign `temp/` is never deleted; refuse to rmtree if the dir wasn't created by us (marker file) |
| P1.07 | Sidecar stem collision silently clobbers + skips second image | llmii.py:1419, 2048-2057 | Per D1: write `file.jpg.json`; read new-name-first with legacy fallback |
| P1.08 | `apply_migrations` failure poisons connection (R1-H3) | llmii_db.py:316-349 | Per-statement SAVEPOINT inside apply_migrations; `conn.rollback()` in `get_connection`'s except before warning |
| P1.09 | Bulk-assign rollback loses earlier keywords but reports them assigned (R1-H4) | tag_review.py:429-497 | SAVEPOINT per keyword, `ROLLBACK TO SAVEPOINT` on failure, single final commit; only append to `assigned` for keywords that survive |
| P1.10 | closeEvent doesn't stop IndexerThread → Qt abort (R1-H1) | llmii_gui.py:2647-2652 | Set `indexer_thread.stopped = True` + emit stop signal; bounded `wait(10000)`; `terminate()` fallback (quit() is a no-op — worker has no event loop) |
| P1.11 | Quick re-run drops still-running IndexerThread → Qt abort (R1-H2) | llmii_gui.py:2523-2539, 2578 | Keep Run/Resume disabled in `stop_indexer()` until `indexer_finished()` fires; defensive `isRunning()` → stop+wait backstop in `run_indexer()` |

**Gate:** pytest green; manual: dry-run on a sample dir mutates nothing; start+stop+restart GUI run; kill a migration mid-way against scratch DB and confirm connection still usable.

---

## Phase 2 — Database semantics & schema

Land schema/semantics before the tools and GUI phases that depend on them.
All verified against `llmii_scratch`.

| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P2.01 | Stale keywords persist across runs | llmii_db.py:634-646 | Per D4: delete all-runs rows when clearing keywords; scoped delete when `update_keywords` on |
| P2.02 | `keyword_count` counts rows across all runs (defeats reprocess_sparse) | llmii_db.py:409-411 | `COUNT(DISTINCT ik.tag_id)` |
| P2.03 | `merge_tag` cascade-deletes performer_tags (pins/tombstones lost) | llmii_db.py:1630-1631 | Before deleting source tag: UPDATE performer_tags→target for pairs absent on target; OR-merge pinned/manually_added/excluded for pairs present on both; then delete |
| P2.04 | Backfills promote negated/uncertain keywords ("not nude") | llmii_db.py:1324-1337, 1181-1197 | Replicate `_NEGATIVE_RE`/`_UNCERTAIN_RE` guards in `_classify` (import or duplicate the patterns with a cross-reference comment) |
| P2.05 | `images.sha256` never populated | llmii_db.py:1770-1785, llmii.py write path | Per D2: hash file bytes during processing, thread through `write_image_to_db`→`_upsert_image` |
| P2.06 | UNIQUE(identifier) violation when two paths share an embedded UUID | llmii_db.py:206-219 | Catch unique-violation on identifier; SELECT existing row by identifier and reuse/update it (do NOT regenerate the UUID — it's file-embedded identity) |
| P2.07 | Failed/orphan writes pump pre-existing embedded keywords into canonical tags | llmii.py:2230-2234, 1896-1900 → llmii_db.py:508 | On failed/orphan status writes, pass keywords=[] to the DB layer (raw/unmatched/canonical inserts are for generated output only) |
| P2.08 | `get_stats` avg counts zero-keyword images as 1 | llmii_db.py:1403-1412 | `COUNT(ik.tag_id)` instead of `COUNT(*)` on the LEFT JOIN |
| P2.09 | `export_tags` drops alias-less tags | llmii_db.py:1253-1262 | LEFT JOIN from tags; emit tags with empty alias list |
| P2.10 | `rename_tag` rejects case-only renames (citext self-match) | llmii_db.py:1543-1545 | Exclude `id = tag_id` from the duplicate check |
| P2.11 | `parse_zip_metadata` reads first parenthesized block, not trailing | llmii_db.py:133-138 | Match the LAST block per docstring; add test with multi-paren names |
| P2.12 | `get_processed_paths` LIKE prefix unescaped (R1) | llmii_db.py:1799-1814 | Replace LIKE with `left(i.path, length(%s)) = %s` literal prefix compare (both slash variants) |
| P2.13 | Path comparisons case-sensitive on Windows (resume breaks, dup rows) | llmii_db.py:1800-1824, 413, 206; llmii.py:1118 | Normalize with `os.path.normcase(os.path.normpath(...))` at all comparison sites (skip_paths build + membership); store paths as given but compare case-folded on win32 |
| P2.14 | No `connect_timeout` (GUI hangs on unreachable host) | llmii_db.py:336-343 | `connect_timeout=5` in psycopg2.connect |
| P2.15 | `_classify` called twice per row in backfills (R1) | llmii_db.py:1081, 1205, 1345 | Compute once per row (loop or for-in-tuple idiom) |
| P2.16 | create_database.py per-step error recovery rolls back ALL prior DDL | create_database.py:500-519 | Commit after each successful step (DDL is transactional in PG; per-step commit makes recovery actually per-step) |
| P2.17 | `CREATE OR REPLACE TRIGGER` requires PG14+ (R1) | create_database.py:173-179, 281-288 | `DROP TRIGGER IF EXISTS …; CREATE TRIGGER …` pairs (works on all versions, stays idempotent) |

**Gate:** pytest green; scratch-DB integration script exercises: write→reprocess→keyword
state per D4, merge_tag preserving pins, backfill skipping "not nude", export/import
round-trip, case-variant resume, duplicate-identifier upsert.

---

## Phase 3 — Keyword pipeline & pure-function fixes (with regression tests)

Highest fix density, all side-effect-free, all locked with pytest cases.

### llmii.py — pipeline logic
| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P3.01 | Caption-based extraction scans OLD caption (feature inert on fresh images) | llmii.py:3063 vs 2431/2452 | Add `caption=None` param to `process_keywords`; pass the freshly generated caption from `generate_metadata` (do NOT pre-mutate metadata — retry path returns it) |
| P3.02 | "nude stockings/heels/lipstick" → false `Nude` tag | llmii.py:2808, 2986 | In `_normalize_nudity`, skip the bare nude/naked rule when the following word is a garment/cosmetic noun (stockings, pantyhose, tights, heels, pumps, shoes, lipstick, lips, nails, bra, panties, dress, bodysuit, top…) |
| P3.03 | `Piercing - X`/`Tattoo - X` destroyed in no-matcher mode | llmii.py:2974-2983, 3038 | Return canonical forms directly with blacklist check (mirroring nudity/pubic/labia families) instead of falling through to normalize_keyword |
| P3.04 | `markdown_list_to_dict` unanchored `\d+\.` turns prose into keywords | llmii.py:187 | `r"^\s*(?:[-*+]|\d+\.)\s*(.+)$"` (verified to keep real lists working) |
| P3.05 | `clean_tags` char-splits string Keywords; non-string element → file retry | llmii.py:295-311 | Type-check: str value → split on commas; non-str elements → str() or skip; mirror the careful handling already in the raw-string-list branch |
| P3.06 | KoboldCpp grammar never applied to `keywords_from_text` | llmii.py:945-982 | `elif task in ("keywords", "keywords_from_text"):` |
| P3.07 | None Description TypeError in caption concat (R1) | llmii.py:2405-2406 | `desc = data.get("Description") or ""`; skip `<generated>` wrapper when empty |
| P3.08 | Hyphenated 1-char-part keywords rejected (t-shirt, v-neck) (R1) | llmii.py:127-133 | Apply min-length check to whole tokens, not hyphen-split parts |
| P3.09 | Hyphenated depluralization skipped (R1) | llmii.py:139-146 | Branch on `len(tokens)` not `len(words)` |
| P3.10 | Blacklist skipped on no-matcher fallback (R1) | llmii.py:3036-3038 | Check blacklist on normalize_keyword result (guard None first) |
| P3.11 | Caption-sentence nudity over-broad (R1) | llmii.py:2808-2811 | Largely addressed by P3.02's shared rule fix; additionally require subject-anchor co-occurrence for bare nude/topless in the SENTENCE path only |
| P3.12 | `split_on_internal_capital` mangles ALL-CAPS (BLONDE→"blon de") | llmii.py:21-23 | Skip splitting when the word is entirely uppercase |
| P3.13 | and/or-split keywords depluralized twice ("boots and buses"→"boot bu") | llmii.py:117-146 | Depluralize once, after the and/or handling, not in both passes |
| P3.14 | AND_EXCEPTIONS idioms still depluralized ("facts and figures"→"…figure") | llmii.py:118-146 | Exempt AND_EXCEPTIONS matches from depluralization |
| P3.15 | Word-count limit allows max_words+1 unconditionally | llmii.py:111-114 | Allow +1 only when token list contains and/or, per documented intent |
| P3.16 | `split_and_entries` merges instead of splitting (contradicts GUI help) | llmii.py:121-149 | Split "X and Y" into two keywords (except AND_EXCEPTIONS); update test + GUI help if behavior intentionally differs |
| P3.17 | `clean_string` smart-quote class is 3 straight quotes; newline-glue | llmii.py:169-170 | Real curly-quote characters in the class; replace newlines with space not empty string |
| P3.18 | Finger jewelry → `Piercing - Finger`; "necklace" substring → neck | llmii.py:2702 | Remove finger from piercing locations (rings aren't piercings); use `\bneck\b` word-boundary not substring |
| P3.19 | Dead `_MULTI_HAIR_RE` (R1) | llmii.py:2616 | Delete |
| P3.20 | Duplicate `_PIERCING_LOCATIONS` entries (R1) | llmii.py:2685-2704 | Remove line 2703; fold `belly` into the 2687 pattern then drop 2698; keep 2696/2697 (verifier: they're distinct) |
| P3.21 | reasoning_content fallback can pollute keywords (R1) | llmii.py:1003-1019 | Strip `<think>…</think>` from fallback; for JSON tasks accept fallback only if it parses to expected shape, else None→retry; run degenerate-loop detector on pre-fallback content |
| P3.22 | LM Studio blank model → 400s (R1) | llmii.py:898 + GUI | Fail-fast validation at run start when lm_studio=True and model empty (GUI message + CLI error) |
| P3.23 | top_k/min_p always sent to LM Studio (R1) | llmii.py:889-906 | Move into the KoboldCpp branch |

### llmii_utils.py
| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P3.24 | `^pie`/`^tie` anchors dead in endswith → pies→py, ties→ty (R1) | llmii_utils.py:953, 963 | Drop the `^` anchors |
| P3.25 | `de_pluralize('s')` → `''` (R1) | llmii_utils.py:839, 1108 | Return original word when result would be empty/whitespace |
| P3.26 | `([octop|vir])i$` is a char class → spaghetti→spaghettus | llmii_utils.py:807 | `(octop|vir)i$` proper alternation; add tests for octopi/virii/spaghetti/yeti |
| P3.27 | `repair_json` IndexError on truncated input (R1) | llmii_utils.py:369+ | Bounds-check eat_* helpers; raise JsonFixError on premature end |
| P3.28 | `eat_circular` char-class regex (R1) | llmii_utils.py:681-684 | Literal-sequence match on remaining substring + bounds guard |
| P3.29 | Mutable default `custom={}` (R1) | llmii_utils.py:1060 | `custom=None` + `if custom is None: custom = {}` |

**Gate:** pytest suite (~40 new cases) green, including golden tests confirming
unchanged behavior for representative correct inputs.

---

## Phase 4 — FileProcessor, run lifecycle & image processing

| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P4.01 | EXIF orientation never applied (sideways images to VLM) | image_processor.py:133-144, 100-106 | `ImageOps.exif_transpose` after open in route_image and the embedded-thumbnail RAW branch (rawpy postprocess path already handles it) |
| P4.02 | Small images blurrily upscaled to res_limit | image_processor.py:73-92 | Clamp `scale = min(scale, 1.0)`; cap final dims at max_dimension after patch rounding |
| P4.03 | BackgroundIndexer unstoppable; untimed join blocks Stop | llmii.py:1039-1102, 3133-3136 | Add `stop_event` checked in walk loop; `daemon=True`; `join(timeout=10)` in main()'s finally; FileProcessor signals stop to indexer |
| P4.04 | Checkpoint records write-FAILED files as processed | llmii.py:1870-1878 | Only checkpoint files whose write succeeded |
| P4.05 | Checkpoint written non-atomically | llmii.py:1518 | Write to `.tmp` + `os.replace` |
| P4.06 | BackgroundIndexer.run lacks try/finally → consumer loops forever | llmii.py:1080-1102 | try/finally guarantees `indexing_complete = True` + sentinel enqueue |
| P4.07 | tagger_runs marked 'success' for stopped/crashed/db-fatal runs | llmii.py:1621-1627 | Thread actual outcome into finish_tagger_run ('stopped'/'failed'/'success'); extend CHECK constraint via migration if needed |
| P4.08 | XMP:Status 'invalid' honored but never written | llmii.py:2131 | Write status 'invalid' (file/sidecar) when validation fails so re-validation isn't repeated every run |
| P4.09 | Orphan + reprocess_all skips reprocess, false error msg (R1) | llmii.py:1896-1913 | `if not written:` error path; otherwise fall through to reprocess_all handling |
| P4.10 | Missing comma in caption_fields (R1) | llmii.py:1285 | Add comma |
| P4.11 | db-only fatal path reports unwritten file as success (R1) | llmii.py:2545-2561 | `success = False` alongside `_db_fatal = True` (and the 'both' fallback branch) |
| P4.12 | skip_folders bare substring over-broad | llmii.py:1054-1078 | Path-component matching; prune via os.walk dirs list so subdir skipping still works (verifier: substring was the only subdir mechanism — replace, don't just delete) |
| P4.13 | Zip extract ignores zf.extract() return path | llmii.py:1752-1767 | Use extract()'s returned path as temp_file |
| P4.14 | use_sidecar read/write sidecar-name mismatch | llmii.py:2030-2048 vs 1815/1419 | Derive JSON-sidecar path from the ORIGINAL image path on both sides |
| P4.15 | Progress treats 100-file chunks as directories; bar resets | llmii.py:1543-1593 | Count directories once at enqueue; per-chunk math removed |
| P4.16 | Stop exception swallowed as per-file "Processing Error" | llmii.py:2336-2345 | Re-raise the stop exception type past the catch-all |
| P4.17 | Writeability-condition comment mismatch (R1, residual of P1.04) | llmii.py:2161 | Covered by P1.04 redesign; verify comment matches behavior |

**Gate:** pytest green; harness scripts: checkpoint kill-resume test, stop-latency test
(< 2s on large tree), zip with hostile member names, sideways-EXIF image visibly upright
in the encoded payload.

---

## Phase 5 — GUI

| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P5.01 | `processEvents()` re-entrancy in update_output (R1) | llmii_gui.py:2625-2628 | Remove the call |
| P5.02 | Concurrent DB-button clicks destroy running QThread | llmii_gui.py:851-881 | Disable ALL DB buttons while a worker runs (or refuse with a message if `_active_db_worker` is alive) |
| P5.03 | load_settings all-or-nothing try → silent default reversion, then Save destroys settings.json | llmii_gui.py:1422-1532 | Per-key try/except helper (`_load_key(settings, key, setter, default)`); collect failures and show one warning dialog naming bad keys |
| P5.04 | Settings Cancel doesn't revert; runs read live widgets | llmii_gui.py:2182, 2425-2509 | Snapshot widget state on dialog open; restore on reject(). (Long-term: runs should read settings.json, but snapshot/restore fixes the bug minimally) |
| P5.05 | pause_handler signals reconnected every run (R1) | llmii_gui.py:2537-2538 | Connect once in `__init__` |
| P5.06 | system_instruction default mismatch (R1) | llmii_gui.py:265, 1439 | Single module-level DEFAULT_SYSTEM_INSTRUCTION matching llmii.py:625 |
| P5.07 | min_p spinbox max 2.0 (R1) | llmii_gui.py:389-392 | `setRange(0.0, 1.0)` |
| P5.08 | start_api_check unbounded wait can freeze UI (R1) | llmii_gui.py:2204-2206 | Probe timeout 2s; `self.running` check inside probe loop; `wait(500)` |
| P5.09 | History trim desyncs displayed image at oldest entries | llmii_gui.py:2243-2257 | Clamp current_position ≥ 0 after trim and refresh display if the shown entry was trimmed |
| P5.10 | `directory` only saved via Settings accept | llmii_gui.py:2188-2195, 1534-1607 | Persist directory on run start as well; save_settings reads current dir_input |

**Gate:** pytest green; manual GUI checklist: open/cancel settings (values revert),
double-click two DB buttons (second refused), close mid-run (clean exit), bad
settings.json value (warning dialog, other settings intact).

---

## Phase 6 — Review tools

### tag_review.py
| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P6.01 | `_assign()` IndexError app abort at done-state | tag_review.py:1470-1476 | Guard `current_idx < len(self.keywords)` in _assign/_create_new_tag/_skip; clear list selection in _show_done |
| P6.02 | Bulk assign / create-new ignore existing alias to a different tag | tag_review.py:442-446, 1612-1617 | Detect conflict (same check the single path does at 1491); in bulk, collect conflicts and report; update alias when user confirmed semantics |
| P6.03 | Skip/Create buttons stay disabled after done→Apply (R1) | tag_review.py:1328-1356 | Re-enable in `_show_current` live branch |
| P6.04 | `_remove_keywords` stale display/index desync (R1) | tag_review.py:1707-1718 | Capture displayed keyword before rebuild; relocate index after; refresh when removed/shifted |
| P6.05 | Near-miss threshold default 90 ≠ engine 85 (R1) | tag_review.py:1229-1245 | `settings.get('tag_fuzzy_threshold', TagMatcher.FUZZY_THRESHOLD)`; fix comment |
| P6.06 | Near-miss candidate set differs from TagMatcher (R1) | tag_review.py:1222-1247 | Match against aliases + canonical with TagMatcher._normalize; reuse TagMatcher where importable |
| P6.07 | Startup load crash bypasses QMessageBox (R1) | tag_review.py:1782-1792 | try/except around window construction; critical dialog + conn.close + exit(1) |
| P6.08 | `--performer` filter doesn't filter the image browser | tag_review.py:1373-1380 | Apply performer join to the _image_paths query when filter active |
| P6.09 | `_create_new_tag` duplicate picker entries | tag_review.py:1646 | Only append when the tag was actually created (check existing first) |
| P6.10 | capwords forced + case-only renames blocked | tag_review.py:740-741, 1581 | Preserve user casing; allow case-only rename (pairs with P2.10) |
| P6.11 | Zip-archived images not previewed ('path::inner') | tag_review.py:1417-1418 | Detect composite path; show "(inside zip archive)" placeholder + zip name instead of broken load |

### explore_performers.py
| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P6.12 | 5 action-handler excepts missing rollback (R1) | 898, 920, 950, 975, 1075 | `self.conn.rollback()` first in each |
| P6.13 | _show_image silent excepts, no rollback (R1) | 1208-1209, 1222-1223 | rollback + print in both |
| P6.14 | All Image Tags pane ignores global blacklist | 987-1000 | `AND NOT COALESCE(t.exclude_from_performers, FALSE)` |
| P6.15 | apply_migrations startup failure not rolled back (R1) | 1274-1278 | rollback in except (also covered by P1.08's in-function fix) |
| P6.16 | Merge leaves performer_tags counts stale | 1116-1127 | Recompute image_count/total_images for merged rows after reassignment |
| P6.17 | Stale 3-tuple comment (R1) | 473 | Update comment |

**Gate:** pytest green; manual: both tools open against scratch DB; assign/skip/bulk
flows; merge preserving pins; blacklist-from-pane immediately disappears.

---

## Phase 7 — Setup, launcher, packaging

| # | Finding | Where | Fix approach |
|---|---------|-------|--------------|
| P7.01 | Download integrity: short reads accepted (R1) | llmii_setup.py:312-346 | Verify `downloaded == total_size` when Content-Length present; delete + False on mismatch; also delete temp on version-parse failure instead of returning it (499-501) |
| P7.02 | koboldcpp.exe never matches `koboldcpp-*` glob → re-download loop (R1) | llmii_setup.py:456-463, 499-501 | Rename to `koboldcpp-unknown.exe` + write version.txt("unknown") on parse failure |
| P7.03 | CUDA `float()` crash on malformed token (R1) | llmii_setup.py:423-424 | try/except around parse; fall back to nocuda/oldpc build |
| P7.04 | `line.split()[3]` fragile CUDA parse (R1) | llmii_setup.py:52-55 | `re.search(r'(\d+\.\d+)', line)`; only set cuda_available on numeric match |
| P7.05 | Vulkan VRAM positional zip mispairs devices (R1) | llmii_setup.py:104-138 | Parse per-device blocks; max DEVICE_LOCAL heap per device; tolerate missing heap section (vram 0 + log) |
| P7.06 | Linux bash -c naive quoting (R1) | launcher.py:208-219 | `shlex.quote` each arg + working_dir |
| P7.07 | `--model` ignored in GUI mode (R1) | llmii_setup.py:1241-1248 | Per D5: clarify help text (documented terminal-only) |
| P7.08 | Dead `sys.exit()` (R1) | llmii_setup.py:1278 | Delete |

**Gate:** pytest green; py_compile; simulated download-truncation test (mock response).

---

## Phase 8 — Verification sweep & wrap-up

1. Full pytest suite + `py_compile` over every source file.
2. Scratch-DB end-to-end: fresh `create_database.py`, then `apply_migrations` from a
   deliberately pre-migration schema copy; run the Phase 2 integration script.
3. GUI smoke: launch app, open/cancel/accept settings, start+stop a small run.
4. Pretend-mode end-to-end run on a sample directory (now genuinely non-mutating per
   P1.05) with the LM Studio backend.
5. **Multi-agent verification workflow:** one agent per fixed finding confirms the fix
   is present and correct in the diff; plus fresh regression reviewers over all changed
   code (same adversarial-verify structure as the review rounds).
6. Docs: update README.md / DEVELOPER_GUIDE.md for behavior changes (dry-run semantics,
   sidecar naming D1, status lifecycle P4.07/P4.08, keyword-history D4); update
   project memory.
7. Present final diff summary; merge `fix/code-review-2026-06` → `main` on approval.

## Risk register

- **llmii.py absorbs ~45 fixes** (3,140-line file) — highest regression surface.
  Mitigations: per-fix commits, golden tests before behavior-changing fixes, Phase 8
  re-review workflow.
- **D1 sidecar naming** is the only intentional format change; legacy-read fallback
  keeps old libraries working.
- **D4 keyword-history deletion** is destructive-by-design on reprocess; gated behind
  the existing "Don't clear existing keywords" setting so user intent controls it.
- **Schema changes** are migration-only and idempotent; tested against scratch DB
  before ever touching production.
- Items where the verifier corrected the original fix (P1.02, P1.04, P2.03, P2.06,
  P3.02, P3.21, P4.12) follow the corrected approach, not the reviewer's original.
