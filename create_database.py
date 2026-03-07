#!/usr/bin/env python3
"""
create_database.py — Initialize the ai_captioning schema in PostgreSQL.

Usage:
    python create_database.py [--host HOST] [--port PORT]
                               [--user USER] [--password PASSWORD]
                               [--dbname DBNAME]

Defaults are read from settings.json if present. Requires PostgreSQL 14+
and the citext extension available on the server (ships with PostgreSQL).
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is not installed.")
    print("       Run: pip install psycopg2-binary")
    sys.exit(1)


# ── DDL steps executed in dependency order ────────────────────────────────────

_DDL = [
    # 1. Schema
    (
        "Create schema",
        "CREATE SCHEMA IF NOT EXISTS ai_captioning",
    ),

    # 2. citext extension — installs ai_captioning.citext type
    (
        "Install citext extension",
        "CREATE EXTENSION IF NOT EXISTS citext SCHEMA ai_captioning",
    ),

    # 3. Trigger function (must exist before tables reference it)
    (
        "Create updated_at trigger function",
        """
        CREATE OR REPLACE FUNCTION ai_captioning.ai_captioning_set_updated_at()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.updated_at := now();
            RETURN NEW;
        END;
        $$
        """,
    ),

    # 4. Base tables (no foreign-key dependencies)

    (
        "Create table: galleries",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.galleries (
            id         bigserial    PRIMARY KEY,
            name       text         NOT NULL,
            created_at timestamptz  NOT NULL DEFAULT now(),
            CONSTRAINT galleries_name_key UNIQUE (name)
        )
        """,
    ),
    (
        "Index: galleries_name_idx",
        "CREATE INDEX IF NOT EXISTS galleries_name_idx ON ai_captioning.galleries (name)",
    ),

    (
        "Create table: performers",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.performers (
            id         bigserial   PRIMARY KEY,
            name       text        NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT performers_name_key UNIQUE (name)
        )
        """,
    ),
    (
        "Index: performers_name_idx",
        "CREATE INDEX IF NOT EXISTS performers_name_idx ON ai_captioning.performers (name)",
    ),

    (
        "Create table: studios",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.studios (
            id         serial4     PRIMARY KEY,
            name       text        NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT studios_name_key UNIQUE (name)
        )
        """,
    ),

    (
        "Create table: tagger_runs",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.tagger_runs (
            id          bigserial   PRIMARY KEY,
            tagger_name text        NOT NULL,
            tagger_ver  text,
            params_json jsonb,
            status      text,
            started_at  timestamptz NOT NULL DEFAULT now(),
            finished_at timestamptz,
            CONSTRAINT tagger_runs_status_check
                CHECK (status = ANY (ARRAY['running','success','failed','cancelled']))
        )
        """,
    ),

    (
        "Create table: tags",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.tags (
            id                      bigserial              PRIMARY KEY,
            tag                     ai_captioning.citext   NOT NULL,
            created_at              timestamptz            NOT NULL DEFAULT now(),
            exclude_from_performers bool                   NOT NULL DEFAULT false,
            CONSTRAINT tags_tag_key UNIQUE (tag)
        )
        """,
    ),

    # 5. images (FK → galleries)

    (
        "Create table: images",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.images (
            id              bigserial   PRIMARY KEY,
            identifier      uuid        NOT NULL,
            filename        text        NOT NULL,
            path            text        NOT NULL,
            gallery_id      int8,
            sha256          text,
            file_size_bytes int8,
            width           int4,
            height          int4,
            mime_type       text,
            created_at      timestamptz NOT NULL DEFAULT now(),
            updated_at      timestamptz NOT NULL DEFAULT now(),
            zip_source      text,
            CONSTRAINT images_identifier_key UNIQUE (identifier),
            CONSTRAINT images_path_key       UNIQUE (path),
            CONSTRAINT images_sha256_key     UNIQUE (sha256),
            CONSTRAINT images_gallery_id_fkey
                FOREIGN KEY (gallery_id) REFERENCES ai_captioning.galleries(id)
                ON DELETE SET NULL
        )
        """,
    ),
    (
        "Index: images_filename_idx",
        "CREATE INDEX IF NOT EXISTS images_filename_idx   ON ai_captioning.images (filename)",
    ),
    (
        "Index: images_gallery_id_idx",
        "CREATE INDEX IF NOT EXISTS images_gallery_id_idx ON ai_captioning.images (gallery_id)",
    ),
    (
        "Index: images_zip_source_idx",
        "CREATE INDEX IF NOT EXISTS images_zip_source_idx ON ai_captioning.images (zip_source) WHERE zip_source IS NOT NULL",
    ),
    (
        "Trigger: trg_images_set_updated_at",
        """
        CREATE OR REPLACE TRIGGER trg_images_set_updated_at
        BEFORE UPDATE ON ai_captioning.images
        FOR EACH ROW EXECUTE FUNCTION ai_captioning.ai_captioning_set_updated_at()
        """,
    ),

    # 6. Tables with FKs to base tables

    (
        "Create table: performer_tags",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.performer_tags (
            performer_id   int4        NOT NULL,
            tag_id         int4        NOT NULL,
            image_count    int4        NOT NULL DEFAULT 0,
            total_images   int4        NOT NULL DEFAULT 0,
            assigned_at    timestamptz NOT NULL DEFAULT now(),
            pinned         bool        NOT NULL DEFAULT false,
            excluded       bool        NOT NULL DEFAULT false,
            manually_added bool        NOT NULL DEFAULT false,
            PRIMARY KEY (performer_id, tag_id),
            CONSTRAINT performer_tags_performer_id_fkey
                FOREIGN KEY (performer_id) REFERENCES ai_captioning.performers(id) ON DELETE CASCADE,
            CONSTRAINT performer_tags_tag_id_fkey
                FOREIGN KEY (tag_id) REFERENCES ai_captioning.tags(id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: performer_tags_tag_idx",
        "CREATE INDEX IF NOT EXISTS performer_tags_tag_idx ON ai_captioning.performer_tags (tag_id)",
    ),

    (
        "Create table: studio_galleries",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.studio_galleries (
            studio_id  int4 NOT NULL,
            gallery_id int8 NOT NULL,
            PRIMARY KEY (studio_id, gallery_id),
            CONSTRAINT studio_galleries_studio_id_fkey
                FOREIGN KEY (studio_id)  REFERENCES ai_captioning.studios(id)   ON DELETE CASCADE,
            CONSTRAINT studio_galleries_gallery_id_fkey
                FOREIGN KEY (gallery_id) REFERENCES ai_captioning.galleries(id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: studio_galleries_gallery_idx",
        "CREATE INDEX IF NOT EXISTS studio_galleries_gallery_idx ON ai_captioning.studio_galleries (gallery_id)",
    ),

    (
        "Create table: studio_images",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.studio_images (
            studio_id int4 NOT NULL,
            image_id  int8 NOT NULL,
            PRIMARY KEY (studio_id, image_id),
            CONSTRAINT studio_images_studio_id_fkey
                FOREIGN KEY (studio_id) REFERENCES ai_captioning.studios(id) ON DELETE CASCADE,
            CONSTRAINT studio_images_image_id_fkey
                FOREIGN KEY (image_id)  REFERENCES ai_captioning.images(id)  ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: studio_images_image_idx",
        "CREATE INDEX IF NOT EXISTS studio_images_image_idx ON ai_captioning.studio_images (image_id)",
    ),

    (
        "Create table: tag_aliases",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.tag_aliases (
            id         bigserial             PRIMARY KEY,
            tag_id     int8                  NOT NULL,
            alias      ai_captioning.citext  NOT NULL,
            created_at timestamptz           NOT NULL DEFAULT now(),
            CONSTRAINT tag_aliases_alias_key UNIQUE (alias),
            CONSTRAINT tag_aliases_tag_id_fkey
                FOREIGN KEY (tag_id) REFERENCES ai_captioning.tags(id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: tag_aliases_tag_id_idx",
        "CREATE INDEX IF NOT EXISTS tag_aliases_tag_id_idx ON ai_captioning.tag_aliases (tag_id)",
    ),

    (
        "Create table: image_descriptions",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_descriptions (
            image_id      int8        NOT NULL PRIMARY KEY,
            description   text        NOT NULL,
            tagger_run_id int8,
            created_at    timestamptz NOT NULL DEFAULT now(),
            updated_at    timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT image_descriptions_image_id_fkey
                FOREIGN KEY (image_id) REFERENCES ai_captioning.images(id) ON DELETE CASCADE,
            CONSTRAINT image_descriptions_tagger_run_id_fkey
                FOREIGN KEY (tagger_run_id) REFERENCES ai_captioning.tagger_runs(id) ON DELETE SET NULL
        )
        """,
    ),
    (
        "Trigger: trg_descriptions_set_updated_at",
        """
        CREATE OR REPLACE TRIGGER trg_descriptions_set_updated_at
        BEFORE UPDATE ON ai_captioning.image_descriptions
        FOR EACH ROW EXECUTE FUNCTION ai_captioning.ai_captioning_set_updated_at()
        """,
    ),

    (
        "Create table: image_keywords",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_keywords (
            image_id      int8        NOT NULL,
            tag_id        int8        NOT NULL,
            tagger_run_id int8        NOT NULL,
            created_at    timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (image_id, tag_id, tagger_run_id),
            CONSTRAINT image_keywords_image_id_fkey
                FOREIGN KEY (image_id)      REFERENCES ai_captioning.images(id)      ON DELETE CASCADE,
            CONSTRAINT image_keywords_tag_id_fkey
                FOREIGN KEY (tag_id)        REFERENCES ai_captioning.tags(id)        ON DELETE RESTRICT,
            CONSTRAINT image_keywords_tagger_run_id_fkey
                FOREIGN KEY (tagger_run_id) REFERENCES ai_captioning.tagger_runs(id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: image_keywords_tag_id_idx",
        "CREATE INDEX IF NOT EXISTS image_keywords_tag_id_idx ON ai_captioning.image_keywords (tag_id)",
    ),

    (
        "Create table: image_keywords_raw",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_keywords_raw (
            image_id      int8                 NOT NULL,
            tagger_run_id int8                 NOT NULL,
            keyword       ai_captioning.citext NOT NULL,
            created_at    timestamptz          NOT NULL DEFAULT now(),
            PRIMARY KEY (image_id, tagger_run_id, keyword),
            CONSTRAINT image_keywords_raw_image_id_fkey
                FOREIGN KEY (image_id)      REFERENCES ai_captioning.images(id)      ON DELETE CASCADE,
            CONSTRAINT image_keywords_raw_tagger_run_id_fkey
                FOREIGN KEY (tagger_run_id) REFERENCES ai_captioning.tagger_runs(id) ON DELETE CASCADE
        )
        """,
    ),

    (
        "Create table: image_keywords_unmatched",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_keywords_unmatched (
            image_id      int8                 NOT NULL,
            tagger_run_id int8                 NOT NULL,
            keyword       ai_captioning.citext NOT NULL,
            created_at    timestamptz          NOT NULL DEFAULT now(),
            PRIMARY KEY (image_id, tagger_run_id, keyword),
            CONSTRAINT image_keywords_unmatched_image_id_fkey
                FOREIGN KEY (image_id)      REFERENCES ai_captioning.images(id)      ON DELETE CASCADE,
            CONSTRAINT image_keywords_unmatched_tagger_run_id_fkey
                FOREIGN KEY (tagger_run_id) REFERENCES ai_captioning.tagger_runs(id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "Index: image_keywords_unmatched_keyword_idx",
        "CREATE INDEX IF NOT EXISTS image_keywords_unmatched_keyword_idx ON ai_captioning.image_keywords_unmatched (keyword)",
    ),

    (
        "Create table: image_performers",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_performers (
            image_id     int8        NOT NULL,
            performer_id int8        NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (image_id, performer_id),
            CONSTRAINT image_performers_image_id_fkey
                FOREIGN KEY (image_id)     REFERENCES ai_captioning.images(id)     ON DELETE CASCADE,
            CONSTRAINT image_performers_performer_id_fkey
                FOREIGN KEY (performer_id) REFERENCES ai_captioning.performers(id) ON DELETE RESTRICT
        )
        """,
    ),
    (
        "Index: image_performers_performer_id_idx",
        "CREATE INDEX IF NOT EXISTS image_performers_performer_id_idx ON ai_captioning.image_performers (performer_id)",
    ),

    (
        "Create table: image_run_status",
        """
        CREATE TABLE IF NOT EXISTS ai_captioning.image_run_status (
            image_id      int8        NOT NULL,
            tagger_run_id int8        NOT NULL,
            status        text        NOT NULL,
            processed_at  timestamptz NOT NULL DEFAULT now(),
            error_message text,
            PRIMARY KEY (image_id, tagger_run_id),
            CONSTRAINT image_run_status_status_check
                CHECK (status = ANY (ARRAY['success','failed','skipped'])),
            CONSTRAINT image_run_status_image_id_fkey
                FOREIGN KEY (image_id)      REFERENCES ai_captioning.images(id)      ON DELETE CASCADE,
            CONSTRAINT image_run_status_tagger_run_id_fkey
                FOREIGN KEY (tagger_run_id) REFERENCES ai_captioning.tagger_runs(id) ON DELETE CASCADE
        )
        """,
    ),

    # 7. Views

    (
        "Create view: v_image_latest_status",
        """
        CREATE OR REPLACE VIEW ai_captioning.v_image_latest_status AS
        SELECT DISTINCT ON (irs.image_id)
            i.id          AS image_id,
            i.path,
            i.filename,
            g.name        AS gallery_name,
            irs.status,
            irs.processed_at,
            irs.error_message,
            tr.tagger_name,
            tr.id         AS tagger_run_id
        FROM       ai_captioning.image_run_status irs
        JOIN       ai_captioning.images       i  ON i.id  = irs.image_id
        JOIN       ai_captioning.tagger_runs  tr ON tr.id = irs.tagger_run_id
        LEFT JOIN  ai_captioning.galleries    g  ON g.id  = i.gallery_id
        ORDER BY irs.image_id, irs.processed_at DESC
        """,
    ),

    (
        "Create view: v_image_tags_latest",
        """
        CREATE OR REPLACE VIEW ai_captioning.v_image_tags_latest AS
        SELECT DISTINCT ON (ik.image_id, ik.tag_id)
            i.path,
            i.filename,
            g.name AS gallery_name,
            t.tag,
            ik.tagger_run_id
        FROM       ai_captioning.image_keywords ik
        JOIN       ai_captioning.images         i  ON i.id = ik.image_id
        JOIN       ai_captioning.tags           t  ON t.id = ik.tag_id
        LEFT JOIN  ai_captioning.galleries      g  ON g.id = i.gallery_id
        ORDER BY ik.image_id, ik.tag_id, ik.tagger_run_id DESC
        """,
    ),

    (
        "Create view: v_top_unmatched_keywords",
        """
        CREATE OR REPLACE VIEW ai_captioning.v_top_unmatched_keywords AS
        SELECT
            keyword,
            count(DISTINCT image_id) AS image_count,
            count(*)                 AS total_occurrences,
            max(created_at)          AS last_seen
        FROM  ai_captioning.image_keywords_unmatched
        GROUP BY keyword
        ORDER BY count(DISTINCT image_id) DESC, count(*) DESC
        """,
    ),
]


# ── Connection helpers ────────────────────────────────────────────────────────

def _load_settings_defaults():
    """Read db_* keys from settings.json next to this script, if present."""
    settings_path = Path(__file__).parent / "settings.json"
    if settings_path.exists():
        try:
            with open(settings_path) as f:
                s = json.load(f)
            return {
                "host":     s.get("db_host",     "localhost"),
                "port":     s.get("db_port",     5432),
                "user":     s.get("db_user",     ""),
                "password": s.get("db_password", ""),
                "dbname":   s.get("db_name",     ""),
            }
        except Exception:
            pass
    return {"host": "localhost", "port": 5432, "user": "", "password": "", "dbname": ""}


def _parse_args():
    defaults = _load_settings_defaults()
    p = argparse.ArgumentParser(description="Initialize the ai_captioning PostgreSQL schema.")
    p.add_argument("--host",     default=defaults["host"],     help="DB host     (default: %(default)s)")
    p.add_argument("--port",     default=defaults["port"],     type=int, help="DB port (default: %(default)s)")
    p.add_argument("--user",     default=defaults["user"],     help="DB user     (default: %(default)s)")
    p.add_argument("--password", default=defaults["password"], help="DB password (default: from settings)")
    p.add_argument("--dbname",   default=defaults["dbname"],   help="DB name     (default: %(default)s)")
    return p.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = _parse_args()

    if not args.dbname:
        print("ERROR: --dbname is required (or set db_name in settings.json).")
        sys.exit(1)

    print(f"Connecting to {args.user}@{args.host}:{args.port}/{args.dbname} …")
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.dbname,
        )
        conn.autocommit = False
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect — {e}")
        sys.exit(1)

    print("Connected. Running DDL …\n")
    errors = 0
    with conn:
        with conn.cursor() as cur:
            for label, sql in _DDL:
                try:
                    cur.execute(sql)
                    print(f"  OK  {label}")
                except psycopg2.Error as e:
                    print(f"  FAIL {label}")
                    print(f"       {e.pgcode}: {e.pgerror.strip() if e.pgerror else e}")
                    conn.rollback()
                    errors += 1
                    # Re-open transaction so remaining steps can still run
                    cur.execute("BEGIN")

    conn.close()

    print()
    if errors == 0:
        print("Database initialized successfully.")
    else:
        print(f"Completed with {errors} error(s). Review output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
