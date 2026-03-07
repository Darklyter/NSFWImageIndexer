#!/usr/bin/env python3
"""
Tag Review Tool

Presents unmatched keywords from ai_captioning.image_keywords_unmatched
in order of occurrence count.  For each keyword you can:

  - Filter the tag list and press Enter when only one match remains
      → automatically assigns that tag
  - Pick a canonical tag and click Assign (or double-click)
      → adds the keyword as an alias in tag_aliases
      → moves all matching rows from image_keywords_unmatched to image_keywords
  - Click Skip to leave the keyword untouched and move on
  - Set a minimum occurrence threshold to skip low-frequency keywords
  - Open "View All Keywords" to browse the full sortable list

Run from the project root:
    python tag_review.py
"""

import sys
import json
import string
from pathlib import Path

try:
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QDialog,
        QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QListWidget, QListWidgetItem, QPushButton,
        QSpinBox, QFrame, QMessageBox, QSizePolicy,
        QTableWidget, QTableWidgetItem, QHeaderView,
        QDialogButtonBox, QInputDialog,
    )
    from PyQt6.QtCore import Qt, QEvent
    from PyQt6.QtGui import QPixmap, QFont
except ImportError:
    print("PyQt6 is required.  Run: pip install PyQt6")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("psycopg2 is required.  Run: pip install psycopg2-binary")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SETTINGS_PATH = Path(__file__).resolve().parent / "settings.json"
_SCHEMA = "ai_captioning"

# Import llmii_db from src/ for merge/rename/import operations
_SRC_DIR = Path(__file__).resolve().parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
try:
    import llmii_db as _llmii_db
    _HAS_LLMII_DB = True
except ImportError:
    _llmii_db = None
    _HAS_LLMII_DB = False


def _load_settings():
    if _SETTINGS_PATH.exists():
        with open(_SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _connect(settings):
    return psycopg2.connect(
        host=settings.get("db_host", "localhost"),
        port=int(settings.get("db_port", 5432)),
        user=settings.get("db_user", ""),
        password=settings.get("db_password", ""),
        dbname=settings.get("db_name", ""),
        options=f"-c search_path={_SCHEMA},public",
    )


# ---------------------------------------------------------------------------
# Full keyword list dialog
# ---------------------------------------------------------------------------

class _NumericItem(QTableWidgetItem):
    """QTableWidgetItem that sorts numerically instead of lexicographically."""

    def __lt__(self, other):
        try:
            return int(self.data(Qt.ItemDataRole.UserRole)) < int(
                other.data(Qt.ItemDataRole.UserRole)
            )
        except (TypeError, ValueError):
            return super().__lt__(other)


class AllKeywordsDialog(QDialog):
    """Non-modal window showing all unmatched keywords with sortable counts."""

    def __init__(self, all_keywords, parent=None):
        super().__init__(parent)
        self.setWindowTitle("All Unmatched Keywords")
        self.setMinimumSize(520, 620)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)

        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        info = QLabel(f"{len(all_keywords)} unique unmatched keyword(s)  —  click a column header to sort")
        info.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(info)

        self.table = QTableWidget(len(all_keywords), 2)
        self.table.setHorizontalHeaderLabels(["Keyword", "Occurrences"])
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch
        )
        self.table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        # Default: sort by occurrences descending
        self.table.horizontalHeader().setSortIndicator(1, Qt.SortOrder.DescendingOrder)

        for row, (keyword, count) in enumerate(all_keywords):
            kw_item = QTableWidgetItem(keyword)
            kw_item.setFlags(kw_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            cnt_item = _NumericItem(str(count))
            cnt_item.setData(Qt.ItemDataRole.UserRole, count)
            cnt_item.setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            )
            cnt_item.setFlags(cnt_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            self.table.setItem(row, 0, kw_item)
            self.table.setItem(row, 1, cnt_item)

        layout.addWidget(self.table)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        layout.addWidget(close_btn)


# ---------------------------------------------------------------------------
# Bulk assign dialog
# ---------------------------------------------------------------------------

class BulkAssignDialog(QDialog):
    """Filter unmatched keywords by substring, check-select any subset,
    then assign them all to one canonical tag in a single operation.

    Each assignment:
      - adds the keyword as an alias in tag_aliases (ON CONFLICT DO NOTHING)
      - moves all matching image_keywords_unmatched rows into image_keywords
      - deletes those rows from image_keywords_unmatched

    After a successful assign the dialog refreshes its own keyword list and
    emits ``keywords_assigned(list)`` so the parent window can drop those
    keywords from its own in-memory lists.
    """

    def __init__(self, all_keywords, all_tags, conn, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.all_keywords = list(all_keywords)   # [(keyword, count), …]
        self.all_tags = list(all_tags)            # [tag_str, …]
        self._filtered = []                       # currently visible rows

        self.setWindowTitle("Bulk Assign Keywords")
        self.setMinimumSize(1000, 640)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
        self._build_ui()
        self._filter_keywords()

    def _build_ui(self):
        from PyQt6.QtWidgets import QGroupBox
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # ── Two-column body ──────────────────────────────────────────────
        mid = QHBoxLayout()
        mid.setSpacing(8)

        # ── LEFT: unmatched keyword list ─────────────────────────────────
        kw_group = QGroupBox("Step 1 — Filter & select unmatched keywords")
        kw_group_layout = QVBoxLayout(kw_group)
        kw_group_layout.setSpacing(5)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Filter:"))

        self.kw_filter = QLineEdit()
        self.kw_filter.setPlaceholderText("Type a substring — e.g. tattoo, hair, blue…")
        self.kw_filter.textChanged.connect(self._filter_keywords)
        filter_row.addWidget(self.kw_filter, stretch=1)

        self.kw_count_label = QLabel("")
        self.kw_count_label.setStyleSheet("color: #888; font-size: 11px; min-width: 100px;")
        filter_row.addWidget(self.kw_count_label)

        kw_group_layout.addLayout(filter_row)

        sel_row = QHBoxLayout()
        sel_all_btn = QPushButton("Select All")
        sel_all_btn.setToolTip("Check every keyword currently visible in the list")
        sel_all_btn.clicked.connect(self._select_all)
        sel_row.addWidget(sel_all_btn)

        desel_btn = QPushButton("Deselect All")
        desel_btn.setToolTip("Uncheck every keyword currently visible in the list")
        desel_btn.clicked.connect(self._deselect_all)
        sel_row.addWidget(desel_btn)
        sel_row.addStretch()

        kw_group_layout.addLayout(sel_row)

        self.kw_list = QListWidget()
        self.kw_list.setSelectionMode(QListWidget.SelectionMode.NoSelection)
        self.kw_list.setAlternatingRowColors(True)
        self.kw_list.itemChanged.connect(self._on_check_changed)
        kw_group_layout.addWidget(self.kw_list, stretch=1)

        mid.addWidget(kw_group, stretch=3)

        # ── RIGHT: tag picker ────────────────────────────────────────────
        tag_group = QGroupBox("Step 2 — Pick the canonical tag to assign to")
        tag_group_layout = QVBoxLayout(tag_group)
        tag_group_layout.setSpacing(5)

        self.tag_search = QLineEdit()
        self.tag_search.setPlaceholderText("Filter tags… (↑↓ to navigate, Enter to assign)")
        self.tag_search.textChanged.connect(self._filter_tags)
        self.tag_search.returnPressed.connect(self._on_tag_search_enter)
        tag_group_layout.addWidget(self.tag_search)

        self.tag_list = QListWidget()
        self.tag_list.setAlternatingRowColors(True)
        self.tag_list.setStyleSheet("""
            QListWidget::item:selected { background: #1a6fa8; color: white; }
            QListWidget::item:selected:!active { background: #1a6fa8; color: white; }
        """)
        self.tag_list.itemSelectionChanged.connect(self._on_tag_selection)
        self.tag_list.itemDoubleClicked.connect(lambda _: self._assign_selected())
        tag_group_layout.addWidget(self.tag_list, stretch=1)

        self.sel_tag_label = QLabel("No tag selected")
        self.sel_tag_label.setStyleSheet("color: #888;")
        tag_group_layout.addWidget(self.sel_tag_label)

        mid.addWidget(tag_group, stretch=2)
        layout.addLayout(mid, stretch=1)

        # ── Bottom: status + buttons ─────────────────────────────────────
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(sep)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self.status_label)

        btn_row = QHBoxLayout()

        self.assign_btn = QPushButton("Assign Selected → Tag")
        self.assign_btn.setEnabled(False)
        self.assign_btn.setMinimumHeight(42)
        self.assign_btn.setStyleSheet(
            "QPushButton:enabled { background: #2a6e2a; color: white; font-weight: bold; }"
        )
        self.assign_btn.clicked.connect(self._assign_selected)
        btn_row.addWidget(self.assign_btn)

        close_btn = QPushButton("Close")
        close_btn.setMinimumHeight(42)
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(close_btn)

        layout.addLayout(btn_row)

        # Populate tag list
        self._repopulate_tags(self.all_tags)

        # Arrow-key navigation from tag search box
        self.tag_search.installEventFilter(self)

    # ------------------------------------------------------------------
    # Event filter — arrow keys in tag_search navigate tag_list
    # ------------------------------------------------------------------

    def eventFilter(self, obj, event):
        if obj is self.tag_search and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            count = self.tag_list.count()
            if count == 0:
                return super().eventFilter(obj, event)
            if key == Qt.Key.Key_Down:
                self.tag_list.setCurrentRow(min(self.tag_list.currentRow() + 1, count - 1))
                return True
            elif key == Qt.Key.Key_Up:
                self.tag_list.setCurrentRow(max(self.tag_list.currentRow() - 1, 0))
                return True
        return super().eventFilter(obj, event)

    # ------------------------------------------------------------------
    # Keyword list management
    # ------------------------------------------------------------------

    def _filter_keywords(self, text=None):
        if text is None:
            text = self.kw_filter.text()
        needle = text.strip().lower()
        self._filtered = [
            (kw, cnt) for kw, cnt in self.all_keywords
            if not needle or needle in kw.lower()
        ]
        self.kw_list.blockSignals(True)
        self.kw_list.clear()
        for kw, cnt in self._filtered:
            item = QListWidgetItem(f"{kw}  ({cnt})")
            item.setData(Qt.ItemDataRole.UserRole, kw)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)
            self.kw_list.addItem(item)
        self.kw_list.blockSignals(False)
        n = len(self._filtered)
        self.kw_count_label.setText(f"{n} keyword{'s' if n != 1 else ''} shown")
        self._update_assign_btn()

    def _select_all(self):
        self.kw_list.blockSignals(True)
        for i in range(self.kw_list.count()):
            self.kw_list.item(i).setCheckState(Qt.CheckState.Checked)
        self.kw_list.blockSignals(False)
        self._update_assign_btn()

    def _deselect_all(self):
        self.kw_list.blockSignals(True)
        for i in range(self.kw_list.count()):
            self.kw_list.item(i).setCheckState(Qt.CheckState.Unchecked)
        self.kw_list.blockSignals(False)
        self._update_assign_btn()

    def _on_check_changed(self, _item):
        self._update_assign_btn()

    def _checked_keywords(self):
        return [
            self.kw_list.item(i).data(Qt.ItemDataRole.UserRole)
            for i in range(self.kw_list.count())
            if self.kw_list.item(i).checkState() == Qt.CheckState.Checked
        ]

    # ------------------------------------------------------------------
    # Tag picker
    # ------------------------------------------------------------------

    def _repopulate_tags(self, tags):
        self.tag_list.clear()
        for t in tags:
            self.tag_list.addItem(t)

    def _filter_tags(self, text):
        needle = text.strip().lower()
        filtered = (
            sorted([t for t in self.all_tags if needle in t.lower()], key=str.lower)
            if needle else self.all_tags
        )
        self._repopulate_tags(filtered)
        if len(filtered) == 1:
            self.tag_list.setCurrentRow(0)

    def _on_tag_search_enter(self):
        if self.tag_list.currentRow() >= 0:
            self._assign_selected()
        elif self.tag_list.count() == 1:
            self.tag_list.setCurrentRow(0)
            self._assign_selected()

    def _on_tag_selection(self):
        items = self.tag_list.selectedItems()
        if items:
            self.sel_tag_label.setText(f"Selected: {items[0].text()}")
            self.sel_tag_label.setStyleSheet("color: #3a9e3a; font-weight: bold;")
        else:
            self.sel_tag_label.setText("No tag selected")
            self.sel_tag_label.setStyleSheet("color: #888;")
        self._update_assign_btn()

    def _update_assign_btn(self):
        checked = self._checked_keywords()
        tag_items = self.tag_list.selectedItems()
        enabled = bool(checked) and bool(tag_items)
        self.assign_btn.setEnabled(enabled)
        if checked and tag_items:
            n = len(checked)
            tag = tag_items[0].text()
            self.assign_btn.setText(
                f"Assign {n} keyword{'s' if n != 1 else ''} → {tag}"
            )
        else:
            self.assign_btn.setText("Assign Selected → Tag")

    # ------------------------------------------------------------------
    # Assignment
    # ------------------------------------------------------------------

    def _assign_selected(self):
        tag_items = self.tag_list.selectedItems()
        if not tag_items:
            return
        tag = tag_items[0].text()
        keywords = self._checked_keywords()
        if not keywords:
            return

        total_moved = 0
        assigned = []
        failed = []

        cur = self.conn.cursor()
        try:
            cur.execute("SELECT id FROM tags WHERE tag = %s", (tag,))
            row = cur.fetchone()
            if not row:
                self.conn.rollback()
                QMessageBox.warning(self, "Error", f"Tag '{tag}' not found.")
                return
            tag_id = row[0]

            for keyword in keywords:
                try:
                    # Alias: keyword → tag  (skip if already aliased to this tag)
                    cur.execute("""
                        INSERT INTO tag_aliases (tag_id, alias)
                        VALUES (%s, %s)
                        ON CONFLICT (alias) DO NOTHING
                    """, (tag_id, keyword))

                    # Promote unmatched rows to image_keywords
                    cur.execute("""
                        INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                        SELECT iku.image_id, %s, iku.tagger_run_id
                        FROM image_keywords_unmatched iku
                        WHERE iku.keyword = %s
                        ON CONFLICT DO NOTHING
                    """, (tag_id, keyword))
                    total_moved += cur.rowcount

                    # Remove from unmatched
                    cur.execute(
                        "DELETE FROM image_keywords_unmatched WHERE keyword = %s",
                        (keyword,),
                    )
                    assigned.append(keyword)

                except Exception as e:
                    self.conn.rollback()
                    failed.append(f"{keyword}: {e}")
                    # Re-open transaction for next keyword
                    cur = self.conn.cursor()

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(self, "Database Error", str(e))
            return
        finally:
            cur.close()

        n = len(assigned)
        msg = (
            f"Assigned {n} keyword{'s' if n != 1 else ''} → '{tag}', "
            f"{total_moved} image row{'s' if total_moved != 1 else ''} updated."
        )
        if failed:
            msg += f"  {len(failed)} failed: {'; '.join(failed)}"
        self.status_label.setText(msg)

        # Drop assigned keywords from local lists and refresh display
        assigned_set = set(assigned)
        self.all_keywords = [(k, c) for k, c in self.all_keywords if k not in assigned_set]
        self._filter_keywords()

        # Notify parent window so its queue stays in sync
        parent = self.parent()
        if parent is not None and hasattr(parent, '_remove_keywords'):
            parent._remove_keywords(assigned)

    # ------------------------------------------------------------------
    # Public update method (called by parent when it assigns a keyword
    # individually so the bulk dialog doesn't show stale data)
    # ------------------------------------------------------------------

    def remove_keywords(self, keywords):
        """Drop keywords from the local list (parent-driven sync)."""
        s = set(keywords)
        self.all_keywords = [(k, c) for k, c in self.all_keywords if k not in s]
        self._filter_keywords()


# ---------------------------------------------------------------------------
# Tag management dialog (rename, merge, import vocabulary)
# ---------------------------------------------------------------------------

class ManageTagsDialog(QDialog):
    """Dialog for renaming canonical tags, merging one tag into another,
    importing a tag vocabulary JSON file, and managing individual aliases.

    Left panel: searchable tag list showing image-count per tag.
    Right panel: alias list for the selected tag with individual delete support.

    After any operation that changes the tag list (rename / merge / import)
    the parent window's ``_update_tags_after_manage`` method is called so
    that the main tag picker and BulkAssignDialog stay in sync.
    """

    def __init__(self, all_tags, conn, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.all_tags = list(all_tags)
        self._usage = {}    # tag_name → image count
        self._aliases = {}  # tag_name → [alias_text, ...]
        self.setWindowTitle("Manage Tags")
        self.setMinimumSize(780, 560)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
        self._load_tag_data()
        self._build_ui()
        self._repopulate(self.all_tags)

    def _load_tag_data(self):
        """Load per-tag image counts and alias lists from the database."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT t.tag, COUNT(DISTINCT ik.image_id)
                FROM tags t
                LEFT JOIN image_keywords ik ON ik.tag_id = t.id
                GROUP BY t.tag
            """)
            self._usage = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute("""
                SELECT t.tag, a.alias
                FROM tag_aliases a
                JOIN tags t ON t.id = a.tag_id
                ORDER BY t.tag, a.alias
            """)
            self._aliases = {}
            for tag, alias in cur.fetchall():
                self._aliases.setdefault(tag, []).append(alias)
        self.conn.commit()

    def _build_ui(self):
        outer = QHBoxLayout(self)
        outer.setSpacing(0)
        outer.setContentsMargins(8, 8, 8, 8)

        # ── LEFT: tag list + actions ────────────────────────────────────
        left_widget = QWidget()
        left = QVBoxLayout(left_widget)
        left.setSpacing(6)

        self.count_label = QLabel(f"{len(self.all_tags)} canonical tag(s)")
        self.count_label.setStyleSheet("color: #888; font-size: 11px;")
        left.addWidget(self.count_label)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Filter tags…")
        self.search.textChanged.connect(self._filter)
        left.addWidget(self.search)

        self.tag_list = QListWidget()
        self.tag_list.setAlternatingRowColors(True)
        self.tag_list.setStyleSheet("""
            QListWidget::item:selected { background: #1a6fa8; color: white; }
            QListWidget::item:selected:!active { background: #1a6fa8; color: white; }
        """)
        self.tag_list.itemSelectionChanged.connect(self._on_selection)
        left.addWidget(self.tag_list, stretch=1)

        self.sel_label = QLabel("No tag selected")
        self.sel_label.setStyleSheet("color: #888;")
        left.addWidget(self.sel_label)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        left.addWidget(sep)

        btn_row = QHBoxLayout()

        self.rename_btn = QPushButton("Rename…")
        self.rename_btn.setEnabled(False)
        self.rename_btn.setToolTip("Rename the selected canonical tag")
        self.rename_btn.clicked.connect(self._rename)
        btn_row.addWidget(self.rename_btn)

        self.merge_btn = QPushButton("Merge into…")
        self.merge_btn.setEnabled(False)
        self.merge_btn.setToolTip(
            "Merge the selected tag into another — reassigns all image keywords "
            "and aliases, then adds the source name as an alias for the target"
        )
        self.merge_btn.clicked.connect(self._merge)
        btn_row.addWidget(self.merge_btn)

        import_btn = QPushButton("Import Vocab…")
        import_btn.setToolTip("Load tags and aliases from a JSON file (Tag/Alias format)")
        import_btn.clicked.connect(self._import)
        btn_row.addWidget(import_btn)

        left.addLayout(btn_row)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        left.addWidget(close_btn)

        outer.addWidget(left_widget, stretch=3)

        # Vertical divider
        vdiv = QFrame()
        vdiv.setFrameShape(QFrame.Shape.VLine)
        vdiv.setFrameShadow(QFrame.Shadow.Sunken)
        outer.addWidget(vdiv)

        # ── RIGHT: alias viewer ─────────────────────────────────────────
        right_widget = QWidget()
        right = QVBoxLayout(right_widget)
        right.setSpacing(6)
        right.setContentsMargins(8, 0, 0, 0)

        self.alias_header = QLabel("Aliases")
        self.alias_header.setStyleSheet("color: #888; font-size: 11px;")
        right.addWidget(self.alias_header)

        self.alias_list = QListWidget()
        self.alias_list.setAlternatingRowColors(True)
        self.alias_list.itemSelectionChanged.connect(self._on_alias_selection)
        right.addWidget(self.alias_list, stretch=1)

        self.del_alias_btn = QPushButton("Delete Alias")
        self.del_alias_btn.setEnabled(False)
        self.del_alias_btn.setToolTip(
            "Remove the selected alias mapping — image keywords already promoted "
            "are NOT removed, only the alias rule is deleted"
        )
        self.del_alias_btn.clicked.connect(self._delete_alias)
        right.addWidget(self.del_alias_btn)

        outer.addWidget(right_widget, stretch=2)

    # ------------------------------------------------------------------
    # Tag list helpers
    # ------------------------------------------------------------------

    def _repopulate(self, tags):
        self.tag_list.clear()
        for t in tags:
            count = self._usage.get(t, 0)
            item = QListWidgetItem(f"{t}  ({count})")
            item.setData(Qt.ItemDataRole.UserRole, t)
            self.tag_list.addItem(item)
        self.count_label.setText(f"{len(self.all_tags)} canonical tag(s)")

    def _filter(self, text=None):
        if text is None:
            text = self.search.text()
        needle = text.strip().lower()
        filtered = (
            sorted([t for t in self.all_tags if needle in t.lower()], key=str.lower)
            if needle else self.all_tags
        )
        self._repopulate(filtered)

    def _on_selection(self):
        tag = self._selected_tag()
        if tag:
            count = self._usage.get(tag, 0)
            self.sel_label.setText(f"Selected: {tag}  \u2022  {count} image(s)")
            self.sel_label.setStyleSheet("color: #3a9e3a; font-weight: bold;")
            self.rename_btn.setEnabled(True)
            self.merge_btn.setEnabled(True)
            self._update_alias_list(tag)
        else:
            self.sel_label.setText("No tag selected")
            self.sel_label.setStyleSheet("color: #888;")
            self.rename_btn.setEnabled(False)
            self.merge_btn.setEnabled(False)
            self._update_alias_list(None)

    def _on_alias_selection(self):
        self.del_alias_btn.setEnabled(bool(self.alias_list.selectedItems()))

    def _update_alias_list(self, tag):
        self.alias_list.clear()
        self.del_alias_btn.setEnabled(False)
        if not tag:
            self.alias_header.setText("Aliases")
            return
        aliases = self._aliases.get(tag, [])
        self.alias_header.setText(f"Aliases for '{tag}'  ({len(aliases)})")
        for a in sorted(aliases, key=str.lower):
            self.alias_list.addItem(a)

    def _selected_tag(self):
        items = self.tag_list.selectedItems()
        return items[0].data(Qt.ItemDataRole.UserRole) if items else None

    def _notify_parent(self):
        parent = self.parent()
        if parent is not None and hasattr(parent, "_update_tags_after_manage"):
            parent._update_tags_after_manage(self.all_tags)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _rename(self):
        if not _HAS_LLMII_DB:
            QMessageBox.critical(self, "Unavailable", "llmii_db module could not be imported.")
            return
        source = self._selected_tag()
        if not source:
            return

        new_name, ok = QInputDialog.getText(
            self, "Rename Tag", f"New name for '{source}':", text=source
        )
        if not ok:
            return
        new_name = string.capwords(new_name.strip())
        if not new_name or new_name.lower() == source.lower():
            return

        try:
            _llmii_db.rename_tag(self.conn, source, new_name)
        except Exception as e:
            QMessageBox.critical(self, "Rename Failed", str(e))
            return

        self.all_tags = sorted(
            [new_name if t == source else t for t in self.all_tags], key=str.lower
        )
        self._load_tag_data()
        self._filter()
        self._update_alias_list(new_name)
        self.sel_label.setText(f"Renamed '{source}' \u2192 '{new_name}'")
        self.rename_btn.setEnabled(False)
        self.merge_btn.setEnabled(False)
        self._notify_parent()

    def _merge(self):
        if not _HAS_LLMII_DB:
            QMessageBox.critical(self, "Unavailable", "llmii_db module could not be imported.")
            return
        source = self._selected_tag()
        if not source:
            return

        targets = [t for t in self.all_tags if t.lower() != source.lower()]
        if not targets:
            QMessageBox.information(self, "Merge", "No other tags to merge into.")
            return

        target, ok = QInputDialog.getItem(
            self, "Merge Tag", f"Merge '{source}' into:", targets, 0, False
        )
        if not ok or not target:
            return

        reply = QMessageBox.question(
            self,
            "Confirm Merge",
            f"Merge '{source}' → '{target}'?\n\n"
            f"This will:\n"
            f"  \u2022  Reassign all image keywords from '{source}' to '{target}'\n"
            f"  \u2022  Move all of '{source}' aliases to '{target}'\n"
            f"  \u2022  Add '{source}' as an alias for '{target}' (future lookups resolve correctly)\n"
            f"  \u2022  Delete the '{source}' tag permanently\n\n"
            f"This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            reassigned = _llmii_db.merge_tag(self.conn, source, target)
        except Exception as e:
            QMessageBox.critical(self, "Merge Failed", str(e))
            return

        QMessageBox.information(
            self,
            "Merge Complete",
            f"Merged '{source}' \u2192 '{target}'\n"
            f"{reassigned} image keyword row(s) reassigned.\n"
            f"'{source}' is now an alias for '{target}'.",
        )
        self.all_tags = [t for t in self.all_tags if t.lower() != source.lower()]
        self._load_tag_data()
        self._filter()
        self._update_alias_list(target)
        self.sel_label.setText(f"Merged '{source}' \u2192 '{target}'")
        self.rename_btn.setEnabled(False)
        self.merge_btn.setEnabled(False)
        self._notify_parent()

    def _import(self):
        if not _HAS_LLMII_DB:
            QMessageBox.critical(self, "Unavailable", "llmii_db module could not be imported.")
            return
        from PyQt6.QtWidgets import QFileDialog
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Tag Vocabulary", "", "JSON Files (*.json);;All Files (*)"
        )
        if not path:
            return

        try:
            stats = _llmii_db.load_tags_from_file(self.conn, path)
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", str(e))
            return

        QMessageBox.information(
            self,
            "Import Complete",
            f"File: {path}\n\n"
            f"Tags:    {stats['tags_added']} added,  {stats['tags_skipped']} already existed\n"
            f"Aliases: {stats['aliases_added']} added,  {stats['aliases_skipped']} already existed",
        )

        # Reload tag list from DB to pick up any new tags
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT tag FROM tags ORDER BY tag")
            self.all_tags = sorted([r[0] for r in cur.fetchall()], key=str.lower)
        finally:
            cur.close()
            self.conn.commit()
        self._load_tag_data()
        self._filter()
        self._notify_parent()

    def _delete_alias(self):
        tag = self._selected_tag()
        items = self.alias_list.selectedItems()
        if not tag or not items:
            return
        alias = items[0].text()

        reply = QMessageBox.question(
            self,
            "Delete Alias",
            f"Delete alias '{alias}' from tag '{tag}'?\n\n"
            f"Image keywords already promoted via this alias are NOT removed — "
            f"only the alias rule itself will be deleted.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM tag_aliases WHERE alias = %s", (alias,))
            self.conn.commit()
        except Exception as e:
            QMessageBox.critical(self, "Delete Failed", str(e))
            return

        if tag in self._aliases:
            self._aliases[tag] = [a for a in self._aliases[tag] if a != alias]
        self._update_alias_list(tag)
        self.sel_label.setText(f"Deleted alias '{alias}' from '{tag}'")
        self.del_alias_btn.setEnabled(False)


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class TagReviewWindow(QMainWindow):

    def __init__(self, conn, performer_filter=None):
        super().__init__()
        self.conn = conn
        self._performer_filter = performer_filter   # str or None
        self.all_keywords = []   # [(keyword, count), …] — full unfiltered list
        self.keywords = []       # [(keyword, count), …] — after min-count filter
        self.current_idx = 0
        self.all_tags = []       # canonical tag name strings
        self._cached_pixmap = None
        self._kw_dialog = None      # AllKeywordsDialog instance (kept open)
        self._bulk_dialog = None    # BulkAssignDialog instance (kept open)
        self._manage_dialog = None  # ManageTagsDialog instance (kept open)

        title = "Tag Review"
        if performer_filter:
            title += f" — {performer_filter}"
        self.setWindowTitle(title)
        self.setMinimumSize(1300, 750)
        self._build_ui()
        self._load_data()
        self._show_current()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        outer = QHBoxLayout(root)
        outer.setContentsMargins(10, 10, 10, 10)
        outer.setSpacing(10)

        # ── Left: keyword info + image ──────────────────────────────────
        left = QWidget()
        left_col = QVBoxLayout(left)
        left_col.setSpacing(6)

        f_big = QFont()
        f_big.setPointSize(18)
        f_big.setBold(True)

        self.kw_label = QLabel("Loading…")
        self.kw_label.setFont(f_big)
        self.kw_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left_col.addWidget(self.kw_label)

        self.count_label = QLabel()
        self.count_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left_col.addWidget(self.count_label)

        self.progress_label = QLabel()
        self.progress_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress_label.setStyleSheet("color: #888;")
        left_col.addWidget(self.progress_label)

        self.img_label = QLabel("Image cannot be loaded")
        self.img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.img_label.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        self.img_label.setStyleSheet(
            "background: #111; color: #666; border: 1px solid #333;"
        )
        left_col.addWidget(self.img_label, stretch=1)

        self.path_label = QLabel()
        self.path_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.path_label.setWordWrap(True)
        self.path_label.setStyleSheet("color: #555; font-size: 10px;")
        left_col.addWidget(self.path_label)

        self.caption_label = QLabel()
        self.caption_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.caption_label.setWordWrap(True)
        self.caption_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
        )
        self.caption_label.setStyleSheet(
            "color: #778; font-size: 10px; font-style: italic; "
            "background: #1a1a1a; padding: 4px; border-radius: 3px;"
        )
        self.caption_label.setMaximumHeight(80)
        left_col.addWidget(self.caption_label)

        outer.addWidget(left, stretch=3)

        # Vertical divider
        div = QFrame()
        div.setFrameShape(QFrame.Shape.VLine)
        div.setFrameShadow(QFrame.Shadow.Sunken)
        outer.addWidget(div)

        # ── Right: controls + tag picker + action buttons ───────────────
        right = QWidget()
        right_col = QVBoxLayout(right)
        right_col.setSpacing(6)

        # ── Min-occurrences filter row ──────────────────────────────────
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Min occurrences:"))

        self.min_spin = QSpinBox()
        self.min_spin.setRange(1, 99999)
        self.min_spin.setValue(1)
        self.min_spin.setFixedWidth(70)
        self.min_spin.setToolTip("Only show keywords with at least this many occurrences")
        filter_row.addWidget(self.min_spin)

        apply_btn = QPushButton("Apply")
        apply_btn.setToolTip("Restart the review queue with the new minimum")
        apply_btn.clicked.connect(self._apply_filter)
        filter_row.addWidget(apply_btn)

        filter_row.addStretch()

        view_all_btn = QPushButton("View All Keywords")
        view_all_btn.setToolTip("Show the full sortable list of unmatched keywords")
        view_all_btn.clicked.connect(self._show_all_keywords)
        filter_row.addWidget(view_all_btn)

        bulk_btn = QPushButton("Bulk Assign…")
        bulk_btn.setToolTip(
            "Filter keywords by substring, select multiple, and assign them all to one tag"
        )
        bulk_btn.clicked.connect(self._open_bulk_assign)
        filter_row.addWidget(bulk_btn)

        manage_btn = QPushButton("Manage Tags…")
        manage_btn.setToolTip("Rename or merge canonical tags, or import a tag vocabulary file")
        manage_btn.clicked.connect(self._open_manage_tags)
        filter_row.addWidget(manage_btn)

        right_col.addLayout(filter_row)

        # Thin separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        right_col.addWidget(sep)

        # ── Tag picker ──────────────────────────────────────────────────
        right_col.addWidget(QLabel("Select canonical tag:"))

        self.search = QLineEdit()
        self.search.setPlaceholderText("Filter tags… (Enter auto-assigns when 1 result)")
        self.search.textChanged.connect(self._filter_tags)
        self.search.returnPressed.connect(self._on_search_enter)
        right_col.addWidget(self.search)

        self.tag_list = QListWidget()
        self.tag_list.setAlternatingRowColors(True)
        self.tag_list.setStyleSheet("""
            QListWidget::item:selected {
                background: #1a6fa8;
                color: white;
            }
            QListWidget::item:selected:!active {
                background: #1a6fa8;
                color: white;
            }
        """)
        self.tag_list.itemSelectionChanged.connect(self._on_selection)
        self.tag_list.itemDoubleClicked.connect(lambda _: self._assign())
        right_col.addWidget(self.tag_list, stretch=1)

        self.sel_label = QLabel("No tag selected")
        self.sel_label.setStyleSheet("color: #888;")
        right_col.addWidget(self.sel_label)

        # ── Near-miss suggestions ───────────────────────────────────────
        near_sep = QFrame()
        near_sep.setFrameShape(QFrame.Shape.HLine)
        near_sep.setFrameShadow(QFrame.Shadow.Sunken)
        right_col.addWidget(near_sep)

        self.near_miss_header = QLabel("Near matches (below threshold):")
        self.near_miss_header.setStyleSheet("color: #888; font-size: 10px;")
        right_col.addWidget(self.near_miss_header)

        self._near_miss_container = QWidget()
        self._near_miss_layout = QHBoxLayout(self._near_miss_container)
        self._near_miss_layout.setContentsMargins(0, 0, 0, 0)
        self._near_miss_layout.setSpacing(4)
        right_col.addWidget(self._near_miss_container)

        # ── Action buttons ──────────────────────────────────────────────
        btn_row = QHBoxLayout()

        self.assign_btn = QPushButton("Assign Tag")
        self.assign_btn.setEnabled(False)
        self.assign_btn.setMinimumHeight(42)
        self.assign_btn.setStyleSheet(
            "QPushButton:enabled { background: #2a6e2a; color: white; font-weight: bold; }"
        )
        self.assign_btn.clicked.connect(self._assign)
        btn_row.addWidget(self.assign_btn)

        self.new_tag_btn = QPushButton("Create New Tag")
        self.new_tag_btn.setMinimumHeight(42)
        self.new_tag_btn.setToolTip("Create a new canonical tag from this keyword")
        self.new_tag_btn.clicked.connect(self._create_new_tag)
        btn_row.addWidget(self.new_tag_btn)

        self.skip_btn = QPushButton("Skip")
        self.skip_btn.setMinimumHeight(42)
        self.skip_btn.clicked.connect(self._skip)
        btn_row.addWidget(self.skip_btn)

        right_col.addLayout(btn_row)
        outer.addWidget(right, stretch=2)

        self.search.installEventFilter(self)
        self.statusBar().showMessage("Ready")

    def eventFilter(self, obj, event):
        """Redirect Up/Down arrow keys from the search box to the tag list."""
        if obj is self.search and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            count = self.tag_list.count()
            if count == 0:
                return super().eventFilter(obj, event)
            if key == Qt.Key.Key_Down:
                cur = self.tag_list.currentRow()
                self.tag_list.setCurrentRow(min(cur + 1, count - 1))
                return True
            elif key == Qt.Key.Key_Up:
                cur = self.tag_list.currentRow()
                self.tag_list.setCurrentRow(max(cur - 1, 0))
                return True
        return super().eventFilter(obj, event)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_data(self):
        cur = self.conn.cursor()
        try:
            if self._performer_filter:
                # Only show unmatched keywords from images associated with this performer
                cur.execute("""
                    SELECT iku.keyword, COUNT(*) AS cnt
                    FROM   image_keywords_unmatched iku
                    JOIN   image_performers ip ON ip.image_id = iku.image_id
                    JOIN   performers p        ON p.id = ip.performer_id
                    WHERE  p.name = %s
                    GROUP  BY iku.keyword
                    ORDER  BY cnt DESC, iku.keyword
                """, (self._performer_filter,))
            else:
                cur.execute("""
                    SELECT keyword, COUNT(*) AS cnt
                    FROM image_keywords_unmatched
                    GROUP BY keyword
                    ORDER BY cnt DESC, keyword
                """)
            self.all_keywords = cur.fetchall()

            cur.execute("SELECT tag FROM tags")
            self.all_tags = sorted([r[0] for r in cur.fetchall()], key=str.lower)
        finally:
            cur.close()
            # Close the implicit read transaction so the connection is clean
            # before any write operations begin.
            self.conn.commit()

        self._repopulate_tags(self.all_tags)
        self._apply_filter(reset_index=True)

    def _apply_filter(self, reset_index=True):
        """Filter all_keywords by the current min-occurrences spinbox value."""
        min_count = self.min_spin.value()
        self.keywords = [
            (kw, cnt) for kw, cnt in self.all_keywords if cnt >= min_count
        ]
        if reset_index:
            self.current_idx = 0
        self._show_current()
        self.statusBar().showMessage(
            f"Filter applied: {len(self.keywords)} keyword(s) with \u2265 {min_count} occurrence(s)"
        )

    # ------------------------------------------------------------------
    # Near-miss helpers
    # ------------------------------------------------------------------

    def _compute_near_misses(self, keyword, floor=55, limit=5):
        """Return [(tag_name, score)] for tags that are close but below the match
        threshold.  Uses the same scorer as TagMatcher.match()."""
        try:
            from rapidfuzz import process as rfprocess, fuzz as rffuzz
        except ImportError:
            return []
        # Load threshold from settings (default 90, matches TagMatcher default)
        settings = _load_settings()
        threshold = settings.get('tag_fuzzy_threshold', 90)
        k = keyword.lower()
        tags_lower = [t.lower() for t in self.all_tags]
        results = rfprocess.extract(
            k, tags_lower,
            scorer=rffuzz.token_sort_ratio,
            score_cutoff=floor,
            limit=limit + 20,  # fetch extra so we can filter and still have enough
        )
        # Keep only candidates BELOW the threshold (true near-misses)
        near = [
            (self.all_tags[idx], round(score))
            for _, score, idx in results
            if score < threshold
        ]
        near.sort(key=lambda x: -x[1])
        return near[:limit]

    def _update_near_misses(self, keyword):
        """Recompute and display near-miss tag buttons for the given keyword."""
        # Clear existing buttons
        while self._near_miss_layout.count():
            item = self._near_miss_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        candidates = self._compute_near_misses(keyword)
        if not candidates:
            self.near_miss_header.setText("Near matches: none")
            self.near_miss_header.setStyleSheet("color: #666; font-size: 10px;")
            return

        self.near_miss_header.setText("Near matches — click to assign, Shift+click to pre-fill:")
        self.near_miss_header.setStyleSheet("color: #aaa; font-size: 10px;")
        for tag_name, score in candidates:
            btn = QPushButton(f"{tag_name}  ({score}%)")
            btn.setToolTip(
                f"Click to assign current keyword directly to '{tag_name}'\n"
                f"Shift+click to pre-fill the search box instead"
            )
            btn.setStyleSheet(
                "QPushButton { font-size: 10px; padding: 2px 6px; "
                "background: #2a2a2a; border: 1px solid #555; } "
                "QPushButton:hover { background: #1a5a1a; }"
            )
            btn.clicked.connect(lambda _checked, t=tag_name: self._assign_near_miss(t))
            self._near_miss_layout.addWidget(btn)
        self._near_miss_layout.addStretch()

    def _prefill_search(self, tag_name):
        """Pre-fill the tag search box with tag_name and scroll to it."""
        self.search.setText(tag_name)
        # Select exact match in list if it's visible
        for i in range(self.tag_list.count()):
            if self.tag_list.item(i).text() == tag_name:
                self.tag_list.setCurrentRow(i)
                self.tag_list.scrollToItem(self.tag_list.item(i))
                break

    def _assign_near_miss(self, tag_name):
        """Directly assign the current keyword to tag_name without pre-filling.

        Uses the same assignment path as _assign() — pre-fills and selects the
        tag first so the shared assignment logic picks it up cleanly.
        """
        if self.current_idx >= len(self.keywords):
            return
        self._prefill_search(tag_name)
        # _prefill_search selects the exact tag if present; assign only if selected
        if self.tag_list.currentRow() >= 0:
            self._assign()

    # ------------------------------------------------------------------
    # Tag list helpers
    # ------------------------------------------------------------------

    def _repopulate_tags(self, tags):
        self.tag_list.clear()
        for t in tags:
            self.tag_list.addItem(t)

    def _filter_tags(self, text):
        needle = text.strip().lower()
        filtered = (
            sorted([t for t in self.all_tags if needle in t.lower()], key=str.lower)
            if needle else self.all_tags
        )
        self._repopulate_tags(filtered)

        # If exactly one result, highlight it so the user can see what Enter will pick
        if len(filtered) == 1:
            self.tag_list.setCurrentRow(0)

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _show_current(self):
        if self.current_idx >= len(self.keywords):
            self._show_done()
            return

        keyword, count = self.keywords[self.current_idx]
        total = len(self.keywords)

        self.kw_label.setText(keyword)
        self.count_label.setText(
            f"{count} occurrence{'s' if count != 1 else ''}"
        )
        self.progress_label.setText(
            f"Keyword {self.current_idx + 1} of {total}"
            f"  \u2022  {total - self.current_idx} remaining"
        )

        # Reset tag picker state
        self.search.clear()
        self._repopulate_tags(self.all_tags)
        self.tag_list.clearSelection()
        self.assign_btn.setEnabled(False)
        self.sel_label.setText("No tag selected")
        self.sel_label.setStyleSheet("color: #888;")

        # Update near-miss suggestions for this keyword
        self._update_near_misses(keyword)

        self._load_image(keyword)

    def _load_image(self, keyword):
        """Fetch one image path for this keyword and display it."""
        self._cached_pixmap = None
        self.img_label.clear()
        self.img_label.setText("Image cannot be loaded")
        self.path_label.setText("")
        self.caption_label.setText("")

        row = None
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT i.path, d.description
                FROM image_keywords_unmatched iku
                JOIN images i ON i.id = iku.image_id
                LEFT JOIN image_descriptions d ON d.image_id = i.id
                WHERE iku.keyword = %s
                LIMIT 1
            """, (keyword,))
            row = cur.fetchone()
        except Exception:
            self.conn.rollback()
            return
        finally:
            cur.close()

        if not row:
            return

        path, description = row[0], row[1]
        self.path_label.setText(path)
        if description:
            caption = description[:280]
            if len(description) > 280:
                caption += "…"
            self.caption_label.setText(caption)
        else:
            self.caption_label.setText("")

        try:
            px = QPixmap(path)
            if not px.isNull():
                self._cached_pixmap = px
                self._display_scaled()
        except Exception:
            pass

    def _display_scaled(self):
        if self._cached_pixmap is None:
            return
        scaled = self._cached_pixmap.scaled(
            self.img_label.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.img_label.setPixmap(scaled)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._display_scaled()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _on_search_enter(self):
        """Assign the selected tag, or auto-select when only one result remains."""
        if self.tag_list.currentRow() >= 0:
            # A tag is already highlighted (via arrow keys or single match) — assign it
            self._assign()
        elif self.tag_list.count() == 1:
            # Nothing highlighted yet but only one option — select and assign
            self.tag_list.setCurrentRow(0)
            self._assign()

    def _on_selection(self):
        items = self.tag_list.selectedItems()
        if items:
            self.sel_label.setText(f"Selected: {items[0].text()}")
            self.sel_label.setStyleSheet("color: #3a9e3a; font-weight: bold;")
            self.assign_btn.setEnabled(True)
        else:
            self.sel_label.setText("No tag selected")
            self.sel_label.setStyleSheet("color: #888;")
            self.assign_btn.setEnabled(False)

    def _assign(self):
        items = self.tag_list.selectedItems()
        if not items:
            return

        tag = items[0].text()
        keyword, _ = self.keywords[self.current_idx]

        moved = 0
        deleted = 0
        cur = self.conn.cursor()
        try:
            # Resolve tag id
            cur.execute("SELECT id FROM tags WHERE tag = %s", (tag,))
            row = cur.fetchone()
            if not row:
                self.conn.rollback()
                QMessageBox.warning(self, "Error", f"Tag '{tag}' not found.")
                return
            tag_id = row[0]

            # Check for an existing alias conflict
            cur.execute("""
                SELECT t.tag
                FROM tag_aliases ta
                JOIN tags t ON t.id = ta.tag_id
                WHERE ta.alias = %s
            """, (keyword,))
            existing = cur.fetchone()
            if existing and existing[0] != tag:
                reply = QMessageBox.question(
                    self,
                    "Alias already exists",
                    f"'{keyword}' is currently aliased to '{existing[0]}'.\n\n"
                    f"Reassign it to '{tag}' instead?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                )
                if reply != QMessageBox.StandardButton.Yes:
                    self.conn.rollback()
                    return
                cur.execute(
                    "UPDATE tag_aliases SET tag_id = %s WHERE alias = %s",
                    (tag_id, keyword),
                )
            else:
                cur.execute("""
                    INSERT INTO tag_aliases (tag_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (alias) DO NOTHING
                """, (tag_id, keyword))

            # Move unmatched rows → image_keywords
            cur.execute("""
                INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                SELECT iku.image_id, %s, iku.tagger_run_id
                FROM image_keywords_unmatched iku
                WHERE iku.keyword = %s
                ON CONFLICT DO NOTHING
            """, (tag_id, keyword))
            moved = cur.rowcount

            # Remove from unmatched
            cur.execute(
                "DELETE FROM image_keywords_unmatched WHERE keyword = %s",
                (keyword,),
            )
            deleted = cur.rowcount

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(self, "Database Error", f"Failed to assign tag:\n\n{e}")
            return
        finally:
            cur.close()

        if deleted == 0:
            self.statusBar().showMessage(
                f"WARNING: '{keyword}' \u2192 '{tag}' \u2014 "
                f"{moved} image(s) updated but 0 unmatched rows were deleted!"
            )
        else:
            self.statusBar().showMessage(
                f"'{keyword}'  \u2192  '{tag}'  \u2022  "
                f"{moved} image{'s' if moved != 1 else ''} updated, "
                f"{deleted} unmatched row{'s' if deleted != 1 else ''} removed"
            )

        # Remove this keyword from both lists so counts stay accurate
        self.all_keywords = [(k, c) for k, c in self.all_keywords if k != keyword]
        self.keywords = [(k, c) for k, c in self.keywords if k != keyword]
        # Keep bulk dialog in sync
        if self._bulk_dialog and self._bulk_dialog.isVisible():
            self._bulk_dialog.remove_keywords([keyword])
        # current_idx already points to the next item after removal
        self._show_current()

    def _create_new_tag(self):
        if self.current_idx >= len(self.keywords):
            return
        keyword, _ = self.keywords[self.current_idx]

        tag_name, ok = QInputDialog.getText(
            self,
            "Create New Tag",
            "New canonical tag name:",
            text=keyword,
        )
        if not ok:
            return
        tag_name = string.capwords(tag_name.strip())
        if not tag_name:
            QMessageBox.warning(self, "Empty Name", "Tag name cannot be empty.")
            return

        moved = 0
        deleted = 0
        cur = self.conn.cursor()
        try:
            # Insert the new tag (error if it already exists — use Assign instead)
            cur.execute(
                "INSERT INTO tags (tag) VALUES (%s) ON CONFLICT (tag) DO NOTHING RETURNING id",
                (tag_name,),
            )
            row = cur.fetchone()
            if not row:
                # Tag already existed — fetch its id and warn
                cur.execute("SELECT id FROM tags WHERE tag = %s", (tag_name,))
                row = cur.fetchone()
                if not row:
                    self.conn.rollback()
                    QMessageBox.critical(self, "Error", f"Could not create or find tag '{tag_name}'.")
                    return
                tag_id = row[0]
                self.statusBar().showMessage(
                    f"Note: tag '{tag_name}' already existed — using existing tag."
                )
            else:
                tag_id = row[0]

            # Add alias if keyword differs from the tag name
            if keyword.lower() != tag_name.lower():
                cur.execute("""
                    INSERT INTO tag_aliases (tag_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (alias) DO NOTHING
                """, (tag_id, keyword))

            # Move unmatched rows → image_keywords
            cur.execute("""
                INSERT INTO image_keywords (image_id, tag_id, tagger_run_id)
                SELECT iku.image_id, %s, iku.tagger_run_id
                FROM image_keywords_unmatched iku
                WHERE iku.keyword = %s
                ON CONFLICT DO NOTHING
            """, (tag_id, keyword))
            moved = cur.rowcount

            # Remove from unmatched
            cur.execute(
                "DELETE FROM image_keywords_unmatched WHERE keyword = %s",
                (keyword,),
            )
            deleted = cur.rowcount

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(self, "Database Error", f"Failed to create tag:\n\n{e}")
            return
        finally:
            cur.close()

        # Add the new tag to the local list so it appears in the picker immediately
        self.all_tags = sorted(self.all_tags + [tag_name], key=str.lower)
        self._repopulate_tags(self.all_tags)

        if deleted == 0:
            self.statusBar().showMessage(
                f"WARNING: created '{tag_name}' from '{keyword}' \u2014 "
                f"{moved} image(s) updated but 0 unmatched rows were deleted!"
            )
        else:
            self.statusBar().showMessage(
                f"Created '{tag_name}'  \u2022  "
                f"{moved} image{'s' if moved != 1 else ''} updated, "
                f"{deleted} unmatched row{'s' if deleted != 1 else ''} removed"
            )

        # Remove from both in-memory lists
        self.all_keywords = [(k, c) for k, c in self.all_keywords if k != keyword]
        self.keywords = [(k, c) for k, c in self.keywords if k != keyword]
        # Keep bulk dialog in sync
        if self._bulk_dialog and self._bulk_dialog.isVisible():
            self._bulk_dialog.remove_keywords([keyword])
        self._show_current()

    def _skip(self):
        if self.current_idx < len(self.keywords):
            self.statusBar().showMessage(
                f"Skipped '{self.keywords[self.current_idx][0]}'"
            )
            self.current_idx += 1
            self._show_current()

    def _open_bulk_assign(self):
        """Open (or raise) the bulk-assign dialog."""
        if self._bulk_dialog is None or not self._bulk_dialog.isVisible():
            self._bulk_dialog = BulkAssignDialog(
                self.all_keywords, self.all_tags, self.conn, parent=self
            )
        self._bulk_dialog.show()
        self._bulk_dialog.raise_()
        self._bulk_dialog.activateWindow()

    def _open_manage_tags(self):
        """Open (or raise) the tag management dialog."""
        if self._manage_dialog is None or not self._manage_dialog.isVisible():
            self._manage_dialog = ManageTagsDialog(self.all_tags, self.conn, parent=self)
        self._manage_dialog.show()
        self._manage_dialog.raise_()
        self._manage_dialog.activateWindow()

    def _update_tags_after_manage(self, new_tags):
        """Called by ManageTagsDialog after a rename / merge / import.

        Updates the main tag picker list and keeps the bulk-assign dialog in
        sync so stale tag names are never shown.
        """
        self.all_tags = new_tags
        self._repopulate_tags(self.all_tags)
        if self._bulk_dialog and self._bulk_dialog.isVisible():
            self._bulk_dialog.all_tags = list(new_tags)
            self._bulk_dialog._repopulate_tags(new_tags)

    def _remove_keywords(self, keywords):
        """Drop keywords from in-memory lists after a bulk-assign operation."""
        s = set(keywords)
        self.all_keywords = [(k, c) for k, c in self.all_keywords if k not in s]
        self.keywords = [(k, c) for k, c in self.keywords if k not in s]
        # If the currently displayed keyword was one of those removed, advance
        if self.current_idx < len(self.keywords):
            current_kw = self.keywords[self.current_idx][0]
            if current_kw in s:
                self._show_current()
        else:
            self._show_current()

    def _show_all_keywords(self):
        """Open (or raise) the full keyword list dialog."""
        if self._kw_dialog is None or not self._kw_dialog.isVisible():
            self._kw_dialog = AllKeywordsDialog(self.all_keywords, parent=self)
        self._kw_dialog.show()
        self._kw_dialog.raise_()
        self._kw_dialog.activateWindow()

    # ------------------------------------------------------------------
    # Done state
    # ------------------------------------------------------------------

    def _show_done(self):
        self.kw_label.setText("All done!")
        self.count_label.setText("No more keywords to review at this threshold.")
        self.progress_label.setText("")
        self.img_label.clear()
        self.img_label.setText("All matched keywords have been processed.")
        self.path_label.setText("")
        self.caption_label.setText("")
        self.assign_btn.setEnabled(False)
        self.new_tag_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)
        self.statusBar().showMessage("Complete — lower the minimum or close the window.")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def closeEvent(self, event):
        if self._kw_dialog:
            self._kw_dialog.close()
        if self._bulk_dialog:
            self._bulk_dialog.close()
        if self._manage_dialog:
            self._manage_dialog.close()
        try:
            self.conn.close()
        except Exception:
            pass
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tag Review Tool")
    parser.add_argument(
        "--performer",
        metavar="NAME",
        default=None,
        help="Filter unmatched keywords to images belonging to this performer",
    )
    args, _unknown = parser.parse_known_args()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    settings = _load_settings()
    try:
        conn = _connect(settings)
    except Exception as e:
        QMessageBox.critical(
            None,
            "Database Connection Failed",
            f"{e}\n\nCheck db_host / db_port / db_user / db_password / db_name in settings.json.",
        )
        sys.exit(1)

    window = TagReviewWindow(conn, performer_filter=args.performer)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
