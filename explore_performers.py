#!/usr/bin/env python3
"""
Explore Performers Tool

Browse performers stored in the ai_captioning database.

  Left panel   — searchable list of all performers with image counts.
  Centre panel — image preview that scales to fill the available height,
                 with ◄ / ► navigation buttons beneath it (or left/right
                 arrow keys).
  Right panel  — vertical splitter:
                   top    performer-level tags (from performer_tags)
                          right-click a tag chip to pin / remove / blacklist
                          "Add Tag…" button to manually assign any tag
                          "Tag Review…" button to open tag_review.py for
                          this performer's unmatched keywords
                   bottom caption and per-image canonical tags

All three panels are independently resizable via drag-handles.

Run from the project root:
    python explore_performers.py
"""

import sys
import json
import random
import subprocess
from pathlib import Path

try:
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QDialog,
        QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QListWidget, QListWidgetItem, QPushButton,
        QScrollArea, QFrame, QSizePolicy, QSplitter, QLayout,
        QMenu, QMessageBox, QDialogButtonBox,
    )
    from PyQt6.QtCore import Qt, QSize, QRect, QPoint
    from PyQt6.QtGui import QPixmap
except ImportError:
    print("PyQt6 is required.  Run: pip install PyQt6")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("psycopg2 is required.  Run: pip install psycopg2-binary")
    sys.exit(1)

# Add src/ to path so we can import llmii_db
_SRC_DIR = Path(__file__).resolve().parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
try:
    import llmii_db as _llmii_db
    _HAS_LLMII_DB = True
except ImportError:
    _llmii_db = None
    _HAS_LLMII_DB = False


# ---------------------------------------------------------------------------
# Settings / connection helpers
# ---------------------------------------------------------------------------

_SETTINGS_PATH = Path(__file__).resolve().parent / "settings.json"
_SCHEMA = "ai_captioning"


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
# Scalable image label — rescales stored pixmap on every resize
# ---------------------------------------------------------------------------

class _ScaledImageLabel(QLabel):
    """QLabel that rescales its stored pixmap whenever the widget is resized."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._source_px = None
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setMinimumSize(120, 120)

    def setSourcePixmap(self, px: QPixmap):
        self._source_px = px
        self._rescale()

    def clearPixmap(self, message: str = ""):
        self._source_px = None
        super().clear()
        if message:
            super().setText(message)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._rescale()

    def _rescale(self):
        if self._source_px and not self._source_px.isNull():
            scaled = self._source_px.scaled(
                self.width(), self.height(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            super().setPixmap(scaled)


# ---------------------------------------------------------------------------
# FlowLayout — wraps tag chips based on available pixel width
# ---------------------------------------------------------------------------

class FlowLayout(QLayout):
    def __init__(self, parent=None, h_spacing=4, v_spacing=4):
        super().__init__(parent)
        self._items = []
        self._h = h_spacing
        self._v = v_spacing

    def addItem(self, item):
        self._items.append(item)

    def count(self):
        return len(self._items)

    def itemAt(self, index):
        return self._items[index] if 0 <= index < len(self._items) else None

    def takeAt(self, index):
        return self._items.pop(index) if 0 <= index < len(self._items) else None

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        return self._do_layout(QRect(0, 0, width, 0), test_only=True)

    def setGeometry(self, rect):
        super().setGeometry(rect)
        self._do_layout(rect, test_only=False)

    def sizeHint(self):
        return self.minimumSize()

    def minimumSize(self):
        sz = QSize()
        for item in self._items:
            sz = sz.expandedTo(item.minimumSize())
        m = self.contentsMargins()
        return sz + QSize(m.left() + m.right(), m.top() + m.bottom())

    def _do_layout(self, rect, test_only):
        m  = self.contentsMargins()
        x0 = rect.x() + m.left()
        y  = rect.y() + m.top()
        x  = x0
        row_h = 0
        for item in self._items:
            w = item.sizeHint().width()
            h = item.sizeHint().height()
            if x + w > rect.right() - m.right() and x > x0:
                y    += row_h + self._v
                x     = x0
                row_h = 0
            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), item.sizeHint()))
            x     += w + self._h
            row_h  = max(row_h, h)
        return y + row_h - rect.y() + m.bottom()


# ---------------------------------------------------------------------------
# CSS styles for tag chips
# ---------------------------------------------------------------------------

_IMG_CSS = (
    "QLabel { background: #e1f0ff; color: #0066cc; border: 1px solid #99ccff; "
    "border-radius: 3px; padding: 1px 6px; font-size: 11px; }"
)
# Threshold-assigned performer tag
_PERF_CSS = (
    "QLabel { background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; "
    "border-radius: 3px; padding: 1px 6px; font-size: 11px; }"
    "QLabel:hover { background: #c8e6c9; }"
)
# Pinned performer tag (orange/amber)
_PINNED_CSS = (
    "QLabel { background: #fff8e1; color: #e65100; border: 1px solid #ffcc02; "
    "border-radius: 3px; padding: 1px 6px; font-size: 11px; font-weight: bold; }"
    "QLabel:hover { background: #fff3cd; }"
)
# Manually added performer tag (purple)
_MANUAL_CSS = (
    "QLabel { background: #f3e5f5; color: #6a1b9a; border: 1px solid #ce93d8; "
    "border-radius: 3px; padding: 1px 6px; font-size: 11px; font-style: italic; }"
    "QLabel:hover { background: #e8d5f0; }"
)


# ---------------------------------------------------------------------------
# Interactive tag chip with right-click context menu
# ---------------------------------------------------------------------------

class _TagChip(QLabel):
    """Performer tag chip that supports a right-click context menu.

    Callbacks (all optional):
        on_pin(tag_id, pinned: bool)
        on_exclude_performer(tag_id)
        on_exclude_global(tag_id, tag_name)
    """

    def __init__(self, tag_id, tag_name, pinned, manually_added,
                 on_pin=None, on_exclude_performer=None, on_exclude_global=None,
                 parent=None):
        super().__init__(tag_name, parent)
        self.tag_id = tag_id
        self.tag_name = tag_name
        self.pinned = pinned
        self.manually_added = manually_added
        self._on_pin = on_pin
        self._on_exclude_performer = on_exclude_performer
        self._on_exclude_global = on_exclude_global
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_menu)
        self._apply_style()

    def _apply_style(self):
        if self.manually_added:
            self.setStyleSheet(_MANUAL_CSS)
            self.setToolTip(f"{self.tag_name}\n(manually added — pinned)")
        elif self.pinned:
            self.setStyleSheet(_PINNED_CSS)
            self.setToolTip(f"{self.tag_name}\n(pinned — kept below threshold)")
        else:
            self.setStyleSheet(_PERF_CSS)
            self.setToolTip(
                f"{self.tag_name}\n(threshold-assigned — right-click to manage)"
            )

    def _show_menu(self, pos):
        menu = QMenu(self)

        if not self.manually_added:
            if self.pinned:
                act = menu.addAction("Unpin tag")
                act.triggered.connect(lambda: self._on_pin and self._on_pin(self.tag_id, False))
            else:
                act = menu.addAction("Pin tag  (keep below threshold)")
                act.triggered.connect(lambda: self._on_pin and self._on_pin(self.tag_id, True))
            menu.addSeparator()

        act_excl = menu.addAction("Remove for this performer")
        act_excl.triggered.connect(
            lambda: self._on_exclude_performer and self._on_exclude_performer(self.tag_id)
        )

        act_global = menu.addAction("Blacklist for ALL performers")
        act_global.triggered.connect(
            lambda: self._on_exclude_global and self._on_exclude_global(self.tag_id, self.tag_name)
        )

        menu.exec(self.mapToGlobal(pos))

    def mouseDoubleClickEvent(self, event):
        """Double-click removes the tag from this performer (same as right-click → Remove)."""
        if event.button() == Qt.MouseButton.LeftButton:
            if self._on_exclude_performer:
                self._on_exclude_performer(self.tag_id)
        else:
            super().mouseDoubleClickEvent(event)


# ---------------------------------------------------------------------------
# Add Tag dialog
# ---------------------------------------------------------------------------

class _AddTagDialog(QDialog):
    """Simple searchable tag picker for manually assigning a tag to a performer."""

    def __init__(self, conn, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.selected_tag = None   # (id, name) or None
        self._all_tags = []

        self.setWindowTitle("Add Tag to Performer")
        self.setMinimumSize(320, 480)
        self._build_ui()
        self._load_tags()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        layout.addWidget(QLabel("Search and select a canonical tag:"))

        self.search = QLineEdit()
        self.search.setPlaceholderText("Filter tags…")
        self.search.textChanged.connect(self._filter)
        layout.addWidget(self.search)

        self.tag_list = QListWidget()
        self.tag_list.setAlternatingRowColors(True)
        self.tag_list.setStyleSheet("""
            QListWidget::item:selected { background: #1a6fa8; color: white; }
            QListWidget::item:selected:!active { background: #1a6fa8; color: white; }
        """)
        self.tag_list.itemDoubleClicked.connect(self._accept)
        layout.addWidget(self.tag_list, stretch=1)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load_tags(self):
        try:
            if _HAS_LLMII_DB:
                self._all_tags = _llmii_db.get_all_tags(self.conn)
            else:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT id, tag FROM tags ORDER BY tag")
                    self._all_tags = cur.fetchall()
        except Exception as e:
            print(f"Error loading tags: {e}")
            self._all_tags = []
        self._filter()

    def _filter(self, text=None):
        needle = (text or self.search.text()).strip().lower()
        self.tag_list.clear()
        for tid, tname in self._all_tags:
            if not needle or needle in tname.lower():
                item = QListWidgetItem(tname)
                item.setData(Qt.ItemDataRole.UserRole, (tid, tname))
                self.tag_list.addItem(item)

    def _accept(self):
        item = self.tag_list.currentItem()
        if item:
            self.selected_tag = item.data(Qt.ItemDataRole.UserRole)
            self.accept()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chip_widget(tags, css=_IMG_CSS):
    """Return a QWidget containing sorted flow-layout image tag chips (read-only)."""
    container = QWidget()
    layout = FlowLayout(container, h_spacing=4, v_spacing=3)
    container.setLayout(layout)
    for tag in sorted(tags, key=str.casefold):
        lbl = QLabel(tag)
        lbl.setStyleSheet(css)
        lbl.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        layout.addWidget(lbl)
    return container


def _placeholder(text):
    lbl = QLabel(text)
    lbl.setStyleSheet("color: #999; font-size: 11px; padding: 6px;")
    lbl.setWordWrap(True)
    lbl.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    return lbl


def _section_hdr(text):
    lbl = QLabel(text)
    lbl.setStyleSheet(
        "QLabel { font-weight: bold; font-size: 11px; "
        "border-bottom: 1px solid #aaa; padding: 3px 2px; }"
    )
    return lbl


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class ExplorePerformersWindow(QMainWindow):

    def __init__(self, conn):
        super().__init__()
        self.conn            = conn
        self._all_performers = []   # [(id, name, image_count), ...]
        self._images         = []   # [(image_id, path), ...]
        self._img_index      = 0
        self._current_pid    = None
        self._current_pname  = None

        self.setWindowTitle("Explore Performers")
        self.resize(1280, 820)
        self._build_ui()
        self._load_performers()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(0)

        self._main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self._main_splitter.setHandleWidth(5)
        root.addWidget(self._main_splitter)

        # ── LEFT: performer list ─────────────────────────────────────
        left = QWidget()
        ll = QVBoxLayout(left)
        ll.setContentsMargins(4, 4, 4, 4)
        ll.setSpacing(4)

        ll.addWidget(_section_hdr("Performers"))

        self.perf_search = QLineEdit()
        self.perf_search.setPlaceholderText("Filter performers…")
        self.perf_search.textChanged.connect(self._filter_performers)
        ll.addWidget(self.perf_search)

        self.perf_list = QListWidget()
        self.perf_list.setAlternatingRowColors(True)
        self.perf_list.setStyleSheet(
            "QListWidget::item { padding: 2px; }"
            "QListWidget::item:selected { background: #1a6fa8; color: white; }"
            "QListWidget::item:selected:!active { background: #1a6fa8; color: white; }"
        )
        self.perf_list.currentItemChanged.connect(self._on_performer_selected)
        ll.addWidget(self.perf_list, stretch=1)

        self.perf_count_lbl = QLabel("")
        self.perf_count_lbl.setStyleSheet("color: #888; font-size: 10px;")
        ll.addWidget(self.perf_count_lbl)

        self._main_splitter.addWidget(left)

        # ── CENTRE: scalable image + filename + nav ──────────────────
        centre = QWidget()
        cl = QVBoxLayout(centre)
        cl.setContentsMargins(2, 2, 2, 2)
        cl.setSpacing(3)

        self.image_lbl = _ScaledImageLabel()
        self.image_lbl.setStyleSheet(
            "QLabel { background: #1c1c1c; color: #666; font-size: 13px; "
            "border: 1px solid #444; }"
        )
        self.image_lbl.clearPixmap("Select a performer")
        cl.addWidget(self.image_lbl, stretch=1)

        self.filename_lbl = QLabel("")
        self.filename_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.filename_lbl.setStyleSheet("color: #888; font-size: 10px;")
        cl.addWidget(self.filename_lbl)

        nav_row = QHBoxLayout()
        nav_row.setSpacing(4)
        nav_row.setContentsMargins(0, 0, 0, 0)

        self.prev_btn = QPushButton("◄")
        self.prev_btn.setFixedSize(50, 28)
        self.prev_btn.setToolTip("Previous image  (←)")
        self.prev_btn.setEnabled(False)
        self.prev_btn.clicked.connect(self._prev_image)
        nav_row.addWidget(self.prev_btn)

        self.nav_lbl = QLabel("—")
        self.nav_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.nav_lbl.setStyleSheet("font-size: 12px; color: #666;")
        nav_row.addWidget(self.nav_lbl, stretch=1)

        self.next_btn = QPushButton("►")
        self.next_btn.setFixedSize(50, 28)
        self.next_btn.setToolTip("Next image  (→)")
        self.next_btn.setEnabled(False)
        self.next_btn.clicked.connect(self._next_image)
        nav_row.addWidget(self.next_btn)

        cl.addLayout(nav_row)
        self._main_splitter.addWidget(centre)

        # ── RIGHT: vertical splitter (performer tags | image info) ────
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        right_splitter.setHandleWidth(5)

        # Top: performer tags
        pt = QWidget()
        ptl = QVBoxLayout(pt)
        ptl.setContentsMargins(4, 4, 4, 4)
        ptl.setSpacing(3)
        ptl.addWidget(_section_hdr("Performer Tags"))
        self.perf_tags_info_lbl = QLabel("")
        self.perf_tags_info_lbl.setStyleSheet("color: #888; font-size: 10px;")
        self.perf_tags_info_lbl.setWordWrap(True)
        ptl.addWidget(self.perf_tags_info_lbl)
        self.perf_tags_scroll = QScrollArea()
        self.perf_tags_scroll.setWidgetResizable(True)
        self.perf_tags_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.perf_tags_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.perf_tags_scroll.setWidget(_placeholder("Select a performer."))
        ptl.addWidget(self.perf_tags_scroll, stretch=1)

        # Action buttons row
        btn_row = QHBoxLayout()
        btn_row.setSpacing(4)
        btn_row.setContentsMargins(0, 2, 0, 0)

        self.add_tag_btn = QPushButton("Add Tag…")
        self.add_tag_btn.setToolTip("Manually assign a tag to this performer")
        self.add_tag_btn.setEnabled(False)
        self.add_tag_btn.clicked.connect(self._add_tag_to_performer)
        btn_row.addWidget(self.add_tag_btn)

        self.tag_review_btn = QPushButton("Tag Review…")
        self.tag_review_btn.setToolTip(
            "Open Tag Review filtered to this performer's unmatched keywords"
        )
        self.tag_review_btn.setEnabled(False)
        self.tag_review_btn.clicked.connect(self._open_tag_review)
        btn_row.addWidget(self.tag_review_btn)

        ptl.addLayout(btn_row)

        # Legend
        legend = QLabel(
            "  green = threshold   orange = pinned   purple = manual"
        )
        legend.setStyleSheet("color: #aaa; font-size: 9px; padding-top: 1px;")
        ptl.addWidget(legend)

        right_splitter.addWidget(pt)

        # Middle: all image tags by frequency (excluding already-attached ones)
        at = QWidget()
        atl = QVBoxLayout(at)
        atl.setContentsMargins(4, 4, 4, 4)
        atl.setSpacing(3)

        # Header row with sort buttons
        hdr_row = QHBoxLayout()
        self.img_tag_stats_hdr = QLabel("All Image Tags")
        self.img_tag_stats_hdr.setStyleSheet(
            "QLabel { font-weight: bold; font-size: 11px; padding: 3px 2px; }"
        )
        hdr_row.addWidget(self.img_tag_stats_hdr, stretch=1)
        self._tag_stats_sort = 'count'    # 'count' | 'alpha'
        self._sort_count_btn = QPushButton("By Count")
        self._sort_alpha_btn = QPushButton("A-Z")
        for btn in (self._sort_count_btn, self._sort_alpha_btn):
            btn.setFixedHeight(20)
            btn.setStyleSheet(
                "QPushButton { font-size: 10px; padding: 1px 6px; }"
                "QPushButton:checked { background: #1a6fa8; color: white; }"
            )
            btn.setCheckable(True)
        self._sort_count_btn.setChecked(True)
        self._sort_count_btn.clicked.connect(lambda: self._set_tag_stats_sort('count'))
        self._sort_alpha_btn.clicked.connect(lambda: self._set_tag_stats_sort('alpha'))
        hdr_row.addWidget(self._sort_count_btn)
        hdr_row.addWidget(self._sort_alpha_btn)
        atl.addLayout(hdr_row)

        self.img_tag_stats_info = QLabel("Double-click to attach · right-click for more options")
        self.img_tag_stats_info.setStyleSheet("color: #888; font-size: 10px;")
        self.img_tag_stats_info.setWordWrap(True)
        atl.addWidget(self.img_tag_stats_info)
        self.img_tag_stats_list = QListWidget()
        self.img_tag_stats_list.setAlternatingRowColors(True)
        self.img_tag_stats_list.setStyleSheet(
            "QListWidget::item { padding: 2px; font-size: 11px; }"
            "QListWidget::item:selected { background: #1a6fa8; color: white; }"
            "QListWidget::item:selected:!active { background: #1a6fa8; color: white; }"
        )
        self.img_tag_stats_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.img_tag_stats_list.customContextMenuRequested.connect(self._on_tag_stats_context)
        self.img_tag_stats_list.itemDoubleClicked.connect(self._on_tag_stats_double_click)
        atl.addWidget(self.img_tag_stats_list, stretch=1)
        right_splitter.addWidget(at)

        # Bottom: caption + image tags
        ii = QWidget()
        iil = QVBoxLayout(ii)
        iil.setContentsMargins(4, 4, 4, 4)
        iil.setSpacing(3)

        iil.addWidget(_section_hdr("Caption"))
        self.caption_lbl = QLabel("")
        self.caption_lbl.setWordWrap(True)
        self.caption_lbl.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.caption_lbl.setStyleSheet(
            "QLabel { font-size: 11px; padding: 3px; }"
        )
        cap_scroll = QScrollArea()
        cap_scroll.setWidgetResizable(True)
        cap_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        cap_scroll.setFrameShape(QFrame.Shape.NoFrame)
        cap_scroll.setWidget(self.caption_lbl)
        iil.addWidget(cap_scroll, stretch=2)

        iil.addWidget(_section_hdr("Image Tags"))
        self.img_tags_scroll = QScrollArea()
        self.img_tags_scroll.setWidgetResizable(True)
        self.img_tags_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.img_tags_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.img_tags_scroll.setWidget(_placeholder("No image selected"))
        iil.addWidget(self.img_tags_scroll, stretch=3)

        right_splitter.addWidget(ii)
        right_splitter.setSizes([280, 320, 250])

        self._main_splitter.addWidget(right_splitter)
        self._main_splitter.setSizes([230, 780, 280])

    # ------------------------------------------------------------------
    # Keyboard navigation
    # ------------------------------------------------------------------

    def keyPressEvent(self, event):
        key = event.key()
        if key == Qt.Key.Key_Left:
            self._prev_image()
        elif key == Qt.Key.Key_Right:
            self._next_image()
        else:
            super().keyPressEvent(event)

    # ------------------------------------------------------------------
    # Performer list
    # ------------------------------------------------------------------

    def _load_performers(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT p.id, p.name, COUNT(ip.image_id) AS img_count
                    FROM   performers p
                    LEFT   JOIN image_performers ip ON ip.performer_id = p.id
                    GROUP  BY p.id, p.name
                    ORDER  BY p.name
                """)
                self._all_performers = cur.fetchall()
        except Exception as e:
            self._all_performers = []
            print(f"Error loading performers: {e}")
            self.conn.rollback()
        self._repopulate_perf_list(self._all_performers)

    def _repopulate_perf_list(self, rows):
        self.perf_list.blockSignals(True)
        self.perf_list.clear()
        for pid, name, count in rows:
            item = QListWidgetItem(f"{name}  ({count})")
            item.setData(Qt.ItemDataRole.UserRole, pid)
            self.perf_list.addItem(item)
        self.perf_list.blockSignals(False)
        n = len(rows)
        self.perf_count_lbl.setText(f"{n} performer{'s' if n != 1 else ''}")

    def _filter_performers(self, text=None):
        needle = (text or self.perf_search.text()).strip().lower()
        filtered = [r for r in self._all_performers if not needle or needle in r[1].lower()]
        self._repopulate_perf_list(filtered)

    def _on_performer_selected(self, item):
        if item is None:
            return
        pid = item.data(Qt.ItemDataRole.UserRole)
        if pid == self._current_pid:
            return
        self._current_pid = pid
        # Resolve name from _all_performers
        self._current_pname = next(
            (name for (p, name, _) in self._all_performers if p == pid), None
        )
        self._load_performer_images(pid)
        self._load_performer_tags(pid)
        self._load_image_tag_stats(pid)
        self.add_tag_btn.setEnabled(True)
        self.tag_review_btn.setEnabled(True)

    # ------------------------------------------------------------------
    # Image list for current performer
    # ------------------------------------------------------------------

    def _load_performer_images(self, performer_id):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT i.id, i.path
                    FROM   images i
                    JOIN   image_performers ip ON ip.image_id = i.id
                    WHERE  ip.performer_id = %s
                    ORDER  BY i.path
                """, (performer_id,))
                self._images = cur.fetchall()
        except Exception as e:
            self._images = []
            print(f"Error loading images for performer {performer_id}: {e}")
            self.conn.rollback()

        if self._images:
            self._img_index = random.randrange(len(self._images))
            self._show_image(self._img_index)
        else:
            self._img_index = 0
            self._clear_centre("No images found for this performer.")

    # ------------------------------------------------------------------
    # Performer tags — interactive chips
    # ------------------------------------------------------------------

    def _load_performer_tags(self, performer_id):
        """Query performer tags (excluding tombstoned/globally-excluded) and render chips."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT t.id, t.tag, pt.pinned, pt.manually_added
                    FROM   performer_tags pt
                    JOIN   tags t ON t.id = pt.tag_id
                    WHERE  pt.performer_id = %s
                      AND  NOT pt.excluded
                      AND  NOT COALESCE(t.exclude_from_performers, FALSE)
                    ORDER  BY t.tag
                """, (performer_id,))
                rows = cur.fetchall()  # [(tag_id, tag_name, pinned, manually_added), ...]
        except Exception as e:
            rows = []
            print(f"Error loading performer tags for {performer_id}: {e}")
            self.conn.rollback()

        if rows:
            threshold_count = sum(1 for _, _, pinned, manually in rows
                                  if not pinned and not manually)
            pinned_count    = sum(1 for _, _, pinned, manually in rows
                                  if pinned and not manually)
            manual_count    = sum(1 for _, _, _, manually in rows if manually)
            parts = []
            if threshold_count:
                parts.append(f"{threshold_count} threshold")
            if pinned_count:
                parts.append(f"{pinned_count} pinned")
            if manual_count:
                parts.append(f"{manual_count} manual")
            self.perf_tags_info_lbl.setText(
                f"{len(rows)} tag(s) — {', '.join(parts)}"
            )
            widget = self._build_perf_tag_widget(rows, performer_id)
        else:
            self.perf_tags_info_lbl.setText("")
            widget = _placeholder(
                "No performer tags assigned.\n\n"
                "Run 'Assign Performer Tags' in the main settings dialog,\n"
                "or use 'Add Tag…' to assign one manually."
            )
        self.perf_tags_scroll.setWidget(widget)

    def _build_perf_tag_widget(self, rows, performer_id):
        container = QWidget()
        layout = FlowLayout(container, h_spacing=4, v_spacing=3)
        container.setLayout(layout)
        for tag_id, tag_name, pinned, manually_added in sorted(rows, key=lambda r: r[1].casefold()):
            chip = _TagChip(
                tag_id, tag_name, pinned, manually_added,
                on_pin=self._pin_performer_tag,
                on_exclude_performer=self._exclude_performer_tag,
                on_exclude_global=self._exclude_tag_globally,
            )
            layout.addWidget(chip)
        return container

    # ------------------------------------------------------------------
    # Performer tag actions
    # ------------------------------------------------------------------

    def _pin_performer_tag(self, tag_id, pinned):
        if self._current_pid is None:
            return
        try:
            if _HAS_LLMII_DB:
                _llmii_db.pin_performer_tag(self.conn, self._current_pid, tag_id, pinned)
            else:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "UPDATE performer_tags SET pinned = %s "
                        "WHERE performer_id = %s AND tag_id = %s AND NOT excluded",
                        (pinned, self._current_pid, tag_id)
                    )
                self.conn.commit()
            self._load_performer_tags(self._current_pid)
            self._load_image_tag_stats(self._current_pid)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _exclude_performer_tag(self, tag_id):
        if self._current_pid is None:
            return
        try:
            if _HAS_LLMII_DB:
                _llmii_db.exclude_performer_tag(self.conn, self._current_pid, tag_id)
            else:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO performer_tags
                            (performer_id, tag_id, image_count, total_images,
                             excluded, pinned, manually_added, assigned_at)
                        VALUES (%s, %s, 0, 0, TRUE, FALSE, FALSE, now())
                        ON CONFLICT (performer_id, tag_id) DO UPDATE
                            SET excluded=TRUE, pinned=FALSE, manually_added=FALSE
                    """, (self._current_pid, tag_id))
                self.conn.commit()
            self._load_performer_tags(self._current_pid)
            self._load_image_tag_stats(self._current_pid)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _exclude_tag_globally(self, tag_id, tag_name):
        """Blacklist a tag from all performers after confirmation."""
        reply = QMessageBox.warning(
            self,
            "Blacklist for ALL Performers",
            f"Remove '{tag_name}' from every performer's tag list and prevent\n"
            f"it from being auto-assigned to any performer in future runs?\n\n"
            f"This cannot be undone from this tool.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        try:
            if _HAS_LLMII_DB:
                _llmii_db.exclude_tag_globally(self.conn, tag_id)
            else:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tags SET exclude_from_performers = TRUE WHERE id = %s",
                        (tag_id,)
                    )
                    cur.execute("DELETE FROM performer_tags WHERE tag_id = %s", (tag_id,))
                self.conn.commit()
            # Refresh current performer
            if self._current_pid is not None:
                self._load_performer_tags(self._current_pid)
                self._load_image_tag_stats(self._current_pid)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _add_tag_to_performer(self):
        if self._current_pid is None:
            return
        dlg = _AddTagDialog(self.conn, parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.selected_tag:
            tag_id, tag_name = dlg.selected_tag
            try:
                if _HAS_LLMII_DB:
                    _llmii_db.add_performer_tag(self.conn, self._current_pid, tag_id)
                else:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO performer_tags
                                (performer_id, tag_id, image_count, total_images,
                                 manually_added, pinned, excluded, assigned_at)
                            VALUES (%s, %s, 0, 0, TRUE, TRUE, FALSE, now())
                            ON CONFLICT (performer_id, tag_id) DO UPDATE
                                SET manually_added=TRUE, pinned=TRUE, excluded=FALSE
                        """, (self._current_pid, tag_id))
                    self.conn.commit()
                self._load_performer_tags(self._current_pid)
                self._load_image_tag_stats(self._current_pid)
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    # ------------------------------------------------------------------
    # All image tags by frequency (middle pane)
    # ------------------------------------------------------------------

    def _load_image_tag_stats(self, performer_id):
        """Query tag frequencies for the selected performer and render the list."""
        self._tag_stats_rows = []
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT t.id, t.tag, COUNT(DISTINCT ik.image_id) AS cnt
                    FROM   image_performers ip
                    JOIN   image_keywords   ik ON ik.image_id = ip.image_id
                    JOIN   tags             t  ON t.id = ik.tag_id
                    WHERE  ip.performer_id = %s
                      AND  t.id NOT IN (
                               SELECT tag_id
                               FROM   performer_tags
                               WHERE  performer_id = %s
                                 AND  NOT excluded
                           )
                    GROUP  BY t.id, t.tag
                """, (performer_id, performer_id))
                self._tag_stats_rows = cur.fetchall()  # [(tag_id, tag_name, count), ...]
        except Exception as e:
            print(f"Error loading image tag stats for {performer_id}: {e}")
            self.conn.rollback()

        self._render_tag_stats()

    def _set_tag_stats_sort(self, mode):
        """Switch sort mode and re-render without re-querying the DB."""
        self._tag_stats_sort = mode
        self._sort_count_btn.setChecked(mode == 'count')
        self._sort_alpha_btn.setChecked(mode == 'alpha')
        self._render_tag_stats()

    def _render_tag_stats(self):
        """Re-render the All Image Tags list from the cached rows in the current sort order."""
        rows = list(getattr(self, '_tag_stats_rows', []))
        if self._tag_stats_sort == 'alpha':
            rows.sort(key=lambda r: r[1].casefold())
        else:
            rows.sort(key=lambda r: (-r[2], r[1].casefold()))

        self.img_tag_stats_list.clear()
        total_images = len(self._images)
        for tag_id, tag_name, cnt in rows:
            pct = f"{100 * cnt // total_images}%" if total_images else ""
            item = QListWidgetItem(f"{tag_name}  ({cnt}{f'  {pct}' if pct else ''})")
            item.setData(Qt.ItemDataRole.UserRole, (tag_id, tag_name))
            self.img_tag_stats_list.addItem(item)

        n = len(rows)
        self.img_tag_stats_hdr.setText(
            f"All Image Tags  ({n})" if n else "All Image Tags"
        )

    def _on_tag_stats_double_click(self, item):
        """Double-clicking a tag in All Image Tags attaches it to the performer."""
        if item is None or self._current_pid is None:
            return
        tag_id, tag_name = item.data(Qt.ItemDataRole.UserRole)
        self._attach_tag_from_stats(tag_id, tag_name)

    def _on_tag_stats_context(self, pos):
        item = self.img_tag_stats_list.itemAt(pos)
        if item is None or self._current_pid is None:
            return
        tag_id, tag_name = item.data(Qt.ItemDataRole.UserRole)
        menu = QMenu(self)
        act = menu.addAction(f"Attach '{tag_name}' to performer")
        act.triggered.connect(lambda: self._attach_tag_from_stats(tag_id, tag_name))
        menu.addSeparator()
        act_global = menu.addAction(f"Blacklist '{tag_name}' for ALL performers")
        act_global.triggered.connect(lambda: self._exclude_tag_globally(tag_id, tag_name))
        menu.exec(self.img_tag_stats_list.mapToGlobal(pos))

    def _attach_tag_from_stats(self, tag_id, tag_name):
        if self._current_pid is None:
            return
        try:
            if _HAS_LLMII_DB:
                _llmii_db.add_performer_tag(self.conn, self._current_pid, tag_id)
            else:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO performer_tags
                            (performer_id, tag_id, image_count, total_images,
                             manually_added, pinned, excluded, assigned_at)
                        VALUES (%s, %s, 0, 0, TRUE, TRUE, FALSE, now())
                        ON CONFLICT (performer_id, tag_id) DO UPDATE
                            SET manually_added=TRUE, pinned=TRUE, excluded=FALSE
                    """, (self._current_pid, tag_id))
                self.conn.commit()
            self._load_performer_tags(self._current_pid)
            self._load_image_tag_stats(self._current_pid)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    # ------------------------------------------------------------------
    # Open Tag Review for current performer
    # ------------------------------------------------------------------

    def _open_tag_review(self):
        if self._current_pname is None:
            return
        script = Path(__file__).resolve().parent / "tag_review.py"
        if not script.exists():
            QMessageBox.warning(self, "Not Found", f"tag_review.py not found at:\n{script}")
            return
        try:
            subprocess.Popen(
                [sys.executable, str(script), "--performer", self._current_pname],
                cwd=str(script.parent),
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not launch tag_review.py:\n{e}")

    # ------------------------------------------------------------------
    # Display one image + caption + image tags
    # ------------------------------------------------------------------

    def _show_image(self, index):
        if not self._images:
            return

        image_id, path = self._images[index]
        total = len(self._images)

        self.nav_lbl.setText(f"{index + 1} / {total}")
        self.prev_btn.setEnabled(total > 1)
        self.next_btn.setEnabled(total > 1)

        display_path = path.split('::', 1)[1] if '::' in path else path
        self.filename_lbl.setText(Path(display_path).name)
        self.filename_lbl.setToolTip(display_path)

        # Image
        if '::' in path:
            self.image_lbl.clearPixmap("(inside zip archive — cannot preview)")
        elif Path(path).exists():
            px = QPixmap(path)
            if not px.isNull():
                self.image_lbl.setSourcePixmap(px)
            else:
                self.image_lbl.clearPixmap("(cannot decode image)")
        else:
            self.image_lbl.clearPixmap("(file not found on disk)")

        # Caption
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT description FROM image_descriptions WHERE image_id = %s",
                    (image_id,),
                )
                row = cur.fetchone()
                self.caption_lbl.setText(row[0] if row else "(no caption)")
        except Exception:
            self.caption_lbl.setText("")

        # Image tags
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT t.tag
                    FROM   image_keywords ik
                    JOIN   tags t ON t.id = ik.tag_id
                    WHERE  ik.image_id = %s
                    ORDER  BY t.tag
                """, (image_id,))
                img_tags = [row[0] for row in cur.fetchall()]
        except Exception:
            img_tags = []

        self.img_tags_scroll.setWidget(
            _chip_widget(img_tags, css=_IMG_CSS) if img_tags
            else _placeholder("No tags for this image")
        )

    def _clear_centre(self, message=""):
        self.image_lbl.clearPixmap(message)
        self.filename_lbl.setText("")
        self.nav_lbl.setText("—")
        self.prev_btn.setEnabled(False)
        self.next_btn.setEnabled(False)
        self.caption_lbl.setText("")
        self.img_tags_scroll.setWidget(_placeholder("No image"))

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _prev_image(self):
        if self._images:
            self._img_index = (self._img_index - 1) % len(self._images)
            self._show_image(self._img_index)

    def _next_image(self):
        if self._images:
            self._img_index = (self._img_index + 1) % len(self._images)
            self._show_image(self._img_index)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    settings = _load_settings()
    if not settings.get("db_name"):
        print(
            "No database settings found in settings.json.\n"
            "Configure the database connection in the main ImageIndexer application first."
        )
        sys.exit(1)

    try:
        conn = _connect(settings)
    except Exception as e:
        print(f"Could not connect to database:\n  {e}")
        sys.exit(1)

    # Apply schema migrations so new columns (pinned, excluded, etc.) exist
    if _HAS_LLMII_DB:
        try:
            _llmii_db.apply_migrations(conn)
        except Exception as e:
            print(f"Warning: could not apply migrations: {e}")

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    win = ExplorePerformersWindow(conn)
    win.show()

    try:
        sys.exit(app.exec())
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
