import sys
import os
import json
import shutil
import base64
import requests

from PyQt6.QtCore import QThread, pyqtSignal, QObject, Qt, QSize, QRect, QPoint
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                           QLabel, QLineEdit, QCheckBox, QPushButton, QFileDialog,
                           QTextEdit, QGroupBox, QSpinBox, QDoubleSpinBox, QRadioButton, QButtonGroup,
                           QProgressBar, QTableWidget, QTableWidgetItem, QHeaderView, QComboBox,
                           QPlainTextEdit, QScrollArea, QMessageBox, QDialog, QMenuBar,
                           QMenu, QSizePolicy, QSplitter, QFrame, QPushButton, QFrame,
                           QSizePolicy, QSpacerItem, QInputDialog, QLayout)


from PyQt6.QtGui import QPixmap, QImage, QPalette, QColor, QFont, QIcon


from . import llmii
from . import help_text

class GuiConfig:
    """ Configuration class for GUI dimensions and properties
    """
    WINDOW_WIDTH = 704
    WINDOW_HEIGHT = 720
    WINDOW_FIXED = False
    
    IMAGE_PREVIEW_WIDTH = 340
    IMAGE_PREVIEW_HEIGHT = 450
    
    METADATA_WIDTH = 360
    METADATA_HEIGHT = 450
    
    LOG_WIDTH = 700
    LOG_HEIGHT = 140
    
    CONTROL_PANEL_HEIGHT = 75
    SPLITTER_HANDLE_WIDTH = 4
    
    SETTINGS_HEIGHT = 660
    SETTINGS_WIDTH = 460
    
    FONT_SIZE_NORMAL = 9
    FONT_SIZE_HEADER = 10
    
    COLOR_KEYWORD_BG = "#e1f0ff"
    COLOR_KEYWORD_TEXT = "#0066cc"
    COLOR_KEYWORD_BORDER = "#99ccff"
    COLOR_CAPTION_BG = "#f9f9f9"
    COLOR_BORDER = "#cccccc"
    
    CONTENT_MARGINS = 1
    SPACING = 1
    KEYWORDS_PER_ROW = 5
    FILENAME_LABEL_HEIGHT = 20
    CAPTION_BOX_HEIGHT = 200
    KEYWORDS_BOX_HEIGHT = abs(METADATA_HEIGHT - (FILENAME_LABEL_HEIGHT + CAPTION_BOX_HEIGHT))
    DEFAULT_INSTRUCTION = """Return a JSON object containing a Description for the image and a list of Keywords.

Write the Description using the active voice.

Generate 5 to 10 Keywords. Each Keyword is an item in a list and will be composed of a maximum of two words.

For both Description and Keywords, make sure to include:

 - Themes, concepts
 - Items, animals, objects
 - Structures, landmarks, setting
 - Foreground and background elements
 - Notable colors, textures, styles
 - Actions, activities

If humans are present, include:
 - Physical appearance
 - Gender
 - Clothing
 - Age range
 - Visibly apparent ancestry
 - Occupation/role
 - Relationships between individuals
 - Emotions, expressions, body language

Use ENGLISH only. Generate ONLY a JSON object with the keys Description and Keywords as follows {"Description": str, "Keywords": []}"""
                
class InstructionDialog(QDialog):
    def __init__(self, instruction_text, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Instruction")
        self.setModal(True)
        self.resize(700, 500)

        layout = QVBoxLayout(self)

        self.instruction_input = QPlainTextEdit()
        self.instruction_input.setPlainText(instruction_text)
        layout.addWidget(QLabel("Edit Instruction:"))
        layout.addWidget(self.instruction_input)

        button_layout = QHBoxLayout()
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.accept)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

    def get_instruction(self):
        return self.instruction_input.toPlainText()

class SkipFoldersDialog(QDialog):
    def __init__(self, skip_folders_text, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Skip Folders")
        self.setModal(True)
        self.resize(700, 400)

        layout = QVBoxLayout(self)

        help_label = QLabel("Enter folder names or paths to skip (one per line or separated by semicolons).\nYou can use full paths or paths relative to the working directory.")
        help_label.setWordWrap(True)
        layout.addWidget(help_label)

        self.skip_folders_input = QPlainTextEdit()
        self.skip_folders_input.setPlainText(skip_folders_text)
        layout.addWidget(QLabel("Skip Folders:"))
        layout.addWidget(self.skip_folders_input)

        button_layout = QHBoxLayout()
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.accept)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

    def get_skip_folders(self):
        return self.skip_folders_input.toPlainText()

class SettingsHelpDialog(QDialog):
    """ Dialog that shows help information for all settings """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings Help")
        self.resize(600, 500)
        
        layout = QVBoxLayout(self)
        
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        
        from src.help_text import get_settings_help
        help_label = QLabel(get_settings_help())
        help_label.setWordWrap(True)
        help_label.setTextFormat(Qt.TextFormat.RichText)
        help_label.setOpenExternalLinks(True)
        
        scroll_area.setWidget(help_label)
        layout.addWidget(scroll_area)
        
        button_layout = QHBoxLayout()
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        button_layout.addStretch(1)
        button_layout.addWidget(close_button)
        layout.addLayout(button_layout)
        
class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.resize(GuiConfig.SETTINGS_WIDTH, GuiConfig.SETTINGS_HEIGHT)
        
        layout = QVBoxLayout(self)
        
        # Scroll area in case it gets too long
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        
        api_layout = QHBoxLayout()
        self.api_url_input = QLineEdit("http://localhost:5001")
        self.api_password_input = QLineEdit()
        api_layout.addWidget(QLabel("API URL:"))
        api_layout.addWidget(self.api_url_input)
        api_layout.addWidget(QLabel("API Password:"))
        api_layout.addWidget(self.api_password_input)
        scroll_layout.addLayout(api_layout)

        system_instruction_layout = QHBoxLayout()
        self.system_instruction_input = QLineEdit("You are a helpful assistant.")
        system_instruction_layout.addWidget(QLabel("System Instruction:"))
        system_instruction_layout.addWidget(self.system_instruction_input)
        scroll_layout.addLayout(system_instruction_layout)

        instruction_button_layout = QHBoxLayout()
        self.edit_instruction_button = QPushButton("Edit Instruction")
        self.edit_instruction_button.clicked.connect(self.edit_instruction)
        instruction_button_layout.addWidget(self.edit_instruction_button)
        scroll_layout.addLayout(instruction_button_layout)

        skip_folders_button_layout = QHBoxLayout()
        self.edit_skip_folders_button = QPushButton("Edit Skip Folders")
        self.edit_skip_folders_button.clicked.connect(self.edit_skip_folders)
        skip_folders_button_layout.addWidget(self.edit_skip_folders_button)
        scroll_layout.addLayout(skip_folders_button_layout)

        ext_filter_layout = QHBoxLayout()
        self.image_extensions_filter_input = QLineEdit("jpg,jpeg,webp,zip")
        ext_filter_layout.addWidget(QLabel("Image extensions (comma-separated):"))
        ext_filter_layout.addWidget(self.image_extensions_filter_input)
        scroll_layout.addLayout(ext_filter_layout)

        tags_file_layout = QHBoxLayout()
        self.tags_file_input = QLineEdit("mastertags.json")
        tags_file_layout.addWidget(QLabel("Tags file:"))
        tags_file_layout.addWidget(self.tags_file_input)
        scroll_layout.addLayout(tags_file_layout)

        caption_group = QGroupBox("Caption Options")
        caption_layout = QVBoxLayout()

        caption_instruction_layout = QHBoxLayout()
        self.caption_instruction_input = QLineEdit("Describe the image in detail. Be specific.")
        caption_instruction_layout.addWidget(QLabel("Caption Instruction:"))
        caption_instruction_layout.addWidget(self.caption_instruction_input)
        caption_layout.addLayout(caption_instruction_layout)

        tag_instruction_layout = QHBoxLayout()
        self.tag_instruction_input = QLineEdit('Return a JSON object with key Keywords with the value as array of Keywords and tags that describe the image as follows: {"Keywords": []}')
        tag_instruction_layout.addWidget(QLabel("Tag Instruction:"))
        tag_instruction_layout.addWidget(self.tag_instruction_input)
        caption_layout.addLayout(tag_instruction_layout)

        self.caption_radio_group = QButtonGroup(self)
        self.detailed_caption_radio = QRadioButton("Separate caption query")
        self.short_caption_radio = QRadioButton("Combined caption query")
        self.no_caption_radio = QRadioButton("No caption query")

        self.caption_radio_group.addButton(self.detailed_caption_radio)
        self.caption_radio_group.addButton(self.short_caption_radio)
        self.caption_radio_group.addButton(self.no_caption_radio)

        self.short_caption_radio.setChecked(True)

        caption_layout.addWidget(self.detailed_caption_radio)
        caption_layout.addWidget(self.short_caption_radio)
        caption_layout.addWidget(self.no_caption_radio)

        caption_group.setLayout(caption_layout)
        scroll_layout.addWidget(caption_group)

        gen_count_layout = QHBoxLayout()
        self.gen_count = QSpinBox()
        self.gen_count.setMinimum(50)
        self.gen_count.setMaximum(1000)
        self.gen_count.setValue(250)
        gen_count_layout.addWidget(QLabel("GenTokens: "))
        gen_count_layout.addWidget(self.gen_count)
        scroll_layout.addLayout(gen_count_layout)
        
        res_limit_layout = QHBoxLayout()
        self.res_limit = QSpinBox()
        self.res_limit.setMinimum(112)
        self.res_limit.setMaximum(896)
        self.res_limit.setValue(448)
        self.res_limit.setSingleStep(14)
        res_limit_layout.addWidget(QLabel("Dimension length: "))
        res_limit_layout.addWidget(self.res_limit)
        scroll_layout.addLayout(res_limit_layout)

        # Sampler Settings Group
        sampler_group = QGroupBox("Sampler Settings")
        sampler_layout = QVBoxLayout()

        # Temperature
        temp_layout = QHBoxLayout()
        self.temperature_spinbox = QDoubleSpinBox()
        self.temperature_spinbox.setMinimum(0.0)
        self.temperature_spinbox.setMaximum(2.0)
        self.temperature_spinbox.setValue(0.2)
        self.temperature_spinbox.setSingleStep(0.05)
        self.temperature_spinbox.setDecimals(2)
        temp_layout.addWidget(QLabel("Temperature:"))
        temp_layout.addWidget(self.temperature_spinbox)
        temp_layout.addStretch()
        sampler_layout.addLayout(temp_layout)

        # Top P
        top_p_layout = QHBoxLayout()
        self.top_p_spinbox = QDoubleSpinBox()
        self.top_p_spinbox.setMinimum(0.0)
        self.top_p_spinbox.setMaximum(1.0)
        self.top_p_spinbox.setValue(1.0)
        self.top_p_spinbox.setSingleStep(0.01)
        self.top_p_spinbox.setDecimals(2)
        top_p_layout.addWidget(QLabel("Top P:"))
        top_p_layout.addWidget(self.top_p_spinbox)
        top_p_layout.addStretch()
        sampler_layout.addLayout(top_p_layout)

        # Top K
        top_k_layout = QHBoxLayout()
        self.top_k_spinbox = QSpinBox()
        self.top_k_spinbox.setMinimum(0)
        self.top_k_spinbox.setMaximum(100)
        self.top_k_spinbox.setValue(100)
        top_k_layout.addWidget(QLabel("Top K:"))
        top_k_layout.addWidget(self.top_k_spinbox)
        top_k_layout.addStretch()
        sampler_layout.addLayout(top_k_layout)

        # Min P
        min_p_layout = QHBoxLayout()
        self.min_p_spinbox = QDoubleSpinBox()
        self.min_p_spinbox.setMinimum(0.0)
        self.min_p_spinbox.setMaximum(2.0)
        self.min_p_spinbox.setValue(0.05)
        self.min_p_spinbox.setSingleStep(0.01)
        self.min_p_spinbox.setDecimals(2)
        min_p_layout.addWidget(QLabel("Min P:"))
        min_p_layout.addWidget(self.min_p_spinbox)
        min_p_layout.addStretch()
        sampler_layout.addLayout(min_p_layout)

        # Repetition Penalty
        rep_pen_layout = QHBoxLayout()
        self.rep_pen_spinbox = QDoubleSpinBox()
        self.rep_pen_spinbox.setMinimum(1.0)
        self.rep_pen_spinbox.setMaximum(2.0)
        self.rep_pen_spinbox.setValue(1.01)
        self.rep_pen_spinbox.setSingleStep(0.01)
        self.rep_pen_spinbox.setDecimals(2)
        rep_pen_layout.addWidget(QLabel("Repetition Penalty:"))
        rep_pen_layout.addWidget(self.rep_pen_spinbox)
        rep_pen_layout.addStretch()
        sampler_layout.addLayout(rep_pen_layout)

        sampler_group.setLayout(sampler_layout)
        scroll_layout.addWidget(sampler_group)

        # JSON Grammar Group
        json_grammar_group = QGroupBox("Structured Output")
        json_grammar_layout = QVBoxLayout()

        self.use_json_grammar_checkbox = QCheckBox("Use JSON grammar to force structured output")
        self.use_json_grammar_checkbox.setChecked(False)
        json_grammar_layout.addWidget(self.use_json_grammar_checkbox)

        json_grammar_group.setLayout(json_grammar_layout)
        scroll_layout.addWidget(json_grammar_group)

        options_group = QGroupBox("File Options")
        options_layout = QVBoxLayout()
        
        self.no_crawl_checkbox = QCheckBox("Don't go in subdirectories")
        self.reprocess_all_checkbox = QCheckBox("Reprocess everything")
        self.reprocess_failed_checkbox = QCheckBox("Reprocess failures")
        self.reprocess_orphans_checkbox = QCheckBox("Fix any orphans")
        self.no_backup_checkbox = QCheckBox("No backups")
        self.dry_run_checkbox = QCheckBox("Pretend mode")
        self.skip_verify_checkbox = QCheckBox("No file validation")
        self.quick_fail_checkbox = QCheckBox("No retries")
        self.rename_invalid_checkbox = QCheckBox("Rename files that cannot be processed to .invalid")
        self.preserve_date_checkbox = QCheckBox("Preserve file modification date (may create temp files)")
        self.fix_extension_checkbox = QCheckBox("Fix file extension if it doesn't match file type")
        #self.write_unsafe_checkbox = QCheckBox("Use unsafe flag when writing metadata")

        # Sparse reprocess row: checkbox + spinbox inline
        self.reprocess_sparse_checkbox = QCheckBox("Reprocess images with fewer than")
        self.reprocess_sparse_checkbox.setToolTip(
            "Re-run the LLM on images whose matched keyword count is below the threshold"
        )
        self.reprocess_sparse_spinbox = QSpinBox()
        self.reprocess_sparse_spinbox.setRange(1, 999)
        self.reprocess_sparse_spinbox.setValue(5)
        self.reprocess_sparse_spinbox.setFixedWidth(60)
        sparse_row = QHBoxLayout()
        sparse_row.setContentsMargins(0, 0, 0, 0)
        sparse_row.addWidget(self.reprocess_sparse_checkbox)
        sparse_row.addWidget(self.reprocess_sparse_spinbox)
        sparse_row.addWidget(QLabel("matched keywords"))
        sparse_row.addStretch()

        options_layout.addWidget(self.no_crawl_checkbox)
        options_layout.addWidget(self.reprocess_all_checkbox)
        options_layout.addWidget(self.reprocess_failed_checkbox)
        options_layout.addWidget(self.reprocess_orphans_checkbox)
        options_layout.addLayout(sparse_row)
        options_layout.addWidget(self.no_backup_checkbox)
        options_layout.addWidget(self.dry_run_checkbox)
        options_layout.addWidget(self.skip_verify_checkbox)
        options_layout.addWidget(self.quick_fail_checkbox)
        options_layout.addWidget(self.rename_invalid_checkbox)
        options_layout.addWidget(self.preserve_date_checkbox)
        options_layout.addWidget(self.fix_extension_checkbox)
        #options_layout.addWidget(self.write_unsafe_checkbox)
        
        options_group.setLayout(options_layout)
        scroll_layout.addWidget(options_group)

        sidecar_group = QGroupBox("JSON Sidecar Location")
        sidecar_layout = QVBoxLayout()

        self.sidecar_with_image_radio = QRadioButton("Save alongside images")
        self.sidecar_custom_dir_radio = QRadioButton("Save to directory:")
        self.sidecar_with_image_radio.setChecked(True)

        sidecar_dir_layout = QHBoxLayout()
        self.sidecar_dir_input = QLineEdit()
        self.sidecar_dir_input.setEnabled(False)
        self.sidecar_dir_input.setPlaceholderText("Path to sidecar output directory...")
        sidecar_browse_button = QPushButton("Browse")
        sidecar_browse_button.setEnabled(False)
        sidecar_browse_button.clicked.connect(self._browse_sidecar_dir)
        sidecar_dir_layout.addWidget(self.sidecar_dir_input)
        sidecar_dir_layout.addWidget(sidecar_browse_button)

        self.sidecar_custom_dir_radio.toggled.connect(self.sidecar_dir_input.setEnabled)
        self.sidecar_custom_dir_radio.toggled.connect(sidecar_browse_button.setEnabled)

        sidecar_layout.addWidget(self.sidecar_with_image_radio)
        sidecar_layout.addWidget(self.sidecar_custom_dir_radio)
        sidecar_layout.addLayout(sidecar_dir_layout)
        sidecar_group.setLayout(sidecar_layout)
        scroll_layout.addWidget(sidecar_group)

        # ---- Output mode + Database connection ----
        output_group = QGroupBox("Output Mode")
        output_layout = QVBoxLayout()

        self.output_json_radio  = QRadioButton("JSON sidecar files only")
        self.output_db_radio    = QRadioButton("PostgreSQL database only")
        self.output_both_radio  = QRadioButton("Both JSON and database")
        self.output_json_radio.setChecked(True)

        self._output_mode_group = QButtonGroup(self)
        self._output_mode_group.addButton(self.output_json_radio)
        self._output_mode_group.addButton(self.output_db_radio)
        self._output_mode_group.addButton(self.output_both_radio)

        output_layout.addWidget(self.output_json_radio)
        output_layout.addWidget(self.output_db_radio)
        output_layout.addWidget(self.output_both_radio)
        output_group.setLayout(output_layout)
        scroll_layout.addWidget(output_group)

        db_group = QGroupBox("Database Connection")
        db_layout = QVBoxLayout()

        db_host_layout = QHBoxLayout()
        db_host_layout.addWidget(QLabel("Host:"))
        self.db_host_input = QLineEdit()
        self.db_host_input.setPlaceholderText("localhost")
        db_host_layout.addWidget(self.db_host_input)
        db_host_layout.addWidget(QLabel("Port:"))
        self.db_port_input = QSpinBox()
        self.db_port_input.setRange(1, 65535)
        self.db_port_input.setValue(5432)
        self.db_port_input.setFixedWidth(70)
        db_host_layout.addWidget(self.db_port_input)

        db_creds_layout = QHBoxLayout()
        db_creds_layout.addWidget(QLabel("User:"))
        self.db_user_input = QLineEdit()
        db_creds_layout.addWidget(self.db_user_input)
        db_creds_layout.addWidget(QLabel("Password:"))
        self.db_pass_input = QLineEdit()
        self.db_pass_input.setEchoMode(QLineEdit.EchoMode.Password)
        db_creds_layout.addWidget(self.db_pass_input)

        db_name_layout = QHBoxLayout()
        db_name_layout.addWidget(QLabel("Database:"))
        self.db_name_input = QLineEdit()
        db_name_layout.addWidget(self.db_name_input)

        db_btn_layout = QHBoxLayout()
        db_test_button = QPushButton("Test Connection")
        db_test_button.clicked.connect(self._test_db_connection)
        db_load_tags_button = QPushButton("Load Tag File into DB...")
        db_load_tags_button.clicked.connect(self._load_tags_to_db)
        db_export_tags_button = QPushButton("Export Tags...")
        db_export_tags_button.clicked.connect(self._export_tags)
        db_clear_button = QPushButton("Clear Database")
        db_clear_button.clicked.connect(self._clear_database)
        db_backfill_button = QPushButton("Backfill Colored Hair")
        db_backfill_button.setToolTip(
            "Promote unmatched hair-colour keywords (blue hair, pink hair, etc.) "
            "from the unmatched table to 'Colored Hair' or 'Multicolored Hair'."
        )
        db_backfill_button.clicked.connect(self._backfill_colored_hair)
        db_backfill_all_button = QPushButton("Backfill All Normalizers")
        db_backfill_all_button.setToolTip(
            "Apply all post-processing normalizers (nudity level, pubic hair, labia) "
            "to unmatched keywords and promote matches to the keywords table."
        )
        db_backfill_all_button.clicked.connect(self._backfill_normalizers)
        db_btn_layout.addWidget(db_test_button)
        db_btn_layout.addWidget(db_load_tags_button)
        db_btn_layout.addWidget(db_export_tags_button)
        db_btn_layout.addWidget(db_backfill_button)
        db_btn_layout.addWidget(db_backfill_all_button)
        db_btn_layout.addWidget(db_clear_button)
        db_btn_layout.addStretch(1)

        db_btn_layout2 = QHBoxLayout()
        db_rename_tag_button = QPushButton("Rename Tag…")
        db_rename_tag_button.setToolTip("Rename a canonical tag (all FK references preserved)")
        db_rename_tag_button.clicked.connect(self._rename_tag)
        db_merge_tag_button = QPushButton("Merge Tags…")
        db_merge_tag_button.setToolTip(
            "Merge one canonical tag into another — reassigns image keywords, "
            "transfers aliases, and adds the source name as an alias for the target"
        )
        db_merge_tag_button.clicked.connect(self._merge_tags)
        db_backfill_raw_button = QPushButton("Backfill From Raw")
        db_backfill_raw_button.setToolTip(
            "Apply all normalizers to every raw LLM keyword ever emitted (image_keywords_raw) "
            "and promote matches directly to image_keywords. "
            "Useful after adding new normalizer rules."
        )
        db_backfill_raw_button.clicked.connect(self._backfill_from_raw)
        db_promote_button = QPushButton("Promote Aliased Unmatched")
        db_promote_button.setToolTip(
            "Find unmatched keywords that have since gained an alias (added via tag_review) "
            "and move them to image_keywords automatically."
        )
        db_promote_button.clicked.connect(self._promote_aliased_unmatched)
        db_stats_button = QPushButton("DB Stats")
        db_stats_button.setToolTip("Show database coverage statistics.")
        db_stats_button.clicked.connect(self._db_stats)
        db_health_button = QPushButton("Health Check")
        db_health_button.setToolTip(
            "Check for stuck tagger runs, promotable unmatched keywords, "
            "and other database health issues."
        )
        db_health_button.clicked.connect(self._db_health_check)
        db_btn_layout2.addWidget(db_rename_tag_button)
        db_btn_layout2.addWidget(db_merge_tag_button)
        db_btn_layout2.addWidget(db_backfill_raw_button)
        db_btn_layout2.addWidget(db_promote_button)
        db_btn_layout2.addWidget(db_stats_button)
        db_btn_layout2.addWidget(db_health_button)
        db_btn_layout2.addStretch(1)

        db_btn_layout3 = QHBoxLayout()
        db_run_history_button = QPushButton("Run History")
        db_run_history_button.setToolTip("Show all tagger runs with status, duration, and model name.")
        db_run_history_button.clicked.connect(self._db_run_history)
        db_find_orphans_button = QPushButton("Find Orphans")
        db_find_orphans_button.setToolTip(
            "Find images in the database whose files no longer exist on disk, "
            "and optionally remove those records."
        )
        db_find_orphans_button.clicked.connect(self._db_find_orphans)
        db_export_csv_button = QPushButton("Export Keywords CSV…")
        db_export_csv_button.setToolTip(
            "Export all images with their canonical keywords to a CSV file "
            "(columns: path, gallery, keywords)."
        )
        db_export_csv_button.clicked.connect(self._db_export_csv)
        db_assign_performer_tags_button = QPushButton("Assign Performer Tags")
        db_assign_performer_tags_button.setToolTip(
            "For each performer, assign any tag that appears on more than 40% of "
            "that performer's images to the performer_tags table. "
            "Removes tags that no longer meet the threshold."
        )
        db_assign_performer_tags_button.clicked.connect(self._assign_performer_tags)
        db_btn_layout3.addWidget(db_run_history_button)
        db_btn_layout3.addWidget(db_find_orphans_button)
        db_btn_layout3.addWidget(db_export_csv_button)
        db_btn_layout3.addWidget(db_assign_performer_tags_button)
        db_btn_layout3.addStretch(1)

        db_layout.addLayout(db_host_layout)
        db_layout.addLayout(db_creds_layout)
        db_layout.addLayout(db_name_layout)
        db_layout.addLayout(db_btn_layout)
        db_layout.addLayout(db_btn_layout2)
        db_layout.addLayout(db_btn_layout3)
        db_group.setLayout(db_layout)
        scroll_layout.addWidget(db_group)

        # Enable/disable DB fields based on output mode
        def _update_db_fields():
            enabled = not self.output_json_radio.isChecked()
            db_group.setEnabled(enabled)

        self.output_json_radio.toggled.connect(_update_db_fields)
        self.output_db_radio.toggled.connect(_update_db_fields)
        self.output_both_radio.toggled.connect(_update_db_fields)

        # ---- Zip extraction temp folder ----
        zip_group = QGroupBox("Zip Extraction")
        zip_layout = QVBoxLayout()

        temp_folder_layout = QHBoxLayout()
        temp_folder_layout.addWidget(QLabel("Temp folder:"))
        self.temp_folder_input = QLineEdit()
        self.temp_folder_input.setPlaceholderText("temp")
        self.temp_folder_input.setToolTip(
            "Directory where zip files are temporarily extracted.\n"
            "Relative paths are resolved from the application working directory.\n"
            "Cleared at the start of every run."
        )
        temp_folder_layout.addWidget(self.temp_folder_input)
        temp_folder_browse = QPushButton("Browse")
        temp_folder_browse.clicked.connect(self._browse_temp_folder)
        temp_folder_layout.addWidget(temp_folder_browse)

        zip_layout.addLayout(temp_folder_layout)
        zip_group.setLayout(zip_layout)
        scroll_layout.addWidget(zip_group)

        xmp_group = QGroupBox("Existing Metadata")
        xmp_layout = QVBoxLayout()
        
        self.update_keywords_checkbox = QCheckBox("Don't clear existing keywords (new will be added)")
        self.update_keywords_checkbox.setChecked(True)
        self.update_caption_checkbox = QCheckBox("Don't clear existing caption (new will be added surrounded by tags)")
        self.update_caption_checkbox.setChecked(False)
        
        xmp_layout.addWidget(self.update_keywords_checkbox)
        xmp_layout.addWidget(self.update_caption_checkbox)
        
        xmp_group.setLayout(xmp_layout)
        scroll_layout.addWidget(xmp_group)

        keyword_corrections_group = QGroupBox("Keyword Corrections")
        corrections_layout = QVBoxLayout()
        
        self.depluralize_checkbox = QCheckBox("Depluralize keywords")
        self.depluralize_checkbox.setChecked(False)
        self.word_limit_layout = QHBoxLayout()
        self.word_limit_checkbox = QCheckBox("Limit to")
        self.word_limit_spinbox = QSpinBox()
        self.word_limit_spinbox.setMinimum(1)
        self.word_limit_spinbox.setMaximum(5)
        self.word_limit_spinbox.setValue(2)
        self.word_limit_layout.addWidget(self.word_limit_checkbox)
        self.word_limit_layout.addWidget(self.word_limit_spinbox)
        self.word_limit_layout.addWidget(QLabel("words in keyword entry"))
        self.word_limit_layout.addStretch(1)
        self.word_limit_checkbox.setChecked(True)
        self.split_and_checkbox = QCheckBox("Split 'and'/'or' entries")
        self.split_and_checkbox.setChecked(True)
        self.ban_prompt_words_checkbox = QCheckBox("Ban prompt word repetitions")
        self.ban_prompt_words_checkbox.setChecked(True)
        self.no_digits_start_checkbox = QCheckBox("Cannot start with 3+ digits")
        self.no_digits_start_checkbox.setChecked(True)
        self.min_word_length_checkbox = QCheckBox("Words must be 2+ characters")
        self.min_word_length_checkbox.setChecked(True)
        self.latin_only_checkbox = QCheckBox("Only Latin characters")
        self.latin_only_checkbox.setChecked(True)

        tag_blacklist_layout = QVBoxLayout()
        blacklist_header = QHBoxLayout()
        blacklist_header.addWidget(QLabel("Blacklisted tag words (one per line):"))
        blacklist_header.addStretch()
        tag_blacklist_layout.addLayout(blacklist_header)
        self.tag_blacklist_input = QPlainTextEdit()
        self.tag_blacklist_input.setPlaceholderText("assorted\nbondage\nexplicit")
        self.tag_blacklist_input.setMaximumHeight(80)
        self.tag_blacklist_input.setToolTip(
            "Keywords containing any of these substrings (one per line) will be "
            "blacklisted and never matched to a canonical tag."
        )
        tag_blacklist_layout.addWidget(self.tag_blacklist_input)

        tag_fuzzy_layout = QHBoxLayout()
        self.tag_fuzzy_spinbox = QSpinBox()
        self.tag_fuzzy_spinbox.setMinimum(50)
        self.tag_fuzzy_spinbox.setMaximum(100)
        self.tag_fuzzy_spinbox.setValue(88)
        self.tag_fuzzy_spinbox.setSingleStep(1)
        tag_fuzzy_layout.addWidget(QLabel("Tag fuzzy match threshold:"))
        tag_fuzzy_layout.addWidget(self.tag_fuzzy_spinbox)
        tag_fuzzy_layout.addStretch(1)

        corrections_layout.addWidget(self.depluralize_checkbox)
        corrections_layout.addLayout(self.word_limit_layout)
        corrections_layout.addWidget(self.split_and_checkbox)
        corrections_layout.addWidget(self.ban_prompt_words_checkbox)
        corrections_layout.addWidget(self.no_digits_start_checkbox)
        corrections_layout.addWidget(self.min_word_length_checkbox)
        corrections_layout.addWidget(self.latin_only_checkbox)
        corrections_layout.addLayout(tag_blacklist_layout)
        corrections_layout.addLayout(tag_fuzzy_layout)

        keyword_corrections_group.setLayout(corrections_layout)
        scroll_layout.addWidget(keyword_corrections_group)
        
        scroll_area.setWidget(scroll_content)
        layout.addWidget(scroll_area, 1)
        
        button_layout = QHBoxLayout()
        help_button = QPushButton("Help")
        help_button.clicked.connect(self.show_help)
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.accept)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(help_button)
        button_layout.addStretch(1)
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        self.instruction_text = GuiConfig.DEFAULT_INSTRUCTION
        self.skip_folders_text = ""

        self.load_settings()
    
    def show_help(self):
        """Show the settings help dialog"""
        dialog = SettingsHelpDialog(self)
        dialog.exec()
        
    def edit_instruction(self):
        dialog = InstructionDialog(self.instruction_text, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.instruction_text = dialog.get_instruction()

    def edit_skip_folders(self):
        dialog = SkipFoldersDialog(self.skip_folders_text, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.skip_folders_text = dialog.get_skip_folders()

    def _browse_sidecar_dir(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Sidecar Output Directory")
        if directory:
            self.sidecar_dir_input.setText(directory)

    def _browse_temp_folder(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Zip Extraction Temp Folder")
        if directory:
            self.temp_folder_input.setText(directory)

    def _get_db_connection(self):
        """Open a DB connection using current field values. Returns conn or None."""
        from . import llmii_db
        if not llmii_db.HAS_PSYCOPG2:
            QMessageBox.critical(self, "Missing Dependency",
                "psycopg2 is not installed.\nRun: pip install psycopg2-binary")
            return None
        try:
            return llmii_db.get_connection(
                host     = self.db_host_input.text().strip() or 'localhost',
                port     = self.db_port_input.value(),
                user     = self.db_user_input.text().strip(),
                password = self.db_pass_input.text(),
                dbname   = self.db_name_input.text().strip(),
            )
        except Exception as e:
            QMessageBox.critical(self, "Connection Failed", str(e))
            return None

    def _run_db_async(self, op_name, db_fn, on_success):
        """Run db_fn(conn) in a background thread.

        The button that triggered the call (via self.sender()) is disabled
        and relabelled while the operation runs, then restored on completion.
        on_success(result) is called on the main thread when done.
        An error QMessageBox is shown automatically on failure.
        """
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return

        btn = self.sender()
        orig_text = btn.text() if btn else ''
        if btn:
            btn.setEnabled(False)
            btn.setText(f"{orig_text}…")

        def _done(result):
            if btn:
                btn.setEnabled(True)
                btn.setText(orig_text)
            try:
                conn.close()
            except Exception:
                pass
            on_success(result)

        def _err(msg):
            if btn:
                btn.setEnabled(True)
                btn.setText(orig_text)
            try:
                conn.close()
            except Exception:
                pass
            QMessageBox.critical(self, f"{op_name} Failed", msg)

        # Keep a reference so the GC doesn't collect the worker mid-run.
        self._active_db_worker = _DbOpWorker(db_fn, conn)
        self._active_db_worker.finished.connect(_done)
        self._active_db_worker.error.connect(_err)
        self._active_db_worker.start()

    def _test_db_connection(self):
        conn = self._get_db_connection()
        if conn:
            conn.close()
            QMessageBox.information(self, "Connection Successful",
                "Successfully connected to the PostgreSQL database.")

    def _load_tags_to_db(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Tag File", "", "JSON Files (*.json);;All Files (*)"
        )
        if not path:
            return
        conn = self._get_db_connection()
        if not conn:
            return
        from . import llmii_db
        try:
            stats = llmii_db.load_tags_from_file(conn, path)
            conn.close()
            QMessageBox.information(
                self, "Tag File Loaded",
                f"File: {path}\n\n"
                f"Tags:    {stats['tags_added']} added,  {stats['tags_skipped']} already existed\n"
                f"Aliases: {stats['aliases_added']} added,  {stats['aliases_skipped']} already existed",
            )
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Load Failed", str(e))

    def _export_tags(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            rows = llmii_db.export_tags(conn)
            conn.close()
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", str(e))
            return

        if not rows:
            QMessageBox.information(self, "No Tags", "No tags or aliases found in the database.")
            return

        from PyQt6.QtWidgets import QFileDialog
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Tags", "tags_export.json",
            "JSON Files (*.json);;All Files (*)"
        )
        if not path:
            return
        try:
            import json as _json
            with open(path, 'w', encoding='utf-8') as f:
                _json.dump(rows, f, indent=2, ensure_ascii=False)
            QMessageBox.information(self, "Export Complete",
                f"Exported {len(rows)} alias rows to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", str(e))

    def _backfill_normalizers(self):
        from . import llmii_db
        def _show(counts):
            total = sum(counts.values())
            if total == 0:
                QMessageBox.information(self, "Backfill Complete",
                    "No unmatched keywords matched any normalizer.")
            else:
                lines = "\n".join(f"  • {count} → {tag}" for tag, count in sorted(counts.items()))
                QMessageBox.information(self, "Backfill Complete",
                    f"Promoted {total} keyword(s):\n{lines}")
        self._run_db_async("Backfill All Normalizers", llmii_db.backfill_normalizers, _show)

    def _backfill_colored_hair(self):
        from . import llmii_db
        def _show(result):
            colored, multicolored = result
            total = colored + multicolored
            if total == 0:
                QMessageBox.information(self, "Backfill Complete",
                    "No unmatched hair-colour keywords found to promote.")
            else:
                QMessageBox.information(self, "Backfill Complete",
                    f"Promoted {total} keyword(s):\n"
                    f"  • {colored} → Colored Hair\n"
                    f"  • {multicolored} → Multicolored Hair")
        self._run_db_async("Backfill Colored Hair", llmii_db.backfill_colored_hair, _show)

    def _assign_performer_tags(self):
        from . import llmii_db
        def _show(result):
            checked  = result.get('performers_checked', 0)
            assigned = result.get('tags_assigned', 0)
            removed  = result.get('tags_removed', 0)
            if checked == 0:
                QMessageBox.information(self, "Assign Performer Tags",
                    "No performers with processed images found.")
            else:
                QMessageBox.information(self, "Assign Performer Tags",
                    f"Checked {checked} performer(s).\n"
                    f"  • {assigned} tag assignment(s) inserted/updated\n"
                    f"  • {removed} stale assignment(s) removed")
        self._run_db_async("Assign Performer Tags", llmii_db.assign_performer_tags, _show)

    def _rename_tag(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        try:
            cur.execute("SELECT tag FROM tags ORDER BY tag")
            all_tags = sorted([r[0] for r in cur.fetchall()], key=str.lower)
            conn.commit()
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Error", f"Could not fetch tags:\n{e}")
            return
        finally:
            cur.close()

        if not all_tags:
            conn.close()
            QMessageBox.information(self, "No Tags", "No canonical tags found in the database.")
            return

        source, ok = QInputDialog.getItem(
            self, "Rename Tag", "Select tag to rename:", all_tags, 0, False
        )
        if not ok or not source:
            conn.close()
            return

        new_name, ok = QInputDialog.getText(
            self, "Rename Tag", f"New name for '{source}':", text=source
        )
        if not ok:
            conn.close()
            return
        new_name = new_name.strip()
        if not new_name or new_name.lower() == source.lower():
            conn.close()
            return

        try:
            llmii_db.rename_tag(conn, source, new_name)
            conn.close()
            QMessageBox.information(self, "Rename Complete",
                f"Renamed '{source}' → '{new_name}'.")
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Rename Failed", str(e))

    def _merge_tags(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        try:
            cur.execute("SELECT tag FROM tags ORDER BY tag")
            all_tags = sorted([r[0] for r in cur.fetchall()], key=str.lower)
            conn.commit()
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Error", f"Could not fetch tags:\n{e}")
            return
        finally:
            cur.close()

        if len(all_tags) < 2:
            conn.close()
            QMessageBox.information(self, "Not Enough Tags",
                "At least two canonical tags are required to merge.")
            return

        source, ok = QInputDialog.getItem(
            self, "Merge Tags — Step 1", "Select source tag (will be deleted):",
            all_tags, 0, False
        )
        if not ok or not source:
            conn.close()
            return

        targets = [t for t in all_tags if t.lower() != source.lower()]
        target, ok = QInputDialog.getItem(
            self, "Merge Tags — Step 2", f"Merge '{source}' into:",
            targets, 0, False
        )
        if not ok or not target:
            conn.close()
            return

        reply = QMessageBox.question(
            self,
            "Confirm Merge",
            f"Merge '{source}' \u2192 '{target}'?\n\n"
            f"This will:\n"
            f"  \u2022  Reassign all image keywords from '{source}' to '{target}'\n"
            f"  \u2022  Move all of '{source}' aliases to '{target}'\n"
            f"  \u2022  Add '{source}' as an alias for '{target}'\n"
            f"  \u2022  Delete the '{source}' tag permanently\n\n"
            f"This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            conn.close()
            return

        try:
            reassigned = llmii_db.merge_tag(conn, source, target)
            conn.close()
            QMessageBox.information(self, "Merge Complete",
                f"Merged '{source}' \u2192 '{target}'\n"
                f"{reassigned} image keyword row(s) reassigned.\n"
                f"'{source}' is now an alias for '{target}'.")
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Merge Failed", str(e))

    def _backfill_from_raw(self):
        from . import llmii_db
        def _show(counts):
            total = sum(counts.values())
            if total == 0:
                QMessageBox.information(self, "Backfill From Raw Complete",
                    "No raw keywords matched any normalizer (or all were already in image_keywords).")
            else:
                lines = "\n".join(f"  • {count} → {tag}" for tag, count in sorted(counts.items()))
                QMessageBox.information(self, "Backfill From Raw Complete",
                    f"Inserted {total} new keyword row(s) from raw LLM output:\n{lines}")
        self._run_db_async("Backfill From Raw", llmii_db.backfill_from_raw, _show)

    def _promote_aliased_unmatched(self):
        from . import llmii_db
        def _show(promoted):
            if promoted == 0:
                QMessageBox.information(self, "Promote Complete",
                    "No unmatched keywords currently have aliases to promote.")
            else:
                QMessageBox.information(self, "Promote Complete",
                    f"Promoted {promoted} keyword row(s) from unmatched to image_keywords.")
        self._run_db_async("Promote Aliased Unmatched", llmii_db.promote_aliased_unmatched, _show)

    def _db_stats(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            stats = llmii_db.get_stats(conn)
            conn.close()
        except Exception as e:
            QMessageBox.critical(self, "Stats Failed", str(e))
            return
        top_tags_lines = "\n".join(
            f"    {i+1}. {tag}  ({count} images)"
            for i, (tag, count) in enumerate(stats['top_tags'])
        ) if stats['top_tags'] else "    (none)"
        msg = (
            f"Database Statistics\n"
            f"{'─' * 40}\n"
            f"Total images:          {stats['total_images']}\n"
            f"Processed images:      {stats['processed_images']}\n"
            f"Avg keywords/image:    {stats['avg_keywords']}\n"
            f"Zero-keyword images:   {stats['zero_keyword_images']}\n"
            f"Sparse images (1–4):   {stats['sparse_images']}\n"
            f"\n"
            f"Unmatched (total):     {stats['total_unmatched']}\n"
            f"Unmatched (unique):    {stats['unique_unmatched']}\n"
            f"\n"
            f"Tagger runs:           {stats['total_runs']}\n"
            f"Stuck runs:            {stats['stuck_runs']}\n"
            f"\n"
            f"Top {len(stats['top_tags'])} tags by image count:\n"
            f"{top_tags_lines}"
        )
        QMessageBox.information(self, "DB Stats", msg)

    def _db_health_check(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            health = llmii_db.health_check(conn)
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Health Check Failed", str(e))
            return

        issues = []
        if health['stuck_runs']:
            issues.append(
                f"  • {len(health['stuck_runs'])} stuck tagger run(s) "
                f"(status='running' for > 1 hour)"
            )
        if health['orphaned_keywords']:
            issues.append(
                f"  • {health['orphaned_keywords']} orphaned image_keywords row(s) "
                f"(tag_id without matching tag — referential integrity issue)"
            )
        promotable = health['promotable_unmatched']
        if promotable:
            issues.append(
                f"  • {promotable} unmatched keyword row(s) now have aliases "
                f"and can be promoted to image_keywords"
            )

        summary = (
            f"Health Check Results\n"
            f"{'─' * 40}\n"
            f"Total unmatched keywords: {health['total_unmatched']}\n"
            f"Promotable (have alias):  {promotable}\n"
            f"Orphaned keyword rows:    {health['orphaned_keywords']}\n"
            f"Stuck tagger runs:        {len(health['stuck_runs'])}\n"
        )
        if issues:
            summary += f"\nIssues found:\n" + "\n".join(issues)
            if promotable:
                summary += f"\n\nClick OK to promote the {promotable} aliased unmatched keyword(s) now."
                reply = QMessageBox.question(
                    self, "Health Check", summary,
                    QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
                    QMessageBox.StandardButton.Ok,
                )
                if reply == QMessageBox.StandardButton.Ok:
                    try:
                        promoted = llmii_db.promote_aliased_unmatched(conn)
                        conn.close()
                        QMessageBox.information(self, "Promote Complete",
                            f"Promoted {promoted} keyword row(s) to image_keywords.")
                    except Exception as e:
                        conn.close()
                        QMessageBox.critical(self, "Promote Failed", str(e))
                else:
                    conn.close()
            else:
                conn.close()
                QMessageBox.information(self, "Health Check", summary)
        else:
            conn.close()
            QMessageBox.information(self, "Health Check", summary + "\nNo issues found.")

    def _db_run_history(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            runs = llmii_db.get_run_history(conn)
            conn.close()
        except Exception as e:
            QMessageBox.critical(self, "Run History Failed", str(e))
            return

        if not runs:
            QMessageBox.information(self, "Run History", "No tagger runs found in the database.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Run History  ({len(runs)} run(s))")
        dlg.setMinimumSize(860, 480)
        layout = QVBoxLayout(dlg)

        table = QTableWidget(len(runs), 5)
        table.setHorizontalHeaderLabels(["ID", "Model", "Status", "Started", "Duration"])
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setAlternatingRowColors(True)
        table.setSortingEnabled(True)

        for row, run in enumerate(runs):
            dur = run['duration_s'] or 0
            dur_str = f"{dur // 3600}h {(dur % 3600) // 60}m {dur % 60}s" if dur >= 3600 else \
                      f"{dur // 60}m {dur % 60}s" if dur >= 60 else f"{dur}s"
            started = str(run['started_at'])[:19] if run['started_at'] else ''
            status_color = {'success': '#3a9e3a', 'running': '#e8a020', 'failed': '#c04040'}.get(
                run['status'], '#888'
            )
            for col, val in enumerate([str(run['id']), run['tagger_name'] or '', run['status'], started, dur_str]):
                item = QTableWidgetItem(val)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if col == 2:
                    item.setForeground(__import__('PyQt6.QtGui', fromlist=['QColor']).QColor(status_color))
                table.setItem(row, col, item)

        layout.addWidget(table)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.close)
        layout.addWidget(close_btn)
        dlg.exec()

    def _db_find_orphans(self):
        from . import llmii_db
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            orphans = llmii_db.find_orphaned_paths(conn)
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Find Orphans Failed", str(e))
            return

        if not orphans:
            conn.close()
            QMessageBox.information(self, "Find Orphans",
                "No orphaned records found — all image paths exist on disk.")
            return

        paths_text = "\n".join(p for _, p in orphans[:50])
        if len(orphans) > 50:
            paths_text += f"\n… and {len(orphans) - 50} more"

        reply = QMessageBox.question(
            self, "Orphaned Records Found",
            f"{len(orphans)} image record(s) have paths that no longer exist on disk:\n\n"
            f"{paths_text}\n\n"
            f"Remove these records from the database?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            conn.close()
            return

        try:
            deleted = llmii_db.remove_orphaned_images(conn, [img_id for img_id, _ in orphans])
            conn.close()
            QMessageBox.information(self, "Orphans Removed",
                f"Removed {deleted} orphaned image record(s) and their associated data.")
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Remove Failed", str(e))

    def _db_export_csv(self):
        from . import llmii_db
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Keywords CSV", "image_keywords.csv",
            "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return
        conn = self._get_db_connection()
        if not conn:
            return
        try:
            count = llmii_db.export_keywords_csv(conn, path)
            conn.close()
            QMessageBox.information(self, "Export Complete",
                f"Exported {count} image(s) to:\n{path}")
        except Exception as e:
            conn.close()
            QMessageBox.critical(self, "Export Failed", str(e))

    def _clear_database(self):
        reply = QMessageBox.warning(
            self, "Clear Database",
            "This will remove all processed image data (images, galleries, performers, "
            "studios, run history, keywords).\n\n"
            "The master tag and alias tables are preserved.\n\n"
            "Are you sure?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        from . import llmii_db
        conn = self._get_db_connection()
        if conn:
            try:
                llmii_db.clear_database(conn)
                conn.close()
                QMessageBox.information(self, "Database Cleared",
                    "Image data has been removed. Tags and aliases are intact.")
            except Exception as e:
                QMessageBox.critical(self, "Clear Failed", str(e))

    def load_settings(self):
        try:
            if os.path.exists('settings.json'):
                with open('settings.json', 'r', encoding='utf-8') as f:
                    settings = json.load(f)

                self.api_url_input.setText(settings.get('api_url', 'http://localhost:5001'))
                self.api_password_input.setText(settings.get('api_password', ''))
                self.system_instruction_input.setText(settings.get('system_instruction', 'You are an image cataloguing assistant. Describe images accurately and completely, including explicit or adult content. Do not omit, censor, or euphemize any visible content.'))
                self.gen_count.setValue(settings.get('gen_count', 250))
                self.res_limit.setValue(settings.get('res_limit', 448))
                self.instruction_text = settings.get('instruction', GuiConfig.DEFAULT_INSTRUCTION)
                self.skip_folders_text = settings.get('skip_folders', '')
                self.image_extensions_filter_input.setText(settings.get('image_extensions_filter', 'jpg,jpeg,webp,zip'))
                self.tags_file_input.setText(settings.get('tags_file', 'mastertags.json'))

                self.no_crawl_checkbox.setChecked(settings.get('no_crawl', False))
                self.reprocess_failed_checkbox.setChecked(settings.get('reprocess_failed', False))
                self.reprocess_all_checkbox.setChecked(settings.get('reprocess_all', False))
                self.reprocess_orphans_checkbox.setChecked(settings.get('reprocess_orphans', True))
                self.reprocess_sparse_checkbox.setChecked(settings.get('reprocess_sparse', False))
                self.reprocess_sparse_spinbox.setValue(settings.get('reprocess_sparse_min', 5))
                self.no_backup_checkbox.setChecked(settings.get('no_backup', False))
                self.dry_run_checkbox.setChecked(settings.get('dry_run', False))
                self.skip_verify_checkbox.setChecked(settings.get('skip_verify', False))
                self.quick_fail_checkbox.setChecked(settings.get('quick_fail', False))
                self.rename_invalid_checkbox.setChecked(settings.get('rename_invalid', False))
                self.preserve_date_checkbox.setChecked(settings.get('preserve_date', False))
                self.fix_extension_checkbox.setChecked(settings.get('fix_extension', False))
                #self.write_unsafe_checkbox.setChecked(settings.get('write_unsafe', False))
                self.caption_instruction_input.setText(settings.get('caption_instruction', 'Describe the image in detail. Be specific.'))
                self.tag_instruction_input.setText(settings.get('tag_instruction', 'Return a JSON object with key Keywords with the value as array of Keywords and tags that describe the image as follows: {"Keywords": []}'))
                
                # Set radio button based on settings
                if settings.get('detailed_caption', False):
                    self.detailed_caption_radio.setChecked(True)
                elif settings.get('no_caption', False):
                    self.no_caption_radio.setChecked(True)
                else:
                    # Default to short caption
                    self.short_caption_radio.setChecked(True)
                    
                self.update_keywords_checkbox.setChecked(settings.get('update_keywords', True))
                self.update_caption_checkbox.setChecked(settings.get('update_caption', False))
                
                # Load keyword correction settings
                self.depluralize_checkbox.setChecked(settings.get('depluralize_keywords', False))
                self.word_limit_checkbox.setChecked(settings.get('limit_word_count', True))
                self.word_limit_spinbox.setValue(settings.get('max_words_per_keyword', 2))
                self.split_and_checkbox.setChecked(settings.get('split_and_entries', True))
                self.ban_prompt_words_checkbox.setChecked(settings.get('ban_prompt_words', True))
                self.no_digits_start_checkbox.setChecked(settings.get('no_digits_start', True))
                self.min_word_length_checkbox.setChecked(settings.get('min_word_length', True))
                self.latin_only_checkbox.setChecked(settings.get('latin_only', True))
                raw_bl = settings.get('tag_blacklist', '')
                # Accept both old comma-separated and new newline-separated formats
                if raw_bl and ',' in raw_bl and '\n' not in raw_bl:
                    bl_lines = '\n'.join(w.strip() for w in raw_bl.split(',') if w.strip())
                else:
                    bl_lines = raw_bl
                self.tag_blacklist_input.setPlainText(bl_lines)
                self.tag_fuzzy_spinbox.setValue(settings.get('tag_fuzzy_threshold', 88))

                # Load sampler settings
                self.temperature_spinbox.setValue(settings.get('temperature', 0.2))
                self.top_p_spinbox.setValue(settings.get('top_p', 1.0))
                self.top_k_spinbox.setValue(settings.get('top_k', 100))
                self.min_p_spinbox.setValue(settings.get('min_p', 0.05))
                self.rep_pen_spinbox.setValue(settings.get('rep_pen', 1.01))

                # Load JSON grammar setting
                self.use_json_grammar_checkbox.setChecked(settings.get('use_json_grammar', False))

                # Load sidecar location setting
                sidecar_dir = settings.get('sidecar_dir', '')
                if sidecar_dir:
                    self.sidecar_custom_dir_radio.setChecked(True)
                    self.sidecar_dir_input.setText(sidecar_dir)
                else:
                    self.sidecar_with_image_radio.setChecked(True)

                # Load output mode
                mode = settings.get('output_mode', 'json')
                if mode == 'db':
                    self.output_db_radio.setChecked(True)
                elif mode == 'both':
                    self.output_both_radio.setChecked(True)
                else:
                    self.output_json_radio.setChecked(True)

                # Load database connection settings
                self.db_host_input.setText(settings.get('db_host', 'localhost'))
                self.db_port_input.setValue(settings.get('db_port', 5432))
                self.db_user_input.setText(settings.get('db_user', ''))
                self.db_pass_input.setText(settings.get('db_password', ''))
                self.db_name_input.setText(settings.get('db_name', ''))

                # Load zip temp folder setting
                self.temp_folder_input.setText(settings.get('temp_folder', 'temp'))

        except Exception as e:
            print(f"Error loading settings: {e}")
            
    def save_settings(self):
        settings = {
            'api_url': self.api_url_input.text(),
            'api_password': self.api_password_input.text(),
            'system_instruction': self.system_instruction_input.text(),
            'instruction': self.instruction_text,
            'skip_folders': self.skip_folders_text,
            'image_extensions_filter': self.image_extensions_filter_input.text(),
            'tags_file': self.tags_file_input.text(),
            'gen_count': self.gen_count.value(),
            'res_limit': self.res_limit.value(),
            'no_crawl': self.no_crawl_checkbox.isChecked(),
            'reprocess_failed': self.reprocess_failed_checkbox.isChecked(),
            'reprocess_all': self.reprocess_all_checkbox.isChecked(),
            'reprocess_orphans': self.reprocess_orphans_checkbox.isChecked(),
            'reprocess_sparse': self.reprocess_sparse_checkbox.isChecked(),
            'reprocess_sparse_min': self.reprocess_sparse_spinbox.value(),
            'no_backup': self.no_backup_checkbox.isChecked(),
            'dry_run': self.dry_run_checkbox.isChecked(),
            'skip_verify': self.skip_verify_checkbox.isChecked(),
            'quick_fail': self.quick_fail_checkbox.isChecked(),
            'update_keywords': self.update_keywords_checkbox.isChecked(),
            'caption_instruction': self.caption_instruction_input.text(),
            'tag_instruction': self.tag_instruction_input.text(),
            'detailed_caption': self.detailed_caption_radio.isChecked(),
            'short_caption': self.short_caption_radio.isChecked(),
            'no_caption': self.no_caption_radio.isChecked(),
            'update_caption': self.update_caption_checkbox.isChecked(),
            'rename_invalid': self.rename_invalid_checkbox.isChecked(),
            'preserve_date': self.preserve_date_checkbox.isChecked(),
            'fix_extension': self.fix_extension_checkbox.isChecked(),
            #'write_unsafe': self.write_unsafe_checkbox.isChecked(),
            'depluralize_keywords': self.depluralize_checkbox.isChecked(),
            'limit_word_count': self.word_limit_checkbox.isChecked(),
            'max_words_per_keyword': self.word_limit_spinbox.value(),
            'split_and_entries': self.split_and_checkbox.isChecked(),
            'ban_prompt_words': self.ban_prompt_words_checkbox.isChecked(),
            'no_digits_start': self.no_digits_start_checkbox.isChecked(),
            'min_word_length': self.min_word_length_checkbox.isChecked(),
            'latin_only': self.latin_only_checkbox.isChecked(),
            'tag_blacklist': self.tag_blacklist_input.toPlainText(),
            'tag_fuzzy_threshold': self.tag_fuzzy_spinbox.value(),
            'temperature': self.temperature_spinbox.value(),
            'top_p': self.top_p_spinbox.value(),
            'top_k': self.top_k_spinbox.value(),
            'min_p': self.min_p_spinbox.value(),
            'rep_pen': self.rep_pen_spinbox.value(),
            'use_json_grammar': self.use_json_grammar_checkbox.isChecked(),
            'sidecar_dir': self.sidecar_dir_input.text() if self.sidecar_custom_dir_radio.isChecked() else '',
            'output_mode': (
                'db'   if self.output_db_radio.isChecked() else
                'both' if self.output_both_radio.isChecked() else
                'json'
            ),
            'db_host':     self.db_host_input.text().strip(),
            'db_port':     self.db_port_input.value(),
            'db_user':     self.db_user_input.text().strip(),
            'db_password': self.db_pass_input.text(),
            'db_name':     self.db_name_input.text().strip(),
            'temp_folder': self.temp_folder_input.text().strip() or 'temp',
        }

        try:
            with open('settings.json', 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=4, ensure_ascii=False)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to save settings: {e}")

class APICheckThread(QThread):
    api_status = pyqtSignal(bool)
    
    def __init__(self, api_url):
        super().__init__()
        self.api_url = api_url
        self.running = True
        
    def run(self):

        while self.running:
            try:
                # Direct HTTP request to the version endpoint
                response = requests.get(f"{self.api_url}/api/extra/version", timeout=5)
                if response.status_code == 200:
                    self.api_status.emit(True)
                    break
                response = requests.get(f"{self.api_url}/health", timeout=5)
                if response.status_code == 200:
                    self.api_status.emit(True)
                    break
            except Exception:
                self.api_status.emit(False)
            self.msleep(1000)
            
    def stop(self):
        self.running = False

class IndexerThread(QThread):
    output_received  = pyqtSignal(str)
    image_processed  = pyqtSignal(str, str, list, list, dict, str, str, list)  # base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers
    progress_update  = pyqtSignal(dict)   # progress snapshot from FileProcessor

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.paused = False
        self.stopped = False

    def process_callback(self, message):
        """Callback for llmii's process_file function"""
        if not isinstance(message, dict):
            self.output_received.emit(str(message))
            return

        msg_type = message.get('type')
        if msg_type == 'image_data':
            base64_image = message.get('base64_image', '')
            caption      = message.get('caption', '')
            keywords     = message.get('keywords') or []
            raw_keywords = message.get('raw_keywords') or []
            debug_map    = message.get('debug_map', {})
            file_path    = message.get('file_path', '')
            studio       = message.get('studio', '')
            performers   = message.get('performers') or []
            self.image_processed.emit(base64_image, caption, keywords, raw_keywords, debug_map, file_path, studio, performers)
        elif msg_type == 'progress':
            self.progress_update.emit(message)
        else:
            self.output_received.emit(str(message))

    def run(self):
        try:
            # Pass our callback function to llmii
            llmii.main(self.config, self.process_callback, self.check_paused_or_stopped)
        except Exception as e:
            self.output_received.emit(f"Error: {str(e)}")

    def check_paused_or_stopped(self):
        if self.stopped:
            raise Exception("Indexer stopped by user")
        if self.paused:
            while self.paused and not self.stopped:
                self.msleep(100)
            if self.stopped:
                raise Exception("Indexer stopped by user")
        return self.paused

class _DbOpWorker(QThread):
    """Runs a single DB operation function in a background thread.

    Usage::
        worker = _DbOpWorker(llmii_db.backfill_normalizers, conn)
        worker.finished.connect(on_result)
        worker.error.connect(on_error)
        worker.start()
    """
    finished = pyqtSignal(object)   # emits the return value of fn(conn)
    error    = pyqtSignal(str)      # emits str(exception) on failure

    def __init__(self, fn, conn):
        super().__init__()
        self._fn   = fn
        self._conn = conn

    def run(self):
        try:
            self.finished.emit(self._fn(self._conn))
        except Exception as e:
            self.error.emit(str(e))


class FlowLayout(QLayout):
    """Wrapping flow layout — places widgets left-to-right and wraps to the
    next line when the available width is exceeded, like CSS flex-wrap."""

    def __init__(self, parent=None, h_spacing=4, v_spacing=4):
        super().__init__(parent)
        self._h_spacing = h_spacing
        self._v_spacing = v_spacing
        self._items = []

    def addItem(self, item):
        self._items.append(item)

    def count(self):
        return len(self._items)

    def itemAt(self, index):
        return self._items[index] if 0 <= index < len(self._items) else None

    def takeAt(self, index):
        return self._items.pop(index) if 0 <= index < len(self._items) else None

    def expandingDirections(self):
        return Qt.Orientation(0)

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        return self._do_layout(QRect(0, 0, width, 0), dry_run=True)

    def setGeometry(self, rect):
        super().setGeometry(rect)
        self._do_layout(rect, dry_run=False)

    def sizeHint(self):
        return self.minimumSize()

    def minimumSize(self):
        size = QSize()
        for item in self._items:
            size = size.expandedTo(item.minimumSize())
        m = self.contentsMargins()
        return size + QSize(m.left() + m.right(), m.top() + m.bottom())

    def _do_layout(self, rect, dry_run):
        m = self.contentsMargins()
        left  = rect.x() + m.left()
        top   = rect.y() + m.top()
        right = rect.x() + rect.width() - m.right()
        x, y, row_h = left, top, 0
        for item in self._items:
            hint = item.sizeHint()
            w, h = hint.width(), hint.height()
            if x + w > right and row_h > 0:
                x = left
                y += row_h + self._v_spacing
                row_h = 0
            if not dry_run:
                item.setGeometry(QRect(QPoint(x, y), hint))
            x += w + self._h_spacing
            row_h = max(row_h, h)
        return (y + row_h + m.bottom()) - rect.y()


class KeywordWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._flow = FlowLayout(self, h_spacing=4, v_spacing=4)
        self._flow.setContentsMargins(
            GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
            GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
        )
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

    def clear(self):
        while self._flow.count():
            item = self._flow.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()

    def set_keywords(self, keywords):
        self.clear()
        for kw in sorted(keywords, key=str.casefold):
            label = QLabel(kw)
            label.setStyleSheet(f"""
                background-color: {GuiConfig.COLOR_KEYWORD_BG};
                color: {GuiConfig.COLOR_KEYWORD_TEXT};
                padding: 1px 4px;
                border-radius: 5px;
                border: 1px solid {GuiConfig.COLOR_KEYWORD_BORDER};
                margin: 2px;
                font-size: {GuiConfig.FONT_SIZE_NORMAL}px;
            """)
            self._flow.addWidget(label)
        self.updateGeometry()


class RawKeywordWidget(QWidget):
    """Displays raw LLM keywords color-coded: green = matched, red = unmatched."""

    COLOR_MATCHED_BG     = "#d4edda"
    COLOR_MATCHED_TEXT   = "#155724"
    COLOR_MATCHED_BORDER = "#a8d5b5"
    COLOR_UNMATCHED_BG     = "#f8d7da"
    COLOR_UNMATCHED_TEXT   = "#721c24"
    COLOR_UNMATCHED_BORDER = "#f0a0a8"
    COLOR_BLACKLISTED_BG     = "#fff3cd"
    COLOR_BLACKLISTED_TEXT   = "#7d5a00"
    COLOR_BLACKLISTED_BORDER = "#ffc107"

    def __init__(self, parent=None):
        super().__init__(parent)
        self._flow = FlowLayout(self, h_spacing=4, v_spacing=4)
        self._flow.setContentsMargins(
            GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
            GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
        )
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

    def clear(self):
        while self._flow.count():
            item = self._flow.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()

    def set_keywords(self, raw_keywords, debug_map):
        self.clear()
        if not raw_keywords:
            return
        for kw in sorted(raw_keywords, key=str.casefold):
            val = debug_map.get(kw)
            if val == "__blacklisted__":
                bg, fg, border = self.COLOR_BLACKLISTED_BG, self.COLOR_BLACKLISTED_TEXT, self.COLOR_BLACKLISTED_BORDER
            elif val:
                bg, fg, border = self.COLOR_MATCHED_BG, self.COLOR_MATCHED_TEXT, self.COLOR_MATCHED_BORDER
            else:
                bg, fg, border = self.COLOR_UNMATCHED_BG, self.COLOR_UNMATCHED_TEXT, self.COLOR_UNMATCHED_BORDER
            label = QLabel(kw)
            label.setStyleSheet(f"""
                background-color: {bg};
                color: {fg};
                padding: 1px 4px;
                border-radius: 5px;
                border: 1px solid {border};
                margin: 2px;
                font-size: {GuiConfig.FONT_SIZE_NORMAL}px;
            """)
            self._flow.addWidget(label)
        self.updateGeometry()


class PauseHandler(QObject):
    pause_signal = pyqtSignal(bool)
    stop_signal = pyqtSignal()

class ImageIndexerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("Image Indexer GUI")
        self.setMinimumSize(GuiConfig.WINDOW_WIDTH, GuiConfig.WINDOW_HEIGHT)
        self._current_pixmap = None  # Store unscaled pixmap for rescaling on resize
            
        self.settings_dialog = SettingsDialog(self)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(2)
        main_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        
        # Upper section with controls - fixed height
        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(GuiConfig.SPACING)
        
        # Directory and Settings section
        dir_layout = QHBoxLayout()
        self.dir_input = QLineEdit()
        dir_button = QPushButton("Select Directory")
        dir_button.clicked.connect(self.select_directory)
        dir_layout.addWidget(QLabel("Directory:"))
        dir_layout.addWidget(self.dir_input)
        dir_layout.addWidget(dir_button)
        controls_layout.addLayout(dir_layout)

        # Settings button and API status in one row
        settings_api_layout = QHBoxLayout()
        settings_button = QPushButton("Settings")
        settings_button.clicked.connect(self.show_settings)
        self.api_status_label = QLabel("API Status: Checking...")
        settings_api_layout.addWidget(settings_button)
        settings_api_layout.addStretch(1)
        settings_api_layout.addWidget(self.api_status_label)
        controls_layout.addLayout(settings_api_layout)
        
        # Control buttons
        button_layout = QHBoxLayout()
        self.run_button = QPushButton("Run Image Indexer")
        self.run_button.clicked.connect(self.run_indexer)
        self.resume_button = QPushButton("Resume Session")
        self.resume_button.clicked.connect(self.resume_indexer)
        self.pause_button = QPushButton("Pause")
        self.pause_button.clicked.connect(self.toggle_pause)
        self.pause_button.setEnabled(False)
        self.stop_button = QPushButton("Stop")
        self.stop_button.clicked.connect(self.stop_indexer)
        self.stop_button.setEnabled(False)
        button_layout.addWidget(self.run_button)
        button_layout.addWidget(self.resume_button)
        button_layout.addWidget(self.pause_button)
        button_layout.addWidget(self.stop_button)
        controls_layout.addLayout(button_layout)
        
        # Set fixed height for controls widget
        controls_widget.setFixedHeight(GuiConfig.CONTROL_PANEL_HEIGHT)
        main_layout.addWidget(controls_widget)

        # ── Progress bars ─────────────────────────────────────────────────
        self.progress_widget = QWidget()
        progress_outer = QVBoxLayout(self.progress_widget)
        progress_outer.setContentsMargins(4, 2, 4, 2)
        progress_outer.setSpacing(2)

        def _make_progress_row(label_text, fixed_label_width=80):
            row = QHBoxLayout()
            lbl = QLabel(label_text)
            lbl.setFixedWidth(fixed_label_width)
            bar = QProgressBar()
            bar.setTextVisible(False)
            bar.setFixedHeight(14)
            count_lbl = QLabel("0 / 0")
            count_lbl.setFixedWidth(70)
            row.addWidget(lbl)
            row.addWidget(bar, 1)
            row.addWidget(count_lbl)
            return row, bar, lbl, count_lbl

        dir_row, self.dir_progress_bar, self.dir_progress_title, self.dir_progress_label = \
            _make_progress_row("Directories:")
        progress_outer.addLayout(dir_row)

        # Zip row — hidden until a directory with zips is encountered
        self.zip_progress_row = QWidget()
        zip_inner = QHBoxLayout(self.zip_progress_row)
        zip_inner.setContentsMargins(0, 0, 0, 0)
        zip_lbl = QLabel("Zip files:")
        zip_lbl.setFixedWidth(80)
        self.zip_progress_bar = QProgressBar()
        self.zip_progress_bar.setTextVisible(False)
        self.zip_progress_bar.setFixedHeight(14)
        self.zip_progress_label = QLabel("0 / 0")
        self.zip_progress_label.setFixedWidth(70)
        zip_inner.addWidget(zip_lbl)
        zip_inner.addWidget(self.zip_progress_bar, 1)
        zip_inner.addWidget(self.zip_progress_label)
        self.zip_progress_row.setVisible(False)
        progress_outer.addWidget(self.zip_progress_row)

        img_row, self.image_progress_bar, self.image_progress_title, self.image_progress_label = \
            _make_progress_row("Images:")
        progress_outer.addLayout(img_row)

        self.progress_widget.setVisible(False)
        main_layout.addWidget(self.progress_widget)
        # ──────────────────────────────────────────────────────────────────

        nav_widget = QWidget()
        
        nav_layout = QHBoxLayout(nav_widget)
        nav_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        nav_layout.setSpacing(GuiConfig.SPACING)

        # Create navigation buttons
        self.first_button = QPushButton("|<")  # Go to first image
        self.prev_button = QPushButton("<")    # Go to previous image
        self.position_label = QLabel("No images processed")
        self.position_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.next_button = QPushButton(">")    # Go to next image
        self.last_button = QPushButton(">|")   # Go to most recent image

        # Add widgets to layout
        nav_layout.addWidget(self.first_button)
        nav_layout.addWidget(self.prev_button)
        nav_layout.addStretch(1)
        nav_layout.addWidget(self.position_label)
        nav_layout.addStretch(1)
        nav_layout.addWidget(self.next_button)
        nav_layout.addWidget(self.last_button)

        # Connect button signals to slots
        self.first_button.clicked.connect(self.navigate_first)
        self.prev_button.clicked.connect(self.navigate_prev)
        self.next_button.clicked.connect(self.navigate_next)
        self.last_button.clicked.connect(self.navigate_last)

        # Set initial button states (disabled until we have images)
        self.first_button.setEnabled(False)
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)
        self.last_button.setEnabled(False)

        # Add to the main layout
        main_layout.addWidget(nav_widget)
        
        # Middle section with image and metadata side by side — splitter allows width resizing
        middle_section = QSplitter(Qt.Orientation.Horizontal)
        middle_section.setHandleWidth(GuiConfig.SPLITTER_HANDLE_WIDTH)
        middle_section.setChildrenCollapsible(False)
        
        # Image preview panel - fixed size
        image_frame = QFrame()
        image_frame.setFrameShape(QFrame.Shape.Box)
        image_frame.setStyleSheet(f"border: 1px solid {GuiConfig.COLOR_BORDER}; padding: 0px;")
        
        image_layout = QVBoxLayout(image_frame)
        image_layout.setContentsMargins(1, 1, 1, 1)
        
        # Image preview label — expands to fill the frame
        self.image_preview = QLabel("No image processed yet")
        self.image_preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_preview.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.image_preview.setFrameShape(QFrame.Shape.NoFrame)
        self.image_preview.setStyleSheet("border: none; background-color: transparent;")

        image_layout.addWidget(self.image_preview)

        image_frame.setMinimumSize(GuiConfig.IMAGE_PREVIEW_WIDTH, GuiConfig.IMAGE_PREVIEW_HEIGHT)
        image_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        middle_section.addWidget(image_frame)
        
        # Metadata panel - fixed size
        metadata_frame = QFrame()
        
        metadata_frame.setFrameShape(QFrame.Shape.Box)
        metadata_frame.setStyleSheet(f"border: 1px solid {GuiConfig.COLOR_BORDER};")
        #metadata_frame.setStyleSheet("")
        metadata_layout = QVBoxLayout(metadata_frame)
        metadata_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        metadata_layout.setSpacing(GuiConfig.SPACING)
        
        # Image filename
        self.filename_label = QLabel("Filename: ")
        self.filename_label.setStyleSheet("font-weight: bold; border:none;")
        self.filename_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.filename_label.setFixedHeight(GuiConfig.FILENAME_LABEL_HEIGHT)
        
        metadata_layout.addWidget(self.filename_label)

        # Studio
        self.studio_label = QLabel("")
        self.studio_label.setStyleSheet("border:none;")
        self.studio_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.studio_label.setFixedHeight(GuiConfig.FILENAME_LABEL_HEIGHT)
        metadata_layout.addWidget(self.studio_label)

        # Performers
        self.performers_label = QLabel("")
        self.performers_label.setStyleSheet("border:none;")
        self.performers_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.performers_label.setFixedHeight(GuiConfig.FILENAME_LABEL_HEIGHT)
        metadata_layout.addWidget(self.performers_label)

        # Caption
        caption_group = QGroupBox("Caption")
        caption_group.setStyleSheet("QGroupBox { border: none; }")
        caption_layout = QVBoxLayout(caption_group)
        
        caption_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        caption_layout.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.caption_label = QLabel("No caption generated yet")
        self.caption_label.setWordWrap(True)
        #self.caption_label.setFrameStyle(QFrame.Shape.NoFrame)
        self.caption_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.caption_label.setStyleSheet(f"padding: 4px; font-weight: normal; border:none;")
        
        # Create a scroll area for caption to ensure it fits in fixed space
        caption_scroll = QScrollArea()
        caption_scroll.setWidgetResizable(True)
        caption_scroll.setWidget(self.caption_label)
        caption_scroll.setMinimumHeight(120)
        caption_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        caption_layout.addWidget(caption_scroll, 1)
        metadata_layout.addWidget(caption_group, 3)  # 60% of remaining vertical space

        # Matched keywords
        keywords_group = QGroupBox("Keywords")
        keywords_group.setStyleSheet("QGroupBox { border: none; }")
        keywords_group_layout = QVBoxLayout(keywords_group)
        keywords_group_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
                                                  GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        self.keywords_widget = KeywordWidget()
        keywords_scroll = QScrollArea()
        keywords_scroll.setWidgetResizable(True)
        keywords_scroll.setWidget(self.keywords_widget)
        keywords_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        keywords_group_layout.addWidget(keywords_scroll, 1)
        metadata_layout.addWidget(keywords_group, 1)  # 20% of remaining vertical space

        # Raw keywords (color-coded)
        raw_group = QGroupBox("Raw Keywords")
        raw_group.setStyleSheet("QGroupBox { border: none; }")
        raw_group_layout = QVBoxLayout(raw_group)
        raw_group_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS,
                                             GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        self.raw_keywords_widget = RawKeywordWidget()
        raw_scroll = QScrollArea()
        raw_scroll.setWidgetResizable(True)
        raw_scroll.setWidget(self.raw_keywords_widget)
        raw_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        raw_group_layout.addWidget(raw_scroll, 1)
        metadata_layout.addWidget(raw_group, 1)  # 20% of remaining vertical space
        
        metadata_frame.setMinimumSize(GuiConfig.METADATA_WIDTH, GuiConfig.METADATA_HEIGHT)
        metadata_frame.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        middle_section.addWidget(metadata_frame)

        # Give the image side most of the initial width
        middle_section.setStretchFactor(0, 1)  # image frame
        middle_section.setStretchFactor(1, 0)  # metadata frame

        # Add middle section to main layout with stretch so it fills vertical space
        main_layout.addWidget(middle_section, 1)
        
        # Bottom section - log output with fixed size
        log_frame = QFrame()
        log_frame.setFrameShape(QFrame.Shape.Box)
        log_frame.setFrameStyle(QFrame.Shape.NoFrame)
        log_layout = QVBoxLayout(log_frame)
        log_layout.setContentsMargins(GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS, GuiConfig.CONTENT_MARGINS)
        log_label = QLabel("Processing Log:")
        log_layout.addWidget(log_label)
        self.output_area = QTextEdit()
        self.output_area.setReadOnly(True)
        log_layout.addWidget(self.output_area)
        
        log_frame.setMinimumHeight(GuiConfig.LOG_HEIGHT)
        log_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        main_layout.addWidget(log_frame)
        
        # Store the previous image data to keep showing something
        self.previous_image_data = None
        self.previous_caption = None
        self.previous_keywords = None
        self.previous_filename = None
        self.pause_handler = PauseHandler()
        self.api_check_thread = None
        self.api_is_ready = False
        self.run_button.setEnabled(False)
        self.image_history = []  # [(base64_image, caption, keywords, filename)]
        self.current_position = -1
        
        if os.path.exists('settings.json'):
            try:
                with open('settings.json', 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    self.dir_input.setText(settings.get('directory', ''))
                    self.start_api_check(settings.get('api_url', 'http://localhost:5001'))
                    
            except Exception as e:
                print(f"Error loading settings: {e}")
                self.start_api_check('http://localhost:5001')
        else:
            self.start_api_check('http://localhost:5001')

    def show_settings(self):
        if self.settings_dialog.exec() == QDialog.DialogCode.Accepted:
            self.settings_dialog.save_settings()
            
            self.start_api_check(self.settings_dialog.api_url_input.text())
            
            try:
                with open('settings.json', 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                settings['directory'] = self.dir_input.text()
                with open('settings.json', 'w', encoding='utf-8') as f:
                    json.dump(settings, f, indent=4, ensure_ascii=False)
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to save directory setting: {e}")

    def select_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.dir_input.setText(directory)

    def start_api_check(self, api_url):
        self.api_url = api_url
        if self.api_check_thread and self.api_check_thread.isRunning():
            self.api_check_thread.stop()
            self.api_check_thread.wait()
            
        self.api_is_ready = False
        self.run_button.setEnabled(False)
        self.api_status_label.setText("API Status: Checking...")
        self.api_status_label.setStyleSheet("color: orange; padding: 4px")

        self.api_check_thread = APICheckThread(api_url if api_url else self.settings_dialog.api_url_input.text())
        self.api_check_thread.api_status.connect(self.update_api_status)
        self.api_check_thread.start()

    def update_api_status(self, is_available):
        if is_available:
            self.api_is_ready = True
            self.api_status_label.setText("API Status: Connected")
            self.api_status_label.setStyleSheet("color: green; padding: 4px")
            self.run_button.setEnabled(True)
            
            # Stop the check thread once we're connected
            if self.api_check_thread:
                self.api_check_thread.stop()
        else:
            self.api_is_ready = False
            self.api_status_label.setText("API Status: Waiting for connection...")
            self.api_status_label.setStyleSheet("color: red; padding: 4px")
            self.run_button.setEnabled(False)
    
    def update_image_preview(self, base64_image, caption, keywords, raw_keywords, debug_map, filename, studio='', performers=None):
        self.previous_image_data = base64_image
        self.previous_caption = caption
        self.previous_keywords = keywords
        self.previous_filename = filename

        if performers is None:
            performers = []

        # Add to history
        self.image_history.append((base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers))

        # If user was viewing the most recent image (or this is the first image),
        # update current_position to point to the new image
        if self.current_position == -1 or len(self.image_history) <= 1:
            self.current_position = -1  # Keep at most recent
            self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)
        else:
            # Just update navigation buttons without changing the view
            self.update_navigation_buttons()

    def display_image(self, base64_image, caption, keywords, raw_keywords, debug_map, filename, studio='', performers=None):
        if performers is None:
            performers = []

        # Update the UI with the image data
        try:
            # Convert base64 to QImage
            image_data = base64.b64decode(base64_image)
            image = QImage.fromData(image_data)
            if not image.isNull():
                self._current_pixmap = QPixmap.fromImage(image)
                self._rescale_image()
            else:
                self.image_preview.setText("Error loading image")
        except Exception as e:
            self.image_preview.setText(f"Error: {str(e)}")

        file_basename = os.path.basename(filename)
        self.filename_label.setText(f"Filename: {file_basename}")
        self.studio_label.setText(f"Studio: {studio}" if studio else "")
        self.performers_label.setText(f"Performers: {', '.join(performers)}" if performers else "")
        self.caption_label.setText(caption or "No caption generated")
        self.keywords_widget.set_keywords(keywords or [])
        self.raw_keywords_widget.set_keywords(raw_keywords or [], debug_map or {})
        self.update_navigation_buttons()

    def navigate_first(self):
        if self.image_history:
            self.current_position = 0
            base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers = self.image_history[0]
            self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)

    def navigate_prev(self):
        if not self.image_history:
            return

        if self.current_position == -1:
            # If at the most recent, go to the second most recent
            if len(self.image_history) > 1:
                self.current_position = len(self.image_history) - 2
                base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers = self.image_history[self.current_position]
                self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)
        elif self.current_position > 0:
            self.current_position -= 1
            base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers = self.image_history[self.current_position]
            self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)

    def navigate_next(self):
        if not self.image_history:
            return

        if self.current_position != -1 and self.current_position < len(self.image_history) - 1:
            self.current_position += 1

            # If we've reached the end, set to -1 to indicate "most recent"
            if self.current_position == len(self.image_history) - 1:
                self.current_position = -1

            base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers = self.image_history[
                len(self.image_history) - 1 if self.current_position == -1 else self.current_position
            ]
            self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)

    def navigate_last(self):
        if self.image_history:
            self.current_position = -1
            base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers = self.image_history[-1]
            self.display_image(base64_image, caption, keywords, raw_keywords, debug_map, filename, studio, performers)

    def update_navigation_buttons(self):
        history_size = len(self.image_history)
        
        if history_size == 0:
            # No images yet
            self.first_button.setEnabled(False)
            self.prev_button.setEnabled(False)
            self.next_button.setEnabled(False)
            self.last_button.setEnabled(False)
            self.position_label.setText("No images processed")
            return
        
        # Determine position for display
        if self.current_position == -1:
            # At the most recent image
            position = history_size
            self.next_button.setEnabled(False)
            self.last_button.setEnabled(False)
        else:
            position = self.current_position + 1  # 1-based for display
            self.next_button.setEnabled(self.current_position < history_size - 1)
            self.last_button.setEnabled(self.current_position < history_size - 1)
        
        # Update position text
        self.position_label.setText(f"Image {position} of {history_size}")
        
        # Enable/disable first/prev buttons
        self.first_button.setEnabled(history_size > 1 and (self.current_position > 0 or self.current_position == -1))
        self.prev_button.setEnabled(history_size > 1 and (self.current_position > 0 or self.current_position == -1))
          
    def resume_indexer(self):
        """Check for resumable state, confirm with user, then start with resume=True."""
        from pathlib import Path as _Path
        directory = self.dir_input.text()
        output_mode = (
            'db'   if self.settings_dialog.output_db_radio.isChecked() else
            'both' if self.settings_dialog.output_both_radio.isChecked() else
            'json'
        )

        if output_mode in ('db', 'both'):
            msg = (
                "Resume will query the database to find already-processed files "
                "and skip them entirely — no ExifTool reads for completed files.\n\n"
                "Continue?"
            )
        else:
            cp = _Path(__file__).resolve().parent.parent / 'llmii_checkpoint.json'
            if cp.exists():
                try:
                    import json as _json
                    data = _json.loads(cp.read_text(encoding='utf-8'))
                    import os as _os
                    if _os.path.normpath(data.get('directory', '')) == _os.path.normpath(directory):
                        n = len(data.get('processed_paths', []))
                        msg = (
                            f"Found checkpoint: {n:,} previously-processed files will be skipped.\n\n"
                            "Continue?"
                        )
                    else:
                        msg = (
                            "Checkpoint found but it is for a different directory.\n"
                            "A fresh run will start with no files skipped.\n\nContinue?"
                        )
                except Exception:
                    msg = "Checkpoint file found but could not be read. Starting fresh.\n\nContinue?"
            else:
                msg = "No checkpoint file found. A normal run will start.\n\nContinue?"

        reply = QMessageBox.question(self, "Resume Session", msg)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self.run_indexer(resume=True)

    def run_indexer(self, resume=False):

        if not self.api_is_ready:
            QMessageBox.warning(self, "API Not Ready", 
                              "Please wait for the API to be available before running the indexer.")
            return
        
        self.image_history = []
        self.current_position = -1
        self.update_navigation_buttons()
        
        config = llmii.Config()
        config.resume_session = resume

        self.image_preview.setText("No image processed yet")
        self.filename_label.setText("Filename: ")
        self.caption_label.setText("No caption generated yet")
        self.keywords_widget.clear()
        
        # Get directory from main window
        config.directory = self.dir_input.text()
        
        # Load settings from settings dialog
        config.api_url = self.settings_dialog.api_url_input.text()
        config.api_password = self.settings_dialog.api_password_input.text()
        config.system_instruction = self.settings_dialog.system_instruction_input.text()
        config.no_crawl = self.settings_dialog.no_crawl_checkbox.isChecked()
        config.reprocess_failed = self.settings_dialog.reprocess_failed_checkbox.isChecked()
        config.reprocess_all = self.settings_dialog.reprocess_all_checkbox.isChecked()
        config.reprocess_orphans = self.settings_dialog.reprocess_orphans_checkbox.isChecked()
        config.reprocess_sparse = self.settings_dialog.reprocess_sparse_checkbox.isChecked()
        config.reprocess_sparse_min = self.settings_dialog.reprocess_sparse_spinbox.value()
        config.no_backup = self.settings_dialog.no_backup_checkbox.isChecked()
        config.dry_run = self.settings_dialog.dry_run_checkbox.isChecked()
        config.skip_verify = self.settings_dialog.skip_verify_checkbox.isChecked()
        config.quick_fail = self.settings_dialog.quick_fail_checkbox.isChecked()
        config.rename_invalid = self.settings_dialog.rename_invalid_checkbox.isChecked()
        config.preserve_date = self.settings_dialog.preserve_date_checkbox.isChecked()
        config.fix_extension = self.settings_dialog.fix_extension_checkbox.isChecked()
        #config.write_unsafe = self.settings_dialog.write_unsafe_checkbox.isChecked()
        config.normalize_keywords = True
        config.depluralize_keywords = self.settings_dialog.depluralize_checkbox.isChecked()
        config.limit_word_count = self.settings_dialog.word_limit_checkbox.isChecked()
        config.max_words_per_keyword = self.settings_dialog.word_limit_spinbox.value()
        config.split_and_entries = self.settings_dialog.split_and_checkbox.isChecked()
        config.ban_prompt_words = self.settings_dialog.ban_prompt_words_checkbox.isChecked()
        config.no_digits_start = self.settings_dialog.no_digits_start_checkbox.isChecked()
        config.min_word_length = self.settings_dialog.min_word_length_checkbox.isChecked()
        config.latin_only = self.settings_dialog.latin_only_checkbox.isChecked()
        raw_blacklist = self.settings_dialog.tag_blacklist_input.toPlainText()
        config.tag_blacklist = [w.strip() for w in raw_blacklist.splitlines() if w.strip()]
        config.tag_fuzzy_threshold = self.settings_dialog.tag_fuzzy_spinbox.value()
        
        # Load caption settings
        config.detailed_caption = self.settings_dialog.detailed_caption_radio.isChecked()
        config.short_caption = self.settings_dialog.short_caption_radio.isChecked()
        config.no_caption = self.settings_dialog.no_caption_radio.isChecked()
        config.caption_instruction = self.settings_dialog.caption_instruction_input.text()
        config.tag_instruction = self.settings_dialog.tag_instruction_input.text()

        # Load instruction from settings
        config.instruction = self.settings_dialog.instruction_text
        
        config.update_keywords = self.settings_dialog.update_keywords_checkbox.isChecked()
        config.update_caption = self.settings_dialog.update_caption_checkbox.isChecked()
        config.gen_count = self.settings_dialog.gen_count.value()
        config.res_limit = self.settings_dialog.res_limit.value()

        # Load sampler settings
        config.temperature = self.settings_dialog.temperature_spinbox.value()
        config.top_p = self.settings_dialog.top_p_spinbox.value()
        config.top_k = self.settings_dialog.top_k_spinbox.value()
        config.min_p = self.settings_dialog.min_p_spinbox.value()
        config.rep_pen = self.settings_dialog.rep_pen_spinbox.value()

        # Load JSON grammar setting
        config.use_json_grammar = self.settings_dialog.use_json_grammar_checkbox.isChecked()

        # Load sidecar location setting
        if self.settings_dialog.sidecar_custom_dir_radio.isChecked():
            config.sidecar_dir = self.settings_dialog.sidecar_dir_input.text().strip()
        else:
            config.sidecar_dir = ""

        config.image_extensions_filter = self.settings_dialog.image_extensions_filter_input.text()
        config.tags_file = self.settings_dialog.tags_file_input.text()

        # Output mode and database
        config.output_mode = (
            'db'   if self.settings_dialog.output_db_radio.isChecked() else
            'both' if self.settings_dialog.output_both_radio.isChecked() else
            'json'
        )
        config.db_host     = self.settings_dialog.db_host_input.text().strip()
        config.db_port     = self.settings_dialog.db_port_input.value()
        config.db_user     = self.settings_dialog.db_user_input.text().strip()
        config.db_password = self.settings_dialog.db_pass_input.text()
        config.db_name     = self.settings_dialog.db_name_input.text().strip()
        config.temp_folder = self.settings_dialog.temp_folder_input.text().strip() or 'temp'

        # Parse skip folders from text (semicolon or newline separated)
        skip_folders_text = self.settings_dialog.skip_folders_text
        if skip_folders_text:
            # Split by newlines first, then by semicolons
            folders = []
            for line in skip_folders_text.split('\n'):
                for folder in line.split(';'):
                    folder = folder.strip()
                    if folder:
                        folders.append(folder)
            config.skip_folders = folders

        self.indexer_thread = IndexerThread(config)
        self.indexer_thread.output_received.connect(self.update_output)
        self.indexer_thread.image_processed.connect(self.update_image_preview)
        self.indexer_thread.progress_update.connect(self.update_progress_bars)
        self.indexer_thread.finished.connect(self.indexer_finished)
        self.pause_handler.pause_signal.connect(self.set_paused)
        self.pause_handler.stop_signal.connect(self.set_stopped)
        self.indexer_thread.start()

        # Reset and show progress bars
        self.dir_progress_bar.setValue(0)
        self.dir_progress_label.setText("0 / 0")
        self.zip_progress_row.setVisible(False)
        self.image_progress_bar.setValue(0)
        self.image_progress_label.setText("0 / 0")
        self.image_progress_title.setText("Images:")
        self.progress_widget.setVisible(True)

        self.output_area.clear()
        self.output_area.append("Running Image Indexer...")
        self.run_button.setEnabled(False)
        self.resume_button.setEnabled(False)
        self.pause_button.setEnabled(True)
        self.stop_button.setEnabled(True)

    def set_paused(self, paused):
        if self.indexer_thread:
            self.indexer_thread.paused = paused

    def set_stopped(self):
        if self.indexer_thread:
            self.indexer_thread.stopped = True

    def toggle_pause(self):
        if self.pause_button.text() == "Pause":
            self.pause_handler.pause_signal.emit(True)
            self.pause_button.setText("Resume")
            self.update_output("Indexer paused.")
        else:
            self.pause_handler.pause_signal.emit(False)
            self.pause_button.setText("Pause")
            self.update_output("Indexer resumed.")

    def stop_indexer(self):
        self.pause_handler.stop_signal.emit()
        self.update_output("Stopping indexer...")
        self.run_button.setEnabled(True)
        self.resume_button.setEnabled(True)
        self.pause_button.setEnabled(False)
        self.stop_button.setEnabled(False)

    def indexer_finished(self):
        self.update_output("\nImage Indexer finished.")
        self.run_button.setEnabled(True)
        self.resume_button.setEnabled(True)
        self.pause_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.pause_button.setText("Pause")
        self.progress_widget.setVisible(False)

    def update_progress_bars(self, data):
        """Update the three progress bars from a 'progress' callback dict."""
        dirs_total   = data.get('dirs_total', 0)
        dirs_done    = data.get('dirs_done', 0)
        zips_total   = data.get('zips_total', 0)
        zips_done    = data.get('zips_done', 0)
        images_total = data.get('images_total', 0)
        images_done  = data.get('images_done', 0)
        mode         = data.get('mode', 'dir')

        # Directories bar
        self.dir_progress_bar.setMaximum(max(dirs_total, 1))
        self.dir_progress_bar.setValue(dirs_done)
        self.dir_progress_label.setText(f"{dirs_done} / {dirs_total}")

        # Zip bar — visible only when the current directory contains zips
        has_zips = zips_total > 0
        self.zip_progress_row.setVisible(has_zips)
        if has_zips:
            self.zip_progress_bar.setMaximum(zips_total)
            self.zip_progress_bar.setValue(zips_done)
            self.zip_progress_label.setText(f"{zips_done} / {zips_total}")

        # Images bar — label changes based on whether we are inside a zip
        self.image_progress_title.setText("Zip images:" if mode == 'zip' else "Images:")
        self.image_progress_bar.setMaximum(max(images_total, 1))
        self.image_progress_bar.setValue(images_done)
        self.image_progress_label.setText(f"{images_done} / {images_total}")

    def update_output(self, text):
        self.output_area.append(text)
        self.output_area.verticalScrollBar().setValue(self.output_area.verticalScrollBar().maximum())
        QApplication.processEvents()

    def _rescale_image(self):
        """Scale the stored pixmap to the current image_preview size."""
        if self._current_pixmap is None:
            return
        size = self.image_preview.size()
        scaled = self._current_pixmap.scaled(
            size.width(), size.height(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        self.image_preview.setPixmap(scaled)
        self.image_preview.setAlignment(Qt.AlignmentFlag.AlignCenter)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._rescale_image()
        
    def closeEvent(self, event):
        # Clean up API check thread when closing the window
        if self.api_check_thread and self.api_check_thread.isRunning():
            self.api_check_thread.stop()
            self.api_check_thread.wait()
        event.accept()

def run_gui():
    
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    app.setStyle("Fusion")  # Modern cross-platform style

    
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Base, QColor(35, 35, 35))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.ToolTipBase, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
    palette.setColor(QPalette.ColorRole.Link, QColor(42, 130, 218))
    palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
    palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.black)
    
    app.setPalette(palette)
    window = ImageIndexerGUI()
    window.show()
    sys.exit(app.exec())    
    
def main():
    run_gui()
    
if __name__ == "__main__":
    main()
