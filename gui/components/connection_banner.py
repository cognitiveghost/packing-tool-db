"""The banner above the page while the server is unreachable.

Spec docs/superpowers/specs/2026-10-08-ui-refresh-phase1-shell-design.md
section 6.5, mockup frame 2e. Qt for now: the pages under it are Qt until
phases 3-5, when each web page draws its own from the kit.
"""

from html import escape

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout

from shared.i18n import _
from shared.icons import icon
from shared.theme import current_tokens, font_css, on_theme_changed


class ConnectionBanner(QFrame):
    retryRequested = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("ConnectionBanner")

        row = QHBoxLayout(self)
        row.setContentsMargins(16, 12, 16, 12)
        row.setSpacing(14)

        self.glyph = QLabel(self)
        self.glyph.setFixedSize(24, 24)
        row.addWidget(self.glyph, 0, Qt.AlignTop)

        text = QVBoxLayout()
        text.setSpacing(2)
        self.title_label = QLabel(_("Server unreachable"), self)
        self.title_label.setObjectName("BannerTitle")
        self.message_label = QLabel(self)
        self.message_label.setObjectName("BannerMessage")
        self.message_label.setTextFormat(Qt.RichText)
        self.message_label.setWordWrap(True)
        text.addWidget(self.title_label)
        text.addWidget(self.message_label)
        row.addLayout(text, 1)

        self.retry_button = QPushButton(_("Retry"), self)
        self.retry_button.setFixedHeight(44)
        self.retry_button.clicked.connect(self.retryRequested.emit)
        row.addWidget(self.retry_button, 0, Qt.AlignVCenter)

        self._path = ""
        self._since = ""
        self._notice: tuple[str, str] | None = None
        self._tone = "danger"
        on_theme_changed(self, self._apply_theme)

    def set_outage(self, server_path: str, since: str) -> None:
        self._path, self._since = server_path, since
        self._write_message()

    def set_notice(self, title: str, message: str, tone: str = "danger") -> None:
        """A fixed title and message for a state with no Retry: the fallback to the file share
        (danger), or the offline banner (warning)."""
        self._notice = (title, message)
        self._tone = tone
        self.retry_button.hide()
        self._write_message()
        self._apply_theme(current_tokens())

    def _write_message(self) -> None:
        if self._notice:
            self.title_label.setText(self._notice[0])
            self.message_label.setText(escape(self._notice[1]))
            return
        mono = self._mono
        self.message_label.setText(
            _(
                '<span style="font-family: {mono};">{path}</span> stopped answering at'
                ' <span style="font-family: {mono};">{since}</span>.'
                " Sessions cannot be opened or ended until it answers.",
                mono=mono,
                path=escape(self._path),
                since=escape(self._since),
            )
        )

    def _apply_theme(self, t) -> None:
        self._mono = t.font_family_mono
        if self._tone == "warning":
            bg, edge, ink = t.status_warning_bg, t.status_warning, t.status_warning
        else:
            bg, edge, ink = t.status_danger_bg, t.status_danger_border, t.status_danger
        self.setStyleSheet(
            f"#ConnectionBanner {{ background-color: {bg};"
            f" border: 1px solid {edge}; border-radius: 12px; }}"
            f"#ConnectionBanner QLabel {{ background: transparent; }}"
            f"#BannerTitle {{ color: {ink}; {font_css('body', bold=True)} }}"
            f"#BannerMessage {{ color: {t.text}; {font_css('body')} }}"
        )
        self.glyph.setPixmap(icon("circle-alert", color=ink).pixmap(24, 24))
        self._write_message()
