"""The waiting-to-sync screen: a list is finished on this PC and its progress is not in the database yet.

Built on shared/components StatePanel (the empty-state card), so it names the cause, the list and
the one action, as every state panel does. The sync driver (main_window) keeps retrying on its
timer while this shows; the page only reports the Sync now press.
"""

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from shared.components.state_panel import StatePanel
from shared.i18n import _


class WaitingSyncPage(QWidget):
    syncRequested = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._panel: StatePanel | None = None

    def show_state(self, list_name: str, error: str | None = None) -> None:
        """Show the screen for a finished list; `error` is the permanent refusal, if any."""
        if self._panel is not None:
            self._layout.removeWidget(self._panel)
            self._panel.deleteLater()
        detail = (
            _("Cannot sync offline progress: {reason}. Nothing is lost; tell the admin.", reason=error)
            if error else ""
        )
        self._panel = StatePanel(
            _("Waiting to sync"),
            _("List {list} is finished and waiting to sync. Retrying every minute. Closing the app is safe.",
              list=list_name),
            detail=detail,
            action_text=_("Sync now"),
            parent=self,
        )
        self._panel.button.clicked.connect(self.syncRequested.emit)
        self._layout.addWidget(self._panel)
