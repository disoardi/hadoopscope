"""Screen — contratto comune per ogni schermata della TUI.

Ogni sezione (Home/Monitoring/Ops) mantiene un piccolo stack di Screen;
handle_input() ritorna:
  - "pop"           -> torna alla schermata precedente nello stack
  - un'istanza Screen -> viene pushata sullo stack (drill-down)
  - "quit"           -> chiude l'applicazione
  - None             -> resta sulla schermata corrente (nessuna transizione)
"""

from __future__ import print_function


class Screen(object):
    def __init__(self, app):
        # type: (object) -> None
        """app: riferimento all'istanza App (config, caps, stdscr condivisi)."""
        self.app = app

    def enter(self):
        # type: () -> None
        """Chiamato quando la schermata diventa attiva (push o ritorno da
        una schermata figlia). Default no-op — sovrascrivere per
        ricaricare dati (es. rileggere state_store)."""
        pass

    def render(self, stdscr):
        # type: (object) -> None
        raise NotImplementedError(
            "Implement render() in {}".format(self.__class__.__name__)
        )

    def handle_input(self, key):
        # type: (int) -> object
        raise NotImplementedError(
            "Implement handle_input() in {}".format(self.__class__.__name__)
        )
