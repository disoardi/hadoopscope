"""Tab Ops — lista tool disponibili, selezione env, input parametri
dichiarati da OpsParam, esecuzione e risultato."""

from __future__ import print_function

import curses

from ops import build_ops_registry
from tui.screens.base import Screen
from tui.widgets import safe_addstr, draw_list, ask_text, C_OK, C_WARN, C_CRIT, C_DIM

_STATUS_COLOR = {
    "OK": C_OK, "WARNING": C_WARN, "CRITICAL": C_CRIT,
    "UNKNOWN": C_DIM, "SKIPPED": C_DIM,
}


class OpsToolListScreen(Screen):
    """Vista principale del tab Ops — elenco tool dal registry."""

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0
        self.tools = sorted(build_ops_registry().items())  # [(name, cls), ...]

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "OPS — {} tool disponibili".format(len(self.tools)), curses.A_BOLD)
        items = ["{} — {}".format(name, cls.description) for name, cls in self.tools]
        draw_list(stdscr, items, self.cursor, y=2, x=20, h=15, w=70)
        safe_addstr(stdscr, 18, 20, "↑↓ naviga · Invio seleziona · ESC torna", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if not self.tools:
            return None
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.tools) - 1:
            self.cursor += 1
        elif key in (curses.KEY_ENTER, 10, 13):
            name, cls = self.tools[self.cursor]
            return OpsEnvPickerScreen(self.app, cls)
        return None


class OpsEnvPickerScreen(Screen):
    """Selezione dell'environment su cui eseguire il tool scelto."""

    def __init__(self, app, tool_cls):
        Screen.__init__(self, app)
        self.tool_cls = tool_cls
        self.cursor = 0
        self.envs = sorted(app.cfg.get("environments", {}).keys())

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "OPS — {} — scegli environment".format(self.tool_cls.name), curses.A_BOLD)
        draw_list(stdscr, self.envs, self.cursor, y=2, x=20, h=15, w=50)
        safe_addstr(stdscr, 18, 20, "↑↓ naviga · Invio seleziona · ESC torna", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if not self.envs:
            return None
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.envs) - 1:
            self.cursor += 1
        elif key in (curses.KEY_ENTER, 10, 13):
            return OpsParamInputScreen(self.app, self.tool_cls, self.envs[self.cursor])
        return None


class OpsParamInputScreen(Screen):
    """Prompt in sequenza per ogni OpsParam del tool, poi esecuzione."""

    def __init__(self, app, tool_cls, env_name):
        Screen.__init__(self, app)
        self.tool_cls = tool_cls
        self.env_name = env_name
        self.values = {}  # type: dict
        self.result_screen = None  # type: object

    def enter(self):
        # type: () -> None
        """I prompt testuali usano stdscr direttamente (non c'e' modo
        pulito di farlo dentro render(), che non ha accesso a getstr()
        in un ciclo bloccante coerente con l'app loop) — raccolti tutti
        qui, poi si passa a esecuzione o a un errore mostrato in render()."""
        stdscr = self.app.stdscr
        for param in self.tool_cls.params:
            value = ask_text(stdscr, param.help)
            if value is None and param.required:
                self.result_screen = OpsResultScreen(
                    self.app, self.tool_cls.name,
                    status="UNKNOWN", message="Annullato — parametro obbligatorio mancante")
                return
            self.values[param.name] = value
        self.result_screen = self._run()

    def _run(self):
        # type: () -> object
        env_config = self.app.cfg["environments"][self.env_name]
        check_config = dict(env_config)
        if "checks" in self.app.cfg:
            check_config["checks"] = self.app.cfg["checks"]
        if "download_dir" in self.app.cfg:
            check_config["download_dir"] = self.app.cfg["download_dir"]

        instance = self.tool_cls(config=check_config, caps=self.app.caps)
        if not instance.can_run():
            return OpsResultScreen(
                self.app, self.tool_cls.name,
                status="SKIPPED", message="Requires: {}".format(self.tool_cls.requires))
        result = instance.run(**self.values)
        return OpsResultScreen(self.app, self.tool_cls.name, result.status, result.message)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "Esecuzione in corso... (premi un tasto)", curses.A_BOLD)

    def handle_input(self, key):
        # type: (int) -> object
        """enter() ha già eseguito prompt+run() in modo sincrono prima che
        questa schermata venisse anche solo disegnata — self.result_screen
        è quindi già pronto. Qualunque tasto ci transita sopra: senza
        questo return la schermata resterebbe bloccata per sempre in
        attesa di un tasto che non fa nulla (bug reale trovato in test
        manuale — 'app-status' restava su 'Esecuzione in corso...')."""
        return self.result_screen


class OpsResultScreen(Screen):
    """Esito dell'esecuzione — status/message del CheckResult."""

    def __init__(self, app, tool_name, status, message):
        Screen.__init__(self, app)
        self.tool_name = tool_name
        self.status = status
        self.message = message

    def render(self, stdscr):
        # type: (object) -> None
        attr = curses.color_pair(_STATUS_COLOR.get(self.status, C_DIM)) | curses.A_BOLD
        safe_addstr(stdscr, 0, 20, "OPS — {} — {}".format(self.tool_name, self.status), attr)
        y = 2
        for line in self.message.splitlines():
            safe_addstr(stdscr, y, 20, line[:80])
            y += 1
        safe_addstr(stdscr, y + 1, 20, "ESC torna alla lista tool", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        return None
