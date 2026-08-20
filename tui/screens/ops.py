"""Tab Ops — lista tool disponibili, selezione env, input parametri
dichiarati da OpsParam, esecuzione e risultato."""

from __future__ import print_function

import curses

from ops import build_ops_registry
from tui.screens.base import Screen
from tui.widgets import safe_addstr, draw_list, draw_kv_table, ask_text, C_OK, C_WARN, C_CRIT, C_DIM

_STATUS_COLOR = {
    "OK": C_OK, "WARNING": C_WARN, "CRITICAL": C_CRIT,
    "UNKNOWN": C_DIM, "SKIPPED": C_DIM,
}

# Icona di stato per la testata del risultato. Non si basa solo su
# CheckResult.status: se details['state'] == 'RUNNING' (tipico di
# AppStatusTool su un'applicazione YARN ancora in corso), mostra
# l'icona "in esecuzione" invece del segno di spunta verde — un'app
# RUNNING non ha "avuto successo", è solo sana in questo momento.
_ICON_DONE    = ("✓", C_OK)
_ICON_FAILED  = ("✗", C_CRIT)
_ICON_RUNNING = ("◐", C_WARN)


def _icon_for(status, details):
    # type: (str, dict) -> tuple
    if (details or {}).get("state") == "RUNNING":
        return _ICON_RUNNING
    if status == "OK":
        return _ICON_DONE
    if status == "CRITICAL":
        return _ICON_FAILED
    return _ICON_RUNNING  # WARNING/UNKNOWN/SKIPPED: esito non definitivo


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
    """Selezione dell'environment, poi esegue subito il tool: prompt dei
    parametri + run(), tutto sincrono dentro handle_input() — nessuna
    schermata intermedia "in esecuzione" da attraversare con un tasto a
    vuoto (bug trovato in test manuale: la vecchia OpsParamInputScreen
    restava bloccata in attesa di un tasto che non faceva nulla)."""

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
            return self._run_tool(self.envs[self.cursor])
        return None

    def _run_tool(self, env_name):
        # type: (str) -> object
        stdscr = self.app.stdscr
        values = {}
        for param in self.tool_cls.params:
            value = ask_text(stdscr, param.help)
            if value is None and param.required:
                return OpsResultScreen(
                    self.app, self.tool_cls.name, "UNKNOWN",
                    "Annullato — parametro obbligatorio mancante", {})
            values[param.name] = value

        env_config = self.app.cfg["environments"][env_name]
        check_config = dict(env_config)
        if "checks" in self.app.cfg:
            check_config["checks"] = self.app.cfg["checks"]
        if "download_dir" in self.app.cfg:
            check_config["download_dir"] = self.app.cfg["download_dir"]

        instance = self.tool_cls(config=check_config, caps=self.app.caps)
        if not instance.can_run():
            return OpsResultScreen(
                self.app, self.tool_cls.name, "SKIPPED",
                "Requires: {}".format(self.tool_cls.requires), {})
        result = instance.run(**values)
        return OpsResultScreen(self.app, self.tool_cls.name, result.status,
                               result.message, result.details)


class OpsResultScreen(Screen):
    """Esito dell'esecuzione — icona di stato + tabella chiave/valore dei
    details del CheckResult (fallback sul testo del messaggio se il tool
    non ha prodotto details strutturati, es. errori di configurazione)."""

    def __init__(self, app, tool_name, status, message, details):
        Screen.__init__(self, app)
        self.tool_name = tool_name
        self.status = status
        self.message = message
        self.details = details or {}

    def render(self, stdscr):
        # type: (object) -> None
        icon, icon_color = _icon_for(self.status, self.details)
        attr = curses.color_pair(icon_color) | curses.A_BOLD
        safe_addstr(stdscr, 0, 20, "OPS — {} — {} {}".format(self.tool_name, icon, self.status), attr)

        rows = [(k, v) for k, v in self.details.items() if k != "counters"]
        if rows:
            y = draw_kv_table(stdscr, rows, y=2, x=20, w=70)
        else:
            y = 2
            for line in self.message.splitlines():
                safe_addstr(stdscr, y, 20, line[:80])
                y += 1

        safe_addstr(stdscr, y + 1, 20, "ESC torna alla lista tool", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        return None
