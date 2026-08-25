"""Tab Monitoring — sottomenu 'esegui check ora' e 'gestisci schedulati'.

La logica di parsing/scrittura crontab e' riusata da cluster_status.py
(decisione esplicita in brainstorming: e' codice non-visivo, non fa parte
della riscrittura del look)."""

from __future__ import print_function

import curses

from cluster_status import (
    _crontab_read, _crontab_write, _parse_hs_block, _format_hs_block,
    _cron_label, CHECK_CATEGORIES,
)
from hadoopscope import run_checks_for_env
import applog as _applog
import state_store
from tui.screens.base import Screen
from tui.widgets import safe_addstr, draw_list, ask_text, confirm, C_DIM, C_OK, C_WARN, C_CRIT


class MonitoringMenuScreen(Screen):
    """Vista principale del tab Monitoring — due voci."""

    _ITEMS = ["Esegui check ora", "Gestisci check schedulati"]

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "MONITORING", curses.A_BOLD)
        draw_list(stdscr, self._ITEMS, self.cursor, y=2, x=20, h=5, w=40)
        safe_addstr(stdscr, 8, 20, "↑↓ naviga · Invio seleziona", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self._ITEMS) - 1:
            self.cursor += 1
        elif key in (curses.KEY_ENTER, 10, 13):
            if self.cursor == 0:
                return MonitoringEnvPickerScreen(self.app)
            return MonitoringScheduleListScreen(self.app)
        return None


class MonitoringEnvPickerScreen(Screen):
    """Multi-select environment su cui lanciare i check."""

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0
        self.envs = sorted(app.envs.keys())
        self.selected = set()

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20,
                   "MONITORING — scegli environment (SPAZIO seleziona, A tutti/nessuno, INVIO conferma)",
                   curses.A_BOLD)
        draw_list(stdscr, self.envs, self.cursor, y=2, x=20, h=15, w=50, selected=self.selected)

    def handle_input(self, key):
        # type: (int) -> object
        if not self.envs:
            return None
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.envs) - 1:
            self.cursor += 1
        elif key == ord(" "):
            if self.cursor in self.selected:
                self.selected.discard(self.cursor)
            else:
                self.selected.add(self.cursor)
        elif key in (ord("a"), ord("A")):
            if len(self.selected) == len(self.envs):
                self.selected.clear()
            else:
                self.selected = set(range(len(self.envs)))
        elif key in (curses.KEY_ENTER, 10, 13) and self.selected:
            chosen = [self.envs[i] for i in sorted(self.selected)]
            return MonitoringCheckPickerScreen(self.app, chosen)
        return None


class MonitoringCheckPickerScreen(Screen):
    """Multi-select categoria check."""

    def __init__(self, app, envs):
        Screen.__init__(self, app)
        self.app_envs = envs
        self.cursor = 0
        self.categories = CHECK_CATEGORIES  # [(key, label), ...]
        self.selected = set([0])  # "all" preselezionato

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20,
                   "MONITORING — scegli check (SPAZIO seleziona, A tutti/nessuno, INVIO conferma)",
                   curses.A_BOLD)
        items = [label for key, label in self.categories]
        draw_list(stdscr, items, self.cursor, y=2, x=20, h=10, w=60, selected=self.selected)

    def handle_input(self, key):
        # type: (int) -> object
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.categories) - 1:
            self.cursor += 1
        elif key == ord(" "):
            if self.cursor == 0:
                # "all" e' mutuamente esclusivo con le categorie specifiche.
                self.selected = set() if 0 in self.selected else set([0])
            elif self.cursor in self.selected:
                self.selected.discard(self.cursor)
            else:
                self.selected.add(self.cursor)
                self.selected.discard(0)  # una categoria specifica esclude "all"
        elif key in (ord("a"), ord("A")):
            if len(self.selected) == len(self.categories):
                self.selected.clear()
            else:
                self.selected = set(range(len(self.categories)))
        elif key in (curses.KEY_ENTER, 10, 13) and self.selected:
            checks = [self.categories[i][0] for i in sorted(self.selected)]
            return MonitoringRunScreen(self.app, self.app_envs, checks)
        return None


class MonitoringRunScreen(Screen):
    """Esecuzione bloccante dei check scelti, poi prompt schedulazione."""

    def __init__(self, app, envs, checks):
        Screen.__init__(self, app)
        self.envs = envs
        self.checks = checks
        self.results_by_env = {}  # type: dict
        self.done = False

    def enter(self):
        # type: () -> None
        class _Args(object):
            pass
        args = _Args()
        args.checks = self.checks
        args.dry_run = False

        for env_name in self.envs:
            # env_config/global vengono dal file di origine di QUESTO
            # specifico environment — isolamento esplicito tra clienti
            # diversi caricati nella stessa sessione TUI.
            env_config = self.app.envs[env_name]
            env_cfg_global = self.app.env_global[env_name]
            results = run_checks_for_env(env_name, env_config, env_cfg_global, self.app.caps, args)
            self.results_by_env[env_name] = results
            for r in results:
                _applog.log_result(r)
                state_store.save_result(env_name, r)
        self.done = True

    def render(self, stdscr):
        # type: (object) -> None
        if not self.done:
            safe_addstr(stdscr, 0, 20, "Esecuzione in corso...", curses.A_BOLD)
            return
        safe_addstr(stdscr, 0, 20, "MONITORING — risultati", curses.A_BOLD)
        y = 2
        for env_name, results in self.results_by_env.items():
            counts = {}  # type: dict
            for r in results:
                counts[r.status] = counts.get(r.status, 0) + 1
            summary = ", ".join("{} {}".format(v, k) for k, v in counts.items())
            safe_addstr(stdscr, y, 20, "{}: {}".format(env_name, summary))
            y += 1
        safe_addstr(stdscr, y + 1, 20, "ESC per tornare, o premi 's' per schedulare questo run", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if self.done and key in (ord("s"), ord("S")):
            return MonitoringScheduleAddScreen(self.app, self.envs, self.checks)
        return None


class MonitoringScheduleAddScreen(Screen):
    """Aggiunge una entry crontab hs: per env/checks scelti (riusa il
    parsing/formato esistente di cluster_status.py)."""

    def __init__(self, app, envs, checks):
        Screen.__init__(self, app)
        self.envs = envs
        self.checks = checks
        self.done_message = ""

    def enter(self):
        # type: () -> None
        # Una entry crontab ha un solo --config: se gli env selezionati
        # vengono da file diversi (client diversi caricati nella stessa
        # sessione TUI), non c'è un path unico corretto da usare — meglio
        # bloccare con un messaggio chiaro che schedulare qualcosa di sbagliato.
        files = set(self.app.env_file[e] for e in self.envs)
        if len(files) > 1:
            self.done_message = (
                "Impossibile schedulare insieme environment di file diversi ({}) "
                "— schedula separatamente per ciascun file.".format(", ".join(sorted(files)))
            )
            return
        config_path = files.pop()

        cron_expr = ask_text(self.app.stdscr, "Espressione cron (es. */15 * * * *)", "*/15 * * * *")
        if not cron_expr:
            self.done_message = "Annullato."
            return
        entry = {
            "config": config_path,
            "envs": self.envs,
            "checks": ",".join(self.checks),
            "cron": cron_expr,
            "enabled": True,
            "log_file": "",
        }
        marker, cmd_line = _format_hs_block(entry)
        other_lines, hs_blocks = _crontab_read()
        if other_lines is None:
            self.done_message = "crontab non disponibile su questo sistema."
            return
        hs_blocks.append({"marker": marker, "cmd_line": cmd_line, "enabled": True})
        ok, err = _crontab_write(other_lines, hs_blocks)
        self.done_message = "Schedulato ({}).".format(_cron_label(cron_expr)) if ok else "Errore: {}".format(err)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "MONITORING — {}".format(self.done_message), curses.A_BOLD)
        safe_addstr(stdscr, 2, 20, "ESC torna al menu Monitoring", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        return None


class MonitoringScheduleListScreen(Screen):
    """Elenca le entry crontab hs: esistenti, con azioni abilita/disabilita/elimina."""

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0
        self.other_lines = []  # type: list
        self.hs_blocks = []    # type: list

    def enter(self):
        # type: () -> None
        other_lines, hs_blocks = _crontab_read()
        self.other_lines = other_lines or []
        self.hs_blocks = hs_blocks
        if self.cursor >= len(self.hs_blocks):
            self.cursor = max(0, len(self.hs_blocks) - 1)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "MONITORING — check schedulati", curses.A_BOLD)
        if not self.hs_blocks:
            safe_addstr(stdscr, 2, 20, "Nessuna entry schedulata.")
            return
        for i, block in enumerate(self.hs_blocks):
            entry = _parse_hs_block(block)
            status = "abilitato" if entry["enabled"] else "disabilitato"
            attr = curses.A_REVERSE if i == self.cursor else 0
            label = "{} — {} — envs={}".format(_cron_label(entry["cron"]), status, ",".join(entry["envs"]))
            safe_addstr(stdscr, 2 + i, 20, label[:80], attr)
        safe_addstr(stdscr, 3 + len(self.hs_blocks), 20,
                   "↑↓ naviga · SPAZIO abilita/disabilita · d elimina · ESC torna", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if not self.hs_blocks:
            return None
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.hs_blocks) - 1:
            self.cursor += 1
        elif key == ord(" "):
            block = self.hs_blocks[self.cursor]
            entry = _parse_hs_block(block)
            entry["enabled"] = not entry["enabled"]
            marker, cmd_line = _format_hs_block(entry)
            self.hs_blocks[self.cursor] = {"marker": marker, "cmd_line": cmd_line, "enabled": entry["enabled"]}
            _crontab_write(self.other_lines, self.hs_blocks)
        elif key == ord("d"):
            if confirm(self.app.stdscr, "Eliminare questa entry schedulata?"):
                del self.hs_blocks[self.cursor]
                _crontab_write(self.other_lines, self.hs_blocks)
                self.cursor = max(0, self.cursor - 1)
        return None
