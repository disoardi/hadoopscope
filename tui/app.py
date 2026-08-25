"""Loop principale della TUI — sidebar persistente Home/Monitoring/Ops,
stack di Screen per sezione."""

from __future__ import print_function

import curses
import os
import sys

# ncurses aspetta ESCDELAY millisecondi (default 1000) prima di trattare
# un byte ESC come tasto ESC "vero" invece che l'inizio di una sequenza
# di escape più lunga (frecce, F1-F12, ecc.) — va abbassato PRIMA che
# curses inizializzi il terminale (env var letta da ncurses a initscr(),
# non da un'API Python — funziona anche sotto Python 3.6, a differenza
# di curses.set_escdelay() che richiede Python 3.9+).
os.environ.setdefault("ESCDELAY", "25")

import glob

from config import load_config
from bootstrap import discover_capabilities, ensure_ansible
import state_store
import tui.polling as polling
from tui.widgets import init_colors, draw_sidebar, draw_frame, confirm, REDRAW_TIMEOUT_MS
from tui.screens.home import HomeGridScreen
from tui.screens.monitoring import MonitoringMenuScreen
from tui.screens.ops import OpsToolListScreen

TABS = ["HOME", "MONITORING", "OPS"]

# File di config esclusi dal caricamento da directory — esempi/template/test,
# stessa convenzione già usata in passato da cluster_status.py.
_IGNORE_PREFIXES = ("example", "docker-", "docker_", "test.")


def _load_configs(path):
    # type: (str) -> tuple
    """Carica uno o più file di config YAML.

    Se path è un file, comportamento invariato (un solo config). Se è
    una directory, carica tutti i *.yaml al suo interno (esclusi
    example/test/docker-*) come config separate — un environment resta
    legato SOLO alle sezioni checks:/alerts:/download_dir del proprio
    file di origine, mai mescolate con quelle di un altro cliente anche
    se le chiavi coincidono per caso.

    Ritorna (envs, env_global, env_file):
      envs:       {env_name: env_config}
      env_global: {env_name: cfg_completo_del_file_di_origine}
      env_file:   {env_name: path_del_file_di_origine}
    """
    if os.path.isdir(path):
        paths = sorted(glob.glob(os.path.join(path, "*.yaml")))
        paths = [p for p in paths if not os.path.basename(p).startswith(_IGNORE_PREFIXES)]
    else:
        paths = [path]

    envs = {}         # type: dict
    env_global = {}   # type: dict
    env_file = {}      # type: dict
    for p in paths:
        cfg = load_config(p)
        for name, env_cfg in cfg.get("environments", {}).items():
            envs[name] = env_cfg
            env_global[name] = cfg
            env_file[name] = p
    return envs, env_global, env_file


class App(object):
    def __init__(self, stdscr, config_path):
        # type: (object, str) -> None
        self.stdscr = stdscr
        self.config_path = config_path
        self.envs, self.env_global, self.env_file = _load_configs(config_path)
        self.caps = ensure_ansible(discover_capabilities())
        self.active_tab = 0
        self.stacks = [
            [HomeGridScreen(self)],
            [MonitoringMenuScreen(self)],
            [OpsToolListScreen(self)],
        ]
        for stack in self.stacks:
            stack[0].enter()
        polling.start(self)

    def current_stack(self):
        # type: () -> list
        return self.stacks[self.active_tab]

    def current_screen(self):
        # type: () -> object
        return self.current_stack()[-1]

    def run(self):
        # type: () -> None
        # Non-bloccante: getch() torna -1 se nessun tasto entro l'intervallo,
        # cosi' il polling YARN in background puo' aggiornare la card Home
        # senza attendere un input dell'utente (vedi tui/polling.py).
        self.stdscr.timeout(REDRAW_TIMEOUT_MS)
        while True:
            self.stdscr.erase()
            draw_frame(self.stdscr, title="HADOOPSCOPE")
            draw_sidebar(self.stdscr, TABS, self.active_tab)
            self.current_screen().render(self.stdscr)
            self.stdscr.refresh()

            key = self.stdscr.getch()

            if key == -1:
                self.current_screen().on_idle_tick()
                continue

            if key == ord("\t") or key == curses.KEY_BTAB:
                # Lasciando una sezione, la si riporta alla schermata radice
                # (scarta lo stack di navigazione) — rientrarci più tardi
                # deve sempre ripartire da capo, mai restare bloccati sulla
                # schermata finale di un flusso precedente (es. risultato
                # dell'ultimo run in Monitoring). enter() sulla radice della
                # sezione di arrivo ne ricarica anche i dati (es. Home legge
                # di nuovo state_store, mostrando lo stato aggiornato).
                del self.current_stack()[1:]
                step = 1 if key == ord("\t") else -1
                self.active_tab = (self.active_tab + step) % len(TABS)
                self.current_screen().enter()
                continue

            if key == 27:  # ESC
                stack = self.current_stack()
                if len(stack) > 1:
                    stack.pop()
                    stack[-1].enter()
                elif confirm(self.stdscr, "Uscire da HadoopScope?"):
                    break
                continue

            result = self.current_screen().handle_input(key)
            if result == "pop":
                stack = self.current_stack()
                if len(stack) > 1:
                    stack.pop()
                    stack[-1].enter()
            elif result is not None:
                self.current_stack().append(result)
                # Disegna subito la nuova schermata (es. "Esecuzione in
                # corso...") PRIMA di chiamare enter(), che per alcune
                # schermate (MonitoringRunScreen) e' una chiamata di rete
                # sincrona e bloccante — altrimenti l'utente resta a
                # guardare la schermata precedente congelata, senza alcun
                # feedback, per tutta la durata reale dei check (bug
                # riportato: "sembra bloccato sulla pagina di selezione").
                self.stdscr.erase()
                draw_frame(self.stdscr, title="HADOOPSCOPE")
                draw_sidebar(self.stdscr, TABS, self.active_tab)
                result.render(self.stdscr)
                self.stdscr.refresh()
                result.enter()


def _run(stdscr, config_path):
    # type: (object, str) -> None
    init_colors()
    curses.curs_set(0)
    stdscr.keypad(True)
    state_store.init()
    App(stdscr, config_path).run()


def main():
    # type: () -> None
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/hadoopscope.yaml"
    try:
        curses.wrapper(_run, config_path)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
