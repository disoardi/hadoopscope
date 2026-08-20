"""Tab Home — grid riassuntiva per cluster (letta da state_store) e
drill-down di dettaglio su un singolo env."""

from __future__ import print_function

import curses

import state_store
from tui.screens.base import Screen
from tui.widgets import safe_addstr, draw_box, C_OK, C_WARN, C_CRIT, C_DIM

_STATUS_COLOR = {
    "OK": C_OK, "WARNING": C_WARN, "CRITICAL": C_CRIT,
    "UNKNOWN": C_DIM, "SKIPPED": C_DIM,
}


class HomeGridScreen(Screen):
    """Vista principale del tab Home — una card per environment configurato."""

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0
        self.envs = []  # type: list

    def enter(self):
        # type: () -> None
        configured = sorted(self.app.cfg.get("environments", {}).keys())
        summary_by_env = {row["env"]: row for row in state_store.get_all_envs_summary()}
        self.envs = []
        for env in configured:
            row = summary_by_env.get(env)
            # HdfsSpace/YarnClusterMetrics sono check "informativi" (non
            # solo salute) letti dallo stesso check_state — nessuna nuova
            # tabella o query aggregata, solo righe extra già persistite.
            by_check = {r["check_name"]: r for r in state_store.get_env_summary(env)}
            hdfs = by_check.get("HdfsSpace")
            yarn = by_check.get("YarnClusterMetrics")
            self.envs.append({
                "env": env,
                "worst_status": row["worst_status"] if row else None,
                "counts": row["counts"] if row else {},
                "hdfs": hdfs["details"] if hdfs else None,
                "yarn": yarn["details"] if yarn else None,
            })
        if self.cursor >= len(self.envs):
            self.cursor = max(0, len(self.envs) - 1)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "HOME — {} environment(s) configurati".format(len(self.envs)),
                   curses.A_BOLD)
        col_w, row_h = 26, 8
        for i, entry in enumerate(self.envs):
            col = i % 3
            row = i // 3
            y = 2 + row * row_h
            x = 20 + col * (col_w + 2)
            attr = curses.A_REVERSE if i == self.cursor else 0
            draw_box(stdscr, y, x, row_h, col_w)
            safe_addstr(stdscr, y + 1, x + 2, entry["env"][:col_w - 4], attr | curses.A_BOLD)
            if entry["worst_status"] is None:
                safe_addstr(stdscr, y + 2, x + 2, "nessun check ancora", curses.color_pair(C_DIM))
            else:
                status_attr = curses.color_pair(_STATUS_COLOR.get(entry["worst_status"], C_DIM))
                safe_addstr(stdscr, y + 2, x + 2, "● {}".format(entry["worst_status"]), status_attr)
                counts_str = "  ".join("{} {}".format(v, k) for k, v in entry["counts"].items())
                safe_addstr(stdscr, y + 3, x + 2, counts_str[:col_w - 4])
                line_y = y + 5
                if entry["hdfs"] and entry["hdfs"].get("used_pct") is not None:
                    safe_addstr(stdscr, line_y, x + 2,
                               "HDFS: {:.0f}% usato".format(entry["hdfs"]["used_pct"])[:col_w - 4])
                    line_y += 1
                if entry["yarn"]:
                    safe_addstr(stdscr, line_y, x + 2,
                               "YARN: {} run / {} pend".format(
                                   entry["yarn"].get("appsRunning", 0),
                                   entry["yarn"].get("appsPending", 0))[:col_w - 4])
        safe_addstr(stdscr, 2 + ((len(self.envs) // 3) + 1) * row_h + 1, 20,
                   "↑↓←→ naviga · Invio dettaglio · Tab cambia sezione", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        if not self.envs:
            return None
        cols = 3
        if key == curses.KEY_LEFT and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_RIGHT and self.cursor < len(self.envs) - 1:
            self.cursor += 1
        elif key == curses.KEY_UP and self.cursor - cols >= 0:
            self.cursor -= cols
        elif key == curses.KEY_DOWN and self.cursor + cols < len(self.envs):
            self.cursor += cols
        elif key in (curses.KEY_ENTER, 10, 13):
            return HomeDetailScreen(self.app, self.envs[self.cursor]["env"])
        return None


class HomeDetailScreen(Screen):
    """Drill-down su un singolo env — tutte le righe check_state."""

    def __init__(self, app, env_name):
        Screen.__init__(self, app)
        self.env_name = env_name
        self.rows = []  # type: list

    def enter(self):
        # type: () -> None
        self.rows = state_store.get_env_summary(self.env_name)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "HOME — dettaglio {}".format(self.env_name), curses.A_BOLD)
        if not self.rows:
            safe_addstr(stdscr, 2, 20, "Nessun check eseguito per questo environment.")
            return
        y = 2
        for row in self.rows:
            attr = curses.color_pair(_STATUS_COLOR.get(row["status"], C_DIM))
            safe_addstr(stdscr, y, 20, "[{}] {}".format(row["status"], row["check_name"]), attr | curses.A_BOLD)
            safe_addstr(stdscr, y + 1, 22, row["message"].splitlines()[0][:70], curses.color_pair(C_DIM))
            y += 3
        safe_addstr(stdscr, y, 20, "ESC torna alla grid", curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        return None  # ESC gestito centralmente da App (pop dello stack)
