"""Tab Home — grid riassuntiva per cluster (letta da state_store) e
drill-down di dettaglio su un singolo env."""

from __future__ import print_function

import curses
import datetime
import time

import state_store
from tui.screens.base import Screen
from tui.widgets import safe_addstr, draw_box, C_OK, C_WARN, C_CRIT, C_DIM

_STALE_AFTER_HOURS = 24

# Il polling YARN in background (tui/polling.py) scrive in state_store ogni
# 30s — qui si rilegge ogni 5s mentre si resta fermi sul tab Home, non ad
# ogni tick di redraw (1s) per non martellare sqlite inutilmente.
_IDLE_REFRESH_SECONDS = 5

_STATUS_COLOR = {
    "OK": C_OK, "WARNING": C_WARN, "CRITICAL": C_CRIT,
    "UNKNOWN": C_DIM, "SKIPPED": C_DIM,
}


def _age_hours(run_at_iso):
    # type: (str) -> object
    """Ore trascorse da run_at_iso (isoformat) a ora. None se non parsabile."""
    try:
        run_at = datetime.datetime.strptime(run_at_iso.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    except (ValueError, AttributeError):
        return None
    return (datetime.datetime.now() - run_at).total_seconds() / 3600.0


class HomeGridScreen(Screen):
    """Vista principale del tab Home — una card per environment configurato."""

    def __init__(self, app):
        Screen.__init__(self, app)
        self.cursor = 0
        self.envs = []  # type: list
        self._last_refresh = 0.0

    def on_idle_tick(self):
        # type: () -> None
        if time.time() - self._last_refresh >= _IDLE_REFRESH_SECONDS:
            self.enter()

    def enter(self):
        # type: () -> None
        self._last_refresh = time.time()
        configured = sorted(self.app.envs.keys())
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
                "stale_hours": _age_hours(row["oldest_run_at"]) if row else None,
            })
        if self.cursor >= len(self.envs):
            self.cursor = max(0, len(self.envs) - 1)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "HOME — {} environment(s) configurati".format(len(self.envs)),
                   curses.A_BOLD)
        col_w, row_h = 26, 9
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
                               "HDFS: {:.2f}% usato".format(entry["hdfs"]["used_pct"])[:col_w - 4])
                    line_y += 1
                if entry["yarn"]:
                    safe_addstr(stdscr, line_y, x + 2,
                               "YARN: {} run / {} pend".format(
                                   entry["yarn"].get("appsRunning", 0),
                                   entry["yarn"].get("appsPending", 0))[:col_w - 4])
                    line_y += 1
                if entry["stale_hours"] is not None and entry["stale_hours"] >= _STALE_AFTER_HOURS:
                    safe_addstr(stdscr, line_y, x + 2,
                               "⚠ dati vecchi {}h, rilancia".format(int(entry["stale_hours"]))[:col_w - 4],
                               curses.color_pair(C_WARN))
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


_ROW_H = 3  # righe per singolo check (status + messaggio + spaziatura)


class HomeDetailScreen(Screen):
    """Drill-down su un singolo env — tutte le righe check_state, scrollabile."""

    def __init__(self, app, env_name):
        Screen.__init__(self, app)
        self.env_name = env_name
        self.rows = []    # type: list
        self.scroll = 0

    def enter(self):
        # type: () -> None
        self.rows = state_store.get_env_summary(self.env_name)
        self.scroll = 0

    def _visible_count(self, stdscr):
        # type: (object) -> int
        max_y, _ = stdscr.getmaxyx()
        available = max_y - 5  # titolo (0) + margine + footer + bordo frame
        return max(1, available // _ROW_H)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "HOME — dettaglio {}".format(self.env_name), curses.A_BOLD)
        if not self.rows:
            safe_addstr(stdscr, 2, 20, "Nessun check eseguito per questo environment.")
            return
        visible = self._visible_count(stdscr)
        y = 2
        for row in self.rows[self.scroll:self.scroll + visible]:
            attr = curses.color_pair(_STATUS_COLOR.get(row["status"], C_DIM))
            safe_addstr(stdscr, y, 20, "[{}] {}".format(row["status"], row["check_name"]), attr | curses.A_BOLD)
            safe_addstr(stdscr, y + 1, 22, row["message"].splitlines()[0][:70], curses.color_pair(C_DIM))
            y += _ROW_H
        footer = "↑↓ scorri · ESC torna alla grid"
        if len(self.rows) > visible:
            footer += "  ({}-{}/{})".format(
                self.scroll + 1, min(self.scroll + visible, len(self.rows)), len(self.rows))
        safe_addstr(stdscr, y, 20, footer, curses.color_pair(C_DIM))

    def handle_input(self, key):
        # type: (int) -> object
        visible = self._visible_count(self.app.stdscr)
        max_scroll = max(0, len(self.rows) - visible)
        if key == curses.KEY_UP and self.scroll > 0:
            self.scroll -= 1
        elif key == curses.KEY_DOWN and self.scroll < max_scroll:
            self.scroll += 1
        elif key == curses.KEY_NPAGE:  # Page Down
            self.scroll = min(max_scroll, self.scroll + visible)
        elif key == curses.KEY_PPAGE:  # Page Up
            self.scroll = max(0, self.scroll - visible)
        return None  # ESC gestito centralmente da App (pop dello stack)
