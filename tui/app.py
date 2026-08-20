"""Loop principale della TUI — sidebar persistente Home/Monitoring/Ops,
stack di Screen per sezione."""

from __future__ import print_function

import curses
import sys

from config import load_config
from bootstrap import discover_capabilities, ensure_ansible
import state_store
from tui.widgets import init_colors, draw_sidebar, confirm
from tui.screens.home import HomeGridScreen
from tui.screens.monitoring import MonitoringMenuScreen
from tui.screens.ops import OpsToolListScreen

TABS = ["HOME", "MONITORING", "OPS"]


class App(object):
    def __init__(self, stdscr, config_path):
        # type: (object, str) -> None
        self.stdscr = stdscr
        self.config_path = config_path
        self.cfg = load_config(config_path)
        self.caps = ensure_ansible(discover_capabilities())
        self.active_tab = 0
        self.stacks = [
            [HomeGridScreen(self)],
            [MonitoringMenuScreen(self)],
            [OpsToolListScreen(self)],
        ]
        for stack in self.stacks:
            stack[0].enter()

    def current_stack(self):
        # type: () -> list
        return self.stacks[self.active_tab]

    def current_screen(self):
        # type: () -> object
        return self.current_stack()[-1]

    def run(self):
        # type: () -> None
        while True:
            self.stdscr.erase()
            draw_sidebar(self.stdscr, TABS, self.active_tab)
            self.current_screen().render(self.stdscr)
            self.stdscr.refresh()

            key = self.stdscr.getch()

            if key == ord("\t"):
                self.active_tab = (self.active_tab + 1) % len(TABS)
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
