#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HadoopScope TUI — Interactive launcher for cluster checks.

Usage:
    python3 tui.py [--config-dir DIR]

Navigation:
    UP/DOWN    Navigate list
    SPACE      Toggle selection
    A          Select / deselect all
    ENTER      Confirm and proceed
    Q / ESC    Go back / quit
"""
from __future__ import print_function

import curses
import glob as _glob
import os
import subprocess
import sys

# Aggiungiamo la directory del progetto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Check categories ──────────────────────────────────────────────────────────

CHECK_CATEGORIES = [
    ("all",    "All checks (health + hdfs + hive + yarn)"),
    ("health", "Service health (Ambari / Cloudera Manager)"),
    ("hdfs",   "HDFS: space, DataNodes, writability"),
    ("hive",   "HiveServer2 connectivity"),
    ("yarn",   "YARN: node health + queue usage"),
]

# ── Config discovery ─────────────────────────────────────────────────────────

_SEARCH_PATTERNS = [
    "config/*.yaml",
    "config/*.yml",
    os.path.expanduser("~/.hadoopscope/*.yaml"),
    os.path.expanduser("~/.hadoopscope/*.yml"),
    "hadoopscope.yaml",
    "hadoopscope.yml",
]


def find_config_files():
    # type: () -> list
    """Return list of (display_label, abs_path) for discovered YAML configs."""
    found = []
    seen = set()  # type: set
    for pattern in _SEARCH_PATTERNS:
        for path in sorted(_glob.glob(pattern)):
            abs_path = os.path.abspath(path)
            if abs_path not in seen:
                seen.add(abs_path)
                found.append((os.path.relpath(path), abs_path))
    return found


def load_env_names(config_path):
    # type: (str) -> list
    """Return list of environment names from a config file."""
    try:
        from config import load_config
        cfg = load_config(config_path)
        return list(cfg.get("environments", {}).keys())
    except Exception:
        # Fallback: naive regex scan (no PyYAML needed)
        import re
        envs = []
        in_envs = False
        try:
            with open(config_path, "r") as fh:
                for line in fh:
                    if re.match(r'^environments\s*:', line):
                        in_envs = True
                        continue
                    if in_envs:
                        m = re.match(r'^  ([\w][\w.-]*)\s*:', line)
                        if m and m.group(1) not in ("type", "enabled", "ambari_url",
                                                     "cm_url", "webhdfs", "kerberos",
                                                     "yarn", "ansible", "hive"):
                            envs.append(m.group(1))
                        elif re.match(r'^\S', line):
                            break
        except IOError:
            pass
        return envs


# ── Colour palette ────────────────────────────────────────────────────────────

_C_HEADER   = 1
_C_BORDER   = 2
_C_SEL      = 3   # selected item (checkbox ticked)
_C_CURSOR   = 4   # cursor highlight
_C_STATUS   = 5   # step indicator
_C_OK       = 6
_C_WARN     = 7
_C_CRIT     = 8
_C_DIM      = 9


def _init_colors():
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(_C_HEADER, curses.COLOR_CYAN,    -1)
    curses.init_pair(_C_BORDER, curses.COLOR_BLUE,    -1)
    curses.init_pair(_C_SEL,    curses.COLOR_GREEN,   -1)
    curses.init_pair(_C_CURSOR, curses.COLOR_BLACK,   curses.COLOR_CYAN)
    curses.init_pair(_C_STATUS, curses.COLOR_YELLOW,  -1)
    curses.init_pair(_C_OK,     curses.COLOR_GREEN,   -1)
    curses.init_pair(_C_WARN,   curses.COLOR_YELLOW,  -1)
    curses.init_pair(_C_CRIT,   curses.COLOR_RED,     -1)
    curses.init_pair(_C_DIM,    curses.COLOR_WHITE,   -1)


# ── Drawing helpers ───────────────────────────────────────────────────────────

def _safe_addstr(win, y, x, text, attr=0):
    """addstr that silently ignores writes outside the window bounds."""
    max_y, max_x = win.getmaxyx()
    if y < 0 or y >= max_y or x < 0 or x >= max_x:
        return
    available = max_x - x - 1
    if available <= 0:
        return
    try:
        win.addstr(y, x, text[:available], attr)
    except curses.error:
        pass


def _draw_box(win, y, x, h, w, title=None):
    """Draw a box with optional centred title in the top border."""
    attr = curses.color_pair(_C_BORDER)
    max_y, max_x = win.getmaxyx()

    def _ch(py, px, ch):
        if 0 <= py < max_y and 0 <= px < max_x - 1:
            try:
                win.addch(py, px, ch, attr)
            except curses.error:
                pass

    _ch(y,     x,     curses.ACS_ULCORNER)
    _ch(y,     x+w-1, curses.ACS_URCORNER)
    _ch(y+h-1, x,     curses.ACS_LLCORNER)
    _ch(y+h-1, x+w-1, curses.ACS_LRCORNER)
    for i in range(1, w - 1):
        _ch(y,     x+i, curses.ACS_HLINE)
        _ch(y+h-1, x+i, curses.ACS_HLINE)
    for i in range(1, h - 1):
        _ch(y+i, x,     curses.ACS_VLINE)
        _ch(y+i, x+w-1, curses.ACS_VLINE)

    if title:
        tw = len(title) + 2
        tx = x + max(1, (w - tw) // 2)
        _safe_addstr(win, y, tx, " {} ".format(title),
                     curses.color_pair(_C_HEADER) | curses.A_BOLD)


def _draw_list(win, items, cursor, selected, y, x, h, w, single=False):
    """
    Draw a scrollable selectable list inside a box (items go from y+1).

    items    : list of (key, label)
    cursor   : current row index (0-based over all items)
    selected : set of keys that are ticked
    single   : True = radio style (●/○), False = checkbox style ([x]/[ ])
    """
    inner_h = h - 2         # rows available between top/bottom borders
    inner_w = w - 4         # chars available (2 border + 1 pad each side)

    # Scroll: keep cursor visible
    offset = max(0, cursor - inner_h + 1)

    for i in range(inner_h):
        row = offset + i
        ry = y + 1 + i
        rx = x + 2
        if row >= len(items):
            # Clear leftover text
            _safe_addstr(win, ry, rx, " " * inner_w)
            continue

        key, label = items[row]
        is_cursor   = (row == cursor)
        is_selected = key in selected

        if single:
            prefix = "(o) " if is_selected else "( ) "
        else:
            prefix = "[x] " if is_selected else "[ ] "

        line = "{}{}" .format(prefix, label)
        if len(line) > inner_w:
            line = line[:inner_w - 3] + "..."

        padded = " {:<{w}} ".format(line, w=inner_w - 2)

        if is_cursor:
            attr = curses.color_pair(_C_CURSOR) | curses.A_BOLD
        elif is_selected:
            attr = curses.color_pair(_C_SEL)
        else:
            attr = curses.A_NORMAL

        _safe_addstr(win, ry, rx, padded, attr)

    # Scroll indicator
    total = len(items)
    if total > inner_h:
        pct = int(round((cursor / float(total - 1)) * (inner_h - 1)))
        try:
            win.addch(y + 1 + pct, x + w - 1, curses.ACS_DIAMOND,
                      curses.color_pair(_C_BORDER))
        except curses.error:
            pass


def _draw_header(win, step_label):
    """Draw the fixed title bar and step indicator."""
    max_y, max_x = win.getmaxyx()
    title = "  HadoopScope TUI  "
    _safe_addstr(win, 0, 0, " " * max_x,
                 curses.color_pair(_C_HEADER) | curses.A_BOLD)
    _safe_addstr(win, 0, (max_x - len(title)) // 2, title,
                 curses.color_pair(_C_HEADER) | curses.A_BOLD)
    _safe_addstr(win, 1, 2, step_label,
                 curses.color_pair(_C_STATUS) | curses.A_BOLD)


def _draw_footer(win, help_text):
    max_y, max_x = win.getmaxyx()
    _safe_addstr(win, max_y - 1, 0, " " * (max_x - 1),
                 curses.color_pair(_C_BORDER))
    _safe_addstr(win, max_y - 1, 1, help_text,
                 curses.color_pair(_C_BORDER))


# ── Step screens ──────────────────────────────────────────────────────────────

def _step_config(stdscr, config_files):
    """STEP 1 — Select a config file (radio / single-select)."""
    items = [(p, d) for d, p in config_files]   # key=abs_path, label=display
    selected = {items[0][0]} if items else set()
    cursor = 0

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bh = min(len(items) + 4, max_y - 6)
        bx, by = 2, 3

        _draw_header(stdscr, "  STEP 1 / 4   Select config file")
        _draw_box(stdscr, by, bx, bh, bw, "Config files")
        _draw_list(stdscr, items, cursor, selected, by, bx, bh, bw, single=True)

        if not items:
            _safe_addstr(stdscr, by + 1, bx + 2,
                         "No config files found in config/ or ~/.hadoopscope/",
                         curses.color_pair(_C_CRIT))

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Select   ENTER Confirm   Q Quit")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord('q'), ord('Q'), 27):          # Q / ESC → quit
            return None
        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1
        elif key == curses.KEY_DOWN and cursor < len(items) - 1:
            cursor += 1
        elif key == ord(' ') and items:
            selected = {items[cursor][0]}            # radio: only one at a time
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            if selected:
                return list(selected)[0]             # return abs_path


def _step_envs(stdscr, env_names, config_label):
    """STEP 2 — Multi-select environments."""
    items = [(e, e) for e in env_names]
    selected = set()  # type: set
    cursor = 0

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bh = min(len(items) + 4, max_y - 8)
        bx, by = 2, 3

        _draw_header(stdscr,
                     "  STEP 2 / 4   Select environments  (config: {})".format(
                         os.path.basename(config_label)))
        _draw_box(stdscr, by, bx, bh, bw, "Environments")
        _draw_list(stdscr, items, cursor, selected, by, bx, bh, bw)

        info = "  {}/{} selected   A = toggle all".format(len(selected), len(items))
        _safe_addstr(stdscr, by + bh, bx, info, curses.color_pair(_C_DIM))

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Toggle   A All   ENTER Confirm   Q Back")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord('q'), ord('Q'), 27):
            return None
        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1
        elif key == curses.KEY_DOWN and cursor < len(items) - 1:
            cursor += 1
        elif key == ord(' '):
            k = items[cursor][0]
            if k in selected:
                selected.discard(k)
            else:
                selected.add(k)
        elif key in (ord('a'), ord('A')):
            if len(selected) == len(items):
                selected = set()
            else:
                selected = set(k for k, _ in items)
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            if selected:
                return list(selected)


def _step_checks(stdscr):
    """STEP 3 — Multi-select check categories."""
    items = list(CHECK_CATEGORIES)
    selected = {"all"}
    cursor = 0

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bh = len(items) + 4
        bx, by = 2, 3

        _draw_header(stdscr, "  STEP 3 / 4   Select check categories")
        _draw_box(stdscr, by, bx, bh, bw, "Check categories")
        _draw_list(stdscr, items, cursor, selected, by, bx, bh, bw)

        note = "  Tip: 'all' runs every category regardless of other selections"
        _safe_addstr(stdscr, by + bh, bx, note, curses.A_DIM)

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Toggle   A = 'all'   ENTER Confirm   Q Back")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord('q'), ord('Q'), 27):
            return None
        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1
        elif key == curses.KEY_DOWN and cursor < len(items) - 1:
            cursor += 1
        elif key == ord(' '):
            k = items[cursor][0]
            if k == "all":
                selected = {"all"}
            else:
                selected.discard("all")
                if k in selected:
                    selected.discard(k)
                else:
                    selected.add(k)
                if not selected:
                    selected = {"all"}
        elif key in (ord('a'), ord('A')):
            selected = {"all"}
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            if selected:
                return list(selected)


def _step_confirm(stdscr, config_path, envs, checks):
    """
    STEP 4 — Options + confirm.

    Returns (options_dict, run) or None to go back.
    """
    options = {"dry_run": False, "debug": False, "json_out": False}
    opt_items = [
        ("dry_run",  "Dry-run  (validate config, no actual checks)"),
        ("debug",    "Debug    (verbose stderr output)"),
        ("json_out", "JSON     (output format: json instead of text)"),
    ]
    cursor = 0

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bx = 2

        _draw_header(stdscr, "  STEP 4 / 4   Options & confirm")

        # ── Summary box ──
        by = 3
        bh = 6
        _draw_box(stdscr, by, bx, bh, bw, "Summary")
        _safe_addstr(stdscr, by + 1, bx + 2,
                     "Config   : {}".format(os.path.relpath(config_path)), curses.A_NORMAL)
        env_str = ", ".join(envs)
        _safe_addstr(stdscr, by + 2, bx + 2,
                     "Envs     : {}".format(env_str))
        chk_str = ", ".join(checks)
        _safe_addstr(stdscr, by + 3, bx + 2,
                     "Checks   : {}".format(chk_str))
        cmd_preview = " ".join(_build_cmd(config_path, envs, checks, options))
        _safe_addstr(stdscr, by + 4, bx + 2,
                     "Preview  : {}".format(cmd_preview),
                     curses.A_DIM)

        # ── Options box ──
        oy = by + bh + 1
        oh = len(opt_items) + 4
        _draw_box(stdscr, oy, bx, oh, bw, "Options")
        for i, (key, label) in enumerate(opt_items):
            is_cursor = (i == cursor)
            tick = "[x]" if options[key] else "[ ]"
            line = "{} {}".format(tick, label)
            attr = (curses.color_pair(_C_CURSOR) | curses.A_BOLD
                    if is_cursor else curses.A_NORMAL)
            _safe_addstr(stdscr, oy + 1 + i, bx + 2,
                         " {:<{w}} ".format(line, w=bw - 6), attr)

        # ── Run button ──
        ry = oy + oh + 1
        btn = "  [  ENTER  ]  Run checks   [D]  Dry-run shortcut   [Q]  Back  "
        _safe_addstr(stdscr, ry, bx,
                     "{:<{w}}".format(btn, w=bw),
                     curses.color_pair(_C_OK) | curses.A_BOLD)

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Toggle option   ENTER Run   Q Back")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord('q'), ord('Q'), 27):
            return None
        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1
        elif key == curses.KEY_DOWN and cursor < len(opt_items) - 1:
            cursor += 1
        elif key == ord(' '):
            k = opt_items[cursor][0]
            options[k] = not options[k]
        elif key in (ord('d'), ord('D')):
            options["dry_run"] = True
            return options
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            return options


# ── Command builder & runner ──────────────────────────────────────────────────

def _build_cmd(config_path, envs, checks, options):
    # type: (str, list, list, dict) -> list
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoopscope.py")
    cmd = [sys.executable, script, "--config", config_path]
    for e in envs:
        cmd += ["--env", e]
    for c in checks:
        cmd += ["--checks", c]
    if options.get("dry_run"):
        cmd.append("--dry-run")
    if options.get("debug"):
        cmd.append("--debug")
    cmd += ["--output", "json" if options.get("json_out") else "text"]
    return cmd


def _run_checks(stdscr, cmd):
    """
    Temporarily suspend curses, run hadoopscope, then resume.

    Returns (exit_code, quit_requested).
    """
    # Suspend curses and restore terminal
    curses.endwin()

    sep = "=" * 68
    print("\n" + sep)
    print("  HadoopScope TUI — Running checks")
    print("  Command: {}".format(" ".join(cmd)))
    print(sep + "\n")

    ret = subprocess.call(cmd)

    print("\n" + sep)
    status_label = {0: "OK", 1: "WARNING", 2: "CRITICAL"}.get(ret, str(ret))
    print("  Exit code: {}  ({})".format(ret, status_label))
    print(sep)
    print("\n  Press ENTER to run again  |  Q + ENTER to quit TUI\n")
    sys.stdout.flush()

    try:
        answer = sys.stdin.readline().strip().lower()
    except KeyboardInterrupt:
        answer = "q"

    # Resume curses mode (stdscr is still valid after endwin+refresh)
    stdscr.refresh()
    return ret, answer.startswith("q")


# ── Main TUI loop ─────────────────────────────────────────────────────────────

def _tui_main(stdscr):
    _init_colors()
    curses.curs_set(0)
    stdscr.keypad(True)

    # Discover config files once at startup
    config_files = find_config_files()
    if not config_files:
        curses.endwin()
        print("ERROR: No config files found.")
        print("Expected locations: config/*.yaml  or  ~/.hadoopscope/*.yaml")
        sys.exit(1)

    # Navigation state
    step = 1
    config_path = None
    envs = None
    checks = None

    while True:
        # ── Step 1: config file ──────────────────────────────────────────────
        if step == 1:
            result = _step_config(stdscr, config_files)
            if result is None:
                break                        # quit
            config_path = result
            step = 2

        # ── Step 2: environments ─────────────────────────────────────────────
        elif step == 2:
            env_names = load_env_names(config_path)
            if not env_names:
                # Show error, go back to step 1
                stdscr.erase()
                max_y, max_x = stdscr.getmaxyx()
                _safe_addstr(stdscr, max_y // 2, 4,
                             "No environments found in: {}".format(config_path),
                             curses.color_pair(_C_CRIT) | curses.A_BOLD)
                _safe_addstr(stdscr, max_y // 2 + 1, 4,
                             "Press any key to go back...", curses.A_DIM)
                stdscr.refresh()
                stdscr.getch()
                step = 1
                continue

            result = _step_envs(stdscr, env_names, config_path)
            if result is None:
                step = 1                     # back
                continue
            envs = result
            step = 3

        # ── Step 3: check categories ─────────────────────────────────────────
        elif step == 3:
            result = _step_checks(stdscr)
            if result is None:
                step = 2                     # back
                continue
            checks = result
            step = 4

        # ── Step 4: options + run ────────────────────────────────────────────
        elif step == 4:
            result = _step_confirm(stdscr, config_path, envs, checks)
            if result is None:
                step = 3                     # back
                continue

            cmd = _build_cmd(config_path, envs, checks, result)
            _ret, quit_after = _run_checks(stdscr, cmd)

            if quit_after:
                break
            step = 1                         # loop: start a new run


def main():
    """Entry point — wraps curses safely."""
    try:
        curses.wrapper(_tui_main)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
