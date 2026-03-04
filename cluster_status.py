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
import time

# Aggiungiamo la directory del progetto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Check categories ──────────────────────────────────────────────────────────

CHECK_CATEGORIES = [
    ("all",    "All checks (health + hdfs + hive + yarn)"),
    ("health", "Service health (Ambari / Cloudera Manager)"),
    ("hdfs",   "HDFS: space, DataNodes, writability"),
    ("hive",   "Hive: HS2 connectivity + partition counts"),
    ("yarn",   "YARN: node health + queue usage"),
]

CRON_PRESETS = [
    ("*/5 * * * *",  "Every  5 minutes"),
    ("*/15 * * * *", "Every 15 minutes"),
    ("*/30 * * * *", "Every 30 minutes"),
    ("0 * * * *",    "Every 1 hour"),
    ("0 */4 * * *",  "Every 4 hours"),
    ("daily",        "Daily at HH:MM..."),
    ("weekdays",     "Weekdays  (Mon-Fri) at HH:MM..."),
    ("custom",       "Custom cron expression..."),
]

# ── Crontab manager helpers ───────────────────────────────────────────────────

_HS_MARKER = "# hs:"   # prefisso per le righe marker HadoopScope nel crontab


def _cron_label(cron_expr):
    # type: (str) -> str
    """Converte espressione cron comune in label human-readable."""
    _fixed = {
        "*/5 * * * *":  "every 5min",
        "*/15 * * * *": "every 15min",
        "*/30 * * * *": "every 30min",
        "0 * * * *":    "every 1h",
        "0 */4 * * *":  "every 4h",
        "0 */6 * * *":  "every 6h",
        "0 */12 * * *": "every 12h",
        "@daily":       "daily 00:00",
        "@hourly":      "every 1h",
    }
    if cron_expr in _fixed:
        return _fixed[cron_expr]
    parts = cron_expr.split()
    if len(parts) == 5:
        try:
            h = int(parts[1])
            m = int(parts[0])
            t = "{:02d}:{:02d}".format(h, m)
            if parts[2] == "*" and parts[3] == "*":
                if parts[4] == "*":
                    return "daily {}".format(t)
                elif parts[4] == "1-5":
                    return "weekdays {}".format(t)
        except ValueError:
            pass
    return cron_expr


def _default_log_path(entry):
    # type: (dict) -> str
    envs = entry.get("envs") or ["hadoopscope"]
    tag  = envs[0].replace("/", "-").replace(" ", "_")
    return "/tmp/hadoopscope-{}.log".format(tag)


def _crontab_read():
    # type: () -> tuple
    """Legge il crontab utente. Ritorna (other_lines, hs_blocks) oppure (None, []).

    other_lines: list di righe non-HadoopScope
    hs_blocks:   list di dict {marker, cmd_line, enabled}
    Ritorna (None, []) se il comando crontab non è disponibile.
    """
    try:
        out   = subprocess.check_output(["crontab", "-l"],
                                        stderr=subprocess.DEVNULL)
        lines = out.decode("utf-8", errors="replace").splitlines()
    except subprocess.CalledProcessError:
        lines = []   # nessun crontab: ok
    except OSError:
        return None, []   # crontab non disponibile

    other_lines = []   # type: list
    hs_blocks   = []   # type: list
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith(_HS_MARKER):
            marker   = line
            i       += 1
            cmd_line = lines[i] if i < len(lines) else ""
            enabled  = not cmd_line.startswith("# ")
            hs_blocks.append({"marker": marker, "cmd_line": cmd_line,
                               "enabled": enabled})
        else:
            other_lines.append(line)
        i += 1
    return other_lines, hs_blocks


def _crontab_write(other_lines, hs_blocks):
    # type: (list, list) -> tuple
    """Scrive il crontab via `crontab -`. Ritorna (ok, err_msg)."""
    lines = list(other_lines)
    for block in hs_blocks:
        lines.append(block["marker"])
        lines.append(block["cmd_line"])
    # Rimuove trailing blank lines duplicate ma mantiene una riga vuota finale
    content = "\n".join(lines).rstrip() + "\n"
    try:
        proc = subprocess.Popen(["crontab", "-"],
                                stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        _, err = proc.communicate(content.encode("utf-8"))
        return proc.returncode == 0, err.decode("utf-8", errors="replace").strip()
    except OSError as e:
        return False, str(e)


def _parse_hs_block(block):
    # type: (dict) -> dict
    """Parsa un blocco crontab HadoopScope in un dict entry."""
    marker  = block["marker"]
    cmd_line = block["cmd_line"]
    enabled  = block["enabled"]
    entry    = {"marker_raw": marker, "cmd_line": cmd_line, "enabled": enabled,
                "config": "", "envs": [], "checks": "all",
                "cron": "", "log_file": ""}

    meta = marker[len(_HS_MARKER):].strip()
    for part in meta.split():
        if "=" in part:
            k, v = part.split("=", 1)
            if k == "config":
                entry["config"] = v
            elif k == "envs":
                entry["envs"] = [e for e in v.split(",") if e]
            elif k == "checks":
                entry["checks"] = v

    # Estrae cron expression dalla riga comando (prima 5 colonne)
    actual = cmd_line.lstrip("# ").strip()
    parts  = actual.split(None, 6)
    if len(parts) >= 5:
        entry["cron"] = " ".join(parts[:5])
    # Log file dopo >>
    if ">>" in cmd_line:
        log_part = cmd_line.split(">>", 1)[1].strip().split()[0]
        entry["log_file"] = log_part
    return entry


def _format_hs_block(entry):
    # type: (dict) -> tuple
    """Formatta entry come (marker_line, cmd_line) per il crontab."""
    envs_str = ",".join(entry.get("envs") or [])
    marker   = "{} config={} envs={} checks={}".format(
        _HS_MARKER,
        entry.get("config", ""),
        envs_str,
        entry.get("checks", "all"),
    )

    python  = sys.executable
    script  = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "hadoopscope.py")
    env_args = " ".join("--env {}".format(e) for e in (entry.get("envs") or []))
    log_file = entry.get("log_file") or _default_log_path(entry)

    cmd = "{python} {script} --config {config} {env_args} --checks {checks} --output text >> {log} 2>&1".format(
        python=python, script=script,
        config=entry.get("config", ""),
        env_args=env_args,
        checks=entry.get("checks", "all"),
        log=log_file,
    )
    full_cmd = "{} {}".format(entry.get("cron", ""), cmd)

    if entry.get("enabled", True):
        cmd_line = full_cmd
    else:
        cmd_line = "# " + full_cmd
    return marker, cmd_line


# ── Config discovery ─────────────────────────────────────────────────────────

_SEARCH_PATTERNS = [
    "config/*.yaml",
    "config/*.yml",
    os.path.expanduser("~/.hadoopscope/*.yaml"),
    os.path.expanduser("~/.hadoopscope/*.yml"),
    "hadoopscope.yaml",
    "hadoopscope.yml",
]

# Nomi (prefissi) da escludere dalla TUI: file di esempio/template/test
_IGNORE_PREFIXES = ("example", "docker-", "docker_", "test.")


def find_config_files():
    # type: () -> list
    """Return list of (display_label, abs_path) for discovered YAML configs.

    Files whose basename starts with a prefix in _IGNORE_PREFIXES are skipped
    (examples, docker test fixtures, etc.).
    """
    found = []
    seen = set()  # type: set
    for pattern in _SEARCH_PATTERNS:
        for path in sorted(_glob.glob(pattern)):
            fname = os.path.basename(path)
            if any(fname.startswith(p) for p in _IGNORE_PREFIXES):
                continue
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
                                                     "cm_url", "hdfs", "webhdfs", "kerberos",
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

        _draw_header(stdscr, "  STEP 1 / 5   Select config file")
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
                     "  STEP 2 / 5   Select environments  (config: {})".format(
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

        _draw_header(stdscr, "  STEP 3 / 5   Select check categories")
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
    STEP 4 / 5 — Options.

    Returns options_dict or None to go back.
    Email default is OFF for manual runs; scheduled mode will force it on.
    """
    options = {"dry_run": False, "debug": False, "send_email": False}
    opt_items = [
        ("dry_run",    "Dry-run     (validate config, no actual checks)"),
        ("debug",      "Debug       (verbose stderr output)"),
        ("send_email", "Send email  (dispatch email alert — default OFF for manual runs)"),
    ]
    cursor = 0

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bx = 2

        _draw_header(stdscr, "  STEP 4 / 5   Options")

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
        # Mostra il comando abbreviato (senza path assoluto di python/script)
        short_cmd = _short_cmd(config_path, envs, checks, options)
        prefix = "Command  : "
        avail = bw - len(prefix) - 4
        if len(short_cmd) > avail:
            short_cmd = short_cmd[:avail - 3] + "..."
        _safe_addstr(stdscr, by + 4, bx + 2,
                     "{}{}".format(prefix, short_cmd),
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

        # ── Next button ──
        ry = oy + oh + 1
        btn = "  [  ENTER  ]  Next: Schedule   [D]  Dry-run shortcut   [Q]  Back  "
        _safe_addstr(stdscr, ry, bx,
                     "{:<{w}}".format(btn, w=bw),
                     curses.color_pair(_C_OK) | curses.A_BOLD)

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Toggle option   ENTER Next   Q Back")
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

def _build_cmd(config_path, envs, checks, options, force_email=False):
    # type: (str, list, list, dict, bool) -> list
    """Build the hadoopscope command list.

    force_email=True overrides send_email option (used by scheduled mode).
    """
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
    cmd += ["--output", "text"]
    if not (force_email or options.get("send_email", False)):
        cmd.append("--no-email")
    return cmd


def _short_cmd(config_path, envs, checks, options, force_email=False):
    # type: (str, list, list, dict, bool) -> str
    """Versione leggibile del comando: solo hadoopscope.py + argomenti."""
    parts = ["hadoopscope.py", "--config", os.path.relpath(config_path)]
    for e in envs:
        parts += ["--env", e]
    for c in checks:
        parts += ["--checks", c]
    if options.get("dry_run"):
        parts.append("--dry-run")
    if options.get("debug"):
        parts.append("--debug")
    if not (force_email or options.get("send_email", False)):
        parts.append("--no-email")
    return " ".join(parts)


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


# ── Crontab TUI dialogs ───────────────────────────────────────────────────────

def _ask_time(stdscr, title="Scheduled time"):
    # type: (object, str) -> object
    """Overlay: chiede un orario HH:MM. Restituisce (hour, minute) o None se annullato.

    Accetta 4 cifre digitate (HHMM): es. 0600 → 06:00.
    Backspace cancella l'ultima cifra. ENTER conferma, ESC annulla.
    """
    max_y, max_x = stdscr.getmaxyx()
    bw = 46
    bh = 7
    bx = max(0, (max_x - bw) // 2)
    by = max(0, (max_y - bh) // 2)

    _draw_box(stdscr, by, bx, bh, bw, title)
    _safe_addstr(stdscr, by + 1, bx + 2, "Enter 4 digits — HHMM  (e.g. 0600 = 06:00):")
    _safe_addstr(stdscr, by + 5, bx + 2, "ENTER Confirm   ESC Cancel", curses.A_DIM)
    curses.curs_set(1)
    buf     = []   # type: list
    err_msg = ""

    while True:
        digits  = "".join(buf)
        padded  = (digits + "____")[:4]
        display = "{}{}:{}{} ".format(padded[0], padded[1], padded[2], padded[3])
        _safe_addstr(stdscr, by + 3, bx + 2,
                     "> {}    ".format(display),
                     curses.color_pair(_C_SEL) | curses.A_BOLD)
        if err_msg:
            _safe_addstr(stdscr, by + 4, bx + 2, "{:<40}".format(err_msg), curses.A_BOLD)
        try:
            cursor_off = len(buf) + (1 if len(buf) >= 2 else 0)
            stdscr.move(by + 3, bx + 4 + cursor_off)
        except curses.error:
            pass
        stdscr.refresh()

        key = stdscr.getch()
        if key == 27:
            curses.curs_set(0)
            return None
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            if len(buf) == 4:
                h = int(digits[0:2])
                m = int(digits[2:4])
                if 0 <= h <= 23 and 0 <= m <= 59:
                    curses.curs_set(0)
                    return (h, m)
                err_msg = "Invalid! Hours 00-23, minutes 00-59"
            else:
                err_msg = "Enter all 4 digits  (e.g. 0600)"
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if buf:
                buf.pop()
                err_msg = ""
        elif ord('0') <= key <= ord('9') and len(buf) < 4:
            buf.append(chr(key))
            err_msg = ""


def _ask_text(stdscr, title, prompt, default="", max_len=80):
    # type: (object, str, str, str, int) -> object
    """Overlay: input testo generico (path log, espressione cron custom).
    Restituisce la stringa inserita oppure None se ESC.
    """
    max_y, max_x = stdscr.getmaxyx()
    bw = min(max_x - 4, 76)
    bh = 6
    bx = max(0, (max_x - bw) // 2)
    by = max(0, (max_y - bh) // 2)

    _draw_box(stdscr, by, bx, bh, bw, title)
    _safe_addstr(stdscr, by + 1, bx + 2, prompt[:bw - 4])
    _safe_addstr(stdscr, by + 4, bx + 2, "ENTER Confirm   ESC Cancel", curses.A_DIM)
    curses.curs_set(1)
    buf = list(default)   # type: list

    while True:
        inp      = "".join(buf)
        disp_w   = bw - 6
        display  = inp[-disp_w:] if len(inp) > disp_w else inp
        _safe_addstr(stdscr, by + 2, bx + 2,
                     "> {:<{w}}".format(display, w=disp_w),
                     curses.color_pair(_C_SEL) | curses.A_BOLD)
        try:
            stdscr.move(by + 2, bx + 4 + min(len(display), disp_w))
        except curses.error:
            pass
        stdscr.refresh()

        key = stdscr.getch()
        if key == 27:
            curses.curs_set(0)
            return None
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            curses.curs_set(0)
            return "".join(buf)
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if buf:
                buf.pop()
        elif 32 <= key <= 126 and len(buf) < max_len:
            buf.append(chr(key))


def _confirm_dialog(stdscr, question):
    # type: (object, str) -> bool
    """Dialogo Sì/No. Ritorna True se confermato (Y/ENTER), False altrimenti."""
    max_y, max_x = stdscr.getmaxyx()
    bw = min(max_x - 4, 60)
    bh = 5
    bx = max(0, (max_x - bw) // 2)
    by = max(0, (max_y - bh) // 2)

    _draw_box(stdscr, by, bx, bh, bw, "Confirm")
    _safe_addstr(stdscr, by + 1, bx + 2, question[:bw - 4])
    _safe_addstr(stdscr, by + 3, bx + 2, "Y / ENTER = Yes    N / ESC = No",
                 curses.A_DIM)
    stdscr.refresh()

    while True:
        key = stdscr.getch()
        if key in (ord('y'), ord('Y'), curses.KEY_ENTER, ord('\n'), ord('\r')):
            return True
        if key in (ord('n'), ord('N'), 27):
            return False


def _show_msg(stdscr, msg, attr=None):
    # type: (object, str, object) -> None
    """Mostra un messaggio temporaneo centrato. L'utente preme un tasto per chiudere."""
    if attr is None:
        attr = curses.A_BOLD
    max_y, max_x = stdscr.getmaxyx()
    bw = min(max_x - 4, len(msg) + 6)
    bh = 3
    bx = max(0, (max_x - bw) // 2)
    by = max(0, (max_y - bh) // 2)

    _draw_box(stdscr, by, bx, bh, bw, "")
    _safe_addstr(stdscr, by + 1, bx + 2, msg[:bw - 4], attr)
    stdscr.refresh()
    stdscr.getch()


def _dialog_add_edit_schedule(stdscr, entry):
    # type: (object, dict) -> object
    """Dialogo add/edit schedule. Restituisce entry aggiornata o None se annullato.

    entry contiene almeno: config, envs (list), checks, cron (può essere ""),
    log_file (può essere ""), enabled.
    """
    items  = list(CRON_PRESETS)
    cursor = 0

    # Preseleziona in base al cron esistente
    cur_cron = entry.get("cron", "")
    for i, (k, _) in enumerate(items):
        if k == cur_cron:
            cursor = i
            break

    selected = {items[cursor][0]}

    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 74)
        bh = len(items) + 4
        bx, by = 2, 3

        is_edit = bool(cur_cron)
        _draw_header(stdscr, "  STEP 5 / 5   {}".format(
            "Edit schedule" if is_edit else "Add schedule"))

        env_str  = ", ".join(entry.get("envs") or [])
        info_str = "  env: {}   checks: {}   config: {}".format(
            env_str, entry.get("checks", "?"),
            os.path.basename(entry.get("config", "?")))
        _safe_addstr(stdscr, 2, 0, info_str[:max_x - 1], curses.A_DIM)

        _draw_box(stdscr, by, bx, bh, bw, "Choose schedule frequency")
        _draw_list(stdscr, items, cursor, selected, by, bx, bh, bw, single=True)

        _draw_footer(stdscr,
                     " UP/DOWN Navigate   SPACE Select   ENTER Confirm   ESC Cancel")
        stdscr.refresh()

        key = stdscr.getch()
        if key == 27:
            return None
        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1
        elif key == curses.KEY_DOWN and cursor < len(items) - 1:
            cursor += 1
        elif key == ord(' '):
            selected = {items[cursor][0]}
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            sel_key = list(selected)[0]
            cron_expr = ""

            if sel_key in ("*/5 * * * *", "*/15 * * * *", "*/30 * * * *",
                           "0 * * * *", "0 */4 * * *"):
                cron_expr = sel_key

            elif sel_key == "daily":
                hm = _ask_time(stdscr, "Daily — enter time")
                if hm is None:
                    continue
                cron_expr = "{} {} * * *".format(hm[1], hm[0])

            elif sel_key == "weekdays":
                hm = _ask_time(stdscr, "Weekdays (Mon-Fri) — enter time")
                if hm is None:
                    continue
                cron_expr = "{} {} * * 1-5".format(hm[1], hm[0])

            elif sel_key == "custom":
                default_cron = cur_cron if cur_cron else "0 6 * * *"
                result = _ask_text(stdscr, "Custom cron expression",
                                   "Enter 5-field cron expression:", default_cron)
                if result is None:
                    continue
                cron_expr = result.strip()

            if not cron_expr:
                continue

            # Chiedi log file
            default_log = entry.get("log_file") or _default_log_path(entry)
            log_file = _ask_text(stdscr, "Log file",
                                 "Output log file (stdout + stderr):", default_log)
            if log_file is None:
                continue

            new_entry = dict(entry)
            new_entry["cron"]     = cron_expr
            new_entry["log_file"] = log_file or _default_log_path(entry)
            new_entry["enabled"]  = entry.get("enabled", True)
            return new_entry


def _step_crontab_manager(stdscr, config_path, envs, checks, options):
    # type: (object, str, list, list, dict) -> object
    """STEP 5 / 5 — Gestione schedule crontab HadoopScope.

    Mostra le voci HadoopScope presenti nel crontab utente.
    Operazioni: R Run once, A Add, ENTER Edit, T Toggle, D Delete, Q Back.

    Ritorna:
        None         → torna allo step 4
        "run_once"   → esegui i check una volta adesso
    """
    cursor  = 0
    msg     = ""   # messaggio di stato (es. "Saved", errore crontab)

    while True:
        other_lines, hs_blocks = _crontab_read()
        crontab_ok = other_lines is not None

        if not crontab_ok:
            hs_blocks = []

        entries = [_parse_hs_block(b) for b in hs_blocks]
        if cursor >= len(entries):
            cursor = max(0, len(entries) - 1)

        # ── Render ───────────────────────────────────────────────────────────
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        bw = min(max_x - 4, 82)
        bx = 2

        env_str  = ", ".join(envs)  if envs   else "—"
        chk_str  = ", ".join(checks) if checks else "all"
        cfg_str  = os.path.basename(config_path or "?")

        _draw_header(stdscr, "  STEP 5 / 5   Scheduled Tasks")
        _safe_addstr(stdscr, 2, bx,
                     "Selection: env={}  checks={}  config={}".format(
                         env_str, chk_str, cfg_str),
                     curses.A_DIM)

        # Avviso se crontab non disponibile
        if not crontab_ok:
            _safe_addstr(stdscr, 4, bx,
                         "  WARNING: 'crontab' command not available on this system.",
                         curses.color_pair(_C_WARN) | curses.A_BOLD)
        else:
            by  = 4
            bh  = max(len(entries), 1) + 4

            _draw_box(stdscr, by, bx, bh, bw,
                      "HadoopScope cron entries  ({} found)".format(len(entries)))

            if not entries:
                _safe_addstr(stdscr, by + 1, bx + 3,
                             "No scheduled tasks. Press A to add one.",
                             curses.A_DIM)
            else:
                # Header colonne
                _safe_addstr(stdscr, by + 1, bx + 3,
                             "{:<3} {:<18} {:<8} {:<17} {}".format(
                                 "", "ENV(s)", "CHECKS", "CRON", "SCHEDULE"),
                             curses.A_DIM)
                for i, e in enumerate(entries):
                    y        = by + 2 + i
                    enabled  = e.get("enabled", True)
                    marker   = "►" if i == cursor else " "
                    tick     = "[✓]" if enabled else "[✗]"
                    env_col  = ",".join(e.get("envs") or ["?"])[:17]
                    chk_col  = (e.get("checks") or "all")[:7]
                    cron_col = (e.get("cron") or "?")[:16]
                    lbl_col  = _cron_label(e.get("cron") or "")[:18]
                    line     = "{} {} {:<18} {:<8} {:<17} {}".format(
                        marker, tick, env_col, chk_col, cron_col, lbl_col)
                    if not enabled:
                        line += "  [OFF]"
                    attr = (curses.A_REVERSE if i == cursor
                            else (curses.A_DIM if not enabled else curses.A_NORMAL))
                    _safe_addstr(stdscr, y, bx + 1, line[:bw - 2], attr)

            if msg:
                _safe_addstr(stdscr, by + bh + 1, bx + 2, msg,
                             curses.color_pair(_C_OK) | curses.A_BOLD)

        _draw_footer(stdscr,
                     " R Run once   A Add   ENTER Edit   T Toggle   D Delete   Q Back")
        stdscr.refresh()
        msg = ""

        # ── Key handling ─────────────────────────────────────────────────────
        key = stdscr.getch()

        if key in (ord('q'), ord('Q'), 27):
            return None

        elif key in (ord('r'), ord('R')):
            return "run_once"

        elif key == curses.KEY_UP and cursor > 0:
            cursor -= 1

        elif key == curses.KEY_DOWN and cursor < len(entries) - 1:
            cursor += 1

        elif key in (ord('a'), ord('A')) and crontab_ok:
            checks_str = ",".join(checks) if isinstance(checks, list) else (checks or "all")
            new_entry = {
                "config":   config_path or "",
                "envs":     list(envs) if envs else [],
                "checks":   checks_str,
                "cron":     "",
                "log_file": "",
                "enabled":  True,
            }
            result = _dialog_add_edit_schedule(stdscr, new_entry)
            if result is not None:
                marker_line, cmd_line = _format_hs_block(result)
                hs_blocks.append({"marker": marker_line, "cmd_line": cmd_line,
                                  "enabled": result["enabled"]})
                ok, err = _crontab_write(other_lines, hs_blocks)
                msg = "Schedule added." if ok else "crontab error: {}".format(err[:50])
                cursor = len(hs_blocks) - 1

        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')) and entries and crontab_ok:
            entry  = entries[cursor]
            result = _dialog_add_edit_schedule(stdscr, entry)
            if result is not None:
                marker_line, cmd_line = _format_hs_block(result)
                hs_blocks[cursor] = {"marker": marker_line, "cmd_line": cmd_line,
                                     "enabled": result["enabled"]}
                ok, err = _crontab_write(other_lines, hs_blocks)
                msg = "Schedule updated." if ok else "crontab error: {}".format(err[:50])

        elif key in (ord('t'), ord('T')) and entries and crontab_ok:
            block   = hs_blocks[cursor]
            enabled = block.get("enabled", True)
            if enabled:
                block["cmd_line"] = "# " + block["cmd_line"]
                block["enabled"]  = False
            else:
                block["cmd_line"] = block["cmd_line"].lstrip("# ")
                block["enabled"]  = True
            ok, err = _crontab_write(other_lines, hs_blocks)
            msg = ("Disabled." if enabled else "Enabled.") if ok \
                  else "crontab error: {}".format(err[:50])

        elif key in (ord('d'), ord('D'), curses.KEY_DC) and entries and crontab_ok:
            if _confirm_dialog(stdscr, "Delete this scheduled task?"):
                hs_blocks.pop(cursor)
                cursor = max(0, min(cursor, len(hs_blocks) - 1))
                ok, err = _crontab_write(other_lines, hs_blocks)
                msg = "Deleted." if ok else "crontab error: {}".format(err[:50])


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
    options = None   # type: dict

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

        # ── Step 4: options ───────────────────────────────────────────────────
        elif step == 4:
            result = _step_confirm(stdscr, config_path, envs, checks)
            if result is None:
                step = 3                     # back
                continue
            options = result
            step = 5

        # ── Step 5: crontab manager ──────────────────────────────────────────
        elif step == 5:
            result = _step_crontab_manager(stdscr, config_path, envs,
                                           checks, options)
            if result is None:
                step = 4                     # back
                continue
            if result == "run_once":
                cmd = _build_cmd(config_path, envs, checks, options,
                                 force_email=False)
                _ret, quit_after = _run_checks(stdscr, cmd)
                if quit_after:
                    break
            step = 1                         # return to start


def main():
    """Entry point — wraps curses safely."""
    try:
        curses.wrapper(_tui_main)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
