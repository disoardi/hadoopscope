"""Primitive di disegno curses per la TUI — palette LCARS, box, liste,
prompt testuali. Scritte da zero (non riusano cluster_status.py, decisione
esplicita in brainstorming): la sola logica non-visiva riusata è il
parsing/scrittura crontab, che vive in cluster_status.py e viene chiamata
da tui/screens/monitoring.py, non duplicata qui.
"""

from __future__ import print_function

import curses

# Indici color_pair — inizializzati da init_colors()
C_TAB_ACTIVE   = 1
C_TAB_HOME     = 2
C_TAB_MON      = 3
C_TAB_OPS      = 4
C_OK           = 5
C_WARN         = 6
C_CRIT         = 7
C_BORDER       = 8
C_DIM          = 9


def init_colors():
    # type: () -> None
    """Inizializza le coppie di colore. Se il terminale supporta almeno
    256 colori, usa RGB custom vicini alla palette LCARS (arancione,
    viola, rosa); altrimenti fallback sugli 8 colori base di curses
    (COLOR_YELLOW/MAGENTA/CYAN) — mai assumere il supporto, va rilevato.
    """
    curses.start_color()
    curses.use_default_colors()

    if curses.COLORS >= 256 and curses.can_change_color():
        # Slot di colore custom (indici alti per non toccare la palette base)
        curses.init_color(16, 910, 545, 47)    # arancione LCARS ~#e8890c
        curses.init_color(17, 608, 420, 788)    # viola LCARS ~#9b6bc9
        curses.init_color(18, 788, 420, 608)    # rosa LCARS ~#c96b9b
        orange, purple, pink = 16, 17, 18
    else:
        orange, purple, pink = curses.COLOR_YELLOW, curses.COLOR_MAGENTA, curses.COLOR_MAGENTA

    curses.init_pair(C_TAB_ACTIVE, curses.COLOR_BLACK, orange)
    curses.init_pair(C_TAB_HOME,   curses.COLOR_BLACK, orange)
    curses.init_pair(C_TAB_MON,    curses.COLOR_BLACK, purple)
    curses.init_pair(C_TAB_OPS,    curses.COLOR_BLACK, pink)
    curses.init_pair(C_OK,         curses.COLOR_GREEN,  -1)
    curses.init_pair(C_WARN,       curses.COLOR_YELLOW, -1)
    curses.init_pair(C_CRIT,       curses.COLOR_RED,    -1)
    curses.init_pair(C_BORDER,     curses.COLOR_CYAN,   -1)
    curses.init_pair(C_DIM,        curses.COLOR_WHITE,  -1)


def safe_addstr(win, y, x, text, attr=0):
    # type: (object, int, int, str, int) -> None
    """addstr che non crolla se il testo esce dai bordi dello schermo
    (curses solleva error in quel caso — capitava già in cluster_status.py,
    stesso pattern riusato)."""
    try:
        max_y, max_x = win.getmaxyx()
        if y < 0 or y >= max_y or x < 0 or x >= max_x:
            return
        win.addstr(y, x, text[:max_x - x - 1], attr)
    except curses.error:
        pass


def draw_box(win, y, x, h, w, color_pair=C_BORDER, double=True):
    # type: (object, int, int, int, int, int, bool) -> None
    """Bordo a caratteri Unicode box-drawing. double=True usa la doppia
    linea (╔═╗║╚╝), più 'pannello LCARS' dei singoli (┌─┐│└┘)."""
    attr = curses.color_pair(color_pair)
    tl, tr, bl, br, hz, vt = ("╔", "╗", "╚", "╝", "═", "║") if double else ("┌", "┐", "└", "┘", "─", "│")
    safe_addstr(win, y, x, tl + hz * (w - 2) + tr, attr)
    for i in range(1, h - 1):
        safe_addstr(win, y + i, x, vt, attr)
        safe_addstr(win, y + i, x + w - 1, vt, attr)
    safe_addstr(win, y + h - 1, x, bl + hz * (w - 2) + br, attr)


def draw_sidebar(win, tabs, active_index):
    # type: (object, list, int) -> None
    """Sidebar persistente a sinistra — un blocco pieno per tab, colore
    diverso per sezione, il tab attivo è sempre in arancione."""
    pairs = [C_TAB_HOME, C_TAB_MON, C_TAB_OPS]
    for i, label in enumerate(tabs):
        pair = C_TAB_ACTIVE if i == active_index else pairs[i % len(pairs)]
        marker = "▶ " if i == active_index else "  "
        text = "{}{}".format(marker, label).ljust(16)
        safe_addstr(win, i * 2 + 1, 0, text, curses.color_pair(pair) | curses.A_BOLD)


def draw_list(win, items, cursor, y, x, h, w, selected=None):
    # type: (object, list, int, int, int, int, int, object) -> None
    """Lista navigabile. items: lista di stringhe. selected: set opzionale
    di indici selezionati (multi-select, mostrato con '[x]' davanti)."""
    visible_h = h
    start = max(0, cursor - visible_h + 1) if cursor >= visible_h else 0
    for row, idx in enumerate(range(start, min(len(items), start + visible_h))):
        prefix = ""
        if selected is not None:
            prefix = "[x] " if idx in selected else "[ ] "
        attr = curses.A_REVERSE if idx == cursor else 0
        safe_addstr(win, y + row, x, (prefix + items[idx])[:w], attr)


def ask_text(stdscr, prompt, default=""):
    # type: (object, str, str) -> object
    """Prompt testuale a riga singola. ESC annulla (ritorna None)."""
    curses.echo()
    curses.curs_set(1)
    max_y, max_x = stdscr.getmaxyx()
    y = max_y - 2
    safe_addstr(stdscr, y, 2, " " * (max_x - 4))
    safe_addstr(stdscr, y, 2, "{}: {}".format(prompt, default))
    stdscr.refresh()
    try:
        raw = stdscr.getstr(y, 2 + len(prompt) + 2, max_x - len(prompt) - 8)
        text = raw.decode("utf-8").strip()
    except Exception:
        text = ""
    finally:
        curses.noecho()
        curses.curs_set(0)
    return text if text else (default or None)


def confirm(stdscr, question):
    # type: (object, str) -> bool
    """Dialogo si'/no. Invio o 's'/'S' -> True, qualunque altro tasto -> False."""
    max_y, max_x = stdscr.getmaxyx()
    y = max_y - 2
    safe_addstr(stdscr, y, 2, " " * (max_x - 4))
    safe_addstr(stdscr, y, 2, "{} [s/N]".format(question), curses.A_BOLD)
    stdscr.refresh()
    key = stdscr.getch()
    return key in (ord("s"), ord("S"), curses.KEY_ENTER, 10, 13)
