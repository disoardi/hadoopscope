# HadoopScope TUI Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sostituire `cluster_status.py` con una TUI a sidebar persistente (Home/Monitoring/Ops) in stile LCARS, con la Home che legge lo stato dei check da una nuova persistenza sqlite invece di ricalcolarlo ogni volta.

**Architecture:** Nuovo modulo `state_store.py` (sqlite stdlib, schema `check_state`) agganciato al loop esistente in `hadoopscope.py::main()`. Nuovo pacchetto `tui/` con widget curses scritti da zero, una classe `Screen` base (enter/render/handle_input), e tre sezioni (Home, Monitoring, Ops) navigabili con uno stack di schermate per sezione dentro una sidebar sempre visibile. La logica di parsing/scrittura crontab esistente in `cluster_status.py` viene riusata as-is (non è codice visivo).

**Tech Stack:** Python 3.6+ stdlib-only (`sqlite3`, `curses`), nessuna nuova dipendenza.

**Riferimento spec:** `docs/superpowers/specs/2026-08-20-hadoopscope-tui-shell-design.md`

---

## Task 1: `state_store.py` — schema e funzioni base

**Files:**
- Create: `state_store.py`
- Test: `tests/test_state_store.py`

- [ ] **Step 1: Scrivere il test per `save_result`/`get_env_summary`/`get_all_envs_summary`**

Creare `tests/test_state_store.py`:

```python
"""Test suite per state_store.py — persistenza sqlite dello stato dei check."""

from __future__ import print_function

import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import state_store
from checks.base import CheckResult


def _make_result(name, status, message, details=None):
    return CheckResult(name, status, message, details or {})


def _reset():
    state_store._DB_PATH = None


def test_save_and_get_env_summary():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("AmbariServiceHealth", CheckResult.OK, "all good"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.WARNING, "1 unhealthy"))
        rows = state_store.get_env_summary("prod-hdp")
        names = sorted(r["check_name"] for r in rows)
        assert names == ["AmbariServiceHealth", "YarnNodeHealth"]
        yarn_row = next(r for r in rows if r["check_name"] == "YarnNodeHealth")
        assert yarn_row["status"] == CheckResult.WARNING
        assert yarn_row["message"] == "1 unhealthy"
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_save_result_upserts_same_env_check():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.CRITICAL, "2 lost"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.OK, "all running"))
        rows = state_store.get_env_summary("prod-hdp")
        assert len(rows) == 1
        assert rows[0]["status"] == CheckResult.OK
        assert rows[0]["message"] == "all running"
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_env_summary_empty_env_returns_empty_list():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        rows = state_store.get_env_summary("never-run")
        assert rows == []
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_all_envs_summary_aggregates_worst_status_and_counts():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("A", CheckResult.OK, "ok"))
        state_store.save_result("prod-hdp", _make_result("B", CheckResult.WARNING, "warn"))
        state_store.save_result("prod-cdp", _make_result("C", CheckResult.CRITICAL, "crit"))
        summary = {row["env"]: row for row in state_store.get_all_envs_summary()}
        assert summary["prod-hdp"]["worst_status"] == CheckResult.WARNING
        assert summary["prod-hdp"]["counts"] == {"OK": 1, "WARNING": 1}
        assert summary["prod-cdp"]["worst_status"] == CheckResult.CRITICAL
        assert summary["prod-cdp"]["counts"] == {"CRITICAL": 1}
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_all_envs_summary_empty_db_returns_empty_list():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        assert state_store.get_all_envs_summary() == []
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_details_roundtrip_as_dict():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result(
            "YarnQueues", CheckResult.WARNING, "queue over 80%",
            details={"queue": "default", "usedCapacity": 87.5}))
        rows = state_store.get_env_summary("prod-hdp")
        assert rows[0]["details"] == {"queue": "default", "usedCapacity": 87.5}
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_init_creates_parent_dir_if_missing():
    tmpdir = tempfile.mkdtemp()
    try:
        db_path = os.path.join(tmpdir, "nested", "dir", "state.db")
        state_store.init(db_path)
        assert os.path.exists(db_path)
    finally:
        shutil.rmtree(tmpdir)
        _reset()


if __name__ == "__main__":
    tests = [
        test_save_and_get_env_summary,
        test_save_result_upserts_same_env_check,
        test_get_env_summary_empty_env_returns_empty_list,
        test_get_all_envs_summary_aggregates_worst_status_and_counts,
        test_get_all_envs_summary_empty_db_returns_empty_list,
        test_details_roundtrip_as_dict,
        test_init_creates_parent_dir_if_missing,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} — {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
```

- [ ] **Step 2: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_state_store.py`
Expected: `ModuleNotFoundError: No module named 'state_store'`

- [ ] **Step 3: Implementare `state_store.py`**

```python
"""Persistenza sqlite dello stato dei check — 'at a glance' per la TUI.

Schema a righe fisse: una riga per coppia (env, check_name), sempre
sovrascritta (INSERT OR REPLACE). La tabella non cresce mai, zero
cleanup necessario — stesso principio già usato per download_dir nel
layer Ops.
"""

from __future__ import print_function

import json
import os
import sqlite3

_DEFAULT_DB_PATH = os.path.expanduser("~/.hadoopscope/state.db")
_DB_PATH = None  # type: object


def init(db_path=None):
    # type: (object) -> None
    """Inizializza il path del DB e crea la tabella se non esiste.

    db_path=None usa il default ~/.hadoopscope/state.db. Chiamare più
    volte è sicuro (idempotente) — usato sia dal main loop sia dai test.
    """
    global _DB_PATH
    _DB_PATH = db_path or _DEFAULT_DB_PATH
    parent = os.path.dirname(_DB_PATH)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    conn = _connect()
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS check_state ("
            "  env TEXT, check_name TEXT, status TEXT, message TEXT,"
            "  details TEXT, run_at TEXT,"
            "  PRIMARY KEY (env, check_name)"
            ")"
        )
        conn.commit()
    finally:
        conn.close()


def _connect():
    # type: () -> sqlite3.Connection
    path = _DB_PATH or _DEFAULT_DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def save_result(env_name, result):
    # type: (str, object) -> None
    """INSERT OR REPLACE la riga (env, check_name) con lo stato corrente."""
    import datetime
    conn = _connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO check_state "
            "(env, check_name, status, message, details, run_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (env_name, result.name, result.status, result.message,
             json.dumps(result.details or {}),
             datetime.datetime.now().isoformat(timespec="seconds"))
        )
        conn.commit()
    finally:
        conn.close()


def get_env_summary(env_name):
    # type: (str) -> list
    """Tutte le righe check_state per un singolo env — per il drill-down Home."""
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT env, check_name, status, message, details, run_at "
            "FROM check_state WHERE env = ? ORDER BY check_name",
            (env_name,)
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_all_envs_summary():
    # type: () -> list
    """Per env: stato peggiore, conteggi per status — per la grid Home."""
    _SEVERITY = {"CRITICAL": 3, "WARNING": 2, "UNKNOWN": 1, "SKIPPED": 1, "OK": 0}
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT env, status FROM check_state ORDER BY env"
        )
        by_env = {}  # type: dict
        for row in cur.fetchall():
            env = row["env"]
            by_env.setdefault(env, []).append(row["status"])
    finally:
        conn.close()

    summary = []
    for env, statuses in by_env.items():
        counts = {}  # type: dict
        for s in statuses:
            counts[s] = counts.get(s, 0) + 1
        worst = max(statuses, key=lambda s: _SEVERITY.get(s, 0))
        summary.append({"env": env, "worst_status": worst, "counts": counts})
    return summary


def _row_to_dict(row):
    # type: (sqlite3.Row) -> dict
    return {
        "env": row["env"],
        "check_name": row["check_name"],
        "status": row["status"],
        "message": row["message"],
        "details": json.loads(row["details"]) if row["details"] else {},
        "run_at": row["run_at"],
    }
```

- [ ] **Step 4: Eseguire il test per verificare che passi**

Run: `python3 tests/test_state_store.py`
Expected: `7/7 passed`

- [ ] **Step 5: Aggiungere `test_state_store.py` a `tests/run_all.py`**

In `tests/run_all.py`, aggiungere alla lista `test_files`:

```python
    os.path.join(TESTS_DIR, "test_state_store.py"),
```

- [ ] **Step 6: Eseguire la suite completa**

Run: `make test`
Expected: `ALL TESTS PASSED`

- [ ] **Step 7: Commit**

```bash
git add state_store.py tests/test_state_store.py tests/run_all.py
git commit -m "feat: state_store.py — persistenza sqlite dello stato dei check (schema check_state)"
```

---

## Task 2: Hook di scrittura in `hadoopscope.py::main()`

**Files:**
- Modify: `hadoopscope.py:1-20` (import)
- Modify: `hadoopscope.py:337-401` (`main()`)

- [ ] **Step 1: Aggiungere l'import e la chiamata `init()` in `main()`**

In `hadoopscope.py`, aggiungere l'import in cima (accanto agli altri import applicativi, dopo `import applog as _applog`):

```python
import state_store
```

In `main()`, subito dopo la riga `_applog.setup(cfg)` (che inizializza il logger rotante), aggiungere:

```python
    state_store.init()
```

- [ ] **Step 2: Aggiungere la scrittura su state_store accanto a `_applog.log_result(r)`**

Sostituire (righe 398-399 attuali):

```python
        for r in results:
            _applog.log_result(r)
```

con:

```python
        for r in results:
            _applog.log_result(r)
            state_store.save_result(env_name, r)
```

- [ ] **Step 3: Verifica manuale — un run scrive nel DB**

Run:
```bash
rm -f ~/.hadoopscope/state.db
python3 hadoopscope.py --config config/test.yaml --env test-hdp --dry-run
python3 -c "import state_store; state_store.init(); print(state_store.get_all_envs_summary())"
```
Expected: la seconda riga stampa una lista con un dict per `test-hdp`
(dry-run produce risultati con status `DRY_RUN` — non tra i valori attesi
in `_SEVERITY`, ricade su severità 0 di default, comportamento accettabile
per un dry-run: non è un vero stato del cluster).

Rilanciare senza `--dry-run` contro `config/test.yaml` (punta a
`localhost:8080`, quindi produce `UNKNOWN` per connection error — comunque
un vero stato salvato):
```bash
python3 hadoopscope.py --config config/test.yaml --env test-hdp
python3 -c "import state_store; state_store.init(); print(state_store.get_env_summary('test-hdp'))"
```
Expected: lista non vuota di dict con `status: "UNKNOWN"` per ogni check
(connection refused verso `localhost:8080`).

- [ ] **Step 4: Eseguire la suite di test per non-regressione**

Run: `make test`
Expected: `ALL TESTS PASSED`

- [ ] **Step 5: Commit**

```bash
git add hadoopscope.py
git commit -m "feat: hadoopscope.py — scrive lo stato dei check su state_store ad ogni run"
```

---

## Task 3: Refactor — `build_ops_registry()` in `ops/__init__.py`

**Files:**
- Modify: `ops/__init__.py` (oggi vuoto)
- Modify: `hadoopscope.py:119-124` (rimuove la funzione, importa da `ops`)
- Modify: `hadoopscope.py:139,180` (nessuna modifica di sostanza — la chiamata resta `build_ops_registry()`, ora risolta dall'import)

- [ ] **Step 1: Spostare `build_ops_registry()` in `ops/__init__.py`**

Contenuto di `ops/__init__.py` (oggi vuoto):

```python
"""Layer Ops — tool on-demand (non schedulati), affiancati al monitoring."""


def build_ops_registry():
    # type: () -> dict
    """Registry dei tool Ops disponibili, per nome."""
    from ops.yarn_app import AppStatusTool, AppLogsTool
    tools = [AppStatusTool, AppLogsTool]
    return {cls.name: cls for cls in tools}
```

- [ ] **Step 2: Aggiornare `hadoopscope.py` per importare invece di definire**

Rimuovere (righe 119-124):

```python
def build_ops_registry():
    # type: () -> dict
    """Registry dei tool Ops disponibili, per nome."""
    from ops.yarn_app import AppStatusTool, AppLogsTool
    tools = [AppStatusTool, AppLogsTool]
    return {cls.name: cls for cls in tools}
```

Aggiungere l'import in cima al file, accanto agli altri import applicativi:

```python
from ops import build_ops_registry
```

Le due chiamate esistenti (`build_ops_registry()` a riga 139 dentro
`build_ops_arg_parser()`, e a riga 180 dentro `ops_main()`) restano
identiche — ora risolvono tramite l'import invece che una funzione locale.

- [ ] **Step 3: Verifica manuale — CLI ops invariato**

Run: `python3 hadoopscope.py ops --config config/test.yaml app-status --env test-hdp --app-id application_x`
Expected: stesso comportamento di prima del refactor (query REST verso
`localhost:8080`, fallisce con connection error — nessun crash da import).

- [ ] **Step 4: Eseguire la suite di test per non-regressione**

Run: `make test`
Expected: `ALL TESTS PASSED`

- [ ] **Step 5: Commit**

```bash
git add ops/__init__.py hadoopscope.py
git commit -m "refactor: build_ops_registry() spostato in ops/__init__.py, importabile senza hadoopscope.py"
```

---

## Task 4: `tui/widgets.py` — primitive di disegno e palette LCARS

**Files:**
- Create: `tui/__init__.py` (vuoto)
- Create: `tui/widgets.py`

Nessun test automatico per questo task (codice curses, richiede un
terminale reale) — verifica manuale a fine task.

- [ ] **Step 1: Creare `tui/__init__.py` vuoto**

```bash
mkdir -p tui
touch tui/__init__.py
```

- [ ] **Step 2: Creare `tui/widgets.py`**

```python
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
```

- [ ] **Step 3: Verifica manuale — palette e box si disegnano senza crash**

Run:
```bash
python3 -c "
import curses
import tui.widgets as w

def main(stdscr):
    w.init_colors()
    curses.curs_set(0)
    stdscr.erase()
    w.draw_sidebar(stdscr, ['HOME', 'MONITORING', 'OPS'], 0)
    w.draw_box(stdscr, 0, 18, 10, 40)
    w.safe_addstr(stdscr, 1, 20, 'Test OK', curses.color_pair(w.C_OK))
    stdscr.refresh()
    stdscr.getch()

curses.wrapper(main)
"
```
Expected: terminale mostra sidebar con HOME evidenziato in arancione,
MONITORING e OPS con colori diversi, un box a doppia linea a destra con
"Test OK" in verde dentro. Premere un tasto per uscire senza crash.

- [ ] **Step 4: Commit**

```bash
git add tui/__init__.py tui/widgets.py
git commit -m "feat: tui/widgets.py — primitive curses e palette LCARS con fallback colore"
```

---

## Task 5: `tui/screens/base.py` — contratto `Screen`

**Files:**
- Create: `tui/screens/__init__.py` (vuoto)
- Create: `tui/screens/base.py`
- Test: `tests/test_tui_screens_base.py`

`Screen` stessa non fa I/O curses nel suo contratto base (solo i metodi da
sovrascrivere), quindi è testabile senza un terminale reale.

- [ ] **Step 1: Scrivere il test**

Creare `tests/test_tui_screens_base.py`:

```python
"""Test per il contratto Screen — nessuna dipendenza da un terminale reale."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tui.screens.base import Screen


def test_screen_default_enter_is_noop():
    s = Screen(app=None)
    s.enter()  # non deve sollevare


def test_screen_render_raises_not_implemented():
    s = Screen(app=None)
    try:
        s.render(stdscr=None)
        assert False, "should raise"
    except NotImplementedError:
        pass


def test_screen_handle_input_raises_not_implemented():
    s = Screen(app=None)
    try:
        s.handle_input(key=ord("q"))
        assert False, "should raise"
    except NotImplementedError:
        pass


if __name__ == "__main__":
    tests = [
        test_screen_default_enter_is_noop,
        test_screen_render_raises_not_implemented,
        test_screen_handle_input_raises_not_implemented,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} — {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
```

- [ ] **Step 2: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_tui_screens_base.py`
Expected: `ModuleNotFoundError: No module named 'tui.screens'`

- [ ] **Step 3: Implementare `tui/screens/__init__.py` e `tui/screens/base.py`**

```bash
touch tui/screens/__init__.py
```

`tui/screens/base.py`:

```python
"""Screen — contratto comune per ogni schermata della TUI.

Ogni sezione (Home/Monitoring/Ops) mantiene un piccolo stack di Screen;
handle_input() ritorna:
  - "pop"           -> torna alla schermata precedente nello stack
  - un'istanza Screen -> viene pushata sullo stack (drill-down)
  - "quit"           -> chiude l'applicazione
  - None             -> resta sulla schermata corrente (nessuna transizione)
"""

from __future__ import print_function


class Screen(object):
    def __init__(self, app):
        # type: (object) -> None
        """app: riferimento all'istanza App (config, caps, stdscr condivisi)."""
        self.app = app

    def enter(self):
        # type: () -> None
        """Chiamato quando la schermata diventa attiva (push o ritorno da
        una schermata figlia). Default no-op — sovrascrivere per
        ricaricare dati (es. rileggere state_store)."""
        pass

    def render(self, stdscr):
        # type: (object) -> None
        raise NotImplementedError(
            "Implement render() in {}".format(self.__class__.__name__)
        )

    def handle_input(self, key):
        # type: (int) -> object
        raise NotImplementedError(
            "Implement handle_input() in {}".format(self.__class__.__name__)
        )
```

- [ ] **Step 4: Eseguire il test per verificare che passi**

Run: `python3 tests/test_tui_screens_base.py`
Expected: `3/3 passed`

- [ ] **Step 5: Aggiungere a `tests/run_all.py`**

```python
    os.path.join(TESTS_DIR, "test_tui_screens_base.py"),
```

- [ ] **Step 6: Eseguire la suite completa**

Run: `make test`
Expected: `ALL TESTS PASSED`

- [ ] **Step 7: Commit**

```bash
git add tui/screens/__init__.py tui/screens/base.py tests/test_tui_screens_base.py tests/run_all.py
git commit -m "feat: tui/screens/base.py — contratto Screen (enter/render/handle_input)"
```

---

## Task 6: `tui/screens/home.py` — grid cluster + drill-down

**Files:**
- Create: `tui/screens/home.py`

Nessun test automatico (rendering curses) — verifica manuale a fine task.
La logica di formattazione (non il rendering) resta minima e diretta,
non giustifica l'overhead di un test separato per questo screen.

- [ ] **Step 1: Implementare `tui/screens/home.py`**

```python
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
            self.envs.append({
                "env": env,
                "worst_status": row["worst_status"] if row else None,
                "counts": row["counts"] if row else {},
            })
        if self.cursor >= len(self.envs):
            self.cursor = max(0, len(self.envs) - 1)

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "HOME — {} environment(s) configurati".format(len(self.envs)),
                   curses.A_BOLD)
        col_w, row_h = 26, 6
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
```

- [ ] **Step 2: Verifica manuale — grid e drill-down**

Precondizione: eseguire almeno un run reale (Task 2, Step 3) così
`~/.hadoopscope/state.db` contiene dati per `test-hdp`.

Run:
```bash
python3 -c "
import curses
import tui.widgets as w
from config import load_config
from tui.screens.home import HomeGridScreen

def main(stdscr):
    w.init_colors()
    curses.curs_set(0)
    class FakeApp:
        cfg = load_config('config/test.yaml')
    screen = HomeGridScreen(FakeApp())
    screen.enter()
    stdscr.erase()
    screen.render(stdscr)
    stdscr.refresh()
    key = stdscr.getch()
    if key in (curses.KEY_ENTER, 10, 13):
        detail = screen.handle_input(key)
        stdscr.erase()
        detail.enter()
        detail.render(stdscr)
        stdscr.refresh()
        stdscr.getch()

curses.wrapper(main)
"
```
Expected: prima schermata mostra una card per `test-hdp` con stato
`UNKNOWN` (dal Task 2). Premendo Invio (con cursore sulla card) passa al
dettaglio, che elenca ogni check con il suo messaggio.

- [ ] **Step 3: Commit**

```bash
git add tui/screens/home.py
git commit -m "feat: tui/screens/home.py — grid cluster da state_store + drill-down dettaglio"
```

---

## Task 7: `tui/screens/ops.py` — lista tool → env → parametri → risultato

**Files:**
- Create: `tui/screens/ops.py`

- [ ] **Step 1: Implementare `tui/screens/ops.py`**

```python
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
        safe_addstr(stdscr, 0, 20, "Esecuzione in corso...", curses.A_BOLD)

    def handle_input(self, key):
        # type: (int) -> object
        return None


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
```

Nota di design: `OpsParamInputScreen.enter()` esegue i prompt e la
chiamata `tool.run()` in modo sincrono/bloccante — coerente con come
`AppStatusTool`/`AppLogsTool` funzionano già da CLI (nessuna esecuzione
asincrona in tutto il progetto). L'`App` (Task 9) deve chiamare `enter()`
sulla nuova schermata pushata PRIMA di provare a fare `render()` su di
essa, altrimenti `self.result_screen` non esiste ancora — vincolo per il
loop principale, verificato nel Task 9.

- [ ] **Step 2: Verifica manuale — flusso Ops completo**

Run:
```bash
python3 -c "
import curses
from config import load_config
from bootstrap import discover_capabilities
import tui.widgets as w
from tui.screens.ops import OpsToolListScreen

def main(stdscr):
    w.init_colors()
    curses.curs_set(0)
    class FakeApp:
        cfg = load_config('config/test.yaml')
        caps = discover_capabilities()
        def __init__(self, s): self.stdscr = s
    app = FakeApp(stdscr)
    screen = OpsToolListScreen(app)
    while True:
        stdscr.erase()
        screen.render(stdscr)
        stdscr.refresh()
        key = stdscr.getch()
        if key == 27:
            break
        result = screen.handle_input(key)
        if result is not None:
            screen = result
            screen.enter()

curses.wrapper(main)
"
```
Expected: lista con `app-status` e `app-logs`. Invio su `app-status` →
lista env (`test-hdp`) → Invio → prompt `app_id` (digitare qualunque
stringa, es. `application_test`) → schermata di esecuzione lampo →
risultato `UNKNOWN` (connection error verso `localhost:8080`, atteso —
`config/test.yaml` non punta a un vero cluster). ESC per uscire.

- [ ] **Step 3: Commit**

```bash
git add tui/screens/ops.py
git commit -m "feat: tui/screens/ops.py — lista tool, picker env, input parametri da OpsParam, risultato"
```

---

## Task 8: `tui/screens/monitoring.py` — sottomenu esegui-ora / gestisci-schedulati

**Files:**
- Create: `tui/screens/monitoring.py`

Riusa la logica crontab esistente in `cluster_status.py`
(`_crontab_read`, `_crontab_write`, `_parse_hs_block`, `_format_hs_block`,
`_HS_MARKER`, `_cron_label`) importandola da lì — non viene duplicata né
riscritta, solo richiamata. `cluster_status.py` resta nel repo fino al
Task 10 (dove viene rimosso/deprecato), quindi l'import funziona per
tutta la durata di questo task.

- [ ] **Step 1: Implementare `tui/screens/monitoring.py`**

```python
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
        self.envs = sorted(app.cfg.get("environments", {}).keys())
        self.selected = set()

    def render(self, stdscr):
        # type: (object) -> None
        safe_addstr(stdscr, 0, 20, "MONITORING — scegli environment (SPAZIO seleziona, INVIO conferma)", curses.A_BOLD)
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
        safe_addstr(stdscr, 0, 20, "MONITORING — scegli check (SPAZIO seleziona, INVIO conferma)", curses.A_BOLD)
        items = [label for key, label in self.categories]
        draw_list(stdscr, items, self.cursor, y=2, x=20, h=10, w=60, selected=self.selected)

    def handle_input(self, key):
        # type: (int) -> object
        if key == curses.KEY_UP and self.cursor > 0:
            self.cursor -= 1
        elif key == curses.KEY_DOWN and self.cursor < len(self.categories) - 1:
            self.cursor += 1
        elif key == ord(" "):
            if self.cursor in self.selected:
                self.selected.discard(self.cursor)
            else:
                self.selected.add(self.cursor)
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
            env_config = self.app.cfg["environments"][env_name]
            results = run_checks_for_env(env_name, env_config, self.app.cfg, self.app.caps, args)
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
        cron_expr = ask_text(self.app.stdscr, "Espressione cron (es. */15 * * * *)", "*/15 * * * *")
        if not cron_expr:
            self.done_message = "Annullato."
            return
        entry = {
            "config": self.app.config_path,
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
```

- [ ] **Step 2: Verifica manuale — sottomenu e crontab (ambiente di test, non toccare crontab reale se non vuoi)**

⚠️ Questo screen scrive davvero sul crontab dell'utente Unix corrente
tramite `crontab -`. Per la verifica, usare una macchina/container di
test o essere pronti a ripulire l'entry aggiunta a mano con `crontab -e`.

Run:
```bash
python3 -c "
import curses
from config import load_config
from bootstrap import discover_capabilities
import tui.widgets as w
from tui.screens.monitoring import MonitoringMenuScreen

def main(stdscr):
    w.init_colors()
    curses.curs_set(0)
    class FakeApp:
        cfg = load_config('config/test.yaml')
        caps = discover_capabilities()
        config_path = 'config/test.yaml'
        def __init__(self, s): self.stdscr = s
    app = FakeApp(stdscr)
    screen = MonitoringMenuScreen(app)
    while True:
        stdscr.erase()
        screen.render(stdscr)
        stdscr.refresh()
        key = stdscr.getch()
        if key == 27:
            break
        result = screen.handle_input(key)
        if result is not None:
            screen = result
            screen.enter()

curses.wrapper(main)
"
```
Expected: menu con le due voci. "Gestisci check schedulati" mostra le
entry `hs:` esistenti nel crontab reale (probabilmente quelle di
produzione se configurate — solo lettura finché non premi SPAZIO/d).

- [ ] **Step 3: Commit**

```bash
git add tui/screens/monitoring.py
git commit -m "feat: tui/screens/monitoring.py — esegui-ora + gestisci-schedulati, riusa parsing crontab esistente"
```

---

## Task 9: `tui/app.py` — loop principale e sidebar

**Files:**
- Create: `tui/app.py`

- [ ] **Step 1: Implementare `tui/app.py`**

```python
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
```

- [ ] **Step 2: Verifica manuale end-to-end completa**

Run: `python3 -m tui.app config/test.yaml`

Percorso di verifica (annotare eventuali crash o comportamenti inattesi,
non procedere al Task 10 finché questo percorso non è pulito):

1. All'avvio: sidebar con HOME evidenziato, grid con la card `test-hdp`
   (stato dal Task 2/6). `Tab` due volte → sidebar mostra OPS
   evidenziato, `Tab` ancora → torna su HOME (ciclo a 3 voci)
2. Su HOME: Invio sulla card `test-hdp` → dettaglio con lista check.
   `ESC` → torna alla grid (non chiude l'app)
3. `Tab` → MONITORING → Invio su "Esegui check ora" → SPAZIO su
   `test-hdp` → Invio → SPAZIO su una categoria → Invio → esecuzione
   (qualche secondo, connection error atteso verso `localhost:8080`) →
   risultati a schermo → `ESC` più volte torna al menu Monitoring
4. `Tab` → OPS → Invio su `app-status` → Invio su `test-hdp` → digitare
   un application id qualsiasi → risultato `UNKNOWN` (atteso) → `ESC`
   torna alla lista tool
5. Dalla vista principale di una sezione (stack con 1 solo elemento),
   `ESC` → prompt "Uscire da HadoopScope? [s/N]" → `s` chiude pulito,
   nessun traceback residuo nel terminale dopo l'uscita (verificare che
   il terminale torni in modalità normale — `curses.wrapper` deve
   ripristinarlo anche se l'app esce con un'eccezione)

Expected: nessun crash lungo tutto il percorso, terminale sempre
utilizzabile dopo l'uscita.

- [ ] **Step 3: Commit**

```bash
git add tui/app.py
git commit -m "feat: tui/app.py — loop principale, sidebar persistente, entry point python3 -m tui.app"
```

---

## Task 10: Rimozione di `cluster_status.py` come entry point

**Files:**
- Modify: `cluster_status.py` (mantiene solo le funzioni riusate da `tui/screens/monitoring.py`, rimuove il resto)
- Modify: `README.md` (se referenzia `cluster_status.py` come entry point — verificare)
- Modify: `tuxbox.toml` (se referenzia `cluster_status.py` — verificare)

`tui/screens/monitoring.py` (Task 8) importa da `cluster_status`:
`_crontab_read`, `_crontab_write`, `_parse_hs_block`, `_format_hs_block`,
`_cron_label`, `CHECK_CATEGORIES`. Queste restano. Tutto il resto del
file (le funzioni `_step_*`, `_draw_*`, `_ask_*`, `_tui_main`, `main`,
`_init_colors`, ecc. — il vecchio wizard curses) non è più raggiungibile
da nessun entry point dopo questo task e va rimosso.

- [ ] **Step 1: Verificare i riferimenti a `cluster_status.py` nel repo**

Run: `grep -rn "cluster_status" --include="*.py" --include="*.md" --include="*.toml" --include="Makefile" .`

Annotare ogni file che lo referenzia come entry point eseguibile (non gli
import da `tui/screens/monitoring.py`, quello resta) — aggiornare quei
riferimenti al nuovo entry point `python3 -m tui.app` nello Step 3.

- [ ] **Step 2: Ridurre `cluster_status.py` alle sole funzioni riusate**

Il file mantiene, nell'ordine in cui appaiono oggi: l'header/docstring
(aggiornato per riflettere che non è più un entry point eseguibile, solo
una libreria di funzioni crontab riusate da `tui/`), gli import
strettamente necessari (`subprocess`, `sys`, `os`), `CHECK_CATEGORIES`,
`CRON_PRESETS`, `_HS_MARKER`, `_cron_label`, `_default_log_path`,
`_crontab_read`, `_crontab_write`, `_parse_hs_block`, `_format_hs_block`.

Aggiornare il docstring in cima al file (righe 1-15 attuali):

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cluster_status — logica di parsing/scrittura crontab HadoopScope.

Non è più un entry point eseguibile: il wizard curses è stato sostituito
da tui/app.py (python3 -m tui.app). Questo modulo resta solo per le
funzioni di gestione crontab (_crontab_read, _crontab_write,
_parse_hs_block, _format_hs_block, _cron_label), riusate da
tui/screens/monitoring.py.
"""
```

Rimuovere tutto il resto del file (dalla sezione `# ── Config discovery
──` in poi, comprese `find_config_files`, `load_env_names`, tutte le
funzioni `_draw_*`/`_ask_*`/`_step_*`/`_run_checks`/`_tui_main`/`main`, e
il blocco `if __name__ == "__main__":` finale) — non più raggiungibili
da nessun punto del programma.

- [ ] **Step 3: Aggiornare i riferimenti trovati allo Step 1**

Per ogni file che menzionava `cluster_status.py` come comando da lanciare
(es. `python3 cluster_status.py` in README o docs), sostituire con
`python3 -m tui.app [config_path]`.

- [ ] **Step 4: Eseguire la suite completa**

Run: `make test`
Expected: `ALL TESTS PASSED` (nessun test importava le funzioni rimosse —
verificato: `tui/screens/monitoring.py` importa solo le funzioni
mantenute).

- [ ] **Step 5: Verifica manuale — la TUI funziona ancora dopo la riduzione**

Ripetere il percorso di verifica del Task 9 Step 2 (almeno i punti 3 e 4,
che dipendono dalle funzioni crontab mantenute in `cluster_status.py`).
Expected: nessuna differenza rispetto a prima — `cluster_status.py`
ridotto non cambia il comportamento di `tui/app.py`.

- [ ] **Step 6: Commit**

```bash
git add cluster_status.py README.md tuxbox.toml
git commit -m "$(cat <<'EOF'
refactor: cluster_status.py non è più un entry point, solo libreria crontab

Il wizard curses è sostituito da tui/app.py (python3 -m tui.app).
Restano solo le funzioni di parsing/scrittura crontab, riusate da
tui/screens/monitoring.py — nessun comportamento cambiato.
EOF
)"
```

---

## Task 11: Aggiornare la documentazione del progetto

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Aggiungere/aggiornare la sezione TUI del README**

Aggiungere (o sostituire la sezione esistente che menziona
`cluster_status.py`, se presente) una sezione:

```markdown
## TUI interattiva

```bash
python3 -m tui.app config/hadoopscope.yaml
```

Navigazione 100% da tastiera, tre sezioni sempre accessibili dalla
sidebar (`Tab` per cambiare sezione, `ESC` per tornare indietro/uscire):

- **Home** — stato riassuntivo di tutti i cluster configurati, Invio su
  una card per il dettaglio per servizio
- **Monitoring** — esegui check on-demand (con opzione di schedularli via
  crontab) o gestisci i check già schedulati
- **Ops** — tool operativi on-demand (es. status/log di un'applicazione
  YARN dato l'application id)
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README — sezione TUI interattiva (python3 -m tui.app)"
```
