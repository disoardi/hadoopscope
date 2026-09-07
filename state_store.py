"""Persistenza dello stato dei check — 'at a glance' per la TUI.

Backend sqlite3 se disponibile (schema a righe fisse, una riga per coppia
(env, check_name), sempre sovrascritta con INSERT OR REPLACE — la tabella
non cresce mai, zero cleanup necessario). Fallback automatico a JSON+flock
se sqlite3 non e' disponibile nell'interprete (l'estensione C _sqlite3
manca — build pyenv compilata senza sqlite-devel, visto su un ambiente
client locked-down senza accesso root per rebuildare Python). Stessa API
pubblica, stessa semantica di upsert, in entrambi i casi — i chiamanti
(tui/, hadoopscope.py) non sanno quale backend e' attivo.
"""

from __future__ import print_function

import datetime
import json
import os

try:
    import sqlite3
    _HAS_SQLITE = True
except ImportError:
    sqlite3 = None
    _HAS_SQLITE = False

_DEFAULT_DB_PATH = os.path.expanduser("~/.hadoopscope/state.db")
_DB_PATH = None  # type: object

_SEVERITY = {"CRITICAL": 3, "WARNING": 2, "UNKNOWN": 1, "SKIPPED": 1, "OK": 0}


def init(db_path=None):
    # type: (object) -> None
    """Inizializza il path del DB e crea la tabella/il file se non esiste.

    db_path=None usa il default ~/.hadoopscope/state.db. Chiamare più
    volte è sicuro (idempotente) — usato sia dal main loop sia dai test.
    """
    global _DB_PATH
    _DB_PATH = db_path or _DEFAULT_DB_PATH
    parent = os.path.dirname(_DB_PATH)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    if _HAS_SQLITE:
        _sqlite_init()
    else:
        _json_init()


def save_result(env_name, result):
    # type: (str, object) -> None
    """Upsert la riga (env, check_name) con lo stato corrente."""
    if _HAS_SQLITE:
        _sqlite_save_result(env_name, result)
    else:
        _json_save_result(env_name, result)


def get_env_summary(env_name):
    # type: (str) -> list
    """Tutte le righe per un singolo env — per il drill-down Home."""
    if _HAS_SQLITE:
        return _sqlite_get_env_summary(env_name)
    return _json_get_env_summary(env_name)


def get_all_envs_summary():
    # type: () -> list
    """Per env: stato peggiore, conteggi per status, run più vecchio."""
    if _HAS_SQLITE:
        return _sqlite_get_all_envs_summary()
    return _json_get_all_envs_summary()


# ---------------------------------------------------------------------------
# Backend sqlite3
# ---------------------------------------------------------------------------

def _sqlite_connect():
    # type: () -> object
    conn = sqlite3.connect(_DB_PATH or _DEFAULT_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_init():
    # type: () -> None
    conn = _sqlite_connect()
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


def _sqlite_save_result(env_name, result):
    # type: (str, object) -> None
    conn = _sqlite_connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO check_state "
            "(env, check_name, status, message, details, run_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (env_name, result.name, result.status, result.message,
             json.dumps(result.details or {}),
             datetime.datetime.now().isoformat())
        )
        conn.commit()
    finally:
        conn.close()


def _sqlite_get_env_summary(env_name):
    # type: (str) -> list
    conn = _sqlite_connect()
    try:
        cur = conn.execute(
            "SELECT env, check_name, status, message, details, run_at "
            "FROM check_state WHERE env = ? ORDER BY check_name",
            (env_name,)
        )
        return [_sqlite_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _sqlite_get_all_envs_summary():
    # type: () -> list
    conn = _sqlite_connect()
    try:
        cur = conn.execute("SELECT env, status, run_at FROM check_state ORDER BY env")
        rows = [{"env": r["env"], "status": r["status"], "run_at": r["run_at"]}
                for r in cur.fetchall()]
    finally:
        conn.close()
    return _aggregate_envs(rows)


def _sqlite_row_to_dict(row):
    # type: (object) -> dict
    return {
        "env": row["env"],
        "check_name": row["check_name"],
        "status": row["status"],
        "message": row["message"],
        "details": json.loads(row["details"]) if row["details"] else {},
        "run_at": row["run_at"],
    }


# ---------------------------------------------------------------------------
# Backend JSON + flock — attivo solo quando sqlite3 non e' disponibile.
#
# Stessa granularita' di upsert di sqlite (chiave env+check_name): ogni
# read-modify-write prende un lock esclusivo sull'intero file, cosi' due
# processi concorrenti (es. due --env diversi lanciati dallo stesso cron)
# non si sovrascrivono a vicenda anche se toccano chiavi diverse — stessa
# garanzia della transazione INSERT OR REPLACE di sqlite, solo piu' grezza
# (lock sul file intero anziche' sulla singola riga).
#
# import fcntl e' locale alle funzioni (non a livello di modulo): e' POSIX-
# only, e non deve diventare un requisito quando sqlite3 e' disponibile
# (percorso di gran lunga piu' comune).
# ---------------------------------------------------------------------------

def _json_path():
    # type: () -> str
    return _DB_PATH or _DEFAULT_DB_PATH


def _json_init():
    # type: () -> None
    path = _json_path()
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({}, f)


def _json_with_lock(mutate):
    # type: (object) -> object
    """Apre il file sotto lock esclusivo, decodifica lo stato corrente,
    applica mutate(data) (puo' modificare data in place e/o ritornare un
    valore), riscrive il file per intero, ritorna il valore di mutate()."""
    import fcntl
    path = _json_path()
    with open(path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else {}
            except ValueError:
                data = {}
            result = mutate(data)
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
            return result
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _json_save_result(env_name, result):
    # type: (str, object) -> None
    def mutate(data):
        env_rows = data.setdefault(env_name, {})
        env_rows[result.name] = {
            "status": result.status,
            "message": result.message,
            "details": result.details or {},
            "run_at": datetime.datetime.now().isoformat(),
        }
    _json_with_lock(mutate)


def _json_get_env_summary(env_name):
    # type: (str) -> list
    def mutate(data):
        env_rows = data.get(env_name, {})
        return [
            {
                "env": env_name, "check_name": check_name,
                "status": row["status"], "message": row["message"],
                "details": row["details"], "run_at": row["run_at"],
            }
            for check_name, row in sorted(env_rows.items())
        ]
    return _json_with_lock(mutate)


def _json_get_all_envs_summary():
    # type: () -> list
    def mutate(data):
        rows = []
        for env_name, env_rows in data.items():
            for row in env_rows.values():
                rows.append({"env": env_name, "status": row["status"], "run_at": row["run_at"]})
        return rows
    rows = _json_with_lock(mutate)
    return _aggregate_envs(rows)


# ---------------------------------------------------------------------------
# Aggregazione condivisa dai due backend
# ---------------------------------------------------------------------------

def _aggregate_envs(rows):
    # type: (list) -> list
    by_env = {}         # type: dict
    oldest_by_env = {}   # type: dict
    for row in rows:
        by_env.setdefault(row["env"], []).append(row["status"])
        prev = oldest_by_env.get(row["env"])
        if prev is None or row["run_at"] < prev:
            oldest_by_env[row["env"]] = row["run_at"]

    summary = []
    for env, statuses in by_env.items():
        counts = {}  # type: dict
        for s in statuses:
            counts[s] = counts.get(s, 0) + 1
        worst = max(statuses, key=lambda s: _SEVERITY.get(s, 0))
        summary.append({
            "env": env, "worst_status": worst, "counts": counts,
            # Il più vecchio run_at tra i check dell'env — usato dalla TUI
            # per segnalare "dati non aggiornati da >24h, rilancia i check".
            "oldest_run_at": oldest_by_env[env],
        })
    return summary
