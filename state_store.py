"""Persistenza sqlite dello stato dei check — 'at a glance' per la TUI.

Schema a righe fisse: una riga per coppia (env, check_name), sempre
sovrascritta (INSERT OR REPLACE). La tabella non cresce mai, zero
cleanup necessario — stesso principio già usato per download_dir nel
layer Ops.
"""

from __future__ import print_function

import datetime
import json
import os
import sqlite3

_DEFAULT_DB_PATH = os.path.expanduser("~/.hadoopscope/state.db")
_DB_PATH = None  # type: object

_SEVERITY = {"CRITICAL": 3, "WARNING": 2, "UNKNOWN": 1, "SKIPPED": 1, "OK": 0}


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
    conn = _connect()
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
    conn = _connect()
    try:
        cur = conn.execute("SELECT env, status FROM check_state ORDER BY env")
        by_env = {}  # type: dict
        for row in cur.fetchall():
            by_env.setdefault(row["env"], []).append(row["status"])
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
