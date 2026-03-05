"""
Application logger per HadoopScope.

Scrive ogni run e risultato check su file rotante (stdlib RotatingFileHandler).
Zero dipendenze aggiuntive.

Default: ~/.hadoopscope/logs/hadoopscope.log (10 MB, 5 backup).

Config (top-level nel YAML):
  logging:
    enabled: true
    file: ~/.hadoopscope/logs/hadoopscope.log
    max_mb: 10        # ruota dopo N MB (default 10)
    backup_count: 5   # file di backup da tenere (default 5)

Formato log:
  2026-03-05 10:00:00 [--------] RUN START  env=prod-cdp checks=hive
  2026-03-05 10:00:00 [OK      ] HiveCheck -- HiveServer2 OK (...)
  2026-03-05 10:00:00 [WARNING ] HivePartitionCheck -- Tables exceeding 10000 partitions:
  2026-03-05 10:00:00 [        ]   sdv00h.t_sdv00_esito_controllo_old: 33638
  ... tutte le entry (lista completa, nessun troncamento) ...
  2026-03-05 10:00:00 [--------] RUN END    env=prod-cdp -- 1 OK, 1 WARNING

Le run schedulate via crontab chiamano hadoopscope.py come subprocess e
loggano automaticamente sullo stesso file rotante.
"""
from __future__ import print_function

import datetime
import logging
import os
from logging.handlers import RotatingFileHandler

_log = None  # type: logging.Logger

_STATUS_ICON = {
    "OK":       "[OK      ]",
    "WARNING":  "[WARNING ]",
    "CRITICAL": "[CRITICAL]",
    "UNKNOWN":  "[UNKNOWN ]",
    "SKIPPED":  "[SKIPPED ]",
    "DRY_RUN":  "[DRY-RUN ]",
}
_CONT = "[        ]"  # icon per righe di continuazione (blank)
_SEP  = "[--------]"  # icon per run start/end


def setup(cfg):
    # type: (dict) -> None
    """Inizializza il logger rotante da config. Chiama una sola volta all'avvio."""
    global _log
    log_cfg = cfg.get("logging", {})

    if str(log_cfg.get("enabled", "true")).lower() in ("false", "0", "no"):
        return

    log_file = os.path.expanduser(
        log_cfg.get("file", "~/.hadoopscope/logs/hadoopscope.log")
    )
    max_bytes    = int(log_cfg.get("max_mb", 10)) * 1024 * 1024
    backup_count = int(log_cfg.get("backup_count", 5))

    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError:
            return  # impossibile creare la dir -- skip silenzioso

    _log = logging.getLogger("hadoopscope.app")
    _log.setLevel(logging.DEBUG)
    _log.propagate = False

    # Rimuovi handler esistenti (evita duplicati su re-inizializzazione)
    for h in _log.handlers[:]:
        _log.removeHandler(h)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    _log.addHandler(handler)


def _now():
    # type: () -> str
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _w(line):
    # type: (str) -> None
    if _log is not None:
        _log.info(line)


def log_run_start(env_name, checks_arg):
    # type: (str, str) -> None
    """Log inizio di un run per un environment."""
    _w("{} {} RUN START  env={} checks={}".format(
        _now(), _SEP, env_name, checks_arg or "all"))


def log_result(result):
    # type: (object) -> None
    """Log il risultato di un check.

    Per WARNING/CRITICAL con over_threshold nei details: logga la lista
    COMPLETA -- non il troncamento '+N more' mostrato a schermo.
    Per gli altri check: logga tutte le righe del message.
    """
    if _log is None:
        return

    ts   = _now()
    icon = _STATUS_ICON.get(result.status, "[?       ]")
    msg_lines = (result.message or "").splitlines()
    first     = msg_lines[0] if msg_lines else ""

    _w("{} {} {} -- {}".format(ts, icon, result.name, first))

    # over_threshold in details -> logga TUTTE le entry (lista completa)
    over = None
    if result.details:
        over = result.details.get("over_threshold")

    if over:
        for entry in over:
            _w("{} {}   {}".format(ts, _CONT, entry))
    else:
        # Nessun over_threshold: logga le righe di continuazione del message
        for line in msg_lines[1:]:
            _w("{} {} {}".format(ts, _CONT, line))


def log_run_end(env_name, results):
    # type: (str, list) -> None
    """Log riepilogo di fine run."""
    counts = {}  # type: dict
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    parts = []
    for s in ("CRITICAL", "WARNING", "OK", "UNKNOWN", "SKIPPED", "DRY_RUN"):
        n = counts.get(s, 0)
        if n > 0:
            parts.append("{} {}".format(n, s))

    _w("{} {} RUN END    env={} -- {}".format(
        _now(), _SEP, env_name,
        ", ".join(parts) if parts else "no checks run"))
