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
from __future__ import print_function

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
