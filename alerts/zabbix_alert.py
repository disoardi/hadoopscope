"""Zabbix alert via zabbix_sender subprocess. Zero deps oltre stdlib."""

from __future__ import print_function

import subprocess
import sys

from checks.base import CheckResult

# Mapping status → Zabbix severity value
_SEVERITY_MAP = {
    CheckResult.OK:       0,
    CheckResult.UNKNOWN:  1,
    CheckResult.WARNING:  2,
    CheckResult.CRITICAL: 4,
    CheckResult.SKIPPED:  0,
}


def dispatch(results, config, env_name):
    # type: (list, dict, str) -> None
    """Invia i risultati a Zabbix tramite zabbix_sender."""
    zab_cfg = config.get("alerts", {}).get("zabbix", {})
    if not zab_cfg.get("enabled", False):
        return

    server      = zab_cfg.get("server", "127.0.0.1")
    port        = str(zab_cfg.get("port", 10051))
    host        = zab_cfg.get("host", env_name)
    zabbix_bin  = zab_cfg.get("binary", "zabbix_sender")

    # Verifica che zabbix_sender sia disponibile
    import shutil
    if not shutil.which(zabbix_bin):
        print("[alert/zabbix] zabbix_sender not found, skipping", file=sys.stderr)
        return

    # Costruiamo il file di dati per zabbix_sender
    # Formato: <hostname> <key> <timestamp> <value>
    lines = []
    for r in results:
        key   = "hadoopscope.check[{}]".format(r.name.lower().replace(" ", "_"))
        value = _SEVERITY_MAP.get(r.status, 1)
        lines.append('"{}" "{}" {}'.format(host, key, value))

        # Invia anche il messaggio come stringa
        msg_key = "hadoopscope.message[{}]".format(r.name.lower().replace(" ", "_"))
        lines.append('"{}" "{}" "{}"'.format(
            host, msg_key, r.message.replace('"', '\\"')[:255]))

    if not lines:
        return

    data = "\n".join(lines)

    try:
        proc = subprocess.Popen(
            [zabbix_bin, "-z", server, "-p", port, "-i", "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate(input=data.encode("utf-8"), timeout=15)
        if proc.returncode == 0:
            print("[alert/zabbix] Sent {} metric(s) to {}:{}".format(
                len(lines), server, port), file=sys.stderr)
        else:
            print("[alert/zabbix] ERROR (rc={}): {}".format(
                proc.returncode,
                stderr.decode("utf-8", errors="replace")[:200]), file=sys.stderr)
    except subprocess.TimeoutExpired:
        print("[alert/zabbix] Timeout sending to Zabbix", file=sys.stderr)
    except Exception as e:
        print("[alert/zabbix] ERROR: {}".format(str(e)), file=sys.stderr)
