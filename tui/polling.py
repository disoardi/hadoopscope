"""Polling YARN in background — refresh periodico di app running/pending
sulla card Home, indipendente dai run manuali/schedulati.

Un thread daemon per environment, non un thread seriale: con decine di
ambienti (es. ISP, ~30) un poll in serie sforerebbe l'intervallo di
refresh. Ogni thread e' quasi sempre in sleep — il costo di N thread
I/O-bound e' trascurabile (nessun pool/coda, sarebbe over-engineering
per questo caso).
"""

from __future__ import print_function

import threading
import time

import state_store
from checks.yarn import YarnClusterMetricsCheck

POLL_INTERVAL_SECONDS = 30

# Le scritture sqlite di thread diversi passano da qui — evita il raro
# "database is locked" sotto scritture concorrenti, senza serializzare
# le chiamate di rete (che restano fuori dal lock).
_SAVE_LOCK = threading.Lock()


def start(app):
    # type: (object) -> None
    """Avvia un thread daemon di polling per ciascun environment configurato."""
    for env_name in app.envs:
        t = threading.Thread(target=_poll_loop, args=(app, env_name))
        t.daemon = True
        t.start()


def _poll_loop(app, env_name):
    # type: (object, str) -> None
    while True:
        _poll_iteration_safe(app, env_name)
        time.sleep(POLL_INTERVAL_SECONDS)


def _poll_iteration_safe(app, env_name):
    # type: (object, str) -> None
    """Un'iterazione di poll che non solleva mai — un env che fallisce
    (rete, config) non deve mai fermare il proprio thread né gli altri."""
    try:
        _poll_once(app, env_name)
    except Exception:
        pass


def _poll_once(app, env_name):
    # type: (object, str) -> None
    env_config = app.envs[env_name]
    env_global = app.env_global[env_name]
    check_config = dict(env_config)
    if "checks" in env_global:
        check_config["checks"] = env_global["checks"]
    result = YarnClusterMetricsCheck(check_config, app.caps).run()
    with _SAVE_LOCK:
        state_store.save_result(env_name, result)
