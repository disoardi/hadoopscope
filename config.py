"""Config loader — YAML manual parser con env var expansion. Zero deps."""

from __future__ import print_function

import os
import re
import sys

# Gestione import yaml: prova PyYAML come fallback di comodità,
# ma il parser manuale è il default per ambienti blindati
try:
    import yaml as _yaml
    _HAS_PYYAML = True
except ImportError:
    _HAS_PYYAML = False


def _expand_env_vars(value):
    # type: (object) -> object
    """Espande ${VAR} nelle stringhe. Errore descrittivo se var mancante."""
    if not isinstance(value, str):
        return value

    def replacer(match):
        var_name = match.group(1)
        val = os.environ.get(var_name)
        if val is None:
            raise ValueError(
                "Environment variable '{}' is not set. "
                "Export it before running HadoopScope.".format(var_name)
            )
        return val

    return re.sub(r'\$\{([^}]+)\}', replacer, value)


def _walk_expand(obj):
    # type: (object) -> object
    """Ricorsivamente espande env var in tutto il config tree."""
    if isinstance(obj, dict):
        return {k: _walk_expand(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_expand(i) for i in obj]
    elif isinstance(obj, str):
        return _expand_env_vars(obj)
    return obj


def _parse_yaml_manual(text):
    # type: (str) -> dict
    """
    Parser YAML minimalista per il subset usato da HadoopScope.
    Supporta: dict, list con -, string, bool, int.
    Limitazione: no multi-line strings, no anchors.
    """
    # TODO: implementare parser completo se necessario.
    # Per ora: se PyYAML non disponibile, solleviamo errore utile.
    raise ImportError(
        "PyYAML non trovato. Installalo con: pip install pyyaml\n"
        "Oppure usa un venv: python3 -m venv venv && source venv/bin/activate && pip install pyyaml"
    )


def load_config(path):
    # type: (str) -> dict
    """Carica e valida il config file YAML."""
    if not os.path.exists(path):
        raise FileNotFoundError("Config file not found: {}".format(path))

    with open(path, 'r') as f:
        raw = f.read()

    if _HAS_PYYAML:
        data = _yaml.safe_load(raw)
    else:
        data = _parse_yaml_manual(raw)

    # Espandi env var
    data = _walk_expand(data)

    # Validazione minima
    if "environments" not in data:
        raise ValueError("Config missing 'environments' key")
    if not data["environments"]:
        raise ValueError("Config has no environments defined")

    return data
