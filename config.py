"""Config loader — YAML manual parser con env var expansion. Zero deps."""

from __future__ import print_function

import os
import re
import sys

# Gestione import yaml: prova PyYAML come convenienza in dev,
# ma il parser manuale garantisce zero-deps in produzione.
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


# ---------------------------------------------------------------------------
# Parser YAML minimale (stdlib-only) — supporta il subset usato da HadoopScope
# ---------------------------------------------------------------------------

def _parse_yaml_manual(text):
    # type: (str) -> dict
    """
    Parser YAML minimale per il subset usato da HadoopScope.
    Supporta: dict annidati, liste con -, inline list [a, b], scalar, bool, int.
    Non supporta: multi-line strings, anchors, merge keys.
    """
    lines = text.splitlines()
    # Rimuovi commenti e righe vuote
    cleaned = []
    for line in lines:
        # Rimuovi commenti inline (ma non # dentro stringhe quotate)
        stripped = line.rstrip()
        if not stripped or stripped.lstrip().startswith('#'):
            continue
        # Rimuovi commento inline (semplice)
        in_quote = False
        quote_char = None
        for i, ch in enumerate(stripped):
            if ch in ('"', "'") and not in_quote:
                in_quote = True
                quote_char = ch
            elif in_quote and ch == quote_char:
                in_quote = False
            elif not in_quote and ch == '#':
                stripped = stripped[:i].rstrip()
                break
        if stripped:
            cleaned.append(stripped)

    result, _ = _parse_block(cleaned, 0, 0)
    return result


def _get_indent(line):
    # type: (str) -> int
    return len(line) - len(line.lstrip(' '))


def _parse_scalar(value):
    # type: (str) -> object
    """Converte una stringa scalare nel tipo Python appropriato."""
    v = value.strip()
    if not v:
        return None
    # Quoted string
    if (v.startswith('"') and v.endswith('"')) or \
       (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    # Bool
    if v.lower() in ('true', 'yes', 'on'):
        return True
    if v.lower() in ('false', 'no', 'off'):
        return False
    # Null
    if v.lower() in ('null', '~', ''):
        return None
    # Int
    try:
        return int(v)
    except ValueError:
        pass
    # Float
    try:
        return float(v)
    except ValueError:
        pass
    # Inline list [a, b, c]
    if v.startswith('[') and v.endswith(']'):
        inner = v[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item.strip()) for item in inner.split(',')]
    return v


def _parse_block(lines, start, base_indent):
    # type: (list, int, int) -> tuple
    """
    Parsa un blocco YAML a partire da `start`.
    Restituisce (oggetto_parsato, indice_prossima_riga_non_consumata).
    """
    result = {}
    i = start

    while i < len(lines):
        line = lines[i]
        indent = _get_indent(line)
        stripped = line.strip()

        if indent < base_indent:
            break

        # Lista item
        if stripped.startswith('- ') or stripped == '-':
            # Siamo in una lista — rilancia come lista
            result_list, i = _parse_list(lines, i, indent)
            return result_list, i

        # Key: value
        if ':' in stripped:
            colon = stripped.index(':')
            key = stripped[:colon].strip()
            rest = stripped[colon + 1:].strip()

            i += 1

            if rest:
                # Valore inline
                result[key] = _parse_scalar(rest)
            else:
                # Valore nel blocco successivo
                if i < len(lines):
                    next_indent = _get_indent(lines[i])
                    if next_indent > indent:
                        child_stripped = lines[i].strip()
                        if child_stripped.startswith('- '):
                            value, i = _parse_list(lines, i, next_indent)
                        else:
                            value, i = _parse_block(lines, i, next_indent)
                        result[key] = value
                    else:
                        result[key] = None
                else:
                    result[key] = None
        else:
            i += 1

    return result, i


def _parse_list(lines, start, base_indent):
    # type: (list, int, int) -> tuple
    result = []
    i = start

    while i < len(lines):
        line = lines[i]
        indent = _get_indent(line)
        stripped = line.strip()

        if indent < base_indent:
            break

        if stripped.startswith('- '):
            item_text = stripped[2:].strip()
            if not item_text:
                # Blocco annidato sotto la lista
                i += 1
                if i < len(lines) and _get_indent(lines[i]) > indent:
                    value, i = _parse_block(lines, i, _get_indent(lines[i]))
                    result.append(value)
                else:
                    result.append(None)
            elif ':' in item_text:
                # Dict inline come item lista
                sub_line = ' ' * (indent + 2) + item_text
                sub_result, _ = _parse_block([sub_line] + lines[i + 1:], 0, indent + 2)
                # Controlla se ci sono righe figlie
                i += 1
                while i < len(lines) and _get_indent(lines[i]) > indent:
                    extra_line = lines[i]
                    extra_key_val = extra_line.strip()
                    if ':' in extra_key_val:
                        colon = extra_key_val.index(':')
                        k = extra_key_val[:colon].strip()
                        v = extra_key_val[colon + 1:].strip()
                        sub_result[k] = _parse_scalar(v)
                    i += 1
                result.append(sub_result)
            else:
                result.append(_parse_scalar(item_text))
                i += 1
        elif stripped.startswith('-') and len(stripped) == 1:
            result.append(None)
            i += 1
        else:
            break

    return result, i


def load_config(path):
    # type: (str) -> dict
    """Carica e valida il config file YAML."""
    if not os.path.exists(path):
        raise IOError("Config file not found: {}".format(path))

    with open(path, 'r') as f:
        raw = f.read()

    if _HAS_PYYAML:
        data = _yaml.safe_load(raw)
    else:
        data = _parse_yaml_manual(raw)

    if not isinstance(data, dict):
        raise ValueError("Config file must be a YAML mapping at root level")

    # Espandi env var
    data = _walk_expand(data)

    # Validazione minima
    if "environments" not in data:
        raise ValueError("Config missing 'environments' key")
    if not data["environments"]:
        raise ValueError("Config has no environments defined")

    return data
