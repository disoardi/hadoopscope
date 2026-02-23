"""
Config loader — YAML manual parser + env var expansion + multi-source secrets.
Zero dipendenze runtime: solo Python 3.6+ stdlib.

Sorgenti di secret supportate nelle stringhe di config:

  ${ENV_VAR}              <- variabile d'ambiente
  ${file:/path/to/secret} <- contenuto di un file (Docker secrets, k8s)
  ${cmd:vault kv get ...} <- stdout di un comando (Vault, pass, aws, az, ...)

Caricamento automatico di .env:
  Se nella stessa directory del config file esiste un file .env, viene caricato
  automaticamente prima dell'espansione delle variabili.
  Il file .env NON viene mai committato (e' in .gitignore).

Esempio di config sicuro:

  ambari_pass: "${AMBARI_PASS}"
  ambari_pass: "${file:/run/secrets/ambari_pass}"
  ambari_pass: "${cmd:vault kv get -field=password secret/hadoop/ambari}"
  ambari_pass: "${cmd:pass show hadoop/ambari}"
  ambari_pass: "${cmd:aws secretsmanager get-secret-value --secret-id ambari --query SecretString --output text}"
"""

from __future__ import print_function

import os
import re
import subprocess
import sys

# Gestione import yaml: prova PyYAML come convenienza,
# ma il parser manuale garantisce zero-deps in produzione.
try:
    import yaml as _yaml
    _HAS_PYYAML = True
except ImportError:
    _HAS_PYYAML = False


# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_dotenv(dotenv_path):
    # type: (str) -> None
    """
    Carica variabili da un file .env in os.environ.
    Formato: KEY=value (una per riga, # commenti, righe vuote ignorate).
    NON sovrascrive variabili gia' presenti nell'ambiente.
    """
    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, _, val = line.partition('=')
            key = key.strip()
            val = val.strip()
            # Rimuovi eventuali quote attorno al valore
            if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or
                                   (val[0] == "'" and val[-1] == "'")):
                val = val[1:-1]
            # Non sovrascrivere variabili gia' presenti nel shell environment
            if key and key not in os.environ:
                os.environ[key] = val


# ---------------------------------------------------------------------------
# Secret resolvers
# ---------------------------------------------------------------------------

def _resolve_env(var_name):
    # type: (str) -> str
    """Legge una variabile d'ambiente. Errore descrittivo se mancante."""
    val = os.environ.get(var_name)
    if val is None:
        raise ValueError(
            "Environment variable '{}' is not set.\n"
            "Options:\n"
            "  1. export {}=yourvalue\n"
            "  2. Add {}=yourvalue to a .env file next to your config\n"
            "  3. Use ${{file:/path/to/secret}} in config\n"
            "  4. Use ${{cmd:your-vault-cmd}} in config".format(
                var_name, var_name, var_name)
        )
    return val


def _resolve_file(file_path):
    # type: (str) -> str
    """Legge il secret dal contenuto di un file (strippato)."""
    path = os.path.expanduser(file_path.strip())
    if not os.path.exists(path):
        raise ValueError(
            "Secret file not found: '{}'\n"
            "Used by config pattern ${{file:{}}}".format(path, file_path)
        )
    with open(path, 'r') as f:
        content = f.read().strip()
    if not content:
        raise ValueError("Secret file is empty: '{}'".format(path))
    return content


def _resolve_cmd(command):
    # type: (str) -> str
    """
    Esegue un comando shell e usa lo stdout come secret.
    Il comando viene passato a /bin/sh -c per supportare pipe e argomenti.
    Timeout: 30 secondi.
    """
    try:
        proc = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate(timeout=30)
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        raise ValueError(
            "Secret command timed out (30s): '{}'".format(command)
        )
    except Exception as e:
        raise ValueError(
            "Error running secret command '{}': {}".format(command, str(e))
        )

    if rc != 0:
        err = stderr.decode("utf-8", errors="replace").strip()[:300]
        raise ValueError(
            "Secret command '{}' exited with code {}:\n{}".format(command, rc, err)
        )

    secret = stdout.decode("utf-8", errors="replace").strip()
    if not secret:
        raise ValueError(
            "Secret command produced no output: '{}'".format(command)
        )
    return secret


# ---------------------------------------------------------------------------
# Expander principale
# ---------------------------------------------------------------------------

# Pattern: ${...} dove ... puo' contenere qualsiasi cosa tranne }
_SECRET_RE = re.compile(r'\$\{([^}]+)\}')


def _resolve_secret(spec):
    # type: (str) -> str
    """
    Risolve un singolo pattern ${...}:
      file:/path  -> contenuto di un file
      cmd:...     -> stdout di un comando shell
      ANYTHING    -> variabile d'ambiente
    """
    spec = spec.strip()
    if spec.startswith("file:"):
        return _resolve_file(spec[5:])
    elif spec.startswith("cmd:"):
        return _resolve_cmd(spec[4:])
    else:
        return _resolve_env(spec)


def _expand_secrets(value):
    # type: (object) -> object
    """Espande tutti i pattern ${...} nelle stringhe."""
    if not isinstance(value, str):
        return value

    def replacer(match):
        return _resolve_secret(match.group(1))

    return _SECRET_RE.sub(replacer, value)


def _walk_expand(obj):
    # type: (object) -> object
    """Ricorsivamente espande secret in tutto il config tree."""
    if isinstance(obj, dict):
        return {k: _walk_expand(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_expand(i) for i in obj]
    elif isinstance(obj, str):
        return _expand_secrets(obj)
    return obj


# ---------------------------------------------------------------------------
# Parser YAML minimale (stdlib-only)
# ---------------------------------------------------------------------------

def _parse_yaml_manual(text):
    # type: (str) -> dict
    """
    Parser YAML minimale per il subset usato da HadoopScope.
    Supporta: dict annidati, liste con -, inline list [a, b], scalar, bool, int.
    Non supporta: multi-line strings, anchors, merge keys.
    """
    lines = []
    for line in text.splitlines():
        stripped = line.rstrip()
        if not stripped or stripped.lstrip().startswith('#'):
            continue
        # Rimuovi commento inline (non dentro stringhe)
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
            lines.append(stripped)

    result, _ = _parse_block(lines, 0, 0)
    return result


def _get_indent(line):
    # type: (str) -> int
    return len(line) - len(line.lstrip(' '))


def _parse_scalar(value):
    # type: (str) -> object
    v = value.strip()
    if not v:
        return None
    if (v.startswith('"') and v.endswith('"')) or \
       (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    if v.lower() in ('true', 'yes', 'on'):
        return True
    if v.lower() in ('false', 'no', 'off'):
        return False
    if v.lower() in ('null', '~', ''):
        return None
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass
    if v.startswith('[') and v.endswith(']'):
        inner = v[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item.strip()) for item in inner.split(',')]
    return v


def _parse_block(lines, start, base_indent):
    # type: (list, int, int) -> tuple
    result = {}
    i = start

    while i < len(lines):
        line = lines[i]
        indent = _get_indent(line)
        stripped = line.strip()

        if indent < base_indent:
            break

        if stripped.startswith('- ') or stripped == '-':
            result_list, i = _parse_list(lines, i, indent)
            return result_list, i

        if ':' in stripped:
            colon = stripped.index(':')
            key = stripped[:colon].strip()
            rest = stripped[colon + 1:].strip()
            i += 1

            if rest in ('|', '|-', '|+', '>', '>-', '>+'):
                # Literal block scalar (|) or folded scalar (>)
                fold = rest[0] == '>'
                block_lines = []
                block_indent = None
                while i < len(lines):
                    nl = lines[i]
                    ni = _get_indent(nl)
                    if block_indent is None:
                        if ni > indent:
                            block_indent = ni
                        else:
                            break
                    if ni < block_indent:
                        break
                    block_lines.append(nl[block_indent:])
                    i += 1
                result[key] = (' ' if fold else '\n').join(block_lines)
            elif rest:
                result[key] = _parse_scalar(rest)
            else:
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
                i += 1
                if i < len(lines) and _get_indent(lines[i]) > indent:
                    value, i = _parse_block(lines, i, _get_indent(lines[i]))
                    result.append(value)
                else:
                    result.append(None)
            elif ':' in item_text:
                sub_line = ' ' * (indent + 2) + item_text
                sub_result, _ = _parse_block([sub_line] + lines[i + 1:], 0, indent + 2)
                i += 1
                while i < len(lines) and _get_indent(lines[i]) > indent:
                    extra_key_val = lines[i].strip()
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
        elif stripped == '-':
            result.append(None)
            i += 1
        else:
            break

    return result, i


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_config(path):
    # type: (str) -> dict
    """
    Carica e valida il config file YAML.

    Sequenza:
    1. Carica .env dalla stessa directory del config (se esiste)
    2. Parsa il YAML (PyYAML se disponibile, parser interno come fallback)
    3. Espande ${ENV_VAR}, ${file:/path}, ${cmd:...} in tutti i valori stringa
    4. Valida la struttura minima (environments non vuota)
    """
    if not os.path.exists(path):
        raise IOError("Config file not found: {}".format(path))

    # 1. Auto-carica .env vicino al config
    config_dir = os.path.dirname(os.path.abspath(path))
    dotenv_path = os.path.join(config_dir, ".env")
    _load_dotenv(dotenv_path)

    # 2. Parsa YAML
    with open(path, 'r') as f:
        raw = f.read()

    if _HAS_PYYAML:
        data = _yaml.safe_load(raw)
    else:
        data = _parse_yaml_manual(raw)

    if not isinstance(data, dict):
        raise ValueError("Config file must be a YAML mapping at root level")

    # 3. Espandi secrets
    data = _walk_expand(data)

    # 4. Validazione minima
    if "environments" not in data:
        raise ValueError("Config missing 'environments' key")
    if not data["environments"]:
        raise ValueError("Config has no environments defined")

    return data
