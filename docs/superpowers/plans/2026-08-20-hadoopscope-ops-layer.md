# HadoopScope Ops Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aggiungere a HadoopScope un nuovo perimetro "Ops" (azioni on-demand, non schedulate) con due tool concreti — `app-status` (status/metriche di un'applicazione YARN via REST) e `app-logs` (fetch log via Ansible sull'edge node) — invocabili da un nuovo verbo CLI `ops`, senza toccare il comportamento esistente del layer di monitoring.

**Architecture:** Nuovo pacchetto `ops/` parallelo a `checks/`, con `OpsToolBase` (contratto analogo a `CheckBase` ma con parametri dichiarativi e `run(**kwargs)`). Due nuove librerie condivise (`ansible_runner.py`, `kerberos_utils.py`) estratte da codice oggi duplicato/privato in `checks/hive.py` e `checks/webhdfs.py`, usate sia dal codice esistente sia dai nuovi tool Ops. `checks/yarn.py` viene generalizzato per supportare l'Application History/Timeline Server oltre alla ResourceManager REST API già in uso.

**Tech Stack:** Python 3.6+ stdlib-only (`urllib`, `subprocess`, `json`, `argparse`), Ansible (via capability esistente `ansible`/`venv_ansible`/`docker`), test runner manuale esistente (no pytest, `tests/run_all.py`).

**Riferimento spec:** `docs/superpowers/specs/2026-08-20-hadoopscope-ops-layer-design.md`

---

## Task 1: Estrarre `ansible_runner.py` da `checks/hive.py`

Refactor a comportamento invariato: sposta `_find_ansible()`, `_build_inventory()`,
`_run_playbook()`, `_extract_stdout()`, `_extract_stderr()`, `_extract_task_error()`
da metodi/funzioni private di `checks/hive.py` a funzioni pubbliche di un nuovo
modulo condiviso. `checks/hive.py` diventa un consumer sottile che delega.
I 94 test esistenti in `tests/test_checks.py` devono continuare a passare
senza modifiche (importano `_extract_stdout`, `_extract_stderr`,
`_extract_task_error` da `checks.hive` — restano alias).

**Files:**
- Create: `ansible_runner.py`
- Modify: `checks/hive.py:162-210` (rimuove `_extract_task_error`, `_extract_stdout`, `_extract_stderr`, sostituiti da alias)
- Modify: `checks/hive.py:579-689` (metodi `_build_inventory`, `_run_playbook` di `HiveCheck` diventano thin wrapper)
- Modify: `checks/hive.py:815-825` (`_find_ansible` diventa thin wrapper)
- Test: `tests/test_checks.py` (nessuna modifica — verifica di non-regressione)

- [ ] **Step 1: Creare `ansible_runner.py` con le funzioni estratte**

```python
"""Ansible helpers condivisi — inventory single-host, esecuzione playbook,
estrazione output. Usato da checks/hive.py e ops/yarn_app.py per eseguire
comandi su un edge node via Ansible (nessun inventory statico: l'inventory
è generato al volo da un singolo hostname in config)."""

from __future__ import print_function

import json
import os
import re
import subprocess
import tempfile

import debug as _debug


def find_ansible_bin():
    # type: () -> object
    """Trova ansible-playbook nel PATH o nel venv bootstrap di hadoopscope."""
    import shutil
    bin_path = shutil.which("ansible-playbook")
    if bin_path:
        return bin_path
    venv_bin = os.path.expanduser("~/.hadoopscope/venv/bin/ansible-playbook")
    if os.path.exists(venv_bin):
        return venv_bin
    return None


def build_inventory(edge_host, ssh_user, ssh_key):
    # type: (str, str, str) -> str
    """Inventory Ansible single-host generato al volo (mai un file statico)."""
    if edge_host in ("localhost", "127.0.0.1", "::1"):
        return "localhost ansible_connection=local"
    return (
        "{host} ansible_user={user} ansible_ssh_private_key_file={key}"
    ).format(
        host=edge_host,
        user=ssh_user,
        key=ssh_key or "~/.ssh/id_rsa"
    )


def extract_task_error(ansible_stdout):
    # type: (str) -> str
    """Estrae l'errore del task reale dallo stdout di Ansible (Ansible incapsula
    il risultato del task come JSON dopo 'FAILED! => ' su una sola riga)."""
    match = re.search(r"FAILED! => (\{.*\})", ansible_stdout)
    if not match:
        return ansible_stdout[-800:]
    try:
        data = json.loads(match.group(1))
        parts = []
        if data.get("msg"):
            parts.append("msg: {}".format(data["msg"]))
        if data.get("stdout"):
            parts.append("stdout: {}".format(data["stdout"][:600]))
        if data.get("stderr"):
            parts.append("stderr: {}".format(data["stderr"][:400]))
        return "\n".join(parts) if parts else ansible_stdout[-800:]
    except (ValueError, KeyError):
        return ansible_stdout[-800:]


def extract_stdout(ansible_out):
    # type: (str) -> str
    """Estrae la stringa r.stdout dall'output del task debug di Ansible."""
    m = re.search(r'"r\.stdout":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        return raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
    return ""


def extract_stderr(ansible_out):
    # type: (str) -> str
    """Estrae la stringa r.stderr dall'output del task debug di Ansible."""
    m = re.search(r'"r\.stderr":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        return raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
    return ""


def run_playbook(ansible_bin, inventory_content, shell_cmd,
                  tag="AnsibleRunner", kinit_cmd=None, timeout=60):
    # type: (str, str, str, str, object, int) -> tuple
    """Esegue shell_cmd sull'host dell'inventory via un playbook generato al volo.

    kinit_cmd: se presente, un comando 'kinit -kt <keytab> <principal>' eseguito
    sull'edge node PRIMA di shell_cmd. keytab/principal devono essere path/valori
    validi sull'edge node, non sulla macchina locale.

    Returns (rc, stdout, stderr):
      rc >= 0  : codice di uscita reale di Ansible
      rc == -1 : timeout del subprocess
      rc == -2 : eccezione inattesa (err contiene il messaggio)
    """
    script_parts = []
    if kinit_cmd:
        script_parts.append(kinit_cmd)
    for line in shell_cmd.splitlines():
        script_parts.append(line)
    shell_lines = "\n".join("        " + l for l in script_parts)

    playbook = (
        "---\n"
        "- name: {tag}\n"
        "  hosts: all\n"
        "  gather_facts: false\n"
        "  tasks:\n"
        "    - name: shell command\n"
        "      shell: |\n"
        "{shell_lines}\n"
        "      register: r\n"
        "    - debug: var=r.stdout\n"
        "    - debug: var=r.stderr\n"
    ).format(tag=tag, shell_lines=shell_lines)

    inv_path = play_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.ini', delete=False, prefix='hs_inv_'
        ) as f:
            f.write(inventory_content)
            inv_path = f.name

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yml', delete=False, prefix='hs_play_'
        ) as f:
            f.write(playbook)
            play_path = f.name

        _debug.log(tag, "playbook: {}".format(play_path), multiline=False)
        _debug.section(tag, "playbook content")
        _debug.log(tag, playbook, multiline=True)

        env = os.environ.copy()
        env["ANSIBLE_HOST_KEY_CHECKING"] = "False"

        proc = subprocess.Popen(
            [ansible_bin, "-i", inv_path, play_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        out = stdout.decode("utf-8", errors="replace")
        err = stderr.decode("utf-8", errors="replace")
        _debug.log(tag, "rc: {}".format(proc.returncode))
        _debug.section(tag, "ansible stdout")
        _debug.log(tag, out if out.strip() else "(empty)", multiline=True)
        return (proc.returncode, out, err)

    except subprocess.TimeoutExpired:
        return -1, "", "timeout after {}s".format(timeout)
    except Exception as e:
        return -2, "", str(e)
    finally:
        for p in (inv_path, play_path):
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass
```

- [ ] **Step 2: Aggiornare `checks/hive.py` per delegare al modulo condiviso**

Sostituire (righe 162-210) le tre funzioni `_extract_task_error`,
`_extract_stdout`, `_extract_stderr` con alias:

```python
import ansible_runner

_extract_task_error = ansible_runner.extract_task_error
_extract_stdout = ansible_runner.extract_stdout
_extract_stderr = ansible_runner.extract_stderr
```

Sostituire (righe 579-589, metodo `_build_inventory` di `HiveCheck`):

```python
    def _build_inventory(self, edge_host, ssh_user, ssh_key):
        # type: (str, str, str) -> str
        return ansible_runner.build_inventory(edge_host, ssh_user, ssh_key)
```

Sostituire (righe 591-689, metodo `_run_playbook` di `HiveCheck` — la
firma resta identica, la funzione delega mantenendo `tag` default
`"HiveCheck"`):

```python
    def _run_playbook(self, ansible_bin, inventory_content, beeline_cmd,
                      tag="HiveCheck", kinit_cmd=None, timeout=60):
        # type: (str, str, str, str, object, int) -> tuple
        return ansible_runner.run_playbook(
            ansible_bin, inventory_content, beeline_cmd,
            tag=tag, kinit_cmd=kinit_cmd, timeout=timeout)
```

Sostituire (righe 815-825, metodo `_find_ansible`):

```python
    def _find_ansible(self):
        # type: () -> object
        return ansible_runner.find_ansible_bin()
```

- [ ] **Step 3: Eseguire la suite di test esistente per verificare zero regressioni**

Run: `python3 tests/test_checks.py`
Expected: `84/84 passed` (stesso numero di prima della modifica)

- [ ] **Step 4: Commit**

```bash
git add ansible_runner.py checks/hive.py
git commit -m "$(cat <<'EOF'
refactor: estrae ansible_runner.py da checks/hive.py

Comportamento invariato — HiveCheck delega al modulo condiviso.
Prepara il terreno per riuso da parte del nuovo layer Ops (app-logs).
EOF
)"
```

---

## Task 2: Estrarre `kerberos_utils.py` da `checks/webhdfs.py` e consolidare la duplicazione con `_find_ansible_bin`/`_build_webhdfs_inventory`

`checks/webhdfs.py` ha una seconda implementazione quasi identica di
"trova ansible-playbook"/"costruisci inventory single-host"
(`_find_ansible_bin`, `_build_webhdfs_inventory`, righe 244-264) — duplicato
di quanto appena estratto in `ansible_runner.py` nel Task 1. La consolidiamo
qui, insieme all'estrazione di `_kinit()`.

**Files:**
- Create: `kerberos_utils.py`
- Modify: `checks/webhdfs.py:65-86` (`_kinit` diventa alias)
- Modify: `checks/webhdfs.py:244-264` (`_find_ansible_bin`/`_build_webhdfs_inventory` diventano alias di `ansible_runner`)
- Test: `tests/test_checks.py` (nessuna modifica — verifica di non-regressione)

- [ ] **Step 1: Creare `kerberos_utils.py`**

```python
"""Helper Kerberos condivisi — kinit via subprocess (stdlib-only).
Usato da checks/webhdfs.py (kinit locale) e ops/yarn_app.py (AppStatusTool,
kinit locale prima delle chiamate REST kerberizzate a YARN RM/History Server)."""

from __future__ import print_function

import subprocess


def kinit(keytab, principal, timeout=30):
    # type: (str, str, int) -> None
    """Ottiene un ticket Kerberos dal keytab. Raises IOError se kinit fallisce."""
    if not keytab:
        raise IOError("kerberos.keytab non configurato")
    if not principal:
        raise IOError("kerberos.principal non configurato")
    try:
        subprocess.check_call(
            ["kinit", "-kt", keytab, principal],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout
        )
    except subprocess.CalledProcessError:
        raise IOError(
            "kinit fallito per principal='{}' keytab='{}'. "
            "Verifica che il keytab sia valido e il KDC raggiungibile.".format(
                principal, keytab)
        )
    except OSError:
        raise IOError(
            "kinit non trovato nel PATH — installa krb5-user (Debian) "
            "o krb5-workstation (RHEL)")
```

- [ ] **Step 2: Aggiornare `checks/webhdfs.py` — alias per `_kinit`**

Sostituire (righe 65-86):

```python
import kerberos_utils

_kinit = kerberos_utils.kinit
```

- [ ] **Step 3: Consolidare `_find_ansible_bin`/`_build_webhdfs_inventory` su `ansible_runner`**

Sostituire (righe 244-264):

```python
import ansible_runner

_find_ansible_bin = ansible_runner.find_ansible_bin


def _build_webhdfs_inventory(ansible_cfg):
    # type: (dict) -> str
    edge_host = ansible_cfg.get("edge_host", "")
    ssh_user  = ansible_cfg.get("ssh_user", "root")
    ssh_key   = ansible_cfg.get("ssh_key", "~/.ssh/id_rsa")
    return ansible_runner.build_inventory(edge_host, ssh_user, ssh_key)
```

Nota: `_find_ansible_bin()` in `ansible_runner` restituisce `None` quando
non trovato, mentre l'originale in `webhdfs.py` restituiva `""`. Verificare
il chiamante (riga 276-278 originale, `if not ansible_bin:`) — `None` e
`""` sono entrambi falsy, il comportamento resta identico.

- [ ] **Step 4: Eseguire la suite di test esistente**

Run: `python3 tests/test_checks.py`
Expected: `84/84 passed`

- [ ] **Step 5: Commit**

```bash
git add kerberos_utils.py checks/webhdfs.py
git commit -m "$(cat <<'EOF'
refactor: estrae kerberos_utils.py, consolida duplicazione ansible in webhdfs.py

_kinit() spostato in libreria condivisa (servirà ad AppStatusTool).
_find_ansible_bin/_build_webhdfs_inventory in webhdfs.py erano una
seconda implementazione quasi identica di quanto già estratto in
ansible_runner.py nel task precedente — consolidati.
EOF
)"
```

---

## Task 3: Helper condiviso `check_requires()` in `checks/base.py`

Estrae la logica OR-of-AND-list di `CheckBase.can_run()` in una funzione
di modulo, riusabile da `OpsToolBase` senza duplicare la logica né
introdurre ereditarietà multipla.

**Files:**
- Modify: `checks/base.py:44-55`
- Test: `tests/test_checks.py` (nessuna modifica — verifica di non-regressione)

- [ ] **Step 1: Aggiungere `check_requires()` e farlo usare da `CheckBase.can_run()`**

In `checks/base.py`, aggiungere prima della classe `CheckBase`:

```python
def check_requires(requires, caps):
    # type: (list, dict) -> bool
    """OR logico tra le require-list, AND logico dentro ogni lista.

    requires = [["ansible"], ["docker"]]  →  ansible OR docker
    requires = [["ansible", "kinit"]]      →  ansible AND kinit
    requires = []                           →  sempre True
    """
    if not requires:
        return True
    for req_list in requires:
        if all(caps.get(r, False) for r in req_list):
            return True
    return False
```

Sostituire il corpo di `CheckBase.can_run()` (righe 44-55):

```python
    def can_run(self):
        # type: () -> bool
        return check_requires(self.requires, self.caps)
```

- [ ] **Step 2: Eseguire la suite di test esistente**

Run: `python3 tests/test_checks.py`
Expected: `84/84 passed` (i test `test_base_can_run_*` verificano già questo comportamento)

- [ ] **Step 3: Commit**

```bash
git add checks/base.py
git commit -m "refactor: estrae check_requires() condiviso da CheckBase.can_run()"
```

---

## Task 4: `ops/base.py` — `OpsParam` e `OpsToolBase`

**Files:**
- Create: `ops/__init__.py` (vuoto)
- Create: `ops/base.py`
- Test: `tests/test_ops.py` (nuovo file)

- [ ] **Step 1: Scrivere il test per `OpsParam`/`OpsToolBase`**

Creare `tests/test_ops.py`:

```python
"""Test suite per il layer Ops di HadoopScope."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.base import CheckResult
from ops.base import OpsParam, OpsToolBase


def test_ops_param_defaults():
    p = OpsParam("app_id", help="YARN application id")
    assert p.name == "app_id"
    assert p.help == "YARN application id"
    assert p.required is True
    assert p.type is str


def test_ops_tool_base_can_run_no_requires():
    class _NoRequires(OpsToolBase):
        name = "noop"
        requires = []
        def run(self, **kwargs):
            return CheckResult("noop", CheckResult.OK, "ok")
    assert _NoRequires({}, {}).can_run() is True


def test_ops_tool_base_can_run_missing_cap():
    class _NeedsAnsible(OpsToolBase):
        name = "needs-ansible"
        requires = [["ansible"]]
        def run(self, **kwargs):
            return CheckResult("x", CheckResult.OK, "ok")
    assert _NeedsAnsible({}, {}).can_run() is False
    assert _NeedsAnsible({}, {"ansible": True}).can_run() is True


def test_ops_tool_base_run_raises_not_implemented():
    class Bad(OpsToolBase):
        name = "bad"
    try:
        Bad({}, {}).run()
        assert False, "should raise"
    except NotImplementedError:
        pass


def test_ops_tool_base_is_write_default_false():
    class _Tool(OpsToolBase):
        name = "x"
        def run(self, **kwargs):
            return CheckResult("x", CheckResult.OK, "ok")
    assert _Tool({}, {}).is_write is False


if __name__ == "__main__":
    tests = [
        test_ops_param_defaults,
        test_ops_tool_base_can_run_no_requires,
        test_ops_tool_base_can_run_missing_cap,
        test_ops_tool_base_run_raises_not_implemented,
        test_ops_tool_base_is_write_default_false,
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

- [ ] **Step 2: Eseguire il test per verificare che fallisca (modulo non esiste ancora)**

Run: `python3 tests/test_ops.py`
Expected: `ModuleNotFoundError: No module named 'ops'`

- [ ] **Step 3: Creare `ops/__init__.py` (vuoto, stesso stile di `checks/__init__.py`)**

```bash
touch ops/__init__.py
```

- [ ] **Step 4: Creare `ops/base.py`**

```python
"""OpsToolBase — base class per i tool Ops (azioni on-demand, non schedulate).

Simmetrico a CheckBase (checks/base.py) ma per azioni che richiedono input
dall'utente e vengono lanciate on-demand da CLI o TUI, non da cron.
"""

from __future__ import print_function

from checks.base import check_requires


class OpsParam(object):
    """Dichiarazione di un parametro richiesto da un tool Ops.

    Un solo posto dichiara nome/help/tipo/obbligatorietà — sia il CLI
    (argparse) sia la futura TUI generano la loro interfaccia di input
    a partire da questa lista, senza duplicare la dichiarazione.
    """

    def __init__(self, name, help, required=True, type=str):
        # type: (str, str, bool, type) -> None
        self.name = name
        self.help = help
        self.required = required
        self.type = type


class OpsToolBase(object):
    """
    Base class per i tool Ops.

    Sottoclassi devono:
    - Definire `name` (id univoco, usato da CLI/registry/TUI)
    - Definire `params` come lista di OpsParam
    - Definire `requires` come lista di liste (stesso OR-of-AND-list di CheckBase)
    - Definire `is_write = True` se il tool modifica stato esterno al cluster
      (nessun tool oggi lo fa — riservato per usi futuri, es. un tool che
      aggiorna un inventory e fa push su un repo esterno)
    - Implementare `run(**kwargs)` che restituisce un CheckResult
    """
    name = ""
    description = ""
    params = []      # type: list
    requires = []     # type: list
    is_write = False

    def __init__(self, config, caps):
        # type: (dict, dict) -> None
        self.config = config
        self.caps = caps

    def can_run(self):
        # type: () -> bool
        return check_requires(self.requires, self.caps)

    def run(self, **kwargs):
        # type: (...) -> object
        raise NotImplementedError(
            "Implement run() in {}".format(self.__class__.__name__)
        )
```

- [ ] **Step 5: Eseguire il test per verificare che passi**

Run: `python3 tests/test_ops.py`
Expected: `5/5 passed`

- [ ] **Step 6: Commit**

```bash
git add ops/__init__.py ops/base.py tests/test_ops.py
git commit -m "feat: OpsToolBase + OpsParam — contratto base per il layer Ops"
```

---

## Task 5: Generalizzare `checks/yarn.py` — REST path completo + resolver multi-host per gli endpoint History Server

Estende `_yarn_get` per accettare un URL completo (serve per l'Application
History/Timeline Server, che ha un prefisso diverso da
`/ws/v1/cluster/`), e unifica in `_resolve_url()` la logica già esistente
per `rm_url`/`rm_urls`, riusata per i nuovi endpoint history.

**Files:**
- Modify: `checks/yarn.py:79-153` (`_rm_url`, `_yarn_get`)
- Test: `tests/test_checks.py` (nessuna modifica — verifica di non-regressione)
- Test: `tests/test_ops.py` (nuovi test per `_resolve_url`)

- [ ] **Step 1: Scrivere il test per `_resolve_url`**

Aggiungere a `tests/test_ops.py` (import in cima al file):

```python
from checks.yarn import _resolve_url
```

E le funzioni di test (prima del blocco `if __name__`):

```python
def test_resolve_url_singular_key():
    cfg = {"rm_url": "http://rm1:8088/"}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"
    assert is_auto is False


def test_resolve_url_plural_key_takes_first():
    cfg = {"rm_urls": ["http://rm1:8088/", "http://rm2:8088/"]}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"


def test_resolve_url_plural_wins_over_singular():
    cfg = {"rm_url": "http://ignored:8088", "rm_urls": ["http://rm1:8088"]}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"


def test_resolve_url_missing_returns_none():
    url, is_auto = _resolve_url({}, "history_url", "history_urls")
    assert url is None
    assert is_auto is True
```

Aggiungere le 4 funzioni alla lista `tests` nel blocco `if __name__`.

- [ ] **Step 2: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `ImportError: cannot import name '_resolve_url'`

- [ ] **Step 3: Implementare `_resolve_url` e rifattorizzare `_rm_url` per usarlo**

In `checks/yarn.py`, sostituire la funzione `_rm_url` (righe 79-103) con:

```python
def _resolve_url(cfg_block, singular_key, plural_key):
    # type: (dict, str, str) -> tuple
    """Risolve un endpoint REST da un blocco di config, dando priorità
    alla forma plurale (lista, primo elemento — il failover HA tra le
    repliche della lista è delegato al redirect 307 seguito da curl,
    non a un retry esplicito su ogni elemento).

    Restituisce (url_or_None, is_auto). is_auto è sempre False qui —
    il flag è solo per compatibilità con chi (es. _rm_url) aggiunge un
    fallback auto-detect sopra questa funzione.
    """
    urls = cfg_block.get(plural_key, [])
    if urls:
        return urls[0].rstrip("/"), False
    if cfg_block.get(singular_key):
        return cfg_block[singular_key].rstrip("/"), False
    return None, True


def _rm_url(config):
    # type: (dict) -> tuple
    """
    Restituisce (url_or_None, is_auto) del YARN Resource Manager.
    Priorità: config[yarn][rm_urls][0] > config[yarn][rm_url] > auto-detect da ambari_url (HDP).
    Con rm_urls la lista viene provata in ordine; il 307 dal standby viene seguito via -L.
    Restituisce (None, True) se non configurabile — il check torna SKIPPED.
    """
    yarn_cfg = config.get("yarn", {})
    url, is_auto = _resolve_url(yarn_cfg, "rm_url", "rm_urls")
    if url:
        return url, False

    # Fallback HDP only: costruiamo dall'ambari_url sostituendo host e porta.
    # Per CDP (cm_url, no ambari_url) non possiamo auto-rilevare il RM.
    ambari_url = config.get("ambari_url")
    if not ambari_url:
        return None, True

    try:
        if "://" in ambari_url:
            _, rest = ambari_url.split("://", 1)
            host = rest.split("/")[0].split(":")[0]
        else:
            host = ambari_url.split("/")[0].split(":")[0]
        return "http://{}:{}".format(host, DEFAULT_RM_PORT), True
    except Exception:
        return None, True
```

- [ ] **Step 4: Generalizzare `_yarn_get` per accettare un URL completo opzionale**

Sostituire la firma e la prima riga del corpo di `_yarn_get` (righe 111-113):

```python
def _yarn_get(base_url, path, timeout=DEFAULT_TIMEOUT, no_proxy=False,
              kerberos=False, full_path=False):
    # type: (str, str, int, bool, bool, bool) -> dict
    """GET REST verso YARN. Se full_path=False (default, comportamento
    esistente), il path è relativo a /ws/v1/cluster/. Se full_path=True,
    'path' è già l'URL completo (usato per l'Application History/Timeline
    Server, che ha un prefisso diverso)."""
    url = path if full_path else "{}/ws/v1/cluster/{}".format(base_url, path.lstrip("/"))
```

Il resto del corpo di `_yarn_get` resta identico (usa `url` come già fa).

- [ ] **Step 5: Eseguire entrambe le suite di test**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `9/9 passed`

- [ ] **Step 6: Commit**

```bash
git add checks/yarn.py tests/test_ops.py
git commit -m "refactor: checks/yarn.py — _resolve_url condiviso, _yarn_get accetta URL completo"
```

---

## Task 6: `AppStatusTool` — path RM base (senza fallback History Server, senza Kerberos, senza counters)

Prima iterazione minima e testabile: query alla ResourceManager REST API,
normalizzazione campi, mapping status. Le iterazioni successive (Task 7-9)
aggiungono fallback History Server, Kerberos e counters senza toccare
questa base.

**Files:**
- Create: `ops/yarn_app.py`
- Create: `tests/fixtures/yarn_app_running.json`
- Create: `tests/fixtures/yarn_app_succeeded.json`
- Create: `tests/fixtures/yarn_app_failed.json`
- Test: `tests/test_ops.py`

- [ ] **Step 1: Creare le fixture JSON (shape reale della YARN RM REST API `/ws/v1/cluster/apps/{id}`)**

`tests/fixtures/yarn_app_running.json`:

```json
{
  "app": {
    "id": "application_1699999999999_0001",
    "user": "hdfs",
    "name": "example-job",
    "queue": "default",
    "state": "RUNNING",
    "finalStatus": "UNDEFINED",
    "progress": 42.5,
    "applicationType": "SPARK",
    "diagnostics": "",
    "startedTime": 1699999999000,
    "finishedTime": 0,
    "elapsedTime": 125000,
    "allocatedMB": 4096,
    "allocatedVCores": 2,
    "runningContainers": 3
  }
}
```

`tests/fixtures/yarn_app_succeeded.json`:

```json
{
  "app": {
    "id": "application_1699999999999_0002",
    "user": "hdfs",
    "name": "example-job-2",
    "queue": "default",
    "state": "FINISHED",
    "finalStatus": "SUCCEEDED",
    "progress": 100.0,
    "applicationType": "MAPREDUCE",
    "diagnostics": "",
    "startedTime": 1699999999000,
    "finishedTime": 1699999999999,
    "elapsedTime": 999,
    "allocatedMB": 0,
    "allocatedVCores": 0,
    "runningContainers": 0
  }
}
```

`tests/fixtures/yarn_app_failed.json`:

```json
{
  "app": {
    "id": "application_1699999999999_0003",
    "user": "hdfs",
    "name": "example-job-3",
    "queue": "default",
    "state": "FAILED",
    "finalStatus": "FAILED",
    "progress": 100.0,
    "applicationType": "SPARK",
    "diagnostics": "Application failed 2 times due to AM Container exit code: 1",
    "startedTime": 1699999999000,
    "finishedTime": 1699999999500,
    "elapsedTime": 500,
    "allocatedMB": 0,
    "allocatedVCores": 0,
    "runningContainers": 0
  }
}
```

- [ ] **Step 2: Scrivere i test per `AppStatusTool` (path RM base)**

Aggiungere a `tests/test_ops.py` (import in cima):

```python
from ops.yarn_app import AppStatusTool
from tests.test_checks import start_mock_server, load_fixture
```

Nota: `start_mock_server`/`load_fixture` sono già definiti in
`tests/test_checks.py` — riusati qui invece di duplicarli (stesso
principio "no duplicazione" del resto del progetto).

```python
def test_app_status_running():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0001")
        assert result.status == CheckResult.OK
        assert "RUNNING" in result.message
        assert result.details["state"] == "RUNNING"
        assert result.details["allocatedMB"] == 4096
    finally:
        server.shutdown()


def test_app_status_succeeded():
    fixture = load_fixture("yarn_app_succeeded.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
    finally:
        server.shutdown()


def test_app_status_failed():
    fixture = load_fixture("yarn_app_failed.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0003": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0003")
        assert result.status == CheckResult.CRITICAL
        assert "AM Container exit code" in result.message
    finally:
        server.shutdown()


def test_app_status_no_rm_url_configured():
    tool = AppStatusTool(config={}, caps={})
    result = tool.run(app_id="application_x")
    assert result.status == CheckResult.SKIPPED
```

Aggiungere le 4 funzioni alla lista `tests`.

- [ ] **Step 3: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `ModuleNotFoundError: No module named 'ops.yarn_app'`

- [ ] **Step 4: Implementare `ops/yarn_app.py` — `AppStatusTool` (path RM base)**

```python
"""Tool Ops per applicazioni YARN — status/metriche (AppStatusTool) e
fetch log (AppLogsTool, vedi Task 10)."""

from __future__ import print_function

from checks.base import CheckResult
from checks.yarn import _rm_url, _resolve_url, _yarn_get
from ops.base import OpsParam, OpsToolBase

_TERMINAL_STATUS_MAP = {
    "SUCCEEDED": CheckResult.OK,
    "KILLED":    CheckResult.WARNING,
    "FAILED":    CheckResult.CRITICAL,
}


def _normalize_app_fields(app):
    # type: (dict) -> dict
    return {
        "state":             app.get("state", "UNKNOWN"),
        "finalStatus":       app.get("finalStatus", "UNDEFINED"),
        "progress":          app.get("progress", 0),
        "applicationType":   app.get("applicationType", ""),
        "diagnostics":       app.get("diagnostics", ""),
        "allocatedMB":       app.get("allocatedMB", 0),
        "allocatedVCores":   app.get("allocatedVCores", 0),
        "runningContainers": app.get("runningContainers", 0),
        "elapsedTime":       app.get("elapsedTime", 0),
    }


def _status_from_fields(fields):
    # type: (dict) -> str
    state = fields["state"]
    if state == "RUNNING":
        return CheckResult.OK
    if state == "FINISHED":
        return _TERMINAL_STATUS_MAP.get(fields["finalStatus"], CheckResult.UNKNOWN)
    if state in ("FAILED", "KILLED"):
        return _TERMINAL_STATUS_MAP.get(state, CheckResult.UNKNOWN)
    return CheckResult.UNKNOWN


def _message_from_fields(app_id, fields):
    # type: (str, dict) -> str
    msg = "{} — state={} finalStatus={} progress={:.1f}%".format(
        app_id, fields["state"], fields["finalStatus"], fields["progress"])
    msg += "\nresources: {}MB / {} vcores / {} running containers".format(
        fields["allocatedMB"], fields["allocatedVCores"], fields["runningContainers"])
    if fields["diagnostics"]:
        msg += "\ndiagnostics: {}".format(fields["diagnostics"][:300])
    return msg


class AppStatusTool(OpsToolBase):
    """Status e metriche di un'applicazione YARN via ResourceManager REST API."""

    name = "app-status"
    description = "Status/metriche di un'applicazione YARN dato l'application id"
    params = [OpsParam("app_id", help="YARN application ID, es. application_1699999999_0001")]
    requires = []  # solo REST, sempre disponibile

    def run(self, app_id):
        # type: (str) -> CheckResult
        base, is_auto = _rm_url(self.config)
        if base is None:
            return CheckResult(
                name=self.name,
                status=CheckResult.SKIPPED,
                message="yarn.rm_url not configured — add yarn.rm_url to config"
            )

        no_proxy = self.config.get("no_proxy", False)
        use_krb  = self.config.get("kerberos", {}).get("enabled", False)

        try:
            data = _yarn_get(base, "apps/{}".format(app_id),
                             no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} — app non trovata su RM ({})".format(app_id, str(e))
            )

        app = data.get("app")
        if not app:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} non trovata su RM (nessun history_url configurato "
                        "per il fallback)".format(app_id)
            )

        fields = _normalize_app_fields(app)
        return CheckResult(
            name=self.name,
            status=_status_from_fields(fields),
            message=_message_from_fields(app_id, fields),
            details=fields
        )
```

- [ ] **Step 5: Eseguire il test per verificare che passi**

Run: `python3 tests/test_ops.py`
Expected: `13/13 passed`

- [ ] **Step 6: Commit**

```bash
git add ops/yarn_app.py tests/fixtures/yarn_app_*.json tests/test_ops.py
git commit -m "feat: AppStatusTool — status/metriche applicazione YARN via RM REST"
```

---

## Task 7: `AppStatusTool` — fallback Application History Server (HDP) / Timeline Service v2 (CDP)

**Files:**
- Modify: `ops/yarn_app.py`
- Create: `tests/fixtures/yarn_app_history_hdp.json`
- Create: `tests/fixtures/yarn_timeline_v2_cdp.json`
- Test: `tests/test_ops.py`

- [ ] **Step 1: Creare le fixture per i due formati History Server**

`tests/fixtures/yarn_app_history_hdp.json` (Application History Server v1,
`GET /ws/v1/applicationhistory/apps/{id}` — shape secondo la
documentazione Apache Hadoop; **da confermare contro un cluster HDP/CDP
reale nel Task 13**, vedi rischio noto in spec):

```json
{
  "app": {
    "appId": "application_1699999999999_9001",
    "user": "hdfs",
    "name": "old-job",
    "queue": "default",
    "type": "MAPREDUCE",
    "appState": "FINISHED",
    "finalAppStatus": "SUCCEEDED",
    "progress": 100.0,
    "diagnosticsInfo": "",
    "elapsedTime": 5000
  }
}
```

`tests/fixtures/yarn_timeline_v2_cdp.json` (Timeline Service v2, shape
diversa e più annidata; **da confermare contro CDP reale nel Task 13**):

```json
{
  "id": "application_1699999999999_9002",
  "type": "YARN_APPLICATION",
  "info": {
    "YARN_APPLICATION_STATE": "FINISHED",
    "YARN_APPLICATION_FINAL_STATUS": "SUCCEEDED",
    "YARN_APPLICATION_USER": "hdfs",
    "YARN_APPLICATION_QUEUE": "default",
    "YARN_APPLICATION_APPLICATION_TYPE": "SPARK",
    "YARN_APPLICATION_PROGRESS": 100.0,
    "YARN_APPLICATION_DIAGNOSTICS_INFO": "",
    "YARN_APPLICATION_ELAPSED_TIME": 8000
  }
}
```

- [ ] **Step 2: Scrivere i test per il fallback**

Aggiungere a `tests/test_ops.py`:

```python
def test_app_status_fallback_to_history_hdp():
    rm_fixture = {"error": "not found"}  # RM non ha più l'app (404 dal mock)
    history_fixture = load_fixture("yarn_app_history_hdp.json")
    server, port = start_mock_server({
        "/ws/v1/applicationhistory/apps/application_1699999999999_9001": history_fixture,
        # nessuna route per /ws/v1/cluster/apps/... -> il mock risponde 404
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {
            "type": "hdp",
            "yarn": {"rm_url": base, "history_url": base},
        }
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_9001")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
    finally:
        server.shutdown()


def test_app_status_fallback_to_timeline_v2_cdp():
    timeline_fixture = load_fixture("yarn_timeline_v2_cdp.json")
    server, port = start_mock_server({
        "/ws/v2/timeline/apps/application_1699999999999_9002": timeline_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {
            "type": "cdp",
            "yarn": {"rm_url": base, "history_url": base},
        }
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_9002")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
        assert result.details["applicationType"] == "SPARK"
    finally:
        server.shutdown()


def test_app_status_not_found_anywhere():
    server, port = start_mock_server({})  # nessuna route -> tutto 404
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"type": "hdp", "yarn": {"rm_url": base, "history_url": base}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_does_not_exist")
        assert result.status == CheckResult.UNKNOWN
    finally:
        server.shutdown()
```

Aggiungere le 3 funzioni alla lista `tests`.

- [ ] **Step 3: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `FAIL` sui 3 nuovi test — l'app non trovata su RM restituisce
UNKNOWN invece di tentare l'History Server (comportamento attuale del
Task 6, nessun fallback implementato)

- [ ] **Step 4: Implementare il fallback in `ops/yarn_app.py`**

Aggiungere dopo `_normalize_app_fields` in `ops/yarn_app.py`:

```python
def _normalize_history_hdp(app):
    # type: (dict) -> dict
    """Normalizza la risposta dell'Application History Server v1 (HDP) —
    shape con chiavi diverse dalla RM REST API (appState invece di state,
    ecc.)."""
    return {
        "state":             app.get("appState", "UNKNOWN"),
        "finalStatus":       app.get("finalAppStatus", "UNDEFINED"),
        "progress":          app.get("progress", 0),
        "applicationType":   app.get("type", ""),
        "diagnostics":       app.get("diagnosticsInfo", ""),
        "allocatedMB":       0,
        "allocatedVCores":   0,
        "runningContainers": 0,
        "elapsedTime":       app.get("elapsedTime", 0),
    }


def _normalize_timeline_v2(data):
    # type: (dict) -> dict
    """Normalizza la risposta di Timeline Service v2 (CDP) — shape annidata
    sotto 'info' con chiavi YARN_APPLICATION_*."""
    info = data.get("info", {})
    return {
        "state":             info.get("YARN_APPLICATION_STATE", "UNKNOWN"),
        "finalStatus":       info.get("YARN_APPLICATION_FINAL_STATUS", "UNDEFINED"),
        "progress":          info.get("YARN_APPLICATION_PROGRESS", 0),
        "applicationType":   info.get("YARN_APPLICATION_APPLICATION_TYPE", ""),
        "diagnostics":       info.get("YARN_APPLICATION_DIAGNOSTICS_INFO", ""),
        "allocatedMB":       0,
        "allocatedVCores":   0,
        "runningContainers": 0,
        "elapsedTime":       info.get("YARN_APPLICATION_ELAPSED_TIME", 0),
    }


def _query_history_server(config, app_id, no_proxy, use_krb):
    # type: (dict, str, bool, bool) -> object
    """Interroga l'Application History Server (HDP) o Timeline Service v2
    (CDP), a seconda di config['type']. Restituisce dict di campi
    normalizzati, o None se non configurato/non trovato."""
    yarn_cfg = config.get("yarn", {})
    history_url, _ = _resolve_url(yarn_cfg, "history_url", "history_urls")
    if not history_url:
        return None

    env_type = config.get("type", "hdp")
    if env_type == "cdp":
        path = "{}/ws/v2/timeline/apps/{}".format(history_url, app_id)
        try:
            data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                             full_path=True)
        except IOError:
            return None
        if not data:
            return None
        return _normalize_timeline_v2(data)
    else:
        path = "{}/ws/v1/applicationhistory/apps/{}".format(history_url, app_id)
        try:
            data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                             full_path=True)
        except IOError:
            return None
        app = data.get("app")
        if not app:
            return None
        return _normalize_history_hdp(app)
```

Modificare `AppStatusTool.run()` per tentare il fallback quando la RM
non trova l'app. Il blocco introdotto nel Task 6 (dalla chiamata
`_yarn_get` iniziale fino a `fields = _normalize_app_fields(app)`
incluso) va sostituito per intero — anche la chiamata RM deve gestire il
404 come "prova il fallback" invece di ritornare subito UNKNOWN.
Sostituire il blocco esistente:

```python
        try:
            data = _yarn_get(base, "apps/{}".format(app_id),
                             no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} — app non trovata su RM ({})".format(app_id, str(e))
            )

        app = data.get("app")
        if not app:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} non trovata su RM (nessun history_url configurato "
                        "per il fallback)".format(app_id)
            )

        fields = _normalize_app_fields(app)
```

con:

```python
        try:
            data = _yarn_get(base, "apps/{}".format(app_id),
                             no_proxy=no_proxy, kerberos=use_krb)
            app = data.get("app")
        except IOError:
            app = None

        if app:
            fields = _normalize_app_fields(app)
        else:
            fields = _query_history_server(self.config, app_id, no_proxy, use_krb)
            if fields is None:
                return CheckResult(
                    name=self.name,
                    status=CheckResult.UNKNOWN,
                    message="{} non trovata né su RM né su History Server "
                            "(id errato o applicazione più vecchia della "
                            "retention configurata)".format(app_id)
                )
```

- [ ] **Step 5: Eseguire tutti i test**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `16/16 passed`

- [ ] **Step 6: Commit**

```bash
git add ops/yarn_app.py tests/fixtures/yarn_app_history_hdp.json tests/fixtures/yarn_timeline_v2_cdp.json tests/test_ops.py
git commit -m "$(cat <<'EOF'
feat: AppStatusTool — fallback su Application History Server / Timeline v2

Se l'app non è più in memoria RM, tenta yarn.history_url (shape diversa
per HDP vs CDP, branch su config type). Shape delle fixture da
confermare contro cluster reale — vedi Task 13 (validazione DXC dev).
EOF
)"
```

---

## Task 8: `AppStatusTool` — Kerberos (kinit locale)

**Files:**
- Modify: `ops/yarn_app.py`
- Test: `tests/test_ops.py`

- [ ] **Step 1: Scrivere il test per il kinit condizionale**

Aggiungere a `tests/test_ops.py` (import in cima):

```python
try:
    from unittest import mock
except ImportError:
    import mock
```

```python
def test_app_status_kinit_called_when_kerberos_enabled():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {
            "yarn": {
                "rm_url": "http://127.0.0.1:{}".format(port),
                "kerberos": {"enabled": True, "keytab": "/x.keytab", "principal": "svc@REALM"},
            },
        }
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            result = tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_called_once_with("/x.keytab", "svc@REALM")
        assert result.status == CheckResult.OK
    finally:
        server.shutdown()


def test_app_status_kinit_not_called_when_kerberos_disabled():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_not_called()
    finally:
        server.shutdown()


def test_app_status_kinit_falls_back_to_top_level_kerberos():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {
            "kerberos": {"enabled": True, "keytab": "/top.keytab", "principal": "top@REALM"},
            "yarn": {"rm_url": "http://127.0.0.1:{}".format(port)},
        }
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_called_once_with("/top.keytab", "top@REALM")
    finally:
        server.shutdown()
```

Aggiungere le 3 funzioni alla lista `tests`.

- [ ] **Step 2: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `AttributeError: module 'ops.yarn_app' has no attribute 'kerberos_utils'`

- [ ] **Step 3: Implementare il kinit condizionale in `AppStatusTool.run()`**

In `ops/yarn_app.py`, aggiungere l'import in cima:

```python
import kerberos_utils
```

Modificare `AppStatusTool.run()` — sostituire le righe che leggono
`use_krb` e aggiungere il kinit prima della prima chiamata REST:

```python
        no_proxy = self.config.get("no_proxy", False)
        yarn_krb = self.config.get("yarn", {}).get("kerberos", {})
        top_krb  = self.config.get("kerberos", {})
        krb_cfg  = yarn_krb if yarn_krb.get("enabled") else top_krb
        use_krb  = krb_cfg.get("enabled", False)

        if use_krb:
            try:
                kerberos_utils.kinit(krb_cfg.get("keytab"), krb_cfg.get("principal"))
            except IOError as e:
                return CheckResult(
                    name=self.name,
                    status=CheckResult.UNKNOWN,
                    message="kinit fallito: {}".format(str(e))
                )
```

Questo blocco sostituisce la riga `use_krb = self.config.get("kerberos", {}).get("enabled", False)`
introdotta nel Task 6 — va posizionato subito dopo il check
`if base is None: ...` e prima della chiamata `_yarn_get` iniziale.

- [ ] **Step 4: Eseguire tutti i test**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `19/19 passed`

- [ ] **Step 5: Commit**

```bash
git add ops/yarn_app.py tests/test_ops.py
git commit -m "feat: AppStatusTool — kinit locale esplicito se yarn.kerberos/kerberos.enabled"
```

---

## Task 9: `AppStatusTool` — counters best-effort (Spark/MapReduce/Tez)

Solo per app in stato terminale, solo se l'endpoint history del tipo
applicativo è configurato. Se manca o fallisce, non deve mai far fallire
il resto del risultato — nota "counters non disponibili".

**Files:**
- Modify: `ops/yarn_app.py`
- Create: `tests/fixtures/spark_history_counters.json`
- Test: `tests/test_ops.py`

- [ ] **Step 1: Creare la fixture per Spark History Server**

`tests/fixtures/spark_history_counters.json` (shape reale di
`GET /api/v1/applications/{id}` sullo Spark History Server):

```json
{
  "id": "application_1699999999999_0002",
  "name": "example-job-2",
  "attempts": [
    {
      "attemptId": "1",
      "completed": true,
      "sparkUser": "hdfs",
      "duration": 15000
    }
  ]
}
```

- [ ] **Step 2: Scrivere il test per i counters best-effort**

Aggiungere a `tests/test_ops.py`:

```python
def test_app_status_counters_best_effort_when_configured():
    app_fixture = load_fixture("yarn_app_succeeded.json")
    counters_fixture = load_fixture("spark_history_counters.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": app_fixture,
        "/api/v1/applications/application_1699999999999_0002": counters_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"yarn": {"rm_url": base, "spark_history_url": base}}
        # forza applicationType Spark per il test dei counters
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert "counters" in result.details
        assert result.details["counters"]["duration"] == 15000
    finally:
        server.shutdown()


def test_app_status_counters_not_available_when_not_configured():
    app_fixture = load_fixture("yarn_app_succeeded.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": app_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"yarn": {"rm_url": base}}  # nessun spark_history_url
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert "counters non disponibili" in result.message
        assert "counters" not in result.details
    finally:
        server.shutdown()
```

Nota: la fixture `yarn_app_succeeded.json` (creata nel Task 6) ha
`applicationType: "MAPREDUCE"`, non Spark — usare il MapReduce History
Server (`mr_history_url`) nel primo test invece di Spark per coerenza,
oppure creare una nuova fixture `yarn_app_succeeded_spark.json` con
`applicationType: "SPARK"`. Scelta più semplice: creare la nuova fixture
dedicata invece di forzare il tipo — aggiungere anche questo file al
Step 1:

`tests/fixtures/yarn_app_succeeded_spark.json`:

```json
{
  "app": {
    "id": "application_1699999999999_0002",
    "user": "hdfs",
    "name": "example-spark-job",
    "queue": "default",
    "state": "FINISHED",
    "finalStatus": "SUCCEEDED",
    "progress": 100.0,
    "applicationType": "SPARK",
    "diagnostics": "",
    "startedTime": 1699999999000,
    "finishedTime": 1699999999999,
    "elapsedTime": 999,
    "allocatedMB": 0,
    "allocatedVCores": 0,
    "runningContainers": 0
  }
}
```

E usare `load_fixture("yarn_app_succeeded_spark.json")` al posto di
`yarn_app_succeeded.json` nel test `test_app_status_counters_best_effort_when_configured`.

Aggiungere le 2 funzioni alla lista `tests`.

- [ ] **Step 3: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `FAIL` — `"counters" in result.details` è False (non ancora implementato)

- [ ] **Step 4: Implementare il fetch counters best-effort**

Aggiungere in `ops/yarn_app.py`, dopo `_query_history_server`:

```python
_COUNTERS_CONFIG_KEY = {
    "SPARK":      "spark_history_url",
    "MAPREDUCE":  "mr_history_url",
    "TEZ":        "tez_history_url",
}

_COUNTERS_PATH = {
    "SPARK":     "/api/v1/applications/{app_id}",
    "MAPREDUCE": "/ws/v1/history/mapreduce/jobs/job_{job_suffix}",
    "TEZ":       "/ws/v1/history/tez/apps/{app_id}",
}


def _fetch_counters_best_effort(config, app_id, app_type, no_proxy, use_krb):
    # type: (dict, str, str, bool, bool) -> object
    """Tenta il fetch dei counters per il tipo applicativo. None se non
    configurato o se qualunque cosa fallisce — mai solleva."""
    cfg_key = _COUNTERS_CONFIG_KEY.get(app_type)
    if not cfg_key:
        return None
    yarn_cfg = config.get("yarn", {})
    plural = cfg_key.replace("_url", "_urls")
    history_url, _ = _resolve_url(yarn_cfg, cfg_key, plural)
    if not history_url:
        return None

    if app_type == "SPARK":
        path = "{}/api/v1/applications/{}".format(history_url, app_id)
    else:
        # MapReduce/Tez: mapping id->job id non standardizzato in questa
        # prima versione, riservato a fast-follow se emerge un bisogno reale
        return None

    try:
        data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                         full_path=True)
        return data
    except IOError:
        return None
```

Modificare `AppStatusTool.run()` — dopo aver costruito `fields` e prima di
costruire il `CheckResult` finale, aggiungere:

```python
        counters_note = ""
        if fields["state"] in ("FINISHED", "FAILED", "KILLED"):
            counters = _fetch_counters_best_effort(
                self.config, app_id, fields["applicationType"], no_proxy, use_krb)
            if counters:
                fields["counters"] = counters
            else:
                counters_note = "\ncounters non disponibili"

        return CheckResult(
            name=self.name,
            status=_status_from_fields(fields),
            message=_message_from_fields(app_id, fields) + counters_note,
            details=fields
        )
```

Questo sostituisce il `return CheckResult(...)` finale già presente in
`run()` (introdotto nel Task 6/7).

- [ ] **Step 5: Eseguire tutti i test**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `21/21 passed`

- [ ] **Step 6: Commit**

```bash
git add ops/yarn_app.py tests/fixtures/spark_history_counters.json tests/fixtures/yarn_app_succeeded_spark.json tests/test_ops.py
git commit -m "$(cat <<'EOF'
feat: AppStatusTool — counters best-effort per app Spark terminate

MapReduce/Tez riservati a fast-follow (mapping application-id -> job-id
non standardizzato, non necessario finché non emerge un caso reale).
EOF
)"
```

---

## Task 10: `AppLogsTool` — fetch log via Ansible sull'edge node

**Files:**
- Modify: `ops/yarn_app.py`
- Test: `tests/test_ops.py`

- [ ] **Step 1: Scrivere i test per `AppLogsTool`**

Aggiungere a `tests/test_ops.py` (import in cima):

```python
import shutil
import tempfile
from ops.yarn_app import AppLogsTool
```

```python
def test_app_logs_no_edge_host_configured():
    tool = AppLogsTool(config={}, caps={"ansible": True})
    result = tool.run(app_id="application_x")
    assert result.status == CheckResult.UNKNOWN
    assert "edge_host" in result.message


def test_app_logs_success_writes_file():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {
            "download_dir": tmpdir,
            "ansible": {"edge_host": "localhost"},
        }
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        fake_output = "log line 1\nlog line 2\n"
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "{}"'.format(
                            fake_output.replace("\n", "\\n")), "")):
            result = tool.run(app_id="application_test_001")
        assert result.status == CheckResult.OK
        out_path = os.path.join(tmpdir, "application_test_001.log")
        assert os.path.exists(out_path)
        with open(out_path) as f:
            content = f.read()
        assert "log line 1" in content
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_ansible_failure():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {"download_dir": tmpdir, "ansible": {"edge_host": "localhost"}}
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(2, "FAILED! => {\"msg\": \"boom\"}", "")):
            result = tool.run(app_id="application_test_002")
        assert result.status == CheckResult.CRITICAL
        assert "boom" in result.message
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_default_download_dir():
    tool = AppLogsTool(config={"ansible": {"edge_host": "x"}}, caps={"ansible": True})
    assert tool._resolve_download_dir() == os.path.expanduser("~/.hadoopscope/downloads")


def test_app_logs_kinit_injected_when_ansible_kerberos_enabled():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {
            "download_dir": tmpdir,
            "ansible": {
                "edge_host": "edge1.example.com",
                "kerberos": {"enabled": True, "keytab": "/edge.keytab",
                             "client_principal": "svc@REALM"},
            },
        }
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "ok\\n"', "")) as mocked_run:
            tool.run(app_id="application_test_003")
        _, kwargs = mocked_run.call_args
        assert kwargs["kinit_cmd"] == "kinit -kt /edge.keytab svc@REALM"
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_no_kinit_when_ansible_kerberos_disabled():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {"download_dir": tmpdir, "ansible": {"edge_host": "edge1.example.com"}}
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "ok\\n"', "")) as mocked_run:
            tool.run(app_id="application_test_004")
        _, kwargs = mocked_run.call_args
        assert kwargs["kinit_cmd"] is None
    finally:
        shutil.rmtree(tmpdir)
```

Aggiungere le 6 funzioni alla lista `tests`.

- [ ] **Step 2: Eseguire il test per verificare che fallisca**

Run: `python3 tests/test_ops.py`
Expected: `ImportError: cannot import name 'AppLogsTool'`

- [ ] **Step 3: Implementare `AppLogsTool` in `ops/yarn_app.py`**

Aggiungere l'import in cima al file (insieme agli altri import già
presenti dopo i Task precedenti):

```python
import os
import ansible_runner
```

Aggiungere in coda a `ops/yarn_app.py`:

```python
class AppLogsTool(OpsToolBase):
    """Scarica i log aggregati di un'applicazione YARN terminata, eseguendo
    'yarn logs -applicationId <id>' sull'edge node via Ansible (nessun
    client Hadoop richiesto sulla macchina locale)."""

    name = "app-logs"
    description = "Scarica i log di un'applicazione YARN terminata"
    params = [OpsParam("app_id", help="YARN application ID")]
    requires = [["ansible"], ["venv_ansible"], ["docker"]]

    def _resolve_download_dir(self):
        # type: () -> str
        configured = self.config.get("download_dir")
        return os.path.expanduser(configured or "~/.hadoopscope/downloads")

    def run(self, app_id):
        # type: (str) -> CheckResult
        ansible_cfg = self.config.get("ansible", {})
        edge_host = ansible_cfg.get("edge_host")
        ssh_user  = ansible_cfg.get("ssh_user", "hadoop")
        ssh_key   = ansible_cfg.get("ssh_key")

        if not edge_host:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="ansible.edge_host not configured"
            )

        ansible_bin = ansible_runner.find_ansible_bin()
        if not ansible_bin:
            return CheckResult(
                name=self.name,
                status=CheckResult.SKIPPED,
                message="ansible binary not found despite can_run() check"
            )

        inventory = ansible_runner.build_inventory(edge_host, ssh_user, ssh_key)

        krb = ansible_cfg.get("kerberos", {})
        kinit_cmd = None
        if krb.get("enabled") and krb.get("keytab") and krb.get("client_principal"):
            kinit_cmd = "kinit -kt {} {}".format(krb["keytab"], krb["client_principal"])

        cmd = "yarn logs -applicationId {}".format(app_id)
        rc, out, err = ansible_runner.run_playbook(
            ansible_bin, inventory, cmd, tag=self.name,
            kinit_cmd=kinit_cmd, timeout=180)

        if rc != 0:
            error_detail = ansible_runner.extract_task_error(out) if out else err
            return CheckResult(
                name=self.name,
                status=CheckResult.CRITICAL,
                message="fetch log fallito per {}: {}".format(app_id, error_detail[:300])
            )

        log_content = ansible_runner.extract_stdout(out)
        download_dir = self._resolve_download_dir()
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
        out_path = os.path.join(download_dir, "{}.log".format(app_id))
        with open(out_path, "w") as f:
            f.write(log_content)

        return CheckResult(
            name=self.name,
            status=CheckResult.OK,
            message="log salvati in {} ({} bytes)".format(out_path, len(log_content)),
            details={"path": out_path, "size": len(log_content)}
        )
```

- [ ] **Step 4: Eseguire tutti i test**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `27/27 passed`

- [ ] **Step 5: Commit**

```bash
git add ops/yarn_app.py tests/test_ops.py
git commit -m "feat: AppLogsTool — fetch log YARN via Ansible sull'edge node, download_dir configurabile"
```

---

## Task 11: Verbo CLI `ops`

**Files:**
- Modify: `hadoopscope.py`

- [ ] **Step 1: Aggiungere `build_ops_registry()` e `ops_main()` a `hadoopscope.py`**

Aggiungere dopo `build_check_registry()` (dopo la riga 116):

```python
def build_ops_registry():
    # type: () -> dict
    """Registry dei tool Ops disponibili, per nome."""
    from ops.yarn_app import AppStatusTool, AppLogsTool
    tools = [AppStatusTool, AppLogsTool]
    return {cls.name: cls for cls in tools}


def build_ops_arg_parser():
    # type: () -> argparse.ArgumentParser
    p = argparse.ArgumentParser(
        prog="hadoopscope.py ops",
        description="HadoopScope Ops — azioni on-demand sui cluster configurati"
    )
    p.add_argument("--config", default="config/hadoopscope.yaml",
                   help="Path to config file (default: config/hadoopscope.yaml)")
    p.add_argument("--output", default="text", choices=["text", "json"],
                   help="Output format (default: text)")
    p.add_argument("--debug", action="store_true")
    subparsers = p.add_subparsers(dest="tool", required=True)
    for name, cls in sorted(build_ops_registry().items()):
        tool_parser = subparsers.add_parser(name, help=cls.description)
        tool_parser.add_argument("--env", required=True,
                                 help="Environment su cui eseguire il tool")
        for param in cls.params:
            tool_parser.add_argument(
                "--{}".format(param.name.replace("_", "-")),
                dest=param.name, required=param.required, help=param.help)
    return p


def ops_main(argv):
    # type: (list) -> None
    parser = build_ops_arg_parser()
    args = parser.parse_args(argv)

    if args.debug:
        _debug.ENABLED = True

    caps = discover_capabilities()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print("ERROR loading config: {}".format(e), file=sys.stderr)
        sys.exit(1)

    caps = ensure_ansible(caps)

    if args.env not in cfg.get("environments", {}):
        print("ERROR: environment '{}' not found in config".format(args.env),
              file=sys.stderr)
        sys.exit(1)

    env_config = cfg["environments"][args.env]
    check_config = dict(env_config)
    if "checks" in cfg:
        check_config["checks"] = cfg["checks"]
    if "download_dir" in cfg:
        check_config["download_dir"] = cfg["download_dir"]

    registry = build_ops_registry()
    tool_cls = registry[args.tool]
    instance = tool_cls(config=check_config, caps=caps)

    if not instance.can_run():
        print("SKIP: {} requires: {}".format(args.tool, tool_cls.requires))
        sys.exit(3)

    tool_kwargs = {p.name: getattr(args, p.name) for p in tool_cls.params}
    result = instance.run(**tool_kwargs)

    if args.output == "json":
        print(json.dumps({
            "tool": args.tool, "env": args.env,
            "status": result.status, "message": result.message,
            "details": result.details
        }, indent=2))
    else:
        print("{} — {}".format(result.status, result.name))
        for line in result.message.splitlines():
            print("  {}".format(line))

    sys.exit({"OK": 0, "WARNING": 1, "CRITICAL": 2}.get(result.status, 3))
```

- [ ] **Step 2: Intercettare `ops` come primo argomento in `main()`**

Modificare la fine del file (righe 356-358):

```python
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "ops":
        ops_main(sys.argv[2:])
    else:
        main()
```

- [ ] **Step 3: Verificare manualmente la nuova CLI (help + errore config)**

Run: `python3 hadoopscope.py ops app-status --help`
Expected: help con `--env` e `--app-id`

Run: `python3 hadoopscope.py ops app-status --config config/test.yaml --env test-hdp --app-id application_x`
Expected: nessun crash — `SKIP: app-status requires: []` non applicabile
(requires vuoto), quindi tenta la query e fallisce con connection error
pulito (config/test.yaml punta a `localhost:8080`, non c'è un vero YARN RM)

- [ ] **Step 4: Eseguire tutta la suite di test per non-regressione**

Run: `python3 tests/test_checks.py && python3 tests/test_ops.py`
Expected: `84/84 passed` e `27/27 passed` (nessun impatto — `ops_main` non è testato da unit test, solo verificato manualmente qui; il flusso interno `AppStatusTool`/`AppLogsTool` è già coperto)

- [ ] **Step 5: Commit**

```bash
git add hadoopscope.py
git commit -m "$(cat <<'EOF'
feat: verbo CLI 'ops' — hadoopscope.py ops <tool> --env ... [--app-id ...]

Intercettato su sys.argv[1] prima del parser principale, per non
impattare l'invocazione esistente (--env, --checks, entry crontab hs:).
Parametri per-tool generati dinamicamente da OpsToolBase.params.
EOF
)"
```

---

## Task 12: Aggiornare `tests/run_all.py`

**Files:**
- Modify: `tests/run_all.py:13-17`

- [ ] **Step 1: Aggiungere `test_ops.py` e il `test_applog.py` mancante**

`tests/test_applog.py` esiste ed è mantenuto (aggiornato nella sessione
odierna con `test_log_run_interrupted`) ma non è mai stato incluso in
`tests/run_all.py` — gap preesistente segnalato nello spec, lo si chiude
qui insieme all'aggiunta di `test_ops.py`.

Sostituire (righe 13-17):

```python
test_files = [
    os.path.join(TESTS_DIR, "test_base.py"),
    os.path.join(TESTS_DIR, "test_config.py"),
    os.path.join(TESTS_DIR, "test_checks.py"),
    os.path.join(TESTS_DIR, "test_applog.py"),
    os.path.join(TESTS_DIR, "test_ops.py"),
]
```

- [ ] **Step 2: Eseguire la suite completa**

Run: `make test`
Expected: `ALL TESTS PASSED`, con `test_applog.py` e `test_ops.py` ora
visibili nell'output (`Running: test_applog.py`, `Running: test_ops.py`)

- [ ] **Step 3: Commit**

```bash
git add tests/run_all.py
git commit -m "test: aggiunge test_ops.py e test_applog.py (mancante) a run_all.py"
```

---

## Task 13: Validazione manuale contro l'ambiente DXC dev reale

Non è un task di codice — verifica dal vivo dei due rischi noti dello
spec (shape History Server/Timeline v2, comportamento Kerberos) contro
uno dei due ambienti CDP raggiungibili da questo Mac (`.claude/local/clients/mds.md`
o `.claude/local/clients/inail.md`, credenziali già presenti lì).
INAIL ha Kerberos attivo (utile per validare il path kinit); MdS no
(utile per validare il path "senza Kerberos" pulito).

Vincolo esplicito dell'utente per questa sessione: nessuna installazione
globale sul Mac. Se serve Ansible per validare `AppLogsTool` e non è già
presente in un venv/container, usare il bootstrap esistente del progetto
(`~/.hadoopscope/venv/`, creato automaticamente da `ensure_ansible()`) o
un container Docker — mai `pip install` di sistema.

**Files:** nessuno — solo comandi ed esito documentato

- [ ] **Step 1: Preparare un config YAML puntato all'ambiente scelto**

Leggere le credenziali/URL da `.claude/local/clients/{mds,inail}.md` e
creare (non committare — file locale) un `config/dxc-dev.yaml` con
`environments.dxc-dev.yarn.rm_url` e, se disponibile, `history_url`
(verificare la porta reale del Timeline/History Server sull'ambiente,
tipicamente 8188 — da confermare leggendo la config Cloudera Manager
dell'ambiente).

- [ ] **Step 2: Trovare un application id reale su cui testare**

Via Cloudera Manager UI o REST (`GET /api/v40/clusters/{cluster}/services/yarn/yarnApplications`)
sull'ambiente scelto, recuperare un application id recente — sia uno
RUNNING/appena completato (per validare il path RM diretto) sia uno più
vecchio se disponibile (per forzare il fallback History Server).

- [ ] **Step 3: Eseguire `app-status` contro l'app reale**

Run: `python3 hadoopscope.py ops app-status --config config/dxc-dev.yaml --env dxc-dev --app-id <ID_REALE>`

Verificare: lo status riportato coincide con quanto mostrato in Cloudera
Manager per la stessa applicazione. Se il fallback History Server viene
esercitato, confrontare i campi normalizzati (`_normalize_history_hdp`
o `_normalize_timeline_v2` a seconda del tipo ambiente) con la risposta
REST grezza (`curl` manuale allo stesso endpoint) — **se la shape reale
differisce da quella assunta nel Task 7, aggiornare `_normalize_history_hdp`/
`_normalize_timeline_v2` di conseguenza e aggiungere/aggiornare le fixture
corrispondenti**, poi rieseguire `tests/test_ops.py`.

- [ ] **Step 4: Se l'ambiente è kerberizzato (INAIL), validare il path kinit**

Configurare `yarn.kerberos.{enabled,keytab,principal}` in
`config/dxc-dev.yaml` con un keytab valido per l'ambiente, quindi
rieseguire il comando dello Step 3 con `--debug` per confermare nei log
che `kinit` viene invocato prima della chiamata REST e che la REST call
successiva non fallisce per credenziali mancanti.

- [ ] **Step 5: Validare `app-logs` su un'app terminata**

Verificare che `ansible.edge_host` sia configurato nel config di test per
l'ambiente scelto (riusa lo stesso edge host già eventualmente configurato
per Hive, se presente in `.claude/local/clients/`). Se Ansible non è
disponibile localmente, lasciare che `ensure_ansible()` lo installi nel
venv isolato `~/.hadoopscope/venv/` (comportamento esistente, nessuna
azione manuale richiesta) — non installare Ansible con `pip install`
diretto sul Mac.

Run: `python3 hadoopscope.py ops app-logs --config config/dxc-dev.yaml --env dxc-dev --app-id <ID_APP_TERMINATA>`

Verificare: il file `~/.hadoopscope/downloads/<ID_APP_TERMINATA>.log`
viene creato e contiene log leggibili (non binario/corrotto — conferma
che l'output di `yarn logs` via Ansible arriva integro attraverso
l'estrazione `ansible_runner.extract_stdout`).

- [ ] **Step 6: Documentare l'esito**

Aggiornare `.claude/local/clients/{mds,inail}.md` (file gitignored, non
committato) con l'esito della validazione: shape REST confermata o
corretta, eventuali differenze trovate, application id di test usati.
Se sono state necessarie correzioni al codice (Step 3), quelle vanno
committate normalmente come fix separati con riferimento a questo task.
