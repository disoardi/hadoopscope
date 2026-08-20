"""Ansible helpers condivisi — inventory single-host, esecuzione playbook,
estrazione output. Usato da checks/hive.py e (in futuro) dal layer Ops per
eseguire comandi su un edge node via Ansible (nessun inventory statico:
l'inventory e' generato al volo da un singolo hostname in config)."""

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
    """Inventory Ansible single-host generato al volo (mai un file statico).

    Se ssh_key non è configurata, NON forza un path di default: lascia che
    SSH risolva l'identità da solo (ssh-agent, chiavi caricate via 1Password
    o simili, ~/.ssh/config Host alias) invece di puntare esplicitamente a
    un file che potrebbe non esistere — forzare '-i ~/.ssh/id_rsa' su un
    utente che usa un agent fa fallire l'autenticazione con 'no such
    identity' invece di lasciar provare l'agent.
    """
    if edge_host in ("localhost", "127.0.0.1", "::1"):
        return "localhost ansible_connection=local"
    if ssh_key:
        return (
            "{host} ansible_user={user} ansible_ssh_private_key_file={key}"
        ).format(host=edge_host, user=ssh_user, key=ssh_key)
    return "{host} ansible_user={user}".format(host=edge_host, user=ssh_user)


def extract_task_error(ansible_stdout):
    # type: (str) -> str
    """Extract the actual task error from Ansible stdout.

    Ansible wraps the task result as JSON after 'FAILED! => '.
    We parse that JSON to get the real stdout/stderr/msg
    instead of returning the truncated Ansible header.
    """
    # Ansible stampa il task result come JSON su una sola riga.
    # re.DOTALL NON va usato: cattura anche il PLAY RECAP che segue,
    # rendendo il JSON non parsabile. Il \} assicura di fermarsi
    # alla chiusura dell'oggetto sulla stessa riga.
    match = re.search(r"FAILED! => (\{.*\})", ansible_stdout)
    if not match:
        return ansible_stdout[-800:]
    try:
        data = json.loads(match.group(1))
        parts = []
        if data.get("msg"):
            parts.append("msg: {}".format(data["msg"]))
        if data.get("stdout"):
            parts.append("beeline stdout: {}".format(data["stdout"][:600]))
        if data.get("stderr"):
            parts.append("beeline stderr: {}".format(data["stderr"][:400]))
        return "\n".join(parts) if parts else ansible_stdout[-800:]
    except (ValueError, KeyError):
        return ansible_stdout[-800:]


def extract_stdout(ansible_out):
    # type: (str) -> str
    """Extract shell stdout string from Ansible debug output (r.stdout)."""
    m = re.search(r'"r\.stdout":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        raw = raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return raw
    return ""


def extract_stderr(ansible_out):
    # type: (str) -> str
    """Extract shell stderr string from Ansible debug output (r.stderr)."""
    m = re.search(r'"r\.stderr":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        raw = raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return raw
    return ""


def run_playbook(ansible_bin, inventory_content, shell_cmd,
                  tag="AnsibleRunner", kinit_cmd=None, timeout=60):
    # type: (str, str, str, str, object, int) -> tuple
    """Run Ansible playbook with optional kinit + shell command.

    Usa il modulo 'raw' invece di 'shell': raw esegue il comando via SSH
    senza passare dal sistema di moduli Python di Ansible (AnsiballZ),
    quindi non richiede nessuna versione minima di Python sull'edge node —
    fondamentale perché il Python remoto varia molto tra i cluster clienti
    (2.7 su HDP vecchi, 3.6 su alcuni CDP, 3.8+ su altri) e ansible-core
    recente (2.14+) sul controller ha smesso di supportare target con
    Python < 3.8 per i moduli standard. 'raw' aggira il problema del tutto.

    kinit_cmd: if set, a 'kinit -kt <keytab> <principal>' command run on the
    edge node BEFORE shell_cmd. Both keytab and principal must be paths/values
    on the edge node, not on the machine running hadoopscope.

    timeout: subprocess timeout in seconds.

    Returns (rc, stdout, stderr):
      rc >= 0  : actual Ansible exit code
      rc == -1 : subprocess timeout
      rc == -2 : unexpected exception (err contains message)
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
        "      raw: |\n"
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
        # Estrai e mostra r.stdout e r.stderr dal debug task Ansible
        r_stdout = extract_stdout(out)
        r_stderr = extract_stderr(out)
        if r_stdout.strip():
            _debug.section(tag, "r.stdout (shell output)")
            _debug.log(tag, r_stdout, multiline=True)
        else:
            _debug.log(tag, "r.stdout: (empty)")
        if r_stderr.strip():
            _debug.section(tag, "r.stderr (shell stderr / debug content)")
            _debug.log(tag, r_stderr, multiline=True)
        if err.strip():
            _debug.section(tag, "ansible process stderr")
            _debug.log(tag, err, multiline=True)
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
