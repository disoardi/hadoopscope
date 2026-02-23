"""Check Hive — via Ansible+beeline (richiede ansible) oppure HiveServer2 REST."""

from __future__ import print_function

import json
import os
import re
import subprocess
import tempfile

from checks.base import CheckBase, CheckResult


_BEELINE_TEST_QUERY = "SELECT 1;"


def _zk_host_str(item):
    # type: (object) -> str
    """Format a ZK host item that may arrive as string or dict {host: port}.

    The manual YAML parser incorrectly parses 'host:port' (no space after colon)
    as a mapping {host: port}. This helper handles both forms defensively.
    """
    if isinstance(item, dict) and len(item) == 1:
        host, port = list(item.items())[0]
        return "{}:{}".format(host, port)
    return str(item)


def _build_beeline_url(hive_cfg):
    # type: (dict) -> str
    """Build JDBC URL for beeline.

    ZooKeeper mode (if zookeeper_hosts is set):
      jdbc:hive2://zk1:2181,zk2:2181/[;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=<ns>]

    Direct mode (fallback):
      jdbc:hive2://host:port/database
    """
    zk_hosts = hive_cfg.get("zookeeper_hosts")
    if zk_hosts:
        if isinstance(zk_hosts, list):
            zk_str = ",".join(_zk_host_str(h) for h in zk_hosts)
        else:
            zk_str = str(zk_hosts)
        url = "jdbc:hive2://{}/".format(zk_str)
        zk_ns = hive_cfg.get("zookeeper_namespace")
        if zk_ns:
            url += ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace={}".format(zk_ns)
    else:
        host = hive_cfg.get("host", "localhost")
        port = hive_cfg.get("port", 10000)
        db   = hive_cfg.get("database", "default")
        url  = "jdbc:hive2://{}:{}/{}".format(host, port, db)
    return url


def _build_beeline_cmd(hive_cfg, default_user):
    # type: (dict, str) -> str
    """Build beeline shell command string.

    Auth: if hive.password is set, adds -p flag (LDAP/PAM).
    Uses double quotes around JDBC URL to handle commas and semicolons.
    beeline_path: full path to beeline binary (default: 'beeline', assumes PATH).
    """
    url      = _build_beeline_url(hive_cfg)
    user     = hive_cfg.get("user", default_user)
    pwd      = hive_cfg.get("password")
    beeline  = hive_cfg.get("beeline_path", "beeline")
    if pwd:
        auth_str = "-n '{user}' -p '{pwd}'".format(user=user, pwd=pwd)
    else:
        auth_str = "-n '{user}'".format(user=user)
    return (
        '{beeline} -u "{url}" {auth}'
        " -e '{query}' --silent=true --outputformat=csv2"
    ).format(beeline=beeline, url=url, auth=auth_str, query=_BEELINE_TEST_QUERY)


def _merge_ns_cfg(hive_cfg, ns_entry):
    # type: (dict, dict) -> dict
    """Build per-namespace config by merging parent hive_cfg with namespace overrides.

    Rules:
    - zookeeper_hosts, host, port, database inherited from parent
    - user: namespace override wins, else parent hive.user
    - password: NOT inherited from parent — each namespace declares its own
    - zookeeper_namespace: taken from ns_entry["name"]
    """
    merged = {k: v for k, v in hive_cfg.items() if k not in ("namespaces", "password")}
    merged["zookeeper_namespace"] = ns_entry.get("name", "")
    if "user" in ns_entry:
        merged["user"] = ns_entry["user"]
    if "password" in ns_entry:
        merged["password"] = ns_entry["password"]
    return merged


def _extract_task_error(ansible_stdout):
    # type: (str) -> str
    """Extract the actual task error from Ansible stdout.

    Ansible wraps the task result as JSON after 'FAILED! => '.
    We parse that JSON to get beeline's real stdout/stderr/msg
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


class HiveCheck(CheckBase):
    """
    Controlla la disponibilità di HiveServer2 via beeline tramite Ansible.
    Richiede ansible (sistema o venv bootstrap) + accesso SSH all'edge node.

    Config single-namespace (backward compat):
      hive:
        zookeeper_hosts: [zk1:2181, zk2:2181]
        zookeeper_namespace: hiveserver2
        user: hive

    Config multi-namespace:
      hive:
        zookeeper_hosts: [zk1:2181, zk2:2181]
        user: hive                         # default user, ereditato dai namespace
        namespaces:
          - name: hiveserver2              # no auth
          - name: hiveserver2ldap          # LDAP — password esplicita
            user: svcaccount              # opzionale: override user
            password: "${HIVE_LDAP_PASS}"
    """

    requires = [["ansible"], ["venv_ansible"], ["docker_ansible_image"]]
    fallback = None

    def run(self):
        # type: () -> CheckResult
        ansible_cfg = self.config.get("ansible", {})
        edge_host   = ansible_cfg.get("edge_host")
        ssh_user    = ansible_cfg.get("ssh_user", "hadoop")
        ssh_key     = ansible_cfg.get("ssh_key")
        hive_cfg    = self.config.get("hive", {})

        if not edge_host:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.UNKNOWN,
                message="ansible.edge_host not configured"
            )

        ansible_bin = self._find_ansible()
        if not ansible_bin:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.SKIPPED,
                message="ansible binary not found despite can_run() check"
            )

        inventory = self._build_inventory(edge_host, ssh_user, ssh_key)

        # Determina le istanze da controllare
        namespaces = hive_cfg.get("namespaces")
        if namespaces:
            # Multi-namespace: un'istanza per ogni entry
            instances = [
                (ns.get("name", "ns{}".format(i)), _merge_ns_cfg(hive_cfg, ns))
                for i, ns in enumerate(namespaces)
            ]
        else:
            # Single-instance (backward compat)
            hive_user = hive_cfg.get("user", ssh_user)
            label = hive_cfg.get("zookeeper_namespace") or "{}:{}".format(
                hive_cfg.get("host", "localhost"), hive_cfg.get("port", 10000))
            instances = [(label, hive_cfg)]

        # Esegui beeline per ogni istanza
        ok_names = []
        failed = []
        for inst_name, inst_cfg in instances:
            inst_user = inst_cfg.get("user", ssh_user)
            cmd = _build_beeline_cmd(inst_cfg, inst_user)
            rc, out, err = self._run_playbook(ansible_bin, inventory, cmd)
            if rc == 0:
                ok_names.append(inst_name)
            else:
                failed.append({
                    "name": inst_name,
                    "rc": rc,
                    "out": out,
                    "err": err,
                })

        if not failed:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.OK,
                message="HiveServer2 OK ({} namespace(s): {})".format(
                    len(ok_names), ", ".join(ok_names)),
                details={"namespaces": ok_names}
            )

        # Almeno uno fallito
        fail_msgs = []
        details = {}
        for f in failed:
            if f["rc"] == -1:
                fail_msgs.append("{}: timeout".format(f["name"]))
                task_error = "timeout after 60s"
            elif f["rc"] == -2:
                fail_msgs.append("{}: error — {}".format(f["name"], f["err"][:100]))
                task_error = f["err"]
            else:
                task_error = _extract_task_error(f["out"])
                fail_msgs.append("{}: rc={} — {}".format(
                    f["name"], f["rc"], task_error[:200]))
            details[f["name"]] = {
                "rc": f["rc"],
                "error": task_error,
            }
        if ok_names:
            details["ok"] = ok_names
            summary = "{}/{} failed — {}".format(
                len(failed), len(instances), "; ".join(fail_msgs))
        else:
            summary = "all namespaces failed — {}".format("; ".join(fail_msgs))

        return CheckResult(
            name="HiveCheck",
            status=CheckResult.CRITICAL,
            message="Hive check failed: {}".format(summary),
            details=details
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_inventory(self, edge_host, ssh_user, ssh_key):
        # type: (str, str, str) -> str
        if edge_host in ("localhost", "127.0.0.1", "::1"):
            return "localhost ansible_connection=local"
        return (
            "{host} ansible_user={user} ansible_ssh_private_key_file={key}"
        ).format(
            host=edge_host,
            user=ssh_user,
            key=ssh_key or "~/.ssh/id_rsa"
        )

    def _run_playbook(self, ansible_bin, inventory_content, beeline_cmd):
        # type: (str, str, str) -> tuple
        """Run Ansible playbook with beeline command.

        Returns (rc, stdout, stderr):
          rc >= 0  : actual Ansible exit code
          rc == -1 : subprocess timeout (60s)
          rc == -2 : unexpected exception (err contains message)
        """
        playbook = (
            "---\n"
            "- name: HiveCheck\n"
            "  hosts: all\n"
            "  gather_facts: false\n"
            "  tasks:\n"
            "    - name: Beeline test\n"
            "      shell: |\n"
            "        {cmd}\n"
            "      register: r\n"
            "    - debug: var=r.stdout\n"
        ).format(cmd=beeline_cmd)

        inv_path = play_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.ini', delete=False, prefix='hs_inv_'
            ) as f:
                f.write(inventory_content)
                inv_path = f.name

            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.yml', delete=False, prefix='hs_hive_'
            ) as f:
                f.write(playbook)
                play_path = f.name

            env = os.environ.copy()
            env["ANSIBLE_HOST_KEY_CHECKING"] = "False"

            proc = subprocess.Popen(
                [ansible_bin, "-i", inv_path, play_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            stdout, stderr = proc.communicate(timeout=60)
            return (
                proc.returncode,
                stdout.decode("utf-8", errors="replace"),
                stderr.decode("utf-8", errors="replace"),
            )

        except subprocess.TimeoutExpired:
            return -1, "", "timeout after 60s"
        except Exception as e:
            return -2, "", str(e)
        finally:
            for p in (inv_path, play_path):
                if p and os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

    def _find_ansible(self):
        # type: () -> str
        """Trova il binary ansible da caps o dal PATH."""
        import shutil
        bin_path = shutil.which("ansible-playbook")
        if bin_path:
            return bin_path
        venv_bin = os.path.expanduser("~/.hadoopscope/venv/bin/ansible-playbook")
        if os.path.exists(venv_bin):
            return venv_bin
        return None
