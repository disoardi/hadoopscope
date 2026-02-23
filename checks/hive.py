"""Check Hive — via Ansible+beeline (richiede ansible) oppure HiveServer2 REST."""

from __future__ import print_function

import os
import subprocess
import tempfile

from checks.base import CheckBase, CheckResult


_BEELINE_TEST_QUERY = "SELECT 1;"


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
            zk_str = ",".join(str(h) for h in zk_hosts)
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
    """
    url  = _build_beeline_url(hive_cfg)
    user = hive_cfg.get("user", default_user)
    pwd  = hive_cfg.get("password")
    if pwd:
        auth_str = "-n '{user}' -p '{pwd}'".format(user=user, pwd=pwd)
    else:
        auth_str = "-n '{user}'".format(user=user)
    return (
        'beeline -u "{url}" {auth}'
        " -e '{query}' --silent=true --outputformat=csv2"
    ).format(url=url, auth=auth_str, query=_BEELINE_TEST_QUERY)


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
            combined = (f["out"] + f["err"]).strip()
            if f["rc"] == -1:
                fail_msgs.append("{}: timeout".format(f["name"]))
            elif f["rc"] == -2:
                fail_msgs.append("{}: error — {}".format(f["name"], f["err"][:100]))
            else:
                fail_msgs.append("{}: rc={}".format(f["name"], f["rc"]))
            details[f["name"]] = {
                "rc": f["rc"],
                "stdout": f["out"][:500],
                "stderr": f["err"][:200],
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
