"""Check Hive — via Ansible+beeline (richiede ansible) oppure HiveServer2 REST."""

from __future__ import print_function

import json
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


class HiveCheck(CheckBase):
    """
    Controlla la disponibilità di HiveServer2 via beeline tramite Ansible.
    Richiede ansible (sistema o venv bootstrap) + accesso SSH all'edge node.
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
        hive_user   = hive_cfg.get("user", ssh_user)

        if not edge_host:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.UNKNOWN,
                message="ansible.edge_host not configured"
            )

        # Determina ansible binary
        ansible_bin = self._find_ansible()
        if not ansible_bin:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.SKIPPED,
                message="ansible binary not found despite can_run() check"
            )

        beeline_cmd = _build_beeline_cmd(hive_cfg, hive_user)

        # Inventory dinamico.
        # Se edge_host è localhost usa connection=local (niente SSH su se stesso).
        if edge_host in ("localhost", "127.0.0.1", "::1"):
            inventory_content = "localhost ansible_connection=local"
        else:
            inventory_content = (
                "{host} ansible_user={user} ansible_ssh_private_key_file={key}"
            ).format(
                host=edge_host,
                user=ssh_user,
                key=ssh_key or "~/.ssh/id_rsa"
            )

        # Il comando beeline contiene virgolette singole — usare block scalar YAML (|)
        # per evitare errori di parsing (Ansible rc=4).
        playbook_content = (
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

        try:
            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.ini', delete=False, prefix='hs_inv_'
            ) as inv_f:
                inv_f.write(inventory_content)
                inv_path = inv_f.name

            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.yml', delete=False, prefix='hs_hive_'
            ) as play_f:
                play_f.write(playbook_content)
                play_path = play_f.name

            env = os.environ.copy()
            env["ANSIBLE_HOST_KEY_CHECKING"] = "False"

            proc = subprocess.Popen(
                [ansible_bin, "-i", inv_path, play_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            stdout, stderr = proc.communicate(timeout=60)
            rc = proc.returncode

            os.unlink(inv_path)
            os.unlink(play_path)

            out = stdout.decode("utf-8", errors="replace")
            err = stderr.decode("utf-8", errors="replace")

            if rc == 0:
                jdbc_url = _build_beeline_url(hive_cfg)
                return CheckResult(
                    name="HiveCheck",
                    status=CheckResult.OK,
                    message="HiveServer2 responded to test query ({})".format(jdbc_url[:80]),
                    details={"stdout": out[:500]}
                )
            else:
                # Ansible scrive i dettagli degli errori su stdout (non stderr)
                combined = (out + err).strip()
                return CheckResult(
                    name="HiveCheck",
                    status=CheckResult.CRITICAL,
                    message="Hive check failed (rc={}): {}".format(rc, combined[:400]),
                    details={"stdout": out[:1000], "stderr": err[:500], "rc": rc}
                )

        except subprocess.TimeoutExpired:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.UNKNOWN,
                message="Hive check timed out (60s)"
            )
        except Exception as e:
            return CheckResult(
                name="HiveCheck",
                status=CheckResult.UNKNOWN,
                message="Hive check error: {}".format(str(e))
            )

    def _find_ansible(self):
        # type: () -> str
        """Trova il binary ansible da caps o dal PATH."""
        import shutil
        # Sistema
        bin_path = shutil.which("ansible-playbook")
        if bin_path:
            return bin_path
        # venv bootstrap
        venv_bin = os.path.expanduser("~/.hadoopscope/venv/bin/ansible-playbook")
        if os.path.exists(venv_bin):
            return venv_bin
        return None
