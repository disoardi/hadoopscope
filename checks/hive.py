"""Check Hive — via Ansible+beeline (richiede ansible) oppure HiveServer2 REST."""

from __future__ import print_function

import json
import os
import re
import subprocess
import tempfile

from checks.base import CheckBase, CheckResult
import debug as _debug


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

    If jdbc_url is set, it is returned verbatim — bypasses all other params.
    Use this for load-balancer or complex SSL/Kerberos setups.

    ZooKeeper mode (if zookeeper_hosts is set):
      jdbc:hive2://zk1:2181,zk2:2181/[;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=<ns>][;ssl=true;...]

    Direct / load-balancer mode (fallback):
      jdbc:hive2://host:port/[;ssl=true;sslTrustStore=...;trustStorePassword=...][;principal=...]

    SSL params come from hive.ssl.{enabled,truststore,truststore_password}.
    Kerberos JDBC principal comes from hive.kerberos_principal.
    """
    # Verbatim JDBC URL — user provides the complete connection string
    if hive_cfg.get("jdbc_url"):
        return hive_cfg["jdbc_url"]

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

    # SSL JDBC properties (appended to ZooKeeper and direct mode alike)
    ssl_cfg = hive_cfg.get("ssl", {})
    if ssl_cfg.get("enabled"):
        url += ";ssl=true"
        if ssl_cfg.get("truststore"):
            url += ";sslTrustStore={}".format(ssl_cfg["truststore"])
        if ssl_cfg.get("truststore_password"):
            url += ";trustStorePassword={}".format(ssl_cfg["truststore_password"])

    # Kerberos principal as JDBC property
    krb_principal = hive_cfg.get("kerberos_principal")
    if krb_principal:
        url += ";principal={}".format(krb_principal)

    return url


def _build_kinit_cmd(hive_cfg):
    # type: (dict) -> object
    """Return kinit shell command if hive.kerberos.keytab/client_principal are set, else None.

    The keytab and client_principal must be paths/values on the EDGE NODE,
    not on the machine running hadoopscope. The command is injected into the
    Ansible playbook shell block and executed remotely before beeline.
    """
    krb = hive_cfg.get("kerberos", {})
    if not isinstance(krb, dict):
        return None
    keytab   = krb.get("keytab")
    client_p = krb.get("client_principal")
    if keytab and client_p:
        return "kinit -kt {keytab} {principal}".format(
            keytab=keytab, principal=client_p
        )
    return None


def _label_from_cfg(hive_cfg):
    # type: (dict) -> str
    """Return a short human-readable label for a HiveServer2 instance."""
    if hive_cfg.get("zookeeper_namespace"):
        return hive_cfg["zookeeper_namespace"]
    if hive_cfg.get("jdbc_url"):
        try:
            after_proto = hive_cfg["jdbc_url"].split("//", 1)[1]
            return after_proto.split("/")[0].split(";")[0]
        except (IndexError, AttributeError):
            return "hive"
    return "{}:{}".format(hive_cfg.get("host", "localhost"), hive_cfg.get("port", 10000))


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
    - zookeeper_hosts, host, port, database, ssl, kerberos_principal inherited from parent
    - user: namespace override wins, else parent hive.user
    - password: NOT inherited from parent — each namespace declares its own
    - jdbc_url: namespace override wins (allows different LB URL per namespace)
    - zookeeper_namespace: taken from ns_entry["name"]
    """
    merged = {k: v for k, v in hive_cfg.items() if k not in ("namespaces", "password")}
    merged["zookeeper_namespace"] = ns_entry.get("name", "")
    if "user" in ns_entry:
        merged["user"] = ns_entry["user"]
    if "password" in ns_entry:
        merged["password"] = ns_entry["password"]
    if "jdbc_url" in ns_entry:
        merged["jdbc_url"] = ns_entry["jdbc_url"]
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


def _extract_stdout(ansible_out):
    # type: (str) -> str
    """Extract shell stdout string from Ansible debug output (r.stdout)."""
    m = re.search(r'"r\.stdout":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        raw = raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return raw
    return ""


def _extract_stderr(ansible_out):
    # type: (str) -> str
    """Extract shell stderr string from Ansible debug output (r.stderr)."""
    m = re.search(r'"r\.stderr":\s*"((?:[^"\\]|\\.)*)"', ansible_out)
    if m:
        raw = m.group(1)
        raw = raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return raw
    return ""


def _parse_databases_output(output):
    # type: (str) -> list
    """Parse beeline tsv2 output from SHOW DATABASES.

    Expects one DB name per line with a header row 'database_name'.
    Returns list of database name strings.
    """
    lines = [l.strip() for l in output.splitlines() if l.strip()]
    if lines and lines[0].lower() in ("database_name", "databasename"):
        lines = lines[1:]
    return [l for l in lines if l]


def _parse_partition_output(output):
    # type: (str) -> dict
    """Parse multi-DB partition count output.

    Supporta due formati:

    Formato A — SHOW PARTITIONS (nuovo, default):
      ###DB:dbname###
      _c0                        <- header SELECT
      ###TAB:table1###           <- marker tabella
      partition                  <- header SHOW PARTITIONS
      dt=20260101/field=val      <- riga partizione (contata)
      ...

    Formato B — information_schema (legacy, per retrocompatibilità test):
      ###DB:dbname###
      table_name<tab>count       <- riga dati tab-separated

    Returns {db_name: {table_name: count}}.
    """
    # Header lines da ignorare in entrambi i formati
    SKIP_LINES = {"_c0", "partition", "tab_name", ""}

    result = {}      # type: dict
    current_db  = None
    current_tbl = None
    tbl_counts  = {}  # type: dict

    for raw_line in output.splitlines():
        line = raw_line.strip()

        # DB marker
        if line.startswith("###DB:") and line.endswith("###"):
            if current_db is not None:
                result[current_db] = dict(tbl_counts)
            current_db  = line[6:-3]
            current_tbl = None
            tbl_counts  = {}
            continue

        if current_db is None:
            continue

        # Salta header noti
        if line in SKIP_LINES:
            continue

        # TAB marker (formato A)
        if line.startswith("###TAB:") and line.endswith("###"):
            current_tbl = line[7:-3]
            if current_tbl not in tbl_counts:
                tbl_counts[current_tbl] = 0
            continue

        # Formato B: table_name<tab>count (legacy information_schema)
        if current_tbl is None and "\t" in line:
            parts = line.split("\t")
            if len(parts) >= 2:
                try:
                    tbl_counts[parts[0].strip()] = int(parts[1].strip())
                except (ValueError, IndexError):
                    pass
            continue

        # Formato A: riga partition spec (qualsiasi riga non vuota dopo ###TAB:)
        if current_tbl is not None:
            tbl_counts[current_tbl] = tbl_counts.get(current_tbl, 0) + 1

    # Salva l'ultimo DB
    if current_db is not None:
        result[current_db] = dict(tbl_counts)

    return result


def _build_db_discovery_cmd(hive_cfg, default_user):
    # type: (dict, str) -> str
    """Build beeline command to list all Hive databases via SHOW DATABASES."""
    beeline = hive_cfg.get("beeline_path", "beeline")
    url     = _build_beeline_url(hive_cfg)
    user    = hive_cfg.get("user", default_user)
    pwd     = hive_cfg.get("password")
    if pwd:
        auth_str = "-n '{user}' -p '{pwd}'".format(user=user, pwd=pwd)
    else:
        auth_str = "-n '{user}'".format(user=user)
    return (
        '{beeline} -u "{url}" {auth} --silent=true --outputformat=tsv2'
        ' -e "SHOW DATABASES;" 2>/dev/null'
    ).format(beeline=beeline, url=url, auth=auth_str)


def _build_partition_query_script(hive_cfg, databases, default_user):
    # type: (dict, list, str) -> str
    """Build multi-command shell script to count partitions per table for each DB.

    Per ogni DB:
      1. SHOW TABLES IN <db> — recupera la lista tabelle (1 connessione beeline)
      2. Costruisce un file SQL temporaneo con SELECT '###TAB:xxx###' + SHOW PARTITIONS
         per ogni tabella, poi lo esegue in un'unica sessione beeline (1 connessione)

    Totale: 2 connessioni beeline per DB, indipendentemente dal numero di tabelle.

    Evita information_schema.partitions che su CDP con Ranger può restituire 0 righe
    anche quando le partizioni esistono.

    Output (stdout Ansible):
      ###DB:dbname###
      _c0
      ###TAB:table1###
      partition
      dt=20260101/field=val
      ...
      _c0
      ###TAB:table2###
      ...
    Parsato da _parse_partition_output().
    """
    beeline = hive_cfg.get("beeline_path", "beeline")
    url     = _build_beeline_url(hive_cfg)
    user    = hive_cfg.get("user", default_user)
    pwd     = hive_cfg.get("password")
    if pwd:
        auth_str = "-n '" + user + "' -p '" + pwd + "'"
    else:
        auth_str = "-n '" + user + "'"
    conn = beeline + ' -u "' + url + '" ' + auth_str + " --silent=true --outputformat=tsv2"

    lines = []
    for db in databases:
        lines.append('echo "###DB:' + db + '###"')
        # Crea file SQL temporaneo
        lines.append('_HS_F=$(mktemp /tmp/hs_XXXXXX.sql 2>/dev/null || echo "/tmp/hs_$$.sql")')
        # Popola il file: per ogni tabella un marker SELECT + SHOW PARTITIONS
        lines.append(
            'for _tbl in $(' + conn + ' -e "SHOW TABLES IN ' + db + ';" 2>/dev/null'
            ' | grep -v "^tab_name$" | grep -v "^$"); do'
        )
        lines.append("    printf \"SELECT '###TAB:%s###';\\n\" \"$_tbl\" >> \"$_HS_F\"")
        lines.append('    printf "SHOW PARTITIONS ' + db + '.%s;\\n" "$_tbl" >> "$_HS_F"')
        lines.append('done')
        # Esegui tutto in una singola sessione beeline
        lines.append('if [ -s "$_HS_F" ]; then')
        # Manda il contenuto del file SQL su stderr (visibile in r.stderr Ansible, --debug)
        lines.append('    echo "=== SQL FILE [' + db + '] ===" >&2')
        lines.append('    cat "$_HS_F" >&2')
        lines.append('    echo "=== END SQL FILE ===" >&2')
        # --force: continua l'esecuzione anche su SHOW PARTITIONS di tabelle non partizionate
        lines.append('    ' + conn + ' --force -f "$_HS_F" 2>/dev/null || true')
        lines.append('fi')
        lines.append('rm -f "$_HS_F"')
    return "\n".join(lines)


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
            instances = [(_label_from_cfg(hive_cfg), hive_cfg)]

        _debug.log("HiveCheck", "ansible_bin: {}".format(ansible_bin))
        _debug.log("HiveCheck", "edge_host: {}".format(edge_host))
        _debug.log("HiveCheck", "inventory: {}".format(inventory))

        # Esegui beeline per ogni istanza
        ok_names = []
        failed = []
        for inst_name, inst_cfg in instances:
            inst_user  = inst_cfg.get("user", ssh_user)
            cmd        = _build_beeline_cmd(inst_cfg, inst_user)
            kinit_cmd  = _build_kinit_cmd(inst_cfg)
            tag        = "HiveCheck[{}]".format(inst_name)
            _debug.log(tag, "beeline_cmd: {}".format(cmd))
            if kinit_cmd:
                _debug.log(tag, "kinit_cmd (edge node): {}".format(kinit_cmd))
            rc, out, err = self._run_playbook(ansible_bin, inventory, cmd, tag,
                                              kinit_cmd=kinit_cmd)
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

    def _run_playbook(self, ansible_bin, inventory_content, beeline_cmd,
                      tag="HiveCheck", kinit_cmd=None, timeout=60):
        # type: (str, str, str, str, object, int) -> tuple
        """Run Ansible playbook with optional kinit + beeline command.

        kinit_cmd: if set, a 'kinit -kt <keytab> <principal>' command run on the
        edge node BEFORE beeline. Both keytab and principal must be paths/values
        on the edge node, not on the machine running hadoopscope.

        timeout: subprocess timeout in seconds (default 60; use higher value for
        long-running scripts like HivePartitionCheck).

        Returns (rc, stdout, stderr):
          rc >= 0  : actual Ansible exit code
          rc == -1 : subprocess timeout
          rc == -2 : unexpected exception (err contains message)
        """
        script_parts = []
        if kinit_cmd:
            script_parts.append(kinit_cmd)
        for line in beeline_cmd.splitlines():
            script_parts.append(line)
        shell_lines = "\n".join("        " + l for l in script_parts)

        playbook = (
            "---\n"
            "- name: HiveCheck\n"
            "  hosts: all\n"
            "  gather_facts: false\n"
            "  tasks:\n"
            "    - name: Beeline test\n"
            "      shell: |\n"
            "{shell_lines}\n"
            "      register: r\n"
            "    - debug: var=r.stdout\n"
            "    - debug: var=r.stderr\n"
        ).format(shell_lines=shell_lines)

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
            r_stdout = _extract_stdout(out)
            r_stderr = _extract_stderr(out)
            if r_stdout.strip():
                _debug.section(tag, "r.stdout (beeline output)")
                _debug.log(tag, r_stdout, multiline=True)
            else:
                _debug.log(tag, "r.stdout: (empty)")
            if r_stderr.strip():
                _debug.section(tag, "r.stderr (beeline stderr / SQL file)")
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


class HivePartitionCheck(HiveCheck):
    """
    Conta le partizioni per tabella su uno o più database Hive via SHOW PARTITIONS.
    Usa Ansible+beeline su edge node — solo metadata, nessun data scan.

    Per ogni DB: SHOW TABLES → build SQL tmpfile → SHOW PARTITIONS <db>.<tbl> per ogni tabella
    in una singola sessione beeline. Evita information_schema.partitions che su CDP con Ranger
    può restituire 0 righe anche quando le partizioni esistono.

    Config:
      checks:
        hive_partitions:
          databases:          # lista DB da controllare; ometti per auto-discover (SHOW DATABASES)
            - mydb
            - prod_dw
          max_partitions: 5000  # WARNING se una tabella supera questa soglia (0 = nessun limite)
          timeout: 300          # secondi per l'Ansible playbook (default 300)

    Connessione: eredita tutta la config hive: (jdbc_url, kerberos, ssl, beeline_path, ecc.)
    dall'environment, identica a HiveCheck.
    """

    def run(self):
        # type: () -> CheckResult
        ansible_cfg = self.config.get("ansible", {})
        edge_host   = ansible_cfg.get("edge_host")
        ssh_user    = ansible_cfg.get("ssh_user", "hadoop")
        ssh_key     = ansible_cfg.get("ssh_key")
        hive_cfg    = self.config.get("hive", {})
        part_cfg    = self.config.get("checks", {}).get("hive_partitions", {})

        if not edge_host:
            return CheckResult(
                name="HivePartitionCheck",
                status=CheckResult.UNKNOWN,
                message="ansible.edge_host not configured"
            )

        ansible_bin = self._find_ansible()
        if not ansible_bin:
            return CheckResult(
                name="HivePartitionCheck",
                status=CheckResult.SKIPPED,
                message="ansible binary not found despite can_run() check"
            )

        inventory  = self._build_inventory(edge_host, ssh_user, ssh_key)
        databases  = list(part_cfg.get("databases") or [])
        max_parts  = int(part_cfg.get("max_partitions", 0))
        play_timeout = int(part_cfg.get("timeout", 300))
        kinit_cmd  = _build_kinit_cmd(hive_cfg)

        _debug.log("HivePartitionCheck",
                   "databases: {}  max_partitions: {}  timeout: {}s".format(
                       databases if databases else "auto-discover",
                       max_parts, play_timeout))

        # Step 1: auto-discover databases se non configurati
        if not databases:
            cmd = _build_db_discovery_cmd(hive_cfg, ssh_user)
            rc, out, err = self._run_playbook(
                ansible_bin, inventory, cmd,
                tag="HivePartitionCheck.discover_db",
                kinit_cmd=kinit_cmd
            )
            if rc != 0:
                return CheckResult(
                    name="HivePartitionCheck",
                    status=CheckResult.UNKNOWN,
                    message="Failed to list databases (rc={})".format(rc)
                )
            raw = _extract_stdout(out)
            databases = _parse_databases_output(raw)
            _debug.log("HivePartitionCheck",
                       "discovered: {}".format(databases))
            if not databases:
                return CheckResult(
                    name="HivePartitionCheck",
                    status=CheckResult.UNKNOWN,
                    message="No databases found — check beeline connectivity"
                )
            kinit_cmd = None  # già eseguito, non ripetere

        # Step 2: conteggio partizioni per tabella per ogni DB
        script = _build_partition_query_script(hive_cfg, databases, ssh_user)
        rc, out, err = self._run_playbook(
            ansible_bin, inventory, script,
            tag="HivePartitionCheck",
            kinit_cmd=kinit_cmd,
            timeout=play_timeout
        )
        if rc != 0:
            task_err = _extract_task_error(out)
            return CheckResult(
                name="HivePartitionCheck",
                status=CheckResult.UNKNOWN,
                message="Failed to get partition counts (rc={}): {}".format(
                    rc, task_err[:200])
            )

        raw = _extract_stdout(out)
        db_data = _parse_partition_output(raw)

        if not db_data:
            return CheckResult(
                name="HivePartitionCheck",
                status=CheckResult.UNKNOWN,
                message="No partition data returned — check beeline output"
            )

        # Calcola totali e verifica soglia
        over   = []   # type: list
        summary = {}  # type: dict
        for db_name, tables in sorted(db_data.items()):
            total = sum(tables.values())
            summary[db_name] = {"total_partitions": total, "tables": len(tables)}
            if max_parts > 0:
                for tbl, cnt in sorted(tables.items(), key=lambda x: -x[1]):
                    if cnt > max_parts:
                        over.append("{}.{}: {}".format(db_name, tbl, cnt))

        details = {"databases": summary}
        if over:
            details["over_threshold"] = over

        if over:
            over_preview = over[:5]
            lines = ["Tables exceeding {} partitions:".format(max_parts)]
            lines += ["  " + t for t in over_preview]
            if len(over) > 5:
                lines.append("  (+{} more)".format(len(over) - 5))
            return CheckResult(
                name="HivePartitionCheck",
                status=CheckResult.WARNING,
                message="\n".join(lines),
                details=details
            )

        db_summaries = [
            "{}: {} tables, {} partitions".format(k, v["tables"], v["total_partitions"])
            for k, v in sorted(summary.items())
        ]
        return CheckResult(
            name="HivePartitionCheck",
            status=CheckResult.OK,
            message="Hive partitions OK — {}".format("; ".join(db_summaries)),
            details=details
        )
