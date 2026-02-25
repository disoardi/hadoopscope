"""Check HDFS via WebHDFS REST API — nessun client Hadoop richiesto.

Autenticazione:
  - simple auth (default): usa il parametro ?user.name=hdfs nell'URL
  - Kerberos (kerberos.enabled: true): chiama kinit -kt keytab principal
    poi usa curl --negotiate per SPNEGO (HTTP Negotiate)

Modalità di esecuzione (webhdfs.via_ansible):
  - false (default): le curl vengono eseguite dalla macchina che lancia hadoopscope.
    Richiede raggiungibilità di WebHDFS/NameNode JMX dalla macchina locale.
  - true: le curl vengono delegate all'edge node via Ansible.
    Utile quando WebHDFS/NameNode non sono raggiungibili dalla macchina locale
    (firewall, routing, VPN). In questo caso i path keytab (kerberos.keytab)
    devono esistere sull'edge node, non sulla macchina locale.

SSL:
  - webhdfs.ssl_insecure: true → aggiunge --insecure a tutti i curl WebHDFS.
    Usare solo in ambienti con CA interna non installata nel trust store di sistema.
"""

from __future__ import print_function

import json
import os
import re
import socket
import subprocess
import tempfile

try:
    from urllib.request import urlopen, Request, build_opener, ProxyHandler
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib2 import urlopen, Request, build_opener, ProxyHandler, URLError, HTTPError


def _open_url(req, timeout, no_proxy=False):
    # type: (Request, int, bool) -> object
    """Open URL, optionally bypassing system HTTP proxy."""
    if no_proxy:
        return build_opener(ProxyHandler({})).open(req, timeout=timeout)
    return urlopen(req, timeout=timeout)


def _make_request(url, method="GET", data=None):
    # type: (str, str, bytes) -> Request
    """Crea Request con metodo HTTP esplicito (Python 2/3 compatible).
    urllib invia POST quando data!=None — serve override per PUT/DELETE.
    """
    req = Request(url, data=data)
    req.get_method = lambda: method
    return req


from checks.base import CheckBase, CheckResult
import debug as _debug

DEFAULT_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Kerberos helpers (stdlib — kinit + curl)
# ---------------------------------------------------------------------------

def _kinit(keytab, principal, timeout=30):
    # type: (str, str, int) -> None
    """Ottieni ticket Kerberos dal keytab. Raises IOError se kinit fallisce."""
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
        raise IOError("kinit non trovato nel PATH — installa krb5-user (Debian) o krb5-workstation (RHEL)")


def _curl_get_json(url, negotiate=False, timeout=DEFAULT_TIMEOUT, no_proxy=False, insecure=False):
    # type: (str, bool, int, bool, bool) -> dict
    """GET JSON via curl. negotiate=True usa SPNEGO. insecure=True disabilita SSL verify."""
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
    if insecure:
        cmd.append("--insecure")
    if no_proxy:
        cmd += ["--noproxy", "*"]
    if negotiate:
        cmd += ["--negotiate", "-u", ":"]
    cmd.append(url)
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL,
                                      timeout=timeout + 5)
        return json.loads(out.decode("utf-8"))
    except subprocess.CalledProcessError as e:
        raise IOError("curl HTTP error (exit {}): {}".format(e.returncode, url))
    except subprocess.TimeoutExpired:
        raise IOError("curl timeout ({}s): {}".format(timeout, url))
    except OSError:
        raise IOError("curl non trovato nel PATH — installa curl")


def _curl_put_webhdfs(base_url, path, content, timeout=DEFAULT_TIMEOUT, no_proxy=False, insecure=False):
    # type: (str, str, bytes, int, bool, bool) -> None
    """PUT (CREATE) su WebHDFS via curl --negotiate. Segue redirect 307 → DataNode."""
    url = "{}/webhdfs/v1{}?op=CREATE&overwrite=true".format(
        base_url.rstrip("/"), path
    )
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
    if insecure:
        cmd.append("--insecure")
    if no_proxy:
        cmd += ["--noproxy", "*"]
    cmd += ["--negotiate", "-u", ":",
            "-X", "PUT", "-L",
            "-H", "Content-Type: application/octet-stream",
            "--data-binary", "@-",
            url]
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        proc.communicate(input=content, timeout=timeout)
        if proc.returncode != 0:
            raise IOError("WebHDFS CREATE fallito (exit {})".format(proc.returncode))
    except subprocess.TimeoutExpired:
        proc.kill()
        raise IOError("WebHDFS CREATE timeout ({}s)".format(timeout))
    except OSError:
        raise IOError("curl non trovato nel PATH")


def _curl_delete_webhdfs(base_url, path, timeout=DEFAULT_TIMEOUT, no_proxy=False, insecure=False):
    # type: (str, str, int, bool, bool) -> None
    """DELETE su WebHDFS via curl --negotiate."""
    url = "{}/webhdfs/v1{}?op=DELETE".format(base_url.rstrip("/"), path)
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
    if insecure:
        cmd.append("--insecure")
    if no_proxy:
        cmd += ["--noproxy", "*"]
    cmd += ["--negotiate", "-u", ":",
            "-X", "DELETE",
            url]
    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, timeout=timeout)
    except subprocess.CalledProcessError:
        raise IOError("WebHDFS DELETE fallito: {}".format(path))
    except subprocess.TimeoutExpired:
        raise IOError("WebHDFS DELETE timeout ({}s)".format(timeout))
    except OSError:
        raise IOError("curl non trovato nel PATH")


# ---------------------------------------------------------------------------
# WebHDFS HTTP helper (simple auth / Kerberos)
# ---------------------------------------------------------------------------

def _webhdfs_get(base_url, path, op, user, extra_params="",
                 timeout=DEFAULT_TIMEOUT, kerberos=False, no_proxy=False, insecure=False):
    # type: (str, str, str, str, str, int, bool, bool, bool) -> dict
    """
    GET WebHDFS. kerberos=True usa curl --negotiate (SPNEGO).
    kerberos=False usa user.name nell'URL (simple auth).
    no_proxy=True bypassa il proxy HTTP di sistema.
    insecure=True disabilita verifica certificato SSL (--insecure).
    """
    if kerberos:
        url = "{}/webhdfs/v1{}?op={}{}".format(
            base_url.rstrip("/"), path, op, extra_params
        )
        return _curl_get_json(url, negotiate=True, timeout=timeout,
                              no_proxy=no_proxy, insecure=insecure)

    url = "{}/webhdfs/v1{}?op={}&user.name={}{}".format(
        base_url.rstrip("/"), path, op, user, extra_params
    )
    try:
        resp = _open_url(Request(url), timeout=timeout, no_proxy=no_proxy)
        return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        raise IOError("WebHDFS HTTP {}: {}".format(e.code, e.reason))
    except URLError as e:
        raise IOError("WebHDFS connection error: {}".format(e.reason))
    except socket.timeout:
        raise IOError("WebHDFS timeout ({}s)".format(timeout))


def _get_kerberos_cfg(config):
    # type: (dict) -> tuple
    """Legge la configurazione Kerberos locale. Restituisce (enabled, keytab, principal).
    Usato per i curl eseguiti LOCALMENTE (via_ansible=false).
    """
    krb = config.get("kerberos", {})
    enabled   = krb.get("enabled", False)
    keytab    = krb.get("keytab", "")
    principal = krb.get("principal", "")
    return enabled, keytab, principal


def _get_ansible_kerberos_cfg(config):
    # type: (dict) -> tuple
    """Kerberos config per via_ansible: usa webhdfs.kerberos se presente,
    altrimenti ricade su kerberos top-level.

    webhdfs.kerberos.keytab/principal → path/principal SULL'EDGE NODE.
    Usare quando il keytab sull'edge node è in un path diverso da kerberos.keytab.
    Se webhdfs.kerberos non è configurato, usa kerberos.keytab (top-level).
    """
    whdfs_krb = config.get("webhdfs", {}).get("kerberos", {})
    if whdfs_krb.get("keytab"):
        use_krb   = config.get("kerberos", {}).get("enabled", True)
        keytab    = whdfs_krb["keytab"]
        principal = whdfs_krb.get("principal", "")
        return use_krb, keytab, principal
    return _get_kerberos_cfg(config)


# ---------------------------------------------------------------------------
# Ansible helpers (via_ansible mode)
# ---------------------------------------------------------------------------

def _find_ansible_bin():
    # type: () -> str
    """Trova ansible-playbook nel PATH o nel venv hadoopscope."""
    import shutil
    b = shutil.which("ansible-playbook")
    if b:
        return b
    venv = os.path.expanduser("~/.hadoopscope/venv/bin/ansible-playbook")
    return venv if os.path.exists(venv) else ""


def _build_webhdfs_inventory(ansible_cfg):
    # type: (dict) -> str
    """Costruisce la stringa inventory Ansible per l'edge node."""
    edge_host = ansible_cfg.get("edge_host", "")
    if edge_host in ("localhost", "127.0.0.1", "::1"):
        return "localhost ansible_connection=local"
    ssh_user = ansible_cfg.get("ssh_user", "root")
    ssh_key  = ansible_cfg.get("ssh_key", "~/.ssh/id_rsa")
    return "{} ansible_user={} ansible_ssh_private_key_file={}".format(
        edge_host, ssh_user, ssh_key)


def _run_ansible_curl(config, shell_script, tag="WebHDFS", timeout=60):
    # type: (dict, str, str, int) -> str
    """
    Esegue uno script shell sull'edge node via Ansible e restituisce stdout.

    Usato quando webhdfs.via_ansible=true. I path keytab nello script
    devono essere validi sull'edge node (non sulla macchina locale).
    Restituisce lo stdout dell'ultimo comando dello script.
    """
    ansible_bin = _find_ansible_bin()
    if not ansible_bin:
        raise IOError("ansible-playbook non trovato — installa ansible o esegui bootstrap.py")

    ansible_cfg = config.get("ansible", {})
    if not ansible_cfg.get("edge_host"):
        raise IOError("via_ansible=true ma ansible.edge_host non configurato")

    inventory = _build_webhdfs_inventory(ansible_cfg)
    indented  = "\n".join("        " + line for line in shell_script.splitlines())
    playbook  = (
        "---\n"
        "- name: WebHDFS check\n"
        "  hosts: all\n"
        "  gather_facts: false\n"
        "  tasks:\n"
        "    - name: run\n"
        "      shell: |\n"
        "{}\n"
        "      register: r\n"
        "    - debug: var=r.stdout\n"
    ).format(indented)

    inv_path = play_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.ini', delete=False, prefix='hs_whdfs_'
        ) as f:
            f.write(inventory)
            inv_path = f.name
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yml', delete=False, prefix='hs_whdfs_'
        ) as f:
            f.write(playbook)
            play_path = f.name

        _debug.log(tag, "via_ansible playbook: {}".format(play_path))
        _debug.section(tag, "playbook content")
        _debug.log(tag, playbook, multiline=True)

        env = os.environ.copy()
        env["ANSIBLE_HOST_KEY_CHECKING"] = "False"
        proc = subprocess.Popen(
            [ansible_bin, "-i", inv_path, play_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        out = stdout.decode("utf-8", errors="replace")
        err = stderr.decode("utf-8", errors="replace")
        _debug.log(tag, "rc: {}".format(proc.returncode))
        _debug.section(tag, "ansible stdout")
        _debug.log(tag, out if out.strip() else "(empty)", multiline=True)
        if err.strip():
            _debug.section(tag, "ansible stderr")
            _debug.log(tag, err, multiline=True)

        if proc.returncode != 0:
            m = re.search(r"FAILED! => (\{.*\})", out)
            if m:
                try:
                    data = json.loads(m.group(1))
                    parts = [x for x in [data.get("stderr"), data.get("stdout"),
                                         data.get("msg")] if x]
                    raise IOError("Ansible WebHDFS failed: {}".format(
                        " | ".join(str(p) for p in parts)[:400]))
                except (ValueError, KeyError):
                    pass
            raise IOError("Ansible WebHDFS failed (rc={}): {}".format(
                proc.returncode, out[-300:]))

        # Estrae r.stdout dall'output del task debug ansible
        m = re.search(r'"r\.stdout":\s*"((?:[^"\\]|\\.)*)"', out)
        if m:
            raw = m.group(1)
            return raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return out

    except subprocess.TimeoutExpired:
        raise IOError("Ansible WebHDFS timeout ({}s)".format(timeout))
    except IOError:
        raise
    except Exception as e:
        raise IOError("Ansible WebHDFS unexpected error: {}".format(str(e)))
    finally:
        for p in (inv_path, play_path):
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Check classes
# ---------------------------------------------------------------------------

class HdfsSpaceCheck(CheckBase):
    """Controlla utilizzo spazio HDFS per path configurati.

    Simple auth: nessun requisito — usa ?user.name=hdfs
    Kerberos:    richiede kinit + curl nel PATH
    via_ansible: esegue curl sull'edge node (utile se WebHDFS non è raggiungibile localmente)
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg    = self.config.get("webhdfs", {})
        base_url    = hdfs_cfg.get("url", "")
        user        = hdfs_cfg.get("user", "hdfs")
        no_proxy    = self.config.get("no_proxy", False)
        insecure    = hdfs_cfg.get("ssl_insecure", False)
        via_ansible = hdfs_cfg.get("via_ansible", False)
        paths_cfg   = self.config.get("checks", {}).get("hdfs_space", {}).get("paths", [])

        if not base_url:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )
        if not paths_cfg:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.SKIPPED,
                message="No paths configured — add checks.hdfs_space.paths to config"
            )

        use_krb, keytab, principal = _get_kerberos_cfg(self.config)
        ansi_krb, ansi_keytab, ansi_principal = _get_ansible_kerberos_cfg(self.config)
        if not via_ansible and use_krb:
            try:
                _kinit(keytab, principal)
            except IOError as e:
                return CheckResult(
                    name="HdfsSpace",
                    status=CheckResult.UNKNOWN,
                    message="Kerberos init failed: {}".format(str(e))
                )

        issues  = []
        details = {}

        for path_cfg in paths_cfg:
            path     = path_cfg["path"]
            warn_pct = path_cfg.get("warning_pct", 75)
            crit_pct = path_cfg.get("critical_pct", 90)
            try:
                if via_ansible:
                    kinit_line    = "kinit -kt {} {}\n".format(ansi_keytab, ansi_principal) \
                        if (ansi_krb and ansi_keytab and ansi_principal) else ""
                    insecure_flag = "--insecure" if insecure else ""
                    url    = "{}/webhdfs/v1{}?op=GETCONTENTSUMMARY".format(
                        base_url.rstrip("/"), path)
                    script = "{}curl -s --fail {} --negotiate -u : '{}'".format(
                        kinit_line, insecure_flag, url)
                    stdout = _run_ansible_curl(
                        self.config, script,
                        tag="HdfsSpace[{}]".format(path))
                    data = json.loads(stdout)
                else:
                    data = _webhdfs_get(base_url, path, "GETCONTENTSUMMARY",
                                        user, kerberos=use_krb, no_proxy=no_proxy,
                                        insecure=insecure)

                summary = data.get("ContentSummary", {})
                used    = summary.get("spaceConsumed", 0)
                quota   = summary.get("spaceQuota", -1)

                if quota <= 0:
                    details[path] = {"used": used, "quota": "none"}
                    continue

                pct = (used / float(quota)) * 100
                details[path] = {
                    "used_bytes": used,
                    "quota_bytes": quota,
                    "used_pct": round(pct, 1)
                }
                if pct >= crit_pct:
                    issues.append((CheckResult.CRITICAL, path, pct, used, quota))
                elif pct >= warn_pct:
                    issues.append((CheckResult.WARNING, path, pct, used, quota))

            except (IOError, ValueError):
                issues.append((CheckResult.UNKNOWN, path, 0, 0, 0))

        if not issues:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.OK,
                message="All {} paths within thresholds".format(len(paths_cfg)),
                details=details
            )

        worst  = max(issues, key=lambda x: (
            0 if x[0] == CheckResult.UNKNOWN else
            1 if x[0] == CheckResult.WARNING else 2
        ))
        status = worst[0]
        msgs   = ["{}: {:.0f}% ({})".format(
            i[1], i[2], "CRITICAL" if i[0] == CheckResult.CRITICAL else i[0])
            for i in issues]
        return CheckResult(
            name="HdfsSpace",
            status=status,
            message="; ".join(msgs),
            details=details
        )


class HdfsDataNodeCheck(CheckBase):
    """Controlla DataNodes morti via JMX NameNode.

    Nota: il JMX HTTP endpoint del NameNode non è protetto da Kerberos
    in configurazione standard. Se hadoop.security.instrumentation.requires.login=true,
    abilita kerberos nel config.
    via_ansible: esegue la query JMX sull'edge node (utile se il NameNode non è raggiungibile localmente)
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg    = self.config.get("webhdfs", {})
        base_url    = hdfs_cfg.get("url", "")
        no_proxy    = self.config.get("no_proxy", False)
        insecure    = hdfs_cfg.get("ssl_insecure", False)
        via_ansible = hdfs_cfg.get("via_ansible", False)

        if not base_url:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        # JMX endpoint: usa namenode_url se configurato (necessario quando webhdfs.url
        # punta a HttpFS, porta 14000/14001, che non espone JMX).
        namenode_url = hdfs_cfg.get("namenode_url") or base_url
        if not hdfs_cfg.get("namenode_url") and (":14000" in base_url or ":14001" in base_url):
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.SKIPPED,
                message=(
                    "webhdfs.url points to HttpFS (port 14000/14001) which has no JMX endpoint. "
                    "Add webhdfs.namenode_url pointing to the NameNode directly "
                    "(e.g. https://namenode.host:9871)"
                )
            )
        jmx_base = namenode_url.replace("/webhdfs/v1", "").rstrip("/")
        jmx_url  = "{}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState".format(jmx_base)

        use_krb, keytab, principal = _get_kerberos_cfg(self.config)
        ansi_krb, ansi_keytab, ansi_principal = _get_ansible_kerberos_cfg(self.config)

        _debug.log("HdfsDataNodes", "insecure={} via_ansible={} use_krb={}".format(
            insecure, via_ansible, use_krb))

        try:
            if via_ansible:
                kinit_line    = "kinit -kt {} {}\n".format(ansi_keytab, ansi_principal) \
                    if (ansi_krb and ansi_keytab and ansi_principal) else ""
                insecure_flag = "--insecure" if insecure else ""
                script = "{}curl -s --fail {} --negotiate -u : '{}'".format(
                    kinit_line, insecure_flag, jmx_url)
                stdout = _run_ansible_curl(self.config, script, tag="HdfsDataNodes")
                data   = json.loads(stdout)
            elif use_krb:
                try:
                    _kinit(keytab, principal)
                except IOError as e:
                    return CheckResult(
                        name="HdfsDataNodes",
                        status=CheckResult.UNKNOWN,
                        message="Kerberos init failed: {}".format(str(e))
                    )
                data = _curl_get_json(jmx_url, negotiate=True, no_proxy=no_proxy,
                                      insecure=insecure)
            else:
                resp = _open_url(Request(jmx_url), timeout=DEFAULT_TIMEOUT, no_proxy=no_proxy)
                data = json.loads(resp.read().decode("utf-8"))

            beans    = data.get("beans", [{}])
            nn_state = beans[0] if beans else {}
            dead     = nn_state.get("NumDeadDataNodes", 0)
            live     = nn_state.get("NumLiveDataNodes", 0)
            stale    = nn_state.get("NumStaleDataNodes", 0)

            warn_thresh = self.config.get("checks", {}).get(
                "hdfs_dead_datanodes", {}).get("warning_threshold", 1)
            crit_thresh = self.config.get("checks", {}).get(
                "hdfs_dead_datanodes", {}).get("critical_threshold", 3)

            details = {"live": live, "dead": dead, "stale": stale}

            if dead >= crit_thresh:
                return CheckResult(
                    name="HdfsDataNodes",
                    status=CheckResult.CRITICAL,
                    message="{} dead DataNodes (threshold: {})".format(dead, crit_thresh),
                    details=details
                )
            if dead >= warn_thresh:
                return CheckResult(
                    name="HdfsDataNodes",
                    status=CheckResult.WARNING,
                    message="{} dead DataNodes".format(dead),
                    details=details
                )
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.OK,
                message="{} live, {} dead, {} stale DataNodes".format(live, dead, stale),
                details=details
            )

        except Exception as e:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.UNKNOWN,
                message="JMX error: {}".format(str(e))
            )


class HdfsWritabilityCheck(CheckBase):
    """Testa scrittura/cancellazione su HDFS via WebHDFS.

    Simple auth: usa ?user.name=hdfs (PUT → redirect 307 → DataNode)
    Kerberos:    usa curl --negotiate -L per seguire il redirect con SPNEGO
    via_ansible: esegue PUT+DELETE sull'edge node (utile se WebHDFS non è raggiungibile localmente)
    """

    requires = []

    TEST_FILE_CONTENT = b"hadoopscope-probe"

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg    = self.config.get("webhdfs", {})
        base_url    = hdfs_cfg.get("url", "")
        user        = hdfs_cfg.get("user", "hdfs")
        no_proxy    = self.config.get("no_proxy", False)
        insecure    = hdfs_cfg.get("ssl_insecure", False)
        via_ansible = hdfs_cfg.get("via_ansible", False)
        test_path   = self.config.get("checks", {}).get(
            "hdfs_writability", {}).get("test_path", "/tmp/.hadoopscope-probe")

        if not base_url:
            return CheckResult(
                name="HdfsWritability",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        timeout  = int(hdfs_cfg.get("timeout", DEFAULT_TIMEOUT))
        use_krb, keytab, principal = _get_kerberos_cfg(self.config)
        ansi_krb, ansi_keytab, ansi_principal = _get_ansible_kerberos_cfg(self.config)

        _debug.log("HdfsWritability", "insecure={} via_ansible={} use_krb={} timeout={}".format(
            insecure, via_ansible, use_krb, timeout))

        if not via_ansible and use_krb:
            try:
                _kinit(keytab, principal)
            except IOError as e:
                return CheckResult(
                    name="HdfsWritability",
                    status=CheckResult.UNKNOWN,
                    message="Kerberos init failed: {}".format(str(e))
                )

        try:
            import time
            test_path_ts = "{}-{}".format(test_path, int(time.time()))

            if via_ansible:
                kinit_line    = "kinit -kt {} {}\n".format(ansi_keytab, ansi_principal) \
                    if (ansi_krb and ansi_keytab and ansi_principal) else ""
                insecure_flag = "--insecure" if insecure else ""
                create_url = "{}/webhdfs/v1{}?op=CREATE&overwrite=true".format(
                    base_url.rstrip("/"), test_path_ts)
                delete_url = "{}/webhdfs/v1{}?op=DELETE".format(
                    base_url.rstrip("/"), test_path_ts)
                script = (
                    "{kinit}"
                    "echo 'hadoopscope-probe' | curl -s --fail {ins} --negotiate -u : "
                    "-X PUT -L --location-trusted "
                    "-H 'Content-Type: application/octet-stream' "
                    "--data-binary '@-' '{create}'\n"
                    "curl -s --fail {ins} --negotiate -u : -X DELETE '{delete}'"
                ).format(kinit=kinit_line, ins=insecure_flag,
                         create=create_url, delete=delete_url)
                _run_ansible_curl(self.config, script, tag="HdfsWritability",
                                  timeout=timeout)

            elif use_krb:
                _curl_put_webhdfs(base_url, test_path_ts, self.TEST_FILE_CONTENT,
                                  timeout=timeout, no_proxy=no_proxy, insecure=insecure)
                _curl_delete_webhdfs(base_url, test_path_ts, timeout=timeout,
                                     no_proxy=no_proxy, insecure=insecure)
            else:
                # Simple auth: urllib + gestione redirect 307
                # WebHDFS CREATE richiede HTTP PUT (non POST) — serve get_method override
                create_url = "{}/webhdfs/v1{}?op=CREATE&overwrite=true&user.name={}".format(
                    base_url.rstrip("/"), test_path_ts, user
                )
                try:
                    # Step 1: PUT a NameNode (no body) → 307 redirect verso DataNode
                    _open_url(_make_request(create_url, "PUT", data=b""),
                              timeout=DEFAULT_TIMEOUT, no_proxy=no_proxy)
                except HTTPError as e:
                    if e.code == 307:
                        location = e.headers.get("Location", "")
                        if location:
                            # Step 2: PUT a DataNode con il contenuto del file
                            _open_url(_make_request(location, "PUT",
                                                    data=self.TEST_FILE_CONTENT),
                                      timeout=DEFAULT_TIMEOUT, no_proxy=no_proxy)
                        else:
                            raise
                    else:
                        raise
                # WebHDFS DELETE richiede HTTP DELETE (non GET)
                del_url = "{}/webhdfs/v1{}?op=DELETE&user.name={}".format(
                    base_url.rstrip("/"), test_path_ts, user
                )
                _open_url(_make_request(del_url, "DELETE"),
                          timeout=DEFAULT_TIMEOUT, no_proxy=no_proxy)

            return CheckResult(
                name="HdfsWritability",
                status=CheckResult.OK,
                message="HDFS write/delete test passed"
            )

        except Exception as e:
            return CheckResult(
                name="HdfsWritability",
                status=CheckResult.CRITICAL,
                message="HDFS write test failed: {}".format(str(e))
            )
