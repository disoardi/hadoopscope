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


def _hdfs_cfg(config):
    # type: (dict) -> dict
    """Legge la sezione HDFS dal config accettando sia 'hdfs:' che 'webhdfs:'.
    'hdfs:' ha la precedenza; 'webhdfs:' è l'alias legacy retrocompatibile.
    """
    return config.get("hdfs") or config.get("webhdfs") or {}


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
    whdfs_krb = _hdfs_cfg(config).get("kerberos", {})
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

        # Estrae r.stdout dall'output del task debug ansible.
        # Caso 1: r.stdout è una stringa quotata (stdout testuale)
        m = re.search(r'"r\.stdout":\s*"((?:[^"\\]|\\.)*)"', out)
        if m:
            raw = m.group(1)
            return raw.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        # Caso 2: r.stdout è un oggetto/array JSON (Ansible lo ha auto-parsato quando
        # il comando ha emesso JSON valido).  Riconverti a stringa perché i caller
        # usano json.loads(stdout).
        m2 = re.search(r'"r\.stdout":\s*(\{|\[)', out)
        if m2:
            try:
                obj, _ = json.JSONDecoder().raw_decode(out, m2.start(1))
                return json.dumps(obj)
            except ValueError:
                pass
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
# Utility
# ---------------------------------------------------------------------------

def _human(n):
    # type: (int) -> str
    """Converte bytes in stringa human-readable (PB/TB/GB/MB/KB/B)."""
    for unit, threshold in [("PB", 1024**5), ("TB", 1024**4),
                             ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)]:
        if n >= threshold:
            return "{:.1f} {}".format(n / float(threshold), unit)
    return "{} B".format(n)


_STATUS_RANK = {
    CheckResult.OK:       0,
    CheckResult.SKIPPED:  1,
    CheckResult.UNKNOWN:  2,
    CheckResult.WARNING:  3,
    CheckResult.CRITICAL: 4,
}


# ---------------------------------------------------------------------------
# Check classes
# ---------------------------------------------------------------------------

class HdfsSpaceCheck(CheckBase):
    """Controlla utilizzo spazio HDFS.

    1. Capacità globale (sempre se possibile):
       CapacityUsed / CapacityTotal dal JMX NameNode (FSNamesystemState bean).
       Richiede webhdfs.namenode_url se webhdfs.url punta a HttpFS (porta 14000/14001).

    2. Quote per path (opzionale):
       GETCONTENTSUMMARY via WebHDFS/HttpFS — attivo solo se checks.hdfs_space.paths
       è configurato E i path hanno quota HDFS impostata (spaceQuota > 0).
       I path senza quota vengono saltati silenziosamente.

    Config:
      checks:
        hdfs_space:
          warning_pct: 80    # soglia WARNING globale (default 75%)
          critical_pct: 90   # soglia CRITICAL globale (default 90%)
          paths:             # opzionale — solo per path con quota HDFS configurata
            - path: /user/etl
              warning_pct: 85
              critical_pct: 95
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg    = _hdfs_cfg(self.config)
        base_url    = hdfs_cfg.get("url", "")
        user        = hdfs_cfg.get("user", "hdfs")
        no_proxy    = self.config.get("no_proxy", False)
        insecure    = hdfs_cfg.get("ssl_insecure", False)
        via_ansible = hdfs_cfg.get("via_ansible", False)
        space_cfg   = self.config.get("checks", {}).get("hdfs_space", {})
        warn_pct    = float(space_cfg.get("warning_pct", 75))
        crit_pct    = float(space_cfg.get("critical_pct", 90))
        paths_cfg   = space_cfg.get("paths", [])

        if not base_url:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        use_krb, keytab, principal         = _get_kerberos_cfg(self.config)
        ansi_krb, ansi_keytab, ansi_principal = _get_ansible_kerberos_cfg(self.config)
        kinit_line    = ("kinit -kt {} {}\n".format(ansi_keytab, ansi_principal)
                         if (ansi_krb and ansi_keytab and ansi_principal) else "")
        insecure_flag = "--insecure" if insecure else ""

        # Kinit locale una sola volta (se non via_ansible)
        if not via_ansible and use_krb:
            try:
                _kinit(keytab, principal)
            except IOError as e:
                return CheckResult(
                    name="HdfsSpace",
                    status=CheckResult.UNKNOWN,
                    message="Kerberos init failed: {}".format(str(e))
                )

        # ----------------------------------------------------------------
        # 1. Capacità globale via JMX NameNode
        # ----------------------------------------------------------------
        # namenode_urls (list, HA) > namenode_url (single) > base_url (fallback)
        _nn_urls_raw = hdfs_cfg.get("namenode_urls") or []
        if not _nn_urls_raw and hdfs_cfg.get("namenode_url"):
            _nn_urls_raw = [hdfs_cfg["namenode_url"]]
        namenode_urls = [u.rstrip("/") for u in _nn_urls_raw] if _nn_urls_raw else None

        is_httpfs     = ":14000" in base_url or ":14001" in base_url
        global_status = CheckResult.OK  # type: str
        global_msg    = ""
        global_details = {}             # type: dict

        if is_httpfs and not namenode_urls:
            global_status = CheckResult.SKIPPED
            global_msg    = (
                "Global HDFS capacity unavailable — webhdfs.url points to HttpFS "
                "(port 14000/14001). Add webhdfs.namenode_url (or namenode_urls) to enable."
            )
        else:
            if not namenode_urls:
                namenode_urls = [base_url.rstrip("/")]

            data          = None
            jmx_errors    = []
            used_nn_url   = None
            for nn_url in namenode_urls:
                jmx_base = nn_url.replace("/webhdfs/v1", "")
                jmx_url  = "{}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState".format(
                    jmx_base)
                try:
                    if via_ansible:
                        script = "{}curl -s --fail {} --negotiate -u : '{}'".format(
                            kinit_line, insecure_flag, jmx_url)
                        stdout = _run_ansible_curl(self.config, script, tag="HdfsSpace/JMX")
                        data   = json.loads(stdout)
                    elif use_krb:
                        data = _curl_get_json(jmx_url, negotiate=True,
                                              no_proxy=no_proxy, insecure=insecure)
                    else:
                        resp = _open_url(Request(jmx_url), timeout=DEFAULT_TIMEOUT,
                                         no_proxy=no_proxy)
                        data = json.loads(resp.read().decode("utf-8"))
                    used_nn_url = nn_url
                    break  # success — stop trying
                except Exception as e:
                    jmx_errors.append("{}: {}".format(nn_url, str(e)))

            if data is None:
                global_status = CheckResult.UNKNOWN
                global_msg    = "JMX error (tried {} NN): {}".format(
                    len(namenode_urls), "; ".join(jmx_errors))
            else:
                beans     = data.get("beans", [{}])
                nn        = beans[0] if beans else {}
                total     = nn.get("CapacityTotal", 0)
                used      = nn.get("CapacityUsed", 0)
                remaining = nn.get("CapacityRemaining", 0)

                if total <= 0:
                    global_status = CheckResult.UNKNOWN
                    global_msg    = "CapacityTotal=0 in JMX (NameNode not yet initialized?)"
                else:
                    pct = (used / float(total)) * 100
                    global_details = {
                        "capacity_total_bytes":     total,
                        "capacity_used_bytes":      used,
                        "capacity_remaining_bytes": remaining,
                        "used_pct":                 round(pct, 1),
                    }
                    if len(namenode_urls) > 1:
                        global_details["namenode_url_used"] = used_nn_url
                    global_msg = "HDFS used: {} / {} ({:.1f}%)".format(
                        _human(used), _human(total), pct)
                    if pct >= crit_pct:
                        global_status = CheckResult.CRITICAL
                        global_msg += " — CRITICAL (threshold: {:.0f}%)".format(crit_pct)
                    elif pct >= warn_pct:
                        global_status = CheckResult.WARNING
                        global_msg += " — WARNING (threshold: {:.0f}%)".format(warn_pct)
                    else:
                        global_status = CheckResult.OK

        # Nessun path configurato → ritorna solo risultato globale
        if not paths_cfg:
            return CheckResult(
                name="HdfsSpace",
                status=global_status,
                message=global_msg,
                details=global_details
            )

        # ----------------------------------------------------------------
        # 2. Quote per path (opzionale)
        # ----------------------------------------------------------------
        quota_issues  = []  # type: list
        quota_details = {}  # type: dict

        for path_cfg in paths_cfg:
            path   = path_cfg["path"]
            p_warn = float(path_cfg.get("warning_pct", 75))
            p_crit = float(path_cfg.get("critical_pct", 90))
            try:
                if via_ansible:
                    url    = "{}/webhdfs/v1{}?op=GETCONTENTSUMMARY".format(
                        base_url.rstrip("/"), path)
                    script = "{}curl -s --fail {} --negotiate -u : '{}'".format(
                        kinit_line, insecure_flag, url)
                    stdout = _run_ansible_curl(
                        self.config, script, tag="HdfsSpace[{}]".format(path))
                    data   = json.loads(stdout)
                else:
                    data = _webhdfs_get(base_url, path, "GETCONTENTSUMMARY",
                                        user, kerberos=use_krb, no_proxy=no_proxy,
                                        insecure=insecure)

                summary = data.get("ContentSummary", {})
                used    = summary.get("spaceConsumed", 0)
                quota   = summary.get("spaceQuota", -1)

                if quota <= 0:
                    # Nessuna quota HDFS su questo path — skip silenzioso
                    quota_details[path] = {"used": _human(used), "quota": "none"}
                    continue

                pct = (used / float(quota)) * 100
                quota_details[path] = {
                    "used_bytes":  used,
                    "quota_bytes": quota,
                    "used_pct":    round(pct, 1),
                }
                if pct >= p_crit:
                    quota_issues.append((CheckResult.CRITICAL, path, pct))
                elif pct >= p_warn:
                    quota_issues.append((CheckResult.WARNING, path, pct))

            except Exception as e:
                quota_issues.append((CheckResult.UNKNOWN, path, 0))
                quota_details[path] = {"error": str(e)}

        # Combina risultati globale + quote
        global_details["path_quotas"] = quota_details

        quota_status = CheckResult.OK
        quota_part   = ""
        if quota_issues:
            quota_status = max(
                (i[0] for i in quota_issues),
                key=lambda s: _STATUS_RANK.get(s, 0)
            )
            quota_part = " | Quota issues: " + "; ".join(
                "{}: {:.0f}% ({})".format(i[1], i[2], i[0])
                for i in quota_issues
            )

        final_status = (
            global_status
            if _STATUS_RANK.get(global_status, 0) >= _STATUS_RANK.get(quota_status, 0)
            else quota_status
        )
        return CheckResult(
            name="HdfsSpace",
            status=final_status,
            message=global_msg + quota_part,
            details=global_details
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
        hdfs_cfg    = _hdfs_cfg(self.config)
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

        # JMX endpoint: namenode_urls (list, HA) > namenode_url (single) > base_url (fallback).
        # Necessario quando webhdfs.url punta a HttpFS (porta 14000/14001) che non espone JMX.
        _nn_urls_raw = hdfs_cfg.get("namenode_urls") or []
        if not _nn_urls_raw and hdfs_cfg.get("namenode_url"):
            _nn_urls_raw = [hdfs_cfg["namenode_url"]]
        namenode_urls = [u.rstrip("/") for u in _nn_urls_raw] if _nn_urls_raw else None

        is_httpfs = ":14000" in base_url or ":14001" in base_url
        if is_httpfs and not namenode_urls:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.SKIPPED,
                message=(
                    "webhdfs.url points to HttpFS (port 14000/14001) which has no JMX endpoint. "
                    "Add webhdfs.namenode_url (or namenode_urls) pointing to the NameNode directly "
                    "(e.g. https://namenode.host:9871)"
                )
            )
        if not namenode_urls:
            namenode_urls = [base_url.rstrip("/")]

        use_krb, keytab, principal = _get_kerberos_cfg(self.config)
        ansi_krb, ansi_keytab, ansi_principal = _get_ansible_kerberos_cfg(self.config)

        _debug.log("HdfsDataNodes", "insecure={} via_ansible={} use_krb={} namenodes={}".format(
            insecure, via_ansible, use_krb, namenode_urls))

        kinit_line    = "kinit -kt {} {}\n".format(ansi_keytab, ansi_principal) \
            if (ansi_krb and ansi_keytab and ansi_principal) else ""
        insecure_flag = "--insecure" if insecure else ""

        try:
            data       = None
            jmx_errors = []
            for nn_url in namenode_urls:
                jmx_base = nn_url.replace("/webhdfs/v1", "")
                jmx_url  = "{}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState".format(
                    jmx_base)
                try:
                    if via_ansible:
                        script = "{}curl -s --fail {} --negotiate -u : '{}'".format(
                            kinit_line, insecure_flag, jmx_url)
                        stdout = _run_ansible_curl(self.config, script, tag="HdfsDataNodes")
                        data   = json.loads(stdout)
                    elif use_krb:
                        if data is None:  # kinit only once
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
                        resp = _open_url(Request(jmx_url), timeout=DEFAULT_TIMEOUT,
                                         no_proxy=no_proxy)
                        data = json.loads(resp.read().decode("utf-8"))
                    break  # success
                except Exception as e:
                    jmx_errors.append("{}: {}".format(nn_url, str(e)))

            if data is None:
                raise IOError("JMX error (tried {} NN): {}".format(
                    len(namenode_urls), "; ".join(jmx_errors)))

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
        hdfs_cfg    = _hdfs_cfg(self.config)
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
                    "set -e\n"
                    "{kinit}"
                    "PUT_HTTP=$(curl -s {ins} --negotiate -u : "
                    "-X PUT -L --location-trusted "
                    "-H 'Content-Type: application/octet-stream' "
                    "--data-binary 'hadoopscope-probe' "
                    "-w '%{{http_code}}' -o /dev/null '{create}')\n"
                    "echo \"WebHDFS CREATE HTTP:$PUT_HTTP\"\n"
                    "[ \"$PUT_HTTP\" -ge 200 ] && [ \"$PUT_HTTP\" -lt 300 ]\n"
                    "DEL_HTTP=$(curl -s {ins} --negotiate -u : "
                    "-X DELETE -w '%{{http_code}}' -o /dev/null '{delete}')\n"
                    "echo \"WebHDFS DELETE HTTP:$DEL_HTTP\"\n"
                    "[ \"$DEL_HTTP\" -ge 200 ] && [ \"$DEL_HTTP\" -lt 300 ]"
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
