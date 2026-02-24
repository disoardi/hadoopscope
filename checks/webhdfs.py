"""Check HDFS via WebHDFS REST API — nessun client Hadoop richiesto.

Autenticazione:
  - simple auth (default): usa il parametro ?user.name=hdfs nell'URL
  - Kerberos (kerberos.enabled: true): chiama kinit -kt keytab principal
    poi usa curl --negotiate per SPNEGO (HTTP Negotiate)

Configurazione esempio per ambiente kerberizzato:
  kerberos:
    enabled: true
    keytab: "${KEYTAB_PATH}"         # es. /etc/security/keytabs/monitor.keytab
    principal: monitor@CORP.COM      # principal del keytab
"""

from __future__ import print_function

import json
import socket
import os
import subprocess

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


def _curl_get_json(url, negotiate=False, timeout=DEFAULT_TIMEOUT, no_proxy=False):
    # type: (str, bool, int, bool) -> dict
    """GET JSON via curl. Con negotiate=True usa SPNEGO (Kerberos)."""
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
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


def _curl_put_webhdfs(base_url, path, content, timeout=DEFAULT_TIMEOUT, no_proxy=False):
    # type: (str, str, bytes, int, bool) -> None
    """PUT (CREATE) su WebHDFS via curl --negotiate. Segue il redirect 307 → DataNode."""
    url = "{}/webhdfs/v1{}?op=CREATE&overwrite=true".format(
        base_url.rstrip("/"), path
    )
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
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


def _curl_delete_webhdfs(base_url, path, timeout=DEFAULT_TIMEOUT, no_proxy=False):
    # type: (str, str, int, bool) -> None
    """DELETE su WebHDFS via curl --negotiate."""
    url = "{}/webhdfs/v1{}?op=DELETE".format(base_url.rstrip("/"), path)
    cmd = ["curl", "-s", "--fail", "--max-time", str(timeout)]
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
                 timeout=DEFAULT_TIMEOUT, kerberos=False, no_proxy=False):
    # type: (str, str, str, str, str, int, bool, bool) -> dict
    """
    GET WebHDFS. Se kerberos=True usa curl --negotiate (SPNEGO).
    Se kerberos=False usa user.name nell'URL (simple auth).
    no_proxy=True bypassa il proxy HTTP di sistema.
    """
    if kerberos:
        url = "{}/webhdfs/v1{}?op={}{}".format(
            base_url.rstrip("/"), path, op, extra_params
        )
        return _curl_get_json(url, negotiate=True, timeout=timeout, no_proxy=no_proxy)

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
    """Legge la configurazione Kerberos. Restituisce (enabled, keytab, principal)."""
    krb = config.get("kerberos", {})
    enabled   = krb.get("enabled", False)
    keytab    = krb.get("keytab", "")
    principal = krb.get("principal", "")
    return enabled, keytab, principal


# ---------------------------------------------------------------------------
# Check classes
# ---------------------------------------------------------------------------

class HdfsSpaceCheck(CheckBase):
    """Controlla utilizzo spazio HDFS per path configurati.

    Simple auth: nessun requisito — usa ?user.name=hdfs
    Kerberos:    richiede kinit + curl nel PATH
    """

    requires = []  # sempre eligible; fallisce con UNKNOWN se gli strumenti mancano

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg  = self.config.get("webhdfs", {})
        base_url  = hdfs_cfg.get("url", "")
        user      = hdfs_cfg.get("user", "hdfs")
        no_proxy  = self.config.get("no_proxy", False)
        paths_cfg = self.config.get("checks", {}).get("hdfs_space", {}).get("paths", [])

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
        if use_krb:
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
                data    = _webhdfs_get(base_url, path, "GETCONTENTSUMMARY",
                                       user, kerberos=use_krb, no_proxy=no_proxy)
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

            except IOError:
                issues.append((CheckResult.UNKNOWN, path, 0, 0, 0))

        if not issues:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.OK,
                message="All {} paths within thresholds".format(len(paths_cfg)),
                details=details
            )

        worst = max(issues, key=lambda x: (
            0 if x[0] == CheckResult.UNKNOWN else
            1 if x[0] == CheckResult.WARNING else 2
        ))
        status = worst[0]
        msgs = ["{}: {:.0f}% ({})".format(i[1], i[2], "CRITICAL" if i[0] == CheckResult.CRITICAL else i[0])
                for i in issues]
        return CheckResult(
            name="HdfsSpace",
            status=status,
            message="; ".join(msgs),
            details=details
        )


class HdfsDataNodeCheck(CheckBase):
    """Controlla DataNodes morti via JMX NameNode.

    Nota: il JMX HTTP endpoint del NameNode non e' protetto da Kerberos
    in configurazione standard (hadoop.security.instrumentation.requires.login=false).
    Se il tuo cluster imposta quella proprieta' a true, abilita kerberos anche qui.
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg = self.config.get("webhdfs", {})
        base_url = hdfs_cfg.get("url", "")
        no_proxy = self.config.get("no_proxy", False)

        if not base_url:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        # JMX endpoint: usa namenode_url se configurato (necessario quando webhdfs.url
        # punta a HttpFS, porta 14000/14001, che non espone JMX).
        # Se omesso, si usa webhdfs.url (corretto quando punta direttamente al NameNode).
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

        try:
            if use_krb:
                # JMX tipicamente non richiede SPNEGO, ma lo usiamo per coerenza
                # se kerberos.enabled=true nel config
                try:
                    _kinit(keytab, principal)
                except IOError as e:
                    return CheckResult(
                        name="HdfsDataNodes",
                        status=CheckResult.UNKNOWN,
                        message="Kerberos init failed: {}".format(str(e))
                    )
                data = _curl_get_json(jmx_url, negotiate=True, no_proxy=no_proxy)
            else:
                resp = _open_url(Request(jmx_url), timeout=DEFAULT_TIMEOUT, no_proxy=no_proxy)
                data = json.loads(resp.read().decode("utf-8"))

            beans    = data.get("beans", [{}])
            nn_state = beans[0] if beans else {}

            dead  = nn_state.get("NumDeadDataNodes", 0)
            live  = nn_state.get("NumLiveDataNodes", 0)
            stale = nn_state.get("NumStaleDataNodes", 0)

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
            elif dead >= warn_thresh:
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
    """Testa scrittura/lettura/cancellazione su HDFS via WebHDFS.

    Simple auth: usa ?user.name=hdfs (PUT → redirect 307 → DataNode)
    Kerberos:    usa curl --negotiate -L per seguire il redirect con SPNEGO
    """

    requires = []

    TEST_FILE_CONTENT = b"hadoopscope-probe"

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg  = self.config.get("webhdfs", {})
        base_url  = hdfs_cfg.get("url", "")
        user      = hdfs_cfg.get("user", "hdfs")
        no_proxy  = self.config.get("no_proxy", False)
        test_path = self.config.get("checks", {}).get(
            "hdfs_writability", {}).get("test_path", "/tmp/.hadoopscope-probe")

        if not base_url:
            return CheckResult(
                name="HdfsWritability",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        use_krb, keytab, principal = _get_kerberos_cfg(self.config)
        if use_krb:
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

            if use_krb:
                _curl_put_webhdfs(base_url, test_path_ts, self.TEST_FILE_CONTENT,
                                  no_proxy=no_proxy)
                _curl_delete_webhdfs(base_url, test_path_ts, no_proxy=no_proxy)
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
