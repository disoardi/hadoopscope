"""Check YARN Resource Manager REST API."""

from __future__ import print_function

import json
import socket
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

from checks.base import CheckBase, CheckResult

DEFAULT_TIMEOUT = 10
DEFAULT_RM_PORT = 8088


def _rm_url(config):
    # type: (dict) -> tuple
    """
    Restituisce (url_or_None, is_auto) del YARN Resource Manager.
    Priorità: config[yarn][rm_urls][0] > config[yarn][rm_url] > auto-detect da ambari_url (HDP).
    Con rm_urls la lista viene provata in ordine; il 307 dal standby viene seguito via -L.
    Restituisce (None, True) se non configurabile — il check torna SKIPPED.
    """
    yarn_cfg = config.get("yarn", {})
    rm_urls = yarn_cfg.get("rm_urls", [])
    if rm_urls:
        return rm_urls[0].rstrip("/"), False
    if yarn_cfg.get("rm_url"):
        return yarn_cfg["rm_url"].rstrip("/"), False

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


def _yarn_get(base_url, path, timeout=DEFAULT_TIMEOUT, no_proxy=False, kerberos=False):
    # type: (str, str, int, bool, bool) -> dict
    url = "{}/ws/v1/cluster/{}".format(base_url, path.lstrip("/"))

    if kerberos:
        # -L segue il 307 redirect standby→active; --location-trusted
        # propaga il token SPNEGO anche verso il nuovo host (active RM)
        cmd = ["curl", "-s", "--fail", "--max-time", str(timeout),
               "--negotiate", "-u", ":", "-L", "--location-trusted",
               "-H", "Accept: application/json"]
        if no_proxy:
            cmd += ["--noproxy", "*"]
        cmd.append(url)
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.PIPE,
                                          timeout=timeout + 5)
            body = out.decode("utf-8")
            try:
                return json.loads(body)
            except ValueError:
                preview = body[:200].replace("\n", " ") if body else "<empty>"
                raise IOError("YARN: risposta non-JSON (body='{}'): {}".format(preview, url))
        except subprocess.CalledProcessError as e:
            stderr_out = e.stderr.decode("utf-8", errors="replace")[:200] if e.stderr else ""
            raise IOError("YARN HTTP error (curl exit {}{}) — {}".format(
                e.returncode,
                " stderr='{}'".format(stderr_out.strip()) if stderr_out else "",
                url))
        except subprocess.TimeoutExpired:
            raise IOError("YARN timeout ({}s) — {}".format(timeout, url))
        except OSError as e:
            raise IOError("YARN curl OSError: {} — {}".format(str(e), url))

    try:
        req = Request(url)
        req.add_header("Accept", "application/json")
        resp = _open_url(req, timeout=timeout, no_proxy=no_proxy)
        return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        raise IOError("YARN HTTP {}: {} — {}".format(e.code, e.reason, url))
    except URLError as e:
        raise IOError("YARN connection error: {} — {}".format(e.reason, url))
    except socket.timeout:
        raise IOError("YARN timeout ({}s) — {}".format(timeout, url))


class YarnNodeHealthCheck(CheckBase):
    """Controlla lo stato dei nodi YARN — segnala nodi UNHEALTHY o LOST.

    Config opzionale:
        yarn:
          decommissioned_nodes:         # nodi decommissionati da CM ma che YARN-RM
            - vmhost1.corp.com          # mostra come LOST invece di DECOMMISSIONED
            - vmhost2.corp.com          # (NodeManager stoppato da CM senza graceful
                                        #  YARN decommission signal).
                                        # Formato: hostname o hostname:porta.
                                        # LOST in questa lista → trattato come decommissionato (OK).
                                        # LOST NON in lista → CRITICAL (nodo davvero perso).
    """

    requires = []  # YARN RM REST, sempre disponibile

    @staticmethod
    def _in_decom_set(node_id, decom_set):
        # type: (str, set) -> bool
        """Controlla se node_id (hostname:port) è in decom_set (hostname o hostname:port)."""
        hostname = node_id.split(":")[0] if ":" in node_id else node_id
        return node_id in decom_set or hostname in decom_set

    def run(self):
        # type: () -> CheckResult
        base, is_auto = _rm_url(self.config)
        if base is None:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.SKIPPED,
                message="yarn.rm_url not configured — add yarn.rm_url to config"
            )
        no_proxy  = self.config.get("no_proxy", False)
        use_krb   = self.config.get("kerberos", {}).get("enabled", False)
        yarn_cfg  = self.config.get("yarn", {})
        decom_set = set(yarn_cfg.get("decommissioned_nodes", []))

        try:
            data = _yarn_get(base, "nodes", no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            msg = str(e)
            if is_auto:
                msg += " — Tip: set yarn.rm_url in config (auto-detected: {})".format(base)
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.UNKNOWN,
                message=msg
            )

        nodes = data.get("nodes", {}).get("node", [])
        if not nodes:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.UNKNOWN,
                message="No nodes returned by YARN RM (or cluster empty)"
            )

        unhealthy = [n["id"] for n in nodes if n.get("state") == "UNHEALTHY"]
        running   = [n["id"] for n in nodes if n.get("state") == "RUNNING"]

        # LOST: distingue nodi davvero persi da nodi stoppati da CM (LOST perché
        # il NodeManager è stato fermato senza graceful YARN decommission)
        lost_real  = [n["id"] for n in nodes
                      if n.get("state") == "LOST"
                      and not self._in_decom_set(n["id"], decom_set)]
        lost_decom = [n["id"] for n in nodes
                      if n.get("state") == "LOST"
                      and self._in_decom_set(n["id"], decom_set)]

        # Nodi che YARN stesso conosce come decommissionati + quelli stoppati da CM
        decommissioned = ([n["id"] for n in nodes
                           if n.get("state") in ("DECOMMISSIONED", "DECOMMISSIONING",
                                                  "SHUTDOWN", "REBOOTED")]
                          + lost_decom)

        details = {
            "total":          len(nodes),
            "running":        len(running),
            "unhealthy":      len(unhealthy),
            "lost":           len(lost_real),
            "decommissioned": len(decommissioned),
        }

        if lost_real:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.CRITICAL,
                message="{} LOST node(s): {}".format(
                    len(lost_real), ", ".join(lost_real[:5])),
                details=details
            )
        if unhealthy:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.WARNING,
                message="{} UNHEALTHY node(s): {}".format(
                    len(unhealthy), ", ".join(unhealthy[:5])),
                details=details
            )

        msg = "{}/{} nodes RUNNING".format(len(running), len(nodes))
        if decommissioned:
            msg += " ({} decommissioned)".format(len(decommissioned))
        return CheckResult(
            name="YarnNodeHealth",
            status=CheckResult.OK,
            message=msg,
            details=details
        )


class YarnQueueCheck(CheckBase):
    """Controlla utilizzo code YARN — WARNING se usedCapacity > soglia."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        base, is_auto = _rm_url(self.config)
        if base is None:
            return CheckResult(
                name="YarnQueues",
                status=CheckResult.SKIPPED,
                message="yarn.rm_url not configured — add yarn.rm_url to config"
            )
        no_proxy = self.config.get("no_proxy", False)
        use_krb  = self.config.get("kerberos", {}).get("enabled", False)
        yarn_cfg = self.config.get("checks", {}).get("yarn_queues", {})
        warn_pct = float(yarn_cfg.get("usage_warning_pct", 80))
        crit_pct = float(yarn_cfg.get("usage_critical_pct", 90))

        try:
            data = _yarn_get(base, "scheduler", no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            msg = str(e)
            if is_auto:
                msg += " — Tip: set yarn.rm_url in config (auto-detected: {})".format(base)
            return CheckResult(
                name="YarnQueues",
                status=CheckResult.UNKNOWN,
                message=msg
            )

        scheduler_info = data.get("scheduler", {}).get("schedulerInfo", {})
        issues = []

        def _check_queues(queues, parent=""):
            # type: (list, str) -> None
            for q in queues:
                name = q.get("queueName", "?")
                full_name = "{}/{}".format(parent, name) if parent else name
                used = float(q.get("usedCapacity", 0))
                if used >= crit_pct:
                    issues.append((CheckResult.CRITICAL, full_name, used))
                elif used >= warn_pct:
                    issues.append((CheckResult.WARNING, full_name, used))
                # Ricorsivo su code figlie
                child_queues = q.get("queues", {})
                if isinstance(child_queues, dict):
                    child_queues = child_queues.get("queue", [])
                if child_queues:
                    _check_queues(child_queues, full_name)

        root_queues = scheduler_info.get("queues", {})
        if isinstance(root_queues, dict):
            root_queues = root_queues.get("queue", [])
        if not root_queues:
            root_queues = [scheduler_info]  # CapacityScheduler ha root direttamente

        _check_queues(root_queues)

        if not issues:
            return CheckResult(
                name="YarnQueues",
                status=CheckResult.OK,
                message="All queues below usage threshold (warn={:.0f}%, crit={:.0f}%)".format(
                    warn_pct, crit_pct)
            )

        worst_status = CheckResult.CRITICAL if any(
            i[0] == CheckResult.CRITICAL for i in issues
        ) else CheckResult.WARNING

        msgs = ["{}: {:.1f}%".format(i[1], i[2]) for i in issues]
        return CheckResult(
            name="YarnQueues",
            status=worst_status,
            message="Queue usage issues: {}".format("; ".join(msgs)),
            details={"issues": [{"queue": i[1], "used_pct": i[2]} for i in issues]}
        )
