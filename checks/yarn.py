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


def _cm_decommissioned_hosts(config, timeout=DEFAULT_TIMEOUT):
    # type: (dict, int) -> set
    """Query Cloudera Manager /api/{version}/hosts for DECOMMISSIONED hosts.

    Returns a set of lowercase hostnames so that YARN nodes that appear as
    LOST (because CM stopped the NodeManager without a graceful YARN
    decommission signal) can be automatically recognised as decommissioned —
    without requiring an explicit yarn.decommissioned_nodes list.

    Returns an empty set if cm_url is absent, CM is unreachable, or any
    error occurs (always safe to call; failures are silently ignored).
    """
    cm_url = config.get("cm_url", "").rstrip("/")
    if not cm_url:
        return set()

    cm_user   = config.get("cm_user", "")
    cm_pass   = config.get("cm_pass", "")
    cm_api    = config.get("cm_api_version", "v40")
    url       = "{}/api/{}/hosts".format(cm_url, cm_api)

    try:
        import base64 as _b64
        token = _b64.b64encode(
            "{}:{}".format(cm_user, cm_pass).encode()
        ).decode()
        auth = "Basic {}".format(token)
    except Exception:
        return set()

    no_proxy = config.get("no_proxy", False)
    try:
        req = Request(url)
        req.add_header("Authorization", auth)
        req.add_header("Accept", "application/json")
        resp = _open_url(req, timeout=timeout, no_proxy=no_proxy)
        data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        # CM unreachable / wrong credentials / SSL error → fall back gracefully
        return set()

    decom = set()  # type: set
    for h in data.get("items", []):
        if h.get("commissionState") == "DECOMMISSIONED":
            hostname = h.get("hostname", "").lower()
            if hostname:
                decom.add(hostname)
    return decom


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


def _yarn_get(base_url, path, timeout=DEFAULT_TIMEOUT, no_proxy=False,
              kerberos=False, full_path=False):
    # type: (str, str, int, bool, bool, bool) -> dict
    """GET REST verso YARN. Se full_path=False (default, comportamento
    esistente), il path è relativo a /ws/v1/cluster/. Se full_path=True,
    'path' è già l'URL completo (usato per l'Application History/Timeline
    Server, che ha un prefisso diverso)."""
    url = path if full_path else "{}/ws/v1/cluster/{}".format(base_url, path.lstrip("/"))

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

        # Manual list from config (always respected)
        decom_set = set(yarn_cfg.get("decommissioned_nodes", []))

        # Auto-detect from CM for CDP: query /api/v{x}/hosts commissionState
        # LOST nodes whose hostname matches a CM-DECOMMISSIONED host are treated
        # as decommissioned — no manual list needed on CDP environments.
        if self.config.get("cm_url"):
            decom_set |= _cm_decommissioned_hosts(self.config)

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


class YarnClusterMetricsCheck(CheckBase):
    """Metriche generali del cluster YARN — app in esecuzione/pending,
    memoria allocata/disponibile. Puramente informativo per la dashboard
    'at a glance' (Home): nessuna soglia, sempre OK se raggiungibile —
    non è un check di salute, è un check di CHECK_CATEGORIES "yarn"
    a scopo pubblicitario di stato, riusa lo stesso _rm_url/_yarn_get
    delle altre YARN check."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        base, is_auto = _rm_url(self.config)
        if base is None:
            return CheckResult(
                name="YarnClusterMetrics",
                status=CheckResult.SKIPPED,
                message="yarn.rm_url not configured — add yarn.rm_url to config"
            )
        no_proxy = self.config.get("no_proxy", False)
        use_krb  = self.config.get("kerberos", {}).get("enabled", False)

        try:
            data = _yarn_get(base, "metrics", no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            msg = str(e)
            if is_auto:
                msg += " — Tip: set yarn.rm_url in config (auto-detected: {})".format(base)
            return CheckResult(
                name="YarnClusterMetrics",
                status=CheckResult.UNKNOWN,
                message=msg
            )

        m = data.get("clusterMetrics", {})
        details = {
            "appsRunning":   m.get("appsRunning", 0),
            "appsPending":   m.get("appsPending", 0),
            "totalMB":       m.get("totalMB", 0),
            "allocatedMB":   m.get("allocatedMB", 0),
            "availableMB":   m.get("availableMB", 0),
        }
        message = "{} running, {} pending — {}/{} MB allocated".format(
            details["appsRunning"], details["appsPending"],
            details["allocatedMB"], details["totalMB"])
        return CheckResult(
            name="YarnClusterMetrics",
            status=CheckResult.OK,
            message=message,
            details=details
        )
