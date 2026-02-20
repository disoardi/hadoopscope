"""Check YARN Resource Manager REST API."""

from __future__ import print_function

import json
import socket

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib2 import urlopen, Request, URLError, HTTPError

from checks.base import CheckBase, CheckResult

DEFAULT_TIMEOUT = 10
DEFAULT_RM_PORT = 8088


def _rm_url(config):
    # type: (dict) -> str
    """
    Restituisce la URL del YARN Resource Manager.
    Priorità: config[yarn][rm_url] > costruita da ambari_url:8088.
    """
    yarn_cfg = config.get("yarn", {})
    if yarn_cfg.get("rm_url"):
        return yarn_cfg["rm_url"].rstrip("/")

    # Fallback: costruiamo dall'ambari_url sostituendo host e porta
    ambari_url = config.get("ambari_url", "http://localhost:8080")
    # Estrai schema + host dall'url ambari (ignoriamo la porta)
    try:
        if "://" in ambari_url:
            schema, rest = ambari_url.split("://", 1)
            host = rest.split("/")[0].split(":")[0]
        else:
            schema, host = "http", ambari_url.split("/")[0].split(":")[0]
        return "http://{}:{}".format(host, DEFAULT_RM_PORT)
    except Exception:
        return "http://localhost:{}".format(DEFAULT_RM_PORT)


def _yarn_get(base_url, path, timeout=DEFAULT_TIMEOUT):
    # type: (str, str, int) -> dict
    url = "{}/ws/v1/cluster/{}".format(base_url, path.lstrip("/"))
    try:
        req = Request(url)
        req.add_header("Accept", "application/json")
        resp = urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        raise IOError("YARN HTTP {}: {} — {}".format(e.code, e.reason, url))
    except URLError as e:
        raise IOError("YARN connection error: {} — {}".format(e.reason, url))
    except socket.timeout:
        raise IOError("YARN timeout ({}s) — {}".format(timeout, url))


class YarnNodeHealthCheck(CheckBase):
    """Controlla lo stato dei nodi YARN — segnala nodi UNHEALTHY o LOST."""

    requires = []  # YARN RM REST, sempre disponibile

    def run(self):
        # type: () -> CheckResult
        base = _rm_url(self.config)
        try:
            data = _yarn_get(base, "nodes")
        except IOError as e:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        nodes = data.get("nodes", {}).get("node", [])
        if not nodes:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.UNKNOWN,
                message="No nodes returned by YARN RM (or cluster empty)"
            )

        unhealthy = [n["id"] for n in nodes if n.get("state") == "UNHEALTHY"]
        lost      = [n["id"] for n in nodes if n.get("state") == "LOST"]
        running   = [n["id"] for n in nodes if n.get("state") == "RUNNING"]

        details = {
            "total": len(nodes),
            "running": len(running),
            "unhealthy": len(unhealthy),
            "lost": len(lost),
        }

        if lost:
            return CheckResult(
                name="YarnNodeHealth",
                status=CheckResult.CRITICAL,
                message="{} LOST node(s): {}".format(len(lost), ", ".join(lost[:5])),
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
        return CheckResult(
            name="YarnNodeHealth",
            status=CheckResult.OK,
            message="{} nodes RUNNING".format(len(running)),
            details=details
        )


class YarnQueueCheck(CheckBase):
    """Controlla utilizzo code YARN — WARNING se usedCapacity > soglia."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        base = _rm_url(self.config)
        yarn_cfg = self.config.get("checks", {}).get("yarn_queues", {})
        warn_pct = float(yarn_cfg.get("usage_warning_pct", 80))
        crit_pct = float(yarn_cfg.get("usage_critical_pct", 90))

        try:
            data = _yarn_get(base, "scheduler")
        except IOError as e:
            return CheckResult(
                name="YarnQueues",
                status=CheckResult.UNKNOWN,
                message=str(e)
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
