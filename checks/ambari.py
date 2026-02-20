"""Check Ambari REST API — HDP cluster health."""

from __future__ import print_function

import json
import socket

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
    from urllib.parse import urljoin
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd).encode()).decode()
        return "Basic {}".format(token)
except ImportError:
    # Python 2 fallback (non dovrebbe servire ma per sicurezza)
    from urllib2 import urlopen, Request, URLError, HTTPError
    from urlparse import urljoin
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd))
        return "Basic {}".format(token)

from checks.base import CheckBase, CheckResult


class AmbariClient(object):
    """Client HTTP minimale per Ambari REST API. Zero deps."""

    TIMEOUT = 10  # secondi

    def __init__(self, base_url, user, password, cluster_name, api_version="v1"):
        # type: (str, str, str, str, str) -> None
        self.base_url     = base_url.rstrip("/")
        self.auth_header  = _make_auth_header(user, password)
        self.cluster_name = cluster_name
        self.api_version  = api_version

    def get(self, path, params=None):
        # type: (str, dict) -> dict
        """GET request verso Ambari. Restituisce dict JSON."""
        url = "{}/api/{}/clusters/{}/{}".format(
            self.base_url, self.api_version, self.cluster_name, path.lstrip("/")
        )
        if params:
            from urllib.parse import urlencode
            url = "{}?{}".format(url, urlencode(params))

        req = Request(url)
        req.add_header("Authorization", self.auth_header)
        req.add_header("X-Requested-By", "hadoopscope")

        try:
            resp = urlopen(req, timeout=self.TIMEOUT)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            raise IOError("Ambari HTTP {}: {} — {}".format(e.code, e.reason, url))
        except URLError as e:
            raise IOError("Ambari connection error: {} — {}".format(e.reason, url))
        except socket.timeout:
            raise IOError("Ambari timeout ({}s) — {}".format(self.TIMEOUT, url))


def _make_ambari_client(config):
    # type: (dict) -> AmbariClient
    return AmbariClient(
        base_url     = config["ambari_url"],
        user         = config["ambari_user"],
        password     = config["ambari_pass"],
        cluster_name = config["cluster_name"],
        api_version  = config.get("ambari_api_version", "v1"),
    )


class AmbariServiceHealthCheck(CheckBase):
    """Controlla lo stato di tutti i servizi HDP via Ambari API."""

    requires = []  # sempre disponibile — pura API REST

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get("services?fields=ServiceInfo/state,ServiceInfo/service_name")
        except IOError as e:
            return CheckResult(
                name    = "AmbariServiceHealth",
                status  = CheckResult.UNKNOWN,
                message = str(e)
            )

        services   = data.get("items", [])
        stopped    = []
        not_started = []

        target_services = self.config.get("checks", {}).get(
            "service_health", {}
        ).get("services", [])

        for svc in services:
            info  = svc.get("ServiceInfo", {})
            name  = info.get("service_name", "?")
            state = info.get("state", "UNKNOWN")

            if target_services and name not in target_services:
                continue

            if state not in ("STARTED", "INSTALLED"):
                not_started.append("{} ({})".format(name, state))
            elif state == "INSTALLED":
                stopped.append(name)

        if not_started:
            return CheckResult(
                name    = "AmbariServiceHealth",
                status  = CheckResult.CRITICAL,
                message = "Services not running: {}".format(", ".join(not_started)),
                details = {"not_started": not_started}
            )
        if stopped:
            return CheckResult(
                name    = "AmbariServiceHealth",
                status  = CheckResult.WARNING,
                message = "Services installed but stopped: {}".format(", ".join(stopped)),
                details = {"stopped": stopped}
            )

        return CheckResult(
            name    = "AmbariServiceHealth",
            status  = CheckResult.OK,
            message = "All {} monitored services are STARTED".format(len(services)),
            details = {"service_count": len(services)}
        )


class ClusterAlertsCheck(CheckBase):
    """Raccoglie alert CRITICAL attivi via Ambari API."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get(
                "alerts?fields=*&Alert/state=CRITICAL&Alert/maintenance_state=OFF"
            )
        except IOError as e:
            return CheckResult(
                name="ClusterAlerts",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        items = data.get("items", [])
        if not items:
            return CheckResult(
                name="ClusterAlerts",
                status=CheckResult.OK,
                message="No active CRITICAL alerts"
            )

        summaries = []
        for item in items[:10]:
            alert = item.get("Alert", {})
            label = alert.get("label", alert.get("definition_name", "?"))
            host  = alert.get("host_name", "")
            summaries.append("{} ({})".format(label, host) if host else label)

        return CheckResult(
            name="ClusterAlerts",
            status=CheckResult.CRITICAL,
            message="{} CRITICAL alert(s): {}".format(
                len(items), "; ".join(summaries)),
            details={"count": len(items), "alerts": summaries}
        )


class ConfigStalenessCheck(CheckBase):
    """Verifica che non ci siano configurazioni stale non propagate ai nodi."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get(
                "services?fields=ServiceInfo/config_staleness_check_issues,"
                "ServiceInfo/service_name"
            )
        except IOError as e:
            return CheckResult(
                name="ConfigStaleness",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        items = data.get("items", [])
        stale = []
        for item in items:
            info   = item.get("ServiceInfo", {})
            name   = info.get("service_name", "?")
            issues = info.get("config_staleness_check_issues", [])
            if issues:
                stale.append(name)

        if stale:
            return CheckResult(
                name="ConfigStaleness",
                status=CheckResult.WARNING,
                message="Stale config on services: {}".format(", ".join(stale)),
                details={"stale_services": stale}
            )
        return CheckResult(
            name="ConfigStaleness",
            status=CheckResult.OK,
            message="All service configs propagated"
        )


class NameNodeHACheck(CheckBase):
    """Verifica stato HA NameNode (active/standby)."""

    requires = []  # API REST, sempre disponibile

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get(
                "services/HDFS/components/NAMENODE"
                "?fields=metrics/dfs/FSNamesystem/HAState,host_components/HostRoles/host_name"
            )
        except IOError as e:
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        host_components = data.get("host_components", [])
        active  = [hc["HostRoles"]["host_name"] for hc in host_components
                   if hc.get("metrics", {}).get("dfs", {}).get(
                       "FSNamesystem", {}).get("HAState") == "active"]
        standby = [hc["HostRoles"]["host_name"] for hc in host_components
                   if hc.get("metrics", {}).get("dfs", {}).get(
                       "FSNamesystem", {}).get("HAState") == "standby"]

        if len(active) == 1 and len(standby) >= 1:
            return CheckResult(
                name    = "NameNodeHA",
                status  = CheckResult.OK,
                message = "Active: {} | Standby: {}".format(active[0], ", ".join(standby)),
                details = {"active": active, "standby": standby}
            )
        elif len(active) == 0:
            return CheckResult(
                name   = "NameNodeHA",
                status = CheckResult.CRITICAL,
                message = "No active NameNode found! HA broken.",
                details = {"active": active, "standby": standby}
            )
        else:
            return CheckResult(
                name   = "NameNodeHA",
                status = CheckResult.WARNING,
                message = "HA state unclear: active={}, standby={}".format(active, standby),
                details = {"active": active, "standby": standby}
            )
