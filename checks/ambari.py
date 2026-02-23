"""Check Ambari REST API — HDP cluster health."""

from __future__ import print_function

import json
import socket

try:
    from urllib.request import urlopen, Request, build_opener, ProxyHandler
    from urllib.error import URLError, HTTPError
    from urllib.parse import urljoin
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd).encode()).decode()
        return "Basic {}".format(token)
except ImportError:
    # Python 2 fallback
    from urllib2 import urlopen, Request, build_opener, ProxyHandler, URLError, HTTPError
    from urlparse import urljoin
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd))
        return "Basic {}".format(token)


def _open_url(req, timeout, no_proxy=False):
    # type: (Request, int, bool) -> object
    """Open URL, optionally bypassing system HTTP proxy."""
    if no_proxy:
        return build_opener(ProxyHandler({})).open(req, timeout=timeout)
    return urlopen(req, timeout=timeout)

from checks.base import CheckBase, CheckResult


class AmbariClient(object):
    """Client HTTP minimale per Ambari REST API. Zero deps."""

    TIMEOUT = 10  # secondi

    def __init__(self, base_url, user, password, cluster_name,
                 api_version="v1", no_proxy=False):
        # type: (str, str, str, str, str, bool) -> None
        self.base_url     = base_url.rstrip("/")
        self.auth_header  = _make_auth_header(user, password)
        self.cluster_name = cluster_name
        self.api_version  = api_version
        self.no_proxy     = no_proxy

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
            resp = _open_url(req, self.TIMEOUT, no_proxy=self.no_proxy)
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
        no_proxy     = config.get("no_proxy", False),
    )


class AmbariServiceHealthCheck(CheckBase):
    """Controlla lo stato di tutti i servizi HDP via Ambari API.

    Logica INSTALLED:
    - Se l'utente configura un filtro 'services' esplicito, INSTALLED e' WARNING
      (quei servizi sono attesi running).
    - Senza filtro, INSTALLED e' ignorato — falso positivo per le librerie client
      HDP (PIG, TEZ, SQOOP, SLIDER, ...) che sono sempre INSTALLED per design.
      Usare 'warn_installed: true' nel config per abilitarlo.

    Maintenance mode: servizi con maintenance_state ON o IMPLIED_FROM_HOST sono
    sempre esclusi dal controllo.
    """

    requires = []  # sempre disponibile — pura API REST

    # Librerie/framework HDP normalmente in stato INSTALLED (non hanno demoni attivi)
    _CLIENT_SERVICES = frozenset(["PIG", "SQOOP", "SLIDER", "TEZ", "MAHOUT"])

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get(
                "services?fields=ServiceInfo/state,"
                "ServiceInfo/service_name,"
                "ServiceInfo/maintenance_state"
            )
        except IOError as e:
            return CheckResult(
                name    = "AmbariServiceHealth",
                status  = CheckResult.UNKNOWN,
                message = str(e)
            )

        services = data.get("items", [])
        stopped  = []
        not_started = []

        svc_cfg         = self.config.get("checks", {}).get("service_health", {})
        target_services = svc_cfg.get("services", [])

        # Warna su INSTALLED solo se filtro esplicito o warn_installed: true
        warn_installed = bool(target_services) or svc_cfg.get("warn_installed", False)

        for svc in services:
            info        = svc.get("ServiceInfo", {})
            name        = info.get("service_name", "?")
            state       = info.get("state", "UNKNOWN")
            maintenance = info.get("maintenance_state", "OFF")

            if target_services and name not in target_services:
                continue

            # Salta servizi in maintenance mode
            if maintenance in ("ON", "IMPLIED_FROM_HOST"):
                continue

            if state not in ("STARTED", "INSTALLED"):
                not_started.append("{} ({})".format(name, state))
            elif state == "INSTALLED" and warn_installed:
                # Le librerie client sono sempre INSTALLED, non segnalare
                if name not in self._CLIENT_SERVICES:
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

        active_count = len([s for s in services
                            if s.get("ServiceInfo", {}).get("maintenance_state", "OFF")
                            not in ("ON", "IMPLIED_FROM_HOST")])
        return CheckResult(
            name    = "AmbariServiceHealth",
            status  = CheckResult.OK,
            message = "All {} monitored services are STARTED".format(active_count),
            details = {"service_count": active_count}
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

        # Raggruppa per label: {label: [short_host, ...]}
        groups = {}  # type: dict
        for item in items:
            alert = item.get("Alert", {})
            label = alert.get("label", alert.get("definition_name", "?"))
            host  = alert.get("host_name", "")
            # Abbrevia FQDN: prendi solo il primo componente (hdslsep040.corp.com → hdslsep040)
            short = host.split(".")[0] if host else ""
            if label not in groups:
                groups[label] = []
            if short:
                groups[label].append(short)

        summaries = []
        for label, hosts in sorted(groups.items()):
            if not hosts:
                summaries.append(label)
            elif len(hosts) == 1:
                summaries.append("{} ({})".format(label, hosts[0]))
            else:
                summaries.append("{} x{} ({})".format(
                    label, len(hosts), ", ".join(hosts[:6])))

        return CheckResult(
            name="ClusterAlerts",
            status=CheckResult.CRITICAL,
            message="{} CRITICAL alert(s): {}".format(
                len(items), "; ".join(summaries)),
            details={"count": len(items), "alerts": summaries}
        )


class ConfigStalenessCheck(CheckBase):
    """Verifica che non ci siano configurazioni stale non propagate ai nodi.

    Compatibilita':
    - Ambari 2.7+: usa ServiceInfo/config_staleness_check_issues
    - Ambari 2.6.x: fallback automatico su ServiceInfo/config_state == STALE_CONFIGS
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
        except (KeyError, TypeError) as e:
            return CheckResult(
                name="ConfigStaleness",
                status=CheckResult.UNKNOWN,
                message="Config error: {}".format(e)
            )

        # Prova campo Ambari 2.7+ (config_staleness_check_issues)
        staleness_field = "config_staleness_check_issues"
        try:
            data = client.get(
                "services?fields=ServiceInfo/config_staleness_check_issues,"
                "ServiceInfo/service_name"
            )
        except IOError as e:
            if "HTTP 400" in str(e):
                # Ambari < 2.7: config_staleness_check_issues non supportato
                # Prova fallback con config_state (Ambari 2.x)
                staleness_field = "config_state"
                try:
                    data = client.get(
                        "services?fields=ServiceInfo/config_state,"
                        "ServiceInfo/service_name"
                    )
                except IOError as e2:
                    if "HTTP 400" in str(e2):
                        # Ambari 2.6.x: nessuno dei due campi è supportato
                        return CheckResult(
                            name="ConfigStaleness",
                            status=CheckResult.SKIPPED,
                            message="Config staleness not available on this Ambari version "
                                    "(requires Ambari 2.7+ for config_staleness_check_issues "
                                    "or Ambari 2.x with config_state field)"
                        )
                    return CheckResult(
                        name="ConfigStaleness",
                        status=CheckResult.UNKNOWN,
                        message=str(e2)
                    )
            else:
                return CheckResult(
                    name="ConfigStaleness",
                    status=CheckResult.UNKNOWN,
                    message=str(e)
                )

        items = data.get("items", [])
        stale = []
        for item in items:
            info = item.get("ServiceInfo", {})
            name = info.get("service_name", "?")
            if staleness_field == "config_state":
                if info.get("config_state") == "STALE_CONFIGS":
                    stale.append(name)
            else:
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
    """Verifica stato HA NameNode (active/standby).

    Usa HostRoles/ha_state — campo diretto sull'host component, non dipende da
    AMS/metrics collector. Compatibile con Ambari 2.x+.

    Se il cluster non ha HA abilitata (single NN), ritorna OK con nota.
    """

    requires = []  # API REST, sempre disponibile

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_ambari_client(self.config)
            data   = client.get(
                "services/HDFS/components/NAMENODE"
                "?fields=host_components/HostRoles/host_name,"
                "host_components/HostRoles/ha_state,"
                "host_components/HostRoles/state"
            )
        except IOError as e:
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        host_components = data.get("host_components", [])
        if not host_components:
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.UNKNOWN,
                message="No NameNode components found in Ambari"
            )

        active        = []
        standby       = []
        running_no_ha = []  # NN started ma ha_state assente (cluster non-HA)

        for hc in host_components:
            roles    = hc.get("HostRoles", {})
            host     = roles.get("host_name", "?")
            ha_state = (roles.get("ha_state") or "").lower()
            nn_state = roles.get("state", "")

            if ha_state == "active":
                active.append(host)
            elif ha_state == "standby":
                standby.append(host)
            elif nn_state == "STARTED":
                # NN in esecuzione ma HA non abilitata (o ha_state non ancora sync)
                running_no_ha.append(host)

        # ha_state non disponibile (Ambari 2.6.x o HA non ancora sync)
        if not active and not standby and running_no_ha:
            if len(running_no_ha) >= 2:
                # Se l'utente dichiara esplicitamente ha_enabled: true nel config
                # (Ambari 2.6.x non espone ha_state), fidarsi e restituire OK.
                ha_cfg = self.config.get("checks", {}).get("namenode_ha", {})
                if ha_cfg.get("ha_enabled", False):
                    return CheckResult(
                        name    = "NameNodeHA",
                        status  = CheckResult.OK,
                        message = "{} NameNodes STARTED, HA assumed OK (ha_state not "
                                  "available from Ambari — set by config)".format(
                                      len(running_no_ha)),
                        details = {"hosts": running_no_ha, "ha_state_available": False,
                                   "ha_enabled": True}
                    )
                # ha_enabled non configurato: WARNING perche' non possiamo verificare
                return CheckResult(
                    name    = "NameNodeHA",
                    status  = CheckResult.WARNING,
                    message = (
                        "HA state undetermined: {} NameNodes STARTED but ha_state "
                        "not available from Ambari (likely OK — verify manually). "
                        "Tip: add 'namenode_ha.ha_enabled: true' to config to suppress."
                    ).format(len(running_no_ha)),
                    details = {"hosts": running_no_ha, "ha_state_available": False}
                )
            # Singolo NN, nessun ha_state → cluster non-HA
            return CheckResult(
                name    = "NameNodeHA",
                status  = CheckResult.OK,
                message = "NameNode running (non-HA): {}".format(", ".join(running_no_ha)),
                details = {"hosts": running_no_ha, "ha_enabled": False}
            )

        # HA OK: esattamente 1 active, >=1 standby
        if len(active) == 1 and len(standby) >= 1:
            return CheckResult(
                name    = "NameNodeHA",
                status  = CheckResult.OK,
                message = "Active: {} | Standby: {}".format(active[0], ", ".join(standby)),
                details = {"active": active, "standby": standby}
            )

        # Split-brain: piu' di 1 active
        if len(active) > 1:
            return CheckResult(
                name   = "NameNodeHA",
                status = CheckResult.CRITICAL,
                message = "HA split-brain: {} active NameNodes: {}".format(
                    len(active), ", ".join(active)),
                details = {"active": active, "standby": standby}
            )

        # HA abilitata ma nessun active trovato
        if standby:
            return CheckResult(
                name   = "NameNodeHA",
                status = CheckResult.CRITICAL,
                message = "No active NameNode! Standby only: {}".format(", ".join(standby)),
                details = {"active": active, "standby": standby}
            )

        # ha_state assente su tutti e nessun NN in STARTED
        return CheckResult(
            name   = "NameNodeHA",
            status = CheckResult.UNKNOWN,
            message = "NameNode HA state unavailable — check if HDFS service is STARTED"
        )
