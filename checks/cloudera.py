"""Check Cloudera Manager REST API — CDP cluster health."""

from __future__ import print_function

import json
import socket
import ssl

try:
    from urllib.request import urlopen, Request, build_opener, ProxyHandler, HTTPSHandler
    from urllib.error import URLError, HTTPError
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode(
            "{}:{}".format(user, passwd).encode()
        ).decode()
        return "Basic {}".format(token)
except ImportError:
    from urllib2 import urlopen, Request, build_opener, ProxyHandler, HTTPSHandler, URLError, HTTPError
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd))
        return "Basic {}".format(token)


def _cm_open(req, timeout, no_proxy=False, ssl_insecure=False):
    # type: (Request, int, bool, bool) -> object
    """Open CM API request, optionally bypassing system HTTP proxy and/or
    skipping TLS certificate verification (Auto-TLS/self-signed CM)."""
    handlers = []
    if no_proxy:
        handlers.append(ProxyHandler({}))
    if ssl_insecure:
        handlers.append(HTTPSHandler(context=ssl._create_unverified_context()))
    if handlers:
        return build_opener(*handlers).open(req, timeout=timeout)
    return urlopen(req, timeout=timeout)

from checks.base import CheckBase, CheckResult

TIMEOUT = 10


class ClouderaClient(object):
    """Client HTTP minimale per Cloudera Manager REST API. Zero deps."""

    def __init__(self, base_url, user, password, cluster_name, api_version="v40",
                 no_proxy=False, ssl_insecure=False):
        # type: (str, str, str, str, str, bool, bool) -> None
        self.base_url     = base_url.rstrip("/")
        self.auth_header  = _make_auth_header(user, password)
        self.cluster_name = cluster_name
        self.api_version  = api_version
        self.no_proxy     = no_proxy
        self.ssl_insecure = ssl_insecure

    def get(self, path):
        # type: (str) -> dict
        url = "{}/api/{}/clusters/{}/{}".format(
            self.base_url, self.api_version, self.cluster_name, path.lstrip("/")
        )
        req = Request(url)
        req.add_header("Authorization", self.auth_header)
        req.add_header("Accept", "application/json")
        try:
            resp = _cm_open(req, timeout=TIMEOUT, no_proxy=self.no_proxy,
                            ssl_insecure=self.ssl_insecure)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            raise IOError("CM HTTP {}: {} — {}".format(e.code, e.reason, url))
        except URLError as e:
            raise IOError("CM connection error: {} — {}".format(e.reason, url))
        except socket.timeout:
            raise IOError("CM timeout ({}s) — {}".format(TIMEOUT, url))

    def get_raw(self, path):
        # type: (str) -> dict
        """GET senza prefisso cluster — per endpoint globali."""
        url = "{}/api/{}/{}".format(self.base_url, self.api_version, path.lstrip("/"))
        req = Request(url)
        req.add_header("Authorization", self.auth_header)
        req.add_header("Accept", "application/json")
        try:
            resp = _cm_open(req, timeout=TIMEOUT, no_proxy=self.no_proxy,
                            ssl_insecure=self.ssl_insecure)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            raise IOError("CM HTTP {}: {} — {}".format(e.code, e.reason, url))
        except URLError as e:
            raise IOError("CM connection error: {} — {}".format(e.reason, url))
        except socket.timeout:
            raise IOError("CM timeout ({}s) — {}".format(TIMEOUT, url))


def _make_cm_client(config):
    # type: (dict) -> ClouderaClient
    return ClouderaClient(
        base_url     = config["cm_url"],
        user         = config["cm_user"],
        password     = config["cm_pass"],
        cluster_name = config["cluster_name"],
        api_version  = config.get("cm_api_version", "v40"),
        no_proxy     = config.get("no_proxy", False),
        ssl_insecure = config.get("ssl_insecure", False),
    )


class ClouderaServiceHealthCheck(CheckBase):
    """Controlla lo stato di tutti i servizi CDP via Cloudera Manager API."""

    requires = []  # pura API REST

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_cm_client(self.config)
            data   = client.get("services")
        except IOError as e:
            return CheckResult(
                name="ClouderaServiceHealth",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        services = data.get("items", [])
        bad   = []
        warn  = []

        for svc in services:
            name    = svc.get("name", "?")
            display = svc.get("displayName", name)
            health  = svc.get("healthSummary", "NOT_AVAILABLE")
            state   = svc.get("serviceState", "UNKNOWN")

            if health == "BAD":
                bad.append("{} ({})".format(display, state))
            elif health in ("CONCERNING", "NOT_AVAILABLE"):
                warn.append("{}: {}".format(display, health))

        if bad:
            return CheckResult(
                name="ClouderaServiceHealth",
                status=CheckResult.CRITICAL,
                message="BAD services: {}".format(", ".join(bad)),
                details={"bad": bad, "concerning": warn}
            )
        if warn:
            return CheckResult(
                name="ClouderaServiceHealth",
                status=CheckResult.WARNING,
                message="Services with issues: {}".format(", ".join(warn)),
                details={"concerning": warn}
            )
        return CheckResult(
            name="ClouderaServiceHealth",
            status=CheckResult.OK,
            message="All {} services GOOD".format(len(services)),
            details={"service_count": len(services)}
        )


class ClouderaParcelCheck(CheckBase):
    """Verifica che tutti i parcel siano in stato ACTIVATED."""

    requires = []

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_cm_client(self.config)
            data   = client.get("parcels")
        except IOError as e:
            msg = str(e)
            if "403" in msg:
                return CheckResult(
                    name="ClouderaParcels",
                    status=CheckResult.SKIPPED,
                    message="Parcels check skipped — 403 Forbidden (CM user needs Cluster Administrator role)"
                )
            return CheckResult(
                name="ClouderaParcels",
                status=CheckResult.UNKNOWN,
                message=msg
            )

        ignore_raw = (
            self.config.get("checks", {})
                       .get("parcels", {})
                       .get("ignore", [])
        )
        ignore = set(x.upper() for x in ignore_raw)

        parcels = data.get("items", [])
        not_activated = []
        for p in parcels:
            product = p.get("product", "?")
            version = p.get("version", "?")
            stage   = p.get("stage", "UNKNOWN")
            if product.upper() in ignore:
                continue
            if stage != "ACTIVATED":
                not_activated.append("{}-{} ({})".format(product, version, stage))

        if not_activated:
            return CheckResult(
                name="ClouderaParcels",
                status=CheckResult.WARNING,
                message="Non-activated parcels: {}".format(", ".join(not_activated)),
                details={"not_activated": not_activated}
            )
        return CheckResult(
            name="ClouderaParcels",
            status=CheckResult.OK,
            message="All {} parcel(s) ACTIVATED".format(len(parcels))
        )


class ClouderaNameNodeHACheck(CheckBase):
    """Verifica stato HA NameNode tramite Cloudera Manager API (ruoli HDFS).

    Usa GET /clusters/{cluster}/services/hdfs/roles e filtra i NAMENODE.
    Ogni role espone `haStatus` (ACTIVE/STANDBY) e `roleState` (STARTED/STOPPED).
    """

    requires = []  # pura API REST

    def run(self):
        # type: () -> CheckResult
        try:
            client = _make_cm_client(self.config)
            data   = client.get("services/hdfs/roles")
        except IOError as e:
            msg = str(e)
            # Il servizio HDFS potrebbe chiamarsi diversamente (hdfs1, ecc.)
            # o l'utente potrebbe non avere permessi — restituiamo UNKNOWN non CRITICAL
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.UNKNOWN,
                message="CM roles API error: {}".format(msg)
            )

        roles = data.get("items", [])
        namenodes = [r for r in roles if r.get("type") == "NAMENODE"]

        if not namenodes:
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.UNKNOWN,
                message="No NAMENODE roles found in CM (check hdfs service name)"
            )

        active  = []
        standby = []
        stopped = []
        unknown = []

        for nn in namenodes:
            name       = nn.get("name", "?")
            host_ref   = nn.get("hostRef", {})
            hostname   = host_ref.get("hostname", name)
            short_host = hostname.split(".")[0]
            role_state = nn.get("roleState", "")
            ha_status  = (nn.get("haStatus") or "").upper()

            if role_state != "STARTED":
                stopped.append(short_host)
            elif ha_status == "ACTIVE":
                active.append(short_host)
            elif ha_status == "STANDBY":
                standby.append(short_host)
            else:
                unknown.append(short_host)

        # Non-HA: un solo NN avviato, senza haStatus
        if len(namenodes) == 1:
            if stopped:
                return CheckResult(
                    name="NameNodeHA",
                    status=CheckResult.CRITICAL,
                    message="NameNode STOPPED: {}".format(", ".join(stopped))
                )
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.OK,
                message="NameNode running (non-HA): {}".format(
                    ", ".join(active + unknown))
            )

        # HA: ci aspettiamo esattamente 1 active + 1 standby
        problems = []
        if len(active) != 1:
            problems.append("{} active NameNode(s) (expected 1)".format(len(active)))
        if not standby:
            problems.append("no standby NameNode")
        if stopped:
            problems.append("stopped: {}".format(", ".join(stopped)))

        if problems:
            return CheckResult(
                name="NameNodeHA",
                status=CheckResult.CRITICAL,
                message="HA problem — {}".format("; ".join(problems)),
                details={"active": active, "standby": standby, "stopped": stopped}
            )

        return CheckResult(
            name="NameNodeHA",
            status=CheckResult.OK,
            message="NameNode HA OK — active: {}, standby: {}".format(
                ", ".join(active), ", ".join(standby)),
            details={"active": active, "standby": standby}
        )


class ClouderaClusterInfoCheck(CheckBase):
    """Informazioni versione — CM, CDP, e presenza/versione di un eventuale
    cluster CDP Private Cloud Data Services (ECS) registrato sulla stessa
    Cloudera Manager. Puramente informativo per la dashboard 'at a
    glance': nessuna soglia, sempre OK se raggiungibile — non è un check
    di salute.

    Il cluster Data Services NON compare in GET /clusters (l'endpoint di
    lista lo filtra) — va scoperto enumerando gli host (campo
    clusterRef.clusterName su GET /hosts) e poi interrogato per nome con
    GET /clusters/{name}. clusterType == "EXPERIENCE_CLUSTER" identifica
    CDP Private Cloud Data Services — verificato contro un ambiente reale
    (MdS dev: cluster "dsDEV", versione 1.5.5). Qualunque altro
    clusterType inatteso viene comunque segnalato con il valore raw, per
    non nascondere sorprese su ambienti diversi.
    """

    requires = []

    def run(self):
        # type: () -> CheckResult
        cluster_name = self.config.get("cluster_name")
        try:
            client       = _make_cm_client(self.config)
            cm_version   = client.get_raw("cm/version")
            this_cluster = client.get_raw("clusters/{}".format(cluster_name))
            hosts        = client.get_raw("hosts?view=full")
        except IOError as e:
            return CheckResult(
                name="ClouderaClusterInfo",
                status=CheckResult.UNKNOWN,
                message=str(e)
            )

        extra_names = set()
        for h in hosts.get("items", []):
            ref  = h.get("clusterRef") or {}
            name = ref.get("clusterName")
            if name and name != cluster_name:
                extra_names.add(name)

        data_services = []
        for name in sorted(extra_names):
            try:
                c = client.get_raw("clusters/{}".format(name))
            except IOError:
                continue
            data_services.append({
                "name":        name,
                "clusterType": c.get("clusterType", "?"),
                "version":     c.get("fullVersion", "?"),
            })

        details = {
            "cm_version":    cm_version.get("version", "?"),
            "cdp_version":   this_cluster.get("fullVersion", "?"),
            "data_services": bool(data_services),
        }
        if data_services:
            details["data_services_clusters"] = data_services

        msg = "CM {} — CDP {}".format(details["cm_version"], details["cdp_version"])
        if data_services:
            ds_str = ", ".join(
                "{} ({})".format(d["name"], d["version"])
                if d["clusterType"] == "EXPERIENCE_CLUSTER"
                else "{} ({}, {})".format(d["name"], d["clusterType"], d["version"])
                for d in data_services
            )
            msg += " — Data Services: {}".format(ds_str)
        else:
            msg += " — nessun servizio Data Services rilevato"

        return CheckResult(
            name="ClouderaClusterInfo",
            status=CheckResult.OK,
            message=msg,
            details=details
        )
