"""Check Cloudera Manager REST API — CDP cluster health."""

from __future__ import print_function

import json
import socket

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode(
            "{}:{}".format(user, passwd).encode()
        ).decode()
        return "Basic {}".format(token)
except ImportError:
    from urllib2 import urlopen, Request, URLError, HTTPError
    import base64 as _base64
    def _make_auth_header(user, passwd):
        token = _base64.b64encode("{}:{}".format(user, passwd))
        return "Basic {}".format(token)

from checks.base import CheckBase, CheckResult

TIMEOUT = 10


class ClouderaClient(object):
    """Client HTTP minimale per Cloudera Manager REST API. Zero deps."""

    def __init__(self, base_url, user, password, cluster_name, api_version="v40"):
        # type: (str, str, str, str, str) -> None
        self.base_url     = base_url.rstrip("/")
        self.auth_header  = _make_auth_header(user, password)
        self.cluster_name = cluster_name
        self.api_version  = api_version

    def get(self, path):
        # type: (str) -> dict
        url = "{}/api/{}/clusters/{}/{}".format(
            self.base_url, self.api_version, self.cluster_name, path.lstrip("/")
        )
        req = Request(url)
        req.add_header("Authorization", self.auth_header)
        req.add_header("Accept", "application/json")
        try:
            resp = urlopen(req, timeout=TIMEOUT)
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
            resp = urlopen(req, timeout=TIMEOUT)
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
