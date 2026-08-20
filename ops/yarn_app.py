"""Tool Ops per applicazioni YARN — status/metriche (AppStatusTool) e
fetch log (AppLogsTool, vedi Task 10)."""

from __future__ import print_function

from checks.base import CheckResult
from checks.yarn import _rm_url, _resolve_url, _yarn_get
from ops.base import OpsParam, OpsToolBase

_TERMINAL_STATUS_MAP = {
    "SUCCEEDED": CheckResult.OK,
    "KILLED":    CheckResult.WARNING,
    "FAILED":    CheckResult.CRITICAL,
}


def _normalize_app_fields(app):
    # type: (dict) -> dict
    return {
        "state":             app.get("state", "UNKNOWN"),
        "finalStatus":       app.get("finalStatus", "UNDEFINED"),
        "progress":          app.get("progress", 0),
        "applicationType":   app.get("applicationType", ""),
        "diagnostics":       app.get("diagnostics", ""),
        "allocatedMB":       app.get("allocatedMB", 0),
        "allocatedVCores":   app.get("allocatedVCores", 0),
        "runningContainers": app.get("runningContainers", 0),
        "elapsedTime":       app.get("elapsedTime", 0),
    }


def _status_from_fields(fields):
    # type: (dict) -> str
    state = fields["state"]
    if state == "RUNNING":
        return CheckResult.OK
    if state == "FINISHED":
        return _TERMINAL_STATUS_MAP.get(fields["finalStatus"], CheckResult.UNKNOWN)
    if state in ("FAILED", "KILLED"):
        return _TERMINAL_STATUS_MAP.get(state, CheckResult.UNKNOWN)
    return CheckResult.UNKNOWN


def _message_from_fields(app_id, fields):
    # type: (str, dict) -> str
    msg = "{} — state={} finalStatus={} progress={:.1f}%".format(
        app_id, fields["state"], fields["finalStatus"], fields["progress"])
    msg += "\nresources: {}MB / {} vcores / {} running containers".format(
        fields["allocatedMB"], fields["allocatedVCores"], fields["runningContainers"])
    if fields["diagnostics"]:
        msg += "\ndiagnostics: {}".format(fields["diagnostics"][:300])
    return msg


class AppStatusTool(OpsToolBase):
    """Status e metriche di un'applicazione YARN via ResourceManager REST API."""

    name = "app-status"
    description = "Status/metriche di un'applicazione YARN dato l'application id"
    params = [OpsParam("app_id", help="YARN application ID, es. application_1699999999_0001")]
    requires = []  # solo REST, sempre disponibile

    def run(self, app_id):
        # type: (str) -> CheckResult
        base, is_auto = _rm_url(self.config)
        if base is None:
            return CheckResult(
                name=self.name,
                status=CheckResult.SKIPPED,
                message="yarn.rm_url not configured — add yarn.rm_url to config"
            )

        no_proxy = self.config.get("no_proxy", False)
        use_krb  = self.config.get("kerberos", {}).get("enabled", False)

        try:
            data = _yarn_get(base, "apps/{}".format(app_id),
                             no_proxy=no_proxy, kerberos=use_krb)
        except IOError as e:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} — app non trovata su RM ({})".format(app_id, str(e))
            )

        app = data.get("app")
        if not app:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="{} non trovata su RM (nessun history_url configurato "
                        "per il fallback)".format(app_id)
            )

        fields = _normalize_app_fields(app)
        return CheckResult(
            name=self.name,
            status=_status_from_fields(fields),
            message=_message_from_fields(app_id, fields),
            details=fields
        )
