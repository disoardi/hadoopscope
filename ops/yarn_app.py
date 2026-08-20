"""Tool Ops per applicazioni YARN — status/metriche (AppStatusTool) e
fetch log (AppLogsTool, vedi Task 10)."""

from __future__ import print_function

from checks.base import CheckResult
from checks.yarn import _rm_url, _resolve_url, _yarn_get
from ops.base import OpsParam, OpsToolBase
import kerberos_utils

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


def _normalize_history_hdp(app):
    # type: (dict) -> dict
    """Normalizza la risposta dell'Application History Server v1 (HDP) —
    shape con chiavi diverse dalla RM REST API (appState invece di state,
    ecc.)."""
    return {
        "state":             app.get("appState", "UNKNOWN"),
        "finalStatus":       app.get("finalAppStatus", "UNDEFINED"),
        "progress":          app.get("progress", 0),
        "applicationType":   app.get("type", ""),
        "diagnostics":       app.get("diagnosticsInfo", ""),
        "allocatedMB":       0,
        "allocatedVCores":   0,
        "runningContainers": 0,
        "elapsedTime":       app.get("elapsedTime", 0),
    }


def _normalize_timeline_v2(data):
    # type: (dict) -> dict
    """Normalizza la risposta di Timeline Service v2 (CDP) — shape annidata
    sotto 'info' con chiavi YARN_APPLICATION_*."""
    info = data.get("info", {})
    return {
        "state":             info.get("YARN_APPLICATION_STATE", "UNKNOWN"),
        "finalStatus":       info.get("YARN_APPLICATION_FINAL_STATUS", "UNDEFINED"),
        "progress":          info.get("YARN_APPLICATION_PROGRESS", 0),
        "applicationType":   info.get("YARN_APPLICATION_APPLICATION_TYPE", ""),
        "diagnostics":       info.get("YARN_APPLICATION_DIAGNOSTICS_INFO", ""),
        "allocatedMB":       0,
        "allocatedVCores":   0,
        "runningContainers": 0,
        "elapsedTime":       info.get("YARN_APPLICATION_ELAPSED_TIME", 0),
    }


def _query_history_server(config, app_id, no_proxy, use_krb):
    # type: (dict, str, bool, bool) -> object
    """Interroga l'Application History Server (HDP) o Timeline Service v2
    (CDP), a seconda di config['type']. Restituisce dict di campi
    normalizzati, o None se non configurato/non trovato."""
    yarn_cfg = config.get("yarn", {})
    history_url, _ = _resolve_url(yarn_cfg, "history_url", "history_urls")
    if not history_url:
        return None

    env_type = config.get("type", "hdp")
    if env_type == "cdp":
        path = "{}/ws/v2/timeline/apps/{}".format(history_url, app_id)
        try:
            data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                             full_path=True)
        except IOError:
            return None
        if not data:
            return None
        return _normalize_timeline_v2(data)
    else:
        path = "{}/ws/v1/applicationhistory/apps/{}".format(history_url, app_id)
        try:
            data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                             full_path=True)
        except IOError:
            return None
        app = data.get("app")
        if not app:
            return None
        return _normalize_history_hdp(app)


_COUNTERS_CONFIG_KEY = {
    "SPARK":      "spark_history_url",
    "MAPREDUCE":  "mr_history_url",
    "TEZ":        "tez_history_url",
}


def _fetch_counters_best_effort(config, app_id, app_type, no_proxy, use_krb):
    # type: (dict, str, str, bool, bool) -> object
    """Tenta il fetch dei counters per il tipo applicativo. None se non
    configurato o se qualunque cosa fallisce — mai solleva."""
    cfg_key = _COUNTERS_CONFIG_KEY.get(app_type)
    if not cfg_key:
        return None
    yarn_cfg = config.get("yarn", {})
    plural = cfg_key.replace("_url", "_urls")
    history_url, _ = _resolve_url(yarn_cfg, cfg_key, plural)
    if not history_url:
        return None

    if app_type == "SPARK":
        path = "{}/api/v1/applications/{}".format(history_url, app_id)
    else:
        # MapReduce/Tez: mapping id->job id non standardizzato in questa
        # prima versione, riservato a fast-follow se emerge un bisogno reale
        return None

    try:
        data = _yarn_get(None, path, no_proxy=no_proxy, kerberos=use_krb,
                         full_path=True)
        return data
    except IOError:
        return None


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
        yarn_krb = self.config.get("yarn", {}).get("kerberos", {})
        top_krb  = self.config.get("kerberos", {})
        krb_cfg  = yarn_krb if yarn_krb.get("enabled") else top_krb
        use_krb  = krb_cfg.get("enabled", False)

        if use_krb:
            try:
                kerberos_utils.kinit(krb_cfg.get("keytab"), krb_cfg.get("principal"))
            except IOError as e:
                return CheckResult(
                    name=self.name,
                    status=CheckResult.UNKNOWN,
                    message="kinit fallito: {}".format(str(e))
                )

        try:
            data = _yarn_get(base, "apps/{}".format(app_id),
                             no_proxy=no_proxy, kerberos=use_krb)
            app = data.get("app")
        except IOError:
            app = None

        if app:
            fields = _normalize_app_fields(app)
        else:
            fields = _query_history_server(self.config, app_id, no_proxy, use_krb)
            if fields is None:
                return CheckResult(
                    name=self.name,
                    status=CheckResult.UNKNOWN,
                    message="{} non trovata né su RM né su History Server "
                            "(id errato o applicazione più vecchia della "
                            "retention configurata)".format(app_id)
                )

        counters_note = ""
        if fields["state"] in ("FINISHED", "FAILED", "KILLED"):
            counters = _fetch_counters_best_effort(
                self.config, app_id, fields["applicationType"], no_proxy, use_krb)
            if counters:
                fields["counters"] = counters
            else:
                counters_note = "\ncounters non disponibili"

        return CheckResult(
            name=self.name,
            status=_status_from_fields(fields),
            message=_message_from_fields(app_id, fields) + counters_note,
            details=fields
        )
