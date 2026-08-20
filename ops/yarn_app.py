"""Tool Ops per applicazioni YARN — status/metriche (AppStatusTool) e
fetch log (AppLogsTool, vedi Task 10)."""

from __future__ import print_function

import os
import re

from checks.base import CheckResult
from checks.yarn import _rm_url, _resolve_url, _yarn_get
from ops.base import OpsParam, OpsToolBase
import ansible_runner
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


class AppLogsTool(OpsToolBase):
    """Scarica i log aggregati di un'applicazione YARN terminata, eseguendo
    'yarn logs -applicationId <id>' sull'edge node via Ansible (nessun
    client Hadoop richiesto sulla macchina locale)."""

    name = "app-logs"
    description = "Scarica i log di un'applicazione YARN terminata"
    params = [OpsParam("app_id", help="YARN application ID")]
    requires = [["ansible"], ["venv_ansible"], ["docker"]]

    def _resolve_download_dir(self):
        # type: () -> str
        configured = self.config.get("download_dir")
        return os.path.expanduser(configured or "~/.hadoopscope/downloads")

    def run(self, app_id):
        # type: (str) -> CheckResult
        ansible_cfg = self.config.get("ansible", {})
        edge_host = ansible_cfg.get("edge_host")
        ssh_user  = ansible_cfg.get("ssh_user", "hadoop")
        ssh_key   = ansible_cfg.get("ssh_key")

        if not edge_host:
            return CheckResult(
                name=self.name,
                status=CheckResult.UNKNOWN,
                message="ansible.edge_host not configured"
            )

        ansible_bin = ansible_runner.find_ansible_bin()
        if not ansible_bin:
            return CheckResult(
                name=self.name,
                status=CheckResult.SKIPPED,
                message="ansible binary not found despite can_run() check"
            )

        inventory = ansible_runner.build_inventory(edge_host, ssh_user, ssh_key)

        krb = ansible_cfg.get("kerberos", {})
        kinit_cmd = None
        if krb.get("enabled") and krb.get("keytab") and krb.get("client_principal"):
            kinit_cmd = "kinit -kt {} {}".format(krb["keytab"], krb["client_principal"])

        # 'yarn logs' esce con rc!=0 (spesso 255) sia per errori reali sia per
        # il semplice "nessun log trovato" — e con il modulo Ansible 'raw'
        # (necessario per non dipendere dalla versione Python del target,
        # vedi ansible_runner.run_playbook) un rc remoto 255 viene confuso
        # da Ansible con un fallimento della connessione SSH stessa (stesso
        # codice usato da OpenSSH), risultando in un fuorviante UNREACHABLE
        # che oltretutto scarta l'output reale del comando. Per evitarlo,
        # lo script cattura il vero exit code in un sentinel e termina
        # sempre con exit 0, cosi' Ansible non lo classifica mai come
        # fallimento di connessione — interpretiamo noi il codice vero.
        cmd = (
            "yarn logs -applicationId {app_id}\n"
            "_HS_RC=$?\n"
            'echo "___HS_EXIT___:$_HS_RC"\n'
            "exit 0"
        ).format(app_id=app_id)
        rc, out, err = ansible_runner.run_playbook(
            ansible_bin, inventory, cmd, tag=self.name,
            kinit_cmd=kinit_cmd, timeout=180)

        if rc != 0:
            error_detail = ansible_runner.extract_task_error(out) if out else err
            return CheckResult(
                name=self.name,
                status=CheckResult.CRITICAL,
                message="fetch log fallito per {}: {}".format(app_id, error_detail[:300])
            )

        raw_stdout = ansible_runner.extract_stdout(out)
        sentinel = re.search(r"___HS_EXIT___:(\d+)", raw_stdout)
        true_rc = int(sentinel.group(1)) if sentinel else 0
        log_content = re.sub(r"___HS_EXIT___:\d+\s*\Z", "", raw_stdout)

        if true_rc != 0:
            return CheckResult(
                name=self.name,
                status=CheckResult.CRITICAL,
                message="fetch log fallito per {} (yarn logs rc={}): {}".format(
                    app_id, true_rc, log_content.strip()[:300])
            )

        download_dir = self._resolve_download_dir()
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
        out_path = os.path.join(download_dir, "{}.log".format(app_id))
        with open(out_path, "w") as f:
            f.write(log_content)

        return CheckResult(
            name=self.name,
            status=CheckResult.OK,
            message="log salvati in {} ({} bytes)".format(out_path, len(log_content)),
            details={"path": out_path, "size": len(log_content)}
        )
