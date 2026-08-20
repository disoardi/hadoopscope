"""Test suite per il layer Ops di HadoopScope."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.yarn import _resolve_url
from checks.base import CheckResult
from ops.base import OpsParam, OpsToolBase
import shutil
import tempfile
from ops.yarn_app import AppStatusTool, AppLogsTool
from tests.test_checks import start_mock_server, load_fixture

try:
    from unittest import mock
except ImportError:
    import mock


def test_resolve_url_singular_key():
    cfg = {"rm_url": "http://rm1:8088/"}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"
    assert is_auto is False


def test_resolve_url_plural_key_takes_first():
    cfg = {"rm_urls": ["http://rm1:8088/", "http://rm2:8088/"]}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"


def test_resolve_url_plural_wins_over_singular():
    cfg = {"rm_url": "http://ignored:8088", "rm_urls": ["http://rm1:8088"]}
    url, is_auto = _resolve_url(cfg, "rm_url", "rm_urls")
    assert url == "http://rm1:8088"


def test_resolve_url_missing_returns_none():
    url, is_auto = _resolve_url({}, "history_url", "history_urls")
    assert url is None
    assert is_auto is True


def test_ops_param_defaults():
    p = OpsParam("app_id", help="YARN application id")
    assert p.name == "app_id"
    assert p.help == "YARN application id"
    assert p.required is True
    assert p.type is str


def test_ops_tool_base_can_run_no_requires():
    class _NoRequires(OpsToolBase):
        name = "noop"
        requires = []
        def run(self, **kwargs):
            return CheckResult("noop", CheckResult.OK, "ok")
    assert _NoRequires({}, {}).can_run() is True


def test_ops_tool_base_can_run_missing_cap():
    class _NeedsAnsible(OpsToolBase):
        name = "needs-ansible"
        requires = [["ansible"]]
        def run(self, **kwargs):
            return CheckResult("x", CheckResult.OK, "ok")
    assert _NeedsAnsible({}, {}).can_run() is False
    assert _NeedsAnsible({}, {"ansible": True}).can_run() is True


def test_ops_tool_base_run_raises_not_implemented():
    class Bad(OpsToolBase):
        name = "bad"
    try:
        Bad({}, {}).run()
        assert False, "should raise"
    except NotImplementedError:
        pass


def test_ops_tool_base_is_write_default_false():
    class _Tool(OpsToolBase):
        name = "x"
        def run(self, **kwargs):
            return CheckResult("x", CheckResult.OK, "ok")
    assert _Tool({}, {}).is_write is False


def test_app_status_running():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0001")
        assert result.status == CheckResult.OK
        assert "RUNNING" in result.message
        assert result.details["state"] == "RUNNING"
        assert result.details["allocatedMB"] == 4096
    finally:
        server.shutdown()


def test_app_status_succeeded():
    fixture = load_fixture("yarn_app_succeeded.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
    finally:
        server.shutdown()


def test_app_status_failed():
    fixture = load_fixture("yarn_app_failed.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0003": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0003")
        assert result.status == CheckResult.CRITICAL
        assert "AM Container exit code" in result.message
    finally:
        server.shutdown()


def test_app_status_no_rm_url_configured():
    tool = AppStatusTool(config={}, caps={})
    result = tool.run(app_id="application_x")
    assert result.status == CheckResult.SKIPPED


def test_app_status_fallback_to_history_hdp():
    history_fixture = load_fixture("yarn_app_history_hdp.json")
    server, port = start_mock_server({
        "/ws/v1/applicationhistory/apps/application_1699999999999_9001": history_fixture,
        # nessuna route per /ws/v1/cluster/apps/... -> il mock risponde 404
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {
            "type": "hdp",
            "yarn": {"rm_url": base, "history_url": base},
        }
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_9001")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
    finally:
        server.shutdown()


def test_app_status_fallback_to_timeline_v2_cdp():
    timeline_fixture = load_fixture("yarn_timeline_v2_cdp.json")
    server, port = start_mock_server({
        "/ws/v2/timeline/apps/application_1699999999999_9002": timeline_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {
            "type": "cdp",
            "yarn": {"rm_url": base, "history_url": base},
        }
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_9002")
        assert result.status == CheckResult.OK
        assert result.details["finalStatus"] == "SUCCEEDED"
        assert result.details["applicationType"] == "SPARK"
    finally:
        server.shutdown()


def test_app_status_not_found_anywhere():
    server, port = start_mock_server({})  # nessuna route -> tutto 404
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"type": "hdp", "yarn": {"rm_url": base, "history_url": base}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_does_not_exist")
        assert result.status == CheckResult.UNKNOWN
    finally:
        server.shutdown()


def test_app_status_kinit_called_when_kerberos_enabled():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {
            "yarn": {
                "rm_url": "http://127.0.0.1:{}".format(port),
                "kerberos": {"enabled": True, "keytab": "/x.keytab", "principal": "svc@REALM"},
            },
        }
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            result = tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_called_once_with("/x.keytab", "svc@REALM")
        assert result.status == CheckResult.OK
    finally:
        server.shutdown()


def test_app_status_kinit_not_called_when_kerberos_disabled():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_not_called()
    finally:
        server.shutdown()


def test_app_status_kinit_falls_back_to_top_level_kerberos():
    fixture = load_fixture("yarn_app_running.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0001": fixture,
    })
    try:
        cfg = {
            "kerberos": {"enabled": True, "keytab": "/top.keytab", "principal": "top@REALM"},
            "yarn": {"rm_url": "http://127.0.0.1:{}".format(port)},
        }
        tool = AppStatusTool(config=cfg, caps={})
        with mock.patch("ops.yarn_app.kerberos_utils.kinit") as mocked_kinit:
            tool.run(app_id="application_1699999999999_0001")
        mocked_kinit.assert_called_once_with("/top.keytab", "top@REALM")
    finally:
        server.shutdown()


def test_app_status_counters_best_effort_when_configured():
    app_fixture = load_fixture("yarn_app_succeeded_spark.json")
    counters_fixture = load_fixture("spark_history_counters.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": app_fixture,
        "/api/v1/applications/application_1699999999999_0002": counters_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"yarn": {"rm_url": base, "spark_history_url": base}}
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert "counters" in result.details
        assert result.details["counters"]["attempts"][0]["duration"] == 15000
    finally:
        server.shutdown()


def test_app_status_counters_not_available_when_not_configured():
    app_fixture = load_fixture("yarn_app_succeeded.json")
    server, port = start_mock_server({
        "/ws/v1/cluster/apps/application_1699999999999_0002": app_fixture,
    })
    try:
        base = "http://127.0.0.1:{}".format(port)
        cfg = {"yarn": {"rm_url": base}}  # nessun spark_history_url
        tool = AppStatusTool(config=cfg, caps={})
        result = tool.run(app_id="application_1699999999999_0002")
        assert result.status == CheckResult.OK
        assert "counters non disponibili" in result.message
        assert "counters" not in result.details
    finally:
        server.shutdown()


def test_app_logs_no_edge_host_configured():
    tool = AppLogsTool(config={}, caps={"ansible": True})
    result = tool.run(app_id="application_x")
    assert result.status == CheckResult.UNKNOWN
    assert "edge_host" in result.message


def test_app_logs_success_writes_file():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {
            "download_dir": tmpdir,
            "ansible": {"edge_host": "localhost"},
        }
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        fake_output = "log line 1\nlog line 2\n"
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "{}"'.format(
                            fake_output.replace("\n", "\\n")), "")):
            result = tool.run(app_id="application_test_001")
        assert result.status == CheckResult.OK
        out_path = os.path.join(tmpdir, "application_test_001.log")
        assert os.path.exists(out_path)
        with open(out_path) as f:
            content = f.read()
        assert "log line 1" in content
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_ansible_failure():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {"download_dir": tmpdir, "ansible": {"edge_host": "localhost"}}
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(2, "FAILED! => {\"msg\": \"boom\"}", "")):
            result = tool.run(app_id="application_test_002")
        assert result.status == CheckResult.CRITICAL
        assert "boom" in result.message
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_no_logs_found_reports_critical_not_ok():
    """yarn logs esce spesso con rc!=0 (255) quando non trova log per
    un'app — con raw+pty Ansible confonde questo con un fallimento SSH
    (UNREACHABLE) e scarta l'output reale. Il sentinel exit-code deve
    permettere di distinguere questo caso e riportarlo come CRITICAL
    con il messaggio reale, non come OK né come errore di connessione."""
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {"download_dir": tmpdir, "ansible": {"edge_host": "localhost"}}
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        fake_output = (
            "Can not find the logs for the application: application_x "
            "with the appOwner: hive\\n___HS_EXIT___:255\\n"
        )
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "{}"'.format(fake_output), "")):
            result = tool.run(app_id="application_x")
        assert result.status == CheckResult.CRITICAL
        assert "Can not find the logs" in result.message
        assert not os.path.exists(os.path.join(tmpdir, "application_x.log"))
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_default_download_dir():
    tool = AppLogsTool(config={"ansible": {"edge_host": "x"}}, caps={"ansible": True})
    assert tool._resolve_download_dir() == os.path.expanduser("~/.hadoopscope/downloads")


def test_app_logs_kinit_injected_when_ansible_kerberos_enabled():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {
            "download_dir": tmpdir,
            "ansible": {
                "edge_host": "edge1.example.com",
                "kerberos": {"enabled": True, "keytab": "/edge.keytab",
                             "client_principal": "svc@REALM"},
            },
        }
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "ok\\n"', "")) as mocked_run:
            tool.run(app_id="application_test_003")
        _, kwargs = mocked_run.call_args
        assert kwargs["kinit_cmd"] == "kinit -kt /edge.keytab svc@REALM"
    finally:
        shutil.rmtree(tmpdir)


def test_app_logs_no_kinit_when_ansible_kerberos_disabled():
    tmpdir = tempfile.mkdtemp()
    try:
        cfg = {"download_dir": tmpdir, "ansible": {"edge_host": "edge1.example.com"}}
        tool = AppLogsTool(config=cfg, caps={"ansible": True})
        with mock.patch("ops.yarn_app.ansible_runner.find_ansible_bin",
                        return_value="/usr/bin/ansible-playbook"), \
             mock.patch("ops.yarn_app.ansible_runner.run_playbook",
                        return_value=(0, '"r.stdout": "ok\\n"', "")) as mocked_run:
            tool.run(app_id="application_test_004")
        _, kwargs = mocked_run.call_args
        assert kwargs["kinit_cmd"] is None
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    tests = [
        test_resolve_url_singular_key,
        test_resolve_url_plural_key_takes_first,
        test_resolve_url_plural_wins_over_singular,
        test_resolve_url_missing_returns_none,
        test_ops_param_defaults,
        test_ops_tool_base_can_run_no_requires,
        test_ops_tool_base_can_run_missing_cap,
        test_ops_tool_base_run_raises_not_implemented,
        test_ops_tool_base_is_write_default_false,
        test_app_status_running,
        test_app_status_succeeded,
        test_app_status_failed,
        test_app_status_no_rm_url_configured,
        test_app_status_fallback_to_history_hdp,
        test_app_status_fallback_to_timeline_v2_cdp,
        test_app_status_not_found_anywhere,
        test_app_status_kinit_called_when_kerberos_enabled,
        test_app_status_kinit_not_called_when_kerberos_disabled,
        test_app_status_kinit_falls_back_to_top_level_kerberos,
        test_app_status_counters_best_effort_when_configured,
        test_app_status_counters_not_available_when_not_configured,
        test_app_logs_no_edge_host_configured,
        test_app_logs_success_writes_file,
        test_app_logs_ansible_failure,
        test_app_logs_no_logs_found_reports_critical_not_ok,
        test_app_logs_default_download_dir,
        test_app_logs_kinit_injected_when_ansible_kerberos_enabled,
        test_app_logs_no_kinit_when_ansible_kerberos_disabled,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} — {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
