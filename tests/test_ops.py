"""Test suite per il layer Ops di HadoopScope."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.yarn import _resolve_url
from checks.base import CheckResult
from ops.base import OpsParam, OpsToolBase
from ops.yarn_app import AppStatusTool
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
