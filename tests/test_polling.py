"""Test per tui/polling.py — poll YARN in background per la card Home."""

from __future__ import print_function

import os
import sys
import shutil
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import state_store
import tui.polling as polling
from checks.base import CheckResult


class _FakeApp(object):
    def __init__(self, envs, env_global):
        self.envs = envs
        self.env_global = env_global
        self.caps = {}


class _FakeCheck(object):
    last_config = None

    def __init__(self, config, caps):
        _FakeCheck.last_config = config

    def run(self):
        return CheckResult("YarnClusterMetrics", CheckResult.OK, "3 running")


class _BoomCheck(object):
    def __init__(self, config, caps):
        pass

    def run(self):
        raise RuntimeError("network error")


def _reset():
    state_store._DB_PATH = None


def test_poll_once_saves_result_and_merges_checks_section():
    tmpdir = tempfile.mkdtemp()
    orig = polling.YarnClusterMetricsCheck
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        polling.YarnClusterMetricsCheck = _FakeCheck

        app = _FakeApp(
            envs={"prod-cdp": {"cm_url": "http://cm"}},
            env_global={"prod-cdp": {"checks": {"hdfs_writability": {"test_path": "/x"}}}},
        )
        polling._poll_once(app, "prod-cdp")

        # La sezione checks: globale va mersa in check_config, stesso
        # pattern di run_checks_for_env — altrimenti eventuali parametri
        # checks.* configurati non sarebbero mai visibili al check.
        assert _FakeCheck.last_config["checks"] == {"hdfs_writability": {"test_path": "/x"}}

        rows = state_store.get_env_summary("prod-cdp")
        assert len(rows) == 1
        assert rows[0]["check_name"] == "YarnClusterMetrics"
        assert rows[0]["status"] == CheckResult.OK
    finally:
        polling.YarnClusterMetricsCheck = orig
        shutil.rmtree(tmpdir)
        _reset()


def test_poll_iteration_safe_swallows_exceptions():
    """Un env che fallisce (rete, config) non deve mai propagare — il
    thread di quell'env, e tutti gli altri, devono continuare a girare."""
    orig = polling.YarnClusterMetricsCheck
    try:
        polling.YarnClusterMetricsCheck = _BoomCheck
        app = _FakeApp(envs={"broken": {}}, env_global={"broken": {}})
        polling._poll_iteration_safe(app, "broken")  # non deve sollevare
    finally:
        polling.YarnClusterMetricsCheck = orig


if __name__ == "__main__":
    tests = [
        test_poll_once_saves_result_and_merges_checks_section,
        test_poll_iteration_safe_swallows_exceptions,
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
