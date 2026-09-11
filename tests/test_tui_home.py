"""Test per tui/screens/home.py — HomeGridScreen.enter() legge HostHealth/
HdfsDataNodes/YarnNodeHealth da state_store insieme a HdfsSpace/YarnClusterMetrics
(nessuna dipendenza da un terminale reale: si esercita solo enter(), mai render())."""

from __future__ import print_function

import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import state_store
from checks.base import CheckResult
from tui.screens.home import HomeGridScreen


class _FakeApp(object):
    def __init__(self, envs):
        self.envs = {name: {} for name in envs}


def _make_result(name, status, message, details=None):
    return CheckResult(name, status, message, details or {})


def _reset():
    state_store._DB_PATH = None


def test_enter_reads_hosts_datanodes_nodemanagers_details():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result(
            "HostHealth", CheckResult.OK, "3/3 hosts active",
            details={"total": 3, "active": 3}))
        state_store.save_result("prod-hdp", _make_result(
            "HdfsDataNodes", CheckResult.OK, "5 live, 0 dead, 0 stale DataNodes",
            details={"live": 5, "dead": 0, "stale": 0}))
        state_store.save_result("prod-hdp", _make_result(
            "YarnNodeHealth", CheckResult.OK, "60/60 nodes RUNNING",
            details={"total": 60, "running": 60, "unhealthy": 0,
                     "lost": 0, "decommissioned": 0}))

        screen = HomeGridScreen(app=_FakeApp(["prod-hdp"]))
        screen.enter()

        entry = screen.envs[0]
        assert entry["hosts"] == {"total": 3, "active": 3}
        assert entry["datanodes"] == {"live": 5, "dead": 0, "stale": 0}
        assert entry["nodemanagers"]["total"] == 60
        assert entry["nodemanagers"]["running"] == 60
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_enter_missing_checks_returns_none():
    """Env senza HostHealth/HdfsDataNodes/YarnNodeHealth ancora eseguiti
    (es. subito dopo un dry-run) — entry deve restare None, non KeyError."""
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result(
            "AmbariServiceHealth", CheckResult.OK, "all good"))

        screen = HomeGridScreen(app=_FakeApp(["prod-hdp"]))
        screen.enter()

        entry = screen.envs[0]
        assert entry["hosts"] is None
        assert entry["datanodes"] is None
        assert entry["nodemanagers"] is None
    finally:
        shutil.rmtree(tmpdir)
        _reset()


if __name__ == "__main__":
    tests = [
        test_enter_reads_hosts_datanodes_nodemanagers_details,
        test_enter_missing_checks_returns_none,
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
