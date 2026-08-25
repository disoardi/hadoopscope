"""Test suite per state_store.py — persistenza sqlite dello stato dei check."""

from __future__ import print_function

import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import state_store
from checks.base import CheckResult


def _make_result(name, status, message, details=None):
    return CheckResult(name, status, message, details or {})


def _reset():
    state_store._DB_PATH = None


def test_save_and_get_env_summary():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("AmbariServiceHealth", CheckResult.OK, "all good"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.WARNING, "1 unhealthy"))
        rows = state_store.get_env_summary("prod-hdp")
        names = sorted(r["check_name"] for r in rows)
        assert names == ["AmbariServiceHealth", "YarnNodeHealth"]
        yarn_row = next(r for r in rows if r["check_name"] == "YarnNodeHealth")
        assert yarn_row["status"] == CheckResult.WARNING
        assert yarn_row["message"] == "1 unhealthy"
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_save_result_upserts_same_env_check():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.CRITICAL, "2 lost"))
        state_store.save_result("prod-hdp", _make_result("YarnNodeHealth", CheckResult.OK, "all running"))
        rows = state_store.get_env_summary("prod-hdp")
        assert len(rows) == 1
        assert rows[0]["status"] == CheckResult.OK
        assert rows[0]["message"] == "all running"
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_env_summary_empty_env_returns_empty_list():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        rows = state_store.get_env_summary("never-run")
        assert rows == []
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_all_envs_summary_aggregates_worst_status_and_counts():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("A", CheckResult.OK, "ok"))
        state_store.save_result("prod-hdp", _make_result("B", CheckResult.WARNING, "warn"))
        state_store.save_result("prod-cdp", _make_result("C", CheckResult.CRITICAL, "crit"))
        summary = {row["env"]: row for row in state_store.get_all_envs_summary()}
        assert summary["prod-hdp"]["worst_status"] == CheckResult.WARNING
        assert summary["prod-hdp"]["counts"] == {"OK": 1, "WARNING": 1}
        assert summary["prod-cdp"]["worst_status"] == CheckResult.CRITICAL
        assert summary["prod-cdp"]["counts"] == {"CRITICAL": 1}
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_all_envs_summary_empty_db_returns_empty_list():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        assert state_store.get_all_envs_summary() == []
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_details_roundtrip_as_dict():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result(
            "YarnQueues", CheckResult.WARNING, "queue over 80%",
            details={"queue": "default", "usedCapacity": 87.5}))
        rows = state_store.get_env_summary("prod-hdp")
        assert rows[0]["details"] == {"queue": "default", "usedCapacity": 87.5}
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_get_all_envs_summary_includes_oldest_run_at():
    tmpdir = tempfile.mkdtemp()
    try:
        state_store.init(os.path.join(tmpdir, "state.db"))
        state_store.save_result("prod-hdp", _make_result("A", CheckResult.OK, "ok"))
        state_store.save_result("prod-hdp", _make_result("B", CheckResult.OK, "ok"))
        summary = {row["env"]: row for row in state_store.get_all_envs_summary()}
        assert summary["prod-hdp"]["oldest_run_at"] is not None
    finally:
        shutil.rmtree(tmpdir)
        _reset()


def test_init_creates_parent_dir_if_missing():
    tmpdir = tempfile.mkdtemp()
    try:
        db_path = os.path.join(tmpdir, "nested", "dir", "state.db")
        state_store.init(db_path)
        assert os.path.exists(db_path)
    finally:
        shutil.rmtree(tmpdir)
        _reset()


if __name__ == "__main__":
    tests = [
        test_save_and_get_env_summary,
        test_save_result_upserts_same_env_check,
        test_get_env_summary_empty_env_returns_empty_list,
        test_get_all_envs_summary_aggregates_worst_status_and_counts,
        test_get_all_envs_summary_empty_db_returns_empty_list,
        test_get_all_envs_summary_includes_oldest_run_at,
        test_details_roundtrip_as_dict,
        test_init_creates_parent_dir_if_missing,
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
