"""Tests per applog.py — rotating file logger."""
from __future__ import print_function

import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import applog
from checks.base import CheckResult


def _make_cfg(tmpdir, **kwargs):
    opts = {"file": os.path.join(tmpdir, "test.log"), "max_mb": 1, "backup_count": 2}
    opts.update(kwargs)
    return {"logging": opts}


def _reset_applog():
    """Reset modulo per isolamento tra test."""
    applog._log = None


def test_setup_creates_log_dir():
    tmpdir = tempfile.mkdtemp()
    try:
        log_file = os.path.join(tmpdir, "subdir", "app.log")
        applog.setup({"logging": {"file": log_file}})
        assert os.path.isdir(os.path.dirname(log_file))
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_setup_disabled():
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup({"logging": {"enabled": "false",
                                  "file": os.path.join(tmpdir, "x.log")}})
        assert applog._log is None
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_log_run_start_writes_line():
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup(_make_cfg(tmpdir))
        applog.log_run_start("prod-cdp", "hive")
        with open(os.path.join(tmpdir, "test.log")) as f:
            content = f.read()
        assert "RUN START" in content
        assert "prod-cdp" in content
        assert "hive" in content
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_log_result_ok():
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup(_make_cfg(tmpdir))
        r = CheckResult("HiveCheck", CheckResult.OK, "HiveServer2 OK")
        applog.log_result(r)
        with open(os.path.join(tmpdir, "test.log")) as f:
            content = f.read()
        assert "[OK      ]" in content
        assert "HiveCheck" in content
        assert "HiveServer2 OK" in content
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_log_result_warning_full_over_threshold():
    """Il log deve contenere TUTTE le voci over_threshold, non solo le prime 5."""
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup(_make_cfg(tmpdir))
        over = ["db.t{}: {}".format(i, 9000 + i) for i in range(10)]
        # message mostra solo 5 + "+5 more"
        lines = ["Tables exceeding 5000 partitions:"] + ["  " + x for x in over[:5]]
        lines.append("  (+5 more)")
        r = CheckResult("HivePartitionCheck", CheckResult.WARNING, "\n".join(lines),
                        details={"over_threshold": over})
        applog.log_result(r)
        with open(os.path.join(tmpdir, "test.log")) as f:
            content = f.read()
        # Tutte e 10 le voci devono essere nel log
        for entry in over:
            assert entry in content, "Manca nel log: {}".format(entry)
        # La riga "+5 more" NON deve essere nel log (sostituita dalla lista completa)
        assert "+5 more" not in content
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_log_result_multiline_message_no_over_threshold():
    """Senza over_threshold: tutte le righe del message vengono loggati."""
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup(_make_cfg(tmpdir))
        msg = "Hive failed:\n  ns1: rc=1\n  ns2: timeout"
        r = CheckResult("HiveCheck", CheckResult.CRITICAL, msg)
        applog.log_result(r)
        with open(os.path.join(tmpdir, "test.log")) as f:
            content = f.read()
        assert "ns1: rc=1" in content
        assert "ns2: timeout" in content
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_log_run_end_summary():
    tmpdir = tempfile.mkdtemp()
    try:
        applog.setup(_make_cfg(tmpdir))
        results = [
            CheckResult("A", CheckResult.OK, "ok"),
            CheckResult("B", CheckResult.WARNING, "warn"),
            CheckResult("C", CheckResult.UNKNOWN, "unk"),
        ]
        applog.log_run_end("prod-cdp", results)
        with open(os.path.join(tmpdir, "test.log")) as f:
            content = f.read()
        assert "RUN END" in content
        assert "1 WARNING" in content
        assert "1 OK" in content
        assert "1 UNKNOWN" in content
    finally:
        shutil.rmtree(tmpdir)
        _reset_applog()


def test_no_log_when_not_setup():
    """log_result e log_run_end sono no-op se setup non e' stato chiamato."""
    _reset_applog()
    r = CheckResult("X", CheckResult.OK, "ok")
    applog.log_result(r)   # non deve sollevare eccezioni
    applog.log_run_end("env", [r])


if __name__ == "__main__":
    tests = [
        test_setup_creates_log_dir,
        test_setup_disabled,
        test_log_run_start_writes_line,
        test_log_result_ok,
        test_log_result_warning_full_over_threshold,
        test_log_result_multiline_message_no_over_threshold,
        test_log_run_end_summary,
        test_no_log_when_not_setup,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} -- {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
