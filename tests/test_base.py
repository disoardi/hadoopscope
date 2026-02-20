"""Unit test per CheckBase — nessuna dipendenza da cluster reale."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.base import CheckBase, CheckResult


class MockCheckAlwaysRun(CheckBase):
    requires = []
    def run(self):
        return CheckResult("MockAlways", CheckResult.OK, "ok")

class MockCheckNeedsAnsible(CheckBase):
    requires = [["ansible"]]
    def run(self):
        return CheckResult("MockAnsible", CheckResult.OK, "ok")

class MockCheckNeedsAnsibleOrDocker(CheckBase):
    requires = [["ansible"], ["docker"]]
    def run(self):
        return CheckResult("MockAnsibleOrDocker", CheckResult.OK, "ok")


def test_can_run_no_requires():
    c = MockCheckAlwaysRun({}, {})
    assert c.can_run() is True

def test_can_run_with_capability():
    caps = {"ansible": True}
    c = MockCheckNeedsAnsible({}, caps)
    assert c.can_run() is True

def test_cannot_run_missing_capability():
    c = MockCheckNeedsAnsible({}, {})
    assert c.can_run() is False

def test_or_logic_first_option():
    caps = {"ansible": True, "docker": False}
    c = MockCheckNeedsAnsibleOrDocker({}, caps)
    assert c.can_run() is True

def test_or_logic_second_option():
    caps = {"ansible": False, "docker": True}
    c = MockCheckNeedsAnsibleOrDocker({}, caps)
    assert c.can_run() is True

def test_or_logic_none_available():
    caps = {"ansible": False, "docker": False}
    c = MockCheckNeedsAnsibleOrDocker({}, caps)
    assert c.can_run() is False


if __name__ == "__main__":
    tests = [
        test_can_run_no_requires,
        test_can_run_with_capability,
        test_cannot_run_missing_capability,
        test_or_logic_first_option,
        test_or_logic_second_option,
        test_or_logic_none_available,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS {}".format(t.__name__))
        except AssertionError as e:
            print("FAIL {} — {}".format(t.__name__, e))
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
