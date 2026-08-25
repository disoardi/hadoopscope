"""Test per tui/screens/monitoring.py — mutua esclusione 'all' vs categorie
specifiche in MonitoringCheckPickerScreen (nessuna dipendenza da un terminale
reale: si esercita solo handle_input, mai render())."""

from __future__ import print_function

import curses
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tui.screens.monitoring import MonitoringCheckPickerScreen


def _screen():
    return MonitoringCheckPickerScreen(app=None, envs=["prod-cdp"])


def test_all_preselected_by_default():
    s = _screen()
    assert s.selected == set([0])


def test_selecting_specific_category_deselects_all():
    s = _screen()
    s.cursor = 2  # "hdfs"
    s.handle_input(ord(" "))
    assert s.selected == set([2])


def test_selecting_all_after_specific_clears_specific():
    s = _screen()
    s.cursor = 2
    s.handle_input(ord(" "))  # seleziona "hdfs", deseleziona "all"
    s.cursor = 0
    s.handle_input(ord(" "))  # riseleziona "all"
    assert s.selected == set([0])


def test_toggling_specific_category_off_again():
    s = _screen()
    s.cursor = 3  # "hive"
    s.handle_input(ord(" "))
    s.handle_input(ord(" "))
    assert s.selected == set()


if __name__ == "__main__":
    tests = [
        test_all_preselected_by_default,
        test_selecting_specific_category_deselects_all,
        test_selecting_all_after_specific_clears_specific,
        test_toggling_specific_category_off_again,
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
