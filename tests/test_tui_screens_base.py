"""Test per il contratto Screen — nessuna dipendenza da un terminale reale."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tui.screens.base import Screen


def test_screen_default_enter_is_noop():
    s = Screen(app=None)
    s.enter()  # non deve sollevare


def test_screen_default_on_idle_tick_is_noop():
    s = Screen(app=None)
    s.on_idle_tick()  # non deve sollevare


def test_screen_render_raises_not_implemented():
    s = Screen(app=None)
    try:
        s.render(stdscr=None)
        assert False, "should raise"
    except NotImplementedError:
        pass


def test_screen_handle_input_raises_not_implemented():
    s = Screen(app=None)
    try:
        s.handle_input(key=ord("q"))
        assert False, "should raise"
    except NotImplementedError:
        pass


if __name__ == "__main__":
    tests = [
        test_screen_default_enter_is_noop,
        test_screen_default_on_idle_tick_is_noop,
        test_screen_render_raises_not_implemented,
        test_screen_handle_input_raises_not_implemented,
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
