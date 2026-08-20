"""Test suite per il layer Ops di HadoopScope."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checks.yarn import _resolve_url


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


if __name__ == "__main__":
    tests = [
        test_resolve_url_singular_key,
        test_resolve_url_plural_key_takes_first,
        test_resolve_url_plural_wins_over_singular,
        test_resolve_url_missing_returns_none,
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
