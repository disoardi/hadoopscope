"""Test suite per ansible_runner.py."""

from __future__ import print_function

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ansible_runner


def test_build_inventory_with_ssh_key():
    inv = ansible_runner.build_inventory("edge1.example.com", "hadoop", "/path/to/key")
    assert inv == "edge1.example.com ansible_user=hadoop ansible_ssh_private_key_file=/path/to/key"


def test_build_inventory_without_ssh_key_omits_key_file():
    """Senza ssh_key non deve forzare un path di default (es. ~/.ssh/id_rsa)
    che potrebbe non esistere per chi usa uno ssh-agent — deve lasciare
    che SSH risolva l'identità da solo."""
    inv = ansible_runner.build_inventory("edge1.example.com", "hadoop", None)
    assert inv == "edge1.example.com ansible_user=hadoop"
    assert "ansible_ssh_private_key_file" not in inv


def test_build_inventory_localhost_uses_local_connection():
    inv = ansible_runner.build_inventory("localhost", "hadoop", "/path/to/key")
    assert inv == "localhost ansible_connection=local"


if __name__ == "__main__":
    tests = [
        test_build_inventory_with_ssh_key,
        test_build_inventory_without_ssh_key_omits_key_file,
        test_build_inventory_localhost_uses_local_connection,
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
