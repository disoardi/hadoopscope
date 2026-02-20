#!/usr/bin/env python3
"""Runner per tutti i test di HadoopScope."""

from __future__ import print_function

import os
import subprocess
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR  = os.path.dirname(TESTS_DIR)

test_files = [
    os.path.join(TESTS_DIR, "test_base.py"),
    os.path.join(TESTS_DIR, "test_checks.py"),
]

total_failed = 0
for test_file in test_files:
    print("\n" + "=" * 60)
    print("Running: {}".format(os.path.basename(test_file)))
    print("=" * 60)
    rc = subprocess.call([sys.executable, test_file])
    total_failed += rc

print("\n" + "=" * 60)
if total_failed == 0:
    print("ALL TESTS PASSED")
else:
    print("{} TEST FILE(S) HAD FAILURES".format(total_failed))
sys.exit(min(total_failed, 1))
