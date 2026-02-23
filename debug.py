"""
Debug mode per HadoopScope.

Attivazione:
  python3 hadoopscope.py --debug ...
  oppure: HADOOPSCOPE_DEBUG=1 python3 hadoopscope.py ...

Output su stderr — non interferisce con --output json su stdout.

Usage nei moduli:
  import debug
  debug.log("HiveCheck", "beeline_cmd={}".format(cmd))
  debug.log("HiveCheck[ns]", "inventory:\n{}".format(inv), multiline=True)
"""

from __future__ import print_function

import os
import sys

# Attivato da --debug CLI o dalla variabile d'ambiente HADOOPSCOPE_DEBUG
ENABLED = os.environ.get("HADOOPSCOPE_DEBUG", "").strip() in ("1", "true", "yes")


def log(tag, msg, multiline=False):
    # type: (str, str, bool) -> None
    """Stampa un messaggio di debug su stderr se debug mode è attivo."""
    if not ENABLED:
        return
    if multiline:
        sys.stderr.write("[DEBUG] {}:\n".format(tag))
        for line in str(msg).splitlines():
            sys.stderr.write("        {}\n".format(line))
    else:
        sys.stderr.write("[DEBUG] {}: {}\n".format(tag, msg))
    sys.stderr.flush()


def section(tag, title):
    # type: (str, str) -> None
    """Stampa un separatore di sezione su stderr."""
    if not ENABLED:
        return
    sys.stderr.write("[DEBUG] {} --- {} ---\n".format(tag, title))
    sys.stderr.flush()
