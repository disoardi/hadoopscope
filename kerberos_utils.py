"""Helper Kerberos condivisi — kinit via subprocess (stdlib-only).
Usato da checks/webhdfs.py (kinit locale) e ops/yarn_app.py (AppStatusTool,
kinit locale prima delle chiamate REST kerberizzate a YARN RM/History Server)."""

from __future__ import print_function

import subprocess


def kinit(keytab, principal, timeout=30):
    # type: (str, str, int) -> None
    """Ottiene un ticket Kerberos dal keytab. Raises IOError se kinit fallisce."""
    if not keytab:
        raise IOError("kerberos.keytab non configurato")
    if not principal:
        raise IOError("kerberos.principal non configurato")
    try:
        subprocess.check_call(
            ["kinit", "-kt", keytab, principal],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout
        )
    except subprocess.CalledProcessError:
        raise IOError(
            "kinit fallito per principal='{}' keytab='{}'. "
            "Verifica che il keytab sia valido e il KDC raggiungibile.".format(
                principal, keytab)
        )
    except OSError:
        raise IOError(
            "kinit non trovato nel PATH — installa krb5-user (Debian) "
            "o krb5-workstation (RHEL)")
