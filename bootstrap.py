"""Bootstrap layer — discovery tool disponibili e auto-install Ansible."""

from __future__ import print_function

import os
import shutil
import subprocess
import sys


def discover_capabilities():
    # type: () -> dict
    """Scansiona la macchina e restituisce la capability map."""
    caps = {}

    # Python version
    caps["python_version"] = "{}.{}.{}".format(*sys.version_info[:3])

    # Ansible
    ansible_bin = shutil.which("ansible") or shutil.which("ansible-playbook")
    caps["ansible"] = bool(ansible_bin)
    if caps["ansible"]:
        try:
            out = subprocess.check_output(
                ["ansible", "--version"],
                stderr=subprocess.STDOUT
            )
            first_line = out.decode("utf-8", errors="replace").split("\n")[0]
            caps["ansible_version"] = first_line
        except Exception:
            caps["ansible_version"] = "unknown"

    # Docker
    caps["docker"] = bool(shutil.which("docker"))

    # Kerberos
    caps["kinit"] = bool(shutil.which("kinit"))
    caps["klist"]  = bool(shutil.which("klist"))

    # Zabbix sender
    caps["zabbix_sender"] = bool(shutil.which("zabbix_sender"))

    # Ansible in venv locale (installato da noi)
    caps["venv_ansible"] = _check_venv_ansible()

    # Docker image con ansible (pulled da noi)
    caps["docker_ansible_image"] = False  # aggiornato da ensure_ansible()

    return caps


def _check_venv_ansible():
    # type: () -> bool
    """Verifica se ansible è installato nel nostro venv locale."""
    venv_ansible = os.path.expanduser("~/.hadoopscope/venv/bin/ansible")
    return os.path.exists(venv_ansible)


def ensure_ansible(caps):
    # type: (dict) -> dict
    """
    Se ansible non è disponibile, prova a installarlo in modo isolato.
    Modifica caps in-place e lo restituisce.
    """
    if caps.get("ansible") or caps.get("venv_ansible"):
        return caps  # già disponibile, niente da fare

    print("[bootstrap] Ansible non trovato. Provo installazione isolata...",
          file=sys.stderr)

    if caps.get("docker"):
        success = _pull_ansible_docker_image()
        if success:
            caps["docker_ansible_image"] = True
            print("[bootstrap] Ansible Docker image pronta.", file=sys.stderr)
            return caps

    # Fallback: venv
    success = _install_ansible_venv()
    if success:
        caps["venv_ansible"] = True
        print("[bootstrap] Ansible installato in venv (~/.hadoopscope/venv/).",
              file=sys.stderr)
    else:
        print("[bootstrap] WARNING: impossibile installare Ansible. "
              "I check che richiedono Ansible saranno skippati.", file=sys.stderr)

    return caps


def _install_ansible_venv():
    # type: () -> bool
    """Installa ansible in ~/.hadoopscope/venv/. Restituisce True se ok."""
    venv_dir = os.path.expanduser("~/.hadoopscope/venv")
    try:
        if not os.path.exists(venv_dir):
            subprocess.check_call(
                [sys.executable, "-m", "venv", venv_dir],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

        pip = os.path.join(venv_dir, "bin", "pip")
        subprocess.check_call(
            [pip, "install", "--quiet", "ansible-core"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return True
    except Exception as e:
        return False


def _pull_ansible_docker_image():
    # type: () -> bool
    """Pusha immagine Docker leggera con ansible. Restituisce True se ok."""
    image = "cytopia/ansible:latest-tools"
    try:
        subprocess.check_call(
            ["docker", "pull", "--quiet", image],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return True
    except Exception:
        return False


def print_capabilities(caps):
    # type: (dict) -> None
    """Stampa la capability map in modo leggibile."""
    print("\n[bootstrap] Capability Map:")
    for key, val in sorted(caps.items()):
        icon = "✓" if val and val is not False else "✗"
        print("  {} {}: {}".format(icon, key, val))
