#!/usr/bin/env python3
"""HadoopScope — Unified Hadoop cluster health monitoring."""

from __future__ import print_function

import argparse
import json
import sys
import os

# Aggiungiamo la directory del progetto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import load_config
from bootstrap import discover_capabilities, ensure_ansible
from checks.base import CheckResult


def build_arg_parser():
    # type: () -> argparse.ArgumentParser
    p = argparse.ArgumentParser(
        description="HadoopScope — Unified Hadoop cluster health monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --env prod-hdp
  %(prog)s --env prod-hdp --checks health
  %(prog)s --env prod-hdp --output json --dry-run
        """
    )
    p.add_argument("--config", default="config/hadoopscope.yaml",
                   help="Path to config file (default: config/hadoopscope.yaml)")
    p.add_argument("--env", required=True, action="append", dest="envs",
                   metavar="ENV", help="Environment to check (can repeat for multi-env)")
    p.add_argument("--checks", default="all",
                   choices=["all", "health", "hdfs", "hive", "yarn"],
                   help="Which checks to run (default: all)")
    p.add_argument("--output", default="text",
                   choices=["text", "json"],
                   help="Output format (default: text)")
    p.add_argument("--dry-run", action="store_true",
                   help="Test config + connectivity without running checks")
    p.add_argument("--version", action="version", version="HadoopScope 0.1.0-dev")
    return p


def run_checks_for_env(env_name, env_config, global_config, caps, args):
    # type: (str, dict, dict, dict, argparse.Namespace) -> list
    """Seleziona e lancia i check per un singolo environment."""
    from checks.ambari import AmbariServiceHealthCheck
    from checks.webhdfs import HdfsSpaceCheck, HdfsDataNodeCheck, HdfsWritabilityCheck

    results = []

    # Mappa checks disponibili per tipo
    check_registry = {
        "health": [
            AmbariServiceHealthCheck,
        ],
        "hdfs": [
            HdfsSpaceCheck,
            HdfsDataNodeCheck,
            HdfsWritabilityCheck,
        ],
        "hive": [],   # TODO: Giorno 4
        "yarn": [],   # TODO: Giorno 4
    }

    if args.checks == "all":
        check_classes = []
        for classes in check_registry.values():
            check_classes.extend(classes)
    else:
        check_classes = check_registry.get(args.checks, [])

    for CheckClass in check_classes:
        instance = CheckClass(config=env_config, caps=caps)

        if not instance.can_run():
            if instance.fallback is not None:
                instance = instance.fallback(config=env_config, caps=caps)
                if not instance.can_run():
                    result = CheckResult(
                        name=CheckClass.__name__,
                        status=CheckResult.SKIPPED,
                        message="Requires: {}".format(CheckClass.requires)
                    )
                    results.append(result)
                    continue
            else:
                result = CheckResult(
                    name=CheckClass.__name__,
                    status=CheckResult.SKIPPED,
                    message="Requires: {}. Install missing tools or use Docker.".format(
                        CheckClass.requires
                    )
                )
                results.append(result)
                continue

        if args.dry_run:
            result = CheckResult(
                name=instance.__class__.__name__,
                status="DRY_RUN",
                message="Would run (capability OK)"
            )
        else:
            try:
                result = instance.run()
            except Exception as e:
                result = CheckResult(
                    name=instance.__class__.__name__,
                    status=CheckResult.UNKNOWN,
                    message="Exception: {}".format(str(e))
                )

        results.append(result)

    return results


def print_text_report(env_name, results, caps_used):
    # type: (str, list, list) -> None
    status_icons = {
        CheckResult.OK:       "[OK]      ",
        CheckResult.WARNING:  "[WARNING] ",
        CheckResult.CRITICAL: "[CRITICAL]",
        CheckResult.UNKNOWN:  "[UNKNOWN] ",
        CheckResult.SKIPPED:  "[SKIPPED] ",
        "DRY_RUN":            "[DRY-RUN] ",
    }

    counts = {s: 0 for s in [CheckResult.OK, CheckResult.WARNING,
                               CheckResult.CRITICAL, CheckResult.UNKNOWN,
                               CheckResult.SKIPPED, "DRY_RUN"]}

    for r in results:
        icon = status_icons.get(r.status, "[?]       ")
        print("{}  {} — {}".format(icon, r.name, r.message))
        counts[r.status] = counts.get(r.status, 0) + 1

    print()
    summary_parts = []
    for s in [CheckResult.CRITICAL, CheckResult.WARNING, CheckResult.OK,
              CheckResult.UNKNOWN, CheckResult.SKIPPED]:
        if counts.get(s, 0) > 0:
            summary_parts.append("{} {}".format(counts[s], s))
    print("Summary: {}".format(", ".join(summary_parts)))

    if caps_used:
        print("Capabilities used: {}".format(", ".join(caps_used)))


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    # Carica config
    try:
        cfg = load_config(args.config)
    except Exception as e:
        print("ERROR loading config: {}".format(e), file=sys.stderr)
        sys.exit(1)

    # Bootstrap
    caps = discover_capabilities()
    caps = ensure_ansible(caps)

    all_results = {}

    for env_name in args.envs:
        if env_name not in cfg.get("environments", {}):
            print("ERROR: environment '{}' not found in config".format(env_name),
                  file=sys.stderr)
            sys.exit(1)

        env_config = cfg["environments"][env_name]
        if not env_config.get("enabled", True):
            print("SKIP: environment '{}' is disabled".format(env_name))
            continue

        results = run_checks_for_env(env_name, env_config, cfg, caps, args)
        all_results[env_name] = results

        if args.output == "text":
            print("\nHadoopScope — {} @ {}".format(env_name, env_config.get("ambari_url", "")))
            print("=" * 60)
            caps_used = [k for k, v in caps.items() if v is True]
            print_text_report(env_name, results, caps_used)

    if args.output == "json":
        output = {
            "version": "0.1.0",
            "environments": {}
        }
        for env_name, results in all_results.items():
            output["environments"][env_name] = [
                {"check": r.name, "status": r.status,
                 "message": r.message, "details": r.details}
                for r in results
            ]
        print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
