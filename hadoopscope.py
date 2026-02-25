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
from bootstrap import discover_capabilities, ensure_ansible, print_capabilities
from checks.base import CheckResult
import debug as _debug


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
  %(prog)s --show-capabilities
        """
    )
    p.add_argument("--config", default="config/hadoopscope.yaml",
                   help="Path to config file (default: config/hadoopscope.yaml)")
    p.add_argument("--env", action="append", dest="envs",
                   metavar="ENV", help="Environment to check (can repeat for multi-env)")
    p.add_argument("--checks", default="all",
                   choices=["all", "health", "hdfs", "hive", "yarn"],
                   help="Which checks to run (default: all)")
    p.add_argument("--output", default="text",
                   choices=["text", "json"],
                   help="Output format (default: text)")
    p.add_argument("--dry-run", action="store_true",
                   help="Test config + connectivity without running checks")
    p.add_argument("--show-capabilities", action="store_true",
                   help="Print capability map and exit")
    p.add_argument("--verbose", action="store_true",
                   help="Verbose output including capability map")
    p.add_argument("--debug", action="store_true",
                   help="Debug mode: print requests, commands and full output to stderr")
    p.add_argument("--version", action="version", version="HadoopScope 0.1.0")
    return p


def build_check_registry(env_config, caps):
    # type: (dict, dict) -> dict
    """Costruisce il check_registry per l'environment dato."""
    from checks.ambari import (
        AmbariServiceHealthCheck, NameNodeHACheck, NameNodeBlocksCheck,
        ClusterAlertsCheck, ConfigStalenessCheck
    )
    from checks.webhdfs import HdfsSpaceCheck, HdfsDataNodeCheck, HdfsWritabilityCheck
    from checks.yarn import YarnNodeHealthCheck, YarnQueueCheck
    from checks.hive import HiveCheck
    from checks.cloudera import (
        ClouderaServiceHealthCheck, ClouderaParcelCheck, ClouderaNameNodeHACheck
    )

    env_type = env_config.get("type", "hdp")

    if env_type == "cdp":
        health_checks = [
            ClouderaServiceHealthCheck,
            ClouderaParcelCheck,
            ClouderaNameNodeHACheck,
        ]
    else:
        health_checks = [
            AmbariServiceHealthCheck,
            NameNodeHACheck,
            NameNodeBlocksCheck,
            ClusterAlertsCheck,
            ConfigStalenessCheck,
        ]

    return {
        "health": health_checks,
        "hdfs": [
            HdfsSpaceCheck,
            HdfsDataNodeCheck,
            HdfsWritabilityCheck,
        ],
        "hive": [HiveCheck],
        "yarn": [
            YarnNodeHealthCheck,
            YarnQueueCheck,
        ],
    }


def run_checks_for_env(env_name, env_config, global_config, caps, args):
    # type: (str, dict, dict, dict, argparse.Namespace) -> list
    """Seleziona e lancia i check per un singolo environment."""
    results = []
    check_registry = build_check_registry(env_config, caps)

    if args.checks == "all":
        check_classes = []
        for classes in check_registry.values():
            check_classes.extend(classes)
    else:
        check_classes = check_registry.get(args.checks, [])

    # Merge sezione globale "checks" in env_config così i check possono
    # leggere checks.hdfs_writability.test_path, checks.hdfs_space.paths, ecc.
    # (la sezione "checks" è top-level nel YAML, non dentro l'environment)
    check_config = dict(env_config)
    if "checks" in global_config:
        check_config["checks"] = global_config["checks"]

    for CheckClass in check_classes:
        instance = CheckClass(config=check_config, caps=caps)

        if not instance.can_run():
            if instance.fallback is not None:
                fb_instance = instance.fallback(config=check_config, caps=caps)
                if fb_instance.can_run():
                    instance = fb_instance
                else:
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
            _debug.log(instance.__class__.__name__, "dry-run, skipping execution")
            result = CheckResult(
                name=instance.__class__.__name__,
                status="DRY_RUN",
                message="Would run (capability OK)"
            )
        else:
            _debug.log(instance.__class__.__name__, "running check")
            try:
                result = instance.run()
            except Exception as e:
                result = CheckResult(
                    name=instance.__class__.__name__,
                    status=CheckResult.UNKNOWN,
                    message="Unhandled exception: {}".format(str(e))
                )
            _debug.log(instance.__class__.__name__,
                       "result: {} — {}".format(result.status, result.message))

        results.append(result)

    return results


def print_text_report(env_name, results, caps_used):
    # type: (str, list, list) -> None
    status_icons = {
        CheckResult.OK:       "[OK      ]",
        CheckResult.WARNING:  "[WARNING ]",
        CheckResult.CRITICAL: "[CRITICAL]",
        CheckResult.UNKNOWN:  "[UNKNOWN ]",
        CheckResult.SKIPPED:  "[SKIPPED ]",
        "DRY_RUN":            "[DRY-RUN ]",
    }

    all_statuses = [CheckResult.OK, CheckResult.WARNING, CheckResult.CRITICAL,
                    CheckResult.UNKNOWN, CheckResult.SKIPPED, "DRY_RUN"]
    counts = {s: 0 for s in all_statuses}

    for r in results:
        icon = status_icons.get(r.status, "[?       ]")
        print("{}  {} — {}".format(icon, r.name, r.message))
        counts[r.status] = counts.get(r.status, 0) + 1

    print()
    summary_parts = []
    for s in [CheckResult.CRITICAL, CheckResult.WARNING, CheckResult.OK,
              CheckResult.UNKNOWN, CheckResult.SKIPPED, "DRY_RUN"]:
        if counts.get(s, 0) > 0:
            summary_parts.append("{} {}".format(counts[s], s))
    print("Summary: {}".format(", ".join(summary_parts) if summary_parts else "no checks run"))

    if caps_used:
        print("Capabilities: {}".format(", ".join(sorted(caps_used))))


def dispatch_alerts(results, cfg, env_name, output_format):
    # type: (list, dict, str, str) -> None
    """Lancia tutti gli alert handler configurati."""
    from alerts import log_alert, email_alert, webhook_alert, zabbix_alert

    log_alert.dispatch(results, cfg, env_name, output_format)
    email_alert.dispatch(results, cfg, env_name)
    webhook_alert.dispatch(results, cfg, env_name)
    zabbix_alert.dispatch(results, cfg, env_name)


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    # Attiva debug mode prima di qualsiasi altra operazione
    if args.debug:
        _debug.ENABLED = True
        _debug.log("main", "debug mode enabled")

    # Bootstrap — discover capabilities first (always, fast)
    caps = discover_capabilities()

    # --show-capabilities: stampa e esci senza caricare config
    if args.show_capabilities:
        print_capabilities(caps)
        sys.exit(0)

    # --env è required salvo --show-capabilities
    if not args.envs:
        parser.error("--env is required (use --show-capabilities to skip)")

    # Carica config
    try:
        cfg = load_config(args.config)
        _debug.log("main", "config loaded from {}".format(args.config))
    except Exception as e:
        print("ERROR loading config: {}".format(e), file=sys.stderr)
        sys.exit(1)

    # Assicura Ansible disponibile se serve
    caps = ensure_ansible(caps)

    if args.verbose:
        print_capabilities(caps)

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
            print("\nHadoopScope — {} @ {}".format(
                env_name,
                env_config.get("ambari_url") or env_config.get("cm_url", "")
            ))
            print("=" * 60)
            caps_used = [k for k, v in caps.items() if v is True]
            print_text_report(env_name, results, caps_used)

        # Dispatch alerts (salvo dry-run)
        if not args.dry_run:
            dispatch_alerts(results, cfg, env_name, args.output)

    if args.output == "json":
        output = {
            "version": "0.1.0",
            "capabilities": {k: v for k, v in caps.items() if v},
            "environments": {}
        }
        for env_name, results in all_results.items():
            output["environments"][env_name] = [
                {"check": r.name, "status": r.status,
                 "message": r.message, "details": r.details}
                for r in results
            ]
        print(json.dumps(output, indent=2))

    # Exit code: 2 if any CRITICAL, 1 if any WARNING, 0 otherwise
    worst = 0
    for results in all_results.values():
        for r in results:
            if r.status == CheckResult.CRITICAL:
                worst = 2
            elif r.status == CheckResult.WARNING and worst < 2:
                worst = 1
    sys.exit(worst)


if __name__ == "__main__":
    main()
