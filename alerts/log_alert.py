"""Log alert — scrive risultati su stdout e/o file JSON/text."""

from __future__ import print_function

import json
import os
import sys
from datetime import datetime

from checks.base import CheckResult


def dispatch(results, config, env_name, output_format="text"):
    # type: (list, dict, str, str) -> None
    """Scrive i risultati sul log configurato."""
    log_cfg = config.get("alerts", {}).get("log", {})
    if not log_cfg.get("enabled", True):
        return

    log_format = log_cfg.get("format", output_format)
    log_path   = log_cfg.get("path")

    if log_format == "json":
        content = json.dumps({
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "environment": env_name,
            "results": [
                {"check": r.name, "status": r.status,
                 "message": r.message, "details": r.details}
                for r in results
            ]
        }, indent=2)
    else:
        lines = []
        for r in results:
            lines.append("[{}] {} — {}".format(r.status.ljust(8), r.name, r.message))
        content = "\n".join(lines)

    if log_path:
        os.makedirs(log_path, exist_ok=True)
        filename = os.path.join(
            log_path,
            "hadoopscope-{}-{}.{}".format(
                env_name,
                datetime.utcnow().strftime("%Y%m%d-%H%M%S"),
                "json" if log_format == "json" else "log"
            )
        )
        with open(filename, "w") as f:
            f.write(content)
