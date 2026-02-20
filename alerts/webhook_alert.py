"""Webhook alert via urllib stdlib. Invia POST JSON al webhook configurato."""

from __future__ import print_function

import json
import sys
from datetime import datetime

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib2 import urlopen, Request, URLError, HTTPError

from checks.base import CheckResult


def dispatch(results, config, env_name):
    # type: (list, dict, str) -> None
    """Invia risultati al webhook se ci sono WARNING/CRITICAL."""
    wh_cfg = config.get("alerts", {}).get("webhook", {})
    if not wh_cfg.get("enabled", False):
        return

    url      = wh_cfg.get("url", "")
    on_sev   = wh_cfg.get("on_severity", [CheckResult.WARNING, CheckResult.CRITICAL])
    secret   = wh_cfg.get("secret", "")
    timeout  = int(wh_cfg.get("timeout", 10))

    if not url:
        print("[alert/webhook] WARNING: url not configured", file=sys.stderr)
        return

    filtered = [r for r in results if r.status in on_sev]
    if not filtered:
        return

    payload = {
        "source":      "hadoopscope",
        "environment": env_name,
        "timestamp":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "alerts": [
            {"check": r.name, "status": r.status,
             "message": r.message, "details": r.details}
            for r in filtered
        ]
    }

    data = json.dumps(payload).encode("utf-8")
    req  = Request(url, data=data)
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "HadoopScope/0.1.0")
    if secret:
        req.add_header("X-HadoopScope-Secret", secret)

    try:
        resp = urlopen(req, timeout=timeout)
        print("[alert/webhook] Sent {} alert(s) to {} (HTTP {})".format(
            len(filtered), url, resp.getcode()), file=sys.stderr)
    except (URLError, HTTPError, Exception) as e:
        print("[alert/webhook] ERROR: {}".format(str(e)), file=sys.stderr)
