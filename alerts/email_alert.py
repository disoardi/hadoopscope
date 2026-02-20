"""Email alert via smtplib stdlib. Zero deps."""

from __future__ import print_function

import smtplib
import sys
from datetime import datetime

try:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
except ImportError:
    # Python 2 fallback
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText

from checks.base import CheckResult


def dispatch(results, config, env_name):
    # type: (list, dict, str) -> None
    """Invia email di alert se ci sono risultati WARNING o CRITICAL."""
    email_cfg = config.get("alerts", {}).get("email", {})
    if not email_cfg.get("enabled", False):
        return

    alert_on = email_cfg.get("on_severity", [CheckResult.WARNING, CheckResult.CRITICAL])
    filtered = [r for r in results if r.status in alert_on]
    if not filtered:
        return

    smtp_host = email_cfg.get("smtp_host", "localhost")
    smtp_port = int(email_cfg.get("smtp_port", 25))
    smtp_tls  = email_cfg.get("smtp_tls", False)
    from_addr = email_cfg.get("from_addr", "hadoopscope@localhost")
    to_addrs  = email_cfg.get("to", [])
    if not to_addrs:
        return

    worst = CheckResult.WARNING
    for r in filtered:
        if r.status == CheckResult.CRITICAL:
            worst = CheckResult.CRITICAL
            break

    subject = "[HadoopScope][{}] {} — {} issue(s) on {}".format(
        worst, env_name, len(filtered),
        datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    )

    # Body text
    lines = ["HadoopScope Alert Report", "=" * 50, ""]
    for r in filtered:
        lines.append("[{}] {} — {}".format(r.status, r.name, r.message))
    lines += ["", "Full results: see log file."]
    body_text = "\n".join(lines)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addrs)
    msg.attach(MIMEText(body_text, "plain"))

    try:
        if smtp_tls:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)

        smtp_user = email_cfg.get("smtp_user")
        smtp_pass = email_cfg.get("smtp_pass")
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)

        server.sendmail(from_addr, to_addrs, msg.as_string())
        server.quit()
        print("[alert/email] Sent to: {}".format(", ".join(to_addrs)), file=sys.stderr)

    except Exception as e:
        print("[alert/email] ERROR: {}".format(str(e)), file=sys.stderr)
