"""Email alert via smtplib stdlib. Zero deps.

HTML body layout (based on cluster health report template):
  - Header bar (colour: green/orange/red based on worst status)
  - Meta box  (timestamp, environment, overall status)
  - Executive Summary tiles  (Healthy / Warning / Critical / Unknown / Skipped)
  - Issues table  (CHECK | SEVERITY | MESSAGE) — only WARNING/CRITICAL/UNKNOWN rows
  - Footer
"""

from __future__ import print_function

import smtplib
import sys
from datetime import datetime

try:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
except ImportError:
    # Python 2 fallback (never used in practice but keeps linters happy)
    from email.MIMEMultipart import MIMEMultipart   # type: ignore
    from email.MIMEText import MIMEText             # type: ignore

from checks.base import CheckResult


# ── HTML helpers ───────────────────────────────────────────────────────────────

def _html_esc(s):
    # type: (str) -> str
    """Minimal HTML escaping for embedding user data in HTML content."""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def _html_msg(s):
    # type: (str) -> str
    """HTML-escape and convert newlines to <br> for multi-line messages."""
    return _html_esc(s).replace("\n", "<br>")


def _build_html_body(results, env_name, timestamp):
    # type: (list, str, str) -> str
    """Build an HTML email body following the cluster health report layout."""

    # ── Status counts ────────────────────────────────────────────────────────
    statuses = [CheckResult.OK, CheckResult.WARNING,
                CheckResult.CRITICAL, CheckResult.UNKNOWN, CheckResult.SKIPPED]
    counts = {s: 0 for s in statuses}
    for r in results:
        if r.status in counts:
            counts[r.status] += 1
    total = max(len(results), 1)

    def pct(n):
        # type: (int) -> str
        return "{:.1f}%".format(100.0 * n / total)

    # Header accent colour + overall label based on worst status
    if counts[CheckResult.CRITICAL] > 0:
        accent  = "#c0392b"
        overall = "CRITICAL"
    elif counts[CheckResult.WARNING] > 0:
        accent  = "#e67e22"
        overall = "WARNING"
    else:
        accent  = "#27ae60"
        overall = "OK"

    # ── Stat tiles (3 + 2 layout, mirrors the reference template) ────────────
    def _tile(label, bg, fg, border, n):
        # type: (str, str, str, str, int) -> str
        return (
            '<td style="width:33%;text-align:center;padding:14px 8px;'
            'background:' + bg + ';border:1px solid ' + border + ';border-radius:3px;">'
            '<div style="font-size:13px;font-weight:bold;color:' + fg + ';'
            'margin-bottom:4px;">' + label + '</div>'
            '<div style="font-size:30px;font-weight:bold;color:' + fg + ';'
            'line-height:1.1;">' + str(n) + '</div>'
            '<div style="font-size:11px;color:' + fg + ';">' + pct(n) + '</div>'
            '</td>'
        )

    tile_row1 = (
        _tile("Healthy",  "#dff0d8", "#3c763d", "#d6e9c6", counts[CheckResult.OK])       +
        _tile("Warning",  "#fcf8e3", "#8a6d3b", "#faebcc", counts[CheckResult.WARNING])  +
        _tile("Critical", "#f2dede", "#a94442", "#ebccd1", counts[CheckResult.CRITICAL])
    )
    tile_row2 = (
        _tile("Unknown", "#f5f5f5", "#555555", "#e0e0e0", counts[CheckResult.UNKNOWN]) +
        _tile("Skipped",  "#f5f5f5", "#aaaaaa", "#e0e0e0", counts[CheckResult.SKIPPED]) +
        '<td style="width:33%;"></td>'
    )

    # ── Issues table ─────────────────────────────────────────────────────────
    issue_results = [r for r in results
                     if r.status in (CheckResult.WARNING, CheckResult.CRITICAL,
                                     CheckResult.UNKNOWN)]

    _badge = {
        CheckResult.WARNING:  ("background:#fcf8e3;color:#8a6d3b;"
                               "border:1px solid #f0ad4e;"),
        CheckResult.CRITICAL: ("background:#f2dede;color:#a94442;"
                               "border:1px solid #ebccd1;"),
        CheckResult.UNKNOWN:  ("background:#f5f5f5;color:#555555;"
                               "border:1px solid #cccccc;"),
    }

    rows_html = ""
    for idx, r in enumerate(issue_results):
        row_bg  = "#ffffff" if idx % 2 == 0 else "#f9f9f9"
        bstyle  = _badge.get(r.status, "background:#eeeeee;color:#555555;")
        rows_html += (
            '<tr style="background:' + row_bg + ';">'
            '<td style="padding:8px 12px;border-bottom:1px solid #eeeeee;'
            'font-size:13px;word-wrap:break-word;overflow-wrap:break-word;">'
            + _html_esc(r.name) + '</td>'
            '<td style="padding:8px 12px;border-bottom:1px solid #eeeeee;">'
            '<span style="display:inline-block;padding:3px 9px;border-radius:3px;'
            'font-size:11px;font-weight:bold;' + bstyle + '">'
            + _html_esc(r.status) + '</span></td>'
            '<td style="padding:8px 12px;border-bottom:1px solid #eeeeee;'
            'font-size:13px;color:#555555;'
            'word-wrap:break-word;overflow-wrap:break-word;">'
            + _html_msg(r.message) + '</td>'
            '</tr>'
        )

    if issue_results:
        issues_section = (
            '<tr><td style="padding:0 24px 20px 24px;">'
            '<h2 style="font-size:14px;color:#8a6d3b;margin:0 0 10px 0;'
            'border-left:4px solid #f0ad4e;padding-left:8px;">'
            '&#9888; Issues Detected &mdash; '
            + str(len(issue_results)) + ' check(s) require attention'
            '</h2>'
            '<table width="100%" cellspacing="0" cellpadding="0" '
            'style="border-collapse:collapse;table-layout:fixed;">'
            '<tr style="background:#2a6496;color:#ffffff;font-size:12px;">'
            '<td style="padding:8px 12px;width:28%;font-weight:bold;">CHECK</td>'
            '<td style="padding:8px 12px;width:16%;font-weight:bold;">SEVERITY</td>'
            '<td style="padding:8px 12px;width:56%;font-weight:bold;">MESSAGE</td>'
            '</tr>'
            + rows_html +
            '</table></td></tr>'
        )
    else:
        issues_section = (
            '<tr><td style="padding:0 24px 20px 24px;font-size:13px;">'
            '<span style="color:#3c763d;font-weight:bold;">&#10003;</span>'
            '&nbsp;All checks passed &mdash; no issues detected.'
            '</td></tr>'
        )

    # ── Assemble full document ────────────────────────────────────────────────
    return (
        '<!DOCTYPE html>'
        '<html><body style="font-family:Arial,Helvetica,sans-serif;color:#333333;'
        'background:#f0f0f0;margin:0;padding:20px;">'
        '<table width="700" cellpadding="0" cellspacing="0" '
        'style="max-width:700px;margin:0 auto;background:#ffffff;border:1px solid #dddddd;">'

        # Header
        '<tr><td style="background:' + accent + ';padding:18px 24px;">'
        '<div style="font-size:18px;font-weight:bold;color:#ffffff;">'
        '&#128269; HadoopScope &mdash; Cluster Health Check Report'
        '</div>'
        '<div style="border-bottom:2px solid rgba(255,255,255,0.35);'
        'margin-top:10px;"></div>'
        '</td></tr>'

        # Meta
        '<tr><td style="padding:14px 24px;background:#f9f9f9;'
        'border-bottom:1px solid #eeeeee;font-size:13px;line-height:1.8;">'
        '<b>Report Generated:</b>&nbsp;' + _html_esc(timestamp) + '<br>'
        '<b>Environment:</b>&nbsp;' + _html_esc(env_name) + '&nbsp;&nbsp;'
        '<b>Overall Status:</b>&nbsp;'
        '<span style="color:' + accent + ';font-weight:bold;">' + overall + '</span>'
        '</td></tr>'

        # Executive Summary
        '<tr><td style="padding:20px 24px 16px 24px;">'
        '<h2 style="font-size:14px;color:#2a6496;margin:0 0 12px 0;'
        'border-left:4px solid #2a6496;padding-left:8px;">'
        '&#9642;&nbsp;Executive Summary'
        '</h2>'
        '<table width="100%" cellspacing="6" cellpadding="0">'
        '<tr>' + tile_row1 + '</tr>'
        '<tr style="height:8px;"><td colspan="3"></td></tr>'
        '<tr>' + tile_row2 + '</tr>'
        '</table>'
        '</td></tr>'

        # Issues section
        + issues_section +

        # Footer
        '<tr><td style="background:#f9f9f9;border-top:1px solid #eeeeee;'
        'padding:10px 24px;font-size:11px;color:#aaaaaa;text-align:center;">'
        'HadoopScope &mdash; Automated Hadoop cluster monitoring'
        '</td></tr>'

        '</table></body></html>'
    )


# ── Alert dispatcher ───────────────────────────────────────────────────────────

def dispatch(results, config, env_name):
    # type: (list, dict, str) -> None
    """Invia email di alert se ci sono risultati WARNING o CRITICAL."""
    email_cfg = config.get("alerts", {}).get("email", {})
    if not email_cfg.get("enabled", False):
        return

    alert_on = email_cfg.get("on_severity",
                              [CheckResult.WARNING, CheckResult.CRITICAL])
    if not any(r.status in alert_on for r in results):
        return

    smtp_host = email_cfg.get("smtp_host", "localhost")
    smtp_port = int(email_cfg.get("smtp_port", 25))
    smtp_tls  = email_cfg.get("smtp_tls", False)
    from_addr = email_cfg.get("from_addr", "hadoopscope@localhost")
    to_addrs  = email_cfg.get("to", [])
    if not to_addrs:
        return

    worst = CheckResult.WARNING
    for r in results:
        if r.status == CheckResult.CRITICAL and r.status in alert_on:
            worst = CheckResult.CRITICAL
            break

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    n_issues  = sum(1 for r in results if r.status in alert_on)
    subject   = "[HadoopScope][{}] {} \u2014 {} issue(s) on {}".format(
        worst, env_name, n_issues, timestamp
    )

    # Plain-text fallback (RFC 2046 — multipart/alternative, text first)
    lines = ["HadoopScope Alert Report", "=" * 50, ""]
    for r in results:
        if r.status in alert_on:
            lines.append("[{}] {} \u2014 {}".format(r.status, r.name, r.message))
    lines += ["", "Generated: " + timestamp]
    body_text = "\n".join(lines)

    # Rich HTML body
    body_html = _build_html_body(results, env_name, timestamp)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addrs)
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html",  "utf-8"))

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        if smtp_tls:
            server.starttls()

        smtp_user = email_cfg.get("smtp_user")
        smtp_pass = email_cfg.get("smtp_pass")
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)

        server.sendmail(from_addr, to_addrs, msg.as_string())
        server.quit()
        print("[alert/email] Sent to: {}".format(", ".join(to_addrs)),
              file=sys.stderr)

    except Exception as e:
        print("[alert/email] ERROR: {}".format(str(e)), file=sys.stderr)
