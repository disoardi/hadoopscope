# Alerts

HadoopScope dispatches alerts after each check run (unless `--dry-run` is active).
Multiple alert channels can be active simultaneously.

## Log file

Always available. Writes JSON or text to a file or directory.

```yaml
alerts:
  log:
    enabled: true
    path: /var/log/hadoopscope/    # directory — files auto-named
    format: json                   # json | text
    rotate_days: 30
```

## Email

Uses `smtplib` from Python stdlib. Supports SMTP with/without TLS and auth.

```yaml
alerts:
  email:
    enabled: true
    smtp_host: smtp.corp.com
    smtp_port: 587
    smtp_tls: true
    smtp_user: "${SMTP_USER}"
    smtp_pass: "${SMTP_PASS}"
    from_addr: hadoopscope@corp.com
    to: [ops@corp.com]
    on_severity: [WARNING, CRITICAL]
```

Emails are sent only when results contain statuses in `on_severity`.

## Webhook

HTTP POST JSON to any endpoint. Useful for Slack, Teams, PagerDuty, etc.

```yaml
alerts:
  webhook:
    enabled: true
    url: https://hooks.slack.com/services/XXX/YYY/ZZZ
    secret: "${WEBHOOK_SECRET}"    # added as X-HadoopScope-Secret header
    on_severity: [WARNING, CRITICAL]
    timeout: 10
```

Payload format:

```json
{
  "source": "hadoopscope",
  "environment": "prod-hdp",
  "timestamp": "2026-02-20T10:00:00Z",
  "alerts": [
    {"check": "HdfsSpace", "status": "WARNING",
     "message": "/user: 82%", "details": {}}
  ]
}
```

## Zabbix

Sends metrics via `zabbix_sender` subprocess.

```yaml
alerts:
  zabbix:
    enabled: true
    server: zabbix.corp.com
    port: 10051
    host: hadoop-monitor    # Zabbix host name
    binary: zabbix_sender   # optional: path to binary
```

Metric keys format: `hadoopscope.check[<check_name>]` (0=OK, 1=UNKNOWN, 2=WARNING, 4=CRITICAL)
