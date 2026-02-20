# HadoopScope

**Unified Hadoop cluster health monitoring — runs anywhere, requires nothing.**

[![Tests](https://github.com/disoardi/hadoopscope/actions/workflows/tests.yml/badge.svg)](https://github.com/disoardi/hadoopscope/actions/workflows/tests.yml)
[![Python 3.6+](https://img.shields.io/badge/python-3.6%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Monitors **HDP** (via Ambari REST API) and **CDP** (via Cloudera Manager REST API) from any machine — no Hadoop client, no Java, no special packages required.

## Quickstart

```bash
# Clone and run (3 commands)
git clone https://github.com/disoardi/hadoopscope
cd hadoopscope
export AMBARI_PASS=yourpassword
python3 hadoopscope.py --config config/example.yaml --env prod-hdp --dry-run
```

Or install via script:

```bash
curl -fsSL https://raw.githubusercontent.com/disoardi/hadoopscope/main/install.sh | bash
hadoopscope --env prod-hdp --dry-run
```

## Requirements

- **Python 3.6+** — stdlib only for the core (no pip needed)
- Network access to Ambari / Cloudera Manager API endpoints
- **Ansible** (optional) — auto-installed in isolated venv if needed for Hive checks

## Usage

```
python3 hadoopscope.py --env <ENV> [options]

Options:
  --config PATH       Config file (default: config/hadoopscope.yaml)
  --env ENV           Environment to check (repeatable for multi-env)
  --checks TYPE       all | health | hdfs | hive | yarn  (default: all)
  --output FORMAT     text | json  (default: text)
  --dry-run           Validate config + show planned checks, no network calls
  --show-capabilities Print available tools and exit
  --verbose           Include capability map in output
  --version           Show version
```

### Examples

```bash
# Check all services on prod-hdp
export AMBARI_PASS=secret
python3 hadoopscope.py --env prod-hdp

# HDFS checks only, JSON output
python3 hadoopscope.py --env prod-hdp --checks hdfs --output json

# Multi-environment
python3 hadoopscope.py --env prod-hdp --env dr-hdp

# Dry-run (no network)
python3 hadoopscope.py --env prod-hdp --dry-run

# Show what tools are available
python3 hadoopscope.py --show-capabilities
```

## Feature Matrix

| Check | Category | Requires | Description |
|-------|----------|----------|-------------|
| `AmbariServiceHealthCheck` | health | REST API | All HDP service states via Ambari |
| `NameNodeHACheck` | health | REST API | NameNode active/standby via Ambari |
| `ClusterAlertsCheck` | health | REST API | Active CRITICAL alerts from Ambari |
| `ConfigStalenessCheck` | health | REST API | Stale configurations not yet deployed |
| `ClouderaServiceHealthCheck` | health | REST API | All CDP service health via CM API |
| `HdfsSpaceCheck` | hdfs | REST API | Space usage per configured path |
| `HdfsDataNodeCheck` | hdfs | REST API | Dead/stale DataNodes via JMX |
| `HdfsWritabilityCheck` | hdfs | REST API | Write/delete test on HDFS |
| `YarnNodeHealthCheck` | yarn | REST API | UNHEALTHY/LOST YARN nodes |
| `YarnQueueCheck` | yarn | REST API | Queue capacity utilization |
| `HiveCheck` | hive | ansible OR docker | Beeline test query via edge node |

## Configuration

Copy and edit the example config:

```bash
cp config/example.yaml config/hadoopscope.yaml
```

### Minimal valid config (HDP)

```yaml
version: "1"

environments:
  prod-hdp:
    type: hdp
    ambari_url: https://ambari.corp.com:8080
    ambari_user: admin
    ambari_pass: "${AMBARI_PASS}"   # never hardcode!
    cluster_name: prod-cluster

    webhdfs:
      url: http://namenode.corp.com:9870
      user: hdfs

checks:
  service_health:
    enabled: true
    services: [HDFS, YARN, HIVE]

  hdfs_space:
    enabled: true
    paths:
      - path: /user/hive/warehouse
        warning_pct: 75
        critical_pct: 90

alerts:
  email:
    enabled: true
    smtp_host: smtp.corp.com
    smtp_port: 587
    smtp_tls: true
    from_addr: hadoopscope@corp.com
    to: [hadoop-ops@corp.com]

  log:
    enabled: true
    path: /var/log/hadoopscope/
    format: json
```

### Minimal valid config (CDP)

```yaml
version: "1"

environments:
  prod-cdp:
    type: cdp
    cm_url: https://cm.corp.com:7180
    cm_user: admin
    cm_pass: "${CM_PASS}"
    cluster_name: prod-cdp-cluster
    cm_api_version: "v40"
```

Full config reference: [config/example.yaml](config/example.yaml)

## Sample Output

### Text format

```
HadoopScope — prod-hdp @ https://ambari.corp.com:8080
============================================================
[OK      ]  AmbariServiceHealth — All 6 monitored services are STARTED
[OK      ]  NameNodeHA — Active: nn1.corp.com | Standby: nn2.corp.com
[OK      ]  ClusterAlerts — No active CRITICAL alerts
[WARNING ]  HdfsSpace — /user/hive/warehouse: 82% (WARNING)
[OK      ]  HdfsDataNodes — 10 live, 0 dead, 1 stale DataNodes
[OK      ]  YarnNodeHealth — 12 nodes RUNNING

Summary: 1 WARNING, 5 OK
Capabilities: ansible, docker, kinit, klist
```

### JSON format

```json
{
  "version": "0.1.0",
  "capabilities": {"ansible": true, "docker": true},
  "environments": {
    "prod-hdp": [
      {
        "check": "AmbariServiceHealth",
        "status": "OK",
        "message": "All 6 monitored services are STARTED",
        "details": {}
      },
      {
        "check": "HdfsSpace",
        "status": "WARNING",
        "message": "/user/hive/warehouse: 82% (WARNING)",
        "details": {"/user/hive/warehouse": {"used_pct": 82.0}}
      }
    ]
  }
}
```

## Alerts

| Alert | Config key | Description |
|-------|-----------|-------------|
| Log file | `alerts.log` | JSON or text log file (always available) |
| Email | `alerts.email` | SMTP with/without TLS and auth |
| Webhook | `alerts.webhook` | HTTP POST JSON to any endpoint |
| Zabbix | `alerts.zabbix` | Via `zabbix_sender` subprocess |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All checks OK or SKIPPED |
| 1 | At least one WARNING |
| 2 | At least one CRITICAL |

## Testing

```bash
# Unit tests (no cluster needed)
python3 tests/test_base.py
python3 tests/test_checks.py

# All tests
python3 tests/run_all.py

# Integration tests with Docker mock
docker compose up --build --abort-on-container-exit
```

## TuxBox

```bash
tbox run hadoopscope -- --env prod-hdp --dry-run
```

## Architecture

```
CLI (argparse)
    └── Bootstrap (discover_capabilities)
            └── Executor (run_checks_for_env)
                    ├── checks/ambari.py   — HDP: Ambari REST API
                    ├── checks/cloudera.py — CDP: Cloudera Manager REST
                    ├── checks/webhdfs.py  — HDFS: WebHDFS + JMX
                    ├── checks/yarn.py     — YARN: RM REST API
                    └── checks/hive.py     — Hive: Ansible + beeline
                            └── alerts/
                                    ├── log_alert.py
                                    ├── email_alert.py
                                    ├── webhook_alert.py
                                    └── zabbix_alert.py
```

Each check extends `CheckBase` with:
- `requires`: capability OR-groups (e.g., `[["ansible"], ["docker"]]`)
- `fallback`: alternative check class if primary can't run
- `run()`: always returns `CheckResult`, never raises

## License

MIT — see [LICENSE](LICENSE)
