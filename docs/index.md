# HadoopScope

**Unified Hadoop cluster health monitoring — runs anywhere, requires nothing.**

HadoopScope monitors **HDP** (Hortonworks Data Platform) via the Ambari REST API and **CDP** (Cloudera Data Platform) via the Cloudera Manager REST API — from any machine, without installing Hadoop clients, Java, or any Python packages beyond the stdlib.

## Key Features

- **Zero dependencies** for the core — only Python 3.6+ stdlib
- **HDP + CDP** support in a single tool
- **10+ checks** covering services, HDFS, YARN, and Hive
- **4 alert channels** — log file, email, webhook, Zabbix
- **Graceful degradation** — checks requiring Ansible/Docker are skipped (not failed) when those tools aren't available
- **Docker-ready** — full integration test environment included

## Quick Example

```bash
export AMBARI_PASS=secret
python3 hadoopscope.py --env prod-hdp --checks health --output text
```

```
HadoopScope — prod-hdp @ https://ambari.corp.com:8080
============================================================
[OK      ]  AmbariServiceHealth — All 6 monitored services are STARTED
[OK      ]  NameNodeHA — Active: nn1 | Standby: nn2
[OK      ]  ClusterAlerts — No active CRITICAL alerts
[OK      ]  ConfigStaleness — All service configs propagated

Summary: 4 OK
```

## Navigation

- [Installation](getting-started/installation.md) — How to install HadoopScope
- [Quickstart](getting-started/quickstart.md) — Run your first check in 5 minutes
- [Configuration](getting-started/configuration.md) — Full config reference
- [Checks](checks/overview.md) — All available checks
- [CLI Reference](cli.md) — Command line options
