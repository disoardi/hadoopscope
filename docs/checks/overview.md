# Checks Overview

HadoopScope groups checks into four categories selectable via `--checks`:

| Category | Flag | Description |
|----------|------|-------------|
| Health | `--checks health` | Service states, HA, alerts |
| HDFS | `--checks hdfs` | Space, DataNodes, writability |
| YARN | `--checks yarn` | Node health, queue utilization |
| Hive | `--checks hive` | HiveServer2 availability |
| All | `--checks all` | All categories (default) |

## Capability-based execution

Each check declares what it `requires` to run. If a requirement isn't met, the check is gracefully **SKIPPED** (not failed). Example:

```
[SKIPPED ]  HiveCheck — Requires: [['ansible'], ['venv_ansible'], ['docker_ansible_image']]
```

HadoopScope tries alternatives in order:
1. If the primary check can run → run it
2. If it can't but has a `fallback` → try the fallback
3. If nothing can run → SKIPPED

## Check results

| Status | Meaning |
|--------|---------|
| `OK` | Check passed, everything looks good |
| `WARNING` | Issue detected, non-critical |
| `CRITICAL` | Serious issue requiring attention |
| `UNKNOWN` | Could not retrieve data (network error, timeout) |
| `SKIPPED` | Required tools not available |

## All checks

| Check | Category | Module | Cluster Type |
|-------|----------|--------|-------------|
| AmbariServiceHealthCheck | health | ambari.py | HDP |
| NameNodeHACheck | health | ambari.py | HDP |
| ClusterAlertsCheck | health | ambari.py | HDP |
| ConfigStalenessCheck | health | ambari.py | HDP |
| ClouderaServiceHealthCheck | health | cloudera.py | CDP |
| ClouderaParcelCheck | health | cloudera.py | CDP |
| HdfsSpaceCheck | hdfs | webhdfs.py | HDP + CDP |
| HdfsDataNodeCheck | hdfs | webhdfs.py | HDP + CDP |
| HdfsWritabilityCheck | hdfs | webhdfs.py | HDP + CDP |
| YarnNodeHealthCheck | yarn | yarn.py | HDP + CDP |
| YarnQueueCheck | yarn | yarn.py | HDP + CDP |
| HiveCheck | hive | hive.py | HDP + CDP |
