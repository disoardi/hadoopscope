# Ambari Checks (HDP)

These checks target **HDP clusters** managed by Apache Ambari.

## AmbariServiceHealthCheck

Checks the state of all HDP services (or a configured subset).

**API:** `GET /api/v1/clusters/{name}/services?fields=ServiceInfo/state,ServiceInfo/service_name`

| Result | Condition |
|--------|-----------|
| OK | All services STARTED |
| WARNING | Services INSTALLED (stopped intentionally) |
| CRITICAL | Services in STOPPED, UNKNOWN, or other non-running state |
| UNKNOWN | Cannot reach Ambari API |

**Config:**
```yaml
checks:
  service_health:
    services: [HDFS, YARN, HIVE]   # omit to check all services
```

## NameNodeHACheck

Verifies NameNode High Availability — one active, one or more standby.

**API:** `GET /api/v1/clusters/{name}/services/HDFS/components/NAMENODE?fields=metrics/dfs/FSNamesystem/HAState,...`

| Result | Condition |
|--------|-----------|
| OK | Exactly 1 active, ≥1 standby |
| WARNING | HA state unclear |
| CRITICAL | No active NameNode |
| UNKNOWN | Cannot reach Ambari API |

## ClusterAlertsCheck

Counts active CRITICAL alerts (not in maintenance mode).

**API:** `GET /api/v1/clusters/{name}/alerts?fields=*&Alert/state=CRITICAL&Alert/maintenance_state=OFF`

| Result | Condition |
|--------|-----------|
| OK | No active CRITICAL alerts |
| CRITICAL | ≥1 active CRITICAL alert |
| UNKNOWN | Cannot reach Ambari API |

## ConfigStalenessCheck

Detects services with stale configuration that hasn't been deployed to all hosts.

**API:** `GET /api/v1/clusters/{name}/services?fields=ServiceInfo/config_staleness_check_issues,...`

| Result | Condition |
|--------|-----------|
| OK | All configs propagated |
| WARNING | ≥1 service has stale config |
| UNKNOWN | Cannot reach Ambari API |
