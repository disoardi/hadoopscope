# YARN Checks

These checks use the **YARN Resource Manager REST API** — no Hadoop client needed.

The RM URL is resolved in this priority order:
1. `config.yarn.rm_url` (explicit)
2. Auto-derived from `ambari_url` (replaces port with 8088)

## YarnNodeHealthCheck

Checks that all YARN nodes are in RUNNING state.

**API:** `GET /ws/v1/cluster/nodes`

| Result | Condition |
|--------|-----------|
| OK | All nodes RUNNING |
| WARNING | ≥1 node UNHEALTHY |
| CRITICAL | ≥1 node LOST |
| UNKNOWN | Cannot reach YARN RM |

**Config:**
```yaml
# Optional — auto-detected from ambari_url if omitted
yarn:
  rm_url: http://rm-host:8088
```

## YarnQueueCheck

Monitors queue capacity utilization in the YARN scheduler.

**API:** `GET /ws/v1/cluster/scheduler`

Works with both `CapacityScheduler` and `FairScheduler` queue hierarchies.

| Result | Condition |
|--------|-----------|
| OK | All queues below warning threshold |
| WARNING | ≥1 queue above `usage_warning_pct` |
| CRITICAL | ≥1 queue above `usage_critical_pct` |
| UNKNOWN | Cannot reach YARN RM |

**Config:**
```yaml
checks:
  yarn_queues:
    usage_warning_pct: 80
    usage_critical_pct: 90
```
