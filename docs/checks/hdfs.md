# HDFS Checks (WebHDFS)

These checks work via the **WebHDFS REST API** — no Hadoop client or HDFS CLI needed.

## HdfsSpaceCheck

Monitors space usage for configured HDFS paths.

**API:** `GET /webhdfs/v1/{path}?op=GETCONTENTSUMMARY`

| Result | Condition |
|--------|-----------|
| OK | All paths below warning threshold |
| WARNING | ≥1 path above `warning_pct` |
| CRITICAL | ≥1 path above `critical_pct` |
| UNKNOWN | Cannot reach WebHDFS or path unreachable |

**Config:**
```yaml
webhdfs:
  url: http://namenode:9870
  user: hdfs

checks:
  hdfs_space:
    paths:
      - path: /user/hive/warehouse
        warning_pct: 75
        critical_pct: 90
      - path: /tmp
        warning_pct: 80
        critical_pct: 95
```

## HdfsDataNodeCheck

Checks the number of dead/stale DataNodes via the NameNode JMX endpoint.

**API:** `GET /jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState`

| Result | Condition |
|--------|-----------|
| OK | Dead DataNodes below warning threshold |
| WARNING | Dead DataNodes ≥ `warning_threshold` |
| CRITICAL | Dead DataNodes ≥ `critical_threshold` |
| UNKNOWN | Cannot reach JMX endpoint |

**Config:**
```yaml
checks:
  hdfs_dead_datanodes:
    warning_threshold: 1
    critical_threshold: 3
```

## HdfsWritabilityCheck

Writes a small test file to HDFS and immediately deletes it to verify write access.

**API:** `PUT /webhdfs/v1/{path}?op=CREATE` + `DELETE /webhdfs/v1/{path}?op=DELETE`

| Result | Condition |
|--------|-----------|
| OK | Write and delete succeeded |
| CRITICAL | Write or delete failed |
| UNKNOWN | WebHDFS not configured |

**Config:**
```yaml
checks:
  hdfs_writability:
    test_path: /tmp/.hadoopscope-probe
```
