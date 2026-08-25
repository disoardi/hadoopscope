# HDFS Checks (WebHDFS)

These checks work via the **WebHDFS REST API** — no Hadoop client or HDFS CLI needed.

## HA: `namenode_urls`

`HdfsSpaceCheck`, `HdfsDataNodeCheck`, and `HdfsWritabilityCheck` all read
this optional list (falling back to a single `webhdfs.url` if absent):

```yaml
webhdfs:
  url: http://nn1:9870          # used when namenode_urls is absent
  namenode_urls:
    - http://nn1:9870
    - http://nn2:9870
```

Reads (JMX queries) usually succeed against either NameNode. **Writes do
not** — the standby NameNode rejects `CREATE` with `StandbyException`
instead of redirecting, so `HdfsWritabilityCheck` retries every URL in
`namenode_urls` in order and only fails if all of them reject the write.

!!! warning "HttpFS vs NameNode WebUI — different ports"
    On clusters without a dedicated HttpFS load balancer, `webhdfs.url`
    (HttpFS, typically port 14000/14001) and `webhdfs.namenode_urls`
    (NameNode WebUI, typically 9870/9871 with Auto-TLS) are **not**
    interchangeable — HttpFS doesn't serve `/jmx`. Pointing
    `namenode_urls` at the HttpFS port doesn't error out: it silently
    returns `CapacityTotal=0` / "0 live DataNodes" instead.

Kerberized clusters: set `webhdfs.via_ansible: true` if these checks need
to run from an edge node instead of the local machine (`webhdfs.kerberos`
overrides the edge-node keytab/principal, see the main `CLAUDE.md`
Kerberos table for the full picture of which config key applies where).

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
