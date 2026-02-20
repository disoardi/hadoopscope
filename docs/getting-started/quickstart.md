# Quickstart

## 1. Get HadoopScope

```bash
git clone https://github.com/disoardi/hadoopscope
cd hadoopscope
```

## 2. Create your config

```bash
cp config/example.yaml config/hadoopscope.yaml
# Edit the file — at minimum set ambari_url, ambari_user, cluster_name
```

## 3. Set credentials

```bash
export AMBARI_PASS=yourpassword
```

## 4. Dry-run (no network calls)

```bash
python3 hadoopscope.py --env prod-hdp --dry-run
```

This validates your config and shows what checks would run — without making any API calls.

## 5. Real run

```bash
python3 hadoopscope.py --env prod-hdp
```

## What you'll see

```
HadoopScope — prod-hdp @ https://ambari.corp.com:8080
============================================================
[OK      ]  AmbariServiceHealth — All 6 monitored services are STARTED
[OK      ]  NameNodeHA — Active: nn1.corp.com | Standby: nn2.corp.com
[OK      ]  ClusterAlerts — No active CRITICAL alerts
[WARNING ]  HdfsSpace — /user/hive/warehouse: 82% (WARNING)
[OK      ]  HdfsDataNodes — 10 live, 0 dead, 1 stale DataNodes
[SKIPPED ]  HiveCheck — Requires: [['ansible'], ['venv_ansible'], ['docker_ansible_image']]. Install missing tools or use Docker.
[OK      ]  YarnNodeHealth — 12 nodes RUNNING
[OK      ]  YarnQueues — All queues below usage threshold (warn=80%, crit=90%)

Summary: 1 WARNING, 6 OK, 1 SKIPPED
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All checks OK or SKIPPED |
| 1 | At least one WARNING |
| 2 | At least one CRITICAL |

Use this in monitoring scripts:

```bash
python3 hadoopscope.py --env prod-hdp
case $? in
  0) echo "All good" ;;
  1) echo "Warnings" ;;
  2) echo "Critical issues!" ;;
esac
```
