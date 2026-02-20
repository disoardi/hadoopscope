# CLI Reference

## Synopsis

```
hadoopscope [--config PATH] --env ENV [--env ENV ...] [options]
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--config PATH` | `config/hadoopscope.yaml` | Path to YAML config file |
| `--env ENV` | (required) | Environment name from config. Repeatable for multi-env checks. |
| `--checks TYPE` | `all` | `all` \| `health` \| `hdfs` \| `hive` \| `yarn` |
| `--output FORMAT` | `text` | `text` \| `json` |
| `--dry-run` | false | Validate config + show planned checks without API calls |
| `--show-capabilities` | false | Print detected tools and exit (no `--env` needed) |
| `--verbose` | false | Include capability map in text output |
| `--version` | — | Show version and exit |
| `--help` | — | Show help and exit |

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All checks OK, SKIPPED, or DRY_RUN |
| 1 | At least one WARNING |
| 2 | At least one CRITICAL |

## Environment variables

HadoopScope itself doesn't require env vars, but your config likely does:

```bash
export AMBARI_PASS=your_ambari_password
export CM_PASS=your_cm_password
export SMTP_PASS=your_smtp_password
```

## Examples

```bash
# Basic health check
python3 hadoopscope.py --env prod-hdp

# HDFS checks only, JSON output → pipe to jq
python3 hadoopscope.py --env prod-hdp --checks hdfs --output json | jq .

# Multi-environment (prod + DR)
python3 hadoopscope.py --env prod-hdp --env dr-hdp

# Dry-run to validate config without hitting APIs
python3 hadoopscope.py --env prod-hdp --dry-run

# Check capabilities (what tools are available)
python3 hadoopscope.py --show-capabilities

# CDP cluster
python3 hadoopscope.py --env prod-cdp --checks health

# Cron-friendly with exit code
python3 hadoopscope.py --env prod-hdp --output json > /tmp/hs.json 2>&1
echo "Exit: $?"
```

## Cron example

```cron
# Run every 5 minutes, alert on failure
*/5 * * * * AMBARI_PASS=secret /path/to/hadoopscope.py \
    --config /etc/hadoopscope/prod.yaml \
    --env prod-hdp \
    --output json \
    > /var/log/hadoopscope/latest.json 2>&1
```
