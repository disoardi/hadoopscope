# HadoopScope

**Unified Hadoop cluster health monitoring — runs anywhere, requires nothing.**

Monitors HDP (via Ambari REST API) and CDP (via Cloudera Manager REST API) from any machine — no Hadoop client required.

## Quickstart

```bash
# Install via TuxBox
tbox run hadoopscope -- --help

# Or direct
git clone https://github.com/disoardi/hadoopscope
cp config/example.yaml config/hadoopscope.yaml
# edit config/hadoopscope.yaml
export AMBARI_PASS=yourpassword
python3 hadoopscope.py --env prod-hdp
```

## Requirements

- Python 3.6+ (stdlib only for core)
- Network access to Ambari/CM API endpoints
- Ansible: optional (auto-installed in venv if needed for CLI checks)

## Status

🚧 Work in progress — v0.1.0-dev
