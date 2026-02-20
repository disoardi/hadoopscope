# Architecture

## Layer stack

```
CLI (argparse)
    │
    ├── Bootstrap layer (bootstrap.py)
    │       discover_capabilities() → capability map
    │       ensure_ansible()        → install if needed
    │
    └── Executor (hadoopscope.py: run_checks_for_env)
            │
            ├── check_registry (per env_type)
            │
            ├── For each CheckClass:
            │       instance = CheckClass(config, caps)
            │       if not instance.can_run():
            │           try instance.fallback
            │           else: SKIPPED
            │       else:
            │           result = instance.run()
            │
            └── Alerts dispatch
                    log_alert.dispatch()
                    email_alert.dispatch()
                    webhook_alert.dispatch()
                    zabbix_alert.dispatch()
```

## CheckBase pattern

```python
class MyCheck(CheckBase):
    requires = [["ansible"], ["docker"]]  # OR logic
    fallback = MyFallbackCheck            # optional

    def run(self):
        # type: () -> CheckResult
        try:
            # do work
            return CheckResult("MyCheck", CheckResult.OK, "all good")
        except Exception as e:
            return CheckResult("MyCheck", CheckResult.UNKNOWN, str(e))
```

Key invariants:
- `run()` **never raises** — always returns a `CheckResult`
- `requires` uses **OR-of-ANDs** logic: `[[a, b], [c]]` means `(a AND b) OR c`
- `CheckResult.status` is one of: OK, WARNING, CRITICAL, UNKNOWN, SKIPPED

## Capability map

`discover_capabilities()` returns:

```python
{
    "python_version":      "3.9.18",
    "ansible":             True,
    "ansible_version":     "ansible core 2.14.0",
    "docker":              False,
    "kinit":               True,
    "klist":               True,
    "zabbix_sender":       False,
    "venv_ansible":        False,
    "docker_ansible_image": False,
}
```

## Config expansion

`load_config()` processes YAML in two passes:
1. Parse YAML (PyYAML if available, else built-in manual parser)
2. Recursively expand `${ENV_VAR}` in all string values

## File structure

```
hadoopscope/
├── hadoopscope.py          # Entry point + CLI
├── bootstrap.py            # Capability discovery + Ansible auto-install
├── config.py               # YAML loader + env var expansion
├── checks/
│   ├── base.py             # CheckResult + CheckBase
│   ├── ambari.py           # HDP: Ambari REST API checks
│   ├── cloudera.py         # CDP: Cloudera Manager checks
│   ├── webhdfs.py          # HDFS: WebHDFS + JMX
│   ├── yarn.py             # YARN: Resource Manager REST
│   └── hive.py             # Hive: Ansible + beeline
├── alerts/
│   ├── log_alert.py        # File log
│   ├── email_alert.py      # SMTP email
│   ├── webhook_alert.py    # HTTP POST
│   └── zabbix_alert.py     # zabbix_sender
├── config/
│   ├── example.yaml        # Annotated example config
│   ├── test.yaml           # Test config (no real creds)
│   └── docker-test.yaml    # Docker compose config
├── tests/
│   ├── test_base.py        # CheckBase unit tests
│   ├── test_checks.py      # Check tests with HTTP mock server
│   ├── mock_api_server.py  # In-process mock API server
│   ├── run_all.py          # Test runner
│   └── fixtures/           # JSON fixtures for mock API
├── docs/                   # MkDocs documentation
├── Dockerfile              # Tool container
├── Dockerfile.mock         # Mock API server container
├── docker-compose.yml      # Integration test environment
├── install.sh              # Install script
├── tuxbox.toml             # TuxBox registry config
└── mkdocs.yml              # Documentation config
```
