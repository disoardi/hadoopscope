# Hive Check

The Hive check verifies **HiveServer2 availability** by running a test query via `beeline` on an edge node using Ansible.

## Requirements

At least one of:
- `ansible` (system-installed)
- `venv_ansible` (Ansible in `~/.hadoopscope/venv/` — auto-installed by bootstrap)
- `docker_ansible_image` (Docker image with Ansible — auto-pulled by bootstrap)

If none are available, the check is gracefully **SKIPPED**.

## How it works

1. HadoopScope SSHes to the configured edge node via Ansible
2. Runs `beeline -u jdbc:hive2://{host}:{port}/{db} -e 'SELECT 1;'`
3. Returns OK if beeline exits 0, CRITICAL otherwise

## Config

```yaml
environments:
  prod-hdp:
    # ...
    ansible:
      edge_host: edge01.corp.com
      ssh_user: monitor
      ssh_key: ~/.ssh/monitor_key

    hive:
      host: hiveserver2.corp.com  # default: edge_host
      port: 10000                 # default: 10000
      database: default           # default: default
      user: hive                  # default: ansible.ssh_user
```

## Results

| Result | Condition |
|--------|-----------|
| OK | Beeline query returned exit code 0 |
| CRITICAL | Beeline query failed |
| UNKNOWN | Timeout (60s) or unexpected error |
| SKIPPED | No ansible/docker available |
