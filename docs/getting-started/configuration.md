# Configuration Reference

HadoopScope uses a YAML config file. The default path is `config/hadoopscope.yaml`.

## Full schema

```yaml
version: "1"    # required

environments:
  <env-name>:
    type: hdp | cdp          # cluster type
    enabled: true | false    # skip if false (default: true)

    # HDP (Ambari)
    ambari_url: https://...
    ambari_user: admin
    ambari_pass: "${AMBARI_PASS}"   # use env vars for secrets!
    cluster_name: my-cluster
    ambari_api_version: "v1"        # optional, default v1

    # CDP (Cloudera Manager)
    cm_url: https://...
    cm_user: admin
    cm_pass: "${CM_PASS}"
    cm_api_version: "v40"           # optional, default v40

    # WebHDFS (both HDP and CDP)
    webhdfs:
      url: http://namenode:9870
      user: hdfs
      ssl: false

    # YARN RM (optional — auto-detected from ambari_url if missing)
    yarn:
      rm_url: http://rm-host:8088

    # Kerberos (optional)
    kerberos:
      enabled: false
      keytab: "${KEYTAB_PATH}"
      principal: monitor@CORP.COM

    # Ansible edge node (for Hive checks)
    ansible:
      edge_host: edge01.corp.com
      ssh_user: monitor
      ssh_key: ~/.ssh/monitor_key
      become: false

    # Per-environment Hive settings
    hive:
      host: hiveserver2.corp.com
      port: 10000
      database: default
      user: hive

checks:
  service_health:
    enabled: true
    services: [HDFS, YARN, HIVE, HBASE, OOZIE, ZOOKEEPER]

  namenode_ha:
    enabled: true

  hdfs_space:
    enabled: true
    paths:
      - path: /user/hive/warehouse
        warning_pct: 75
        critical_pct: 90
      - path: /tmp
        warning_pct: 80
        critical_pct: 95

  hdfs_writability:
    enabled: true
    test_path: /tmp/.hadoopscope-probe

  hdfs_dead_datanodes:
    enabled: true
    warning_threshold: 1
    critical_threshold: 3

  yarn_nodes:
    enabled: true

  yarn_queues:
    enabled: true
    usage_warning_pct: 80
    usage_critical_pct: 90

alerts:
  email:
    enabled: false
    smtp_host: smtp.corp.com
    smtp_port: 587
    smtp_tls: true
    smtp_user: "${SMTP_USER}"
    smtp_pass: "${SMTP_PASS}"
    from_addr: hadoopscope@corp.com
    to: [ops@corp.com, hadoop-team@corp.com]
    on_severity: [WARNING, CRITICAL]

  webhook:
    enabled: false
    url: https://hooks.slack.com/services/...
    secret: "${WEBHOOK_SECRET}"
    on_severity: [WARNING, CRITICAL]
    timeout: 10

  zabbix:
    enabled: false
    server: zabbix.corp.com
    port: 10051
    host: hadoop-monitor    # Zabbix host name

  log:
    enabled: true
    path: /var/log/hadoopscope/
    format: json | text     # default: text
    rotate_days: 30
```

## Environment variables in config

Any string value can reference environment variables with `${VAR_NAME}` syntax.
HadoopScope will raise a descriptive error if the variable is not set.

```yaml
ambari_pass: "${AMBARI_PASS}"   # required — export AMBARI_PASS=secret
smtp_pass: "${SMTP_PASS:-}"    # optional — empty string if not set (not supported yet)
```

!!! warning
    Never commit config files containing real passwords.
    Always use `${ENV_VAR}` references for secrets.
    Add your local config to `.gitignore`.
