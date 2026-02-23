# HadoopScope — Runbook di Test su Ambiente Remoto

Questo runbook ti guida dal `git clone` fino alla verifica completa di tutti i check su un cluster reale.
Eseguilo su un nodo che abbia visibilità di rete verso Ambari / Cloudera Manager / NameNode / RM.

---

## Indice

1. [Prerequisiti](#1-prerequisiti)
2. [Installazione](#2-installazione)
3. [Configurazione](#3-configurazione)
4. [Verifica capabilities](#4-verifica-capabilities)
5. [Dry-run](#5-dry-run)
6. [Test HDP (Ambari)](#6-test-hdp-ambari)
7. [Test WebHDFS / HDFS](#7-test-webhdfs--hdfs)
8. [Test YARN](#8-test-yarn)
9. [Test CDP (Cloudera Manager)](#9-test-cdp-cloudera-manager)
10. [Test multi-ambiente](#10-test-multi-ambiente)
11. [Test Kerberos / SPNEGO](#11-test-kerberos--spnego)
12. [Test alert log](#12-test-alert-log)
13. [Verifica exit code](#13-verifica-exit-code)
14. [Output JSON](#14-output-json)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Prerequisiti

### Sul nodo che esegue HadoopScope

```bash
# Minimo assoluto
python3 --version          # deve essere >= 3.6
python3 -c "import urllib.request; print('OK')"

# Opzionali (abilitano check aggiuntivi)
which curl                 # richiesto per Kerberos/SPNEGO
which kinit && klist       # richiesto per Kerberos
which ansible              # richiesto per HiveCheck (altrimenti auto-installato in venv)
which docker               # alternativa ad ansible per HiveCheck
which zabbix_sender        # richiesto solo per alert Zabbix
```

### Connettività di rete richiesta

| Componente | Porta default | Protocollo |
|-----------|--------------|------------|
| Ambari | 8080 (http) / 8443 (https) | HTTP |
| Cloudera Manager | 7180 (http) / 7183 (https) | HTTP |
| WebHDFS NameNode | 9870 (HDP 3.x) / 50070 (HDP 2.x) | HTTP |
| NameNode JMX | 9870 (same endpoint) | HTTP |
| YARN ResourceManager | 8088 | HTTP |

```bash
# Verifica raggiungibilità (adatta gli host)
curl -s -o /dev/null -w "%{http_code}" http://AMBARI_HOST:8080/api/v1/clusters
# atteso: 200 o 401 (unauthorized = raggiungibile)

curl -s -o /dev/null -w "%{http_code}" http://NAMENODE_HOST:9870/webhdfs/v1/?op=LISTSTATUS
# atteso: 200 o 401
```

---

## 2. Installazione

```bash
# Clone del repo (no pip, nessun pacchetto da installare)
git clone https://github.com/disoardi/hadoopscope.git
cd hadoopscope

# Verifica che l'entry point funzioni
python3 hadoopscope.py --version
# Atteso: HadoopScope 0.1.0

python3 hadoopscope.py --show-capabilities
# Atteso: tabella con le capabilities rilevate (ansible, docker, curl, kinit, ...)
```

---

## 3. Configurazione

### 3a. Crea il file config

```bash
cp config/example.yaml config/hadoopscope.yaml
# Edita con i dati reali del tuo ambiente:
vi config/hadoopscope.yaml
```

### 3b. Config minima HDP

```yaml
version: "1"

environments:
  prod-hdp:
    type: hdp
    enabled: true
    ambari_url: http://AMBARI_HOST:8080       # <-- adatta
    ambari_user: monitor                       # utente read-only
    ambari_pass: "${AMBARI_PASS}"
    cluster_name: YOUR_CLUSTER_NAME            # <-- nome cluster in Ambari

    webhdfs:
      url: http://NAMENODE_HOST:9870           # <-- adatta
      user: hdfs                               # utente WebHDFS

    yarn:
      rm_url: http://RM_HOST:8088              # <-- opzionale se RM è sullo stesso host di Ambari

checks:
  service_health:
    enabled: true

alerts:
  log:
    enabled: true
    path: /tmp/hadoopscope-logs/
    format: text
```

### 3c. Config minima CDP

```yaml
version: "1"

environments:
  prod-cdp:
    type: cdp
    enabled: true
    cm_url: http://CM_HOST:7180                # <-- adatta
    cm_user: monitor
    cm_pass: "${CM_PASS}"
    cluster_name: YOUR_CDP_CLUSTER_NAME        # <-- nome cluster in CM

checks:
  service_health:
    enabled: true

alerts:
  log:
    enabled: true
    path: /tmp/hadoopscope-logs/
    format: text
```

### 3d. Imposta le variabili d'ambiente

```bash
# HDP
export AMBARI_PASS="la_password_reale"

# CDP
export CM_PASS="la_password_reale"

# Opzionale: usa un file .env (non committato)
cat > config/.env <<'EOF'
AMBARI_PASS=la_password_reale
CM_PASS=la_password_reale
EOF
# Il file .env viene caricato automaticamente se nella stessa dir del config
```

---

## 4. Verifica capabilities

```bash
python3 hadoopscope.py --show-capabilities
```

**Output atteso (esempio):**
```
Capabilities:
  python_version    : 3.8.10
  ansible           : False
  ansible_version   : n/a
  docker            : True
  kinit             : True
  klist             : True
  curl              : True
  zabbix_sender     : False
  venv_ansible      : False
  docker_ansible_image: False
```

**Cosa verificare:**
- `python_version` >= 3.6
- `curl: True` se userai Kerberos
- `kinit: True` e `klist: True` se l'ambiente è kerberizzato
- `ansible: True` o `docker: True` solo se vuoi testare `HiveCheck`

---

## 5. Dry-run

Il dry-run valida il config, espande i secrets e mostra il piano di esecuzione **senza fare chiamate di rete**.

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --dry-run

# Output atteso:
# HadoopScope — prod-hdp @ http://AMBARI_HOST:8080
# ============================================================
# [DRY-RUN ]  AmbariServiceHealthCheck — Would run (capability OK)
# [DRY-RUN ]  NameNodeHACheck — Would run (capability OK)
# [DRY-RUN ]  ClusterAlertsCheck — Would run (capability OK)
# [DRY-RUN ]  ConfigStalenessCheck — Would run (capability OK)
# [DRY-RUN ]  HdfsSpaceCheck — Would run (capability OK)
# [DRY-RUN ]  HdfsDataNodeCheck — Would run (capability OK)
# [DRY-RUN ]  HdfsWritabilityCheck — Would run (capability OK)
# [DRY-RUN ]  YarnNodeHealthCheck — Would run (capability OK)
# [DRY-RUN ]  YarnQueueCheck — Would run (capability OK)
# [SKIPPED ]  HiveCheck — Requires: [['ansible'], ['venv_ansible'], ['docker']]. ...
```

**Cosa verificare:**
- Nessun errore `ERROR loading config`
- I check attesi mostrano `[DRY-RUN]`, non `[SKIPPED]`
- `HiveCheck` è SKIPPED se ansible/docker non sono presenti — è normale

---

## 6. Test HDP (Ambari)

### 6a. Solo health check (Ambari API)

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --checks health \
  --output text
```

**Output atteso (cluster sano):**
```
HadoopScope — prod-hdp @ http://AMBARI_HOST:8080
============================================================
[OK      ]  AmbariServiceHealth — All N monitored services are STARTED
[OK      ]  NameNodeHA — Active: nn1.host | Standby: nn2.host
[OK      ]  ClusterAlerts — No active CRITICAL alerts
[OK      ]  ConfigStaleness — No stale configurations

Summary: 4 OK
```

**Cosa verificare:**
- `AmbariServiceHealth`: lista servizi avviati. Se qualcuno è `STOPPED` vedrai `[CRITICAL]`
- `NameNodeHA`: riporta quale NameNode è active e quale è standby
- `ClusterAlerts`: se ci sono alert CRITICAL attivi in Ambari, li lista
- `ConfigStaleness`: avvisa se ci sono configurazioni non deployate

**Casistica WARNING/CRITICAL attesa:**
```
[CRITICAL]  AmbariServiceHealth — STOPPED services: YARN
[WARNING ]  ClusterAlerts — 2 active CRITICAL alerts: NAMENODE, DATANODE
[WARNING ]  ConfigStaleness — Stale services: HDFS, HIVE
```

---

## 7. Test WebHDFS / HDFS

### 7a. Tutti i check HDFS

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --checks hdfs \
  --output text
```

**Output atteso:**
```
[OK      ]  HdfsSpace — /user/hive/warehouse: 45% used (OK)
[OK      ]  HdfsDataNodes — 10 live, 0 dead, 0 stale DataNodes
[OK      ]  HdfsWritability — Write/delete probe OK on /tmp/.hadoopscope-probe
```

**Cosa verificare:**

| Check | Problema comune | Causa |
|-------|----------------|-------|
| `HdfsSpace` | `UNKNOWN: Connection refused` | WebHDFS porta sbagliata o non esposta |
| `HdfsSpace` | `UNKNOWN: HTTP 403` | L'utente WebHDFS non ha accesso al path |
| `HdfsDataNodes` | `WARNING: 2 dead DataNodes` | DataNode down — verificare in Ambari |
| `HdfsWritability` | `CRITICAL: Write failed` | L'utente non ha permesso di scrittura su `/tmp/.hadoopscope-probe` |
| `HdfsWritability` | `UNKNOWN: HTTP 307 redirect` | Solo in Kerberos — vedi sezione 11 |

**Fix permessi per HdfsWritability:**
```bash
# Su un nodo con client HDFS
hdfs dfs -mkdir -p /tmp/.hadoopscope-probe
hdfs dfs -chown monitor /tmp/.hadoopscope-probe
hdfs dfs -chmod 700 /tmp/.hadoopscope-probe
```

### 7b. Verifica manuale WebHDFS (debug)

```bash
# Test diretto senza hadoopscope (semplice auth)
curl -s "http://NAMENODE_HOST:9870/webhdfs/v1/user?op=LISTSTATUS&user.name=hdfs" | python3 -m json.tool | head -20

# Test JMX NameNode
curl -s "http://NAMENODE_HOST:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | python3 -m json.tool | grep -E 'NumLive|NumDead|Capacity'
```

---

## 8. Test YARN

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --checks yarn \
  --output text
```

**Output atteso:**
```
[OK      ]  YarnNodeHealth — 12 nodes RUNNING, 0 UNHEALTHY, 0 LOST
[OK      ]  YarnQueueCheck — default: 23% used (OK)
```

**Debug diretto:**
```bash
# Stato cluster
curl -s "http://RM_HOST:8088/ws/v1/cluster/info" | python3 -m json.tool | grep state

# Lista nodi
curl -s "http://RM_HOST:8088/ws/v1/cluster/nodes" | python3 -m json.tool | grep -E '"state"|"id"'

# Scheduler
curl -s "http://RM_HOST:8088/ws/v1/cluster/scheduler" | python3 -m json.tool | grep -E 'usedCapacity|name'
```

---

## 9. Test CDP (Cloudera Manager)

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-cdp \
  --checks health \
  --output text
```

**Output atteso:**
```
HadoopScope — prod-cdp @ http://CM_HOST:7180
============================================================
[OK      ]  ClouderaServiceHealth — All 8 services GOOD
[OK      ]  ClouderaParcels — All 2 parcel(s) ACTIVATED

Summary: 2 OK
```

**Cosa verificare:**

| Check | Problema | Causa |
|-------|----------|-------|
| `ClouderaServiceHealth` | `UNKNOWN: CM HTTP 401` | Credenziali errate o user non ha ruolo Read-Only |
| `ClouderaServiceHealth` | `WARNING: HDFS: CONCERNING` | Salute degradata — verificare in CM |
| `ClouderaParcels` | `WARNING: CDH-7.1.7 (DOWNLOADED)` | Parcel scaricato ma non attivato |

**Debug diretto:**
```bash
# Lista servizi
curl -s -u monitor:$CM_PASS "http://CM_HOST:7180/api/v40/clusters/CLUSTER_NAME/services" \
  | python3 -m json.tool | grep -E '"name"|"healthSummary"|"serviceState"'

# Lista parcels
curl -s -u monitor:$CM_PASS "http://CM_HOST:7180/api/v40/clusters/CLUSTER_NAME/parcels" \
  | python3 -m json.tool | grep -E '"product"|"version"|"stage"'
```

---

## 10. Test multi-ambiente

Se hai più cluster nella config, testali in un solo comando:

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --env prod-cdp \
  --checks health \
  --output text
```

Ogni ambiente produce un blocco separato nell'output. L'exit code è il peggiore tra tutti.

---

## 11. Test Kerberos / SPNEGO

### 11a. Prerequisiti Kerberos sul nodo

```bash
# Verifica tools
which kinit && which klist && which curl
kinit --version
curl --version | head -1

# Verifica /etc/krb5.conf
grep -A3 "\[realms\]" /etc/krb5.conf | head -10

# Verifica keytab
klist -kt /etc/security/keytabs/monitor.keytab
# Deve mostrare: principal, timestamp, encryption type
```

### 11b. Test kinit manuale

```bash
# Ottieni ticket dal keytab
kinit -kt /etc/security/keytabs/monitor.keytab monitor@YOUR.REALM

# Verifica ticket
klist
# Atteso: "Valid starting" con data futura

# Test WebHDFS con SPNEGO manuale
curl -s --negotiate -u : \
  "http://NAMENODE_HOST:9870/webhdfs/v1/?op=LISTSTATUS" \
  | python3 -m json.tool | head -10
# Atteso: struttura JSON con file/directory listing
```

### 11c. Config Kerberos

```yaml
environments:
  prod-hdp-kerb:
    type: hdp
    ambari_url: http://AMBARI_HOST:8080
    ambari_user: monitor
    ambari_pass: "${AMBARI_PASS}"         # Ambari usa sempre Basic Auth
    cluster_name: YOUR_CLUSTER_NAME
    webhdfs:
      url: http://NAMENODE_HOST:9870
      user: monitor                       # non usato con kerberos.enabled=true
    kerberos:
      enabled: true
      keytab: /etc/security/keytabs/monitor.keytab
      principal: monitor@YOUR.REALM
```

### 11d. Run con Kerberos

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp-kerb \
  --checks hdfs \
  --output text \
  --verbose
```

**Output atteso:**
```
[OK      ]  HdfsSpace — /user/hive/warehouse: 45% used (OK)
[OK      ]  HdfsDataNodes — 10 live, 0 dead
[OK      ]  HdfsWritability — Write/delete probe OK
```

**Problemi comuni Kerberos:**

| Errore | Causa | Fix |
|--------|-------|-----|
| `kinit fallito per principal='...'` | Keytab scaduto o realm sbagliato | Rigenera keytab o correggi `principal` in config |
| `kinit non trovato nel PATH` | Pacchetto Kerberos non installato | `yum install krb5-workstation` o `apt install krb5-user` |
| `curl: (35) SSL connect error` | WebHDFS su HTTPS con cert auto-firmato | Aggiungi `-k` o configura il trust store |
| `HTTP 403 Forbidden` dopo kinit | Principal non autorizzato su HDFS | Aggiungi ACL HDFS per il principal |
| `Clock skew too great` | Orario del nodo non sincronizzato con KDC | `ntpdate KDC_HOST` o abilita `chronyd` |

---

## 12. Test alert log

```bash
# Crea la dir di output
mkdir -p /tmp/hadoopscope-logs/

# Esegui con alert log abilitato
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --checks health \
  --output text

# Verifica che il log sia stato scritto
ls -la /tmp/hadoopscope-logs/
cat /tmp/hadoopscope-logs/hadoopscope_prod-hdp_*.log 2>/dev/null || \
  ls /tmp/hadoopscope-logs/
```

**Output log atteso (format text):**
```
[2026-02-23 10:15:42] prod-hdp  OK       AmbariServiceHealth      All 6 services STARTED
[2026-02-23 10:15:42] prod-hdp  OK       NameNodeHA               Active: nn1 | Standby: nn2
[2026-02-23 10:15:43] prod-hdp  WARNING  HdfsSpace                /tmp: 88% used (WARNING)
```

**Format JSON:**
```bash
# Modifica temporanea in config: format: json
# Poi:
cat /tmp/hadoopscope-logs/*.log | python3 -m json.tool | head -30
```

---

## 13. Verifica exit code

L'exit code è usato da Nagios/Icinga/Zabbix per determinare lo stato:

```bash
# Cluster OK
python3 hadoopscope.py --config config/hadoopscope.yaml --env prod-hdp --checks health
echo "Exit code: $?"
# Atteso: 0

# Verifica exit code 1 (WARNING) — con un check forzato
# (modifica temporanea: abbassa il threshold hdfs_space a 1%)
python3 hadoopscope.py --config config/hadoopscope.yaml --env prod-hdp --checks hdfs
echo "Exit code: $?"
# Atteso: 0 (OK), 1 (WARNING) o 2 (CRITICAL) in base allo stato reale

# Tabella exit code:
# 0 = tutto OK o SKIPPED
# 1 = almeno un WARNING
# 2 = almeno un CRITICAL
```

**Integrazione Nagios/NRPE:**
```bash
# /etc/nrpe.d/hadoopscope.cfg
# command[check_hadoopscope]=/usr/local/bin/python3 /opt/hadoopscope/hadoopscope.py \
#   --config /etc/hadoopscope/hadoopscope.yaml \
#   --env prod-hdp --checks health --output text
```

---

## 14. Output JSON

```bash
python3 hadoopscope.py \
  --config config/hadoopscope.yaml \
  --env prod-hdp \
  --output json | python3 -m json.tool
```

**Output atteso:**
```json
{
  "version": "0.1.0",
  "capabilities": {
    "ansible": false,
    "docker": true,
    "kinit": true,
    "curl": true
  },
  "environments": {
    "prod-hdp": [
      {
        "check": "AmbariServiceHealth",
        "status": "OK",
        "message": "All 6 monitored services are STARTED",
        "details": {"service_count": 6, "started": ["HDFS", "YARN", "HIVE", "HBASE", "OOZIE", "ZOOKEEPER"]}
      },
      {
        "check": "HdfsSpace",
        "status": "WARNING",
        "message": "/user/hive/warehouse: 82% used (WARNING)",
        "details": {"/user/hive/warehouse": {"used_pct": 82.0, "used_gb": 410.5, "quota_gb": 500.0}}
      }
    ]
  }
}
```

**Parsing da script shell:**
```bash
# Estrai solo i check CRITICAL
python3 hadoopscope.py --config config/hadoopscope.yaml --env prod-hdp --output json \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for env, checks in data['environments'].items():
    for c in checks:
        if c['status'] in ('CRITICAL', 'WARNING'):
            print('{} {} {} — {}'.format(env, c['status'], c['check'], c['message']))
"
```

---

## 15. Troubleshooting

### Config non si carica

```bash
# Testa il config in isolamento
python3 -c "
from config import load_config
cfg = load_config('config/hadoopscope.yaml')
import json
print(json.dumps(list(cfg['environments'].keys()), indent=2))
"
```

### Variabile d'ambiente mancante

```
ERROR loading config: Environment variable 'AMBARI_PASS' is not set.
```
→ `export AMBARI_PASS=yourpassword` oppure crea `config/.env`

### Timeout / Connection refused

```
[UNKNOWN ]  AmbariServiceHealth — CM connection error: [Errno 111] Connection refused
```
→ Verifica host/porta nel config, verifica firewall:
```bash
nc -zv AMBARI_HOST 8080
curl -v http://AMBARI_HOST:8080/api/v1/clusters 2>&1 | grep "< HTTP"
```

### HTTP 401 Unauthorized

```
[UNKNOWN ]  AmbariServiceHealth — Ambari HTTP 401: Unauthorized
```
→ Credenziali errate o utente non abilitato:
```bash
curl -u monitor:$AMBARI_PASS "http://AMBARI_HOST:8080/api/v1/clusters" -v 2>&1 | grep "< HTTP"
```

### SSL certificate verify failed

```
[UNKNOWN ]  ClouderaServiceHealth — CM connection error: [SSL: CERTIFICATE_VERIFY_FAILED]
```
→ Aggiungi `ssl_verify: false` nella sezione environment del config (non ancora implementato).
  Workaround temporaneo: usa `http://` invece di `https://` per i test interni.

### Parser YAML fallback (senza PyYAML)

```bash
# Verifica se PyYAML è disponibile
python3 -c "import yaml; print('PyYAML OK')" 2>/dev/null || echo "Usando parser interno"

# Testa il parser interno con il tuo config
python3 -c "
import sys; sys.modules.pop('yaml', None)
# Forza il parser manuale sovrascrivendo il flag
import config as cfg_module
cfg_module._HAS_PYYAML = False
data = cfg_module.load_config('config/hadoopscope.yaml')
print('Environments:', list(data['environments'].keys()))
"
```

### Check SKIPPED inaspettato

```
[SKIPPED ]  HdfsWritabilityCheck — Requires: []. Install missing tools or use Docker.
```
→ Questo non dovrebbe accadere per i check REST-only (`requires = []`). Controlla la versione:
```bash
git log --oneline -5
python3 hadoopscope.py --version
```

### Tutti i check tornano UNKNOWN

Possibile causa: l'account monitor non ha permessi sufficienti.
```bash
# Test credenziali Ambari manuale
curl -u monitor:$AMBARI_PASS \
  "http://AMBARI_HOST:8080/api/v1/clusters/CLUSTER_NAME/services" \
  | python3 -m json.tool | grep -E '"ServiceInfo"|"state"' | head -20
```

---

## Checklist finale

Spunta ogni voce prima di considerare il test completato:

- [ ] `python3 hadoopscope.py --version` → `HadoopScope 0.1.0`
- [ ] `--show-capabilities` mostra le capabilities corrette
- [ ] `--dry-run` completa senza errori, tutti i check attesi sono `DRY_RUN`
- [ ] `--checks health --env prod-hdp` restituisce status corretto Ambari
- [ ] `--checks hdfs` restituisce spazio e DataNode corretti
- [ ] `--checks yarn` restituisce stato nodi e code
- [ ] (se CDP) `--checks health --env prod-cdp` mostra `ClouderaServiceHealth` + `ClouderaParcels`
- [ ] (se Kerberos) `klist` mostra ticket valido dopo l'esecuzione
- [ ] Alert log scritto in `/tmp/hadoopscope-logs/`
- [ ] Exit code 0 su cluster sano, 1/2 su warning/critical
- [ ] Output JSON valido (parse con `python3 -m json.tool`)
