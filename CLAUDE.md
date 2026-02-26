# HadoopScope — CLAUDE.md

Questo file guida Claude Code nello sviluppo autonomo del progetto.
Leggilo per intero prima di scrivere qualsiasi codice.

---

## 🎯 Cos'è HadoopScope

CLI tool Python per monitoraggio unificato di cluster Hadoop **HDP** (Ambari REST API) e **CDP** (Cloudera Manager REST API). Progettato per girare su macchine **senza client Hadoop installati** — tutto via REST API o Ansible remoto.

**Tagline:** Runs anywhere, requires nothing.

**Distribuzione:** TuxBox registry — `tbox run hadoopscope`

---

## ⚙️ Regole Tecniche NON Negoziabili

### Python compatibility
- Target: **Python 3.6+** (alcuni ambienti cliente sono bloccati su 3.6.x RHEL)
- **No f-strings con espressioni** tipo `f"{x:.2f}"` — ok solo `f"{var}"` semplici o `.format()`
- **No walrus operator** (`:=`)
- **No dataclasses** (introdotte in 3.7)
- **No `typing` con `|`** — usa `Optional[X]`, `Union[X, Y]`, `Dict[k,v]`, `List[X]`
- Type hints sempre con `# type: (...)` inline oppure `typing` import esplicito

### Zero dipendenze per il core
- Il file `hadoopscope.py` e tutto `checks/`, `alerts/`, `config.py`, `bootstrap.py`
  devono girare con **solo la stdlib Python 3.6** — zero `pip install`
- L'unica eccezione è Ansible, che il bootstrap installa in un **venv isolato**
  (`~/.hadoopscope/venv/`) se necessario — mai globale

### Isolation
- Mai installare nulla globalmente
- Se serve Ansible: venv in `~/.hadoopscope/venv/` oppure container Docker `python:3.9-slim`
- Il bootstrap layer gestisce questa logica automaticamente

---

## 🏗️ Architettura

```
hadoopscope/
├── hadoopscope.py          # Entry point + CLI (argparse stdlib)
├── bootstrap.py            # Discovery capability + auto-install Ansible
├── config.py               # YAML parser + env var expansion (no PyYAML!)
├── checks/
│   ├── base.py             # CheckResult + CheckBase (requires/fallback)
│   ├── ambari.py           # HDP: service health, NameNode HA, alerts
│   ├── cloudera.py         # CDP: CM API
│   ├── webhdfs.py          # HDFS via WebHDFS REST (no client)
│   ├── hive.py             # Hive via Ansible+beeline
│   └── yarn.py             # YARN RM REST API
├── alerts/
│   ├── email_alert.py      # smtplib stdlib
│   ├── zabbix_alert.py     # subprocess zabbix_sender
│   ├── webhook_alert.py    # urllib stdlib
│   └── log_alert.py        # file JSON/text
├── ansible/
│   ├── hadoopscope.yml
│   └── tasks/
├── config/
│   └── example.yaml
├── install.sh
├── tuxbox.toml
└── README.md
```

### Layer stack (dall'alto in basso)
```
CLI (argparse) → Bootstrap (capability map) → Executor → Checks → Alerts
```

---

## 📐 Pattern Chiave: CheckBase

```python
# checks/base.py
class CheckResult(object):
    OK = "OK"; WARNING = "WARNING"; CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"; SKIPPED = "SKIPPED"

    def __init__(self, name, status, message, details=None):
        # type: (str, str, str, dict) -> None
        self.name    = name
        self.status  = status
        self.message = message
        self.details = details or {}

class CheckBase(object):
    requires = []   # type: list  # OR logico di AND-list: [[req1,req2], [req3]]
    fallback = None # type: type  # altra classe Check da usare se can_run() False

    def __init__(self, config, caps):
        # type: (dict, dict) -> None
        self.config = config
        self.caps = caps

    def can_run(self):
        # type: () -> bool
        if not self.requires:
            return True
        for req_list in self.requires:
            if all(self.caps.get(r, False) for r in req_list):
                return True
        return False

    def run(self):
        # type: () -> CheckResult
        raise NotImplementedError
```

**Regola executor:** se `check.can_run()` è False e `check.fallback` esiste,
istanzia e prova il fallback. Se anche il fallback non può girare, produce
`CheckResult(status=SKIPPED, message="requires: ...")`.

---

## 🔍 Bootstrap / Capability Map

```python
# bootstrap.py — valori che discover_capabilities() deve popolare
{
    "python_version": "3.6.8",
    "ansible":        True,
    "ansible_version":"2.9.27",
    "docker":         False,
    "kinit":          True,   # Kerberos
    "klist":          True,
    "zabbix_sender":  False,
    "venv_ansible":   False,  # ansible installato dal nostro bootstrap in venv
    "docker_ansible_image": False,
}
```

`ensure_ansible(caps)` è chiamato SE ci sono check che richiedono ansible:
1. Se `docker` disponibile → pull `ghcr.io/ansible/community-general:latest` (o simile leggero)
2. Altrimenti → crea venv `~/.hadoopscope/venv/`, `pip install ansible` dentro

---

## 📡 API Reference Rapida

### Ambari (HDP)
```
GET /api/v1/clusters/{name}/services
    → ServiceInfo.state: STARTED | STOPPED | INSTALLED | ...

GET /api/v1/clusters/{name}/services/HDFS/components/NAMENODE
    → metrics.dfs.FSNamesystem.HAState: active | standby

GET /api/v1/clusters/{name}/alerts?fields=*&Alert/state=CRITICAL

GET /api/v1/clusters/{name}/hosts?fields=Hosts/host_state,Hosts/host_status
```

### WebHDFS (HDFS, no client)
```
GET http://{nn}:{port}/webhdfs/v1/{path}?op=GETCONTENTSUMMARY
    → ContentSummary.spaceConsumed, .spaceQuota, .length, .fileCount

GET http://{nn}:{port}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
    → NumDeadDataNodes, NumLiveDataNodes, CapacityUsed, CapacityTotal

PUT /webhdfs/v1/{path}?op=CREATE&overwrite=true  (writability test)
DELETE /webhdfs/v1/{path}?op=DELETE
```

### Cloudera Manager (CDP)
```
GET /api/v40/clusters/{name}/services
    → healthSummary: GOOD | CONCERNING | BAD | DISABLED | NOT_AVAILABLE

GET /api/v40/clusters/{name}/parcels
    → stage: ACTIVATED | AVAILABLE_REMOTELY | DOWNLOADING | ...
```

### YARN RM
```
GET http://{rm}:8088/ws/v1/cluster/info → clusterInfo.state
GET http://{rm}:8088/ws/v1/cluster/nodes → nodes[].state
GET http://{rm}:8088/ws/v1/cluster/scheduler → schedulerInfo.queues[].usedCapacity
```

---

## 🔐 Config YAML — Regole

- **Mai** credenziali hardcoded — sempre `"${ENV_VAR}"`
- Il parser in `config.py` deve espandere env var con `os.environ.get()`
  e sollevare errore descrittivo se variabile mancante
- Kerberos keytab: sempre path assoluto o env var
- File config con credenziali reali vanno nel `.gitignore`

### Kerberos — due contesti distinti, due keytab diversi

| Chiave config | Dove viene usato | Dove deve esistere il file |
|---|---|---|
| `kerberos.keytab` | WebHDFS checks locali: `kinit -kt` + `curl --negotiate` | **Macchina locale** che esegue HadoopScope |
| `webhdfs.kerberos.keytab` | WebHDFS checks `via_ansible=true`: kinit sull'edge node (override di `kerberos.keytab`) | **Nodo edge** remoto |
| `hive.kerberos.keytab` | HiveCheck: `kinit -kt` iniettato nel playbook Ansible | **Nodo edge** remoto (dove gira beeline) |
| `hive.ssl.truststore` | Beeline JDBC URL property `sslTrustStore=...` | **Nodo edge** remoto (dove gira beeline) |

**Regola**: i path in `hive.kerberos.*` e `hive.ssl.*` sono sempre path sul nodo edge,
mai sulla macchina locale. Possono essere diversi dal path in `kerberos.keytab`.
Non esiste fallback incrociato: `HiveCheck` legge solo `hive.kerberos`, mai il top-level `kerberos`.

### HiveServer2 — modalità di connessione

Due modalità supportate in `hive.`:

1. **`jdbc_url` verbatim** — URL completo passato direttamente a beeline (bypassa tutto):
   ```yaml
   hive:
     jdbc_url: "jdbc:hive2://lb:10000/;ssl=true;sslTrustStore=...;principal=..."
   ```

2. **Config strutturata** — HadoopScope costruisce l'URL dai parametri:
   ```yaml
   hive:
     host: lb.corp.com          # oppure: zookeeper_hosts: [zk1:2181, ...]
     port: 10000
     ssl:
       enabled: true
       truststore: /path/on/edge/node/truststore.jks
       truststore_password: "${HS2_TRUSTSTORE_PASS}"
     kerberos_principal: "hive/lb.corp.com@REALM"   # server principal (JDBC URL)
     kerberos:
       keytab: /path/on/edge/node/client.keytab     # client kinit (edge node)
       client_principal: "svc@REALM"
   ```

### YARN — auto-detect solo per HDP

`YarnNodeHealthCheck` e `YarnQueueCheck`:
- **HDP**: se `yarn.rm_url` non è configurato, tenta auto-detect dall'host di `ambari_url` porta 8088
- **CDP**: se `yarn.rm_url` non è configurato → `SKIPPED` (non c'è `ambari_url` per l'auto-detect)
- Se il proxy aziendale blocca le chiamate → `HTTP 403: URLBlocked` → imposta `no_proxy: true`

**Schema minimo valido per test:**
```yaml
version: "1"
environments:
  test-hdp:
    type: hdp
    ambari_url: http://localhost:8080
    ambari_user: admin
    ambari_pass: "${AMBARI_PASS}"
    cluster_name: test
    webhdfs:
      url: http://localhost:9870
      user: hdfs
checks:
  service_health:
    enabled: true
alerts:
  log:
    enabled: true
    format: text
```

---

## 🐛 Known Gotchas (dal campo)

Problemi reali incontrati in produzione — leggili prima di scrivere codice che tocca questi moduli.

### HttpFS — accesso sempre tramite load balancer
Il keytab HttpFS contiene `HTTP/<LB-hostname>@REALM` ma NON `HTTP/<nodo-diretto>@REALM`.
Se `webhdfs.url` punta direttamente al nodo (es. `vmgclalpr1615:14000`), la negoziazione
SPNEGO fallisce con `GSSException: No valid credentials` → HTTP 403.
**Regola**: `webhdfs.url` deve puntare **sempre al hostname del load balancer HttpFS**.

### Ansible auto-parsa JSON stdout
Se un comando shell emette JSON valido, Ansible 2.x/3.x renderizza `r.stdout` come
dict Python (non stringa quotata) nell'output del modulo `debug`. Il regex `"r.stdout": "..."` non trova nulla.
**Fix**: usare `json.JSONDecoder().raw_decode(out, pos)` come fallback dopo il regex stringa.

### `check_config` deve includere la sezione `checks` globale
`env_config` contiene solo le chiavi dell'environment (ambari_url, webhdfs, ecc.).
La sezione `checks:` è top-level nel YAML. Se non è mergiata, `self.config.get("checks", {})`
restituisce sempre `{}` e tutti i parametri (test_path, paths, ecc.) sono ignorati.
**Fix** (già in `hadoopscope.py`):
```python
check_config = dict(env_config)
if "checks" in global_config:
    check_config["checks"] = global_config["checks"]
```
**Non rimuovere questo merge senza capire perché esiste.**

### `curl --data-binary '@-'` non funziona su redirect 307
Leggere il body da stdin (`@-`) funziona solo sulla prima request. Su redirect 307
(tipico in HDFS: DataNode redirect), lo stdin è già esaurito → body vuoto → HTTP 400/403/500.
**Fix**: usare sempre literal inline `--data-binary 'contenuto'` nei playbook Ansible.

### Catturare HTTP status code da curl
Non usare `--fail` (sopprime output utile in debug). Usare invece:
```bash
set -e
HTTP=$(curl -s ... -w '%{http_code}' -o /dev/null 'URL')
echo "HTTP:$HTTP"
[ "$HTTP" -ge 200 ] && [ "$HTTP" -lt 300 ]
```
Questo mostra il codice HTTP nell'output Ansible e fa fallire lo script se fuori range 2xx.

### `no_proxy` — mai usare `urlopen` diretto nei check
Ogni modulo con chiamate HTTP (`checks/*.py`) deve avere un proprio helper locale
(es. `_open_url`, `_cm_open`) che gestisce il flag `no_proxy`:
```python
def _my_open(req, timeout, no_proxy=False):
    if no_proxy:
        return build_opener(ProxyHandler({})).open(req, timeout=timeout)
    return urlopen(req, timeout=timeout)
```
**Verifica**: `grep -r "urlopen(" checks/` non deve mai restituire chiamate dirette —
ogni `urlopen` deve essere avvolto nell'helper.

### Crontab manager — formato entry HadoopScope
Le entry gestite da `_step_crontab_manager` in `cluster_status.py` seguono questo formato:
```
# hs: config=/path envs=env1,env2 checks=all
*/15 * * * * /path/python /path/hadoopscope.py --config ... >> /tmp/log 2>&1
```
- Riga marker `# hs:` identifica e separa le entry HS dalle altre
- Entry **disabilitata**: la riga di comando viene commentata con `# ` (il marker rimane attivo)
- Parse/format in `_parse_hs_block` / `_format_hs_block`

---

## 🗓️ Sprint Plan (questo è il task corrente)

### Giorno 1 — Foundation (parti da qui)
1. `hadoopscope.py` — CLI con argparse: `--env`, `--checks`, `--dry-run`, `--output`, `--config`
2. `config.py` — YAML parser manuale (no PyYAML) + env var expansion
3. `checks/base.py` — `CheckResult` + `CheckBase`
4. `checks/ambari.py` — `AmbariServiceHealthCheck` con `requires=[]` (sempre disponibile via API)
5. `alerts/log_alert.py` — output JSON/text su stdout + file opzionale

**Test di fine giorno:**
```bash
AMBARI_PASS=admin python3 hadoopscope.py \
  --config config/test.yaml --env test-hdp \
  --checks health --output text --dry-run
```
→ deve stampare il piano di esecuzione senza fare chiamate reali

### Giorno 2 — WebHDFS + Email
- `checks/webhdfs.py`: HdfsSpaceCheck, HdfsWritabilityCheck, HdfsDataNodeCheck
- `alerts/email_alert.py`: smtplib, template HTML+text

### Giorno 3 — Bootstrap Layer
- `bootstrap.py`: discover_capabilities(), ensure_ansible()
- Integrazione nel main loop

### Giorno 4 — Check avanzati
- NameNode HA, Config Staleness, YARN node health

### Giorno 5 — TuxBox + Packaging
- `tuxbox.toml`, `install.sh`, README, tag v0.1.0

---

## ✅ Testing Strategy

- Per unit test: usa fixture JSON (file `.json` in `tests/fixtures/`) che simulano
  risposte API reali — no mock library, solo `json.loads()` da file
- Per integration test: usa `--dry-run` che verifica config + connettività senza check
- Ogni `CheckBase.run()` deve gestire `urllib.error.URLError` e `socket.timeout`
  restituendo `CheckResult(status=UNKNOWN, ...)` invece di crashare

---

## 📦 TuxBox Integration

`tuxbox.toml` (metti nella root del repo):
```toml
[tool]
name        = "hadoopscope"
version     = "0.1.0"
repo        = "https://github.com/disoardi/hadoopscope"
language    = "python"

[tool.run]
entrypoint  = "hadoopscope.py"
python_min  = "3.6"

[tool.isolation]
prefer      = "venv"
```

---

## 🔗 Documentazione Completa

La documentazione IdeaFlow completa è in:
`~/Progetti/silverbullet/space/Idee/ideas/`
- `idea-003-hadoop-monitor-elaborated.md` — architettura dettagliata
- `idea-003-hadoop-monitor-validated.md` — risk assessment, scope MVP
- `idea-003-hadoop-monitor-document.md` — API reference, config schema, output examples
- `idea-003-hadoop-monitor-prepare.md` — sprint plan dettagliato, checklist
