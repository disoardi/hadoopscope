# HadoopScope — Layer Ops: design

**Data**: 2026-08-20
**Stato**: approvato in brainstorming, in attesa di piano di implementazione
**Sotto-progetto**: primo dei 4 che compongono il "target 2" (persistenza stato,
layer Ops, shell TUI LCARS, dashboard "at a glance") — vedi Contesto.

---

## Contesto

HadoopScope oggi copre solo il perimetro "monitoring": 14 check read-only
(Ambari/CM, WebHDFS, YARN, Hive) eseguiti in modo schedulabile via il pattern
`CheckBase.run()` (nessun parametro, nessun input umano, pensato per cron).
Non esiste nel CLI nessun comando che richieda input dall'utente o compia
un'azione on-demand.

Il target 2 introduce un secondo perimetro, "Ops": tool operativi lanciati
on-demand, con input dall'utente, che affiancano (senza sostituire) il layer
di monitoring. Il target 2 è stato scomposto in 4 sotto-progetti indipendenti
per dimensione e dipendenze:

1. Persistenza stato (sqlite) — non ancora disegnato
2. **Layer Ops + primo tool "application id"** — oggetto di questo documento
3. Shell di navigazione TUI in stile LCARS — non ancora disegnato
4. Dashboard "at a glance" — non ancora disegnato, dipende da (1) e (3)

Questo documento copre solo il punto 2.

## Obiettivo

Dato un application id YARN, un utente deve poter recuperare status e
metriche dell'applicazione da qualunque environment configurato, e — se
l'applicazione è terminata — scaricarne i log. La macchina che esegue
HadoopScope raggiunge la rete del cluster ma non è un nodo del cluster
stesso (nessun client Hadoop locale).

## Non-goal espliciti (fuori scope per questo documento)

- La shell TUI (verrà disegnata a parte); questo documento copre solo il
  layer dati/logica invocabile da CLI, con un contratto già pensato per
  essere riusato dalla TUI in seguito.
- La persistenza in sqlite: i risultati dei tool Ops sono on-demand ed
  effimeri, **non** vengono scritti nello stato "at a glance" (quella
  tabella è riservata ai check di monitoring schedulati).
- Un terzo tool Ops proposto in brainstorming — "inventory check": query a
  Cloudera Manager per aggiornare un inventory e push su un repo GitHub
  esterno (l'unico raggiungibile dalla macchina ponte in alcuni ambienti
  cliente). Introduce credenziali git e un vero write su stato esterno:
  richiede un giro di design a parte. Il contratto `OpsToolBase` include
  però già un flag `is_write` pensato per questo caso futuro.
- Supporto multi edge-host con failover per Ansible: resta un singolo
  `ansible.edge_host`, come oggi per `HiveCheck`. Si estende solo se emerge
  un bisogno reale.

## Architettura

Nuovo pacchetto `ops/`, parallelo a `checks/`, stessa filosofia di
`CheckBase` ma per azioni on-demand invece che schedulate:

```
ops/
├── __init__.py       # vuoto, come checks/__init__.py
├── base.py           # OpsParam, OpsToolBase
└── yarn_app.py        # AppStatusTool, AppLogsTool
```

### Refactor condivisi (nessuna duplicazione)

Principio guida per l'intero sotto-progetto: qualunque funzione toccata da
2+ moduli va estratta in una libreria condivisa, mai copiata.

- **`ansible_runner.py`** (nuovo modulo root): estrae `_build_inventory()`,
  `_run_playbook()`, `_extract_stdout()`, `_extract_stderr()`,
  `_extract_task_error()` da `checks/hive.py`, dove oggi sono metodi/funzioni
  private specifiche di `HiveCheck`. `HiveCheck` viene aggiornato per usare
  il modulo condiviso (comportamento identico). `AppLogsTool` usa lo stesso
  modulo per eseguire `yarn logs` sull'edge node.
- **`kerberos_utils.py`** (nuovo modulo root): estrae `_kinit()` da
  `checks/webhdfs.py`. Usato da `AppStatusTool` per il kinit locale prima
  delle chiamate REST kerberizzate.
- **Mixin `can_run()`** in `checks/base.py`: la logica OR-of-AND-list di
  `CheckBase.can_run()` viene estratta in un mixin minimo, riusato sia da
  `CheckBase` sia da `OpsToolBase` — stesso comportamento, zero duplicazione.
- **`checks/yarn.py`**: `_yarn_get()` generalizzato per accettare un path
  REST completo invece di assumere sempre il prefisso `/ws/v1/cluster/`
  (serve per interrogare l'Application History/Timeline Server, che ha un
  prefisso diverso). Nuovo helper `_resolve_url(cfg_block, key)` che
  unifica la logica già esistente per `rm_url`/`rm_urls` ed è riusato per
  tutti i nuovi endpoint (`history_url`/`_urls`, `spark_history_url`/`_urls`,
  ecc.) invece di N resolver quasi identici.

## Contratto `OpsToolBase`

```python
# ops/base.py
class OpsParam(object):
    def __init__(self, name, help, required=True, type=str):
        # type: (str, str, bool, type) -> None
        self.name = name
        self.help = help
        self.required = required
        self.type = type


class OpsToolBase(object):
    name = ""          # es. "app-status" — id univoco, usato da CLI/registry/TUI
    description = ""
    params = []          # type: list[OpsParam] — dichiarazione unica, letta da CLI e TUI
    requires = []         # stesso OR-of-AND-list di CheckBase.requires
    is_write = False       # True per tool che modificano stato esterno (nessuno oggi,
                            # riservato per il futuro tool "inventory check")

    def __init__(self, config, caps):
        self.config = config
        self.caps = caps

    def can_run(self):
        # riusa il mixin condiviso con CheckBase
        ...

    def run(self, **kwargs):
        # type: (...) -> CheckResult
        raise NotImplementedError
```

Tipo di ritorno: si riusa **`CheckResult`** (status/message/details) così
com'è — struttura già adatta, non serve un `OpsResult` parallelo.

## `AppStatusTool`

- `name = "app-status"`
- `params = [OpsParam("app_id", help="YARN application ID, es. application_1699999999_0001")]`
- `requires = []` — solo REST, nessuna dipendenza da Ansible/Docker

Flusso `run(self, app_id)`:

1. Risolve l'URL RM riusando `_rm_url()`/`_yarn_get()` esistenti (stessa
   auto-detect logic di `YarnNodeHealthCheck`, stesso supporto multi-host
   via `rm_urls` con redirect 307 seguito da curl per il failover HA).
2. Query `{rm_url}/ws/v1/cluster/apps/{app_id}`. Se l'app non è trovata
   (404 o assente dalla risposta), fallback su `yarn.history_url`/`_urls`
   (Application History Server per HDP — endpoint ATS v1 generico — o
   Timeline Service v2 per CDP — shape diversa, branch su
   `env_config["type"]`, stesso pattern già usato tra `checks/ambari.py` e
   `checks/cloudera.py`).
3. Normalizza la risposta (qualunque sia la fonte) in campi comuni:
   `state`, `finalStatus`, `progress`, `allocatedMB`, `allocatedVCores`,
   `runningContainers`, `elapsedTime`, `diagnostics`.
4. Se lo stato è terminale e `applicationType` è spark/mapreduce/tez **e**
   il relativo `yarn.<type>_history_url` è configurato: tenta il fetch dei
   counters, best-effort — se fallisce o non è configurato, il messaggio
   nota semplicemente "counters non disponibili" senza far fallire il resto
   del risultato.
5. Kerberos: se `yarn.kerberos.enabled` (o `kerberos.enabled` top-level come
   fallback) è true, chiama `kinit_utils._kinit()` esplicitamente prima
   della prima REST call (non assume che un altro check l'abbia già fatto
   nello stesso run) usando `yarn.kerberos.{keytab,principal}` con fallback
   su `kerberos.{keytab,principal}` top-level.
6. Ritorna `CheckResult`: OK se RUNNING o FINISHED+SUCCEEDED, WARNING se
   KILLED, CRITICAL se FAILED, UNKNOWN se non trovata né in RM né in
   History Server.

## `AppLogsTool`

- `name = "app-logs"`
- `params = [OpsParam("app_id", help="YARN application ID")]`
- `requires = [["ansible"], ["venv_ansible"], ["docker"]]` — stesso vincolo
  di `HiveCheck`

Flusso `run(self, app_id)`:

1. Richiede `ansible.edge_host` configurato (stesso controllo runtime già
   presente in `HiveCheck` — errore chiaro se assente).
2. Inventory single-host generato al volo via `ansible_runner.build_inventory()`
   (stesso meccanismo di `HiveCheck`, nessun inventory statico da mantenere).
3. Se `ansible.kerberos.enabled`, inietta un kinit nel playbook (stesso
   pattern di `_build_kinit_cmd` usato da `HiveCheck`, spostato in
   `ansible_runner.py`) usando `ansible.kerberos.{keytab,client_principal}` —
   contesto edge-node distinto da `hive.kerberos`, nessun fallback incrociato.
4. Esegue `yarn logs -applicationId {app_id}` sull'edge node via
   `ansible_runner.run_playbook()`.
5. Salva lo stdout in `{download_dir}/{app_id}.log` (crea la directory se
   manca). `download_dir` è una chiave di config top-level, default
   `~/.hadoopscope/downloads/` se assente (stessa filosofia di
   `~/.hadoopscope/logs/` e `~/.hadoopscope/venv/` già esistenti).
6. Ritorna `CheckResult` con path e dimensione del file su successo;
   CRITICAL con dettaglio dell'errore Ansible su fallimento (riuso di
   `_extract_stderr`/`_extract_task_error`, ora in `ansible_runner.py`).

## Config — chiavi aggiuntive

Estende la struttura esistente (`environments.<env>.yarn`,
`environments.<env>.ansible`), più una chiave top-level nuova. Nessun nuovo
file di config, nessuna sezione parallela — stessa config YAML condivisa da
monitoring, ops e (in futuro) dashboard.

```yaml
download_dir: ~/.hadoopscope/downloads   # opzionale, default se assente

environments:
  prod-cdp:
    type: cdp
    # ... chiavi esistenti (cm_url, webhdfs, ecc.) ...
    yarn:
      rm_url: http://rm1:8088                 # già esistente
      history_url: http://ats1:8188            # NUOVO — fallback status/metriche
      spark_history_url: http://shs:18089       # NUOVO, opzionale — counters
      mr_history_url: http://jhs:19888          # NUOVO, opzionale — counters
      tez_history_url: http://tez:9999          # NUOVO, opzionale — counters
      kerberos:                                  # NUOVO, opzionale
        enabled: true
        keytab: /path/locale/al/keytab           # macchina LOCALE — vedi tabella sotto
        principal: "svc@REALM"
    ansible:
      edge_host: edge1.example.com               # già esistente, riusato per i log
      ssh_user: svc_hadoopscope
      ssh_key: /path/to/key
      kerberos:                                   # NUOVO, opzionale
        enabled: true
        keytab: /path/edge/al/keytab              # NODO EDGE — vedi tabella sotto
        client_principal: "svc@REALM"
```

### Kerberos — due contesti aggiuntivi (coerenti con la tabella già in CLAUDE.md)

| Chiave config | Usato da | Dove vive il keytab |
|---|---|---|
| `yarn.kerberos.{keytab,principal}` | `AppStatusTool` — kinit locale prima delle REST call a RM/History Server | Macchina **locale** |
| `ansible.kerberos.{keytab,client_principal}` | `AppLogsTool` (e futuri tool Ops via Ansible) — kinit iniettato nel playbook prima di `yarn logs` | Nodo **edge** remoto |

Nessun fallback incrociato tra i due contesti, stessa regola già in vigore
per `hive.kerberos` vs `webhdfs.kerberos`. Convenzione consigliata (non
imposta) per la posizione dei keytab locali: `~/.hadoopscope/keytabs/` —
stessa filosofia di `logs/`, `venv/`, `downloads/`. `yarn.kerberos.keytab`
resta comunque un path libero in config.

## Inventory Ansible — nota di design

Valutata e scartata l'idea di centralizzare i riferimenti host (edge node,
ecc.) in inventory Ansible statici stile `isp_ansible`
(`inventories/<Cliente>/<Tecnologia>/<Ambiente>/...`). Motivo: la config
YAML di HadoopScope è già l'unica fonte di verità per tutto ciò che serve
(URL REST, host ZK, edge host) — centralizzare negli inventory
significherebbe duplicare le stesse informazioni in due file diversi,
esattamente il problema di manutenzione che si voleva evitare. L'inventory
Ansible resta un artefatto generato a runtime in Python da
`ansible.edge_host`, mai un file scritto a mano — stesso schema già in uso
da `HiveCheck`. Il modello isp_ansible resta il riferimento naturale per un
*futuro* tool Ops che debba operare su gruppi di macchine (es. restart di
un intero cluster), non per questo sotto-progetto.

## CLI

Nuovo verbo `ops` intercettato come primo argomento posizionale, *prima*
del parser principale esistente — non un subparser innestato (evita le
incongruenze note di argparse sui subparser opzionali tra versioni 3.6-3.7)
e mantiene le due CLI surface (monitoring esistente, ops nuovo) totalmente
separate, coerente con la separazione concettuale Monitoring/Ops:

```
hadoopscope.py ops app-status --config ... --env prod-cdp --app-id application_123
hadoopscope.py ops app-logs   --config ... --env prod-cdp --app-id application_123
```

- `ops_main(argv)` in `hadoopscope.py`: parser dedicato con un subparser
  per tool, i cui argomenti sono generati dinamicamente da
  `OpsToolBase.params` — un solo posto dichiara i parametri, sia CLI sia
  (in futuro) TUI li leggono da lì.
- Nessun impatto sull'invocazione esistente (`--env`, `--checks`, entry
  crontab `hs:`): quella resta identica, `ops` è un ramo separato
  controllato da `sys.argv[1]`.
- Registry: `build_ops_registry()` in `hadoopscope.py`, stesso stile
  minimale di `build_check_registry()` —
  `{cls.name: cls for cls in [AppStatusTool, AppLogsTool]}`.
- Prima di `run()`: check `can_run()` — se False, messaggio chiaro (stesso
  principio di `CheckBase`, mai un crash).
- Output: riusa `--output {text,json}` esistente.

## Testing

Stesso pattern di `tests/test_checks.py`: fixture JSON in `tests/fixtures/`
che simulano risposte RM, Application History Server (HDP), Timeline
Service v2 (CDP), Spark/MapReduce/Tez History Server. Nuovo file
`tests/test_ops.py`, esplicitamente aggiunto alla lista `test_files` in
`tests/run_all.py` (gap noto: `test_applog.py` non c'era, da non ripetere).

## Rischi noti / da verificare in fase di implementazione

- **Shape della risposta Timeline Service v2 (CDP)** vs Application History
  Server v1 (HDP): non verificata contro un cluster reale in questa fase di
  design, solo assunta diversa in base alla documentazione YARN. Va
  validata contro gli ambienti di test MdS/INAIL prima di considerare il
  fallback affidabile.
- **argparse e il verbo `ops`**: l'approccio "intercetta `sys.argv[1]` prima
  del parser principale" è scelto per bypassare i problemi noti dei
  subparser opzionali, ma va verificato che `--help` resti coerente (es.
  `hadoopscope.py --help` non deve menzionare `ops` in modo fuorviante se
  non lo si vuole esporre lì).
- **Kerberos per YARN REST**: il codice esistente (`_yarn_get` con
  `kerberos=True`) assume che un ticket sia già in cache o lo ottiene via
  `curl --negotiate` con credential cache di sistema; questo documento
  aggiunge un kinit esplicito in `AppStatusTool` per non dipendere
  dall'ordine di esecuzione di altri check nello stesso run — va testato
  che le due modalità (kinit esplicito vs cache già popolata da un altro
  check) non entrino in conflitto.
