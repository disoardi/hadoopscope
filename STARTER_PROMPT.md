# HadoopScope — Starter Prompt per Claude Code

> Copia e incolla questo prompt all'inizio di ogni sessione Claude Code
> dentro la directory `~/Progetti/hadoopscope/`.

---

## Prompt da incollare (sessione Giorno 1)

```
Stiamo sviluppando HadoopScope, un CLI tool Python per monitoraggio unificato
di cluster Hadoop HDP e CDP. Leggi CLAUDE.md per il contesto completo prima
di fare qualsiasi cosa.

Oggi siamo al Giorno 1 dello sprint MVP. Gli obiettivi di oggi sono:

1. Verificare che il progetto scaffold sia corretto (struttura file, import)
2. Eseguire i test base: python3 tests/test_base.py
3. Completare il main loop in hadoopscope.py per gestire il routing
   check_registry → can_run() → fallback → alerts
4. Verificare che il dry-run funzioni senza errori:
   python3 hadoopscope.py --config config/example.yaml --env prod-hdp
   --checks health --dry-run --output text
5. Aggiungere AmbariServiceHealthCheck a checks/ambari.py se mancante
   o correggerla se necessario

Constraint importanti da rispettare sempre (vedi CLAUDE.md per dettagli):
- Python 3.6 compatible: no walrus operator, no dataclasses, no typing con |
- Zero pip install per il core (solo stdlib)
- CheckBase.run() non deve mai crashare — gestire sempre le eccezioni
  restituendo CheckResult(status=UNKNOWN) invece di propagare l'errore

Inizia leggendo CLAUDE.md, poi controlla la struttura del progetto con
ls -la e tree (se disponibile), poi esegui i test.
```

---

## Prompt Giorno 2 — WebHDFS + Email

```
Continuiamo HadoopScope. Leggi CLAUDE.md per il contesto.

Oggi: Giorno 2 — WebHDFS checks + email alert funzionante.

Obiettivi:
1. Completare/verificare checks/webhdfs.py:
   - HdfsSpaceCheck: space usage per path configurati
   - HdfsDataNodeCheck: dead DataNodes via JMX NameNode
   - HdfsWritabilityCheck: test write/read/delete su /tmp

2. Completare alerts/email_alert.py con smtplib:
   - Invia email solo se ci sono WARNING o CRITICAL
   - Template plain text (no HTML per ora)
   - Gestire SMTP con e senza TLS, con e senza auth

3. Integrare alerts nel main loop di hadoopscope.py:
   - Dopo i check, chiamare log_alert.dispatch() e email_alert.dispatch()

4. Test dry-run con checks hdfs:
   python3 hadoopscope.py --config config/example.yaml --env prod-hdp
   --checks hdfs --dry-run --output text

Ricorda: gestire sempre timeout e errori di rete nei check WebHDFS —
URLError, HTTPError, socket.timeout → CheckResult(UNKNOWN) mai crash.
```

---

## Prompt Giorno 3 — Bootstrap Layer

```
Continuiamo HadoopScope. Leggi CLAUDE.md per il contesto.

Oggi: Giorno 3 — Bootstrap layer e capability map completa.

Obiettivi:
1. Completare bootstrap.py:
   - discover_capabilities(): scansione tool disponibili (ansible, docker,
     kinit, klist, zabbix_sender)
   - ensure_ansible(): installa ansible in venv ~/.hadoopscope/venv/ se manca
   - Stampa capability map se --verbose

2. Integrare bootstrap nel main loop:
   - discover_capabilities() chiamato una volta all'inizio
   - ensure_ansible() chiamato solo se ci sono check che richiedono ansible
   - capability_map passato a tutti i check

3. Test graceful degradation:
   - Simulare caps = {} (nessun tool) e verificare che tutti i check
     che richiedono ansible/docker tornino SKIPPED senza crash
   - python3 -c "from bootstrap import discover_capabilities;
     import json; print(json.dumps(discover_capabilities(), indent=2))"

4. Aggiungere --show-capabilities flag al CLI per stampare la cap map
   e uscire (utile per debug su ambienti nuovi)
```

---

## Prompt Giorno 4 — Check Avanzati

```
Continuiamo HadoopScope. Leggi CLAUDE.md per il contesto.

Oggi: Giorno 4 — Check avanzati HDP + YARN.

Obiettivi:
1. checks/ambari.py — aggiungere:
   - NameNodeHACheck: verifica active/standby via Ambari API
     GET /api/v1/clusters/{name}/services/HDFS/components/NAMENODE
   - ClusterAlertsCheck: riassunto alert CRITICAL attivi
     GET /api/v1/clusters/{name}/alerts?fields=*&Alert/state=CRITICAL
   - ConfigStalenessCheck: config non propagate
     GET /api/v1/clusters/{name}/services?fields=ServiceInfo/config_staleness_check_issues

2. checks/yarn.py — creare:
   - YarnNodeHealthCheck: DataNodes UNHEALTHY via YARN RM REST
     GET http://{rm_host}:8088/ws/v1/cluster/nodes
   - YarnQueueCheck: utilizzo code (usedCapacity > 90%)
     GET http://{rm_host}:8088/ws/v1/cluster/scheduler

3. Aggiungere al check_registry in hadoopscope.py e testare con dry-run

Nota: il YARN RM url va letto da config["yarn"]["rm_url"] se presente,
oppure costruito da ambari_url con la porta default 8088.
```

---

## Prompt Giorno 5 — Packaging + TuxBox

```
Continuiamo HadoopScope. Leggi CLAUDE.md per il contesto.

Oggi: Giorno 5 — Packaging, TuxBox integration, README, release v0.1.0.

Obiettivi:
1. Verificare tuxbox.toml è completo e corretto
   - Testa: tbox run hadoopscope -- --help (se TuxBox disponibile)

2. Completare install.sh:
   - Clone repo + symlink CLI
   - Gestire update se già installato (git pull)
   - Test su macchina pulita (senza Python deps installati)

3. README.md completo:
   - Quickstart (3 comandi)
   - Feature matrix: cosa funziona con quali capability
   - Config reference (link a example.yaml commentato)
   - Esempio output text e JSON

4. Verifica finale end-to-end:
   python3 tests/test_base.py           # unit tests
   python3 hadoopscope.py --dry-run ... # dry-run su tutti gli env
   python3 hadoopscope.py --show-capabilities

5. Tag v0.1.0:
   git tag -a v0.1.0 -m "MVP: Ambari health + WebHDFS + email + bootstrap"
   git push origin main --tags
```

---

## Note Generali per Tutte le Sessioni

- Il progetto si trova in `~/Progetti/hadoopscope/`
- CLAUDE.md nella root contiene architettura, constraint e API reference
- Documentazione IdeaFlow completa: `~/Progetti/silverbullet/space/Idee/ideas/`
- Test senza cluster reale: usa `--dry-run` o i test in `tests/`
- Per test con cluster reale: `export AMBARI_PASS=xxx` prima di lanciare

**Prima di chiudere ogni sessione:**
```bash
git add -p          # revisiona cosa stai committando
git commit -m "..."
```
