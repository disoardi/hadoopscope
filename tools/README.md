# tools/ — script standalone (da annettere al tool)

Script autonomi, sola lettura, senza dipendenze oltre alla stdlib. Pensati per essere passati a chi ha i
permessi sulle macchine o lanciati dalla workstation. Non fanno ancora parte della CLI `hadoopscope`.

| Script | Dove gira | Cosa fa |
|---|---|---|
| `os_inventory/os_inventory.sh` | sul nodo, come root | Inventario host per pianificare la sostituzione del SO: distribuzione, dischi/LVM/fstab, rete, pacchetti non di distribuzione, servizi, cron, utenti e processi non di sistema, python/R/java/conda, client DB, Cloudera agent e parcels. Produce un tgz |
| `os_inventory/cm_inventory.py` | workstation con accesso al CM | Export in sola lettura da Cloudera Manager: versioni, parcels, servizi, ruoli per host; deriva `hosts_roles.csv` (ruoli stateful/coordinator per host) e `compat_inputs.csv` |
| `yarn_rm_monitor/yarn_rm_monitor.py` | su un nodo che raggiunge il RM YARN | Campiona il ResourceManager (cluster, code, utenti, app, nodi) in CSV per diagnosticare app in ACCEPTED con code non sature. Compatibile Python 2.7 e 3 |

Ogni script ha in testa uso, esempi e cosa produce.
