# HadoopScope — Shell TUI LCARS: design

**Data**: 2026-08-20
**Stato**: approvato in brainstorming, in attesa di piano di implementazione
**Sotto-progetto**: terzo dei 4 che compongono il "target 2", ma assorbe anche
il quarto (dashboard "at a glance") — vedi Contesto.

---

## Contesto

Il target 2 era stato scomposto in 4 sotto-progetti: (1) persistenza stato
sqlite, (2) layer Ops — **completato e mergiato in `main`** — (3) shell TUI
LCARS, (4) dashboard "at a glance". Durante il brainstorming di questo
documento è emerso che la visione dell'utente per la TUI (tab "Home" con
card riassuntive per cluster + drill-down di dettaglio) **è** la dashboard
"at a glance" — non sono due cose separate. Di conseguenza questo documento
assorbe anche il sotto-progetto (1) sqlite, che diventa una dipendenza
diretta invece che un giro a parte: senza stato persistito il tab Home non
avrebbe nulla da mostrare.

Non esiste più un sotto-progetto (4) separato: il target 2 si chiude con
questo documento più il layer Ops già fatto.

## Obiettivo

Un punto d'accesso interattivo, 100% da tastiera, in stile visivo LCARS
(palette nero/arancione/viola/rosa, bordi squadrati a doppia linea, blocchi
colorati pieni per la navigazione — niente angoli arrotondati, impossibili
in un terminale), da cui l'utente:
- vede a colpo d'occhio lo stato di tutti i cluster configurati (Home)
- lancia o schedula check di monitoring (Monitoring)
- esegue i tool Ops on-demand già disponibili — `app-status`, `app-logs`
  (Ops)

## Decisione: riscrittura completa, non estensione di `cluster_status.py`

Discusso esplicitamente in brainstorming: si riscrivono da zero sia i widget
di disegno sia il modello di navigazione (non si eredita lo state-machine
lineare a step numerati di `cluster_status.py`). **Eccezione**: la logica
di parsing/scrittura del crontab (`_crontab_read`, `_parse_hs_block`,
`_format_hs_block`, ecc.) è codice non-visivo, indipendente dalla
riscrittura del look — viene riusata com'è, cambia solo come le entry
vengono disegnate a schermo.

`cluster_status.py` viene rimosso come entry point una volta che la nuova
TUI lo sostituisce funzionalmente (stesso esito pratico, wizard di
monitoring + crontab manager, ma dentro la nuova struttura a tab).

## Architettura

Nuovo pacchetto `tui/`:

```
tui/
├── __init__.py
├── widgets.py         # box, liste, prompt testuali, palette LCARS
├── app.py              # loop principale, sidebar persistente, dispatch tab
└── screens/
    ├── __init__.py
    ├── base.py          # Screen: enter()/render()/handle_input()
    ├── home.py           # grid cluster + drill-down dettaglio
    ├── monitoring.py      # sottomenu: esegui ora / gestisci schedulati
    └── ops.py              # lista tool → env → parametri → risultato
```

Nuovo modulo `state_store.py` (root, accanto a `applog.py`), sqlite stdlib.

### Modello di navigazione

Ibrido: **sidebar persistente** (Home/Monitoring/Ops, sempre visibile,
cambio sezione con `Tab` o frecce sinistra/destra) + **stack di schermate
per sezione** per il drill-down (`Screen.enter()`/`Screen.render()`/
`Screen.handle_input()`; `ESC` fa pop e torna al livello precedente
*dentro la stessa sezione*). Cambiare sezione dalla sidebar resetta lo
stack di quella sezione alla vista principale — nessuno stato "sporco"
nascosto tra le sezioni.

**Nessun contesto ambiente condiviso tra tab**: ogni sezione mantiene la
propria selezione env in modo indipendente (scelta esplicita in
brainstorming) — la selezione fatta in Home non si "ricorda" passando a
Monitoring o Ops.

## Persistenza — `state_store.py`

```python
def save_result(env_name, result):
    # type: (str, CheckResult) -> None
    """INSERT OR REPLACE — stesso punto di chiamata di applog.log_result()."""

def get_env_summary(env_name):
    # type: (str) -> list
    """Righe check_state per un singolo env — per il drill-down Home."""

def get_all_envs_summary():
    # type: () -> list
    """Per env: stato peggiore, conteggi per status, ultimo run_at — per la grid Home."""
```

Schema:
```sql
CREATE TABLE check_state (
  env TEXT, check_name TEXT, status TEXT, message TEXT,
  details TEXT, run_at TEXT,
  PRIMARY KEY (env, check_name)
)
```

`env` è la colonna guida della chiave primaria: le query per singolo
ambiente (`WHERE env = ?`, il caso dominante nei deep dive — confermato
esplicitamente dall'utente: "a parte l'overview generale, i deep dive sono
sempre per singolo env") sono un lookup efficiente sul prefisso della PK;
l'aggregazione per la Home è un `GROUP BY env` sulla stessa tabella. Nessuna
tabella separata per ambiente — inutile in sqlite con questo schema.

`INSERT OR REPLACE` mantiene la tabella sempre a *N righe* (una per coppia
env×check), mai in crescita, zero cleanup necessario — stessa logica già
validata nello spec del layer Ops per lo stesso motivo.

**Hook unico**: in `hadoopscope.py::run_checks_for_env()`, accanto alla
chiamata esistente `_applog.log_result(r)` — sia i run schedulati via cron
sia quelli lanciati dalla TUI aggiornano lo stesso stato, stesso path di
codice, nessuna duplicazione.

## Tab Home

**Vista principale**: grid con una card per environment configurato, letta
da `get_all_envs_summary()` — pallino di stato aggregato (peggiore tra i
check), conteggi OK/WARNING/CRITICAL, timestamp ultimo run. Se un env non
ha mai girato: card con "nessun check ancora eseguito", nessun crash.

**Drill-down** (Invio su una card): dettaglio con tutte le righe di
`get_env_summary(env)` per quell'env — status per servizio, stato code
YARN, motivo esteso di un WARNING/CRITICAL (già nel campo `message`/
`details` di ogni `CheckResult`, nessun dato nuovo da calcolare).

## Tab Monitoring

Sottomenu con due voci:

1. **Esegui check ora**: multi-select environment → multi-select categoria
   check (`all`/`health`/`hdfs`/`hive`/`yarn`, stessi valori già supportati
   dal CLI) → conferma → esecuzione bloccante con progress a schermo
   (stesso path di `run_checks_for_env()`, che ora scrive anche su
   `state_store`) → a fine corsa, prompt "Rendere schedulato? [s/N]" → se
   sì, riusa `_dialog_add_edit_schedule` (rilavorato per il nuovo look) per
   aggiungere l'entry crontab `hs:`.
2. **Gestisci check schedulati**: lista le entry `hs:` esistenti (riuso
   `_crontab_read`/`_parse_hs_block`), permette abilita/disabilita
   (commenta/decommenta la riga comando, marker `hs:` resta attivo — stessa
   convenzione già in CLAUDE.md), modifica orario, elimina.

## Tab Ops

Lista tool letta dal registry `ops` (vedi refactor sotto) → seleziona un
solo environment → per ogni `OpsParam` dichiarato in `tool_cls.params`, un
prompt testuale in sequenza (stesso meccanismo dichiarativo già usato dal
CLI `ops_main()` — un solo posto dichiara i parametri, CLI e TUI lo
leggono identico) → `can_run()` (se `False`, messaggio chiaro invece di
tentare l'esecuzione) → `tool.run(**kwargs)` → schermata di risultato con
status/message/details del `CheckResult`. `ESC` torna alla lista tool.

### Refactor necessario: registry Ops importabile senza `hadoopscope.py`

`build_ops_registry()` oggi vive dentro `hadoopscope.py`. Si sposta in
`ops/__init__.py` (stesso identico corpo, solo posizione) così sia il CLI
sia la nuova TUI lo importano direttamente da `ops`, senza che la TUI debba
importare `hadoopscope.py` (che a sua volta avvierebbe side-effect di CLI
parsing) solo per ottenere la lista dei tool disponibili.

## Palette LCARS (curses)

Concordata in brainstorming, adattamento realistico ai vincoli di un vero
terminale (niente angoli arrotondati, niente vere "pillole" — quelli sono
concetti da browser):

- Arancione pieno (background) su testo nero — header applicazione e tab
  attivo nella sidebar
- Viola e rosa pieni (background) su testo nero — tab inattivi nella
  sidebar, un colore diverso per sezione così restano identificabili anche
  da spenti
- Verde/giallo/rosso (foreground, sfondo terminale) — stato OK/WARNING/
  CRITICAL, già usati in `cluster_status.py`, si riusano
- Bordi con caratteri Unicode box-drawing a doppia linea (`╔═╗║╚╝`) per i
  pannelli principali, invece dei singoli (`┌─┐│└┘`) usati oggi — look più
  "pannello LCARS", meno "form testuale"

`curses.init_pair()` richiede un terminale che supporti almeno 8 colori
(quasi universale); l'arancione/viola/rosa pieni richiedono che il
terminale supporti la ridefinizione dei colori di sfondo custom (256 colori
o true color) — se il terminale supporta solo 8 colori base, fallback su
`COLOR_YELLOW`/`COLOR_MAGENTA`/`COLOR_MAGENTA` (approssimazione più vicina
disponibile) invece di RGB custom. Rilevamento e fallback vanno gestiti in
`widgets.py` all'avvio (`curses.COLORS` per capire quanti colori sono
disponibili), non assunti.

## Testing

Le funzioni di `state_store.py` (query SQL pure, nessuna dipendenza da
curses) sono testabili come oggi si testa `applog.py` — nuovo
`tests/test_state_store.py`, aggiunto a `tests/run_all.py`. Le screen/
widget curses non sono testabili in modo automatico in modo significativo
(stesso limite già esistente per `cluster_status.py`, mai stato testato
automaticamente) — restano verificate manualmente.

## Rischi noti / da verificare in fase di implementazione

- **Supporto colori del terminale target**: da verificare il fallback su
  terminali con meno di 256 colori (es. sessioni SSH con `TERM=xterm` senza
  256color) — non bloccante, ma va testato esplicitamente, non assunto.
- **Dimensione minima terminale**: la sidebar + area contenuto richiede più
  colonne del wizard lineare attuale — va definita una larghezza minima
  sotto la quale mostrare un messaggio invece di un layout rotto.
