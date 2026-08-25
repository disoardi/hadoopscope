# Interactive TUI

A curses-based terminal UI for keyboard-only monitoring — no mouse, no
browser, works over SSH on any Python 3.6+ system without extra
dependencies (stdlib `curses` only).

```bash
# Single config file
python3 -m tui.app config/hadoopscope.yaml

# Directory: loads every *.yaml inside as a separate environment set —
# each environment stays tied to the checks:/alerts:/download_dir of its
# own source file, never mixed with another client's even if key names
# collide
python3 -m tui.app config/local
```

## Navigation

- `Tab` / `Shift+Tab` — switch section (forward / backward)
- `↑↓←→` — move within a list or grid
- `Enter` — drill down / confirm
- `ESC` — go back one level, or quit from the top level of a section

Leaving a section resets its navigation stack back to the starting
screen — returning to it later always starts fresh, it never leaves you
stuck on the last screen of a previous flow (e.g. a Monitoring run
result). The screen you land on is also reloaded from disk (`state_store`)
at that point, so it reflects the latest state.

## Sections

### Home

One card per configured environment: worst status, per-status counts,
and — when available — HDFS usage and YARN running/pending apps. `Enter`
on a card opens the per-check detail (scrollable with `↑↓`/`PgUp`/`PgDn`
if it doesn't fit on screen).

A background thread polls `YarnClusterMetricsCheck` every 30 seconds for
**every** configured environment (one lightweight daemon thread per
environment, not sequential — with dozens of environments a serial poll
would fall behind the refresh interval) and persists the result to
`state_store`, so the running/pending counters stay current even without
triggering a full check run. While sitting on the Home tab, the grid
re-reads `state_store` every 5 seconds.

A card shows `⚠ dati vecchi Nh, rilancia` when its oldest check result is
older than 24 hours.

### Monitoring

- **Esegui check ora** — pick one or more environments, then one or more
  check categories (`all` is mutually exclusive with the specific
  categories — picking one deselects the other), run them, and optionally
  schedule the same combination via crontab afterward
- **Gestisci check schedulati** — list, enable/disable, or delete existing
  `hs:`-tagged crontab entries (same parsing as `cluster_status.py`)

### Ops

Same on-demand YARN tools available from the CLI's `ops` subcommand (see
[CLI Reference](cli.md#ops-subcommand)) — pick an environment, fill in
the parameters, get the result directly, no intermediate screen.

## Architecture notes

- `tui/app.py` — main loop, one navigation stack per section, non-blocking
  input (`stdscr.timeout`) so the background poll can refresh the screen
  without waiting for a keypress
- `tui/screens/` — one module per section (`home.py`, `monitoring.py`,
  `ops.py`), plus the shared `Screen` contract (`base.py`): `enter()` to
  (re)load data, `render()` to draw, `handle_input()` to react to a key,
  `on_idle_tick()` for an optional periodic refresh while idle
- `tui/widgets.py` — curses primitives (LCARS-inspired palette with a
  graceful fallback for terminals without 256-color support)
- `tui/polling.py` — the background YARN poller described above
- `state_store.py` — sqlite persistence for "at a glance" state, shared
  between scheduled/manual runs and the background poller
