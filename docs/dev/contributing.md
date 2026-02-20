# Contributing

## Setup

```bash
git clone https://github.com/disoardi/hadoopscope
cd hadoopscope
python3 tests/run_all.py   # verify baseline
```

No virtual environment needed — the core uses only stdlib.

## Coding rules

These are non-negotiable (see [CLAUDE.md](../../CLAUDE.md)):

- **Python 3.6+** compatible — no walrus operator, no dataclasses, no `f"{x:.2f}"`
- **Zero pip dependencies** for core (`checks/`, `alerts/`, `config.py`, `bootstrap.py`)
- **`run()` never raises** — always return `CheckResult(status=UNKNOWN)` on error
- Use `# type: (...)` annotations inline or import from `typing` explicitly
- No `|` in type hints — use `Optional[X]`, `Union[X, Y]`

## Adding a new check

1. Create or edit a file in `checks/`
2. Extend `CheckBase`:

```python
class MyCheck(CheckBase):
    requires = []   # or [["ansible"]] if needed

    def run(self):
        # type: () -> CheckResult
        try:
            # ... your logic ...
            return CheckResult("MyCheck", CheckResult.OK, "all good")
        except Exception as e:
            return CheckResult("MyCheck", CheckResult.UNKNOWN, str(e))
```

3. Register it in `hadoopscope.py → build_check_registry()`
4. Add a fixture + test in `tests/test_checks.py`

## Adding a new alert

1. Create `alerts/my_alert.py` with a `dispatch(results, config, env_name)` function
2. Import and call it in `hadoopscope.py → dispatch_alerts()`

## Pull requests

- Run `python3 tests/run_all.py` before submitting
- One feature/fix per PR
- Update docs if adding a new check or config option
