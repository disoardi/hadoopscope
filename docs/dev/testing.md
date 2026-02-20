# Testing

## Unit tests

Run without any cluster or network:

```bash
python3 tests/test_base.py      # CheckBase logic
python3 tests/test_checks.py    # All checks with HTTP mock server
python3 tests/run_all.py        # Both suites
```

The test suite uses Python's built-in `http.server` to serve fixture JSON files
that simulate real API responses. No `mock` library, no `pytest` — stdlib only.

## Test fixtures

Fixture files live in `tests/fixtures/`:

| File | Simulates |
|------|-----------|
| `ambari_services_ok.json` | All services STARTED |
| `ambari_services_critical.json` | YARN STOPPED, HIVE UNKNOWN |
| `jmx_namenode_ok.json` | 10 live DataNodes, 0 dead |
| `jmx_namenode_dead_dn.json` | 3 dead DataNodes |
| `yarn_nodes_ok.json` | 3 RUNNING nodes |
| `yarn_nodes_unhealthy.json` | 1 UNHEALTHY, 1 LOST |
| `cloudera_services_ok.json` | All services GOOD |

## Mock API server

The standalone mock server can be used for manual testing:

```bash
# Start mock server on port 18080 (ok scenario)
python3 tests/mock_api_server.py --port 18080 --scenario ok

# Start mock server (critical scenario)
python3 tests/mock_api_server.py --port 18080 --scenario critical

# Run hadoopscope against the mock
python3 hadoopscope.py --config config/test.yaml --env test-hdp --checks all
```

## Docker integration tests

```bash
# Build and run all tests in containers
docker compose up --build --abort-on-container-exit

# Run only the test-runner service
docker compose run --rm test-runner

# Interactive dry-run against mock API
docker compose run --rm hadoopscope \
    --config /app/config/docker-test.yaml \
    --env mock-hdp \
    --dry-run
```

## Zero-deps test

Verify the tool works without PyYAML:

```bash
pip uninstall -y pyyaml
python3 tests/test_base.py
python3 hadoopscope.py --config config/test.yaml --env test-hdp --dry-run
pip install pyyaml  # restore
```

## Adding a new test

1. Add a fixture file to `tests/fixtures/` (JSON from a real API response)
2. Add a test function to `tests/test_checks.py`:

```python
def test_my_new_check():
    fixture = load_fixture("my_fixture.json")
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin",
        "ambari_pass": "test",
        "cluster_name": "test",
    }
    try:
        result = MyCheck(config, {}).run()
        assert result.status == CheckResult.OK
    finally:
        server.shutdown()
```
