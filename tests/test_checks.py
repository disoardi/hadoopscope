"""
Test suite per i check HadoopScope — usa fixture JSON, nessun cluster reale.
Utilizza un HTTP server in-process per servire le risposte fixture.
"""

from __future__ import print_function

import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import urllib.parse as _urlparse
except ImportError:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
    import urlparse as _urlparse

from checks.base import CheckBase, CheckResult
from checks.ambari import (
    AmbariServiceHealthCheck, ClusterAlertsCheck,
    ConfigStalenessCheck, NameNodeHACheck,
)
from checks.webhdfs import HdfsDataNodeCheck, HdfsSpaceCheck
from checks.yarn import YarnNodeHealthCheck, YarnQueueCheck
from checks.cloudera import ClouderaServiceHealthCheck

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name):
    # type: (str) -> dict
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Minimal HTTP server per mock API
# ---------------------------------------------------------------------------

class _MockHandler(BaseHTTPRequestHandler):
    """Serve risposte JSON dalle fixture in base al path della request."""

    route_map = {}  # type: dict  # path_prefix -> fixture_name

    def do_GET(self):
        # type: () -> None
        response = None
        for prefix, fixture in self.__class__.route_map.items():
            if self.path.startswith(prefix):
                response = fixture
                break

        if response is None:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "not found"}')
            return

        data = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass  # silenzia il log HTTP


def start_mock_server(route_map, port=0):
    # type: (dict, int) -> tuple
    """
    Avvia un HTTP server mock in un thread daemon.
    Restituisce (server, actual_port).
    """
    # Crea handler class dedicata con route map
    handler_class = type(
        "MockHandler_{}".format(id(route_map)),
        (_MockHandler,),
        {"route_map": route_map}
    )
    server = HTTPServer(("127.0.0.1", port), handler_class)
    actual_port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()
    return server, actual_port


# ---------------------------------------------------------------------------
# Test: CheckBase
# ---------------------------------------------------------------------------

class _NoRequires(CheckBase):
    requires = []
    def run(self): return CheckResult("X", CheckResult.OK, "ok")

class _NeedsAnsible(CheckBase):
    requires = [["ansible"]]
    def run(self): return CheckResult("X", CheckResult.OK, "ok")

class _NeedsAnsibleOrDocker(CheckBase):
    requires = [["ansible"], ["docker"]]
    def run(self): return CheckResult("X", CheckResult.OK, "ok")


def test_base_can_run_no_requires():
    assert _NoRequires({}, {}).can_run() is True

def test_base_can_run_with_cap():
    assert _NeedsAnsible({}, {"ansible": True}).can_run() is True

def test_base_cannot_run_missing_cap():
    assert _NeedsAnsible({}, {}).can_run() is False

def test_base_or_logic():
    assert _NeedsAnsibleOrDocker({}, {"docker": True}).can_run() is True
    assert _NeedsAnsibleOrDocker({}, {"ansible": True}).can_run() is True
    assert _NeedsAnsibleOrDocker({}, {}).can_run() is False

def test_base_run_raises_not_implemented():
    class Bad(CheckBase):
        requires = []
    try:
        Bad({}, {}).run()
        assert False, "should raise"
    except NotImplementedError:
        pass


# ---------------------------------------------------------------------------
# Test: AmbariServiceHealthCheck
# ---------------------------------------------------------------------------

def test_ambari_service_health_ok():
    fixture = load_fixture("ambari_services_ok.json")
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin",
        "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        check  = AmbariServiceHealthCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.OK, "Expected OK, got {}: {}".format(
            result.status, result.message)
    finally:
        server.shutdown()


def test_ambari_service_health_critical():
    fixture = load_fixture("ambari_services_critical.json")
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin",
        "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        check  = AmbariServiceHealthCheck(config, {})
        result = check.run()
        # HIVE UNKNOWN state → CRITICAL (non-INSTALLED/non-STARTED state)
        assert result.status == CheckResult.CRITICAL, \
            "Expected CRITICAL, got {}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


def test_ambari_service_health_installed_no_filter_is_ok():
    """INSTALLED senza filtro services non deve warnare (falso positivo per client libs)."""
    fixture = {
        "items": [
            {"ServiceInfo": {"service_name": "HDFS",   "state": "STARTED",   "maintenance_state": "OFF"}},
            {"ServiceInfo": {"service_name": "YARN",   "state": "STARTED",   "maintenance_state": "OFF"}},
            {"ServiceInfo": {"service_name": "PIG",    "state": "INSTALLED", "maintenance_state": "OFF"}},
            {"ServiceInfo": {"service_name": "TEZ",    "state": "INSTALLED", "maintenance_state": "OFF"}},
            {"ServiceInfo": {"service_name": "SOLR",   "state": "INSTALLED", "maintenance_state": "ON"}},
        ]
    }
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = AmbariServiceHealthCheck(config, {}).run()
        assert result.status == CheckResult.OK, \
            "INSTALLED client libs should not warn: {}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


def test_ambari_service_health_installed_with_filter_is_warning():
    """INSTALLED su servizio nel filtro esplicito deve essere WARNING."""
    fixture = {
        "items": [
            {"ServiceInfo": {"service_name": "HDFS", "state": "STARTED",   "maintenance_state": "OFF"}},
            {"ServiceInfo": {"service_name": "YARN", "state": "INSTALLED", "maintenance_state": "OFF"}},
        ]
    }
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
        "checks": {"service_health": {"services": ["HDFS", "YARN"]}},
    }
    try:
        result = AmbariServiceHealthCheck(config, {}).run()
        assert result.status == CheckResult.WARNING, \
            "INSTALLED in explicit filter should warn: {}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


def test_ambari_connection_error_returns_unknown():
    config = {
        "ambari_url": "http://127.0.0.1:19999",  # porta non in ascolto
        "ambari_user": "admin",
        "ambari_pass": "admin",
        "cluster_name": "test",
    }
    check  = AmbariServiceHealthCheck(config, {})
    result = check.run()
    assert result.status == CheckResult.UNKNOWN


# ---------------------------------------------------------------------------
# Test: NameNodeHACheck
# ---------------------------------------------------------------------------

def test_namenode_ha_ok():
    fixture = load_fixture("namenode_ha_ok.json")
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeHACheck(config, {}).run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
        assert "Active" in result.message
        assert result.details.get("active") == ["nn1.test"]
        assert result.details.get("standby") == ["nn2.test"]
    finally:
        server.shutdown()


def test_namenode_ha_no_active():
    fixture = load_fixture("namenode_ha_no_active.json")
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeHACheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, \
            "Both standby should be CRITICAL: {}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


def test_namenode_ha_non_ha_cluster():
    """Cluster senza HA: ha_state assente, NN started -> OK con nota non-HA."""
    fixture = {
        "host_components": [
            {"HostRoles": {"host_name": "nn1.test", "state": "STARTED", "ha_state": None}}
        ]
    }
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeHACheck(config, {}).run()
        assert result.status == CheckResult.OK, \
            "Non-HA cluster should be OK: {}: {}".format(result.status, result.message)
        assert result.details.get("ha_enabled") is False
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# Test: HdfsDataNodeCheck
# ---------------------------------------------------------------------------

def test_hdfs_datanode_ok():
    fixture = load_fixture("jmx_namenode_ok.json")
    route_map = {"/jmx": fixture}
    server, port = start_mock_server(route_map)

    config = {"webhdfs": {"url": "http://127.0.0.1:{}".format(port), "user": "hdfs"}}
    try:
        check  = HdfsDataNodeCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
        assert result.details.get("dead") == 0
    finally:
        server.shutdown()


def test_hdfs_datanode_critical():
    fixture = load_fixture("jmx_namenode_dead_dn.json")
    route_map = {"/jmx": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "webhdfs": {"url": "http://127.0.0.1:{}".format(port), "user": "hdfs"},
        "checks": {"hdfs_dead_datanodes": {"warning_threshold": 1, "critical_threshold": 3}},
    }
    try:
        check  = HdfsDataNodeCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(
            result.status, result.message)
        assert result.details.get("dead") == 3
    finally:
        server.shutdown()


def test_hdfs_datanode_no_url():
    check  = HdfsDataNodeCheck({}, {})
    result = check.run()
    assert result.status == CheckResult.UNKNOWN
    assert "not configured" in result.message


# ---------------------------------------------------------------------------
# Test: YarnNodeHealthCheck
# ---------------------------------------------------------------------------

def test_yarn_nodes_ok():
    fixture = load_fixture("yarn_nodes_ok.json")
    route_map = {"/ws/v1/cluster/nodes": fixture}
    server, port = start_mock_server(route_map)

    config = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
    try:
        check  = YarnNodeHealthCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
        assert result.details.get("running") == 3
    finally:
        server.shutdown()


def test_yarn_nodes_unhealthy():
    fixture = load_fixture("yarn_nodes_unhealthy.json")
    route_map = {"/ws/v1/cluster/nodes": fixture}
    server, port = start_mock_server(route_map)

    config = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
    try:
        check  = YarnNodeHealthCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(
            result.status, result.message)
    finally:
        server.shutdown()


def test_yarn_nodes_connection_error():
    config = {"yarn": {"rm_url": "http://127.0.0.1:19998"}}
    check  = YarnNodeHealthCheck(config, {})
    result = check.run()
    assert result.status == CheckResult.UNKNOWN


# ---------------------------------------------------------------------------
# Test: ClouderaServiceHealthCheck
# ---------------------------------------------------------------------------

def test_cloudera_service_ok():
    fixture = load_fixture("cloudera_services_ok.json")
    route_map = {"/api/v40/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "cm_url":       "http://127.0.0.1:{}".format(port),
        "cm_user":      "admin",
        "cm_pass":      "admin",
        "cluster_name": "cdp-test",
        "cm_api_version": "v40",
    }
    try:
        check  = ClouderaServiceHealthCheck(config, {})
        result = check.run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# Test: config.py manual YAML parser
# ---------------------------------------------------------------------------

def test_config_manual_parser():
    from config import _parse_yaml_manual
    yaml_text = """
version: "1"
environments:
  test:
    type: hdp
    enabled: true
    port: 8080
    url: http://localhost
    tags: [a, b, c]
checks:
  service_health:
    enabled: true
    services: [HDFS, YARN]
"""
    result = _parse_yaml_manual(yaml_text)
    assert result["version"] == "1"
    assert result["environments"]["test"]["type"] == "hdp"
    assert result["environments"]["test"]["enabled"] is True
    assert result["environments"]["test"]["port"] == 8080
    assert result["checks"]["service_health"]["enabled"] is True
    assert "HDFS" in result["checks"]["service_health"]["services"]


def test_config_load_test_yaml():
    from config import load_config
    cfg = load_config(os.path.join(
        os.path.dirname(__file__), "..", "config", "test.yaml"))
    assert "environments" in cfg
    assert "test-hdp" in cfg["environments"]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_base_can_run_no_requires,
        test_base_can_run_with_cap,
        test_base_cannot_run_missing_cap,
        test_base_or_logic,
        test_base_run_raises_not_implemented,
        test_ambari_service_health_ok,
        test_ambari_service_health_critical,
        test_ambari_service_health_installed_no_filter_is_ok,
        test_ambari_service_health_installed_with_filter_is_warning,
        test_ambari_connection_error_returns_unknown,
        test_namenode_ha_ok,
        test_namenode_ha_no_active,
        test_namenode_ha_non_ha_cluster,
        test_hdfs_datanode_ok,
        test_hdfs_datanode_critical,
        test_hdfs_datanode_no_url,
        test_yarn_nodes_ok,
        test_yarn_nodes_unhealthy,
        test_yarn_nodes_connection_error,
        test_cloudera_service_ok,
        test_config_manual_parser,
        test_config_load_test_yaml,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} — {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
