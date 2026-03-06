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
    ConfigStalenessCheck, NameNodeHACheck, NameNodeBlocksCheck,
)
from checks.webhdfs import HdfsDataNodeCheck, HdfsSpaceCheck
from checks.yarn import YarnNodeHealthCheck, YarnQueueCheck
from checks.cloudera import ClouderaServiceHealthCheck
from checks.hive import (
    _build_beeline_url, _build_beeline_cmd, _merge_ns_cfg, _zk_host_str, _label_from_cfg,
    _extract_stdout, _extract_stderr, _parse_databases_output, _parse_partition_output,
    _build_db_discovery_cmd, _build_partition_query_script,
    _build_show_tables_script, _parse_show_tables_output, _build_show_partitions_script,
    HivePartitionCheck,
)

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


def test_namenode_ha_ambari26_two_nn_warning():
    """Ambari 2.6.x: 2 NN STARTED ma ha_state null -> WARNING (indeterminato, non falso OK)."""
    fixture = {
        "host_components": [
            {"HostRoles": {"host_name": "nn1.test", "state": "STARTED", "ha_state": None}},
            {"HostRoles": {"host_name": "nn2.test", "state": "STARTED", "ha_state": None}},
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
        assert result.status == CheckResult.WARNING, \
            "2 NNs with no ha_state should be WARNING: {}: {}".format(
                result.status, result.message)
        assert "undetermined" in result.message.lower()
    finally:
        server.shutdown()


def test_namenode_ha_ambari26_ha_enabled_config_ok():
    """Ambari 2.6.x + ha_enabled: true nel config -> OK (l'utente garantisce che l'HA e' attiva)."""
    fixture = {
        "host_components": [
            {"HostRoles": {"host_name": "nn1.test", "state": "STARTED", "ha_state": None}},
            {"HostRoles": {"host_name": "nn2.test", "state": "STARTED", "ha_state": None}},
        ]
    }
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
        "checks": {"namenode_ha": {"ha_enabled": True}},
    }
    try:
        result = NameNodeHACheck(config, {}).run()
        assert result.status == CheckResult.OK, \
            "ha_enabled:true should suppress WARNING: {}: {}".format(
                result.status, result.message)
    finally:
        server.shutdown()


def test_namenode_ha_ambari26_metrics_ha_state_ok():
    """Ambari 2.6.x: ha_state null in HostRoles ma HAState in metrics -> OK (fix per 2.6.x)."""
    fixture = {
        "host_components": [
            {
                "HostRoles": {"host_name": "nn1.test", "state": "STARTED", "ha_state": None},
                "metrics": {"dfs": {"FSNamesystem": {"HAState": "active"}}},
            },
            {
                "HostRoles": {"host_name": "nn2.test", "state": "STARTED", "ha_state": None},
                "metrics": {"dfs": {"FSNamesystem": {"HAState": "standby"}}},
            },
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
            "metrics HAState should resolve HA state on Ambari 2.6.x: {}: {}".format(
                result.status, result.message)
        assert "Active" in result.message
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# Helper per fixture NameNodeBlocks
# ---------------------------------------------------------------------------

def _nn_blocks_fixture(ha_state="active", safemode="", corrupt=0, missing=0,
                       under_rep=0, name_dir_failed=None):
    # type: (str, str, int, int, int, list) -> dict
    failed_dirs = {}
    if name_dir_failed:
        for d in name_dir_failed:
            failed_dirs[d] = "UNKNOWN"
    nd_status = json.dumps({
        "active": {"/grid/01/hadoop/hdfs/namenode": "IMAGE_AND_EDITS"},
        "failed": failed_dirs,
    })
    return {
        "host_components": [{
            "HostRoles": {"host_name": "nn1.test", "state": "STARTED"},
            "metrics": {"dfs": {
                "FSNamesystem": {
                    "HAState": ha_state,
                    "CorruptBlocks": corrupt,
                    "MissingBlocks": missing,
                    "UnderReplicatedBlocks": under_rep,
                },
                "namenode": {
                    "Safemode": safemode,
                    "NameDirStatuses": nd_status,
                },
            }},
        }]
    }


# ---------------------------------------------------------------------------
# Test: NameNodeBlocksCheck
# ---------------------------------------------------------------------------

def test_namenode_blocks_ok():
    """Tutti i blocchi OK, no safemode -> OK."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture()}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
        assert "nn1" in result.message
    finally:
        server.shutdown()


def test_namenode_blocks_safemode_critical():
    """SafeMode attivo -> CRITICAL."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(
        safemode="Safe mode ON. The reported blocks 23900000 need additional 10 replication."
    )}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(result.status, result.message)
        assert "SafeMode" in result.message
    finally:
        server.shutdown()


def test_namenode_blocks_corrupt_critical():
    """CorruptBlocks > 0 -> CRITICAL."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(corrupt=3)}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(result.status, result.message)
        assert "corrupt" in result.message.lower()
        assert result.details.get("corrupt_blocks") == 3
    finally:
        server.shutdown()


def test_namenode_blocks_missing_critical():
    """MissingBlocks > 0 -> CRITICAL (data loss)."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(missing=2)}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(result.status, result.message)
        assert "MISSING" in result.message
        assert result.details.get("missing_blocks") == 2
    finally:
        server.shutdown()


def test_namenode_blocks_namedir_failed_critical():
    """NameDir fallita -> CRITICAL."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(
        name_dir_failed=["/grid/02/hadoop/hdfs/namenode"]
    )}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, "{}: {}".format(result.status, result.message)
        assert "NameDir" in result.message
        assert len(result.details.get("name_dir_failed", [])) == 1
    finally:
        server.shutdown()


def test_namenode_blocks_under_replicated_warning():
    """UnderReplicatedBlocks > soglia -> WARNING."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(under_rep=500)}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
        "checks": {"namenode_blocks": {"under_replicated_warning": 100}},
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.WARNING, "{}: {}".format(result.status, result.message)
        assert "under-replicated" in result.message.lower()
    finally:
        server.shutdown()


def test_namenode_blocks_under_replicated_ok_below_threshold():
    """UnderReplicatedBlocks <= soglia -> OK."""
    route_map = {"/api/v1/clusters/": _nn_blocks_fixture(under_rep=50)}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
        "checks": {"namenode_blocks": {"under_replicated_warning": 100}},
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
    finally:
        server.shutdown()


def test_namenode_blocks_metrics_unavailable():
    """Metrics assenti (AMS non risponde) -> UNKNOWN."""
    fixture = {
        "host_components": [{
            "HostRoles": {"host_name": "nn1.test", "state": "STARTED"},
            # metrics assente
        }]
    }
    route_map = {"/api/v1/clusters/": fixture}
    server, port = start_mock_server(route_map)
    config = {
        "ambari_url": "http://127.0.0.1:{}".format(port),
        "ambari_user": "admin", "ambari_pass": "admin",
        "cluster_name": "test-cluster",
    }
    try:
        result = NameNodeBlocksCheck(config, {}).run()
        assert result.status == CheckResult.UNKNOWN, "{}: {}".format(result.status, result.message)
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


def test_hdfs_space_no_paths_jmx_unreachable():
    """HdfsSpace senza paths: tenta JMX per capacità globale.
    Se il NameNode non è raggiungibile restituisce UNKNOWN (non SKIPPED),
    perché il refactor f359485 fa sempre il check JMX, paths è opzionale.
    """
    config = {
        "webhdfs": {"url": "http://127.0.0.1:19997", "user": "hdfs"},
        # checks.hdfs_space.paths assente — check ritorna solo risultato JMX
    }
    result = HdfsSpaceCheck(config, {}).run()
    assert result.status == CheckResult.UNKNOWN, \
        "JMX unreachable should give UNKNOWN: {}: {}".format(result.status, result.message)
    assert "JMX error" in result.message or "jmx" in result.message.lower()


# ---------------------------------------------------------------------------
# Test: namenode_urls HA fallback
# ---------------------------------------------------------------------------

def test_hdfs_datanode_namenode_urls_first_ok():
    """namenode_urls: il primo NN risponde → OK."""
    fixture = load_fixture("jmx_namenode_ok.json")
    route_map = {"/jmx": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "webhdfs": {
            "url":           "http://127.0.0.1:14000",  # HttpFS — non ha JMX
            "namenode_urls": [
                "http://127.0.0.1:{}".format(port),      # NN1 attivo
                "http://127.0.0.1:19996",                 # NN2 non raggiungibile
            ],
            "user": "hdfs",
        }
    }
    try:
        result = HdfsDataNodeCheck(config, {}).run()
        assert result.status == CheckResult.OK, "{}: {}".format(result.status, result.message)
        assert result.details.get("dead") == 0
    finally:
        server.shutdown()


def test_hdfs_datanode_namenode_urls_fallback_second():
    """namenode_urls: il primo NN non risponde → fallback al secondo."""
    fixture = load_fixture("jmx_namenode_ok.json")
    route_map = {"/jmx": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "webhdfs": {
            "url":           "http://127.0.0.1:14000",  # HttpFS — non ha JMX
            "namenode_urls": [
                "http://127.0.0.1:19996",               # NN1 non raggiungibile
                "http://127.0.0.1:{}".format(port),     # NN2 attivo
            ],
            "user": "hdfs",
        }
    }
    try:
        result = HdfsDataNodeCheck(config, {}).run()
        assert result.status == CheckResult.OK, \
            "Expected OK with fallback NN, got {}: {}".format(result.status, result.message)
        assert result.details.get("dead") == 0
    finally:
        server.shutdown()


def test_hdfs_datanode_namenode_urls_both_down():
    """namenode_urls: entrambi i NN non raggiungibili → UNKNOWN con messaggio errore."""
    config = {
        "webhdfs": {
            "url":           "http://127.0.0.1:14000",
            "namenode_urls": [
                "http://127.0.0.1:19994",
                "http://127.0.0.1:19995",
            ],
            "user": "hdfs",
        }
    }
    result = HdfsDataNodeCheck(config, {}).run()
    assert result.status == CheckResult.UNKNOWN, "{}: {}".format(result.status, result.message)
    assert "2 NN" in result.message or "JMX error" in result.message


def test_hdfs_space_namenode_urls_fallback():
    """HdfsSpace: namenode_urls fallback al secondo NN funzionante."""
    fixture = load_fixture("jmx_namenode_ok.json")
    route_map = {"/jmx": fixture}
    server, port = start_mock_server(route_map)

    config = {
        "webhdfs": {
            "url":           "http://127.0.0.1:14000",  # HttpFS — non ha JMX
            "namenode_urls": [
                "http://127.0.0.1:19993",               # NN1 non raggiungibile
                "http://127.0.0.1:{}".format(port),     # NN2 attivo
            ],
            "user": "hdfs",
        }
    }
    try:
        result = HdfsSpaceCheck(config, {}).run()
        assert result.status in (CheckResult.OK, CheckResult.WARNING, CheckResult.CRITICAL), \
            "Expected capacity check result, got {}: {}".format(result.status, result.message)
        assert "HDFS used" in result.message
        assert result.details.get("namenode_url_used") is not None
    finally:
        server.shutdown()


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


def test_yarn_connection_error_suggests_rm_url():
    """Quando URL è auto-costruito e la connessione fallisce, il messaggio suggerisce yarn.rm_url."""
    config = {
        "ambari_url": "http://127.0.0.1:8080",
        # yarn.rm_url NON configurato -> verrà auto-costruito
    }
    result = YarnNodeHealthCheck(config, {}).run()
    assert result.status == CheckResult.UNKNOWN
    assert "yarn.rm_url" in result.message, \
        "Auto-detected URL error should suggest yarn.rm_url: {}".format(result.message)


def test_yarn_cdp_lost_nodes_autodetect_from_cm():
    """CDP: nodi LOST corrispondenti a CM-DECOMMISSIONED sono trattati come decommissionati.

    Due server mock:
      - YARN RM: /ws/v1/cluster/nodes → 2 RUNNING + 2 LOST
      - CM API: /api/v40/hosts → worker03 DECOMMISSIONED, worker04 DECOMMISSIONED
    Risultato atteso: OK — 2/4 RUNNING (2 decommissioned), nessun CRITICAL.
    """
    from checks.yarn import _cm_decommissioned_hosts

    yarn_fixture = load_fixture("yarn_nodes_lost_decom.json")
    cm_hosts_fixture = {
        "items": [
            {"hostname": "worker01", "commissionState": "COMMISSIONED"},
            {"hostname": "worker02", "commissionState": "COMMISSIONED"},
            {"hostname": "worker03", "commissionState": "DECOMMISSIONED"},
            {"hostname": "worker04", "commissionState": "DECOMMISSIONED"},
        ]
    }
    # Serve entrambi gli endpoint sullo stesso server mock
    route_map = {
        "/ws/v1/cluster/nodes": yarn_fixture,
        "/api/v40/hosts":       cm_hosts_fixture,
    }
    server, port = start_mock_server(route_map)
    base = "http://127.0.0.1:{}".format(port)
    config = {
        "yarn":   {"rm_url": base},
        "cm_url": base,
        "cm_user": "admin",
        "cm_pass": "admin",
    }
    try:
        result = YarnNodeHealthCheck(config, {}).run()
        assert result.status == CheckResult.OK, \
            "Expected OK with CM auto-detect, got {}: {}".format(
                result.status, result.message)
        assert result.details.get("decommissioned") == 2
        assert "decommissioned" in result.message
    finally:
        server.shutdown()


def test_yarn_cdp_lost_nodes_without_cm_is_critical():
    """Senza cm_url, i nodi LOST che non sono in decommissioned_nodes restano CRITICAL."""
    yarn_fixture = load_fixture("yarn_nodes_lost_decom.json")
    route_map = {"/ws/v1/cluster/nodes": yarn_fixture}
    server, port = start_mock_server(route_map)
    config = {"yarn": {"rm_url": "http://127.0.0.1:{}".format(port)}}
    try:
        result = YarnNodeHealthCheck(config, {}).run()
        assert result.status == CheckResult.CRITICAL, \
            "Expected CRITICAL without CM config, got {}: {}".format(
                result.status, result.message)
    finally:
        server.shutdown()


def test_cm_decommissioned_hosts_no_cm_url():
    """Senza cm_url nel config, _cm_decommissioned_hosts restituisce set vuoto."""
    from checks.yarn import _cm_decommissioned_hosts
    assert _cm_decommissioned_hosts({}) == set()
    assert _cm_decommissioned_hosts({"yarn": {"rm_url": "http://x"}}) == set()


def test_cm_decommissioned_hosts_cm_unreachable():
    """CM non raggiungibile → set vuoto (fallback silenzioso)."""
    from checks.yarn import _cm_decommissioned_hosts
    result = _cm_decommissioned_hosts(
        {"cm_url": "http://127.0.0.1:19997", "cm_user": "u", "cm_pass": "p"}
    )
    assert result == set()


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
# HiveCheck — _build_beeline_url / _build_beeline_cmd
# ---------------------------------------------------------------------------

def test_beeline_url_direct():
    cfg = {"host": "hiveserver2.example.com", "port": 10000, "database": "default"}
    url = _build_beeline_url(cfg)
    assert url == "jdbc:hive2://hiveserver2.example.com:10000/default", url


def test_beeline_url_direct_defaults():
    url = _build_beeline_url({})
    assert url == "jdbc:hive2://localhost:10000/default", url


def test_beeline_url_zk_list_no_namespace():
    cfg = {"zookeeper_hosts": ["zk1:2181", "zk2:2181", "zk3:2181"]}
    url = _build_beeline_url(cfg)
    assert url == "jdbc:hive2://zk1:2181,zk2:2181,zk3:2181/", url
    assert "serviceDiscovery" not in url


def test_beeline_url_zk_list_with_namespace():
    cfg = {
        "zookeeper_hosts": ["zk1:2181", "zk2:2181"],
        "zookeeper_namespace": "hiveserver2ldap",
    }
    url = _build_beeline_url(cfg)
    assert url == (
        "jdbc:hive2://zk1:2181,zk2:2181/"
        ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2ldap"
    ), url


def test_beeline_url_zk_string_single():
    cfg = {"zookeeper_hosts": "zk1:2181"}
    url = _build_beeline_url(cfg)
    assert url == "jdbc:hive2://zk1:2181/", url


def test_beeline_cmd_no_auth():
    cfg = {"host": "hs2.example.com", "port": 10000, "database": "default", "user": "hive"}
    cmd = _build_beeline_cmd(cfg, "hadoop")
    assert 'beeline -u "jdbc:hive2://hs2.example.com:10000/default"' in cmd
    assert "-n 'hive'" in cmd
    assert "-p" not in cmd
    assert "-e 'SELECT 1;'" in cmd


def test_beeline_cmd_with_ldap_password():
    cfg = {"host": "hs2.example.com", "port": 10000, "user": "svcuser", "password": "s3cr3t"}
    cmd = _build_beeline_cmd(cfg, "hadoop")
    assert "-n 'svcuser'" in cmd
    assert "-p 's3cr3t'" in cmd


def test_beeline_cmd_zk_with_namespace_no_auth():
    cfg = {
        "zookeeper_hosts": ["hdmasep001:2181", "hdmasep002:2181", "hdmasep003:2181"],
        "zookeeper_namespace": "hiveserver2ldap",
        "user": "hive",
    }
    cmd = _build_beeline_cmd(cfg, "hadoop")
    assert 'beeline -u "jdbc:hive2://hdmasep001:2181,hdmasep002:2181,hdmasep003:2181/' in cmd
    assert "serviceDiscoveryMode=zooKeeper" in cmd
    assert "zooKeeperNamespace=hiveserver2ldap" in cmd
    assert "-n 'hive'" in cmd
    assert "-p" not in cmd


def test_beeline_cmd_default_user_fallback():
    cfg = {"host": "hs2.example.com", "port": 10000}
    cmd = _build_beeline_cmd(cfg, "sshdefault")
    assert "-n 'sshdefault'" in cmd


def test_merge_ns_cfg_inherits_zk_hosts_and_user():
    parent = {
        "zookeeper_hosts": ["zk1:2181", "zk2:2181"],
        "user": "hive",
    }
    ns = {"name": "hiveserver2"}
    merged = _merge_ns_cfg(parent, ns)
    assert merged["zookeeper_hosts"] == ["zk1:2181", "zk2:2181"]
    assert merged["zookeeper_namespace"] == "hiveserver2"
    assert merged["user"] == "hive"
    assert "password" not in merged
    assert "namespaces" not in merged


def test_merge_ns_cfg_password_not_inherited():
    parent = {
        "zookeeper_hosts": ["zk1:2181"],
        "user": "hive",
        "password": "parent_secret",
    }
    ns = {"name": "hiveserver2"}
    merged = _merge_ns_cfg(parent, ns)
    # password from parent must NOT be inherited
    assert "password" not in merged


def test_merge_ns_cfg_ns_overrides_user_and_password():
    parent = {
        "zookeeper_hosts": ["zk1:2181"],
        "user": "hive",
    }
    ns = {"name": "hiveserver2ldap", "user": "svcaccount", "password": "ldap_pass"}
    merged = _merge_ns_cfg(parent, ns)
    assert merged["zookeeper_namespace"] == "hiveserver2ldap"
    assert merged["user"] == "svcaccount"
    assert merged["password"] == "ldap_pass"


def test_merge_ns_cfg_url_with_namespace():
    parent = {"zookeeper_hosts": ["zk1:2181", "zk2:2181"], "user": "hive"}
    ns = {"name": "hiveserver2ldap", "password": "pass"}
    merged = _merge_ns_cfg(parent, ns)
    url = _build_beeline_url(merged)
    assert "serviceDiscoveryMode=zooKeeper" in url
    assert "zooKeeperNamespace=hiveserver2ldap" in url
    cmd = _build_beeline_cmd(merged, "hive")
    assert "-p 'pass'" in cmd


def test_zk_host_str_from_string():
    assert _zk_host_str("zk1:2181") == "zk1:2181"


def test_zk_host_str_from_dict():
    # Defensive: manual YAML parser may return {'host': port} for 'host:port'
    assert _zk_host_str({"zk1": 2181}) == "zk1:2181"


def test_beeline_url_zk_dict_items():
    # Simulate what the manual YAML parser produces for unquoted host:port items
    cfg = {"zookeeper_hosts": [
        {"hdmasep001.example.com": 2181},
        {"hdmasep002.example.com": 2181},
    ]}
    url = _build_beeline_url(cfg)
    assert url == "jdbc:hive2://hdmasep001.example.com:2181,hdmasep002.example.com:2181/", url


def test_beeline_cmd_custom_path():
    cfg = {"host": "hs2", "port": 10000, "beeline_path": "/opt/hive/bin/beeline"}
    cmd = _build_beeline_cmd(cfg, "hive")
    assert cmd.startswith("/opt/hive/bin/beeline"), cmd


def test_beeline_url_verbatim_jdbc_url():
    """jdbc_url is returned unchanged — bypasses all other config."""
    raw = (
        "jdbc:hive2://lb.example.com:10000/;"
        "ssl=true;sslTrustStore=/etc/ssl/hs2.jks;trustStorePassword=s3cr3t;"
        "principal=hive/lb.example.com@REALM.COM"
    )
    cfg = {"jdbc_url": raw, "host": "ignored", "zookeeper_hosts": ["ignored:2181"]}
    assert _build_beeline_url(cfg) == raw


def test_beeline_url_ssl_structured():
    """SSL params are appended to direct-mode URL."""
    cfg = {
        "host": "hs2.example.com",
        "port": 10000,
        "ssl": {
            "enabled": True,
            "truststore": "/etc/ssl/hs2.jks",
            "truststore_password": "s3cr3t",
        },
    }
    url = _build_beeline_url(cfg)
    assert "ssl=true" in url, url
    assert "sslTrustStore=/etc/ssl/hs2.jks" in url, url
    assert "trustStorePassword=s3cr3t" in url, url


def test_beeline_url_ssl_with_kerberos_principal():
    """kerberos_principal appended as JDBC property."""
    cfg = {
        "host": "hs2.example.com",
        "port": 10000,
        "ssl": {"enabled": True, "truststore": "/etc/ssl/hs2.jks", "truststore_password": "pw"},
        "kerberos_principal": "hive/hs2.example.com@REALM.COM",
    }
    url = _build_beeline_url(cfg)
    assert "principal=hive/hs2.example.com@REALM.COM" in url, url


def test_beeline_url_ssl_zk_mode():
    """SSL params also append to ZooKeeper-mode URL."""
    cfg = {
        "zookeeper_hosts": ["zk1:2181", "zk2:2181"],
        "zookeeper_namespace": "hiveserver2",
        "ssl": {"enabled": True, "truststore": "/etc/ssl/hs2.jks", "truststore_password": "pw"},
        "kerberos_principal": "hive/_HOST@REALM",
    }
    url = _build_beeline_url(cfg)
    assert "serviceDiscoveryMode=zooKeeper" in url, url
    assert "ssl=true" in url, url
    assert "principal=hive/_HOST@REALM" in url, url


def test_beeline_url_ssl_disabled_not_appended():
    """ssl.enabled=False means no SSL params in URL."""
    cfg = {
        "host": "hs2.example.com",
        "port": 10000,
        "ssl": {"enabled": False, "truststore": "/etc/ssl/hs2.jks"},
    }
    url = _build_beeline_url(cfg)
    assert "ssl" not in url, url


def test_merge_ns_cfg_jdbc_url_override():
    """Namespace entry can override jdbc_url from parent."""
    parent = {
        "jdbc_url": "jdbc:hive2://lb1:10000/;ssl=true",
        "user": "svc",
    }
    ns = {"name": "ns1", "jdbc_url": "jdbc:hive2://lb2:10000/;ssl=true"}
    merged = _merge_ns_cfg(parent, ns)
    assert merged["jdbc_url"] == "jdbc:hive2://lb2:10000/;ssl=true"


def test_merge_ns_cfg_jdbc_url_inherited():
    """Namespace entry inherits jdbc_url from parent if not overridden."""
    parent = {"jdbc_url": "jdbc:hive2://lb:10000/;ssl=true", "user": "svc"}
    ns = {"name": "ns1"}
    merged = _merge_ns_cfg(parent, ns)
    assert merged["jdbc_url"] == "jdbc:hive2://lb:10000/;ssl=true"


def test_label_from_cfg_zk_namespace():
    cfg = {"zookeeper_namespace": "hiveserver2ldap"}
    assert _label_from_cfg(cfg) == "hiveserver2ldap"


def test_label_from_cfg_jdbc_url():
    cfg = {"jdbc_url": "jdbc:hive2://lb.example.com:10000/;ssl=true"}
    assert _label_from_cfg(cfg) == "lb.example.com:10000"


def test_label_from_cfg_direct():
    cfg = {"host": "hs2.example.com", "port": 10001}
    assert _label_from_cfg(cfg) == "hs2.example.com:10001"


def test_yaml_parser_hostport_as_string():
    from config import _parse_yaml_manual
    yaml_text = (
        "zookeeper_hosts:\n"
        "  - hdmasep001.example.com:2181\n"
        "  - hdmasep002.example.com:2181\n"
    )
    result = _parse_yaml_manual(yaml_text)
    assert result["zookeeper_hosts"] == [
        "hdmasep001.example.com:2181",
        "hdmasep002.example.com:2181",
    ], result["zookeeper_hosts"]


def test_extract_task_error_parses_ansible_json():
    import json
    from checks.hive import _extract_task_error
    task_result = {
        "changed": False,
        "cmd": 'beeline -u "jdbc:hive2://zk1:2181/"',
        "msg": "non-zero return code",
        "rc": 127,
        "stderr": "/bin/sh: beeline: command not found",
        "stdout": "",
    }
    ansible_out = (
        "PLAY [HiveCheck] ****\n\n"
        "TASK [Beeline test] ****\n"
        "fatal: [localhost]: FAILED! => {}\n\n"
        "PLAY RECAP ****\n"
        "localhost : ok=0 changed=0 failed=1\n"
    ).format(json.dumps(task_result))
    result = _extract_task_error(ansible_out)
    assert "non-zero return code" in result
    assert "beeline: command not found" in result
    assert "PLAY RECAP" not in result


# ---------------------------------------------------------------------------
# HivePartitionCheck — parsing helpers
# ---------------------------------------------------------------------------

def test_extract_stdout_from_ansible_output():
    ansible_out = (
        'TASK [debug] ****\n'
        'ok: [host] => {\n'
        '    "r.stdout": "database_name\\nmydb\\nprod_dw\\n"\n'
        '}\n'
    )
    result = _extract_stdout(ansible_out)
    assert result == "database_name\nmydb\nprod_dw\n", repr(result)


def test_extract_stdout_missing():
    result = _extract_stdout("no stdout here")
    assert result == ""


def test_extract_stderr_from_ansible_output():
    ansible_out = (
        'TASK [debug] ****\n'
        'ok: [host] => {\n'
        '    "r.stderr": "=== SQL FILE [mydb] ===\\nSELECT \'###TAB:t1###\';\\n=== END SQL FILE ===\\n"\n'
        '}\n'
    )
    result = _extract_stderr(ansible_out)
    assert "SQL FILE [mydb]" in result, repr(result)
    assert "SELECT" in result, repr(result)


def test_extract_stderr_missing():
    result = _extract_stderr("no stderr here")
    assert result == ""


def test_parse_databases_output_normal():
    raw = "database_name\ndefault\nmydb\nprod_dw\n"
    dbs = _parse_databases_output(raw)
    assert dbs == ["default", "mydb", "prod_dw"], dbs


def test_parse_databases_output_empty():
    dbs = _parse_databases_output("")
    assert dbs == []


def test_parse_databases_output_no_header():
    raw = "default\nmydb\n"
    dbs = _parse_databases_output(raw)
    assert "default" in dbs
    assert "mydb" in dbs


def test_parse_partition_output_single_db():
    raw = (
        "###DB:mydb###\n"
        "table_name\tcnt\n"
        "sales_fact\t12450\n"
        "events_log\t800\n"
    )
    result = _parse_partition_output(raw)
    assert "mydb" in result
    assert result["mydb"]["sales_fact"] == 12450
    assert result["mydb"]["events_log"] == 800


def test_parse_partition_output_multi_db():
    raw = (
        "###DB:db1###\n"
        "table_name\tcnt\n"
        "tbl_a\t100\n"
        "###DB:db2###\n"
        "table_name\tcnt\n"
        "tbl_b\t200\n"
        "tbl_c\t300\n"
    )
    result = _parse_partition_output(raw)
    assert result["db1"] == {"tbl_a": 100}
    assert result["db2"] == {"tbl_b": 200, "tbl_c": 300}


def test_parse_partition_output_empty():
    result = _parse_partition_output("")
    assert result == {}


def test_parse_partition_output_skips_header():
    raw = "###DB:mydb###\ntable_name\tcnt\nonly_table\t42\n"
    result = _parse_partition_output(raw)
    assert "table_name" not in result.get("mydb", {})
    assert result["mydb"]["only_table"] == 42


def test_build_db_discovery_cmd_contains_show_databases():
    cfg = {"host": "hs2.example.com", "port": 10000, "user": "hive"}
    cmd = _build_db_discovery_cmd(cfg, "hadoop")
    assert "SHOW DATABASES" in cmd
    assert "--outputformat=tsv2" in cmd
    assert "--silent=true" in cmd


def test_build_partition_query_script_markers():
    cfg = {"host": "hs2.example.com", "port": 10000, "user": "hive"}
    script = _build_partition_query_script(cfg, ["db1", "db2"], "hadoop")
    assert "###DB:db1###" in script
    assert "###DB:db2###" in script
    assert "SHOW TABLES IN db1" in script
    assert "SHOW TABLES IN db2" in script
    assert "SHOW PARTITIONS db1" in script
    assert "SHOW PARTITIONS db2" in script
    assert "###TAB:" in script


def test_build_partition_query_script_uses_tmpfile():
    cfg = {"host": "hs2.example.com", "port": 10000}
    script = _build_partition_query_script(cfg, ["mydb"], "hadoop")
    assert "mktemp" in script
    assert "-f " in script   # beeline -f <tmpfile>
    assert "rm -f" in script


def test_parse_partition_output_tab_format_new():
    # Formato A: ###TAB: markers + partition spec lines
    raw = (
        "###DB:mydb###\n"
        "_c0\n"
        "###TAB:sales_fact###\n"
        "partition\n"
        "dt=20260101/region=IT\n"
        "dt=20260102/region=IT\n"
        "dt=20260103/region=IT\n"
        "_c0\n"
        "###TAB:events_log###\n"
        "partition\n"
        "dt=20260101/region=IT\n"
    )
    result = _parse_partition_output(raw)
    assert result["mydb"]["sales_fact"] == 3
    assert result["mydb"]["events_log"] == 1


def test_hive_partition_check_no_edge_host():
    cfg = {"hive": {"host": "hs2.example.com"}, "checks": {}}
    caps = {"ansible": True}
    check = HivePartitionCheck(cfg, caps)
    result = check.run()
    assert result.status == CheckResult.UNKNOWN
    assert "edge_host" in result.message


def test_hive_partition_threshold_logic():
    raw = (
        "###DB:mydb###\n"
        "table_name\tcnt\n"
        "big_table\t9999\n"
        "small_table\t100\n"
    )
    result_data = _parse_partition_output(raw)
    max_parts = 5000
    over = []
    for db, tables in result_data.items():
        for tbl, cnt in tables.items():
            if cnt > max_parts:
                over.append("{}.{}".format(db, tbl))
    assert "mydb.big_table" in over
    assert "mydb.small_table" not in over


def test_hive_partition_no_threshold():
    raw = (
        "###DB:mydb###\n"
        "table_name\tcnt\n"
        "tbl\t100\n"
    )
    result_data = _parse_partition_output(raw)
    max_parts = 0  # disabled
    over = [
        tbl for db, tables in result_data.items()
        for tbl, cnt in tables.items()
        if max_parts > 0 and cnt > max_parts
    ]
    assert over == []


def test_hive_partition_default_timeout():
    """HivePartitionCheck usa timeout 300s di default se non configurato."""
    part_cfg = {}
    play_timeout = int(part_cfg.get("timeout", 300))
    assert play_timeout == 300


def test_hive_partition_custom_timeout():
    """HivePartitionCheck rispetta timeout configurato via checks.hive_partitions.timeout."""
    part_cfg = {"timeout": 600}
    play_timeout = int(part_cfg.get("timeout", 300))
    assert play_timeout == 600


# ---------------------------------------------------------------------------
# _build_show_tables_script + _parse_show_tables_output + _build_show_partitions_script
# ---------------------------------------------------------------------------

def test_build_show_tables_script_one_beeline_call():
    """Una sola invocazione beeline per tutti i DB (non N)."""
    cfg = {"host": "hs2.example.com", "port": 10000, "user": "hive"}
    script = _build_show_tables_script(cfg, ["db1", "db2", "db3"], "hadoop")
    # Deve contenere i marker per tutti e 3 i DB
    assert "###DB:db1###" in script
    assert "###DB:db2###" in script
    assert "###DB:db3###" in script
    # SHOW TABLES per ogni DB
    assert "SHOW TABLES IN db1" in script
    assert "SHOW TABLES IN db2" in script
    assert "SHOW TABLES IN db3" in script
    # tmpfile creato e rimosso
    assert "mktemp" in script
    assert "rm -f" in script
    # Una sola chiamata beeline -f (non N chiamate nel loop)
    assert "--force -f" in script
    assert script.count("--force -f") == 1


def test_build_show_tables_script_first_db_creates_file():
    """Il primo DB usa > (crea il file), i successivi >>."""
    cfg = {"host": "hs2.example.com", "port": 10000}
    script = _build_show_tables_script(cfg, ["db1", "db2"], "hadoop")
    lines = script.splitlines()
    # La prima printf deve usare >, le successive >>
    first_printf = next(l for l in lines if "###DB:db1###" in l)
    assert '> "$_HS_F"' in first_printf or "> \"$_HS_F\"" in first_printf
    second_printf = next(l for l in lines if "###DB:db2###" in l)
    assert '>>' in second_printf


def test_parse_show_tables_output_normal():
    """Parsing corretto di output con due DB e tabelle."""
    raw = (
        "_c0\n"
        "###DB:db1###\n"
        "tab_name\n"
        "tbl1\n"
        "tbl2\n"
        "_c0\n"
        "###DB:db2###\n"
        "tab_name\n"
        "tbl3\n"
    )
    result = _parse_show_tables_output(raw)
    assert result == {"db1": ["tbl1", "tbl2"], "db2": ["tbl3"]}, result


def test_parse_show_tables_output_empty():
    result = _parse_show_tables_output("")
    assert result == {}


def test_parse_show_tables_output_skips_headers():
    """_c0 e tab_name vengono saltati."""
    raw = "_c0\n###DB:mydb###\ntab_name\ntbl1\n"
    result = _parse_show_tables_output(raw)
    assert result == {"mydb": ["tbl1"]}
    assert "_c0" not in result.get("mydb", [])
    assert "tab_name" not in result.get("mydb", [])


def test_parse_show_tables_output_empty_db():
    """DB senza tabelle restituisce lista vuota."""
    raw = "_c0\n###DB:emptydb###\ntab_name\n"
    result = _parse_show_tables_output(raw)
    assert result == {"emptydb": []}


def test_build_show_partitions_script_one_beeline_call():
    """Una sola invocazione beeline --force -f per tutte le tabelle di tutti i DB."""
    cfg = {"host": "hs2.example.com", "port": 10000, "user": "hive"}
    db_tables = {"db1": ["tbl_a", "tbl_b"], "db2": ["tbl_c"]}
    script = _build_show_partitions_script(cfg, db_tables, "hadoop")
    # Marker DB
    assert "###DB:db1###" in script
    assert "###DB:db2###" in script
    # Marker TAB
    assert "###TAB:tbl_a###" in script
    assert "###TAB:tbl_b###" in script
    assert "###TAB:tbl_c###" in script
    # SHOW PARTITIONS per ogni tabella
    assert "SHOW PARTITIONS db1.tbl_a" in script
    assert "SHOW PARTITIONS db1.tbl_b" in script
    assert "SHOW PARTITIONS db2.tbl_c" in script
    # Una sola chiamata beeline --force -f
    assert "--force -f" in script
    assert script.count("--force -f") == 1
    # Debug SQL file su stderr
    assert "=== SQL FILE ===" in script
    assert "cat \"$_HS_F\" >&2" in script or 'cat "$_HS_F" >&2' in script


def test_build_show_partitions_script_sorted_output():
    """DB e tabelle sono ordinati alfabeticamente per output deterministico."""
    cfg = {"host": "hs2.example.com", "port": 10000}
    db_tables = {"zdb": ["z_tbl", "a_tbl"], "adb": ["m_tbl"]}
    script = _build_show_partitions_script(cfg, db_tables, "hadoop")
    # adb deve venire prima di zdb
    pos_adb = script.index("###DB:adb###")
    pos_zdb = script.index("###DB:zdb###")
    assert pos_adb < pos_zdb
    # a_tbl deve venire prima di z_tbl
    pos_atbl = script.index("###TAB:a_tbl###")
    pos_ztbl = script.index("###TAB:z_tbl###")
    assert pos_atbl < pos_ztbl


def test_parse_partition_output_compatible_with_show_partitions_script():
    """_parse_partition_output legge correttamente l'output del nuovo script."""
    # Simula output beeline tsv2 da _build_show_partitions_script:
    # SELECT '###DB:db###' -> _c0 / ###DB:db###
    # SELECT '###TAB:tbl###' -> _c0 / ###TAB:tbl###
    # SHOW PARTITIONS -> partition / <spec>
    raw = (
        "_c0\n"
        "###DB:mydb###\n"
        "_c0\n"
        "###TAB:sales###\n"
        "partition\n"
        "dt=20260101/region=IT\n"
        "dt=20260102/region=IT\n"
        "_c0\n"
        "###TAB:events###\n"
        "partition\n"
        "dt=20260101/region=IT\n"
    )
    result = _parse_partition_output(raw)
    assert result["mydb"]["sales"] == 2
    assert result["mydb"]["events"] == 1


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
        test_namenode_ha_ambari26_two_nn_warning,
        test_namenode_ha_ambari26_ha_enabled_config_ok,
        test_namenode_ha_ambari26_metrics_ha_state_ok,
        test_namenode_blocks_ok,
        test_namenode_blocks_safemode_critical,
        test_namenode_blocks_corrupt_critical,
        test_namenode_blocks_missing_critical,
        test_namenode_blocks_namedir_failed_critical,
        test_namenode_blocks_under_replicated_warning,
        test_namenode_blocks_under_replicated_ok_below_threshold,
        test_namenode_blocks_metrics_unavailable,
        test_hdfs_datanode_ok,
        test_hdfs_datanode_critical,
        test_hdfs_datanode_no_url,
        test_hdfs_space_no_paths_jmx_unreachable,
        test_yarn_nodes_ok,
        test_yarn_nodes_unhealthy,
        test_yarn_nodes_connection_error,
        test_yarn_connection_error_suggests_rm_url,
        test_cloudera_service_ok,
        test_config_manual_parser,
        test_config_load_test_yaml,
        test_beeline_url_direct,
        test_beeline_url_direct_defaults,
        test_beeline_url_zk_list_no_namespace,
        test_beeline_url_zk_list_with_namespace,
        test_beeline_url_zk_string_single,
        test_beeline_cmd_no_auth,
        test_beeline_cmd_with_ldap_password,
        test_beeline_cmd_zk_with_namespace_no_auth,
        test_beeline_cmd_default_user_fallback,
        test_merge_ns_cfg_inherits_zk_hosts_and_user,
        test_merge_ns_cfg_password_not_inherited,
        test_merge_ns_cfg_ns_overrides_user_and_password,
        test_merge_ns_cfg_url_with_namespace,
        test_zk_host_str_from_string,
        test_zk_host_str_from_dict,
        test_beeline_url_zk_dict_items,
        test_beeline_cmd_custom_path,
        test_yaml_parser_hostport_as_string,
        test_extract_task_error_parses_ansible_json,
        test_extract_stdout_from_ansible_output,
        test_extract_stdout_missing,
        test_extract_stderr_from_ansible_output,
        test_extract_stderr_missing,
        test_parse_databases_output_normal,
        test_parse_databases_output_empty,
        test_parse_databases_output_no_header,
        test_parse_partition_output_single_db,
        test_parse_partition_output_multi_db,
        test_parse_partition_output_empty,
        test_parse_partition_output_skips_header,
        test_build_db_discovery_cmd_contains_show_databases,
        test_build_partition_query_script_markers,
        test_build_partition_query_script_uses_tmpfile,
        test_parse_partition_output_tab_format_new,
        test_hive_partition_check_no_edge_host,
        test_hive_partition_threshold_logic,
        test_hive_partition_no_threshold,
        test_hive_partition_default_timeout,
        test_hive_partition_custom_timeout,
        test_build_show_tables_script_one_beeline_call,
        test_build_show_tables_script_first_db_creates_file,
        test_parse_show_tables_output_normal,
        test_parse_show_tables_output_empty,
        test_parse_show_tables_output_skips_headers,
        test_parse_show_tables_output_empty_db,
        test_build_show_partitions_script_one_beeline_call,
        test_build_show_partitions_script_sorted_output,
        test_parse_partition_output_compatible_with_show_partitions_script,
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
