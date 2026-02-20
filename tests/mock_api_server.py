#!/usr/bin/env python3
"""
Mock API server per test di integrazione HadoopScope.
Serve risposte JSON che simulano Ambari, WebHDFS, YARN RM e Cloudera Manager.

Usage:
  python3 tests/mock_api_server.py [--port 8080] [--scenario ok|critical]
"""

from __future__ import print_function

import argparse
import json
import os
import sys

try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

SCENARIOS = {
    "ok": {
        # Ambari
        "/api/v1/clusters/test-cluster/services": {
            "items": [
                {"ServiceInfo": {"service_name": "HDFS",      "state": "STARTED"}},
                {"ServiceInfo": {"service_name": "YARN",      "state": "STARTED"}},
                {"ServiceInfo": {"service_name": "HIVE",      "state": "STARTED"}},
                {"ServiceInfo": {"service_name": "ZOOKEEPER", "state": "STARTED"}},
            ]
        },
        "/api/v1/clusters/test-cluster/services/HDFS/components/NAMENODE": {
            "host_components": [
                {"HostRoles": {"host_name": "nn1.test"},
                 "metrics": {"dfs": {"FSNamesystem": {"HAState": "active"}}}},
                {"HostRoles": {"host_name": "nn2.test"},
                 "metrics": {"dfs": {"FSNamesystem": {"HAState": "standby"}}}},
            ]
        },
        "/api/v1/clusters/test-cluster/alerts": {"items": []},
        # JMX NameNode
        "/jmx": {
            "beans": [{
                "name": "Hadoop:service=NameNode,name=FSNamesystemState",
                "NumLiveDataNodes": 10, "NumDeadDataNodes": 0, "NumStaleDataNodes": 0,
                "CapacityTotal": 107374182400, "CapacityUsed": 32212254720,
            }]
        },
        # WebHDFS content summary
        "/webhdfs/v1/user": {
            "ContentSummary": {
                "spaceConsumed": 10737418240, "spaceQuota": 107374182400,
                "length": 10737418240, "fileCount": 1234
            }
        },
        "/webhdfs/v1/tmp": {
            "ContentSummary": {
                "spaceConsumed": 1073741824, "spaceQuota": 10737418240,
                "length": 1073741824, "fileCount": 100
            }
        },
        # YARN RM
        "/ws/v1/cluster/nodes": {
            "nodes": {"node": [
                {"id": "w1:45678", "state": "RUNNING"},
                {"id": "w2:45678", "state": "RUNNING"},
                {"id": "w3:45678", "state": "RUNNING"},
            ]}
        },
        "/ws/v1/cluster/scheduler": {
            "scheduler": {"schedulerInfo": {
                "queues": {"queue": [
                    {"queueName": "default", "usedCapacity": 25.0},
                    {"queueName": "prod",    "usedCapacity": 40.0},
                ]}
            }}
        },
        # Cloudera Manager
        "/api/v40/clusters/cdp-test/services": {
            "items": [
                {"name": "hdfs", "displayName": "HDFS", "serviceState": "STARTED",
                 "healthSummary": "GOOD"},
                {"name": "yarn", "displayName": "YARN", "serviceState": "STARTED",
                 "healthSummary": "GOOD"},
            ]
        },
    },

    "critical": {
        "/api/v1/clusters/test-cluster/services": {
            "items": [
                {"ServiceInfo": {"service_name": "HDFS", "state": "STARTED"}},
                {"ServiceInfo": {"service_name": "YARN", "state": "STOPPED"}},
                {"ServiceInfo": {"service_name": "HIVE", "state": "UNKNOWN"}},
            ]
        },
        "/api/v1/clusters/test-cluster/alerts": {
            "items": [
                {"Alert": {"label": "NameNode Heap Usage", "host_name": "nn1.test",
                           "state": "CRITICAL", "maintenance_state": "OFF"}},
                {"Alert": {"label": "DataNode Health",     "host_name": "dn01.test",
                           "state": "CRITICAL", "maintenance_state": "OFF"}},
            ]
        },
        "/jmx": {
            "beans": [{
                "name": "Hadoop:service=NameNode,name=FSNamesystemState",
                "NumLiveDataNodes": 7, "NumDeadDataNodes": 3, "NumStaleDataNodes": 0,
            }]
        },
        "/ws/v1/cluster/nodes": {
            "nodes": {"node": [
                {"id": "w1:45678", "state": "RUNNING"},
                {"id": "w2:45678", "state": "LOST"},
                {"id": "w3:45678", "state": "UNHEALTHY"},
            ]}
        },
        "/api/v40/clusters/cdp-test/services": {
            "items": [
                {"name": "hdfs", "displayName": "HDFS", "serviceState": "STARTED",
                 "healthSummary": "BAD"},
            ]
        },
    },
}


class MockHandler(BaseHTTPRequestHandler):
    scenario_data = {}  # type: dict

    def do_GET(self):
        # type: () -> None
        # Cerca il match più lungo nel path
        path_no_qs = self.path.split("?")[0]
        matched = None
        best_len = 0
        for route, data in self.__class__.scenario_data.items():
            if path_no_qs.startswith(route) and len(route) > best_len:
                matched = data
                best_len = len(route)

        if matched is None:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "route not found"}')
            print("[mock] 404 {}".format(path_no_qs), file=sys.stderr)
            return

        body = json.dumps(matched).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        print("[mock] 200 {}".format(path_no_qs), file=sys.stderr)

    def do_PUT(self):
        # type: () -> None
        # WebHDFS CREATE — risposta 307 redirect (semplificato: 201)
        self.send_response(201)
        self.end_headers()

    def do_DELETE(self):
        # type: () -> None
        body = json.dumps({"boolean": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # silenzioso, stampiamo noi sopra


def main():
    parser = argparse.ArgumentParser(description="HadoopScope Mock API Server")
    parser.add_argument("--port",     type=int, default=8080)
    parser.add_argument("--scenario", choices=list(SCENARIOS.keys()), default="ok")
    parser.add_argument("--host",     default="0.0.0.0")
    args = parser.parse_args()

    scenario_data = SCENARIOS[args.scenario]
    handler_class = type("BoundMockHandler", (MockHandler,),
                         {"scenario_data": scenario_data})

    server = HTTPServer((args.host, args.port), handler_class)
    print("[mock] HadoopScope Mock API Server listening on {}:{} (scenario={})".format(
        args.host, args.port, args.scenario), file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[mock] Stopped.", file=sys.stderr)


if __name__ == "__main__":
    main()
