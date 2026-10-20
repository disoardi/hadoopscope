#!/usr/bin/env python3
"""ambari_inventory.py - read-only Ambari (HDP) inventory: CPU, RAM, disks and components per host.

WHAT IT DOES
  Using only GET calls on the Ambari REST API (a read-only user is enough), it saves for every cluster the hosts with
  vCPU, RAM, disk capacity, rack, state and the installed components, and writes the same hosts_roles.csv schema as
  cm_inventory.py (so sizing_summary.py can read both):
    hostname, ip, rack, cores, ram_gib, disk_tib, layer, health, last_heartbeat, n_roles, stateful_roles,
    coordinator_roles, all_roles
  "layer" is a heuristic from the components (master / worker / kafka / utility / edge): check it against the host
  templates before using it in a document.
  Credentials are asked at the terminal (user and hidden password) or read from AMBARI_USER / AMBARI_PASS and are
  never written to disk.

USAGE
  DIRECT=1 python3 ...   # ignore http_proxy/https_proxy ("Tunnel connection failed: 403 URLBlocked" = the proxy blocks the host)
  python3 ambari_inventory.py                                   # asks for URL, user and password
  AMBARI_URL=http://ambari-host:8080 OUT=./amb-prod python3 ambari_inventory.py
  CLUSTER=<cluster name> ...                                    # only one cluster (default: all)
  CA_BUNDLE=/path/ca.pem ...                                    # verify the certificate (default: do not verify)

Standard library only, Python 3.6+.
"""
from __future__ import print_function

import base64
import csv
import getpass
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request

DIRECT = os.environ.get("DIRECT", "") not in ("", "0")  # DIRECT=1: ignore http(s)_proxy (internal hosts behind a URL filter)

MASTER = set(["NAMENODE", "JOURNALNODE", "SECONDARY_NAMENODE", "RESOURCEMANAGER", "HISTORYSERVER",
              "APP_TIMELINE_SERVER", "HBASE_MASTER", "ZKFC"])
UTILITY = set(["HIVE_METASTORE", "RANGER_ADMIN", "RANGER_USERSYNC", "RANGER_TAGSYNC", "ATLAS_SERVER", "OOZIE_SERVER",
               "INFRA_SOLR", "WEBHCAT_SERVER", "LIVY_SERVER", "LIVY2_SERVER", "DATA_ANALYTICS_STUDIO_WEBAPP",
               "DATA_ANALYTICS_STUDIO_EVENT_PROCESSOR", "METRICS_COLLECTOR", "SPARK2_JOBHISTORYSERVER",
               "SPARK_JOBHISTORYSERVER"])
EDGE_HINT = set(["HIVE_SERVER", "HUE_SERVER"])
STATEFUL = set(["NAMENODE", "JOURNALNODE", "SECONDARY_NAMENODE", "ZOOKEEPER_SERVER", "HIVE_METASTORE", "HBASE_MASTER",
                "RANGER_ADMIN", "ATLAS_SERVER", "OOZIE_SERVER", "INFRA_SOLR", "KAFKA_BROKER", "METRICS_COLLECTOR"])
COORD = set(["NAMENODE", "JOURNALNODE", "ZOOKEEPER_SERVER", "RESOURCEMANAGER", "HISTORYSERVER", "HIVE_METASTORE",
             "HIVE_SERVER", "HBASE_MASTER", "RANGER_ADMIN", "ATLAS_SERVER", "OOZIE_SERVER", "KAFKA_BROKER"])

OUT = os.environ.get("OUT") or "ambari-inventory-" + time.strftime("%Y%m%d-%H%M%S")
BASE = (os.environ.get("AMBARI_URL") or "").rstrip("/")
ONLY = os.environ.get("CLUSTER") or ""


def layer_of(comps):
    c = set(comps)
    if c & MASTER:
        return "master"
    if "DATANODE" in c or "NODEMANAGER" in c:
        return "worker"
    if "KAFKA_BROKER" in c:
        return "kafka"
    if c & UTILITY:
        return "utility"
    return "edge"


def ssl_ctx():
    ca = os.environ.get("CA_BUNDLE")
    if ca:
        return ssl.create_default_context(cafile=ca)
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


class Ambari(object):
    def __init__(self, base, user, password):
        self.base = base
        tok = base64.b64encode(("%s:%s" % (user, password)).encode("utf-8")).decode("ascii")
        # no Accept header: the Ambari REST API answers JSON as text/plain and replies 406 to "Accept: application/json"
        self.hdr = {"Authorization": "Basic " + tok, "X-Requested-By": "inventory"}
        self.ctx = ssl_ctx()

    def get(self, path, **params):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params, safe="/,")
        req = urllib.request.Request(url, headers=self.hdr)  # GET only
        opener = urllib.request.build_opener(*([urllib.request.ProxyHandler({})] if DIRECT else []),
                                             urllib.request.HTTPSHandler(context=self.ctx))
        with opener.open(req, timeout=180) as r:
            return json.loads(r.read().decode("utf-8"))


def disk_tib(disk_info):
    """Sum of the size of every distinct /dev device (Ambari reports sizes in KB)."""
    seen = {}
    for d in disk_info or []:
        dev = d.get("device") or ""
        if dev.startswith("/dev/") and dev not in seen:
            try:
                seen[dev] = float(d.get("size") or 0)
            except ValueError:
                pass
    return sum(seen.values()) / 1024.0 ** 3


def main():
    global BASE
    if not BASE:
        BASE = input("Ambari URL (e.g. http://ambari-host:8080): ").strip().rstrip("/")
    user = os.environ.get("AMBARI_USER") or input("Ambari user: ").strip()
    password = os.environ.get("AMBARI_PASS") or getpass.getpass("Ambari password: ")
    if not os.path.isdir(OUT):
        os.makedirs(OUT)
    try:
        os.chmod(OUT, 0o700)
    except OSError:
        pass
    am = Ambari(BASE, user, password)
    try:
        clusters = [c["Clusters"]["cluster_name"] for c in am.get("/api/v1/clusters").get("items", [])]
    except Exception as e:  # noqa
        print("Cannot reach %s or credentials rejected: %s" % (BASE, e), file=sys.stderr)
        return 1
    print("Ambari %s  clusters: %s  output: %s" % (BASE, ", ".join(clusters), OUT))
    rc = 0
    for cname in clusters:
        if ONLY and cname != ONLY:
            continue
        hosts = []
        start, page = 0, 100
        fields = ("Hosts/host_name,Hosts/ip,Hosts/rack_info,Hosts/cpu_count,Hosts/ph_cpu_count,Hosts/total_mem,"
                  "Hosts/disk_info,Hosts/host_state,Hosts/host_status,Hosts/last_heartbeat_time,"
                  "host_components/HostRoles/component_name")
        while True:
            try:
                doc = am.get("/api/v1/clusters/%s/hosts" % urllib.parse.quote(cname),
                             fields=fields, page_size=page, **{"from": start})
            except Exception as e:  # noqa
                print("FAIL  hosts of %s (%s)" % (cname, e))
                rc = 2
                break
            items = doc.get("items", [])
            hosts.extend(items)
            if len(items) < page:
                break
            start += page
        slug = "".join(ch if ch.isalnum() else "_" for ch in cname)
        with open(os.path.join(OUT, "hosts_%s.json" % slug), "w") as f:
            json.dump({"items": hosts}, f, indent=1, sort_keys=True)
        path = os.path.join(OUT, "hosts_roles_%s.csv" % slug)
        with open(path, "w") as f:
            w = csv.writer(f)
            w.writerow(["hostname", "ip", "rack", "cores", "ram_gib", "disk_tib", "layer", "health", "last_heartbeat",
                        "n_roles", "stateful_roles", "coordinator_roles", "all_roles"])
            for it in sorted(hosts, key=lambda i: i["Hosts"].get("host_name", "")):
                h = it["Hosts"]
                comps = [c["HostRoles"]["component_name"] for c in it.get("host_components", [])]
                mem = h.get("total_mem")
                w.writerow([
                    h.get("host_name", ""), h.get("ip", ""), h.get("rack_info", ""),
                    h.get("cpu_count") or h.get("ph_cpu_count") or "",
                    "%.0f" % (float(mem) / 1024.0 ** 2) if mem else "",
                    "%.1f" % disk_tib(h.get("disk_info")) if h.get("disk_info") else "",
                    layer_of(comps), h.get("host_status", ""), h.get("last_heartbeat_time", ""),
                    len(comps),
                    " ".join(sorted(set(c for c in comps if c in STATEFUL))),
                    " ".join(sorted(set(c for c in comps if c in COORD))),
                    " ".join(sorted(comps)),
                ])
        print("ok    %s (%d hosts)" % (path, len(hosts)))
    print("Done. Files in: " + OUT)
    return rc


if __name__ == "__main__":
    sys.exit(main())
