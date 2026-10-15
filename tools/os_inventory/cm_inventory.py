#!/usr/bin/env python3
"""cm_inventory.py - read-only Cloudera Manager export to plan an OS replacement.

WHAT IT DOES
  Using only GET calls on the CM REST API (a read-only role is enough, e.g. cluster administrator),
  it saves what is needed to decide the target OS and the order of the node replacement:
    - CM version and API version, license summary, CM management service roles
    - for every cluster: runtime/CDH version, parcels (CDP runtime, CFM/NiFi, Spark3, ...), services
    - for every service: roles with host, state and health, and the configuration group
    - for every host: hostname, IP, rack, cores, RAM, role assignment, health, last heartbeat
    - deployment export with passwords redacted (view=export_redacted)
  and derives two CSV files you can open in a spreadsheet:
    hosts_roles.csv   one row per host with all its roles and the flags "stateful" and "coordinator"
    compat_inputs.csv the versions to cross with the vendor support matrix (CM, runtime, CFM, python of Hue...)
  Credentials are asked at the terminal (user and hidden password) or read from CM_USER / CM_PASS; they are
  never written to disk.

USAGE
  python3 cm_inventory.py                        # asks for URL, user and password
  CM_URL=https://cm.example:7183 python3 cm_inventory.py
  CM_URL=... CLUSTER="Cluster 1" OUT=./cm-inv python3 cm_inventory.py   # only one cluster
  CA_BUNDLE=/path/ca.pem python3 cm_inventory.py     # verify the server certificate (default: do not verify)

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

STATEFUL = set([
    "NAMENODE", "JOURNALNODE", "SECONDARYNAMENODE", "ZOOKEEPER", "SERVER",  # SERVER is the ZK role type
    "HIVEMETASTORE", "KUDU_MASTER", "KUDU_TSERVER", "HBASE_MASTER", "REGIONSERVER",
    "CATALOGSERVER", "STATESTORE", "KAFKA_BROKER", "RANGER_ADMIN", "RANGER_USERSYNC", "ATLAS_SERVER",
    "OOZIE_SERVER", "SOLR_SERVER", "KMS", "KEYTRUSTEE_SERVER", "NIFI_NODE", "NIFIREGISTRY_SERVER",
    "ZEPPELIN_SERVER", "HUE_SERVER", "HUE_LOAD_BALANCER", "SCHEMAREGISTRY_SERVER",
])
COORDINATOR = set([
    "NAMENODE", "JOURNALNODE", "ZOOKEEPER", "SERVER", "RESOURCEMANAGER", "JOBHISTORY", "SPARK_YARN_HISTORY_SERVER",
    "HIVEMETASTORE", "HIVESERVER2", "CATALOGSERVER", "STATESTORE", "KUDU_MASTER", "HBASE_MASTER",
    "RANGER_ADMIN", "ATLAS_SERVER", "OOZIE_SERVER", "KMS", "HUE_SERVER", "HUE_LOAD_BALANCER", "KAFKA_BROKER",
    "QUEUEMANAGER_WEBAPP", "QUEUEMANAGER_STORE",
])

OUT = os.environ.get("OUT") or "cm-inventory-" + time.strftime("%Y%m%d-%H%M%S")
BASE = (os.environ.get("CM_URL") or "").rstrip("/")
ONLY_CLUSTER = os.environ.get("CLUSTER") or ""


def ctx():
    ca = os.environ.get("CA_BUNDLE")
    if ca:
        return ssl.create_default_context(cafile=ca)
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


class Cm(object):
    def __init__(self, base, user, password):
        self.base = base
        tok = base64.b64encode(("%s:%s" % (user, password)).encode("utf-8")).decode("ascii")
        self.hdr = {"Authorization": "Basic " + tok, "Accept": "application/json"}
        self.ctx = ctx()
        self.api = None

    def get(self, path, **params):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=self.hdr)  # GET only
        with urllib.request.urlopen(req, timeout=120, context=self.ctx) as r:
            return r.read().decode("utf-8")

    def j(self, path, **params):
        return json.loads(self.get("/api/%s%s" % (self.api, path), **params))


def save(name, data):
    path = os.path.join(OUT, name + ".json")
    with open(path, "w") as f:
        f.write(data if isinstance(data, str) else json.dumps(data, indent=2, sort_keys=True))
    print("ok    " + name)


def main():
    global BASE
    if not BASE:
        BASE = input("CM URL (e.g. https://cm.example:7183): ").strip().rstrip("/")
    user = os.environ.get("CM_USER") or input("CM user: ").strip()
    password = os.environ.get("CM_PASS") or getpass.getpass("CM password: ")
    if not os.path.isdir(OUT):
        os.makedirs(OUT)
    try:
        os.chmod(OUT, 0o700)
    except OSError:
        pass

    cm = Cm(BASE, user, password)
    try:
        cm.api = cm.get("/api/version").strip()
    except Exception as e:  # noqa
        print("Cannot reach %s or credentials rejected: %s" % (BASE, e), file=sys.stderr)
        return 1
    print("CM API %s   output: %s" % (cm.api, OUT))

    def step(name, fn):
        try:
            save(name, fn())
            return True
        except Exception as e:  # noqa
            print("FAIL  %s (%s)" % (name, e))
            return False

    step("cm_version", lambda: cm.j("/cm/version"))
    step("cm_license", lambda: cm.j("/cm/license"))
    step("cm_management_roles", lambda: cm.j("/cm/service/roles"))
    step("deployment_redacted", lambda: cm.get("/api/%s/cm/deployment" % cm.api, view="export_redacted"))
    hosts_doc = None
    try:
        hosts_doc = cm.j("/hosts", view="full")
        save("hosts_full", hosts_doc)
    except Exception as e:  # noqa
        print("FAIL  hosts_full (%s)" % e)

    clusters = []
    try:
        clusters = cm.j("/clusters", view="full").get("items", [])
        save("clusters", {"items": clusters})
    except Exception as e:  # noqa
        print("FAIL  clusters (%s)" % e)

    rows = {}      # hostId -> dict
    compat = []    # (what, name, version)
    compat.append(("CM", "cloudera-manager", json.loads(cm.get("/api/%s/cm/version" % cm.api)).get("version", "")))

    for c in clusters:
        cname = c.get("name") or c.get("displayName")
        if ONLY_CLUSTER and ONLY_CLUSTER != cname:
            continue
        slug = "".join(ch if ch.isalnum() else "_" for ch in cname)
        compat.append(("Runtime", cname, "%s %s" % (c.get("fullVersion", ""), c.get("clusterType", ""))))
        enc = urllib.parse.quote(cname)
        step("parcels_" + slug, lambda: cm.j("/clusters/%s/parcels" % enc))
        try:
            for p in cm.j("/clusters/%s/parcels" % enc).get("items", []):
                if p.get("stage") in ("ACTIVATED", "DISTRIBUTED"):
                    compat.append(("Parcel", p.get("product"), p.get("version")))
        except Exception:  # noqa
            pass
        try:
            services = cm.j("/clusters/%s/services" % enc, view="full").get("items", [])
        except Exception as e:  # noqa
            print("FAIL  services of %s (%s)" % (cname, e))
            continue
        save("services_" + slug, {"items": services})
        for s in services:
            sname = s.get("name")
            try:
                roles = cm.j("/clusters/%s/services/%s/roles" % (enc, urllib.parse.quote(sname)), view="full")
            except Exception as e:  # noqa
                print("FAIL  roles of %s/%s (%s)" % (cname, sname, e))
                continue
            save("roles_%s_%s" % (slug, sname), roles)
            for r in roles.get("items", []):
                hid = (r.get("hostRef") or {}).get("hostId")
                if not hid:
                    continue
                h = rows.setdefault(hid, {"roles": []})
                h["roles"].append({
                    "cluster": cname, "service": s.get("type"), "type": r.get("type"),
                    "state": r.get("roleState"), "health": r.get("healthSummary"),
                    "group": (r.get("roleConfigGroupRef") or {}).get("roleConfigGroupName", ""),
                })

    # management service roles (Host Monitor, Service Monitor, ...)
    try:
        for r in cm.j("/cm/service/roles").get("items", []):
            hid = (r.get("hostRef") or {}).get("hostId")
            if hid:
                rows.setdefault(hid, {"roles": []})["roles"].append({
                    "cluster": "(CM management)", "service": "MGMT", "type": r.get("type"),
                    "state": r.get("roleState"), "health": r.get("healthSummary"), "group": ""})
    except Exception:  # noqa
        pass

    hosts_by_id = {}
    for h in (hosts_doc or {}).get("items", []):
        hosts_by_id[h.get("hostId")] = h

    with open(os.path.join(OUT, "hosts_roles.csv"), "w") as f:
        w = csv.writer(f)
        w.writerow(["hostname", "ip", "rack", "cores", "ram_gib", "health", "last_heartbeat",
                    "n_roles", "stateful_roles", "coordinator_roles", "all_roles"])
        ids = sorted(set(list(rows.keys()) + list(hosts_by_id.keys())),
                     key=lambda i: (hosts_by_id.get(i, {}).get("hostname") or i))
        for hid in ids:
            h = hosts_by_id.get(hid, {})
            roles = rows.get(hid, {}).get("roles", [])
            types = [r["type"] for r in roles]
            ram = h.get("totalPhysMemBytes")
            w.writerow([
                h.get("hostname", hid), h.get("ipAddress", ""), h.get("rackId", ""), h.get("numCores", ""),
                "%.0f" % (ram / 1024.0 ** 3) if ram else "", h.get("healthSummary", ""), h.get("lastHeartbeat", ""),
                len(roles),
                " ".join(sorted(set(t for t in types if t in STATEFUL))),
                " ".join(sorted(set(t for t in types if t in COORDINATOR))),
                " ".join("%s:%s:%s" % (r["service"], r["type"], r["state"]) for r in roles),
            ])
    print("ok    hosts_roles.csv (%d hosts)" % len(ids))

    with open(os.path.join(OUT, "compat_inputs.csv"), "w") as f:
        w = csv.writer(f)
        w.writerow(["what", "name", "version"])
        for t in compat:
            w.writerow(t)
        w.writerow(["NOTE", "OS python and Hue python", "from os_inventory.sh section 60_python and the Hue role config"])
    print("ok    compat_inputs.csv")
    print("Done. Files in: " + OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
