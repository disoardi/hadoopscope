#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""yarn_rm_monitor - read-only YARN ResourceManager monitor (HDP and CDP).

WHAT IT DOES
  Every INTERVAL seconds it records compact CSV time series that give a complete picture of a busy
  period (month-end loads, batch windows, "applications stuck in ACCEPTED" investigations):
    cluster.csv  cluster totals: apps pending/running, vcores and memory allocated/available/reserved/pending,
                 containers, active/unhealthy nodes
    queues.csv   every queue: guaranteed share, usage, weight, active/pending apps, containers,
                 AM usage and AM limit, guaranteed memory/vcores, user-limit-factor
    users.csv    every user in every queue: usage, AM usage, user limit, active/pending apps
    apps.csv     every RUNNING and ACCEPTED application: user, queue, name, elapsed, resources and, for the
                 ACCEPTED ones, the ResourceManager diagnostics (why the app is waiting)
    nodes.csv    every NodeManager: state, memory/vcores used and available, containers
    raw/         raw scheduler and apps JSON, only while something is pending, at most every RAW_EVERY seconds
    apps_finished_end.json   finished apps still in RM memory at the end (launchTime - startedTime = wait)
  At the end everything is packed in <OUT>.tgz.
  Only HTTP GET calls on the ResourceManager web services. No credentials, no writes on the cluster.

REQUIREMENTS
  python 2.7 or 3.x, standard library only. Run it on a cluster node (or any host that reaches the RM
  web UI) with nohup / at / cron. If the RM web UI requires Kerberos this script cannot be used as is.

CONFIGURATION (environment variables)
  RM_URL          http://<rm-host>:8088            use one RM (no failover detection)
  RM_HOSTS        "rm1.fqdn rm2.fqdn"              probe http :8088 then https :8090, pick the ACTIVE one,
                                                   re-detect after 3 consecutive errors (failover)
  INTERVAL        seconds between samples          (default 30)
  DURATION_HOURS  total run time in hours          (default 1)   or SAMPLES=<n>
  OUT             output directory                 (default ./yarn-monitor-<timestamp>)
  RAW_EVERY       seconds between raw dumps        (default 600)
  NODES_EVERY     nodes sampled every N samples    (default 2, 0 = never)

QUICK START
  RM_HOSTS="rm1.fqdn rm2.fqdn" DURATION_HOURS=1 INTERVAL=30 python yarn_rm_monitor.py

ONE-OFF IN BACKGROUND (at), e.g. a month-end window
  echo 'RM_HOSTS="rm1.fqdn rm2.fqdn" OUT=/var/tmp/yarn-monitor-20261030 DURATION_HOURS=72 INTERVAL=30 \
        python /root/yarn_rm_monitor.py > /var/tmp/yarn-monitor-20261030.log 2>&1' | at 00:00 10/30/2026
  atq

CRON (24 h capture started every day at midnight, no overlap, 14 days retention)
  0 0 * * * flock -n /var/tmp/yarn-monitor.lock env RM_HOSTS="rm1.fqdn rm2.fqdn" OUT=/var/tmp/yarn-monitor/$(date +\%Y\%m\%d) DURATION_HOURS=24 INTERVAL=60 python /root/yarn_rm_monitor.py >> /var/tmp/yarn-monitor.log 2>&1
  30 23 * * * find /var/tmp/yarn-monitor -maxdepth 1 -mtime +14 -exec rm -rf {} +
  (in crontab the % must be escaped as \%)

PRACTICAL EXAMPLE - 72 h capture of a month-end load window (copy and paste, replace the <...>)
  # from your workstation: copy the script on the node that can reach the active RM
  scp yarn_rm_monitor.py root@<rm-node>:/root/
  # on the node, as root (check df -h /var/tmp first: expect a few hundred MB for 72 h):
  echo 'RM_HOSTS="<rm1.fqdn> <rm2.fqdn>" OUT=/var/tmp/yarn-monitor-<YYYYMMDD> DURATION_HOURS=72 INTERVAL=30 python /root/yarn_rm_monitor.py > /var/tmp/yarn-monitor-<YYYYMMDD>.log 2>&1' | at 00:00 <MM/DD/YYYY>
  atq                                                  # the job must be listed
  tail -f /var/tmp/yarn-monitor-<YYYYMMDD>.log         # once started, one line per sample
  # when the run ends, fetch the archive
  scp root@<rm-node>:/var/tmp/yarn-monitor-<YYYYMMDD>.tgz .
  # NOTE: if the node's python is 2.7 (CentOS 7) use "python"; on python3-only hosts use "python3".
"""
from __future__ import print_function

import csv
import io
import json
import os
import re
import ssl
import sys
import tarfile
import time

try:
    from urllib.request import urlopen
except ImportError:  # python 2
    from urllib2 import urlopen

RM_URL = os.environ.get("RM_URL", "")
RM_HOSTS = os.environ.get("RM_HOSTS", "").split()
INTERVAL = int(os.environ.get("INTERVAL", "30"))
RAW_EVERY = int(os.environ.get("RAW_EVERY", "600"))
NODES_EVERY = int(os.environ.get("NODES_EVERY", "2"))
OUT = os.environ.get("OUT", "yarn-monitor-" + time.strftime("%Y%m%d-%H%M%S"))
if os.environ.get("SAMPLES"):
    SAMPLES = int(os.environ["SAMPLES"])
else:
    SAMPLES = int(float(os.environ.get("DURATION_HOURS", "1")) * 3600 / INTERVAL)

try:
    _CTX = ssl._create_unverified_context()
except AttributeError:  # very old python 2.7: https unverified by default
    _CTX = None


def get_json(url, timeout=60):
    if url.startswith("https") and _CTX is not None:
        body = urlopen(url, timeout=timeout, context=_CTX).read()
    else:
        body = urlopen(url, timeout=timeout).read()
    return json.loads(body.decode("utf-8"))


def find_rm():
    if RM_URL:
        return RM_URL
    for h in RM_HOSTS:
        for base in ("http://%s:8088" % h, "https://%s:8090" % h):
            try:
                info = get_json(base + "/ws/v1/cluster/info", timeout=5)["clusterInfo"]
                if info.get("haState", "ACTIVE") == "ACTIVE":
                    return base
            except Exception:
                pass
    return ""


def res(d, key):
    """(memory MB, vcores) of a resource dict inside d, or (0, 0)."""
    r = (d or {}).get(key) or {}
    return int(r.get("memory", 0) or 0), int(r.get("vCores", 0) or 0)


def clean(s, n=400):
    return re.sub(r"\s+", " ", (s or "")).strip()[:n]


class Csv(object):
    def __init__(self, path, header):
        new = not os.path.exists(path)
        self.f = io.open(path, "a", encoding="utf-8", newline="")
        self.w = csv.writer(self.f)
        self.header = header
        if new:
            self._row(header)

    def _row(self, row):
        if sys.version_info[0] < 3:
            row = [(c.encode("utf-8") if isinstance(c, type(u"")) else c) for c in row]
            fb = open(self.f.name, "ab")
            csv.writer(fb).writerow(row)
            fb.close()
        else:
            self.w.writerow(row)

    def row(self, row):
        if sys.version_info[0] < 3:
            self._row(row)
        else:
            self.w.writerow(row)
            self.f.flush()


def walk_queues(q, ts, queues_csv, users_csv):
    path = q.get("queuePath") or q.get("queueName")
    used_mb, used_vc = res(q, "resourcesUsed")
    am_mb, am_vc = res(q, "usedAMResource")
    lim_mb, lim_vc = res(q, "AMResourceLimit")
    min_mb, min_vc = res(q, "minEffectiveCapacity")
    queues_csv.row([ts, path, q.get("absoluteCapacity", ""), q.get("absoluteUsedCapacity", ""), q.get("weight", ""),
                    q.get("numActiveApplications", ""), q.get("numPendingApplications", ""),
                    q.get("allocatedContainers", ""), q.get("pendingContainers", ""), q.get("reservedContainers", ""),
                    used_mb, used_vc, am_mb, am_vc, lim_mb, lim_vc, min_mb, min_vc,
                    q.get("userLimitFactor", ""), q.get("maxApplications", "")])
    users = q.get("users") or {}
    for u in (users.get("user") or []) if isinstance(users, dict) else []:
        umb, uvc = res(u, "resourcesUsed")
        amb, avc = res(u, "AMResourcesUsed")
        lmb, lvc = res(u, "userResourceLimit")
        users_csv.row([ts, path, u.get("username", ""), umb, uvc, amb, avc, lmb, lvc,
                       u.get("numActiveApplications", ""), u.get("numPendingApplications", "")])
    for c in ((q.get("queues") or {}).get("queue") or []):
        walk_queues(c, ts, queues_csv, users_csv)


def main():
    if not RM_URL and not RM_HOSTS:
        print("Set RM_URL (e.g. http://<rm-host>:8088) or RM_HOSTS (space separated list). See the header of this file.",
              file=sys.stderr)
        sys.exit(2)
    rm = find_rm()
    if not rm:
        print("No active ResourceManager found on %s" % " ".join(RM_HOSTS), file=sys.stderr)
        sys.exit(1)
    os.makedirs(os.path.join(OUT, "raw"))
    print("RM: %s | interval %ss | samples %d | output %s" % (rm, INTERVAL, SAMPLES, OUT))

    cluster = Csv(os.path.join(OUT, "cluster.csv"), [
        "ts", "appsPending", "appsRunning", "appsSubmitted", "appsCompleted", "allocatedMB", "availableMB", "reservedMB",
        "pendingMB", "totalMB", "allocatedVcores", "availableVcores", "reservedVcores", "pendingVcores", "totalVcores",
        "containersAllocated", "containersReserved", "containersPending", "activeNodes", "unhealthyNodes", "lostNodes"])
    queues = Csv(os.path.join(OUT, "queues.csv"), [
        "ts", "queue", "absCapacityPct", "absUsedPct", "weight", "numActiveApps", "numPendingApps", "allocatedContainers",
        "pendingContainers", "reservedContainers", "usedMB", "usedVcores", "amUsedMB", "amUsedVcores", "amLimitMB",
        "amLimitVcores", "minGuaranteedMB", "minGuaranteedVcores", "userLimitFactor", "maxApplications"])
    users = Csv(os.path.join(OUT, "users.csv"), [
        "ts", "queue", "user", "usedMB", "usedVcores", "amUsedMB", "amUsedVcores", "userLimitMB", "userLimitVcores",
        "numActiveApps", "numPendingApps"])
    apps = Csv(os.path.join(OUT, "apps.csv"), [
        "ts", "id", "user", "queue", "type", "name", "state", "elapsedSec", "allocatedMB", "allocatedVcores",
        "runningContainers", "queueUsagePct", "clusterUsagePct", "diagnostics"])
    nodes = Csv(os.path.join(OUT, "nodes.csv"), [
        "ts", "host", "state", "usedMB", "availMB", "usedVcores", "availVcores", "numContainers", "health"])

    run_start_ms = int(time.time() * 1000)
    last_raw = 0.0
    fails = 0
    for i in range(SAMPLES):
        t0 = time.time()
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t0)) + "Z"
        try:
            m = get_json(rm + "/ws/v1/cluster/metrics")["clusterMetrics"]
            sched = get_json(rm + "/ws/v1/cluster/scheduler")
            alist = get_json(rm + "/ws/v1/cluster/apps?states=ACCEPTED,RUNNING")
            fails = 0
        except Exception as e:
            fails += 1
            print("[%s] error: %s" % (ts, e), file=sys.stderr)
            if fails >= 3:
                new = find_rm()
                if new:
                    rm = new
                    print("[%s] RM re-detected: %s" % (ts, rm))
            time.sleep(INTERVAL)
            continue

        cluster.row([ts, m.get("appsPending"), m.get("appsRunning"), m.get("appsSubmitted"), m.get("appsCompleted"),
                     m.get("allocatedMB"), m.get("availableMB"), m.get("reservedMB"), m.get("pendingMB"), m.get("totalMB"),
                     m.get("allocatedVirtualCores"), m.get("availableVirtualCores"), m.get("reservedVirtualCores"),
                     m.get("pendingVirtualCores"), m.get("totalVirtualCores"), m.get("containersAllocated"),
                     m.get("containersReserved"), m.get("containersPending"), m.get("activeNodes"),
                     m.get("unhealthyNodes"), m.get("lostNodes")])
        walk_queues(sched["scheduler"]["schedulerInfo"], ts, queues, users)

        accepted = 0
        for a in ((alist.get("apps") or {}).get("app") or []):
            if a.get("state") == "ACCEPTED":
                accepted += 1
            apps.row([ts, a.get("id"), a.get("user"), a.get("queue"), a.get("applicationType"), clean(a.get("name"), 60),
                      a.get("state"), int((a.get("elapsedTime") or 0) / 1000), a.get("allocatedMB"),
                      a.get("allocatedVCores"), a.get("runningContainers"), a.get("queueUsagePercentage"),
                      a.get("clusterUsagePercentage"), clean(a.get("diagnostics")) if a.get("state") == "ACCEPTED" else ""])

        if NODES_EVERY and i % NODES_EVERY == 0:
            try:
                nl = get_json(rm + "/ws/v1/cluster/nodes")
                for n in ((nl.get("nodes") or {}).get("node") or []):
                    nodes.row([ts, n.get("nodeHostName"), n.get("state"), n.get("usedMemoryMB"),
                               n.get("availMemoryMB"), n.get("usedVirtualCores"), n.get("availableVirtualCores"),
                               n.get("numContainers"), clean(n.get("healthReport"), 80)])
            except Exception as e:
                print("[%s] nodes error: %s" % (ts, e), file=sys.stderr)

        pending = (m.get("appsPending") or 0) + accepted
        if pending > 0 and t0 - last_raw >= RAW_EVERY:
            stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime(t0))
            with io.open(os.path.join(OUT, "raw", stamp + "_scheduler.json"), "w", encoding="utf-8") as f:
                f.write(json.dumps(sched) if sys.version_info[0] >= 3 else json.dumps(sched).decode("utf-8"))
            with io.open(os.path.join(OUT, "raw", stamp + "_apps.json"), "w", encoding="utf-8") as f:
                f.write(json.dumps(alist) if sys.version_info[0] >= 3 else json.dumps(alist).decode("utf-8"))
            last_raw = t0

        print("[%s] running=%s pending=%s accepted=%d vcores=%s/%s mem=%s/%s MB" % (
            ts, m.get("appsRunning"), m.get("appsPending"), accepted, m.get("allocatedVirtualCores"),
            m.get("totalVirtualCores"), m.get("allocatedMB"), m.get("totalMB")))
        sys.stdout.flush()
        time.sleep(max(0.0, INTERVAL - (time.time() - t0)))

    # applications finished during the run (a plain limit=N returns the OLDEST N kept by the RM, not the latest)
    try:
        fin = get_json(rm + "/ws/v1/cluster/apps?states=FINISHED,FAILED,KILLED&finishedTimeBegin=%d" % run_start_ms,
                       timeout=120)
        s = json.dumps(fin)
        with io.open(os.path.join(OUT, "apps_finished_end.json"), "w", encoding="utf-8") as f:
            f.write(s if sys.version_info[0] >= 3 else s.decode("utf-8"))
    except Exception as e:
        print("finished apps error: %s" % e, file=sys.stderr)
    tar = OUT.rstrip("/") + ".tgz"
    with tarfile.open(tar, "w:gz") as t:
        t.add(OUT, arcname=os.path.basename(OUT.rstrip("/")))
    print("Done. Archive: %s" % tar)


if __name__ == "__main__":
    main()
