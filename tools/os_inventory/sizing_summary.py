#!/usr/bin/env python3
"""sizing_summary.py - turn one or more hosts_roles.csv into the sizing table (nodes per layer and machine type).

Input files come from cm_inventory.py (CDP) and ambari_inventory.py (HDP): same schema
(hostname, ip, rack, cores, ram_gib, disk_tib, layer, ...). Nodes of a layer with the same vCPU, RAM and
disk size (rounded) are grouped into one row.

USAGE
  python3 sizing_summary.py "CDP prod=cm-prod/hosts_roles.csv" "CDP dr=cm-dr/hosts_roles.csv" \
                            "HDP prod=amb-prod/hosts_roles_BFD.csv"
  python3 sizing_summary.py --md ...      # markdown output (default: plain text, easy to paste in a mail)

Standard library only, Python 3.6+.
"""
from __future__ import print_function

import collections
import csv
import sys

LAYERS = ["master", "utility", "edge", "worker", "kafka", "nifi"]


def rnd(v, step):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return int(round(v / step)) * step


def load(path):
    out = []
    with open(path) as f:
        for r in csv.DictReader(f):
            out.append(r)
    return out


def summarise(rows):
    groups = collections.OrderedDict()
    for layer in LAYERS:
        for r in rows:
            if r.get("layer") != layer:
                continue
            key = (layer, int(float(r["cores"])) if r.get("cores") else None,
                   rnd(r.get("ram_gib"), 4), rnd(r.get("disk_tib"), 0.5) if r.get("disk_tib") else None)
            groups.setdefault(key, []).append(r["hostname"].split(".")[0])
    return groups


def main(argv):
    md = "--md" in argv
    args = [a for a in argv if a != "--md"]
    if not args:
        print(__doc__)
        return 2
    head = ["Environment", "Layer", "Nodes", "vCPU/node", "RAM/node (GiB)", "Storage/node (TiB)"]
    table = []
    for a in args:
        label, _, path = a.partition("=")
        rows = load(path)
        g = summarise(rows)
        total = sum(len(v) for v in g.values())
        for (layer, cores, ram, disk), hosts in g.items():
            table.append([label, layer, str(len(hosts)), str(cores or "?"), str(ram if ram is not None else "?"),
                          ("%.1f" % disk) if disk is not None else "n/a"])
        table.append([label, "total", str(total), "", "", ""])
    widths = [max(len(r[i]) for r in [head] + table) for i in range(len(head))]
    sep = " | " if md else "  "
    def line(r):
        return (("| " if md else "") + sep.join(c.ljust(w) for c, w in zip(r, widths)) + (" |" if md else "")).rstrip()
    print(line(head))
    print(("|" + "|".join("-" * (w + 2) for w in widths) + "|") if md else "  ".join("-" * w for w in widths))
    for r in table:
        print(line(r))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
