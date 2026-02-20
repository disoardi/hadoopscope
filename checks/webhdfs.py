"""Check HDFS via WebHDFS REST API — nessun client Hadoop richiesto."""

from __future__ import print_function

import json
import socket
import os

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib2 import urlopen, Request, URLError, HTTPError

from checks.base import CheckBase, CheckResult

DEFAULT_TIMEOUT = 10


def _webhdfs_get(base_url, path, op, user, extra_params="", timeout=DEFAULT_TIMEOUT):
    # type: (str, str, str, str, str, int) -> dict
    url = "{}/webhdfs/v1{}?op={}&user.name={}{}".format(
        base_url.rstrip("/"), path, op, user, extra_params
    )
    try:
        resp = urlopen(Request(url), timeout=timeout)
        return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        raise IOError("WebHDFS HTTP {}: {}".format(e.code, e.reason))
    except URLError as e:
        raise IOError("WebHDFS connection error: {}".format(e.reason))
    except socket.timeout:
        raise IOError("WebHDFS timeout ({}s)".format(timeout))


class HdfsSpaceCheck(CheckBase):
    """Controlla utilizzo spazio HDFS per path configurati."""

    requires = []  # WebHDFS è sempre disponibile se configurato

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg  = self.config.get("webhdfs", {})
        base_url  = hdfs_cfg.get("url", "")
        user      = hdfs_cfg.get("user", "hdfs")
        paths_cfg = self.config.get("checks", {}).get("hdfs_space", {}).get("paths", [])

        if not base_url:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        issues    = []
        details   = {}

        for path_cfg in paths_cfg:
            path      = path_cfg["path"]
            warn_pct  = path_cfg.get("warning_pct", 75)
            crit_pct  = path_cfg.get("critical_pct", 90)

            try:
                data    = _webhdfs_get(base_url, path, "GETCONTENTSUMMARY", user)
                summary = data.get("ContentSummary", {})
                used    = summary.get("spaceConsumed", 0)
                quota   = summary.get("spaceQuota", -1)

                if quota <= 0:
                    details[path] = {"used": used, "quota": "none"}
                    continue

                pct = (used / float(quota)) * 100
                details[path] = {
                    "used_bytes": used,
                    "quota_bytes": quota,
                    "used_pct": round(pct, 1)
                }

                if pct >= crit_pct:
                    issues.append((CheckResult.CRITICAL, path, pct, used, quota))
                elif pct >= warn_pct:
                    issues.append((CheckResult.WARNING, path, pct, used, quota))

            except IOError as e:
                issues.append((CheckResult.UNKNOWN, path, 0, 0, 0))

        if not issues:
            return CheckResult(
                name="HdfsSpace",
                status=CheckResult.OK,
                message="All {} paths within thresholds".format(len(paths_cfg)),
                details=details
            )

        worst = max(issues, key=lambda x: (
            0 if x[0] == CheckResult.UNKNOWN else
            1 if x[0] == CheckResult.WARNING else 2
        ))
        status = worst[0]
        msgs = ["{}: {:.0f}% ({})".format(i[1], i[2], "CRITICAL" if i[0] == CheckResult.CRITICAL else i[0])
                for i in issues]
        return CheckResult(
            name="HdfsSpace",
            status=status,
            message="; ".join(msgs),
            details=details
        )


class HdfsDataNodeCheck(CheckBase):
    """Controlla DataNodes morti via JMX NameNode."""

    requires = []  # WebHDFS/JMX sempre disponibile

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg = self.config.get("webhdfs", {})
        base_url = hdfs_cfg.get("url", "")

        if not base_url:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        # Usa JMX endpoint del NameNode
        jmx_url = base_url.replace("/webhdfs/v1", "").rstrip("/")
        jmx_url = "{}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState".format(jmx_url)

        try:
            resp = urlopen(Request(jmx_url), timeout=DEFAULT_TIMEOUT)
            data = json.loads(resp.read().decode("utf-8"))
            beans = data.get("beans", [{}])
            nn_state = beans[0] if beans else {}

            dead   = nn_state.get("NumDeadDataNodes", 0)
            live   = nn_state.get("NumLiveDataNodes", 0)
            stale  = nn_state.get("NumStaleDataNodes", 0)

            warn_thresh = self.config.get("checks", {}).get(
                "hdfs_dead_datanodes", {}).get("warning_threshold", 1)
            crit_thresh = self.config.get("checks", {}).get(
                "hdfs_dead_datanodes", {}).get("critical_threshold", 3)

            details = {"live": live, "dead": dead, "stale": stale}

            if dead >= crit_thresh:
                return CheckResult(
                    name="HdfsDataNodes",
                    status=CheckResult.CRITICAL,
                    message="{} dead DataNodes (threshold: {})".format(dead, crit_thresh),
                    details=details
                )
            elif dead >= warn_thresh:
                return CheckResult(
                    name="HdfsDataNodes",
                    status=CheckResult.WARNING,
                    message="{} dead DataNodes".format(dead),
                    details=details
                )
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.OK,
                message="{} live, {} dead, {} stale DataNodes".format(live, dead, stale),
                details=details
            )

        except Exception as e:
            return CheckResult(
                name="HdfsDataNodes",
                status=CheckResult.UNKNOWN,
                message="JMX error: {}".format(str(e))
            )


class HdfsWritabilityCheck(CheckBase):
    """Testa scrittura/lettura/cancellazione su HDFS via WebHDFS."""

    requires = []

    TEST_FILE_CONTENT = b"hadoopscope-probe"

    def run(self):
        # type: () -> CheckResult
        hdfs_cfg  = self.config.get("webhdfs", {})
        base_url  = hdfs_cfg.get("url", "")
        user      = hdfs_cfg.get("user", "hdfs")
        test_path = self.config.get("checks", {}).get(
            "hdfs_writability", {}).get("test_path", "/tmp/.hadoopscope-probe")

        if not base_url:
            return CheckResult(
                name="HdfsWritability",
                status=CheckResult.UNKNOWN,
                message="webhdfs.url not configured"
            )

        # PUT (create + write)
        try:
            import time
            test_path_ts = "{}-{}".format(test_path, int(time.time()))
            create_url = "{}/webhdfs/v1{}?op=CREATE&overwrite=true&user.name={}".format(
                base_url.rstrip("/"), test_path_ts, user
            )
            # Step 1: richiesta iniziale → redirect a DataNode
            try:
                resp = urlopen(Request(create_url, data=b""), timeout=DEFAULT_TIMEOUT)
            except HTTPError as e:
                if e.code == 307:
                    location = e.headers.get("Location", "")
                    if location:
                        resp = urlopen(
                            Request(location, data=self.TEST_FILE_CONTENT),
                            timeout=DEFAULT_TIMEOUT
                        )
                    else:
                        raise
                else:
                    raise

            # DELETE cleanup
            del_url = "{}/webhdfs/v1{}?op=DELETE&user.name={}".format(
                base_url.rstrip("/"), test_path_ts, user
            )
            urlopen(Request(del_url), timeout=DEFAULT_TIMEOUT)

            return CheckResult(
                name   = "HdfsWritability",
                status = CheckResult.OK,
                message = "HDFS write/delete test passed"
            )

        except Exception as e:
            return CheckResult(
                name   = "HdfsWritability",
                status = CheckResult.CRITICAL,
                message = "HDFS write test failed: {}".format(str(e))
            )
