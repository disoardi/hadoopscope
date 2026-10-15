#!/usr/bin/env bash
# os_inventory.sh - read-only host inventory to plan an OS replacement (e.g. CentOS 7 -> RHEL/Oracle Linux 9).
#
# WHAT IT DOES
#   Collects, on ONE host, everything that matters when the OS is reinstalled: distribution and kernel,
#   hardware, disks/LVM/fstab, network, time and auth config, installed packages (with vendor, to spot what
#   is not from the distribution), repos, services, cron, listening ports, non-system users and their
#   processes, language runtimes (python, R, java, conda), custom software under /opt, /usr/local, /data,
#   Cloudera agent/parcels, JDBC/ODBC/Oracle clients, sysctl and limits.
#   Writes only text files under OUT and one tgz. Does not change anything on the host, makes no network
#   calls, does not read private keys, keytabs, shadow or password files.
#
# WHO RUNS IT
#   root (some commands need it, e.g. lvs, ss -p, crontabs of other users). Without root it still runs and
#   marks the sections it could not read.
#
# USAGE
#   bash os_inventory.sh                                # writes /var/tmp/os-inventory-<host>-<ts>.tgz
#   OUT=/var/tmp/inv bash os_inventory.sh               # custom output directory
#   FAST=1 bash os_inventory.sh                         # skips the slow parts (du on /opt, /data, /home)
#   GIVE_TO=<user> bash os_inventory.sh                 # as root: hand the archive to <user> (mode 640) so it
#                                                       # can be fetched with scp; default is root-only (600)
#   OUT=/tmp/inv GIVE_TO=<user> bash os_inventory.sh    # archive ends up in /tmp/inv.tgz
#   The uncompressed directory is removed after packing (KEEP=1 keeps it). No parameter is required.
#
# ON MANY HOSTS (from a jump host, as root, with ssh keys)
#   for h in $(cat hosts.txt); do
#     ssh "$h" 'bash -s' < os_inventory.sh && scp "$h":/var/tmp/os-inventory-*.tgz ./inv/
#   done
#
# Python 2.7 / 3.x not needed: bash and coreutils only.
set -uo pipefail

HOST=$(hostname -f 2>/dev/null || hostname)
TS=$(date +%Y%m%d-%H%M%S)
OUT=${OUT:-/var/tmp/os-inventory-${HOST}-${TS}}
FAST=${FAST:-}
mkdir -p "$OUT"
chmod 700 "$OUT"

IS_ROOT=0; [ "$(id -u)" -eq 0 ] && IS_ROOT=1

# sec <file> <command...>: run a command and save its stdout/stderr, never abort
sec() {
  local f="$OUT/$1.txt"; shift
  { echo "### $*"; echo "### $(date -u +%FT%TZ)"; eval "$*" 2>&1 || echo "(exit $?)"; } > "$f"
}
have() { command -v "$1" >/dev/null 2>&1; }

echo "Host: $HOST   root: $IS_ROOT   out: $OUT"

# --- identity and hardware ---
sec 00_summary 'echo "hostname: $(hostname -f 2>/dev/null)"; echo "date: $(date -u +%FT%TZ)"; echo "uptime: $(uptime)"; echo "is_root: '"$IS_ROOT"'"; echo "virt: $(systemd-detect-virt 2>/dev/null)"'
sec 01_os 'cat /etc/os-release /etc/redhat-release /etc/oracle-release 2>/dev/null; uname -a; getenforce 2>/dev/null; cat /proc/cmdline'
sec 02_hardware 'lscpu | head -25; echo; free -h; echo; cat /proc/meminfo | head -5; echo; dmidecode -s system-product-name 2>/dev/null; dmidecode -s bios-version 2>/dev/null'

# --- storage ---
sec 10_block 'lsblk -o NAME,KNAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,ROTA,MODEL 2>/dev/null || lsblk'
sec 11_df 'df -hT -x tmpfs -x devtmpfs; echo; df -i -x tmpfs -x devtmpfs | head -30'
sec 12_fstab 'cat /etc/fstab; echo; findmnt -rn -o TARGET,SOURCE,FSTYPE,OPTIONS'
sec 13_lvm 'pvs 2>&1; vgs 2>&1; lvs -o lv_name,vg_name,lv_size,lv_attr,devices 2>&1'
sec 14_mdraid_multipath 'cat /proc/mdstat 2>/dev/null; multipath -ll 2>/dev/null | head -40'
sec 15_nfs_mounts 'grep -E " nfs| cifs| autofs" /proc/mounts; cat /etc/auto.master /etc/auto.* 2>/dev/null | grep -v "^#"'

# --- network, time, name resolution, authentication ---
sec 20_network 'ip -br addr; echo; ip route; echo; cat /etc/hosts; echo; cat /etc/resolv.conf; echo; nmcli -t -f NAME,DEVICE,TYPE con show 2>/dev/null'
sec 21_time 'cat /etc/chrony.conf 2>/dev/null | grep -v "^#" | grep -v "^$"; echo; chronyc tracking 2>/dev/null; echo; timedatectl 2>/dev/null; grep -v "^#" /etc/ntp.conf 2>/dev/null | grep -v "^$"'
sec 22_firewall 'systemctl is-active firewalld iptables 2>&1; iptables -S 2>/dev/null | head -50; firewall-cmd --list-all 2>/dev/null'
sec 23_auth_config 'cat /etc/nsswitch.conf | grep -v "^#"; echo; cat /etc/krb5.conf 2>/dev/null | grep -v "^#"; echo; sed -E "/(authtok|password|secret)/Id" /etc/sssd/sssd.conf 2>/dev/null; echo; ls /etc/pam.d | head -60; echo; realm list 2>/dev/null'
sec 24_ssh_config 'grep -v "^#" /etc/ssh/sshd_config | grep -v "^$"'
sec 25_sudoers 'cat /etc/sudoers 2>/dev/null | grep -v "^#" | grep -v "^$"; ls -la /etc/sudoers.d 2>/dev/null'
sec 26_certs_hint 'ls -la /etc/pki/ca-trust/source/anchors 2>/dev/null; ls /etc/pki/tls/certs 2>/dev/null | head -30; find /opt/cloudera/security -maxdepth 2 -type f 2>/dev/null | head -50'

# --- packages and repos ---
sec 30_rpm_all 'rpm -qa --qf "%{NAME}|%{VERSION}-%{RELEASE}|%{ARCH}|%{VENDOR}|%{PACKAGER}|%{INSTALLTIME:date}\n" | sort'
sec 31_rpm_not_distro 'rpm -qa --qf "%{NAME}-%{VERSION}-%{RELEASE}|%{VENDOR}\n" | grep -v -i -E "CentOS|Red Hat|Oracle|Rocky|AlmaLinux|Fedora|\(none\)$" | sort'
sec 32_repos 'ls -la /etc/yum.repos.d; echo; grep -h -E "^\[|^name|^baseurl|^mirrorlist|^enabled" /etc/yum.repos.d/*.repo'
sec 33_rpm_files_outside 'rpm -qa --qf "%{NAME}\n" | grep -i -E "cloudera|cdh|oracle|odbc|jdbc|mssql|mysql|postgres|splunk|filebeat|telegraf|zabbix|nagios|nrpe|datadog|commvault|veritas|tivoli|mcafee|crowdstrike|falcon|qualys|nessus|ossec|wazuh|aide"'

# --- services, scheduling, processes ---
sec 40_systemd_enabled 'systemctl list-unit-files --type=service --state=enabled --no-legend; echo; systemctl list-units --type=service --state=running --no-legend'
sec 41_systemd_custom 'ls -la /etc/systemd/system/*.service /etc/systemd/system/*/*.service 2>/dev/null | grep -v " -> /usr/lib" | head -60'
sec 42_init_scripts 'ls -la /etc/init.d 2>/dev/null; chkconfig --list 2>/dev/null | head -60'
sec 43_cron 'for u in $(cut -d: -f1 /etc/passwd); do c=$(crontab -l -u "$u" 2>/dev/null); [ -n "$c" ] && { echo "## user: $u"; echo "$c"; }; done; echo "## /etc/cron.d"; ls -la /etc/cron.d; cat /etc/cron.d/* 2>/dev/null | grep -v "^#"; echo "## anacron/daily"; ls /etc/cron.daily /etc/cron.hourly /etc/cron.weekly 2>/dev/null; echo "## at"; atq 2>/dev/null'
sec 44_listening 'ss -tlnp 2>/dev/null || netstat -tlnp'
sec 45_processes_by_user 'ps -eo user:20,pid,ppid,etime,pcpu,pmem,rss,args --sort=user | awk "NR==1 || (\$1!=\"root\")" | cut -c1-260'
sec 46_top_memory 'ps -eo user:20,pid,rss,pcpu,args --sort=-rss | head -25 | cut -c1-240'
sec 47_last_logins 'last -n 40 -w 2>/dev/null; echo; lastlog 2>/dev/null | grep -v "Never logged in" | head -60'

# --- users ---
sec 50_users 'awk -F: "(\$3>=1000 && \$3<65534) || \$3==0 {print \$1\":\"\$3\":\"\$4\":\"\$6\":\"\$7}" /etc/passwd; echo; echo "## groups of interest"; getent group wheel sudo hadoop supergroup hive impala hdfs yarn 2>/dev/null'
sec 51_home_dirs 'ls -la /home 2>/dev/null; ls -la /root | head -40'
sec 52_ulimits_sysctl 'sysctl -a 2>/dev/null | grep -E "^(vm\.(swappiness|max_map_count|overcommit_memory|dirty_ratio|dirty_background_ratio)|net\.core\.somaxconn|net\.ipv4\.(tcp_|ip_local_port_range)|kernel\.(shmmax|pid_max|threads-max)|fs\.(file-max|nr_open))"; echo; cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null; echo; cat /etc/security/limits.conf /etc/security/limits.d/* 2>/dev/null | grep -v "^#" | grep -v "^$"; echo; ls /etc/sysctl.d /etc/sysctl.conf; cat /etc/sysctl.d/*.conf /etc/sysctl.conf 2>/dev/null | grep -v "^#" | grep -v "^$"'

# --- language runtimes and custom software ---
sec 60_python 'for p in python python2 python2.7 python3 python3.6 python3.8 python3.9 python3.11; do command -v $p >/dev/null 2>&1 && echo "$p -> $(command -v $p) : $($p --version 2>&1)"; done; echo; alternatives --display python 2>/dev/null | head; echo; pip --version 2>&1; pip3 --version 2>&1'
sec 61_python_packages '(pip list 2>/dev/null | head -150); echo "## pip3"; (pip3 list 2>/dev/null | head -150)'
sec 62_conda_envs 'for c in /opt/miniconda* /opt/anaconda* /opt/conda* /home/*/miniconda* /home/*/anaconda* /home/*/.conda /usr/local/miniconda* ; do [ -d "$c" ] && { echo "## $c"; du -sh "$c" 2>/dev/null; ls "$c/envs" 2>/dev/null; }; done; command -v conda 2>&1'
sec 63_r_runtime 'command -v R && R --version | head -2; Rscript --version 2>&1; ls /usr/lib64/R/library /usr/local/lib/R/site-library /usr/lib/R/library 2>/dev/null | head -80'
sec 64_java 'for j in /usr/java/* /usr/lib/jvm/* /opt/jdk* /opt/java* /opt/cloudera/parcels/*/jdk* ; do [ -d "$j" ] && echo "$j"; done; echo; command -v java && java -version 2>&1; alternatives --display java 2>/dev/null | head -5; echo; echo "JAVA_HOME=${JAVA_HOME:-}"'
sec 65_other_runtimes 'for b in node npm perl ruby go php scala sbt mvn gradle docker podman git gcc make cmake; do command -v $b >/dev/null 2>&1 && echo "$b: $($b --version 2>&1 | head -1)"; done'
sec 66_db_clients 'ls -d /usr/lib/oracle* /opt/oracle* /usr/share/oracle* 2>/dev/null; rpm -qa | grep -i -E "oracle|odbc|unixODBC|mysql|postgresql|mssql|freetds|sqlite" ; ls /usr/share/java 2>/dev/null | grep -i -E "ojdbc|mysql|postgres|mssql|sqljdbc|terajdbc|hive|impala" ; cat /etc/odbc.ini /etc/odbcinst.ini 2>/dev/null | grep -v "^#"'
sec 67_custom_dirs 'ls -la /opt /usr/local /usr/local/bin /usr/local/sbin /srv /data 2>/dev/null | head -150'
sec 68_global_env 'ls -la /etc/profile.d; echo; cat /etc/environment 2>/dev/null'
if [ -z "$FAST" ]; then
  sec 69_dir_sizes 'du -xsh /opt/* /usr/local/* /data/* /home/* /var/lib/* /var/log/* 2>/dev/null | sort -rh | head -40'
fi

# --- Cloudera ---
sec 70_cloudera_agent 'systemctl status cloudera-scm-agent 2>&1 | head -6; grep -v "^#" /etc/cloudera-scm-agent/config.ini 2>/dev/null | grep -v "^$" | grep -v -i -E "password|secret"; echo; ls -la /var/lib/cloudera-scm-agent 2>/dev/null | head; cat /var/lib/cloudera-scm-agent/uuid 2>/dev/null'
sec 71_parcels 'ls -la /opt/cloudera/parcels 2>/dev/null; ls /opt/cloudera/parcel-repo 2>/dev/null; ls /opt/cloudera/csd 2>/dev/null'
sec 72_cloudera_roles_on_host 'ls /var/run/cloudera-scm-agent/process 2>/dev/null | sed -E "s/^[0-9]+-//" | sort -u'
sec 73_cloudera_data_dirs 'df -h 2>/dev/null | grep -E "dfs|data|kudu|kafka|zookeeper|impala|yarn|nn|dn"; ls -d /dfs/* /data/* /var/lib/zookeeper /var/lib/kudu* /var/lib/hadoop-* /var/lib/impala /var/local/kafka* 2>/dev/null'

# --- pack ---
tar -czf "$OUT.tgz" -C "$(dirname "$OUT")" "$(basename "$OUT")"
if [ -n "${GIVE_TO:-}" ] && [ "$IS_ROOT" -eq 1 ]; then
  chown "$GIVE_TO" "$OUT.tgz" && chmod 640 "$OUT.tgz"
else
  chmod 600 "$OUT.tgz"
fi
[ -z "${KEEP:-}" ] && rm -rf "$OUT"
echo "Done. Archive: $OUT.tgz ($(du -h "$OUT.tgz" | cut -f1))"
[ "$IS_ROOT" -eq 0 ] && echo "NOTE: not run as root, some sections are incomplete (lvs, ss -p, other users' crontabs)."
exit 0
