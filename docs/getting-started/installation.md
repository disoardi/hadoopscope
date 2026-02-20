# Installation

## Option 1: Install Script (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/disoardi/hadoopscope/main/install.sh | bash
```

This installs HadoopScope to `~/.hadoopscope/repo/` and creates a `hadoopscope` wrapper in `~/.local/bin/`.

To update later:

```bash
hadoopscope-install --update
# or
~/.hadoopscope/repo/install.sh --update
```

## Option 2: Git Clone (direct)

```bash
git clone https://github.com/disoardi/hadoopscope
cd hadoopscope
python3 hadoopscope.py --help
```

## Option 3: TuxBox

```bash
tbox run hadoopscope -- --help
```

## Option 4: Docker

```bash
docker run --rm ghcr.io/disoardi/hadoopscope:latest --help
```

## Requirements

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.6+ | stdlib only for core checks |
| PyYAML | optional | automatic manual parser fallback |
| Ansible | optional | only for Hive checks via beeline |
| Docker | optional | alternative to Ansible for Hive checks |
| zabbix_sender | optional | only for Zabbix alerts |

## Verifying Installation

```bash
python3 hadoopscope.py --version
python3 hadoopscope.py --show-capabilities
```
