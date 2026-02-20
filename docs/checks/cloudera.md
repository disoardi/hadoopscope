# Cloudera Manager Checks (CDP)

These checks target **CDP clusters** managed by Cloudera Manager.

Set `type: cdp` in your environment config to activate these checks instead of Ambari checks.

## ClouderaServiceHealthCheck

Checks the health summary of all CDP services.

**API:** `GET /api/{version}/clusters/{name}/services`

Health states:
- `GOOD` → OK
- `CONCERNING` → WARNING
- `BAD` → CRITICAL
- `NOT_AVAILABLE`, `HISTORY_NOT_AVAILABLE` → WARNING

| Result | Condition |
|--------|-----------|
| OK | All services GOOD |
| WARNING | ≥1 service CONCERNING or NOT_AVAILABLE |
| CRITICAL | ≥1 service BAD |
| UNKNOWN | Cannot reach CM API |

**Config:**
```yaml
environments:
  prod-cdp:
    type: cdp
    cm_url: https://cm.corp.com:7180
    cm_user: admin
    cm_pass: "${CM_PASS}"
    cluster_name: prod-cdp-cluster
    cm_api_version: "v40"
```

## ClouderaParcelCheck

Verifies that all parcels (software distributions) are in `ACTIVATED` state.

**API:** `GET /api/{version}/clusters/{name}/parcels`

Parcel stages:
- `ACTIVATED` → OK
- Any other stage → WARNING (DOWNLOADING, DISTRIBUTING, etc.)

| Result | Condition |
|--------|-----------|
| OK | All parcels ACTIVATED |
| WARNING | ≥1 parcel not in ACTIVATED state |
| UNKNOWN | Cannot reach CM API |
