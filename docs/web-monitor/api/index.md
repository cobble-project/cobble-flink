---
title: HTTP API
parent: Web Monitor
nav_order: 3
---

# HTTP API

Use the monitor HTTP API when another program needs to discover and inspect
Cobble checkpoints or datasource snapshots. The API is read-only and uses
pinned sessions, so one client cannot change another client's selected path or
checkpoint.

The monitor binds to `127.0.0.1` by default. It has no authentication. Do not
bind it to a public interface unless access is protected by your deployment.

## Session Workflow

Set the server address once:

```bash
export COBBLE_MONITOR=http://127.0.0.1:8088
```

Discover a path without opening a reader:

```bash
curl -sS "$COBBLE_MONITOR/api/v1/discovery" \
  -H 'Content-Type: application/json' \
  -d '{"source":"file:///path/to/checkpoints"}'
```

Open `latest`. The returned `checkpoint_id` is concrete and remains pinned for
the lifetime of this session:

```bash
curl -sS "$COBBLE_MONITOR/api/v1/sessions" \
  -H 'Content-Type: application/json' \
  -d '{
    "source":"file:///path/to/checkpoints",
    "checkpoint":"latest",
    "operator_id":"<operator-id>"
  }'
```

Save the returned `session_id`, then read its Overview and generated SQL:

```bash
curl -sS "$COBBLE_MONITOR/api/v1/sessions/$SESSION_ID/overview"
```

Close the session when finished:

```bash
curl -sS -X DELETE "$COBBLE_MONITOR/api/v1/sessions/$SESSION_ID"
```

Sessions also expire after the configured idle timeout. A deleted or expired
session returns HTTP `410`.

## Scan

Scan a target returned by session creation:

```bash
curl -sS "$COBBLE_MONITOR/api/v1/sessions/$SESSION_ID/scan" \
  -H 'Content-Type: application/json' \
  -d '{"target_id":"orders","limit":50}'
```

When `next_page_token` is not null, pass it back unchanged:

```json
{
  "target_id": "orders",
  "limit": 50,
  "page_token": "<opaque-token>"
}
```

The token belongs to that session, target, projection, and filter. Do not parse
or reuse it with a different request.

Raw scans accept `bucket`, `prefix_b64`, and `columns`. Schema-aware sink and
state scans accept ordered typed fields. This sink example scans an `id`
prefix:

```json
{
  "target_id": "orders",
  "limit": 50,
  "filter": {
    "kind": "sink",
    "fields": [
      {"name":"id","value":{"kind":"INTEGER","value":"9223372036854775807"}}
    ]
  }
}
```

Use a decimal string for `INTEGER` when a 64-bit value may exceed the exact
integer range of your JSON client.

## Exact Lookup

Look up one or more raw keys in one request:

```bash
curl -sS "$COBBLE_MONITOR/api/v1/sessions/$SESSION_ID/lookup" \
  -H 'Content-Type: application/json' \
  -d '{
    "target_id":"raw",
    "keys":[
      {"kind":"raw","bucket":0,"key_b64":"YWxwaGE="}
    ]
  }'
```

For a typed sink lookup, provide every primary-key field in schema order:

```json
{
  "target_id": "orders",
  "keys": [{
    "kind": "sink",
    "fields": [
      {"name":"tenant","value":{"kind":"STRING","value":"east"}},
      {"name":"id","value":{"kind":"INTEGER","value":42}}
    ]
  }]
}
```

Typed state lookup uses `kind = "state"` and a `state_key` containing the
complete state key, namespace when present, and MapState key when required.
Session target metadata and Overview SQL list the required fields.

Each result has `found`, raw base64 fields, decoded values when available, and
`decode_issues`. A decode issue belongs to that row and does not fail the whole
scan or lookup request.

## Errors And Limits

Errors use a stable envelope:

```json
{
  "code": "UNKNOWN_TARGET",
  "message": "Unknown inspect target: missing",
  "request_id": "..."
}
```

Keep `request_id` when reporting a failed request. The server limits request
body size, scan rows, lookup batch size, open sessions, and idle time. These can
be adjusted with the monitor's `--request-body-max-bytes`,
`--inspect-max-limit`, `--lookup-max-keys`, `--max-sessions`, and
`--session-idle-timeout-seconds` options.

The complete machine-readable contract is served from the monitor jar at
`/openapi/cobble-inspect-v1.yaml`.
