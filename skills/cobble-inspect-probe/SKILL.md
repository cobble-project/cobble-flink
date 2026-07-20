---
name: cobble-inspect-probe
description: Open the Cobble Flink Java Inspect SDK and perform a safe, bounded, read-only probe of a sink table, checkpoint, savepoint, operator path, metadata file, or shard snapshot. Use when asked to discover inspectable checkpoints, list operators and state targets, generate Overview SQL, sample decoded rows, diagnose decode issues, or verify that a Cobble path can be consumed without starting the web monitor.
---

# Cobble Inspect Probe

Use `cobble-flink-inspect` as the source of truth. Do not parse checkpoint files or Cobble
manifests manually when the SDK can discover them.

## Workflow

1. Locate the `cobble-flink` repository root and run probe work from that checkout.
2. Read `references/sdk-api.md` before writing probe code. Recheck the public classes when the
   workspace has changed since this skill was written.
3. Collect only the inputs that matter:
   - source path or URI;
   - `latest` or an exact checkpoint/snapshot ID;
   - optional operator ID;
   - optional Flink configuration directory and `storage.option.*` values;
   - optional trusted user jars for custom serializers.
4. Build a `CobbleInspectClient`, call `discover(path)`, then open an `InspectSession` with
   `InspectSelection.latest(...)` or `InspectSelection.checkpoint(...)`.
5. Use try-with-resources for both client and session. The SDK owns native readers, temporary
   files, and user-jar classloaders.
6. Report discovery before reading rows:
   - normalized source kind/root;
   - checkpoint IDs and operators;
   - pinned checkpoint/operator from `session.info()`;
   - Overview items and generated source DDL;
   - target ID, kind, state kind, semantic fields, and lookup capability.
7. Probe at most one page and 20 rows per requested target by default. Continue pagination only
   when the user explicitly requests a full scan.
8. Summarize decoded values and `decodeIssues()`. Keep raw bytes available for diagnosis, but do
   not print large payloads, credentials, or complete binary values by default.
9. Report `InspectException.errorCode()` and its concise message. For `latest`, reopen a fresh
   session if a later operation reports `CHECKPOINT_UNAVAILABLE`; never silently switch an exact
   checkpoint selection.

## Guardrails

- Inspection is read-only. Do not create, modify, expire, or materialize snapshots from probe
  code.
- Prefer `latest` for an operational probe and an exact ID for reproducible validation.
- Never guess an operator when discovery lists more than one. Report the choices or use the
  operator requested by the user.
- Use typed lookup only when every required field reported by `SourceSqlExample` is available.
  Otherwise use a bounded scan; do not fabricate key values.
- Add only trusted user jars. First probe without them so the report distinguishes classless
  decoding from user-classpath-assisted decoding.
- Preserve row-level decode failures. A decode issue is evidence, not a reason to discard the
  remaining rows.

## Validation

After changing this skill, run:

```bash
python3 skills/validate.py skills/cobble-inspect-probe
```

For a real probe, compile against the current reactor or the documented released SDK version and
verify that the process exits with all clients/sessions closed.
