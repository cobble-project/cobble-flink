---
title: Introduction
nav_order: 2
---

# Introduction

Cobble Flink lets you use Cobble inside Flink in three common ways:

- as a **state backend** for Flink jobs
- as a **source** to read Cobble data in Flink SQL
- as a **sink** to write Flink SQL results into Cobble

## The Supported Usage Pattern

Using Cobble Flink usually means choosing one of these two packaging styles:

1. Put the runtime jar into the **Flink cluster**
2. Add Maven dependencies in the **user job**

For most users, the runtime jar in the Flink distribution is the simplest
choice. Job-side Maven dependencies are an alternative when you prefer to carry
the integration together with the job. In most cases, choosing one of the two
is enough.

When choosing artifacts from a Maven repository, make sure you pick the correct
version. The current naming rule is:

```text
${cobble-version}-{minor-version}-flink-{flink-minor-version}
```

Example:

```text
0.2.0-1-flink-1.17
```

## When To Use Which Part

- Use **State Backend** when your Flink job keeps state and you want that state
  stored in Cobble.
- Use **Source** when you already have data in Cobble and want to read it from
  Flink SQL.
- Use **Sink** when you want Flink SQL to write results into a Cobble table.

## Current Scope

- Target Flink version: `1.17.0` and later
- State backend: for stateful Flink jobs
- Source: for reading Cobble tables in Flink SQL
- Sink: for writing keyed Flink SQL tables into Cobble

For concrete setup and examples, continue with
[Getting Started](../getting-started/).
