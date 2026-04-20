# wa-sqlite Error Reproduction Tests

A pnpm monorepo for stress-testing [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) with the `OPFSWriteAheadVFS`, focusing on high-volume writes with spatial (geopoly) and full-text (FTS5) indexes.

## How It Works

A web worker initializes a wa-sqlite database backed by OPFS using the `OPFSWriteAheadVFS`. On start, it:

1. Creates a `test_objects` table storing JSON blobs keyed by UUID.
2. Creates value indexes on `$.id` and `$.updatedAt`.
3. Creates a **geopoly spatial index** on `$.geometry` — a `LineString` is simplified into a polygon via a custom UDF (`convert_to_simple_polygon`) using Turf.js, stored in a geopoly virtual table with insert/update/delete triggers.
4. Creates an **FTS5 full-text index** on `$.description` using a trigram tokenizer, also maintained via triggers.
5. Generates batches of 3,000 test objects (each with 30+ fields including geometry, timestamps, metadata, etc.) and inserts them in a loop with serialized transactions (`BEGIN IMMEDIATE` / `COMMIT`).

All writes go through a `ConnectionMutex` to serialize access to the single database connection. Triggers automatically keep the spatial and full-text indexes in sync on every insert/update/delete.

## Setup

```bash
pnpm install
```

## Build

```bash
pnpm --filter main build
```

## Run

```bash
pnpm --filter main preview
```

Then open the URL shown in the terminal (usually `http://localhost:4173`). Click the **Start Test** button to begin the write loop. Open the browser DevTools console to see progress and any errors.

> **Note:** This requires a Chromium browser (Chrome/Edge) — `OPFSWriteAheadVFS` depends on the `readwrite-unsafe` OPFS mode which is Chromium-only.
