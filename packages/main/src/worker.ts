import * as SQLite from "wa-sqlite";
import SQLiteESMFactory from "wa-sqlite/dist/wa-sqlite.mjs";
import { OPFSWriteAheadVFS } from "wa-sqlite/src/examples/OPFSWriteAheadVFS.js";
import { TestObject } from "./types";
import { LineString } from "geojson";
import { simplifyFeature, SPATIAL_SIMPLIFY_UDF_NAME } from "./simplify-json";

async function initDatabase(): Promise<{ sqlite3: SQLiteAPI; db: number }> {
  const module = await SQLiteESMFactory();
  const sqlite3 = SQLite.Factory(module);
  const vfs = await OPFSWriteAheadVFS.create("write-ahead-vfs", module);
  sqlite3.vfs_register(vfs, true);
  const db = await sqlite3.open_v2("test");
  return { sqlite3, db };
}

function generateTestObjects(n: number): TestObject[] {
  const geometry: LineString = {
    type: "LineString",
    coordinates: [
      [-122.4194, 37.7749],
      [-122.418, 37.7755],
      [-122.4165, 37.776],
      [-122.415, 37.7768],
      [-122.4135, 37.7775],
      [-122.412, 37.778],
      [-122.4105, 37.7788],
      [-122.409, 37.7795],
      [-122.4075, 37.78],
      [-122.406, 37.7808],
      [-122.4045, 37.7815],
      [-122.403, 37.782],
      [-122.4015, 37.7828],
      [-122.4, 37.7835],
      [-122.3985, 37.784],
    ],
  };
  const text = "The quick brown fox jumps over the lazy dog nearby";
  return Array.from({ length: n }, (_, i) => ({
    id: crypto.randomUUID(),
    geometry,
    text,
    name: `Object-${i}-${Date.now()}`,
    description:
      "A test object used for validating database write performance and integrity under concurrent load conditions",
    category: [
      "transport",
      "infrastructure",
      "environment",
      "survey",
      "utility",
    ][i % 5],
    tags: ["field-collected", "gps-tracked", "auto-generated", "batch-insert"],
    priority: (i % 10) + 1,
    score: Math.round(i * 3.14159 * 100) / 100,
    latitude: 37.7749 + i * 0.0001,
    longitude: -122.4194 + i * 0.0001,
    altitude: 10.5 + (i % 200),
    speed: (i % 120) * 0.44704,
    heading: (i * 7) % 360,
    accuracy: 3.5 + (i % 15),
    timestamp: new Date(1700000000000 + i * 1000).toISOString(),
    createdAt: new Date(1700000000000 + i * 500).toISOString(),
    updatedAt: new Date(1700000000000 + i * 800).toISOString(),
    expiresAt: new Date(1700000000000 + i * 1000 + 86400000).toISOString(),
    isActive: i % 3 !== 0,
    isVerified: i % 5 === 0,
    isPublic: i % 2 === 0,
    version: 1,
    revision: i % 8,
    parentId: i % 10 === 0 ? null : crypto.randomUUID(),
    ownerId: crypto.randomUUID(),
    groupId: crypto.randomUUID(),
    sourceUrl: `https://example.com/api/v2/objects/${i}`,
    checksum: `sha256-${i.toString(16).padStart(64, "0")}`,
    mimeType: "application/geo+json",
    encoding: "utf-8",
    sizeBytes: 1024 + ((i * 37) % 65536),
    durationMs: (i * 123) % 300000,
    metadata: {
      source: "field-device-alpha",
      firmware: "v2.8.1",
      batchId: `batch-${Math.floor(i / 100)}`,
      region: ["us-west", "us-east", "eu-central", "ap-south"][i % 4],
      quality: ["high", "medium", "low"][i % 3],
    },
  }));
}

async function createTable(sqlite3: SQLiteAPI, db: number): Promise<void> {
  const createTableSQL = `
    CREATE TABLE IF NOT EXISTS test_objects (
      id TEXT PRIMARY KEY,
      data JSON
    )
  `;
  await sqlite3.exec(db, "BEGIN IMMEDIATE");
  await sqlite3.exec(db, createTableSQL);
  await sqlite3.exec(db, "COMMIT");
}

async function createValueIndexes(
  sqlite3: SQLiteAPI,
  db: number,
): Promise<void> {
  const createIdIndexSQL = `
    CREATE INDEX IF NOT EXISTS idx_test_objects_id ON test_objects (json_extract(data, '$.id'));
  `;
  const createUpdatedAtIndexSQL = `
    CREATE INDEX IF NOT EXISTS idx_test_objects_updatedAt ON test_objects (json_extract(data, '$.updatedAt'));
  `;
  await sqlite3.exec(db, "BEGIN IMMEDIATE");
  await sqlite3.exec(db, createIdIndexSQL);
  await sqlite3.exec(db, createUpdatedAtIndexSQL);
  await sqlite3.exec(db, "COMMIT");
}

async function createGeometryIndex(
  sqlite3: SQLiteAPI,
  db: number,
): Promise<void> {
  await sqlite3.exec(db, "BEGIN IMMEDIATE");

  // Geopoly virtual table for spatial queries
  await sqlite3.exec(
    db,
    `CREATE VIRTUAL TABLE IF NOT EXISTS geo_spatial USING geopoly (
      object_id TEXT NOT NULL,
      index_name TEXT NOT NULL
    )`,
  );

  // Mapping table to track which geopoly rows belong to which objects
  await sqlite3.exec(
    db,
    `CREATE TABLE IF NOT EXISTS geo_spatial_mapping (
      geo_rowid INTEGER NOT NULL,
      object_id TEXT NOT NULL,
      index_name TEXT NOT NULL,
      PRIMARY KEY (geo_rowid, object_id, index_name)
    )`,
  );

  await sqlite3.exec(
    db,
    `CREATE INDEX IF NOT EXISTS idx_geo_mapping_object
     ON geo_spatial_mapping (object_id, index_name)`,
  );

  await sqlite3.exec(
    db,
    `CREATE INDEX IF NOT EXISTS idx_geo_mapping_index
     ON geo_spatial_mapping (index_name)`,
  );

  // Trigger: INSERT — when a new row is inserted with non-null data, add its geometry
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_spatial_insert
     AFTER INSERT ON test_objects
     WHEN NEW.data IS NOT NULL
     BEGIN
       INSERT INTO geo_spatial (_shape, object_id, index_name)
       SELECT
         convert_to_simple_polygon(json_extract(NEW.data, '$.geometry')),
         NEW.id,
         'geometry'
       WHERE convert_to_simple_polygon(json_extract(NEW.data, '$.geometry')) IS NOT NULL;

       INSERT INTO geo_spatial_mapping (geo_rowid, object_id, index_name)
       SELECT last_insert_rowid(), NEW.id, 'geometry'
       WHERE changes() > 0;
     END`,
  );

  // Trigger: UPDATE with non-null data — remove old geometry, insert new
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_spatial_update_not_null
     AFTER UPDATE OF data ON test_objects
     WHEN NEW.data IS NOT NULL
     BEGIN
       DELETE FROM geo_spatial
       WHERE rowid IN (
         SELECT geo_rowid FROM geo_spatial_mapping
         WHERE object_id = NEW.id AND index_name = 'geometry'
       );

       DELETE FROM geo_spatial_mapping
       WHERE object_id = NEW.id AND index_name = 'geometry';

       INSERT INTO geo_spatial (_shape, object_id, index_name)
       SELECT
         convert_to_simple_polygon(json_extract(NEW.data, '$.geometry')),
         NEW.id,
         'geometry'
       WHERE convert_to_simple_polygon(json_extract(NEW.data, '$.geometry')) IS NOT NULL;

       INSERT INTO geo_spatial_mapping (geo_rowid, object_id, index_name)
       SELECT last_insert_rowid(), NEW.id, 'geometry'
       WHERE changes() > 0;
     END`,
  );

  // Trigger: DELETE — clean up spatial data when a row is removed
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_spatial_delete
     AFTER DELETE ON test_objects
     BEGIN
       DELETE FROM geo_spatial
       WHERE rowid IN (
         SELECT geo_rowid FROM geo_spatial_mapping
         WHERE object_id = OLD.id AND index_name = 'geometry'
       );

       DELETE FROM geo_spatial_mapping
       WHERE object_id = OLD.id AND index_name = 'geometry';
     END`,
  );

  // Backfill existing rows
  await sqlite3.exec(
    db,
    `INSERT INTO geo_spatial (_shape, object_id, index_name)
     SELECT
       convert_to_simple_polygon(json_extract(data, '$.geometry')),
       id,
       'geometry'
     FROM test_objects
     WHERE data IS NOT NULL
       AND convert_to_simple_polygon(json_extract(data, '$.geometry')) IS NOT NULL`,
  );

  await sqlite3.exec(
    db,
    `INSERT INTO geo_spatial_mapping (geo_rowid, object_id, index_name)
     SELECT s.rowid, s.object_id, s.index_name
     FROM geo_spatial s
     WHERE s.index_name = 'geometry'
       AND s.rowid > COALESCE(
         (SELECT MAX(geo_rowid) FROM geo_spatial_mapping WHERE index_name = 'geometry'),
         0
       )`,
  );

  await sqlite3.exec(db, "COMMIT");
}

async function createFullTextIndex(
  sqlite3: SQLiteAPI,
  db: number,
): Promise<void> {
  await sqlite3.exec(db, "BEGIN IMMEDIATE");

  // FTS5 virtual table for full-text search on description
  await sqlite3.exec(
    db,
    `CREATE VIRTUAL TABLE IF NOT EXISTS fulltext_search USING fts5 (
      fulltext_data,
      prefix='2, 3, 4',
      tokenize='trigram',
      object_id UNINDEXED,
      index_name UNINDEXED
    )`,
  );

  // Mapping table to track which FTS rows belong to which objects
  await sqlite3.exec(
    db,
    `CREATE TABLE IF NOT EXISTS fulltext_mapping (
      fulltext_rowid INTEGER NOT NULL,
      object_id TEXT NOT NULL,
      index_name TEXT NOT NULL,
      PRIMARY KEY (fulltext_rowid, object_id, index_name)
    )`,
  );

  await sqlite3.exec(
    db,
    `CREATE INDEX IF NOT EXISTS idx_fulltext_mapping_object
     ON fulltext_mapping (object_id, index_name)`,
  );

  await sqlite3.exec(
    db,
    `CREATE INDEX IF NOT EXISTS idx_fulltext_mapping_index
     ON fulltext_mapping (index_name)`,
  );

  // Trigger: INSERT — index description when a row is inserted
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_fulltext_insert
     AFTER INSERT ON test_objects
     WHEN NEW.data IS NOT NULL
       AND json_extract(NEW.data, '$.description') IS NOT NULL
     BEGIN
       INSERT INTO fulltext_search (fulltext_data, object_id, index_name)
       VALUES (json_extract(NEW.data, '$.description'), NEW.id, 'description');

       INSERT INTO fulltext_mapping (fulltext_rowid, object_id, index_name)
       VALUES (last_insert_rowid(), NEW.id, 'description');
     END`,
  );

  // Trigger: UPDATE — re-index description when data changes
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_fulltext_update
     AFTER UPDATE OF data ON test_objects
     WHEN NEW.data IS NOT NULL
       AND json_extract(NEW.data, '$.description') IS NOT NULL
     BEGIN
       DELETE FROM fulltext_search
       WHERE rowid IN (
         SELECT fulltext_rowid FROM fulltext_mapping
         WHERE object_id = NEW.id AND index_name = 'description'
       );

       DELETE FROM fulltext_mapping
       WHERE object_id = NEW.id AND index_name = 'description';

       INSERT INTO fulltext_search (fulltext_data, object_id, index_name)
       VALUES (json_extract(NEW.data, '$.description'), NEW.id, 'description');

       INSERT INTO fulltext_mapping (fulltext_rowid, object_id, index_name)
       VALUES (last_insert_rowid(), NEW.id, 'description');
     END`,
  );

  // Trigger: DELETE — clean up FTS data when a row is removed
  await sqlite3.exec(
    db,
    `CREATE TRIGGER IF NOT EXISTS trigger_fulltext_delete
     AFTER DELETE ON test_objects
     BEGIN
       DELETE FROM fulltext_search
       WHERE rowid IN (
         SELECT fulltext_rowid FROM fulltext_mapping
         WHERE object_id = OLD.id AND index_name = 'description'
       );

       DELETE FROM fulltext_mapping
       WHERE object_id = OLD.id AND index_name = 'description';
     END`,
  );

  // Backfill existing rows
  await sqlite3.exec(
    db,
    `INSERT INTO fulltext_search (fulltext_data, object_id, index_name)
     SELECT
       json_extract(data, '$.description'),
       id,
       'description'
     FROM test_objects
     WHERE data IS NOT NULL
       AND json_extract(data, '$.description') IS NOT NULL`,
  );

  await sqlite3.exec(
    db,
    `INSERT INTO fulltext_mapping (fulltext_rowid, object_id, index_name)
     SELECT s.rowid, s.object_id, s.index_name
     FROM fulltext_search s
     WHERE s.index_name = 'description'`,
  );

  await sqlite3.exec(db, "COMMIT");
}

function registerUDF(sqlite3: SQLiteAPI, db: number): void {
  sqlite3.create_function(
    db,
    SPATIAL_SIMPLIFY_UDF_NAME,
    1, // nArg
    SQLite.SQLITE_UTF8 | SQLite.SQLITE_DETERMINISTIC,
    0, // pApp
    (context: number, values: Uint32Array) => {
      const geometryJson = sqlite3.value(values[0]) as string | null;
      const result = simplifyFeature(geometryJson as string);
      if (result === null) {
        sqlite3.result_null(context);
      } else {
        sqlite3.result_text(context, result);
      }
    },
    undefined, // xStep (not an aggregate)
    undefined, // xFinal (not an aggregate)
  );
}

let batchCounter = 0;
async function storeTestObjects(
  sqlite3: SQLiteAPI,
  db: number,
  testObjects: TestObject[],
  mutex: ConnectionMutex,
): Promise<void> {
  await mutex.lock();
  console.log(`Storing batch ${batchCounter++}...`);
  try {
    await sqlite3.exec(db, "BEGIN IMMEDIATE");

    const iter = testObjects.values();
    for (
      let chunk = iter.take(1000).toArray();
      chunk.length > 0;
      chunk = iter.take(1000).toArray()
    ) {
      const placeholders = chunk.map(() => "(?, json(?))").join(", ");
      const sql = `INSERT OR REPLACE INTO test_objects (id, data) VALUES ${placeholders}`;
      const bindings = chunk.flatMap((obj) => [obj.id, JSON.stringify(obj)]);

      for await (const stmt of sqlite3.statements(db, sql)) {
        sqlite3.bind_collection(stmt, bindings);
        await sqlite3.step(stmt);
      }
    }

    await sqlite3.exec(db, "COMMIT");

    // Try manually intiating a checkpoint as autocheckpoint can't keep
    // up with the write volume.
    await sqlite3.exec(db, "PRAGMA wal_checkpoint");
  } catch (err) {
    await sqlite3.exec(db, "ROLLBACK").catch(() => {});
    throw err;
  } finally {
    mutex.unlock();
  }
}

async function runReadQuery(sqlite3: SQLiteAPI, db: number): Promise<void> {
  const sql =
    "SELECT * FROM test_objects WHERE json_extract(data, '$.geometry') IS NOT NULL LIMIT 2000 ORDER BY id DESC";
  for await (const stmt of sqlite3.statements(db, sql)) {
    while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
      const id = sqlite3.column_text(stmt, 0);
      const data = JSON.parse(sqlite3.column_text(stmt, 1));
      console.log({ id, data });
    }
  }
}

class ConnectionMutex {
  #promise?: Promise<void>;
  #resolve?: () => void;

  async lock(): Promise<void> {
    while (this.#promise) {
      await this.#promise;
    }

    this.#promise = new Promise((resolve) => {
      this.#resolve = resolve;
    });
  }

  unlock(): void {
    const resolve = this.#resolve;

    this.#promise = undefined;
    this.#resolve = undefined;

    resolve?.();
  }
}

async function main(): Promise<void> {
  const { sqlite3, db } = await initDatabase();
  registerUDF(sqlite3, db);
  const mutex = new ConnectionMutex();
  await sqlite3.exec(db, "PRAGMA wal_autocheckpoint=0"); // disable auto-checkpoint
  await createTable(sqlite3, db);
  await createValueIndexes(sqlite3, db);
  await createGeometryIndex(sqlite3, db);
  await createFullTextIndex(sqlite3, db);

  let errored = false;
  try {
    const promises: Promise<void>[] = [];
    for (let i = 0; i < 16383; i++) {
      if (errored) {
        break;
      }

      const testObjects = generateTestObjects(3000);
      await new Promise((resolve) => setTimeout(resolve, 100));
      promises.push(storeTestObjects(sqlite3, db, testObjects, mutex));
    }
    await Promise.all(promises);

    await runReadQuery(sqlite3, db);
  } catch (err) {
    console.error("Error during database operations:", err);
    await sqlite3.close(db);
  }
}

self.onmessage = () => {
  main();
};
