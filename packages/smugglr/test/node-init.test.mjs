// A bare `Smugglr.init()` in Node, with no `setWasm()` call, against a local
// SqlExecutor and a local generic-profile HTTP-SQL endpoint (#437).
//
// Before the fix, this failed with `fetch failed`: the wasm-bindgen loader
// resolves the .wasm file relative to its glue module and fetches it, and
// Node's fetch has no `file:` scheme. init() now detects the Node runtime
// and reads the bundled binary itself.
//
// Uses node:sqlite on both sides (no better-sqlite3 build step needed) --
// the same request/response shape docs/examples/node-server-to-d1's
// local-endpoint.mjs answers.
//
// Run against the built package: `pnpm build && node --test test/`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";
import { Smugglr } from "../dist/index.js";

const SCHEMA = `
  CREATE TABLE widgets (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  );
`;

function query(db, sql, params) {
  const stmt = db.prepare(sql);
  stmt.setReturnArrays(true);
  const rows = stmt.all(...params);
  return { columns: stmt.columns().map((c) => c.name), rows };
}

test("Smugglr.init() loads its own wasm in Node and pushes a row", async () => {
  const source = new DatabaseSync(":memory:");
  source.exec(SCHEMA);
  source.exec(
    "INSERT INTO widgets (id, name, updated_at) VALUES " +
      "('01991000-0000-7000-8000-000000000001', 'sprocket', 1756411200)",
  );
  const executor = {
    async run(sql, params) {
      return query(source, sql, params);
    },
  };

  const dest = new DatabaseSync(":memory:");
  dest.exec(SCHEMA);

  const server = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    const { sql, params = [] } = JSON.parse(body);
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(query(dest, sql, params)));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    // No setWasm() call: the whole point of #437 is that init() loads the
    // bundled .wasm itself under Node.
    const s = await Smugglr.init({
      source: { type: "local", executor },
      dest: { url: `http://127.0.0.1:${port}`, profile: "generic" },
      sync: { tables: ["widgets"], conflictResolution: "local_wins" },
    });
    try {
      const result = await s.push();
      assert.equal(result.status, "ok");
      const table = result.tables.find((t) => t.name === "widgets");
      assert.ok(table, "widgets table missing from push result");
      assert.equal(table.rowsPushed, 1);
    } finally {
      s.dispose();
    }
  } finally {
    server.close();
  }

  const pushed = query(dest, "SELECT id, name FROM widgets WHERE id = ?", [
    "01991000-0000-7000-8000-000000000001",
  ]);
  assert.equal(pushed.rows.length, 1);
  assert.deepEqual(pushed.rows[0], ["01991000-0000-7000-8000-000000000001", "sprocket"]);
});
