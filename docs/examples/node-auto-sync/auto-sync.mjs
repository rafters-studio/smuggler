// Long-running sync loop with exponential backoff and a clean SIGTERM.
//
// Run: node --env-file=.env auto-sync.mjs
//
// The package's own `autoSync` config (0.4.0) is browser-only: it hangs off
// navigator.locks and the `online` event, and is a no-op in Node. This loop
// is the Node equivalent.

import Database from "better-sqlite3";
import { Smugglr, SmugglrError } from "smugglr";

for (const key of ["DEST_URL", "LOCAL_DB"]) {
  if (!process.env[key]) {
    console.error(`missing env var: ${key}`);
    process.exit(2);
  }
}

const db = new Database(process.env.LOCAL_DB);
const executor = {
  async run(sql, params) {
    const stmt = db.prepare(sql);
    if (stmt.reader) {
      return { columns: stmt.columns().map((c) => c.name), rows: stmt.raw().all(params) };
    }
    stmt.run(params);
    return { columns: [], rows: [] };
  },
};

const s = await Smugglr.init({
  source: { type: "local", executor },
  dest: {
    url: process.env.DEST_URL,
    authToken: process.env.DEST_TOKEN,
    profile: process.env.DEST_PROFILE ?? "generic",
  },
  sync: { conflictResolution: "newer_wins" },
});

const tickInterval = Number(process.env.SYNC_INTERVAL_MS ?? 30_000);
const minBackoff = 1_000;
const maxBackoff = 5 * 60_000;
let backoff = minBackoff;
let inflight = null; // the sync() promise while one is running
let timer = null; // the pending setTimeout for the next tick
let stopping = false;

function ts() {
  return new Date().toISOString().slice(11, 23);
}

// One tick schedules the next only after its own sync() settles, so two
// sync() calls are never in flight at once. That matters: the WASM instance
// borrows its endpoints across awaits and does not tolerate a concurrent call.
async function tick() {
  let delay = tickInterval;
  inflight = s.sync();
  try {
    const result = await inflight;
    const rows = result.tables.reduce(
      (n, t) => n + (t.rowsPushed ?? 0) + (t.rowsPulled ?? 0),
      0,
    );
    console.log(`[${ts()}] sync ok: ${rows} rows`);
    backoff = minBackoff;
  } catch (err) {
    if (err instanceof SmugglrError && err.retryable) {
      console.warn(`[${ts()}] sync failed (retryable): ${err.message} -- backing off ${backoff}ms`);
      delay = backoff;
      backoff = Math.min(backoff * 2, maxBackoff);
    } else {
      console.error(`[${ts()}] sync failed (fatal):`, err);
      close(1);
      return;
    }
  } finally {
    inflight = null;
  }
  if (!stopping) timer = setTimeout(tick, delay);
}

function close(code) {
  stopping = true;
  clearTimeout(timer);
  s.dispose();
  db.close();
  process.exit(code);
}

// A signal cancels the next tick and waits for the current sync() to settle,
// so no batch is cut off mid-write. The tick's own handler logs the outcome.
async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  console.log(`[${ts()}] received ${signal}, stopping after the current sync`);
  clearTimeout(timer);
  await inflight?.catch(() => {});
  close(0);
}
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

tick();
