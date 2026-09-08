// setWasm() must pass `{ module_or_path }` to the wasm-bindgen initializer,
// not the bare input (#437). The bare form is wasm-bindgen's deprecated
// calling convention and logs `console.warn('using deprecated parameters
// for the initialization function; pass a single object instead')` on every
// call.
//
// This is its own test file (rather than a case inside node-init.test.mjs)
// because wasm-bindgen's `__wbg_init` short-circuits on `if (wasm !==
// undefined) return wasm` -- once a module is initialized once in a
// process, a later bare-vs-wrapped call never reaches the warn branch to
// prove anything. `node --test` runs each file in its own process, so this
// file's module state starts fresh.
//
// Run against the built package: `pnpm build && node --test test/`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { setWasm } from "../dist/index.js";
import * as wasm from "../dist/wasm/smugglr_wasm.js";

test("setWasm() does not trip wasm-bindgen's deprecation warning", async () => {
  const bytes = await readFile(
    new URL("../dist/wasm/smugglr_wasm_bg.wasm", import.meta.url),
  );

  const warnings = [];
  const originalWarn = console.warn;
  console.warn = (...args) => warnings.push(args.join(" "));

  try {
    // Pre-fix, this passed `bytes` (a BufferSource, not `{ module_or_path }`)
    // straight to the glue module's `default()`, which is exactly the shape
    // wasm-bindgen's deprecated bare-argument path warns about.
    await setWasm(wasm, bytes);
  } finally {
    console.warn = originalWarn;
  }

  const deprecationWarnings = warnings.filter((w) => w.includes("deprecated"));
  assert.deepEqual(
    deprecationWarnings,
    [],
    `expected no deprecation warnings, got: ${JSON.stringify(warnings)}`,
  );
});
