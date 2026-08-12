import assert from "node:assert/strict";
import test from "node:test";
import {
  MAX_SOURCE_BYTES,
  evaluateSourceSizes,
  isEligibleSourcePath,
  validateBaseline,
} from "./source-size-policy.mjs";

const emptyBaseline = {
  schemaVersion: 1,
  maximumBytes: MAX_SOURCE_BYTES,
  entries: [],
};

function baseline(entries) {
  return { ...emptyBaseline, entries };
}

test("classifies handwritten sources and explicit exclusions", () => {
  assert.equal(isEligibleSourcePath("crates/example/src/lib.rs"), true);
  assert.equal(isEligibleSourcePath("apps\\console\\src\\App.tsx"), true);
  assert.equal(isEligibleSourcePath("scripts/check.ps1"), true);
  assert.equal(isEligibleSourcePath("Cargo.lock"), false);
  assert.equal(isEligibleSourcePath("packages/example/pnpm-lock.yaml"), false);
  assert.equal(
    isEligibleSourcePath("apps/desktop/src-tauri/gen/schemas/desktop-schema.json"),
    false,
  );
  assert.equal(isEligibleSourcePath("apps/console/src/assets/vendor.svg"), false);
  assert.equal(isEligibleSourcePath("fixtures/non-utf8-binary.bin"), false);
  assert.equal(isEligibleSourcePath("apps/desktop/src-tauri/icons/icon.ico"), false);
});

test("accepts the exact threshold and an exact oversized baseline", () => {
  const result = evaluateSourceSizes(
    [
      { path: "src/boundary.ts", bytes: MAX_SOURCE_BYTES },
      { path: "src/legacy.rs", bytes: MAX_SOURCE_BYTES + 10 },
    ],
    baseline([{ path: "src/legacy.rs", bytes: MAX_SOURCE_BYTES + 10 }]),
  );
  assert.deepEqual(result.errors, []);
  assert.equal(result.eligibleCount, 2);
  assert.equal(result.oversizedCount, 1);
});

test("rejects a new oversized file and growth above an existing baseline", () => {
  const result = evaluateSourceSizes(
    [
      { path: "src/grew.rs", bytes: MAX_SOURCE_BYTES + 20 },
      { path: "src/new.ts", bytes: MAX_SOURCE_BYTES + 1 },
    ],
    baseline([{ path: "src/grew.rs", bytes: MAX_SOURCE_BYTES + 10 }]),
  );
  assert.deepEqual(result.errors, [
    `src/grew.rs: grew from the baseline limit ${MAX_SOURCE_BYTES + 10} to ${MAX_SOURCE_BYTES + 20} bytes`,
    `src/new.ts: ${MAX_SOURCE_BYTES + 1} bytes exceeds ${MAX_SOURCE_BYTES} without a baseline entry`,
  ]);
});

test("allows oversized shrinkage but requires obsolete entries to be removed", () => {
  const allowed = baseline([
    { path: "src/legacy.rs", bytes: MAX_SOURCE_BYTES + 100 },
  ]);
  assert.deepEqual(
    evaluateSourceSizes(
      [{ path: "src/legacy.rs", bytes: MAX_SOURCE_BYTES + 1 }],
      allowed,
    ).errors,
    [],
  );
  assert.deepEqual(
    evaluateSourceSizes([{ path: "src/legacy.rs", bytes: 10 }], allowed).errors,
    ["src/legacy.rs: now 10 bytes; remove its obsolete baseline entry"],
  );
});

test("rejects stale, unsorted, duplicate, and excluded baseline entries", () => {
  assert.deepEqual(
    evaluateSourceSizes(
      [{ path: "src/current.rs", bytes: 1 }],
      baseline([{ path: "src/missing.rs", bytes: MAX_SOURCE_BYTES + 1 }]),
    ).errors,
    ["src/missing.rs: stale baseline entry; path is not a tracked eligible source file"],
  );
  assert.throws(
    () =>
      validateBaseline(
        baseline([
          { path: "src/z.rs", bytes: MAX_SOURCE_BYTES + 1 },
          { path: "src/a.rs", bytes: MAX_SOURCE_BYTES + 1 },
        ]),
      ),
    /unique and sorted/,
  );
  assert.throws(
    () =>
      validateBaseline(
        baseline([
          { path: "src/a.rs", bytes: MAX_SOURCE_BYTES + 1 },
          { path: "src/a.rs", bytes: MAX_SOURCE_BYTES + 1 },
        ]),
      ),
    /unique and sorted/,
  );
  assert.throws(
    () =>
      validateBaseline(
        baseline([{ path: "Cargo.lock", bytes: MAX_SOURCE_BYTES + 1 }]),
      ),
    /excluded path/,
  );
  assert.throws(
    () => validateBaseline({ ...emptyBaseline, note: "not allowed" }),
    /may contain only/,
  );
});

test("rejects invalid tracked sizes and unsafe repository paths", () => {
  assert.throws(
    () => evaluateSourceSizes([{ path: "src/lib.rs", bytes: -1 }], emptyBaseline),
    /invalid byte size/,
  );
  assert.throws(() => isEligibleSourcePath("../outside.rs"), /repository-relative/);
  assert.throws(() => isEligibleSourcePath("C:\\outside.rs"), /repository-relative/);
});
