import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";

import {
  MAX_BYTES,
  parseCliArguments,
  splitSource,
  TARGET_BYTES,
} from "./split-rust-source.mjs";

const execFileAsync = promisify(execFile);

async function fixture(source) {
  const root = await mkdtemp(join(tmpdir(), "ordadb-rust-split-"));
  const sourceDirectory = join(root, "src");
  await mkdir(sourceDirectory);
  await writeFile(join(sourceDirectory, "lib.rs"), source, "utf8");
  return root;
}

function paddedMethod(index, indentation = "    ") {
  const padding = "x".repeat(620);
  return `${indentation}fn method_${index}(&self) -> usize {\n${indentation}    // ${padding}\n${indentation}    ${index}\n${indentation}}\n\n`;
}

function paddedTest(index) {
  const padding = "x".repeat(620);
  return `    #[test]\n    fn case_${index}() {\n        // ${padding}\n        assert_eq!(${index}, ${index});\n    }\n\n`;
}

async function generatedFiles(root) {
  const source = await readFile(join(root, "src", "lib.rs"), "utf8");
  const names = [...source.matchAll(/include!\("lib_parts\/([^\"]+)"\)/g)].map(
    (match) => match[1],
  );
  const contents = await Promise.all(
    names.map((name) => readFile(join(root, "src", "lib_parts", name), "utf8")),
  );
  return { source, names, contents };
}

test("preserves crate preamble, item order, and the target ceiling", async () => {
  const methods = Array.from({ length: 90 }, (_, index) => paddedMethod(index)).join("");
  const root = await fixture(`//! Crate docs.\n#![deny(unsafe_code)]\n\nuse std::fmt;\n\npub struct First;\n\nimpl First {\n    #[must_use]\n${methods}}\n\npub const LAST: usize = 9;\n`);

  const result = await splitSource("src/lib.rs", undefined, root);
  const { source, contents } = await generatedFiles(root);

  assert.match(source, /^\/\/! Crate docs\.\n#!\[deny\(unsafe_code\)\]/);
  assert.ok(result.parts > 2);
  assert.ok(contents.every((content) => Buffer.byteLength(content) <= TARGET_BYTES));
  const combined = contents.join("\n");
  assert.ok(combined.indexOf("use std::fmt") < combined.indexOf("pub struct First"));
  assert.ok(combined.indexOf("fn method_0") < combined.indexOf("fn method_89"));
  assert.ok(combined.indexOf("fn method_89") < combined.indexOf("pub const LAST"));
  assert.equal(combined.match(/#\[must_use\]/g)?.length, 1);
});

test("splits an oversized test module through ordered include files", async () => {
  const tests = Array.from({ length: 90 }, (_, index) => paddedTest(index)).join("");
  const root = await fixture(`pub fn value() -> usize { 1 }\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n${tests}}\n`);

  await splitSource("src/lib.rs", undefined, root);
  const { contents } = await generatedFiles(root);
  const modulePart = contents.find((content) => content.includes("mod tests"));
  const testNames = [...modulePart.matchAll(/include!\("([^\"]+_tests_[^\"]+)"\)/g)].map(
    (match) => match[1],
  );
  const testContents = await Promise.all(
    testNames.map((name) => readFile(join(root, "src", "lib_parts", name), "utf8")),
  );
  const combinedTests = testContents.join("\n");

  assert.match(modulePart, /#\[cfg\(test\)\]\nmod tests \{/);
  assert.ok(modulePart.indexOf("lib_tests_01.rs") < modulePart.indexOf("lib_tests_02.rs"));
  assert.ok(combinedTests.indexOf("fn case_0") < combinedTests.indexOf("fn case_89"));
  await execFileAsync("rustc", ["--edition=2024", "--test", "src/lib.rs", "-o", "split-test.exe"], {
    cwd: root,
  });
});

test("fails before replacing output when one function cannot be split", async () => {
  const root = await fixture(`pub fn enormous() {\n    // ${"z".repeat(MAX_BYTES)}\n}\n`);
  const output = join(root, "src", "lib_parts");
  await mkdir(output);
  await writeFile(join(output, "marker.txt"), "keep", "utf8");

  await assert.rejects(
    splitSource("src/lib.rs", undefined, root),
    /single Rust item .* must be extracted first/,
  );
  assert.equal(await readFile(join(output, "marker.txt"), "utf8"), "keep");
});

test("does not split a trait implementation into conflicting impl blocks", async () => {
  const body = Array.from({ length: 90 }, (_, index) => paddedMethod(index)).join("");
  const root = await fixture(`struct Holder;\ntrait Large {}\nimpl Large for Holder {\n${body}}\n`);

  await assert.rejects(
    splitSource("src/lib.rs", undefined, root),
    /single Rust item .* must be extracted first/,
  );
});

test("rejects paths outside the requested repository root", async () => {
  const root = await fixture("pub fn value() {}\n");
  await assert.rejects(splitSource("../outside.rs", undefined, root), /source leaves repository/);
  await assert.rejects(splitSource(".", undefined, root), /source leaves repository/);
  await assert.rejects(stat(join(root, "outside.rs")));
});

test("requires an explicit output-name option before any split can run", () => {
  assert.deepEqual(parseCliArguments(["src/lib.rs"]), {
    source: "src/lib.rs",
    outputName: undefined,
  });
  assert.deepEqual(parseCliArguments(["src/lib.rs", "--output-name", "index_parts"]), {
    source: "src/lib.rs",
    outputName: "index_parts",
  });
  assert.throws(
    () => parseCliArguments(["src/lib.rs", "index_parts"]),
    /only one source is accepted/,
  );
  assert.throws(() => parseCliArguments(["src/lib.rs", "--output-name"]), /requires/);
});

test("rejects output directories that escape or replace the source", async () => {
  const root = await fixture("pub fn value() {}\n");
  await assert.rejects(
    splitSource("src/lib.rs", "../outside", root),
    /one directory name/,
  );
  await assert.rejects(
    splitSource("src/lib.rs", "lib.rs", root),
    /conflicts with the source file/,
  );
  assert.equal(await readFile(join(root, "src", "lib.rs"), "utf8"), "pub fn value() {}\n");
});
