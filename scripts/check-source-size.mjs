import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { evaluateSourceSizes } from "./source-size-policy.mjs";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const baselinePath = fileURLToPath(
  new URL("./source-size-baseline.json", import.meta.url),
);

function runGit(gitArguments, options = {}) {
  const result = spawnSync("git", gitArguments, {
    cwd: repositoryRoot,
    encoding: options.encoding,
    input: options.input,
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error) {
    throw new Error(`failed to launch Git: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const stderr = Buffer.isBuffer(result.stderr)
      ? result.stderr.toString("utf8")
      : result.stderr;
    throw new Error(`Git ${gitArguments[0]} failed: ${stderr.trim()}`);
  }
  return result.stdout;
}

function readIndexEntries() {
  const output = runGit(["ls-files", "--stage", "-z"]);
  const records = new TextDecoder("utf-8", { fatal: true }).decode(output).split("\0");
  records.pop();
  const entries = [];
  const objectIds = new Set();

  for (const record of records) {
    const match = /^(\d{6}) ([0-9a-f]{40,64}) ([0-3])\t([\s\S]+)$/.exec(record);
    if (!match) {
      throw new Error("Git returned an invalid index record");
    }
    const [, mode, objectId, stage, path] = match;
    if (stage !== "0") {
      throw new Error(`source-size gate refuses an unmerged index entry: ${path}`);
    }
    entries.push({ mode, objectId, path });
    objectIds.add(objectId);
  }

  const input = `${[...objectIds].join("\n")}\n`;
  const checks = runGit(
    ["cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
    { encoding: "utf8", input },
  );
  const objects = new Map();
  for (const line of checks.trimEnd().split("\n")) {
    const match = /^([0-9a-f]{40,64}) (\S+) (\d+)$/.exec(line.trimEnd());
    if (!match) {
      throw new Error(`Git returned an invalid object record: ${line}`);
    }
    const [, objectId, type, rawBytes] = match;
    objects.set(objectId, { bytes: Number(rawBytes), type });
  }

  return entries.map(({ mode, objectId, path }) => {
    const object = objects.get(objectId);
    if (!object) {
      throw new Error(`Git did not report index object ${objectId} for ${path}`);
    }
    if (object.type !== "blob") {
      throw new Error(
        `source-size gate refuses a non-blob index entry: ${path} (${mode} ${object.type})`,
      );
    }
    return { bytes: object.bytes, path };
  });
}

async function main() {
  const baseline = JSON.parse(await readFile(baselinePath, "utf8"));
  const result = evaluateSourceSizes(readIndexEntries(), baseline);
  if (result.errors.length > 0) {
    console.error(
      `Source-size gate failed (${result.errors.length} violation${result.errors.length === 1 ? "" : "s"}):`,
    );
    for (const error of result.errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }
  console.log(
    `Source-size gate passed: ${result.eligibleCount} tracked sources, ` +
      `${result.oversizedCount} temporary baseline entries, ` +
      `${result.maximumBytes} byte limit.`,
  );
}

main().catch((error) => {
  console.error(`Source-size gate could not run: ${error.message}`);
  process.exitCode = 1;
});
