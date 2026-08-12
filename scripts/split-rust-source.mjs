import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const TARGET_BYTES = 30_000;
export const MAX_BYTES = 32_768;
const repositoryRoot = resolve(fileURLToPath(new URL("../", import.meta.url)));

function bytes(value) {
  return Buffer.byteLength(value, "utf8");
}

function normalizeNewlines(value) {
  return value.replaceAll("\r\n", "\n").replaceAll("\r", "\n");
}

function repositoryPath(path) {
  return relative(repositoryRoot, path).split(sep).join("/");
}

function topLevelStart(line) {
  return /^(?:(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|unsafe)\s+)*(?:use|mod|const|static|type|struct|enum|trait|impl|fn)\b|extern\s+crate\b|macro_rules!\s+)/.test(
    line,
  );
}

function nestedItemStart(line) {
  return /^ {4}(?:(?:(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|unsafe)\s+)*(?:use|mod|const|static|type|struct|enum|trait|impl|fn)\b)|extern\s+crate\b|macro_rules!\s+)/.test(
    line,
  );
}

function implItemStart(line) {
  return /^ {4}(?:(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|const|unsafe)\s+)*(?:fn|const|type)\b)/.test(
    line,
  );
}

function isLeadingMetadata(line, indentation = "") {
  return (
    line === "" ||
    line.startsWith(`${indentation}#`) ||
    line.startsWith(`${indentation}//`)
  );
}

function itemRanges(lines, isStart, metadataIndentation = "") {
  const rawStarts = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (isStart(lines[index])) rawStarts.push(index);
  }
  if (rawStarts.length === 0) return [{ start: 0, end: lines.length }];

  const starts = rawStarts.map((rawStart, rawIndex) => {
    const minimum = rawIndex === 0 ? 0 : rawStarts[rawIndex - 1] + 1;
    let start = rawStart;
    while (start > minimum && isLeadingMetadata(lines[start - 1], metadataIndentation)) {
      start -= 1;
    }
    return start;
  });
  if (starts[0] !== 0) starts.unshift(0);
  return starts.map((start, index) => ({
    start,
    end: index + 1 < starts.length ? starts[index + 1] : lines.length,
  }));
}

function textForRange(lines, range) {
  return `${lines.slice(range.start, range.end).join("\n").trimEnd()}\n`;
}

function groupItems(items, limit = TARGET_BYTES) {
  const groups = [];
  let current = "";
  for (const item of items) {
    if (bytes(item) > MAX_BYTES) {
      throw new Error(`single Rust item is ${bytes(item)} bytes and must be extracted first`);
    }
    if (bytes(item) > limit) {
      if (current !== "") groups.push(current);
      groups.push(item);
      current = "";
      continue;
    }
    if (current !== "" && bytes(current) + bytes(item) > limit) {
      groups.push(current);
      current = "";
    }
    current += item;
  }
  if (current !== "") groups.push(current);
  return groups;
}

function splitImpl(item) {
  if (bytes(item) <= TARGET_BYTES) return [item];
  const lines = item.trimEnd().split("\n");
  const implLine = lines.findIndex((line) => /^impl(?:<[^\n]*>)?\s/.test(line));
  if (implLine < 0) return [item];
  const firstMethod = lines.findIndex(implItemStart);
  if (firstMethod <= implLine || lines.at(-1)?.trim() !== "}") return [item];
  let firstItem = firstMethod;
  while (firstItem > implLine + 1 && isLeadingMetadata(lines[firstItem - 1], "    ")) {
    firstItem -= 1;
  }
  const metadata = lines.slice(0, implLine).join("\n").trimEnd();
  const header = lines.slice(implLine, firstItem).join("\n").trimEnd();
  if (/\bfor\b/.test(header.slice(0, header.indexOf("{") + 1))) return [item];
  const body = lines.slice(firstItem, -1);
  const ranges = itemRanges(body, implItemStart, "    ");
  const methods = ranges.map((range) => textForRange(body, range));
  const prefix = metadata === "" ? "" : `${metadata}\n`;
  const overhead = bytes(`${prefix}${header}\n}\n`);
  const methodGroups = groupItems(methods, TARGET_BYTES - overhead);
  return methodGroups.map((group) => `${prefix}${header}\n${group.trimEnd()}\n}\n`);
}

function splitTestsModule(item, fileStem) {
  if (!/^#\[cfg\(test\)\]$/m.test(item.trimStart()) || bytes(item) <= TARGET_BYTES) {
    return null;
  }
  const lines = item.trimEnd().split("\n");
  const moduleLine = lines.findIndex((line) => line === "mod tests {");
  if (moduleLine < 0 || lines.at(-1)?.trim() !== "}") return null;
  const body = lines.slice(moduleLine + 1, -1);
  const ranges = itemRanges(body, nestedItemStart, "    ");
  const items = ranges.map((range) =>
    textForRange(body, range)
      .split("\n")
      .map((line) => (line.startsWith("    ") ? line.slice(4) : line))
      .join("\n"),
  );
  const groups = groupItems(items);
  const files = groups.map((_, index) =>
    `${fileStem}_tests_${String(index + 1).padStart(2, "0")}.rs`,
  );
  const replacement = [...lines.slice(0, moduleLine), "mod tests {"];
  for (const file of files) replacement.push(`    include!("${file}");`);
  replacement.push("}", "");
  return { files: groups.map((content, index) => [files[index], content]), replacement: replacement.join("\n") };
}

function keepCratePreamble(lines) {
  let end = 0;
  while (
    end < lines.length &&
    (lines[end].startsWith("//!") || lines[end].startsWith("#![") || lines[end] === "")
  ) {
    end += 1;
  }
  return { preamble: lines.slice(0, end).join("\n").trimEnd(), rest: lines.slice(end) };
}

function resolveContainedPath(root, relativeSource) {
  const sourcePath = resolve(root, relativeSource);
  const pathFromRoot = relative(root, sourcePath);
  if (pathFromRoot === "" || pathFromRoot === ".." || pathFromRoot.startsWith(`..${sep}`) || isAbsolute(pathFromRoot)) {
    throw new Error("source leaves repository");
  }
  return sourcePath;
}

function validateOutputName(outputName) {
  if (
    outputName === "" ||
    outputName === "." ||
    outputName === ".." ||
    outputName.includes("\0") ||
    basename(outputName) !== outputName
  ) {
    throw new Error("output name must be one directory name");
  }
  return outputName;
}

export function parseCliArguments(arguments_) {
  let source;
  let outputName;
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--output-name") {
      if (outputName !== undefined) throw new Error("--output-name may only be specified once");
      outputName = arguments_[index + 1];
      if (outputName === undefined || outputName.startsWith("-")) {
        throw new Error("--output-name requires a directory name");
      }
      validateOutputName(outputName);
      index += 1;
    } else if (argument.startsWith("-")) {
      throw new Error(`unknown option: ${argument}`);
    } else if (source !== undefined) {
      throw new Error("only one source is accepted; use --output-name for the output directory");
    } else {
      source = argument;
    }
  }
  if (source === undefined) {
    throw new Error(
      "usage: node scripts/split-rust-source.mjs <source.rs> [--output-name <directory>]",
    );
  }
  return { source, outputName };
}

export async function splitSource(relativeSource, outputName, root = repositoryRoot) {
  const normalizedRoot = resolve(root);
  const sourcePath = resolveContainedPath(normalizedRoot, relativeSource);
  const original = normalizeNewlines(await readFile(sourcePath, "utf8"));
  const { preamble, rest } = keepCratePreamble(original.split("\n"));
  const ranges = itemRanges(rest, topLevelStart);
  const rawItems = ranges.map((range) => textForRange(rest, range));
  const outputDirectoryName = validateOutputName(
    outputName ?? `${basename(sourcePath, ".rs")}_parts`,
  );
  const outputDirectory = join(dirname(sourcePath), outputDirectoryName);
  if (resolve(outputDirectory) === sourcePath) {
    throw new Error("output directory conflicts with the source file");
  }
  const fileStem = basename(sourcePath, ".rs");
  const expandedItems = [];
  const additionalFiles = [];
  for (const item of rawItems) {
    const tests = splitTestsModule(item, fileStem);
    if (tests) {
      expandedItems.push(tests.replacement);
      additionalFiles.push(...tests.files);
      continue;
    }
    expandedItems.push(...splitImpl(item));
  }

  const groups = groupItems(expandedItems);
  const partFiles = groups.map((content, index) => [
    `${fileStem}_${String(index + 1).padStart(2, "0")}.rs`,
    content,
  ]);
  for (const [name, content] of [...partFiles, ...additionalFiles]) {
    if (bytes(content) > MAX_BYTES) throw new Error(`${name} exceeds ${MAX_BYTES} bytes`);
  }

  await rm(outputDirectory, { force: true, recursive: true });
  await mkdir(outputDirectory, { recursive: true });
  for (const [name, content] of [...partFiles, ...additionalFiles]) {
    await writeFile(join(outputDirectory, name), content, "utf8");
  }

  const rootSource = [];
  if (preamble !== "") rootSource.push(preamble, "");
  for (const [name] of partFiles) rootSource.push(`include!("${outputDirectoryName}/${name}");`);
  rootSource.push("");
  await writeFile(sourcePath, rootSource.join("\n"), "utf8");
  return {
    source: repositoryPath(sourcePath),
    parts: partFiles.length + additionalFiles.length,
    largestBytes: Math.max(...[...partFiles, ...additionalFiles].map(([, content]) => bytes(content))),
  };
}

async function main() {
  const { source, outputName } = parseCliArguments(process.argv.slice(2));
  console.log(await splitSource(source, outputName));
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  main().catch((error) => {
    console.error(`Rust source split failed: ${error.message}`);
    process.exitCode = 1;
  });
}
