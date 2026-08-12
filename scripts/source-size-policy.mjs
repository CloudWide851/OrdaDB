export const MAX_SOURCE_BYTES = 32_768;

const SOURCE_EXTENSIONS = new Set([
  ".cjs",
  ".css",
  ".htm",
  ".html",
  ".js",
  ".jsx",
  ".json",
  ".md",
  ".mjs",
  ".ps1",
  ".py",
  ".rs",
  ".sh",
  ".toml",
  ".ts",
  ".tsx",
  ".yaml",
  ".yml",
]);

const EXCLUDED_NAMES = new Set(["Cargo.lock", "pnpm-lock.yaml"]);
const GENERATED_PREFIXES = ["apps/desktop/src-tauri/gen/"];

function comparePaths(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function extension(path) {
  const basename = path.slice(path.lastIndexOf("/") + 1);
  const dot = basename.lastIndexOf(".");
  return dot < 0 ? "" : basename.slice(dot).toLowerCase();
}

export function normalizeRepositoryPath(path) {
  if (typeof path !== "string" || path.length === 0 || path.includes("\0")) {
    throw new TypeError("source-size paths must be non-empty strings without NUL bytes");
  }
  const normalized = path.replaceAll("\\", "/");
  if (
    normalized.startsWith("/") ||
    /^[A-Za-z]:\//.test(normalized) ||
    normalized.split("/").some((part) => part === "" || part === "." || part === "..")
  ) {
    throw new TypeError(`source-size path is not repository-relative: ${path}`);
  }
  return normalized;
}

export function isEligibleSourcePath(path) {
  const normalized = normalizeRepositoryPath(path);
  const basename = normalized.slice(normalized.lastIndexOf("/") + 1);
  return (
    !EXCLUDED_NAMES.has(basename) &&
    !GENERATED_PREFIXES.some((prefix) => normalized.startsWith(prefix)) &&
    SOURCE_EXTENSIONS.has(extension(normalized))
  );
}

export function validateBaseline(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("source-size baseline must be an object");
  }
  const keys = Object.keys(value).sort(comparePaths);
  if (
    keys.length !== 3 ||
    keys[0] !== "entries" ||
    keys[1] !== "maximumBytes" ||
    keys[2] !== "schemaVersion"
  ) {
    throw new TypeError(
      "source-size baseline may contain only schemaVersion, maximumBytes, and entries",
    );
  }
  if (value.schemaVersion !== 1 || value.maximumBytes !== MAX_SOURCE_BYTES) {
    throw new TypeError(
      `source-size baseline must use schemaVersion 1 and maximumBytes ${MAX_SOURCE_BYTES}`,
    );
  }
  if (!Array.isArray(value.entries)) {
    throw new TypeError("source-size baseline entries must be an array");
  }

  let previousPath;
  const entries = new Map();
  for (const entry of value.entries) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw new TypeError("each source-size baseline entry must be an object");
    }
    const keys = Object.keys(entry).sort(comparePaths);
    if (keys.length !== 2 || keys[0] !== "bytes" || keys[1] !== "path") {
      throw new TypeError("source-size baseline entries may contain only path and bytes");
    }
    const path = normalizeRepositoryPath(entry.path);
    if (!isEligibleSourcePath(path)) {
      throw new TypeError(`source-size baseline contains an excluded path: ${path}`);
    }
    if (!Number.isSafeInteger(entry.bytes) || entry.bytes <= MAX_SOURCE_BYTES) {
      throw new TypeError(
        `source-size baseline bytes must exceed ${MAX_SOURCE_BYTES}: ${path}`,
      );
    }
    if (previousPath !== undefined && comparePaths(previousPath, path) >= 0) {
      throw new TypeError("source-size baseline paths must be unique and sorted");
    }
    previousPath = path;
    entries.set(path, entry.bytes);
  }
  return entries;
}

export function evaluateSourceSizes(trackedEntries, baselineValue) {
  if (!Array.isArray(trackedEntries)) {
    throw new TypeError("tracked source entries must be an array");
  }
  const baseline = validateBaseline(baselineValue);
  const eligible = new Map();

  for (const entry of trackedEntries) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw new TypeError("each tracked source entry must be an object");
    }
    const path = normalizeRepositoryPath(entry.path);
    if (!Number.isSafeInteger(entry.bytes) || entry.bytes < 0) {
      throw new TypeError(`tracked blob has an invalid byte size: ${path}`);
    }
    if (eligible.has(path)) {
      throw new TypeError(`tracked source path appears more than once: ${path}`);
    }
    if (isEligibleSourcePath(path)) {
      eligible.set(path, entry.bytes);
    }
  }

  const errors = [];
  for (const [path, allowedBytes] of baseline) {
    const actualBytes = eligible.get(path);
    if (actualBytes === undefined) {
      errors.push(`${path}: stale baseline entry; path is not a tracked eligible source file`);
    } else if (actualBytes <= MAX_SOURCE_BYTES) {
      errors.push(
        `${path}: now ${actualBytes} bytes; remove its obsolete baseline entry`,
      );
    } else if (actualBytes > allowedBytes) {
      errors.push(
        `${path}: grew from the baseline limit ${allowedBytes} to ${actualBytes} bytes`,
      );
    }
  }

  for (const [path, actualBytes] of eligible) {
    if (actualBytes > MAX_SOURCE_BYTES && !baseline.has(path)) {
      errors.push(
        `${path}: ${actualBytes} bytes exceeds ${MAX_SOURCE_BYTES} without a baseline entry`,
      );
    }
  }

  errors.sort(comparePaths);
  return {
    baselineCount: baseline.size,
    eligibleCount: eligible.size,
    errors,
    maximumBytes: MAX_SOURCE_BYTES,
    oversizedCount: [...eligible.values()].filter((bytes) => bytes > MAX_SOURCE_BYTES).length,
  };
}
