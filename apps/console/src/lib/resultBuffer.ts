export type ResultRow = Array<string | null>;

export interface ResultPage {
  start: number;
  rows: ResultRow[];
  bytes: number;
}

export interface ResultBuffer {
  pages: ResultPage[];
  rowCount: number;
  totalRows: number;
  bytes: number;
  droppedRows: number;
}

export const RESULT_PAGE_ROWS = 256;
export const MAX_RESULT_ROWS = 10_000;
export const MAX_RESULT_BYTES = 16 * 1024 * 1024;

export interface ResultBufferLimits {
  pageRows: number;
  maxRows: number;
  maxBytes: number;
}

export const DEFAULT_RESULT_BUFFER_LIMITS: ResultBufferLimits = {
  pageRows: RESULT_PAGE_ROWS,
  maxRows: MAX_RESULT_ROWS,
  maxBytes: MAX_RESULT_BYTES,
};

export function emptyResultBuffer(): ResultBuffer {
  return {
    pages: [],
    rowCount: 0,
    totalRows: 0,
    bytes: 0,
    droppedRows: 0,
  };
}

export function appendResultRows(
  current: ResultBuffer,
  incoming: ResultRow[],
  limits: ResultBufferLimits = DEFAULT_RESULT_BUFFER_LIMITS,
): ResultBuffer {
  if (incoming.length === 0) {
    return current;
  }

  const next: ResultBuffer = {
    pages: current.pages.slice(),
    rowCount: current.rowCount,
    totalRows: current.totalRows + incoming.length,
    bytes: current.bytes,
    droppedRows: current.droppedRows,
  };

  let offset = 0;
  let activePage: ResultPage | undefined;
  while (offset < incoming.length) {
    if (next.rowCount >= limits.maxRows || next.bytes >= limits.maxBytes) {
      next.droppedRows += incoming.length - offset;
      break;
    }

    const source = incoming[offset];
    const bytes = estimateResultRowBytes(source);
    if (next.bytes + bytes > limits.maxBytes) {
      next.droppedRows += incoming.length - offset;
      break;
    }

    if (!activePage || activePage.rows.length >= limits.pageRows) {
      const last = next.pages.at(-1);
      if (last && last.rows.length < limits.pageRows) {
        activePage = {
          start: last.start,
          rows: last.rows.slice(),
          bytes: last.bytes,
        };
        next.pages[next.pages.length - 1] = activePage;
      } else {
        activePage = {
          start: next.rowCount,
          rows: [],
          bytes: 0,
        };
        next.pages.push(activePage);
      }
    }
    activePage.rows.push(source);
    activePage.bytes += bytes;
    next.rowCount += 1;
    next.bytes += bytes;
    offset += 1;
  }

  return next;
}

export function resultRowAt(
  pages: ResultPage[],
  index: number,
): ResultRow | undefined {
  let low = 0;
  let high = pages.length - 1;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const page = pages[middle];
    if (index < page.start) {
      high = middle - 1;
    } else if (index >= page.start + page.rows.length) {
      low = middle + 1;
    } else {
      return page.rows[index - page.start];
    }
  }
  return undefined;
}

export function resultRows(pages: ResultPage[]): ResultRow[] {
  return pages.flatMap((page) => page.rows);
}

function estimateResultRowBytes(row: ResultRow): number {
  return row.reduce(
    (total, value) => total + (value === null ? 4 : value.length * 2 + 8),
    24,
  );
}
