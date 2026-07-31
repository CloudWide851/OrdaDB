import { describe, expect, it } from "vitest";
import {
  appendResultRows,
  emptyResultBuffer,
  MAX_RESULT_BYTES,
  MAX_RESULT_ROWS,
  RESULT_PAGE_ROWS,
  resultRowAt,
  resultRows,
} from "./resultBuffer";

describe("result buffer", () => {
  it("appends by page without copying retained full pages", () => {
    const first = appendResultRows(
      emptyResultBuffer(),
      Array.from({ length: RESULT_PAGE_ROWS }, (_, index) => [
        index.toString(),
      ]),
    );
    const retainedPage = first.pages[0];
    const second = appendResultRows(first, [["next"]]);
    const third = appendResultRows(second, [["same-page"]]);

    expect(second.pages[0]).toBe(retainedPage);
    expect(third.pages[0]).toBe(retainedPage);
    expect(resultRowAt(third.pages, RESULT_PAGE_ROWS + 1)).toEqual([
      "same-page",
    ]);
  });

  it("copies only the active tail and keeps completed page identities stable", () => {
    const first = appendResultRows(
      emptyResultBuffer(),
      Array.from({ length: 10 }, (_, index) => [index.toString()]),
    );
    const originalTail = first.pages[0];
    const originalTailRows = originalTail.rows;
    const second = appendResultRows(
      first,
      Array.from({ length: RESULT_PAGE_ROWS }, (_, index) => [
        `second-${index}`,
      ]),
    );

    expect(second.pages).not.toBe(first.pages);
    expect(second.pages[0]).not.toBe(originalTail);
    expect(second.pages[0].rows).not.toBe(originalTailRows);
    expect(first.pages[0]).toBe(originalTail);
    expect(first.pages[0].rows).toBe(originalTailRows);
    const completed = second.pages[0];
    const activeTail = second.pages[1];

    const third = appendResultRows(second, [["third"]]);
    expect(third.pages[0]).toBe(completed);
    expect(third.pages[1]).not.toBe(activeTail);
    expect(third.pages[1].rows).not.toBe(activeTail.rows);
    expect(resultRowAt(third.pages, third.rowCount - 1)).toEqual(["third"]);
  });

  it("returns the original snapshot for an empty append", () => {
    const current = appendResultRows(emptyResultBuffer(), [["one"]]);
    expect(appendResultRows(current, [])).toBe(current);
  });

  it("keeps total progress while bounding resident rows and bytes", () => {
    const rowBounded = appendResultRows(
      emptyResultBuffer(),
      Array.from({ length: MAX_RESULT_ROWS + 7 }, (_, index) => [
        index.toString(),
      ]),
    );
    expect(rowBounded).toMatchObject({
      rowCount: MAX_RESULT_ROWS,
      totalRows: MAX_RESULT_ROWS + 7,
      droppedRows: 7,
    });

    const byteBounded = appendResultRows(emptyResultBuffer(), [
      ["x".repeat(MAX_RESULT_BYTES)],
    ]);
    expect(byteBounded).toMatchObject({
      rowCount: 0,
      totalRows: 1,
      droppedRows: 1,
      bytes: 0,
    });
  });

  it("uses configured page, row, and byte limits", () => {
    const rowBounded = appendResultRows(
      emptyResultBuffer(),
      [["one"], ["two"], ["three"]],
      { pageRows: 1, maxRows: 2, maxBytes: 1_024 },
    );
    expect(rowBounded).toMatchObject({
      rowCount: 2,
      totalRows: 3,
      droppedRows: 1,
    });
    expect(rowBounded.pages).toHaveLength(2);

    const byteBounded = appendResultRows(
      emptyResultBuffer(),
      [["too large"]],
      { pageRows: 50, maxRows: 100, maxBytes: 8 },
    );
    expect(byteBounded).toMatchObject({
      rowCount: 0,
      totalRows: 1,
      droppedRows: 1,
    });
  });

  it("supports random access across pages and bounds a 12,000-row stream", () => {
    const incoming = Array.from({ length: 12_000 }, (_, index) => [
      `row-${index}`,
    ]);
    const buffered = appendResultRows(emptyResultBuffer(), incoming);

    expect(buffered).toMatchObject({
      rowCount: 10_000,
      totalRows: 12_000,
      droppedRows: 2_000,
    });
    expect(resultRows(buffered.pages)).toHaveLength(10_000);
    for (const index of [0, 255, 256, 511, 4_321, 9_999]) {
      expect(resultRowAt(buffered.pages, index)).toEqual([`row-${index}`]);
    }
    expect(resultRowAt(buffered.pages, -1)).toBeUndefined();
    expect(resultRowAt(buffered.pages, 10_000)).toBeUndefined();
  });
});
