import { describe, expect, it } from "vitest";
import {
  appendResultRows,
  emptyResultBuffer,
  MAX_RESULT_BYTES,
  MAX_RESULT_ROWS,
  RESULT_PAGE_ROWS,
  resultRowAt,
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
});
