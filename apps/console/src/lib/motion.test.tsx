import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  motionDurations,
  resolveReducedMotion,
  usePresence,
} from "./motion";

describe("motion primitives", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    document.documentElement.dataset.reduceMotion = "false";
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
      window.setTimeout(() => callback(performance.now()), 1),
    );
    vi.spyOn(window, "cancelAnimationFrame").mockImplementation((handle) =>
      window.clearTimeout(handle),
    );
  });

  afterEach(() => {
    delete document.documentElement.dataset.reduceMotion;
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("keeps every interaction tier at or below the 180 ms contract", () => {
    expect(motionDurations).toEqual({
      press: 80,
      feedback: 130,
      panel: 180,
      exitFeedback: 100,
      exitPanel: 135,
    });
    expect(Math.max(...Object.values(motionDurations))).toBe(180);
  });

  it("reduces motion when either the system or application requests it", () => {
    expect(resolveReducedMotion(false, false)).toBe(false);
    expect(resolveReducedMotion(true, false)).toBe(true);
    expect(resolveReducedMotion(false, true)).toBe(true);
    expect(resolveReducedMotion(true, true)).toBe(true);
  });

  it("retains an exiting surface for 135 ms and cancels stale exits", () => {
    const { result, rerender } = renderHook(
      ({ visible }) => usePresence(visible),
      { initialProps: { visible: true } },
    );

    act(() => vi.advanceTimersByTime(180));
    expect(result.current).toMatchObject({ mounted: true, phase: "present" });

    rerender({ visible: false });
    expect(result.current).toMatchObject({ mounted: true, phase: "exiting" });
    act(() => vi.advanceTimersByTime(100));
    rerender({ visible: true });
    act(() => vi.advanceTimersByTime(40));
    expect(result.current.mounted).toBe(true);
    act(() => vi.advanceTimersByTime(140));
    expect(result.current.phase).toBe("present");

    rerender({ visible: false });
    act(() => vi.advanceTimersByTime(134));
    expect(result.current.mounted).toBe(true);
    act(() => vi.advanceTimersByTime(1));
    expect(result.current.mounted).toBe(false);
  });

  it("skips exit retention for the application reduced-motion setting", () => {
    document.documentElement.dataset.reduceMotion = "true";
    const { result, rerender } = renderHook(
      ({ visible }) => usePresence(visible),
      { initialProps: { visible: true } },
    );

    rerender({ visible: false });
    expect(result.current).toMatchObject({
      mounted: false,
      phase: "exiting",
      reducedMotion: true,
    });
  });
});
