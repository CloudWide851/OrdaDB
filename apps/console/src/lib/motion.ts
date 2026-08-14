import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type RefObject,
} from "react";

export const motionDurations = {
  press: 80,
  feedback: 130,
  panel: 180,
  exitFeedback: 100,
  exitPanel: 135,
} as const;

export const motionEasings = {
  quartOut: "cubic-bezier(0.25, 1, 0.5, 1)",
  expoOut: "cubic-bezier(0.16, 1, 0.3, 1)",
  exit: "cubic-bezier(0.7, 0, 0.84, 0)",
} as const;

export type PresencePhase = "entering" | "present" | "exiting";

interface PresenceOptions {
  enterDurationMs?: number;
  exitDurationMs?: number;
}

export function resolveReducedMotion(
  systemReduced: boolean,
  applicationReduced: boolean,
) {
  return systemReduced || applicationReduced;
}

export function applicationPrefersReducedMotion() {
  return document.documentElement.dataset.reduceMotion === "true";
}

export function systemPrefersReducedMotion() {
  return window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
}

export function useReducedMotion() {
  const [systemReduced, setSystemReduced] = useState(systemPrefersReducedMotion);

  useEffect(() => {
    const query = window.matchMedia?.("(prefers-reduced-motion: reduce)");
    if (!query) return;
    const update = (event: MediaQueryListEvent) => setSystemReduced(event.matches);
    query.addEventListener?.("change", update);
    return () => query.removeEventListener?.("change", update);
  }, []);

  return resolveReducedMotion(
    systemReduced,
    applicationPrefersReducedMotion(),
  );
}

export function usePresence(
  visible: boolean,
  {
    enterDurationMs = motionDurations.panel,
    exitDurationMs = motionDurations.exitPanel,
  }: PresenceOptions = {},
) {
  const reducedMotion = useReducedMotion();
  const [mounted, setMounted] = useState(visible);
  const [phase, setPhase] = useState<PresencePhase>(
    visible ? "entering" : "exiting",
  );

  useEffect(() => {
    if (visible) {
      setMounted(true);
      if (reducedMotion) {
        setPhase("present");
        return;
      }
      setPhase("entering");
      const timer = window.setTimeout(
        () => setPhase("present"),
        enterDurationMs,
      );
      return () => window.clearTimeout(timer);
    }

    if (!mounted || reducedMotion) {
      setMounted(false);
      setPhase("exiting");
      return;
    }

    setPhase("exiting");
    const timer = window.setTimeout(() => setMounted(false), exitDurationMs);
    return () => window.clearTimeout(timer);
  }, [enterDurationMs, exitDurationMs, reducedMotion, visible]);

  return { mounted, phase, reducedMotion } as const;
}

export function useCenterWorkspaceFlip(
  schemaVisible: boolean,
  inspectorVisible: boolean,
): RefObject<HTMLDivElement | null> {
  const centerRef = useRef<HTMLDivElement>(null);
  const previousRectRef = useRef<DOMRect | null>(null);

  useLayoutEffect(() => {
    const element = centerRef.current;
    if (!element) return;

    const nextRect = element.getBoundingClientRect();
    const previousRect = previousRectRef.current;
    previousRectRef.current = nextRect;
    if (
      !previousRect ||
      resolveReducedMotion(
        systemPrefersReducedMotion(),
        applicationPrefersReducedMotion(),
      ) ||
      typeof element.animate !== "function" ||
      nextRect.width === 0
    ) {
      return;
    }

    const deltaX = previousRect.left - nextRect.left;
    const scaleX = previousRect.width / nextRect.width;
    if (Math.abs(deltaX) < 0.5 && Math.abs(scaleX - 1) < 0.002) return;

    const animation = element.animate(
      [
        {
          transform: `translateX(${deltaX}px) scaleX(${scaleX})`,
          transformOrigin: "left center",
        },
        {
          transform: "translateX(0) scaleX(1)",
          transformOrigin: "left center",
        },
      ],
      {
        duration: motionDurations.panel,
        easing: motionEasings.expoOut,
      },
    );

    return () => animation.cancel();
  }, [inspectorVisible, schemaVisible]);

  return centerRef;
}
