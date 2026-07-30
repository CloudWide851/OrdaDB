import {
  CheckCircle2,
  CircleOff,
  LoaderCircle,
  TriangleAlert,
  XCircle,
  type LucideIcon,
} from "lucide-react";

export type ConnectionVisualState =
  | "idle"
  | "connecting"
  | "connected"
  | "error";

interface ConnectionStatusIconProps {
  state: ConnectionVisualState;
  preview?: boolean;
  className?: string;
}

type ConnectionStatusPresentation = {
  kind: "success" | "error" | "warning" | "disconnected" | "loading";
  label: string;
  icon: LucideIcon;
};

export function ConnectionStatusIcon({
  state,
  preview = false,
  className,
}: ConnectionStatusIconProps) {
  const presentation = getConnectionStatusPresentation(state, preview);
  const Icon = presentation.icon;

  return (
    <span
      className={[
        "connection-status",
        `connection-status--${presentation.kind}`,
        className,
      ]
        .filter(Boolean)
        .join(" ")}
      aria-label={`连接状态：${presentation.label}`}
    >
      <Icon size={13} aria-hidden="true" />
      <span>{presentation.label}</span>
    </span>
  );
}

function getConnectionStatusPresentation(
  state: ConnectionVisualState,
  preview: boolean,
): ConnectionStatusPresentation {
  if (state === "connecting") {
    return { kind: "loading", label: "连接中", icon: LoaderCircle };
  }
  if (state === "error") {
    return { kind: "error", label: "连接错误", icon: XCircle };
  }
  if (state === "connected" && preview) {
    return { kind: "warning", label: "界面预览", icon: TriangleAlert };
  }
  if (state === "connected") {
    return { kind: "success", label: "已连接", icon: CheckCircle2 };
  }
  return { kind: "disconnected", label: "未连接", icon: CircleOff };
}
