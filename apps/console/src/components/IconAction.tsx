import { Tooltip } from "antd";
import type { ButtonHTMLAttributes, ReactNode } from "react";

interface IconActionProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  label: string;
  icon: ReactNode;
  tone?: "plain" | "brand" | "danger";
}

export function IconAction({
  label,
  icon,
  tone = "plain",
  className = "",
  ...buttonProps
}: IconActionProps) {
  return (
    <Tooltip title={label} mouseEnterDelay={0.35}>
      <button
        type="button"
        className={`icon-action icon-action--${tone} ${className}`}
        aria-label={label}
        {...buttonProps}
      >
        {icon}
      </button>
    </Tooltip>
  );
}
