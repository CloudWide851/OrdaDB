import { Tooltip } from "antd";
import {
  forwardRef,
  type ButtonHTMLAttributes,
  type ReactNode,
} from "react";

interface IconActionProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  label: string;
  icon: ReactNode;
  tone?: "plain" | "brand" | "danger";
}

export const IconAction = forwardRef<HTMLButtonElement, IconActionProps>(
  function IconAction(
    {
      label,
      icon,
      tone = "plain",
      className = "",
      ...buttonProps
    },
    ref,
  ) {
    return (
      <Tooltip title={label} mouseEnterDelay={0.35}>
        <button
          ref={ref}
          type="button"
          className={`icon-action icon-action--${tone} ${className}`}
          aria-label={label}
          {...buttonProps}
        >
          {icon}
        </button>
      </Tooltip>
    );
  },
);
