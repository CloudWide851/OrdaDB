import { Settings2, X } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import type { ConsoleSettingsV1 } from "../lib/consoleClient";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

interface SettingsDialogProps {
  open: boolean;
  onClose: () => void;
}

export function SettingsDialog({ open, onClose }: SettingsDialogProps) {
  const settings = useWorkbenchStore((state) => state.settings);
  const saveSettings = useWorkbenchStore((state) => state.saveSettings);
  const [draft, setDraft] = useState<ConsoleSettingsV1>(settings);
  const [saving, setSaving] = useState(false);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!open) return;
    setDraft(settings);
    previousFocusRef.current = document.activeElement as HTMLElement;
    window.setTimeout(() => closeButtonRef.current?.focus());
  }, [open, settings]);

  if (!open) return null;

  const close = () => {
    onClose();
    window.setTimeout(() => previousFocusRef.current?.focus());
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSaving(true);
    try {
      await saveSettings(draft);
      close();
    } catch {
      // The store owns the compact error notice.
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      className="dbms-dialog-backdrop"
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        className="dbms-dialog settings-dialog"
        role="dialog"
        aria-modal="true"
        aria-label="设置"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            close();
          }
        }}
      >
        <header className="dbms-dialog-heading">
          <div className="dbms-dialog-title">
            <Settings2 size={18} aria-hidden="true" />
            <div>
              <h2>设置</h2>
              <span>界面与工作区</span>
            </div>
          </div>
          <IconAction
            ref={closeButtonRef}
            label="关闭设置"
            icon={<X size={17} aria-hidden="true" />}
            onClick={close}
          />
        </header>

        <form className="settings-form" onSubmit={(event) => void submit(event)}>
          <div className="settings-grid">
            <FontSizeField
              label="界面字体"
              value={draft.uiFontSize}
              minimum={9}
              maximum={16}
              onChange={(uiFontSize) =>
                setDraft((current) => ({ ...current, uiFontSize }))
              }
            />
            <FontSizeField
              label="数据字体"
              value={draft.dataFontSize}
              minimum={10}
              maximum={18}
              onChange={(dataFontSize) =>
                setDraft((current) => ({ ...current, dataFontSize }))
              }
            />
            <FontSizeField
              label="编辑器字体"
              value={draft.editorFontSize}
              minimum={10}
              maximum={18}
              onChange={(editorFontSize) =>
                setDraft((current) => ({ ...current, editorFontSize }))
              }
            />
          </div>

          <label className="settings-check">
            <input
              type="checkbox"
              checked={draft.reopenLastProject}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  reopenLastProject: event.target.checked,
                }))
              }
            />
            <span>启动时自动恢复上次 SQL 项目</span>
          </label>
          <label className="settings-check">
            <input
              type="checkbox"
              checked={draft.hideEmptyCatalog}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  hideEmptyCatalog: event.target.checked,
                }))
              }
            />
            <span>隐藏空的 Catalog 分类</span>
          </label>

          <footer className="dialog-actions">
            <button className="secondary-action" type="button" onClick={close}>
              取消
            </button>
            <button className="primary-action" type="submit" disabled={saving}>
              {saving ? "保存中" : "保存设置"}
            </button>
          </footer>
        </form>
      </section>
    </div>
  );
}

function FontSizeField({
  label,
  value,
  minimum,
  maximum,
  onChange,
}: {
  label: string;
  value: number;
  minimum: number;
  maximum: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="form-field">
      <span>{label}</span>
      <input
        aria-label={label}
        type="number"
        min={minimum}
        max={maximum}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      <small>px</small>
    </label>
  );
}
