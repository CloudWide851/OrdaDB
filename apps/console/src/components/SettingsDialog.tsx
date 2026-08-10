import {
  Bot,
  Braces,
  Check,
  CircleAlert,
  Database,
  FileCode2,
  KeyRound,
  Palette,
  Search,
  ShieldCheck,
  Trash2,
  X,
  type LucideIcon,
} from "lucide-react";
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type FormEvent,
  type ReactNode,
  type SetStateAction,
} from "react";
import {
  cloneConsoleSettings,
  type ConsoleSettingsV2,
} from "../lib/consoleClient";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

interface SettingsDialogProps {
  open: boolean;
  onClose: () => void;
}

type SettingsCategory =
  | "appearance"
  | "editor"
  | "files"
  | "results"
  | "connections"
  | "ai";

interface CategoryDefinition {
  id: SettingsCategory;
  label: string;
  keywords: string;
  icon: LucideIcon;
}

const categories: CategoryDefinition[] = [
  {
    id: "appearance",
    label: "外观",
    keywords: "主题 缩放 字体 密度 动效 catalog",
    icon: Palette,
  },
  {
    id: "editor",
    label: "编辑器",
    keywords: "字体 缩进 换行 minimap 格式化",
    icon: Braces,
  },
  {
    id: "files",
    label: "文件与工作区",
    keywords: "恢复 自动保存 关闭 项目",
    icon: FileCode2,
  },
  {
    id: "results",
    label: "数据结果",
    keywords: "分页 行数 内存 null 超时",
    icon: Database,
  },
  {
    id: "connections",
    label: "连接与安全",
    keywords: "连接 重连 超时 写入 确认",
    icon: ShieldCheck,
  },
  {
    id: "ai",
    label: "AI",
    keywords: "provider 模型 endpoint reasoning 数据 凭据",
    icon: Bot,
  },
];

export function SettingsDialog({ open, onClose }: SettingsDialogProps) {
  const settings = useWorkbenchStore((state) => state.settings);
  const saveSettings = useWorkbenchStore((state) => state.saveSettings);
  const [draft, setDraft] = useState<ConsoleSettingsV2>(() =>
    cloneConsoleSettings(settings),
  );
  const [category, setCategory] =
    useState<SettingsCategory>("appearance");
  const [search, setSearch] = useState("");
  const [saving, setSaving] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);

  const visibleCategories = useMemo(() => {
    const query = search.trim().toLocaleLowerCase("zh-CN");
    if (!query) return categories;
    return categories.filter((candidate) =>
      `${candidate.label} ${candidate.keywords}`
        .toLocaleLowerCase("zh-CN")
        .includes(query),
    );
  }, [search]);

  useEffect(() => {
    if (!open) return;
    setDraft(cloneConsoleSettings(settings));
    setCategory("appearance");
    setSearch("");
    previousFocusRef.current = document.activeElement as HTMLElement;
    window.setTimeout(() => searchRef.current?.focus());
  }, [open, settings]);

  useEffect(() => {
    if (
      visibleCategories.length > 0 &&
      !visibleCategories.some((candidate) => candidate.id === category)
    ) {
      setCategory(visibleCategories[0].id);
    }
  }, [category, visibleCategories]);

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
      // The store renders the structured persistence error.
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
        className="dbms-dialog settings-dialog settings-dialog--v2"
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
        <header className="settings-dialog__heading">
          <div>
            <h2>设置</h2>
            <span>OrdaDB 工作台</span>
          </div>
          <IconAction
            label="关闭设置"
            icon={<X size={17} aria-hidden="true" />}
            onClick={close}
          />
        </header>

        <form
          className="settings-workspace"
          onSubmit={(event) => void submit(event)}
        >
          <aside className="settings-sidebar" aria-label="设置分类">
            <label className="settings-search">
              <Search size={14} aria-hidden="true" />
              <input
                ref={searchRef}
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="搜索设置"
                aria-label="搜索设置"
              />
            </label>
            <nav>
              {visibleCategories.map((candidate) => {
                const Icon = candidate.icon;
                return (
                  <button
                    key={candidate.id}
                    className={
                      candidate.id === category
                        ? "settings-category settings-category--active"
                        : "settings-category"
                    }
                    type="button"
                    onClick={() => setCategory(candidate.id)}
                    aria-current={
                      candidate.id === category ? "page" : undefined
                    }
                  >
                    <Icon size={15} aria-hidden="true" />
                    {candidate.label}
                  </button>
                );
              })}
            </nav>
          </aside>

          <div className="settings-content">
            {visibleCategories.length === 0 ? (
              <div className="settings-empty" role="status">
                没有匹配的设置
              </div>
            ) : (
              <SettingsCategoryPanel
                category={category}
                draft={draft}
                setDraft={setDraft}
              />
            )}
          </div>

          <footer className="settings-actions">
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

function SettingsCategoryPanel({
  category,
  draft,
  setDraft,
}: {
  category: SettingsCategory;
  draft: ConsoleSettingsV2;
  setDraft: Dispatch<SetStateAction<ConsoleSettingsV2>>;
}) {
  const update = <K extends keyof Omit<ConsoleSettingsV2, "formatVersion">>(
    section: K,
    values: Partial<ConsoleSettingsV2[K]>,
  ) => {
    setDraft((current) => ({
      ...current,
      [section]: { ...current[section], ...values },
    }));
  };

  switch (category) {
    case "appearance":
      return (
        <SettingsSection title="外观">
          <SelectField
            label="主题"
            value={draft.appearance.theme}
            onChange={(theme) =>
              update("appearance", {
                theme: theme as ConsoleSettingsV2["appearance"]["theme"],
              })
            }
            options={[
              ["system", "跟随系统"],
              ["light", "浅色"],
              ["dark", "深色"],
            ]}
          />
          <NumberField
            label="界面缩放"
            value={draft.appearance.zoomPercent}
            minimum={80}
            maximum={150}
            suffix="%"
            onChange={(zoomPercent) =>
              update("appearance", { zoomPercent })
            }
          />
          <NumberField
            label="界面字体"
            value={draft.appearance.uiFontSize}
            minimum={9}
            maximum={16}
            onChange={(uiFontSize) =>
              update("appearance", { uiFontSize })
            }
          />
          <NumberField
            label="数据字体"
            value={draft.appearance.dataFontSize}
            minimum={10}
            maximum={18}
            onChange={(dataFontSize) =>
              update("appearance", { dataFontSize })
            }
          />
          <SelectField
            label="界面密度"
            value={draft.appearance.density}
            onChange={(density) =>
              update("appearance", {
                density:
                  density as ConsoleSettingsV2["appearance"]["density"],
              })
            }
            options={[
              ["compact", "紧凑"],
              ["comfortable", "舒适"],
            ]}
          />
          <CheckField
            label="减少动态效果"
            checked={draft.appearance.reduceMotion}
            onChange={(reduceMotion) =>
              update("appearance", { reduceMotion })
            }
          />
          <CheckField
            label="隐藏空的 Catalog 分类"
            checked={draft.appearance.hideEmptyCatalog}
            onChange={(hideEmptyCatalog) =>
              update("appearance", { hideEmptyCatalog })
            }
          />
        </SettingsSection>
      );
    case "editor":
      return (
        <SettingsSection title="编辑器">
          <TextField
            label="字体"
            value={draft.editor.fontFamily}
            onChange={(fontFamily) => update("editor", { fontFamily })}
          />
          <NumberField
            label="字号"
            value={draft.editor.fontSize}
            minimum={10}
            maximum={24}
            onChange={(fontSize) => update("editor", { fontSize })}
          />
          <NumberField
            label="缩进宽度"
            value={draft.editor.tabSize}
            minimum={1}
            maximum={8}
            suffix="空格"
            onChange={(tabSize) => update("editor", { tabSize })}
          />
          <SelectField
            label="自动换行"
            value={draft.editor.wordWrap}
            onChange={(wordWrap) =>
              update("editor", {
                wordWrap:
                  wordWrap as ConsoleSettingsV2["editor"]["wordWrap"],
              })
            }
            options={[
              ["off", "关闭"],
              ["on", "开启"],
              ["bounded", "按视口"],
            ]}
          />
          <CheckField
            label="显示 Minimap"
            checked={draft.editor.minimap}
            onChange={(minimap) => update("editor", { minimap })}
          />
          <CheckField
            label="保存时格式化"
            checked={draft.editor.formatOnSave}
            onChange={(formatOnSave) =>
              update("editor", { formatOnSave })
            }
          />
        </SettingsSection>
      );
    case "files":
      return (
        <SettingsSection title="文件与工作区">
          <SelectField
            label="草稿恢复"
            value={draft.files.recoveryPolicy}
            onChange={(recoveryPolicy) =>
              update("files", {
                recoveryPolicy:
                  recoveryPolicy as ConsoleSettingsV2["files"]["recoveryPolicy"],
              })
            }
            options={[
              ["prompt", "每次询问"],
              ["never", "不恢复"],
              ["automatic", "自动恢复"],
            ]}
          />
          <SelectField
            label="自动保存"
            value={draft.files.autoSave}
            onChange={(autoSave) =>
              update("files", {
                autoSave:
                  autoSave as ConsoleSettingsV2["files"]["autoSave"],
              })
            }
            options={[
              ["off", "关闭"],
              ["afterDelay", "延迟后"],
              ["onFocusChange", "焦点切换时"],
            ]}
          />
          {draft.files.autoSave === "afterDelay" && (
            <NumberField
              label="自动保存延迟"
              value={draft.files.autoSaveDelayMs}
              minimum={250}
              maximum={60_000}
              suffix="ms"
              onChange={(autoSaveDelayMs) =>
                update("files", { autoSaveDelayMs })
              }
            />
          )}
          <CheckField
            label="关闭未保存文件时确认"
            checked={draft.files.confirmDirtyClose}
            onChange={(confirmDirtyClose) =>
              update("files", { confirmDirtyClose })
            }
          />
          <CheckField
            label="启动时恢复上次 SQL 项目"
            checked={draft.files.reopenLastProject}
            onChange={(reopenLastProject) =>
              update("files", { reopenLastProject })
            }
          />
        </SettingsSection>
      );
    case "results":
      return (
        <SettingsSection title="数据结果">
          <NumberField
            label="每页行数"
            value={draft.results.pageSize}
            minimum={50}
            maximum={1_000}
            suffix="行"
            onChange={(pageSize) => update("results", { pageSize })}
          />
          <NumberField
            label="驻留行数上限"
            value={draft.results.residentRowLimit}
            minimum={100}
            maximum={100_000}
            suffix="行"
            onChange={(residentRowLimit) =>
              update("results", { residentRowLimit })
            }
          />
          <NumberField
            label="结果内存上限"
            value={Math.round(draft.results.residentMemoryBytes / 1024 / 1024)}
            minimum={1}
            maximum={64}
            suffix="MiB"
            onChange={(memoryMiB) =>
              update("results", {
                residentMemoryBytes: memoryMiB * 1024 * 1024,
              })
            }
          />
          <TextField
            label="NULL 显示"
            value={draft.results.nullDisplay}
            onChange={(nullDisplay) => update("results", { nullDisplay })}
          />
          <NumberField
            label="查询超时"
            value={Math.round(draft.results.queryTimeoutMs / 1_000)}
            minimum={1}
            maximum={600}
            suffix="秒"
            onChange={(seconds) =>
              update("results", { queryTimeoutMs: seconds * 1_000 })
            }
          />
        </SettingsSection>
      );
    case "connections":
      return (
        <SettingsSection title="连接与安全">
          <NumberField
            label="连接超时"
            value={Math.round(draft.connections.timeoutMs / 1_000)}
            minimum={1}
            maximum={120}
            suffix="秒"
            onChange={(seconds) =>
              update("connections", { timeoutMs: seconds * 1_000 })
            }
          />
          <CheckField
            label="本地 OrdaDB 自动重连"
            checked={draft.connections.autoReconnectLocal}
            onChange={(autoReconnectLocal) =>
              update("connections", { autoReconnectLocal })
            }
          />
          <CheckField
            label="危险写入前确认"
            checked={draft.connections.confirmDangerousWrites}
            onChange={(confirmDangerousWrites) =>
              update("connections", { confirmDangerousWrites })
            }
          />
        </SettingsSection>
      );
    case "ai":
      return <AiSettingsSection draft={draft} setDraft={setDraft} />;
  }
}

function AiSettingsSection({
  draft,
  setDraft,
}: {
  draft: ConsoleSettingsV2;
  setDraft: Dispatch<SetStateAction<ConsoleSettingsV2>>;
}) {
  const runtimeMode = useWorkbenchStore((state) => state.aiRuntimeMode);
  const credentialStatus = useWorkbenchStore(
    (state) => state.aiCredentialStatus,
  );
  const credentialBusy = useWorkbenchStore((state) => state.aiCredentialBusy);
  const credentialError = useWorkbenchStore(
    (state) => state.aiCredentialError,
  );
  const refreshCredential = useWorkbenchStore(
    (state) => state.refreshAiCredentialStatus,
  );
  const promptCredential = useWorkbenchStore(
    (state) => state.promptAiCredential,
  );
  const deleteCredential = useWorkbenchStore(
    (state) => state.deleteAiCredential,
  );
  const credentialId = draft.ai.credentialId;
  const providerLabel =
    draft.ai.provider === "openai"
      ? "OpenAI"
      : draft.ai.provider === "openaiCompatible"
        ? "OpenAI-compatible"
        : "Ollama";

  useEffect(() => {
    void refreshCredential(credentialId);
  }, [credentialId, refreshCredential]);

  const updateAi = (values: Partial<ConsoleSettingsV2["ai"]>) => {
    setDraft((current) => ({
      ...current,
      ai: { ...current.ai, ...values },
    }));
  };

  const setProvider = (value: string) => {
    const provider = value as ConsoleSettingsV2["ai"]["provider"];
    if (provider === "openai") {
      updateAi({ provider, endpoint: undefined });
    } else if (provider === "ollama") {
      updateAi({ provider, endpoint: undefined, credentialId: undefined });
    } else {
      updateAi({ provider });
    }
  };

  const configureCredential = async () => {
    const targetId = credentialId ?? `provider-${draft.ai.provider}-default`;
    const status = await promptCredential(targetId, providerLabel).catch(
      () => null,
    );
    if (status) updateAi({ credentialId: status.credentialId });
  };

  const removeCredential = async () => {
    if (!credentialId) return;
    const removed = await deleteCredential(credentialId)
      .then(() => true)
      .catch(() => false);
    if (removed) updateAi({ credentialId: undefined });
  };

  const configured =
    credentialId !== undefined &&
    credentialStatus?.credentialId === credentialId &&
    credentialStatus.configured;

  return (
    <SettingsSection title="AI">
      <SelectField
        label="Provider"
        value={draft.ai.provider}
        onChange={setProvider}
        options={[
          ["openai", "OpenAI"],
          ["openaiCompatible", "OpenAI-compatible"],
          ["ollama", "Ollama"],
        ]}
      />
      <TextField
        label="模型"
        value={draft.ai.model}
        onChange={(model) => updateAi({ model })}
      />
      {draft.ai.provider !== "openai" && (
        <TextField
          label="端点"
          value={draft.ai.endpoint ?? ""}
          placeholder={
            draft.ai.provider === "ollama"
              ? "http://127.0.0.1:11434"
              : "https://example.com/v1/responses"
          }
          onChange={(endpoint) =>
            updateAi({ endpoint: endpoint || undefined })
          }
        />
      )}
      <SelectField
        label="Reasoning"
        value={draft.ai.reasoning}
        onChange={(reasoning) =>
          updateAi({
            reasoning: reasoning as ConsoleSettingsV2["ai"]["reasoning"],
          })
        }
        options={[
          ["low", "Low"],
          ["medium", "Medium"],
          ["high", "High"],
        ]}
      />
      <SelectField
        label="数据共享"
        value={draft.ai.dataSharing}
        onChange={(dataSharing) =>
          updateAi({
            dataSharing:
              dataSharing as ConsoleSettingsV2["ai"]["dataSharing"],
          })
        }
        options={[
          ["schemaOnly", "仅 Schema、SQL 与错误"],
          ["askEachTime", "样例数据逐次确认"],
          ["allowSamples", "允许脱敏样例"],
        ]}
      />

      <div className="settings-credential" aria-label="AI API Key 状态">
        <div>
          {configured ? (
            <Check size={14} aria-hidden="true" />
          ) : (
            <KeyRound size={14} aria-hidden="true" />
          )}
          <span>
            <strong>{configured ? "API Key 已安全保存" : "API Key 未配置"}</strong>
            <small>
              {runtimeMode === "preview"
                ? "Browser Preview 不读取或保存系统凭据"
                : configured
                  ? credentialStatus.accountLabel ?? "Windows Credential Manager"
                  : "仅通过 Windows 原生凭据窗口设置"}
            </small>
          </span>
        </div>
        {draft.ai.provider !== "ollama" && (
          <div>
            <button
              className="secondary-action"
              type="button"
              disabled={credentialBusy || runtimeMode === "preview"}
              onClick={() => void configureCredential()}
            >
              <KeyRound size={12} aria-hidden="true" />
              {configured ? "替换" : "设置"}
            </button>
            {credentialId && (
              <button
                className="danger-action"
                type="button"
                disabled={credentialBusy || runtimeMode === "preview"}
                onClick={() => void removeCredential()}
              >
                <Trash2 size={12} aria-hidden="true" />
                删除
              </button>
            )}
          </div>
        )}
      </div>
      {credentialError && (
        <p className="settings-credential-error" role="alert">
          <CircleAlert size={13} aria-hidden="true" />
          {credentialError.sqlState} · {credentialError.message}
        </p>
      )}
      <p className="settings-note">
        API Key 永远不会进入 React 状态、设置文件或 JavaScript 请求载荷。
      </p>
    </SettingsSection>
  );
}

function SettingsSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="settings-section" aria-labelledby={`settings-${title}`}>
      <header>
        <h3 id={`settings-${title}`}>{title}</h3>
      </header>
      <div className="settings-fields">{children}</div>
    </section>
  );
}

function TextField({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string;
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="settings-row">
      <span>{label}</span>
      <input
        type="text"
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function NumberField({
  label,
  value,
  minimum,
  maximum,
  suffix = "px",
  onChange,
}: {
  label: string;
  value: number;
  minimum: number;
  maximum: number;
  suffix?: string;
  onChange: (value: number) => void;
}) {
  return (
    <label className="settings-row">
      <span>{label}</span>
      <span className="settings-number">
        <input
          aria-label={label}
          type="number"
          min={minimum}
          max={maximum}
          value={value}
          onChange={(event) => onChange(Number(event.target.value))}
        />
        <small>{suffix}</small>
      </span>
    </label>
  );
}

function SelectField({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: Array<[value: string, label: string]>;
  onChange: (value: string) => void;
}) {
  return (
    <label className="settings-row">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map(([optionValue, optionLabel]) => (
          <option key={optionValue} value={optionValue}>
            {optionLabel}
          </option>
        ))}
      </select>
    </label>
  );
}

function CheckField({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="settings-check settings-check--row">
      <span>{label}</span>
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
    </label>
  );
}
