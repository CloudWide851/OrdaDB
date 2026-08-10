import { invoke as tauriInvoke } from "@tauri-apps/api/core";
import {
  listen as tauriListen,
  type UnlistenFn,
} from "@tauri-apps/api/event";
import { isTauriRuntime } from "./tauri";

export type AiProviderKind =
  | "openAi"
  | "openAiCompatible"
  | "ollama"
  | "fake";
export type AiReasoningEffort = "low" | "medium" | "high";
export type AiDataSharingPolicy =
  | "schemaOnly"
  | "askEachTime"
  | "allowSamples";
export type AiHistoryRole = "user" | "assistant";
export type AiAuditStatus =
  | "proposed"
  | "approvalRequired"
  | "approved"
  | "denied"
  | "started"
  | "completed"
  | "cancelled"
  | "error";

export interface AiProviderSettings {
  kind: AiProviderKind;
  model: string;
  endpoint?: string;
  reasoning: AiReasoningEffort;
  dataSharing: AiDataSharingPolicy;
  credentialId?: string;
}

export interface AiHistoryEntry {
  role: AiHistoryRole;
  text: string;
  createdAtMs: number;
}

export interface AiAuditEntry {
  runId: string;
  toolCallId: string;
  toolName: string;
  argumentHash: string;
  status: AiAuditStatus;
  summary: string;
  createdAtMs: number;
}

export interface AiPersistenceV1 {
  version: 1;
  history: AiHistoryEntry[];
  audit: AiAuditEntry[];
}

export interface AiRunRequest {
  runId: string;
  connectionId: string;
  userText: string;
  settings: AiProviderSettings;
  history: AiHistoryEntry[];
  includeSampleValues: boolean;
}

export interface AiContextDisclosure {
  categories: string[];
  columns: string[];
  itemCount: number;
  estimatedBytes: number;
  redactionSummary: string;
  valuesIncluded: boolean;
}

export interface AiApprovalRequest {
  approvalId: string;
  expiresInMs: number;
  connectionId: string;
  toolName: string;
  preview: string;
  impactSummary: string;
}

export interface AiUsage {
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
}

export interface AiError {
  sqlState: string;
  message: string;
  detail: string | null;
  hint: string | null;
  position: number | null;
  queryId: string;
}

interface AiRunEventBase {
  runId: string;
  sequence: number;
}

export type AiRunEvent = AiRunEventBase &
  (
    | { kind: "started" }
    | { kind: "textDelta"; delta: string }
    | { kind: "contextDisclosure"; disclosure: AiContextDisclosure }
    | { kind: "toolProposed"; callId: string; toolName: string }
    | { kind: "toolStarted"; callId: string; toolName: string }
    | {
        kind: "toolCompleted";
        callId: string;
        toolName: string;
        summary: string;
        truncated: boolean;
      }
    | { kind: "approvalRequired"; request: AiApprovalRequest }
    | { kind: "approvalResolved"; approvalId: string; approved: boolean }
    | { kind: "usage"; usage: AiUsage }
    | { kind: "cancelled" }
    | { kind: "completed" }
    | { kind: "error"; error: AiError }
  );

export interface AiRunOperation {
  runId: string;
  events: AsyncIterable<AiRunEvent>;
}

export interface AiCredentialPromptRequest {
  credentialId: string;
  providerLabel: string;
}

export interface AiCredentialStatus {
  credentialId: string;
  configured: boolean;
  accountLabel: string | null;
}

export interface AiApprovalDecision {
  approvalId: string;
  approve: boolean;
}

export interface AiClient {
  readonly mode: "desktop" | "preview";
  start(request: AiRunRequest): Promise<AiRunOperation>;
  cancel(runId: string): Promise<void>;
  decide(decision: AiApprovalDecision): Promise<void>;
  state(): Promise<AiPersistenceV1>;
  promptCredential(
    request: AiCredentialPromptRequest,
  ): Promise<AiCredentialStatus | null>;
  credentialStatus(credentialId: string): Promise<AiCredentialStatus>;
  deleteCredential(credentialId: string): Promise<void>;
}

export interface AiTauriBridge {
  invoke(command: string, payload?: Record<string, unknown>): Promise<unknown>;
  listen(
    eventName: string,
    onPayload: (payload: unknown) => void,
  ): Promise<UnlistenFn>;
}

const AI_RUN_EVENT = "ai://run";
const MAX_PREVIEW_HISTORY = 64;
const MAX_PREVIEW_AUDIT = 256;

class TauriAiClient implements AiClient {
  readonly mode = "desktop";

  constructor(private readonly bridge: AiTauriBridge) {}

  async start(request: AiRunRequest): Promise<AiRunOperation> {
    const stream = createAiEventStream(this.bridge);
    try {
      await stream.listen();
      const started = decodeAiRunStarted(
        await this.bridge.invoke("ai_start_run", { request }),
      );
      stream.select(started.runId);
      return { runId: started.runId, events: stream.events() };
    } catch (error) {
      stream.dispose();
      throw error;
    }
  }

  async cancel(runId: string) {
    await this.bridge.invoke("ai_cancel_run", { runId });
  }

  async decide(decision: AiApprovalDecision) {
    await this.bridge.invoke("ai_decide", { decision });
  }

  async state() {
    return decodeAiPersistence(await this.bridge.invoke("ai_state"));
  }

  async promptCredential(request: AiCredentialPromptRequest) {
    const value = await this.bridge.invoke("ai_prompt_credential", { request });
    return value === null ? null : decodeAiCredentialStatus(value);
  }

  async credentialStatus(credentialId: string) {
    return decodeAiCredentialStatus(
      await this.bridge.invoke("ai_credential_status", { credentialId }),
    );
  }

  async deleteCredential(credentialId: string) {
    await this.bridge.invoke("ai_delete_credential", { credentialId });
  }
}

interface PreviewRunController {
  request: AiRunRequest;
  approvalId: string | null;
  decision: boolean | null | undefined;
  cancelled: boolean;
  resume: (() => void) | null;
}

export class PreviewAiClient implements AiClient {
  readonly mode = "preview";
  private readonly activeRuns = new Map<string, PreviewRunController>();
  private history: AiHistoryEntry[] = [];
  private audit: AiAuditEntry[] = [];

  async start(request: AiRunRequest): Promise<AiRunOperation> {
    if (this.activeRuns.has(request.runId)) {
      throw previewError("55000", "Preview AI run ID is already active");
    }
    const controller: PreviewRunController = {
      request: cloneRunRequest(request),
      approvalId: null,
      decision: undefined,
      cancelled: false,
      resume: null,
    };
    this.activeRuns.set(request.runId, controller);
    this.appendHistory({
      role: "user",
      text: request.userText,
      createdAtMs: Date.now(),
    });
    return {
      runId: request.runId,
      events: this.previewEvents(controller),
    };
  }

  async cancel(runId: string) {
    const controller = this.activeRuns.get(runId);
    if (!controller) {
      throw previewError("42704", "Preview AI run is not active");
    }
    controller.cancelled = true;
    controller.resume?.();
  }

  async decide(decision: AiApprovalDecision) {
    const controller = [...this.activeRuns.values()].find(
      (candidate) => candidate.approvalId === decision.approvalId,
    );
    if (!controller || controller.decision !== undefined) {
      throw previewError("55000", "Preview AI approval is no longer active");
    }
    controller.decision = decision.approve;
    controller.resume?.();
  }

  async state(): Promise<AiPersistenceV1> {
    return {
      version: 1,
      history: this.history.map((entry) => ({ ...entry })),
      audit: this.audit.map((entry) => ({ ...entry })),
    };
  }

  async promptCredential(
    request: AiCredentialPromptRequest,
  ): Promise<AiCredentialStatus | null> {
    void request;
    throw previewError(
      "0A000",
      "Browser Preview does not open or persist native credentials",
    );
  }

  async credentialStatus(credentialId: string): Promise<AiCredentialStatus> {
    return { credentialId, configured: false, accountLabel: null };
  }

  async deleteCredential(credentialId: string): Promise<void> {
    void credentialId;
    throw previewError(
      "0A000",
      "Browser Preview does not modify native credentials",
    );
  }

  resetForTests() {
    for (const controller of this.activeRuns.values()) {
      controller.cancelled = true;
      controller.resume?.();
    }
    this.activeRuns.clear();
    this.history = [];
    this.audit = [];
  }

  private async *previewEvents(
    controller: PreviewRunController,
  ): AsyncIterable<AiRunEvent> {
    const { request } = controller;
    let sequence = 0;
    let assistantText = "";
    const event = <T extends Omit<AiRunEvent, "runId" | "sequence">>(
      payload: T,
    ) =>
      ({
        runId: request.runId,
        sequence: (sequence += 1),
        ...payload,
      }) as AiRunEvent;
    const appendAssistant = (text: string) => {
      assistantText += text;
      return event({ kind: "textDelta", delta: text });
    };

    try {
      yield event({ kind: "started" });
      yield event({
        kind: "contextDisclosure",
        disclosure: {
          categories: ["Preview schema", "用户输入"],
          columns: [],
          itemCount: 0,
          estimatedBytes: request.userText.length,
          redactionSummary: "不包含数据库样例值",
          valuesIncluded: false,
        },
      });

      if (/等待|\/wait|slow/i.test(request.userText)) {
        yield appendAssistant("Preview 正在等待取消；未连接或查询任何数据库。\n");
        await waitForPreviewDecision(controller);
        yield event({ kind: "cancelled" });
        return;
      }

      const mutationRequested = /\b(delete|drop|insert|update|alter|create)\b|删除|写入|修改/i.test(
        request.userText,
      );
      if (mutationRequested) {
        const callId = `preview-tool-${request.runId}`;
        const approvalId = `preview-approval-${request.runId}`;
        controller.approvalId = approvalId;
        this.appendAudit(request.runId, callId, "execute_sql", "proposed", "Preview proposal");
        yield event({ kind: "toolProposed", callId, toolName: "execute_sql" });
        this.appendAudit(
          request.runId,
          callId,
          "execute_sql",
          "approvalRequired",
          "Preview approval required",
        );
        yield event({
          kind: "approvalRequired",
          request: {
            approvalId,
            expiresInMs: 120_000,
            connectionId: request.connectionId,
            toolName: "execute_sql",
            preview: "Preview mutation proposal",
            impactSummary: "可能修改数据库；Preview 永远不会实际执行",
          },
        });
        await waitForPreviewDecision(controller);
        if (controller.cancelled) {
          this.appendAudit(
            request.runId,
            callId,
            "execute_sql",
            "cancelled",
            "Preview run cancelled",
          );
          yield event({ kind: "cancelled" });
          return;
        }
        const approved = controller.decision === true;
        yield event({ kind: "approvalResolved", approvalId, approved });
        this.appendAudit(
          request.runId,
          callId,
          "execute_sql",
          approved ? "approved" : "denied",
          approved ? "Preview approval accepted" : "Preview approval denied",
        );
        if (!approved) {
          yield appendAssistant("已拒绝该操作。Preview 未执行任何数据库命令。");
          yield event({
            kind: "usage",
            usage: { inputTokens: 12, outputTokens: 10, reasoningTokens: 0 },
          });
          yield event({ kind: "completed" });
          return;
        }
        yield event({ kind: "toolStarted", callId, toolName: "execute_sql" });
        yield event({
          kind: "toolCompleted",
          callId,
          toolName: "execute_sql",
          summary: "Preview 安全演示完成；数据库操作未执行",
          truncated: false,
        });
        this.appendAudit(
          request.runId,
          callId,
          "execute_sql",
          "completed",
          "Preview only; no database operation executed",
        );
        yield appendAssistant(
          "审批流程已演示。Browser Preview 不会执行数据库写入；请在 Windows 桌面端连接后运行。",
        );
      } else {
        const callId = `preview-tool-${request.runId}`;
        this.appendAudit(request.runId, callId, "validate_sql", "proposed", "Preview proposal");
        yield event({ kind: "toolProposed", callId, toolName: "validate_sql" });
        yield event({ kind: "toolStarted", callId, toolName: "validate_sql" });
        yield event({
          kind: "toolCompleted",
          callId,
          toolName: "validate_sql",
          summary: "确定性 Preview 响应；未访问数据库",
          truncated: false,
        });
        this.appendAudit(
          request.runId,
          callId,
          "validate_sql",
          "completed",
          "Deterministic Preview response",
        );
        yield appendAssistant(
          "这是确定性 Browser Preview。桌面端连接数据库后，我可以解释 Schema、生成 SQL，并在安全边界内运行只读工具。",
        );
      }
      yield event({
        kind: "usage",
        usage: { inputTokens: 24, outputTokens: 32, reasoningTokens: 0 },
      });
      yield event({ kind: "completed" });
    } finally {
      if (assistantText) {
        this.appendHistory({
          role: "assistant",
          text: assistantText,
          createdAtMs: Date.now(),
        });
      }
      this.activeRuns.delete(request.runId);
    }
  }

  private appendHistory(entry: AiHistoryEntry) {
    this.history = [...this.history, entry].slice(-MAX_PREVIEW_HISTORY);
  }

  private appendAudit(
    runId: string,
    toolCallId: string,
    toolName: string,
    status: AiAuditStatus,
    summary: string,
  ) {
    this.audit = [
      ...this.audit,
      {
        runId,
        toolCallId,
        toolName,
        argumentHash: "preview-redacted",
        status,
        summary,
        createdAtMs: Date.now(),
      },
    ].slice(-MAX_PREVIEW_AUDIT);
  }
}

export function createTauriAiClient(bridge: AiTauriBridge): AiClient {
  return new TauriAiClient(bridge);
}

let client: AiClient | undefined;

export function getAiClient(): AiClient {
  client ??= isTauriRuntime()
    ? createTauriAiClient({
        invoke: (command, payload) => tauriInvoke<unknown>(command, payload),
        listen: async (eventName, onPayload) =>
          tauriListen<unknown>(eventName, (event) => onPayload(event.payload)),
      })
    : new PreviewAiClient();
  return client;
}

export function resetPreviewAiClientForTests() {
  if (client instanceof PreviewAiClient) client.resetForTests();
}

export function decodeAiRunEvent(value: unknown): AiRunEvent {
  const record = requireRecord(value, "AI run event");
  const runId = requireString(record.runId, "AI run event runId");
  const sequence = requireInteger(record.sequence, "AI run event sequence");
  const kind = requireString(record.kind, "AI run event kind");
  const base = { runId, sequence };
  switch (kind) {
    case "started":
    case "cancelled":
    case "completed":
      return { ...base, kind };
    case "textDelta":
      return {
        ...base,
        kind,
        delta: requireString(record.delta, "AI text delta"),
      };
    case "contextDisclosure":
      return {
        ...base,
        kind,
        disclosure: decodeDisclosure(record.disclosure),
      };
    case "toolProposed":
    case "toolStarted":
      return {
        ...base,
        kind,
        callId: requireString(record.callId, "AI tool callId"),
        toolName: requireString(record.toolName, "AI tool name"),
      };
    case "toolCompleted":
      return {
        ...base,
        kind,
        callId: requireString(record.callId, "AI tool callId"),
        toolName: requireString(record.toolName, "AI tool name"),
        summary: requireString(record.summary, "AI tool summary"),
        truncated: requireBoolean(record.truncated, "AI tool truncated"),
      };
    case "approvalRequired":
      return {
        ...base,
        kind,
        request: decodeApprovalRequest(record.request),
      };
    case "approvalResolved":
      return {
        ...base,
        kind,
        approvalId: requireString(record.approvalId, "AI approval ID"),
        approved: requireBoolean(record.approved, "AI approval decision"),
      };
    case "usage":
      return { ...base, kind, usage: decodeUsage(record.usage) };
    case "error":
      return { ...base, kind, error: decodeAiError(record.error) };
    default:
      throw new Error(`Unsupported AI run event kind: ${kind}`);
  }
}

export function decodeAiPersistence(value: unknown): AiPersistenceV1 {
  const record = requireRecord(value, "AI persistence");
  if (record.version !== 1) throw new Error("Unsupported AI persistence version");
  const history = requireArray(record.history, "AI history").map<AiHistoryEntry>((entry) => {
    const item = requireRecord(entry, "AI history entry");
    const role = requireString(item.role, "AI history role");
    if (role !== "user" && role !== "assistant") {
      throw new Error("Unsupported AI history role");
    }
    return {
      role: role as AiHistoryRole,
      text: requireString(item.text, "AI history text"),
      createdAtMs: requireInteger(item.createdAtMs, "AI history timestamp"),
    };
  });
  const audit = requireArray(record.audit, "AI audit").map(decodeAuditEntry);
  return { version: 1, history, audit };
}

export function normalizeAiError(error: unknown): AiError {
  try {
    return decodeAiError(error);
  } catch {
    return previewError(
      "XX000",
      error instanceof Error ? error.message : "未知 AI 运行错误",
    );
  }
}

function createAiEventStream(bridge: AiTauriBridge) {
  let selectedRunId: string | undefined;
  let unlisten: UnlistenFn | undefined;
  const buffered: Array<AiRunEvent | Error> = [];
  const queue: Array<AiRunEvent | Error> = [];
  const waiters: Array<() => void> = [];

  const push = (item: AiRunEvent | Error) => {
    if (!selectedRunId) {
      buffered.push(item);
      return;
    }
    if (!(item instanceof Error) && item.runId !== selectedRunId) return;
    queue.push(item);
    waiters.shift()?.();
  };

  return {
    async listen() {
      unlisten = await bridge.listen(AI_RUN_EVENT, (payload) => {
        try {
          push(decodeAiRunEvent(payload));
        } catch (error) {
          push(error instanceof Error ? error : new Error("Invalid AI event"));
        }
      });
    },
    select(runId: string) {
      selectedRunId = runId;
      for (const item of buffered.splice(0)) push(item);
    },
    dispose() {
      unlisten?.();
      unlisten = undefined;
      for (const resolve of waiters.splice(0)) resolve();
    },
    async *events(): AsyncIterable<AiRunEvent> {
      try {
        while (true) {
          if (queue.length === 0) {
            await new Promise<void>((resolve) => waiters.push(resolve));
          }
          const item = queue.shift();
          if (!item) continue;
          if (item instanceof Error) throw item;
          yield item;
          if (isTerminalAiEvent(item)) return;
        }
      } finally {
        unlisten?.();
        unlisten = undefined;
      }
    },
  };
}

function decodeAiRunStarted(value: unknown) {
  const record = requireRecord(value, "AI run start");
  return { runId: requireString(record.runId, "AI run start runId") };
}

function decodeAiCredentialStatus(value: unknown): AiCredentialStatus {
  const record = requireRecord(value, "AI credential status");
  return {
    credentialId: requireString(record.credentialId, "AI credential ID"),
    configured: requireBoolean(record.configured, "AI credential configured"),
    accountLabel: optionalString(record.accountLabel, "AI credential account"),
  };
}

function decodeDisclosure(value: unknown): AiContextDisclosure {
  const record = requireRecord(value, "AI context disclosure");
  return {
    categories: requireStringArray(record.categories, "AI disclosure categories"),
    columns: requireStringArray(record.columns, "AI disclosure columns"),
    itemCount: requireInteger(record.itemCount, "AI disclosure item count"),
    estimatedBytes: requireInteger(
      record.estimatedBytes,
      "AI disclosure estimated bytes",
    ),
    redactionSummary: requireString(
      record.redactionSummary,
      "AI disclosure redaction summary",
    ),
    valuesIncluded: requireBoolean(
      record.valuesIncluded,
      "AI disclosure values included",
    ),
  };
}

function decodeApprovalRequest(value: unknown): AiApprovalRequest {
  const record = requireRecord(value, "AI approval request");
  return {
    approvalId: requireString(record.approvalId, "AI approval ID"),
    expiresInMs: requireInteger(record.expiresInMs, "AI approval expiry"),
    connectionId: requireString(record.connectionId, "AI approval connection"),
    toolName: requireString(record.toolName, "AI approval tool"),
    preview: requireString(record.preview, "AI approval preview"),
    impactSummary: requireString(record.impactSummary, "AI approval impact"),
  };
}

function decodeUsage(value: unknown): AiUsage {
  const record = requireRecord(value, "AI usage");
  return {
    inputTokens: requireInteger(record.inputTokens, "AI input tokens"),
    outputTokens: requireInteger(record.outputTokens, "AI output tokens"),
    reasoningTokens: requireInteger(record.reasoningTokens, "AI reasoning tokens"),
  };
}

function decodeAiError(value: unknown): AiError {
  const record = requireRecord(value, "AI error");
  return {
    sqlState: requireString(record.sqlState, "AI error SQLSTATE"),
    message: requireString(record.message, "AI error message"),
    detail: optionalString(record.detail, "AI error detail"),
    hint: optionalString(record.hint, "AI error hint"),
    position:
      record.position === null || record.position === undefined
        ? null
        : requireInteger(record.position, "AI error position"),
    queryId: requireString(record.queryId, "AI error queryId"),
  };
}

function decodeAuditEntry(value: unknown): AiAuditEntry {
  const record = requireRecord(value, "AI audit entry");
  const status = requireString(record.status, "AI audit status");
  if (!isAuditStatus(status)) throw new Error("Unsupported AI audit status");
  return {
    runId: requireString(record.runId, "AI audit runId"),
    toolCallId: requireString(record.toolCallId, "AI audit toolCallId"),
    toolName: requireString(record.toolName, "AI audit tool name"),
    argumentHash: requireString(record.argumentHash, "AI audit argument hash"),
    status,
    summary: requireString(record.summary, "AI audit summary"),
    createdAtMs: requireInteger(record.createdAtMs, "AI audit timestamp"),
  };
}

function requireRecord(value: unknown, context: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${context} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, context: string) {
  if (typeof value !== "string") throw new Error(`${context} must be a string`);
  return value;
}

function optionalString(value: unknown, context: string): string | null {
  if (value === null || value === undefined) return null;
  return requireString(value, context);
}

function requireBoolean(value: unknown, context: string) {
  if (typeof value !== "boolean") throw new Error(`${context} must be a boolean`);
  return value;
}

function requireInteger(value: unknown, context: string) {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`${context} must be a non-negative safe integer`);
  }
  return value as number;
}

function requireArray(value: unknown, context: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`${context} must be an array`);
  return value;
}

function requireStringArray(value: unknown, context: string) {
  return requireArray(value, context).map((item) => requireString(item, context));
}

function isAuditStatus(value: string): value is AiAuditStatus {
  return [
    "proposed",
    "approvalRequired",
    "approved",
    "denied",
    "started",
    "completed",
    "cancelled",
    "error",
  ].includes(value);
}

function isTerminalAiEvent(event: AiRunEvent) {
  return (
    event.kind === "cancelled" ||
    event.kind === "completed" ||
    event.kind === "error"
  );
}

function previewError(sqlState: string, message: string): AiError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: "ai-preview",
  };
}

function cloneRunRequest(request: AiRunRequest): AiRunRequest {
  return {
    ...request,
    settings: { ...request.settings },
    history: request.history.map((entry) => ({ ...entry })),
  };
}

async function waitForPreviewDecision(controller: PreviewRunController) {
  if (controller.cancelled || controller.decision !== undefined) return;
  await new Promise<void>((resolve) => {
    controller.resume = resolve;
  });
  controller.resume = null;
}
