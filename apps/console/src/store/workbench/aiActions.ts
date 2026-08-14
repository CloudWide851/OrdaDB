import {
  normalizeAiError,
  type AiError,
  type AiPersistenceV1,
  type AiRunEvent,
} from "../../lib/aiClient";
import type { WorkbenchActionContext, StoreGet, StoreSet } from "./context";
import type {
  AiToolActivity,
  AiVisibleMessage,
  WorkbenchState,
} from "./types";

export function createAiActions({
  ai,
  get,
  set,
}: WorkbenchActionContext) {
  return {  startAiRun: async (userText, includeSampleValues = false) => {
    const trimmed = userText.trim();
    if (!trimmed) {
      set({ aiError: localAiError("22023", "请输入要交给 AI 的问题") });
      return;
    }
    if (get().aiRunId) {
      set({ aiError: localAiError("55000", "已有 AI 任务正在运行") });
      return;
    }
    const connection = get().connection;
    if (ai.mode === "desktop" && !connection) {
      set({
        dataSourceOpen: true,
        aiError: localAiError("08003", "请先连接数据源，再启动桌面 AI 任务"),
      });
      return;
    }

    const runId = nextAiRunId();
    const createdAtMs = Date.now();
    const settings = get().settings.ai;
    const request = {
      runId,
      connectionId: connection?.connectionId ?? "preview-connection",
      userText: trimmed,
      settings: {
        kind:
          settings.provider === "openai"
            ? ("openAi" as const)
            : settings.provider === "openaiCompatible"
              ? ("openAiCompatible" as const)
              : ("ollama" as const),
        model: settings.model,
        endpoint: settings.endpoint,
        reasoning: settings.reasoning,
        dataSharing: settings.dataSharing,
        credentialId: settings.credentialId,
      },
      history: get().aiPersistedHistory.slice(-32).map((entry) => ({
        ...entry,
      })),
      includeSampleValues,
    };
    set((state) => ({
      inspectorMode: "ai",
      inspectorVisible: true,
      aiMessages: appendAiMessage(state.aiMessages, {
        id: `user:${runId}`,
        role: "user",
        text: trimmed,
        createdAtMs,
      }),
      aiDisclosures: [],
      aiTools: [],
      aiApproval: null,
      aiUsage: null,
      aiRunId: runId,
      aiRunStatus: "running",
      aiLastSequence: 0,
      aiError: null,
      notice: ai.mode === "preview" ? "AI Preview 正在生成" : "AI 正在生成",
    }));

    try {
      const operation = await ai.start(request);
      if (operation.runId !== runId) {
        throw localAiError("XX000", "AI 启动响应的 runId 不匹配");
      }
      let terminal = false;
      for await (const event of operation.events) {
        if (event.runId !== runId) {
          throw localAiError("XX000", "AI 事件流包含其他任务的事件");
        }
        if (event.sequence !== get().aiLastSequence + 1) {
          throw localAiError("XX000", "AI 事件序列不连续");
        }
        terminal = applyAiRunEvent(set, get, event) || terminal;
      }
      if (!terminal) {
        throw localAiError("XX000", "AI 事件流在终态之前结束");
      }
      try {
        applyAiPersistenceProjection(set, await ai.state(), false);
      } catch (error) {
        set({ aiError: normalizeAiError(error) });
      }
    } catch (error) {
      const normalized = normalizeAiError(error);
      set({
        aiRunId: null,
        aiRunStatus: "error",
        aiApproval: null,
        aiError: normalized,
        notice: `${normalized.sqlState} · ${normalized.message}`,
      });
    }
  },

  cancelAiRun: async () => {
    const runId = get().aiRunId;
    if (!runId) return;
    try {
      await ai.cancel(runId);
      set({ notice: "已发送 AI 取消请求" });
    } catch (error) {
      set({ aiError: normalizeAiError(error) });
    }
  },

  decideAiApproval: async (approve) => {
    const approval = get().aiApproval;
    if (!approval) return;
    try {
      await ai.decide({ approvalId: approval.approvalId, approve });
    } catch (error) {
      set({ aiError: normalizeAiError(error) });
    }
  },

  refreshAiCredentialStatus: async (credentialId) => {
    if (!credentialId) {
      set({ aiCredentialStatus: null, aiCredentialError: null });
      return;
    }
    try {
      const status = await ai.credentialStatus(credentialId);
      set({ aiCredentialStatus: status, aiCredentialError: null });
    } catch (error) {
      set({
        aiCredentialStatus: null,
        aiCredentialError: normalizeAiError(error),
      });
    }
  },

  promptAiCredential: async (credentialId, providerLabel) => {
    set({ aiCredentialBusy: true, aiCredentialError: null });
    try {
      const status = await ai.promptCredential({ credentialId, providerLabel });
      if (status) set({ aiCredentialStatus: status });
      return status;
    } catch (error) {
      const normalized = normalizeAiError(error);
      set({ aiCredentialError: normalized });
      throw normalized;
    } finally {
      set({ aiCredentialBusy: false });
    }
  },

  deleteAiCredential: async (credentialId) => {
    set({ aiCredentialBusy: true, aiCredentialError: null });
    try {
      await ai.deleteCredential(credentialId);
      set({
        aiCredentialStatus: {
          credentialId,
          configured: false,
          accountLabel: null,
        },
      });
    } catch (error) {
      const normalized = normalizeAiError(error);
      set({ aiCredentialError: normalized });
      throw normalized;
    } finally {
      set({ aiCredentialBusy: false });
    }
  },

  } satisfies Partial<WorkbenchState>;
}
const MAX_VISIBLE_AI_MESSAGES = 64;
const MAX_VISIBLE_AI_TEXT = 64 * 1024;
const MAX_VISIBLE_AI_TOOLS = 64;
const MAX_VISIBLE_AI_DISCLOSURES = 16;

export function applyAiPersistenceProjection(
  set: StoreSet,
  persistence: AiPersistenceV1,
  replaceMessages = true,
) {
  set({
    aiPersistedHistory: persistence.history.map((entry) => ({ ...entry })),
    aiAudit: persistence.audit.map((entry) => ({ ...entry })),
    ...(replaceMessages
      ? {
          aiMessages: persistence.history.map((entry, index) => ({
            id: `history:${entry.createdAtMs}:${index}`,
            role: entry.role,
            text: entry.text,
            createdAtMs: entry.createdAtMs,
          })),
        }
      : {}),
  });
}

function applyAiRunEvent(set: StoreSet, get: StoreGet, event: AiRunEvent) {
  const sequence = event.sequence;
  switch (event.kind) {
    case "started":
      set({ aiLastSequence: sequence, aiRunStatus: "running" });
      return false;
    case "textDelta":
      set((state) => ({
        aiLastSequence: sequence,
        aiMessages: appendAssistantDelta(
          state.aiMessages,
          event.runId,
          event.delta,
        ),
      }));
      return false;
    case "contextDisclosure":
      set((state) => ({
        aiLastSequence: sequence,
        aiDisclosures: [...state.aiDisclosures, event.disclosure].slice(
          -MAX_VISIBLE_AI_DISCLOSURES,
        ),
      }));
      return false;
    case "toolProposed":
      set((state) => ({
        aiLastSequence: sequence,
        aiTools: upsertAiTool(state.aiTools, {
          callId: event.callId,
          toolName: event.toolName,
          status: "proposed",
          summary: null,
          truncated: false,
        }),
      }));
      return false;
    case "toolStarted":
      set((state) => ({
        aiLastSequence: sequence,
        aiRunStatus: "running",
        aiTools: upsertAiTool(state.aiTools, {
          callId: event.callId,
          toolName: event.toolName,
          status: "running",
          summary: null,
          truncated: false,
        }),
      }));
      return false;
    case "toolCompleted":
      set((state) => ({
        aiLastSequence: sequence,
        aiTools: upsertAiTool(state.aiTools, {
          callId: event.callId,
          toolName: event.toolName,
          status: "completed",
          summary: event.summary,
          truncated: event.truncated,
        }),
      }));
      return false;
    case "approvalRequired":
      set((state) => ({
        aiLastSequence: sequence,
        aiApproval: event.request,
        aiRunStatus: "waitingApproval",
        aiTools: markAiToolWaiting(
          state.aiTools,
          event.request.toolName,
        ),
      }));
      return false;
    case "approvalResolved":
      set((state) => ({
        aiLastSequence: sequence,
        aiApproval:
          state.aiApproval?.approvalId === event.approvalId
            ? null
            : state.aiApproval,
        aiRunStatus: "running",
      }));
      return false;
    case "usage":
      set({ aiLastSequence: sequence, aiUsage: event.usage });
      return false;
    case "cancelled":
      set({
        aiLastSequence: sequence,
        aiRunId: null,
        aiRunStatus: "cancelled",
        aiApproval: null,
        notice: "AI 任务已取消",
      });
      return true;
    case "completed":
      set({
        aiLastSequence: sequence,
        aiRunId: null,
        aiRunStatus: "completed",
        aiApproval: null,
        notice: get().aiRuntimeMode === "preview" ? "AI Preview 已完成" : "AI 回复完成",
      });
      return true;
    case "error":
      set({
        aiLastSequence: sequence,
        aiRunId: null,
        aiRunStatus: "error",
        aiApproval: null,
        aiError: event.error,
        notice: `${event.error.sqlState} · ${event.error.message}`,
      });
      return true;
  }
}

function appendAssistantDelta(
  messages: AiVisibleMessage[],
  runId: string,
  delta: string,
) {
  const id = `assistant:${runId}`;
  const existing = messages.findIndex((message) => message.id === id);
  if (existing < 0) {
    return appendAiMessage(messages, {
      id,
      role: "assistant",
      text: delta.slice(0, MAX_VISIBLE_AI_TEXT),
      createdAtMs: Date.now(),
    });
  }
  return messages.map((message, index) =>
    index === existing
      ? {
          ...message,
          text: `${message.text}${delta}`.slice(0, MAX_VISIBLE_AI_TEXT),
        }
      : message,
  );
}

function appendAiMessage(
  messages: AiVisibleMessage[],
  message: AiVisibleMessage,
) {
  return [...messages, message].slice(-MAX_VISIBLE_AI_MESSAGES);
}

function upsertAiTool(
  tools: AiToolActivity[],
  activity: AiToolActivity,
) {
  const existing = tools.findIndex((tool) => tool.callId === activity.callId);
  if (existing < 0) {
    return [...tools, activity].slice(-MAX_VISIBLE_AI_TOOLS);
  }
  return tools.map((tool, index) =>
    index === existing ? { ...tool, ...activity } : tool,
  );
}

function markAiToolWaiting(tools: AiToolActivity[], toolName: string) {
  let index = -1;
  for (let candidate = tools.length - 1; candidate >= 0; candidate -= 1) {
    const tool = tools[candidate];
    if (tool?.toolName === toolName && tool.status === "proposed") {
      index = candidate;
      break;
    }
  }
  return tools.map((tool, candidateIndex) =>
    candidateIndex === index ? { ...tool, status: "waitingApproval" as const } : tool,
  );
}

let aiRunCounter = 0;

function nextAiRunId() {
  aiRunCounter += 1;
  return `console-${Date.now().toString(36)}-${aiRunCounter.toString(36)}`;
}

function localAiError(sqlState: string, message: string): AiError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: "ai-console",
  };
}
