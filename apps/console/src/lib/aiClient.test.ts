import { describe, expect, it } from "vitest";
import {
  PreviewAiClient,
  createTauriAiClient,
  decodeAiPersistence,
  decodeAiRunEvent,
  type AiRunEvent,
  type AiRunRequest,
  type AiTauriBridge,
} from "./aiClient";

const request = (userText = "解释当前 Schema"): AiRunRequest => ({
  runId: "run-1",
  connectionId: "preview-connection",
  userText,
  settings: {
    kind: "fake",
    model: "preview",
    reasoning: "medium",
    dataSharing: "schemaOnly",
  },
  history: [],
  includeSampleValues: false,
});

describe("AI client boundary", () => {
  it("decodes the flattened camelCase event and persistence contracts once", () => {
    expect(
      decodeAiRunEvent({
        runId: "run-1",
        sequence: 3,
        kind: "approvalRequired",
        request: {
          approvalId: "approval-1",
          expiresInMs: 120_000,
          connectionId: "connection-1",
          toolName: "execute_sql",
          preview: "UPDATE items SET active = false",
          impactSummary: "May update rows",
        },
      }),
    ).toMatchObject({
      kind: "approvalRequired",
      request: { approvalId: "approval-1", expiresInMs: 120_000 },
    });
    expect(
      decodeAiPersistence({
        version: 1,
        history: [{ role: "assistant", text: "done", createdAtMs: 1 }],
        audit: [
          {
            runId: "run-1",
            toolCallId: "call-1",
            toolName: "query",
            argumentHash: "redacted",
            status: "completed",
            summary: "bounded result",
            createdAtMs: 2,
          },
        ],
      }),
    ).toMatchObject({ version: 1, history: [{ text: "done" }] });
    expect(() =>
      decodeAiRunEvent({ runId: "run-1", sequence: 1, kind: "unknownTool" }),
    ).toThrow(/Unsupported AI run event/);
    expect(() =>
      decodeAiRunEvent({ runId: "run-1", sequence: -1, kind: "started" }),
    ).toThrow(/non-negative safe integer/);
  });

  it("listens before invoke, replays buffered events, and cleans up at terminal", async () => {
    const order: string[] = [];
    let onPayload: ((payload: unknown) => void) | undefined;
    let unlistenCount = 0;
    const bridge: AiTauriBridge = {
      async listen(eventName, listener) {
        order.push(`listen:${eventName}`);
        onPayload = listener;
        return () => {
          unlistenCount += 1;
        };
      },
      async invoke(command, payload) {
        order.push(`invoke:${command}`);
        expect(command).toBe("ai_start_run");
        expect(payload).toMatchObject({
          request: { runId: "run-1", userText: "解释当前 Schema" },
        });
        onPayload?.({ runId: "run-1", sequence: 1, kind: "started" });
        queueMicrotask(() => {
          onPayload?.({
            runId: "run-1",
            sequence: 2,
            kind: "textDelta",
            delta: "ok",
          });
          onPayload?.({ runId: "run-1", sequence: 3, kind: "completed" });
        });
        return { runId: "run-1" };
      },
    };
    const client = createTauriAiClient(bridge);
    const operation = await client.start({
      ...request(),
      settings: {
        kind: "openAi",
        model: "gpt-5.6",
        reasoning: "medium",
        dataSharing: "schemaOnly",
        credentialId: "provider-openai-default",
      },
    });
    const events: AiRunEvent[] = [];
    for await (const event of operation.events) events.push(event);

    expect(order).toEqual(["listen:ai://run", "invoke:ai_start_run"]);
    expect(events.map((event) => event.kind)).toEqual([
      "started",
      "textDelta",
      "completed",
    ]);
    expect(unlistenCount).toBe(1);
  });
});

describe("PreviewAiClient", () => {
  it("produces deterministic, visibly non-executing tool output", async () => {
    const client = new PreviewAiClient();
    const operation = await client.start(request());
    const events: AiRunEvent[] = [];
    for await (const event of operation.events) events.push(event);

    expect(events.map((event) => event.kind)).toEqual([
      "started",
      "contextDisclosure",
      "toolProposed",
      "toolStarted",
      "toolCompleted",
      "textDelta",
      "usage",
      "completed",
    ]);
    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "toolCompleted",
          summary: expect.stringContaining("未访问数据库"),
        }),
      ]),
    );
    await expect(client.state()).resolves.toMatchObject({
      version: 1,
      history: [
        { role: "user", text: "解释当前 Schema" },
        { role: "assistant", text: expect.stringContaining("Browser Preview") },
      ],
    });
  });

  it("keeps mutation approval interactive while never executing it", async () => {
    const client = new PreviewAiClient();
    const operation = await client.start(request("删除旧记录"));
    const events: AiRunEvent[] = [];
    for await (const event of operation.events) {
      events.push(event);
      if (event.kind === "approvalRequired") {
        await client.decide({
          approvalId: event.request.approvalId,
          approve: true,
        });
      }
    }

    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: "approvalResolved", approved: true }),
        expect.objectContaining({
          kind: "toolCompleted",
          summary: expect.stringContaining("数据库操作未执行"),
        }),
        expect.objectContaining({
          kind: "textDelta",
          delta: expect.stringContaining("不会执行数据库写入"),
        }),
      ]),
    );
  });

  it("cancels a waiting Preview run and rejects native credential mutation", async () => {
    const client = new PreviewAiClient();
    const operation = await client.start(request("/wait"));
    const events: AiRunEvent[] = [];
    for await (const event of operation.events) {
      events.push(event);
      if (event.kind === "textDelta") await client.cancel(operation.runId);
    }

    expect(events.at(-1)?.kind).toBe("cancelled");
    await expect(
      client.promptCredential({
        credentialId: "preview-key",
        providerLabel: "OpenAI",
      }),
    ).rejects.toMatchObject({ sqlState: "0A000" });
    await expect(client.credentialStatus("preview-key")).resolves.toEqual({
      credentialId: "preview-key",
      configured: false,
      accountLabel: null,
    });
  });
});
