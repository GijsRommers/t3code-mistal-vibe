import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type * as EffectAcpErrors from "effect-acp/errors";

import {
  applyVibeSessionConfiguration,
  buildVibeAcpSpawnInput,
  buildVibeEnvironment,
  readVibeWorkspaceTrust,
  resolveVibeModelId,
  trustVibeWorkspace,
  vibeModeForRuntimeMode,
} from "./VibeAcpSupport.ts";

describe("VibeAcpSupport", () => {
  it("spawns vibe-acp without interactive authentication arguments", () => {
    expect(buildVibeAcpSpawnInput(undefined, "/tmp/project", { TEST: "1" })).toEqual({
      command: "vibe-acp",
      args: [],
      cwd: "/tmp/project",
      env: { TEST: "1" },
    });
  });

  it("overrides Vibe's Local preset with an Ollama-backed Devstral model", () => {
    const environment = buildVibeEnvironment(
      {
        binaryPath: "vibe-acp",
        ollamaEnabled: true,
        ollamaBaseUrl: "http://127.0.0.1:11434/v1",
        ollamaModel: "devstral-small-2",
      },
      { KEEP_ME: "yes" },
    );
    expect(environment?.KEEP_ME).toBe("yes");
    expect(JSON.parse(environment?.VIBE_PROVIDERS ?? "[]")).toEqual([
      {
        name: "t3-ollama",
        api_base: "http://127.0.0.1:11434/v1",
        api_style: "openai",
        backend: "generic",
        api_key_env_var: "",
      },
    ]);
    expect(JSON.parse(environment?.VIBE_MODELS ?? "[]")).toEqual([
      {
        name: "devstral-small-2",
        provider: "t3-ollama",
        alias: "local",
        input_price: 0,
        output_price: 0,
      },
    ]);
  });

  it("maps T3 runtime and interaction modes to Vibe modes", () => {
    expect(vibeModeForRuntimeMode("approval-required")).toBe("default");
    expect(vibeModeForRuntimeMode("auto-accept-edits")).toBe("accept-edits");
    expect(vibeModeForRuntimeMode("auto")).toBe("accept-edits");
    expect(vibeModeForRuntimeMode("full-access")).toBe("auto-approve");
    expect(vibeModeForRuntimeMode("full-access", "plan")).toBe("plan");
  });

  it.effect("uses ACP extension method names for Vibe workspace trust", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, unknown]> = [];
      const runtime = {
        request: (method: string, payload: unknown) =>
          Effect.sync(() => {
            calls.push([method, payload]);
            return { trust_status: "trusted", details: null };
          }),
      };

      yield* readVibeWorkspaceTrust(runtime, "/tmp/project");
      yield* trustVibeWorkspace(runtime, {
        cwd: "/tmp/project",
        sessionId: "session-1",
      });

      expect(calls).toEqual([
        ["_trust/status", { cwd: "/tmp/project" }],
        [
          "_trust/decision",
          {
            cwd: "/tmp/project",
            sessionId: "session-1",
            decision: "trust_cwd",
          },
        ],
      ]);
    }),
  );

  it.effect("applies model, thinking, and mode through config options", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, string | boolean]> = [];
      const runtime = {
        getConfigOptions: Effect.succeed([
          {
            id: "model",
            name: "Model",
            category: "model" as const,
            type: "select" as const,
            currentValue: "mistral-medium-3.5",
            options: [
              { value: "mistral-medium-3.5", name: "Mistral Medium 3.5" },
              { value: "devstral-small", name: "Devstral Small" },
            ],
          },
          {
            id: "thinking",
            name: "Thinking",
            category: "model_config" as const,
            type: "select" as const,
            currentValue: "high",
            options: [
              { value: "off", name: "Off" },
              { value: "high", name: "High" },
            ],
          },
        ]),
        setModel: (value: string) =>
          Effect.sync(() => {
            calls.push(["model", value]);
          }),
        setConfigOption: (id: string, value: string | boolean) =>
          Effect.sync(() => {
            calls.push([id, value]);
          }),
        setMode: (value: string) =>
          Effect.sync(() => {
            calls.push(["mode", value]);
            return {};
          }),
      };

      yield* applyVibeSessionConfiguration({
        runtime,
        model: "devstral-small",
        selections: [
          { id: "thinking", value: "off" },
          { id: "unknown", value: "ignored" },
        ],
        mode: "plan",
        mapError: (cause: EffectAcpErrors.AcpError) => cause,
      });
      expect(calls).toEqual([
        ["model", "devstral-small"],
        ["thinking", "off"],
        ["mode", "plan"],
      ]);
      expect(resolveVibeModelId(undefined)).toBe("mistral-medium-3.5");
      expect(ProviderDriverKind.make("vibe")).toBe("vibe");
    }),
  );
});
