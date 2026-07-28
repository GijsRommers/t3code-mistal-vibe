// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { createModelCapabilities } from "@t3tools/shared/model";
import { VibeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  buildVibeCapabilities,
  buildVibeModelsFromConfigOptions,
  checkVibeProviderStatus,
} from "./VibeProvider.ts";

const decodeVibeSettings = Schema.decodeSync(VibeSettings);
const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");

function shellSingleQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

const configOptions = [
  {
    id: "model",
    name: "Model",
    category: "model" as const,
    type: "select" as const,
    currentValue: "mistral-medium-3.5",
    options: [
      { value: "mistral-medium-3.5", name: "Mistral Medium 3.5" },
      { value: "devstral-small", name: "Devstral Small" },
      { value: "local", name: "Local" },
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
      { value: "low", name: "Low" },
      { value: "high", name: "High" },
      { value: "max", name: "Max" },
    ],
  },
];

describe("VibeProvider", () => {
  it("discovers every Vibe model and marks the live default", () => {
    const models = buildVibeModelsFromConfigOptions(configOptions);
    expect(models.map((model) => model.slug)).toEqual([
      "mistral-medium-3.5",
      "devstral-small",
      "local",
    ]);
    expect(models[0]?.isDefault).toBe(true);
    expect(models.every((model) => model.capabilities?.optionDescriptors?.length === 1)).toBe(true);
  });

  it("exposes Vibe thinking levels as a model option", () => {
    expect(buildVibeCapabilities(configOptions)).toEqual(
      createModelCapabilities({
        optionDescriptors: [
          {
            id: "thinking",
            label: "Thinking",
            type: "select",
            currentValue: "high",
            options: [
              { id: "off", label: "Off" },
              { id: "low", label: "Low" },
              { id: "high", label: "High", isDefault: true },
              { id: "max", label: "Max" },
            ],
          },
        ],
      }),
    );
  });
});

it.layer(NodeServices.layer)("checkVibeProviderStatus", (it) => {
  it.effect("does not infer authentication from successful ACP discovery", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-vibe-discovery-" });
          const vibePath = path.join(dir, "vibe-acp");
          yield* fs.writeFileString(
            vibePath,
            [
              "#!/bin/sh",
              'if [ "$1" = "--version" ]; then',
              '  printf "vibe 0.0.99\\n"',
              "  exit 0",
              "fi",
              "export T3_ACP_VIBE_CONFIG=1",
              `exec ${shellSingleQuote(process.execPath)} ${shellSingleQuote(mockAgentPath)} "$@"`,
              "",
            ].join("\n"),
          );
          yield* fs.chmod(vibePath, 0o755);

          return yield* checkVibeProviderStatus(
            decodeVibeSettings({ enabled: true, binaryPath: vibePath }),
          );
        }),
      );

      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth.status).toBe("unknown");
      expect(snapshot.models.map((model) => model.slug)).toContain("mistral-medium-3.5");
    }),
  );

  it.effect("does not claim authentication failed when ACP discovery fails", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-vibe-discovery-" });
          const vibePath = path.join(dir, "vibe-acp");
          yield* fs.writeFileString(
            vibePath,
            ["#!/bin/sh", 'printf "vibe 0.0.99\\n"', "exit 0", ""].join("\n"),
          );
          yield* fs.chmod(vibePath, 0o755);

          return yield* checkVibeProviderStatus(
            decodeVibeSettings({ enabled: true, binaryPath: vibePath }),
          );
        }),
      );

      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.auth.status).toBe("unknown");
      expect(snapshot.message).toContain("ACP discovery failed");
      expect(snapshot.message).not.toContain("vibe --setup");
    }),
  );
});
