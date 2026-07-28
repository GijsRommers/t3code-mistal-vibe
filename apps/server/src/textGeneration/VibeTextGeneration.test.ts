// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, VibeSettings } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import { makeVibeTextGeneration } from "./VibeTextGeneration.ts";

const decodeVibeSettings = Schema.decodeSync(VibeSettings);
const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../scripts/acp-mock-agent.ts");

function shellSingleQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function makeVibeWrapper(dir: string, env: Record<string, string>): string {
  const binaryPath = NodePath.join(dir, "vibe-acp");
  NodeFS.writeFileSync(
    binaryPath,
    [
      "#!/bin/sh",
      "export T3_ACP_VIBE_CONFIG=1",
      ...Object.entries(env).map(([key, value]) => `export ${key}=${shellSingleQuote(value)}`),
      `exec ${JSON.stringify(process.execPath)} ${JSON.stringify(mockAgentPath)} "$@"`,
      "",
    ].join("\n"),
    "utf8",
  );
  NodeFS.chmodSync(binaryPath, 0o755);
  return binaryPath;
}

it.layer(NodeServices.layer)("VibeTextGeneration", (it) => {
  it.effect("uses Vibe's valid default mode for headless text generation", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-vibe-text-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          NodeFS.rmSync(tempDir, { recursive: true, force: true });
        }),
      );
      const binaryPath = makeVibeWrapper(tempDir, {
        T3_ACP_PROMPT_RESPONSE_TEXT: '{"title":"Add Mistral Vibe support"}',
      });
      const textGeneration = yield* makeVibeTextGeneration(decodeVibeSettings({ binaryPath }));

      const generated = yield* textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "support Mistral Vibe through ACP",
        modelSelection: createModelSelection(ProviderInstanceId.make("vibe"), "mistral-medium-3.5"),
      });

      expect(generated.title).toBe("Add Mistral Vibe support");
    }).pipe(Effect.scoped),
  );
});
