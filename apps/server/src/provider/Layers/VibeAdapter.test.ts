// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderDriverKind, ThreadId, TurnId, VibeSettings } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import { ServerConfig } from "../../config.ts";
import { makeVibeAdapter, vibePromptSettlementBelongsToTurn } from "./VibeAdapter.ts";

const decodeVibeSettings = Schema.decodeSync(VibeSettings);
const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = process.execPath;

async function makeMockVibeWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-acp-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-vibe.sh");
  const envExports = Object.entries({ T3_ACP_VIBE_CONFIG: "1", ...extraEnv })
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
exec ${JSON.stringify(mockAgentCommand)} ${JSON.stringify(mockAgentPath)} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

const vibeAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-vibe-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string) =>
  makeVibeAdapter(decodeVibeSettings({ binaryPath })).pipe(Effect.orDie);

function waitForFileContent(filePath: string, attempts = 80): Effect.Effect<string> {
  const readAttempt = (remainingAttempts: number): Effect.Effect<string> =>
    Effect.gen(function* () {
      if (remainingAttempts <= 0) {
        return yield* Effect.die(new Error(`Timed out waiting for file content at ${filePath}`));
      }
      const raw = yield* Effect.tryPromise(() => NodeFSP.readFile(filePath, "utf8")).pipe(
        Effect.orElseSucceed(() => ""),
      );
      if (raw.trim().length > 0) return raw;
      yield* Effect.sleep("25 millis");
      return yield* readAttempt(remainingAttempts - 1);
    });
  return readAttempt(attempts);
}

it("requires a prompt settlement to match the active Vibe turn", () => {
  const staleTurnId = TurnId.make("stale-turn");
  const replacementTurnId = TurnId.make("replacement-turn");

  assert.isFalse(vibePromptSettlementBelongsToTurn(undefined, staleTurnId));
  assert.isFalse(vibePromptSettlementBelongsToTurn(replacementTurnId, staleTurnId));
  assert.isTrue(vibePromptSettlementBelongsToTurn(staleTurnId, staleTurnId));
});

it.layer(vibeAdapterTestLayer)("VibeAdapter", (it) => {
  it.effect("restarts an existing thread when session inputs change", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(() => makeMockVibeWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-restart-thread");
      const firstCwd = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-first-cwd-")),
      );
      const secondCwd = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-second-cwd-")),
      );

      const firstSession = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: firstCwd,
        runtimeMode: "approval-required",
      });
      const restartedSession = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: secondCwd,
        runtimeMode: "full-access",
      });

      assert.equal(firstSession.cwd, NodePath.resolve(firstCwd));
      assert.equal(firstSession.runtimeMode, "approval-required");
      assert.equal(restartedSession.cwd, NodePath.resolve(secondCwd));
      assert.equal(restartedSession.runtimeMode, "full-access");
      assert.lengthOf(yield* adapter.listSessions(), 1);
    }),
  );

  it.effect("stops active ACP sessions when the adapter scope closes", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-adapter-finalizer-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({ T3_ACP_EXIT_LOG_PATH: exitLogPath }),
      );
      const adapterScope = yield* Scope.make();
      const adapter = yield* makeTestAdapter(wrapperPath).pipe(
        Effect.provideService(Scope.Scope, adapterScope),
      );
      const threadId = ThreadId.make("vibe-finalizer-thread");

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.isTrue(yield* adapter.hasSession(threadId));

      yield* Scope.close(adapterScope, Exit.void);

      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.include(yield* waitForFileContent(exitLogPath), "SIGTERM");
    }),
  );
});
