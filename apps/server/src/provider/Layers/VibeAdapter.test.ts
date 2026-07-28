// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
  VibeSettings,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { ServerConfig } from "../../config.ts";
import { drainVibeEventsUnlessStopped, makeVibeAdapter } from "./VibeAdapter.ts";

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

const makeTestAdapter = (binaryPath: string, overrides?: { readonly trustWorkspace?: boolean }) =>
  makeVibeAdapter(decodeVibeSettings({ binaryPath, ...overrides })).pipe(Effect.orDie);

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

function waitForFileOccurrences(
  filePath: string,
  expected: string,
  count: number,
  attempts = 80,
): Effect.Effect<string> {
  const readAttempt = (remainingAttempts: number): Effect.Effect<string> =>
    Effect.gen(function* () {
      if (remainingAttempts <= 0) {
        return yield* Effect.die(
          new Error(`Timed out waiting for ${count} '${expected}' entries in ${filePath}`),
        );
      }
      const raw = yield* Effect.tryPromise(() => NodeFSP.readFile(filePath, "utf8")).pipe(
        Effect.orElseSucceed(() => ""),
      );
      if (raw.split(expected).length - 1 >= count) return raw;
      yield* Effect.sleep("25 millis");
      return yield* readAttempt(remainingAttempts - 1);
    });
  return readAttempt(attempts);
}

it.effect("stops waiting for ACP event drainage when the session stops", () =>
  Effect.gen(function* () {
    const stoppedSignal = yield* Deferred.make<void>();
    yield* Deferred.succeed(stoppedSignal, undefined);

    yield* drainVibeEventsUnlessStopped(Effect.never, stoppedSignal).pipe(
      Effect.timeout("250 millis"),
    );
  }),
);

it.effect("ignores ACP event drainage failures so turn settlement can continue", () =>
  Effect.gen(function* () {
    const stoppedSignal = yield* Deferred.make<void>();

    yield* drainVibeEventsUnlessStopped(
      Effect.die(new Error("simulated drain failure")),
      stoppedSignal,
    ).pipe(Effect.timeout("250 millis"));
  }),
);

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

  it.effect("serializes concurrent starts for the same thread", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-concurrent-start-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({ T3_ACP_EXIT_LOG_PATH: exitLogPath }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-concurrent-start-thread");
      const startInput = {
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access" as const,
      };

      const started = yield* Effect.all(
        [adapter.startSession(startInput), adapter.startSession(startInput)],
        { concurrency: "unbounded" },
      );

      assert.lengthOf(started, 2);
      assert.lengthOf(yield* adapter.listSessions(), 1);
      assert.include(yield* waitForFileOccurrences(exitLogPath, "SIGTERM", 1), "SIGTERM");

      yield* adapter.stopAll();
      assert.include(yield* waitForFileOccurrences(exitLogPath, "SIGTERM", 2), "SIGTERM");
    }),
  );

  it.effect("starts an untrusted workspace when automatic trust is disabled", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-untrusted-workspace-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_VIBE_TRUST_STATUS: "untrusted",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath, { trustWorkspace: false });
      const threadId = ThreadId.make("vibe-untrusted-workspace-thread");

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      assert.equal(session.status, "ready");
      const requests = yield* waitForFileContent(requestLogPath);
      assert.include(requests, '"method":"_trust/status"');
      assert.notInclude(requests, '"method":"_trust/decision"');

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("serializes stopSession with an in-flight session start", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-start-stop-race-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_SET_CONFIG_OPTION_DELAY_MS: "300",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-start-stop-race-thread");

      const startFiber = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("vibe"),
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        })
        .pipe(Effect.forkChild);
      yield* waitForFileContent(requestLogPath);
      const stopFiber = yield* adapter.stopSession(threadId).pipe(Effect.forkChild);

      yield* Fiber.join(startFiber);
      yield* Fiber.join(stopFiber);
      assert.isFalse(yield* adapter.hasSession(threadId));
    }).pipe(Effect.scoped, Effect.timeout("4 seconds"), TestClock.withLive),
  );

  it.effect("releases the reserved turn when prompt preparation fails", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(() => makeMockVibeWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-prompt-preparation-failure");

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const failedTurn = yield* adapter
        .sendTurn({ threadId, input: " ", attachments: [] })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failedTurn));
      const [readySession] = yield* adapter.listSessions();
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      const retry = yield* adapter.sendTurn({
        threadId,
        input: "continue after preparation failure",
        attachments: [],
      });
      assert.equal(retry.threadId, threadId);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("settles a cancellation queued during turn preparation without prompting Vibe", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-prepare-cancel-race-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_SET_CONFIG_OPTION_DELAY_MS: "300",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-prepare-cancel-race-thread");
      const events: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const requestsAfterStart = yield* waitForFileContent(requestLogPath);
      const initialConfigRequestCount =
        requestsAfterStart.split('"method":"session/set_config_option"').length - 1;

      const sendFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "cancel while preparing",
          attachments: [],
          modelSelection: {
            instanceId: ProviderInstanceId.make("vibe"),
            model: "devstral-small",
            options: [{ id: "thinking", value: "low" }],
          },
        })
        .pipe(Effect.forkChild);
      yield* waitForFileOccurrences(
        requestLogPath,
        '"method":"session/set_config_option"',
        initialConfigRequestCount + 1,
      );
      yield* adapter.interruptTurn(threadId);

      const error = yield* Fiber.join(sendFiber).pipe(Effect.flip);
      yield* Effect.yieldNow;

      const requests = yield* waitForFileOccurrences(
        requestLogPath,
        '"method":"session/cancel"',
        1,
      );
      const started = events.filter(
        (event) => event.type === "turn.started" && event.threadId === threadId,
      );
      const completed = events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && event.threadId === threadId,
      );
      assert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag === "ProviderAdapterRequestError") {
        assert.equal(error.method, "session/prompt");
        assert.equal(error.detail, "Mistral Vibe prompt was interrupted during preparation.");
      }
      assert.lengthOf(started, 0);
      assert.lengthOf(completed, 1);
      assert.equal(completed[0]?.payload.state, "cancelled");
      assert.notInclude(requests, '"method":"session/prompt"');
      assert.include(requests, '"method":"session/cancel"');

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.timeout("5 seconds"), TestClock.withLive),
  );

  it.effect("settles a cancelled prompt once and permits a follow-up turn", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-in-flight-cancel-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
          T3_ACP_PROMPT_DELAY_MS: "200",
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-interrupt-turn-race");
      const events: ProviderRuntimeEvent[] = [];
      const firstTurnStarted = yield* Deferred.make<TurnId>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.started" && event.turnId !== undefined
              ? Deferred.succeed(firstTurnStarted, event.turnId).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const firstSendFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel the first prompt", attachments: [] })
        .pipe(Effect.forkChild);
      const firstTurnId = yield* Deferred.await(firstTurnStarted).pipe(Effect.timeout("2 seconds"));
      yield* waitForFileOccurrences(requestLogPath, '"method":"session/prompt"', 1);
      yield* adapter.interruptTurn(threadId, firstTurnId).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(firstSendFiber).pipe(Effect.timeout("2 seconds"));

      const followUp = yield* adapter
        .sendTurn({ threadId, input: "complete the follow-up", attachments: [] })
        .pipe(Effect.timeout("2 seconds"));

      const completed = events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && event.threadId === threadId,
      );
      const lateDelta = events.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
          event.type === "content.delta" && event.payload.delta === "late after cancel",
      );

      assert.deepEqual(
        completed.map((event) => [event.turnId, event.payload.state]),
        [
          [firstTurnId, "cancelled"],
          [followUp.turnId, "completed"],
        ],
      );
      assert.isUndefined(lateDelta);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.timeout("6 seconds"), TestClock.withLive),
  );

  it.effect("cancels a running prompt while a steering prompt waits to start", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-steering-cancel-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_SET_CONFIG_OPTION_DELAY_MS: "100",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-steering-cancel");
      const firstSendSettled = yield* Deferred.make<void>();
      const events: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const requestsAfterStart = yield* waitForFileContent(requestLogPath);
      const initialConfigRequestCount =
        requestsAfterStart.split('"method":"session/set_config_option"').length - 1;

      const firstSendFiber = yield* adapter
        .sendTurn({ threadId, input: "keep running", attachments: [] })
        .pipe(
          Effect.ensuring(Deferred.succeed(firstSendSettled, undefined).pipe(Effect.ignore)),
          Effect.forkChild,
        );
      yield* waitForFileOccurrences(requestLogPath, '"method":"session/prompt"', 1);

      const steeringSendFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "steer the running turn",
          attachments: [],
          modelSelection: {
            instanceId: ProviderInstanceId.make("vibe"),
            model: "mistral-medium-3.5",
            options: [{ id: "thinking", value: "low" }],
          },
        })
        .pipe(Effect.forkChild);
      yield* waitForFileOccurrences(
        requestLogPath,
        '"method":"session/set_config_option"',
        initialConfigRequestCount + 1,
      );
      yield* Effect.sleep("150 millis");
      assert.isFalse(yield* Deferred.isDone(firstSendSettled));

      const interruptFiber = yield* adapter.interruptTurn(threadId).pipe(Effect.forkChild);
      yield* waitForFileOccurrences(requestLogPath, '"method":"session/cancel"', 1, 20);
      assert.isFalse(yield* Deferred.isDone(firstSendSettled));

      yield* Fiber.join(interruptFiber);
      yield* Fiber.join(firstSendFiber);
      yield* Fiber.join(steeringSendFiber);
      const completed = events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && event.threadId === threadId,
      );
      assert.lengthOf(completed, 1);
      assert.equal(completed[0]?.payload.state, "cancelled");

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.timeout("5 seconds"), TestClock.withLive),
  );

  it.effect("releases the active turn when the sendTurn caller is interrupted", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-caller-interrupt-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-caller-interrupt");

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const sendFiber = yield* adapter
        .sendTurn({ threadId, input: "abandon me", attachments: [] })
        .pipe(Effect.forkChild);
      yield* waitForFileOccurrences(requestLogPath, '"method":"session/prompt"', 1);
      yield* Fiber.interrupt(sendFiber);

      const [abandonedSession] = yield* adapter.listSessions();
      assert.equal(abandonedSession?.status, "ready");
      assert.isUndefined(abandonedSession?.activeTurnId);

      const followUp = yield* adapter
        .sendTurn({ threadId, input: "continue after the caller went away", attachments: [] })
        .pipe(Effect.timeout("2 seconds"));
      assert.equal(followUp.threadId, threadId);

      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.timeout("6 seconds"), TestClock.withLive),
  );

  it.effect("steers a running turn instead of opening a second Vibe turn", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "vibe-concurrent-turn-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockVibeWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_PROMPT_DELAY_MS: "1200",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("vibe-concurrent-send-turn");
      const events: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("vibe"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const firstFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "first concurrent turn",
          attachments: [],
          modelSelection: {
            instanceId: ProviderInstanceId.make("vibe"),
            model: "mistral-medium-3.5",
            options: [{ id: "thinking", value: "low" }],
          },
        })
        .pipe(Effect.forkChild);
      yield* waitForFileOccurrences(requestLogPath, '"method":"session/prompt"', 1);
      const second = yield* adapter
        .sendTurn({ threadId, input: "second concurrent turn", attachments: [] })
        .pipe(Effect.timeout("4 seconds"));
      const first = yield* Fiber.join(firstFiber);

      assert.equal(String(first.turnId), String(second.turnId));

      const started = events.filter(
        (event) => event.type === "turn.started" && event.threadId === threadId,
      );
      const completed = events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && event.threadId === threadId,
      );
      assert.equal(started.length, 1);
      assert.deepEqual(
        completed.map((event) => event.payload.state),
        ["completed"],
      );
      assert.equal(completed[0]?.turnId, started[0]?.turnId);
      const requests = yield* waitForFileOccurrences(
        requestLogPath,
        '"method":"session/prompt"',
        2,
      );
      assert.equal(requests.split('"method":"session/prompt"').length - 1, 2);
      const thread = yield* adapter.readThread(threadId);
      assert.lengthOf(thread.turns, 1);
      assert.lengthOf(thread.turns[0]?.items ?? [], 2);
      const [session] = yield* adapter.listSessions();
      assert.equal(session?.status, "ready");
      assert.isUndefined(session?.activeTurnId);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.timeout("6 seconds"), TestClock.withLive),
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
