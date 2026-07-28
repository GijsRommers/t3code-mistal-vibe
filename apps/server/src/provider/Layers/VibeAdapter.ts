import {
  ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
  type VibeSettings,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  applyVibeSessionConfiguration,
  makeVibeAcpRuntime,
  readVibeWorkspaceTrust,
  resolveVibeModelId,
  trustVibeWorkspace,
  vibeModeForRuntimeMode,
} from "../acp/VibeAcpSupport.ts";
import type { VibeAdapterShape } from "../Services/VibeAdapter.ts";

const PROVIDER = ProviderDriverKind.make("vibe");
const RESUME_VERSION = 1 as const;

export interface VibeAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface VibeSessionContext {
  readonly threadId: ThreadId;
  readonly acpSessionId: string;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly stoppedSignal: Deferred.Deferred<void>;
  session: ProviderSession;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly interruptedTurnIds: Set<TurnId>;
  notificationFiber?: Fiber.Fiber<void, never>;
  activeTurnId?: TurnId;
  promptStartedSignal?: Deferred.Deferred<void>;
  turnPreparationInProgress: boolean;
  interruptPreparationRequested: boolean;
  stopped: boolean;
}

export function vibePromptSettlementBelongsToTurn(
  activeTurnId: TurnId | undefined,
  turnId: TurnId,
): boolean {
  return activeTurnId === turnId;
}

export function drainVibeEventsUnlessStopped(
  drainEvents: Effect.Effect<void>,
  stoppedSignal: Deferred.Deferred<void>,
): Effect.Effect<void> {
  return Effect.raceFirst(drainEvents, Deferred.await(stoppedSignal)).pipe(
    Effect.catchCause(() => Effect.void),
  );
}

function selectPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  return request.options.find((option) => option.kind === kind)?.optionId;
}

function resumeSessionId(raw: unknown): string | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  return record.schemaVersion === RESUME_VERSION &&
    typeof record.sessionId === "string" &&
    record.sessionId.trim()
    ? record.sessionId.trim()
    : undefined;
}

export function makeVibeAdapter(settings: VibeSettings, options?: VibeAdapterOptions) {
  return Effect.gen(function* () {
    const instanceId = options?.instanceId ?? ProviderInstanceId.make("vibe");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* ServerConfig;
    const crypto = yield* Crypto.Crypto;
    const sessions = new Map<ThreadId, VibeSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextId = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Mistral Vibe runtime identifier.",
            cause,
          }),
      ),
    );
    const stamp = () =>
      Effect.all({
        eventId: Effect.map(nextId, EventId.make),
        createdAt: nowIso,
      });
    const publish = (event: ProviderRuntimeEvent) =>
      PubSub.publish(events, event).pipe(Effect.asVoid);
    const requireSession = (threadId: ThreadId) => {
      const ctx = sessions.get(threadId);
      return !ctx || ctx.stopped
        ? Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }))
        : Effect.succeed(ctx);
    };
    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });
    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));
    const callbackError = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpTransportError({
              detail: "Failed to process a Mistral Vibe ACP callback.",
              cause,
            }),
        ),
      );

    const stopContext = (ctx: VibeSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* Deferred.succeed(ctx.stoppedSignal, undefined).pipe(Effect.ignore);
        yield* Effect.forEach(
          ctx.pendingApprovals.values(),
          (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
          { discard: true },
        );
        yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
        sessions.delete(ctx.threadId);
        yield* publish({
          type: "session.exited",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSessionUnlocked: VibeAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required.",
          });
        }
        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped) {
          yield* stopContext(existing);
        }

        const cwd = path.resolve(input.cwd);
        const sessionScope = yield* Scope.make();
        let sessionScopeTransferred = false;
        yield* Effect.addFinalizer(() =>
          sessionScopeTransferred
            ? Effect.void
            : Effect.sync(() => {
                sessions.delete(input.threadId);
              }).pipe(Effect.andThen(Scope.close(sessionScope, Exit.void).pipe(Effect.ignore))),
        );
        const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
        const stoppedSignal = yield* Deferred.make<void>();
        const resumedSessionId = resumeSessionId(input.resumeCursor);
        const modelSelection =
          input.modelSelection?.instanceId === instanceId ? input.modelSelection : undefined;
        const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
        let liveContext: VibeSessionContext | undefined;
        const acp = yield* makeVibeAcpRuntime({
          vibeSettings: settings,
          ...(options?.environment ? { environment: options.environment } : {}),
          childProcessSpawner,
          cwd,
          ...(resumedSessionId ? { resumeSessionId: resumedSessionId } : {}),
          clientInfo: { name: "t3-code", version: "0.0.0" },
          requestLogger: (event) => {
            const promptStartedSignal = liveContext?.promptStartedSignal;
            return event.method === "session/prompt" &&
              event.status === "started" &&
              promptStartedSignal
              ? Deferred.succeed(promptStartedSignal, undefined).pipe(Effect.asVoid)
              : Effect.void;
          },
          ...(mcpSession
            ? {
                mcpServers: [
                  {
                    type: "http" as const,
                    name: "t3-code",
                    url: mcpSession.endpoint,
                    headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
                  },
                ],
              }
            : {}),
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: cause.message,
                cause,
              }),
          ),
        );

        yield* acp.handleRequestPermission((params) =>
          callbackError(
            Effect.gen(function* () {
              if (input.runtimeMode === "full-access") {
                const autoOption =
                  selectPermissionOptionId(params, "acceptForSession") ??
                  selectPermissionOptionId(params, "accept");
                if (autoOption) {
                  return { outcome: { outcome: "selected" as const, optionId: autoOption } };
                }
              }
              const permissionRequest = parsePermissionRequest(params);
              const requestId = ApprovalRequestId.make(yield* nextId);
              const decision = yield* Deferred.make<ProviderApprovalDecision>();
              pendingApprovals.set(requestId, { decision });
              const live = sessions.get(input.threadId);
              const turnId = live?.activeTurnId;
              yield* publish(
                makeAcpRequestOpenedEvent({
                  stamp: yield* stamp(),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  requestId: RuntimeRequestId.make(requestId),
                  permissionRequest,
                  detail: permissionRequest.detail ?? "Mistral Vibe requests permission.",
                  args: params,
                  source: "acp.jsonrpc",
                  method: "session/request_permission",
                  rawPayload: params,
                }),
              );
              const resolved = yield* Deferred.await(decision);
              pendingApprovals.delete(requestId);
              yield* publish(
                makeAcpRequestResolvedEvent({
                  stamp: yield* stamp(),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  requestId: RuntimeRequestId.make(requestId),
                  permissionRequest,
                  decision: resolved,
                }),
              );
              const optionId =
                resolved === "cancel" ? undefined : selectPermissionOptionId(params, resolved);
              return optionId
                ? { outcome: { outcome: "selected" as const, optionId } }
                : { outcome: { outcome: "cancelled" as const } };
            }),
          ),
        );

        const started = yield* acp
          .start()
          .pipe(
            Effect.mapError((cause) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", cause),
            ),
          );
        const trust = yield* readVibeWorkspaceTrust(acp, cwd).pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "trust/status", cause),
          ),
        );
        if (trust.trust_status === "untrusted" && settings.trustWorkspace) {
          const trusted = yield* trustVibeWorkspace(acp, {
            cwd,
            sessionId: started.sessionId,
          }).pipe(
            Effect.mapError((cause) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "trust/decision", cause),
            ),
          );
          if (trusted.trust_status === "untrusted") {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "Vibe declined to trust this workspace.",
            });
          }
        }

        yield* applyVibeSessionConfiguration({
          runtime: acp,
          model: modelSelection?.model,
          selections: modelSelection?.options,
          mode: vibeModeForRuntimeMode(input.runtimeMode),
          mapError: (cause, method) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, method, cause),
        });

        const now = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: instanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          model: resolveVibeModelId(modelSelection?.model),
          threadId: input.threadId,
          resumeCursor: { schemaVersion: RESUME_VERSION, sessionId: started.sessionId },
          createdAt: now,
          updatedAt: now,
        };
        const ctx: VibeSessionContext = {
          threadId: input.threadId,
          acpSessionId: started.sessionId,
          scope: sessionScope,
          acp,
          pendingApprovals,
          stoppedSignal,
          session,
          turns: [],
          interruptedTurnIds: new Set(),
          turnPreparationInProgress: false,
          interruptPreparationRequested: false,
          stopped: false,
        };
        sessions.set(input.threadId, ctx);
        liveContext = ctx;

        ctx.notificationFiber = yield* Stream.runForEach(acp.getEvents(), (event) =>
          Effect.gen(function* () {
            if (event._tag === "EventStreamBarrier") {
              yield* Deferred.succeed(event.acknowledge, undefined);
              return;
            }
            if (event._tag === "ModeChanged") return;
            const turnId = ctx.activeTurnId;
            if (!turnId) return;
            const eventStamp = yield* stamp();
            switch (event._tag) {
              case "AssistantItemStarted":
              case "AssistantItemCompleted":
                yield* publish(
                  makeAcpAssistantItemEvent({
                    stamp: eventStamp,
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId,
                    itemId: event.itemId,
                    lifecycle:
                      event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
                  }),
                );
                return;
              case "PlanUpdated":
                yield* publish(
                  makeAcpPlanUpdatedEvent({
                    stamp: eventStamp,
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId,
                    payload: event.payload,
                    source: "acp.jsonrpc",
                    method: "session/update",
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
              case "ToolCallUpdated":
                yield* publish(
                  makeAcpToolCallEvent({
                    stamp: eventStamp,
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId,
                    toolCall: event.toolCall,
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
              case "ContentDelta":
                yield* publish(
                  makeAcpContentDeltaEvent({
                    stamp: eventStamp,
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId,
                    ...(event.itemId ? { itemId: event.itemId } : {}),
                    text: event.text,
                    rawPayload: event.rawPayload,
                  }),
                );
            }
          }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("Failed to process a Mistral Vibe notification.", { cause }),
          ),
          Effect.forkIn(sessionScope),
        );
        sessionScopeTransferred = true;

        yield* publish({
          type: "session.started",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { resume: started.initializeResult },
        });
        yield* publish({
          type: "session.state.changed",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { state: "ready", reason: "Mistral Vibe ACP session ready" },
        });
        yield* publish({
          type: "thread.started",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { providerThreadId: started.sessionId },
        });
        return session;
      }).pipe(Effect.scoped);

    const startSession: VibeAdapterShape["startSession"] = (input) =>
      withThreadLock(input.threadId, startSessionUnlocked(input));

    const settleActiveTurn = (
      ctx: VibeSessionContext,
      turnId: TurnId,
      payload: Extract<ProviderRuntimeEvent, { type: "turn.completed" }>["payload"],
    ) =>
      Effect.gen(function* () {
        if (
          ctx.stopped ||
          sessions.get(ctx.threadId) !== ctx ||
          !vibePromptSettlementBelongsToTurn(ctx.activeTurnId, turnId)
        ) {
          return false;
        }
        delete ctx.activeTurnId;
        delete ctx.promptStartedSignal;
        ctx.interruptedTurnIds.delete(turnId);
        const { activeTurnId: _activeTurnId, ...ready } = ctx.session;
        ctx.session = {
          ...ready,
          status: "ready",
          updatedAt: yield* nowIso,
        };
        yield* publish({
          type: "turn.completed",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload,
        });
        return true;
      });

    /**
     * Claims the thread's single turn slot under the per-thread lock. Both the
     * active-turn check and the configuration RPCs happen inside the lock, so
     * concurrent sendTurn calls can neither both pass the check nor interleave
     * their session configuration. The lock is released before the prompt runs
     * so a long turn never blocks session lifecycle operations.
     */
    const prepareTurn = (input: Parameters<VibeAdapterShape["sendTurn"]>[0]) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          if (ctx.activeTurnId) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Mistral Vibe already has an active turn.",
            });
          }
          ctx.turnPreparationInProgress = true;
          ctx.interruptPreparationRequested = false;
          return yield* Effect.gen(function* () {
            const modelSelection =
              input.modelSelection?.instanceId === instanceId ? input.modelSelection : undefined;
            yield* applyVibeSessionConfiguration({
              runtime: ctx.acp,
              model: modelSelection?.model ?? ctx.session.model,
              selections: modelSelection?.options,
              mode: vibeModeForRuntimeMode(ctx.session.runtimeMode, input.interactionMode),
              mapError: (cause, method) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, method, cause),
            });
            const text = input.input?.trim();
            const images = yield* Effect.forEach(input.attachments ?? [], (attachment) =>
              Effect.gen(function* () {
                const attachmentPath = resolveAttachmentPath({
                  attachmentsDir: serverConfig.attachmentsDir,
                  attachment,
                });
                if (!attachmentPath) {
                  return yield* new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "session/prompt",
                    detail: `Invalid attachment id '${attachment.id}'.`,
                  });
                }
                const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "session/prompt",
                        detail: cause.message,
                        cause,
                      }),
                  ),
                );
                return {
                  type: "image" as const,
                  data: Buffer.from(bytes).toString("base64"),
                  mimeType: attachment.mimeType,
                } satisfies EffectAcpSchema.ContentBlock;
              }),
            );
            const prompt: Array<EffectAcpSchema.ContentBlock> = [
              ...(text ? [{ type: "text" as const, text }] : []),
              ...images,
            ];
            if (prompt.length === 0) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "sendTurn",
                issue: "Turn requires non-empty text or attachments.",
              });
            }
            const turnId = TurnId.make(yield* nextId);
            const promptStartedSignal = yield* Deferred.make<void>();
            ctx.activeTurnId = turnId;
            ctx.promptStartedSignal = promptStartedSignal;
            if (ctx.interruptPreparationRequested) {
              ctx.interruptedTurnIds.add(turnId);
            }
            ctx.session = {
              ...ctx.session,
              status: "running",
              activeTurnId: turnId,
              updatedAt: yield* nowIso,
              model: resolveVibeModelId(modelSelection?.model ?? ctx.session.model),
            };
            yield* publish({
              type: "turn.started",
              ...(yield* stamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: { model: ctx.session.model },
            });
            return { ctx, turnId, prompt, promptStartedSignal };
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.turnPreparationInProgress = false;
                ctx.interruptPreparationRequested = false;
              }),
            ),
          );
        }),
      );

    const sendTurn: VibeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const { ctx, turnId, prompt, promptStartedSignal } = yield* prepareTurn(input);
        const promptStart = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const live = sessions.get(input.threadId);
            if (
              live !== ctx ||
              ctx.stopped ||
              !vibePromptSettlementBelongsToTurn(ctx.activeTurnId, turnId)
            ) {
              return { _tag: "Skipped" as const };
            }
            if (ctx.interruptedTurnIds.has(turnId)) {
              yield* settleActiveTurn(ctx, turnId, {
                state: "cancelled",
                stopReason: "cancelled",
              });
              return { _tag: "Skipped" as const };
            }
            const fiber = yield* ctx.acp.prompt({ prompt }).pipe(Effect.forkIn(ctx.scope));
            yield* Effect.raceFirst(
              Deferred.await(promptStartedSignal),
              Fiber.await(fiber).pipe(Effect.asVoid),
            );
            return { _tag: "Started" as const, fiber };
          }),
        );
        if (promptStart._tag === "Skipped") {
          return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
        }

        const result = yield* Fiber.join(promptStart.fiber).pipe(
          Effect.tapError((cause) =>
            Effect.gen(function* () {
              yield* drainVibeEventsUnlessStopped(ctx.acp.drainEvents, ctx.stoppedSignal);
              yield* withThreadLock(
                input.threadId,
                settleActiveTurn(ctx, turnId, {
                  state: "failed",
                  errorMessage: cause.message || "Mistral Vibe prompt request failed.",
                }),
              );
            }).pipe(Effect.catchCause(() => Effect.void)),
          ),
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", cause),
          ),
        );
        yield* drainVibeEventsUnlessStopped(ctx.acp.drainEvents, ctx.stoppedSignal);
        yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            if (
              ctx.stopped ||
              sessions.get(input.threadId) !== ctx ||
              !vibePromptSettlementBelongsToTurn(ctx.activeTurnId, turnId)
            ) {
              return;
            }
            ctx.turns.push({ id: turnId, items: [{ prompt, result }] });
            const wasInterrupted = ctx.interruptedTurnIds.has(turnId);
            yield* settleActiveTurn(ctx, turnId, {
              state:
                wasInterrupted || result.stopReason === "cancelled" ? "cancelled" : "completed",
              stopReason: wasInterrupted ? "cancelled" : result.stopReason,
            });
          }),
        );
        return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
      });

    const interruptTurn: VibeAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        yield* Effect.sync(() => {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped) return;
          const activeTurnId = ctx.activeTurnId;
          if (turnId && activeTurnId && turnId !== activeTurnId) return;
          if (activeTurnId) {
            ctx.interruptedTurnIds.add(activeTurnId);
          } else if (ctx.turnPreparationInProgress && turnId === undefined) {
            ctx.interruptPreparationRequested = true;
          }
        });
        const shouldCancelAcp = yield* withThreadLock(
          threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            const activeTurnId = ctx.activeTurnId;
            if (turnId && activeTurnId && turnId !== activeTurnId) return false;
            yield* Effect.forEach(
              ctx.pendingApprovals.values(),
              (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
              { discard: true },
            );
            if (!activeTurnId) return false;
            const promptStarted =
              ctx.promptStartedSignal !== undefined &&
              (yield* Deferred.isDone(ctx.promptStartedSignal));
            if (!promptStarted) {
              yield* settleActiveTurn(ctx, activeTurnId, {
                state: "cancelled",
                stopReason: "cancelled",
              });
              return false;
            }
            return true;
          }),
        );
        if (shouldCancelAcp) {
          const ctx = sessions.get(threadId);
          if (ctx && !ctx.stopped) {
            yield* ctx.acp.cancel.pipe(Effect.ignore);
          }
        }
      });

    const respondToRequest: VibeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: VibeAdapterShape["respondToUserInput"] = (threadId, requestId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/user_input",
          detail: `Mistral Vibe has no pending user-input request '${requestId}'.`,
        });
      });

    const readThread: VibeAdapterShape["readThread"] = (threadId) =>
      Effect.map(requireSession(threadId), (ctx) => ({ threadId, turns: ctx.turns }));
    const rollbackThread: VibeAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "Mistral Vibe ACP sessions do not support provider-side rollback yet.",
        });
      });

    const stopSession: VibeAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.suspend(() => Effect.flatMap(requireSession(threadId), stopContext)),
      );
    const listSessions: VibeAdapterShape["listSessions"] = () =>
      Effect.succeed(Array.from(sessions.values(), (ctx) => ctx.session));
    const hasSession: VibeAdapterShape["hasSession"] = (threadId) =>
      Effect.succeed(sessions.has(threadId));
    const stopAll: VibeAdapterShape["stopAll"] = () =>
      Effect.forEach(
        Array.from(sessions.values()),
        (ctx) =>
          withThreadLock(
            ctx.threadId,
            Effect.suspend(() =>
              sessions.get(ctx.threadId) === ctx ? stopContext(ctx) : Effect.void,
            ),
          ),
        { discard: true },
      );

    yield* Effect.addFinalizer(() =>
      Effect.forEach(Array.from(sessions.values()), stopContext, { discard: true }).pipe(
        Effect.ignore,
      ),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(events),
    } satisfies VibeAdapterShape;
  });
}
