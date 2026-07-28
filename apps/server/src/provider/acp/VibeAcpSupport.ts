import {
  type ProviderOptionSelection,
  type ProviderInteractionMode,
  type RuntimeMode,
  type VibeSettings,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const DEFAULT_VIBE_MODEL = "mistral-medium-3.5";

type VibeAcpRuntimeSettings = Pick<
  VibeSettings,
  "binaryPath" | "ollamaBaseUrl" | "ollamaEnabled" | "ollamaModel"
>;

export interface VibeAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly vibeSettings: VibeAcpRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

const WorkspaceTrustStatusResponse = Schema.Struct({
  trust_status: Schema.Literals(["trusted", "session", "untrusted"]),
  details: Schema.NullOr(
    Schema.Struct({
      cwd: Schema.String,
      repoRoot: Schema.NullOr(Schema.String),
      ignoredFiles: Schema.Array(Schema.String),
      availableDecisions: Schema.Array(
        Schema.Literals(["trust_repo", "trust_cwd", "trust_session", "decline"]),
      ),
    }),
  ),
});
export type WorkspaceTrustStatusResponse = typeof WorkspaceTrustStatusResponse.Type;
const decodeWorkspaceTrustStatusResponse = Schema.decodeUnknownEffect(WorkspaceTrustStatusResponse);

export type VibeWorkspaceTrustDecision = "trust_repo" | "trust_cwd" | "trust_session";

/** Broadest grant first: trusting the repo covers later sessions in any subdir. */
const TRUST_DECISION_PREFERENCE: ReadonlyArray<VibeWorkspaceTrustDecision> = [
  "trust_cwd",
  "trust_repo",
  "trust_session",
];

/**
 * Picks the decision to answer an outstanding Vibe trust prompt with.
 *
 * Vibe only accepts `_trust/decision` while it has a decision pending — it
 * reports the acceptable ones in `details.availableDecisions` and rejects
 * anything else with `-32602 No workspace trust decision is available`.
 * `details` is null when nothing is pending, which is not an error: the
 * workspace simply cannot be auto-trusted from here.
 */
export function selectVibeTrustDecision(
  status: WorkspaceTrustStatusResponse,
): VibeWorkspaceTrustDecision | undefined {
  const available = status.details?.availableDecisions;
  if (!available || available.length === 0) return undefined;
  return TRUST_DECISION_PREFERENCE.find((decision) => available.includes(decision));
}

export function buildVibeAcpSpawnInput(
  vibeSettings: VibeAcpRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  const env = buildVibeEnvironment(vibeSettings, environment);
  return {
    command: vibeSettings?.binaryPath || "vibe-acp",
    args: [],
    cwd,
    ...(env ? { env } : {}),
  };
}

export function buildVibeEnvironment(
  settings: VibeAcpRuntimeSettings | null | undefined,
  environment?: NodeJS.ProcessEnv,
): NodeJS.ProcessEnv | undefined {
  if (!settings?.ollamaEnabled) return environment;
  const apiBase = settings.ollamaBaseUrl.trim() || "http://127.0.0.1:11434/v1";
  const model = settings.ollamaModel.trim() || "devstral-small-2";
  return {
    ...environment,
    VIBE_PROVIDERS: JSON.stringify([
      {
        name: "local",
        api_base: apiBase,
        api_style: "openai",
        backend: "generic",
        api_key_env_var: "",
      },
    ]),
    VIBE_MODELS: JSON.stringify([
      {
        name: model,
        provider: "local",
        alias: "local",
        input_price: 0,
        output_price: 0,
      },
    ]),
  };
}

export const makeVibeAcpRuntime = (
  input: VibeAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        interruptPromptOnCancel: false,
        spawn: buildVibeAcpSpawnInput(input.vibeSettings, input.cwd, input.environment),
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });

export function resolveVibeModelId(model: string | null | undefined): string {
  return model?.trim() || DEFAULT_VIBE_MODEL;
}

export function vibeModeForRuntimeMode(
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode = "default",
): string {
  if (interactionMode === "plan") return "plan";
  switch (runtimeMode) {
    case "approval-required":
      return "default";
    case "auto-accept-edits":
    case "auto":
      return "accept-edits";
    case "full-access":
      return "auto-approve";
  }
}

export function applyVibeSessionConfiguration<E>(input: {
  readonly runtime: {
    readonly getConfigOptions: AcpSessionRuntime.AcpSessionRuntime["Service"]["getConfigOptions"];
    readonly setConfigOption: (
      configId: string,
      value: string | boolean,
    ) => Effect.Effect<unknown, EffectAcpErrors.AcpError>;
    readonly setMode: (modeId: string) => Effect.Effect<unknown, EffectAcpErrors.AcpError>;
    readonly setModel: (model: string) => Effect.Effect<unknown, EffectAcpErrors.AcpError>;
  };
  readonly model: string | null | undefined;
  readonly selections: ReadonlyArray<ProviderOptionSelection> | null | undefined;
  readonly mode: string;
  readonly mapError: (cause: EffectAcpErrors.AcpError, method: string) => E;
}): Effect.Effect<void, E> {
  return Effect.gen(function* () {
    yield* input.runtime
      .setModel(resolveVibeModelId(input.model))
      .pipe(Effect.mapError((cause) => input.mapError(cause, "session/set_config_option:model")));

    const configOptions = yield* input.runtime.getConfigOptions;
    const validConfigIds = new Set(configOptions.map((option) => option.id));
    for (const selection of input.selections ?? []) {
      if (selection.id === "model" || !validConfigIds.has(selection.id)) continue;
      yield* input.runtime
        .setConfigOption(selection.id, selection.value)
        .pipe(
          Effect.mapError((cause) =>
            input.mapError(cause, `session/set_config_option:${selection.id}`),
          ),
        );
    }

    yield* input.runtime
      .setMode(input.mode)
      .pipe(Effect.mapError((cause) => input.mapError(cause, "session/set_config_option:mode")));
  });
}

export const readVibeWorkspaceTrust = (
  runtime: Pick<AcpSessionRuntime.AcpSessionRuntime["Service"], "request">,
  cwd: string,
): Effect.Effect<WorkspaceTrustStatusResponse, EffectAcpErrors.AcpError> =>
  runtime.request("_trust/status", { cwd }).pipe(
    Effect.flatMap((response) =>
      decodeWorkspaceTrustStatusResponse(response).pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpRequestError({
              code: -32603,
              errorMessage: `Vibe returned an invalid workspace trust response: ${cause.message}`,
              data: cause,
            }),
        ),
      ),
    ),
  );

export const trustVibeWorkspace = (
  runtime: Pick<AcpSessionRuntime.AcpSessionRuntime["Service"], "request">,
  input: {
    readonly cwd: string;
    readonly sessionId: string;
    readonly decision?: VibeWorkspaceTrustDecision;
  },
): Effect.Effect<WorkspaceTrustStatusResponse, EffectAcpErrors.AcpError> =>
  runtime
    .request("_trust/decision", {
      cwd: input.cwd,
      sessionId: input.sessionId,
      decision: input.decision ?? "trust_cwd",
    })
    .pipe(
      Effect.flatMap((response) =>
        decodeWorkspaceTrustStatusResponse(response).pipe(
          Effect.mapError(
            (cause) =>
              new EffectAcpErrors.AcpRequestError({
                code: -32603,
                errorMessage: `Vibe returned an invalid workspace trust response: ${cause.message}`,
                data: cause,
              }),
          ),
        ),
      ),
    );
