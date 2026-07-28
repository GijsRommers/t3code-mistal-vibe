import {
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
  type VibeSettings,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { makeVibeAcpRuntime, resolveVibeModelId } from "../acp/VibeAcpSupport.ts";

const VIBE_PRESENTATION = {
  displayName: "Mistral Vibe",
  badgeLabel: "Early Access",
  showInteractionModeToggle: true,
  requiresNewThreadForModelChange: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
const VERSION_PROBE_TIMEOUT_MS = 4_000;
const ACP_PROBE_TIMEOUT_MS = 15_000;

const FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "mistral-medium-3.5",
    name: "Mistral Medium 3.5",
    isCustom: false,
    isDefault: true,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "devstral-small",
    name: "Devstral Small",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "local",
    name: "Local",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function flattenSelectOptions(
  option: EffectAcpSchema.SessionConfigOption,
): ReadonlyArray<{ value: string; label: string }> {
  if (option.type !== "select") return [];
  return option.options.flatMap((entry) =>
    "value" in entry
      ? [{ value: entry.value, label: entry.name }]
      : entry.options.map((nested) => ({ value: nested.value, label: nested.name })),
  );
}

export function buildVibeCapabilities(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
): ModelCapabilities {
  const thinking = configOptions.find(
    (option) => option.id.trim().toLowerCase() === "thinking" && option.type === "select",
  );
  if (!thinking) return EMPTY_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: thinking.id,
        label: thinking.name.trim() || "Thinking",
        options: flattenSelectOptions(thinking).map((entry) => ({
          value: entry.value,
          label: entry.label,
          ...(thinking.currentValue === entry.value ? { isDefault: true } : {}),
        })),
      }),
    ],
  });
}

export function buildVibeModelsFromConfigOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
): ReadonlyArray<ServerProviderModel> {
  const model = configOptions.find(
    (option) =>
      (option.category === "model" || option.id.trim().toLowerCase() === "model") &&
      option.type === "select",
  );
  if (!model) return [];
  const capabilities = buildVibeCapabilities(configOptions);
  const seen = new Set<string>();
  return flattenSelectOptions(model).flatMap((entry) => {
    const slug = resolveVibeModelId(entry.value);
    if (seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: entry.label.trim() || slug,
        isCustom: false,
        ...(model.currentValue === entry.value ? { isDefault: true } : {}),
        capabilities,
      },
    ];
  });
}

function modelsFromSettings(
  settings: VibeSettings,
  builtIn: ReadonlyArray<ServerProviderModel> = FALLBACK_MODELS,
) {
  return providerModelsFromSettings(builtIn, settings.customModels, EMPTY_CAPABILITIES);
}

export function buildInitialVibeProviderSnapshot(
  settings: VibeSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: modelsFromSettings(settings),
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Mistral Vibe availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Mistral Vibe is disabled in T3 Code settings.",
          },
    });
  });
}

const runVersionCommand = (settings: VibeSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = settings.binaryPath || "vibe-acp";
    const spawn = yield* resolveSpawnCommand(command, ["--version"], { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawn.command, spawn.args, {
        env: environment,
        shell: spawn.shell,
      }),
    );
  });

const discoverViaAcp = (settings: VibeSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const acp = yield* makeVibeAcpRuntime({
      vibeSettings: settings,
      environment,
      childProcessSpawner,
      cwd: process.cwd(),
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    yield* acp.start();
    return buildVibeModelsFromConfigOptions(yield* acp.getConfigOptions);
  }).pipe(Effect.scoped);

export const checkVibeProviderStatus = Effect.fn("checkVibeProviderStatus")(function* (
  settings: VibeSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = modelsFromSettings(settings);
  if (!settings.enabled) {
    return yield* buildInitialVibeProviderSnapshot(settings);
  }

  const versionResult = yield* runVersionCommand(settings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    const missing = isCommandMissingCause(versionResult.failure);
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !missing,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: missing
          ? "Mistral Vibe (`vibe-acp`) is not installed or not on PATH."
          : "Failed to execute the Mistral Vibe health check.",
      },
    });
  }
  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Mistral Vibe timed out while checking its version.",
      },
    });
  }

  const output = versionResult.success.value;
  const version = parseGenericCliVersion(`${output.stdout}\n${output.stderr}`);
  if (output.code !== 0) {
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Mistral Vibe is installed but failed to run.",
      },
    });
  }

  const discovery = yield* discoverViaAcp(settings, environment).pipe(
    Effect.timeoutOption(ACP_PROBE_TIMEOUT_MS),
    Effect.exit,
  );
  if (Exit.isFailure(discovery)) {
    yield* Effect.logWarning("Mistral Vibe ACP discovery failed", {
      errorTag: causeErrorTag(discovery.cause),
    });
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unknown" },
        message:
          "Mistral Vibe is installed but ACP discovery failed. Check the Vibe configuration and server logs, then refresh.",
      },
    });
  }
  if (Option.isNone(discovery.value)) {
    return buildServerProvider({
      presentation: VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Mistral Vibe ACP startup timed out.",
      },
    });
  }

  const discovered = discovery.value.value;
  return buildServerProvider({
    presentation: VIBE_PRESENTATION,
    enabled: true,
    checkedAt,
    models: modelsFromSettings(settings, discovered.length > 0 ? discovered : FALLBACK_MODELS),
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

export const enrichVibeSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap(input.publishSnapshot),
    Effect.catchCause((cause) =>
      Effect.logWarning("Mistral Vibe version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
