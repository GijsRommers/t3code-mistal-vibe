import { createModelCapabilities } from "@t3tools/shared/model";
import { describe, expect, it } from "vite-plus/test";

import { buildVibeCapabilities, buildVibeModelsFromConfigOptions } from "./VibeProvider.ts";

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
