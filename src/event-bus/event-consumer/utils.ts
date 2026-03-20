import * as timers from "node:timers/promises";

const BASE_DELAY = Math.max(
  parseInt(process.env.EVENT_RETRY_BASE_DELAY ?? "0", 10) || 0,
  5_000,
);
const MAX_DELAY = Math.max(
  parseInt(process.env.EVENT_RETRY_MAX_DELAY ?? "0", 10) || 0,
  60_000,
);
const RANDOMIZED_DELAY_MAX = Math.max(
  parseInt(process.env.EVENT_RANDOMIZED_DELAY_MAX ?? "0", 10) || 0,
  10_000,
);

export interface ExponentialDelayOptions {
  baseDelayMs?: number;
  maxDelayMs?: number;
  randomizationFactor?: number;
  signal?: AbortSignal;
}

export async function exponentialDelay(
  attempt: number,
  options?: ExponentialDelayOptions,
): Promise<void> {
  const baseDelayMs = options?.baseDelayMs ?? BASE_DELAY;
  const maxDelayMs = options?.maxDelayMs ?? MAX_DELAY;
  const randomizationFactor = options?.randomizationFactor ?? 0.5;
  const signal = options?.signal;
  let delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  if (randomizationFactor > 0) {
    const randomFactor = (Math.random() - 0.5) * randomizationFactor * delay;
    delay = Math.max(0, delay + randomFactor);
  }
  try {
    await timers.setTimeout(delay, undefined, signal ? { signal } : undefined);
  } catch (err) {
    if ((err as Error).name === "AbortError") return;
    throw err;
  }
}

export async function randomDelay(
  max: number = RANDOMIZED_DELAY_MAX,
  signal?: AbortSignal,
) {
  const delay = Math.ceil(Math.random() * max);
  try {
    await timers.setTimeout(delay, undefined, signal ? { signal } : undefined);
  } catch (err) {
    if ((err as Error).name === "AbortError") return;
    throw err;
  }
}
