import * as timers from "node:timers/promises";

const BASE_DELAY = Math.max(
  parseInt(process.env.EVENT_RETRY_BASE_DELAY ?? "0", 10) || 0,
  5_000,
);
const MAX_DELAY = Math.max(
  parseInt(process.env.EVENT_RETRY_MAX_DELAY ?? "0", 10) || 0,
  60_000,
);

export async function exponentialDelay(
  attempt: number,
  baseDelayMs: number = BASE_DELAY,
  maxDelayMs: number = MAX_DELAY,
  randomizationFactor: number = 0.5,
): Promise<void> {
  let delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  if (randomizationFactor > 0) {
    const randomFactor = (Math.random() - 0.5) * randomizationFactor * delay;
    delay = Math.max(0, delay + randomFactor);
  }
  await timers.setTimeout(delay);
}
