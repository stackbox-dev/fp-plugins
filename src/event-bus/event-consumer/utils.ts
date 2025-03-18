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
): Promise<void> {
  const delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  await timers.setTimeout(delay);
}
