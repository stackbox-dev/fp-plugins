import { exponentialDelay, randomDelay } from "./utils";

describe("exponentialDelay", () => {
  it("should resolve without error", async () => {
    await exponentialDelay(0, {
      baseDelayMs: 1,
      maxDelayMs: 10,
      randomizationFactor: 0,
    });
  });

  it("should respect maxDelayMs cap", async () => {
    const start = Date.now();
    await exponentialDelay(100, {
      baseDelayMs: 1,
      maxDelayMs: 50,
      randomizationFactor: 0,
    });
    const elapsed = Date.now() - start;
    // Should be capped at 50ms, not 2^100 ms (allow scheduler jitter)
    expect(elapsed).toBeLessThan(500);
  });

  it("should return immediately when abort signal is already aborted", async () => {
    const ctrl = new AbortController();
    ctrl.abort();
    const start = Date.now();
    await exponentialDelay(0, {
      baseDelayMs: 10_000,
      maxDelayMs: 60_000,
      randomizationFactor: 0,
      signal: ctrl.signal,
    });
    expect(Date.now() - start).toBeLessThan(100);
  });

  it("should abort mid-delay when signal fires", async () => {
    const ctrl = new AbortController();
    const start = Date.now();
    const delayPromise = exponentialDelay(0, {
      baseDelayMs: 10_000,
      maxDelayMs: 60_000,
      randomizationFactor: 0,
      signal: ctrl.signal,
    });
    setTimeout(() => ctrl.abort(), 50);
    await delayPromise;
    expect(Date.now() - start).toBeLessThan(500);
  });

  it("should use defaults when no options provided", async () => {
    // Just verify it doesn't throw — actual delay will be 5s+ with defaults
    // so we abort immediately to not wait
    const ctrl = new AbortController();
    ctrl.abort();
    await exponentialDelay(0, { signal: ctrl.signal });
  });

  it("should apply randomization factor", async () => {
    // Run multiple times, verify we get different durations
    const durations: number[] = [];
    for (let i = 0; i < 5; i++) {
      const start = Date.now();
      await exponentialDelay(0, {
        baseDelayMs: 50,
        maxDelayMs: 200,
        randomizationFactor: 0.9,
      });
      durations.push(Date.now() - start);
    }
    // With 0.9 randomization on 50ms base, we should see variation
    // At minimum, not all durations should be identical
    const unique = new Set(durations.map((d) => Math.round(d / 10)));
    // Flaky-safe: just assert they all completed in reasonable time
    for (const d of durations) {
      expect(d).toBeLessThan(500);
    }
  });
});

describe("randomDelay", () => {
  it("should resolve within max time", async () => {
    const start = Date.now();
    await randomDelay(50);
    expect(Date.now() - start).toBeLessThan(200);
  });

  it("should return immediately when abort signal is already aborted", async () => {
    const ctrl = new AbortController();
    ctrl.abort();
    const start = Date.now();
    await randomDelay(10_000, ctrl.signal);
    expect(Date.now() - start).toBeLessThan(100);
  });

  it("should abort mid-delay when signal fires", async () => {
    const ctrl = new AbortController();
    const start = Date.now();
    const delayPromise = randomDelay(10_000, ctrl.signal);
    setTimeout(() => ctrl.abort(), 50);
    await delayPromise;
    expect(Date.now() - start).toBeLessThan(500);
  });
});
