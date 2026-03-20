import { safeCloseAll } from "./utils";

describe("safeCloseAll", () => {
  it("should run all functions even if earlier ones throw", async () => {
    const calls: number[] = [];
    const errors = await safeCloseAll(
      async () => {
        calls.push(1);
        throw new Error("fail-1");
      },
      async () => {
        calls.push(2);
      },
      async () => {
        calls.push(3);
        throw new Error("fail-3");
      },
    );
    expect(calls).toEqual([1, 2, 3]);
    expect(errors).toHaveLength(2);
    expect((errors[0] as Error).message).toBe("fail-1");
    expect((errors[1] as Error).message).toBe("fail-3");
  });

  it("should return empty array when all succeed", async () => {
    const errors = await safeCloseAll(
      async () => {},
      async () => {},
    );
    expect(errors).toEqual([]);
  });

  it("should handle sync functions", async () => {
    const calls: number[] = [];
    const errors = await safeCloseAll(
      () => {
        calls.push(1);
      },
      () => {
        calls.push(2);
        throw new Error("sync-fail");
      },
      () => {
        calls.push(3);
      },
    );
    expect(calls).toEqual([1, 2, 3]);
    expect(errors).toHaveLength(1);
  });

  it("should handle empty arguments", async () => {
    const errors = await safeCloseAll();
    expect(errors).toEqual([]);
  });
});
