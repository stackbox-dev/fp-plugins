import { getHandlerMap } from "./commons";
import { EventBusOptions, EventHandler } from "./interfaces";

describe("getHandlerMap", () => {
  it("should create handler map from options", () => {
    const mockHandler1: EventHandler = async () => {};
    const mockHandler2: EventHandler = async () => {};

    const options: Pick<EventBusOptions, "handlers"> = {
      handlers: [
        {
          file: "file1.ts",
          handlers: {
            event1: mockHandler1,
            event2: mockHandler2,
          },
        },
        {
          file: "file2.ts",
          handlers: {
            event1: mockHandler2,
          },
        },
      ],
    };

    const handlerMap = getHandlerMap(options);

    expect(handlerMap.size).toBe(2);
    expect(handlerMap.get("event1")).toEqual([
      { file: "file1.ts", handler: mockHandler1 },
      { file: "file2.ts", handler: mockHandler2 },
    ]);
    expect(handlerMap.get("event2")).toEqual([
      { file: "file1.ts", handler: mockHandler2 },
    ]);
  });

  it("should handle empty handlers", () => {
    const options: Pick<EventBusOptions, "handlers"> = {
      handlers: [],
    };

    const handlerMap = getHandlerMap(options);
    expect(handlerMap.size).toBe(0);
  });
});
