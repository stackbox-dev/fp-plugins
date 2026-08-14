import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { DataStream, streamToBuffer } from "./utils";

describe("Utils", () => {
  describe("streamToBuffer", () => {
    it("should convert readable stream to buffer", async () => {
      const testData = "Hello, world!";
      const stream = Readable.from([Buffer.from(testData)]);

      const result = await streamToBuffer(stream);

      expect(result).toBeInstanceOf(Buffer);
      expect(result.toString()).toBe(testData);
    });

    it("should handle multiple chunks", async () => {
      const chunks = ["chunk1", "chunk2", "chunk3"].map((c) => Buffer.from(c));
      const stream = Readable.from(chunks);

      const result = await streamToBuffer(stream);

      expect(result.toString()).toBe("chunk1chunk2chunk3");
    });

    it("should handle empty stream", async () => {
      const stream = Readable.from([]);

      const result = await streamToBuffer(stream);

      expect(result).toBeInstanceOf(Buffer);
      expect(result.length).toBe(0);
    });

    it("should handle single chunk optimization", async () => {
      const testData = Buffer.from("single chunk");
      const stream = Readable.from([testData]);

      const result = await streamToBuffer(stream);

      expect(result).toBe(testData); // Should return the same buffer instance
    });

    it("should concatenate multiple buffer chunks", async () => {
      const chunk1 = Buffer.from("first");
      const chunk2 = Buffer.from("second");
      const chunk3 = Buffer.from("third");
      const stream = Readable.from([chunk1, chunk2, chunk3]);

      const result = await streamToBuffer(stream);

      expect(result.toString()).toBe("firstsecondthird");
    });

    it("should handle mixed string and buffer chunks", async () => {
      const stringChunk = Buffer.from("string");
      const bufferChunk = Buffer.from("buffer");
      const stream = Readable.from([stringChunk, bufferChunk]);

      const result = await streamToBuffer(stream);

      expect(result.toString()).toBe("stringbuffer");
    });

    it("should reject on stream error", async () => {
      const stream = new Readable({
        read() {
          this.emit("error", new Error("Stream error"));
        },
      });

      await expect(streamToBuffer(stream)).rejects.toThrow("Stream error");
    });

    it("should handle stream that emits end immediately", async () => {
      const stream = new Readable({
        read() {
          this.push(null); // End stream immediately
        },
      });

      const result = await streamToBuffer(stream);

      expect(result).toBeInstanceOf(Buffer);
      expect(result.length).toBe(0);
    });

    it("should work with custom data stream interface", async () => {
      class CustomStream implements DataStream {
        private listeners: Map<string, Function[]> = new Map();

        on(eventName: string, listener: Function): this {
          if (!this.listeners.has(eventName)) {
            this.listeners.set(eventName, []);
          }
          this.listeners.get(eventName)!.push(listener);
          return this;
        }

        emit(eventName: string, ...args: any[]): void {
          const listeners = this.listeners.get(eventName) || [];
          listeners.forEach((listener) => listener(...args));
        }

        start(): void {
          setTimeout(() => {
            this.emit("data", Buffer.from("custom"));
            this.emit("data", Buffer.from("stream"));
            this.emit("end");
          }, 0);
        }
      }

      const customStream = new CustomStream();
      const promise = streamToBuffer(customStream);
      customStream.start();

      const result = await promise;
      expect(result.toString()).toBe("customstream");
    });

    it("should handle large streams efficiently", async () => {
      const largeData = "x".repeat(10000);
      const chunks = [];

      // Split into smaller chunks
      for (let i = 0; i < largeData.length; i += 1000) {
        chunks.push(Buffer.from(largeData.slice(i, i + 1000)));
      }

      const stream = Readable.from(chunks);
      const result = await streamToBuffer(stream);

      expect(result.toString()).toBe(largeData);
      expect(result.length).toBe(largeData.length);
    });

    it("should handle binary data correctly", async () => {
      const binaryData = new Uint8Array([0, 1, 2, 3, 255, 254, 253]);
      const stream = Readable.from([Buffer.from(binaryData)]);

      const result = await streamToBuffer(stream);

      expect(Array.from(result)).toEqual(Array.from(binaryData));
    });

    it("should handle concurrent stream operations", async () => {
      const createStream = (data: string) => Readable.from([Buffer.from(data)]);

      const streams = [
        createStream("stream1"),
        createStream("stream2"),
        createStream("stream3"),
      ];

      const promises = streams.map((stream) => streamToBuffer(stream));
      const results = await Promise.all(promises);

      expect(results.map((r) => r.toString())).toEqual([
        "stream1",
        "stream2",
        "stream3",
      ]);
    });
  });

  describe("premature close", () => {
    // A stream destroyed without an error emits only 'close'. Before this was handled
    // the promise stayed pending forever and hung the request awaiting getAsBuffer.
    it("rejects when the stream is destroyed without an error", async () => {
      const stream = new Readable({ read() {} });
      stream.push("partial");
      setImmediate(() => stream.destroy());

      await expect(streamToBuffer(stream)).rejects.toThrow(
        "stream closed before end",
      );
    });

    it("rejects with the original error when destroyed with one", async () => {
      const stream = new Readable({ read() {} });
      setImmediate(() => stream.destroy(new Error("boom")));

      await expect(streamToBuffer(stream)).rejects.toThrow("boom");
    });

    it("still resolves normally when close follows end", async () => {
      const stream: DataStream = Readable.from(["a", "b"]);

      expect((await streamToBuffer(stream)).toString()).toBe("ab");
    });

    // Late events must not settle the promise a second time. EventEmitter is used
    // directly so the exact sequence can be driven; a real Readable will not emit
    // 'error' after 'end'.
    it("ignores an error arriving after end", async () => {
      const em = new EventEmitter() as unknown as DataStream;
      const p = streamToBuffer(em);
      const e = em as unknown as EventEmitter;
      e.emit("data", Buffer.from("ok"));
      e.emit("end");
      e.emit("error", new Error("too late"));
      e.emit("close");

      expect((await p).toString()).toBe("ok");
    });

    it("ignores end and close arriving after an error", async () => {
      const em = new EventEmitter() as unknown as DataStream;
      const p = streamToBuffer(em);
      const e = em as unknown as EventEmitter;
      e.emit("error", new Error("first"));
      e.emit("end");
      e.emit("close");

      await expect(p).rejects.toThrow("first");
    });
  });
});
