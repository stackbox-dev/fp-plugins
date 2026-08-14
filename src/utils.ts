export interface DataStream {
  // Must stay any[]: node's EventEmitter declares `on` with `...args: any[]`, and a
  // narrower element type makes Readable structurally incompatible.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(eventName: string | symbol, listener: (...args: any[]) => void): this;
}

export function streamToBuffer(stream: DataStream): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let settled = false;

    stream.on("data", (chunk: Buffer | string) =>
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)),
    );
    // Typed as Error so the rejection carries a stack; node streams always emit one.
    stream.on("error", (err: Error) => {
      if (settled) return;
      settled = true;
      reject(err);
    });
    stream.on("end", () => {
      if (settled) return;
      settled = true;
      resolve(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks));
    });
    // A stream destroyed without an error argument emits only 'close', which would
    // otherwise leave this promise pending forever and hang the awaiting request.
    // 'close' follows 'end' in the normal path, where the settled flag ignores it.
    stream.on("close", () => {
      if (settled) return;
      settled = true;
      reject(new Error("stream closed before end"));
    });
  });
}
