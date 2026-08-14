export interface DataStream {
  // Must stay any[]: node's EventEmitter declares `on` with `...args: any[]`, and a
  // narrower element type makes Readable structurally incompatible.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(eventName: string | symbol, listener: (...args: any[]) => void): this;
}

export function streamToBuffer(stream: DataStream): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk: Buffer | string) =>
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)),
    );
    // Typed as Error so the rejection carries a stack; node streams always emit one.
    stream.on("error", (err: Error) => reject(err));
    stream.on("end", () =>
      resolve(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks)),
    );
  });
}
