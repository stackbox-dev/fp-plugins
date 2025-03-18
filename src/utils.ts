export interface DataStream {
  on(eventName: string | symbol, listener: (...args: any[]) => void): this;
}

export function streamToBuffer(stream: DataStream): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", (err) => reject(err));
    stream.on("end", () =>
      resolve(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks)),
    );
  });
}
