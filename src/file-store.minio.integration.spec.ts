import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Readable } from "node:stream";
import * as S3 from "@aws-sdk/client-s3";
import Fastify from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";

// The only test that speaks a real S3 wire protocol. Everything else mocks the SDK,
// which cannot catch a behavioural change in the client — the NoSuchKey/NotFound
// mismatch fixed in #9 was invisible to the mocked suite.
//
// Opt-in: set MINIO_TEST_ENDPOINT. Skipped otherwise so the default suite needs no
// Docker. See CONTRIBUTING.md for the one-liner that starts a container.
const endpoint = process.env.MINIO_TEST_ENDPOINT;
const describeIf = endpoint ? describe : describe.skip;

describeIf("MinIO integration", () => {
  let fastify: ReturnType<typeof Fastify>;
  let store: FileStore;
  const ORIGINAL_ENV = process.env;
  const bucket = "fp-plugins-it";

  beforeAll(async () => {
    const client = new S3.S3Client({
      region: "us-east-1",
      endpoint,
      credentials: {
        accessKeyId: process.env.MINIO_TEST_ACCESS_KEY ?? "minioadmin",
        secretAccessKey: process.env.MINIO_TEST_SECRET_KEY ?? "minioadmin",
      },
      forcePathStyle: true,
    });
    try {
      await client.send(new S3.CreateBucketCommand({ Bucket: bucket }));
    } catch {
      // already exists
    }
  }, 60_000);

  beforeEach(async () => {
    process.env = { ...ORIGINAL_ENV };
    process.env.MINIO_ENDPOINT = endpoint;
    process.env.MINIO_ACCESS_KEY_ID =
      process.env.MINIO_TEST_ACCESS_KEY ?? "minioadmin";
    process.env.MINIO_SECRET_ACCESS_KEY =
      process.env.MINIO_TEST_SECRET_KEY ?? "minioadmin";
    process.env.MINIO_BUCKET = bucket;
    fastify = Fastify({ logger: false });
    await fastify.register(FileStorePlugin, { type: "minio" });
    store = fastify.FileStore;
  });

  afterEach(async () => {
    process.env = ORIGINAL_ENV;
    await fastify.close();
  });

  it("reports a missing key as false rather than throwing", async () => {
    expect(await store.exists(`missing-${Date.now()}.txt`)).toBe(false);
  });

  it("returns null from getInfo for a missing key", async () => {
    expect(await store.getInfo(`missing-${Date.now()}.txt`)).toBeNull();
  });

  it("round-trips a string payload", async () => {
    const key = `round-trip-${Date.now()}.txt`;
    await store.save(key, "text/plain", "hello minio");
    expect(await store.exists(key)).toBe(true);
    expect((await store.getAsBuffer(key)).toString()).toBe("hello minio");
  });

  it("reports size and content type from a real HeadObject", async () => {
    const key = `info-${Date.now()}.txt`;
    await store.save(key, "text/plain", "12345");
    const info = await store.getInfo(key);
    expect(info?.size).toBe(5);
    expect(info?.contentType).toBe("text/plain");
    expect(info?.lastModified.getTime()).toBeGreaterThan(0);
  });

  it("streams a payload in and back out", async () => {
    const key = `stream-${Date.now()}.txt`;
    await store.copyFromStream(
      key,
      "text/plain",
      Readable.from(["a", "b", "c"]),
    );
    const rs = await store.getAsStream(key);
    const chunks: Buffer[] = [];
    for await (const c of rs as unknown as AsyncIterable<Buffer>)
      chunks.push(c);
    expect(Buffer.concat(chunks).toString()).toBe("abc");
  });

  it("uploads a local file through the multipart path", async () => {
    const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "fp-it-"));
    const local = path.join(dir, "src.bin");
    await fs.promises.writeFile(local, Buffer.alloc(64 * 1024, 7));
    const key = `upload-${Date.now()}.bin`;
    await store.copyFromLocalFile(key, "application/octet-stream", local);
    expect((await store.getInfo(key))?.size).toBe(64 * 1024);
    await fs.promises.rm(dir, { recursive: true });
  }, 30_000);
});
