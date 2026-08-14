import { Readable } from "node:stream";
import * as S3 from "@aws-sdk/client-s3";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { Upload } from "@aws-sdk/lib-storage";
import Fastify from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";

jest.mock("@aws-sdk/client-s3");
jest.mock("@aws-sdk/lib-storage");
jest.mock("@aws-sdk/credential-provider-node");

const ORIGINAL_ENV = process.env;

describe("S3FileStore", () => {
  let mockSend: jest.Mock;
  let mockDone: jest.Mock;
  let fastify: ReturnType<typeof Fastify>;

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
    fastify = Fastify();
    jest.clearAllMocks();

    mockSend = jest.fn();
    (S3.S3Client as unknown as jest.Mock).mockImplementation(() => ({
      send: mockSend,
    }));

    mockDone = jest.fn().mockResolvedValue(undefined);
    (Upload as unknown as jest.Mock).mockImplementation(() => ({
      done: mockDone,
    }));
  });

  afterEach(async () => {
    await fastify.close();
    process.env = ORIGINAL_ENV;
  });

  const clientConfig = () =>
    (S3.S3Client as unknown as jest.Mock).mock.calls[0][0];

  describe("ConfigureAWS", () => {
    beforeEach(() => {
      process.env.S3_BUCKET = "test-bucket";
    });

    it("uses AWS_S3_REGION when set", async () => {
      process.env.AWS_S3_REGION = "ap-south-1";

      await fastify.register(FileStorePlugin, { type: "s3" });

      expect(fastify.FileStore).toBeDefined();
      expect(clientConfig()).toEqual({
        region: "ap-south-1",
        credentialDefaultProvider: defaultProvider,
      });
    });

    it("falls back to the standard AWS_REGION when AWS_S3_REGION is unset", async () => {
      delete process.env.AWS_S3_REGION;
      process.env.AWS_REGION = "eu-central-1";

      await fastify.register(FileStorePlugin, { type: "s3" });

      expect(clientConfig().region).toBe("eu-central-1");
    });

    it("prefers AWS_S3_REGION over AWS_REGION", async () => {
      process.env.AWS_S3_REGION = "ap-south-1";
      process.env.AWS_REGION = "eu-central-1";

      await fastify.register(FileStorePlugin, { type: "s3" });

      expect(clientConfig().region).toBe("ap-south-1");
    });

    it("defaults the region to us-east-1 when neither is set", async () => {
      delete process.env.AWS_S3_REGION;
      delete process.env.AWS_REGION;

      await fastify.register(FileStorePlugin, { type: "s3" });

      expect(clientConfig().region).toBe("us-east-1");
    });

    it("validates S3_BUCKET before constructing the client", async () => {
      delete process.env.S3_BUCKET;

      await expect(
        fastify.register(FileStorePlugin, { type: "s3" }),
      ).rejects.toThrow("S3_BUCKET env-var is not defined");
      expect(S3.S3Client).not.toHaveBeenCalled();
    });

    it("throws when S3_BUCKET is not defined", async () => {
      delete process.env.S3_BUCKET;

      await expect(
        fastify.register(FileStorePlugin, { type: "s3" }),
      ).rejects.toThrow("S3_BUCKET env-var is not defined");
    });
  });

  describe("ConfigureMinio", () => {
    beforeEach(() => {
      process.env.MINIO_ENDPOINT = "http://localhost:9000";
      process.env.MINIO_ACCESS_KEY_ID = "minio-key";
      process.env.MINIO_SECRET_ACCESS_KEY = "minio-secret";
      process.env.MINIO_BUCKET = "minio-bucket";
    });

    it.each([
      ["MINIO_ENDPOINT", "MINIO_ENDPOINT env-var is not defined"],
      ["MINIO_ACCESS_KEY_ID", "MINIO_ACCESS_KEY_ID env-var is not defined"],
      [
        "MINIO_SECRET_ACCESS_KEY",
        "MINIO_SECRET_ACCESS_KEY env-var is not defined",
      ],
      ["MINIO_BUCKET", "MINIO_BUCKET env-var is not defined"],
    ])("throws when %s is missing", async (envVar, message) => {
      delete process.env[envVar];

      await expect(
        fastify.register(FileStorePlugin, { type: "minio" }),
      ).rejects.toThrow(message);
    });

    it("uses MINIO_REGION when set", async () => {
      process.env.MINIO_REGION = "eu-west-1";

      await fastify.register(FileStorePlugin, { type: "minio" });

      expect(fastify.FileStore).toBeDefined();
      expect(clientConfig()).toEqual({
        region: "eu-west-1",
        endpoint: "http://localhost:9000",
        credentials: {
          accessKeyId: "minio-key",
          secretAccessKey: "minio-secret",
        },
        forcePathStyle: true,
      });
    });

    it("defaults the region to us-east-1 when MINIO_REGION is unset", async () => {
      delete process.env.MINIO_REGION;

      await fastify.register(FileStorePlugin, { type: "minio" });

      expect(clientConfig().region).toBe("us-east-1");
    });
  });

  describe("FileStore operations", () => {
    let store: FileStore;

    beforeEach(async () => {
      process.env.S3_BUCKET = "test-bucket";
      await fastify.register(FileStorePlugin, { type: "s3" });
      store = fastify.FileStore;
    });

    describe("exists", () => {
      it("returns true when the head request succeeds", async () => {
        mockSend.mockResolvedValueOnce({});

        await expect(store.exists("a/b.txt")).resolves.toBe(true);
        expect(S3.HeadObjectCommand).toHaveBeenCalledWith({
          Bucket: "test-bucket",
          Key: "a/b.txt",
        });
      });

      // HeadObject reports a missing key as NotFound, not NoSuchKey.
      it("returns false on a NotFound error", async () => {
        mockSend.mockRejectedValueOnce(new S3.NotFound({} as any));

        await expect(store.exists("a/b.txt")).resolves.toBe(false);
      });

      it("returns false on a 404 $metadata status", async () => {
        const err: any = new Error("Not Found");
        err["$metadata"] = { httpStatusCode: 404 };
        mockSend.mockRejectedValueOnce(err);

        await expect(store.exists("a/b.txt")).resolves.toBe(false);
      });

      it("rethrows any other error", async () => {
        mockSend.mockRejectedValueOnce(new Error("boom"));

        await expect(store.exists("a/b.txt")).rejects.toThrow("boom");
      });
    });

    describe("save", () => {
      it("sends a PutObjectCommand", async () => {
        mockSend.mockResolvedValueOnce({});

        await store.save("a/b.txt", "text/plain", "content");

        expect(S3.PutObjectCommand).toHaveBeenCalledWith({
          Bucket: "test-bucket",
          Key: "a/b.txt",
          Body: "content",
          ContentType: "text/plain",
        });
        expect(mockSend).toHaveBeenCalledWith(expect.any(S3.PutObjectCommand));
      });
    });

    describe("getAsBuffer", () => {
      it("buffers the response body", async () => {
        mockSend.mockResolvedValueOnce({ Body: Readable.from(["hello"]) });

        const buf = await store.getAsBuffer("a/b.txt");

        expect(buf.toString()).toBe("hello");
        expect(S3.GetObjectCommand).toHaveBeenCalledWith({
          Bucket: "test-bucket",
          Key: "a/b.txt",
        });
      });

      it("throws when the response has no Body", async () => {
        mockSend.mockResolvedValueOnce({});

        await expect(store.getAsBuffer("a/b.txt")).rejects.toThrow(
          "No Body in response for a/b.txt",
        );
      });
    });

    describe("getAsStream", () => {
      it("returns the response body", async () => {
        const body = Readable.from(["hello"]);
        mockSend.mockResolvedValueOnce({ Body: body });

        await expect(store.getAsStream("a/b.txt")).resolves.toBe(body);
      });

      it("throws when the response has no Body", async () => {
        mockSend.mockResolvedValueOnce({});

        await expect(store.getAsStream("a/b.txt")).rejects.toThrow(
          "No Body in response for a/b.txt",
        );
      });
    });

    describe("copyFromLocalFile", () => {
      it("uploads a read stream of the local file", async () => {
        await store.copyFromLocalFile("a/b.ts", "text/plain", __filename);

        const params = (Upload as unknown as jest.Mock).mock.calls[0][0];
        expect(params.params).toMatchObject({
          Bucket: "test-bucket",
          Key: "a/b.ts",
          ContentType: "text/plain",
        });
        expect(params.params.Body.path).toBe(__filename);
        expect(mockDone).toHaveBeenCalled();

        params.params.Body.destroy();
      });
    });

    describe("copyFromStream", () => {
      it("uploads the given stream", async () => {
        const rs = Readable.from(["hello"]);

        await store.copyFromStream("a/b.txt", "text/plain", rs);

        expect(Upload).toHaveBeenCalledWith({
          client: expect.anything(),
          params: {
            Bucket: "test-bucket",
            Key: "a/b.txt",
            Body: rs,
            ContentType: "text/plain",
          },
        });
        expect(mockDone).toHaveBeenCalled();
      });
    });

    describe("getInfo", () => {
      it("maps the head response", async () => {
        const lastModified = new Date("2023-01-01T00:00:00.000Z");
        mockSend.mockResolvedValueOnce({
          ContentLength: 100,
          ContentType: "text/plain",
          LastModified: lastModified,
        });

        await expect(store.getInfo("a/b.txt")).resolves.toEqual({
          size: 100,
          contentType: "text/plain",
          lastModified,
        });
        expect(S3.HeadObjectCommand).toHaveBeenCalledWith({
          Bucket: "test-bucket",
          Key: "a/b.txt",
        });
      });

      it("falls back when the head response is missing fields", async () => {
        mockSend.mockResolvedValueOnce({});

        const info = await store.getInfo("a/b.txt");

        // epoch, matching Azure and GCP — an unknown timestamp must not read as "now".
        expect(info).toEqual({
          size: 0,
          contentType: "application/octet-stream",
          lastModified: new Date(0),
        });
      });

      it("returns null on a NotFound error", async () => {
        mockSend.mockRejectedValueOnce(new S3.NotFound({} as any));

        await expect(store.getInfo("a/b.txt")).resolves.toBeNull();
      });

      it("returns null on a 404 $metadata status", async () => {
        const err: any = new Error("Not Found");
        err["$metadata"] = { httpStatusCode: 404 };
        mockSend.mockRejectedValueOnce(err);

        await expect(store.getInfo("a/b.txt")).resolves.toBeNull();
      });

      it("rethrows any other error", async () => {
        mockSend.mockRejectedValueOnce(new Error("boom"));

        await expect(store.getInfo("a/b.txt")).rejects.toThrow("boom");
      });
    });
  });
});
