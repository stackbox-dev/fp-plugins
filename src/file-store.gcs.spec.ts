import { Readable, Writable } from "node:stream";
import { Storage } from "@google-cloud/storage";
import Fastify from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";

jest.mock("@google-cloud/storage");

describe("GCPFileStore", () => {
  let fastify: ReturnType<typeof Fastify>;
  let mockFile: any;
  let mockBucket: any;
  let mockStorage: any;
  let store: FileStore;

  beforeEach(async () => {
    jest.clearAllMocks();

    mockFile = {
      exists: jest.fn(),
      save: jest.fn(),
      getMetadata: jest.fn(),
      createReadStream: jest.fn(),
      createWriteStream: jest.fn(),
    };

    mockBucket = {
      file: jest.fn(() => mockFile),
      upload: jest.fn(),
    };

    mockStorage = {
      bucket: jest.fn(() => mockBucket),
    };

    (Storage as unknown as jest.Mock).mockImplementation(() => mockStorage);

    process.env.STORAGE_BUCKET = "test-bucket";

    fastify = Fastify();
    await fastify.register(FileStorePlugin, { type: "gcs" });
    store = fastify.FileStore;
  });

  afterEach(async () => {
    delete process.env.STORAGE_BUCKET;
    await fastify.close();
  });

  describe("ConfigureGCP", () => {
    it("should throw when STORAGE_BUCKET is not defined", async () => {
      delete process.env.STORAGE_BUCKET;
      const f = Fastify();
      await expect(
        f.register(FileStorePlugin, { type: "gcs" }),
      ).rejects.toThrow("STORAGE_BUCKET env-var is not defined");
      await f.close();
    });

    it("should decorate the instance with a FileStore", () => {
      expect(store).toBeDefined();
      expect(Storage).toHaveBeenCalled();
    });
  });

  describe("exists", () => {
    it("should return true when the file exists", async () => {
      mockFile.exists.mockResolvedValueOnce([true]);

      expect(await store.exists("test.txt")).toBe(true);
      expect(mockStorage.bucket).toHaveBeenCalledWith("test-bucket");
      expect(mockBucket.file).toHaveBeenCalledWith("test.txt");
    });

    it("should return false when the file does not exist", async () => {
      mockFile.exists.mockResolvedValueOnce([false]);

      expect(await store.exists("missing.txt")).toBe(false);
    });
  });

  describe("save", () => {
    it("should save the data with the content type", async () => {
      mockFile.save.mockResolvedValueOnce(undefined);

      await store.save("test.txt", "text/plain", "content");

      expect(mockFile.save).toHaveBeenCalledWith("content", {
        contentType: "text/plain",
      });
    });
  });

  describe("getAsBuffer", () => {
    it("should collect the read stream into a buffer", async () => {
      mockFile.createReadStream.mockReturnValueOnce(
        Readable.from(["hello ", "world"]),
      );

      const buf = await store.getAsBuffer("test.txt");

      expect(buf.toString()).toBe("hello world");
    });
  });

  describe("copyFromLocalFile", () => {
    it("should upload the local file to the destination", async () => {
      mockBucket.upload.mockResolvedValueOnce(undefined);

      await store.copyFromLocalFile("dest.txt", "text/plain", "/tmp/src.txt");

      expect(mockBucket.upload).toHaveBeenCalledWith("/tmp/src.txt", {
        contentType: "text/plain",
        destination: "dest.txt",
      });
    });
  });

  describe("getAsStream", () => {
    it("should return the read stream", async () => {
      const rs = Readable.from(["data"]);
      mockFile.createReadStream.mockReturnValueOnce(rs);

      expect(await store.getAsStream("test.txt")).toBe(rs);
    });
  });

  describe("copyFromStream", () => {
    it("should pipe the source stream into the gcs write stream", async () => {
      const chunks: Buffer[] = [];
      const ws = new Writable({
        write(chunk, _enc, cb) {
          chunks.push(chunk);
          cb();
        },
      });
      mockFile.createWriteStream.mockReturnValueOnce(ws);

      await store.copyFromStream(
        "test.txt",
        "text/plain",
        Readable.from(["piped content"]),
      );

      expect(mockFile.createWriteStream).toHaveBeenCalledWith({
        resumable: false,
        contentType: "text/plain",
      });
      expect(Buffer.concat(chunks).toString()).toBe("piped content");
    });
  });

  describe("getInfo", () => {
    it("should use a numeric size as-is", async () => {
      mockFile.getMetadata.mockResolvedValueOnce([
        {
          size: 100,
          contentType: "text/plain",
          updated: "2023-01-01T00:00:00.000Z",
        },
      ]);

      expect(await store.getInfo("test.txt")).toEqual({
        size: 100,
        contentType: "text/plain",
        lastModified: new Date("2023-01-01T00:00:00.000Z"),
      });
    });

    it("should parse a numeric string size", async () => {
      mockFile.getMetadata.mockResolvedValueOnce([
        {
          size: "512",
          contentType: "application/pdf",
          updated: "2024-06-01T12:00:00.000Z",
        },
      ]);

      expect(await store.getInfo("test.pdf")).toEqual({
        size: 512,
        contentType: "application/pdf",
        lastModified: new Date("2024-06-01T12:00:00.000Z"),
      });
    });

    it("should default size to 0 when metadata.size is absent", async () => {
      mockFile.getMetadata.mockResolvedValueOnce([
        { contentType: "text/plain", updated: "2023-01-01T00:00:00.000Z" },
      ]);

      const info = await store.getInfo("test.txt");

      expect(info!.size).toBe(0);
    });

    it("should fall back to 0 when the size string is not a number", async () => {
      mockFile.getMetadata.mockResolvedValueOnce([
        {
          size: "not-a-number",
          contentType: "text/plain",
          updated: "2023-01-01T00:00:00.000Z",
        },
      ]);

      const info = await store.getInfo("test.txt");

      expect(info!.size).toBe(0);
    });

    it("should fall back to octet-stream and epoch when contentType and updated are absent", async () => {
      mockFile.getMetadata.mockResolvedValueOnce([{ size: 10 }]);

      expect(await store.getInfo("test.bin")).toEqual({
        size: 10,
        contentType: "application/octet-stream",
        lastModified: new Date(0),
      });
    });

    it("should return null on a 404", async () => {
      const err: any = new Error("Not Found");
      err.code = 404;
      mockFile.getMetadata.mockRejectedValueOnce(err);

      expect(await store.getInfo("missing.txt")).toBeNull();
    });

    it("should rethrow a non-404 error", async () => {
      const err: any = new Error("boom");
      err.code = 500;
      mockFile.getMetadata.mockRejectedValueOnce(err);

      await expect(store.getInfo("test.txt")).rejects.toThrow("boom");
    });
  });
});
