import { Readable } from "node:stream";
import { BlobServiceClient } from "@azure/storage-blob";
import Fastify from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";

jest.mock("@azure/storage-blob");
jest.mock("@azure/identity");

describe("AzureFileStore", () => {
  let fastify: ReturnType<typeof Fastify>;
  let mockBlobClient: any;
  let mockContainerClient: any;
  let store: FileStore;

  beforeEach(async () => {
    jest.clearAllMocks();
    fastify = Fastify();

    mockBlobClient = {
      exists: jest.fn(),
      uploadData: jest.fn(),
      uploadFile: jest.fn(),
      uploadStream: jest.fn(),
      download: jest.fn(),
      getProperties: jest.fn(),
    };

    mockContainerClient = {
      getBlobClient: jest.fn(() => mockBlobClient),
      getBlockBlobClient: jest.fn(() => mockBlobClient),
    };

    (BlobServiceClient as unknown as jest.Mock).mockImplementation(() => ({
      getContainerClient: jest.fn(() => mockContainerClient),
    }));

    process.env.AZURE_STORAGE_ACCOUNT_URL =
      "https://test.blob.core.windows.net";
    process.env.AZURE_STORAGE_CONTAINER = "test-container";

    await fastify.register(FileStorePlugin, { type: "azureBlob" });
    store = fastify.FileStore;
  });

  afterEach(async () => {
    await fastify.close();
    delete process.env.AZURE_STORAGE_ACCOUNT_URL;
    delete process.env.AZURE_STORAGE_CONTAINER;
  });

  describe("ConfigureAzure", () => {
    it("should build the container client from the env vars", () => {
      expect(BlobServiceClient).toHaveBeenCalledWith(
        "https://test.blob.core.windows.net",
        expect.anything(),
        {},
      );
      expect(store).toBeDefined();
    });

    it("should throw when AZURE_STORAGE_ACCOUNT_URL is missing", async () => {
      delete process.env.AZURE_STORAGE_ACCOUNT_URL;
      const f = Fastify();
      await expect(
        f.register(FileStorePlugin, { type: "azureBlob" }),
      ).rejects.toThrow("AZURE_STORAGE_ACCOUNT_URL is not defined");
      await f.close();
    });

    it("should throw when AZURE_STORAGE_CONTAINER is missing", async () => {
      delete process.env.AZURE_STORAGE_CONTAINER;
      const f = Fastify();
      await expect(
        f.register(FileStorePlugin, { type: "azureBlob" }),
      ).rejects.toThrow("AZURE_STORAGE_CONTAINER is not defined");
      await f.close();
    });
  });

  describe("exists", () => {
    it("should return true when the blob exists", async () => {
      mockBlobClient.exists.mockResolvedValueOnce(true);
      await expect(store.exists("a.txt")).resolves.toBe(true);
      expect(mockContainerClient.getBlobClient).toHaveBeenCalledWith("a.txt");
    });

    it("should return false when the blob does not exist", async () => {
      mockBlobClient.exists.mockResolvedValueOnce(false);
      await expect(store.exists("a.txt")).resolves.toBe(false);
    });
  });

  describe("save", () => {
    it("should encode a string payload as utf8", async () => {
      mockBlobClient.uploadData.mockResolvedValueOnce({});

      await store.save("a.txt", "text/plain", "content");

      expect(mockBlobClient.uploadData).toHaveBeenCalledWith(
        Buffer.from("content", "utf8"),
        { blobHTTPHeaders: { blobContentType: "text/plain" } },
      );
    });

    it("should pass a Buffer payload through untouched", async () => {
      mockBlobClient.uploadData.mockResolvedValueOnce({});
      const data = Buffer.from([1, 2, 3]);

      await store.save("a.bin", "application/octet-stream", data);

      expect(mockBlobClient.uploadData.mock.calls[0][0]).toBe(data);
    });

    it("should throw when the response carries an errorCode", async () => {
      mockBlobClient.uploadData.mockResolvedValueOnce({
        errorCode: "BadThing",
      });
      await expect(store.save("a.txt", "text/plain", "x")).rejects.toThrow(
        "BadThing",
      );
    });
  });

  describe("getAsBuffer", () => {
    it("should collect the readable stream body", async () => {
      mockBlobClient.download.mockResolvedValueOnce({
        readableStreamBody: Readable.from(["hello"]),
      });

      const buf = await store.getAsBuffer("a.txt");

      expect(buf.toString()).toBe("hello");
    });

    it("should throw when there is no readable stream body", async () => {
      mockBlobClient.download.mockResolvedValueOnce({});
      await expect(store.getAsBuffer("a.txt")).rejects.toThrow(
        "No readableStreamBody",
      );
    });
  });

  describe("copyFromLocalFile", () => {
    it("should upload the local file", async () => {
      mockBlobClient.uploadFile.mockResolvedValueOnce({});

      await store.copyFromLocalFile("a.txt", "text/plain", "/tmp/a.txt");

      expect(mockBlobClient.uploadFile).toHaveBeenCalledWith("/tmp/a.txt", {
        blobHTTPHeaders: { blobContentType: "text/plain" },
      });
    });

    it("should throw when the response carries an errorCode", async () => {
      mockBlobClient.uploadFile.mockResolvedValueOnce({ errorCode: "Nope" });
      await expect(
        store.copyFromLocalFile("a.txt", "text/plain", "/tmp/a.txt"),
      ).rejects.toThrow("Nope");
    });
  });

  describe("getAsStream", () => {
    it("should return the readable stream body", async () => {
      const body = Readable.from(["hello"]);
      mockBlobClient.download.mockResolvedValueOnce({
        readableStreamBody: body,
      });

      await expect(store.getAsStream("a.txt")).resolves.toBe(body);
    });

    it("should throw when there is no readable stream body", async () => {
      mockBlobClient.download.mockResolvedValueOnce({});
      await expect(store.getAsStream("a.txt")).rejects.toThrow(
        "No readableStreamBody",
      );
    });
  });

  describe("copyFromStream", () => {
    it("should upload the stream", async () => {
      mockBlobClient.uploadStream.mockResolvedValueOnce({});
      const rs = Readable.from(["hello"]);

      await store.copyFromStream("a.txt", "text/plain", rs);

      expect(mockBlobClient.uploadStream).toHaveBeenCalledWith(
        rs,
        undefined,
        undefined,
        { blobHTTPHeaders: { blobContentType: "text/plain" } },
      );
    });

    it("should throw when the response carries an errorCode", async () => {
      mockBlobClient.uploadStream.mockResolvedValueOnce({ errorCode: "Nope" });
      await expect(
        store.copyFromStream("a.txt", "text/plain", Readable.from(["x"])),
      ).rejects.toThrow("Nope");
    });
  });

  describe("getInfo", () => {
    it("should map the blob properties", async () => {
      mockBlobClient.getProperties.mockResolvedValueOnce({
        contentLength: 100,
        contentType: "text/plain",
        lastModified: new Date("2023-01-01"),
      });

      await expect(store.getInfo("a.txt")).resolves.toEqual({
        size: 100,
        contentType: "text/plain",
        lastModified: new Date("2023-01-01"),
      });
    });

    it("should fall back when the properties are absent", async () => {
      mockBlobClient.getProperties.mockResolvedValueOnce({});

      await expect(store.getInfo("a.txt")).resolves.toEqual({
        size: 0,
        contentType: "application/octet-stream",
        lastModified: new Date(0),
      });
    });

    it("should return null on a 404", async () => {
      mockBlobClient.getProperties.mockRejectedValueOnce({ statusCode: 404 });
      await expect(store.getInfo("a.txt")).resolves.toBeNull();
    });

    it("should rethrow a non-404 error", async () => {
      const err = Object.assign(new Error("boom"), { statusCode: 500 });
      mockBlobClient.getProperties.mockRejectedValueOnce(err);
      await expect(store.getInfo("a.txt")).rejects.toThrow("boom");
    });
  });
});
