import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Readable } from "node:stream";
import Fastify, { FastifyInstance } from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";
import { streamToBuffer } from "./utils";

// LocalFileStore is not exported; reach it through the plugin decorator.
const storeOf = (f: FastifyInstance): FileStore & { dir: string } =>
  (f as any).FileStore;

const defaultDir = path.join(os.tmpdir(), "stackboxwms");

describe("LocalFileStore", () => {
  let fastify: FastifyInstance;
  let tempDir: string;
  let srcDir: string;
  const envBefore = process.env.LOCAL_STORAGE_DIR;

  beforeEach(async () => {
    fastify = Fastify({ logger: false });
    tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "local-store-"));
    srcDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "local-src-"));
    process.env.LOCAL_STORAGE_DIR = tempDir;
  });

  afterEach(async () => {
    jest.restoreAllMocks();
    await fastify.close();
    await fs.promises.rm(tempDir, { recursive: true, force: true });
    await fs.promises.rm(srcDir, { recursive: true, force: true });
    if (envBefore === undefined) {
      delete process.env.LOCAL_STORAGE_DIR;
    } else {
      process.env.LOCAL_STORAGE_DIR = envBefore;
    }
  });

  const register = async (): Promise<FileStore & { dir: string }> => {
    await fastify.register(FileStorePlugin, { type: "local" });
    return storeOf(fastify);
  };

  describe("ConfigureLocal", () => {
    it("uses LOCAL_STORAGE_DIR when set", async () => {
      const store = await register();
      expect(store.dir).toBe(tempDir);
      expect((await fs.promises.stat(tempDir)).isDirectory()).toBe(true);
    });

    it("falls back to os.tmpdir()/stackboxwms when LOCAL_STORAGE_DIR is unset", async () => {
      delete process.env.LOCAL_STORAGE_DIR;
      const store = await register();
      expect(store.dir).toBe(defaultDir);
      expect((await fs.promises.stat(defaultDir)).isDirectory()).toBe(true);
    });

    it("falls back to os.tmpdir()/stackboxwms when LOCAL_STORAGE_DIR is empty", async () => {
      process.env.LOCAL_STORAGE_DIR = "";
      const store = await register();
      expect(store.dir).toBe(defaultDir);
      expect((await fs.promises.stat(defaultDir)).isDirectory()).toBe(true);
    });
  });

  describe("plugin dispatch", () => {
    it("registers the local store", async () => {
      await fastify.register(FileStorePlugin, { type: "local" });
      expect(storeOf(fastify)).toBeDefined();
    });

    it("throws for an unknown storage type", async () => {
      await expect(
        fastify.register(FileStorePlugin, { type: "nope" as any }),
      ).rejects.toThrow("Unknown storage type: nope");
    });
  });

  describe("exists", () => {
    it("returns true for an existing file", async () => {
      const store = await register();
      await store.save("there.txt", "text/plain", "x");
      expect(await store.exists("there.txt")).toBe(true);
    });

    it("returns false for a missing file rather than throwing", async () => {
      const store = await register();
      expect(await store.exists("not-there.txt")).toBe(false);
    });

    it("rethrows non-ENOENT errors", async () => {
      const store = await register();
      await store.save("a-file.txt", "text/plain", "x");
      // stat()ing through a regular file yields ENOTDIR, not ENOENT
      await expect(store.exists("a-file.txt/child")).rejects.toMatchObject({
        code: "ENOTDIR",
      });
    });
  });

  describe("getInfo", () => {
    it("returns size, content type and mtime", async () => {
      const store = await register();
      await store.save("info.txt", "text/plain", "hello");
      const info = await store.getInfo("info.txt");
      expect(info?.size).toBe(5);
      expect(info?.contentType).toBe("application/octet-stream");
      // fs.Stats dates come from another realm, so instanceof Date is false here
      expect(info?.lastModified.getTime()).toBeGreaterThan(0);
    });

    it("returns null when the file does not exist", async () => {
      const store = await register();
      expect(await store.getInfo("missing.txt")).toBeNull();
    });

    it("rethrows non-ENOENT errors", async () => {
      const store = await register();
      await store.save("a-file.txt", "text/plain", "x");
      // stat()ing through a regular file yields ENOTDIR, not ENOENT
      await expect(store.getInfo("a-file.txt/child")).rejects.toMatchObject({
        code: "ENOTDIR",
      });
    });
  });

  describe("save", () => {
    it("writes a string payload", async () => {
      const store = await register();
      await store.save("str.txt", "text/plain", "string payload");
      expect(
        await fs.promises.readFile(path.join(tempDir, "str.txt"), "utf8"),
      ).toBe("string payload");
    });

    it("writes a Buffer payload", async () => {
      const store = await register();
      await store.save(
        "buf.bin",
        "application/octet-stream",
        Buffer.from([1, 2, 3]),
      );
      expect(await fs.promises.readFile(path.join(tempDir, "buf.bin"))).toEqual(
        Buffer.from([1, 2, 3]),
      );
    });

    it("creates missing parent directories, like its sibling methods", async () => {
      const store = await register();
      await store.save("deep/nested/dir/file.txt", "text/plain", "nested");
      expect(
        await fs.promises.readFile(
          path.join(tempDir, "deep/nested/dir/file.txt"),
          "utf8",
        ),
      ).toBe("nested");
    });
  });

  describe("getAsBuffer", () => {
    it("reads the file", async () => {
      const store = await register();
      await store.save("read.txt", "text/plain", "buffered");
      expect((await store.getAsBuffer("read.txt")).toString()).toBe("buffered");
    });

    it("throws File not found when stat yields nothing", async () => {
      const store = await register();
      jest
        .spyOn(fs.promises, "stat")
        .mockResolvedValue(undefined as unknown as fs.Stats);
      await expect(store.getAsBuffer("ghost.txt")).rejects.toThrow(
        `File not found: ${path.join(tempDir, "ghost.txt")}`,
      );
    });
  });

  describe("getAsStream", () => {
    it("streams the file", async () => {
      const store = await register();
      await store.save("stream.txt", "text/plain", "streamed");
      const rs = await store.getAsStream("stream.txt");
      expect((await streamToBuffer(rs)).toString()).toBe("streamed");
    });

    it("throws File not found when stat yields nothing", async () => {
      const store = await register();
      jest
        .spyOn(fs.promises, "stat")
        .mockResolvedValue(undefined as unknown as fs.Stats);
      await expect(store.getAsStream("ghost.txt")).rejects.toThrow(
        `File not found: ${path.join(tempDir, "ghost.txt")}`,
      );
    });
  });

  describe("copyFromLocalFile", () => {
    it("copies into a nested destination", async () => {
      const store = await register();
      const src = path.join(srcDir, "src.txt");
      await fs.promises.writeFile(src, "copied");
      await store.copyFromLocalFile("a/b/c.txt", "text/plain", src);
      expect(
        await fs.promises.readFile(path.join(tempDir, "a/b/c.txt"), "utf8"),
      ).toBe("copied");
    });
  });

  describe("copyFromStream", () => {
    it("pipes into a nested destination", async () => {
      const store = await register();
      await store.copyFromStream(
        "x/y/z.txt",
        "text/plain",
        Readable.from([Buffer.from("piped")]),
      );
      expect(
        await fs.promises.readFile(path.join(tempDir, "x/y/z.txt"), "utf8"),
      ).toBe("piped");
    });
  });
});

describe("streamToBuffer string chunks", () => {
  it("converts a single string chunk", async () => {
    const result = await streamToBuffer(Readable.from(["solo"]));
    expect(result).toBeInstanceOf(Buffer);
    expect(result.toString()).toBe("solo");
  });

  it("converts multiple string chunks", async () => {
    const result = await streamToBuffer(Readable.from(["one", "two"]));
    expect(result.toString()).toBe("onetwo");
  });

  it("rejects when the stream errors", async () => {
    const boom = new Error("boom");
    const rs = new Readable({
      read() {
        this.destroy(boom);
      },
    });
    await expect(streamToBuffer(rs)).rejects.toThrow("boom");
  });
});

describe("index barrel", () => {
  afterEach(() => {
    jest.dontMock("./file-store");
  });

  it("re-exports the plugin", () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    expect(require("./index").Plugins.FileStore).toBe(FileStorePlugin);
  });

  it("handles a file-store module without __esModule", () => {
    jest.isolateModules(() => {
      const cjs = { default: "cjs-plugin" };
      jest.doMock("./file-store", () => cjs);
      // __importDefault wraps a non-ESM module, so .default is the module itself
      expect(require("./index").Plugins.FileStore).toBe(cjs);
    });
  });
});
