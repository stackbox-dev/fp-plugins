import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import Fastify from "fastify";
import { Plugins } from "./index";

describe("Plugin Integration Tests", () => {
  let fastify: ReturnType<typeof Fastify>;
  let tempDir: string;

  beforeEach(async () => {
    fastify = Fastify({ logger: false });
    tempDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "integration-test-"),
    );
  });

  afterEach(async () => {
    await fastify.close();
    await fs.promises.rm(tempDir, { recursive: true });
  });

  describe("FileStore", () => {
    it("should register successfully", async () => {
      process.env.LOCAL_STORAGE_DIR = tempDir;
      await fastify.register(Plugins.FileStore, { type: "local" });
      expect(fastify.FileStore).toBeDefined();
    });

    it("should handle basic file operations", async () => {
      process.env.LOCAL_STORAGE_DIR = tempDir;
      await fastify.register(Plugins.FileStore, { type: "local" });

      await fastify.FileStore.save("test.txt", "text/plain", "test content");
      expect(await fastify.FileStore.exists("test.txt")).toBe(true);
    });

    it("should handle multiple independent instances", async () => {
      const fastify1 = Fastify({ logger: false });
      const fastify2 = Fastify({ logger: false });

      try {
        process.env.LOCAL_STORAGE_DIR = tempDir;
        await fastify1.register(Plugins.FileStore, { type: "local" });

        const tempDir2 = await fs.promises.mkdtemp(
          path.join(os.tmpdir(), "integration-test-2-"),
        );
        process.env.LOCAL_STORAGE_DIR = tempDir2;
        await fastify2.register(Plugins.FileStore, { type: "local" });

        await (fastify1 as any).FileStore.save(
          "file1.txt",
          "text/plain",
          "content1",
        );
        await (fastify2 as any).FileStore.save(
          "file2.txt",
          "text/plain",
          "content2",
        );

        expect((fastify1 as any).FileStore).toBeDefined();
        expect((fastify2 as any).FileStore).toBeDefined();

        await fs.promises.rm(tempDir2, { recursive: true });
      } finally {
        await fastify1.close();
        await fastify2.close();
      }
    });

    it("should fail registration with an invalid path", async () => {
      process.env.LOCAL_STORAGE_DIR = "/invalid/path/that/does/not/exist";
      await expect(
        fastify.register(Plugins.FileStore, { type: "local" }),
      ).rejects.toThrow();
    });

    it("should handle high volume file operations", async () => {
      process.env.LOCAL_STORAGE_DIR = tempDir;
      await fastify.register(Plugins.FileStore, { type: "local" });

      const promises = [];
      for (let i = 0; i < 100; i++) {
        promises.push(
          fastify.FileStore.save(`file${i}.txt`, "text/plain", `content ${i}`),
        );
      }
      await Promise.all(promises);

      for (let i = 0; i < 100; i++) {
        expect(await fastify.FileStore.exists(`file${i}.txt`)).toBe(true);
      }
    });
  });
});
