import type { FileStore } from "./file-store";

export {};

declare module "fastify" {
  export interface FastifySchema {
    operationId?: string;
    summary?: string;
    description?: string;
  }

  // FileStore is decorated onto the instance by the plugin. Without this, consumers
  // get no compile-time safety on fastify.FileStore and have to declare it themselves.
  export interface FastifyInstance {
    FileStore: FileStore;
  }
}
