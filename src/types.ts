export {};

declare module "fastify" {
  export interface FastifySchema {
    operationId?: string;
    summary?: string;
    description?: string;
  }
}
