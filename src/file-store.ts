import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as S3 from "@aws-sdk/client-s3";
import * as AzureIden from "@azure/identity";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { BlobServiceClient, ContainerClient } from "@azure/storage-blob";
import { Storage } from "@google-cloud/storage";
import { FastifyInstance, FastifyPluginAsync } from "fastify";
import fp from "fastify-plugin";
import { streamToBuffer } from "./utils";

export interface FileStore {
  exists(filepath: string): Promise<boolean>;
  save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void>;
  getAsBuffer(filepath: string): Promise<Buffer>;
  getAsStream(filepath: string): Promise<NodeJS.ReadableStream>;
  copyFromLocalFile(
    filepath: string,
    contentType: string,
    localFilepath: string,
  ): Promise<void>;
}

const Configure = {
  s3: ConfigureAWS,
  azureBlob: ConfigureAzure,
  gcs: ConfigureGCP,
  local: ConfigureLocal,
  minio: ConfigureMinio,
};

const plugin: FastifyPluginAsync<{
  type: keyof typeof Configure;
}> = async function (f, opts): Promise<void> {
  const configure = Configure[opts.type];
  if (!configure) {
    throw new Error(`Unknown storage type: ${opts.type}`);
  }
  await configure(f);
};

class LocalFileStore implements FileStore {
  constructor(public dir: string) {}
  async exists(filepath: string): Promise<boolean> {
    const p = path.join(this.dir, filepath);
    return !!(await fs.promises.stat(p));
  }
  async save(
    filepath: string,
    _contentType: string,
    data: string | Buffer,
  ): Promise<void> {
    const p = path.join(this.dir, filepath);
    await fs.promises.writeFile(p, data);
  }
  async getAsBuffer(filepath: string): Promise<Buffer> {
    const p = path.join(this.dir, filepath);
    if (await fs.promises.stat(p)) {
      return fs.promises.readFile(p);
    }
    throw new Error(`File not found: ${p}`);
  }
  async copyFromLocalFile(
    filepath: string,
    _contentType: string,
    localFilepath: string,
  ) {
    await fs.promises.mkdir(path.dirname(path.join(this.dir, filepath)), {
      recursive: true,
    });
    await fs.promises.copyFile(localFilepath, path.join(this.dir, filepath));
  }
  async getAsStream(filepath: string): Promise<NodeJS.ReadableStream> {
    const p = path.join(this.dir, filepath);
    if (await fs.promises.stat(p)) {
      return fs.createReadStream(p);
    }
    throw new Error(`File not found: ${p}`);
  }
}

class AzureFileStore implements FileStore {
  private client: ContainerClient;

  constructor(accountClient: BlobServiceClient, container: string) {
    this.client = accountClient.getContainerClient(container);
  }

  exists(filepath: string): Promise<boolean> {
    return this.client.getBlobClient(filepath).exists();
  }

  async save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void> {
    const blob = this.client.getBlockBlobClient(filepath);
    const resp = await blob.uploadData(
      typeof data === "string" ? Buffer.from(data, "utf8") : data,
      {
        blobHTTPHeaders: {
          blobContentType: contentType,
        },
      },
    );
    if (resp.errorCode) {
      throw new Error(resp.errorCode);
    }
  }

  async getAsBuffer(filepath: string): Promise<Buffer> {
    const blob = this.client.getBlobClient(filepath);
    const resp = await blob.download();
    if (!resp.readableStreamBody) {
      throw new Error("No readableStreamBody");
    }
    return streamToBuffer(resp.readableStreamBody);
  }

  async copyFromLocalFile(
    filepath: string,
    contentType: string,
    localFilepath: string,
  ) {
    const blob = this.client.getBlockBlobClient(filepath);
    const resp = await blob.uploadFile(localFilepath, {
      blobHTTPHeaders: {
        blobContentType: contentType,
      },
    });
    if (resp.errorCode) {
      throw new Error(resp.errorCode);
    }
  }

  async getAsStream(filepath: string): Promise<NodeJS.ReadableStream> {
    const blob = this.client.getBlobClient(filepath);
    const resp = await blob.download();
    if (!resp.readableStreamBody) {
      throw new Error("No readableStreamBody");
    }
    return resp.readableStreamBody;
  }
}

class GCPFileStore implements FileStore {
  private storage: Storage;

  constructor(private bucket: string) {
    this.storage = new Storage();
  }

  async exists(filepath: string): Promise<boolean> {
    const gcsfile = this.storage.bucket(this.bucket).file(filepath);
    const flags = await gcsfile.exists();
    return flags[0];
  }

  async save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void> {
    const gcsfile = this.storage.bucket(this.bucket).file(filepath);
    await gcsfile.save(data, { contentType });
  }

  async getAsBuffer(filepath: string): Promise<Buffer> {
    const gcsfile = this.storage.bucket(this.bucket).file(filepath);
    const strm = gcsfile.createReadStream();
    return streamToBuffer(strm);
  }

  async copyFromLocalFile(
    filepath: string,
    contentType: string,
    localFilepath: string,
  ) {
    await this.storage.bucket(this.bucket).upload(localFilepath, {
      contentType,
      destination: filepath,
    });
  }

  async getAsStream(filepath: string): Promise<NodeJS.ReadableStream> {
    const gcsfile = this.storage.bucket(this.bucket).file(filepath);
    return gcsfile.createReadStream();
  }
}

class S3FileStore implements FileStore {
  constructor(
    private client: S3.S3Client,
    private bucket: string,
  ) {}

  async exists(filepath: string): Promise<boolean> {
    try {
      await this.client.send(
        new S3.HeadObjectCommand({ Bucket: this.bucket, Key: filepath }),
      );
      return true;
    } catch (err) {
      if (err instanceof S3.NoSuchKey) {
        return false;
      }
      if (err["$metadata"].httpStatusCode === 404) {
        return false;
      }
      throw err;
    }
  }

  async save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void> {
    await this.client.send(
      new S3.PutObjectCommand({
        Bucket: this.bucket,
        Key: filepath,
        Body: data,
        ContentType: contentType,
      }),
    );
  }

  async getAsBuffer(filepath: string): Promise<Buffer> {
    const data = await this.client.send(
      new S3.GetObjectCommand({
        Bucket: this.bucket,
        Key: filepath,
      }),
    );
    if (!data.Body) {
      throw new Error(`No Body in response for ${filepath}`);
    }
    return streamToBuffer(data.Body as NodeJS.ReadableStream);
  }

  async copyFromLocalFile(
    filepath: string,
    contentType: string,
    localFilepath: string,
  ) {
    await this.client.send(
      new S3.PutObjectCommand({
        Bucket: this.bucket,
        Key: filepath,
        Body: fs.createReadStream(localFilepath),
        ContentType: contentType,
      }),
    );
  }

  async getAsStream(filepath: string): Promise<NodeJS.ReadableStream> {
    const data = await this.client.send(
      new S3.GetObjectCommand({
        Bucket: this.bucket,
        Key: filepath,
      }),
    );
    if (!data.Body) {
      throw new Error(`No Body in response for ${filepath}`);
    }
    return data.Body as NodeJS.ReadableStream;
  }
}

async function ConfigureLocal(f: FastifyInstance) {
  let dir =
    process.env.LOCAL_STORAGE_DIR ?? path.join(os.tmpdir(), "stackboxwms");
  if (dir === "") {
    dir = path.join(os.tmpdir(), "stackboxwms");
  }
  await fs.promises.mkdir(dir, { recursive: true });
  f.log.warn(`Using LocalFileStore. Dir=${dir}`);
  f.decorate("FileStore", new LocalFileStore(dir));
}

export default fp(plugin, { name: "fp-filestore" });

async function ConfigureAzure(f: FastifyInstance) {
  if (!process.env.AZURE_STORAGE_ACCOUNT_URL) {
    throw new Error("AZURE_STORAGE_ACCOUNT_URL is not defined");
  }
  if (!process.env.AZURE_STORAGE_CONTAINER) {
    throw new Error("AZURE_STORAGE_CONTAINER is not defined");
  }
  const accountClient = new BlobServiceClient(
    process.env.AZURE_STORAGE_ACCOUNT_URL,
    new AzureIden.DefaultAzureCredential({}),
    {},
  );
  f.decorate(
    "FileStore",
    new AzureFileStore(accountClient, process.env.AZURE_STORAGE_CONTAINER),
  );
}

async function ConfigureGCP(f: FastifyInstance) {
  const bucket = process.env.STORAGE_BUCKET;
  if (!bucket) {
    throw new Error("STORAGE_BUCKET env-var is not defined");
  }
  f.decorate("FileStore", new GCPFileStore(bucket));
}

async function ConfigureAWS(f: FastifyInstance) {
  const client = new S3.S3Client({
    region: process.env.AWS_S3_REGION ?? "us-east-1",
    credentialDefaultProvider: defaultProvider() as any,
  });

  const bucket = process.env.S3_BUCKET;
  if (!bucket) {
    throw new Error("S3_BUCKET env-var is not defined");
  }

  f.decorate("FileStore", new S3FileStore(client, bucket));
}

async function ConfigureMinio(f: FastifyInstance) {
  const S3 = await import("@aws-sdk/client-s3");
  if (!process.env.MINIO_ENDPOINT) {
    throw new Error("MINIO_ENDPOINT env-var is not defined");
  }
  if (!process.env.MINIO_ACCESS_KEY_ID) {
    throw new Error("MINIO_ACCESS_KEY_ID env-var is not defined");
  }
  if (!process.env.MINIO_SECRET_ACCESS_KEY) {
    throw new Error("MINIO_SECRET_ACCESS_KEY env-var is not defined");
  }
  const client = new S3.S3Client({
    region: process.env.MINIO_REGION ?? "us-east-1",
    endpoint: process.env.MINIO_ENDPOINT,
    credentials: {
      accessKeyId: process.env.MINIO_ACCESS_KEY_ID,
      secretAccessKey: process.env.MINIO_SECRET_ACCESS_KEY,
    },
    forcePathStyle: true,
  });

  const bucket = process.env.MINIO_BUCKET;
  if (!bucket) {
    throw new Error("MINIO_BUCKET env-var is not defined");
  }

  f.decorate("FileStore", new S3FileStore(client, bucket));
}
