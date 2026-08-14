import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as stream from "node:stream";
import type * as S3Mod from "@aws-sdk/client-s3";
import type * as AzureBlobMod from "@azure/storage-blob";
import type * as GcsMod from "@google-cloud/storage";
import type { BlobServiceClient, ContainerClient } from "@azure/storage-blob";
import { FastifyInstance, FastifyPluginAsync } from "fastify";
import fp from "fastify-plugin";
import { streamToBuffer } from "./utils";

// The cloud SDKs are require()d by the provider that needs them rather than imported
// at module scope. A deployment only ever uses one backend, but loading all three
// costs ~360ms and ~31MB of heap on every boot (GCS alone is ~123ms / ~13.6MB).
//
// Each holder is assigned by its load* function before any class that reads it can be
// constructed — every FileStore implementation is private to this module and reachable
// only through the matching Configure* entry point below.
let S3!: typeof S3Mod;
let Upload!: typeof import("@aws-sdk/lib-storage").Upload;
let AzureBlob!: typeof AzureBlobMod;
let AzureIden!: typeof import("@azure/identity");
let Gcs!: typeof GcsMod;

// Plain assignment rather than ??=: node memoises require(), so re-assigning on a
// second call is free and keeps these branchless.
function loadAWS(): void {
  S3 = require("@aws-sdk/client-s3");
  Upload = require("@aws-sdk/lib-storage").Upload;
}

function loadAzure(): void {
  AzureBlob = require("@azure/storage-blob");
  AzureIden = require("@azure/identity");
}

function loadGCP(): void {
  Gcs = require("@google-cloud/storage");
}

export interface FileInfo {
  size: number;
  contentType: string;
  lastModified: Date;
}

export interface FileStore {
  exists(filepath: string): Promise<boolean>;
  // return null if file does not exist
  getInfo(filepath: string): Promise<FileInfo | null>;
  save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void>;
  getAsBuffer(filepath: string): Promise<Buffer>;
  getAsStream(filepath: string): Promise<NodeJS.ReadableStream>;
  copyFromStream(
    filepath: string,
    contentType: string,
    stream: stream.Readable,
  ): Promise<void>;
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
    try {
      await fs.promises.stat(p);
      return true;
    } catch (err: any) {
      if (err.code === "ENOENT") {
        return false;
      }
      throw err;
    }
  }
  async getInfo(filepath: string): Promise<FileInfo | null> {
    try {
      const p = path.join(this.dir, filepath);
      const stat = await fs.promises.stat(p);
      return {
        size: stat.size,
        contentType: "application/octet-stream",
        lastModified: stat.mtime,
      };
    } catch (err: any) {
      if (err.code === "ENOENT") {
        return null;
      }
      throw err;
    }
  }

  async save(
    filepath: string,
    _contentType: string,
    data: string | Buffer,
  ): Promise<void> {
    const p = path.join(this.dir, filepath);
    await fs.promises.mkdir(path.dirname(p), { recursive: true });
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
  async copyFromStream(
    filepath: string,
    _contentType: string,
    rs: NodeJS.ReadableStream,
  ): Promise<void> {
    const pth = path.join(this.dir, filepath);
    await fs.promises.mkdir(path.dirname(pth), {
      recursive: true,
    });
    await stream.promises.pipeline(rs, fs.createWriteStream(pth));
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
  async copyFromStream(
    filepath: string,
    contentType: string,
    rs: stream.Readable,
  ): Promise<void> {
    const blob = this.client.getBlockBlobClient(filepath);
    const resp = await blob.uploadStream(rs, undefined, undefined, {
      blobHTTPHeaders: {
        blobContentType: contentType,
      },
    });
    if (resp.errorCode) {
      throw new Error(resp.errorCode);
    }
  }

  async getInfo(filepath: string): Promise<FileInfo | null> {
    try {
      const blob = this.client.getBlobClient(filepath);
      const properties = await blob.getProperties();

      return {
        size: properties.contentLength || 0,
        contentType: properties.contentType || "application/octet-stream",
        lastModified: properties.lastModified || new Date(0),
      };
    } catch (err: any) {
      if (err.statusCode === 404) {
        return null;
      }
      throw err;
    }
  }
}

class GCPFileStore implements FileStore {
  private storage: GcsMod.Storage;

  constructor(private bucket: string) {
    this.storage = new Gcs.Storage();
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

  async copyFromStream(
    filepath: string,
    contentType: string,
    rs: NodeJS.ReadableStream,
  ): Promise<void> {
    const gcsfile = this.storage.bucket(this.bucket).file(filepath);
    await stream.promises.pipeline(
      rs,
      gcsfile.createWriteStream({
        resumable: false,
        contentType,
      }),
    );
  }

  async getInfo(filepath: string): Promise<FileInfo | null> {
    try {
      const gcsfile = this.storage.bucket(this.bucket).file(filepath);
      const [metadata] = await gcsfile.getMetadata();

      return {
        size:
          typeof metadata.size === "number"
            ? metadata.size
            : parseInt(metadata.size ?? "0", 10) || 0,
        contentType: metadata.contentType || "application/octet-stream",
        lastModified: metadata.updated
          ? new Date(metadata.updated)
          : new Date(0),
      };
    } catch (err: any) {
      if (err.code === 404) {
        return null;
      }
      throw err;
    }
  }
}

class S3FileStore implements FileStore {
  constructor(
    private client: S3Mod.S3Client,
    private bucket: string,
  ) {}

  async exists(filepath: string): Promise<boolean> {
    try {
      await this.client.send(
        new S3.HeadObjectCommand({ Bucket: this.bucket, Key: filepath }),
      );
      return true;
    } catch (err: any) {
      // HeadObject reports a missing key as NotFound; NoSuchKey is modelled on GetObject
      // and never reaches here. The status check stays as the belt-and-braces path.
      if (err instanceof S3.NotFound) {
        return false;
      }
      if (err["$metadata"]?.httpStatusCode === 404) {
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
    const upload = new Upload({
      client: this.client,
      params: {
        Bucket: this.bucket,
        Key: filepath,
        Body: fs.createReadStream(localFilepath),
        ContentType: contentType,
      },
    });
    await upload.done();
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

  async copyFromStream(
    filepath: string,
    contentType: string,
    rs: stream.Readable,
  ): Promise<void> {
    const upload = new Upload({
      client: this.client,
      params: {
        Bucket: this.bucket,
        Key: filepath,
        Body: rs,
        ContentType: contentType,
      },
    });
    await upload.done();
  }

  async getInfo(filepath: string): Promise<FileInfo | null> {
    try {
      const data = await this.client.send(
        new S3.HeadObjectCommand({
          Bucket: this.bucket,
          Key: filepath,
        }),
      );

      return {
        size: data.ContentLength || 0,
        contentType: data.ContentType || "application/octet-stream",
        // epoch, not now — an unknown timestamp must not read as "just modified",
        // matching AzureFileStore and GCPFileStore.
        lastModified: data.LastModified || new Date(0),
      };
    } catch (err: any) {
      if (
        err instanceof S3.NotFound ||
        err["$metadata"]?.httpStatusCode === 404
      ) {
        return null;
      }
      throw err;
    }
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
  loadAzure();
  const accountClient = new AzureBlob.BlobServiceClient(
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
  loadGCP();
  f.decorate("FileStore", new GCPFileStore(bucket));
}

async function ConfigureAWS(f: FastifyInstance) {
  const bucket = process.env.S3_BUCKET;
  if (!bucket) {
    throw new Error("S3_BUCKET env-var is not defined");
  }

  loadAWS();
  const client = new S3.S3Client({
    // AWS_REGION is what every other AWS tool in the environment sets. Passing a region
    // unconditionally skips the SDK's own resolution chain, so without this fallback a
    // pod configured the conventional way would silently talk to us-east-1.
    region: process.env.AWS_S3_REGION ?? process.env.AWS_REGION ?? "us-east-1",
    credentialDefaultProvider: require("@aws-sdk/credential-provider-node")
      .defaultProvider,
  });

  f.decorate("FileStore", new S3FileStore(client, bucket));
}

async function ConfigureMinio(f: FastifyInstance) {
  if (!process.env.MINIO_ENDPOINT) {
    throw new Error("MINIO_ENDPOINT env-var is not defined");
  }
  if (!process.env.MINIO_ACCESS_KEY_ID) {
    throw new Error("MINIO_ACCESS_KEY_ID env-var is not defined");
  }
  if (!process.env.MINIO_SECRET_ACCESS_KEY) {
    throw new Error("MINIO_SECRET_ACCESS_KEY env-var is not defined");
  }
  loadAWS();
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
