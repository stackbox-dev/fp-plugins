# @stackbox-dev/fp-plugins

Fastify plugins for Stackbox applications. Currently one plugin: **FileStore**, a
single interface over five storage backends.

## Installation

The package is published to GitHub Packages, not npmjs.org. Point the
`@stackbox-dev` scope at the GitHub registry in `.npmrc`:

```
@stackbox-dev:registry=https://npm.pkg.github.com
```

```bash
pnpm add @stackbox-dev/fp-plugins
```

Requires Node.js >= 22. Fastify 3, 4 or 5 is a peer dependency.

## FileStore

Register the plugin with a provider `type`; all other configuration comes from
environment variables.

```typescript
import { fastify } from "fastify";
import { Plugins } from "@stackbox-dev/fp-plugins";

const app = fastify();
await app.register(Plugins.FileStore, {
  type: "s3", // "local" | "gcs" | "s3" | "minio" | "azureBlob"
});
```

- The plugin decorates the instance as `app.FileStore` and augments Fastify's
  types, so `app.FileStore` is fully typed with no declaration on your side.
- Cloud SDKs load lazily: registering `type: "s3"` loads only the AWS SDK; the
  GCS and Azure SDKs stay untouched, and vice versa.
- An unknown `type` throws `Unknown storage type: ...` at registration.

### Providers

#### `local` — local filesystem

| Variable            | Required | Default                   |
| ------------------- | -------- | ------------------------- |
| `LOCAL_STORAGE_DIR` | no       | `<os tmpdir>/stackboxwms` |

For development. `getInfo` always reports `contentType: "application/octet-stream"`.

#### `s3` — AWS S3

| Variable        | Required | Default                                      |
| --------------- | -------- | -------------------------------------------- |
| `S3_BUCKET`     | yes      | —                                            |
| `AWS_S3_REGION` | no       | falls back to `AWS_REGION`, then `us-east-1` |

Set `AWS_S3_REGION` only to point S3 at a different region from the rest of the
process. Credentials come from the standard AWS chain: env vars, ECS/EC2 instance
roles, IRSA, web identity, profiles.

#### `gcs` — Google Cloud Storage

| Variable         | Required |
| ---------------- | -------- |
| `STORAGE_BUCKET` | yes      |

Credentials via Application Default Credentials: GKE Workload Identity, Compute
Engine service accounts, or `GOOGLE_APPLICATION_CREDENTIALS`.

#### `azureBlob` — Azure Blob Storage

| Variable                    | Required |
| --------------------------- | -------- |
| `AZURE_STORAGE_ACCOUNT_URL` | yes      |
| `AZURE_STORAGE_CONTAINER`   | yes      |

Credentials via `DefaultAzureCredential`: managed identity, workload identity,
service principal, CLI login.

#### `minio` — MinIO (S3-compatible, path-style addressing)

| Variable                  | Required | Default     |
| ------------------------- | -------- | ----------- |
| `MINIO_ENDPOINT`          | yes      | —           |
| `MINIO_ACCESS_KEY_ID`     | yes      | —           |
| `MINIO_SECRET_ACCESS_KEY` | yes      | —           |
| `MINIO_BUCKET`            | yes      | —           |
| `MINIO_REGION`            | no       | `us-east-1` |

### API

```typescript
interface FileStore {
  exists(filepath: string): Promise<boolean>;
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

interface FileInfo {
  size: number;
  contentType: string;
  lastModified: Date;
}
```

For a missing file, `exists` returns `false` and `getInfo` returns `null`;
`getAsBuffer` and `getAsStream` throw.

### Example

```typescript
app.post("/upload", async (request, reply) => {
  const { filepath, contentType, data } = request.body;

  await request.server.FileStore.save(filepath, contentType, data);

  const info = await request.server.FileStore.getInfo(filepath);
  if (info) {
    console.log(`size=${info.size} contentType=${info.contentType}`);
  }

  const buffer = await request.server.FileStore.getAsBuffer(filepath);
  const stream = await request.server.FileStore.getAsStream(filepath);

  return { success: true };
});
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, PR rules, and the release
process. Architecture notes live in [CLAUDE.md](CLAUDE.md).

## License

[MIT License](LICENSE)
