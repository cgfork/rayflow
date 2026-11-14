# Catalog and object storage

`crates/catalog` separates physical object metadata (`Catalog`) from logical file
names (`File`). A catalog stores `id`, `uuid`, `key`, `url`, `size` in bytes, `format`,
`uploaded` (a boolean upload acknowledgement), `created_at`, and `updated_at`. A file stores `id`, `uuid`, `fullpath`,
`filename`, `catalog_uuid`, and the same two timestamps. Multiple logical files
can reference one catalog UUID. Catalog `key` and `url` each use `VARCHAR(750)`
with separate unique constraints. These lengths keep each `utf8mb4` index within
MySQL's 3072-byte limit. A file's `fullpath` typically equals its catalog's key;
other logical paths can reference the same catalog.

Register `catalog::models()` with a Toasty database, or use `server::models()`,
which includes them. Run `./init-db.sh` to apply the migrations (8 creates these
tables; 10 adds `uploaded`; 11 changes the URL column; 12 and 13 remove deletion
timestamps; 17 adds the unique catalog key; 18 renames file `fullname` to `fullpath`). `rayflow.sql` contains
the matching schema for a fresh installation.

Migration 17 initializes existing keys from their URLs, preserving UUIDs, file
references, and timestamps. Use `update_catalog(id, data)` to assign another key
to existing metadata, including the logical full path for existing job artifacts.
Migration 18 preserves file paths, UUIDs, references, and path uniqueness.

## Metadata operations

`CatalogSystem` manages object metadata with three generic associated types:

- `CatalogId<'a>` identifies a catalog.
- `CatalogData<'a>: Catalog` supplies its object data.
- `CatalogOptionalData<'a>` supplies partial write data.

`FileSystem: CatalogSystem` adds logical files with three more associated types:

- `FileId<'a>` identifies a logical file.
- `FileData<'a>: File<CatalogId = Self::CatalogId<'a>>` supplies its file data.
- `FileOptionalData<'a>` supplies partial file write data.

Both traits share `CatalogSystem::Err`. Stores that only manage catalogs can
implement `CatalogSystem` alone. Import `CatalogSystem` for catalog methods and
`FileSystem` for file methods on concrete stores.

The MySQL implementation uses UUIDs for both IDs, owned Toasty records for
data, and the concrete `CatalogOptionalData` and `FileOptionalData<Uuid>` structs
for its input GATs. Other stores can choose their own input types, including
borrowed inputs, along with numeric or borrowed IDs and borrowed data views.
Identities implement `Id` (`Clone`, `Eq`, `Hash`, `Debug`, and `Display`).

`Catalog` exposes `key()`, `url()`, `size()`, `format()`, and `uploaded()`. `File` exposes
`fullpath()`, `filename()`, and `catalog_id()` using its associated catalog ID
type. Neither data trait exposes the object's own identity. Database IDs and
audit timestamps remain on the persistence models.
`CatalogInput` and `FileInput<I>` are convenient write payloads without their
own identities or audit fields; custom types and trait objects also work.

`CatalogOptionalData` has optional `key`, `url`, `size`, `format`, and `uploaded` fields.
`None` (including omitted or null JSON fields) leaves a stored value unchanged.
`Some(false)` and `Some(0)` explicitly write false and zero. The key is required
to locate a row. Creating a new row also requires URL, size, and format; uploaded
defaults to false. A key-only payload returns an existing ID without changing
its timestamps. Missing required fields return `Error::MissingField`.

`upsert_catalog` and `update_catalog` accept `Into<Self::CatalogOptionalData<'a>>`;
the corresponding file methods accept `Into<Self::FileOptionalData<'a>>`.
For MySQL, pass the concrete optional data directly or by reference; references
to complete `Catalog` or `File` implementations (including input structs and
owned records) supply every field.

`FileOptionalData<I>` has optional `fullpath`, `filename`, and `catalog_id`
fields with the same omission semantics. Upserts require a full path, and new
files also require filename and catalog ID. A full-path-only payload returns an
existing ID without changing its timestamps.

`CatalogSystem` supports:

- `upsert_catalog(&data)` inserts or updates by key and returns the canonical
  `CatalogId`. The MySQL store generates a UUID only for new rows.
- `get_catalog(id)` returns catalog data or `None`.
- `update_catalog(id, data)` updates supplied fields by ID, including a new key or URL.

`FileSystem` adds:

- `upsert_file(&data)` inserts or updates by full path and returns `FileId`.
- `update_file(id, data)` updates supplied fields by ID, including a new full path.
- `get_file(FileKey::Id(id))` or `get_file(FileKey::Fullpath(name))` returns
  `FileWithCatalog<FileData, CatalogData>`, or `None` if either record is missing.

Upserts preserve the ID and creation timestamp of the row that already owns
the key or full path. A new key or full path creates a new record. Changing the
URL for an existing key preserves the catalog UUID and file references. File writes
validate supplied catalog references. Timestamps are assigned by the store.
Updates return `Result<(), Err>` and do not create missing rows: MySQL returns
`CatalogNotFound` or `FileNotFound`. Empty updates preserve timestamps. Updates
do not require the key or full path; either can be supplied to rename a record,
subject to the existing unique constraints. Constraint failures remain database
errors and roll back the write.

```rust,ignore
use catalog::{CatalogInput, CatalogSystem, FileInput, FileKey, FileSystem};

let catalog_id = db.upsert_catalog(&CatalogInput::new(
    "/run-1/results.csv", "s3://artifacts/run-1/results.csv", 1024, "text/csv",
)).await?;
let file_id = db.upsert_file(&FileInput::new(
    "/run-1/results.csv", "results.csv", catalog_id,
)).await?;
let loaded = db.get_file(FileKey::Id(file_id)).await?;
```

Writes use transactions or nested savepoints and leave an outer commit or
rollback to the caller. Both stores use Toasty to select by key or full path,
update only supplied fields, or create a missing row. A concurrent creation can
hit a unique constraint. Retry the entire caller transaction with a fresh
snapshot after a creation race, deadlock, or snapshot conflict. Catalog references
are checked with a select; concurrent deletion can leave a missing reference,
which `get_file` reports as `None`.

Toasty 0.11 validates VARCHAR inserts by UTF-8 bytes. For valid Unicode strings
whose byte count exceeds the character limit, the store inserts temporary values
and replaces them with ORM updates inside the same transaction. Uniquely generated
temporary keys are never committed. Character limits and the SQL schema remain
unchanged.

`CatalogInput` includes `uploaded: bool`. Its constructor and JSON payloads that
omit the field default to `false`. Catalog upserts persist the supplied
flag, including when the size or format changes. Callers set it to `false`
before uploading and `true` after a successful upload. Metadata reads include
unacknowledged records so their status remains inspectable.

The job upload `commit` endpoint sets `uploaded: true` and publishes the logical
file in one transaction after the agent acknowledges a successful PUT. Repeating
commit is safe. Preparing another upload resets the flag, even when the metadata
is identical. Job artifact download endpoints require `uploaded: true`, including
for directory manifests and their members. The low-level `ObjectStorage` signer
only signs URLs; acknowledgement enforcement belongs to the server transfer API.

Migration 10 initializes existing catalogs to `false` too, since metadata alone
does not confirm that an upload finished. Previously stored objects must be
acknowledged before job downloads. Applications managing objects outside the job
protocol can acknowledge their upload through `CatalogSystem` after verifying it:

```rust,ignore
let data = catalog::CatalogOptionalData {
    uploaded: Some(true),
    ..Default::default()
};
db.update_catalog(catalog_id, &data).await?;
```

URLs must be absolute object locations, for example `s3://a/b/c`, `oss://a/b/c`,
`file://host/path`, or `file:///path`. Other hierarchical provider schemes are
accepted. URLs are stored as supplied; credentials and fragments are rejected.
These operations store metadata and do not access the object's bytes. `format`
is a nonempty string of up to 255 characters, such as `parquet` or `text/csv`.
`fullpath` allows 512 characters and `filename` 255. The deployed schema uses a
case-sensitive collation for names.

## Deletion

Catalogs and files have no deletion timestamp. Their keys, URLs, and full paths are
unique across all stored rows. Delete records through the persistence models:

```rust,ignore
File::filter_by_uuid(file.uuid).delete().exec(&mut db).await?;
```

Deleting a file releases its full path. Deleting a catalog releases its key and URL;
linked file records remain, but `get_file` returns `None` while their catalog is
missing. Remove or repoint linked files when deleting their catalog. Metadata
deletion does not remove bytes from object storage.

Migrations 12 and 13 preserve existing rows and remove `deleted_at`. All retained
rows become readable. Duplicate URLs or full paths across historical rows must
be resolved before upgrading; the migrations fail instead of merging identities
or discarding records.

## Presigned uploads and downloads

`ObjectStorage: FileSystem` adds:

```rust,ignore
async fn presign<'a>(
    &'a mut self,
    catalog_id: Self::CatalogId<'a>,
    operation: PresignOperation,
    expires_in: std::time::Duration,
) -> Result<PresignedUrl, Self::Err>;
```

`PresignOperation::Upload` authorizes HTTP PUT; `Download` authorizes HTTP GET.
The result contains the URL, operation, required headers, and actual expiry.
Provider adapters implement this trait using their configured endpoints and
credentials. Enable `s3` for the S3-compatible adapter below. OSS signing is not
implemented. Adapters reject unsupported schemes (including `file://`), invalid
lifetimes, and missing catalogs.

### S3-compatible storage

Enable the optional feature in a consuming workspace crate:

```toml
catalog = { workspace = true, features = ["s3"] }
```

`catalog::s3::S3<S>` wraps any `S: FileSystem`, including Toasty `Db` or
`Transaction`. It also supports `S: CatalogSystem` for catalog operations alone.
Callers that already have catalog data can use the synchronous
`presign_catalog(&data, operation, ttl)` method to avoid another lookup.
Metadata methods delegate to `S` and preserve its borrowed or
owned GAT results. Signing uses AWS SigV4 and path-style S3 URLs, mapping
`s3://artifacts/run-1/results.csv` to
`https://objects.example.com/artifacts/run-1/results.csv` with signature query
parameters. The adapter works with configured endpoints that support path-style
S3 requests and SigV4, including RustFS. Provider selection comes from the
endpoint, region, and credentials; catalog locations use the common `s3` scheme.

```rust,ignore
use std::{env, time::Duration};
use catalog::{CatalogInput, CatalogSystem, ObjectStorage, PresignOperation};
use catalog::s3::{Config, Credentials, S3};

// db already has catalog::models() registered and current migrations applied.
let credentials = Credentials::new(
    env::var("S3_ACCESS_KEY")?,
    env::var("S3_SECRET_KEY")?,
    None, // Optional session token for temporary credentials.
    None, // Optional credential expiry as std::time::SystemTime.
    "rayflow",
);
let mut config = Config::new(env::var("S3_ENDPOINT")?, credentials);
config.region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into());
let mut storage = S3::new(db, config)?;
let catalog_id = storage.upsert_catalog(&CatalogInput::new(
    "/run-1/results.csv", "s3://artifacts/run-1/results.csv", 1024, "text/csv",
)).await?;

let upload = storage.presign(
    catalog_id, PresignOperation::Upload, Duration::from_secs(900),
).await?;
// Send the bytes with HTTP PUT to upload.url, including upload.headers.
let download = storage.presign(
    catalog_id, PresignOperation::Download, Duration::from_secs(900),
).await?;
// Fetch the bytes with HTTP GET from download.url, including download.headers.
```

The environment variable names above are an application wiring example; the
adapter takes explicit configuration and does not load environment variables.
Use your provider's S3 API endpoint (for example `http://localhost:9000` for a
local RustFS instance), with the scheme, hostname and port clients will use.
The endpoint must be an HTTP(S)
origin without credentials, a path prefix, query, or fragment. The region
defaults to `us-east-1` and must match the provider configuration. Buckets must
already exist. Signing itself performs no network calls, does not check whether
an object exists, and does not upload bytes or enforce metadata size/format.

Lifetimes must be whole seconds from 1 through 604800 (seven days). A supplied
credential expiry caps the signed lifetime and `expires_at`; expired credentials
are rejected. For temporary credentials, supply both the session token and known
expiry. The caller obtains and refreshes credentials; reconstruct the adapter
with `storage.into_inner()` and a new `Config` when rotating them.

Only `s3://bucket/key` catalog locations can be signed by this adapter. Keys
are decoded once and encoded using S3 rules, preserving slashes, repeated
slashes, and dot segments. Percent-encode literal `%`, `?`, and `#` as `%25`,
`%3F`, and `%23`. Queries, fragments, URL credentials, ports in bucket names,
empty keys, and malformed percent escapes are rejected. Clients must send the
returned path unchanged; clients that normalize `.` or `..` path segments cannot
access such keys using these URLs. Metadata errors are retained in
`s3::Error::FileSystem`; missing catalogs return
`s3::Error::CatalogNotFound`.

The former `rustfs` feature and `catalog::rustfs::RustFs` API are now `s3` and
`catalog::s3::S3`. Change any stored `rustfs://bucket/key` locations to
`s3://bucket/key` before signing them with this adapter.

Validate the optional adapter with `cargo test -p catalog --features s3`.
The feature is disabled by default, so default builds do not include the AWS
signing dependencies.


## HTTP API and agent transfers

The catalog HTTP API has three POST endpoints under
`/catalog/prepare`, `/catalog/commit`, and `/catalog/download`. All request parameters
are in JSON bodies; these URLs have no path parameters.

Configure storage on the **server**, using an existing S3-compatible bucket and
an endpoint reachable from agents:

```yaml
storage:
  endpoint: http://localhost:9000
  bucket: rayflow-artifacts
  prefix: artifacts
  region: us-east-1
  access_key: deployment-access-key
  secret_key: deployment-secret-key
  presign_ttl: 15m
  # session_token: optional-temporary-token
  # credential_expiry: 2027-01-01T00:00:00Z
```

Nested overrides use `RAYFLOW_STORAGE__ENDPOINT`, `RAYFLOW_STORAGE__BUCKET`,
`RAYFLOW_STORAGE__PREFIX`, `RAYFLOW_STORAGE__ACCESS_KEY`,
`RAYFLOW_STORAGE__SECRET_KEY`, and the same convention for the other fields.
Keep credentials in deployment configuration; agents only receive temporary
signed URLs. Signatures are limited by `presign_ttl`, the remaining job claim,
and credential expiry. Uploads and downloads must finish within the job lease;
set the lease and agent `request_timeout` to accommodate large transfers.

Logical names follow:

```text
/<prefix>/<run_uuid>/<graph_name>/<version>/<node_name>/<revision>/<filename>
```

Graph and node names are encoded as single path components, preserving names
containing spaces or slashes without adding directory levels. The catalog URL
is `s3://<bucket>/<prefix>/<key>` with URL escaping applied. `version` comes from the owning `Run.version` for current transfers, and
`revision` is the producing node run's revision. Historical output lookup uses
`NodeRun.version` to find the graph and node names used by that producer.

`Filed` resolves output locations and input sources on the agent. Every request
includes `job_id`, `agent_id`, and a `key` relative to the server's configured
prefix. The key contains the run UUID, graph name/version, node name/revision,
and filename, with no leading slash:

```json
{
  "job_id": "00000000-0000-0000-0000-000000000001",
  "agent_id": "00000000-0000-0000-0000-000000000002",
  "key": "00000000-0000-0000-0000-000000000003/flow/1/producer/0/result.csv",
  "size": 1024,
  "format": "csv"
}
```

| POST endpoint | Additional fields | Result |
| --- | --- | --- |
| `/catalog/prepare` | `size`, `format` | `{catalog, presigned}` for PUT; uploaded is false |
| `/catalog/commit` | `catalog_uuid`, `size`, `format` | Published file; uploaded is true |
| `/catalog/download` | None | `{catalog, presigned}` for GET |

The agent sends prepare, uploads the bytes using the returned URL and headers,
then sends commit with the same key and metadata plus `catalog.uuid`. Prepare
upserts by catalog key, so retries reuse the catalog UUID; commit verifies the prepared
metadata and atomically acknowledges the upload and publishes its logical file.
The server trusts the agent's completed PUT; it does not HEAD the object.
The agent reports success only after all outputs are committed.

The server adds its configured prefix to the supplied relative key to form the
catalog key and file full path, both `/prefix/<key>`. It adds the bucket to form
the object URL used for signing. Catalog handlers do not load graph definitions,
interpret input/output names, or choose producer revisions. Directory members
have their own keys; the root directory uses a key ending in
`.rayflow-directory.json`.

For inputs, the agent resolves `source: "producer.output_name"` against its
pinned graph. It reads `GET /runs/{run_id}?revision=<consumer_revision>` to find
the latest effective producer at or before the consumer revision, requires
`Succeeded`, and uses the returned node's `version` to load the producer's
original graph and names. This preserves input references after reruns and
graph upgrades. The agent sends the resulting key to download, then fetches the
signed URL. Optional missing inputs are skipped; required missing inputs fail
before startup.

Each catalog request requires an active, unexpired claim owned by `agent_id`.
Keys must stay within that claim's run; path traversal and absolute keys are
rejected. Agents are responsible for declared input/output bindings within the
run. With authentication enabled, fleet credentials can call the three POST
routes and read graph definitions and individual run states. Browser writes
also require `X-Rayflow-Request: 1`. Transfers return 503 when storage is absent,
409 for a lost claim, and 404 for unavailable artifacts. Object requests use a
separate HTTP client without the fleet credential.

Deploy matching server and agent binaries: the old `/jobs/{job_id}/artifacts/*`
routes and `input`/`output`/`path` request fields have been removed.

### Directories

A `Directory` output, or an output with `multiple: true`, is a directory beneath
the output root. Its files are uploaded individually; then Filed writes and
uploads `.rayflow-directory.json` last, using format
`application/vnd.rayflow.directory+json`. Its structure is:

```json
{
  "version": 1,
  "entries": [
    { "path": "empty", "kind": "directory", "size": 0 },
    { "path": "results.csv", "kind": "file", "size": 123 }
  ]
}
```

Entries are relative to the declared directory. Nested and empty directories
are preserved. Downloads validate the manifest and check each file's byte size
against catalog metadata. Manifests are limited to 2 MiB and 10,000 entries.
Traversal, absolute paths, duplicate entries, symlinks, and reserved manifest
names inside user content are rejected. The root internal manifest is managed
by Filed and must not be used as a declared file name.

### Local data layout

Configure the agent's `data_dir` (default `.rayflow/data` when loading settings),
or `RAYFLOW_AGENT_DATA_DIR`. `FileFs` manages artifacts independently of `JobFs`:

```text
<data_dir>/<graph_run_uuid>/<node_id>/<revision>/inputs/<input.file_name>
<data_dir>/<graph_run_uuid>/<node_id>/<revision>/outputs/<output.file_name>
<data_dir>/<graph_run_uuid>/<node_id>/<revision>/.rayflow/<job_uuid>/...
```

`graph_run_uuid` is the execution UUID, not the graph definition UUID. Nodes in
one run share the run directory, while separate executions remain isolated.
Transfer checkpoints are job-specific so resumed jobs can reuse the node's data
without inheriting an earlier job's completed-upload marker. Outputs and
checkpoints survive job acknowledgement. Retain the run directory until its
downstream nodes and any planned reruns have finished; cleanup is explicit
(`FileFs::clear` for a node revision), with no automatic retention policy.

Docker mounts the node's inputs read-only at `/data/inputs` and outputs writable
at `/data/outputs`, also available as `RAYFLOW_INPUT_DIR` and
`RAYFLOW_OUTPUT_DIR`. Commands should write declared outputs there. Downstream
inputs are resolved through the catalog and staged in the consumer's input
directory, including when the producer ran on another agent.

Library users can supply separate roots through `Agentd::open_with_data_dir`
and matching `Settings.data_dir`. The convenience `Agentd::open` and
`Settings::new` use `workdir/data`; set an explicit root for deployments.
