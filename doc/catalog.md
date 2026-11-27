# Catalog and object storage

`crates/catalog` separates physical object metadata (`Catalog`) from node-run-owned
files (`File`). Catalog records store `id`, `uuid`, `key`, `url`, byte `size`,
`format`, `uploaded`, `created_at`, and `updated_at`. File records store `id`,
`uuid`, `node_run_id` (the producing node run's UUID), `filename`, `catalog_uuid`,
and the same timestamps. There is no `file.fullpath`: use the referenced
catalog's key for the path. Catalog keys and URLs have separate unique
`VARCHAR(750)` constraints. A catalog UUID identifies at most one file record.

Register `catalog::models()` or use `server::models()`. Run `./init-db.sh` before
starting updated binaries. `rayflow.sql` contains the schema for fresh installs.
Migrations 19–22 add stable node-run and configuration UUIDs, backfill file ownership, and remove
`file.fullpath`. Numeric node-run primary keys and task/job references remain
unchanged. Each new node-run revision gets a new UUID; Pend/Resume keeps the same
UUID. Run inspection exposes each node run's UUID as `state.nodes[].uuid`.

The ownership migration resolves existing files from their catalog keys and
pinned graph node names, including percent-encoded names. It preserves file,
catalog, and node-run identities and timestamps. If a legacy file has no
resolvable producer, migration 21 stops before removing `fullpath`; explicitly
map its `file.node_run_id` to the correct existing `node_run.uuid` and rerun
initialization. A duplicate catalog reference also stops migration rather than
dropping rows. Migration 20 is tracked separately so this retry is supported.
Stop server, pilot, and agents before applying these schema changes.

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
`node_run_id()`, `filename()`, and `catalog_id()` using its associated catalog ID
type. Neither data trait exposes the object's own identity. Database IDs and
audit timestamps remain on the persistence models.
`CatalogOptionalData` and `FileInput<I>` are convenient write payloads without their
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

`FileOptionalData<I>` has optional `node_run_id`, `filename`, and `catalog_id`
fields with the same omission semantics. Upserts require a catalog ID, and new
files also require filename and node-run UUID. A catalog-ID-only payload returns
an existing file ID without changing its timestamps.

`CatalogSystem` supports:

- `upsert_catalog(&data)` inserts or updates by key and returns the canonical
  `CatalogId`. The MySQL store generates a UUID only for new rows.
- `get_catalog(id)` returns catalog data or `None`.
- `update_catalog(id, data)` updates supplied fields by ID, including a new key or URL.

`FileSystem` adds:

- `upsert_file(&data)` inserts or updates by catalog ID and returns `FileId`.
- `update_file(id, data)` updates supplied fields by ID, including a new filename or catalog reference.
- `get_file(FileKey::Id(id))` or `get_file(FileKey::CatalogKey(key))` returns
  `FileWithCatalog<FileData, CatalogData>`, or `None` if either record is missing.

Upserts preserve the ID and creation timestamp of the row that already owns
the catalog key or catalog reference. A new catalog key or reference creates a new record. Changing the
URL for an existing key preserves the catalog UUID and file references. File writes
validate supplied catalog references. Timestamps are assigned by the store.
Updates return `Result<(), Err>` and do not create missing rows: MySQL returns
`CatalogNotFound` or `FileNotFound`. Empty updates preserve timestamps. Updates
do not require the catalog key or catalog reference; partial fields can rename a record,
subject to the existing unique constraints. Constraint failures remain database
errors and roll back the write.

```rust,ignore
use catalog::{CatalogOptionalData, CatalogSystem, FileInput, FileKey, FileSystem};

let catalog_id = db.upsert_catalog(&CatalogOptionalData {
    key: Some("/run-1/results.csv".into()),
    url: Some("s3://artifacts/run-1/results.csv".into()),
    size: Some(1024),
    format: Some("text/csv".into()),
    uploaded: Some(false),
}).await?;
let file_id = db.upsert_file(&FileInput::new(
    node_run.uuid, "results.csv", catalog_id,
)).await?;
let loaded = db.get_file(FileKey::Id(file_id)).await?;
```

Writes use transactions or nested savepoints and leave an outer commit or
rollback to the caller. Both stores use Toasty to select by catalog key or catalog UUID,
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

`CatalogOptionalData` includes `uploaded: bool`. JSON payloads that omit the field
default to `false`. Catalog upserts persist the supplied
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
`filename` allows 255 characters. The deployed schema uses a
case-sensitive collation for names.

## Deletion

Catalogs and files have no deletion timestamp. Catalog keys and URLs are unique across
all stored rows, as are file catalog references. Delete records through the persistence models:

```rust,ignore
File::filter_by_uuid(file.uuid).delete().exec(&mut db).await?;
```

Deleting a file releases its catalog reference. Deleting a catalog releases its key and URL;
linked file records remain, but `get_file` returns `None` while their catalog is
missing. Remove or repoint linked files when deleting their catalog. Metadata
deletion does not remove bytes from object storage.

Migrations 12 and 13 preserve existing rows and remove `deleted_at`. All retained
rows become readable. Duplicate URLs or full paths across historical rows must
be resolved before upgrading; the migrations fail instead of merging identities
or discarding records.

The server constructs job-specific keys from the claimed job and filename. Directory
manifest types live in `catalog::directory`, and job artifact request/response
types live in `server::api::job`. The catalog crate contains metadata,
provider signing contracts, and generic relative-path validation in
`catalog::path`; it does not depend on job transfer payloads.

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
The same trait provides synchronous `presign_catalog` for already-loaded data
and `supports_multipart`. Loaded-record signing performs no persistence I/O, so
upload preparation can sign uncommitted catalog data without moving lookups
outside its transaction. Server handlers use only this contract; provider
construction is confined to storage configuration.

`PresignOperation` carries all signing parameters. In addition to `Upload`,
`Download`, and `CreateMultipart`, its variants include:

```rust,ignore
UploadPart { upload_id: String, part_number: u32 },
CompleteMultipart { upload_id: String },
AbortMultipart { upload_id: String },
```

The same signing methods handle whole objects and whole-object operations. The
server selects this enum from the whole-object endpoint and its request fields;
HTTP payload types are not part of `ObjectStorage`. Whole-object operation JSON remains
`"upload"` or `"download"`; a signed part operation includes its parameters, for
example `{"upload_part":{"upload_id":"...","part_number":1}}`. Agents verify
that the signed response matches the requested upload ID and part number.

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
use catalog::{CatalogOptionalData, CatalogSystem, ObjectStorage, PresignOperation};
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
let catalog_id = storage.upsert_catalog(&CatalogOptionalData {
    key: Some("/run-1/results.csv".into()),
    url: Some("s3://artifacts/run-1/results.csv".into()),
    size: Some(1024),
    format: Some("text/csv".into()),
    uploaded: Some(false),
}).await?;

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

### S3 CORS requirements

When the console and object storage have different origins, configure CORS on
Rayflow's storage bucket. The endpoint in each presigned URL must be reachable
from the browser. Set `AllowedOrigins` to the console origins as sent by the
browser, including the scheme, host, and any non-default port.

| Console operation | `AllowedMethods` | `AllowedHeaders` |
| --- | --- | --- |
| Read `result.json` and `usage.json` in Runtime | `GET` | Omit for the current presigned archive requests. |
| Upload a file in Assets | `PUT` | `content-type` and any additional headers listed in the browser's `Access-Control-Request-Headers`. |

The browser sends an `OPTIONS` preflight before the upload. S3 matches the
requested `PUT` method and must allow the origin and requested headers. The
actual `GET` and `PUT` responses must also include `Access-Control-Allow-Origin`
for the console origin.

[rustfs-cors.json](../rustfs-cors.json) records the rules for the
`rayflow-artifacts` bucket. To apply them from the repository root, use an AWS
CLI identity with `s3:PutBucketCORS` permission.

```sh
aws --endpoint-url http://10.10.2.199:9000 --region us-east-1 \
  s3api put-bucket-cors --bucket rayflow-artifacts \
  --cors-configuration file://rustfs-cors.json
```

`put-bucket-cors` replaces the bucket's rule set. When adapting the example,
update the console origins and include any rules other clients still need.

## Shared assets

Migration 27 adds one `asset` row per named version. A ready row points to a
verified, private object through `catalog_uuid`. Migration 28 tracks private
copies while publication is in progress so the server can clean up unused
copies after a failed commit or restart. Nodes pin the asset version
UUID in `assets` and choose a relative mount filename:

```json
{
  "assets": [{
    "asset_id": "f27e2c15-e49b-49bf-85cf-f646ed55cfea",
    "file_name": "scripts/transform.lua"
  }]
}
```

Asset references can be used on root nodes. A published graph pins the version,
so editing a draft to use another version affects jobs on the new graph.

| Endpoint | Operation |
| --- | --- |
| `GET /api/v1/assets` | List published asset versions. |
| `GET /api/v1/assets/{asset_id}` | Read an exact published version. |
| `POST /api/v1/assets/prepare` | Reserve an upload with `name`, `version`, `size`, `format`, and hex `sha256`. |
| `POST /api/v1/assets/{asset_id}/commit` | Verify and publish the staged object selected by `upload_id`. |
| `POST /api/v1/assets/{name}/versions/{version}/cancel` | Discard a pending version reserved by the calling user. |
| `POST /api/v1/assets/{name}/versions/{version}/import` | Publish an existing object using `source_key`, `format`, and hex `sha256`. |
| `POST /api/v1/assets/{asset_id}/download` | Presign a named attachment for the published object. |
| `POST /api/v1/job/asset/presign` | Presign a job asset selected by `job_id`, `agent_id`, and declared `filename`. |

The browser sends `PUT` directly to the URL from a pending prepare response
using the returned headers and the selected file as the body. The signature
binds the declared file size to the browser's `Content-Length`. Configure the
bucket according to the [S3 CORS requirements](#s3-cors-requirements). On commit
Rayflow checks the staging object's size, copies it to a private key, and
verifies the copied size and SHA-256 before publishing. Matching retries reuse
the reservation or return the published version. After an upload attempt,
**Discard pending upload** releases the calling user's reservation and lets
them publish different bytes for that version. Configure storage
lifecycle expiry for objects under the `asset-staging` prefix and retain
published objects for reruns.

Imports are available when `storage.asset_import_prefix` is configured.
The source key is relative to that prefix, which must be disjoint from
`storage.prefix`. The agent's endpoint checks the active claim and the
consuming node's pinned graph version before granting a download.

## Job artifact API

Agents use job-scoped endpoints. The server constructs catalog keys from the
claimed job, its pinned node run, revision, and the supplied relative filename.
Agents do not submit storage keys.

| Endpoint | Request | Result |
| --- | --- | --- |
| `POST /api/v1/job/result/prepare` | `job_id`, `agent_id`, `size` | Result catalog UUID, catalog metadata, and a presigned upload URL |
| `POST /api/v1/job/result/commit` | `job_id`, `agent_id`, `catalog_uuid` | Marks the job's prepared result catalog uploaded |
| `POST /api/v1/job/output/prepare` | `job_id`, `agent_id`, `filename`, `size`, `format` | Catalog UUID and ordinary upload signature |
| `POST /api/v1/job/output/commit` | `job_id`, `agent_id`, `filename`, `catalog_uuid` | Marks the catalog uploaded and publishes the file |
| `POST /api/v1/job/output/multipart/create` | `job_id`, `agent_id`, `filename`, `size`, `format` | Creates the catalog and returns a multipart-create signature |
| `POST /api/v1/job/output/multipart/upload` | `job_id`, `agent_id`, `filename`, `catalog_uuid`, `upload_id`, `part_number` | Returns an upload-part signature |
| `POST /api/v1/job/output/multipart/complete` | `job_id`, `agent_id`, `filename`, `catalog_uuid`, `upload_id` | Returns a multipart-complete signature |
| `POST /api/v1/job/output/multipart/abort` | `job_id`, `agent_id`, `filename`, `catalog_uuid`, `upload_id` | Returns a multipart-abort signature |
| `POST /api/v1/job/input/presign` | `job_id`, `agent_id`, `filename` | Download signature and catalog metadata |

The server validates the active job claim, checks the declared input or output,
and derives the producer node run for input downloads. Upload state and transfer
markers are persisted in the job filesystem. Files larger than the agent's
`multipart_threshold` setting use multipart upload and ranged GET requests;
smaller files use ordinary presigned HTTP PUT and GET requests. The default
threshold is 64 MiB. Multipart
completion must succeed before `/job/output/commit` marks the catalog uploaded.

The agent uploads `result.json` and `usage.json` as `application/json` at
`/<prefix>/jobs/<job_uuid>/<filename>`. Their catalog associations are keyed by
job and filename in `job_artifact`, independently of declared outputs. Apply
migration 24 and upgrade the server before upgrading agents. Existing result
rows remain associated with `result.json`. Both `/job/result/prepare` and
`/job/result/commit` accept `filename`, which defaults to `result.json` for older
agents. The assigned agent may use either filename after the lease expires or
the scheduler clears active ownership.
For a terminal job with a cleared claim and no recorded result owner, prepare
returns `409 RAYFLOW_JOB_RESULT_OWNER_UNKNOWN`. The agent retains local files
whose uploads lack a receipt.

Prepare and commit are idempotent per filename. Repeated preparation returns
the same catalog and requires the same byte size. If `catalog.uploaded` is
already true, the agent skips PUT and commit and the returned signature permits
download. Otherwise the agent uploads the exact local bytes using the returned
URL and headers and commits the catalog UUID after a successful 2xx PUT.
The archive commit must return a 2xx response with the expected catalog UUID.
The agent validates and commits required outputs before reporting success.
It retries the JSON uploads independently and keeps both files across restarts.
A server without object storage returns `RAYFLOW_STORAGE_UNAVAILABLE`. Pending
output uploads delay completion, and pending JSON uploads delay local cleanup.

### Local data layout

`JobManager` stores metadata and artifact data under separate configured roots. Both
roots are scoped by the job UUID; artifact data is isolated per job:

```text
<metadata_dir>/<job_uuid>/job.json
<metadata_dir>/<job_uuid>/lock
<metadata_dir>/<job_uuid>/container.json
<metadata_dir>/<job_uuid>/result.json
<metadata_dir>/<job_uuid>/usage.json
<metadata_dir>/<job_uuid>/completion.json
<metadata_dir>/<job_uuid>/result.json.upload
<metadata_dir>/<job_uuid>/usage.json.upload
<metadata_dir>/<job_uuid>/pid
<metadata_dir>/<job_uuid>/runtime/started_at
<metadata_dir>/<job_uuid>/graph.json
<data_dir>/<job_uuid>/inputs/<filename>
<data_dir>/<job_uuid>/asset/<filename>
<data_dir>/<job_uuid>/outputs/<filename>
<data_dir>/<job_uuid>/.rayflow/<job_uuid>/inputs-ready.json
<data_dir>/<job_uuid>/.rayflow/<job_uuid>/assets-ready.json
<data_dir>/<job_uuid>/.rayflow/<job_uuid>/outputs-uploaded.json
```

`metadata_dir` is the configured agent `workdir`; `data_dir` is configured
separately (the defaults are `.rayflow/jobs` and `.rayflow/data`). Docker mounts
the read-only job metadata directory at `/rayflow`, including `job.json` and
the pinned `graph.json`; `RAYFLOW_JOB_FILE` and `RAYFLOW_GRAPH_FILE` point to
those files. The agent keeps input readiness, output completion, and transfer
state in the per-job marker directory. Local artifacts stay available until the
server acknowledges the outcome and both JSON archives have upload receipts.
Docker mounts inputs and assets read-only at `/data/inputs` and `/data/assets`
and outputs writable at `/data/outputs`. The asset directory is empty for
nodes with no declared assets. The agent checks asset digests before creation
and sets `RAYFLOW_ASSET_DIR` to the container path.
