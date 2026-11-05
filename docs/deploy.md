# Deploy

## Run

```sh
cp example.env .env   # fill in required values
set -a; . ./.env; set +a
GOWORK=off go run ./cmd/title-storage
```

## Environment

| Variable | Meaning |
| --- | --- |
| `TITLE_STORAGE_DATABASE_URL` | Postgres database `title_storage`; the schema migrates at start |
| `TITLE_STORAGE_HTTP_ADDR` / `_GRPC_ADDR` / `_METRICS_ADDR` | `:20230` / `:20231` / `:20232` |
| `TITLE_STORAGE_CORE_ADDRESS`, `TITLE_STORAGE_CORE_INTERNAL_KEY` | the account core gRPC and the key presented there |
| `TITLE_STORAGE_INTERNAL_KEY` | what adapters (gRPC `x-internal-key`) and the erasure finaliser (`X-Internal-Key`) present |
| `TITLE_STORAGE_QUOTA_BYTES` / `_QUOTA_OBJECTS` / `_MAX_OBJECT_BYTES` | per-owner-per-title defaults (32 MiB / 256 / 4 MiB); `UpsertTitle` overrides per title. `max_object_bytes` caps uploads too |
| `TITLE_STORAGE_S3_ENDPOINT` | the blob tier's S3 as this process dials it (`http://openpak-minio:9000`); unset = no blob tier |
| `TITLE_STORAGE_S3_PUBLIC_URL` | the base consoles are given (`http://title-storage.openpak.org`, Traefik `PathPrefix(/title-storage/)` to MinIO); URLs are signed for this host. Default: the endpoint |
| `TITLE_STORAGE_S3_BUCKET` / `_PREFIX` / `_REGION` | `title-storage` / empty / `us-east-1`; the bucket is created at start when missing |
| `TITLE_STORAGE_S3_ACCESS_KEY` / `_SECRET_KEY` | a MinIO user whose policy allows only `title-storage` (shared `.env`) |
| `TITLE_STORAGE_URL_TTL` / `TITLE_STORAGE_UPLOAD_TTL` | presigned URL lifetime (15m) / how long a prepared upload waits (1h) |

Adapters that fetch bodies larger than gRPC's 4 MiB default receive limit must raise
`MaxCallRecvMsgSize` (the service accepts `max_object_bytes` + 1 MiB per call).

The website reads `TITLE_STORAGE_URL` (`http://openpak-title-storage:20230`) and
`TITLE_STORAGE_INTERNAL_KEY` from the shared `.env` for `/healthz/title-storage` (the status page)
and an account-erasure step. Both are optional there.

## Production (rootless Podman)

1. Create the database: `podman exec openpak-postgres psql -U postgres -c 'CREATE DATABASE title_storage'`.
2. Add the `TITLE_STORAGE_*` lines to the shared `~/openpak/deploy/.env`.
3. Copy `deploy/openpak-title-storage.container` to `~/.config/containers/systemd/`, run
   `~/openpak/deploy/pin-ips.sh --apply`, `systemctl --user daemon-reload`, then
   `systemctl --user start openpak-title-storage`.
4. Check inside the network: `podman exec openpak-chat wget -qO- http://openpak-title-storage:20230/healthz`
   (HTTP 200, empty body).

The HTTP port is on the container network and loopback only; never route it through Traefik.

### Blob tier (v0.3.0)

1. MinIO user and bucket (inside `openpak-minio`, root credentials from its environment):
   `mc mb local/title-storage`, a policy `title-storage-rw` allowing `s3:*` on
   `arn:aws:s3:::title-storage` and `arn:aws:s3:::title-storage/*` only, `mc admin user add` +
   `mc admin policy attach local title-storage-rw --user <access key>`.
2. `TITLE_STORAGE_S3_ACCESS_KEY` / `_SECRET_KEY` in the shared `.env`; the endpoint, public URL and
   bucket are in the quadlet (`deploy/openpak-title-storage.container`).
3. Traefik (`~/openpak/deploy/traefik/dynamic.yml`): router `title-storage-ds-web`,
   ``Host(`title-storage.openpak.org`) && PathPrefix(`/title-storage/`)`` on `web`, service
   `title-storage-ds` -> `http://openpak-minio:9000` (the `smm2-ds` pattern).

Image `ghcr.io/openpak/title-storage`, built by the tag-only release workflow.
