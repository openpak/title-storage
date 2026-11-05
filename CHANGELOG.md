# Changelog

## v0.3.0 — 2026-11-05

- Blob tier (board openpak/prds#134, gap D1 of nex `docs/core-backed-storage.md`): `PrepareUpload`
  (write rule, `max_object_bytes` and quota checked up front, object id reserved, presigned PUT or
  S3 POST form pinned to the exact size) and `CompleteUpload` (size and optional SHA-256 verified
  against the bucket, then the object appears or replaces its namesake). Blob-backed objects carry
  `get_url` (presigned GET, 15 min) and `blob`/`sha256`; their `body` is empty. Small objects stay
  inline, existing calls are unchanged.
- Pending uploads reserve quota, expire after an hour, and their bytes are deleted. Delete,
  replacement, inline overwrite, expiry and account erasure delete the blob (a `blob_trash` row in
  the same transaction, deleted at once, retried by the janitor).
- Account erasure also deletes pending uploads and every blob; the answer is
  `{"deleted": rows, "blobs": n}`.
- `TITLE_STORAGE_S3_*`, `TITLE_STORAGE_URL_TTL`, `TITLE_STORAGE_UPLOAD_TTL`; schema adds
  `objects.blob_key`, `objects.sha256`, `uploads`, `blob_trash` (additive, migrated at start).
- Tests: the whole flow against an in-process fake S3 (and a real S3 when
  `TITLE_STORAGE_TEST_S3_ENDPOINT` is set).

## v0.2.0 — 2026-11-03
- Per-request logging: one slog line per HTTP request (method, path, status, ms) and per gRPC unary call (method, code, ms) on stderr via internal/httplog. Query strings, headers and bodies are never logged; /healthz and /metrics are skipped.

## v0.1.2 — 2026-11-03

- Adapter guide: gRPC message-size note (raise MaxCallRecvMsgSize or page lists).

## v0.1.1

- `GetObject` with an empty owner finds the newest object of a collection and name under any owner
  (viewer rules still apply), for upload references that are unguessable by construction.

## v0.1.0

- Neutral title storage core: title keys across networks, int64 variables with the Title User
  Storage operations (set, add, try-set with operator, write conditions, multi-slot, multi-owner and
  friend reads), objects (info, JSON metadata filter, stable ids, expiry, collections),
  private / friends / public visibility from the account core's friend graph, per-owner quotas per
  title, per-title network opt-out, account erasure.
- gRPC `openpak.titlestorage.v1.TitleStorage` on 20231; HTTP health and erasure on 20230.
