# Changelog

## v0.6.0 — 2026-11-05

- Recipients (board openpak/prds#135, gap D2 of nex `docs/core-backed-storage.md`): visibility
  `VISIBILITY_RECIPIENTS` for objects, with `recipients` (account ids, at most 256) and
  `recipients_include_friends` on `PutObjectRequest`, `PrepareUploadRequest` and `Object`. Readable
  by the owner, the named accounts and, with the flag, the owner's friends; the same rule applies to
  `GetObject`, `GetObjectByID`, `ListObjects` (every scope), `RateObject` and `GetRatings`. A write
  that sets a visibility replaces the list; one that leaves it unspecified keeps it.
  `ListObjects.received` keeps objects that name the viewer (NEX `SearchTypeReceived`). Variables
  and title defaults refuse the new visibility. Existing visibilities answer as before.
- Friend lists from the account core are cached per account for `TITLE_STORAGE_FRIENDS_CACHE_TTL`
  (default 2 minutes).
- Account erasure also takes the account out of every other object's and pending upload's
  recipient list; the answer adds `"recipient_of"`.
- Schema: `objects.recipients`, `objects.recipients_friends`, the same on `uploads`, GIN indexes,
  and the objects visibility check widened to `objects_visibility_v2` (additive, migrated at start).

## v0.5.0 — 2026-11-05

- Ratings (board openpak/prds#137, gap D4 of nex `docs/core-backed-storage.md`): `InitRating`,
  `RateObject`, `GetRatings`, `ResetRatings`. A slot (0-255) per object holds a `RatingPolicy`
  (initial value, inclusive range, lock `none` / `interval` / `period` / `permanent`, modifiable,
  round-minus, no self-rating), a running total and a count; one row per (object, slot, rater)
  account holds the rater's contribution and last rating time. A locked rater gets `locked=true`
  and the lock's expiry. Objects deleted (or expired) take their ratings with them.
- `ListObjects`: `sort "rating:<slot>"` / `"rating_avg:<slot>"` (slot-less objects last),
  `min_rating_count`, `include_ratings`; `GetObjectByID`: `include_ratings`.
- Account erasure also deletes the ratings the account made (subtracting them from each slot's
  total and count) and the ratings others made on its objects; the answer adds
  `"ratings_made"` and `"ratings_received"`.
- Schema: `rating_slots`, `ratings` (additive, migrated at start).

## v0.4.0 — 2026-11-05

- Search (board openpak/prds#136, gap D3 of nex `docs/core-backed-storage.md`): `ListObjects`
  gains `all_owners` (every owner, visibility still applied), `exclude_owners`, `collections`,
  `created_after_ms`/`created_before_ms`, `updated_after_ms`/`updated_before_ms` (inclusive) and
  `meta_range_json` (inclusive numeric `ge`/`le` on top-level metadata keys, exact decimals, bound
  parameters only). `sort` recognises `rating:<slot>` / `rating_avg:<slot>` and refuses them with
  `InvalidArgument` until the ratings tier (v0.5.0). Additive: existing requests answer as before.
- Schema: index `objects_created (title_key, collection, created)` (migrated at start).

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
