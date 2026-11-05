# Protocol: title storage

| Client | Surface |
| --- | --- |
| console adapters (`np-tus`, `np-uploader`, later `wfc-sake`, `nex`, `xb-stfd`, `npln`) | gRPC `openpak.titlestorage.v1.TitleStorage` on 20231, `x-internal-key` metadata |
| account-erasure finaliser | `DELETE /internal/accounts/{account}/data` with `X-Internal-Key` on 20230 |
| status page (through the website) | `GET /healthz` on 20230 |

## Naming a title

Every gRPC request carries `TitleRef{network, title}`:

* `network` is the caller's account-core link namespace: `np`, `x360`, `wiiu`, `switch`, `wfc`, `xbox`.
* `title` is the title as that network knows it. `title_keys` maps `(network, title)` to the
  neutral key; an unmapped id creates the title `<network>:<id>` on first write, a read of an
  unwritten title is empty. With `network` empty, `title` is the neutral key itself (a new neutral
  key is created by a write or `UpsertTitle`): `np-messages` is one.

Operators join networks with `MapTitle(title, name, network, native_id)`: one neutral title, one
mapping per network. A mapping whose title already holds rows refuses to move. Seeded:
`terraria` = PS3 `NPWR04270_00`.

## Owners, viewers and writers

* An owner is a core account id (UUID, case-insensitive) or a virtual user `v:<name>`
  (`[A-Za-z0-9_.-]{1,64}`), a title-wide slot set that every account reads and writes (PS3 virtual
  users, a title's global counters).
* A **writer** (account id) must be the owner, or the owner is virtual, or the account core's
  `CheckInteraction(writer, owner, DISCOVER)` allows it (no block either way). Otherwise
  `PermissionDenied`. The `setter` string is whatever the adapter wants remembered with the row
  (the PS3 JID); the service stores it as given and compares it for `expect_setter`.
* A **viewer** (account id, or empty for an anonymous read) sees: its own rows; virtual owners'
  rows; `public` rows; `friends` rows of its friends. Unreadable rows are left out of lists and
  answer "not found" on a single get, so a private row cannot be probed.
* Every row has a visibility; a write that names none keeps the stored one, a new row takes the
  title's default (`private` until `UpsertTitle` says otherwise).

## Variables

| Call | Meaning |
| --- | --- |
| `SetVariables(ref, owner, writer, setter, items[{slot,value}], visibility)` | upsert slots, one transaction per batch (max 1024) |
| `ListVariables(ref, viewer, owners[] or friends_of_viewer [+include_self], slots[], sort, ascending, offset, limit)` | multi-slot, multi-user and friend reads in one call; `hits` counts matches before paging; `sort` is `updated` (default), `value` or `slot`, newest/largest first unless `ascending`; limit default 100, max 1000 |
| `AddVariable(..., slot, delta, condition)` | atomic add; a missing slot counts as 0; overflow is `InvalidArgument` |
| `TrySetVariable(..., slot, value, op, operand, condition)` | stores `value` when `operand <op> current` holds (`eq ne gt ge lt le`, missing = 0). Not stored is not an error: `stored=false`, the unchanged `variable`, `previous_value` |
| `DeleteVariables(ref, owner, writer, slots[])` | |

`Condition{expect_updated_unix_ms, expect_setter}` is the TUS comp-date / comp-setter: the write
happens only if the existing row was last changed at that time by that setter; a missing row fails
it (`condition_failed=true`, nothing written). Times are stored at 10 ms resolution so a console
that prints centiseconds can echo them back and match.

## Objects

| Call | Meaning |
| --- | --- |
| `PutObject(ref, owner, writer, setter, collection, name, info, meta_json, body, visibility, ttl_seconds, condition)` | create or replace. `collection` defaults to `default`; an empty `name` is named by the new id. The numeric `id` is assigned once and survives replacement. `ttl_seconds` sets an expiry (0 keeps the current one). `Condition` is checked against the existing object |
| `GetObject(ref, viewer, owner, collection, name, include_body)` | `found=false` for missing, expired or unreadable. With `owner` empty it finds the newest object of that collection and name under any owner (for upload references that are unguessable by construction; the viewer rules still apply) |
| `GetObjectByID(network, viewer, id, include_body)` | the same by global id |
| `ListObjects(ref, viewer, owners[] or friends_of_viewer [+include_self] or all_owners, exclude_owners[], collection, collections[], names[], meta_filter_json, meta_range_json, created_after_ms, created_before_ms, updated_after_ms, updated_before_ms, sort, ascending, offset, limit, include_body)` | the search; see below |
| `DeleteObjects(ref, owner, writer, collection, names[])` | empty `names` = the whole collection |
| `Usage(ref, owner)` | bytes, objects, variables and the limits that apply |

### Searching objects (`ListObjects`)

Every filter is optional and they combine with AND; `hits` is the size of the whole match before
`offset`/`limit` (default 100, max 1000).

| Field | Meaning |
| --- | --- |
| scope | exactly one of `owners[]` (max 1000), `friends_of_viewer` (+ `include_self`), or `all_owners` (every owner in the title). The viewer rules apply to every scope: with `all_owners` an anonymous viewer sees public rows and virtual owners only, a signed-in one also its own and its friends' `friends` rows |
| `exclude_owners[]` | owners left out of the scope (NEX `SearchTargetAnybodyExcludeSpecified`) |
| `collection`, `collections[]` | one collection, or any of a list (NEX `DataTypes`); both given = any of them |
| `names[]` | exact names |
| `meta_filter_json` | metadata contains this JSON (jsonb `@>`): equality on keys, membership in arrays (`{"tags":["kart"]}`) |
| `meta_range_json` | inclusive numeric ranges on top-level metadata keys: `{"data_type":{"ge":1,"le":5},"plays":{"ge":10}}`. Only `ge` and `le`; a key with `{}` only requires a number. An object whose key is missing or not a JSON number does not match. Bounds compare as exact decimals (int64 values are safe). At most 8 keys, keys `[A-Za-z0-9_.-]{1,64}`; key and bounds are bound parameters, never SQL text |
| `created_after_ms`, `created_before_ms`, `updated_after_ms`, `updated_before_ms` | inclusive Unix-ms bounds; 0 = unbounded |
| `sort` | `updated` (default), `created`, `size`, `name`, `id`, newest/largest first unless `ascending`; ties by id. `rating:<slot>` sorts by that rating slot's total, `rating_avg:<slot>` by its average (`(total - initial_value) / count`); objects without the slot (or, for the average, without ratings) come last in both directions (v0.5.0) |
| `min_rating_count` | with a rating sort: only objects whose sorted slot has at least this many ratings (NEX `MinimalRatingFrequency`) |
| `include_ratings` | every result carries its rating slots (`Object.ratings`), as `GetObjectByID` can |

Mapping from NEX `DataStoreSearchParam`: `OwnerType` anybody / friends / anybody-except =
`all_owners` / `friends_of_viewer` / `all_owners` + `exclude_owners`; `OwnerIDs` = `owners`;
`DataType`/`DataTypes` = `collection`/`collections` (or a `data_type` metadata range);
`CreatedAfter/Before`, `UpdatedAfter/Before` = the time bounds; `Tags` = `meta_filter_json`
`{"tags":[...]}`; `ResultOrderColumn`/`ResultOrder` = `sort`/`ascending`; `ResultRange` =
`offset`/`limit`.

## Ratings

Numeric rating slots on an object (NEX DataStore ratings, stars, likes), 0-255 per object. A slot
holds its policy, a running `total` (starting at `initial_value`) and a `count`; each rater has
one row per (object, slot) with what it contributed (`rater_value`, `rater_count`) and when it last
rated.

| Call | Meaning |
| --- | --- |
| `InitRating(network, id, writer, slot, policy)` | creates the slot; one that exists is returned as it is (the first policy wins). The writer follows the write rule for the object's owner. `found=false`: no such object |
| `RateObject(network, id, slot, rater, value, policy)` | the rater (an account id, never a virtual owner) must be able to read the object (`found=false` otherwise, as a get). A missing slot is created with `policy`; without one it is `InvalidArgument`. Answers the slot after the call with the rater's row |
| `GetRatings(network, viewer, ids[], slots[], rater)` | the slots of the objects the viewer can read (max 1000 ids; empty `slots` = all), by id then slot; with `rater`, that account's row and lock |
| `ResetRatings(network, id, writer, slots[])` | slots back to `initial_value`, count 0, every rater's row deleted; answers the rows deleted |

`RatingPolicy`:

| Field | Meaning |
| --- | --- |
| `initial_value` | where `total` starts |
| `lock` | `none` (default): any number of ratings; `interval`: one per `interval_seconds`; `period`: one until the next boundary, `period_hour` (0-23 UTC) on `period_day` (-17 = the 1st of the next month, -1..-7 = the next Monday..Sunday after the rating day, never that day itself); `permanent`: one per rater, ever |
| `range_min`, `range_max` | set = enforced: a value outside is `InvalidArgument` |
| `round_minus` | a negative value counts as 0 (before the range check) |
| `modifiable` | a rater's repeat replaces its value (the total moves by the difference, the count stays) instead of adding a rating |
| `disable_self_rating` | the object's owner rating it is `PermissionDenied` |

A locked rater is not an error: `locked=true`, nothing changed, and `lock_expires_unix_ms` says
until when (-1 = never). One `RateObject` is one transaction under the slot's row lock, so
concurrent ratings add up exactly; an int64 overflow is `InvalidArgument`. Deleting, replacing by
a new id, or expiring an object deletes its slots and every rating on them (foreign keys,
`ON DELETE CASCADE`); a replacement that keeps the id keeps its ratings.

## Blob tier (large bodies)

A body that should not travel inline over gRPC (a video, a course with replays) goes straight from
the console to S3 (MinIO on the box, bucket `title-storage`) with a presigned request. The object
row stays the index; small objects keep their inline `body` exactly as before.

| Call | Meaning |
| --- | --- |
| `PrepareUpload(ref, owner, writer, setter, collection, name, size, sha256, info, meta_json, visibility, ttl_seconds, post_form)` | the write rule, `size <= max_object_bytes` and the quota are checked now; the object `id` is reserved (the existing object's id when `name` already exists); returns `{id, method, put_url, headers, form_fields, expires_unix_ms}`. `post_form=false`: a presigned `PUT` (send `headers`); `true`: an S3 POST policy pinned to the key and the exact size (`form_fields` then the file field `file`), as NEX DataStore consoles upload |
| `CompleteUpload(id, writer)` | checks the stored size equals `size` (and the SHA-256 when one was given), then the object appears (or replaces the one of that name) with `blob=true`, `size`, `sha256`. A mismatch or a missing body is `FailedPrecondition`; the bad body is deleted and the upload stays open, so the console can send again. An unknown, expired or completed upload is `NotFound` |
| reads (`GetObject`, `GetObjectByID`, `ListObjects`) | a blob-backed object carries `get_url`, a presigned `GET` signed for the public host; its `body` is always empty, `include_body` or not |

Rules:

* A pending upload is invisible to reads and reserves its bytes and one object against the quota
  until it completes or expires (default one hour, `TITLE_STORAGE_UPLOAD_TTL`). A second
  `PrepareUpload` of the same object replaces the first.
* Presigned URLs live 15 minutes (`TITLE_STORAGE_URL_TTL`) and are signed for
  `TITLE_STORAGE_S3_PUBLIC_URL` (the host consoles connect to; S3 SigV4 binds the host).
* Every row change that orphans bytes (delete, replacement, an inline `PutObject` over a blob
  object, expiry, an abandoned upload, erasure) moves the key into `blob_trash` in the same
  transaction; the caller deletes it from the bucket at once, and the ten-minute janitor retries
  whatever failed. Blob keys are `<id>/<random>`, so a replacement never overwrites bytes a reader
  is fetching and keys cannot be guessed.
* No bucket configured (`TITLE_STORAGE_S3_ENDPOINT` unset): both calls are `FailedPrecondition`;
  nothing else changes.

Limits: info 4096 bytes, metadata 16 KiB (a JSON object), name 256 bytes, body up to the title's
`max_object_bytes` (default 4 MiB). Per owner per title: `quota_bytes` (default 32 MiB) and
`quota_objects` (default 256); a replacement counts only the difference. Both are
`ResourceExhausted`; `value too large` and `quota exceeded` are in the message. Expired objects are
never served and do not count; a janitor deletes them every ten minutes.

## Title policy (operator and adapters)

`UpsertTitle(title, name, cross_network, quota_bytes, quota_objects, max_object_bytes,
default_visibility)` changes only the fields it sets; adapters call it at start for the reserved
titles they own. `cross_network` (default true) can only change while the title holds no rows:
with `false` every network gets its own rows under the title (same key and slot, separate data),
and a request without a network is `FailedPrecondition`. `ListTitles` returns the policies and the
native ids per network.

## Errors

`InvalidArgument` (bad owner, slot, op, JSON, digest, overflow, a rating out of range or on a slot
that does not exist), `ResourceExhausted` (too large, quota), `PermissionDenied` (writer,
self-rating), `FailedPrecondition` (a per-network title without a network, a
title conflict, no blob tier, an uploaded body that does not match), `NotFound` (unknown neutral
key, unknown or expired upload), `Unavailable` (the account core or the bucket did not answer),
`Unauthenticated`.

## Events

None. Storage writes are not announced on the account core's event stream: no consumer needs a
slot or an upload as an event (the website has no storage view, and rewards count scores and
trophies, which have their own cores). If a consumer appears, `object.put` / `object.delete` with
`subject_id = <title>/<collection>/<name>` is where it would go. Account erasure deletes every
variable, object and pending upload the account owns and their blobs in the bucket, every rating
the account made (its contribution is subtracted from each slot's total and count, in one
transaction under the slots' locks) and every rating others made on its objects (they go with the
objects), and answers `{"deleted": rows, "blobs": n, "ratings_made": n, "ratings_received": n}`; when the bucket fails it answers 502 and the finaliser's repeat
deletes the rest (the trash remembers the keys under the account). The `setter` strings other
owners' rows carry for a writer are adapter-opaque and stay.

## Store

Postgres `title_storage`: `titles` (policy), `title_keys`, `variables` (primary key
`(title_key, ns, owner, slot)`), `objects` (unique `(title_key, ns, owner, collection, name)`, a
global `id`, `meta jsonb`, `body bytea`, `blob_key`/`sha256` for a blob-backed body), `uploads`
(prepared uploads, the reserved id, `expires_at`), `blob_trash` (bucket keys still to delete).
`rating_slots` (primary key `(object_id, slot)`, policy, `total`, `rating_count`; foreign key to
`objects(id)` on delete cascade) and `ratings` (primary key `(object_id, slot, rater)`, `value`,
`times`, `rated`; cascade from its slot; index on `rater` for erasure). Search indexes: `objects_list (title_key, collection, updated)` and `objects_created (title_key,
collection, created)`; metadata ranges and containment filter within those (no per-key index:
title sizes do not need one yet).
`ns` is empty on a cross-network title and the network on
a per-network one. Migrated at start.
