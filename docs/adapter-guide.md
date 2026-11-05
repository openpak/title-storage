# Adapter guide

For the owners of `wfc-sake`/`wfc-gamestats`, `nex` DataStore, `xb-stfd`, `npln` UGC, SMM2 and any
other per-title storage server. Read [protocol.md](protocol.md) first. The rule (board #40): the
adapter keeps the wire (codecs, ids, signatures, paging shape) and the core keeps the data.
Reference adapters: `np-tus` (variables and slots) and `np-uploader` (attachments as objects).

## What an adapter does

1. Map the console identity (NpId, XUID, PID, profile id) to a core account id through the account
   core's links. The service only takes account ids (or a `v:<name>` virtual owner).
2. Name the title: `TitleRef{network, title}` with your link namespace and the title id as the
   console knows it. `MapTitle` once per game so every network that sells it shares one key.
3. Declare policy at start with `UpsertTitle` (quota, `max_object_bytes`, default visibility,
   `cross_network=false` to opt a title out). Never keep a table of player data.
4. State the visibility your console implies on every write; do not rely on the title default.
5. Translate calls. Variables: `SetVariables`, `ListVariables`, `AddVariable`, `TrySetVariable`.
   Blobs: `PutObject`, `GetObject`, `ListObjects`, `DeleteObjects`. Render your own wire from
   the results (`updated_unix_ms`, `setter`, `size`, `id`).
6. Env: `<SERVICE>_TITLE_STORAGE_ADDR` (`openpak-title-storage:20231`) and `..._KEY`
   (`TITLE_STORAGE_INTERNAL_KEY`), sent as gRPC metadata `x-internal-key`. Generate the client from
   `proto/openpak/titlestorage/v1/titlestorage.proto` into your own `internal/titlestoragepb`
   (np-tus is the pattern). Raise `MaxCallRecvMsgSize` if your bodies exceed 4 MiB.

## Per console

### PS3 (`np-tus`, `np-uploader`) — done

* TUS slots: variables for `set/get/addget/tryset/delete_*_variable` and the three read forms; a
  data slot is the object `(collection "tus", name = slot)` with `info` = the 384-byte info blob.
  `comp-date`/`comp-setter` are `Condition`. Friend reads are `friends_of_viewer`; `sort=date` is
  `updated`, `sort=variable` is `value` (data: `size`). Everything is written `public`: TUS lets any
  player read any player's slot.
* Attachments: reserved neutral title `np-messages`, collection `attachments`, owner = uploader.

### Wii / DS (`wfc-sake`, `wfc-gamestats`)

* Network `wfc`, title = the game code. A SAKE table is a collection (`sake:<table>`), a record id is
  the name, the record's fields are `meta_json` (filterable by equality with `meta_filter_json`),
  and a file column or MKW ghost is the `body`. The profile id maps to the account through the
  `wfc` link.
* Visibility `public` for browsable tables (ghosts, friend info readable by anyone with the id),
  `private` for per-player GameStats; the Mii filtering for non-owners stays in `wfc-sake`.
* Gaps: numeric ranges on metadata exist (`meta_range_json`, v0.4.0), but there is no sort by a
  metadata key, so the "nearest slower ghost" query is a range plus an adapter-side pick, or a
  numeric sort key added here before moving MKW ghosts.

### NEX DataStore (Wii U/3DS `nex`, Switch `switch-nex`)

* Network `wiiu` / `3ds` / `switch`, title = the NEX game server id. `DataStoreMetaInfo.data_id` is
  the object `id` (stable across replacement), owner = the uploader's account, `name` = the NEX
  `name` or empty (server-named), `collection` = the NEX `data_type`, `meta_json` carries tags,
  period, flags and the `refer_data_id`, `info` the persistence-slot blob.
* Permissions (`DataStorePermission`): 0 public to `public`, 1 friends to `friends`, 3 private to
  `private`, 2 specified to `recipients` with `recipients` = the `RecipientIDs` PIDs mapped to
  accounts, 4 to `recipients` + `recipients_include_friends` (v0.6.0). For 0, 1 and 3 still pass
  `RecipientIDs` as `recipients`: they are stored and returned (the meta echoes them) but grant
  nothing. A PID with no account cannot be a recipient: leave it out. `SearchTypeReceived` is
  `ListObjects(all_owners, received=true)`. `DelPermission` and its recipients go in `meta_json`:
  the write rule (owner, or an account the core lets interact) is the core's delete check; a
  stricter delete list is the adapter's check before `DeleteObjects`.
* Search (`DataStoreSearchParam`): `ListObjects` with `all_owners` / `friends_of_viewer` /
  `owners` (+ `exclude_owners`), `collections`, `meta_filter_json` (tags), `meta_range_json`
  (numeric ranges such as `data_type`), the created/updated bounds and `sort`; mapping table in
  [protocol.md](protocol.md#searching-objects-listobjects). A rating order
  (`ResultOrderColumn` 64-79 / 96-111) is `sort "rating:<slot>"` / `"rating_avg:<slot>"`,
  `MinimalRatingFrequency` is `min_rating_count`. The object's `period` (expiry) is `ttl_seconds`.
* Upload (`PreparePostObject` / `CompletePostObject`): `PrepareUpload(..., size, post_form=true)`;
  the `id` is the data id, `put_url` + `form_fields` are the `DataStoreReqPostInfo` URL and form
  fields (no request headers). `CompletePostObject(success)` is `CompleteUpload(id)`; a
  `FailedPrecondition` there is the console's upload not matching (`DataStore::OperationNotAllowed`
  or the title's equivalent). `GetObjectInfos`/`PrepareGetObject` answer with `get_url`.
* Ratings (v0.5.0): `InitializeObjectRatingWithSlot` / the rating params of `PostObject` are
  `InitRating(id, writer, slot, policy)`: `initial_value`, `lock` from `RatingLockType`
  (0 none, 1 `interval` with `interval_seconds` = `PeriodDuration`, 2 `period` with `period_day` =
  `PeriodDuration` (-17, -1..-7) and `period_hour` = `PeriodHour`, 3 `permanent`), `range_min` /
  `range_max` only when `InternalFlag` has UseRangeMin (0x2) / UseRangeMax (0x4), `Flag` 0x4
  `modifiable`, 0x8 `round_minus`, 0x10 `disable_self_rating`. `RateObject(s)` is
  `RateObject(id, slot, rater = the caller's account, value)`; `locked=true` is
  `DataStore::OperationNotAllowed`, `PermissionDenied` (self-rating) likewise, `InvalidArgument`
  (range, no slot) is `DataStore::InvalidArgument`. `DataStoreRatingInfo{TotalValue, Count,
  InitialValue}` is `Rating{total, count, initial_value}`; `GetRating(s)` and the ratings in
  `GetMeta`/search results are `GetRatings` or `include_ratings` on `GetObjectByID` /
  `ListObjects` (one call, no extra hop); `GetRatingWithLog`'s log is the rater's row
  (`rated`, `rater_value`, `lock_expires_unix_ms`). `ResetRating(s)` is `ResetRatings`.
  Custom rankings stay a leaderboards concern.
* Metadata changes (v0.7.0): `ChangeMeta` / `UpdateObjectMetaBinary` / `UpdateObjectPeriod` /
  `UpdateObjectDataType` (and their `...WithPassword` forms) are `UpdateObjectMeta(id, writer,
  ...)`: meta binary = `info`, tags / period / flags = `meta_merge_json` (or `meta_json` for the
  whole map), data type = `collection`, permission = `visibility` + `recipients`, period expiry =
  `expires_unix_ms`. It keeps the uploaded body and the ratings; never use `PutObject` for these,
  it replaces the body and drops an uploaded blob. `condition.expect_updated_unix_ms` with the
  `updated_unix_ms` you read guards a read-modify-write.
* SMM2 courses: a course is a public object (`collection "courses"`); the 36-bit data id and
  checksum are derived from `id`, which is stable. Thumbnails/replays are separate objects named
  `<id>.thumb`/`<id>.replay` or share a meta key.

### Xbox / Xbox 360 (`xb-stfd`, x360 title storage)

* `readfile`/`writefile`: owner = the XUID's account, collection and name from the path, `expires`
  as `ttl_seconds`, the per-user quota is the title's `quota_bytes` (set it per title via
  `UpsertTitle`; the old 16 MiB per file is `max_object_bytes`). Voice-mail attachments are
  `private` objects whose id travels in the message.

### Switch (`npln` UGC, Stardew, Splatoon 3, T17 blobs, PlayFab player data)

* Network `switch`, title = application id; documents are objects (`collection "ugc"`), player
  profiles and characters owner-scoped `private` objects. The SigV4 host rules stay in the adapter.

## Checklist

* Calls carry `x-internal-key`; the adapter has no database for player data.
* Write tests against a fake title-storage server (see np-tus's `fakestorage`).
* Account erasure needs nothing from you: the finaliser calls
  `DELETE /internal/accounts/{account}/data`.

## Message sizes

List calls can exceed gRPC's 4 MB default reply size (nx-pctl hit 5 MB for one device against the
parental core). Adapters must raise `MaxCallRecvMsgSize` on the client, or page the list and keep pages
small. Server-side, keep list replies bounded (page size caps) so one account cannot force a huge reply.
