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
* Gaps: range/ordering filters beyond equality (the "nearest slower ghost" query) are not
  expressible yet. Add a `meta` index or a numeric sort key before moving MKW ghosts here, or keep
  the ghost *query* adapter-side over `ListObjects` until then.

### NEX DataStore (Wii U/3DS `nex`, Switch `switch-nex`)

* Network `wiiu` / `3ds` / `switch`, title = the NEX game server id. `DataStoreMetaInfo.data_id` is
  the object `id` (stable across replacement), owner = the uploader's account, `name` = the NEX
  `name` or empty (server-named), `collection` = the NEX `data_type`, `meta_json` carries tags,
  period, flags and the `refer_data_id`, `info` the persistence-slot blob.
* Permissions: owner-only to `private`, friends to `friends`, public to `public`. "Specified
  people" has no neutral form yet: keep the list in `meta_json` and filter adapter-side, or use
  `private` and have the adapter check its own list.
* Search by tags/owner/period: `ListObjects` with `meta_filter_json` and `owners`.
  `expires` is `ttl_seconds`.
* Upload (`PreparePostObject` / `CompletePostObject`): `PrepareUpload(..., size, post_form=true)`;
  the `id` is the data id, `put_url` + `form_fields` are the `DataStoreReqPostInfo` URL and form
  fields (no request headers). `CompletePostObject(success)` is `CompleteUpload(id)`; a
  `FailedPrecondition` there is the console's upload not matching (`DataStore::OperationNotAllowed`
  or the title's equivalent). `GetObjectInfos`/`PrepareGetObject` answer with `get_url`.
* Gaps: ratings (`DataStoreRatingInfo`) and custom rankings are not a core concept yet. Keep rating
  counters in `meta_json` via `PutObject` with a `Condition` until a `ratings` table exists in this
  service (planned as a sibling of `objects`, keyed by object id).
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
