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
| `ListObjects(ref, viewer, owners[] or friends_of_viewer [+include_self], collection, names[], meta_filter_json, sort, ascending, offset, limit, include_body)` | `sort` is `updated`, `created`, `size`, `name` or `id`; `meta_filter_json` matches objects whose metadata contains it (jsonb `@>`) |
| `DeleteObjects(ref, owner, writer, collection, names[])` | empty `names` = the whole collection |
| `Usage(ref, owner)` | bytes, objects, variables and the limits that apply |

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

`InvalidArgument` (bad owner, slot, op, JSON, overflow), `ResourceExhausted` (too large, quota),
`PermissionDenied` (writer), `FailedPrecondition` (a per-network title without a network, a title
conflict), `NotFound` (unknown neutral key), `Unavailable` (the account core did not answer; a
friends/interaction check could not be made), `Unauthenticated`.

## Events

None. Storage writes are not announced on the account core's event stream: no consumer needs a
slot or an upload as an event (the website has no storage view, and rewards count scores and
trophies, which have their own cores). If a consumer appears, `object.put` / `object.delete` with
`subject_id = <title>/<collection>/<name>` is where it would go. Account erasure deletes every
variable and object the account owns (the `setter` strings other owners' rows carry for a
writer are adapter-opaque and stay).

## Store

Postgres `title_storage`: `titles` (policy), `title_keys`, `variables` (primary key
`(title_key, ns, owner, slot)`), `objects` (unique `(title_key, ns, owner, collection, name)`, a
global `id`, `meta jsonb`, `body bytea`). `ns` is empty on a cross-network title and the network on
a per-network one. Migrated at start.
