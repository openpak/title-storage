# Title storage

OpenPak's neutral per-title, per-account storage.

| | |
| --- | --- |
| Network | Core (all consoles) |
| Role | Integer variables with Title User Storage semantics and named object blobs (metadata, visibility, quotas, expiry) per title and owner, for every network's storage server |
| Depends on | [`account`](https://github.com/openpak/account) (friend list, interaction check), PostgreSQL, an S3 bucket for the blob tier (optional) |
| Used by | [`np-tus`](https://github.com/openpak/np-tus) and [`np-uploader`](https://github.com/openpak/np-uploader) (PS3); later `wfc-sake`, `nex` DataStore, `xb-stfd`, `npln` UGC |
| Image | `ghcr.io/openpak/title-storage` |
| Ports | 20230 HTTP, 20231 gRPC, 20232 metrics (block 20230) |
| Licence | AGPL-3.0-only |

## Model

* **Title key**: one neutral key per game across networks. `title_keys` maps
  `(network, native id)` to it (a PS3 `NPWR04270_00`, an Xbox 360 title id, a Switch application
  id). An unmapped native id registers itself as `<network>:<native>` so nothing is lost. A
  neutral key can also be used directly (network empty): `np-messages` holds PS3 message
  attachments.
* **Owner**: a core account id, or a title-wide virtual user `v:<name>` that every account may
  read and write. Adapters map their own ids to accounts.
* **Variable** `(title, owner, slot)`: an int64 with the setter and a 10 ms write time. Set, add,
  compare-and-set (`TrySetVariable`), write conditions (same date / same setter), multi-slot,
  multi-owner and friend reads with sort and paging.
* **Object** `(title, owner, collection, name)`: info (4 KiB), JSON metadata (16 KiB, filterable),
  a body, a stable numeric id, a visibility and an optional expiry. Collections keep several uses
  apart inside one title.
* **Visibility**: `private` (owner), `friends` (the owner's friends, from the account core),
  `public`, and for objects `recipients` (the owner, a list of accounts, optionally the owner's
  friends). A title's default is private; adapters state the visibility their console implies.
* **Blob tier**: large bodies go to S3 (MinIO) directly from the console with presigned requests
  (`PrepareUpload` -> PUT or POST form -> `CompleteUpload`; reads carry a presigned `get_url`).
  Quota and size cap are checked before the upload, size and digest after it; deletes, expiry,
  abandoned uploads and account erasure delete the bytes. Small objects stay inline.
* **Ratings**: numeric slots per object (`InitRating`, `RateObject`, `GetRatings`,
  `ResetRatings`) with an initial value, range, lock (none, interval, period, permanent) and
  flags; one row per rater, so a lock holds and an erased rater's ratings come back out of the
  totals. Searches sort by a slot's total or average.
* **Quota** per owner per title: bytes, objects and the largest body, with service defaults
  and per-title overrides (`UpsertTitle`).
* **Cross-network by default**; a title with `cross_network=false` keeps each network's rows apart.

Every request names the caller's network and the title as that network knows it. See
[docs/protocol.md](docs/protocol.md), [docs/adapter-guide.md](docs/adapter-guide.md) and
[docs/deploy.md](docs/deploy.md).

## Run

```sh
cp example.env .env   # fill in required values
set -a; . ./.env; set +a
GOWORK=off go run ./cmd/title-storage
```

Tests need PostgreSQL: `TITLE_STORAGE_TEST_DATABASE_URL=postgres://… GOWORK=off go test ./...`
(the tests truncate the tables). The blob tier runs against an in-process fake S3, and against a
real one too when `TITLE_STORAGE_TEST_S3_ENDPOINT`, `_ACCESS_KEY`, `_SECRET_KEY` (`_BUCKET`, default
`title-storage-test`) are set.
