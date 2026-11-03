# Title storage

OpenPak's neutral per-title, per-account storage.

| | |
| --- | --- |
| Network | Core (all consoles) |
| Role | Integer variables with Title User Storage semantics and named object blobs (metadata, visibility, quotas, expiry) per title and owner, for every network's storage server |
| Depends on | [`account`](https://github.com/openpak/account) (friend list, interaction check), PostgreSQL |
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
  `public`. A title's default is private; adapters state the visibility their console implies.
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
(the tests truncate the tables).
