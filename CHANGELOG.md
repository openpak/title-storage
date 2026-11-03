# Changelog

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
