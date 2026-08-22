# CloudMergeTree — design

An OSS table engine with the SharedMergeTree invariant: **the authoritative set
of active parts lives in Keeper; part data lives on a shared object-storage disk;
replicas are stateless and own no parts.**

This is *not* `ReplicatedMergeTree` + zero-copy. Zero-copy bolts shared objects
onto the per-replica-ownership model and lets Keeper ref-count objects it does not
own — the source of the premature-delete / leak / double-materialization races.
CloudMergeTree inverts ownership: Keeper owns the part set by construction, so
ref-counting and GC are exact, not bolted on.

## Invariant (the thing that must never break)

1. A part is *active* iff its znode exists under `<root>/parts/`. Keeper is the
   single source of truth. A replica's in-memory `data_parts_indexes` is a cache
   of that set, never the authority.
2. A part's data object on shared storage may be deleted **only** when its znode
   is absent AND a grace period has elapsed AND no lease references it. GC is
   driven by the part set, never by a replica-local refcount.
3. Every merge/mutation result is materialized **exactly once**: the commit that
   adds the result and removes its sources is a single Keeper `multi()` guarded by
   a lease check. The loser of a race fails the `multi()` and discards its output.

If any of these is violated we have rebuilt zero-copy. They are the acceptance
criteria for every phase.

## Base class decision

Subclass `MergeTreeData` directly, modelled on `StorageMergeTree` — **not** a
fork of `StorageReplicatedMergeTree`.

Rationale: `StorageReplicatedMergeTree` (~11.8k lines) is welded to the
replication log — `ReplicatedMergeTreeQueue`, `LogEntry` replay, restarting/attach
threads, leader election, `DataPartsExchange` peer fetch. CloudMergeTree's whole
point is to have none of that. `StorageMergeTree` already performs local merges
and mutations with no replication queue; it is the correct shape. We change three
things relative to it:
- the part set is persisted to and loaded from Keeper (not just local disk),
- part data lives on a shared disk that every replica can read,
- writes/merges/GC are coordinated across replicas through Keeper.

## Keeper layout

```
<root>/                         root = /clickhouse/cloud_tables/{uuid} (configurable)
  metadata                      table schema (reuse ReplicatedMergeTreeTableMetadata)
  columns
  parts/                        THE CANONICAL ACTIVE PART SET
    <part_name>                 value = part header (columns + checksums), like
                                Replicated minimalistic header. cversion of this
                                node is the part-set version for cheap change checks.
  block_numbers/<partition>/    block-number allocation (ephemeral-sequential lock)
  mutations/<id>                mutation commands + target version
  leases/                       merge/mutation assignment leases (ephemeral)
    <part_range>                holder session writes here before producing a result
  replicas/<replica_session>    ephemeral liveness nodes. NO per-replica parts.
  temp/                         in-flight part registrations for crash cleanup
```

The part set is **global**, not per-replica (the key departure from Replicated).
Per-part znodes (not one big set znode) — same reason Replicated uses them: a
single znode hits the ~1 MB limit fast. The `parts` parent's `cversion` is the
monotonically-increasing part-set version used for read snapshots and fencing.

## Commit seams (where we hook the existing code)

INSERT (multi-writer, must coordinate):
- Reuse `MergeTreeDataWriter::writeTempPart` to build the part on the shared disk.
- Allocate a block number via an ephemeral-sequential node under `block_numbers/`.
- Build a Keeper `multi()`: create `parts/<name>` (+ optional dedup block id),
  check the block-number lock. Run `tryMultiNoThrow` **before** committing
  in-memory — mirrors `ReplicatedMergeTreeSink::commitPart`
  (`ReplicatedMergeTreeSink.cpp:1020`).
- Only on Keeper success call `MergeTreeData::Transaction::commit`
  (`MergeTreeData.cpp:8885`) to flip the part Active in this replica's cache.

MERGE / MUTATE (exactly-once):
- A replica that wins assignment writes a lease under `leases/<range>`
  (ephemeral, fenced by its session).
- It reads sources from shared storage, writes the merged part once.
- Commit = single `multi()`: create `parts/<merged>` + remove `parts/<source_i>`
  + check the lease still holds. Lose the race → `multi()` fails → discard output.

DROP / DROP PARTITION:
- `multi()` removing the relevant `parts/<name>` znodes. Object deletion is left
  to GC (invariant 2), never inline.

## Background services (Keeper-owned, replace the queue)

- **Part-set watcher**: watch `parts` cversion; on change, reload the diff into
  `data_parts_indexes`. This replaces log replay. A replica converges to the
  Keeper set; it never reasons about a per-replica queue.
- **Merge assigner**: a lease-holder (or simple per-table elected coordinator)
  selects merges against the Keeper part set and records assignments. No
  single-leader-proposes-to-log; assignment is a Keeper lease.
- **Parts killer (GC)**: periodically list shared-storage objects, delete those
  whose `parts/<name>` znode is absent past the grace period and unreferenced by
  any lease. This is the structural fix for the zero-copy delete race.

## Reused unchanged

`IDataPartStorage` over object storage, `MergeTreeDataWriter`, the entire read
path (`MergeTreeDataSelectExecutor`, sparse primary index, skip indexes,
projections, FINAL), `MergeTreePartInfo`, checksums, `MergeTreeSettings`,
`zkutil::ZooKeeper` (`multi`/`tryMultiNoThrow`, `makeCreateRequest` etc.).

## Phase plan (each phase ends green on its invariant)

- **Phase 0 — walking skeleton, single replica.** Register engine `CloudMergeTree`.
  `StorageCloudMergeTree : MergeTreeData` compiles with all pure virtuals stubbed.
  `CREATE TABLE … ENGINE = CloudMergeTree`. INSERT writes a part to the disk and
  creates `parts/<name>` in Keeper; on startup the part set is loaded from Keeper,
  not the local disk listing. SELECT works. No merges, no GC, no second replica.
  Proves: registration, Keeper part-set CRUD, part read/write through
  `IDataPartStorage`. Invariant 1 only.
- **Phase 1 — stateless second replica.** A second server reads the same Keeper
  part set and the same shared objects with no local copy and no peer fetch.
  Enforce shared-disk requirement. Proves the decoupling.
- **Phase 2 — merges + atomic commit + leases.** Exactly-once materialization.
  Invariant 3.
- **Phase 3 — Keeper-owned GC (parts killer).** Invariant 2.
- **Phase 4 — mutations, DROP PARTITION, ALTER, dedup.** Feature completeness.
- **Phase 5 — cross-table MOVE/REPLACE PARTITION.** Transfer Keeper ownership
  of a partition's parts from one table's `<root>` to another's (register
  under the destination, deregister from the source), for the standard
  ClickHouse bulk-swap/archival use cases (`REPLACE PARTITION ... FROM` for
  atomic staging-table reloads; `MOVE PARTITION ... TO TABLE` for archiving
  into cold-storage tables sharing the same schema). Not yet started.

Deferred (the SMT periphery, not correctness): sequential-consistency read
fencing, per-AZ leader fan-out, snapshot cleaner tuning, backup/restore
conversion.
