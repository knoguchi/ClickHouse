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
    <part_name>                 value = part header (columns + checksums, like
                                Replicated minimalistic header) followed by a
                                CloudPartLocation trailer: the part's plain_rewritable
                                remote directory token(s) (root + any projections) and
                                complete file list with sizes, so a replica resolves
                                the part's bytes from this committed payload instead
                                of an object storage listing. cversion of this node's
                                parent is the part-set version for cheap change checks.
  block_numbers/<partition>/    block-number allocation (ephemeral-sequential lock)
  mutations/<id>                mutation commands + target version
  leases/                       merge/mutation assignment leases (ephemeral)
    <part_range>                holder session writes here before producing a result
  replicas/<az>/<seq>            ephemeral-sequential per-AZ election nodes (only when this
                                replica's availability zone is known). NO per-replica parts;
                                doubles as the per-AZ merge-selection leader election.
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
  into cold-storage tables sharing the same schema). Done: `REPLACE`/`ATTACH
  PARTITION ... FROM` and `MOVE PARTITION ... TO TABLE` are both implemented.

Deferred list is empty as of the per-AZ leader fan-out landing (below) --
every item ever listed here is now done.

Sequential-consistency read fencing is done: `select_sequential_consistency`
(the same global setting `StorageReplicatedMergeTree` uses) makes
`read()`/`totalRows()`/`totalBytes()`/`totalBytesUncompressed()`
synchronously catch this replica's local part-set cache up to Keeper's
current version first, instead of relying on the async background watcher
alone.

Snapshot cleaner tuning is done.

Per-AZ merge-selection leader fan-out is done: when a replica's
availability zone is known (`PlacementInfo::getAvailabilityZone()`), only
the elected leader within that AZ attempts background merge/mutation
selection, cutting the redundant Keeper traffic every other same-AZ
replica used to spend racing (and losing) leases they were never going to
win. A no-op wherever AZ info isn't configured. Deliberately per-AZ, not a
single global leader -- see `zkutil::checkNoOldLeaders`
(`Storages/MergeTree/LeaderElection.h`) on why upstream itself moved away
from single-leader merge assignment.

`DELETE FROM ... WHERE` (lightweight delete) works: it's implemented as a
mutation under the hood (`lightweight_delete_mode` defaults to
`ALTER_UPDATE`), the same Keeper-backed mutation path Phase 4 already
covers -- no CloudMergeTree-specific code needed.

`UPDATE ... SET` (lightweight update) is **not** supported: it writes a
separate "patch part" instead of a mutation, and CloudMergeTree has no
patch-part support in its Keeper part-set model (every part-set operation
in this engine filters to `DataPartKind::Regular` only --
see `CloudMergeTreePartsCollector`'s own doc comment). `IStorage`'s
default `updateLightweight()` throws `NOT_IMPLEMENTED` before anything is
written, so this fails closed rather than silently mishandling a patch
part -- confirmed by a regression test. Teaching the Keeper part-set model
about patch parts (commit, adopt, GC, backup/restore, all of it) would be
a real, separate feature, not attempted here.

`BACKUP`/`RESTORE` is done for part data: `backupData()` wraps each active
part (optionally partition-filtered) via the inherited `backupParts()`;
`attachRestoredParts()` commits restored parts through the same
`commitInsertedPart()` Keeper-commit hook `INSERT` uses, giving each a
fresh identity in the destination table. Pending (not-yet-applied)
mutations are not backed up -- a smaller, separate follow-up.

TTL (row delete) is done: `selectPartsToMerge()` now passes
`merge_with_ttl_allowed=true` to `MergeSelectorApplier`, and
`getActionLock()`/`onActionLockRemove()` wire up `ActionLocks::PartsTTLMerge`
the same way `PartsMerge` already was. The TTL-aware merge selector and its
own bookkeeping are already generic, shared `MergeTreeDataMergerMutator`
state -- this was a two-hunk flip, not a new subsystem. Move TTL (`TO DISK/
VOLUME`) stays unenforced, correctly: `startBackgroundMovesIfNeeded()` is a
deliberate no-op since the storage policy is constructor-enforced to
exactly one disk, so there is nowhere for a part to move *to*.

**Cross-replica part-visibility race (found 2026-08-18, structurally closed
2026-08-19/20).** A part discovered via a second replica's watcher could
pass every adoption check, get marked active, and later throw a raw
`FILE_DOESNT_EXIST` (or silently return wrong data) on a real `SELECT` --
reproduced at a 33-67% rate. Root cause: `plain_rewritable`'s in-memory
directory tree is refreshed via a full listing-based clobber-and-rebuild,
and that listing pass can itself momentarily miss an object the underlying
S3-compatible backend (any of them -- not an AWS-S3-specific quirk) hasn't
fully propagated yet. Existing single-writer uses of this disk type never
stress this, because a process's own writes update its own in-memory tree
synchronously, never by discovering them via a remote listing round-trip --
CloudMergeTree's "no peer fetch, read shared storage directly" architecture
is the first thing asking a replica to discover a *different process's*
writes purely through this listing-based refresh, pushing the metadata
layer outside the consistency envelope it was built for.

A first fix (2026-08-18) required a cross-replica-discovered part to
independently pass adoption checks on two separate observations, spaced
past `disk->refresh()`'s own throttle window, before being admitted -- a
heuristic timeout that traded latency (multiple seconds per cross-replica
commit) for safety, and, when tightened, reintroduced a related class: a
directory pinning mechanism meant to close the residual listing-gap window
had to be withdrawn precisely when a part left the working set, and an
early version of that withdrawal missed a case, letting a lease-losing
replica's cleanup delete a lease-winning replica's live objects under
concurrent mutation contention.

The structural fix (2026-08-19/20) removes the listing dependency from the
read path entirely, rather than racing it: every part znode payload now
carries, alongside the header, the part's `plain_rewritable` remote
directory token(s) and complete file list (see `CloudPartLocation`),
captured at commit time and travelling through the same atomic `multi()`
as the part's registration. Adoption, startup, ATTACH-from-detached, and
GC all resolve a part's bytes by reading this Keeper-committed location
and registering it as an authoritative override on the shared disk's
in-memory tree (`IMetadataStorage::setAuthoritativeDirectory`) --
overriding, not merely surviving, whatever the object storage listing
does or doesn't show -- then verify what was actually read back against
the header's checksums end to end. `plain_rewritable`'s listing keeps
working exactly as before for every other user of that disk type; only
CloudMergeTree's own directories carry an authoritative pointer.
Tombstones (`dropped_parts/`) and detach markers (`detached_parts/`) carry
the same location, copied from the part's znode at removal time, so GC
deletes by token instead of re-resolving a name through a possibly-stale
tree, and a replica attaching a part detached by a *different* replica
never depends on its own listing having ever observed that directory.
Cross-replica visibility drops from several seconds (the prior scheme's
settling window plus refresh-cycle quantization) to roughly one watch
fire plus a handful of strongly-consistent `GET`s.

This is not a workaround grafted onto the architecture -- it is the
architecture's own original design (see the top of this document:
Keeper as the sole authority, object storage addressed by exact committed
keys, mutability only in metadata) finally applied to part *location*
resolution, not just part *membership*. The prior schemes treated names as
Keeper-authoritative but locations as something to be rediscovered from an
eventually-consistent listing on every replica, on every cycle -- the gap
this fix closes.

A commit that crashes between its Keeper `multi()` and the local
rename-into-place is handled explicitly at startup: the part is already
readable at its final path via the authoritative override, so the
constructor resolves any of this replica's own leftover temp directories
by token before the ordinary startup temp-cleanup runs, completing the
rename rather than letting that cleanup delete the part's only physical
copy.

Known limits, accepted rather than engineered around: the location trailer
adds roughly 0.5-2 KB to each part znode and to each removal's tombstone
payload, so a `REPLACE PARTITION` spanning hundreds of parts approaches
Keeper's `multi()` payload ceiling well before it approaches the part-count
limits `ReplicatedMergeTree` already lives with -- if this bites in
practice, the escape hatch is an Iceberg-style layout (part locations in a
manifest file on object storage, Keeper holding only a pointer to the
current manifest) rather than shrinking the payload. Disaster recovery
still works without Keeper: `plain_rewritable` keeps writing its own
`prefix.path` markers on every commit regardless of the authoritative
override, so listing-based recovery tooling that never consults Keeper
remains possible; only the hot read/adoption/GC paths are Keeper-only.
