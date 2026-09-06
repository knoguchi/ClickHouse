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
  lightweight_updates/           required by the generic, reused-unmodified
    in_progress/<seq>           getLockForLightweightUpdateInKeeper() (update_parallel_mode=
                                'auto' conflict detection; 'sync' locks lightweight_updates/lock
                                directly). Created once at table bootstrap (createRootNodes()),
                                like every other subtree here -- both getLockForSyncMode() and
                                getLockForAutoMode() unconditionally operate directly under this
                                path with no ZNONODE tolerance on the parent.
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

`UPDATE ... SET` (lightweight update, `enable_lightweight_update = 1,
apply_patch_parts = 1`) is done. It writes a "patch part" -- a normal
`DataPartKind::Patch` part carrying only the updated columns plus system
columns (`_block_number`, `_block_offset`, `_data_version`, v2's sort-key
columns), living in a synthetic `patch-<hash>-<original_partition_id>`
partition -- committed through the exact same `commitInsertedPart()`
Keeper-commit hook `INSERT` already uses (`CloudMergeTreeSinkPatch`,
modelled on `CloudMergeTreeSink`). v1 patches (`Join` mode, tied to a
specific source part name) are rejected outright
(`getDefaultSettings()` forces `patch_parts_version = 'v2'`); v2 keys on
`(sort-key columns, _block_number, _block_offset)`, invariant across
merges by construction, so the entire read/merge path stays generic and
unmodified. `MergeTreeData::updateLightweightImpl()` (the
read-snapshot-and-produce-patch-rows pipeline), `getMutationsSnapshot()`'s
patch reporting, `getMergingParamsForPatchParts()`, and `MergeTask`'s own
patch-merge/apply-on-merge branches are all inherited unchanged -- the
CloudMergeTree-specific work was entirely in teaching the Keeper part-set
model (adoption, GC, DROP/DETACH cascade, backup) about the second part
kind, plus one new Keeper subtree (`lightweight_updates/`, required by
the generic, reused-unmodified `getLockForLightweightUpdateInKeeper()`).
Patch-to-patch compaction and apply-on-merge both run automatically via
the same background merge-selection scheduler regular parts use (a
second, patch-scoped `CloudMergeTreePartsCollector` instance, tried after
regular-merge selection finds nothing). Patch-absorption GC folds into
the existing `runPartsKillerCycle()` cadence: each active patch's
Keeper-stored `max_data_version` (carried in its `CloudPartLocation`
trailer, so no object-storage read of `source_parts.dat` is needed per
cycle) is compared against the Keeper-fresh minimum `data_version` among
active regular parts in its original partition -- deliberately
Keeper-fresh, not this replica's local part-set cache, the same
substitution that closed the cross-replica visibility race below applied
to a second bug class. `DETACH PART`/`DETACH PARTITION` reject outright
when unabsorbed patches would be left behind (`assertNoPatchesForParts()`,
a generic `MergeTreeData` method reused unchanged, matching vanilla's own
guard) -- proven necessary by a reproduced SIGSEGV (see below). Cross-table
`REPLACE`/`MOVE PARTITION` with unabsorbed patches in the source partition
are rejected too (real support needs the destination table to also
support patches plus an atomic dual-kind transfer -- out of scope for v1).

**Lightweight-UPDATE adoption SIGSEGV (found and fixed 2026-08-22).**
Cross-replica reads of a lightweight-UPDATE patch applied against a
freshly-adopted, never-merged (0-level) regular part crashed with a
segfault in vanilla's own `MergeTreeReadersChain::applyPatches()`
(`BlockCursor::blockNumber()` indexing into an unpopulated column) --
100% reproducible, and *not* a vanilla bug: an identical
`ReplicatedMergeTree` table, same S3 storage, same 2-replica/ZooKeeper
setup, same UPDATE sequence, was verified clean. Root cause:
`buildPartFromDisk()` (CMT's routine cross-replica part-adoption path,
called from `updatePartSetFromKeeper()` for every newly-discovered part)
called the generic `MergeTreeData::loadPartAndFixMetadataImpl()`
unconditionally -- the same helper vanilla itself only ever calls for a
genuine ATTACH-from-detached (`StorageReplicatedMergeTree`'s own
attach-parts-to-missing-partitions path, `MergeTreeData`'s own
loaded-from-a-detached-directory path), where a part's provenance
relative to the *current* schema is genuinely unknown and its
`writeInvalidatedSystemColumnsFile()` call for `_block_number`/
`_block_offset` is the correct, deliberate choice. CMT's routine
adoption of an already-active, freshly-committed-elsewhere part is
structurally equivalent to `StorageReplicatedMergeTree::fetchPart()`
instead -- which does no such invalidation -- so applying the
ATTACH-semantic invalidation there was simply wrong, just never
triggered before lightweight UPDATE became the first CMT feature to
depend on these columns' per-row values surviving adoption intact. Fixed
by splitting `buildPartFromDisk()` on a new `is_attach` parameter: the
genuine-ATTACH call site (`attachPartition()`) keeps calling
`loadPartAndFixMetadataImpl()` unchanged; the routine-adoption call site
(`updatePartSetFromKeeper()`) now runs the same sequence minus the
invalidation call. Closing this also exposed a real, separate gap:
vanilla itself rejects `DETACH PARTITION`/`DETACH PART` outright when
unabsorbed patches are present (`assertNoPatchesForParts()`, `SUPPORT_IS_
DISABLED`, "run `APPLY PATCHES IN PARTITION` first") -- exactly because a
later re-ATTACH re-triggers this same invalidation-vs-patch hazard even
with the adoption-path fix in place. CMT's DETACH path didn't replicate
that guard; it now does, calling the same generic, unmodified
`assertNoPatchesForParts()` vanilla uses.

**Patch-to-patch merge predicate bug (found and fixed 2026-08-22).**
`CloudMergeTreeMergePredicate::canMergeParts()`'s mutation-version-
equality check compared `left.info.mutation` directly (correct for
regular parts, where CMT -- unlike vanilla, which has no local
`current_mutations_by_version` map to consult either -- has no cheaper
derivation available). Patch parts repurpose `.mutation` entirely (it
holds the patch's own `max_data_version`, stamped by
`writeTempPartImpl()`), so any two patches in the same synthetic patch
partition necessarily have *different* `.mutation` values by
construction -- the direct comparison rejected every patch-to-patch
merge candidate, silently disabling automatic patch compaction
(`selectPartsToMerge()`'s patch-scoped second attempt always ran, just
never found anything it could merge). Fixed by skipping this check when
`left.info.isPatch()` (both sides are always the same kind, checked just
above by partition-id equality). Found via
`test_lightweight_update_many_small_updates_merge_into_one_patch`, which
failed with 6 patch parts never converging to 1.

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

## Deployment notes (learned on a 5-replica staging cluster, 2026-09-04/05)

- DDL is not replicated. Create tables with `ON CLUSTER` (or inside a
  `Replicated` database) so every replica gets the same UUID; the Keeper root
  is derived from it. A plain `CREATE` on one replica makes a private, empty
  table with the same name and no warning. A replica rebuilt on an empty disk
  needs `CREATE TABLE ... UUID '<same uuid>'`.
- Set `placement.availability_zone` on every replica. Without it every replica
  runs merge selection; on five replicas that meant five copies of each merge
  reading the same source parts from object storage, with four losers
  discarding their output. With one zone configured a single replica leads.
- Parallel replicas need `parallel_replicas_for_non_replicated_merge_tree = 1`
  (the engine is not a `StorageReplicatedMergeTree`); without it the query
  silently runs on the initiator alone.
- `SYSTEM STOP MERGES` without a table name only covers tables that exist at
  that moment, as in every MergeTree engine.
- The `cache` disk type over `plain_rewritable` works (see the fix below), and
  is the configuration to use: there is no distributed cache, each replica
  caches for itself, and a cold replica pays the full object storage read once
  per column.

Three fixes came out of that deployment:

1. Part removal on `plain` / `plain_rewritable` disks now uses recursive
   directory removal (`DataPartStorageOnDiskBase::clearDirectory`). The
   per-file path did a copy, a delete and a second delete per file while
   holding the disk-wide `metadata_mutex`, so removing one 105-column part
   was over a thousand sequential round trips during which every insert and
   merge commit on the disk waited. Measured on MinIO for 8 parts of 100
   columns: 4150 requests before, 24 after, identical object count afterwards.
2. `CloudPartLocation::read` accepts trailers without the trailing
   `patch_max_data_version` field, so tables written before patch-part
   support load instead of failing with `ATTEMPT_TO_READ_AFTER_EOF`.
3. `MetadataStorageFromCacheObjectStorage` forwards
   `setAuthoritativeDirectory` / `removeAuthoritativeDirectory` to the
   underlying storage. Before, a `cache` disk dropped the Keeper-supplied part
   locations: a 289-part table was fully visible on the writer and empty on
   the other four replicas, and never merged because the leader saw nothing.

## Benchmark: ClickBench on 5 replicas over DigitalOcean Spaces (2026-09-05)

Setup: 5 replicas (12 GiB memory limit each, 16-core nodes), Keeper with one
node, `plain_rewritable` disk on DigitalOcean Spaces nyc3 reached across the
Internet (50 ms RTT, roughly 2 MiB/s per connection, zero 429/503 responses
ever observed). ClickBench `hits`, 99 997 497 rows, 105 columns, loaded from
the public parquet with 4 insert threads in 14 minutes with merges running.
Each of the 43 queries ran 3 times; mark, primary index, uncompressed and
filesystem caches dropped on the initiator before each query, so run 1 is
cold and runs 2-3 are hot. Times are wall-clock seconds summed over the 43
queries; geomean is over per-query times.

| configuration                        | cold sum | hot sum | geomean cold | geomean hot |
|--------------------------------------|---------:|--------:|-------------:|------------:|
| 1 replica, no data cache             |   1776 s |  1446 s |        9.0 s |       7.0 s |
| 1 replica, `cache` disk (10 GiB)     |   1737 s |    84 s |       11.6 s |      0.37 s |
| 3 replicas, parallel replicas        |    840 s |   787 s |        7.7 s |       6.4 s |
| 5 replicas, parallel replicas        |    611 s |   575 s |        6.2 s |       5.5 s |

Cold sums by query class, 1 / 3 / 5 replicas: the six wide string scans
(`URL`, `Title`, `Referer`) 1166 / 412 / 273 s; the 24 medium queries
595 / 398 / 313 s; the 13 small queries 15 / 30 / 25 s.

Reading it:

- Every cold number is object storage bandwidth. Narrow columns read at about
  85 MB/s per replica, wide string columns at about 15 MB/s because fewer,
  larger range requests hit the per-connection ceiling. Hot runs without a
  data cache only gain what the mark, index and query condition caches save.
- Parallel replicas split marks evenly and all replicas finish together. The
  wide scans scale close to the replica count. The medium queries stop near
  2x because the five replicas share one path to the bucket that saturates
  around 50 MB/s. Small queries get slower: a coordination round trip costs
  more than the query.
- A warm local cache beats four extra replicas by a wide margin: the whole
  suite in 84 s on one cached replica versus 575 s on five uncached ones.
  Cold is the same either way, so cold start is paid once per column per
  replica.
- Before fix 1 above neither load completed: the first merge round's source
  removals held the disk lock for hours and inserts froze at 24 M and 63 M
  rows.

Raw per-query results: `tmp/clickbench/results_staging_*.tsv` in the
working tree that produced them (not committed).
