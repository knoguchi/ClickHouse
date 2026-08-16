#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeCoordination.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <expected>
#include <functional>

namespace DB
{

/// Defined in CloudMergePlainMergeTreeTask.h. Only forward-declared here: that header includes
/// this one (it needs the full StorageCloudMergeTree type for CloudCurrentlyMergingPartsTagger),
/// so including it back would be circular.
struct CloudMergeMutateSelectedEntry;
using CloudMergeMutateSelectedEntryPtr = std::shared_ptr<CloudMergeMutateSelectedEntry>;

/// Defined in CloudMergeMutateTask.h, forward-declared here for the same circular-include reason
/// as CloudMergeMutateSelectedEntry above.
struct CloudMutateSelectedEntry;
using CloudMutateSelectedEntryPtr = std::shared_ptr<CloudMutateSelectedEntry>;

/// Defined in ReplicatedMergeTreeMutationEntry.h. Only forward-declared here since it's only used
/// by value in one private method's signature, not stored as a member.
struct ReplicatedMergeTreeMutationEntry;

/** CloudMergeTree: a stateless-replica MergeTree whose authoritative active part
  * set lives in Keeper and whose part data lives on a shared object-storage disk.
  *
  * Unlike StorageReplicatedMergeTree there is no replication log, no per-replica
  * part ownership, and no peer-to-peer part fetch: every replica reads the same
  * global part set from Keeper (CloudMergeTreeCoordination) and the same objects
  * from shared storage. See DESIGN.md.
  *
  * Phase 1 scope: multi-replica CREATE / INSERT / SELECT / DROP on a single, shared
  * (remote) disk. A replica bootstraps its working set from Keeper's active part
  * names, not a local directory listing, and a background watcher reconciles it
  * against Keeper as the set changes so parts inserted by another replica become
  * visible without a restart. Merges, mutations and partition commands are not
  * implemented yet and throw NOT_IMPLEMENTED.
  */
class StorageCloudMergeTree final : public MergeTreeData
{
public:
    StorageCloudMergeTree(
        const String & zookeeper_root_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

    std::string getName() const override { return "Cloud" + merging_params.getModeName() + "MergeTree"; }

    void startup() override;
    void shutdown(bool is_drop) override;

    bool supportsParallelInsert() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void drop() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    /// IStorage::totalRows/totalBytes/totalBytesUncompressed default to std::nullopt; ServerAsynchronousMetrics
    /// calls .value() on them unconditionally for every MergeTreeData-derived table, so a
    /// MergeTreeData subclass that doesn't override these throws (and, at server startup, before
    /// the metrics thread's own steady-state try/catch is established, crashes the whole process).
    std::optional<UInt64> totalRows(ContextPtr) const override { return getTotalActiveSizeInRows(); }
    std::optional<UInt64> totalBytes(ContextPtr) const override { return getTotalActiveSizeInBytes(); }
    std::optional<UInt64> totalBytesUncompressed(const Settings &) const override;

    /// data.insert_increment is a per-process counter, meant to give temp part directories
    /// process-local uniqueness -- sufficient on ordinary MergeTree/ReplicatedMergeTree, where
    /// every replica's temp directories live on that replica's own local disk. CloudMergeTree
    /// writes temp directories to the *shared* disk every replica sees, so two replicas whose
    /// local counters reach the same value at the same time would otherwise collide on the same
    /// path. A fresh UUID per call (this is invoked once per writeTempPart, i.e. once per INSERT)
    /// makes every temp directory name globally unique regardless of what any replica's local
    /// counter says.
    std::string getPostfixForTempInsertName() const override;

    /// The part-set coordinator and a freshly-resolved Keeper session. Used by the sink.
    const CloudMergeTreeCoordination & getCoordination() const { return coordination; }
    zkutil::ZooKeeperPtr getZooKeeper() const;

    /// Commit a freshly written part: register it in Keeper, then flip it Active in the cache.
    /// Throws on a Keeper failure so the INSERT fails closed (no silent local-only commit).
    /// Returns false (part discarded via removeIfNeeded(), nothing touched in Keeper's active set
    /// or the local cache) if insert_deduplicate is enabled and this exact block content was
    /// already committed by some part -- same silent-no-op contract as ReplicatedMergeTreeSink,
    /// not a query failure. Returns true once the part is genuinely active.
    bool commitInsertedPart(MutableDataPartPtr & part, ContextPtr context);

    /// Returns the maximum level of all outdated parts strictly between left and right, or 0 for
    /// an empty range. Used by the merge predicate to reject a merge that would paper over a gap
    /// containing a higher-level outdated part (see StorageMergeTree::getMaxLevelInBetween, which
    /// this is a verbatim copy of -- the logic isn't specific to how parts get into the set).
    UInt32 getMaxLevelInBetween(const PartProperties & left, const PartProperties & right) const;

    /// Keeper-fenced merge commit (DESIGN.md invariant 3, exactly-once materialization): a single
    /// multi() that creates the merged part, removes the sources, and checks the lease is still
    /// held at lease_version. Returns false (without throwing, without touching local state) if
    /// the lease was lost to another replica -- the caller must discard new_part in that case.
    /// Mirrors commitInsertedPart()'s Keeper-first pattern.
    bool commitMergedPart(
        MutableDataPartPtr & new_part, const DataPartsVector & source_parts,
        const String & lease_path, int32_t lease_version, ContextPtr local_context);

    /// Plain `OPTIMIZE TABLE t` only: synchronously selects and runs merges (via
    /// CloudMergePlainMergeTreeTask::executeHere) until nothing more is selectable. Every other
    /// form (PARTITION/FINAL/DEDUPLICATE/CLEANUP) throws NOT_IMPLEMENTED, same stub style as
    /// dropPart/dropPartition/attachPartition above.
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    /// ALTER TABLE ... UPDATE/DELETE: snapshots the current block-number watermark for every
    /// affected partition (a part with min_block below the snapshot existed before this call and
    /// needs the mutation; a later INSERT's part does not -- see CloudMergeTreeCoordination's
    /// class doc comment) and persists a Keeper-allocated mutations/<id> entry. Returns once
    /// durably recorded; does not wait for completion (mutations_sync is not implemented -- see
    /// DESIGN.md/the Phase 4 Step C plan for why this is a deliberate, documented scope cut, not
    /// an oversight).
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// The base MergeTreeData::checkMutationIsPossible() unconditionally rejects mutations on any
    /// disk with supportsHardLinks() == false -- true for every CloudMergeTree disk, since it's
    /// always plain_rewritable object storage. That check exists only to guard MutateTask's
    /// "reuse untouched columns via a real hardlink" optimization; getDefaultSettings() above
    /// forces always_use_copy_instead_of_hardlinks on for every CloudMergeTree table, which routes
    /// MutateTask to copy instead unconditionally, making the disk-capability check moot. No-op:
    /// CloudMergeTree does not yet support unique-key dedup-bypass rejection either (no unique key
    /// support at all yet), so there is nothing else from the base check to preserve.
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    /// MergeTreeData::checkAlterIsPossible() (~1000 lines: subscription checks, type-conversion
    /// legality, primary-key/partition-key column restrictions, statistics, indices, TTL -- all
    /// genuinely reusable and NOT reimplemented here) contains exactly one clause that doesn't
    /// apply to CloudMergeTree: it rejects any non-settings/comment ALTER outright on a disk with
    /// supportsHardLinks() == false (true for every CloudMergeTree disk), the same disk-capability
    /// guard checkMutationIsPossible() has above -- except unlike mutations, there is no
    /// MergeTreeSettings knob to route around it, because CloudMergeTree's ALTER (see alter()
    /// below) never touches part files at all for a metadata-only command: it's a pure Keeper CAS
    /// + local catalog update, so the restriction this guards against structurally cannot apply.
    /// With no seam to skip just that one clause inside a function this large, calls the base
    /// unchanged and swallows only that specific, narrowly-identified exception (matched by both
    /// error code and message substring, not code alone -- SUPPORT_IS_DISABLED is also used
    /// earlier in the same function for an unrelated text-index check that must still propagate).
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// ALTER TABLE ... ADD/DROP/MODIFY/RENAME COLUMN: CAS-writes the new column list to Keeper's
    /// metadata znode (fenced on the version this replica last saw), reloading and reapplying
    /// `params` on top of the latest columns text if another replica's ALTER won the race first.
    /// checkAlterIsPossible() above already validated `params` by this point -- invoked
    /// automatically by the standard AlterCommands validation path before this is ever called, same
    /// as every other MergeTree engine. A command requiring an actual data rewrite (checked via
    /// AlterCommands::getMutationCommands() returning non-empty, recomputed fresh on every retry
    /// attempt against that attempt's own Keeper read) additionally submits a mutations/<id> entry,
    /// committed atomically together with the metadata change via
    /// coordination.trySetMetadataAndCreateMutation() -- mirrors
    /// StorageReplicatedMergeTree::alter()'s own atomic-together shape (DESIGN.md invariant 3).
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    /// KILL MUTATION: removes the mutations/<mutation_id> znode outright (CloudMergeTree has no
    /// persistent per-engine mutation bookkeeping to reconcile the way StorageMergeTree's
    /// current_mutations_by_version or StorageReplicatedMergeTree's queue does -- every status/
    /// selection call already derives live state straight from Keeper via loadSortedMutations(),
    /// see getMutationsStatus()/selectPartsToMutate()). Once the znode is gone, no *new*
    /// CloudMergeMutateTask is ever selected for it; an already in-flight execution for it still
    /// finishes its current attempt rather than being interrupted mid-flight -- the same
    /// coarser-grained, best-effort cancellation model StorageMergeTree/StorageReplicatedMergeTree
    /// themselves settle for. Returns NotFound if the mutation was already gone (finished, or
    /// killed by a concurrent racer).
    CancellationCode killMutation(const String & mutation_id) override;

    /// SYSTEM STOP/START MERGES: wires ActionLocks::PartsMerge to merger_mutator.merges_blocker,
    /// same as StorageMergeTree::getActionLock(). Without this override, IStorage's default
    /// (returns an empty, no-op ActionLock for every action_type) makes STOP MERGES silently do
    /// nothing for CloudMergeTree -- scheduleDataProcessingJob() checks this same blocker before
    /// selecting merges *or* mutations (see its own comment), so this also pauses mutation
    /// execution, matching StorageMergeTree's own scheduleDataProcessingJob semantics exactly.
    /// PartsTTLMerge/PartsMove/Cleanup stay unwired (default no-op): CloudMergeTree has no
    /// TTL-driven merges, no multi-disk part moves, and no separate cleanup thread yet -- correctly
    /// reflecting the engine's actual current feature set, not an oversight.
    ActionLock getActionLock(StorageActionBlockType action_type) override;

    /// SYSTEM START MERGES: without this, background_operations_assignee stays idle until some
    /// unrelated event (e.g. the next INSERT) happens to trigger a scheduling cycle -- merges/
    /// mutations paused by STOP MERGES would resume only incidentally, not promptly. Matches
    /// StorageMergeTree::onActionLockRemove()'s identical trigger() call.
    void onActionLockRemove(StorageActionBlockType action_type) override;

private:
    MergeTreeDataWriter writer;
    CloudMergeTreeCoordination coordination;
    MergeTreeDataMergerMutator merger_mutator;

    /// Guards currently_merging_mutating_parts, same as StorageMergeTree -- these two live on
    /// StorageMergeTree, not MergeTreeData, so CloudMergeTree needs its own copies for Phase 2
    /// merge selection (CloudMergeTreeMergePredicate::canUsePartInMerges()).
    mutable std::mutex currently_processing_in_background_mutex;
    DataParts currently_merging_mutating_parts;

    std::atomic<bool> shutdown_called{false};

    /// The Keeper `parts` cversion this replica has last reconciled its working set against.
    /// Set from the initial Keeper-driven load in the constructor, advanced by the watcher.
    std::atomic<int32_t> current_parts_version{0};

    /// The Keeper `metadata` znode version (its own Stat.version, doubling as metadata_version --
    /// see CloudMergeTreeCoordination's class doc comment) this replica's in-memory metadata
    /// currently reflects. Seeded in the constructor from whatever Keeper already holds (not
    /// necessarily 0 -- see the constructor's comment on the documented ATTACH-doesn't-reconcile
    /// gap), advanced by alter() and by the watcher below.
    std::atomic<int32_t> current_metadata_version{0};

    /// Watches the Keeper `parts` set and reconciles data_parts_indexes against it, so parts
    /// inserted by another replica become visible here without a restart. Also watches the
    /// `metadata` znode (Phase 4 Step D) on the same cycle/callback -- see its doc comment for why
    /// this piggybacks on the existing task rather than adding a second one.
    BackgroundSchedulePoolTaskHolder part_set_updating_task;
    void updatePartSetFromKeeper();

    /// Phase 3 "parts killer": periodically scans dropped_parts/ (tombstones written atomically
    /// whenever a part leaves the active Keeper set -- merge sources, or a dropped table's parts)
    /// and physically deletes a tombstoned part's shared-storage objects once
    /// cloud_merge_tree_gc_grace_period_seconds has elapsed, per DESIGN.md invariant 2. Simple
    /// polling on cloud_merge_tree_gc_interval_ms, not watch-driven -- GC latency is bounded by
    /// the grace period regardless, unlike part_set_updating_task's correctness-critical immediacy.
    BackgroundSchedulePoolTaskHolder parts_killer_task;
    void runPartsKillerCycle();

    /// Shared primitive behind dropPartition/dropPart/truncate (Phase 4 Step A): removes every
    /// currently-active part whose name matches `predicate` via one or more coordination.tryRemoveParts()
    /// multi()s (each atomically deactivates + tombstones its parts, same as drop() already does for
    /// the whole table), then immediately reflects the removal in data_parts_indexes -- same
    /// removePartsFromWorkingSet() pattern updatePartSetFromKeeper() uses -- rather than waiting for
    /// the watcher's next cycle. Retries against a concurrent merge/DROP racing on an overlapping part:
    /// the multi() fails closed (ZNONODE on a source already removed elsewhere) and this reloads the
    /// live active set and rebuilds before trying again, same fail-closed/retry shape commitMergedPart's
    /// lease check already relies on for exactly-once materialization. Returns the number of parts removed.
    size_t removeActivePartsMatching(const std::function<bool(const String &)> & predicate);

    /// DETACH counterpart of removeActivePartsMatching: same bounded-retry shape, but deactivates
    /// via coordination.tryDetachParts() (records detached_parts/, not dropped_parts/) so the
    /// parts-killer GC task never touches these parts' shared-storage objects while detached. No
    /// local disk I/O -- unlike StorageReplicatedMergeTree's DETACH, there is nothing to clone or
    /// rename: CloudMergeTree parts have exactly one shared copy, already at its final path, and it
    /// simply stays there until ATTACH re-registers the same name in Keeper. Returns the number of
    /// parts detached.
    size_t detachActivePartsMatching(const std::function<bool(const String &)> & predicate);

    /// Serialize a part's columns+checksums into the header stored in its znode.
    String serializePartHeader(const DataPartPtr & part) const;

    /// Build a DataPart object from an on-disk directory that is already correctly named (no
    /// rename needed), without admitting it into the working set yet. Factored out of
    /// updatePartSetFromKeeper()'s adoption loop so attachPartition() can also use it -- it needs
    /// the built part's header (via serializePartHeader) to re-register the part in Keeper *before*
    /// admitting it locally. Returns nullptr if the directory isn't visible on this disk yet, or if
    /// loading fails -- see the extensive comments on the two checks this preserves, originally
    /// written for updatePartSetFromKeeper's try_adopt_part lambda.
    MutableDataPartPtr buildPartFromDisk(const String & name);

    /// Admit a part built by buildPartFromDisk() into the local working set (addTempPart +
    /// Transaction::commit). Caller must hold lock. Never fails: if something already active
    /// locally covers this part (e.g. a later merge result adopted earlier in the same batch), this
    /// name is already effectively satisfied and there is nothing more to do.
    ///
    /// Returns whatever Transaction::commit() itself returns: any local parts this admission
    /// covered/superseded. MergeTreeData::Transaction::commit() only ever demotes a covered part to
    /// Outdated with a future remove_time, relying on the generic old-parts cleanup thread to later
    /// erase it from data_parts_indexes -- a thread CloudMergeTree never runs (physical deletion is
    /// exclusively owned by the Keeper-driven parts-killer). Left alone, every merge/adoption would
    /// leak its covered parts' DataPart objects in memory forever. Callers are responsible for
    /// immediately transitioning the returned parts to Deleting and calling removePartsFinally()
    /// once their own lock is released (mirrors detachActivePartsMatching()'s and
    /// updatePartSetFromKeeper()'s identical treatment of explicitly-removed parts) -- this only
    /// forgets the DataPart *object*, it never touches the shared disk.
    DataPartsVector admitPartLocally(MutableDataPartPtr part, DataPartsLock & lock);

    /// Build a mutation entry ready to serialize into a mutations/<id> znode -- shared by mutate()
    /// (alter_version = -1, a manually-submitted mutation) and alter() (alter_version = the metadata
    /// znode's resulting version, for a mutation submitted atomically alongside an ALTER requiring a
    /// data rewrite). Snapshots the current block-number watermark for every partition `commands`
    /// affects (or every partition with active parts, if none named explicitly) via
    /// coordination.snapshotBlockNumbers() -- see CloudMergeTreeCoordination's class doc comment.
    ReplicatedMergeTreeMutationEntry buildMutationEntry(
        const MutationCommands & commands, ContextPtr local_context, int32_t alter_version);

    // --- MergeTreeData pure virtuals ---
    // Phase 0: implemented minimally or stubbed (NOT_IMPLEMENTED) until later phases.
    MutationCounters getMutationCounters() const override;
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;
    CursorPromotersMap buildPromoters() override;

    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    void attachRestoredParts(MutableDataPartsVector && parts, const std::optional<ZooKeeperRetriesInfo> & zookeeper_retries_info) override;
    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    /// Merge selection + lease acquisition, run together under currently_processing_in_background_mutex
    /// (lock already held by the caller) so a MergeTask (space reservation, part tagging) is never
    /// built for a range whose lease already belongs to someone else. Losing the lease race to a
    /// faster replica surfaces as SelectMergeFailure::Reason::NOTHING_TO_MERGE -- an expected,
    /// benign outcome each scheduling cycle, not an error worth logging above trace level.
    std::expected<CloudMergeMutateSelectedEntryPtr, SelectMergeFailure> selectPartsToMerge(
        const StorageMetadataPtr & metadata_snapshot, std::unique_lock<std::mutex> & lock, bool aggressive = false);

    /// Mutation counterpart of selectPartsToMerge(), tried by scheduleDataProcessingJob() only when
    /// merge selection found nothing this cycle (same two-phase shape StorageMergeTree's own
    /// scheduleDataProcessingJob already uses). One mutation version applied per selected part per
    /// call -- see the Phase 4 Step C plan's "explicit scope cut" on batching. Reuses
    /// CloudCurrentlyMergingPartsTagger unchanged: it only ever tags/untags future_part->parts, with
    /// no merge-specific assumption baked in, so a part already claimed by a merge (or vice versa) is
    /// correctly rejected by both selectors sharing the one currently_merging_mutating_parts set.
    std::expected<CloudMutateSelectedEntryPtr, SelectMergeFailure> selectPartsToMutate(
        const StorageMetadataPtr & metadata_snapshot, std::unique_lock<std::mutex> & lock);

    friend class CloudMergeTreeSink;
    friend class CloudMergeTreeMergePredicate;
    friend class CloudMergePlainMergeTreeTask;
    friend class CloudMergeMutateTask;
    friend struct CloudCurrentlyMergingPartsTagger;

    struct MutationsSnapshot;
};

}
