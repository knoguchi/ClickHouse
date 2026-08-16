#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeCoordination.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <expected>

namespace DB
{

/// Defined in CloudMergePlainMergeTreeTask.h. Only forward-declared here: that header includes
/// this one (it needs the full StorageCloudMergeTree type for CloudCurrentlyMergingPartsTagger),
/// so including it back would be circular.
struct CloudMergeMutateSelectedEntry;
using CloudMergeMutateSelectedEntryPtr = std::shared_ptr<CloudMergeMutateSelectedEntry>;

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
    void commitInsertedPart(MutableDataPartPtr & part, ContextPtr context);

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

    /// Watches the Keeper `parts` set and reconciles data_parts_indexes against it, so parts
    /// inserted by another replica become visible here without a restart.
    BackgroundSchedulePoolTaskHolder part_set_updating_task;
    void updatePartSetFromKeeper();

    /// Serialize a part's columns+checksums into the header stored in its znode.
    String serializePartHeader(const DataPartPtr & part) const;

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

    friend class CloudMergeTreeSink;
    friend class CloudMergeTreeMergePredicate;
    friend class CloudMergePlainMergeTreeTask;
    friend struct CloudCurrentlyMergingPartsTagger;

    struct MutationsSnapshot;
};

}
