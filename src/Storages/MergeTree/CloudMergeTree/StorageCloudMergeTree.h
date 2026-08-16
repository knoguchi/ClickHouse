#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeCoordination.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/SimpleIncrement.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

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

    /// The part-set coordinator and a freshly-resolved Keeper session. Used by the sink.
    const CloudMergeTreeCoordination & getCoordination() const { return coordination; }
    zkutil::ZooKeeperPtr getZooKeeper() const;

    /// Commit a freshly written part: register it in Keeper, then flip it Active in the cache.
    /// Throws on a Keeper failure so the INSERT fails closed (no silent local-only commit).
    void commitInsertedPart(MutableDataPartPtr & part, ContextPtr context);

private:
    MergeTreeDataWriter writer;
    CloudMergeTreeCoordination coordination;

    /// Phase 0/1: single-writer block-number allocation, serialized by lockParts() in
    /// commitInsertedPart(). Multi-writer allocation via Keeper arrives in Phase 2 together
    /// with merges.
    SimpleIncrement increment;

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

    /// Phase 0: no background merge/mutation scheduling yet.
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    friend class CloudMergeTreeSink;

    struct MutationsSnapshot;
};

}
