#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>

namespace DB
{

/** Reserves space and tags future_part->parts as currently-merging, mirroring
  * StorageMergeTree's CurrentlyMergingPartsTagger, trimmed to the merge-only case (no
  * is_mutation branch -- CloudMergeTree has no mutations yet) and to single-disk reservation
  * (CloudMergeTree requires exactly one shared disk in its storage policy -- see the constructor
  * check in StorageCloudMergeTree.cpp -- so there is no multi-volume/TTL balancing decision to
  * make, unlike StorageMergeTree::balancedReservation()/tryReserveSpacePreferringTTLRules()).
  *
  * Like the original, the constructor assumes currently_processing_in_background_mutex is
  * already held by the caller (constructed from within
  * StorageCloudMergeTree::selectPartsToMerge(), which runs under that lock); only finalize()
  * acquires its own lock, since it runs later, after the original lock has been released.
  */
struct CloudCurrentlyMergingPartsTagger
{
    FutureMergedMutatedPartPtr future_part;
    ReservationSharedPtr reserved_space;
    StorageCloudMergeTree & storage;
    bool finalized{false};

    CloudCurrentlyMergingPartsTagger(
        FutureMergedMutatedPartPtr future_part_,
        size_t total_size,
        StorageCloudMergeTree & storage_);

    void finalize();
    ~CloudCurrentlyMergingPartsTagger();
};

using CloudCurrentlyMergingPartsTaggerPtr = std::unique_ptr<CloudCurrentlyMergingPartsTagger>;

/// CloudMergeTree's equivalent of MergeMutateSelectedEntry. Not reusable as-is: that struct's
/// `tagger` field is hard-typed to CurrentlyMergingPartsTaggerPtr (a StorageMergeTree type), and
/// it carries mutation-specific fields (commands, mutation_ids) CloudMergeTree doesn't have.
/// Carries the lease acquired for this merge instead.
struct CloudMergeMutateSelectedEntry
{
    FutureMergedMutatedPartPtr future_part;
    CloudCurrentlyMergingPartsTaggerPtr tagger;
    String lease_path;
    int32_t lease_version;
    bool finalized{false};

    void finalize();
    ~CloudMergeMutateSelectedEntry();
};

using CloudMergeMutateSelectedEntryPtr = std::shared_ptr<CloudMergeMutateSelectedEntry>;

/** CloudMergeTree's merge execution task, modeled on MergePlainMergeTreeTask's 3-stage
  * IExecutableTask shape. Reuses MergeTreeDataMergerMutator::mergePartsToTemporaryPart()/MergeTask
  * verbatim for the actual I/O (NEED_EXECUTE); the one substantive difference is finish(), which
  * replaces the local-only renameMergedTemporaryPart()+Transaction::commit() with a Keeper-fenced
  * commit via StorageCloudMergeTree::commitMergedPart() -- discarding the part on a lost lease
  * race instead of crashing or silently keeping it. See README.md invariant 3.
  *
  * Also heartbeats the lease during NEED_EXECUTE so a merge that's still genuinely in progress
  * doesn't get its lease stolen out from under it by another replica's staleness check.
  */
class CloudMergePlainMergeTreeTask : public IExecutableTask
{
public:
    CloudMergePlainMergeTreeTask(
        StorageCloudMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        CloudMergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback & task_result_callback_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        bool cleanup_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , cleanup(cleanup_)
    {
        for (auto & item : merge_mutate_entry->future_part->parts)
            priority.value += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + merge_mutate_entry->future_part->name; }

    void cancel() noexcept override;

private:
    void prepare();
    void finish();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,

        SUCCESS,
    };

    State state{State::NEED_PREPARE};

    StorageCloudMergeTree & storage;

    StorageMetadataPtr metadata_snapshot;
    CloudMergeMutateSelectedEntryPtr merge_mutate_entry{nullptr};
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    Priority priority;

    std::function<void(const ExecutionStatus &)> write_part_log;
    std::function<void()> transfer_profile_counters_to_initial_query;
    IExecutableTask::TaskResultCallback task_result_callback;
    MergeTaskPtr merge_task{nullptr};

    /// OPTIMIZE TABLE ... DEDUPLICATE [BY ...] / CLEANUP: threaded through as constructor
    /// arguments rather than fields on CloudMergeMutateSelectedEntry, same shape
    /// MergePlainMergeTreeTask uses -- selection is purely about which parts, these decide how to
    /// merge them. StorageCloudMergeTree::optimize()'s synchronous loop passes real values;
    /// scheduleDataProcessingJob()'s background path always passes false/{}/false, since ordinary
    /// background merging must never deduplicate or cleanup on its own.
    bool deduplicate{false};
    Names deduplicate_by_columns;
    bool cleanup{false};

    ProfileEvents::Counters profile_counters;

    ContextMutablePtr task_context;

    /// Last heartbeat-refreshed lease version; updated by touchLease() during NEED_EXECUTE and
    /// used as the fencing version for the final commit.
    int32_t current_lease_version{0};
    UInt64 last_heartbeat_ms{0};

    /// Set when NEED_EXECUTE bails into NEED_FINISH via a lost lease rather than merge_task
    /// actually completing -- see finish()'s own doc comment on why this must be checked before
    /// touching merge_task->getFuture().
    bool lease_lost = false;

    /// Bumps the lease's mtime/version so another replica's staleness check won't steal it out
    /// from under an in-progress merge. Returns false if the lease was already stolen (ZBADVERSION)
    /// -- the caller must abort immediately in that case.
    bool heartbeatLeaseIfDue();

    ContextMutablePtr createTaskContext() const;
};

using CloudMergePlainMergeTreeTaskPtr = std::shared_ptr<CloudMergePlainMergeTreeTask>;

/// Drives a task to completion inline instead of scheduling it on the background pool -- used by
/// StorageCloudMergeTree::optimize() so a plain OPTIMIZE TABLE finishes synchronously. Mirrors
/// executeHere(MergePlainMergeTreeTaskPtr) in MergePlainMergeTreeTask.h.
[[ maybe_unused ]] static void executeHere(CloudMergePlainMergeTreeTaskPtr task)
{
    while (task->executeStep()) {}
}

}
