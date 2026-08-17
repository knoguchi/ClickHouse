#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergePlainMergeTreeTask.h>

namespace DB
{

/// CloudMergeTree's equivalent of MergeMutateSelectedEntry for the mutation case (one source part,
/// one target mutation id/commands). Mirrors CloudMergeMutateSelectedEntry (CloudMergePlainMergeTreeTask.h)
/// plus the fields a mutation needs that a merge doesn't: which mutation is being applied and its
/// commands. Reuses CloudCurrentlyMergingPartsTagger unchanged -- see its own doc comment and
/// StorageCloudMergeTree::selectPartsToMutate()'s.
struct CloudMutateSelectedEntry
{
    FutureMergedMutatedPartPtr future_part;
    CloudCurrentlyMergingPartsTaggerPtr tagger;
    String lease_path;
    int32_t lease_version;
    MutationCommandsConstPtr commands;
    String mutation_id;
    bool finalized{false};

    void finalize();
    ~CloudMutateSelectedEntry();
};

using CloudMutateSelectedEntryPtr = std::shared_ptr<CloudMutateSelectedEntry>;

/** CloudMergeTree's mutation execution task, mirroring CloudMergePlainMergeTreeTask almost exactly
  * (same 3-stage IExecutableTask shape, same lease-heartbeat-during-NEED_EXECUTE, same
  * lost-race/lost-lease handling in finish()). The one substantive difference: prepare() calls
  * MergeTreeDataMergerMutator::mutatePartToTemporaryPart() instead of mergePartsToTemporaryPart() --
  * confirmed storage-agnostic and reusable as-is (see the Phase 4 Step C plan's Context section).
  * finish() reuses StorageCloudMergeTree::commitMergedPart() completely unchanged: a 1-source-part
  * mutation commit is not a distinct Keeper multi() shape from an N-source-part merge commit.
  */
class CloudMergeMutateTask : public IExecutableTask
{
public:
    CloudMergeMutateTask(
        StorageCloudMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        CloudMutateSelectedEntryPtr mutate_entry_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , mutate_entry(std::move(mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
    {
        for (auto & item : mutate_entry->future_part->parts)
            priority.value += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + mutate_entry->future_part->name; }

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
    CloudMutateSelectedEntryPtr mutate_entry{nullptr};
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    Priority priority;

    std::function<void(const ExecutionStatus &)> write_part_log;
    IExecutableTask::TaskResultCallback task_result_callback;
    MutateTaskPtr mutate_task{nullptr};

    ProfileEvents::Counters profile_counters;

    ContextMutablePtr task_context;

    /// Same fencing as CloudMergePlainMergeTreeTask -- see its own field doc comment.
    int32_t current_lease_version{0};
    UInt64 last_heartbeat_ms{0};

    /// Set when NEED_EXECUTE bails into NEED_FINISH via a lost lease rather than mutate_task
    /// actually completing -- see finish()'s own doc comment on why this must be checked before
    /// touching mutate_task->getFuture().
    bool lease_lost = false;

    bool heartbeatLeaseIfDue();

    ContextMutablePtr createTaskContext() const;
};

using CloudMergeMutateTaskPtr = std::shared_ptr<CloudMergeMutateTask>;

/// Mirrors executeHere(CloudMergePlainMergeTreeTaskPtr) in CloudMergePlainMergeTreeTask.h -- not
/// currently used (OPTIMIZE has no mutation-equivalent synchronous entry point yet), kept for
/// symmetry and potential future use (e.g. a synchronous mutations_sync path).
[[ maybe_unused ]] static void executeHere(CloudMergeMutateTaskPtr task)
{
    while (task->executeStep()) {}
}

}
