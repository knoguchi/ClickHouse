#include <Storages/MergeTree/CloudMergeTree/CloudMergePlainMergeTreeTask.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadFuzzer.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// How often (at most) to bump the lease's mtime/version during a merge's NEED_EXECUTE loop.
/// executeStep() is called far more often than this by the cooperative scheduler; there's no
/// value in hitting Keeper on every single call.
static constexpr UInt64 LEASE_HEARTBEAT_INTERVAL_MS = 10000;

CloudCurrentlyMergingPartsTagger::CloudCurrentlyMergingPartsTagger(
    FutureMergedMutatedPartPtr future_part_, size_t total_size, StorageCloudMergeTree & storage_)
    : future_part(std::move(future_part_)), storage(storage_)
{
    /// CloudMergeTree requires exactly one (shared) disk in its storage policy -- see the
    /// constructor check in StorageCloudMergeTree.cpp -- so there is no multi-volume/TTL
    /// balancing decision to make here, unlike StorageMergeTree's CurrentlyMergingPartsTagger.
    reserved_space = storage.getStoragePolicy()->reserveAndCheck(total_size);

    future_part->updatePath(storage, reserved_space.get());

    /// Assume currently_processing_in_background_mutex is already held by the caller (this is
    /// constructed from within StorageCloudMergeTree::selectPartsToMerge(), which runs under
    /// that lock) -- mirrors StorageMergeTree's CurrentlyMergingPartsTagger exactly.
    for (const auto & part : future_part->parts)
    {
        if (storage.currently_merging_mutating_parts.contains(part))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tagging already tagged part {}. This is a bug.", part->name);
    }
    storage.currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());
}

void CloudCurrentlyMergingPartsTagger::finalize()
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);
    finalized = true;

    for (const auto & part : future_part->parts)
        storage.currently_merging_mutating_parts.erase(part);
}

CloudCurrentlyMergingPartsTagger::~CloudCurrentlyMergingPartsTagger()
{
    if (!finalized)
        finalize();
}

void CloudMergeMutateSelectedEntry::finalize()
{
    finalized = true;
    if (tagger)
        tagger->finalize();
}

CloudMergeMutateSelectedEntry::~CloudMergeMutateSelectedEntry()
{
    if (!finalized)
        finalize();
}

StorageID CloudMergePlainMergeTreeTask::getStorageID() const
{
    return storage.getStorageID();
}

void CloudMergePlainMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool CloudMergePlainMergeTreeTask::executeStep()
{
    auto component_guard = Coordination::setCurrentComponent("CloudMergePlainMergeTreeTask::executeStep");
    ProfileEventsScope profile_events_scope(&profile_counters);

    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
        switcher.emplace((*merge_list_entry)->thread_group, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            try
            {
                if (!heartbeatLeaseIfDue())
                {
                    /// Another replica's staleness check stole this lease out from under us --
                    /// stop immediately rather than finish work that can never commit.
                    write_part_log(ExecutionStatus(0, "Lost the merge lease to another replica (went stale)"));
                    lease_lost = true;
                    state = State::NEED_FINISH;
                    return true;
                }

                if (merge_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception is in merge_task.");
                write_part_log(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
        }
        case State::NEED_FINISH:
        {
            finish();

            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }
}

bool CloudMergePlainMergeTreeTask::heartbeatLeaseIfDue()
{
    UInt64 now_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    if (last_heartbeat_ms != 0 && now_ms - last_heartbeat_ms < LEASE_HEARTBEAT_INTERVAL_MS)
        return true;

    auto zk = storage.getZooKeeper();
    auto result = storage.getCoordination().touchLease(zk, merge_mutate_entry->lease_path, current_lease_version);
    if (!result)
        return false;

    current_lease_version = *result;
    merge_mutate_entry->lease_version = current_lease_version;
    last_heartbeat_ms = now_ms;
    return true;
}

void CloudMergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();
    current_lease_version = merge_mutate_entry->lease_version;

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(storage.getStorageID(), future_part, task_context);

    storage.writePartLog(
        PartLogElement::MERGE_PARTS_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {}, {}, {});

    write_part_log = [this](const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        auto projections_duration_ms = merge_task ? merge_task->grabProjectionsMergeTime() : std::map<String, UInt64>{};
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot),
            {},
            projections_duration_ms);
    };

    transfer_profile_counters_to_initial_query = [this, query_thread_group = CurrentThread::getGroup()]()
    {
        if (query_thread_group)
        {
            auto task_thread_group = (*merge_list_entry)->thread_group;
            auto task_counters_snapshot = task_thread_group->performance_counters.getPartiallyAtomicSnapshot();

            auto & query_counters = query_thread_group->performance_counters;
            for (ProfileEvents::Event i = ProfileEvents::Event(0); i < ProfileEvents::end(); ++i)
                query_counters.incrementNoTrace(i, task_counters_snapshot[i]);
        }
    };

    /// txn=nullptr: CloudMergeTree has no MVCC transactions. deduplicate/deduplicate_by_columns/
    /// cleanup come from the constructor -- real values for an explicit OPTIMIZE TABLE ...
    /// DEDUPLICATE/CLEANUP (StorageCloudMergeTree::optimize()'s synchronous loop), always
    /// false/{}/false for ordinary background merging (scheduleDataProcessingJob()).
    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
        future_part,
        metadata_snapshot,
        merge_list_entry.get(),
        {} /* projection_merge_list_element */,
        table_lock_holder,
        time(nullptr),
        task_context,
        merge_mutate_entry->tagger->reserved_space,
        deduplicate,
        deduplicate_by_columns,
        cleanup,
        storage.merging_params,
        /*txn=*/ nullptr);
}

void CloudMergePlainMergeTreeTask::finish()
{
    if (lease_lost)
    {
        /// merge_task never ran to completion when NEED_EXECUTE bailed here via a lost lease --
        /// its std::future promise is only fulfilled on successful completion (MergeTask::execute()
        /// last iteration), so the unconditional getFuture().get() below would block this thread
        /// forever waiting for a result that will never arrive. Nothing was produced to commit;
        /// release merge_task and finalize, mirroring cancel()'s own cleanup.
        if (merge_task)
            merge_task->cancel();
        merge_mutate_entry->finalize();
        return;
    }

    new_part = merge_task->getFuture().get();

    bool committed = storage.commitMergedPart(
        new_part, future_part->parts, merge_mutate_entry->lease_path, current_lease_version, task_context);

    if (!committed)
    {
        /// Lost the commit race (or the lease): discard our output, touch nothing local. The
        /// winner's version of this merge is (or will be) reflected via the part-set watcher.
        new_part->removeIfNeeded();
        write_part_log(ExecutionStatus(0, "Lost the merge commit race"));
        merge_mutate_entry->finalize();
        return;
    }

    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    auto prewarm_caches = storage.getCachesToPrewarm(new_part->getBytesUncompressedOnDisk());

    if (prewarm_caches.mark_cache)
    {
        auto marks = merge_task->releaseCachedMarks();
        addMarksToCache(*new_part, marks, prewarm_caches.mark_cache.get());
    }

    if (prewarm_caches.index_mark_cache)
    {
        auto index_marks = merge_task->releaseCachedIndexMarks();
        addMarksToCache(*new_part, index_marks, prewarm_caches.index_mark_cache.get());
    }

    if (prewarm_caches.primary_index_cache)
        new_part->moveIndexToCache(*prewarm_caches.primary_index_cache);

    write_part_log({});

    StorageCloudMergeTree::incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();

    merge_mutate_entry->finalize();
}

void CloudMergePlainMergeTreeTask::cancel() noexcept
{
    auto component_guard = Coordination::setCurrentComponent("CloudMergePlainMergeTreeTask::cancel");
    if (merge_task)
        merge_task->cancel();

    if (new_part)
        new_part->removeIfNeeded();

    /// Destroy here (not just let it go out of scope) so its RAII temp-directory guard releases
    /// before merge_mutate_entry->finalize() below, same reasoning as MergePlainMergeTreeTask.
    merge_task.reset();

    if (merge_mutate_entry)
        merge_mutate_entry->finalize();
}

ContextMutablePtr CloudMergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext()->getBackgroundContext());
    context->makeQueryContextForMerge(*storage.getSettings());
    auto query_id = getQueryId();
    context->setCurrentQueryId(query_id);
    return context;
}

}
