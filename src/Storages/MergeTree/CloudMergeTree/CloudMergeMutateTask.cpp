#include <Storages/MergeTree/CloudMergeTree/CloudMergeMutateTask.h>
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

/// Same interval as CloudMergePlainMergeTreeTask's own LEASE_HEARTBEAT_INTERVAL_MS -- see its
/// doc comment, identical reasoning applies here.
static constexpr UInt64 MUTATE_LEASE_HEARTBEAT_INTERVAL_MS = 10000;

void CloudMutateSelectedEntry::finalize()
{
    finalized = true;
    if (tagger)
        tagger->finalize();
}

CloudMutateSelectedEntry::~CloudMutateSelectedEntry()
{
    if (!finalized)
        finalize();
}

StorageID CloudMergeMutateTask::getStorageID() const
{
    return storage.getStorageID();
}

void CloudMergeMutateTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool CloudMergeMutateTask::executeStep()
{
    auto component_guard = Coordination::setCurrentComponent("CloudMergeMutateTask::executeStep");
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
                    write_part_log(ExecutionStatus(0, "Lost the mutation lease to another replica (went stale)"));
                    lease_lost = true;
                    state = State::NEED_FINISH;
                    return true;
                }

                if (mutate_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception is in mutate_task.");
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

bool CloudMergeMutateTask::heartbeatLeaseIfDue()
{
    UInt64 now_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    if (last_heartbeat_ms != 0 && now_ms - last_heartbeat_ms < MUTATE_LEASE_HEARTBEAT_INTERVAL_MS)
        return true;

    auto zk = storage.getZooKeeper();
    auto result = storage.getCoordination().touchLease(zk, mutate_entry->lease_path, current_lease_version);
    if (!result)
        return false;

    current_lease_version = *result;
    mutate_entry->lease_version = current_lease_version;
    last_heartbeat_ms = now_ms;
    return true;
}

void CloudMergeMutateTask::prepare()
{
    future_part = mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();
    current_lease_version = mutate_entry->lease_version;

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(storage.getStorageID(), future_part, task_context);

    storage.writePartLog(
        PartLogElement::MUTATE_PART_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {}, {mutate_entry->mutation_id}, {});

    write_part_log = [this](const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MUTATE_PART,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot),
            {mutate_entry->mutation_id},
            {});
    };

    /// txn=nullptr: CloudMergeTree has no MVCC transactions anywhere (see the same note in
    /// StorageCloudMergeTree.cpp's part-adoption path) -- every part is stamped Tx::NonTransactionalTID.
    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
        future_part,
        metadata_snapshot,
        mutate_entry->commands,
        merge_list_entry.get(),
        time(nullptr),
        task_context,
        /*txn=*/ nullptr,
        mutate_entry->tagger->reserved_space,
        table_lock_holder);
}

void CloudMergeMutateTask::finish()
{
    if (lease_lost)
    {
        /// mutate_task never ran to completion when NEED_EXECUTE bailed here via a lost lease --
        /// its std::future promise is only fulfilled on successful completion (MutateTask::execute()
        /// last iteration), so the unconditional getFuture().get() below would block this thread
        /// forever waiting for a result that will never arrive. Nothing was produced to commit;
        /// release mutate_task and finalize, mirroring cancel()'s own cleanup.
        if (mutate_task)
            mutate_task->cancel();
        mutate_entry->finalize();
        return;
    }

    new_part = mutate_task->getFuture().get();

    bool committed = storage.commitMergedPart(
        new_part, future_part->parts, mutate_entry->lease_path, current_lease_version, task_context);

    if (!committed)
    {
        /// Lost the commit race (or the lease): discard our output, touch nothing local. Some
        /// other replica's version of this same mutation (or a later one covering it) is or will
        /// be reflected via the part-set watcher.
        new_part->removeIfNeeded();
        write_part_log(ExecutionStatus(0, "Lost the mutation commit race"));
        mutate_entry->finalize();
        return;
    }

    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    write_part_log({});

    mutate_entry->finalize();
}

void CloudMergeMutateTask::cancel() noexcept
{
    auto component_guard = Coordination::setCurrentComponent("CloudMergeMutateTask::cancel");
    if (mutate_task)
        mutate_task->cancel();

    if (new_part)
        new_part->removeIfNeeded();

    mutate_task.reset();

    if (mutate_entry)
        mutate_entry->finalize();
}

ContextMutablePtr CloudMergeMutateTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext()->getBackgroundContext());
    context->makeQueryContextForMerge(*storage.getSettings());
    auto query_id = getQueryId();
    context->setCurrentQueryId(query_id);
    return context;
}

}
