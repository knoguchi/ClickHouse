#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSink.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreePartsCollector.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/IDisk.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Core/UUID.h>
#include <Core/ServerUUID.h>
#include <base/defines.h>
#include <chrono>
#include <filesystem>

namespace ProfileEvents
{
    extern const Event MergesRejectedByMemoryLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_lease_staleness_ms;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_gc_grace_period_seconds;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_gc_interval_ms;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
}

/// Minimal mutations snapshot: CloudMergeTree has no mutations in Phase 0, so the snapshot is
/// always empty. MutationsSnapshotBase already implements the patch/flags accessors.
struct StorageCloudMergeTree::MutationsSnapshot final : public MergeTreeData::MutationsSnapshotBase
{
    MutationsSnapshot() = default;

    MutationCommands getOnFlyMutationCommandsForPart(const DataPartPtr & /*part*/) const override { return {}; }
    NameSet getAllUpdatedColumns() const override { return {}; }
    std::shared_ptr<IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<MutationsSnapshot>(); }
};

StorageCloudMergeTree::StorageCloudMergeTree(
    const String & zookeeper_root_,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(settings_),
          false, /// require_part_metadata
          mode)
    , writer(*this)
    , coordination(zookeeper_root_)
    , merger_mutator(*this)
{
    /// Phase 1: every disk in the storage policy must be a single, shared (remote) disk. This
    /// keeps "which disk is a Keeper-known part on" unambiguous for the watcher below, and is
    /// the structural precondition for "no local copy" in DESIGN.md's Phase 1 description.
    if (!isStaticStorage())
    {
        auto storage_policy = getStoragePolicy();
        const auto & disks = storage_policy->getDisks();
        if (disks.size() != 1 || !disks.front()->isRemote())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CloudMergeTree requires a storage policy with exactly one remote (shared) disk");
    }

    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, date_column_name);

    if (!isStaticStorage())
    {
        auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::StorageCloudMergeTree");

        /// Keeper is authoritative for which parts are active (invariant 1 in DESIGN.md): load
        /// only the parts Keeper knows about, not whatever a local directory listing turns up.
        auto zk = getZooKeeper();
        coordination.createRootNodes(zk);

        int32_t loaded_version = 0;
        Strings active_names = coordination.loadActivePartNames(zk, loaded_version);
        std::unordered_set<std::string> expected_parts(active_names.begin(), active_names.end());

        loadDataParts(LoadingStrictnessLevel::FORCE_RESTORE <= mode, expected_parts);

        /// loadDataParts() only classifies on-disk-but-unexpected parts; it does not error when
        /// an expected (Keeper-active) name never materializes locally. Silently proceeding would
        /// mean SELECT quietly returns fewer rows than the committed set, so check explicitly.
        std::unordered_set<std::string> loaded_names;
        for (const auto & part : getDataPartsVectorForInternalUsage())
            loaded_names.insert(part->name);

        for (const auto & name : expected_parts)
            if (!loaded_names.contains(name))
                throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                    "Part {} is active in the Keeper part set at {} but is missing from every disk "
                    "in the storage policy", name, coordination.partPath(name));

        current_parts_version.store(loaded_version);

        part_set_updating_task = getContext()->getSchedulePool()->createTask(
            getStorageID(), getStorageID().getFullTableName() + " (CloudMergeTree::partSetUpdatingTask)",
            [this] { updatePartSetFromKeeper(); });
        part_set_updating_task->deactivate();

        parts_killer_task = getContext()->getSchedulePool()->createTask(
            getStorageID(), getStorageID().getFullTableName() + " (CloudMergeTree::partsKillerTask)",
            [this] { runPartsKillerCycle(); });
        parts_killer_task->deactivate();
    }
    else
    {
        loadDataParts(LoadingStrictnessLevel::FORCE_RESTORE <= mode, std::nullopt);
    }
}

zkutil::ZooKeeperPtr StorageCloudMergeTree::getZooKeeper() const
{
    auto zk = getContext()->getZooKeeper();
    if (!zk)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "CloudMergeTree requires a configured Keeper/ZooKeeper");
    return zk;
}

String StorageCloudMergeTree::serializePartHeader(const DataPartPtr & part) const
{
    return ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(part->getColumns(), part->checksums).toString();
}

UInt32 StorageCloudMergeTree::getMaxLevelInBetween(const PartProperties & left, const PartProperties & right) const
{
    auto parts_lock = readLockParts();

    auto begin = data_parts_by_info.find(left.info);
    if (begin == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find left part, left part {}. It's a bug", left.name);

    auto end = data_parts_by_info.find(right.info);
    if (end == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find right part, right part {}. It's a bug", right.name);

    UInt32 level = 0;

    for (auto it = begin++; it != end; ++it)
    {
        if (it == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "left and right parts in the wrong order, left part {}, right part {}. It's a bug", left.name, right.name);

        level = std::max(level, (*it)->info.level);
    }

    return level;
}

void StorageCloudMergeTree::startup()
{
    if (isStaticStorage())
        return;

    clearEmptyParts();
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    part_set_updating_task->activateAndSchedule();
    parts_killer_task->activateAndSchedule();
    background_operations_assignee.start();
}

void StorageCloudMergeTree::shutdown(bool)
{
    if (shutdown_called.exchange(true))
        return;

    if (part_set_updating_task)
        part_set_updating_task->deactivate();

    if (parts_killer_task)
        parts_killer_task->deactivate();

    background_operations_assignee.finish();

    stopOutdatedAndUnexpectedDataPartsLoadingTask();
}

void StorageCloudMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    /// Phase 0: the local read path only. Sparse primary index, skip indexes, projections and
    /// FINAL all come for free from the shared MergeTree read machinery.
    QueryPlanPtr plan = MergeTreeDataSelectExecutor(*this).read(
        column_names,
        storage_snapshot,
        query_info,
        local_context,
        max_block_size,
        num_streams,
        local_context->getPartitionIdToMaxBlock(getStorageID().uuid),
        /*enable_parallel_reading=*/ false);

    if (plan)
        query_plan = std::move(*plan);
}

SinkToStoragePtr StorageCloudMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    const auto & settings = (*getSettings());
    return std::make_shared<CloudMergeTreeSink>(
        *this, metadata_snapshot, settings[MergeTreeSetting::parts_to_throw_insert], local_context);
}

void StorageCloudMergeTree::commitInsertedPart(MutableDataPartPtr & part, ContextPtr local_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::commitInsertedPart");
    auto zk = getZooKeeper();
    auto zk_fault = std::make_shared<ZooKeeperWithFaultInjection>(zk);

    /// Multi-writer block-number allocation: an ephemeral-sequential node under
    /// block_numbers/<partition_id>/ is the counter, so two replicas inserting into the same
    /// partition at once always get distinct numbers -- no in-process lock can provide that
    /// across replicas. Mirrors StorageReplicatedMergeTree::allocateBlockNumber.
    const String partition_id = part->info.getPartitionId();
    coordination.ensureBlockNumbersPartition(zk, partition_id);

    auto block_lock = createEphemeralLockInZooKeeper(
        coordination.blockNumbersPartitionPath(partition_id) + "/block-",
        coordination.tempPath(),
        zk_fault,
        /*deduplication_paths=*/{},
        /*znode_data=*/std::nullopt);

    part->info.min_block = part->info.max_block = block_lock.getNumber();
    part->setName(part->getNewName(part->info));

    /// The lock's unlock op rides along in the same multi() as the part commit below, so
    /// allocation and commit land atomically together (mirrors ReplicatedMergeTreeSink::commitPart).
    Coordination::Requests extra_ops;
    block_lock.getUnlockOp(extra_ops);

    Transaction transaction(*this, local_context->getCurrentTransaction().get());
    {
        auto lock = lockParts();

        /// Keeper-first: the part is only active once its znode exists in the canonical set.
        auto code = coordination.tryCommitInsert(zk, part->name, serializePartHeader(part), extra_ops);
        if (code == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Part {} already exists in the Keeper part set", part->name);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot register part {} in Keeper", part->name);
        block_lock.assumeUnlocked();

        /// Only now reflect it in this replica's in-memory cache.
        renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
        transaction.commit(lock);
    }
}

bool StorageCloudMergeTree::commitMergedPart(
    MutableDataPartPtr & new_part, const DataPartsVector & source_parts,
    const String & lease_path, int32_t lease_version, ContextPtr /*local_context*/)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::commitMergedPart");
    auto zk = getZooKeeper();

    Strings source_names;
    source_names.reserve(source_parts.size());
    for (const auto & part : source_parts)
        source_names.push_back(part->name);

    /// Keeper-first, exactly-once: a single multi() creates the merged part, removes the
    /// sources, and checks the lease is still ours at lease_version. If another replica's lease
    /// won this range in the meantime, the check fails and the whole multi() fails with it --
    /// we lost the race, so touch nothing local and let the caller discard new_part.
    auto code = coordination.tryCommitMerge(
        zk, new_part->name, serializePartHeader(new_part), source_names, lease_path, lease_version);
    if (code != Coordination::Error::ZOK)
    {
        LOG_DEBUG(getLogger("StorageCloudMergeTree"), "Lost the merge commit race for {}: {}", new_part->name, code);
        return false;
    }

    coordination.releaseLease(zk, lease_path, lease_version); /// best-effort; we won, safe to release now

    /// No outer lockParts() here: renameTempPartAndReplace()/Transaction::commit() each acquire
    /// their own lock internally, same as MergeTreeDataMergerMutator::renameMergedTemporaryPart()
    /// does for the ordinary (non-Cloud) merge path. Wrapping them in an extra lockParts() would
    /// self-deadlock, the same trap fixed in updatePartSetFromKeeper() earlier.
    Transaction transaction(*this, nullptr);
    renameTempPartAndReplace(new_part, transaction, /*rename_in_transaction=*/ true);
    transaction.renameParts();
    transaction.commit();
    return true;
}

std::string StorageCloudMergeTree::getPostfixForTempInsertName() const
{
    return toString(UUIDHelpers::generateV4());
}

std::optional<UInt64> StorageCloudMergeTree::totalBytesUncompressed(const Settings &) const
{
    UInt64 res = 0;
    for (const auto & part : getDataPartsForInternalUsage())
        res += part->getBytesUncompressedOnDisk();
    return res;
}

void StorageCloudMergeTree::drop()
{
    shutdown(true);

    /// Remove the canonical part set from Keeper synchronously -- every replica gets its own
    /// independent DROP TABLE query, and this is cheap/fast regardless of how many replicas run
    /// it concurrently (each part's remove-and-tombstone multi() is independent). Physical
    /// shared-storage deletion is NOT done here: S3 deletion is slow, and per DESIGN.md invariant
    /// 2 it must go through the same grace-period-gated parts-killer GC task as merge-source
    /// cleanup (see tryRemoveParts, which now tombstones every part it deactivates). The GC task
    /// also removes the table's own root directory once every part -- active or tombstoned -- has
    /// drained, gated on the `dropped` marker written below.
    if (!isStaticStorage())
    {
        auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::drop");
        auto zk = getZooKeeper();

        int32_t version = 0;
        auto names = coordination.loadActivePartNames(zk, version);
        if (!names.empty())
        {
            auto code = coordination.tryRemoveParts(zk, names);
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException(code, "Cannot remove parts from Keeper while dropping table {}", getStorageID().getNameForLogs());
        }

        auto code = coordination.markTableDropped(zk);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot mark table {} as dropped in Keeper", getStorageID().getNameForLogs());
    }

    /// Local-only teardown: nothing here touches shared storage.
    auto lock = lockParts();
    data_parts_indexes.clear();
    unregisterFromMergeSelection(getSettings());
}

void StorageCloudMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TRUNCATE is not implemented for CloudMergeTree yet");
}

StorageCloudMergeTree::MutationsSnapshotPtr StorageCloudMergeTree::getMutationsSnapshot(const IMutationsSnapshot::Params & /*params*/) const
{
    return std::make_shared<MutationsSnapshot>();
}

CursorPromotersMap StorageCloudMergeTree::buildPromoters()
{
    const auto data_parts = getDataPartsVectorForInternalUsage();
    std::map<String, PartBlockNumberRanges> partition_ranges;
    for (const auto & part : data_parts)
        partition_ranges[part->info.getPartitionId()].addPart(part->info.min_block, part->info.max_block);
    return constructPromoters(/*committing_block_numbers=*/{}, std::move(partition_ranges));
}

std::unique_ptr<MergeTreeSettings> StorageCloudMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

std::expected<CloudMergeMutateSelectedEntryPtr, SelectMergeFailure> StorageCloudMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & /*metadata_snapshot*/, std::unique_lock<std::mutex> & lock, bool aggressive)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::selectPartsToMerge");
    auto zk = getZooKeeper();

    /// Selecting against a locally stale view is dangerous, not just wasteful: the merge predicate's
    /// gap check (getMaxLevelInBetween) only ever sees locally-known parts, so a replica that hasn't
    /// yet adopted a part someone else committed can select a merge that silently skips over it --
    /// producing a result whose name (min_block..max_block) claims a block range it doesn't actually
    /// fully own. When the skipped part is later merged by anyone, the two results' names collide
    /// even though their real source parts never overlapped (reproduced under concurrent-insert +
    /// concurrent-merge stress: two disjoint-source merges named as if their ranges overlapped,
    /// tripping "Part X intersects part Y (state Active). It is a bug."). Comparing against Keeper's
    /// current parts-version (cheap: no children listing) catches the case where our watcher hasn't
    /// caught up yet, which current_parts_version alone can't -- it only reflects the last version we
    /// fully reconciled, not whether Keeper has since moved on.
    if (coordination.getPartsVersion(zk) != current_parts_version.load())
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::NOTHING_TO_MERGE,
            .explanation = PreformattedMessage::create(
                "Local part-set view is behind Keeper; skipping merge selection until the watcher catches up"),
        });

    auto merge_predicate = std::make_shared<CloudMergeTreeMergePredicate>(*this, lock);
    auto parts_collector = std::make_shared<CloudMergeTreePartsCollector>(*this, merge_predicate);

    if (!canEnqueueBackgroundTask())
    {
        ProfileEvents::increment(ProfileEvents::MergesRejectedByMemoryLimit);
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create(
                "Current background tasks memory usage ({}) is more than the limit ({})",
                formatReadableSizeWithBinarySuffix(background_memory_tracker.get()),
                formatReadableSizeWithBinarySuffix(background_memory_tracker.getSoftLimit())),
        });
    }

    UInt64 max_source_parts_bytes_for_merge = CompactionStatistics::getMaxSourcePartsBytesForMerge(*this);
    UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(*this);

    if (max_source_parts_bytes_for_merge == 0)
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create("Current value of max_source_parts_bytes is zero"),
        });

    auto select_result = merger_mutator.selectPartsToMerge(
        parts_collector,
        merge_predicate,
        MergeSelectorApplier(
            /*merge_constraints=*/{{max_source_parts_bytes_for_merge, max_result_part_rows}},
            /*merge_with_ttl_allowed=*/false, /// CloudMergeTree has no TTL-driven merges yet
            aggressive,
            /*range_filter_=*/nullptr,
            /*storage_id_=*/getStorageID()),
        /*partitions_hint=*/std::nullopt);

    if (!select_result.has_value())
        return std::unexpected(select_result.error());

    chassert(select_result.value().size() == 1);
    MergeSelectorChoice choice = std::move(select_result.value()[0]);

    auto future_part = constructFuturePart(*this, choice, {DataPartState::Active});
    if (!future_part)
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create(
                "Can't construct future part from source parts. Probably there was a drop part/partition user query."),
        });

    /// Lease acquisition is part of selection, not execution: don't reserve space or tag parts
    /// (CloudCurrentlyMergingPartsTagger below) for a range whose lease already belongs to another
    /// replica. A lost race here is NOTHING_TO_MERGE, not an error -- see DESIGN.md invariant 3 and
    /// the decentralized-selection design note at the top of this file's plan.
    const String lease_path = coordination.leasePath(future_part->name);
    const String holder_data = toString(ServerUUID::get());
    const Int64 staleness_ms = static_cast<Int64>((*getSettings())[MergeTreeSetting::cloud_merge_tree_lease_staleness_ms]);

    auto lease = coordination.acquireOrStealLease(zk, lease_path, holder_data, staleness_ms);
    if (!lease)
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::NOTHING_TO_MERGE,
            .explanation = PreformattedMessage::create(
                "Lease for {} is already held by another replica: {}", future_part->name, lease.error()),
        });

    try
    {
        uint64_t needed_disk_space = CompactionStatistics::estimateNeededDiskSpace(future_part->parts, true);
        auto tagger = std::make_unique<CloudCurrentlyMergingPartsTagger>(future_part, needed_disk_space, *this);

        auto entry = std::make_shared<CloudMergeMutateSelectedEntry>();
        entry->future_part = future_part;
        entry->tagger = std::move(tagger);
        entry->lease_path = lease->path;
        entry->lease_version = lease->version;
        return entry;
    }
    catch (...)
    {
        /// Reservation/tagging failed after we already won the lease -- release it so we don't
        /// starve every other replica of this range until the staleness threshold elapses.
        coordination.releaseLease(zk, lease->path, lease->version);
        throw;
    }
}

bool StorageCloudMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called)
        return false;

    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);

    CloudMergeMutateSelectedEntryPtr merge_entry;
    {
        std::unique_lock lock(currently_processing_in_background_mutex);
        auto merge_select_result = selectPartsToMerge(metadata_snapshot, lock);
        if (merge_select_result)
            merge_entry = std::move(merge_select_result.value());
        else
            LOG_TRACE(getLogger("StorageCloudMergeTree"), "Didn't start merge: {}", merge_select_result.error().explanation.text);
    }

    if (!merge_entry)
        return false;

    auto task = std::make_shared<CloudMergePlainMergeTreeTask>(
        *this, metadata_snapshot, merge_entry, table_lock_holder, common_assignee_trigger);
    return assignee.scheduleMergeMutateTask(task);
}

bool StorageCloudMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /*deduplicate_by_columns*/,
    bool cleanup,
    ContextPtr local_context)
{
    /// Only the plain form is implemented -- PARTITION/FINAL/DEDUPLICATE/CLEANUP all need
    /// machinery CloudMergeTree doesn't have yet (partition-scoped selection, mutations,
    /// replacing-merge cleanup). Same stub style as dropPart/dropPartition/attachPartition above.
    if (partition || final || deduplicate || cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "OPTIMIZE TABLE with PARTITION, FINAL, DEDUPLICATE or CLEANUP is not implemented for "
            "CloudMergeTree yet; only a plain OPTIMIZE TABLE is supported");

    auto metadata_snapshot = getInMemoryMetadataPtr(local_context, false);

    /// Synchronous loop: run selection+execution inline (no background pool involved) until
    /// there's genuinely nothing left to merge, so OPTIMIZE has deterministic, testable
    /// completion instead of racing background scheduling timing.
    while (true)
    {
        auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

        CloudMergeMutateSelectedEntryPtr merge_entry;
        {
            std::unique_lock lock(currently_processing_in_background_mutex);
            /// aggressive=true: an explicit OPTIMIZE is a direct user request to consolidate now,
            /// not passive background upkeep -- it should be willing to merge parts of mismatched
            /// sizes that the normal cost function would rather wait on. Matches
            /// StorageMergeTree::optimize(), which always calls merge() with aggressive=true for
            /// both the FINAL and plain forms. Without this, OPTIMIZE behaved identically to
            /// background scheduling and could sit on a converged-enough-looking set of small,
            /// unevenly-sized parts (e.g. one merged 6-row part plus two 1-row parts) far longer
            /// than callers reasonably wait for a supposedly-forced OPTIMIZE to finish.
            auto merge_select_result = selectPartsToMerge(metadata_snapshot, lock, /*aggressive=*/true);
            if (!merge_select_result)
            {
                /// Both real convergence ("no need to merge parts according to merge selector
                /// algorithm", reported as CANNOT_SELECT -- selectPartsToMerge's top-level
                /// SelectMergeFailure::Reason never actually comes back as NOTHING_TO_MERGE for
                /// the whole-table case, only its inner detail does) and a lost lease race are
                /// unremarkable stopping points for a plain OPTIMIZE, not failures: whatever could
                /// be merged already has been. Log for visibility and stop the loop successfully.
                LOG_TRACE(getLogger("StorageCloudMergeTree"), "Stopping OPTIMIZE: {}", merge_select_result.error().explanation.text);
                return true;
            }
            merge_entry = std::move(merge_select_result.value());
        }

        IExecutableTask::TaskResultCallback f = [](bool) {};
        auto task = std::make_shared<CloudMergePlainMergeTreeTask>(*this, metadata_snapshot, merge_entry, table_lock_holder, f);
        executeHere(task);
    }
}

void StorageCloudMergeTree::startBackgroundMovesIfNeeded()
{
}

MutationCounters StorageCloudMergeTree::getMutationCounters() const
{
    return {};
}

std::map<std::string, MutationCommands> StorageCloudMergeTree::getUnfinishedMutationCommands() const
{
    return {};
}

std::vector<MergeTreeMutationStatus> StorageCloudMergeTree::getMutationsStatus() const
{
    return {};
}

bool StorageCloudMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr &) const
{
    return false;
}

void StorageCloudMergeTree::attachRestoredParts(MutableDataPartsVector &&, const std::optional<ZooKeeperRetriesInfo> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RESTORE is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::dropPartNoWaitNoThrow(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PART is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::dropPart(const String &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PART is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::dropPartition(const ASTPtr &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PARTITION is not implemented for CloudMergeTree yet");
}

PartitionCommandsResultInfo StorageCloudMergeTree::attachPartition(const PartitionCommand &, const StorageMetadataPtr &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ATTACH PARTITION is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REPLACE PARTITION is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION is not implemented for CloudMergeTree yet");
}

void StorageCloudMergeTree::updatePartSetFromKeeper()
try
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::updatePartSetFromKeeper");
    auto zk = getZooKeeper();

    auto lock = lockParts();
    /// lockParts() already holds the exclusive lock; the no-argument overload would try to take
    /// its own shared lock on the same non-recursive data_parts_mutex and deadlock this thread
    /// against itself. Pass the held lock through instead.
    auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);

    /// The Keeper read must happen after taking `known`, while still holding lockParts() -- not
    /// before, as it originally did. Every part in `known` was added by a commitInsertedPart()
    /// call that writes to Keeper and then adds locally, both under this same lock; reading
    /// Keeper strictly after `known` is captured guarantees active_names is causally at least as
    /// fresh, so it can never be missing a part that's already visible locally. Reading Keeper
    /// first left a race window: a concurrent commitInsertedPart() could land its part locally
    /// between that early read and this lock acquisition, and the stale active_names would then
    /// make a perfectly valid, still-active part look removed from Keeper -- evicted from the
    /// working set below, permanently, since known_names was also built from the pre-removal
    /// snapshot and so the adoption loop thought the evicted part was still known and never
    /// re-added it. Concurrent multi-writer INSERT reproduced this within seconds.
    int32_t new_version = 0;
    Strings active_names = coordination.loadActivePartNames(zk, new_version, part_set_updating_task->getWatchCallback());

    if (new_version == current_parts_version.load())
        return;

    std::unordered_set<std::string> active_set(active_names.begin(), active_names.end());

    /// Parts no longer active in Keeper are only removed from the local working set after the
    /// adoption loop below has confirmed every currently-active part is present locally (see
    /// all_adopted) -- not upfront. A part superseded by a merge is atomically replaced by its
    /// adoption below (Transaction::commit() recomputes and removes whatever the newly-adopted
    /// part covers), so removing it here first, before its replacement is adopted, used to open a
    /// window where a concurrent query could see neither the old parts (already removed) nor
    /// their replacement (not yet adopted) -- reproduced as SELECT sum()/count() briefly
    /// returning fewer rows than truly exist, down to 0 when one merge consolidated everything.
    std::unordered_set<std::string> known_names;
    for (const auto & part : known)
        known_names.insert(part->name);

    auto disks = getStoragePolicy()->getDisks();
    const auto & disk = disks.front();

    /// Try to build and admit one adopted part. Returns false (instead of throwing) for the
    /// "not visible on this disk's in-memory listing yet" case specifically, so the caller can
    /// refresh and retry without losing track of every *other* name still pending in this batch.
    auto try_adopt_part = [&](const String & name) -> bool
    {
        /// Another replica registered this part in Keeper and wrote it to the shared disk; build
        /// the part object from the on-disk directory (already named exactly `name`, no rename
        /// needed) and admit it into the active set.
        ///
        /// Must check existence here, before touching anything else: loadPartAndFixMetadataImpl()
        /// does NOT fail closed for an absent directory. It unconditionally calls
        /// writeInvalidatedSystemColumnsFile() first, which *writes* into the part's directory --
        /// and for plain_rewritable storage, writing into a directory this replica's local metadata
        /// snapshot doesn't know about yet silently creates it, with a fresh random remote key, at
        /// the shared logical path. That races the real writer's concurrent rename-into-place: two
        /// physical directories briefly both claim the same logical part path, and a later
        /// disk->refresh() can rebuild the in-memory tree with the bogus empty one winning,
        /// permanently orphaning the real data behind FILE_DOESNT_EXIST. Reproduced end-to-end via
        /// concurrent multi-replica INSERT. Checking existence first keeps this path read-only
        /// when the part genuinely isn't visible yet, so the caller's refresh-and-retry has nothing
        /// destructive to undo.
        if (!disk->existsDirectory(std::filesystem::path(getRelativeDataPath()) / name))
            return false;

        /// Defensive: if the writer ever persists a txn_version.txt for this part (e.g. because
        /// it ran under a real transaction), drop it before building the part object below --
        /// DataPartBuilder would otherwise read that foreign creation TID into memory, and
        /// loadPartAndFixMetadataImpl()'s own txn_version.txt cleanup runs too late to undo it.
        /// Matches the same ordering MergeTreeData uses for its own attach-from-disk path.
        disk->removeFileIfExists(std::filesystem::path(getRelativeDataPath()) / name / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);

        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + name, disk, 0);
        auto part = getDataPartBuilder(name, single_disk_volume, name, getReadSettings(), PartDirIntent::OpenExisting)
            .withPartFormatFromDisk()
            .build();

        try
        {
            loadPartAndFixMetadataImpl(part, getContext());
        }
        catch (const Exception &)
        {
            return false;
        }

        /// A freshly-built part's in-memory VersionMetadata is never lazily loaded from disk --
        /// IMergeTreeDataPart::version::getInfo() just returns whatever's already in memory, which
        /// for a brand-new DataPartBuilder object is a zero-initialized (all-default) TransactionID,
        /// not Tx::NonTransactionalTID. That default fails isNonTransactional(), so the commit/rollback
        /// path below tries to resolve it via the global TransactionLog -- which this replica may
        /// never have touched before and can fail to construct. CloudMergeTree has no MVCC
        /// transactions anywhere (writers already stamp every part Tx::NonTransactionalTID; see
        /// MergeTreeDataWriter), so make the adopted part consistent with that before it's committed.
        part->version->setAndStoreCreationTID(Tx::NonTransactionalTID, nullptr);

        Transaction transaction(*this, nullptr);
        if (!addTempPart(part, transaction, lock, /*out_covered_parts=*/ nullptr))
        {
            /// Something already active locally covers this part -- e.g. a later merge result
            /// adopted earlier in this same batch already supersedes it. Not an error: this name
            /// is already effectively satisfied, nothing more to do for it.
            return true;
        }
        transaction.commit(lock);
        return true;
    };

    /// Object-storage metadata layers that cache a listing in memory (e.g. plain_rewritable) may
    /// not know about a part another replica just wrote. Rather than proactively refreshing the
    /// whole in-memory directory tree on every watch trigger, only refresh reactively, once, right
    /// when a specific part turns out to be missing -- a resync a moment later is harmless, a
    /// proactive refresh on every trigger is wasted work most of the time.
    ///
    /// This used to be able to race destructively against this same process's own concurrent
    /// INSERT/merge temp-directory writes: disk->refresh() is a full clobber-and-rebuild from one
    /// remote-listing snapshot, and a temp directory created after that snapshot's listing started
    /// (but before it finished) wasn't in it yet, so refresh() would evict it from the cache as if
    /// it had been deleted, corrupting that unrelated write's later rename-to-final-name step
    /// ("Directory ... does not exist"). Reproduced independently on plain MergeTree via SYSTEM
    /// RESTART DISK racing a concurrent INSERT -- a core plain_rewritable bug, not
    /// CloudMergeTree-specific. Confirmed fixed upstream as of the 2026-08 rebase onto
    /// upstream/master: MetadataStorageFromPlainRewritableObjectStorageTransaction::commit() and
    /// refresh()/load() now share metadata_mutex for the whole operation, fully serializing refresh
    /// against every commit, so this call is safe as-is.
    bool all_adopted = true;
    for (const auto & name : active_names)
    {
        if (known_names.contains(name))
            continue;

        if (try_adopt_part(name))
            continue;

        disk->refresh(1000);
        if (!try_adopt_part(name))
        {
            /// Still not visible (or genuinely broken) -- leave it out of known_names/this
            /// replica's working set for now and retry on the next cycle rather than crash the
            /// whole reconciliation over one part; the others in this batch already succeeded.
            all_adopted = false;
            LOG_DEBUG(getLogger("StorageCloudMergeTree"),
                "Part {} is active in Keeper but not yet adoptable from the shared disk; will retry", name);
        }
    }

    /// Only advance the version watermark if every part in this batch was actually adopted --
    /// otherwise the early-return check at the top would skip retrying the stragglers on a future
    /// call where the Keeper version hasn't changed further. Force a retry soon instead.
    if (all_adopted)
    {
        /// Safe to drop local parts no longer active in Keeper only now, with every part Keeper
        /// currently considers active confirmed present locally (adopted above, or already
        /// known): anything superseded by a part adopted above was already atomically removed by
        /// that adoption's own Transaction::commit(), so it can't appear here. Whatever remains
        /// has no replacement coming this cycle and can be dropped outright.
        auto still_known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);
        DataPartsVector to_remove;
        for (const auto & part : still_known)
            if (!active_set.contains(part->name))
                to_remove.push_back(part);

        if (!to_remove.empty())
            removePartsFromWorkingSet(/*txn=*/ nullptr, to_remove, /*clear_without_timeout=*/ false, lock);

        current_parts_version.store(new_version);
    }
    else
        part_set_updating_task->scheduleAfter(1000);
}
catch (const Coordination::Exception & e)
{
    tryLogCurrentException(getLogger("StorageCloudMergeTree"), __PRETTY_FUNCTION__);
    if (Coordination::isHardwareError(e.code))
        part_set_updating_task->scheduleAfter(10000);
    else
        throw;
}

void StorageCloudMergeTree::runPartsKillerCycle()
try
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::runPartsKillerCycle");
    auto zk = getZooKeeper();

    const auto settings = getSettings();
    const Int64 grace_period_ms = static_cast<Int64>((*settings)[MergeTreeSetting::cloud_merge_tree_gc_grace_period_seconds]) * 1000;
    const UInt64 interval_ms = (*settings)[MergeTreeSetting::cloud_merge_tree_gc_interval_ms];
    const Int64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto disks = getStoragePolicy()->getDisks();
    const auto & disk = disks.front();

    for (const auto & tombstone : coordination.listTombstones(zk))
    {
        if (now_ms - tombstone.dropped_at_ms < grace_period_ms)
            continue;

        /// Defensive, close to a no-op in practice: leases are keyed by merge *result* names,
        /// never by source names, so a live lease essentially never covers a tombstoned source
        /// part. Kept as a cheap trip-wire, not real protection -- the grace period above is
        /// what actually guards against a concurrent reader on another replica.
        if (zk->exists(coordination.leasePath(tombstone.part_name)))
            continue;

        if (!coordination.tryClaimTombstoneForDeletion(zk, tombstone.part_name))
            continue; /// another replica's GC cycle claimed it first this round

        try
        {
            auto part_path = std::filesystem::path(getRelativeDataPath()) / tombstone.part_name;
            if (disk->existsDirectory(part_path))
                disk->removeRecursive(part_path);
            coordination.releaseTombstone(zk, tombstone.part_name);
        }
        catch (...)
        {
            coordination.releaseTombstoneClaim(zk, tombstone.part_name);
            tryLogCurrentException(getLogger("StorageCloudMergeTree"),
                fmt::format("Failed to physically delete tombstoned part {}, will retry", tombstone.part_name));
        }
    }

    /// Trailing table-directory teardown: once DROP TABLE has been issued (the `dropped` marker
    /// exists, written by drop()) and every part -- active or tombstoned -- has drained, the
    /// table's own root directory is safe to remove. Mirrors dropAllData()'s own sequence
    /// exactly (format_version.txt, then detached/moving, then the now-flat root) rather than a
    /// single removeRecursive() on the still-nested root directly: that blanket call was tried
    /// first and reproducibly orphaned the underlying S3 objects -- plain_rewritable's
    /// remove-recursive "move to a garbage name, then finalize the delete" sequence renamed the
    /// directory (making it logically unreachable) but the objects themselves were never
    /// actually deleted, silently leaking them forever. Test A's per-part removeRecursive calls
    /// never hit this because a part directory has no nested subdirectories; the table root does
    /// (detached/), which is what the flat sequence below avoids by removing each piece before
    /// the final recursive call ever sees anything nested under it.
    int32_t unused_version = 0;
    if (zk->exists(coordination.dropMarkerPath())
        && coordination.loadActivePartNames(zk, unused_version).empty()
        && coordination.listTombstones(zk).empty())
    {
        auto table_path = getRelativeDataPath();
        if (disk->existsDirectory(table_path))
        {
            disk->removeFileIfExists(std::filesystem::path(table_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME);

            auto detached_path = std::filesystem::path(table_path) / MergeTreeData::DETACHED_DIR_NAME;
            if (disk->existsDirectory(detached_path))
                disk->removeRecursive(detached_path);

            auto moving_path = std::filesystem::path(table_path) / MergeTreeData::MOVING_DIR_NAME;
            if (disk->existsDirectory(moving_path))
                disk->removeRecursive(moving_path);

            disk->removeRecursive(table_path);
        }
    }

    parts_killer_task->scheduleAfter(interval_ms);
}
catch (const Coordination::Exception & e)
{
    tryLogCurrentException(getLogger("StorageCloudMergeTree"), __PRETTY_FUNCTION__);
    parts_killer_task->scheduleAfter(Coordination::isHardwareError(e.code) ? 10000 : (*getSettings())[MergeTreeSetting::cloud_merge_tree_gc_interval_ms]);
}
catch (...)
{
    tryLogCurrentException(getLogger("StorageCloudMergeTree"), __PRETTY_FUNCTION__);
    parts_killer_task->scheduleAfter((*getSettings())[MergeTreeSetting::cloud_merge_tree_gc_interval_ms]);
}

}
