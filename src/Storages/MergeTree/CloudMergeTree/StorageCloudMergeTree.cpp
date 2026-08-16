#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSink.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/IDisk.h>
#include <Common/logger_useful.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
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

        part_set_updating_task = getContext()->getSchedulePool().createTask(
            getStorageID(), getStorageID().getFullTableName() + " (CloudMergeTree::partSetUpdatingTask)",
            [this] { updatePartSetFromKeeper(); });
        part_set_updating_task->deactivate();
    }
    else
    {
        loadDataParts(LoadingStrictnessLevel::FORCE_RESTORE <= mode, std::nullopt);
    }

    increment.set(getMaxBlockNumber());
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

void StorageCloudMergeTree::startup()
{
    if (isStaticStorage())
        return;

    clearEmptyParts();
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    part_set_updating_task->activateAndSchedule();
}

void StorageCloudMergeTree::shutdown(bool)
{
    if (shutdown_called.exchange(true))
        return;

    if (part_set_updating_task)
        part_set_updating_task->deactivate();

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
    auto zk = getZooKeeper();

    Transaction transaction(*this, local_context->getCurrentTransaction().get());
    {
        auto lock = lockParts();

        /// Assign the durable part name / block number. lockParts() above serializes this against
        /// every other commitInsertedPart() call on this replica, so allocation through the
        /// Keeper commit below is single-writer for as long as there is only one replica (Phase 0).
        /// Phase 1 (second replica) needs Keeper-side fencing here, not just this in-process lock.
        part->info.min_block = part->info.max_block = increment.get();
        part->setName(part->getNewName(part->info));

        /// Keeper-first: the part is only active once its znode exists in the canonical set.
        auto code = coordination.tryCommitInsert(zk, part->name, serializePartHeader(part));
        if (code == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Part {} already exists in the Keeper part set", part->name);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot register part {} in Keeper", part->name);

        /// Only now reflect it in this replica's in-memory cache.
        renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
        transaction.commit(lock);
    }
}

void StorageCloudMergeTree::drop()
{
    shutdown(true);

    /// Remove the canonical part set from Keeper, then the data. Object GC ownership lands in
    /// Phase 3; for Phase 0 a DROP of the whole table removes its data directly.
    if (!isStaticStorage())
    {
        auto zk = getZooKeeper();

        int32_t version = 0;
        auto names = coordination.loadActivePartNames(zk, version);
        if (!names.empty())
        {
            auto code = coordination.tryRemoveParts(zk, names);
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException(code, "Cannot remove parts from Keeper while dropping table {}", getStorageID().getNameForLogs());
        }
    }

    dropAllData();
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

bool StorageCloudMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee &)
{
    /// Phase 0: no background merges/mutations. Merge assignment via Keeper leases is Phase 2.
    return false;
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
    auto zk = getZooKeeper();

    int32_t new_version = 0;
    Strings active_names = coordination.loadActivePartNames(zk, new_version, part_set_updating_task->getWatchCallback());

    if (new_version == current_parts_version.load())
        return;

    std::unordered_set<std::string> active_set(active_names.begin(), active_names.end());

    auto lock = lockParts();
    /// lockParts() already holds the exclusive lock; the no-argument overload would try to take
    /// its own shared lock on the same non-recursive data_parts_mutex and deadlock this thread
    /// against itself. Pass the held lock through instead.
    auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);

    DataPartsVector to_remove;
    for (const auto & part : known)
        if (!active_set.contains(part->name))
            to_remove.push_back(part);

    if (!to_remove.empty())
        removePartsFromWorkingSet(/*txn=*/ nullptr, to_remove, /*clear_without_timeout=*/ false, lock);

    std::unordered_set<std::string> known_names;
    for (const auto & part : known)
        known_names.insert(part->name);

    auto disks = getStoragePolicy()->getDisks();
    const auto & disk = disks.front();

    /// Another replica may have written new part directories to the shared disk since our last
    /// refresh. Object-storage metadata layers that cache a listing in memory (e.g. plain_rewritable)
    /// won't see them otherwise -- this is a no-op for disks whose in-memory state can't go stale.
    disk->refresh(0);

    for (const auto & name : active_names)
    {
        if (known_names.contains(name))
            continue;

        /// Another replica registered this part in Keeper and wrote it to the shared disk; build
        /// the part object from the on-disk directory (already named exactly `name`, no rename
        /// needed) and admit it into the active set. loadPartAndFixMetadataImpl() throws naturally
        /// if the directory is absent or unreadable -- fail-closed for free.
        ///
        /// Defensive: if the writer ever persists a txn_version.txt for this part (e.g. because
        /// it ran under a real transaction), drop it before building the part object below --
        /// DataPartBuilder would otherwise read that foreign creation TID into memory, and
        /// loadPartAndFixMetadataImpl()'s own txn_version.txt cleanup runs too late to undo it.
        /// Matches the same ordering MergeTreeData uses for its own attach-from-disk path.
        disk->removeFileIfExists(std::filesystem::path(getRelativeDataPath()) / name / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);

        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + name, disk, 0);
        auto part = getDataPartBuilder(name, single_disk_volume, name, getReadSettings())
            .withPartFormatFromDisk()
            .build();
        loadPartAndFixMetadataImpl(part, getContext());

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
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Part {} from the Keeper part set is covered by an already-active part; "
                "this should be impossible before Phase 2 merges exist", name);
        transaction.commit(lock);
    }

    current_parts_version.store(new_version);
}
catch (const Coordination::Exception & e)
{
    tryLogCurrentException(getLogger("StorageCloudMergeTree"), __PRETTY_FUNCTION__);
    if (Coordination::isHardwareError(e.code))
        part_set_updating_task->scheduleAfter(10000);
    else
        throw;
}

}
