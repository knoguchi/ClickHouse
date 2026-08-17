#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSink.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreePartsCollector.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergePlainMergeTreeTask.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeMutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/Settings.h>
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
#include <Interpreters/InsertDeduplication.h>
#include <Core/DeduplicateInsert.h>
#include <Core/UUID.h>
#include <Core/ServerUUID.h>
#include <IO/ReadHelpers.h>
#include <base/defines.h>
#include <algorithm>
#include <chrono>
#include <filesystem>

namespace ProfileEvents
{
    extern const Event MergesRejectedByMemoryLimit;
}

namespace DB
{

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int UNKNOWN_POLICY;
    extern const int ABORTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_lease_staleness_ms;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_gc_grace_period_seconds;
    extern const MergeTreeSettingsUInt64 cloud_merge_tree_gc_interval_ms;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsBool allow_experimental_replacing_merge_with_cleanup;
}

namespace Setting
{
    extern const SettingsBool optimize_skip_merged_partitions;
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

        /// Idempotent: the first replica to CREATE (or ATTACH) wins and establishes version 0;
        /// every later replica's call is a no-op. Every replica -- winner or not -- then validates
        /// its own metadata_ (from its own CREATE/ATTACH statement) against whatever Keeper actually
        /// holds, mirroring StorageReplicatedMergeTree::checkTableStructureAttempt: a mismatch is a
        /// real error (stale copy-pasted ATTACH, or one written before another replica's ALTER), not
        /// something to silently paper over. current_metadata_version is stamped from Keeper's actual
        /// current version regardless of who won the initial race, and this replica's in-memory
        /// metadata_version is corrected to match so parts it writes stamp metadata_version.txt
        /// correctly. Only later ALTERs are picked up incrementally via the watcher below.
        coordination.ensureInitialMetadata(zk, metadata_.getColumns().toString(/*include_comments=*/ true));

        auto [canonical_columns_text, canonical_metadata_version] = coordination.getMetadata(zk);
        auto canonical_columns = ColumnsDescription::parse(canonical_columns_text);
        if (!(canonical_columns == metadata_.getColumns()))
            throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
                "Table columns structure in Keeper is different from local table structure for table {}. "
                "Local columns:\n{}\nKeeper columns:\n{}",
                table_id_.getNameForLogs(), metadata_.getColumns().toString(/*include_comments=*/ true), canonical_columns_text);

        current_metadata_version.store(canonical_metadata_version);
        if (canonical_metadata_version != metadata_.getMetadataVersion())
        {
            auto fixed_metadata = metadata_;
            fixed_metadata.setMetadataVersion(canonical_metadata_version);
            setInMemoryMetadata(fixed_metadata);
        }

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

StorageCloudMergeTree::MutableDataPartPtr StorageCloudMergeTree::buildPartFromDisk(const String & name)
{
    auto disks = getStoragePolicy()->getDisks();
    const auto & disk = disks.front();

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
        return nullptr;

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
        return nullptr;
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

    return part;
}

DataPartsVector StorageCloudMergeTree::admitPartLocally(MutableDataPartPtr part, DataPartsLock & lock)
{
    Transaction transaction(*this, nullptr);
    if (!addTempPart(part, transaction, lock, /*out_covered_parts=*/ nullptr))
    {
        /// Something already active locally covers this part -- e.g. a later merge result
        /// adopted earlier in this same batch already supersedes it. Not an error: this name
        /// is already effectively satisfied, nothing more to do for it.
        return {};
    }
    return transaction.commit(lock);
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

bool StorageCloudMergeTree::commitInsertedPart(MutableDataPartPtr & part, ContextPtr local_context)
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

    /// Whole-part-content insert dedup, same semantics as insert_deduplicate on
    /// ReplicatedMergeTree: identical block content (e.g. a client retry after a timeout) becomes
    /// a silent no-op rather than a duplicate row. Reuses the exact same hash and Keeper CAS
    /// primitives ReplicatedMergeTreeSink does (see DeduplicationHash, Interpreters/InsertDeduplication.h).
    const bool dedup_enabled = isDeduplicationEnabledForInsert(/*is_async_insert=*/ false, local_context->getSettingsRef());
    std::optional<DeduplicationHash> dedup_hash;
    std::vector<String> deduplication_paths;
    if (dedup_enabled)
    {
        dedup_hash.emplace(DeduplicationHash::createUnifiedHash(part->checksums.getTotalChecksumUInt128(), partition_id));
        deduplication_paths.push_back(dedup_hash->getPath(coordination.getRootPath()));
    }

    auto block_lock = createEphemeralLockInZooKeeper(
        coordination.blockNumbersPartitionPath(partition_id) + "/block-",
        coordination.tempPath(),
        zk_fault,
        deduplication_paths,
        /*znode_data=*/std::nullopt);

    if (!block_lock.isLocked())
    {
        /// Pre-check found this content hash already committed by some other part -- discard
        /// ours. Never registered in Keeper or the local cache, so there's nothing to unwind
        /// beyond the on-disk temp directory (same situation CloudMergePlainMergeTreeTask uses
        /// removeIfNeeded() for: a part that lost its race to ever become active).
        LOG_DEBUG(getLogger("StorageCloudMergeTree"), "INSERT of part {} was deduplicated (pre-check): {}",
            part->name, block_lock.getConflictPath());
        part->removeIfNeeded();
        return false;
    }

    part->info.min_block = part->info.max_block = block_lock.getNumber();
    part->setName(part->getNewName(part->info));

    /// The dedup-path create (if any) always goes first, so its index within tryCommitInsert's
    /// full multi() is fixed at 1 (index 0 is always the part znode itself) -- required to tell a
    /// dedup collision apart from a genuine part-name collision below. The lock's unlock op rides
    /// along in the same multi() as the part commit, so allocation and commit land atomically
    /// together (mirrors ReplicatedMergeTreeSink::commitPart).
    Coordination::Requests extra_ops;
    if (dedup_enabled)
        extra_ops.emplace_back(zkutil::makeCreateRequest(deduplication_paths.front(), part->name, zkutil::CreateMode::Persistent));
    block_lock.getUnlockOp(extra_ops);

    Transaction transaction(*this, local_context->getCurrentTransaction().get());
    DataPartsVector covered_parts;
    {
        auto lock = lockParts();

        /// Keeper-first: the part is only active once its znode exists in the canonical set.
        Coordination::Responses responses;
        auto code = coordination.tryCommitInsert(zk, part->name, serializePartHeader(part), extra_ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            if (dedup_enabled && zkutil::getFailedOpIndex(code, responses) == 1)
            {
                /// Lost a race against another replica's concurrent insert of the same content
                /// between our pre-check above and this commit -- same outcome as the pre-check
                /// branch, not an error.
                LOG_DEBUG(getLogger("StorageCloudMergeTree"), "INSERT of part {} was deduplicated (commit race)", part->name);
                /// The failed multi() means the lock's own unlock op (bundled into extra_ops
                /// above) never ran either -- release it explicitly via the normal unlock() path.
                block_lock.unlock();
                part->removeIfNeeded();
                return false;
            }
            throw Exception(ErrorCodes::INCORRECT_DATA, "Part {} already exists in the Keeper part set", part->name);
        }
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot register part {} in Keeper", part->name);
        block_lock.assumeUnlocked();

        /// Only now reflect it in this replica's in-memory cache.
        renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
        covered_parts = transaction.commit(lock);
        for (const auto & covered : covered_parts)
            modifyPartState(covered, DataPartState::Deleting, lock);
    }
    /// See admitPartLocally()'s doc comment: a covered part must not linger as a timer-based
    /// Outdated entry, since CloudMergeTree never runs the generic cleanup thread that would
    /// eventually erase it. removePartsFinally() takes its own lockParts() internally, so it must
    /// run after the block above releases this function's own lock. Rare in practice for INSERT
    /// (a fresh part covering something existing is an edge case, not the common path), but handled
    /// uniformly with commitMergedPart()/admitPartLocally()'s callers for the same reason.
    if (!covered_parts.empty())
        removePartsFinally(covered_parts);
    return true;
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
        /// We may still validly hold this lease (e.g. code == ZNODEEXISTS means our lease check
        /// itself passed and only the result-part create lost to a faster winner -- a real,
        /// reproducible window: the winner's own releaseLease() below can free the path in time
        /// for us to acquire a *fresh* lease there before our own commit attempt runs). Release it
        /// unconditionally rather than only on the success path below -- otherwise a losing
        /// replica's lease sits as a permanently-dangling ephemeral node, forever blocking the
        /// parts-killer GC task's defensive live-lease check for the tombstoned source part with
        /// the same name (see runPartsKillerCycle). If we'd instead lost the lease itself (someone
        /// stole it), this call just no-ops on a version mismatch -- safe either way.
        coordination.releaseLease(zk, lease_path, lease_version);
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
    DataPartsVector covered_parts = transaction.commit();

    /// See admitPartLocally()'s doc comment: a source part covered by this merge/mutation result
    /// must not linger as a timer-based Outdated entry -- CloudMergeTree never runs the generic
    /// cleanup thread that would eventually erase it. transaction.commit() (the no-arg overload)
    /// already acquired and released its own lock internally by the time it returns here, so
    /// re-acquiring one below is a fresh, separate acquisition, not nested with anything above.
    if (!covered_parts.empty())
    {
        {
            auto lock = lockParts();
            for (const auto & covered : covered_parts)
                modifyPartState(covered, DataPartState::Deleting, lock);
        }
        removePartsFinally(covered_parts);
    }
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
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::truncate");

    /// Unlike drop(), the table itself stays alive: no markTableDropped(), no root-directory
    /// teardown. The GC task's trailing-teardown check is gated specifically on the `dropped`
    /// marker, so a plain TRUNCATE correctly leaves the table's Keeper root nodes and directory
    /// intact for future inserts -- only the parts themselves are deactivated and tombstoned.
    removeActivePartsMatching([](const String &) { return true; });

    /// deduplication_hashes/ znodes are otherwise permanent: without this, the exact same content
    /// re-inserted after TRUNCATE is silently discarded as a dedup hit against data that no longer
    /// exists -- the canonical staging-table reload workflow would appear to succeed while leaving
    /// the table empty. See CloudMergeTreeCoordination::clearDeduplicationHashes()'s doc comment.
    coordination.clearDeduplicationHashes(getZooKeeper());
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
    auto settings = std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());

    /// CloudMergeTree's disk is always plain_rewritable object storage (DiskObjectStorage::
    /// supportsHardLinks() returns false for it -- see metadata_storage->isPlain()), so mutations
    /// must never try the real-hardlink "reuse untouched columns" optimization MutateTask
    /// otherwise takes by default; this setting routes it to copy instead, which works
    /// unconditionally on any disk. See checkMutationIsPossible() below, which skips the disk
    /// hard-link check this same setting exists to make unnecessary.
    settings->set("always_use_copy_instead_of_hardlinks", true);
    return settings;
}

std::expected<CloudMergeMutateSelectedEntryPtr, SelectMergeFailure> StorageCloudMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot, std::unique_lock<std::mutex> & lock, bool aggressive,
    const String & partition_id, bool final, bool optimize_skip_merged_partitions)
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

    std::expected<MergeSelectorChoices, SelectMergeFailure> select_result;
    if (partition_id.empty())
    {
        /// Normal background/plain-OPTIMIZE path: cost-based selection bounded by
        /// max_source_parts_bytes_for_merge -- see MergeSelectorApplier.
        UInt64 max_source_parts_bytes_for_merge = CompactionStatistics::getMaxSourcePartsBytesForMerge(*this);
        UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(*this);

        if (max_source_parts_bytes_for_merge == 0)
            return std::unexpected(SelectMergeFailure{
                .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                .explanation = PreformattedMessage::create("Current value of max_source_parts_bytes is zero"),
            });

        select_result = merger_mutator.selectPartsToMerge(
            parts_collector,
            merge_predicate,
            MergeSelectorApplier(
                /*merge_constraints=*/{{max_source_parts_bytes_for_merge, max_result_part_rows}},
                /*merge_with_ttl_allowed=*/false, /// CloudMergeTree has no TTL-driven merges yet
                aggressive,
                /*range_filter_=*/nullptr,
                /*storage_id_=*/getStorageID()),
            /*partitions_hint=*/std::nullopt);
    }
    else
    {
        /// OPTIMIZE TABLE ... PARTITION p [FINAL]: unconditionally grabs every active part of that
        /// one partition as a single merge range, bypassing max_source_parts_bytes_for_merge's cost
        /// heuristic entirely -- see MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition.
        select_result = merger_mutator.selectAllPartsToMergeWithinPartition(
            metadata_snapshot, parts_collector, merge_predicate, partition_id, final, optimize_skip_merged_partitions);
    }

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

namespace
{
    /// A part needs mutation_id applied iff it hasn't already (its own .mutation field is behind)
    /// and it existed before that mutation's per-partition block-number snapshot boundary -- see
    /// CloudMergeTreeCoordination's class doc comment on deduplication_hashes/mutations for the
    /// same barrier-lock reasoning applied here. Shared by selectPartsToMutate() and the
    /// system.mutations-visibility methods below so the two can't drift apart.
    bool partNeedsMutation(const IMergeTreeDataPart & part, Int64 mutation_id, const ReplicatedMergeTreeMutationEntry & entry)
    {
        if (part.info.mutation >= mutation_id)
            return false;
        auto it = entry.block_numbers.find(part.info.getPartitionId());
        return it != entry.block_numbers.end() && part.info.min_block < it->second;
    }

    /// Every mutation entry currently recorded, parsed and sorted by numeric id ascending -- so a
    /// part needing several pending mutations always picks up the lowest-id one first (one
    /// mutation applied per selected part per selectPartsToMutate() call, see the Phase 4 Step C
    /// plan's explicit scope cut on batching). A corrupt entry is logged and skipped rather than
    /// failing the whole scan.
    ///
    /// The numeric id used here is the znode's raw Keeper sequential number PLUS ONE, not the raw
    /// number itself: Keeper's PersistentSequential counter starts at 0, which would collide with
    /// MergeTreePartInfo::mutation's own default (0 == "never mutated") -- an unmutated part would
    /// then look like it already had mutation 0 applied, permanently hiding the very first
    /// mutation from every part. +1 keeps real mutation ids strictly positive, matching the
    /// "0 is the sentinel" invariant partNeedsMutation() and new_part_info.mutation assignment
    /// below both rely on.
    std::vector<std::pair<Int64, ReplicatedMergeTreeMutationEntry>> loadSortedMutations(
        const CloudMergeTreeCoordination & coordination, const zkutil::ZooKeeperPtr & zk)
    {
        std::vector<std::pair<Int64, ReplicatedMergeTreeMutationEntry>> mutations;
        for (auto & [name, text] : coordination.listMutations(zk))
        {
            try
            {
                mutations.emplace_back(parse<Int64>(name) + 1, ReplicatedMergeTreeMutationEntry::parse(text, name));
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("StorageCloudMergeTree"),
                    fmt::format("Failed to parse mutation entry {}, skipping", name));
            }
        }
        std::sort(mutations.begin(), mutations.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
        return mutations;
    }
}

std::expected<CloudMutateSelectedEntryPtr, SelectMergeFailure> StorageCloudMergeTree::selectPartsToMutate(
    const StorageMetadataPtr & /*metadata_snapshot*/, std::unique_lock<std::mutex> & /*lock*/)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::selectPartsToMutate");
    auto zk = getZooKeeper();

    auto mutations = loadSortedMutations(coordination, zk);
    if (mutations.empty())
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::NOTHING_TO_MERGE,
            .explanation = PreformattedMessage::create("No pending mutations"),
        });

    for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
    {
        if (currently_merging_mutating_parts.contains(part))
            continue;

        for (const auto & [id, entry] : mutations)
        {
            if (!partNeedsMutation(*part, id, entry))
                continue;

            auto new_part_info = part->info;
            new_part_info.mutation = id;

            auto future_part = std::make_shared<FutureMergedMutatedPart>();
            future_part->parts.push_back(part);
            future_part->part_info = new_part_info;
            future_part->name = part->getNewName(new_part_info);
            future_part->part_format = part->getFormat();

            /// Same lease namespace merges use -- a mutated part's name (bumped .mutation field)
            /// never collides with any concurrent merge's result name, so no separate namespace
            /// is needed. Losing this race is NOTHING_TO_MERGE for this part, not fatal -- try the
            /// next part instead of giving up the whole selection cycle.
            const String lease_path = coordination.leasePath(future_part->name);
            const String holder_data = toString(ServerUUID::get());
            const Int64 staleness_ms = static_cast<Int64>((*getSettings())[MergeTreeSetting::cloud_merge_tree_lease_staleness_ms]);

            auto lease = coordination.acquireOrStealLease(zk, lease_path, holder_data, staleness_ms);
            if (!lease)
                continue;

            try
            {
                uint64_t needed_disk_space = CompactionStatistics::estimateNeededDiskSpace({part}, false);
                auto tagger = std::make_unique<CloudCurrentlyMergingPartsTagger>(future_part, needed_disk_space, *this);

                auto selected = std::make_shared<CloudMutateSelectedEntry>();
                selected->future_part = future_part;
                selected->tagger = std::move(tagger);
                selected->lease_path = lease->path;
                selected->lease_version = lease->version;
                selected->commands = std::make_shared<MutationCommands>(entry.commands);
                selected->mutation_id = entry.znode_name;
                return selected;
            }
            catch (...)
            {
                coordination.releaseLease(zk, lease->path, lease->version);
                throw;
            }
        }
    }

    return std::unexpected(SelectMergeFailure{
        .reason = SelectMergeFailure::Reason::NOTHING_TO_MERGE,
        .explanation = PreformattedMessage::create("No active part currently needs a pending mutation applied"),
    });
}

void StorageCloudMergeTree::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    try
    {
        MergeTreeData::checkAlterIsPossible(commands, local_context);
    }
    catch (const Exception & e)
    {
        /// See this method's doc comment in StorageCloudMergeTree.h: the one clause of the base
        /// validator that doesn't apply to CloudMergeTree. Matched by message substring, not error
        /// code alone -- SUPPORT_IS_DISABLED is also thrown earlier in the same base function for
        /// an unrelated text-index check, which must still propagate.
        if (e.code() == ErrorCodes::SUPPORT_IS_DISABLED && e.message().contains("immutable disk"))
            return;
        throw;
    }
}

ReplicatedMergeTreeMutationEntry StorageCloudMergeTree::buildMutationEntry(
    const MutationCommands & commands, ContextPtr local_context, int32_t alter_version)
{
    auto zk = getZooKeeper();

    std::set<String> affected_partition_ids = getPartitionIdsAffectedByCommands(commands, local_context);
    if (affected_partition_ids.empty())
    {
        /// No command carried an explicit PARTITION clause: applies table-wide, to every partition
        /// with currently-active parts.
        for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
            affected_partition_ids.insert(part->info.getPartitionId());
    }

    ReplicatedMergeTreeMutationEntry entry;
    entry.create_time = time(nullptr);
    entry.source_replica = toString(ServerUUID::get());
    entry.commands = commands;
    entry.alter_version = alter_version;
    if (!affected_partition_ids.empty())
        entry.block_numbers = coordination.snapshotBlockNumbers(zk, affected_partition_ids);

    return entry;
}

void StorageCloudMergeTree::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::mutate");
    /// alter_version -1: this is its "not an ALTER-metadata-driven mutation" default -- a manually
    /// submitted mutation (ALTER TABLE ... UPDATE/DELETE) has no metadata-alter-vs-mutation ordering
    /// to record. See alter() below for the ALTER-driven case, which passes its own resulting
    /// metadata version instead.
    auto entry = buildMutationEntry(commands, local_context, /*alter_version=*/ -1);
    coordination.createMutation(getZooKeeper(), entry.toString());
}

void StorageCloudMergeTree::checkMutationIsPossible(const MutationCommands &, const Settings &) const
{
    /// See the declaration's doc comment in StorageCloudMergeTree.h: deliberately not calling
    /// MergeTreeData::checkMutationIsPossible() here, since its disk hard-link check would reject
    /// every CloudMergeTree table outright.
}

void StorageCloudMergeTree::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::alter");

    auto table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    auto zk = getZooKeeper();

    /// The very first CAS attempt must be just as content-consistent with its fenced version as
    /// every retry is: build new_metadata's columns from the SAME Keeper read that produced
    /// expected_version, not from our separately-taken (and possibly already-stale, if another
    /// replica's ALTER landed in between) in-memory metadata_snapshot. Getting this wrong is a
    /// silent-clobber bug, not just a missed optimization -- if expected_version happens to
    /// already match Keeper's actual current version on the first try (another replica's change
    /// already landed there), the CAS succeeds immediately since Keeper only checks the version
    /// number, not that our payload actually derives from that version's content -- overwriting
    /// the other replica's change instead of stacking on top of it, with no ZBADVERSION to catch
    /// it. Reproduced reliably under two concurrent ALTERs from different replicas.
    auto initial = coordination.getMetadata(zk);
    String columns_text = initial.first;
    int32_t expected_version = initial.second;

    /// Bounded retry against a concurrent ALTER from another replica: trySetMetadata()'s (and
    /// trySetMetadataAndCreateMutation()'s) CAS fails closed (ZBADVERSION) if the version we
    /// fenced on is stale, same fail-closed/retry shape removeActivePartsMatching() and
    /// commitMergedPart() already rely on elsewhere in this file.
    for (int attempt = 0; attempt < 20; ++attempt)
    {
        StorageInMemoryMetadata new_metadata = *metadata_snapshot;
        new_metadata.columns = ColumnsDescription::parse(columns_text);

        /// getMutationCommands() must be recomputed against THIS attempt's own baseline every time
        /// (not just once, up front) -- the same content/version-consistency reasoning as above: a
        /// command's mutation-requiring-ness can depend on a column's CURRENT type, which must come
        /// from the same Keeper read that fences this attempt's CAS. Called before apply() below,
        /// against the still-unmodified metadata, matching upstream's own ordering.
        auto mutation_commands = params.getMutationCommands(new_metadata, /*materialize_ttl=*/ false, local_context, /*with_alters=*/ false);
        params.apply(new_metadata, local_context);
        String new_columns_text = new_metadata.getColumns().toString(/*include_comments=*/ true);

        if (mutation_commands.empty())
        {
            int32_t new_version = 0;
            auto code = coordination.trySetMetadata(zk, new_columns_text, expected_version, new_version);

            if (code == Coordination::Error::ZOK)
            {
                new_metadata.setMetadataVersion(new_version);
                /// Safe because checkAlterIsPossible() already validated this metadata (invoked
                /// automatically by the standard AlterCommands validation path before alter() is
                /// called, same as every other MergeTree engine).
                DatabaseCatalog::instance().getDatabase(table_id.database_name)
                    ->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/ true);
                setInMemoryMetadata(new_metadata);
                current_metadata_version.store(new_version);
                return;
            }

            if (code != Coordination::Error::ZBADVERSION)
                throw zkutil::KeeperException(code, "Cannot update metadata in Keeper for table {}", table_id.getNameForLogs());
        }
        else
        {
            /// A command requiring an actual data rewrite (e.g. a genuine type conversion): commit
            /// the metadata change and the mutation that migrates existing parts to it atomically
            /// together, in one multi() -- mirrors StorageReplicatedMergeTree::alter()'s own
            /// atomic-together shape (DESIGN.md invariant 3's "exactly-once materialization"). A
            /// crash between two *separate* writes would otherwise leave either a live schema
            /// change with no mutation to ever rewrite old-typed data, or an orphaned mutation
            /// naming a metadata state that was never actually published.
            int32_t new_version_if_success = expected_version + 1;
            auto entry = buildMutationEntry(mutation_commands, local_context, new_version_if_success);
            auto result = coordination.trySetMetadataAndCreateMutation(zk, new_columns_text, expected_version, entry.toString());

            if (result.has_value())
            {
                new_metadata.setMetadataVersion(result->new_metadata_version);
                DatabaseCatalog::instance().getDatabase(table_id.database_name)
                    ->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/ true);
                setInMemoryMetadata(new_metadata);
                current_metadata_version.store(result->new_metadata_version);
                return;
            }

            if (result.error() != Coordination::Error::ZBADVERSION)
                throw zkutil::KeeperException(result.error(),
                    "Cannot update metadata and create mutation in Keeper for table {}", table_id.getNameForLogs());
        }

        /// Someone else's ALTER landed first: reload the actual current columns and version, and
        /// retry from the top of the loop, which rebuilds new_metadata/mutation_commands against
        /// THAT fresh baseline -- not our now-stale snapshot, otherwise we'd silently clobber their
        /// change instead of stacking on it.
        auto latest = coordination.getMetadata(zk);
        columns_text = latest.first;
        expected_version = latest.second;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed to update metadata in Keeper for table {} after repeated concurrent-modification retries",
        table_id.getNameForLogs());
}

CancellationCode StorageCloudMergeTree::killMutation(const String & mutation_id)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::killMutation");
    auto zk = getZooKeeper();

    /// Read before removing: block_numbers is needed below to cancel any currently-running part
    /// mutations tracked in the MergeList, and tryRemove() doesn't hand back the payload.
    String entry_text;
    if (!zk->tryGet(coordination.mutationPath(mutation_id), entry_text))
        return CancellationCode::NotFound;

    auto code = zk->tryRemove(coordination.mutationPath(mutation_id));
    if (code == Coordination::Error::ZNONODE)
        return CancellationCode::NotFound; /// lost a race against a concurrent KILL MUTATION for the same id
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException(code, "Cannot remove mutation {} from Keeper for table {}", mutation_id, getStorageID().getNameForLogs());

    try
    {
        auto entry = ReplicatedMergeTreeMutationEntry::parse(entry_text, mutation_id);
        for (const auto & [partition_id, block_number] : entry.block_numbers)
            getContext()->getMergeList().cancelPartMutations(getStorageID(), partition_id, block_number);
    }
    catch (const Exception &)
    {
        /// Malformed entry text -- already removed from Keeper above regardless, nothing more to
        /// safely clean up, but the mutation is genuinely gone either way.
    }

    return CancellationCode::CancelSent;
}

ActionLock StorageCloudMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();
    return {};
}

void StorageCloudMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        background_operations_assignee.trigger();
}

bool StorageCloudMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called)
        return false;

    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);

    CloudMergeMutateSelectedEntryPtr merge_entry;
    CloudMutateSelectedEntryPtr mutate_entry;
    {
        std::unique_lock lock(currently_processing_in_background_mutex);

        /// SYSTEM STOP MERGES: matches StorageMergeTree::scheduleDataProcessingJob's own
        /// merges_blocker.isCancelled() check, which gates mutation selection too, not just merge
        /// selection -- both use this same background-scheduling cycle. See getActionLock()'s doc
        /// comment for why this check alone isn't enough without that override.
        if (merger_mutator.merges_blocker.isCancelled())
            return false;

        auto merge_select_result = selectPartsToMerge(metadata_snapshot, lock);
        if (merge_select_result)
            merge_entry = std::move(merge_select_result.value());
        else
            LOG_TRACE(getLogger("StorageCloudMergeTree"), "Didn't start merge: {}", merge_select_result.error().explanation.text);

        /// Only tried when merge selection found nothing this cycle -- same two-phase shape
        /// StorageMergeTree's own scheduleDataProcessingJob already uses upstream.
        if (!merge_entry)
        {
            auto mutate_select_result = selectPartsToMutate(metadata_snapshot, lock);
            if (mutate_select_result)
                mutate_entry = std::move(mutate_select_result.value());
            else
                LOG_TRACE(getLogger("StorageCloudMergeTree"), "Didn't start mutation: {}", mutate_select_result.error().explanation.text);
        }
    }

    if (merge_entry)
    {
        /// Ordinary background merging must never deduplicate or cleanup on its own -- only an
        /// explicit OPTIMIZE TABLE ... DEDUPLICATE/CLEANUP does (see optimize()'s own construction
        /// of this task below).
        auto task = std::make_shared<CloudMergePlainMergeTreeTask>(
            *this, metadata_snapshot, merge_entry, table_lock_holder, common_assignee_trigger,
            /*deduplicate_=*/ false, /*deduplicate_by_columns_=*/ Names{}, /*cleanup_=*/ false);
        return assignee.scheduleMergeMutateTask(task);
    }

    if (mutate_entry)
    {
        auto task = std::make_shared<CloudMergeMutateTask>(
            *this, metadata_snapshot, mutate_entry, table_lock_holder, common_assignee_trigger);
        return assignee.scheduleMergeMutateTask(task);
    }

    return false;
}

bool StorageCloudMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    /// Same legality guards as StorageMergeTree::optimize(): CLEANUP's semantic requirements are
    /// engine-independent, not something CloudMergeTree gets to relax. Phase 0 registration only
    /// supports MergingParams::Mode::Ordinary (see registerStorageMergeTree.cpp's isCloud()) --
    /// there is no way to CREATE a Replacing-mode CloudMergeTree table yet, so the first check
    /// below is unconditionally true for every table this engine can currently create. That's
    /// accurate, not a bug: it's exactly what upstream would also throw for any non-Replacing
    /// table, and this plumbing is forward-compatible for whenever Replacing-mode registration
    /// lands separately.
    if (cleanup && merging_params.mode != MergingParams::Mode::Replacing)
        throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, "Cannot OPTIMIZE with CLEANUP table: only ReplacingMergeTree can be CLEANUP");
    if (cleanup && !(*getSettings())[MergeTreeSetting::allow_experimental_replacing_merge_with_cleanup])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental merges with CLEANUP are not allowed");

    /// Matches StorageMergeTree::merge() (called from its own optimize()): an explicit OPTIMIZE
    /// still must not proceed while SYSTEM STOP MERGES is in effect for this table -- getActionLock()
    /// only made scheduleDataProcessingJob() (background scheduling) respect the blocker; without
    /// this check here too, an explicit OPTIMIZE would silently ignore SYSTEM STOP MERGES entirely.
    if (merger_mutator.merges_blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    auto metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    bool optimize_skip_merged_partitions = local_context->getSettingsRef()[Setting::optimize_skip_merged_partitions];

    if (!partition && final)
    {
        /// Whole-table FINAL: one converge-to-one-part call per currently-existing partition,
        /// matching StorageMergeTree::optimize()'s own expansion -- not a repeated-until-converged
        /// loop *within* a partition (one optimizeUntilConverged() call already grabs everything in
        /// that partition each iteration via selectAllPartsToMergeWithinPartition).
        std::unordered_set<String> partition_ids;
        for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
            partition_ids.insert(part->info.getPartitionId());

        for (const auto & partition_id : partition_ids)
        {
            optimizeUntilConverged(metadata_snapshot, partition_id, /*final=*/true, optimize_skip_merged_partitions, deduplicate, deduplicate_by_columns, cleanup);

            /// A merge just committed for this partition (if any) only advances
            /// current_parts_version asynchronously, via the background watcher -- without forcing
            /// a synchronous resync here, the *next* partition's own selectPartsToMerge call would
            /// immediately see this table's Keeper parts-version having just moved, judge its local
            /// view stale, and silently skip itself for this entire OPTIMIZE call (repeatable: nothing
            /// else prompts a resync before moving on to it). updatePartSetFromKeeper() is safe to
            /// call inline here -- it only reconciles parts *we* already know about (our own just-
            /// committed merge already added its result to the local working set synchronously), so
            /// there is nothing to adopt from disk and it resolves in one round trip.
            updatePartSetFromKeeper();
        }

        return true;
    }

    String partition_id;
    if (partition)
        partition_id = getPartitionIDFromQuery(partition, local_context);

    optimizeUntilConverged(metadata_snapshot, partition_id, final, optimize_skip_merged_partitions, deduplicate, deduplicate_by_columns, cleanup);
    return true;
}

void StorageCloudMergeTree::optimizeUntilConverged(
    const StorageMetadataPtr & metadata_snapshot, const String & partition_id, bool final,
    bool optimize_skip_merged_partitions, bool deduplicate, const Names & deduplicate_by_columns, bool cleanup)
{
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
            auto merge_select_result = selectPartsToMerge(
                metadata_snapshot, lock, /*aggressive=*/true, partition_id, final, optimize_skip_merged_partitions);
            if (!merge_select_result)
            {
                /// Both real convergence ("no need to merge parts according to merge selector
                /// algorithm" / "There is only one part inside partition", reported as
                /// CANNOT_SELECT) and a lost lease race are unremarkable stopping points for a plain
                /// OPTIMIZE, not failures: whatever could be merged already has been. Log for
                /// visibility and stop the loop successfully.
                LOG_TRACE(getLogger("StorageCloudMergeTree"), "Stopping OPTIMIZE: {}", merge_select_result.error().explanation.text);
                return;
            }
            merge_entry = std::move(merge_select_result.value());
        }

        IExecutableTask::TaskResultCallback f = [](bool) {};
        auto task = std::make_shared<CloudMergePlainMergeTreeTask>(
            *this, metadata_snapshot, merge_entry, table_lock_holder, f,
            deduplicate, deduplicate_by_columns, cleanup);
        executeHere(task);
    }
}

void StorageCloudMergeTree::startBackgroundMovesIfNeeded()
{
}

MutationCounters StorageCloudMergeTree::getMutationCounters() const
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::getMutationCounters");
    MutationCounters counters;
    auto zk = getZooKeeper();
    auto mutations = loadSortedMutations(coordination, zk);
    if (mutations.empty())
        return counters;

    auto active_parts = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular});
    for (const auto & [id, entry] : mutations)
    {
        bool pending = std::any_of(active_parts.begin(), active_parts.end(),
            [&](const auto & part) { return partNeedsMutation(*part, id, entry); });
        if (pending)
            ++counters.num_data;
    }
    /// CloudMergeTree has no ALTER ADD/DROP/MODIFY COLUMN yet (Phase 4 Step D), so every mutation
    /// recorded here is a plain data mutation -- num_alter/num_metadata stay 0.
    return counters;
}

std::map<std::string, MutationCommands> StorageCloudMergeTree::getUnfinishedMutationCommands() const
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::getUnfinishedMutationCommands");
    std::map<std::string, MutationCommands> result;
    auto zk = getZooKeeper();
    auto mutations = loadSortedMutations(coordination, zk);
    if (mutations.empty())
        return result;

    auto active_parts = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular});
    for (const auto & [id, entry] : mutations)
    {
        bool pending = std::any_of(active_parts.begin(), active_parts.end(),
            [&](const auto & part) { return partNeedsMutation(*part, id, entry); });
        if (pending)
            result.emplace(entry.znode_name, entry.commands);
    }
    return result;
}

std::vector<MergeTreeMutationStatus> StorageCloudMergeTree::getMutationsStatus() const
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::getMutationsStatus");
    std::vector<MergeTreeMutationStatus> result;
    auto zk = getZooKeeper();
    auto mutations = loadSortedMutations(coordination, zk);
    if (mutations.empty())
        return result;

    auto active_parts = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular});
    result.reserve(mutations.size());
    for (const auto & [id, entry] : mutations)
    {
        MergeTreeMutationStatus status;
        status.id = entry.znode_name;
        status.command = entry.commands.toString(/*with_pure_metadata_commands=*/ false);
        status.create_time = entry.create_time;
        status.block_numbers = entry.block_numbers;

        for (const auto & part : active_parts)
            if (partNeedsMutation(*part, id, entry))
                status.parts_to_do_names.push_back(part->name);

        status.is_done = status.parts_to_do_names.empty();
        result.push_back(std::move(status));
    }
    return result;
}

bool StorageCloudMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr &) const
{
    return false;
}

void StorageCloudMergeTree::attachRestoredParts(MutableDataPartsVector &&, const std::optional<ZooKeeperRetriesInfo> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RESTORE is not implemented for CloudMergeTree yet");
}

size_t StorageCloudMergeTree::removeActivePartsMatching(const std::function<bool(const String &)> & predicate)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::removeActivePartsMatching");
    auto zk = getZooKeeper();

    /// Bounded retry against a concurrent merge/DROP racing on an overlapping part: tryRemoveParts()'s
    /// multi() fails closed (ZNONODE) if any matched name was already deactivated elsewhere between
    /// our read and our commit, same as commitMergedPart()'s lease check fails closed against us.
    for (int attempt = 0; attempt < 20; ++attempt)
    {
        int32_t version = 0;
        Strings active_names = coordination.loadActivePartNames(zk, version);

        Strings matched;
        for (const auto & name : active_names)
            if (predicate(name))
                matched.push_back(name);

        if (matched.empty())
            return 0;

        auto code = coordination.tryRemoveParts(zk, matched);
        if (code == Coordination::Error::ZNONODE)
            continue; /// lost a race against a concurrent merge/DROP on one of these names -- retry

        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot remove parts from Keeper for table {}", getStorageID().getNameForLogs());

        std::unordered_set<std::string> matched_set(matched.begin(), matched.end());
        auto lock = lockParts();
        auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);
        DataPartsVector to_remove;
        for (const auto & part : known)
            if (matched_set.contains(part->name))
                to_remove.push_back(part);

        if (!to_remove.empty())
            removePartsFromWorkingSet(/*txn=*/ nullptr, to_remove, /*clear_without_timeout=*/ false, lock);

        return matched.size();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed to remove parts from Keeper for table {} after repeated concurrent-modification retries",
        getStorageID().getNameForLogs());
}

size_t StorageCloudMergeTree::detachActivePartsMatching(const std::function<bool(const String &)> & predicate)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::detachActivePartsMatching");
    auto zk = getZooKeeper();

    /// Same bounded-retry shape as removeActivePartsMatching -- see its comment.
    for (int attempt = 0; attempt < 20; ++attempt)
    {
        int32_t version = 0;
        Strings active_names = coordination.loadActivePartNames(zk, version);

        Strings matched;
        for (const auto & name : active_names)
            if (predicate(name))
                matched.push_back(name);

        if (matched.empty())
            return 0;

        auto code = coordination.tryDetachParts(zk, matched);
        if (code == Coordination::Error::ZNONODE)
            continue; /// lost a race against a concurrent merge/DROP/DETACH on one of these names -- retry

        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot detach parts in Keeper for table {}", getStorageID().getNameForLogs());

        std::unordered_set<std::string> matched_set(matched.begin(), matched.end());
        DataPartsVector to_remove;
        {
            auto lock = lockParts();
            auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);
            for (const auto & part : known)
                if (matched_set.contains(part->name))
                    to_remove.push_back(part);

            if (!to_remove.empty())
            {
                /// Unlike a permanent DROP (where the same part name is never reused, so leaving it
                /// Outdated pending the generic old-parts cleanup timer is harmless), DETACH followed
                /// immediately by ATTACH re-registers the *same* part name. MergeTreeData's ordinary
                /// Outdated-part retention would otherwise leave a stale in-memory entry blocking that
                /// re-add (checkPartDuplicate() rejects any name still present as Outdated/Deleting) --
                /// and CloudMergeTree never runs the generic local-disk old-parts cleanup thread that
                /// would eventually erase it (physical deletion is exclusively owned by the Keeper-driven
                /// parts-killer, gated on dropped_parts/ tombstones, which DETACH deliberately never
                /// writes), so nothing would ever erase it on its own. Transition straight to Deleting
                /// here (still under this lock); removePartsFinally() -- called just below, after this
                /// lock is released, since it takes its own -- then erases the in-memory bookkeeping
                /// immediately. This only forgets the DataPart *object*, it does not touch anything on
                /// the shared disk -- the directory stays exactly where it is, found again by
                /// buildPartFromDisk() on a later ATTACH.
                removePartsFromWorkingSet(/*txn=*/ nullptr, to_remove, /*clear_without_timeout=*/ true, lock);
                for (const auto & part : to_remove)
                    modifyPartState(part, DataPartState::Deleting, lock);
            }
        }
        if (!to_remove.empty())
            removePartsFinally(to_remove);

        return matched.size();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed to detach parts in Keeper for table {} after repeated concurrent-modification retries",
        getStorageID().getNameForLogs());
}

void StorageCloudMergeTree::dropPartNoWaitNoThrow(const String & part_name)
try
{
    removeActivePartsMatching([&](const String & name) { return name == part_name; });
}
catch (...)
{
    tryLogCurrentException(getLogger("StorageCloudMergeTree"),
        fmt::format("dropPartNoWaitNoThrow failed for part {}, ignoring (best-effort)", part_name));
}

void StorageCloudMergeTree::dropPart(const String & part_name, bool detach, ContextPtr)
{
    size_t removed = detach
        ? detachActivePartsMatching([&](const String & name) { return name == part_name; })
        : removeActivePartsMatching([&](const String & name) { return name == part_name; });
    if (removed == 0)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} not found, won't try to drop it.", part_name);
}

void StorageCloudMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr local_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::dropPartition");

    String partition_id = getPartitionIDFromQuery(partition, local_context);
    auto predicate = [&](const String & name)
    {
        return MergeTreePartInfo::fromPartName(name, format_version).getPartitionId() == partition_id;
    };
    if (detach)
        detachActivePartsMatching(predicate);
    else
    {
        removeActivePartsMatching(predicate);
        /// Only for the real DROP -- DETACH keeps the data recoverable (see attachPartition()), so
        /// a later re-ATTACH of the exact same content must still be able to correctly dedup
        /// against itself if inserted again meanwhile. Same reasoning as truncate()'s identical call.
        coordination.clearDeduplicationHashes(getZooKeeper(), partition_id);
    }
}

PartitionCommandsResultInfo StorageCloudMergeTree::attachPartition(
    const PartitionCommand & command, const StorageMetadataPtr &, ContextPtr local_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::attachPartition");
    auto zk = getZooKeeper();

    Strings detached_names = coordination.listDetachedPartNames(zk);
    Strings candidates;
    if (command.part)
    {
        /// Same literal-string extraction MergeTreeData's static (internal-linkage)
        /// getPartNameFromAST() does -- inlined here since that helper isn't reachable from this file.
        const auto * literal = command.partition->as<ASTLiteral>();
        if (!literal)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a string literal for part name, got: {}", command.partition->formatForErrorMessage());
        String part_name = literal->value.safeGet<String>();
        if (std::ranges::find(detached_names, part_name) != detached_names.end())
            candidates.push_back(part_name);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(command.partition, local_context);
        for (const auto & name : detached_names)
            if (MergeTreePartInfo::fromPartName(name, format_version).getPartitionId() == partition_id)
                candidates.push_back(name);
    }

    if (candidates.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No detached part(s) found to attach for table {}", getStorageID().getNameForLogs());

    PartitionCommandsResultInfo results;
    for (const auto & name : candidates)
    {
        auto part = buildPartFromDisk(name);
        if (!part)
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                "Detached part {} is registered in Keeper but not found on the shared disk", name);

        String header = serializePartHeader(part);
        auto code = coordination.tryReattachPart(zk, name, header);
        /// ZNONODE (detached_parts/<name> already gone) or ZNODEEXISTS (parts/<name> already
        /// there) both mean another replica's concurrent ATTACH of the same name already won --
        /// not an error, the part is active in Keeper either way, so just admit it locally too.
        if (code != Coordination::Error::ZOK
            && code != Coordination::Error::ZNONODE
            && code != Coordination::Error::ZNODEEXISTS)
            throw zkutil::KeeperException(code, "Cannot reattach part {} in Keeper for table {}", name, getStorageID().getNameForLogs());

        DataPartsVector covered_parts;
        {
            auto lock = lockParts();
            covered_parts = admitPartLocally(part, lock);
            for (const auto & covered : covered_parts)
                modifyPartState(covered, DataPartState::Deleting, lock);
        }
        /// See admitPartLocally()'s doc comment: erase any covered part's in-memory bookkeeping
        /// immediately (removePartsFinally() takes its own lockParts(), so must run after the block
        /// above releases this one) rather than leaving it as a timer-based Outdated entry nothing
        /// would ever clean up.
        if (!covered_parts.empty())
            removePartsFinally(covered_parts);

        results.push_back(PartitionCommandResultInfo{
            .command_type = command.part ? "ATTACH PART" : "ATTACH PARTITION",
            .partition_id = part->info.getPartitionId(),
            .part_name = name,
            .old_part_name = name, /// no rename occurs -- CloudMergeTree's shared part directory
                                    /// never moved while detached, so the reattached part keeps
                                    /// its original name (unlike StorageReplicatedMergeTree, which
                                    /// reallocates a fresh block number on ATTACH).
        });
    }
    return results;
}

void StorageCloudMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr local_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::replacePartitionFrom");

    /// Phase 5 Step A: only between two CloudMergeTree tables -- matches both upstream engines'
    /// own dynamic-cast-fail behavior for cross-engine REPLACE/ATTACH PARTITION ... FROM.
    auto * source_storage = dynamic_cast<StorageCloudMergeTree *>(source_table.get());
    if (!source_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "REPLACE/ATTACH PARTITION ... FROM is only implemented between two CloudMergeTree tables");

    auto my_metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    auto src_metadata_snapshot = source_storage->getInMemoryMetadataPtr(local_context, false);

    /// Generic, engine-agnostic structural check (column list, sorting/partition/primary key,
    /// format_version) -- same helper both StorageMergeTree and StorageReplicatedMergeTree already
    /// call unchanged.
    checkStructureAndGetMergeTreeData(*source_storage, src_metadata_snapshot, my_metadata_snapshot);

    /// Stricter than StorageMergeTree's isCompatibleForPartitionOps() allowance -- matches
    /// StorageReplicatedMergeTree's own exact-equality requirement instead. CloudMergeTree's
    /// constructor already enforces "exactly one remote disk policy" per table, so this reduces to
    /// "same bucket/disk," not a deep multi-disk compatibility question. Simplest, safest first
    /// cut; easy to relax later.
    if (getStoragePolicy()->getName() != source_storage->getStoragePolicy()->getName())
        throw Exception(ErrorCodes::UNKNOWN_POLICY,
            "Destination and source table have different storage policies, cannot REPLACE/ATTACH PARTITION between table {} and {}",
            getStorageID().getNameForLogs(), source_storage->getStorageID().getNameForLogs());

    String partition_id = getPartitionIDFromQuery(partition, local_context);

    /// Read-lock the source for the duration of the clone below -- it is only ever read from
    /// (its active parts queried, its part data cloned), never mutated by this operation.
    auto source_table_lock = source_storage->lockForShare(
        RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    DataPartsVector src_parts;
    for (const auto & part : source_storage->getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
        if (part->info.getPartitionId() == partition_id)
            src_parts.push_back(part);

    for (const auto & src_part : src_parts)
        if (!canReplacePartition(src_part))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot replace partition '{}': part '{}' has incompatible granularity for table {}",
                partition_id, src_part->name, getStorageID().getNameForLogs());

    auto zk = getZooKeeper();
    auto zk_fault = std::make_shared<ZooKeeperWithFaultInjection>(zk);
    coordination.ensureBlockNumbersPartition(zk, partition_id);

    /// Clone once -- source data doesn't need re-cloning if the commit below loses a race and
    /// retries; only the destination-side "which of my own parts am I replacing" set can go stale.
    /// Fresh block numbers on self (never the source's -- an unrelated table's counter sequence),
    /// same choice both upstream engines make: getLevelForAdoptedPart() resets the merge level to
    /// 0 unless both tables' merging_params have identical merge semantics, so a later
    /// FINAL/OPTIMIZE doesn't wrongly trust an adopted part as already fully merged (issue #106798).
    MutableDataPartsVector new_parts;
    std::vector<std::pair<String, String>> new_parts_with_headers;
    std::vector<scope_guard> temp_dir_guards;
    for (const auto & src_part : src_parts)
    {
        auto block_lock = createEphemeralLockInZooKeeper(
            coordination.blockNumbersPartitionPath(partition_id) + "/block-",
            coordination.tempPath(), zk_fault, /*deduplication_paths=*/ {}, /*znode_data=*/ std::nullopt);
        Int64 block_number = block_lock.getNumber();
        block_lock.unlock();

        auto dst_part_info = src_part->info;
        dst_part_info.min_block = dst_part_info.max_block = block_number;
        dst_part_info.level = getLevelForAdoptedPart(*source_storage, src_part->info.level);
        dst_part_info.mutation = 0;

        IDataPartStorage::ClonePartParams clone_params;
        /// CloudMergeTree's plain_rewritable disk already has supportsHardLinks() == false -- the
        /// same reason checkMutationIsPossible()/always_use_copy_instead_of_hardlinks already force
        /// copy-mode elsewhere in this engine.
        clone_params.copy_instead_of_hardlink = true;
        clone_params.metadata_version_to_write = my_metadata_snapshot->getMetadataVersion();

        auto [dst_part, temp_dir_guard] = cloneAndLoadDataPart(
            src_part, "tmp_replace_from_", dst_part_info, my_metadata_snapshot, clone_params,
            local_context->getReadSettings(), local_context->getWriteSettings(), /*must_on_same_disk=*/ true);

        new_parts.push_back(dst_part);
        new_parts_with_headers.emplace_back(dst_part->name, serializePartHeader(dst_part));
        temp_dir_guards.push_back(std::move(temp_dir_guard));
    }

    if (new_parts_with_headers.empty() && !replace)
        return; /// Nothing to attach: empty source partition.

    /// Bounded retry against a concurrent DROP/merge/REPLACE racing on our own old parts in this
    /// partition: tryReplacePartition()'s multi() fails closed (ZNONODE) if any old part we're
    /// replacing was already deactivated elsewhere between our read and our commit -- same
    /// fail-closed/retry shape removeActivePartsMatching()/commitMergedPart() already rely on. A
    /// failed multi() is all-or-nothing, so new_parts_with_headers is safe to reuse unchanged on
    /// retry (none of them were actually created in Keeper on a failed attempt).
    for (int attempt = 0; attempt < 20; ++attempt)
    {
        Strings old_part_names_to_remove;
        if (replace)
            for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
                if (part->info.getPartitionId() == partition_id)
                    old_part_names_to_remove.push_back(part->name);

        auto code = coordination.tryReplacePartition(zk, new_parts_with_headers, old_part_names_to_remove);
        if (code == Coordination::Error::ZNONODE)
            continue;
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot commit REPLACE/ATTACH PARTITION in Keeper for table {}", getStorageID().getNameForLogs());

        /// Keeper-first, then the local rename+admit -- other replicas' adoption watchers correctly
        /// retry until the physical rename below becomes visible, same contract every other commit
        /// in this engine already relies on (buildPartFromDisk()'s existsDirectory() pre-check).
        Transaction transaction(*this, nullptr);
        DataPartsVector covered_parts;
        {
            auto lock = lockParts();
            for (auto & part : new_parts)
                renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
            covered_parts = transaction.commit(lock);
            for (const auto & covered : covered_parts)
                modifyPartState(covered, DataPartState::Deleting, lock);

            if (!old_part_names_to_remove.empty())
            {
                std::unordered_set<std::string> old_names_set(old_part_names_to_remove.begin(), old_part_names_to_remove.end());
                auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);
                DataPartsVector to_remove;
                for (const auto & part : known)
                    if (old_names_set.contains(part->name))
                        to_remove.push_back(part);

                if (!to_remove.empty())
                {
                    removePartsFromWorkingSet(/*txn=*/ nullptr, to_remove, /*clear_without_timeout=*/ true, lock);
                    for (const auto & part : to_remove)
                        modifyPartState(part, DataPartState::Deleting, lock);
                    covered_parts.insert(covered_parts.end(), to_remove.begin(), to_remove.end());
                }
            }
        }
        if (!covered_parts.empty())
            removePartsFinally(covered_parts);

        /// Only for REPLACE (which discards this partition's prior content), not plain ATTACH ...
        /// FROM (which only adds alongside it) -- same reasoning as dropPartition()'s identical
        /// call. Without this, re-inserting content byte-identical to what REPLACE just discarded
        /// is silently deduplicated against it.
        if (replace)
            coordination.clearDeduplicationHashes(zk, partition_id);

        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed to commit REPLACE/ATTACH PARTITION in Keeper for table {} after repeated concurrent-modification retries",
        getStorageID().getNameForLogs());
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS: the std::lock()+adopt_lock dance below acquires two distinct
/// mutex instances (this table's and dest_storage's) whose static identity clang's thread-safety
/// analysis can't verify are always both released by every return path -- same reason upstream's
/// own StorageMergeTree::movePartitionToTable carries this exact attribute.
void StorageCloudMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr local_context) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::movePartitionToTable");

    /// Phase 5 Step B: only between two CloudMergeTree tables, same as replacePartitionFrom().
    auto * dest_storage = dynamic_cast<StorageCloudMergeTree *>(dest_table.get());
    if (!dest_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "MOVE PARTITION ... TO TABLE is only implemented between two CloudMergeTree tables");

    auto my_metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    auto dest_metadata_snapshot = dest_storage->getInMemoryMetadataPtr(local_context, false);

    /// this (self) is the SOURCE here -- opposite role from replacePartitionFrom(), where self was
    /// the destination. checkStructureAndGetMergeTreeData() is symmetric (column/key/format_version
    /// equality), so the direction of the call doesn't matter for the check itself.
    checkStructureAndGetMergeTreeData(*dest_storage, my_metadata_snapshot, dest_metadata_snapshot);

    if (getStoragePolicy()->getName() != dest_storage->getStoragePolicy()->getName())
        throw Exception(ErrorCodes::UNKNOWN_POLICY,
            "Source and destination table have different storage policies, cannot MOVE PARTITION between table {} and {}",
            getStorageID().getNameForLogs(), dest_storage->getStorageID().getNameForLogs());

    String partition_id = getPartitionIDFromQuery(partition, local_context);

    /// Deadlock-free lock ordering across two tables -- MergeTreeData::operation_with_data_parts_mutex
    /// exists specifically for this (its own doc comment references StorageMergeTree's own
    /// movePartitionToTable use of std::lock() for exactly this reason). Held for the whole
    /// select+clone+commit+admit sequence below, so a concurrent MOVE in the opposite direction (or
    /// a second MOVE overlapping the same parts) can't race destructively.
    std::lock(operation_with_data_parts_mutex, dest_storage->operation_with_data_parts_mutex);
    OperationDataPartsLock src_op_lock(operation_with_data_parts_mutex, std::adopt_lock);
    OperationDataPartsLock dest_op_lock(dest_storage->operation_with_data_parts_mutex, std::adopt_lock);

    DataPartsVector src_parts;
    for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
        if (part->info.getPartitionId() == partition_id)
            src_parts.push_back(part);

    for (const auto & src_part : src_parts)
        if (!canReplacePartition(src_part))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot move partition '{}': part '{}' has incompatible granularity for table {}",
                partition_id, src_part->name, getStorageID().getNameForLogs());

    auto zk = getZooKeeper();
    auto zk_fault = std::make_shared<ZooKeeperWithFaultInjection>(zk);
    dest_storage->coordination.ensureBlockNumbersPartition(zk, partition_id);

    /// Clone onto the DESTINATION (it owns the target disk path -- relative_data_path is per-table
    /// even though both tables share one storage policy per the check above), fresh block numbers
    /// allocated on the destination's own counter -- matches replacePartitionFrom()'s identical
    /// choice, and upstream's own dest_table_storage->cloneAndLoadDataPart(...).
    MutableDataPartsVector new_parts;
    std::vector<std::pair<String, String>> new_parts_with_headers;
    std::vector<scope_guard> temp_dir_guards;
    for (const auto & src_part : src_parts)
    {
        auto block_lock = createEphemeralLockInZooKeeper(
            dest_storage->coordination.blockNumbersPartitionPath(partition_id) + "/block-",
            dest_storage->coordination.tempPath(), zk_fault, /*deduplication_paths=*/ {}, /*znode_data=*/ std::nullopt);
        Int64 block_number = block_lock.getNumber();
        block_lock.unlock();

        auto dst_part_info = src_part->info;
        dst_part_info.min_block = dst_part_info.max_block = block_number;
        dst_part_info.level = dest_storage->getLevelForAdoptedPart(*this, src_part->info.level);
        dst_part_info.mutation = 0;

        IDataPartStorage::ClonePartParams clone_params;
        clone_params.copy_instead_of_hardlink = true;
        clone_params.metadata_version_to_write = dest_metadata_snapshot->getMetadataVersion();

        auto [dst_part, temp_dir_guard] = dest_storage->cloneAndLoadDataPart(
            src_part, "tmp_move_to_table_", dst_part_info, dest_metadata_snapshot, clone_params,
            local_context->getReadSettings(), local_context->getWriteSettings(), /*must_on_same_disk=*/ true);

        new_parts.push_back(dst_part);
        new_parts_with_headers.emplace_back(dst_part->name, serializePartHeader(dst_part));
        temp_dir_guards.push_back(std::move(temp_dir_guard));
    }

    if (new_parts_with_headers.empty())
        return; /// Nothing to move: empty source partition.

    /// Bounded retry against a concurrent DROP/merge racing on the SOURCE's own parts in this
    /// partition: the multi() below fails closed (ZNONODE) if any old part we're moving away was
    /// already deactivated elsewhere -- same fail-closed/retry shape replacePartitionFrom() already
    /// established. A failed multi() is all-or-nothing, so new_parts_with_headers (already cloned
    /// onto the destination) is safe to reuse unchanged on retry.
    for (int attempt = 0; attempt < 20; ++attempt)
    {
        Strings old_part_names_to_remove;
        for (const auto & part : getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}))
            if (part->info.getPartitionId() == partition_id)
                old_part_names_to_remove.push_back(part->name);

        /// No single CloudMergeTreeCoordination instance can build this multi() -- it spans two
        /// tables' Keeper roots. Built directly here from both instances' already-public path
        /// helpers (plain string concatenation, safe to call on either instance); one ZooKeeperPtr
        /// issues the whole multi() regardless, since both roots live in the same Keeper cluster.
        /// This is the one thing neither StorageReplicatedMergeTree nor StorageMergeTree does
        /// atomically -- see the Context section.
        Coordination::Requests ops;
        for (const auto & [name, header] : new_parts_with_headers)
            ops.emplace_back(zkutil::makeCreateRequest(dest_storage->coordination.partPath(name), header, zkutil::CreateMode::Persistent));

        const String tombstone_ts = toString(static_cast<Int64>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()));
        for (const auto & name : old_part_names_to_remove)
        {
            ops.emplace_back(zkutil::makeRemoveRequest(coordination.partPath(name), -1));
            ops.emplace_back(zkutil::makeCreateRequest(coordination.droppedPartPath(name), tombstone_ts, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        auto code = zk->tryMultiNoThrow(ops, responses);
        if (code == Coordination::Error::ZNONODE)
            continue;
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, "Cannot commit MOVE PARTITION in Keeper between table {} and {}",
                getStorageID().getNameForLogs(), dest_storage->getStorageID().getNameForLogs());

        /// Admit the new parts on the destination.
        Transaction dest_transaction(*dest_storage, nullptr);
        DataPartsVector dest_covered_parts;
        {
            auto dest_lock = dest_storage->lockParts();
            for (auto & part : new_parts)
                dest_storage->renameTempPartAndAdd(part, dest_transaction, dest_lock, /*rename_in_transaction=*/ false);
            dest_covered_parts = dest_transaction.commit(dest_lock);
            for (const auto & covered : dest_covered_parts)
                dest_storage->modifyPartState(covered, DataPartState::Deleting, dest_lock);
        }
        if (!dest_covered_parts.empty())
            dest_storage->removePartsFinally(dest_covered_parts);

        /// Evict the moved-away parts on the source (self).
        DataPartsVector src_to_remove;
        {
            auto src_lock = lockParts();
            std::unordered_set<std::string> old_names_set(old_part_names_to_remove.begin(), old_part_names_to_remove.end());
            auto known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, src_lock);
            for (const auto & part : known)
                if (old_names_set.contains(part->name))
                    src_to_remove.push_back(part);

            if (!src_to_remove.empty())
            {
                removePartsFromWorkingSet(/*txn=*/ nullptr, src_to_remove, /*clear_without_timeout=*/ true, src_lock);
                for (const auto & part : src_to_remove)
                    modifyPartState(part, DataPartState::Deleting, src_lock);
            }
        }
        if (!src_to_remove.empty())
            removePartsFinally(src_to_remove);
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed to commit MOVE PARTITION in Keeper between table {} and {} after repeated concurrent-modification retries",
        getStorageID().getNameForLogs(), dest_storage->getStorageID().getNameForLogs());
}

void StorageCloudMergeTree::updatePartSetFromKeeper()
try
{
    auto component_guard = Coordination::setCurrentComponent("StorageCloudMergeTree::updatePartSetFromKeeper");
    auto zk = getZooKeeper();

    /// Phase 4 Step D: piggyback the metadata watch on this same task/callback rather than adding
    /// a second background task -- see current_metadata_version's doc comment in the header.
    /// Checked unconditionally, before the parts-version early-return below, so an ALTER issued on
    /// another replica (no part-set change of its own) still gets picked up this cycle instead of
    /// being skipped. setInMemoryMetadata() is a lock-free MultiVersion swap, safe to call from
    /// this background thread concurrently with query threads reading getInMemoryMetadataPtr().
    {
        auto [columns_text, new_metadata_version] = coordination.getMetadata(zk, part_set_updating_task->getWatchCallback());
        if (new_metadata_version != current_metadata_version.load())
        {
            auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
            StorageInMemoryMetadata new_metadata = *metadata_snapshot;
            new_metadata.columns = ColumnsDescription::parse(columns_text);
            new_metadata.setMetadataVersion(new_metadata_version);
            setInMemoryMetadata(new_metadata);
            current_metadata_version.store(new_metadata_version);
        }
    }

    /// Populated below, under lock; removePartsFinally() (after the lock-scoping block ends) needs
    /// its own internal lockParts() call, so it must run once `lock` itself has been released --
    /// see the comment where to_remove is filled in.
    DataPartsVector to_remove;
    {
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
    /// Another replica registered this part in Keeper and wrote it to the shared disk;
    /// buildPartFromDisk() builds the part object from the on-disk directory (already named
    /// exactly `name`, no rename needed) and admitPartLocally() adds it to the active set.
    auto try_adopt_part = [&](const String & name) -> bool
    {
        auto part = buildPartFromDisk(name);
        if (!part)
            return false;
        /// See admitPartLocally()'s doc comment: any part this adoption covers/supersedes goes
        /// into the same to_remove collected below for the explicit-removal case -- both are
        /// erased together, once, after this function's lock is released.
        auto covered = admitPartLocally(part, lock);
        for (const auto & part_to_forget : covered)
            modifyPartState(part_to_forget, DataPartState::Deleting, lock);
        to_remove.insert(to_remove.end(), covered.begin(), covered.end());
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
        /// that adoption's own Transaction::commit(), so it can't appear here (still_known only
        /// queries {Active}, and try_adopt_part's covered parts were already transitioned to
        /// Deleting above -- kept in a genuinely separate vector below so they're never passed to
        /// removePartsFromWorkingSet() a second time). Whatever remains has no replacement coming
        /// this cycle and can be dropped outright.
        auto still_known = getDataPartsVectorForInternalUsage({DataPartState::Active}, {DataPartKind::Regular}, lock);
        DataPartsVector no_longer_active;
        for (const auto & part : still_known)
            if (!active_set.contains(part->name))
                no_longer_active.push_back(part);

        if (!no_longer_active.empty())
        {
            /// Same reasoning as detachActivePartsMatching's identical pattern: a name removed
            /// here (merge-superseded source, permanent DROP, or -- since this loop can't tell
            /// which -- a DETACH run on another replica) must not linger as a stale Outdated
            /// entry, since a DETACH's name can be reused by an immediately-following ATTACH.
            /// Transition to Deleting now, under lock; removePartsFinally() actually erases it
            /// from data_parts_indexes, called below once this lock-scoping block has ended (it
            /// takes its own lockParts() internally).
            removePartsFromWorkingSet(/*txn=*/ nullptr, no_longer_active, /*clear_without_timeout=*/ true, lock);
            for (const auto & part : no_longer_active)
                modifyPartState(part, DataPartState::Deleting, lock);
            to_remove.insert(to_remove.end(), no_longer_active.begin(), no_longer_active.end());
        }

        current_parts_version.store(new_version);
    }
    else
        part_set_updating_task->scheduleAfter(1000);
    }
    if (!to_remove.empty())
        removePartsFinally(to_remove);
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
