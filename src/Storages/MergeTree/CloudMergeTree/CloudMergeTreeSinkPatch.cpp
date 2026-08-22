#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSinkPatch.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CloudMergeTreeSinkPatch::CloudMergeTreeSinkPatch(
    StorageCloudMergeTree & storage_,
    PatchPartMetadata patch_metadata_,
    std::unordered_map<String, Int64> partition_data_versions_,
    zkutil::EphemeralNodeHolderPtr update_lock_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(patch_metadata_.metadata->getSampleBlock()))
    , storage(storage_)
    , patch_metadata(std::move(patch_metadata_))
    , partition_data_versions(std::move(partition_data_versions_))
    , update_lock(std::move(update_lock_))
    , context(std::move(context_))
{
}

CloudMergeTreeSinkPatch::~CloudMergeTreeSinkPatch()
{
    /// update_lock's EphemeralNodeHolder::~EphemeralNodeHolder() removes its znode via the same
    /// Keeper wrapper every other CMT operation goes through -- which asserts a component is set
    /// for tracing/fault-injection scoping (see Coordination::setCurrentComponent's own doc
    /// comment). Without this guard, destroying the sink outside of any other CMT call's scope
    /// (e.g. this replica's server shutting down with a still-alive sink) throws LOGICAL_ERROR
    /// from inside a destructor. Mirrors ReplicatedMergeTreeSinkPatch::
    /// ~ReplicatedMergeTreeSinkPatch()'s identical guard around its own update_holder.reset().
    auto component_guard = Coordination::setCurrentComponent("CloudMergeTreeSinkPatch::~CloudMergeTreeSinkPatch");
    update_lock.reset();
}

void CloudMergeTreeSinkPatch::consume(Chunk & chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    /// Deliberately no `out_selector`/DeduplicationInfo here (unlike CloudMergeTreeSink::consume):
    /// dedup is always off for patch parts, see this class's own header doc comment.
    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(
        std::move(block), /*max_parts=*/ 0, patch_metadata.metadata, context);

    for (auto & current_block : part_blocks)
    {
        /// current_block.partition_id is MergeTreePartition::getID()'s generic hash of the
        /// partition-by expression's result -- NOT usable here: patch_metadata.metadata's
        /// partition-by expression (__patchPartitionID(_part, hash), see PatchPartsUtils.cpp's
        /// getPatchPartMetadataV2()) already evaluates to the full, literal
        /// 'patch-<hash>-<original_partition_id>' string as its one partition-key Field, and
        /// getPartitionIdForPatch() is the accessor that reads it back out of
        /// current_block.partition directly instead of re-hashing it -- mirrors
        /// ReplicatedMergeTreeSinkPatch::writeNewTempPart()'s identical call.
        const String patch_partition_id = getPartitionIdForPatch(current_block.partition);
        const String original_partition_id = getOriginalPartitionIdOfPatch(patch_partition_id);
        auto it = partition_data_versions.find(original_partition_id);
        if (it == partition_data_versions.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No block number allocated for partition {} (patch partition {}) -- "
                "updateLightweight() should have allocated one for every partition "
                "getPartitionIdsAffectedByCommands() reported",
                original_partition_id, patch_partition_id);
        const Int64 data_version = it->second;

        auto patch_part_index = buildPatchPartIndex(*current_block.block, data_version, patch_metadata);

        auto temp_part = storage.writer.writeTempPatchPart(
            current_block, patch_metadata.metadata, patch_partition_id, std::move(patch_part_index), context);

        /// Same explicit finalize()+commitTransaction() sequencing as CloudMergeTreeSink::consume()
        /// -- see its own doc comment for why: commitInsertedPart() below needs the part's
        /// directory and files to genuinely resolve (CloudPartLocation::capture() reads them via
        /// getRemotePaths()) before it builds the Keeper payload.
        temp_part->finalize();
        temp_part->part->getDataPartStorage().commitTransaction();

        /// Empty deduplication_hashes: dedup is always off for patch parts (see header doc
        /// comment) -- this is the one required behavioral difference from CloudMergeTreeSink's
        /// otherwise-identical commit call. Everything else about this call -- the Keeper-first
        /// commit, admitPartLocally(), CloudPartLocation capture/authority sequencing -- is
        /// unmodified: a patch part is a completely ordinary IMergeTreeDataPart to
        /// commitInsertedPart(), which is the entire point (see this class's header doc comment
        /// on why there must be no second, parallel commit path for patch parts).
        storage.commitInsertedPart(temp_part->part, /*deduplication_hashes=*/{}, context);
    }
}

}
