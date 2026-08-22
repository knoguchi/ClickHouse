#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSink.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/Context.h>
#include <Core/DeduplicateInsert.h>

namespace DB
{

CloudMergeTreeSink::CloudMergeTreeSink(
    StorageCloudMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , storage(storage_)
    , metadata_snapshot(std::move(metadata_snapshot_))
    , max_parts_per_block(max_parts_per_block_)
    , context(std::move(context_))
{
}

CloudMergeTreeSink::~CloudMergeTreeSink() = default;

void CloudMergeTreeSink::consume(Chunk & chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    /// Present whenever the query pipeline attaches insert-dedup bookkeeping to the chunk (e.g.
    /// to honor insert_deduplication_token) -- absent for pipelines that never set one up. Either
    /// way, commitInsertedPart() below tolerates an empty hash list as "dedup disabled for this
    /// part", matching the pre-existing behavior when the caller wants no deduplication at all.
    auto deduplication_info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();
    IColumn::Selector partition_selector;
    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(
        std::move(block), max_parts_per_block, metadata_snapshot, context, &partition_selector);

    const bool dedup_enabled = isDeduplicationEnabledForInsert(/*is_async_insert=*/ false, context->getSettingsRef());

    for (size_t part_index = 0; part_index < part_blocks.size(); ++part_index)
    {
        auto & current_block = part_blocks[part_index];
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

        /// Wait for all (possibly asynchronous, for object storage) writes to land before we
        /// advertise the part in Keeper.
        temp_part->finalize();

        /// Commit the part's own storage transaction now (mirrors MergeTreeSink's identical
        /// call): finalize() only flushes writer buffers, it does not publish the accumulated
        /// createDirectory/writeFile operations to the shared disk's in-memory tree or the
        /// object storage itself -- those stay queued in an uncommitted DiskObjectStorage
        /// transaction (see IDataPartStorage::beginTransaction/commitTransaction) until
        /// something calls commitTransaction(). commitInsertedPart() below needs the part's
        /// directory and files to genuinely resolve (CloudPartLocation::capture() reads them
        /// via getRemotePaths()) before it builds the Keeper payload -- committing here, before
        /// that capture, is what makes them resolvable. Safe on a dedup/race loss: the part
        /// stays under its temp name either way, and removeIfNeeded() removes it whether or not
        /// its transaction was committed.
        temp_part->part->getDataPartStorage().commitTransaction();

        /// Token-aware dedup hash for just this part's share of the chunk: honors
        /// insert_deduplication_token when the caller set one (via DeduplicationInfo::setUserToken
        /// upstream), falls back to a whole-content hash of this part's own rows otherwise -- see
        /// DeduplicationInfo::getDeduplicationHashes()'s own doc comment. Unlike MergeTreeSink,
        /// there is no self-deduplication pass (async-insert token coalescing) or retry-with-
        /// partial-rows-removed loop on conflict: CloudMergeTree doesn't support async insert yet
        /// (is_async_insert is hard-coded false above, matching its current feature scope), and a
        /// dedup collision here already means "discard the whole part", not "rewrite a smaller
        /// one" -- matching this sink's existing all-or-nothing part commit model.
        std::vector<DeduplicationHash> deduplication_hashes;
        if (deduplication_info && dedup_enabled)
        {
            auto current_info = deduplication_info->filterToPartition(partition_selector, part_index, dedup_enabled);
            deduplication_hashes = current_info->getDeduplicationHashes(current_block.partition_id, dedup_enabled);
        }

        /// Keeper-first commit: register the part as active in the canonical set, then flip it
        /// Active in this replica's cache. Throws (INSERT fails) if Keeper rejects it.
        storage.commitInsertedPart(temp_part->part, deduplication_hashes, context);
    }
}

}
