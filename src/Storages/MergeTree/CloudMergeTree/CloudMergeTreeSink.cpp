#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeSink.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

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

    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    for (auto & current_block : part_blocks)
    {
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

        /// Wait for all (possibly asynchronous, for object storage) writes to land before we
        /// advertise the part in Keeper.
        temp_part->finalize();

        /// Keeper-first commit: register the part as active in the canonical set, then flip it
        /// Active in this replica's cache. Throws (INSERT fails) if Keeper rejects it.
        storage.commitInsertedPart(temp_part->part, context);
    }
}

}
