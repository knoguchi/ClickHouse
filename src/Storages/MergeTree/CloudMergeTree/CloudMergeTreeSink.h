#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class StorageCloudMergeTree;

/** INSERT sink for CloudMergeTree.
  *
  * Synchronous and deliberately simple. For each partitioned block it writes a
  * temporary part to the shared object-storage disk, finalizes it, then asks the
  * storage to commit it: register the part in Keeper first and only then flip it
  * Active in the local cache. The commit fails closed if Keeper rejects it, so we
  * never end up with a part that is local-only and invisible to other replicas.
  * Deduplication is token-aware (honors insert_deduplication_token via
  * DeduplicationInfo, see commitInsertedPart()'s doc comment), whole-part-or-nothing
  * -- no per-token partial application/retry-on-conflict like MergeTreeSink's own
  * async-insert path.
  *
  * Async inserts and delayed chunks are not handled yet.
  */
class CloudMergeTreeSink : public SinkToStorage
{
public:
    CloudMergeTreeSink(
        StorageCloudMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        size_t max_parts_per_block_,
        ContextPtr context_);

    ~CloudMergeTreeSink() override;

    String getName() const override { return "CloudMergeTreeSink"; }
    void consume(Chunk & chunk) override;

private:
    StorageCloudMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    ContextPtr context;
};

}
