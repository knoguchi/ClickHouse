#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <unordered_map>

namespace DB
{

class StorageCloudMergeTree;

/** Sink for `ALTER TABLE ... UPDATE ... SET` (lightweight update). Completes the QueryPipeline
  * MergeTreeData::updateLightweightImpl() builds -- see StorageCloudMergeTree::updateLightweight().
  *
  * Modeled on CloudMergeTreeSink (synchronous, no async-insert/delayed-chunk handling), but
  * writes patch parts (MergeTreeDataWriter::writeTempPatchPart(), stamped with a Keeper-allocated
  * per-partition data version, see PatchPartIndex) instead of regular parts. Commits through the
  * exact same StorageCloudMergeTree::commitInsertedPart() Keeper-commit hook CloudMergeTreeSink
  * already uses for INSERT -- deliberately: a patch part is a completely ordinary
  * IMergeTreeDataPart (checksums.txt, marks/data files, one extra checksummed file --
  * source_parts.dat) with no format-level distinction from a regular part at the storage/Keeper
  * layer, so it needs no second, parallel commit path. Dedup is always disabled for patch parts
  * (matches ReplicatedMergeTreeSinkPatch's identical choice): two different UPDATE statements
  * producing byte-identical patch content (same predicate, same SET, retried) must both apply,
  * not silently dedup-collide and drop the second, semantically-required update.
  */
class CloudMergeTreeSinkPatch : public SinkToStorage
{
public:
    CloudMergeTreeSinkPatch(
        StorageCloudMergeTree & storage_,
        PatchPartMetadata patch_metadata_,
        std::unordered_map<String, Int64> partition_data_versions_,
        zkutil::EphemeralNodeHolderPtr update_lock_,
        ContextPtr context_);

    ~CloudMergeTreeSinkPatch() override;

    String getName() const override { return "CloudMergeTreeSinkPatch"; }
    void consume(Chunk & chunk) override;

private:
    StorageCloudMergeTree & storage;
    PatchPartMetadata patch_metadata;
    /// Original (non-patch) partition id -> Keeper-allocated block number, one per partition
    /// affected by this UPDATE, allocated up front by updateLightweight() before this sink was
    /// constructed. Every patch part produced for a given original partition is stamped with
    /// this same data version (see PatchPartIndex::build()'s doc comment) -- mirrors
    /// commitInsertedPart()'s own per-partition block allocation for regular INSERTs, just
    /// allocated once for the whole UPDATE rather than once per part.
    std::unordered_map<String, Int64> partition_data_versions;
    /// Held for this sink's whole lifetime, released on destruction (matches
    /// ReplicatedMergeTreeSinkPatch::~ReplicatedMergeTreeSinkPatch()'s identical
    /// update_holder.reset()) -- null when update_parallel_mode='sync' didn't need one or the
    /// generic getLockForLightweightUpdateInKeeper() decided no lock was necessary for this
    /// specific command set.
    zkutil::EphemeralNodeHolderPtr update_lock;
    ContextPtr context;
};

}
