#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <base/defines.h>
#include <limits>

namespace DB
{

namespace
{
    std::vector<MergeTreePartInfo> getActivePatchPartInfos(const StorageCloudMergeTree & storage)
    {
        auto patches_vector = storage.getPatchPartsVectorForInternalUsage();

        std::vector<MergeTreePartInfo> patch_infos;
        patch_infos.reserve(patches_vector.size());
        for (const auto & patch : patches_vector)
            patch_infos.push_back(patch->info);

        return patch_infos;
    }
}

CloudMergeTreeMergePredicate::CloudMergeTreeMergePredicate(
    const StorageCloudMergeTree & storage_, std::unique_lock<std::mutex> & currently_processing_lock_)
    : storage(storage_)
    , currently_processing_lock(currently_processing_lock_)
    , patches_by_partition(getPatchPartsByPartition(getActivePatchPartInfos(storage), std::numeric_limits<Int64>::max()))
{
}

std::expected<void, PreformattedMessage> CloudMergeTreeMergePredicate::canMergeParts(const PartProperties & left, const PartProperties & right) const
{
    if (left.info.getPartitionId() != right.info.getPartitionId())
        return std::unexpected(PreformattedMessage::create("Parts {} and {} belong to different partitions", left.name, right.name));

    if (left.is_in_volume_where_merges_avoid || right.is_in_volume_where_merges_avoid)
        return std::unexpected(PreformattedMessage::create("One of parts ({}, {}) lies on volume where merges should be avoided", left.name, right.name));

    if (left.projection_names != right.projection_names)
    {
        return std::unexpected(PreformattedMessage::create(
            "Parts have different projection sets: {} in '{}' and {} in '{}'",
            left.projection_names, left.name, right.projection_names, right.name));
    }

    /// Patch parts repurpose the .mutation field entirely (it holds the patch's own
    /// max_data_version, stamped by writeTempPartImpl() from its PatchPartIndex -- see
    /// MergeTreeDataWriter.cpp) -- two patches in the same synthetic patch partition are EXPECTED
    /// to differ here (that's exactly what patch-to-patch compaction consolidates), so this check
    /// only applies to regular parts. canMergeParts() is only ever called on same-partition-id
    /// pairs (checked just above), and a patch partition can never contain a mix of regular and
    /// patch parts by construction, so checking one side is sufficient.
    if (!left.info.isPatch() && left.info.mutation != right.info.mutation)
    {
        /// A merge result is stamped mutation = max(left.info.mutation, right.info.mutation) (see
        /// FutureMergedMutatedPart), which would make partNeedsMutation() treat it as already
        /// covering every mutation up to the higher of the two -- true for whichever source
        /// already had it applied, false for the other, whose data would then silently keep its
        /// pre-mutation content forever while system.mutations reports the mutation done. Matches
        /// upstream's own equal-current-mutation-version requirement in canMergeParts.
        return std::unexpected(PreformattedMessage::create(
            "Parts {} and {} have different mutation version", left.name, right.name));
    }

    {
        uint32_t max_possible_level = storage.getMaxLevelInBetween(left, right);

        if (max_possible_level > std::max(left.info.level, right.info.level))
            return std::unexpected(PreformattedMessage::create(
                    "There is an outdated part in a gap between two active parts ({}, {}) with merge level {} higher than these active parts have",
                    left.name, right.name, max_possible_level));
    }

    return {};
}

std::expected<void, PreformattedMessage> CloudMergeTreeMergePredicate::canUsePartInMerges(const MergeTreeDataPartPtr & part) const
{
    chassert(currently_processing_lock.owns_lock()); /// guards currently_merging_mutating_parts

    if (storage.currently_merging_mutating_parts.contains(part->info))
        return std::unexpected(PreformattedMessage::create("Part {} currently in a merging process", part->name));

    return {};
}

PartsRange CloudMergeTreeMergePredicate::getPatchesToApplyOnMerge(const PartsRange & range) const
{
    if (range.empty())
        return {};

    /// A patch-to-patch merge (both range and the patches map itself only ever hold *regular*
    /// parts/partitions -- see this class's header doc comment on why patch *selection* stays out
    /// of scope here) never needs patches applied to it.
    const auto & first_part = range.front().info;
    if (first_part.isPatch())
        return {};

    auto it = patches_by_partition.find(first_part.getPartitionId());
    if (it == patches_by_partition.end() || it->second.empty())
        return {};

    /// next_mutation_version = 0 ("infinity", see DB::getPatchesToApplyOnMerge's own doc comment)
    /// -- deliberate simplification: unlike StorageMergeTree, CMT has no local
    /// current_mutations_by_version map to consult (mutations live in Keeper, read fresh via
    /// loadSortedMutations() elsewhere), so this doesn't fence patches against a specific
    /// about-to-be-applied heavy mutation the way upstream's getNextMutationVersion() does. Not a
    /// correctness gap: at worst a merge applies a patch that a concurrently-running heavy
    /// mutation will shortly overwrite anyway -- redundant work, never a wrong result.
    return DB::getPatchesToApplyOnMerge(it->second, range, /*next_mutation_version=*/ 0);
}

}
