#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <base/defines.h>

namespace DB
{

CloudMergeTreeMergePredicate::CloudMergeTreeMergePredicate(
    const StorageCloudMergeTree & storage_, std::unique_lock<std::mutex> & currently_processing_lock_)
    : storage(storage_)
    , currently_processing_lock(currently_processing_lock_)
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

    if (left.info.mutation != right.info.mutation)
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

}
