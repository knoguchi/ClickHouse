#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreePartsCollector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>
#include <base/defines.h>

namespace DB
{

namespace
{
    MergeTreeDataPartsVector collectInitial(const StorageCloudMergeTree & storage, MergeTreeData::DataPartKind kind)
    {
        return storage.getDataPartsVectorForInternalUsage(
            {MergeTreeData::DataPartState::Active}, {kind});
    }

    auto constructPreconditionsPredicate(const CloudMergeTreeMergePredicatePtr & merge_pred)
    {
        return [merge_pred](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
        {
            chassert(merge_pred);
            return merge_pred->canUsePartInMerges(part);
        };
    }
}

CloudMergeTreePartsCollector::CloudMergeTreePartsCollector(
    const StorageCloudMergeTree & storage_, CloudMergeTreeMergePredicatePtr merge_pred_, MergeTreeData::DataPartKind kind_)
    : storage(storage_)
    , merge_pred(std::move(merge_pred_))
    , kind(kind_)
{
}

CollectedPartsRanges CloudMergeTreePartsCollector::grabAllPossibleRanges(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log) const
{
    auto parts = filterByPartitions(collectInitial(storage, kind), partitions_hint);
    auto partitions_stats = calculateStatisticsForParts(parts, current_time);
    auto ranges = splitRangeByPredicate(std::move(parts), constructPreconditionsPredicate(merge_pred), series_log);
    return {constructPartsRanges(std::move(ranges), metadata_snapshot, storage_policy, current_time), std::move(partitions_stats)};
}

std::expected<PartsRange, PreformattedMessage> CloudMergeTreePartsCollector::grabAllPartsInsidePartition(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id) const
{
    auto parts = filterByPartitions(collectInitial(storage, kind), PartitionIdsHint{partition_id});
    if (auto result = checkAllPartsSatisfyPredicate(parts, constructPreconditionsPredicate(merge_pred)); !result)
        return std::unexpected(std::move(result.error()));

    auto ranges = constructPartsRanges({std::move(parts)}, metadata_snapshot, storage_policy, current_time);
    chassert(ranges.size() == 1);

    return std::move(ranges.front());
}

}
