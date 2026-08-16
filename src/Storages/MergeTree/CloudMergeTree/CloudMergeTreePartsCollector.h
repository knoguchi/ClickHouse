#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>

namespace DB
{

/** Parts collector for CloudMergeTree, modeled on MergeTreePartsCollector but trimmed: no MVCC
  * transactions, so collectInitial() is a single getDataPartsVectorForInternalUsage() call --
  * none of MergeTreePartsCollector's active/outdated merge-and-restore dance for a transaction
  * snapshot that doesn't exist here -- and DataPartKind::Regular only, since CloudMergeTree has
  * no patch parts.
  */
class CloudMergeTreePartsCollector final : public IPartsCollector
{
public:
    CloudMergeTreePartsCollector(const StorageCloudMergeTree & storage_, CloudMergeTreeMergePredicatePtr merge_pred_);
    ~CloudMergeTreePartsCollector() override = default;

    CollectedPartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint,
        LogSeriesLimiter & series_log) const override;

    std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::string & partition_id) const override;

private:
    const StorageCloudMergeTree & storage;
    const CloudMergeTreeMergePredicatePtr merge_pred;
};

}
