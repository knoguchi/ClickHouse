#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeMergePredicate.h>

namespace DB
{

/** Parts collector for CloudMergeTree, modeled on MergeTreePartsCollector but trimmed: no MVCC
  * transactions, so collectInitial() is a single getDataPartsVectorForInternalUsage() call --
  * none of MergeTreePartsCollector's active/outdated merge-and-restore dance for a transaction
  * snapshot that doesn't exist here.
  *
  * Collects exactly one DataPartKind at a time (constructor parameter, defaults to Regular),
  * never both together like upstream's collector does -- deliberately: a "regular merge" (N
  * regular parts -> 1 regular part) and a "patch merge" (N patch parts in the same patch
  * partition -> 1 patch part) are structurally different selection problems that happen to share
  * MergeTask underneath. StorageCloudMergeTree::selectPartsToMerge() instantiates a second,
  * separate instance of this same class configured for DataPartKind::Patch for its patch-merge
  * attempt, rather than this collector ever mixing both kinds into one range -- specifically so a
  * future maintainer can't accidentally conflate merge-input *source* selection with patch
  * *application* (CloudMergeTreeMergePredicate::getPatchesToApplyOnMerge()) -- two different
  * operations.
  */
class CloudMergeTreePartsCollector final : public IPartsCollector
{
public:
    CloudMergeTreePartsCollector(
        const StorageCloudMergeTree & storage_,
        CloudMergeTreeMergePredicatePtr merge_pred_,
        MergeTreeData::DataPartKind kind_ = MergeTreeData::DataPartKind::Regular);
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
    const MergeTreeData::DataPartKind kind;
};

}
