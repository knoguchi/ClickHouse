#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>

namespace DB
{

/** Merge-selection predicate for CloudMergeTree, modeled on MergeTreeMergePredicate but trimmed:
  * no patch parts (getPatchesToApplyOnMerge always returns {}), no MVCC transactions (every part
  * is stamped Tx::NonTransactionalTID; see StorageCloudMergeTree::updatePartSetFromKeeper(), so
  * there is no snapshot/visibility concept to check here).
  *
  * What's left -- partition equality, the avoid-merges-volume and projection-set checks, the
  * mutation-version equality check, and the outdated-part-level gap check via
  * getMaxLevelInBetween() -- is unchanged from MergeTreeMergePredicate: none of it is specific to
  * how a part got into the set (Keeper-synced vs. a local disk listing), only to what's currently
  * in it.
  */
class CloudMergeTreeMergePredicate final : public IMergePredicate
{
public:
    CloudMergeTreeMergePredicate(const StorageCloudMergeTree & storage_, std::unique_lock<std::mutex> & currently_processing_lock_);
    ~CloudMergeTreeMergePredicate() override = default;

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override;
    std::expected<void, PreformattedMessage> canUsePartInMerges(const MergeTreeDataPartPtr & part) const;
    PartsRange getPatchesToApplyOnMerge(const PartsRange &) const override { return {}; }

private:
    const StorageCloudMergeTree & storage;
    std::unique_lock<std::mutex> & currently_processing_lock;
};

using CloudMergeTreeMergePredicatePtr = std::shared_ptr<const CloudMergeTreeMergePredicate>;

}
