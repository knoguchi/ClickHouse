#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/CloudMergeTree/StorageCloudMergeTree.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>

namespace DB
{

/** Merge-selection predicate for CloudMergeTree, modeled on MergeTreeMergePredicate but trimmed:
  * no MVCC transactions (every part is stamped Tx::NonTransactionalTID; see
  * StorageCloudMergeTree::updatePartSetFromKeeper(), so there is no snapshot/visibility concept
  * to check here). getPatchesToApplyOnMerge() *is* implemented for real (see StorageCloudMergeTree
  * lightweight-UPDATE support) -- it looks up currently-active patch parts in the merge range's
  * corresponding patch-partition(s), gated on the `apply_patches_on_merge` setting, the same way
  * MergeTreeMergePredicate's own implementation does; nothing about patch *application* during a
  * merge is Keeper-sync-specific, only patch *discovery* during merge-input *selection* is
  * deliberately kept out of this predicate's scope (see CloudMergeTreePartsCollector's own doc
  * comment on why merge-input selection and patch-application are different operations).
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
    PartsRange getPatchesToApplyOnMerge(const PartsRange &) const override;

private:
    const StorageCloudMergeTree & storage;
    std::unique_lock<std::mutex> & currently_processing_lock;

    /// Active patch parts, grouped by the *original* (non-patch) partition id they apply to --
    /// precomputed once per predicate instance (mirrors MergeTreeMergePredicate's identical
    /// caching), not recomputed per getPatchesToApplyOnMerge() call. No committing-blocks/
    /// min-update-block fencing (unlike MergeTreeMergePredicate's own construction of this map):
    /// CMT has no local, in-memory "currently committing" bookkeeping at all (see this class's own
    /// header doc comment -- no MVCC/snapshot concept here), and Keeper-first commit already means
    /// a patch simply isn't in this map until its znode genuinely exists -- the same
    /// adoption-lag-is-fine reasoning already accepted for regular parts.
    PatchInfosByPartition patches_by_partition;
};

using CloudMergeTreeMergePredicatePtr = std::shared_ptr<const CloudMergeTreeMergePredicate>;

}
