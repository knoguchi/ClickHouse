#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <base/types.h>
#include <expected>
#include <vector>

namespace DB
{

/** Keeper coordination for CloudMergeTree.
  *
  * Owns the canonical, global set of active parts. This is the single source of
  * truth: a part is active iff its znode exists under <root>/parts/. Replicas are
  * stateless caches of this set; they never own parts.
  *
  * The layout under <root>:
  *   parts/<part_name>          -> part header (columns + checksums). The cversion
  *                                 of the parts parent is the part-set version.
  *   block_numbers/<partition>  -> block-number allocation
  *   mutations/<id>             -> mutation commands
  *   leases/<range>             -> merge/mutation assignment leases (ephemeral)
  *   replicas/<session>         -> replica liveness (ephemeral, owns no parts)
  *   temp/                      -> in-flight registrations for crash cleanup
  *   dropped_parts/<part_name>  -> tombstone (value = ms-since-epoch when the part left parts/),
  *                                 written atomically with its parts/<part_name> removal. The
  *                                 parts-killer GC task physically deletes a tombstoned part's
  *                                 shared-storage objects once cloud_merge_tree_gc_grace_period_seconds
  *                                 has elapsed, then removes the tombstone.
  *     .../claim                -> ephemeral, held only while a replica is actively deleting that
  *                                 part's objects; self-heals on crash via session death.
  *
  * All methods take the ZooKeeperPtr per call so a session reconnect cannot leave
  * the object holding a dead handle (mirrors StorageReplicatedMergeTree).
  */
class CloudMergeTreeCoordination
{
public:
    explicit CloudMergeTreeCoordination(String root_path_);

    const String & getRootPath() const { return root_path; }
    String partsPath() const { return root_path + "/parts"; }
    String partPath(const String & part_name) const { return partsPath() + "/" + part_name; }
    String blockNumbersPath() const { return root_path + "/block_numbers"; }
    String blockNumbersPartitionPath(const String & partition_id) const { return blockNumbersPath() + "/" + partition_id; }
    String tempPath() const { return root_path + "/temp"; }
    String leasesPath() const { return root_path + "/leases"; }
    String leasePath(const String & merged_part_name) const { return leasesPath() + "/" + merged_part_name; }
    String dropMarkerPath() const { return root_path + "/dropped"; }
    String droppedPartsPath() const { return root_path + "/dropped_parts"; }
    String droppedPartPath(const String & part_name) const { return droppedPartsPath() + "/" + part_name; }
    String droppedPartClaimPath(const String & part_name) const { return droppedPartPath(part_name) + "/claim"; }

    /// Idempotently create the root node hierarchy. Safe to call from every replica on startup.
    void createRootNodes(const zkutil::ZooKeeperPtr & zk) const;

    /// Idempotently ensure block_numbers/<partition_id> exists. Safe to call from every replica;
    /// createRootNodes() only creates the block_numbers parent, not per-partition children.
    void ensureBlockNumbersPartition(const zkutil::ZooKeeperPtr & zk, const String & partition_id) const;

    /// INSERT commit: atomically register a freshly written part as active. extra_ops (e.g. the
    /// block-number lock's unlock op) ride along in the same multi() so allocation and commit are
    /// atomic together.
    /// Returns ZOK on success, ZNODEEXISTS if a part with that name is already active.
    /// The caller must only flip the part Active in its in-memory cache after ZOK.
    Coordination::Error tryCommitInsert(
        const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header,
        Coordination::Requests extra_ops = {}) const;

    /// MERGE/MUTATE commit: atomically add the result and remove its sources, fenced by a lease.
    /// The whole thing is a single multi(): create parts/<merged>, remove parts/<source_i>,
    /// and check that lease_path still holds at lease_version. If the lease moved (we lost the
    /// race) the multi() fails and the caller must discard the produced part. This is the
    /// exactly-once-materialization guarantee.
    Coordination::Error tryCommitMerge(
        const zkutil::ZooKeeperPtr & zk,
        const String & merged_part_name,
        const String & merged_part_header,
        const Strings & source_part_names,
        const String & lease_path,
        int32_t lease_version) const;

    struct LeaseHandle
    {
        String path;
        int32_t version;
    };

    /// Acquire (or, if stale, steal) the lease for a merge result name. Ephemeral, tied to this
    /// session: a crashed holder's lease vanishes on its own when the session dies. A *live but
    /// stuck* holder's lease does not, though -- staleness_threshold_ms bounds how long one can
    /// go unheartbeated (see touchLease) before another replica may steal it, fenced by the
    /// existing lease's own version so the steal only lands if the holder truly hasn't touched
    /// it since we read it.
    /// A ZNODEEXISTS-shaped failure (lease held and fresh) means someone else is already merging
    /// this range -- the caller should treat that as "nothing to do this cycle", not retry in a
    /// loop.
    std::expected<LeaseHandle, Coordination::Error> acquireOrStealLease(
        const zkutil::ZooKeeperPtr & zk, const String & lease_path, const String & holder_data,
        Int64 staleness_threshold_ms) const;

    /// Heartbeat: bump the lease's mtime/version so acquireOrStealLease() won't consider it
    /// stale. Returns the new version to use for the next heartbeat and for the eventual
    /// commit's lease_version. ZBADVERSION means another replica already stole this lease (we
    /// went stale) -- the caller must stop and discard its in-progress work, not keep going.
    std::expected<int32_t, Coordination::Error> touchLease(
        const zkutil::ZooKeeperPtr & zk, const String & lease_path, int32_t current_version) const;

    /// Best-effort release after a successful commit. Safe (silently ignored) if already gone.
    void releaseLease(const zkutil::ZooKeeperPtr & zk, const String & lease_path, int32_t lease_version) const;

    /// DROP: atomically deactivate parts. Object data is left for GC, never deleted inline.
    Coordination::Error tryRemoveParts(const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const;

    /// DROP: marks that DROP TABLE was issued for this table. Every replica gets its own
    /// independent DROP TABLE query and may call this concurrently -- idempotent, ZNODEEXISTS
    /// from a losing racer is treated as success, the marker only needs to exist once. Physical
    /// cleanup is no longer gated on winning this call (each part is now individually tombstoned
    /// and claimed for deletion by the parts-killer GC task, see dropped_parts/ above); this
    /// marker is read by that same GC task as "once parts/ and dropped_parts/ are both empty for
    /// this table, the table root directory itself is safe to remove."
    Coordination::Error markTableDropped(const zkutil::ZooKeeperPtr & zk) const;

    /// A tombstoned part awaiting GC: its name and when it left parts/ (ms since epoch).
    struct Tombstone
    {
        String part_name;
        Int64 dropped_at_ms;
    };

    /// List all tombstones currently recorded under dropped_parts/, with their drop timestamps.
    std::vector<Tombstone> listTombstones(const zkutil::ZooKeeperPtr & zk) const;

    /// Best-effort mutual exclusion for physically deleting one tombstoned part's objects.
    /// Returns true iff we now hold the claim; false (ZNODEEXISTS) means another replica's GC
    /// cycle already claimed it -- skip, don't retry this cycle. Ephemeral: a crashed claimant's
    /// claim vanishes with its session, unblocking retry without needing a staleness threshold
    /// (GC claims are held only for the duration of one delete, much shorter than a Keeper
    /// session timeout).
    bool tryClaimTombstoneForDeletion(const zkutil::ZooKeeperPtr & zk, const String & part_name) const;

    /// Release a claim without removing the tombstone (e.g. the delete failed) so another
    /// replica's next cycle can retry promptly rather than waiting for session death.
    void releaseTombstoneClaim(const zkutil::ZooKeeperPtr & zk, const String & part_name) const;

    /// Remove a tombstone and its claim together, atomically, once its objects are confirmed
    /// deleted. Safe/no-op if already gone (a concurrent retry got there first).
    void releaseTombstone(const zkutil::ZooKeeperPtr & zk, const String & part_name) const;

    /// Load the current active part set. Fills out_parts_version with the parts-parent cversion,
    /// the monotonic part-set version used for change detection and read snapshots.
    Strings loadActivePartNames(const zkutil::ZooKeeperPtr & zk, int32_t & out_parts_version) const;

    /// Same as above, but arms a one-shot watch on the `parts` parent so watch_callback fires the
    /// next time the active part set changes. Used by the part-set watcher to notice inserts made
    /// by other replicas without polling.
    Strings loadActivePartNames(
        const zkutil::ZooKeeperPtr & zk, int32_t & out_parts_version, Coordination::WatchCallbackPtr watch_callback) const;

    /// Read a single part header. Returns false if the part is no longer active.
    bool tryGetPartHeader(const zkutil::ZooKeeperPtr & zk, const String & part_name, String & out_header) const;

    /// Cheap freshness check: just the parts-parent cversion, no children listing. Used before
    /// merge selection to detect a replica whose local view is behind Keeper's current state --
    /// selecting against a known-stale view can silently skip a part this replica hasn't adopted
    /// yet but is genuinely still active, producing a merge whose name claims a block range it
    /// doesn't actually fully own (the merge predicate's gap check only sees locally-known parts,
    /// so it can't catch this). See StorageCloudMergeTree::selectPartsToMerge().
    int32_t getPartsVersion(const zkutil::ZooKeeperPtr & zk) const;

private:
    const String root_path;
};

}
