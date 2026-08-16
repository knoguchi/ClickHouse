#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <base/types.h>
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

    /// Idempotently create the root node hierarchy. Safe to call from every replica on startup.
    void createRootNodes(const zkutil::ZooKeeperPtr & zk) const;

    /// INSERT commit: atomically register a freshly written part as active.
    /// Returns ZOK on success, ZNODEEXISTS if a part with that name is already active.
    /// The caller must only flip the part Active in its in-memory cache after ZOK.
    Coordination::Error tryCommitInsert(
        const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header) const;

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

    /// DROP: atomically deactivate parts. Object data is left for GC, never deleted inline.
    Coordination::Error tryRemoveParts(const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const;

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

private:
    const String root_path;
};

}
