#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <base/types.h>
#include <expected>
#include <map>
#include <set>
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
  *   metadata                   -> the table's current column list, serialized via
  *                                 ColumnsDescription::toString()/parse(). The znode's own Keeper
  *                                 version *is* the table's metadata_version (same trick `parts`
  *                                 plays with its cversion) -- no separate counter in the payload.
  *                                 CAS-updated by ALTER ADD/DROP/MODIFY/RENAME COLUMN. A command
  *                                 requiring an actual data rewrite (e.g. a type-changing MODIFY
  *                                 COLUMN) additionally submits a mutations/<id> entry atomically in
  *                                 the same multi(), see trySetMetadataAndCreateMutation() and
  *                                 StorageCloudMergeTree::alter(). Parts stamp this version into
  *                                 their own metadata_version.txt at write time (upstream MergeTree
  *                                 machinery, unmodified); the shared reader path already
  *                                 materializes defaults on the fly for a part missing a newer
  *                                 column, so old parts need no rewrite when a column is added.
  *   parts/<part_name>          -> part header (columns + checksums). The cversion
  *                                 of the parts parent is the part-set version.
  *   block_numbers/<partition>  -> block-number allocation
  *   mutations/<id>             -> mutation commands, serialized as a ReplicatedMergeTreeMutationEntry
  *                                 (source_replica unused, kept only because the struct is shared).
  *                                 alter_version is -1 for a manually-submitted mutation, or the
  *                                 metadata znode's resulting version for one submitted atomically
  *                                 alongside an ALTER requiring a data rewrite (see
  *                                 trySetMetadataAndCreateMutation() below). id is Keeper-allocated
  *                                 (PersistentSequential).
  *                                 block_numbers in the entry is a per-partition snapshot taken via
  *                                 the same barrier-lock primitive INSERT uses for block_numbers/ --
  *                                 a part with min_block below the snapshot existed before the
  *                                 mutation was submitted and needs it applied; a part created after
  *                                 does not.
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
  *   detached_parts/<part_name> -> DETACH marker (value = ms-since-epoch when detached), written
  *                                 atomically with its parts/<part_name> removal, same shape as
  *                                 dropped_parts/ but a deliberately separate namespace: the
  *                                 parts-killer GC task only ever scans dropped_parts/, so a
  *                                 detached part's shared-storage objects are never touched by GC.
  *                                 The part's directory itself never moves (there is only ever one
  *                                 shared copy) -- ATTACH re-creates parts/<part_name> with the same
  *                                 name and removes the marker, atomically, via tryReattachPart().
  *   deduplication_hashes/<id>  -> insert dedup: value = the part name that won this content hash.
  *                                 Written atomically with parts/<part_name> on INSERT when
  *                                 insert_deduplicate is enabled (see DeduplicationHash in
  *                                 Interpreters/InsertDeduplication.h, whose HashType::UNIFIED
  *                                 directory name this must match literally).
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
    String mutationsPath() const { return root_path + "/mutations"; }
    String mutationPath(const String & mutation_id) const { return mutationsPath() + "/" + mutation_id; }
    String metadataPath() const { return root_path + "/metadata"; }
    String replicasPath() const { return root_path + "/replicas"; }
    String replicasAzPath(const String & az) const { return replicasPath() + "/" + az; }
    String dropMarkerPath() const { return root_path + "/dropped"; }
    String droppedPartsPath() const { return root_path + "/dropped_parts"; }
    String droppedPartPath(const String & part_name) const { return droppedPartsPath() + "/" + part_name; }
    String droppedPartClaimPath(const String & part_name) const { return droppedPartPath(part_name) + "/claim"; }
    String detachedPartsPath() const { return root_path + "/detached_parts"; }
    String detachedPartPath(const String & part_name) const { return detachedPartsPath() + "/" + part_name; }
    /// Must match DeduplicationHash::HashType::UNIFIED's directory name literally (see
    /// Interpreters/InsertDeduplication.cpp) -- createUnifiedHash()'s produced paths land here.
    String deduplicationHashesPath() const { return root_path + "/deduplication_hashes"; }

    /// Idempotently create the root node hierarchy. Safe to call from every replica on startup.
    void createRootNodes(const zkutil::ZooKeeperPtr & zk) const;

    /// Idempotently ensure block_numbers/<partition_id> exists. Safe to call from every replica;
    /// createRootNodes() only creates the block_numbers parent, not per-partition children.
    void ensureBlockNumbersPartition(const zkutil::ZooKeeperPtr & zk, const String & partition_id) const;

    /// INSERT commit: atomically register a freshly written part as active. extra_ops (e.g. the
    /// block-number lock's unlock op, or an insert-dedup path's create request) ride along in the
    /// same multi() so allocation and commit are atomic together.
    /// Returns ZOK on success, ZNODEEXISTS if a part with that name -- or one of extra_ops's own
    /// create requests, e.g. a dedup path -- already exists. out_responses lets the caller use
    /// zkutil::getFailedOpIndex() to tell which op collided (index 0 is always the part znode
    /// itself; extra_ops occupy the following indices in the order given).
    /// The caller must only flip the part Active in its in-memory cache after ZOK.
    Coordination::Error tryCommitInsert(
        const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header,
        Coordination::Requests extra_ops, Coordination::Responses & out_responses) const;

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

    /// REPLACE/ATTACH PARTITION ... FROM commit: atomically register the newly-cloned parts and,
    /// when replacing (not attaching), deactivate+tombstone whatever they replace -- same shape as
    /// tryCommitMerge's "N creates + M removes-and-tombstones", but no lease check (this executes
    /// synchronously against the resolved part set, like optimize()'s own synchronous loop; a
    /// concurrent race on the same names fails closed via ZNONODE on the remove ops, same
    /// fail-closed/retry contract every other commit in this file already relies on).
    /// old_part_names_to_remove is empty for ATTACH PARTITION ... FROM (replace=false).
    Coordination::Error tryReplacePartition(
        const zkutil::ZooKeeperPtr & zk,
        const std::vector<std::pair<String, String>> & new_parts_with_headers,
        const Strings & old_part_names_to_remove) const;

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

    /// Per-AZ merge-selection leader election (see StorageCloudMergeTree::az_leadership_recheck_task
    /// for why): registers this replica under replicas/<az>/ with an ephemeral-sequential node,
    /// creating that AZ's subtree first if this is the first replica to register in it. The node
    /// name itself is a fixed literal prefix plus Keeper's zero-padded sequence number, identical
    /// for every replica in the AZ -- required so plain lexicographic comparison in
    /// isLowestSequenceInAz() actually orders by sequence number; display_name is only the node's
    /// *value*, for introspection. Returns the created node's full path, which the caller must
    /// remember: both to recognize its own node in isLowestSequenceInAz()'s listing, and to
    /// remove it explicitly on a clean shutdown.
    String registerReplicaForAzElection(const zkutil::ZooKeeperPtr & zk, const String & az, const String & display_name) const;

    /// Whether own_node_path (as returned by registerReplicaForAzElection()) currently holds the
    /// lowest sequence number among replicas/<az>/'s children -- the standard ZK leader-election
    /// recipe. A crashed leader's node is already gone from this listing (ephemeral, tied to its
    /// session), so this alone is enough to detect failover on the next call; no staleness
    /// threshold needed here, unlike acquireOrStealLease()'s merge leases (see its own doc comment
    /// for why that one does).
    bool isLowestSequenceInAz(const zkutil::ZooKeeperPtr & zk, const String & az, const String & own_node_path) const;

    /// DROP: atomically deactivate parts. Object data is left for GC, never deleted inline.
    Coordination::Error tryRemoveParts(const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const;

    /// TRUNCATE/DROP PARTITION/the destination side of REPLACE PARTITION: deduplication_hashes/
    /// znodes are otherwise permanent (see the class doc comment above) -- without this, the exact
    /// same content re-inserted after one of these commands is silently discarded as a dedup hit
    /// against data that no longer exists. Mirrors StorageReplicatedMergeTree::clearBlocksInPartition,
    /// adapted to this table's flat "<partition_id>_<hash0>_<hash1>" naming (see DeduplicationHash::
    /// getBlockId(), Interpreters/InsertDeduplication.h) instead of a per-partition subtree: an empty
    /// partition_id clears every hash in the table (TRUNCATE), a non-empty one only that partition's.
    void clearDeduplicationHashes(const zkutil::ZooKeeperPtr & zk, const String & partition_id = {}) const;

    /// DETACH: atomically deactivate parts, same as tryRemoveParts, but records the removal under
    /// detached_parts/ instead of dropped_parts/ -- the parts-killer GC task never scans that
    /// namespace, so the parts' shared-storage objects are never deleted while detached.
    Coordination::Error tryDetachParts(const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const;

    /// ATTACH: the reverse of tryDetachParts for one part -- atomically removes
    /// detached_parts/<part_name> and re-creates parts/<part_name> with the given header. ZNONODE
    /// (the detached marker is already gone) or ZNODEEXISTS (parts/<part_name> already exists)
    /// both mean another replica's concurrent ATTACH of the same part already won; the caller
    /// should treat that as success (the part is active in Keeper either way), not an error.
    Coordination::Error tryReattachPart(const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header) const;

    /// List the names of every part currently recorded under detached_parts/.
    Strings listDetachedPartNames(const zkutil::ZooKeeperPtr & zk) const;

    /// DROP: marks that DROP TABLE was issued for this table. Every replica gets its own
    /// independent DROP TABLE query and may call this concurrently -- idempotent, ZNODEEXISTS
    /// from a losing racer is treated as success, the marker only needs to exist once. Physical
    /// cleanup is no longer gated on winning this call (each part is now individually tombstoned
    /// and claimed for deletion by the parts-killer GC task, see dropped_parts/ above); this
    /// marker is read by that same GC task as "once parts/ and dropped_parts/ are both empty for
    /// this table, the table root directory itself is safe to remove."
    Coordination::Error markTableDropped(const zkutil::ZooKeeperPtr & zk) const;

    /// A tombstoned part awaiting GC: its name, when it left parts/ (ms since epoch), and its
    /// location trailer (see CloudPartLocation) copied from its part znode at removal time --
    /// empty for a tombstone written before locations existed (not expected in a clean-slate
    /// deployment; see the class doc comment on dropped_parts/).
    struct Tombstone
    {
        String part_name;
        Int64 dropped_at_ms;
        String location_text;
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

    /// Snapshot the current block-number watermark for each given partition: for each, acquires
    /// and immediately releases an ephemeral-sequential lock under block_numbers/<partition>/ (the
    /// same primitive INSERT uses to allocate a part's own block number) and records its number.
    /// Since that counter only ever advances, any part already active with min_block below the
    /// returned number is guaranteed to have existed before this call -- the barrier a mutation
    /// needs to know which parts it must apply to versus which post-date it. Creates
    /// block_numbers/<partition> first if not already present (mirrors ensureBlockNumbersPartition).
    std::map<String, Int64> snapshotBlockNumbers(const zkutil::ZooKeeperPtr & zk, const std::set<String> & partition_ids) const;

    /// Persist a new mutation entry (caller-serialized text, see the class doc comment) under
    /// mutations/, Keeper-allocating its id. Returns the allocated znode name (the mutation id).
    String createMutation(const zkutil::ZooKeeperPtr & zk, const String & entry_text) const;

    /// List every mutation entry currently recorded, as (znode_name, raw text) pairs -- the caller
    /// (which already depends on MutationCommands/ReplicatedMergeTreeMutationEntry) parses them;
    /// this class deliberately stays decoupled from those types, same as everywhere else here.
    std::vector<std::pair<String, String>> listMutations(const zkutil::ZooKeeperPtr & zk) const;

    /// Idempotently establish the table's initial schema in Keeper on first CREATE/ATTACH -- a
    /// no-op if already present (an earlier CREATE, or a faster-starting replica, already wrote
    /// it). Every replica calls this on startup; whichever wins establishes version 0 for
    /// getMetadataVersion()/trySetMetadata() below to CAS against.
    void ensureInitialMetadata(const zkutil::ZooKeeperPtr & zk, const String & initial_columns_text) const;

    /// Cheap freshness check: the metadata znode's own Keeper version, reused directly as the
    /// table's metadata_version (same trick getPartsVersion() plays with the parts parent's
    /// cversion) -- no separate counter needed in the payload.
    int32_t getMetadataVersion(const zkutil::ZooKeeperPtr & zk) const;

    /// Read the current columns text plus its version in one round trip -- for a fresh CAS
    /// baseline after losing a race (see StorageCloudMergeTree::alter()) or a watcher refresh.
    /// The watch_callback overload additionally arms a one-shot watch so watch_callback fires the
    /// next time the schema changes -- used by the part-set watcher to notice an ALTER run by
    /// another replica without polling.
    std::pair<String, int32_t> getMetadata(const zkutil::ZooKeeperPtr & zk) const;
    std::pair<String, int32_t> getMetadata(const zkutil::ZooKeeperPtr & zk, Coordination::WatchCallbackPtr watch_callback) const;

    /// CAS-write new columns text, fenced on expected_version (from a prior getMetadataVersion()/
    /// getMetadata() call). ZBADVERSION means someone else's ALTER landed first -- the caller must
    /// reload via getMetadata() and reapply its own commands on top of the fresh baseline, not
    /// retry blindly with the same text. On success, out_new_version is the version to store as
    /// the table's new metadata_version (see StorageInMemoryMetadata::withMetadataVersion).
    Coordination::Error trySetMetadata(
        const zkutil::ZooKeeperPtr & zk, const String & new_columns_text, int32_t expected_version, int32_t & out_new_version) const;

    struct MetadataAndMutationResult
    {
        int32_t new_metadata_version;
        String mutation_id;
    };

    /// ALTER commands requiring a data rewrite (e.g. a type-changing MODIFY COLUMN): commit the
    /// metadata change and the mutation that migrates existing parts to it atomically together, in
    /// one multi() -- mirrors StorageReplicatedMergeTree::alter()'s own atomic-together shape. A
    /// crash between two *separate* writes would otherwise leave either a live schema change with
    /// no mutation to ever rewrite old-typed data, or an orphaned mutation naming a metadata state
    /// that was never actually published. ZBADVERSION means someone else's ALTER landed first --
    /// same reload-and-retry contract as trySetMetadata(). On success, new_metadata_version is
    /// always expected_metadata_version + 1 (Keeper znode versions increment by exactly 1 per
    /// successful write) -- computed without a second round trip, same precomputation
    /// StorageReplicatedMergeTree::alter() itself relies on before its own multi().
    std::expected<MetadataAndMutationResult, Coordination::Error> trySetMetadataAndCreateMutation(
        const zkutil::ZooKeeperPtr & zk, const String & new_columns_text, int32_t expected_metadata_version,
        const String & mutation_entry_text) const;

private:
    const String root_path;
};

}
