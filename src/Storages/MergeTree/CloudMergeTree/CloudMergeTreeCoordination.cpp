#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeCoordination.h>

#include <Storages/MergeTree/CloudMergeTree/CloudPartLocation.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/sort.h>
#include <chrono>
#include <optional>

namespace DB
{

namespace
{
    String trimTrailingSlashes(String s)
    {
        while (!s.empty() && s.back() == '/')
            s.pop_back();
        return s;
    }

    Int64 nowMilliseconds()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    /// A removal/detach znode's payload is "<ms-since-epoch>\n<location trailer>": the location
    /// is copied verbatim from the part's own znode payload before it's removed, so GC (or a
    /// later ATTACH) can resolve the part's directories from Keeper without ever having to
    /// re-derive them by re-listing the shared disk. Returns nullopt if the part's znode is
    /// already gone -- the caller must treat that exactly like a ZNONODE from the remove
    /// op it was about to issue (a concurrent racer got there first; fail closed and retry).
    std::optional<String> buildRemovalPayload(const zkutil::ZooKeeperPtr & zk, const String & part_znode_path, Int64 timestamp_ms)
    {
        String part_payload;
        if (!zk->tryGet(part_znode_path, part_payload))
            return std::nullopt;
        return toString(timestamp_ms) + "\n" + CloudPartLocation::extractTrailerText(part_payload);
    }
}

CloudMergeTreeCoordination::CloudMergeTreeCoordination(String root_path_)
    : root_path(trimTrailingSlashes(std::move(root_path_)))
{
}

void CloudMergeTreeCoordination::createRootNodes(const zkutil::ZooKeeperPtr & zk) const
{
    /// Idempotent: every replica may race to create these on startup.
    zk->createAncestors(root_path + "/");
    zk->createIfNotExists(root_path, "");
    zk->createIfNotExists(partsPath(), "");
    zk->createIfNotExists(blockNumbersPath(), "");
    zk->createIfNotExists(root_path + "/mutations", "");
    zk->createIfNotExists(root_path + "/leases", "");
    zk->createIfNotExists(replicasPath(), "");
    zk->createIfNotExists(tempPath(), "");
    zk->createIfNotExists(droppedPartsPath(), "");
    zk->createIfNotExists(detachedPartsPath(), "");
    zk->createIfNotExists(deduplicationHashesPath(), "");
}

void CloudMergeTreeCoordination::ensureBlockNumbersPartition(const zkutil::ZooKeeperPtr & zk, const String & partition_id) const
{
    zk->createIfNotExists(blockNumbersPartitionPath(partition_id), "");
}

Coordination::Error CloudMergeTreeCoordination::tryCommitInsert(
    const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header,
    Coordination::Requests extra_ops, Coordination::Responses & out_responses) const
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(partPath(part_name), part_header, zkutil::CreateMode::Persistent));
    for (auto & op : extra_ops)
        ops.emplace_back(std::move(op));

    return zk->tryMultiNoThrow(ops, out_responses);
}

Coordination::Error CloudMergeTreeCoordination::tryCommitMerge(
    const zkutil::ZooKeeperPtr & zk,
    const String & merged_part_name,
    const String & merged_part_header,
    const Strings & source_part_names,
    const String & lease_path,
    int32_t lease_version) const
{
    Coordination::Requests ops;

    /// Fence: the commit only lands if our lease is still the one at this version.
    /// If another replica took over the range, its lease write bumped the version and
    /// this check fails the whole multi() -> we lose the race and discard our output.
    ops.emplace_back(zkutil::makeCheckRequest(lease_path, lease_version));

    /// Add the merged result.
    ops.emplace_back(zkutil::makeCreateRequest(partPath(merged_part_name), merged_part_header, zkutil::CreateMode::Persistent));

    /// Deactivate the sources and tombstone them in the same atomic step, so the parts-killer
    /// GC task's grace-period clock starts exactly when the source genuinely left parts/, never
    /// derived after the fact (Keeper doesn't retain deleted-znode history). Each tombstone
    /// payload carries the source's own location, read from its still-active znode just before
    /// this multi() -- see buildRemovalPayload().
    const Int64 tombstone_ts = nowMilliseconds();
    for (const auto & source : source_part_names)
    {
        auto payload = buildRemovalPayload(zk, partPath(source), tombstone_ts);
        if (!payload)
            return Coordination::Error::ZNONODE;
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(source), -1));
        ops.emplace_back(zkutil::makeCreateRequest(droppedPartPath(source), *payload, zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

Coordination::Error CloudMergeTreeCoordination::tryReplacePartition(
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<std::pair<String, String>> & new_parts_with_headers,
    const Strings & old_part_names_to_remove) const
{
    Coordination::Requests ops;

    for (const auto & [name, header] : new_parts_with_headers)
        ops.emplace_back(zkutil::makeCreateRequest(partPath(name), header, zkutil::CreateMode::Persistent));

    const Int64 tombstone_ts = nowMilliseconds();
    for (const auto & name : old_part_names_to_remove)
    {
        auto payload = buildRemovalPayload(zk, partPath(name), tombstone_ts);
        if (!payload)
            return Coordination::Error::ZNONODE;
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(name), -1));
        ops.emplace_back(zkutil::makeCreateRequest(droppedPartPath(name), *payload, zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

std::expected<CloudMergeTreeCoordination::LeaseHandle, Coordination::Error> CloudMergeTreeCoordination::acquireOrStealLease(
    const zkutil::ZooKeeperPtr & zk, const String & lease_path, const String & holder_data, Int64 staleness_threshold_ms) const
{
    auto code = zk->tryCreate(lease_path, holder_data, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
    {
        Coordination::Stat stat;
        if (!zk->exists(lease_path, &stat))
            return std::unexpected(Coordination::Error::ZNONODE);
        return LeaseHandle{lease_path, stat.version};
    }

    if (code != Coordination::Error::ZNODEEXISTS)
        return std::unexpected(code);

    /// Someone already holds it. Only worth stealing if it's gone stale (no heartbeat for
    /// longer than the threshold) -- a live, actively-heartbeating holder just means we
    /// genuinely lost the race for this range this cycle.
    Coordination::Stat existing_stat;
    String existing_data;
    if (!zk->tryGet(lease_path, existing_data, &existing_stat))
        return std::unexpected(Coordination::Error::ZNODEEXISTS);

    if (nowMilliseconds() - existing_stat.mtime < staleness_threshold_ms)
        return std::unexpected(Coordination::Error::ZNODEEXISTS);

    /// Stale: atomically replace it, fenced by the version we just read. If the holder
    /// heartbeated in the meantime this fails with ZBADVERSION -- treat identically to "lost
    /// the race", no retry loop here.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(lease_path, existing_stat.version));
    ops.emplace_back(zkutil::makeCreateRequest(lease_path, holder_data, zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    auto steal_code = zk->tryMultiNoThrow(ops, responses);
    if (steal_code != Coordination::Error::ZOK)
        return std::unexpected(steal_code);

    Coordination::Stat new_stat;
    if (!zk->exists(lease_path, &new_stat))
        return std::unexpected(Coordination::Error::ZNONODE);
    return LeaseHandle{lease_path, new_stat.version};
}

std::expected<int32_t, Coordination::Error> CloudMergeTreeCoordination::touchLease(
    const zkutil::ZooKeeperPtr & zk, const String & lease_path, int32_t current_version) const
{
    Coordination::Stat stat;
    auto code = zk->trySet(lease_path, "", current_version, &stat);
    if (code != Coordination::Error::ZOK)
        return std::unexpected(code);
    return stat.version;
}

void CloudMergeTreeCoordination::releaseLease(const zkutil::ZooKeeperPtr & zk, const String & lease_path, int32_t lease_version) const
{
    zk->tryRemove(lease_path, lease_version);
}

String CloudMergeTreeCoordination::registerReplicaForAzElection(
    const zkutil::ZooKeeperPtr & zk, const String & az, const String & display_name) const
{
    zk->createIfNotExists(replicasPath(), "");
    zk->createIfNotExists(replicasAzPath(az), "");
    return zk->create(replicasAzPath(az) + "/replica-", display_name, zkutil::CreateMode::EphemeralSequential);
}

bool CloudMergeTreeCoordination::isLowestSequenceInAz(
    const zkutil::ZooKeeperPtr & zk, const String & az, const String & own_node_path) const
{
    Strings children = zk->getChildren(replicasAzPath(az));
    if (children.empty())
        return false;

    /// Every node under this AZ shares the identical "replica-" prefix and Keeper's fixed-width
    /// zero-padded sequence suffix (see registerReplicaForAzElection()'s own comment), so plain
    /// lexicographic sort here is equivalent to sorting by sequence number.
    ::sort(children.begin(), children.end());

    const String own_name = own_node_path.substr(own_node_path.find_last_of('/') + 1);
    return children.front() == own_name;
}

Coordination::Error CloudMergeTreeCoordination::tryRemoveParts(
    const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const
{
    Coordination::Requests ops;
    const Int64 tombstone_ts = nowMilliseconds();
    for (const auto & part_name : part_names)
    {
        auto payload = buildRemovalPayload(zk, partPath(part_name), tombstone_ts);
        if (!payload)
            return Coordination::Error::ZNONODE;
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(part_name), -1));
        ops.emplace_back(zkutil::makeCreateRequest(droppedPartPath(part_name), *payload, zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

void CloudMergeTreeCoordination::clearDeduplicationHashes(const zkutil::ZooKeeperPtr & zk, const String & partition_id) const
{
    /// DeduplicationHash::getBlockId() names each znode "<partition_id>_<hash0>_<hash1>" (a flat
    /// namespace, not a per-partition subtree) -- filter by that exact prefix (with the separating
    /// "_") rather than a raw substring match, so partition_id "1" can never falsely match "10"'s
    /// entries.
    const String prefix = partition_id.empty() ? String{} : partition_id + "_";

    Strings names = zk->getChildren(deduplicationHashesPath());
    Coordination::Requests ops;
    for (const auto & name : names)
        if (prefix.empty() || name.starts_with(prefix))
            ops.emplace_back(zkutil::makeRemoveRequest(deduplicationHashesPath() + "/" + name, -1));

    if (ops.empty())
        return;

    Coordination::Responses responses;
    zk->tryMultiNoThrow(ops, responses);
}

Coordination::Error CloudMergeTreeCoordination::tryDetachParts(
    const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const
{
    Coordination::Requests ops;
    const Int64 detached_ts = nowMilliseconds();
    for (const auto & part_name : part_names)
    {
        /// Same "<ms>\n<location trailer>" shape as a tombstone -- see buildRemovalPayload() --
        /// so a later ATTACH can resolve the detached part's directories from Keeper instead of
        /// depending on the attaching replica's own object storage listing having caught up.
        auto payload = buildRemovalPayload(zk, partPath(part_name), detached_ts);
        if (!payload)
            return Coordination::Error::ZNONODE;
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(part_name), -1));
        ops.emplace_back(zkutil::makeCreateRequest(detachedPartPath(part_name), *payload, zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

Coordination::Error CloudMergeTreeCoordination::tryReattachPart(
    const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header) const
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(detachedPartPath(part_name), -1));
    ops.emplace_back(zkutil::makeCreateRequest(partPath(part_name), part_header, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

Strings CloudMergeTreeCoordination::listDetachedPartNames(const zkutil::ZooKeeperPtr & zk) const
{
    return zk->getChildren(detachedPartsPath());
}

Coordination::Error CloudMergeTreeCoordination::markTableDropped(const zkutil::ZooKeeperPtr & zk) const
{
    auto code = zk->tryCreate(dropMarkerPath(), "", zkutil::CreateMode::Persistent);
    return code == Coordination::Error::ZNODEEXISTS ? Coordination::Error::ZOK : code;
}

std::vector<CloudMergeTreeCoordination::Tombstone> CloudMergeTreeCoordination::listTombstones(const zkutil::ZooKeeperPtr & zk) const
{
    std::vector<Tombstone> result;
    Strings names = zk->getChildren(droppedPartsPath());
    result.reserve(names.size());
    for (const auto & name : names)
    {
        String value;
        if (!zk->tryGet(droppedPartPath(name), value) || value.empty())
            continue;

        /// Payload is "<ms>\n<location trailer>" (see buildRemovalPayload()) -- parse only the
        /// timestamp line; the trailer is opaque here and handed to the caller verbatim.
        const auto newline = value.find('\n');
        Tombstone tombstone;
        tombstone.part_name = name;
        tombstone.dropped_at_ms = parse<Int64>(value.substr(0, newline));
        if (newline != String::npos)
            tombstone.location_text = value.substr(newline + 1);
        result.push_back(std::move(tombstone));
    }
    return result;
}

bool CloudMergeTreeCoordination::tryClaimTombstoneForDeletion(const zkutil::ZooKeeperPtr & zk, const String & part_name) const
{
    auto code = zk->tryCreate(droppedPartClaimPath(part_name), "", zkutil::CreateMode::Ephemeral);
    return code == Coordination::Error::ZOK;
}

void CloudMergeTreeCoordination::releaseTombstoneClaim(const zkutil::ZooKeeperPtr & zk, const String & part_name) const
{
    zk->tryRemove(droppedPartClaimPath(part_name), -1);
}

void CloudMergeTreeCoordination::releaseTombstone(const zkutil::ZooKeeperPtr & zk, const String & part_name) const
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(droppedPartClaimPath(part_name), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(droppedPartPath(part_name), -1));

    Coordination::Responses responses;
    if (zk->tryMultiNoThrow(ops, responses) == Coordination::Error::ZOK)
        return;

    /// The claim may already be gone (e.g. a previous attempt deleted the objects but crashed
    /// before reaching this call, and its ephemeral claim died with its session in the meantime)
    /// -- fall back to removing just the tombstone. Best-effort, silently ignored if already gone.
    zk->tryRemove(droppedPartPath(part_name), -1);
}

Strings CloudMergeTreeCoordination::loadActivePartNames(
    const zkutil::ZooKeeperPtr & zk, int32_t & out_parts_version) const
{
    Coordination::Stat stat;
    Strings names = zk->getChildren(partsPath(), &stat);
    out_parts_version = stat.cversion;
    return names;
}

Strings CloudMergeTreeCoordination::loadActivePartNames(
    const zkutil::ZooKeeperPtr & zk, int32_t & out_parts_version, Coordination::WatchCallbackPtr watch_callback) const
{
    Coordination::Stat stat;
    Strings names = zk->getChildrenWatch(partsPath(), &stat, std::move(watch_callback));
    out_parts_version = stat.cversion;
    return names;
}

int32_t CloudMergeTreeCoordination::getPartsVersion(const zkutil::ZooKeeperPtr & zk) const
{
    Coordination::Stat stat;
    zk->exists(partsPath(), &stat);
    return stat.cversion;
}

std::map<String, Int64> CloudMergeTreeCoordination::snapshotBlockNumbers(
    const zkutil::ZooKeeperPtr & zk, const std::set<String> & partition_ids) const
{
    auto zk_fault = std::make_shared<ZooKeeperWithFaultInjection>(zk);
    std::map<String, Int64> result;
    for (const auto & partition_id : partition_ids)
    {
        zk->createIfNotExists(blockNumbersPartitionPath(partition_id), "");
        auto lock = createEphemeralLockInZooKeeper(
            blockNumbersPartitionPath(partition_id) + "/block-", tempPath(), zk_fault, /*deduplication_paths=*/{}, /*znode_data=*/std::nullopt);
        result[partition_id] = static_cast<Int64>(lock.getNumber());
        lock.unlock();
    }
    return result;
}

String CloudMergeTreeCoordination::createMutation(const zkutil::ZooKeeperPtr & zk, const String & entry_text) const
{
    String created_path = zk->create(mutationsPath() + "/", entry_text, zkutil::CreateMode::PersistentSequential);
    return created_path.substr(mutationsPath().size() + 1);
}

std::vector<std::pair<String, String>> CloudMergeTreeCoordination::listMutations(const zkutil::ZooKeeperPtr & zk) const
{
    std::vector<std::pair<String, String>> result;
    Strings names = zk->getChildren(mutationsPath());
    result.reserve(names.size());
    for (const auto & name : names)
    {
        String text;
        if (zk->tryGet(mutationPath(name), text))
            result.emplace_back(name, std::move(text));
    }
    return result;
}

bool CloudMergeTreeCoordination::tryGetPartHeader(
    const zkutil::ZooKeeperPtr & zk, const String & part_name, String & out_header) const
{
    return zk->tryGet(partPath(part_name), out_header);
}

void CloudMergeTreeCoordination::ensureInitialMetadata(const zkutil::ZooKeeperPtr & zk, const String & initial_columns_text) const
{
    zk->createIfNotExists(metadataPath(), initial_columns_text);
}

int32_t CloudMergeTreeCoordination::getMetadataVersion(const zkutil::ZooKeeperPtr & zk) const
{
    Coordination::Stat stat;
    zk->exists(metadataPath(), &stat);
    return stat.version;
}

std::pair<String, int32_t> CloudMergeTreeCoordination::getMetadata(const zkutil::ZooKeeperPtr & zk) const
{
    Coordination::Stat stat;
    String text = zk->get(metadataPath(), &stat);
    return {text, stat.version};
}

std::pair<String, int32_t> CloudMergeTreeCoordination::getMetadata(
    const zkutil::ZooKeeperPtr & zk, Coordination::WatchCallbackPtr watch_callback) const
{
    Coordination::Stat stat;
    String text = zk->getWatch(metadataPath(), &stat, watch_callback);
    return {text, stat.version};
}

Coordination::Error CloudMergeTreeCoordination::trySetMetadata(
    const zkutil::ZooKeeperPtr & zk, const String & new_columns_text, int32_t expected_version, int32_t & out_new_version) const
{
    Coordination::Stat stat;
    auto code = zk->trySet(metadataPath(), new_columns_text, expected_version, &stat);
    if (code == Coordination::Error::ZOK)
        out_new_version = stat.version;
    return code;
}

std::expected<CloudMergeTreeCoordination::MetadataAndMutationResult, Coordination::Error>
CloudMergeTreeCoordination::trySetMetadataAndCreateMutation(
    const zkutil::ZooKeeperPtr & zk, const String & new_columns_text, int32_t expected_metadata_version,
    const String & mutation_entry_text) const
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeSetRequest(metadataPath(), new_columns_text, expected_metadata_version));
    ops.emplace_back(zkutil::makeCreateRequest(mutationsPath() + "/", mutation_entry_text, zkutil::CreateMode::PersistentSequential));

    Coordination::Responses responses;
    auto code = zk->tryMultiNoThrow(ops, responses);
    if (code != Coordination::Error::ZOK)
        return std::unexpected(code);

    const auto & create_response = dynamic_cast<const Coordination::CreateResponse &>(*responses[1]);
    String mutation_id = create_response.path_created.substr(mutationsPath().size() + 1);
    return MetadataAndMutationResult{expected_metadata_version + 1, mutation_id};
}

}
