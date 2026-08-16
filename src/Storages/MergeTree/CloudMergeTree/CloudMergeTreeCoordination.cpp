#include <Storages/MergeTree/CloudMergeTree/CloudMergeTreeCoordination.h>

#include <Common/ZooKeeper/KeeperException.h>

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
    zk->createIfNotExists(root_path + "/block_numbers", "");
    zk->createIfNotExists(root_path + "/mutations", "");
    zk->createIfNotExists(root_path + "/leases", "");
    zk->createIfNotExists(root_path + "/replicas", "");
    zk->createIfNotExists(root_path + "/temp", "");
}

Coordination::Error CloudMergeTreeCoordination::tryCommitInsert(
    const zkutil::ZooKeeperPtr & zk, const String & part_name, const String & part_header) const
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(partPath(part_name), part_header, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
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

    /// Deactivate the sources in the same atomic step.
    for (const auto & source : source_part_names)
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(source), -1));

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
}

Coordination::Error CloudMergeTreeCoordination::tryRemoveParts(
    const zkutil::ZooKeeperPtr & zk, const Strings & part_names) const
{
    Coordination::Requests ops;
    for (const auto & part_name : part_names)
        ops.emplace_back(zkutil::makeRemoveRequest(partPath(part_name), -1));

    Coordination::Responses responses;
    return zk->tryMultiNoThrow(ops, responses);
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

bool CloudMergeTreeCoordination::tryGetPartHeader(
    const zkutil::ZooKeeperPtr & zk, const String & part_name, String & out_header) const
{
    return zk->tryGet(partPath(part_name), out_header);
}

}
