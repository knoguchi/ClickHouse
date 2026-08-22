#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/CurrentMetrics.h>
#include <Common/MultiVersion.h>

#include <base/defines.h>

namespace DB
{

class FsMetadata
{
public:
    FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);

    void applySnapshot(std::shared_ptr<FsSnapshot> snapshot);
    void applyLayout(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout);

    /// Same, but with authoritative directory overrides applied on top of the listing-derived
    /// layout (see IMetadataStorage::setAuthoritativeDirectory). Per override: a directory the
    /// listing missed is added as given; a listing entry with a DIFFERENT remote token is
    /// replaced (the authority knows which physical directory backs the logical path); a
    /// listing entry with the SAME token keeps the listing's file set -- it may legitimately
    /// contain files written after the authority's snapshot was taken -- with any
    /// authority-only files unioned in (files are immutable, so both sources agree on sizes).
    /// Membership and comparison go through the tree's own canonical path walk, never raw
    /// path-string comparison (the layout's keys come from prefix.path contents; override keys
    /// from callers; their textual forms differ for the same directory).
    void applyLayout(
        std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout,
        const std::vector<std::pair<std::string, DirectoryRemoteInfo>> & authoritative_overrides);

    std::shared_ptr<FsSnapshot> takeReadWriteSnapshot() const;
    std::shared_ptr<const FsSnapshot> takeReadOnlySnapshot() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<FsSnapshot> latest_snapshot TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_directories_count TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_files_count TSA_GUARDED_BY(mutex);
};

}
