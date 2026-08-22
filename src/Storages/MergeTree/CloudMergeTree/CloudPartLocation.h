#pragma once

#include <base/types.h>

#include <map>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class ReadBuffer;
class WriteBuffer;

/// The physical location of a CloudMergeTree part on its shared plain_rewritable disk:
/// for the part's own directory and each projection subdirectory, the random remote
/// directory token plus the complete file list with sizes. Serialized as a trailer appended
/// after the part znode's `ReplicatedMergeTreePartHeader` text (whose reader tolerates
/// trailing bytes), and copied verbatim into tombstone payloads at part removal, so both
/// adoption and GC resolve name -> bytes from Keeper -- committed atomically with the part
/// itself -- instead of inferring it from eventually-consistent object storage listings.
///
/// `{token, file -> size}` is everything the plain_rewritable read path needs to reconstruct
/// a directory mapping (object key = <disk prefix>/<token>/<file>); see
/// IMetadataStorage::setAuthoritativeDirectory.
struct CloudPartLocation
{
    struct Directory
    {
        /// "" for the part's own directory; "<name>.proj" for a projection subdirectory
        /// (each has its own remote token in plain_rewritable).
        String subpath;
        String remote_token;
        std::map<String, UInt64> files_with_sizes;
    };

    std::vector<Directory> directories;

    /// Captures token + complete file set (checksums.files ∪ getFileNamesWithoutChecksums(),
    /// the same canonical union DataPartsExchange/backups use) from a fully-written part.
    /// The token is invariant under the later rename-into-place (plain_rewritable renames
    /// only rewrite prefix.path content), so capturing from the still-temporary directory
    /// before the Keeper commit is safe.
    static CloudPartLocation capture(const IMergeTreeDataPart & part);

    /// Trailer serialization. write() appends to a buffer already containing the header text;
    /// read() expects the buffer positioned right after ReplicatedMergeTreePartHeader::read
    /// and throws if the trailer is absent (payloads written by pre-location builds are not
    /// supported -- recreate the table).
    void write(WriteBuffer & out) const;
    static CloudPartLocation read(ReadBuffer & in);

    /// The trailer text alone, extracted from a full part-znode payload (header + trailer),
    /// for copying into tombstone payloads without reparsing. Throws if absent.
    static String extractTrailerText(const String & znode_payload);

    static constexpr auto FORMAT_MARKER = "cloud part location format version: 1\n";
};

}
