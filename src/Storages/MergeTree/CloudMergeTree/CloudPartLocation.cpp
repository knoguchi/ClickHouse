#include <Storages/MergeTree/CloudMergeTree/CloudPartLocation.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/PatchParts/PatchPartIndex.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// One directory's token + files, relative to the given part (or projection part).
CloudPartLocation::Directory captureDirectory(const IMergeTreeDataPart & part, const String & subpath)
{
    CloudPartLocation::Directory dir;
    dir.subpath = subpath;

    const auto & storage = part.getDataPartStorage();

    /// checksums.txt exists in every finalized part, and in plain_rewritable every file of a
    /// directory maps to exactly one object <disk prefix>/<token>/<file> -- so the token is
    /// the parent path component of any file's remote key.
    const auto remote_paths = storage.getRemotePaths("checksums.txt");
    if (remote_paths.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot capture remote location of part {}: no remote objects for checksums.txt", part.name);
    dir.remote_token = std::filesystem::path(remote_paths.front()).parent_path().filename().string();
    if (dir.remote_token.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot capture remote location of part {}: empty directory token in remote path {}", part.name, remote_paths.front());

    /// The canonical complete file set: checksummed files (sizes come with the checksums) plus
    /// the fixed set of non-checksummed metadata files (sizes from the storage) -- same union
    /// DataPartsExchange::sendPart and backups rely on. Never a directory listing.
    for (const auto & [file_name, checksum] : part.checksums.files)
        dir.files_with_sizes.emplace(file_name, checksum.file_size);
    for (const auto & file_name : part.getFileNamesWithoutChecksums())
        if (storage.existsFile(file_name))
            dir.files_with_sizes.emplace(file_name, storage.getFileSize(file_name));

    return dir;
}

}

CloudPartLocation CloudPartLocation::capture(const IMergeTreeDataPart & part)
{
    CloudPartLocation location;
    location.directories.push_back(captureDirectory(part, ""));
    for (const auto & [projection_name, projection_part] : part.getProjectionParts())
        location.directories.push_back(captureDirectory(*projection_part, projection_name + ".proj"));

    /// Read back from the part's own in-memory PatchPartIndex (set directly by
    /// MergeTreeDataWriter::writeTempPatchPart()/MergeTask's patch-merge result, never lazily
    /// loaded from source_parts.dat here) -- see this field's own doc comment in the header.
    if (part.info.isPatch())
        location.patch_max_data_version = static_cast<Int64>(part.getPatchPartIndex().getMaxDataVersion());

    return location;
}

void CloudPartLocation::write(WriteBuffer & out) const
{
    writeString(FORMAT_MARKER, out);
    writeText(directories.size(), out);
    writeChar('\n', out);
    for (const auto & dir : directories)
    {
        writeEscapedString(dir.subpath, out);
        writeChar('\n', out);
        writeEscapedString(dir.remote_token, out);
        writeChar('\n', out);
        writeText(dir.files_with_sizes.size(), out);
        writeChar('\n', out);
        for (const auto & [file_name, file_size] : dir.files_with_sizes)
        {
            writeEscapedString(file_name, out);
            writeChar('\t', out);
            writeText(file_size, out);
            writeChar('\n', out);
        }
    }
    writeText(patch_max_data_version, out);
    writeChar('\n', out);
}

CloudPartLocation CloudPartLocation::read(ReadBuffer & in)
{
    if (in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Part znode payload has no location trailer. It was written by a CloudMergeTree build "
            "that predates Keeper-authoritative part locations; such tables must be recreated.");

    assertString(FORMAT_MARKER, in);

    CloudPartLocation location;
    size_t dir_count = 0;
    readText(dir_count, in);
    assertChar('\n', in);
    location.directories.resize(dir_count);
    for (auto & dir : location.directories)
    {
        readEscapedString(dir.subpath, in);
        assertChar('\n', in);
        readEscapedString(dir.remote_token, in);
        assertChar('\n', in);
        size_t file_count = 0;
        readText(file_count, in);
        assertChar('\n', in);
        for (size_t i = 0; i < file_count; ++i)
        {
            String file_name;
            UInt64 file_size = 0;
            readEscapedString(file_name, in);
            assertChar('\t', in);
            readText(file_size, in);
            assertChar('\n', in);
            dir.files_with_sizes.emplace(std::move(file_name), file_size);
        }
    }
    readText(location.patch_max_data_version, in);
    assertChar('\n', in);
    return location;
}

String CloudPartLocation::extractTrailerText(const String & znode_payload)
{
    const auto pos = znode_payload.find(FORMAT_MARKER);
    if (pos == String::npos)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Part znode payload has no location trailer. It was written by a CloudMergeTree build "
            "that predates Keeper-authoritative part locations; such tables must be recreated.");
    return znode_payload.substr(pos);
}

}
