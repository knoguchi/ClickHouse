#pragma once

#include <Common/config.h>

#if USE_PROTOBUF
#include <common/logger_useful.h>
#include <Core/Block.h>
#include <Formats/IRowInputStream.h>

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

// TODO: some types require a bigger one to create a Field. Is it really how it should work?
#define FOR_PROTOBUF_CPP_TYPES(M)                \
  M(FieldDescriptor::CPPTYPE_INT32,  Int64,   GetInt32)        \
  M(FieldDescriptor::CPPTYPE_INT64,  Int64,   GetInt64)        \
  M(FieldDescriptor::CPPTYPE_UINT32, UInt64,  GetUInt32)    \
  M(FieldDescriptor::CPPTYPE_UINT64, UInt64,  GetUInt64)    \
  M(FieldDescriptor::CPPTYPE_FLOAT,  Float64, GetFloat)        \
  M(FieldDescriptor::CPPTYPE_DOUBLE, Float64, GetDouble)    \
  M(FieldDescriptor::CPPTYPE_STRING, String,  GetString)    \
  // TODO:
/* M(FieldDescriptor::CPPTYPE_BOOL,   UInt8)  \ */

/* {Enum, FieldDescriptor::CPPTYPE_ENUM}, */
/* FieldDescriptor::CPPTYPE_MESSAGE, */
/* FieldDescriptor::MAX_CPPTYPE */


namespace DB
{

enum ErrorFormat
{
    ERROR_FORMAT_BLAH
};
ErrorFormat error_format_;

class ReadBuffer;

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

/** A stream for reading messages in Protobuf format in given schema.
 * Protobuf messages are strongly-typed and not self-describing.
 * The schema in this case cannot be compiled in, so it uses a runtime schema parser.
 */
class ProtobufRowInputStream : public IRowInputStream
{
public:
    ProtobufRowInputStream(ReadBuffer & istr_, const Block & header_, const String & schema_dir, const String & root_obj);

    bool read(MutableColumns & columns) override;

    // KENJI: implement them?
    /*
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
    */
    bool ParseInputFiles(DescriptorPool *descriptor_pool, vector<const FileDescriptor *> *parsed_files);

private:
    Logger *log;

    ReadBuffer & istr;
    Block header;
    vector<string> proto_files_;
    vector<string> descriptor_set_in_names_;
    vector<pair<string, string>> proto_path_;

    DescriptorPool *descriptor_pool;
    DescriptorDatabase *descriptor_database;
    SourceTreeDescriptorDatabase *database;
    DynamicMessageFactory dynamic_factory;
    const Message *prototype_msg;

    unordered_map<string, const FieldDescriptor *> name_to_field;
    static const unordered_map<FieldDescriptor::CppType, const string> protobuf_type_to_column_type_name;

    vector<pair<string, const FieldDescriptor *>> traverse_message(const Descriptor *type);

    void validateSchema();

    void insertOneMessage(MutableColumns & columns, Message *mutable_msg);

};

}

#endif // USE_PROTOBUF
