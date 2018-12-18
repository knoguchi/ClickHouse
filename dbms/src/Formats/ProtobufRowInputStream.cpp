#include <Common/config.h>

#if USE_PROTOBUF

#include <Common/escapeForFileName.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Formats/ProtobufRowInputStream.h> // Y_IGNORE
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <IO/copyData.h>
#include <boost/algorithm/string.hpp>
#include <boost/range/join.hpp>
#include <boost/filesystem.hpp>

#include <common/logger_useful.h>

#include <unistd.h>
#include <iostream>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

using namespace std;
using namespace google::protobuf::compiler;
using namespace google::protobuf;
using namespace boost::filesystem;

class ErrorPrinter : public MultiFileErrorCollector,
                     public io::ErrorCollector,
                     public DescriptorPool::ErrorCollector
{
public:
    ErrorPrinter(ErrorFormat format, DiskSourceTree *tree = nullptr) : found_errors_(false)
    {
        printf("%u %p", format, tree);
    }

    ~ErrorPrinter()
    {}

    // implements MultiFileErrorCollector
    void AddError(const string & filename, int line, int column, const string & message)
    {
        found_errors_ = true;
        AddErrorOrWarning(filename, line, column, message, "error", cerr);
    }

    void AddWarning(const string & filename, int line, int column, const string & message)
    {
        found_errors_ = true;
        AddErrorOrWarning(filename, line, column, message, "warning", clog);
    }

    // implements io::ErrorCollector
    void AddError(int line, int column, const string & message)
    {
        AddError("input", line, column, message);
    }

    void AddWarning(int line, int column, const string & message)
    {
        AddErrorOrWarning("input", line, column, message, "warning", clog);
    }

    // implements DescriptorPool::ErrorCollector
    void AddError(const string & filename, const string & element_name, const Message *descriptor, ErrorLocation location, const string & message)
    {
        printf("%s", element_name.c_str());
        printf("%s", descriptor->DebugString().c_str());
        cout << location;

        AddErrorOrWarning(filename, -1, -1, message, "error", cerr);
    }

    void AddWarning(const string & filename, const string & element_name, const Message *descriptor, ErrorLocation location, const string & message)
    {
        printf("%s", element_name.c_str());
        printf("%s", descriptor->DebugString().c_str());
        cout << location;
        AddErrorOrWarning(filename, -1, -1, message, "warning", cerr);
    }

    bool FoundErrors() const
    { return found_errors_; }

private:
    void AddErrorOrWarning(const string & filename, int line, int column, const string & message, const string & type, ostream & out)
    {
        printf("%s:%d:%d %s %s", filename.c_str(), line, column, message.c_str(), type.c_str());
        out << "done";
    }

    bool found_errors_;
};

bool ProtobufRowInputStream::ParseInputFiles(DescriptorPool *descriptor_pool, vector<const FileDescriptor *> *parsed_files)
{
    for (size_t i = 0; i < proto_files_.size(); i++)
    {
        // import the file
        descriptor_pool->AddUnusedImportTrackFile(proto_files_[i]);
        const FileDescriptor *parsed_file = descriptor_pool->FindFileByName(proto_files_[i]);
        descriptor_pool->ClearUnusedImportTrackFiles();
        if (parsed_file == nullptr)
        {
            if (!descriptor_set_in_names_.empty())
            {
                cerr << proto_files_[i] << ": " << strerror(ENOENT) << endl;
            }
            return false;
        }
        parsed_files->push_back(parsed_file);
    }
    return true;
}

const unordered_map<FieldDescriptor::CppType, const string> ProtobufRowInputStream::protobuf_type_to_column_type_name = {
    // TODO: what if numeric values are always smaller than int32/64?
    {FieldDescriptor::CPPTYPE_INT32,  "Int32"},
    {FieldDescriptor::CPPTYPE_INT64,  "Int64"},
    {FieldDescriptor::CPPTYPE_UINT32, "UInt32"},
    {FieldDescriptor::CPPTYPE_UINT64, "UInt64"},
    {FieldDescriptor::CPPTYPE_DOUBLE, "Double"},
    {FieldDescriptor::CPPTYPE_FLOAT,  "Float"},
    {FieldDescriptor::CPPTYPE_BOOL,   "UInt8"},
    {FieldDescriptor::CPPTYPE_STRING, "String"}
    // TODO:
    /* {Enum, FieldDescriptor::CPPTYPE_ENUM}, */
    /* FieldDescriptor::CPPTYPE_MESSAGE, */
    /* FieldDescriptor::MAX_CPPTYPE */
};

void ProtobufRowInputStream::validateSchema()
{
    // TODO: support NULLs
    for (size_t column_i = 0; column_i != header.columns(); ++column_i)
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_i);

        if (name_to_field.find(header_column.name) == name_to_field.end())
            throw Exception(
                "Column \"" + header_column.name + "\" is not presented in a schema" /*, ErrorCodes::TODO*/);

        const FieldDescriptor *field = name_to_field[header_column.name];
        FieldDescriptor::CppType protobuf_type = field->cpp_type();

        if (protobuf_type_to_column_type_name.find(protobuf_type) == protobuf_type_to_column_type_name.end())
        {
            throw Exception("Unsupported type " + string(field->type_name()) + " of a column " +
                            header_column.name/*, ErrorCodes::TODO*/);
        }

        const string internal_type_name = protobuf_type_to_column_type_name.at(protobuf_type);
        const string column_type_name = header_column.type->getName();

        // TODO: can it be done with typeid_cast?
        if (internal_type_name != column_type_name)
        {
            throw Exception(
                "Input data type " + internal_type_name + " for column \"" + header_column.name + "\" "
                                                                                                  "is not compatible with a column type " +
                column_type_name/*, ErrorCodes::TODO*/
            );
        }
    }
}

ProtobufRowInputStream::ProtobufRowInputStream(ReadBuffer & istr_, const Block & header_, const String & schema_dir, const String & root_type)
    : istr(istr_), header(header_)
{

    log = &Logger::get("ProtobufRowInputStream");
    LOG_DEBUG(log, "Initialized ProtobufRowInputStream");
    // Parse the schema and fetch the root object
    vector<const FileDescriptor *> parsed_files;
    unique_ptr<DiskSourceTree> disk_source_tree;
    unique_ptr<ErrorPrinter> error_collector;
    // unique_ptr<DescriptorPool> descriptor_pool;
    //unique_ptr<DescriptorDatabase> descriptor_database;
    // unique_ptr<SourceTreeDescriptorDatabase> source_tree_database;

    // add search path
    proto_path_.clear();

    if (!exists(schema_dir) || !is_directory(schema_dir))
    {
        throw Exception("format_schema directory does not exist: " + schema_dir);
    }

    disk_source_tree.reset(new DiskSourceTree());
    set<string> _dup_check;
    recursive_directory_iterator it(schema_dir);
    recursive_directory_iterator endit;
    
    while (it != endit)
    {
        if (is_regular_file(*it) && it->path().extension().c_str() == string(".proto"))
        {
            string dir = it->path().parent_path().c_str();
            if (_dup_check.find(dir) == _dup_check.end())
            {
                LOG_DEBUG(log, "Adding dir " + dir);
                disk_source_tree->MapPath("", dir);
                _dup_check.insert(dir);
            }
            proto_files_.push_back(it->path().filename().c_str());
        }
        ++it;
    }
    error_collector.reset(new ErrorPrinter(error_format_, disk_source_tree.get()));

    // at this point, it knows the dirs to look for proto files
    database = new SourceTreeDescriptorDatabase(disk_source_tree.get());
    database->RecordErrorsTo(error_collector.get());
    descriptor_pool = new DescriptorPool(database, database->GetValidationErrorCollector());
    descriptor_pool->EnforceWeakDependencies(true);
    
    if (!ParseInputFiles(descriptor_pool, &parsed_files))
    {
        throw ("Parse input files failed");
    }

    // check the type exists
    const Descriptor *type = descriptor_pool->FindMessageTypeByName(root_type);
    if (type == nullptr)
    {
        throw ("Type not defined: " + root_type);
    }

    // create dynamic message factory
    LOG_DEBUG(log, "creating dynamic message factory");
    prototype_msg = dynamic_factory.GetPrototype(type);
    LOG_DEBUG(log, "finished dynamic message factory");

    if (nullptr == prototype_msg)
        throw Exception("Failed to create a prototype message from a message descriptor"/*, ErrorCodes::TODO*/);

    // finished setting up the dynamic message parser

    vector<pair<string, const FieldDescriptor *>> fields = traverse_message(type);
    for (size_t i = 0; i < fields.size(); i++)
    {
        LOG_DEBUG(log, fields[i].first.c_str());
    }

    for (size_t field_i = 0; field_i != static_cast<size_t>(type->field_count()); ++field_i)
    {
        const FieldDescriptor *field = type->field(field_i);
        name_to_field[field->name()] = field;
    }

    validateSchema();
    LOG_DEBUG(log, "finished setup ProtobufRowInputStream");
}

vector<pair<string, const FieldDescriptor *>> ProtobufRowInputStream::traverse_message(const Descriptor *type)
{
    vector<pair<string, const FieldDescriptor *>> fields;
    for (int i = 0; i < type->field_count(); i++)
    {
        const FieldDescriptor *f = type->field(i);
        if (f->type() == FieldDescriptor::Type::TYPE_MESSAGE)
        {
            vector<pair<string, const FieldDescriptor *>> nested_fields = traverse_message(f->message_type());
            for (int j = 0; j < int(nested_fields.size()); j++)
            {
                fields.push_back(make_pair(f->name() + "-" + nested_fields[j].first, nested_fields[j].second));
            }
        }
        else if (f->type() == FieldDescriptor::Type::TYPE_ENUM)
        {
            fields.push_back(make_pair(f->name(), f));
        }
        else
        {
            fields.push_back(make_pair(f->name(), f));
        }
    }
    return fields;
}

void ProtobufRowInputStream::insertOneMessage(MutableColumns & columns, Message *mutable_msg)
{
    const Reflection *reflection = mutable_msg->GetReflection();
    vector<const FieldDescriptor *> fields;
    reflection->ListFields(*mutable_msg, &fields);

    // TODO: what if a message has different fields order
    // TODO: what if there are more fields?
    // TODO: what if some field is not presented?
    // TODO: "Field" types intersect (naming)
    for (size_t field_i = 0; field_i != fields.size(); ++field_i)
    {
        const FieldDescriptor *field = fields[field_i];
        if (nullptr == field)
            throw Exception(
                "FieldDescriptor for a column " + columns[field_i]->getName() + " is NULL"/*, ErrorCodes::TODO*/);

        // TODO: check field name?
        switch (field->cpp_type())
        {
#define DISPATCH(PROTOBUF_CPP_TYPE, CPP_TYPE, GETTER)            \
        case PROTOBUF_CPP_TYPE:                    \
          {                                \
        const CPP_TYPE value = reflection->GETTER(*mutable_msg, field); \
        const Field field_value = value;            \
        columns[field_i]->insert(field_value);            \
        break;                            \
          }

            FOR_PROTOBUF_CPP_TYPES(DISPATCH);
#undef DISPATCH
            default:
                // TODO
                throw Exception("Unsupported type");
        }
    }
}

bool ProtobufRowInputStream::read(MutableColumns & columns)
{
    LOG_DEBUG(log, "reading message");
  
    if (istr.eof())
        return false;

    // copy bytes from stream to a string. potentially we can read it straight from the stream
    String file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(istr, file_buffer);
    }

    // "message" has been setup by the constructor.
    Message *mutable_msg = prototype_msg->New();
    if (nullptr == mutable_msg)
        throw Exception("Failed to create a mutable message"/*, ErrorCodes::TODO*/);
    
    LOG_DEBUG(log, "payload size " << file_data.size());
    
    // Deserialize the bytes
    if (!mutable_msg->ParseFromArray(file_data.c_str(), file_data.size()))
        throw Exception("Failed to parse an input protobuf message"/*, ErrorCodes::TODO*/);

    LOG_DEBUG(log, "deserialized");

    // Now, flatten the nested structure, and insert.
    insertOneMessage(columns, mutable_msg);
    LOG_DEBUG(log, "inserted");

    // potentially it has to move the position
    return true;
}

void registerInputFormatProtobuf(FormatFactory & factory)
{
    factory.registerInputFormat("Protobuf", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        size_t max_block_size,
        const FormatSettings & settings)
    {
        vector<String> tokens;
        // format_schema directory from the config
        auto root_type = context.getSettingsRef().format_schema.toString();

        // scClickHouse config
        const String & schema_dir = context.getFormatSchemaPath();

        return make_shared<BlockInputStreamFromRowInputStream>(
            make_shared<ProtobufRowInputStream>(buf, sample, schema_dir, root_type),
            sample, max_block_size, settings);
    });
}
}

#else

namespace DB
{
  class FormatFactory;
  void registerInputFormatProtobuf(FormatFactory &) {}
}

#endif // USE_PROTOBUF
