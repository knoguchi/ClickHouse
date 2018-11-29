#include <Common/config.h>
#if USE_PROTOBUF

#include <Common/escapeForFileName.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Formats/ProtobufRowInputStream.h> // Y_IGNORE
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <IO/copyData.h> // could be IO/copyData.h
#include <capnp/serialize.h> // Y_IGNORE
#include <capnp/dynamic.h> // Y_IGNORE
#include <boost/algorithm/string.hpp>
#include <boost/range/join.hpp>
#include <common/logger_useful.h>

#include <unistd.h>
#include <iostream>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

namespace DB
{

  using namespace google::protobuf;
  using namespace google::protobuf::compiler;

  class ErrorPrinter: public MultiFileErrorCollector,
		      public io::ErrorCollector,
		      public DescriptorPool::ErrorCollector {
  public:
    ErrorPrinter(ErrorFormat format, DiskSourceTree *tree = NULL):
      format_(format), tree_(tree), found_errors_(false) {}
    ~ErrorPrinter() {}

    // implements MultiFileErrorCollector
    void AddError(const string& filename, int line, int column, const string& message) {
      found_errors_ = true;
      AddErrorOrWarning(filename, line, column, message, "error", std::cerr);
    }

    void AddWarning(const string& filename, int line, int column, const string& message) {
      found_errors_ = true;
      AddErrorOrWarning(filename, line, column, message, "warning", std::clog);
    }

  
		    
    // implements io::ErrorCollector
    void AddError(int line, int column, const string& message) {
      AddError("input", line, column, message);
    }

    void AddWarning(int line, int column, const string& message) {
      AddErrorOrWarning("input", line, column, message, "warning", std::clog);
    }

    // implements DescriptorPool::ErrorCollector
    void AddError(
		  const string& filename,
		  const string& element_name,
		  const Message* descriptor,
		  ErrorLocation location,
		  const string& message) {
      cout << descriptor;
      cout << location;
      AddErrorOrWarning(filename + ":" + element_name, -1, -1, message, "error", std::cerr);
    }

    void AddWarning(
		    const string& filename,
		    const string& element_name,
		    const Message* descriptor,
		    ErrorLocation location,
		    const string& message) {
      cout << descriptor;
      cout << location;
      AddErrorOrWarning(filename + ":" + element_name, -1, -1, message, "warning", std::cerr);
    }

    bool FoundErrors() const { return found_errors_;}

  private:
    void AddErrorOrWarning(const string& filename, int line, int column, const string& message, const string& type, std::ostream& out) {
      out << filename
	  << ":"
	  << line
	  << ":"
	  << column
	  << ":"
	  << message
	  << ":"
	  << type
	  << std::endl;
    }

  
		    
    const ErrorFormat format_;
    DiskSourceTree *tree_;
    bool found_errors_;
  };

  
  bool InitializeDiskSourceTree(DiskSourceTree* source_tree) {
    // Add default proto paths
  
    // Set up the source tree.
    for (size_t i=0; i < proto_path_.size(); i++) {
      source_tree->MapPath(proto_path_[i].first, proto_path_[i].second);
    }

    /*
    // Map input files to virtual paths if possible.
    if(!MakeInputsBeProtoPathRelative(source_tree)) {
    return false;
    }
    */
    return true;
  }

  bool MakeProtoProtoPathRelative(
				  DiskSourceTree* source_tree, string* proto,
				  DescriptorDatabase* fallback_database) {
    FileDescriptorProto fallback_file;
    bool in_fallback_database = fallback_database != nullptr && fallback_database->FindFileByName(*proto, &fallback_file);

    if(access(proto->c_str(), F_OK) < 0) {
      string disk_file;
      if(source_tree->VirtualFileToDiskFile(*proto, &disk_file) ||
	 in_fallback_database) {
	return true;
      }
      else {
	std::cerr <<*proto <<": "<< strerror(ENOENT) << std::endl;
	return false;
      }
    }

    string virtual_file, shadowing_disk_file;
    switch(source_tree->DiskFileToVirtualFile(
					      *proto, &virtual_file, &shadowing_disk_file)) {
    case DiskSourceTree::SUCCESS:
      *proto = virtual_file;
      break;
    case DiskSourceTree::SHADOWED:
      std:: cerr<< *proto
		<< ": Input is shadowed in the --proto_path by \""
		<< shadowing_disk_file
		<< "\". Fiether use the latter file as your input or reoder "
	" the --proto_path so that the former file's location comes firs,." << std::endl;
      return false;
    case DiskSourceTree::CANNOT_OPEN:
      if (in_fallback_database) {
	return true;
      }
      std::cerr << *proto << ": " << strerror(errno) << std::endl;
      return false;
    case DiskSourceTree::NO_MAPPING:
      {
	// try to intepret the path as a virtual path.
	string disk_file;
	if (source_tree->VirtualFileToDiskFile(*proto, &disk_file) ||
	    in_fallback_database) {
	  return true;
	} else {
	  // the input file path can't be mapped to any --proto_path and it also
	  // can't be interpreted as a virtual path.
	  return false;
	}
      }
    }
    return true;
  }

  bool MakeInputsBeProtoPathRelative(
				     DiskSourceTree* source_tree, DescriptorDatabase* fallback_database) {
    for (auto& input_file: input_files_) {
      if(!MakeProtoProtoPathRelative(source_tree, &input_file, fallback_database)) {
	return false;
      }
    }
    return true;
  }

  bool ParseInputFiles(
		       DescriptorPool* descriptor_pool,
		       std::vector<const FileDescriptor*>* parsed_files) {
    for (size_t i=0; i < input_files_.size(); i++) {
      // import the file
      descriptor_pool->AddUnusedImportTrackFile(input_files_[i]);
      const FileDescriptor* parsed_file = descriptor_pool->FindFileByName(input_files_[i]);
      descriptor_pool->ClearUnusedImportTrackFiles();
      if (parsed_file == NULL) {
	if (!descriptor_set_in_names_.empty()) {
	  std::cerr <<input_files_[i] <<": " <<strerror(ENOENT) << std::endl;
	}
	return false;
      }
      parsed_files->push_back(parsed_file);
    }
    return true;
  }
  
  const std::unordered_map<FieldDescriptor::CppType, const std::string> ProtobufRowInputStream::protobuf_type_to_column_type_name = {
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
	  throw Exception("Column \"" + header_column.name + "\" is not presented in a schema" /*, ErrorCodes::TODO*/);

        const FieldDescriptor * field = name_to_field[header_column.name];
        FieldDescriptor::CppType protobuf_type = field->cpp_type();

        if (protobuf_type_to_column_type_name.find(protobuf_type) == protobuf_type_to_column_type_name.end())
	  {
            throw Exception("Unsupported type " + std::string(field->type_name()) + " of a column " + header_column.name/*, ErrorCodes::TODO*/);
	  }

        const std::string internal_type_name = protobuf_type_to_column_type_name.at(protobuf_type);
        const std::string column_type_name = header_column.type->getName();

        // TODO: can it be done with typeid_cast?
        if (internal_type_name != column_type_name)
	  {
            throw Exception(
			    "Input data type " + internal_type_name + " for column \"" + header_column.name + "\" "
			    "is not compatible with a column type " + column_type_name/*, ErrorCodes::TODO*/
			    );
	  }
      }
  }

  // TODO: support Proto3

  ProtobufRowInputStream::ProtobufRowInputStream(
						 ReadBuffer & istr_, const Block & header_,
						 const String & schema_dir, const String & schema_file, const String & root_obj
						 ) : istr(istr_), header(header_)
  {
    cout << root_obj;
    std::vector<const FileDescriptor*> parsed_files;
    std::unique_ptr<DiskSourceTree> disk_source_tree;
    std::unique_ptr<ErrorPrinter> error_collector;
    std::unique_ptr<DescriptorPool> descriptor_pool;
    std::unique_ptr<DescriptorDatabase> descriptor_database;
    std::unique_ptr<SourceTreeDescriptorDatabase> source_tree_database;

    proto_path_.clear();
    // add search path
    proto_path_.push_back(std::pair<string, string>("", schema_dir));

    // add proto files
    input_files_.push_back(std::string(schema_file));
  
    if (descriptor_set_in_names_.empty()) {
      disk_source_tree.reset(new DiskSourceTree());
      if(!InitializeDiskSourceTree(disk_source_tree.get())) {
	throw Exception("DiskSourceTree init failed: " + schema_file);
      }
    
      // at this point, it knows the dirs to look for proto files

      error_collector.reset(new ErrorPrinter(error_format_, disk_source_tree.get()));

      SourceTreeDescriptorDatabase* database = new SourceTreeDescriptorDatabase(disk_source_tree.get());
      database->RecordErrorsTo(error_collector.get());
      descriptor_database.reset(database);
      descriptor_pool.reset(new DescriptorPool(descriptor_database.get(), database->GetValidationErrorCollector()));
    }

    descriptor_pool->EnforceWeakDependencies(true);
    if(!ParseInputFiles(descriptor_pool.get(), &parsed_files)) {
      throw Exception("Can't parse input file");
    }

    const Descriptor* type = descriptor_pool->FindMessageTypeByName(codec_type_);
    if (type == NULL) {
      throw Exception("Type not defined");
    }
    DynamicMessageFactory dynamic_factory(descriptor_pool.get());
    std::unique_ptr<Message> message(dynamic_factory.GetPrototype(type)->New());
			
    if (NULL == message)
      throw Exception("Failed to create a prototype message from a message descriptor"/*, ErrorCodes::TODO*/);

    for (size_t field_i = 0; field_i != static_cast<size_t>(type->field_count()); ++field_i)
      {
        const FieldDescriptor * field = type->field(field_i);
        name_to_field[field->name()] = field;
      }

    validateSchema();
  }

  // TODO: some types require a bigger one to create a Field. Is it really how it should work?
#define FOR_PROTOBUF_CPP_TYPES(M)				\
  M(FieldDescriptor::CPPTYPE_INT32,  Int64,   GetInt32)		\
  M(FieldDescriptor::CPPTYPE_INT64,  Int64,   GetInt64)		\
  M(FieldDescriptor::CPPTYPE_UINT32, UInt64,  GetUInt32)	\
  M(FieldDescriptor::CPPTYPE_UINT64, UInt64,  GetUInt64)	\
  M(FieldDescriptor::CPPTYPE_FLOAT,  Float64, GetFloat)		\
  M(FieldDescriptor::CPPTYPE_DOUBLE, Float64, GetDouble)	\
  M(FieldDescriptor::CPPTYPE_STRING, String,  GetString)	\
  // TODO:
  /* M(FieldDescriptor::CPPTYPE_BOOL,   UInt8)  \ */

  /* {Enum, FieldDescriptor::CPPTYPE_ENUM}, */
  /* FieldDescriptor::CPPTYPE_MESSAGE, */
  /* FieldDescriptor::MAX_CPPTYPE */

  void ProtobufRowInputStream::insertOneMessage(MutableColumns & columns, Message * mutable_msg)
  {
    const Reflection * reflection = mutable_msg->GetReflection();
    std::vector<const FieldDescriptor *> fields;
    reflection->ListFields(*mutable_msg, &fields);

    // TODO: what if a message has different fields order
    // TODO: what if there are more fields?
    // TODO: what if some field is not presented?


    // TODO: "Field" types intersect (naming)
    for (size_t field_i = 0; field_i != fields.size(); ++field_i)
      {
        const FieldDescriptor * field = fields[field_i];
        if (nullptr == field)
	  throw Exception("FieldDescriptor for a column " + columns[field_i]->getName() + " is NULL"/*, ErrorCodes::TODO*/);

        // TODO: check field name?
        switch(field->cpp_type())
	  {
#define DISPATCH(PROTOBUF_CPP_TYPE, CPP_TYPE, GETTER)			\
	    case PROTOBUF_CPP_TYPE:					\
	      {								\
		const CPP_TYPE value = reflection->GETTER(*mutable_msg, field); \
		const Field field_value = value;			\
		columns[field_i]->insert(field_value);			\
		break;							\
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
    if (istr.eof())
      return false;

    // TODO: handle an array of messages, not just one message
    String file_data;
    {
      WriteBufferFromString file_buffer(file_data);
      copyData(istr, file_buffer);
    }

    Message * mutable_msg = message->New();
    if (NULL == mutable_msg)
      throw Exception("Failed to create a mutable message"/*, ErrorCodes::TODO*/);

    if (!mutable_msg->ParseFromArray(file_data.c_str(), file_data.size()))
      throw Exception("Failed to parse an input protobuf message"/*, ErrorCodes::TODO*/);

    insertOneMessage(columns, mutable_msg);

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
				  std::vector<String> tokens;
				  auto schema_and_root = context.getSettingsRef().format_schema.toString();
				  boost::split(tokens, schema_and_root, boost::is_any_of(":"));
				  if (tokens.size() != 2)
				    throw Exception("Format Protobuf requires 'format_schema' setting to have a schema_file:root_object format, e.g. 'schema.protobuf:Message'");

				  const String & schema_dir = context.getFormatSchemaPath();

				  return std::make_shared<BlockInputStreamFromRowInputStream>(
											      std::make_shared<ProtobufRowInputStream>(buf, sample, schema_dir, tokens[0], tokens[1]),
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
