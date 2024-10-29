#include "ReaderUtils.hpp"

#include "../clp/aws/AwsAuthenticationSigner.hpp"
#include "../clp/FileReader.hpp"
#include "../clp/NetworkReader.hpp"
#include "../clp/ReaderInterface.hpp"
#include "archive_constants.hpp"
#include "Utils.hpp"

namespace clp_s {
std::shared_ptr<SchemaTree> ReaderUtils::read_schema_tree(ArchiveReaderAdaptor& adaptor) {
    ZstdDecompressor schema_tree_decompressor;
    std::shared_ptr<SchemaTree> tree = std::make_shared<SchemaTree>();

    auto& schema_tree_reader
            = adaptor.checkout_reader_for_section(constants::cArchiveSchemaTreeFile);
    schema_tree_decompressor.open(schema_tree_reader, cDecompressorFileReadBufferCapacity);

    size_t num_nodes;
    auto error_code = schema_tree_decompressor.try_read_numeric_value(num_nodes);
    if (ErrorCodeSuccess != error_code) {
        throw OperationFailed(error_code, __FILENAME__, __LINE__);
    }

    for (size_t i = 0; i < num_nodes; i++) {
        int32_t parent_id;
        size_t key_length;
        std::string key;
        uint8_t node_type;

        error_code = schema_tree_decompressor.try_read_numeric_value(parent_id);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        error_code = schema_tree_decompressor.try_read_numeric_value(key_length);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        error_code = schema_tree_decompressor.try_read_string(key_length, key);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        error_code = schema_tree_decompressor.try_read_numeric_value(node_type);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        tree->add_node(parent_id, (NodeType)node_type, key);
    }

    schema_tree_decompressor.close();
    adaptor.checkin_reader_for_section(constants::cArchiveSchemaTreeFile);

    return tree;
}

std::shared_ptr<VariableDictionaryReader> ReaderUtils::get_variable_dictionary_reader(
        ArchiveReaderAdaptor& adaptor
) {
    auto reader = std::make_shared<VariableDictionaryReader>(adaptor);
    reader->open(constants::cArchiveVarDictFile);
    return reader;
}

std::shared_ptr<LogTypeDictionaryReader> ReaderUtils::get_log_type_dictionary_reader(
        ArchiveReaderAdaptor& adaptor
) {
    auto reader = std::make_shared<LogTypeDictionaryReader>(adaptor);
    reader->open(constants::cArchiveLogDictFile);
    return reader;
}

std::shared_ptr<LogTypeDictionaryReader> ReaderUtils::get_array_dictionary_reader(
        ArchiveReaderAdaptor& adaptor
) {
    auto reader = std::make_shared<LogTypeDictionaryReader>(adaptor);
    reader->open(constants::cArchiveArrayDictFile);
    return reader;
}

std::shared_ptr<ReaderUtils::SchemaMap> ReaderUtils::read_schemas(ArchiveReaderAdaptor& adaptor) {
    auto schemas_pointer = std::make_unique<SchemaMap>();
    SchemaMap& schemas = *schemas_pointer;
    ZstdDecompressor schema_id_decompressor;

    auto& schema_id_reader = adaptor.checkout_reader_for_section(constants::cArchiveSchemaMapFile);
    schema_id_decompressor.open(schema_id_reader, cDecompressorFileReadBufferCapacity);

    size_t schema_size;
    auto error_code = schema_id_decompressor.try_read_numeric_value(schema_size);
    if (ErrorCodeSuccess != error_code) {
        throw OperationFailed(error_code, __FILENAME__, __LINE__);
    }

    // TODO: consider decompressing all schemas into the same buffer and providing access to them
    // via const spans.
    for (size_t i = 0; i < schema_size; i++) {
        int32_t schema_id;
        error_code = schema_id_decompressor.try_read_numeric_value(schema_id);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        uint32_t schema_node_size;
        error_code = schema_id_decompressor.try_read_numeric_value(schema_node_size);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        uint32_t num_ordered_nodes;
        error_code = schema_id_decompressor.try_read_numeric_value(num_ordered_nodes);
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }

        auto& schema = schemas[schema_id];
        if (0 == schema_node_size) {
            continue;
        }
        schema.resize(schema_node_size);
        error_code = schema_id_decompressor.try_read_exact_length(
                reinterpret_cast<char*>(schema.begin().base()),
                sizeof(int32_t) * schema_node_size
        );
        if (ErrorCodeSuccess != error_code) {
            throw OperationFailed(error_code, __FILENAME__, __LINE__);
        }
        schema.set_num_ordered(num_ordered_nodes);
    }

    schema_id_decompressor.close();
    adaptor.checkin_reader_for_section(constants::cArchiveSchemaMapFile);

    return schemas_pointer;
}

std::vector<std::string> ReaderUtils::get_archives(std::string const& archives_dir) {
    std::vector<std::string> archive_paths;

    if (false == boost::filesystem::is_directory(archives_dir)) {
        throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
    }

    boost::filesystem::directory_iterator iter(archives_dir);
    boost::filesystem::directory_iterator end;
    for (; iter != end; ++iter) {
        if (boost::filesystem::is_directory(iter->path())) {
            archive_paths.push_back(iter->path().string());
        }
    }

    return archive_paths;
}

bool ReaderUtils::validate_and_populate_input_paths(
        std::vector<std::string> const& input,
        std::vector<std::string>& validated_input,
        InputOption const& config
) {
    if (CommandLineArguments::InputSource::Filesystem == config.source) {
        if (false == FileUtils::validate_path(input)) {
            return false;
        }
        for (auto& file_path : input) {
            FileUtils::find_all_files(file_path, validated_input);
        }
    } else if (CommandLineArguments::InputSource::S3 == config.source) {
        clp::aws::AwsAuthenticationSigner signer{
                config.s3_config.access_key_id,
                config.s3_config.secret_access_key
        };
        try {
            for (auto const& url : input) {
                std::string signed_url;
                clp::aws::S3Url s3_url{url};
                auto rc = signer.generate_presigned_url(s3_url, signed_url);
                if (clp::ErrorCode::ErrorCode_Success != rc) {
                    SPDLOG_ERROR("Failed to sign S3 URL - {} - {}", rc, url);
                    return false;
                }
                validated_input.emplace_back(signed_url);
            }
        } catch (std::exception const& e) {
            SPDLOG_ERROR("Encountered error while signing S3 URLs - {}", e.what());
            return false;
        }
    } else {
        return false;
    }
    return true;
}

namespace {
std::shared_ptr<clp::ReaderInterface> try_create_file_reader(std::string const& file_path) {
    try {
        return std::make_shared<clp::FileReader>(file_path);
    } catch (clp::FileReader::OperationFailed const& e) {
        SPDLOG_ERROR("Failed to open file for reading - {} - {}", file_path, e.what());
        return nullptr;
    }
}

std::shared_ptr<clp::ReaderInterface> try_create_network_reader(std::string const& url) {
    try {
        return std::make_shared<clp::NetworkReader>(url);
    } catch (clp::NetworkReader::OperationFailed const& e) {
        SPDLOG_ERROR("Failed to open url for reading - {}", e.what());
        return nullptr;
    }
}
}  // namespace

std::shared_ptr<clp::ReaderInterface>
ReaderUtils::try_create_reader(std::string const& path, InputOption const& config) {
    if (CommandLineArguments::InputSource::Filesystem == config.source) {
        return try_create_file_reader(path);
    } else if (CommandLineArguments::InputSource::S3 == config.source) {
        return try_create_network_reader(path);
    } else {
        return nullptr;
    }
}
}  // namespace clp_s
