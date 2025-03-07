#include "spider_tasks.hpp"

#include <stdio.h>
#include <sys/stat.h>

#include <exception>
#include <filesystem>
#include <spider/client/spider.hpp>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <curl/curl.h>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "../clp/aws/AwsAuthenticationSigner.hpp"
#include "../clp/CurlEasyHandle.hpp"
#include "../clp/CurlGlobalInstance.hpp"
#include "ArchiveReader.hpp"
#include "CommandLineArguments.hpp"
#include "Defs.hpp"
#include "InputConfig.hpp"
#include "JsonParser.hpp"
#include "Utils.hpp"

namespace terrablob {

std::string get_upload_name_from_path(std::filesystem::path archive_path) {
    // Extract timestamp range metadata by simply reading archive metadata
    std::string upload_archive_name = "0-" + std::to_string(clp_s::cEpochTimeMax);
    clp_s::ArchiveReader reader;
    auto path
            = clp_s::Path{.source = clp_s::InputSource::Filesystem, .path = archive_path.string()};
    reader.open(path, clp_s::NetworkAuthOption{});
    auto timestamp_dict = reader.get_timestamp_dictionary();
    auto it = timestamp_dict->tokenized_column_to_range_begin();
    if (timestamp_dict->tokenized_column_to_range_end() != it) {
        auto range = it->second;
        upload_archive_name = std::to_string(range->get_begin_timestamp()) + "-"
                  + std::to_string(range->get_end_timestamp());
    }
    reader.close();
    return upload_archive_name;
}

bool upload_all_files_in_directory(std::filesystem::path const& archive_dir_path, std::string_view topic) {
    std::vector<std::string> file_paths;

    clp_s::FileUtils::find_all_files_in_directory(archive_dir_path, file_paths);

    for (auto const& path : file_paths) {
        FILE* fd = fopen(path.c_str(), "rb");
        struct stat file_info;
        if (nullptr == fd) {
            return false;
        }

        if (0 != fstat(fileno(fd), &file_info)) {
            fclose(fd);
            return false;
        }

        std::filesystem::path archive_path{path};
        std::string upload_archive_name = get_upload_name_from_path(archive_path);
        // Get the first timestamp in the file name and extract YYYY/MM/DD from it
        size_t dash_pos = upload_archive_name.find('-');
        if (std::string::npos == dash_pos) {
            fclose(fd);
            return false;
        }

        time_t min_ts = std::stoull(upload_archive_name.substr(0, dash_pos));
        struct tm time_info;
        gmtime_r(&min_ts, &time_info);
        size_t year = time_info.tm_year + 1900;
        size_t month = time_info.tm_mon + 1;
        size_t day = time_info.tm_mday;
        system(fmt::format("tb-cli put -t 9999s {} /prod/personal/jluo14/presto/demo-clp-presto-velox-staging/{}/{}/{}/{}/{}", path, topic, year, month, day, upload_archive_name).c_str());

        fclose(fd);
    }
    return true;
}

void cleanup_generated_archives_and_schema_files(std::string archives_path) {
    std::error_code ec;
    std::filesystem::remove_all(std::filesystem::path(archives_path), ec);
    if (ec) {
        SPDLOG_ERROR("Failed to clean up archives path: ({}) {}", ec.value(), ec.message());
    }
}

size_t get_size_of_buffered_raw_data(std::filesystem::path const& raw_data_path) {
    std::filesystem::path parent = raw_data_path.parent_path();
    size_t total_size{0};
    for (auto const& entry : std::filesystem::directory_iterator(parent)) {
        if (std::filesystem::is_regular_file(entry)) {
            total_size += std::filesystem::file_size(entry);
        }
    }
    return total_size;
}

}  // namespace

// Task function implementation
int compress(
        spider::TaskContext& context,
        std::string_view topic,
        uint8_t input_file_type_uint8,
        std::string_view timestamp_key,
        size_t buffer_size,
        std::string_view raw_data_path_str
) {
    clp_s::FileType input_file_type{input_file_type_uint8};
    std::filesystem::path raw_data_path{raw_data_path_str};
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    if (buffer_size > terrablob::get_size_of_buffered_raw_data(raw_data_path)) {
        return 1;
    }

    std::filesystem::path archive_dir_path{fmt::format("/tmp/{}-archives-{}/", std::string(topic), boost::uuids::to_string(context.get_id()))};
    clp_s::JsonParserOption option{};
    option.input_file_type = input_file_type;
    option.timestamp_key = std::string(timestamp_key);
    option.archives_dir = archive_dir_path;
    option.target_encoded_size = 256 * 1024 * 1024;
    option.max_document_size = 512 * 1024 * 1024;
    option.min_table_size = 1 * 1024 * 1024;
    option.compression_level = 3;
    option.single_file_archive = true;
    option.network_auth = clp_s::NetworkAuthOption{};

    std::filesystem::path parent = raw_data_path.parent_path();
    for (auto const& entry : std::filesystem::directory_iterator(parent)) {
        if (std::filesystem::is_regular_file(entry)) {
            option.input_paths.emplace_back(clp_s::Path{.source = clp_s::InputSource::Filesystem, .path = std::string(entry.path())});
        }
    }

    std::vector<std::string> parsed_raw_data_paths;
    try {
        std::filesystem::create_directory(option.archives_dir);
        clp_s::JsonParser parser{option};
        if (clp_s::FileType::KeyValueIr == option.input_file_type) {
            if (false == parser.parse_from_ir(&parsed_raw_data_paths)) {
                throw std::runtime_error("Encountered error during parsing.");
            }
        } else {
            if (false == parser.parse(&parsed_raw_data_paths)) {
                throw std::runtime_error("Encountered error during parsing.");
            }
        }
        parser.store();

        // trigger upload
        if (false == terrablob::upload_all_files_in_directory(option.archives_dir, topic)) {
            throw std::runtime_error("Encountered error during upload.");
        }
    } catch (std::exception const& e) {
        terrablob::cleanup_generated_archives_and_schema_files(option.archives_dir);
        return 2;
    }

    // TODO log the ingested file
    terrablob::cleanup_generated_archives_and_schema_files(option.archives_dir);
    return 0;
}

// Register the task with Spider
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(compress);
